//! Unit tests of the podcast-discovery overlay: records, ordering, cache and
//! search aggregation. Multi-node behavior is in `tests/podcast_dht.rs`.

use federation_net::{NetworkId, SecretKey};

use crate::routing::NodeId;

use super::search::{aggregate_hits, rank_hits};
use super::*;
use crate::podcast::{FeedUrl, PodcastMeta};
use crate::record::{ItemId, ItemKind, LibraryItem};

const NOW: u64 = 1_750_000_000_000;

fn network() -> NetworkId {
    NetworkId::from_name("podcast-dht-unit")
}

fn secret(seed: u8) -> SecretKey {
    SecretKey::from_bytes(&[seed; 32])
}

fn feed(path: &str) -> FeedUrl {
    FeedUrl::parse(&format!("https://feeds.example.test/{path}")).unwrap()
}

fn spec(path: &str, title: &str) -> PodcastCardSpec {
    let feed_url = feed(path);
    PodcastCardSpec {
        podcast_id: PodcastId::from_feed_url(&feed_url),
        feed_url,
        id_feed_url: None,
        title: title.to_string(),
        author: Some("Jane Doe".to_string()),
        description: Some("A show about\nthings.".to_string()),
        image_url: Some("https://cdn.example.test/art.jpg".to_string()),
        link: Some("https://example.test/show".to_string()),
        language: Some("en".to_string()),
        categories: vec!["Technology".to_string()],
        episode_count: Some(12),
        latest_episode_at_ms: Some(1_700_000_000_000),
    }
}

fn card_with(
    spec: PodcastCardSpec,
    key: &SecretKey,
    revision: u64,
    issued: u64,
) -> SignedPodcastRecord {
    let card = PodcastCard::from_spec(spec, network(), key.public(), revision, issued);
    SignedPodcastRecord::sign_card(card, |bytes| key.sign(bytes)).unwrap()
}

fn card(key: &SecretKey, revision: u64) -> SignedPodcastRecord {
    card_with(spec("show.xml", "Rust Radio"), key, revision, NOW)
}

fn tombstone(key: &SecretKey, revision: u64, issued: u64) -> SignedPodcastRecord {
    let retracted = card(key, 1);
    let tombstone = PodcastTombstone::new(
        network(),
        key.public(),
        retracted.podcast_id().clone(),
        revision,
        issued,
        retracted.dht_keys(),
    );
    SignedPodcastRecord::sign_tombstone(tombstone, |bytes| key.sign(bytes)).unwrap()
}

fn resign(record: &mut SignedPodcastRecord, key: &SecretKey) {
    record.signature = key.sign(&record.signing_bytes());
}

fn subscription(spec: &PodcastCardSpec) -> SubscriptionRecord {
    SubscriptionRecord {
        podcast_id: spec.podcast_id.clone(),
        subscribed: true,
        podcast: PodcastMeta {
            feed_url: spec.feed_url.clone(),
            title: spec.title.clone(),
            author: spec.author.clone(),
            description: spec.description.clone(),
            link: spec.link.clone(),
            image_url: spec.image_url.clone(),
        },
        feed_aliases: Vec::new(),
        generation: 1,
        hlc_ms: 100,
        op_id: "private-op-id-123".to_string(),
    }
}

// ---------------------------------------------------------------- signatures

#[test]
fn signed_card_verifies_and_rejects_tampering() {
    let key = secret(1);
    let record = card(&key, 1);
    record.verify_at(&network(), NOW).unwrap();

    let mut tampered = record.clone();
    if let PodcastRecordBody::Card(card) = &mut tampered.body {
        card.title = "Evil Radio".to_string();
    }
    assert!(tampered.verify(&network()).is_err());

    let mut moved = record.clone();
    if let PodcastRecordBody::Card(card) = &mut moved.body {
        card.revision += 1;
    }
    assert!(moved.verify(&network()).is_err());

    assert!(record.verify(&NetworkId::from_name("other")).is_err());
}

#[test]
fn record_signed_by_another_key_is_rejected() {
    let publisher = secret(1);
    let forger = secret(2);
    let card = PodcastCard::from_spec(
        spec("show.xml", "Rust Radio"),
        network(),
        publisher.public(),
        1,
        NOW,
    );
    let forged = SignedPodcastRecord::sign_card(card, |bytes| forger.sign(bytes)).unwrap();
    assert!(forged.verify(&network()).is_err());

    let mut forged_tombstone = tombstone(&forger, 2, NOW);
    if let PodcastRecordBody::Tombstone(tombstone) = &mut forged_tombstone.body {
        tombstone.publisher = publisher.public();
    }
    resign(&mut forged_tombstone, &forger);
    assert!(forged_tombstone.verify(&network()).is_err());
}

#[test]
fn canonical_encoding_is_stable() {
    let key = secret(9);
    let record = card_with(spec("vector.xml", "Vector Show"), &key, 7, NOW);
    let bytes = record.signing_bytes();
    assert!(bytes.starts_with(b"furumi-podcast-card:v1\0\x01\x00"));
    assert_eq!(
        blake3::hash(&bytes).to_hex().as_str(),
        "cbbc512b077ad05baf01728fb5d179a1d89301f19ea15c66058df284a716e9b5",
        "card signing input changed: this breaks every published card"
    );
    let tombstone = PodcastTombstone::new(
        network(),
        key.public(),
        record.podcast_id().clone(),
        8,
        NOW,
        record.dht_keys(),
    );
    let bytes = tombstone.signing_bytes();
    assert!(bytes.starts_with(b"furumi-podcast-card-tombstone:v1\0\x01\x00"));
    assert_eq!(
        blake3::hash(&bytes).to_hex().as_str(),
        "032ccef911bff5f2f37367137d428d60c111efb65529c8d75754434434dbd083",
        "tombstone signing input changed"
    );
    assert_eq!(
        hex(PodcastDhtKey::token(&network(), "vector").as_bytes()),
        "4367d03f8ea2a541ebec43b73d767b42ad733e3f26f90372cff9f92cf19ccbf1"
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn optional_fields_are_unambiguous_in_the_signing_input() {
    let key = secret(1);
    let mut a = spec("show.xml", "Rust Radio");
    a.author = Some(String::new());
    let mut b = spec("show.xml", "Rust Radio");
    b.author = None;
    let a = PodcastCard::from_spec(a, network(), key.public(), 1, NOW);
    let b = PodcastCard::from_spec(b, network(), key.public(), 1, NOW);
    assert_ne!(a.signing_bytes(), b.signing_bytes());
}

// -------------------------------------------------------------------- bounds

#[test]
fn card_text_bounds_accept_the_limit_and_reject_one_more() {
    let key = secret(1);
    let sign = |spec: PodcastCardSpec| {
        let card = PodcastCard::from_spec(spec, network(), key.public(), 1, NOW);
        SignedPodcastRecord::sign_card(card, |bytes| key.sign(bytes))
    };
    let mut at = spec("show.xml", &"я".repeat(MAX_CARD_TITLE_CHARS));
    at.author = Some("a".repeat(MAX_CARD_AUTHOR_CHARS));
    at.description = Some("d".repeat(MAX_CARD_DESCRIPTION_CHARS));
    at.categories = (0..MAX_CARD_CATEGORIES)
        .map(|index| format!("{index}{}", "c".repeat(MAX_CARD_CATEGORY_CHARS - 1)))
        .collect();
    at.language = Some("x".repeat(MAX_CARD_LANGUAGE_BYTES));
    let record = sign(at.clone()).unwrap();
    assert!(record.encoded_len() <= MAX_PODCAST_RECORD_BYTES);

    let mut over = at.clone();
    over.title.push('я');
    assert!(sign(over).is_err());
    let mut over = at.clone();
    over.author.as_mut().unwrap().push('a');
    assert!(sign(over).is_err());
    let mut over = at.clone();
    over.description.as_mut().unwrap().push('d');
    assert!(sign(over).is_err());
    let mut over = at.clone();
    over.categories.push("extra".into());
    assert!(sign(over).is_err());
    let mut over = at.clone();
    over.categories[0].push('c');
    assert!(sign(over).is_err());
    let mut over = at.clone();
    over.language.as_mut().unwrap().push('x');
    assert!(sign(over).is_err());
    let mut over = at;
    over.title = " ".into();
    assert!(sign(over).is_err());
}

#[test]
fn card_rejects_bad_urls_ids_and_control_characters() {
    let key = secret(1);
    let sign = |spec: PodcastCardSpec| {
        let card = PodcastCard::from_spec(spec, network(), key.public(), 1, NOW);
        SignedPodcastRecord::sign_card(card, |bytes| key.sign(bytes))
    };
    let mut bad = spec("show.xml", "Rust Radio");
    bad.image_url = Some("file:///etc/passwd".into());
    assert!(sign(bad).is_err());
    let mut bad = spec("show.xml", "Rust Radio");
    bad.podcast_id = PodcastId::from_feed_url(&feed("other.xml"));
    assert!(sign(bad).is_err(), "id must hash from a URL in the card");
    let mut bad = spec("show.xml", "Rust Radio");
    bad.title = "Rust\u{7}Radio".into();
    assert!(sign(bad).is_err());
    let mut bad = spec("show.xml", "Rust Radio");
    bad.id_feed_url = Some(bad.feed_url.clone());
    assert!(sign(bad).is_err());

    let mut moved = spec("new.xml", "Rust Radio");
    moved.id_feed_url = Some(feed("show.xml"));
    moved.podcast_id = PodcastId::from_feed_url(&feed("show.xml"));
    sign(moved).unwrap();
}

#[test]
fn bounded_spec_fits_the_card() {
    let mut raw = spec("show.xml", &format!("  {}\n", "t".repeat(2_000)));
    raw.author = Some("\u{1}".into());
    raw.description = Some(format!("{}\u{0}", "d".repeat(5_000)));
    raw.image_url = Some("not a url".into());
    raw.language = Some("en US".into());
    raw.categories = (0..20).map(|index| format!("Cat {}", index % 10)).collect();
    raw.categories.push("cat 1".into());
    raw.latest_episode_at_ms = Some(-5);
    let bounded = raw.bounded();
    bounded.validate().unwrap();
    assert_eq!(bounded.title.chars().count(), MAX_CARD_TITLE_CHARS);
    assert_eq!(bounded.author, None);
    assert_eq!(bounded.image_url, None);
    assert_eq!(bounded.language, None);
    assert_eq!(bounded.categories.len(), MAX_CARD_CATEGORIES);
    assert_eq!(bounded.latest_episode_at_ms, None);
    assert_eq!(bounded.bounded(), bounded);
}

#[test]
fn index_tokens_are_distinct_bounded_and_skip_long_tokens() {
    let long = "x".repeat(MAX_INDEX_TOKEN_BYTES + 1);
    let title = format!("The Rust the {long} radio");
    assert_eq!(
        card_index_tokens(&title, Some("Rust Jane")),
        vec!["the", "rust", "radio", "jane"]
    );
    let many: Vec<String> = (0..50).map(|index| format!("w{index}")).collect();
    let tokens = card_index_tokens(&many.join(" "), Some("author"));
    assert_eq!(tokens.len(), MAX_CARD_INDEX_TOKENS);
    assert!(!tokens.contains(&"author".to_string()));
    let record = card_with(spec("show.xml", &many.join(" ")), &secret(1), 1, NOW);
    assert_eq!(record.dht_keys().len(), MAX_RECORD_DHT_KEYS);
}

#[test]
fn oversized_tombstone_key_lists_are_rejected() {
    let key = secret(1);
    let id = PodcastId::from_feed_url(&feed("show.xml"));
    let keys: Vec<_> = (0..=MAX_CARD_INDEX_TOKENS)
        .map(|index| PodcastDhtKey::token(&network(), &format!("t{index}")))
        .collect();
    let mut tombstone = PodcastTombstone::new(network(), key.public(), id.clone(), 2, NOW, keys);
    assert_eq!(tombstone.index_keys.len(), MAX_CARD_INDEX_TOKENS);
    SignedPodcastRecord::sign_tombstone(tombstone.clone(), |bytes| key.sign(bytes)).unwrap();
    tombstone
        .index_keys
        .push(PodcastDhtKey::from_bytes([0xff; 32]));
    assert!(
        SignedPodcastRecord::sign_tombstone(tombstone.clone(), |bytes| key.sign(bytes)).is_err()
    );
    tombstone.index_keys.pop();
    tombstone.index_keys.reverse();
    assert!(
        SignedPodcastRecord::sign_tombstone(tombstone, |bytes| key.sign(bytes)).is_err(),
        "unsorted keys are not canonical"
    );
}

// ------------------------------------------------------------------ lifetime

#[test]
fn expiry_future_issue_and_overlong_lifetimes_are_rejected() {
    let key = secret(1);
    let record = card(&key, 1);
    let ttl = PODCAST_CARD_TTL.as_millis() as u64;
    record.verify_at(&network(), NOW + ttl - 1).unwrap();
    assert!(record.verify_at(&network(), NOW + ttl).is_err());

    let skew = PODCAST_DHT_MAX_CLOCK_SKEW.as_millis() as u64;
    assert!(record.verify_at(&network(), NOW - skew).is_ok());
    assert!(record.verify_at(&network(), NOW - skew - 1).is_err());

    let mut overlong = record.clone();
    if let PodcastRecordBody::Card(card) = &mut overlong.body {
        card.expires_at_ms += 1;
    }
    resign(&mut overlong, &key);
    assert!(overlong.verify(&network()).is_err());

    let grave = tombstone(&key, 2, NOW);
    let tomb_ttl = PODCAST_TOMBSTONE_TTL.as_millis() as u64;
    assert!(tomb_ttl > ttl);
    grave.verify_at(&network(), NOW + tomb_ttl - 1).unwrap();
    assert!(grave.verify_at(&network(), NOW + tomb_ttl).is_err());
}

// ------------------------------------------------------------------ ordering

#[test]
fn record_order_is_total_and_tombstones_win_ties() {
    let key = secret(1);
    let older = card(&key, 1);
    let newer = card(&key, 2);
    let refresh = card_with(spec("show.xml", "Rust Radio"), &key, 1, NOW + 1);
    let edited = card_with(spec("show.xml", "Rust Radio!"), &key, 1, NOW);
    let grave = tombstone(&key, 1, NOW);

    assert!(compare_records(&newer, &older).is_gt());
    assert!(compare_records(&refresh, &older).is_gt());
    assert!(compare_records(&grave, &newer).is_lt());
    assert!(compare_records(&grave, &older).is_gt());
    assert!(compare_records(&grave, &refresh).is_gt());
    assert_ne!(compare_records(&edited, &older), std::cmp::Ordering::Equal);
    assert_eq!(
        compare_records(&edited, &older),
        compare_records(&older, &edited).reverse()
    );
}

// --------------------------------------------------------------------- cache

async fn open_db(temp: &tempfile::TempDir) -> store::PodcastDhtDatabase {
    store::PodcastDhtDatabase::open(&temp.path().join("podcast-dht.sqlite3"), network())
        .await
        .unwrap()
}

fn entry(record: &SignedPodcastRecord) -> (Vec<PodcastDhtKey>, SignedPodcastRecord) {
    (record.dht_keys(), record.clone())
}

#[tokio::test]
async fn cache_replay_of_an_older_revision_is_ignored() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let key = secret(1);
    let newer = card(&key, 2);
    let older = card(&key, 1);
    let id_key = PodcastDhtKey::podcast(&network(), newer.podcast_id());
    assert!(db.store(vec![entry(&newer)], NOW).await.unwrap() > 0);
    assert_eq!(db.store(vec![entry(&older)], NOW).await.unwrap(), 0);
    assert_eq!(
        db.records_by_key(id_key, Vec::new(), NOW, 10)
            .await
            .unwrap(),
        vec![newer.clone()]
    );
    // Storing the same record again changes nothing.
    assert_eq!(db.store(vec![entry(&newer)], NOW).await.unwrap(), 0);
}

#[tokio::test]
async fn cache_converges_regardless_of_delivery_order() {
    let key = secret(1);
    let records = [
        card(&key, 1),
        card_with(spec("show.xml", "Rust Radio"), &key, 1, NOW + 5),
        card_with(spec("show.xml", "Rust Radio Weekly"), &key, 2, NOW),
        tombstone(&key, 2, NOW + 1),
        card_with(spec("show.xml", "Rust Radio"), &key, 3, NOW + 2),
    ];
    let id_key = PodcastDhtKey::podcast(&network(), records[0].podcast_id());
    let mut results = Vec::new();
    for order in [
        [0, 1, 2, 3, 4],
        [4, 3, 2, 1, 0],
        [3, 0, 4, 2, 1],
        [2, 4, 0, 3, 1],
    ] {
        let temp = tempfile::tempdir().unwrap();
        let db = open_db(&temp).await;
        for index in order {
            db.store(vec![entry(&records[index])], NOW).await.unwrap();
        }
        results.push(
            db.records_by_key(id_key, Vec::new(), NOW, 10)
                .await
                .unwrap(),
        );
    }
    assert!(
        results
            .iter()
            .all(|result| *result == vec![records[4].clone()])
    );
}

#[tokio::test]
async fn cache_tombstone_supersedes_lower_and_equal_revisions_under_every_key_it_holds() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let key = secret(1);
    let active = card(&key, 1);
    db.store(vec![entry(&active)], NOW).await.unwrap();
    let token_key = PodcastDhtKey::token(&network(), "rust");
    assert_eq!(
        db.records_by_key(token_key, vec!["rust".into()], NOW, 10)
            .await
            .unwrap(),
        vec![active.clone()]
    );

    // A tombstone stored only under the id key still replaces the card under
    // the text keys of this node.
    let grave = tombstone(&key, 2, NOW);
    let id_key = PodcastDhtKey::podcast(&network(), grave.podcast_id());
    db.store(vec![(vec![id_key], grave.clone())], NOW)
        .await
        .unwrap();
    assert_eq!(
        db.records_by_key(token_key, vec!["rust".into()], NOW, 10)
            .await
            .unwrap(),
        vec![grave.clone()]
    );
    // An equal-revision card arriving later loses, under any key.
    let same = card(&key, 2);
    assert_eq!(db.store(vec![entry(&same)], NOW).await.unwrap(), 0);
    assert_eq!(
        db.records_by_key(id_key, Vec::new(), NOW, 10)
            .await
            .unwrap(),
        vec![grave.clone()]
    );
    // The tombstone is retained (and served) until it expires.
    let card_ttl = PODCAST_CARD_TTL.as_millis() as u64;
    assert_eq!(
        db.records_by_key(id_key, Vec::new(), NOW + card_ttl + 1, 10)
            .await
            .unwrap(),
        vec![grave.clone()]
    );
    // Republishing with a higher revision wins over the tombstone.
    let again = card(&key, 3);
    assert!(db.store(vec![entry(&again)], NOW).await.unwrap() > 0);
    assert_eq!(
        db.records_by_key(id_key, Vec::new(), NOW, 10)
            .await
            .unwrap(),
        vec![again]
    );
}

#[tokio::test]
async fn cache_drops_stale_text_keys_of_older_revisions() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let key = secret(1);
    let old = card_with(spec("show.xml", "Rust Radio"), &key, 1, NOW);
    let renamed = card_with(spec("show.xml", "Ferris Hour"), &key, 2, NOW);
    db.store(vec![entry(&old), entry(&renamed)], NOW)
        .await
        .unwrap();
    let rust = PodcastDhtKey::token(&network(), "rust");
    let ferris = PodcastDhtKey::token(&network(), "ferris");
    assert!(
        db.records_by_key(rust, Vec::new(), NOW, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.records_by_key(ferris, Vec::new(), NOW, 10)
            .await
            .unwrap(),
        vec![renamed]
    );
}

#[tokio::test]
async fn cache_read_path_filters_tokens_expiry_and_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let key = secret(1);
    let record = card(&key, 1);
    db.store(vec![entry(&record)], NOW).await.unwrap();
    let rust = PodcastDhtKey::token(&network(), "rust");
    assert_eq!(
        db.records_by_key(rust, vec!["rust".into(), "radio".into()], NOW, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        db.records_by_key(rust, vec!["rust".into(), "jazz".into()], NOW, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let ttl = PODCAST_CARD_TTL.as_millis() as u64;
    assert!(
        db.records_by_key(rust, Vec::new(), NOW + ttl, 10)
            .await
            .unwrap()
            .is_empty()
    );

    // A row whose payload no longer verifies is never served.
    let conn = rusqlite::Connection::open(temp.path().join("podcast-dht.sqlite3")).unwrap();
    let mut tampered = record.clone();
    if let PodcastRecordBody::Card(card) = &mut tampered.body {
        card.title = "Rust Radio Evil".into();
    }
    conn.execute(
        "UPDATE podcast_dht_records SET payload = ?1",
        [postcard::to_stdvec(&tampered).unwrap()],
    )
    .unwrap();
    assert!(
        db.records_by_key(rust, Vec::new(), NOW, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(db.delete_expired(NOW + ttl).await.unwrap(), 1);
}

fn numbered_secret(index: usize) -> SecretKey {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&(index as u64 + 1).to_le_bytes());
    bytes[31] = 0x5a;
    SecretKey::from_bytes(&bytes)
}

async fn open_db_with(
    temp: &tempfile::TempDir,
    limits: store::StoreLimits,
) -> store::PodcastDhtDatabase {
    store::PodcastDhtDatabase::open_with_limits(
        &temp.path().join("podcast-dht.sqlite3"),
        network(),
        limits,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn cache_full_key_keeps_its_records_and_refuses_newcomers() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let token_key = PodcastDhtKey::token(&network(), "rust");
    let mut entries = Vec::new();
    for index in 0..=MAX_PODCAST_RECORDS_PER_KEY {
        // Later publishers expire later; that buys them nothing.
        let record = card_with(
            spec("show.xml", "Rust Radio"),
            &numbered_secret(index),
            1,
            NOW + index as u64,
        );
        entries.push((vec![token_key], record));
    }
    let first = entries[0].1.clone();
    let last = entries[MAX_PODCAST_RECORDS_PER_KEY].1.clone();
    // The first publisher arrived first; the rest a moment later.
    db.store(entries[..1].to_vec(), NOW).await.unwrap();
    for chunk in entries[1..].chunks(64) {
        db.store(chunk.to_vec(), NOW + 1).await.unwrap();
    }
    let cached = db
        .records_by_key(token_key, Vec::new(), NOW, usize::MAX)
        .await
        .unwrap();
    assert_eq!(cached.len(), MAX_PODCAST_RECORDS_PER_KEY);
    assert!(cached.contains(&first));
    assert!(!cached.contains(&last));
    assert_eq!(cached[0], first, "records indexed longest come first");
    // The first publisher keeps refreshing its place.
    let refresh = card_with(
        spec("show.xml", "Rust Radio"),
        &numbered_secret(0),
        1,
        NOW + 1_000,
    );
    assert_eq!(
        db.store(vec![(vec![token_key], refresh)], NOW)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn sybil_tombstones_cannot_suppress_a_card() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let victim = card(&secret(1), 1);
    let id_key = PodcastDhtKey::podcast(&network(), victim.podcast_id());
    let rust = PodcastDhtKey::token(&network(), "rust");
    db.store(vec![entry(&victim)], NOW).await.unwrap();

    // 256 throwaway identities sign tombstones of the victim's podcast that
    // list the victim card's text keys, and issue them as late as allowed.
    let skew = PODCAST_DHT_MAX_CLOCK_SKEW.as_millis() as u64;
    let sybils: Vec<_> = (0..256)
        .map(|index| {
            let key = numbered_secret(index);
            let tombstone = PodcastTombstone::new(
                network(),
                key.public(),
                victim.podcast_id().clone(),
                u64::MAX,
                NOW + skew,
                victim.dht_keys(),
            );
            let record =
                SignedPodcastRecord::sign_tombstone(tombstone, |bytes| key.sign(bytes)).unwrap();
            (record.dht_keys(), record)
        })
        .collect();
    let mut written = 0;
    for chunk in sybils.chunks(MAX_PODCAST_STORE_RECORDS) {
        written += db.store(chunk.to_vec(), NOW).await.unwrap();
    }
    assert_eq!(
        written, MAX_PODCAST_TOMBSTONES_PER_KEY,
        "only the id key's tombstone budget is used"
    );

    let by_id = db
        .records_by_key(id_key, Vec::new(), NOW, MAX_PODCAST_DHT_RECORDS)
        .await
        .unwrap();
    assert!(by_id.contains(&victim));
    assert_eq!(
        by_id.iter().filter(|record| record.is_tombstone()).count(),
        MAX_PODCAST_RESPONSE_TOMBSTONES
    );
    assert_eq!(
        db.records_by_key(rust, vec!["rust".into()], NOW, MAX_PODCAST_DHT_RECORDS)
            .await
            .unwrap(),
        vec![victim.clone()],
        "no tombstone is indexed under a text key without its owner's card"
    );
    // Eleven hours later the victim's republish still lands.
    let later = NOW + 11 * 3_600_000;
    let republished = card_with(spec("show.xml", "Rust Radio"), &secret(1), 1, later);
    assert_eq!(
        db.store(vec![entry(&republished)], later).await.unwrap(),
        republished.dht_keys().len()
    );
    // Sybil cards issued in the future cannot displace it either.
    let sybil_cards: Vec<_> = (300..600)
        .map(|index| {
            entry(&card_with(
                spec("show.xml", "Rust Radio"),
                &numbered_secret(index),
                u64::MAX,
                later + skew,
            ))
        })
        .collect();
    for chunk in sybil_cards.chunks(MAX_PODCAST_STORE_RECORDS) {
        db.store(chunk.to_vec(), later).await.unwrap();
    }
    for key in [id_key, rust] {
        let records = db
            .records_by_key(key, vec![], later, MAX_PODCAST_DHT_RECORDS)
            .await
            .unwrap();
        assert!(records.contains(&republished));
        assert!(records.len() <= MAX_PODCAST_DHT_RECORDS);
    }
}

#[tokio::test]
async fn cache_enforces_publisher_record_and_byte_quotas() {
    let limits = store::StoreLimits {
        per_key: 100,
        tombstones_per_key: 10,
        per_publisher: 3,
        records: 5,
        bytes: usize::MAX,
    };
    let temp = tempfile::tempdir().unwrap();
    let db = open_db_with(&temp, limits).await;
    let show = |index: usize, key: &SecretKey| {
        entry(&card_with(
            spec(&format!("show-{index}.xml"), "Rust Radio"),
            key,
            1,
            NOW,
        ))
    };
    let heavy = secret(1);
    let stored: Vec<usize> = {
        let mut out = Vec::new();
        for index in 0..4 {
            out.push(db.store(vec![show(index, &heavy)], NOW).await.unwrap());
        }
        out
    };
    assert!(stored[..3].iter().all(|written| *written > 0));
    assert_eq!(
        stored[3], 0,
        "the fourth podcast of one publisher is refused"
    );
    // A refresh of an admitted record is always accepted.
    let refresh = card_with(spec("show-0.xml", "Rust Radio"), &heavy, 1, NOW + 1);
    assert!(db.store(vec![entry(&refresh)], NOW).await.unwrap() > 0);
    assert!(db.store(vec![show(10, &secret(2))], NOW).await.unwrap() > 0);
    assert!(db.store(vec![show(11, &secret(3))], NOW).await.unwrap() > 0);
    assert_eq!(
        db.store(vec![show(12, &secret(4))], NOW).await.unwrap(),
        0,
        "the cache holds at most five records"
    );

    let one = card(&secret(1), 1);
    let limits = store::StoreLimits {
        bytes: one.encoded_len() * 2,
        ..store::StoreLimits::default()
    };
    let temp = tempfile::tempdir().unwrap();
    let db = open_db_with(&temp, limits).await;
    assert!(
        db.store(vec![entry(&card(&secret(1), 1))], NOW)
            .await
            .unwrap()
            > 0
    );
    assert!(
        db.store(vec![entry(&card(&secret(2), 1))], NOW)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(
        db.store(vec![entry(&card(&secret(3), 1))], NOW)
            .await
            .unwrap(),
        0,
        "the byte budget is full"
    );
}

#[tokio::test]
async fn cache_stores_each_payload_once() {
    let temp = tempfile::tempdir().unwrap();
    let db = open_db(&temp).await;
    let many: Vec<String> = (0..50).map(|index| format!("w{index}")).collect();
    let record = card_with(spec("show.xml", &many.join(" ")), &secret(1), 1, NOW);
    assert_eq!(
        db.store(vec![entry(&record)], NOW).await.unwrap(),
        MAX_RECORD_DHT_KEYS
    );
    let conn = rusqlite::Connection::open(temp.path().join("podcast-dht.sqlite3")).unwrap();
    let payloads: i64 = conn
        .query_row("SELECT COUNT(*) FROM podcast_dht_records", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(payloads, 1);
}

#[tokio::test]
async fn cache_refuses_a_newer_schema_and_keeps_local_publications() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("podcast-dht.sqlite3");
    let key = secret(1);
    let record = card(&key, 5);
    {
        let db = store::PodcastDhtDatabase::open(&path, network())
            .await
            .unwrap();
        db.put_local_publication(
            store::LocalPublication {
                podcast_id: record.podcast_id().clone(),
                revision: 5,
                spec: Some(record.card().unwrap().spec()),
                record: record.clone(),
            },
            NOW,
        )
        .await
        .unwrap();
    }
    let db = store::PodcastDhtDatabase::open(&path, network())
        .await
        .unwrap();
    let local = db.local_publications().await.unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].revision, 5);
    assert_eq!(local[0].record, record);
    drop(db);

    // A pre-release version-1 replica table is dropped; own publications
    // survive.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP TABLE podcast_dht_records;
             CREATE TABLE podcast_dht_records (dht_key BLOB, payload BLOB);
             INSERT INTO podcast_dht_records VALUES (x'00', x'00');
             PRAGMA user_version = 1;",
        )
        .unwrap();
    }
    let db = store::PodcastDhtDatabase::open(&path, network())
        .await
        .unwrap();
    assert_eq!(db.local_publications().await.unwrap().len(), 1);
    assert!(db.store(vec![entry(&record)], NOW).await.unwrap() > 0);
    drop(db);

    rusqlite::Connection::open(&path)
        .unwrap()
        .pragma_update(None, "user_version", PODCAST_DHT_CACHE_SCHEMA_VERSION + 1)
        .unwrap();
    assert!(
        store::PodcastDhtDatabase::open(&path, network())
            .await
            .is_err()
    );
}

// ------------------------------------------------------------------ batches

#[test]
fn store_batches_are_bounded_and_keys_must_belong() {
    let key = secret(1);
    let record = card(&key, 1);
    let good = PodcastStoreEntry {
        keys: record.dht_keys(),
        record: record.clone(),
    };
    let publisher = key.public();
    validate_store_batch(vec![good.clone()], &network(), publisher, NOW).unwrap();
    assert!(validate_store_batch(Vec::new(), &network(), publisher, NOW).is_err());
    assert!(
        validate_store_batch(
            vec![good.clone(); MAX_PODCAST_STORE_RECORDS],
            &network(),
            publisher,
            NOW
        )
        .is_ok()
    );
    assert!(
        validate_store_batch(
            vec![good.clone(); MAX_PODCAST_STORE_RECORDS + 1],
            &network(),
            publisher,
            NOW
        )
        .is_err()
    );
    let mut foreign = good.clone();
    foreign.keys = vec![PodcastDhtKey::token(&network(), "jazz")];
    assert!(validate_store_batch(vec![foreign], &network(), publisher, NOW).is_err());
    let mut duplicated = good.clone();
    duplicated.keys = vec![duplicated.keys[0], duplicated.keys[0]];
    assert!(validate_store_batch(vec![duplicated], &network(), publisher, NOW).is_err());
    let mut forged = good.clone();
    resign(&mut forged.record, &secret(2));
    assert!(validate_store_batch(vec![forged], &network(), publisher, NOW).is_err());
    // A valid record of another publisher may not be stored (or replayed)
    // by anyone else.
    let error = validate_store_batch(vec![good], &network(), secret(2).public(), NOW).unwrap_err();
    assert!(error.to_string().contains("publisher"), "{error}");
}

#[test]
fn replicas_only_accept_keys_they_are_close_to() {
    let own = NodeId::from_bytes([0; 32]);
    let key = PodcastDhtKey::from_bytes([0; 32]);
    let far: Vec<NodeId> = (0..100)
        .map(|index| NodeId::from_bytes([0x80 | index as u8; 32]))
        .collect();
    assert!(is_responsible(&own, &far, &key));
    let near: Vec<NodeId> = (1..=PODCAST_STORE_RESPONSIBILITY_WINDOW)
        .map(|index| {
            let mut bytes = [0; 32];
            bytes[31] = index as u8;
            NodeId::from_bytes(bytes)
        })
        .collect();
    let own = NodeId::from_bytes([0xff; 32]);
    assert!(!is_responsible(&own, &near, &key));
    assert!(is_responsible(&own, &near[1..], &key));
}

#[test]
fn inbound_requests_are_limited_per_peer() {
    let inflight = Arc::new(Mutex::new(HashMap::new()));
    let peer = secret(1).public();
    let slots: Vec<_> = (0..MAX_PODCAST_DHT_REQUESTS_PER_PEER)
        .map(|_| PeerSlot::acquire(&inflight, peer).unwrap())
        .collect();
    assert!(PeerSlot::acquire(&inflight, peer).is_none());
    assert!(PeerSlot::acquire(&inflight, secret(2).public()).is_some());
    drop(slots);
    assert!(PeerSlot::acquire(&inflight, peer).is_some());
    assert!(lock(&inflight).is_empty());
}

#[test]
fn lookups_keep_a_bounded_number_of_owners() {
    let mut winners = HashMap::new();
    let id_key = PodcastDhtKey::podcast(&network(), card(&secret(1), 1).podcast_id());
    let records: Vec<_> = (0..=MAX_PODCAST_LOOKUP_RECORDS)
        .map(|index| {
            card_with(
                spec("show.xml", "Rust Radio"),
                &numbered_secret(index),
                1,
                now_ms(),
            )
        })
        .collect();
    let first = records[0].clone();
    merge_records(&mut winners, records, &network(), &id_key, &[]);
    assert_eq!(winners.len(), MAX_PODCAST_LOOKUP_RECORDS);
    // A newer record of a known owner still replaces its winner.
    let newer = card_with(
        spec("show.xml", "Rust Radio"),
        &numbered_secret(0),
        2,
        now_ms(),
    );
    merge_records(&mut winners, vec![newer.clone()], &network(), &id_key, &[]);
    let owner = (first.podcast_id().clone(), first.publisher());
    assert_eq!(winners[&owner], newer);
}

#[test]
fn filter_tokens_are_bounded() {
    let at: Vec<String> = (0..MAX_PODCAST_QUERY_TOKENS)
        .map(|i| format!("t{i}"))
        .collect();
    validate_filter_tokens(&at).unwrap();
    let mut over = at.clone();
    over.push("x".into());
    assert!(validate_filter_tokens(&over).is_err());
    assert!(validate_filter_tokens(&["x".repeat(MAX_INDEX_TOKEN_BYTES + 1)]).is_err());
    assert!(validate_filter_tokens(&[String::new()]).is_err());
}

// ------------------------------------------------------------------ privacy

#[test]
fn card_from_subscription_is_a_public_projection_only() {
    let base = spec("show.xml", "Rust Radio");
    let mut subscription = subscription(&base);
    let spec = PodcastCardSpec::from_subscription(&subscription).unwrap();
    assert_eq!(spec.feed_url, base.feed_url);
    assert_eq!(spec.id_feed_url, None);
    let record = card_with(spec, &secret(1), 1, NOW);
    let bytes = postcard::to_stdvec(&record).unwrap();
    let private = b"private-op-id-123";
    assert!(!bytes.windows(private.len()).any(|window| window == private));
    assert!(
        !record
            .signing_bytes()
            .windows(private.len())
            .any(|window| window == private)
    );

    // After a feed move, the card binds the id to the original URL.
    subscription.feed_aliases = vec![base.feed_url.clone()];
    subscription.podcast.feed_url = feed("moved.xml");
    let moved = PodcastCardSpec::from_subscription(&subscription).unwrap();
    assert_eq!(moved.feed_url, feed("moved.xml"));
    assert_eq!(moved.id_feed_url, Some(base.feed_url.clone()));
    moved.validate().unwrap();

    // Without the original URL the id cannot be proven and nothing is built.
    subscription.feed_aliases.clear();
    assert!(PodcastCardSpec::from_subscription(&subscription).is_err());
}

#[test]
fn privacy_warnings_cover_both_feed_urls() {
    let mut card = spec("show.xml", "Rust Radio");
    assert!(card.privacy_warnings().is_empty());
    card.id_feed_url =
        Some(FeedUrl::parse("https://feeds.example.test/show.xml?token=abc").unwrap());
    assert_eq!(card.privacy_warnings().len(), 1);
}

// ------------------------------------------------------------------- search

fn music(name: &str, kind: ItemKind) -> LibraryItem {
    LibraryItem {
        id: ItemId::from_bytes([1; 32]),
        owner: secret(5).public(),
        kind,
        name: name.to_string(),
        normalized_name: crate::normalize_name(name),
        artist_names: Vec::new(),
        featured_artist_names: Vec::new(),
        year: None,
        release_type: None,
        release_title: None,
        track_number: None,
        disc_number: None,
        duration_seconds: None,
        content_id: None,
        revision: 1,
        deleted: false,
        updated_at_ms: NOW,
    }
}

fn moved_spec(path: &str, origin: &str, title: &str) -> PodcastCardSpec {
    let mut moved = spec(path, title);
    moved.id_feed_url = Some(feed(origin));
    moved.podcast_id = PodcastId::from_feed_url(&feed(origin));
    moved
}

#[test]
fn hits_merge_publishers_and_drop_tombstoned_ones() {
    let a = secret(1);
    let b = secret(2);
    let c = secret(3);
    let records = vec![
        card(&a, 1),
        card(&a, 2),
        card_with(
            moved_spec("moved.xml", "show.xml", "Rust Radio"),
            &b,
            1,
            NOW + 10,
        ),
        card(&c, 1),
        tombstone(&c, 2, NOW),
        card_with(spec("other.xml", "Rust Hour"), &a, 1, NOW),
    ];
    let hits = rank_hits(
        aggregate_hits(records),
        "rust radio",
        &podcast_query_tokens("rust radio"),
    );
    assert_eq!(hits.len(), 1, "Rust Hour lacks the token radio");
    let hit = &hits[0];
    assert_eq!(hit.kind(), SearchHitKind::Podcast);
    let mut expected = vec![a.public(), b.public()];
    expected.sort_by_key(|key| *key.as_bytes());
    assert_eq!(hit.publishers, expected);
    // The card under the origin URL represents, whoever issued later.
    assert_eq!(hit.card.publisher, a.public());
    assert_eq!(hit.feed_url, feed("show.xml"));
    assert_eq!(
        hit.claimed_feed_urls,
        vec![ClaimedFeedUrl {
            feed_url: feed("moved.xml"),
            publishers: vec![b.public()],
        }]
    );
}

#[test]
fn sybil_cards_cannot_redirect_an_existing_podcast() {
    let genuine = card(&secret(1), 1);
    let id = genuine.podcast_id().clone();
    let evil_feed = FeedUrl::parse("https://evil.example/feed.xml").unwrap();
    let hijack = |seed: usize| {
        let mut spec = spec("show.xml", "Rust Radio (official)");
        spec.id_feed_url = Some(spec.feed_url.clone());
        spec.feed_url = evil_feed.clone();
        spec.validate().unwrap();
        assert_eq!(spec.podcast_id, id, "a hijack card claims the genuine id");
        let skew = PODCAST_DHT_MAX_CLOCK_SKEW.as_millis() as u64;
        card_with(spec, &numbered_secret(seed), u64::MAX, NOW + skew)
    };
    let mut records = vec![genuine.clone()];
    records.extend((0..10).map(hijack));
    let hit = aggregate_hits(records).remove(0);
    assert_eq!(
        hit.feed_url,
        feed("show.xml"),
        "only the origin URL is offered"
    );
    assert_eq!(
        hit.card,
        *genuine.card().unwrap(),
        "the origin card represents"
    );
    assert_eq!(hit.claimed_feed_urls.len(), 1);
    assert_eq!(hit.claimed_feed_urls[0].feed_url, evil_feed);
    assert_eq!(hit.claimed_feed_urls[0].publishers.len(), 10);

    // Without any origin card the hit still offers the origin URL, never the
    // claimed one, and ranks below podcasts that have an origin card.
    let only_claims = aggregate_hits((0..3).map(hijack)).remove(0);
    assert_eq!(only_claims.feed_url, feed("show.xml"));
    assert!(!only_claims.card.feed_url_is_origin());
    let honest = aggregate_hits(vec![card_with(
        spec("b.xml", "Rust Radio Weekly"),
        &secret(2),
        1,
        NOW,
    )])
    .remove(0);
    let ranked = rank_hits(
        vec![only_claims.clone(), honest.clone()],
        "rust",
        &podcast_query_tokens("rust"),
    );
    assert_eq!(ranked[0].podcast_id, honest.podcast_id);
    // Publisher counts do not rank.
    let crowded = aggregate_hits(
        (20..60).map(|seed| card_with(spec("c.xml", "Rust Zone"), &numbered_secret(seed), 1, NOW)),
    )
    .remove(0);
    let ranked = rank_hits(
        vec![crowded, honest.clone()],
        "rust",
        &podcast_query_tokens("rust"),
    );
    assert_eq!(
        ranked[0].podcast_id, honest.podcast_id,
        "ranked by title, not crowd"
    );
}

#[test]
fn federated_results_are_typed_and_exact_matches_lead() {
    let podcast = aggregate_hits(vec![card(&secret(1), 1)]).remove(0);
    let merged = merge_search_results(
        "rust radio",
        vec![
            music("Rust Radio Theme", ItemKind::Track),
            music("Rust", ItemKind::Artist),
        ],
        vec![podcast],
    );
    let kinds: Vec<_> = merged.iter().map(FederatedSearchHit::kind).collect();
    assert_eq!(
        kinds,
        vec![
            SearchHitKind::Podcast,
            SearchHitKind::Track,
            SearchHitKind::Artist
        ]
    );
    let json = serde_json::to_value(&merged[0]).unwrap();
    assert_eq!(json["type"], "podcast");
    assert_eq!(json["hit"]["card"]["title"], "Rust Radio");
    let json = serde_json::to_value(&merged[1]).unwrap();
    assert_eq!(json["type"], "music");
    assert_eq!(json["hit"]["kind"], "track");
    assert_eq!(SearchHitKind::Podcast.as_str(), "podcast");
}

#[test]
fn query_tokens_are_normalized_distinct_and_bounded() {
    assert_eq!(
        podcast_query_tokens("  Rust, RUST radio!"),
        vec!["rust", "radio"]
    );
    let long: Vec<String> = (0..20).map(|i| format!("w{i}")).collect();
    assert_eq!(
        podcast_query_tokens(&long.join(" ")).len(),
        MAX_PODCAST_QUERY_TOKENS
    );
    assert!(podcast_query_tokens("!!!").is_empty());
}

#[test]
fn error_responses_are_bounded() {
    let response = PodcastDhtResponse::error("é".repeat(MAX_ERROR_BYTES));
    assert!(response.error.as_ref().unwrap().len() <= MAX_ERROR_BYTES);
    validate_response(&response).unwrap();
}
