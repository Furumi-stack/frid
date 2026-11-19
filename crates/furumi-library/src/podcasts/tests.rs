use std::path::Path;

use music_dht::device_sync::{SyncOpPayload, SyncOpWire, SyncSnapshot};
use music_dht::podcast::feed::{ParsedFeed, parse_feed_with_base};
use music_dht::podcast::{
    EpisodeId, EpisodeIdentityHints, FeedUrl, ListenSession, PodcastEpisodeRef, PodcastId,
    PodcastMeta, ProgressStatus, resolve_episode_ids,
};
use rusqlite::{Connection, params};

use super::*;

// ---------------------------------------------------------------------------
// Fixtures

/// Deterministic stamps: `op_id = <device>:<seq>`, `hlc_ms` from a counter.
struct TestStamps {
    device: String,
    seq: i64,
    hlc_ms: i64,
}

impl TestStamps {
    fn new(device: &str, hlc_start: i64) -> Self {
        Self {
            device: device.to_string(),
            seq: 0,
            hlc_ms: hlc_start,
        }
    }
}

impl StampSource for TestStamps {
    fn device_id(&self) -> &str {
        &self.device
    }

    fn next_stamp(&mut self) -> Result<OpStamp> {
        self.seq += 1;
        self.hlc_ms += 1_000;
        Ok(OpStamp::new(
            format!("{}:{}", self.device, self.seq),
            self.device.clone(),
            self.hlc_ms,
        ))
    }
}

fn wire(ops: &[PodcastLocalOp]) -> Vec<SyncOpWire> {
    ops.iter()
        .enumerate()
        .map(|(index, op)| op.clone().into_wire(index as i64 + 1))
        .collect()
}

fn url(value: &str) -> FeedUrl {
    FeedUrl::parse(value).unwrap()
}

fn meta(feed_url: &str, title: &str) -> PodcastMeta {
    PodcastMeta {
        feed_url: url(feed_url),
        title: title.to_string(),
        author: Some("Host".to_string()),
        description: Some("About the show".to_string()),
        link: Some("https://example.test/".to_string()),
        image_url: None,
    }
}

fn episode_ref(podcast_url: &str, guid: &str) -> PodcastEpisodeRef {
    let feed_url = url(podcast_url);
    let podcast_id = PodcastId::from_feed_url(&feed_url);
    PodcastEpisodeRef {
        episode_id: EpisodeId::from_guid(&podcast_id, guid).unwrap(),
        podcast_id,
        feed_url,
        podcast_title: "Show".to_string(),
        episode_title: format!("Episode {guid}"),
        audio_url: format!("https://cdn.example.test/{guid}.mp3"),
        mime: Some("audio/mpeg".to_string()),
        duration_ms: Some(3_600_000),
        published_at_ms: Some(1_700_000_000_000),
        season: None,
        episode_number: None,
        image_url: None,
        progress_generation: None,
    }
}

struct Item<'a> {
    guid: Option<&'a str>,
    title: &'a str,
    link: Option<&'a str>,
    audio: &'a str,
    date: Option<&'a str>,
    season: Option<u32>,
    number: Option<u32>,
    description: Option<&'a str>,
}

impl<'a> Item<'a> {
    fn new(guid: Option<&'a str>, title: &'a str, audio: &'a str) -> Self {
        Self {
            guid,
            title,
            link: None,
            audio,
            date: None,
            season: None,
            number: None,
            description: None,
        }
    }
}

fn rss(title: &str, new_feed_url: Option<&str>, items: &[Item<'_>]) -> String {
    let mut body = String::new();
    for item in items {
        body.push_str("<item>");
        body.push_str(&format!("<title>{}</title>", item.title));
        if let Some(guid) = item.guid {
            body.push_str(&format!("<guid isPermaLink=\"false\">{guid}</guid>"));
        }
        if let Some(link) = item.link {
            body.push_str(&format!("<link>{link}</link>"));
        }
        if let Some(date) = item.date {
            body.push_str(&format!("<pubDate>{date}</pubDate>"));
        }
        if let Some(season) = item.season {
            body.push_str(&format!("<itunes:season>{season}</itunes:season>"));
        }
        if let Some(number) = item.number {
            body.push_str(&format!("<itunes:episode>{number}</itunes:episode>"));
        }
        if let Some(description) = item.description {
            body.push_str(&format!("<description>{description}</description>"));
        }
        body.push_str(&format!(
            "<enclosure url=\"{}\" length=\"1000\" type=\"audio/mpeg\"/>",
            item.audio
        ));
        body.push_str("</item>\n");
    }
    let new_feed = new_feed_url
        .map(|url| format!("<itunes:new-feed-url>{url}</itunes:new-feed-url>"))
        .unwrap_or_default();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel><title>{title}</title><link>https://example.test/</link>{new_feed}
{body}</channel></rss>"#
    )
}

fn parse(xml: &str, base: &FeedUrl) -> ParsedFeed {
    parse_feed_with_base(xml.as_bytes(), Some(base.as_str())).unwrap()
}

fn refresh(
    store: &PodcastStore,
    stamps: &mut TestStamps,
    podcast_id: &PodcastId,
    feed: &ParsedFeed,
    fetched_from: &FeedUrl,
    moved_permanently: bool,
) -> RefreshOutcome {
    store
        .apply_feed(
            stamps,
            &FeedRefresh {
                podcast_id,
                feed,
                fetched_from,
                moved_permanently,
            },
        )
        .unwrap()
}

/// Every permutation of `items` (Heap's algorithm).
fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    fn heap<T: Clone>(k: usize, items: &mut Vec<T>, out: &mut Vec<Vec<T>>) {
        if k <= 1 {
            out.push(items.clone());
            return;
        }
        heap(k - 1, items, out);
        for i in 0..k - 1 {
            if k.is_multiple_of(2) {
                items.swap(i, k - 1);
            } else {
                items.swap(0, k - 1);
            }
            heap(k - 1, items, out);
        }
    }
    let mut items = items.to_vec();
    let mut out = Vec::new();
    heap(items.len(), &mut items, &mut out);
    out
}

fn sync_state(store: &PodcastStore) -> SyncSnapshot {
    store.export_snapshot().unwrap()
}

fn tables(conn: &Connection) -> Vec<String> {
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[test]
fn store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PodcastStore>();
}

// ---------------------------------------------------------------------------
// Migrations

/// The prototype migration of the original podcast PR, verbatim
/// (`furumi-tui` `src/podcasts/store.rs`, `MIGRATIONS[0]`).
const PROTOTYPE_MIGRATION_V1: &str = "
    CREATE TABLE IF NOT EXISTS podcasts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        feed_url        TEXT NOT NULL UNIQUE,
        title           TEXT NOT NULL,
        author          TEXT,
        description     TEXT,
        link            TEXT,
        image_url       TEXT,
        cover_url       TEXT,
        cover_path      TEXT,
        etag            TEXT,
        last_modified   TEXT,
        added_at        INTEGER NOT NULL,
        last_checked_at INTEGER,
        last_error      TEXT
    );
    CREATE TABLE IF NOT EXISTS podcast_episodes (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        podcast_id       INTEGER NOT NULL REFERENCES podcasts(id) ON DELETE CASCADE,
        guid             TEXT NOT NULL,
        title            TEXT NOT NULL,
        description      TEXT,
        published_at     INTEGER,
        duration_seconds REAL,
        audio_url        TEXT NOT NULL,
        mime_type        TEXT,
        byte_len         INTEGER,
        link             TEXT,
        image_url        TEXT,
        first_seen_at    INTEGER NOT NULL,
        UNIQUE (podcast_id, guid)
    );
    CREATE INDEX IF NOT EXISTS podcast_episodes_by_date
        ON podcast_episodes (podcast_id, published_at DESC, id DESC);
    ";

/// Creates a database exactly as the prototype `Store::open` did.
fn fabricate_prototype(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "foreign_keys", true).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .unwrap();
    conn.execute_batch(&format!(
        "BEGIN IMMEDIATE; {PROTOTYPE_MIGRATION_V1}; PRAGMA user_version = 1; COMMIT;"
    ))
    .unwrap();
    conn
}

#[allow(clippy::too_many_arguments)]
fn prototype_episode(
    conn: &Connection,
    podcast: i64,
    guid: &str,
    title: &str,
    published_at: Option<i64>,
    audio_url: &str,
    link: Option<&str>,
) -> i64 {
    conn.execute(
        "INSERT INTO podcast_episodes
             (podcast_id, guid, title, description, published_at, duration_seconds,
              audio_url, mime_type, byte_len, link, image_url, first_seen_at)
         VALUES (?1, ?2, ?3, 'Notes', ?4, 1801.5, ?5, 'audio/mpeg', 1000, ?6, NULL, 1700000500)",
        params![podcast, guid, title, published_at, audio_url, link],
    )
    .unwrap();
    conn.last_insert_rowid()
}

#[test]
fn empty_file_migrates_to_the_current_schema_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join(PODCAST_DB_FILE);
    let store = PodcastStore::open(&path).unwrap();
    assert_eq!(store.schema_version().unwrap(), PODCAST_SCHEMA_VERSION);
    assert_eq!(store.path(), Some(path.as_path()));
    {
        let conn = store.lock();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        let fk: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fk, 1);
        let names = tables(&conn);
        for table in [
            "podcasts",
            "podcast_feed_aliases",
            "episodes",
            "podcast_seasons",
            "podcast_subscriptions",
            "podcast_shares",
            "episode_progress",
            "podcast_listen_sessions",
            "podcast_fetch_state",
            "legacy_quarantine",
            "legacy_podcasts",
            "legacy_podcast_episodes",
        ] {
            assert!(names.iter().any(|name| name == table), "missing {table}");
        }
    }
    assert!(
        store
            .list_podcasts(PodcastListFilter::All)
            .unwrap()
            .is_empty()
    );
    drop(store);
    // Reopening is idempotent.
    let store = PodcastStore::open(&path).unwrap();
    assert_eq!(store.schema_version().unwrap(), PODCAST_SCHEMA_VERSION);
    assert!(store.legacy_quarantine().unwrap().is_empty());
}

#[test]
fn empty_prototype_database_migrates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(PODCAST_DB_FILE);
    drop(fabricate_prototype(&path));
    let store = PodcastStore::open(&path).unwrap();
    assert_eq!(store.schema_version().unwrap(), PODCAST_SCHEMA_VERSION);
    assert!(
        store
            .list_podcasts(PodcastListFilter::All)
            .unwrap()
            .is_empty()
    );
    let conn = store.lock();
    let names = tables(&conn);
    assert!(names.contains(&"legacy_podcasts".to_string()));
    assert!(!names.iter().any(|name| name == "podcast_episodes"));
}

#[test]
fn intermediate_version_one_written_by_this_store_migrates() {
    // A store interrupted after migration 1 (or created by it) continues.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(PODCAST_DB_FILE);
    {
        let mut conn = Connection::open(&path).unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute_batch(super::schema::PROTOTYPE_SCHEMA_V1_FOR_TESTS)
            .unwrap();
        tx.pragma_update(None, "user_version", 1).unwrap();
        tx.commit().unwrap();
    }
    let store = PodcastStore::open(&path).unwrap();
    assert_eq!(store.schema_version().unwrap(), PODCAST_SCHEMA_VERSION);
}

#[test]
fn prototype_database_is_imported_without_losing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(PODCAST_DB_FILE);
    let conn = fabricate_prototype(&path);
    conn.execute(
        "INSERT INTO podcasts (feed_url, title, author, description, link, image_url,
                               cover_url, cover_path, etag, last_modified, added_at,
                               last_checked_at, last_error)
         VALUES ('HTTPS://Example.TEST:443/feed.xml?token=Ab', 'Show', 'Host', 'About',
                 'https://example.test/', 'https://example.test/cover.jpg',
                 'https://example.test/cover.jpg', '/covers/1.jpg', '\"etag-1\"',
                 'Tue, 05 Mar 2024 10:00:00 GMT', 1700000000, 1700000600, NULL)",
        [],
    )
    .unwrap();
    let show = conn.last_insert_rowid();
    let with_guid = prototype_episode(
        &conn,
        show,
        "guid-1",
        "First",
        Some(1_700_000_100),
        "https://cdn.example.test/1.mp3",
        Some("https://example.test/1"),
    );
    // The prototype stored the enclosure URL as guid for items without one.
    let without_guid = prototype_episode(
        &conn,
        show,
        "https://cdn.example.test/2.mp3",
        "Second",
        Some(1_700_000_200),
        "https://cdn.example.test/2.mp3",
        Some("https://example.test/2"),
    );
    let bad_audio = prototype_episode(
        &conn,
        show,
        "guid-3",
        "Third",
        None,
        "ftp://cdn.example.test/3.mp3",
        None,
    );
    conn.execute(
        "INSERT INTO podcasts (feed_url, title, added_at, last_checked_at, last_error)
         VALUES ('not a feed url', 'Broken', 1700000000, 1700000600, 'HTTP 500')",
        [],
    )
    .unwrap();
    let broken = conn.last_insert_rowid();
    prototype_episode(
        &conn,
        broken,
        "b-1",
        "Kept",
        None,
        "https://cdn.example.test/b1.mp3",
        None,
    );
    // Same canonical URL as `show`: merges into the same podcast.
    conn.execute(
        "INSERT INTO podcasts (feed_url, title, added_at)
         VALUES ('https://example.test/feed.xml?token=Ab#fragment', 'Show again', 1700000900)",
        [],
    )
    .unwrap();
    let duplicate = conn.last_insert_rowid();
    prototype_episode(
        &conn,
        duplicate,
        "guid-1",
        "First (dup)",
        Some(1_700_000_100),
        "https://cdn.example.test/1.mp3",
        None,
    );
    drop(conn);

    let store = PodcastStore::open(&path).unwrap();
    assert_eq!(store.schema_version().unwrap(), PODCAST_SCHEMA_VERSION);

    let canonical = url("https://example.test/feed.xml?token=Ab");
    let podcast_id = PodcastId::from_feed_url(&canonical);
    let podcasts = store.list_podcasts(PodcastListFilter::Subscribed).unwrap();
    assert_eq!(podcasts.len(), 1);
    let summary = &podcasts[0];
    assert_eq!(summary.podcast_id, podcast_id);
    assert_eq!(summary.meta.feed_url, canonical);
    assert_eq!(summary.meta.title, "Show");
    assert!(summary.subscribed);
    assert!(!summary.shared, "imported subscriptions are private");
    assert_eq!(summary.episode_count, 2);
    assert_eq!(summary.unplayed_count, 2);

    let register = store.subscription(&podcast_id).unwrap().unwrap();
    assert_eq!(register.generation, 0);
    assert_eq!(register.hlc_ms, 1_700_000_000_000);
    assert_eq!(
        register.op_id,
        format!("{LEGACY_IMPORT_OP_PREFIX}{podcast_id}")
    );
    assert_eq!(register.feed_aliases, vec![canonical.clone()]);

    // Episode ids follow the contract identity rule.
    let guid_id = EpisodeId::from_guid(&podcast_id, "guid-1").unwrap();
    let expected_no_guid = resolve_episode_ids(
        &podcast_id,
        Some("https://example.test/"),
        &[EpisodeIdentityHints {
            guid: None,
            enclosure_url: Some("https://cdn.example.test/2.mp3".to_string()),
            link: Some("https://example.test/2".to_string()),
            published_at_ms: Some(1_700_000_200_000),
            title: Some("Second".to_string()),
        }],
        &[],
    )[0]
    .clone()
    .unwrap();
    let first = store.episode(&guid_id).unwrap().unwrap();
    assert_eq!(first.episode.title, "First");
    assert_eq!(first.episode.published_at_ms, Some(1_700_000_100_000));
    assert_eq!(first.episode.duration_ms, Some(1_801_500));
    assert!(first.progress.is_none());
    let second = store.episode(&expected_no_guid).unwrap().unwrap();
    assert_eq!(second.episode.guid, None);

    let state = store.fetch_state(&podcast_id).unwrap();
    assert_eq!(state.etag.as_deref(), Some("\"etag-1\""));
    assert_eq!(state.checked_at_ms, Some(1_700_000_600_000));
    assert_eq!(state.last_success_at_ms, Some(1_700_000_600_000));
    assert_eq!(state.cover_path.as_deref(), Some("/covers/1.jpg"));
    assert_eq!(state.consecutive_failures, 0);

    let quarantine = store.legacy_quarantine().unwrap();
    let ids: Vec<(String, i64)> = quarantine
        .iter()
        .map(|entry| (entry.legacy_table.clone(), entry.legacy_id))
        .collect();
    assert!(ids.contains(&("legacy_podcasts".to_string(), broken)));
    assert!(ids.contains(&("legacy_podcast_episodes".to_string(), bad_audio)));

    // Nothing was dropped: every prototype row is still in a legacy table.
    {
        let conn = store.lock();
        let podcasts: i64 = conn
            .query_row("SELECT COUNT(*) FROM legacy_podcasts", [], |row| row.get(0))
            .unwrap();
        let episodes: i64 = conn
            .query_row("SELECT COUNT(*) FROM legacy_podcast_episodes", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!((podcasts, episodes), (3, 5));
        let legacy_guid: Option<String> = conn
            .query_row(
                "SELECT legacy_guid FROM episodes WHERE episode_id = ?1",
                [expected_no_guid.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            legacy_guid.as_deref(),
            Some("https://cdn.example.test/2.mp3")
        );
    }
    // The imported subscription is part of the snapshot; progress is empty.
    let snapshot = sync_state(&store);
    assert_eq!(snapshot.podcast_subscriptions.len(), 1);
    assert!(snapshot.podcast_progress.is_empty());
    let _ = (with_guid, without_guid);

    // A later refresh whose feed has a real GUID equal to the stored enclosure
    // URL re-keys the imported episode instead of duplicating it.
    let mut stamps = TestStamps::new("device-a", 1_800_000_000_000);
    let feed = parse(
        &rss(
            "Show",
            None,
            &[
                Item::new(Some("guid-1"), "First", "https://cdn.example.test/1.mp3"),
                Item::new(
                    Some("https://cdn.example.test/2.mp3"),
                    "Second",
                    "https://cdn.example.test/2.mp3",
                ),
            ],
        ),
        &canonical,
    );
    let outcome = refresh(&store, &mut stamps, &podcast_id, &feed, &canonical, false);
    assert_eq!(outcome.new_episodes, 0);
    let rekeyed = EpisodeId::from_guid(&podcast_id, "https://cdn.example.test/2.mp3").unwrap();
    assert!(store.episode(&rekeyed).unwrap().is_some());
    assert!(store.episode(&expected_no_guid).unwrap().is_none());
    assert_eq!(
        store.podcast(&podcast_id).unwrap().unwrap().episode_count,
        2
    );
}

#[test]
fn newer_schema_is_refused_without_modifying_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(PODCAST_DB_FILE);
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE future (x INTEGER); INSERT INTO future VALUES (1);
             PRAGMA user_version = 3;",
        )
        .unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    let error = PodcastStore::open(&path).unwrap_err();
    assert_eq!(
        error.downcast_ref::<PodcastStoreError>(),
        Some(&PodcastStoreError::NewerSchema {
            found: 3,
            supported: PODCAST_SCHEMA_VERSION
        })
    );
    assert!(error.to_string().contains("newer"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!dir.path().join(format!("{PODCAST_DB_FILE}-wal")).exists());
}

#[test]
fn foreign_databases_are_refused_without_modification() {
    let dir = tempfile::tempdir().unwrap();
    // A library database passed by mistake.
    let library = dir.path().join("library.db");
    {
        let conn = Connection::open(&library).unwrap();
        conn.execute_batch("CREATE TABLE tracks (id INTEGER PRIMARY KEY);")
            .unwrap();
    }
    let before = std::fs::read(&library).unwrap();
    let error = PodcastStore::open(&library).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<PodcastStoreError>(),
        Some(PodcastStoreError::UnrecognizedDatabase(_))
    ));
    assert_eq!(std::fs::read(&library).unwrap(), before);

    // Version 1 whose `podcasts` table is not the prototype's.
    let odd = dir.path().join("odd.sqlite3");
    {
        let conn = Connection::open(&odd).unwrap();
        conn.execute_batch(
            "CREATE TABLE podcasts (id INTEGER PRIMARY KEY, url TEXT);
             PRAGMA user_version = 1;",
        )
        .unwrap();
    }
    let error = PodcastStore::open(&odd).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<PodcastStoreError>(),
        Some(PodcastStoreError::UnrecognizedDatabase(_))
    ));
}

// ---------------------------------------------------------------------------
// Local actions

const FEED: &str = "https://example.test/feed.xml";

#[test]
fn subscribe_is_idempotent_and_private_until_shared() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let outcome = store.subscribe(&mut stamps, &meta(FEED, "Show")).unwrap();
    assert!(!outcome.already_subscribed);
    assert_eq!(outcome.ops.len(), 1);
    assert_eq!(outcome.podcast_id, PodcastId::from_feed_url(&url(FEED)));
    let SyncOpPayload::PodcastSubscriptionSet {
        subscribed,
        generation,
        feed_aliases,
        ..
    } = &outcome.ops[0].payload
    else {
        panic!("subscription op expected");
    };
    assert!(*subscribed);
    assert_eq!(*generation, 1);
    assert_eq!(feed_aliases, &vec![url(FEED)]);

    let again = store.subscribe(&mut stamps, &meta(FEED, "Show")).unwrap();
    assert!(again.already_subscribed && again.ops.is_empty());
    assert_eq!(stamps.seq, 1, "no stamp drawn for a no-op");

    let summary = store.podcast(&outcome.podcast_id).unwrap().unwrap();
    assert!(summary.subscribed && !summary.shared);
    let shared = store
        .set_shared(&mut stamps, &outcome.podcast_id, true)
        .unwrap();
    assert_eq!(shared.len(), 1);
    assert!(store.podcast(&outcome.podcast_id).unwrap().unwrap().shared);
    assert!(
        store
            .set_shared(&mut stamps, &outcome.podcast_id, true)
            .unwrap()
            .is_empty()
    );

    // Sharing an unknown podcast is refused.
    let other = PodcastId::from_feed_url(&url("https://other.test/"));
    let error = store.set_shared(&mut stamps, &other, true).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<PodcastStoreError>(),
        Some(PodcastStoreError::NotSubscribed(_))
    ));
}

#[test]
fn unsubscribe_keeps_progress_and_history_and_resubscribe_continues() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let podcast_id = store
        .subscribe(&mut stamps, &meta(FEED, "Show"))
        .unwrap()
        .podcast_id;
    store.set_shared(&mut stamps, &podcast_id, true).unwrap();
    let episode = episode_ref(FEED, "e1");
    let session = store
        .start_session(&mut stamps, &episode, "s1", 0, None)
        .unwrap()
        .session;
    store.checkpoint(&mut stamps, &session, 600_000).unwrap();
    let listened = ListenSession {
        session_id: "s1".to_string(),
        episode_id: episode.episode_id.clone(),
        podcast_id: podcast_id.clone(),
        device_id: "device-a".to_string(),
        started_at_ms: 10,
        ended_at_ms: 600_010,
        start_position_ms: 0,
        end_position_ms: 600_000,
        listened_ms: 600_000,
        completed: false,
    };
    assert!(
        store
            .record_listen_session(&mut stamps, &listened, None)
            .unwrap()
            .is_some()
    );

    let ops = store.unsubscribe(&mut stamps, &podcast_id).unwrap();
    assert_eq!(ops.len(), 2);
    assert!(matches!(
        ops[0].payload,
        SyncOpPayload::PodcastSubscriptionSet {
            subscribed: false,
            ..
        }
    ));
    assert!(matches!(
        ops[1].payload,
        SyncOpPayload::PodcastShareSet { shared: false, .. }
    ));
    let summary = store.podcast(&podcast_id).unwrap().unwrap();
    assert!(!summary.subscribed && !summary.shared);
    assert!(
        store
            .list_podcasts(PodcastListFilter::Subscribed)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.list_podcasts(PodcastListFilter::All).unwrap().len(),
        1
    );
    assert_eq!(
        store.resume_position_ms(&episode.episode_id).unwrap(),
        600_000
    );
    assert_eq!(store.listen_history(None, 10).unwrap().len(), 1);
    assert!(
        store
            .unsubscribe(&mut stamps, &podcast_id)
            .unwrap()
            .is_empty()
    );

    // Subscribing again (even through an alias) reuses the podcast and its state.
    let again = store.subscribe(&mut stamps, &meta(FEED, "Show")).unwrap();
    assert_eq!(again.podcast_id, podcast_id);
    assert_eq!(
        store.resume_position_ms(&episode.episode_id).unwrap(),
        600_000
    );
    let summary = store.podcast(&podcast_id).unwrap().unwrap();
    assert!(summary.subscribed);
    assert!(!summary.shared, "a new subscription is private again");
}

#[test]
fn progress_actions_follow_the_generation_rules() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let episode = episode_ref(FEED, "e1");
    let started = store
        .start_session(&mut stamps, &episode, "s1", 5_000, Some(4))
        .unwrap();
    assert_eq!(started.record.generation, 5, "max(local, carried) + 1");
    assert_eq!(started.record.session_id, "s1");
    assert_eq!(started.record.device_id, "device-a");
    let session = started.session;
    let CheckpointOutcome::Written(checkpoint) =
        store.checkpoint(&mut stamps, &session, 65_000).unwrap()
    else {
        panic!("checkpoint expected");
    };
    assert_eq!(checkpoint.record.generation, 5);
    assert_eq!(
        store.resume_position_ms(&episode.episode_id).unwrap(),
        65_000
    );

    let seek = store.seek(&mut stamps, &session, 10_000).unwrap();
    assert_eq!(seek.record.generation, 6);
    let session = seek.session;
    assert_eq!(session.generation, 6);

    // Another device took over at a greater generation: the old session stops.
    let remote = SyncOpWire {
        op_id: "device-b:1".to_string(),
        origin_device_id: "device-b".to_string(),
        seq: 1,
        hlc_ms: 1,
        payload: SyncOpPayload::PodcastProgressSet {
            episode_id: episode.episode_id.clone(),
            podcast_id: episode.podcast_id.clone(),
            position_ms: 1_000,
            status: ProgressStatus::InProgress,
            generation: 7,
            session_id: "s2".to_string(),
            episode: Some(episode.clone()),
        },
    };
    assert_eq!(
        store.apply_remote_op(&remote).unwrap(),
        ApplyOutcome::Changed
    );
    assert!(matches!(
        store.checkpoint(&mut stamps, &session, 70_000).unwrap(),
        CheckpointOutcome::Superseded { .. }
    ));
    assert_eq!(
        store.resume_position_ms(&episode.episode_id).unwrap(),
        1_000
    );

    let played = store.mark_played(&mut stamps, &episode).unwrap();
    assert_eq!(played.record.generation, 8);
    assert_eq!(played.record.status, ProgressStatus::Completed);
    assert_eq!(store.resume_position_ms(&episode.episode_id).unwrap(), 0);
    assert_eq!(
        store.completed_listen_count(&episode.episode_id).unwrap(),
        0,
        "mark played invents no session"
    );
    let unplayed = store.mark_unplayed(&mut stamps, &episode).unwrap();
    assert_eq!(unplayed.record.status, ProgressStatus::NotStarted);
    let restart = store.restart(&mut stamps, &episode, "s3").unwrap();
    assert_eq!(
        (restart.record.position_ms, restart.record.status),
        (0, ProgressStatus::InProgress)
    );
    let done = store
        .complete_session(&mut stamps, &restart.session, 3_590_000)
        .unwrap();
    assert_eq!(done.record.status, ProgressStatus::Completed);
    assert!(matches!(
        store
            .checkpoint(&mut stamps, &done.session, 3_595_000)
            .unwrap(),
        CheckpointOutcome::AlreadyCompleted { .. }
    ));
    // The episode learned through sync is in the catalog.
    let reference = store.episode_ref(&episode.episode_id).unwrap().unwrap();
    assert_eq!(reference.progress_generation, Some(done.record.generation));
    assert!(
        store
            .episode_track(&episode.episode_id)
            .unwrap()
            .unwrap()
            .is_podcast()
    );
}

#[test]
fn listen_sessions_are_deduplicated_and_counted_once() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let episode = episode_ref(FEED, "e1");
    let mut session = ListenSession {
        session_id: "s1".to_string(),
        episode_id: episode.episode_id.clone(),
        podcast_id: episode.podcast_id.clone(),
        device_id: "device-a".to_string(),
        started_at_ms: 0,
        ended_at_ms: 10_000,
        start_position_ms: 0,
        end_position_ms: 10_000,
        listened_ms: 10_000,
        completed: false,
    };
    // Below the history threshold: nothing recorded.
    assert!(
        store
            .record_listen_session(&mut stamps, &session, Some(&episode))
            .unwrap()
            .is_none()
    );
    session.listened_ms = 3_600_000;
    session.ended_at_ms = 3_600_000;
    session.completed = true;
    let op = store
        .record_listen_session(&mut stamps, &session, Some(&episode))
        .unwrap()
        .unwrap();
    assert!(
        store
            .record_listen_session(&mut stamps, &session, Some(&episode))
            .unwrap()
            .is_none()
    );
    let op = op.into_wire(1);
    assert_eq!(store.apply_remote_op(&op).unwrap(), ApplyOutcome::Unchanged);
    assert_eq!(
        store.completed_listen_count(&episode.episode_id).unwrap(),
        1
    );
    let history = store.listen_history(Some(&episode.podcast_id), 10).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].episode_title.as_deref(), Some("Episode e1"));

    // Another device's session with a foreign device id is refused locally.
    let mut foreign = session.clone();
    foreign.session_id = "s2".to_string();
    foreign.device_id = "device-b".to_string();
    assert!(
        store
            .record_listen_session(&mut stamps, &foreign, None)
            .is_err()
    );
}

#[test]
fn remote_ops_are_validated_and_non_podcast_ops_ignored() {
    let store = PodcastStore::open_in_memory().unwrap();
    let episode = episode_ref(FEED, "e1");
    let mut bad = SyncOpWire {
        op_id: "device-b:1".to_string(),
        origin_device_id: "device-b".to_string(),
        seq: 1,
        hlc_ms: 1,
        payload: SyncOpPayload::PodcastProgressSet {
            episode_id: episode.episode_id.clone(),
            podcast_id: episode.podcast_id.clone(),
            position_ms: -5,
            status: ProgressStatus::InProgress,
            generation: 1,
            session_id: "s".to_string(),
            episode: None,
        },
    };
    assert!(matches!(
        store.apply_remote_op(&bad).unwrap(),
        ApplyOutcome::Rejected(_)
    ));
    bad.payload = SyncOpPayload::PodcastProgressSet {
        episode_id: episode.episode_id.clone(),
        podcast_id: episode.podcast_id.clone(),
        position_ms: 5,
        status: ProgressStatus::InProgress,
        generation: u64::MAX,
        session_id: "s".to_string(),
        episode: None,
    };
    assert!(matches!(
        store.apply_remote_op(&bad).unwrap(),
        ApplyOutcome::Rejected(_)
    ));
    assert!(store.progress(&episode.episode_id).unwrap().is_none());
    bad.payload = SyncOpPayload::PlaylistDeleted {
        playlist_id: "p".to_string(),
    };
    assert_eq!(store.apply_remote_op(&bad).unwrap(), ApplyOutcome::Ignored);
}

// ---------------------------------------------------------------------------
// Convergence

fn assert_converges(ops: &[SyncOpWire], extra_snapshots: &[SyncSnapshot]) -> SyncSnapshot {
    let reference = PodcastStore::open_in_memory().unwrap();
    for op in ops {
        reference.apply_remote_op(op).unwrap();
    }
    let expected = sync_state(&reference);
    for order in permutations(ops) {
        let store = PodcastStore::open_in_memory().unwrap();
        for op in &order {
            store.apply_remote_op(op).unwrap();
        }
        // Duplicate delivery changes nothing.
        for op in order.iter().rev() {
            assert_ne!(store.apply_remote_op(op).unwrap(), ApplyOutcome::Changed);
        }
        assert_eq!(sync_state(&store), expected);
    }
    // Recovery through the snapshot gives the same state.
    let restored = PodcastStore::open_in_memory().unwrap();
    restored.apply_snapshot(&expected).unwrap();
    assert_eq!(sync_state(&restored), expected);
    // Snapshot first, then the operations again (and stale snapshots).
    for snapshot in extra_snapshots {
        restored.apply_snapshot(snapshot).unwrap();
    }
    for op in ops {
        restored.apply_remote_op(op).unwrap();
    }
    assert_eq!(sync_state(&restored), expected);
    expected
}

#[test]
fn subscription_share_and_alias_ops_converge_in_every_order() {
    let a = PodcastStore::open_in_memory().unwrap();
    let b = PodcastStore::open_in_memory().unwrap();
    // Device A's clock runs ten minutes ahead.
    let mut stamps_a = TestStamps::new("device-a", 600_000);
    let mut stamps_b = TestStamps::new("device-b", 0);

    let subscribe = a.subscribe(&mut stamps_a, &meta(FEED, "Show")).unwrap();
    let podcast_id = subscribe.podcast_id.clone();
    let shared = a.set_shared(&mut stamps_a, &podcast_id, true).unwrap();
    let snapshot_after_share = sync_state(&a);
    for op in wire(&subscribe.ops).iter().chain(&wire(&shared)) {
        b.apply_remote_op(op).unwrap();
    }
    // B saw everything and unsubscribes with a slower clock.
    let unsubscribe = b.unsubscribe(&mut stamps_b, &podcast_id).unwrap();
    // A learns a redirect target while offline.
    let feed = parse(
        &rss(
            "Show",
            None,
            &[Item::new(
                Some("e1"),
                "One",
                "https://cdn.example.test/1.mp3",
            )],
        ),
        &url("https://new.example.test/feed.xml"),
    );
    let moved = refresh(
        &a,
        &mut stamps_a,
        &podcast_id,
        &feed,
        &url("https://new.example.test/feed.xml"),
        true,
    );
    assert_eq!(moved.ops.len(), 1);

    let mut ops = Vec::new();
    ops.extend(wire(&subscribe.ops));
    ops.extend(wire(&shared));
    ops.extend(wire(&unsubscribe));
    ops.extend(wire(&moved.ops));
    assert_eq!(ops.len(), 5);
    let state = assert_converges(&ops, &[snapshot_after_share]);

    // Unsubscribed on every device despite A's fast clock; share revoked.
    let store = PodcastStore::open_in_memory().unwrap();
    store.apply_snapshot(&state).unwrap();
    let summary = store.podcast(&podcast_id).unwrap().unwrap();
    assert!(!summary.subscribed && !summary.shared);
    let aliases = store.feed_aliases(&podcast_id).unwrap();
    assert!(aliases.contains(&url("https://new.example.test/feed.xml")));
    assert_eq!(
        store
            .resolve_podcast_id(&url("https://new.example.test/feed.xml"))
            .unwrap(),
        podcast_id
    );
}

#[test]
fn progress_and_session_ops_converge_in_every_order() {
    let a = PodcastStore::open_in_memory().unwrap();
    let b = PodcastStore::open_in_memory().unwrap();
    let mut stamps_a = TestStamps::new("device-a", 600_000);
    let mut stamps_b = TestStamps::new("device-b", 0);
    let episode = episode_ref(FEED, "e1");

    let start = a
        .start_session(&mut stamps_a, &episode, "s-a", 0, None)
        .unwrap();
    let CheckpointOutcome::Written(checkpoint) = a
        .checkpoint(&mut stamps_a, &start.session, 120_000)
        .unwrap()
    else {
        panic!("checkpoint expected");
    };
    b.apply_remote_op(&start.op.clone().into_wire(1)).unwrap();
    // B seeks back after observing A's session, with a slower clock.
    let b_start = b
        .start_session(&mut stamps_b, &episode, "s-b", 0, None)
        .unwrap();
    let seek = b.seek(&mut stamps_b, &b_start.session, 30_000).unwrap();
    // A's late checkpoint of the old session (it has not seen B).
    let CheckpointOutcome::Written(late) = a
        .checkpoint(&mut stamps_a, &start.session, 240_000)
        .unwrap()
    else {
        panic!("checkpoint expected");
    };
    let session = |listened_ms: i64, completed: bool| ListenSession {
        session_id: "s-a".to_string(),
        episode_id: episode.episode_id.clone(),
        podcast_id: episode.podcast_id.clone(),
        device_id: "device-a".to_string(),
        started_at_ms: 0,
        ended_at_ms: listened_ms,
        start_position_ms: 0,
        end_position_ms: listened_ms,
        listened_ms,
        completed,
    };
    let first = a
        .record_listen_session(&mut stamps_a, &session(120_000, false), Some(&episode))
        .unwrap()
        .unwrap();
    let grown = a
        .record_listen_session(&mut stamps_a, &session(240_000, false), Some(&episode))
        .unwrap()
        .unwrap();

    let ops: Vec<SyncOpWire> = [start.op, checkpoint.op, seek.op, late.op, first, grown]
        .into_iter()
        .enumerate()
        .map(|(index, op)| op.into_wire(index as i64 + 1))
        .collect();
    let state = assert_converges(&ops, &[]);
    // B's seek (greater generation) beats A's later-stamped checkpoint.
    assert_eq!(state.podcast_progress.len(), 1);
    assert_eq!(state.podcast_progress[0].position_ms, 30_000);
    assert_eq!(state.podcast_progress[0].device_id, "device-b");
    assert_eq!(state.podcast_listen_sessions.len(), 1);
    assert_eq!(state.podcast_listen_sessions[0].listened_ms, 240_000);
}

#[test]
fn snapshot_import_never_loses_newer_local_state() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let episode = episode_ref(FEED, "e1");
    store.subscribe(&mut stamps, &meta(FEED, "Show")).unwrap();
    let old = sync_state(&store);
    let start = store
        .start_session(&mut stamps, &episode, "s1", 0, None)
        .unwrap();
    store.seek(&mut stamps, &start.session, 42_000).unwrap();
    let podcast_id = episode.podcast_id.clone();
    store.unsubscribe(&mut stamps, &podcast_id).unwrap();
    let newer = sync_state(&store);
    let stats = store.apply_snapshot(&old).unwrap();
    assert_eq!(stats.changed, 0);
    assert_eq!(sync_state(&store), newer);
    assert_eq!(
        store.resume_position_ms(&episode.episode_id).unwrap(),
        42_000
    );

    // Over-limit sections are ignored as a whole.
    let huge = SyncSnapshot {
        podcast_shares: vec![
            music_dht::podcast::ShareRecord {
                podcast_id: podcast_id.clone(),
                shared: true,
                generation: 99,
                hlc_ms: 1,
                op_id: "x".to_string(),
            };
            music_dht::podcast::MAX_SNAPSHOT_SHARES + 1
        ],
        ..SyncSnapshot::default()
    };
    let stats = store.apply_snapshot(&huge).unwrap();
    assert!(stats.ignored_over_limits);
    assert_eq!(sync_state(&store), newer);
    // Invalid records are rejected one by one.
    let invalid = SyncSnapshot {
        podcast_shares: vec![music_dht::podcast::ShareRecord {
            podcast_id,
            shared: true,
            generation: 99,
            hlc_ms: 1,
            op_id: String::new(),
        }],
        ..SyncSnapshot::default()
    };
    assert_eq!(store.apply_snapshot(&invalid).unwrap().rejected, 1);
    assert_eq!(sync_state(&store), newer);
}

#[test]
fn fetch_state_and_catalog_are_not_in_the_snapshot() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let podcast_id = store
        .subscribe(&mut stamps, &meta(FEED, "Show"))
        .unwrap()
        .podcast_id;
    store
        .record_fetch_success(
            &podcast_id,
            5,
            Some("\"secret-etag\""),
            Some("lm-value"),
            Some(99),
        )
        .unwrap();
    assert_eq!(
        store
            .record_fetch_failure(&podcast_id, 6, "connection refused", Some(100))
            .unwrap(),
        1
    );
    store
        .set_cover_cache(
            &podcast_id,
            Some("https://example.test/c.jpg"),
            Some("/tmp/cover-path"),
        )
        .unwrap();
    let state = store.fetch_state(&podcast_id).unwrap();
    assert_eq!(state.etag.as_deref(), Some("\"secret-etag\""));
    assert_eq!(state.last_success_at_ms, Some(5));
    assert_eq!(state.last_error.as_deref(), Some("connection refused"));
    let feed = parse(
        &rss(
            "Show",
            None,
            &[Item {
                description: Some("catalog-only-description"),
                ..Item::new(Some("e1"), "One", "https://cdn.example.test/1.mp3")
            }],
        ),
        &url(FEED),
    );
    refresh(&store, &mut stamps, &podcast_id, &feed, &url(FEED), false);

    let mut snapshot = SyncSnapshot::default();
    store
        .fill_snapshot(
            &mut snapshot,
            music_dht::podcast::MAX_PODCAST_SNAPSHOT_BYTES,
        )
        .unwrap();
    let json = serde_json::to_string(&snapshot).unwrap();
    for secret in [
        "secret-etag",
        "lm-value",
        "connection refused",
        "cover-path",
        "catalog-only-description",
    ] {
        assert!(!json.contains(secret), "{secret} leaked into the snapshot");
    }
    assert_eq!(snapshot.podcast_subscriptions.len(), 1);

    assert_eq!(
        store.podcasts_due_for_refresh(99, 10).unwrap(),
        Vec::<(PodcastId, FeedUrl)>::new()
    );
    assert_eq!(store.podcasts_due_for_refresh(100, 10).unwrap().len(), 1);
    store
        .record_fetch_not_modified(&podcast_id, 7, None)
        .unwrap();
    let state = store.fetch_state(&podcast_id).unwrap();
    assert_eq!(state.consecutive_failures, 0);
    assert_eq!(state.etag.as_deref(), Some("\"secret-etag\""));
}

// ---------------------------------------------------------------------------
// Feed refresh

#[test]
fn feed_refresh_keeps_ids_and_progress_when_metadata_changes() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let feed_url = url(FEED);
    let podcast_id = store
        .subscribe(&mut stamps, &meta(FEED, "Show"))
        .unwrap()
        .podcast_id;
    let v1 = parse(
        &rss(
            "Show",
            None,
            &[
                Item {
                    season: Some(1),
                    number: Some(1),
                    date: Some("Mon, 01 Jan 2024 10:00:00 GMT"),
                    ..Item::new(
                        Some("g1"),
                        "Guid one",
                        "https://cdn.example.test/g1.mp3?v=1",
                    )
                },
                Item {
                    link: Some("https://example.test/ep/2"),
                    season: Some(1),
                    number: Some(2),
                    date: Some("Mon, 08 Jan 2024 10:00:00 GMT"),
                    ..Item::new(None, "Linked two", "https://cdn.example.test/2.mp3?v=1")
                },
                Item {
                    date: Some("Mon, 15 Jan 2024 10:00:00 GMT"),
                    ..Item::new(None, "Dated three", "https://cdn.example.test/3.mp3?v=1")
                },
                Item::new(
                    Some("g4"),
                    "Leaves the feed",
                    "https://cdn.example.test/4.mp3",
                ),
            ],
        ),
        &feed_url,
    );
    let first = refresh(&store, &mut stamps, &podcast_id, &v1, &feed_url, false);
    assert_eq!(first.new_episodes, 4);
    assert!(first.ops.is_empty());
    let mut episodes = store
        .episodes(&EpisodeQuery::new(podcast_id.clone()))
        .unwrap();
    assert_eq!(episodes.len(), 4);
    episodes.sort_by(|a, b| a.episode.title.cmp(&b.episode.title));
    let ids: Vec<EpisodeId> = episodes
        .iter()
        .map(|item| item.episode.episode_id.clone())
        .collect();
    let progress_before: Vec<i64> = ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let reference = store.episode_ref(id).unwrap().unwrap();
            let start = store
                .start_session(&mut stamps, &reference, &format!("s{index}"), 0, None)
                .unwrap();
            store
                .seek(&mut stamps, &start.session, 10_000 * (index as i64 + 1))
                .unwrap();
            10_000 * (index as i64 + 1)
        })
        .collect();
    let progress_snapshot = sync_state(&store).podcast_progress;

    // Titles, audio URLs and dates of identified items change; one item leaves.
    let v2 = parse(
        &rss(
            "Show (renamed)",
            None,
            &[
                Item {
                    season: Some(1),
                    number: Some(1),
                    date: Some("Mon, 01 Jan 2024 10:00:00 GMT"),
                    ..Item::new(
                        Some("g1"),
                        "Guid one, remastered",
                        "https://cdn2.example.test/g1.mp3",
                    )
                },
                Item {
                    link: Some("https://example.test/ep/2"),
                    season: Some(1),
                    number: Some(2),
                    date: Some("Mon, 08 Jan 2024 10:00:00 GMT"),
                    ..Item::new(
                        None,
                        "Linked two (edited)",
                        "https://cdn2.example.test/2.mp3",
                    )
                },
                Item {
                    date: Some("Mon, 15 Jan 2024 10:00:00 GMT"),
                    ..Item::new(None, "Dated three", "https://cdn2.example.test/3.mp3")
                },
            ],
        ),
        &feed_url,
    );
    let second = refresh(&store, &mut stamps, &podcast_id, &v2, &feed_url, false);
    assert_eq!(second.new_episodes, 0);
    assert_eq!(second.updated_episodes, 3);
    let after = store
        .episodes(&EpisodeQuery::new(podcast_id.clone()))
        .unwrap();
    assert_eq!(after.len(), 4, "the episode that left the feed is kept");
    for (index, id) in ids.iter().enumerate() {
        let item = store.episode(id).unwrap().unwrap();
        assert_eq!(item.resume_position_ms(), progress_before[index]);
        if item.episode.guid.as_deref() == Some("g4") {
            assert!(!item.in_feed);
        } else {
            assert!(item.in_feed);
            assert!(
                item.episode
                    .audio_url
                    .starts_with("https://cdn2.example.test/")
            );
        }
    }
    assert_eq!(sync_state(&store).podcast_progress, progress_snapshot);
    assert_eq!(
        store.podcast(&podcast_id).unwrap().unwrap().meta.title,
        "Show (renamed)"
    );
    // The register keeps the subscription metadata: refreshes never write it.
    assert_eq!(
        store
            .subscription(&podcast_id)
            .unwrap()
            .unwrap()
            .podcast
            .title,
        "Show"
    );
}

#[test]
fn feed_refresh_records_aliases_once() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let old_url = url(FEED);
    let podcast_id = store
        .subscribe(&mut stamps, &meta(FEED, "Show"))
        .unwrap()
        .podcast_id;
    let items = [Item::new(
        Some("g1"),
        "One",
        "https://cdn.example.test/1.mp3",
    )];
    let temporary = url("https://mirror.example.test/feed.xml");
    let feed = parse(&rss("Show", None, &items), &temporary);
    let outcome = refresh(&store, &mut stamps, &podcast_id, &feed, &temporary, false);
    assert!(
        outcome.ops.is_empty(),
        "temporary redirects are not aliases"
    );
    assert_eq!(outcome.feed_url.as_ref(), Some(&old_url));

    let moved = url("https://new.example.test/feed.xml");
    let feed = parse(
        &rss("Show", Some("https://newest.example.test/rss?k=1"), &items),
        &moved,
    );
    let outcome = refresh(&store, &mut stamps, &podcast_id, &feed, &moved, true);
    assert_eq!(outcome.ops.len(), 1);
    let newest = url("https://newest.example.test/rss?k=1");
    assert_eq!(outcome.feed_url.as_ref(), Some(&newest));
    assert!(matches!(
        outcome.ops[0].payload,
        SyncOpPayload::PodcastFeedAliasesAdded { .. }
    ));
    let again = refresh(&store, &mut stamps, &podcast_id, &feed, &newest, false);
    assert!(
        again.ops.is_empty(),
        "no op when the alias set does not change"
    );
    for alias in [&old_url, &moved, &newest] {
        assert_eq!(store.resolve_podcast_id(alias).unwrap(), podcast_id);
    }
    // Subscribing through an alias resolves to the same podcast.
    let outcome = store
        .subscribe(
            &mut stamps,
            &meta("https://new.example.test/feed.xml", "Show"),
        )
        .unwrap();
    assert_eq!(outcome.podcast_id, podcast_id);
    assert!(outcome.already_subscribed);
    // Refreshing an unknown podcast fails.
    let unknown = PodcastId::from_feed_url(&url("https://unknown.test/"));
    assert!(
        store
            .apply_feed(
                &mut stamps,
                &FeedRefresh {
                    podcast_id: &unknown,
                    feed: &feed,
                    fetched_from: &newest,
                    moved_permanently: false,
                },
            )
            .is_err()
    );
}

#[test]
fn episodes_learned_through_sync_keep_their_ids_after_refresh() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-b", 1_000);
    let feed_url = url(FEED);
    let podcast_id = PodcastId::from_feed_url(&feed_url);
    // Device A identified a GUID-less episode by its link; B learns it from
    // a progress operation before it ever fetches the feed.
    let item = EpisodeIdentityHints {
        guid: None,
        enclosure_url: Some("https://cdn.example.test/x.mp3".to_string()),
        link: Some("https://example.test/ep/x".to_string()),
        published_at_ms: None,
        title: Some("X".to_string()),
    };
    let id = resolve_episode_ids(&podcast_id, Some("https://example.test/"), &[item], &[])[0]
        .clone()
        .unwrap();
    let reference = PodcastEpisodeRef {
        episode_id: id.clone(),
        episode_title: "X".to_string(),
        audio_url: "https://cdn.example.test/x.mp3".to_string(),
        ..episode_ref(FEED, "unused")
    };
    let op = SyncOpWire {
        op_id: "device-a:9".to_string(),
        origin_device_id: "device-a".to_string(),
        seq: 9,
        hlc_ms: 9,
        payload: SyncOpPayload::PodcastProgressSet {
            episode_id: id.clone(),
            podcast_id: podcast_id.clone(),
            position_ms: 77_000,
            status: ProgressStatus::InProgress,
            generation: 3,
            session_id: "s".to_string(),
            episode: Some(reference),
        },
    };
    assert_eq!(store.apply_remote_op(&op).unwrap(), ApplyOutcome::Changed);
    assert!(
        store
            .list_podcasts(PodcastListFilter::Subscribed)
            .unwrap()
            .is_empty()
    );
    store.subscribe(&mut stamps, &meta(FEED, "Show")).unwrap();
    // The feed now has a different link for the item, but the same enclosure.
    let feed = parse(
        &rss(
            "Show",
            None,
            &[Item {
                link: Some("https://example.test/ep/x-renamed"),
                ..Item::new(None, "X (renamed)", "https://cdn.example.test/x.mp3")
            }],
        ),
        &feed_url,
    );
    let outcome = refresh(&store, &mut stamps, &podcast_id, &feed, &feed_url, false);
    assert_eq!((outcome.new_episodes, outcome.updated_episodes), (0, 1));
    let item = store.episode(&id).unwrap().unwrap();
    assert_eq!(item.episode.title, "X (renamed)");
    assert_eq!(item.resume_position_ms(), 77_000);
}

// ---------------------------------------------------------------------------
// Queries

#[test]
fn seasons_filters_sorting_and_search() {
    let store = PodcastStore::open_in_memory().unwrap();
    let mut stamps = TestStamps::new("device-a", 1_000);
    let feed_url = url(FEED);
    let podcast_id = store
        .subscribe(&mut stamps, &meta(FEED, "Show"))
        .unwrap()
        .podcast_id;
    let feed = parse(
        &rss(
            "Show",
            None,
            &[
                Item {
                    season: Some(2),
                    number: Some(1),
                    date: Some("Mon, 01 Apr 2024 10:00:00 GMT"),
                    description: Some("Café talk"),
                    ..Item::new(
                        Some("s2e1"),
                        "Season two opener",
                        "https://cdn.example.test/a.mp3",
                    )
                },
                Item {
                    season: Some(1),
                    number: Some(2),
                    date: Some("Mon, 08 Jan 2024 10:00:00 GMT"),
                    ..Item::new(Some("s1e2"), "Second", "https://cdn.example.test/b.mp3")
                },
                Item {
                    season: Some(1),
                    number: Some(1),
                    date: Some("Mon, 01 Jan 2024 10:00:00 GMT"),
                    ..Item::new(Some("s1e1"), "Pilot", "https://cdn.example.test/c.mp3")
                },
                Item::new(
                    Some("bonus"),
                    "Bonus without season",
                    "https://cdn.example.test/d.mp3",
                ),
            ],
        ),
        &feed_url,
    );
    refresh(&store, &mut stamps, &podcast_id, &feed, &feed_url, false);
    let pilot = EpisodeId::from_guid(&podcast_id, "s1e1").unwrap();
    let reference = store.episode_ref(&pilot).unwrap().unwrap();
    store.mark_played(&mut stamps, &reference).unwrap();
    let second = store
        .episode_ref(&EpisodeId::from_guid(&podcast_id, "s1e2").unwrap())
        .unwrap()
        .unwrap();
    store
        .start_session(&mut stamps, &second, "s", 1_000, None)
        .unwrap();

    let seasons = store.seasons(&podcast_id).unwrap();
    let shape: Vec<(Option<u32>, u64, u64)> = seasons
        .iter()
        .map(|season| (season.season, season.episode_count, season.unplayed_count))
        .collect();
    assert_eq!(shape, vec![(Some(1), 2, 1), (Some(2), 1, 1), (None, 1, 1)]);
    let summary = store.podcast(&podcast_id).unwrap().unwrap();
    assert_eq!(
        (
            summary.episode_count,
            summary.unplayed_count,
            summary.in_progress_count,
            summary.season_count
        ),
        (4, 3, 1, 2)
    );

    let titles = |query: EpisodeQuery| -> Vec<String> {
        store
            .episodes(&query)
            .unwrap()
            .into_iter()
            .map(|item| item.episode.title)
            .collect()
    };
    let base = EpisodeQuery::new(podcast_id.clone());
    assert_eq!(
        titles(base.clone()),
        vec![
            "Season two opener",
            "Second",
            "Pilot",
            "Bonus without season"
        ]
    );
    assert_eq!(
        titles(EpisodeQuery {
            sort: EpisodeSort::EpisodeNumber,
            ..base.clone()
        }),
        vec![
            "Pilot",
            "Second",
            "Season two opener",
            "Bonus without season"
        ]
    );
    assert_eq!(
        titles(EpisodeQuery {
            season: SeasonFilter::Season(1),
            sort: EpisodeSort::OldestFirst,
            ..base.clone()
        }),
        vec!["Pilot", "Second"]
    );
    assert_eq!(
        titles(EpisodeQuery {
            season: SeasonFilter::NoSeason,
            ..base.clone()
        }),
        vec!["Bonus without season"]
    );
    assert_eq!(
        titles(EpisodeQuery {
            played: PlayedFilter::Played,
            ..base.clone()
        }),
        vec!["Pilot"]
    );
    assert_eq!(
        titles(EpisodeQuery {
            played: PlayedFilter::InProgress,
            ..base.clone()
        }),
        vec!["Second"]
    );
    assert_eq!(
        titles(EpisodeQuery {
            played: PlayedFilter::NotStarted,
            ..base.clone()
        })
        .len(),
        2
    );
    assert_eq!(
        titles(EpisodeQuery {
            search: Some("CAFE".to_string()),
            ..base.clone()
        }),
        Vec::<String>::new(),
        "normalization keeps accents distinct from their ASCII form"
    );
    assert_eq!(
        titles(EpisodeQuery {
            search: Some("CAFÉ".to_string()),
            ..base.clone()
        }),
        vec!["Season two opener"]
    );
    assert_eq!(
        titles(EpisodeQuery {
            search: Some("pilot".to_string()),
            limit: Some(1),
            ..base.clone()
        }),
        vec!["Pilot"]
    );
    assert_eq!(
        titles(EpisodeQuery {
            limit: Some(2),
            offset: 1,
            ..base
        }),
        vec!["Second", "Pilot"]
    );
}

#[test]
fn concurrent_openers_migrate_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(PODCAST_DB_FILE);
    drop(fabricate_prototype(&path));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let path = path.clone();
            std::thread::spawn(move || {
                PodcastStore::open(&path).map(|store| store.schema_version())
            })
        })
        .collect();
    for handle in handles {
        assert_eq!(
            handle.join().unwrap().unwrap().unwrap(),
            PODCAST_SCHEMA_VERSION
        );
    }
}
