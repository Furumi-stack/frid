//! Integration test: public podcast discovery on three real loopback iroh
//! nodes.
//!
//! * A publishes a podcast its user shared. B is another device of the same
//!   user: it applies the same merged registers (as device sync would deliver
//!   them) and publishes the same podcast under its own key. C is an
//!   unrelated peer outside the user's trusted-device group, with no
//!   device-sync pairing.
//! * C finds the podcast by text and by id, merged over publishers {A, B};
//!   once B unshares and goes offline, C still finds A's card, so B is
//!   irrelevant to discovery. A podcast the user never shared (held by both
//!   A and B) is never discoverable; after A unpublishes, the tombstone hides
//!   the card from C.
//! * Forged, foreign, tampered, replayed, malformed, oversized and
//!   wrong-version requests are rejected by a real peer.
//! * D, a music-DHT peer without the overlay, is dialed by bootstrap but
//!   never answers on the overlay ALPN, so it never becomes an overlay contact
//!   and is never asked for records or to store them.
//! * `sync_shares` stays fail-safe for revocation and holds back URLs that
//!   look private (single node).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use music_dht::capabilities::{CapabilityManifest, PODCAST_DHT_ID};
use music_dht::podcast::{FeedUrl, PodcastId, PodcastMeta, ShareRecord, SubscriptionRecord};
use music_dht::podcast_dht::{
    MAX_PODCAST_DHT_REQUEST_BYTES, PODCAST_DHT_ALPN, PODCAST_DHT_PROTOCOL_VERSION, PodcastCard,
    PodcastCardSpec, PodcastDht, PodcastDhtOperation, PodcastDhtRequest, PodcastDhtResponse,
    PodcastRecordBody, PodcastSearchHit, PodcastShareState, PodcastStoreEntry, SignedPodcastRecord,
};
use music_dht::{MusicDhtConfig, MusicDhtError, MusicDhtService, NetworkId, NodeContact, NodeId};
use tokio::io::AsyncReadExt;

const TEST_TIMEOUT: Duration = Duration::from_secs(240);

struct Node {
    service: Arc<MusicDhtService>,
    dht: Arc<PodcastDht>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Node {
    async fn start(directory: &Path, network: NetworkId) -> Self {
        let (service, events) = start_service(directory, network, true).await;
        let acceptor = service.stream_acceptor(PODCAST_DHT_ALPN).unwrap();
        let dht = PodcastDht::open(Arc::clone(&service), directory.join("podcast-dht.sqlite3"))
            .await
            .unwrap();
        let tasks = vec![events, tokio::spawn(Arc::clone(&dht).serve(acceptor))];
        Self {
            service,
            dht,
            tasks,
        }
    }

    async fn stop(self) {
        for task in &self.tasks {
            task.abort();
        }
        self.service.shutdown().await.unwrap();
    }

    async fn contact(&self) -> NodeContact {
        let peer_id = self.service.endpoint_id();
        NodeContact {
            node_id: NodeId::from_endpoint(&peer_id),
            peer_id,
            ticket: self.service.ticket().await.unwrap().to_string(),
            last_seen_ms: 1,
        }
    }
}

async fn start_service(
    directory: &Path,
    network: NetworkId,
    overlay: bool,
) -> (Arc<MusicDhtService>, tokio::task::JoinHandle<()>) {
    std::fs::create_dir_all(directory).unwrap();
    let mut config = MusicDhtConfig::builder()
        .data_dir(directory)
        .network_id(network)
        .request_timeout(Duration::from_secs(2))
        .lookup_timeout(Duration::from_secs(5))
        .transport_timeout(Duration::from_secs(5))
        .dial_timeout(Duration::from_secs(2));
    if overlay {
        config = config.schema_independent_stream_protocol(PODCAST_DHT_ALPN);
    }
    let (service, mut events) = MusicDhtService::start(config.build().unwrap())
        .await
        .unwrap();
    let events = tokio::spawn(async move { while events.recv().await.is_some() {} });
    (Arc::new(service), events)
}

fn feed(path: &str) -> FeedUrl {
    FeedUrl::parse(&format!("https://feeds.example.test/{path}")).unwrap()
}

/// A subscription of A's user (generation 1).
fn subscription(path: &str, title: &str) -> SubscriptionRecord {
    let feed_url = feed(path);
    SubscriptionRecord {
        podcast_id: PodcastId::from_feed_url(&feed_url),
        subscribed: true,
        podcast: PodcastMeta {
            feed_url,
            title: title.to_string(),
            author: Some("Ferris Crab".to_string()),
            description: Some("Weekly news.".to_string()),
            link: None,
            image_url: None,
        },
        feed_aliases: Vec::new(),
        generation: 1,
        hlc_ms: 1_000,
        op_id: "device-a-op-1".to_string(),
    }
}

fn share(subscription: &SubscriptionRecord, shared: bool, generation: u64) -> ShareRecord {
    ShareRecord {
        podcast_id: subscription.podcast_id.clone(),
        shared,
        generation,
        hlc_ms: 2_000 + generation as i64,
        op_id: format!("device-a-share-{generation}"),
    }
}

async fn wait_for<T>(what: &str, mut check: impl AsyncFnMut() -> Option<T>) -> T {
    let started = std::time::Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Sends raw bytes as one overlay request and decodes the response.
async fn raw_request(from: &Node, to: &Node, payload: &[u8]) -> PodcastDhtResponse {
    let ticket = to.service.ticket().await.unwrap();
    let mut stream = from
        .service
        .open_stream_to(&ticket, PODCAST_DHT_ALPN)
        .await
        .unwrap();
    stream.send.write_all(payload).await.unwrap();
    stream.send.finish().unwrap();
    let mut bytes = Vec::new();
    AsyncReadExt::read_to_end(&mut stream.recv, &mut bytes)
        .await
        .unwrap();
    postcard::from_bytes(&bytes).unwrap()
}

async fn store_request(from: &Node, to: &Node, record: SignedPodcastRecord) -> PodcastDhtResponse {
    let request = PodcastDhtRequest {
        version: PODCAST_DHT_PROTOCOL_VERSION,
        requester: from.contact().await,
        operation: PodcastDhtOperation::StoreBatch {
            records: vec![PodcastStoreEntry {
                keys: record.dht_keys(),
                record,
            }],
        },
    };
    raw_request(from, to, &postcard::to_stdvec(&request).unwrap()).await
}

async fn search(node: &Node, query: &str) -> Vec<PodcastSearchHit> {
    node.dht.search(query, 10).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_podcast_is_discoverable_by_strangers_and_retractable() {
    tokio::time::timeout(TEST_TIMEOUT, scenario())
        .await
        .unwrap();
}

async fn scenario() {
    let temp = tempfile::tempdir().unwrap();
    let network = NetworkId::from_name("podcast-dht-integration");
    let a = Node::start(&temp.path().join("a"), network).await;
    let b = Node::start(&temp.path().join("b"), network).await;
    let c = Node::start(&temp.path().join("c"), network).await;
    // D runs the music DHT only.
    let (d, d_events) = start_service(&temp.path().join("d"), network, false).await;

    // B and C reach A through the ordinary music-DHT transport; C has no
    // device-sync pairing with A or B.
    b.service
        .connect(a.service.ticket().await.unwrap())
        .await
        .unwrap();
    c.service
        .connect(a.service.ticket().await.unwrap())
        .await
        .unwrap();
    d.connect(a.service.ticket().await.unwrap()).await.unwrap();
    wait_for("music DHT contacts", async || {
        (a.service.known_peers().len() >= 3
            && !b.service.known_peers().is_empty()
            && !c.service.known_peers().is_empty())
        .then_some(())
    })
    .await;
    for node in [&a, &b, &c] {
        node.dht.bootstrap().await.unwrap();
    }
    wait_for("podcast overlay contacts", async || {
        (a.dht.known_peers() >= 2 && c.dht.known_peers() >= 1).then_some(())
    })
    .await;
    // D is dialed by bootstrap but never answers on the overlay ALPN, so it
    // never becomes an overlay contact: it is never asked for records or to
    // store them.
    a.dht.bootstrap().await.unwrap();
    assert_eq!(a.dht.known_peers(), 2);

    // A capability manifest without podcast_dht evicts a peer; one with it
    // lets the peer be learned again.
    let manifest = CapabilityManifest::frid("furumi", "test").with_podcast_dht();
    a.dht
        .note_peer_capabilities(
            c.service.endpoint_id(),
            &manifest.clone().without_protocol(PODCAST_DHT_ID),
        )
        .await
        .unwrap();
    assert_eq!(a.dht.known_peers(), 1);
    a.dht.bootstrap().await.unwrap();
    assert_eq!(
        a.dht.known_peers(),
        1,
        "the evicted peer is not bootstrapped"
    );
    a.dht
        .note_peer_capabilities(c.service.endpoint_id(), &manifest)
        .await
        .unwrap();
    a.dht.bootstrap().await.unwrap();
    assert_eq!(a.dht.known_peers(), 2);

    // A request with another overlay version is refused.
    let request = PodcastDhtRequest {
        version: PODCAST_DHT_PROTOCOL_VERSION + 1,
        requester: c.contact().await,
        operation: PodcastDhtOperation::FindNode { target: [0; 32] },
    };
    let response = raw_request(&c, &a, &postcard::to_stdvec(&request).unwrap()).await;
    assert!(!response.ok);

    // A's user shares "Rust Radio Weekly" but never shares "Secret Garden".
    let shared = subscription("rust-radio.xml", "Rust Radio Weekly");
    let private = subscription("secret-garden.xml", "Secret Garden Diaries");
    let states = vec![
        PodcastShareState {
            subscription: shared.clone(),
            share: Some(share(&shared, true, 2)),
            card: None,
            privacy_confirmed: false,
        },
        PodcastShareState {
            subscription: private.clone(),
            share: None,
            card: None,
            privacy_confirmed: false,
        },
    ];
    let stats = a.dht.sync_shares(states.clone()).await.unwrap();
    assert_eq!(
        (stats.published, stats.unchanged, stats.unpublished),
        (1, 0, 0)
    );
    assert!(stats.push.local_replica);
    assert!(stats.push.remote_nodes >= 1);
    let error = a
        .dht
        .publish(
            &private,
            &share(&private, false, 2),
            PodcastCardSpec::from_subscription(&private).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MusicDhtError::PodcastPublication(_)));
    // Idempotent: nothing new is published.
    let again = a.dht.sync_shares(states).await.unwrap();
    assert_eq!(
        (again.published, again.unchanged, again.unpublished),
        (0, 1, 0)
    );
    let published = a.dht.published().await.unwrap();
    assert_eq!(published.len(), 1);
    let first_card = published[0].record.clone();

    // B, the same user's other device, holds the same merged registers and
    // publishes the same podcast under its own key; the private podcast
    // stays private there too.
    let b_states = vec![
        PodcastShareState {
            subscription: shared.clone(),
            share: Some(share(&shared, true, 2)),
            card: None,
            privacy_confirmed: false,
        },
        PodcastShareState {
            subscription: private.clone(),
            share: None,
            card: None,
            privacy_confirmed: false,
        },
    ];
    let stats = b.dht.sync_shares(b_states).await.unwrap();
    assert_eq!((stats.published, stats.unpublished), (1, 0));
    let mut both = vec![a.service.endpoint_id(), b.service.endpoint_id()];
    both.sort_by_key(|key| *key.as_bytes());
    let hits = wait_for("C sees both publishers", async || {
        let hits = search(&c, "rust radio").await;
        (hits.len() == 1 && hits[0].publishers == both).then_some(hits)
    })
    .await;
    assert_eq!(hits[0].feed_url, shared.podcast.feed_url);
    assert!(hits[0].claimed_feed_urls.is_empty());

    // B unshares (for example a stale register on B) and goes offline: B is
    // not needed for discovery, A's card is still found.
    let stats = b
        .dht
        .sync_shares(vec![PodcastShareState {
            subscription: shared.clone(),
            share: Some(share(&shared, false, 3)),
            card: None,
            privacy_confirmed: false,
        }])
        .await
        .unwrap();
    assert_eq!(stats.unpublished, 1);
    let a_only = vec![a.service.endpoint_id()];
    wait_for("B's tombstone reaches C's lookup", async || {
        let hits = search(&c, "rust radio").await;
        (hits.len() == 1 && hits[0].publishers == a_only).then_some(())
    })
    .await;
    b.stop().await;

    let hits = wait_for("C finds the card by text", async || {
        let hits = search(&c, "rust radio").await;
        (!hits.is_empty()).then_some(hits)
    })
    .await;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].podcast_id, shared.podcast_id);
    assert_eq!(hits[0].publishers, vec![a.service.endpoint_id()]);
    assert_eq!(hits[0].feed_url, shared.podcast.feed_url);
    assert_eq!(hits[0].card.feed_url, shared.podcast.feed_url);
    assert_eq!(hits[0].card.title, "Rust Radio Weekly");
    assert_eq!(search(&c, "ferris weekly").await.len(), 1, "author tokens");
    assert!(
        search(&c, "rust jazz").await.is_empty(),
        "every token must match"
    );
    let by_id = c.dht.lookup_podcast(&shared.podcast_id).await.unwrap();
    assert_eq!(by_id.unwrap().card.podcast_id, shared.podcast_id);

    // The never-shared podcast leaves no trace anywhere.
    assert!(search(&c, "secret garden").await.is_empty());
    assert!(search(&a, "secret garden").await.is_empty());
    assert!(
        c.dht
            .lookup_podcast(&private.podcast_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        a.dht
            .lookup_podcast(&private.podcast_id)
            .await
            .unwrap()
            .is_none()
    );

    // Forged: C signs a card that claims A as publisher.
    let forged_spec =
        PodcastCardSpec::from_subscription(&subscription("forged.xml", "Forged Show")).unwrap();
    let forged_card = PodcastCard::from_spec(
        forged_spec.clone(),
        network,
        a.service.endpoint_id(),
        u64::MAX - 1,
        first_card.issued_at_ms(),
    );
    let forged = SignedPodcastRecord {
        signature: c.service.sign_identity(&forged_card.signing_bytes()),
        body: PodcastRecordBody::Card(Box::new(forged_card)),
    };
    let response = store_request(&c, &a, forged).await;
    assert!(!response.ok && response.stored == 0, "{response:?}");

    // Foreign: A's genuine card stored by C. Only publishers store records.
    let response = store_request(&c, &a, first_card.clone()).await;
    assert!(!response.ok && response.stored == 0, "{response:?}");

    // Tampered: A's genuine card with a different description.
    let mut tampered = first_card.clone();
    if let PodcastRecordBody::Card(card) = &mut tampered.body {
        card.description = Some("Now with malware".into());
    }
    let response = store_request(&c, &a, tampered).await;
    assert!(!response.ok, "{response:?}");

    // Malformed and oversized requests.
    let response = raw_request(&c, &a, b"definitely not postcard").await;
    assert!(!response.ok);
    let response = raw_request(&c, &a, &vec![0u8; MAX_PODCAST_DHT_REQUEST_BYTES + 1]).await;
    assert!(!response.ok);
    assert!(response.error.unwrap().contains("size"));

    // A valid card signed by C is accepted: the overlay is open to anyone
    // who signs their own cards.
    let own_card = PodcastCard::from_spec(
        forged_spec,
        network,
        c.service.endpoint_id(),
        1,
        first_card.issued_at_ms(),
    );
    let own =
        SignedPodcastRecord::sign_card(own_card, |bytes| c.service.sign_identity(bytes)).unwrap();
    let response = store_request(&c, &a, own).await;
    assert!(response.ok && response.stored > 0, "{response:?}");
    let forged_hits = search(&a, "forged show").await;
    assert_eq!(forged_hits.len(), 1);
    assert_eq!(forged_hits[0].publishers, vec![c.service.endpoint_id()]);

    // A's user stops sharing: a tombstone with the next revision replaces
    // the card and C stops seeing it.
    let unshare = vec![PodcastShareState {
        subscription: shared.clone(),
        share: Some(share(&shared, false, 3)),
        card: None,
        privacy_confirmed: false,
    }];
    let stats = a.dht.sync_shares(unshare).await.unwrap();
    assert_eq!(stats.unpublished, 1);
    let published = a.dht.published().await.unwrap();
    assert!(!published[0].active);
    assert!(published[0].record.is_tombstone());
    assert_eq!(published[0].revision, first_card.revision() + 1);
    wait_for("tombstone hides the card from C", async || {
        search(&c, "rust radio").await.is_empty().then_some(())
    })
    .await;
    assert!(
        c.dht
            .lookup_podcast(&shared.podcast_id)
            .await
            .unwrap()
            .is_none()
    );

    // A replaying its own old card to a replica that stores the tombstone
    // does not bring it back; nobody else may replay it at all.
    let response = store_request(&a, &c, first_card.clone()).await;
    assert!(response.ok && response.stored == 0, "{response:?}");
    let response = store_request(&c, &a, first_card).await;
    assert!(!response.ok, "{response:?}");
    assert!(search(&c, "rust radio").await.is_empty());
    assert!(search(&a, "rust radio").await.is_empty());

    a.stop().await;
    c.stop().await;
    d_events.abort();
    d.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_shares_is_fail_safe_and_holds_back_private_urls() {
    tokio::time::timeout(TEST_TIMEOUT, single_node_scenario())
        .await
        .unwrap();
}

async fn single_node_scenario() {
    let temp = tempfile::tempdir().unwrap();
    let network = NetworkId::from_name("podcast-dht-sync-shares");
    let a = Node::start(&temp.path().join("a"), network).await;
    let state =
        |subscription: &SubscriptionRecord, shared: bool, generation: u64| PodcastShareState {
            subscription: subscription.clone(),
            share: Some(share(subscription, shared, generation)),
            card: None,
            privacy_confirmed: false,
        };
    let first = subscription("first.xml", "First Show");
    let second = subscription("second.xml", "Second Show");
    let stats = a
        .dht
        .sync_shares(vec![state(&first, true, 2), state(&second, true, 2)])
        .await
        .unwrap();
    assert_eq!(stats.published, 2);
    assert!(stats.failed.is_empty() && stats.held_back.is_empty());

    // In one call the user unshares the first podcast while the second
    // one's card cannot be built (the URL its id derives from is unknown).
    let mut broken = second.clone();
    broken.podcast.feed_url = feed("elsewhere.xml");
    let stats = a
        .dht
        .sync_shares(vec![state(&first, false, 3), state(&broken, true, 2)])
        .await
        .unwrap();
    assert_eq!(stats.failed.len(), 1);
    assert_eq!(stats.failed[0].podcast_id, second.podcast_id);
    assert_eq!(stats.unpublished, 2, "the unshared and the broken podcast");
    let published = a.dht.published().await.unwrap();
    assert!(published.iter().all(|publication| !publication.active));
    assert!(
        published
            .iter()
            .all(|publication| publication.record.is_tombstone())
    );
    // Republishing re-sends tombstones only; nothing comes back.
    a.dht.republish().await.unwrap();
    assert!(search(&a, "first show").await.is_empty());
    assert!(search(&a, "second show").await.is_empty());

    // A feed URL that looks private is held back until the user confirms.
    let mut member = subscription("member.xml?token=abc", "Member Show");
    member.podcast_id = PodcastId::from_feed_url(&member.podcast.feed_url);
    let stats = a
        .dht
        .sync_shares(vec![state(&member, true, 2)])
        .await
        .unwrap();
    assert_eq!(stats.published, 0);
    assert_eq!(stats.held_back.len(), 1);
    assert_eq!(stats.held_back[0].podcast_id, member.podcast_id);
    assert!(!stats.held_back[0].warnings.is_empty());
    assert!(search(&a, "member show").await.is_empty());
    let mut confirmed = state(&member, true, 2);
    confirmed.privacy_confirmed = true;
    let stats = a.dht.sync_shares(vec![confirmed]).await.unwrap();
    assert_eq!(stats.published, 1);
    assert_eq!(search(&a, "member show").await.len(), 1);
    // Once published with the user's consent, the same URLs are kept.
    let stats = a
        .dht
        .sync_shares(vec![state(&member, true, 2)])
        .await
        .unwrap();
    assert_eq!((stats.unchanged, stats.held_back.len()), (1, 0));

    a.stop().await;
}
