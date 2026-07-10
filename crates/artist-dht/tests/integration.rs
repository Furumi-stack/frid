//! Integration tests: three DHT nodes in one Tokio runtime.

use std::path::Path;
use std::time::Duration;

use artist_dht::{
    ArtistDhtConfig, ArtistDhtError, ArtistDhtEventReceiver, ArtistDhtService, EndpointId,
    NetworkId,
};

/// Hard cap on every test so a regression can never hang CI.
const TEST_TIMEOUT: Duration = Duration::from_secs(240);

/// Serializes the network-facing tests: many concurrent endpoints contend on
/// relay discovery and produce spurious timeouts.
static NET_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type Started = (ArtistDhtService, ArtistDhtEventReceiver);

async fn start(dir: &Path, network: &str) -> Started {
    let config = ArtistDhtConfig::builder()
        .data_dir(dir)
        .network_id(NetworkId::from_name(network))
        // Fast timers so republish/expiry behavior is observable in tests.
        .republish_interval(Duration::from_secs(5))
        .expire_interval(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(5))
        .lookup_timeout(Duration::from_secs(10))
        .build()
        .expect("valid config");
    ArtistDhtService::start(config)
        .await
        .expect("service starts")
}

/// Polls `check` until it returns `Some` or the deadline passes.
async fn wait_for<T>(
    what: &str,
    deadline: Duration,
    mut check: impl AsyncFnMut() -> Option<T>,
) -> T {
    let started = std::time::Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(started.elapsed() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

fn knows_peer(service: &ArtistDhtService, peer: EndpointId) -> bool {
    service
        .known_peers()
        .iter()
        .any(|contact| contact.peer_id == peer)
}

/// The full demo scenario: bootstrap through one peer, peer exchange,
/// publish, distributed search, replica survival, restart and tombstones.
#[tokio::test]
async fn full_distributed_flow() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let dir_c = tempfile::tempdir().expect("tempdir");

        // Step 1: peer A starts alone and issues a ticket.
        let (a, _events_a) = start(dir_a.path(), "test-artists").await;
        let ticket_a = a.ticket().await.expect("ticket");

        // Step 2: peer B joins through A.
        let (b, _events_b) = start(dir_b.path(), "test-artists").await;
        let peer_a = b.connect(ticket_a.clone()).await.expect("b connects to a");
        assert_eq!(peer_a, a.endpoint_id());

        // Step 3: peer C joins through A only...
        let (c, _events_c) = start(dir_c.path(), "test-artists").await;
        c.connect(ticket_a.clone()).await.expect("c connects to a");

        // ...and learns about B via peer exchange, without connecting to it.
        wait_for(
            "C to learn about B via peer exchange",
            Duration::from_secs(30),
            async || knows_peer(&c, b.endpoint_id()).then_some(()),
        )
        .await;

        // Step 4: B adds artists; they are published into the DHT.
        let (massive, stats) = b
            .add_artist("Massive Attack".into())
            .await
            .expect("add artist");
        assert_eq!(massive.normalized_name, "massive attack");
        // 1 exact key + 2 token keys.
        assert_eq!(stats.keys, 3);
        assert!(
            stats.remote_nodes >= 1,
            "the record must be replicated to at least one other node"
        );
        b.add_artist("Portishead".into()).await.expect("add artist");

        // Step 5: A finds the record through the DHT (exact search).
        let outcome = a.search_network("massive attack").await.expect("search");
        assert!(outcome.local_results.is_empty(), "A has no local artists");
        assert_eq!(outcome.network_results.len(), 1);
        assert_eq!(outcome.network_results[0].name, "Massive Attack");
        // The result names the owner, peer B.
        assert_eq!(outcome.network_results[0].owner, b.endpoint_id());

        // Step 6: C finds Portishead via a single-token search.
        let outcome = c.search_network("portishead").await.expect("search");
        assert_eq!(outcome.network_results.len(), 1);
        assert_eq!(outcome.network_results[0].owner, b.endpoint_id());

        // Token search also finds multi-token names.
        let outcome = c.search_network("massive").await.expect("search");
        assert!(
            outcome
                .network_results
                .iter()
                .any(|artist| artist.name == "Massive Attack"),
            "token search must find 'Massive Attack'"
        );

        // Step 7: stop B; replicas must keep the record findable.
        let b_id = b.endpoint_id();
        b.shutdown().await.expect("shutdown b");
        let outcome = a.search_network("massive attack").await.expect("search");
        assert_eq!(
            outcome.network_results.len(),
            1,
            "the record must survive on replicas after the owner left"
        );
        let outcome = c.search_network("massive attack").await.expect("search");
        assert_eq!(outcome.network_results.len(), 1);

        // Step 8: B restarts with the same data dir: same identity, and its
        // local database is intact.
        let (b, _events_b) = start(dir_b.path(), "test-artists").await;
        assert_eq!(b.endpoint_id(), b_id, "endpoint id must persist");
        let local = b.list_local_artists().await.expect("list");
        assert_eq!(local.len(), 2, "local database must persist");
        b.connect(ticket_a).await.expect("b reconnects to a");

        // Step 9: B deletes Massive Attack; the tombstone propagates and the
        // other peers stop returning the record.
        let stats = b.delete_artist(massive.id).await.expect("delete");
        assert!(stats.remote_nodes >= 1, "tombstone must reach replicas");
        let outcome = a.search_network("massive attack").await.expect("search");
        assert!(
            outcome.network_results.is_empty(),
            "A must not return a tombstoned record, got {:?}",
            outcome.network_results
        );
        let outcome = c.search_network("massive").await.expect("search");
        assert!(
            outcome
                .network_results
                .iter()
                .all(|artist| artist.id != massive.id),
            "C must not return the tombstoned record"
        );
        // Portishead is still there.
        let outcome = a.search_network("portishead").await.expect("search");
        assert_eq!(outcome.network_results.len(), 1);

        a.shutdown().await.expect("shutdown a");
        b.shutdown().await.expect("shutdown b");
        c.shutdown().await.expect("shutdown c");
    })
    .await
    .expect("test timed out");
}

/// A peer from a different network is rejected by the transport handshake.
#[tokio::test]
async fn different_network_is_rejected() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (a, _events_a) = start(dir_a.path(), "network-one").await;
        let (b, _events_b) = start(dir_b.path(), "network-two").await;

        let ticket = a.ticket().await.expect("ticket");
        let err = b.connect(ticket).await.expect_err("must be rejected");
        assert!(
            matches!(err, ArtistDhtError::Network(_)),
            "expected a network error, got {err:?}"
        );
        assert!(b.connected_peers().is_empty());

        a.shutdown().await.expect("shutdown a");
        b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}

/// A lookup over dead contacts finishes within its budget and timeout
/// instead of hanging.
#[tokio::test]
async fn lookup_terminates_with_dead_contacts() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (a, _events_a) = start(dir_a.path(), "test-artists").await;
        let (b, _events_b) = start(dir_b.path(), "test-artists").await;

        let ticket_a = a.ticket().await.expect("ticket");
        b.connect(ticket_a).await.expect("connect");
        wait_for("A to learn about B", Duration::from_secs(30), async || {
            knows_peer(&a, b.endpoint_id()).then_some(())
        })
        .await;

        // Kill B: A still remembers it in the routing table.
        b.shutdown().await.expect("shutdown b");

        let started = std::time::Instant::now();
        let outcome = a.search_network("anything").await.expect("search finishes");
        assert!(outcome.network_results.is_empty());
        // Bounded by the lookup timeout per key (exact + 1 token) with slack
        // for connection attempts; the essential property is that it returns.
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "lookup took too long: {:?}",
            started.elapsed()
        );

        a.shutdown().await.expect("shutdown a");
    })
    .await
    .expect("test timed out");
}
