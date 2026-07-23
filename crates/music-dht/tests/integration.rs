//! Integration test: three DHT nodes in one Tokio runtime synchronizing and
//! searching a small music library (artists, releases, tracks).

use std::path::Path;
use std::time::Duration;

use music_dht::{
    EndpointId, ItemKind, ItemSpec, MusicDhtConfig, MusicDhtEventReceiver, MusicDhtService,
    NetworkId,
};

/// Hard cap on the test so a regression can never hang CI.
const TEST_TIMEOUT: Duration = Duration::from_secs(240);

/// Serializes the network-facing tests: many concurrent endpoints contend on
/// relay discovery and produce spurious timeouts.
static NET_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type Started = (MusicDhtService, MusicDhtEventReceiver);

async fn start(dir: &Path, network: &str) -> Started {
    let config = MusicDhtConfig::builder()
        .data_dir(dir)
        .network_id(NetworkId::from_name(network))
        .republish_interval(Duration::from_secs(5))
        .expire_interval(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(5))
        .lookup_timeout(Duration::from_secs(10))
        .build()
        .expect("valid config");
    MusicDhtService::start(config)
        .await
        .expect("service starts")
}

fn spec(local_key: &str, kind: ItemKind, name: &str, artists: &[&str]) -> ItemSpec {
    ItemSpec {
        local_key: local_key.to_string(),
        kind,
        name: name.to_string(),
        artist_names: artists.iter().map(|s| s.to_string()).collect(),
        featured_artist_names: Vec::new(),
        year: Some(1998),
        release_type: (kind == ItemKind::Release).then(|| "album".to_string()),
        release_title: (kind == ItemKind::Track).then(|| "Mezzanine".to_string()),
        track_number: (kind == ItemKind::Track).then_some(10),
        disc_number: (kind == ItemKind::Track).then_some(1),
        duration_seconds: (kind == ItemKind::Track).then_some(287.0),
        content_id: (kind == ItemKind::Track).then(|| {
            "b3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string()
        }),
    }
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

fn knows_peer(service: &MusicDhtService, peer: EndpointId) -> bool {
    service
        .known_peers()
        .iter()
        .any(|contact| contact.peer_id == peer)
}

/// Searches repeatedly until `accept` returns true for the results, or the
/// deadline passes (the DHT is eventually consistent and a transiently
/// dropped connection can make a single search come up short).
async fn search_until(
    what: &str,
    service: &MusicDhtService,
    query: &str,
    accept: impl Fn(&[music_dht::LibraryItem]) -> bool,
) -> Vec<music_dht::LibraryItem> {
    wait_for(what, Duration::from_secs(45), async || {
        let outcome = service.search_network(query).await.expect("search");
        accept(&outcome.network_results).then_some(outcome.network_results)
    })
    .await
}

#[tokio::test]
async fn library_sync_and_distributed_search() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let dir_c = tempfile::tempdir().expect("tempdir");

        // Bootstrap: B and C join through A's ticket; C learns about B via
        // peer exchange.
        let (a, _events_a) = start(dir_a.path(), "test-library").await;
        let ticket_a = a.ticket().await.expect("ticket");
        let (b, _events_b) = start(dir_b.path(), "test-library").await;
        b.connect(ticket_a.clone()).await.expect("b connects to a");
        let (c, _events_c) = start(dir_c.path(), "test-library").await;
        c.connect(ticket_a.clone()).await.expect("c connects to a");
        wait_for(
            "C to learn about B via peer exchange",
            Duration::from_secs(30),
            async || knows_peer(&c, b.endpoint_id()).then_some(()),
        )
        .await;

        // B publishes a small library: an artist, their release, one track.
        let library = vec![
            spec("artist:1", ItemKind::Artist, "Massive Attack", &[]),
            spec(
                "release:1",
                ItemKind::Release,
                "Mezzanine",
                &["Massive Attack"],
            ),
            spec("track:1", ItemKind::Track, "Teardrop", &["Massive Attack"]),
        ];
        let stats = b.sync_library(library.clone()).await.expect("sync");
        assert_eq!(stats.added, 3, "all three items are new");
        assert_eq!(stats.failed, 0);

        // A immediately-repeated sync publishes nothing.
        let stats = b.sync_library(library.clone()).await.expect("sync");
        assert_eq!(stats.unchanged, 3, "identical sync must be a no-op");

        // A finds the track by its exact title.
        let results = search_until("A to find the track", &a, "teardrop", |items| {
            items.len() == 1
        })
        .await;
        let track = &results[0];
        assert_eq!(track.kind, ItemKind::Track);
        assert_eq!(track.name, "Teardrop");
        assert_eq!(track.artist_names, vec!["Massive Attack".to_string()]);
        assert_eq!(track.duration_seconds, Some(287.0));
        assert_eq!(track.owner, b.endpoint_id());

        // C searches by the artist's name and finds all three kinds: the
        // artist itself plus the release and track carrying its name.
        search_until("C to find all three kinds", &c, "massive attack", |items| {
            let kinds: Vec<ItemKind> = items.iter().map(|item| item.kind).collect();
            [ItemKind::Artist, ItemKind::Release, ItemKind::Track]
                .iter()
                .all(|kind| kinds.contains(kind))
        })
        .await;

        // A content change is republished with a higher revision. The
        // "mezzanine" query also returns the track (its release title is
        // indexed), so pick the release out of the results.
        let mut changed = library.clone();
        changed[1].year = Some(1997);
        let stats = b.sync_library(changed.clone()).await.expect("sync");
        assert_eq!(stats.updated, 1);
        assert_eq!(stats.unchanged, 2);
        let results = search_until("A to see the updated release", &a, "mezzanine", |items| {
            items
                .iter()
                .any(|item| item.kind == ItemKind::Release && item.year == Some(1997))
        })
        .await;
        let release = results
            .iter()
            .find(|item| item.kind == ItemKind::Release)
            .expect("release in results");
        assert!(release.revision >= 2);

        // Removing the track from the library tombstones it network-wide.
        let mut shrunk = changed.clone();
        shrunk.retain(|spec| spec.local_key != "track:1");
        let stats = b.sync_library(shrunk).await.expect("sync");
        assert_eq!(stats.removed, 1);
        search_until("the tombstone to hide the track", &a, "teardrop", |items| {
            items.is_empty()
        })
        .await;

        // The rest of the library is still reachable — including from
        // replicas after the owner shuts down.
        b.shutdown().await.expect("shutdown b");
        search_until(
            "the release to survive on replicas after the owner left",
            &c,
            "mezzanine",
            |items| items.len() == 1,
        )
        .await;

        a.shutdown().await.expect("shutdown a");
        c.shutdown().await.expect("shutdown c");
    })
    .await
    .expect("test timed out");
}

/// Two nodes exchange raw bytes over an auxiliary stream protocol: the
/// requester finds an item through the DHT and then opens a byte stream to
/// its owner (the flow a file-transfer application follows).
#[tokio::test]
async fn byte_stream_to_item_owner() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        const BLOB_ALPN: &[u8] = b"music-test/blob/1";

        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");

        // Node A owns the library and serves byte streams.
        let config_a = MusicDhtConfig::builder()
            .data_dir(dir_a.path())
            .network_id(NetworkId::from_name("stream-test-net"))
            .request_timeout(Duration::from_secs(5))
            .lookup_timeout(Duration::from_secs(10))
            .stream_protocol(BLOB_ALPN)
            .build()
            .expect("valid config");
        let (node_a, _events_a) = MusicDhtService::start(config_a)
            .await
            .expect("service starts");
        let mut acceptor = node_a.stream_acceptor(BLOB_ALPN).expect("acceptor");
        let payload = b"pretend this is FLAC".to_vec();
        let served = payload.clone();
        let serve_task = tokio::spawn(async move {
            let mut stream = acceptor.accept().await.expect("incoming stream");
            stream.send.write_all(&served).await.expect("write");
            stream.send.finish().expect("finish");
            let _ = stream.send.stopped().await;
            stream.peer_id
        });
        node_a
            .sync_library(vec![spec(
                "track:1",
                ItemKind::Track,
                "Teardrop",
                &["Massive Attack"],
            )])
            .await
            .expect("sync");

        // Node B joins via ticket and finds the track and its owner.
        let (node_b, _events_b) = start(dir_b.path(), "stream-test-net").await;
        let ticket = node_a.ticket().await.expect("ticket");
        node_b.connect(ticket).await.expect("connect");
        let owner = search_until("the track by name", &node_b, "teardrop", |items| {
            !items.is_empty()
        })
        .await
        .pop()
        .expect("found item")
        .owner;
        assert_eq!(owner, node_a.endpoint_id());

        // B streams the bytes from the owner.
        let mut stream = node_b
            .open_stream(owner, BLOB_ALPN)
            .await
            .expect("open stream");
        let mut received = Vec::new();
        let mut chunk = [0u8; 1024];
        while let Some(n) = stream.recv.read(&mut chunk).await.expect("read") {
            received.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(received, payload);
        assert_eq!(serve_task.await.expect("serve task"), node_b.endpoint_id());

        node_a.shutdown().await.expect("shutdown a");
        node_b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}
