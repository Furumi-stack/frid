use std::sync::Arc;
use std::time::Duration;

use music_dht::similarity_dht::SimilarityDht;
use music_dht::similarity_lsh::{SIMILARITY_DHT_ALPN, routing_signature};
use music_dht::{MusicDhtConfig, MusicDhtService, NetworkId};

const TEST_DIRECT_ALPN: &[u8] = b"music-dht-test/similarity-owner/1";

async fn start_node(
    directory: &std::path::Path,
    network: NetworkId,
) -> (Arc<MusicDhtService>, tokio::task::JoinHandle<()>) {
    std::fs::create_dir_all(directory).unwrap();
    let config = MusicDhtConfig::builder()
        .data_dir(directory)
        .network_id(network)
        .schema_independent_stream_protocol(SIMILARITY_DHT_ALPN)
        .schema_independent_stream_protocol(TEST_DIRECT_ALPN)
        .request_timeout(Duration::from_secs(2))
        .lookup_timeout(Duration::from_secs(5))
        .transport_timeout(Duration::from_secs(5))
        .dial_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let (service, mut events) = MusicDhtService::start(config).await.unwrap();
    let task = tokio::spawn(async move { while events.recv().await.is_some() {} });
    (Arc::new(service), task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_lsh_summary_routes_a_query_to_another_peer() {
    let temp = tempfile::tempdir().unwrap();
    let network = NetworkId::from_name("similarity-dht-integration");
    let (first, first_events) = start_node(&temp.path().join("first"), network).await;
    let (second, second_events) = start_node(&temp.path().join("second"), network).await;

    let first_acceptor = first.stream_acceptor(SIMILARITY_DHT_ALPN).unwrap();
    let second_acceptor = second.stream_acceptor(SIMILARITY_DHT_ALPN).unwrap();
    let mut second_direct_acceptor = second.stream_acceptor(TEST_DIRECT_ALPN).unwrap();
    let first_routing = SimilarityDht::open(
        Arc::clone(&first),
        temp.path().join("first-routing.sqlite3"),
    )
    .await
    .unwrap();
    let second_routing = SimilarityDht::open(
        Arc::clone(&second),
        temp.path().join("second-routing.sqlite3"),
    )
    .await
    .unwrap();
    let first_serve = tokio::spawn(Arc::clone(&first_routing).serve(first_acceptor));
    let second_serve = tokio::spawn(Arc::clone(&second_routing).serve(second_acceptor));

    second.connect(first.ticket().await.unwrap()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while first.known_peers().is_empty() || second.known_peers().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let vector = vec![0.5; 4];
    let profile = "sim1:integration";
    let stats = second_routing
        .sync_local_signatures(
            profile.to_string(),
            vec![routing_signature(&vector).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(stats.records, 12);
    assert!(stats.local_replica);
    assert_eq!(stats.remote_nodes, 1);

    let peers = tokio::time::timeout(
        Duration::from_secs(10),
        first_routing.find_peers(profile, &vector, 16),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        peers.first().map(|ticket| ticket.endpoint_id()),
        Some(second.endpoint_id())
    );
    let mut outbound = first
        .open_stream_to(peers.first().unwrap(), TEST_DIRECT_ALPN)
        .await
        .unwrap();
    let inbound = tokio::time::timeout(Duration::from_secs(5), second_direct_acceptor.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inbound.peer_id, first.endpoint_id());
    outbound.send.finish().unwrap();
    drop(inbound);
    drop(outbound);

    first_serve.abort();
    second_serve.abort();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    first_events.abort();
    second_events.abort();
}
