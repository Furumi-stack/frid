//! Integration tests: two engines in one Tokio runtime.

use std::path::Path;
use std::time::Duration;

use federation_net::{
    ConnectionDirection, EndpointId, NetworkConfig, NetworkEngine, NetworkError, NetworkEvent,
    NetworkEventReceiver, NetworkId, PeerTicket, SchemaId,
};

/// Hard cap on every test so a regression can never hang CI.
const TEST_TIMEOUT: Duration = Duration::from_secs(120);
/// Timeout used when waiting for a single event.
const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum TestMessage {
    Text { sender: String, body: String },
    Ping { nonce: u64 },
}

type Engine = NetworkEngine<TestMessage>;
type Events = NetworkEventReceiver<TestMessage>;

/// Serializes the network-facing tests: running many endpoints at once makes
/// relay discovery contend and produces spurious connect timeouts.
static NET_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn config(dir: &Path, network: &str, schema: &str) -> NetworkConfig {
    NetworkConfig::builder()
        .data_dir(dir)
        .network_id(NetworkId::from_name(network))
        .schema_id(SchemaId::from_name(schema))
        .request_timeout(Duration::from_secs(10))
        .build()
        .expect("valid test config")
}

async fn start(dir: &Path, network: &str, schema: &str) -> (Engine, Events) {
    NetworkEngine::start(config(dir, network, schema))
        .await
        .expect("engine starts")
}

/// Waits for the next event, panicking on timeout or channel close.
async fn next_event(events: &mut Events) -> NetworkEvent<TestMessage> {
    tokio::time::timeout(EVENT_TIMEOUT, events.recv())
        .await
        .expect("timed out waiting for an event")
        .expect("event channel closed unexpectedly")
}

/// Waits until a `PeerConnected` event for `peer` arrives, skipping unrelated
/// events (e.g. protocol errors from earlier rejected attempts).
async fn wait_connected(events: &mut Events, peer: EndpointId) -> ConnectionDirection {
    loop {
        if let NetworkEvent::PeerConnected { peer_id, direction } = next_event(events).await
            && peer_id == peer
        {
            return direction;
        }
    }
}

async fn wait_disconnected(events: &mut Events, peer: EndpointId) {
    loop {
        if let NetworkEvent::PeerDisconnected { peer_id, .. } = next_event(events).await
            && peer_id == peer
        {
            return;
        }
    }
}

async fn wait_message(events: &mut Events, peer: EndpointId) -> TestMessage {
    loop {
        if let NetworkEvent::MessageReceived { peer_id, message } = next_event(events).await
            && peer_id == peer
        {
            return message;
        }
    }
}

#[tokio::test]
async fn connect_and_exchange_messages() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (engine_a, mut events_a) = start(dir_a.path(), "test-net", "test-schema-v1").await;
        let (engine_b, mut events_b) = start(dir_b.path(), "test-net", "test-schema-v1").await;

        // 1. Peer A creates a ticket.
        let ticket = engine_a.ticket().await.expect("ticket");
        assert_eq!(ticket.endpoint_id(), engine_a.endpoint_id());

        // 2. Peer B connects using the ticket.
        let peer_a = engine_b.connect(ticket).await.expect("connect");
        assert_eq!(peer_a, engine_a.endpoint_id());

        // 3. Both sides observe PeerConnected with the right direction.
        let dir_on_b = wait_connected(&mut events_b, engine_a.endpoint_id()).await;
        assert_eq!(dir_on_b, ConnectionDirection::Outgoing);
        let dir_on_a = wait_connected(&mut events_a, engine_b.endpoint_id()).await;
        assert_eq!(dir_on_a, ConnectionDirection::Incoming);

        // 8. connected_peers contains the expected endpoint ids.
        assert_eq!(engine_b.connected_peers(), vec![engine_a.endpoint_id()]);
        assert_eq!(engine_a.connected_peers(), vec![engine_b.endpoint_id()]);

        // 4-5. B sends a message; A receives the correctly typed object.
        let hello = TestMessage::Text {
            sender: "bob".into(),
            body: "hello alice".into(),
        };
        engine_b
            .send(engine_a.endpoint_id(), &hello)
            .await
            .expect("send b -> a");
        let received = wait_message(&mut events_a, engine_b.endpoint_id()).await;
        assert_eq!(received, hello);

        // 6-7. A sends back over the same connection; B receives it.
        let pong = TestMessage::Ping { nonce: 4242 };
        engine_a
            .send(engine_b.endpoint_id(), &pong)
            .await
            .expect("send a -> b");
        let received = wait_message(&mut events_b, engine_a.endpoint_id()).await;
        assert_eq!(received, pong);

        engine_a.shutdown().await.expect("shutdown a");
        engine_b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn network_mismatch_is_rejected() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (engine_a, _events_a) = start(dir_a.path(), "network-one", "schema-v1").await;
        let (engine_b, _events_b) = start(dir_b.path(), "network-two", "schema-v1").await;

        let ticket = engine_a.ticket().await.expect("ticket");

        // Local pre-validation: the ticket carries A's network id.
        let err = engine_b
            .connect(ticket.clone())
            .await
            .expect_err("must fail");
        assert!(matches!(err, NetworkError::NetworkMismatch), "got {err:?}");

        // Remote validation: forge a ticket claiming B's own network id, so
        // the local check passes and the remote handshake must reject it.
        let forged = PeerTicket {
            network_id: engine_b.network_id(),
            ..ticket
        };
        let err = engine_b.connect(forged).await.expect_err("must fail");
        assert!(matches!(err, NetworkError::NetworkMismatch), "got {err:?}");

        assert!(engine_a.connected_peers().is_empty());
        assert!(engine_b.connected_peers().is_empty());

        engine_a.shutdown().await.expect("shutdown a");
        engine_b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn schema_mismatch_is_rejected() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (engine_a, _events_a) = start(dir_a.path(), "same-network", "schema-one").await;
        let (engine_b, _events_b) = start(dir_b.path(), "same-network", "schema-two").await;

        let ticket = engine_a.ticket().await.expect("ticket");

        // Local pre-validation.
        let err = engine_b
            .connect(ticket.clone())
            .await
            .expect_err("must fail");
        assert!(matches!(err, NetworkError::SchemaMismatch), "got {err:?}");

        // Remote validation with a forged schema id.
        let forged = PeerTicket {
            schema_id: engine_b.schema_id(),
            ..ticket
        };
        let err = engine_b.connect(forged).await.expect_err("must fail");
        assert!(matches!(err, NetworkError::SchemaMismatch), "got {err:?}");

        engine_a.shutdown().await.expect("shutdown a");
        engine_b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn shutdown_disconnects_peers() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (engine_a, mut events_a) = start(dir_a.path(), "test-net", "schema-v1").await;
        let (engine_b, mut events_b) = start(dir_b.path(), "test-net", "schema-v1").await;

        let ticket = engine_a.ticket().await.expect("ticket");
        engine_b.connect(ticket).await.expect("connect");
        wait_connected(&mut events_a, engine_b.endpoint_id()).await;
        wait_connected(&mut events_b, engine_a.endpoint_id()).await;

        let id_b = engine_b.endpoint_id();
        engine_b.shutdown().await.expect("shutdown b");

        // 11. A notices the disconnect and drops the peer from its registry.
        wait_disconnected(&mut events_a, id_b).await;
        assert!(engine_a.connected_peers().is_empty());

        // Sending to the gone peer now fails without hanging.
        let err = engine_a
            .send(id_b, &TestMessage::Ping { nonce: 1 })
            .await
            .expect_err("peer is gone");
        assert!(
            matches!(err, NetworkError::PeerNotConnected(_)),
            "got {err:?}"
        );

        // The event channel of the stopped engine closes after draining.
        while events_b.recv().await.is_some() {}

        engine_a.shutdown().await.expect("shutdown a");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn identity_persists_across_restarts() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (engine, _events) = start(dir.path(), "test-net", "schema-v1").await;
        let first_id = engine.endpoint_id();
        engine.shutdown().await.expect("shutdown");

        let (engine, _events) = start(dir.path(), "test-net", "schema-v1").await;
        let second_id = engine.endpoint_id();
        engine.shutdown().await.expect("shutdown");

        assert_eq!(first_id, second_id);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn disconnect_removes_peer() {
    let _net = NET_LOCK.lock().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (engine_a, mut events_a) = start(dir_a.path(), "test-net", "schema-v1").await;
        let (engine_b, mut events_b) = start(dir_b.path(), "test-net", "schema-v1").await;

        let ticket = engine_a.ticket().await.expect("ticket");
        let peer_a = engine_b.connect(ticket).await.expect("connect");
        wait_connected(&mut events_a, engine_b.endpoint_id()).await;
        wait_connected(&mut events_b, peer_a).await;

        engine_b.disconnect(peer_a).await.expect("disconnect");
        wait_disconnected(&mut events_b, peer_a).await;
        assert!(engine_b.connected_peers().is_empty());

        // Disconnecting twice reports PeerNotConnected.
        let err = engine_b.disconnect(peer_a).await.expect_err("already gone");
        assert!(
            matches!(err, NetworkError::PeerNotConnected(_)),
            "got {err:?}"
        );

        engine_a.shutdown().await.expect("shutdown a");
        engine_b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}
