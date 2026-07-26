# federation-net

`federation-net` is a generic Rust networking engine for equal peers, built on
[iroh](https://iroh.computer). It establishes authenticated QUIC connections
through NATs, exchanges typed application messages, and exposes direct byte
streams for additional protocols.

The crate contains no music or application database logic. Applications own
their message schema and persistent domain state.

## Capabilities

- Persistent Ed25519/iroh identity.
- Self-contained, versioned peer tickets.
- Optional peer rendezvous through BEP44 records in the BitTorrent Mainline
  DHT.
- Network and message-schema isolation during handshake.
- Typed bidirectional messages with bounded framing and backpressure.
- Application-defined ALPN byte streams.
- Connection path and traffic statistics.
- Graceful disconnect and shutdown events.

## Basic usage

```rust,no_run
use federation_net::{NetworkConfig, NetworkEngine, NetworkEvent, NetworkId, SchemaId};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum Message {
    Ping { nonce: u64 },
}

# async fn run() -> federation_net::Result<()> {
let config = NetworkConfig::builder()
    .data_dir("./peer-a")
    .network_id(NetworkId::from_name("example-network"))
    .schema_id(SchemaId::from_name("example-message-v1"))
    .build()?;

let (engine, mut events) = NetworkEngine::<Message>::start(config).await?;
println!("share this ticket: {}", engine.ticket().await?);

while let Some(event) = events.recv().await {
    if let NetworkEvent::PeerConnected { peer_id, .. } = event {
        engine.send(peer_id, &Message::Ping { nonce: 1 }).await?;
    }
}

engine.shutdown().await
# }
```

## Network and schema IDs

`NetworkId` selects an independent peer network. `SchemaId` selects the wire
format of the application's typed messages. Both are checked during the
application handshake; peers with mismatched values are rejected before
domain messages are decoded.

Treat a backwards-incompatible message change as a schema change. Derive a new
schema ID instead of attempting to decode incompatible payloads under the old
identifier.

## Discovery

Rendezvous is optional. When enabled, peers publish signed, expiring endpoint
records into a network-specific BEP44 record in the public Mainline DHT. A
shared network ID is enough to discover current participants; no
Frid-operated registry is involved.

```rust,no_run
# use federation_net::{NetworkConfig, NetworkId, RendezvousConfig, SchemaId};
# fn config() -> federation_net::Result<()> {
let config = NetworkConfig::builder()
    .data_dir("./peer-a")
    .network_id(NetworkId::from_name("example-network"))
    .schema_id(SchemaId::from_name("example-message-v1"))
    .rendezvous(RendezvousConfig::default())
    .build()?;
# drop(config);
# Ok(())
# }
```

The rendezvous record is discoverable by anyone who knows the network ID. It
provides discovery, not authorization. Applications that need restricted
membership must implement and enforce that policy in their own protocol.

Without rendezvous, peers can connect through tickets and operate on isolated
networks.

## Byte streams

Applications may register additional ALPNs in `NetworkConfig` and accept or
open authenticated streams through the same endpoint. This is how higher
layers add catalog, audio, or synchronization protocols without putting large
payloads into the typed message channel.

## Events and backpressure

The bounded event channel reports connection, disconnection, decoded message,
and protocol-error events. Consumers must drain it continuously. The engine
applies backpressure instead of allowing unbounded memory growth.

Per-connection protocol failures are reported without stopping the engine.

## Verification

From the workspace root:

```bash
cargo test -p federation-net
cargo clippy -p federation-net --all-targets -- -D warnings
```
