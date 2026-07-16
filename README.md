# federation-net

A reusable Rust library for direct peer-to-peer networking between equal
peers, built on [Iroh](https://iroh.computer). It establishes QUIC
connections that work through NATs (with hole punching and relay assistance),
and exchanges typed, application-defined messages over them.

The library contains **no domain logic** — no music, video, file libraries or
databases. The application defines its own message type; the engine treats it
as an opaque serde-serializable payload.

## What the first version does

* Persistent peer identity (`<data_dir>/identity.key`, created on first start).
* Connection establishment via a shareable string ticket (`fnet...`).
* Optional automatic peer discovery (**rendezvous**): peers of a network find
  each other through the BitTorrent Mainline DHT knowing nothing but the
  network id — no tickets and no bootstrap servers.
* An application-level handshake that isolates networks and schemas.
* Typed message exchange in both directions over one QUIC connection.
* Network events (connect, disconnect, message, protocol error).
* Graceful shutdown.

## What it deliberately does not do (yet)

No gossip, broadcast overlay, content search, file/chunk/streaming transfer,
database sync, CRDTs, authorization, ACLs, HTTP APIs or metrics. The
architecture allows adding these later as separate modules or ALPN protocols.

## Usage

Define your own message type — any `serde`-serializable type works, no extra
traits to implement:

```rust
use federation_net::{NetworkConfig, NetworkEngine, NetworkEvent, NetworkId, SchemaId};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum DemoMessage {
    Text { sender: String, body: String },
    Ping { nonce: u64 },
}

#[tokio::main]
async fn main() -> federation_net::Result<()> {
    let config = NetworkConfig::builder()
        .data_dir("./peer-a")
        .network_id(NetworkId::from_name("example-network"))
        .schema_id(SchemaId::from_name("demo-message-v1"))
        .build()?;

    let (engine, mut events) = NetworkEngine::<DemoMessage>::start(config).await?;

    // Share this string with another peer out of band.
    println!("Ticket: {}", engine.ticket().await?);

    while let Some(event) = events.recv().await {
        match event {
            NetworkEvent::PeerConnected { peer_id, .. } => {
                engine
                    .send(peer_id, &DemoMessage::Ping { nonce: 1 })
                    .await?;
            }
            NetworkEvent::MessageReceived { peer_id, message } => {
                println!("{peer_id}: {message:?}");
            }
            _ => {}
        }
    }
    engine.shutdown().await
}
```

## Running the demo

Start the first peer:

```bash
cargo run -p federation-net-demo -- \
  --data-dir ./tmp/peer-a \
  --network-id demo-network \
  --name alice
```

It prints its endpoint id and a ticket:

```text
Endpoint ID: ...
Network ID: ...
Schema ID: ...
Ticket: fnet...
Waiting for peers...
```

Start the second peer with that ticket:

```bash
cargo run -p federation-net-demo -- \
  --data-dir ./tmp/peer-b \
  --network-id demo-network \
  --name bob \
  --connect 'fnet...'
```

Type a line and press Enter to send it to all connected peers. Commands:
`/peers` lists connections, `/ticket` prints your ticket, `/ping` sends a
ping, `/quit` (or Ctrl+C) shuts down gracefully.

If the second peer uses a different `--network-id`, the connection is refused
with `Connection rejected: network id mismatch`; an incompatible schema is
refused with `Connection rejected: schema id mismatch`.

## NetworkId

A `NetworkId` identifies one distinct P2P network. It is 32 bytes, derived
deterministically from a name: `BLAKE3("federation-net:network:" + name)`.
The same name always yields the same id. Peers whose network ids differ
refuse to establish an application-level session, even though they share the
same transport protocol — this isolates independent deployments from each
other.

## SchemaId

A `SchemaId` identifies the wire format of the domain message type, derived
as `BLAKE3("federation-net:schema:" + name)` (e.g. `music-domain-v1`,
`demo-chat-v1`). Peers on the same network but with different schema ids
reject each other, because they could not decode each other's messages. Any
backwards-incompatible change to your message type requires a new schema
name.

## Rendezvous (peer discovery by network id)

Passing a `RendezvousConfig` to the config builder enables automatic peer
discovery: every peer periodically publishes its own Iroh address into a
shared BEP44 mutable record in the public BitTorrent Mainline DHT and dials
the addresses other peers published there. The record's signing key is
derived deterministically from the `NetworkId`, so knowing the network id is
enough to find and join the network; the first peer of a new network simply
publishes itself and waits.

```rust
let config = NetworkConfig::builder()
    .data_dir("./peer-a")
    .network_id(NetworkId::from_name("example-network"))
    .schema_id(SchemaId::from_name("demo-message-v1"))
    .rendezvous(federation_net::RendezvousConfig::default())
    .build()?;
```

Because the rendezvous record is world-readable, the network id acts as a
public rendezvous token: anyone who knows it can discover and join the
network. Treat the id of a private network like a shared secret (e.g.
`myorg-prod-7f3a`); the handshake still rejects peers whose network id does
not match exactly. Rendezvous is off by default — without it, peers are
connected via tickets only.

## Tickets

A `PeerTicket` is a self-contained invitation string with the `fnet` prefix
(base32-encoded postcard, versioned). It carries the peer's Iroh address
(endpoint id, relay URL and direct addresses) plus the network id, schema id
and protocol version, so incompatibility is detected before any message is
exchanged. Tickets implement `Display`/`FromStr` and round-trip through their
string form. The remote peer's identity is always taken from the
authenticated Iroh connection, never trusted from the ticket payload.

## Events

The engine reports what happens on the network through a single bounded
event channel (`NetworkEventReceiver`):

* `PeerConnected { peer_id, direction }` — a handshake completed
  (`Incoming` or `Outgoing`).
* `PeerDisconnected { peer_id, reason }` — a connection closed.
* `MessageReceived { peer_id, message }` — a domain message arrived and was
  decoded into your type.
* `ProtocolError { peer_id, error }` — a per-connection error; the engine
  itself keeps running.

Consume events promptly: the channel is bounded and the engine applies
back-pressure instead of buffering without limit.

## Example: distributed search (`artist-dht`)

The workspace also contains a bigger example built entirely on this library:
[`crates/artist-dht`](crates/artist-dht/README.md) — a Kademlia-style DHT
where every peer stores, routes and searches artist records without any
dedicated servers, plus its interactive CLI
[`apps/artist-dht-cli`](apps/artist-dht-cli). See its README for the
three-peer demo scenario.

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The integration tests establish real connections between two engines in one
process (they may use Iroh's public relay/discovery infrastructure), so they
need network access and take a few seconds.
