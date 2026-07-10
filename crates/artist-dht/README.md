# artist-dht

A proof-of-concept **distributed artist directory** built on top of
[`federation-net`](../federation-net). Every running process is a full DHT
participant — client, router and storage node at once. There are no
bootstrap servers, index nodes or search servers: the "server" is the set of
running peers itself.

## What it demonstrates

```text
/add on Peer B  →  name normalization  →  publish index records into the DHT
                →  replication to regular peers
/search on A/C  →  iterative Kademlia-style lookup (no broadcast)
                →  record of Peer B is found, with its owner id
```

* Records are published under BLAKE3 keys: one **exact key** for the whole
  normalized name and one **token key** per word, so `/search massive` finds
  *Massive Attack*.
* Each peer has a stable 256-bit `NodeId` derived from its persistent
  `federation-net` endpoint id; records live on the `K = 8` XOR-closest
  nodes.
* Peers discover each other through automatic Hello/PeerExchange gossip;
  connections to newly learned peers are opened **on demand** from stored
  tickets.
* Deletions propagate as **tombstones** (revision-based, they beat active
  records of the same or lower revision). Replicas expire by TTL (30 min
  active, 2 h tombstones); owners republish every 10 minutes.
* Local state (identity, own artists, replicas, known peers) lives in
  `<data_dir>/state.sqlite3` and `<data_dir>/identity.key`.

Out of scope (by design): fuzzy search, content transfer, CRDTs, consensus,
signatures on DHT records, Sybil protection, accounts, GUI.

## Bootstrapping limitation

There is deliberately **no bootstrap server**, so a peer cannot join a
network it has no contact in: you must pass the ticket of *any* already
running peer via `--connect`. That first peer has no special role — once
contacts have spread through peer exchange, it can be switched off.

## Running the demo (three peers)

Start Peer A (alice):

```bash
cargo run -p artist-dht-cli -- \
  --data-dir ./peer-a \
  --network-id demo-artists \
  --name alice
```

Copy the printed `Ticket: fnet...`. Start Peer B (bob) with it:

```bash
cargo run -p artist-dht-cli -- \
  --data-dir ./peer-b \
  --network-id demo-artists \
  --name bob \
  --connect 'fnet...'
```

Start Peer C (charlie), connected **only to A** — it will learn about B via
peer exchange:

```bash
cargo run -p artist-dht-cli -- \
  --data-dir ./peer-c \
  --network-id demo-artists \
  --name charlie \
  --connect 'fnet...'
```

`--connect` may be repeated to dial several peers. All three processes must
use the same `--network-id`; a peer from another network is rejected during
the transport handshake.

### Demo scenario

1. On **bob**: `/add Massive Attack` and `/add Portishead` — each prints the
   artist id and how many DHT nodes stored the replicas.
2. On **alice**: `/search massive attack` — the DHT results list *Massive
   Attack* with bob's endpoint id as the owner.
3. On **charlie**: `/search portishead` (and `/search massive`) — same
   records found through an iterative lookup, not a broadcast.
4. Stop **bob** (Ctrl+C). Both alice and charlie still find the records from
   replicas until the TTL expires.
5. Restart **bob** with the same `--data-dir`: the endpoint id is unchanged,
   the local database is intact and records are republished automatically.
6. On **bob**: `/delete <ARTIST_ID>` (a unique hex prefix is enough) — a
   tombstone propagates and the other peers stop returning the record.

### Commands

```text
/help                  show help
/id                    show endpoint and node ids
/ticket                print the connection ticket
/peers                 list open connections
/routing               list known DHT contacts (connected / last seen)
/list                  list local artists
/add <ARTIST_NAME>     add and publish an artist
/delete <ARTIST_ID>    delete a local artist (id or unique hex prefix)
/search-local <QUERY>  search the local database only
/search <QUERY>        search locally and across the DHT
/republish             republish local records now
/quit                  shut down gracefully
```

## Protocol limits

```text
K = 8, ALPHA = 3, max lookup requests = 32
max contacts per PeerExchange = 32
max records per FindValue response = 100
max artist name = 512 bytes, max tokens = 32
max pending requests = 1024
request timeout = 5 s, lookup timeout = 15 s
```

## Library usage

The CLI is a thin wrapper around the `artist-dht` library:

```rust
use artist_dht::{ArtistDhtConfig, ArtistDhtService};
use federation_net::NetworkId;

let config = ArtistDhtConfig::builder()
    .data_dir("./peer-a")
    .network_id(NetworkId::from_name("demo-artists"))
    .build()?;
let (service, mut events) = ArtistDhtService::start(config).await?;

let (artist, stats) = service.add_artist("Massive Attack".into()).await?;
let outcome = service.search_network("massive").await?;
service.shutdown().await?;
```

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The integration tests start three real DHT nodes in one process and walk the
whole demo scenario (publish, distributed search, replica survival after the
owner leaves, restart, tombstones), so they need network access and take a
couple of minutes.
