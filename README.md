# frid

Frid is the decentralized networking core behind Furumi. It provides reusable
Rust libraries for building equal-peer applications with no application
server, central peer registry, or privileged node.

The workspace has two layers:

- [`federation-net`](crates/federation-net) establishes authenticated,
  NAT-traversing P2P connections over iroh, isolates independent networks and
  message schemas, and optionally discovers peers through the BitTorrent
  Mainline DHT.
- [`music-dht`](crates/music-dht) builds a Kademlia-style distributed music
  directory on top of that transport, including catalog discovery, content-id
  lookup, signed similarity-LSH routing, direct byte streams, and shared Furumi
  wire types.

Each participant is a client, router, and storage peer. Automatic rendezvous
has no Frid-operated bootstrap service, while self-contained peer tickets
remain available for explicit or isolated connections.

## Design principles

- **Equal peers.** No node receives a permanent coordinator or server role.
- **Application-owned protocols.** The transport moves typed messages and
  byte streams without owning domain state.
- **Bounded inputs.** Frames, tickets, routing tables, requests, and record
  batches have explicit limits.
- **Offline-tolerant discovery.** Records are replicated, refreshed by their
  owners, expired by TTL, and removed through revisioned tombstones.
- **Protocol isolation.** Network IDs, schema IDs, versions, and ALPNs prevent
  unrelated or incompatible applications from being merged accidentally.
- **Local persistence.** Identity, routing knowledge, owned records, and
  replicas survive restarts in application-controlled storage.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the protocol layers, trust model,
failure handling, and compatibility rules.

## Workspace

```text
crates/
  federation-net/       generic iroh transport and rendezvous
  music-dht/            distributed music catalog and content discovery
```

## Using the libraries

Until crates are published to a registry, depend on the repository directly:

```toml
[dependencies]
federation-net = { git = "https://gt.hexor.cy/ab/frid.git" }
music-dht = { git = "https://gt.hexor.cy/ab/frid.git" }
```

Start with the crate documentation:

- [`federation-net` guide](crates/federation-net/README.md)
- [`music-dht` guide](crates/music-dht/README.md)

## Development

Frid uses Rust edition 2024 and requires Rust 1.97 or newer.

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The integration tests open real local sockets and establish iroh connections.
Some rendezvous tests also use public discovery infrastructure, so a restricted
sandbox may need explicit network permission.

## License

Frid is released under the
[Do What The Fuck You Want To Public License, Version 2](LICENSE).
