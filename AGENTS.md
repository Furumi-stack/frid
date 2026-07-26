# AGENTS.md

These instructions apply to the entire Frid workspace.

## Project role

Frid is shared networking infrastructure. `federation-net` is the generic
transport; `music-dht` is the distributed music-directory overlay used by
Furumi applications. Treat public APIs, persisted state, hash derivations, and
wire formats as compatibility-sensitive.

Read `ARCHITECTURE.md` before changing protocols, routing, persistence,
rendezvous, tickets, identity, or record derivation.

## Required checks

The workspace uses Rust edition 2024 and Rust 1.97 or newer.

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Integration tests open real sockets and establish iroh connections. If a
restricted environment prevents socket binding or discovery, rerun them with
the required permission and report the limitation; do not weaken or delete the
tests.

## Compatibility

Do not change these without an explicit migration/versioning decision:

- ALPN byte strings;
- transport, DHT, device-sync, catalog, or ticket versions;
- `NetworkId`, `SchemaId`, `NodeId`, `ItemId`, or `DhtKey` derivation domains;
- postcard/serde wire enum layouts;
- content-ID normalization;
- SQLite schema or identity-key encoding;
- public re-exports from either crate.

Postcard is not self-describing. Adding a field to a serialized type can be a
breaking wire change even when serde would accept the Rust source change.

## Layer boundaries

- `federation-net` owns identity, iroh endpoints, handshake, tickets,
  rendezvous, typed framing, event backpressure, and generic byte streams.
- `federation-net` must not acquire music, catalog, playlist, or application
  authorization logic.
- `music-dht` owns Kademlia routing, requests, records, replication, search,
  content lookup, and its persistence abstraction.
- Rich catalogs, audio, and device sync use dedicated ALPN stream protocols;
  do not push bulk data through the typed DHT event channel.
- Applications own policy: knowing a network ID or discovering a peer is not
  proof of authorization.

## Async and concurrency

- Never hold a mutex guard across `.await`.
- Keep SQLite and blocking filesystem work off Tokio reactor threads.
- Bound channels, request maps, batches, frames, and peer-provided collections.
- Make shutdown idempotent and ensure background tasks cannot retain event
  senders indefinitely.
- Preserve request peer-matching so a response from the wrong peer cannot
  resolve another peer's request.
- Use dial backoff and bounded lookup work for dead contacts.

## DHT behavior

- Routing uses stable node IDs and XOR distance; do not replace derivation or
  ordering as a cosmetic refactor.
- Validate that a record belongs under the key on which it was received.
- Owners advance revisions; replicas do not rewrite ownership.
- Tombstones must beat active records at the same or older revision.
- TTL refresh, expiry, and republish behavior must remain deterministic.
- `sync_library` is declarative and idempotent: unchanged items stay stable,
  changed items advance, and removed items are tombstoned.
- Search may return partial results when peers fail, but lookup must terminate.

## Tests

Add unit tests beside deterministic routing, framing, validation, storage, and
merge logic. Use integration tests for behavior that depends on authenticated
connections, handshakes, streams, disconnects, or multi-node lookup.

Tests must use temporary directories and isolated network/schema IDs. Never
use a developer identity, repository `tmp/` state, fixed public network, or
real application database as a fixture.

For protocol changes, test both rejection of incompatible peers and the
intended compatible path. For input-bound changes, test the limit and the first
value beyond it.

## Documentation and packaging

- Keep crate-level rustdoc and README examples compiling.
- Root documentation describes the workspace; crate READMEs describe their
  public API and operational model.
- Do not describe Mainline-DHT rendezvous as private or as authorization.
- Keep demo crates `publish = false`.
- Published packages must include the WTFPL version 2 license text.
- Update `ARCHITECTURE.md` when changing a protocol boundary or invariant.
