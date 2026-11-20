# frid

Frid is the shared infrastructure core behind Furumi. It provides reusable
Rust libraries for local music persistence and equal-peer networking without
an application server, central peer registry, or privileged node.

The workspace has three layers:

- [`federation-net`](crates/federation-net) establishes authenticated,
  NAT-traversing P2P connections over iroh, isolates independent networks and
  message schemas, and optionally discovers peers through the BitTorrent
  Mainline DHT.
- [`music-dht`](crates/music-dht) builds a Kademlia-style distributed music
  directory on top of that transport, including catalog discovery, content-id
  lookup, signed similarity-LSH routing, direct byte streams, and shared Furumi
  wire types, including the shared podcast contract ([PODCASTS.md](PODCASTS.md)).
- [`furumi-library`](crates/furumi-library) owns the shared local `library.db`
  contract, migrations, catalog operations, metadata import, artwork,
  playlists, likes, history, and similarity persistence used by Furumi clients.

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
  furumi-library/       client-independent local music database and import
```

## Using the libraries

Use the published crates from crates.io:

```toml
[dependencies]
federation-net = "0.3.1"
music-dht = "0.6.0"
furumi-library = "0.3.0"
```

Start with the crate documentation:

- [`federation-net` guide](crates/federation-net/README.md)
- [`music-dht` guide](crates/music-dht/README.md)
- [`furumi-library` guide](crates/furumi-library/README.md)

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

## Publishing

Add these repository Actions secrets:

- `CARGO_REGISTRY_TOKEN`: a crates.io API token with publish access to
  `federation-net`, `music-dht` and `furumi-library`.
- `GITEA_PACKAGES_TOKEN`: a raw Gitea personal access token with
  `write:package` and permission to publish under the `Furumi` organization.
  Create it under user Settings → Applications. The workflow adds the
  required `Bearer` prefix itself; no username secret is needed.

Update changed crate versions, workspace dependency requirements, `Cargo.lock`
and `CHANGELOG.md`, then merge to `main`. Create `<crate>-v<version>` tags for
the changed crates and one workspace release tag, for example:

```bash
git tag -a music-dht-v0.6.0 -m 'music-dht 0.6.0'
git tag -a furumi-library-v0.3.0 -m 'furumi-library 0.3.0'
git tag -a v0.6.0 -m 'Frid podcast release'
git push origin music-dht-v0.6.0 furumi-library-v0.3.0 v0.6.0
```

Only the `v*` workspace tag triggers `.github/workflows/publish.yml`; the
crate tags identify each package release without starting duplicate uploads.
The workflow runs the workspace checks and verifies the release packages in
one build job. Two publishing jobs then upload without rebuilding
(`cargo publish --no-verify`), selecting missing versions independently in
crates.io and the Gitea mirror at
`https://gt.hexor.cy/api/packages/Furumi/cargo/`. The mirror includes all three
crates, including an unchanged version missing there. Already published
versions are skipped on retries; a registry lookup error fails the release
instead of assuming absence. Mirrored manifests retain crates.io dependency
references, so the mirror waits up to ten minutes for new workspace
dependencies to appear in crates.io before packaging its uploads.

Preview the selection with `python3 scripts/publish_crates.py --plan`, or
verify the packages without uploading with `--dry-run`. Add `--registry gitea`
to preview or publish to the mirror. If a secret was
missing or a publish failed, fix the cause and rerun the existing tag workflow;
do not move release tags. Manual dispatch must select the release tag as its
ref. Publication is performed by CI, not by the tagging commands above.

## License

Frid is released under the
[Do What The Fuck You Want To Public License, Version 2](LICENSE).
