# music-dht

A distributed **music library directory** built on
[`federation-net`](../federation-net): peers publish their local library
index — artists, releases and tracks (names and small metadata, **never
files**) — into a Kademlia-style DHT and search each other's libraries.
Every running node is simultaneously a client, a DHT router and a storage
node; there are no dedicated servers of any kind.

It combines the original DHT proof of concept with the richer record model and
application-oriented API used by Furumi.

## Records

A [`LibraryItem`] carries: `kind` (artist | release | track), `name`,
`artist_names` (main artists for releases/tracks), `featured_artist_names`
(for track guest appearances), `year`, `release_type`, `release_title`,
`track_number`, `disc_number`, `duration_seconds`, optional track `content_id`
(`b3:<64 hex>`), plus ownership and versioning metadata. Records are
published under one exact key (the
normalized name) and one token key per word of the name, every main/featured
artist name and the track release title, so searching for an artist also
returns their releases, tracks and guest appearances. Track records with a
`content_id` are also published under a content key for exact-audio fallback.

## API

The application does not add or delete records one by one — it declares the
desired state and the service diffs:

```rust
let (service, events) = MusicDhtService::start(config).await?;
// Publish (and later re-publish) the whole library; matched by local_key.
service.sync_library(vec![
    ItemSpec {
        local_key: "artist:1".into(),
        kind: ItemKind::Artist,
        name: "Massive Attack".into(),
        artist_names: vec![],
        featured_artist_names: vec![],
        year: None,
        release_type: None,
        release_title: None,
        track_number: None,
        disc_number: None,
        duration_seconds: None,
        content_id: None,
    },
    // ...
]).await?;
let outcome = service.search_network("teardrop").await?;
```

`sync_library` is idempotent: item ids are derived from
`(owner, kind, local_key)`, so unchanged items are skipped, changed ones are
republished with a bumped revision and items that disappeared from the input
are tombstoned network-wide.

## Peer discovery

With `.rendezvous(RendezvousConfig::default())` in the config, peers of a
network find each other knowing **only the network id** (a shared rendezvous
record in the public BitTorrent Mainline DHT — see the `federation-net`
README). Tickets (`service.ticket()` / `service.connect(ticket)`) remain as a
manual fallback for isolated networks.

The network id is a public rendezvous token: anyone who knows it can join
and see the published names. Use a unique, hard-to-guess name for a private
network.

## Consumers

[`furumi-fd`](../../../furumi-stack/furumi-fd) uses this crate for its
federation feature: every instance publishes its library index and can search
the libraries of all other instances on the same network.

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p music-dht
```

The integration test starts three real nodes in one process (they use Iroh's
public relay infrastructure), so it needs network access.
