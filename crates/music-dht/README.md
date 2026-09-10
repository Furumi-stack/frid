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

## Similarity protocol

`music_dht::similarity` is the shared, model-neutral wire contract for finding
tracks by a compatible embedding. It provides the versioned ALPN, bounded
request/response types, validation, and JSON byte-stream framing. It does not
generate embeddings or prescribe a vector database, model, preprocessing
pipeline, peer-routing policy, or user-consent UI; those remain client-owned.

Requests carry an L2-normalized vector and an exact model/profile fingerprint.
A peer must reject fingerprints it cannot search. This lets native, web, and
future Furumi clients implement local inference differently while remaining
wire-compatible when they choose the same embedding contract.

Results may carry a shared 128-bit SimHash of their embedding. Clients can use
its Hamming distance to suppress near-duplicate recordings across peers without
transmitting every result vector.

`music_dht::similarity_lsh` and `music_dht::similarity_dht` add decentralized
peer routing without putting models or vector storage into this crate. Every
client computes the same 256-bit SimHash, groups its local signatures into a
fixed two-level LSH (`12 × 20` bits, split into a 10-bit DHT key and a compact
10-bit suffix), and publishes one anonymous summary per peer and coarse bucket.
The summaries contain no track metadata, only the owner's self-contained route,
and are signed by its existing iroh identity, so a forwarding peer cannot alter
or impersonate them.

The routing overlay has its own schema-independent ALPN. Old clients therefore
remain compatible with ordinary catalog federation while upgraded peers find
one another and replicate expiring LSH summaries without a coordinator or a
global calibration file. Applications provide local routing signatures through
`SimilarityDht::sync_local_signatures`, use `SimilarityDht::find_peers` before
the direct similarity stream, and continue to perform exact search locally.

## Trusted-device sync and listening history

`music_dht::device_sync` is the canonical wire contract shared by Furumi
clients. Applications own persistence and UI policy, but must import the
protocol types from this module instead of maintaining local serde-compatible
copies.

Trusted-device sync is private user state and is separate from the public music
DHT. A sync group may contain desktop, web and future mobile clients. A
multi-user server behaves as one independent sync client per user and sync
group.

Protocol v2 adds append-only listening history:

```rust
use music_dht::device_sync::{
    ListenEndReason, ListenEvent, ListenTrackMetadata, SyncOpPayload,
};

let event = ListenEvent {
    // Generate when playback starts and reuse for every retry.
    listen_id: "0195d0b0-...".into(),
    content_id: "b3:...".into(),
    started_at_ms: 1_740_000_000_000,
    listened_ms: 151_000,
    track_duration_ms: Some(300_000),
    ended_reason: ListenEndReason::Skipped,
    track: ListenTrackMetadata {
        title: "Teardrop".into(),
        artist_names: vec!["Massive Attack".into()],
        featured_artist_names: vec![],
        release_title: Some("Mezzanine".into()),
    },
};

if event.should_record() {
    let payload = SyncOpPayload::ListenRecorded { event };
    // Append `payload` through the client's ordinary per-origin sync op log.
}
```

Client requirements:

1. Generate a stable `listen_id` at playback start. Retried HTTP requests and
   sync delivery must reuse it.
2. Accumulate actual listening time, excluding pauses and large seek jumps.
3. Finalize one immutable event when the track finishes, is skipped, is
   stopped, or is replaced.
4. Reject invalid events and interrupted listens shorter than
   `MIN_RECORDED_LISTEN_MS`. Use `ListenEvent::should_record`; do not implement
   a client-specific threshold.
5. Use `ListenEvent::qualifies_as_play` for play counts and the default history
   view. Natural completion qualifies; otherwise the threshold is the smaller
   of half the track duration and four minutes.
6. Materialize under unique `(sync_group, listen_id)`. The transport `op_id`
   remains independently unique and provides delivery deduplication.
7. Resolve tracks by `content_id`, never by another client's numeric database
   id. Keep `local_track_id` nullable and retain the metadata snapshot so
   network-only history remains displayable and scrobblable.
8. Preserve `origin_device_id` from `SyncOpWire`; resolve its current display
   name from the replicated device registry when rendering history.
9. Do not place history in `SyncSnapshot`. Catch up missing immutable listen
   operations through vector clocks and bounded op batches.
10. Do not publish listening history into DHT records or expose it outside the
    trusted-device group.

The library defines scrobble-neutral facts only. A client may implement an
optional integration such as Last.fm after materializing a qualifying event.
It must deduplicate that side effect by `listen_id`; other clients do not need
to know that the integration exists.

### Compatibility

Protocol v1 uses `furumi/sync/1`; v2 uses `furumi/sync/2`. During migration,
new clients should accept both ALPNs and only send `ListenRecorded` when
`supports_listen_history(peer.protocol_version)` returns true. Likes,
playlists and membership remain compatible with v1. Profile version 3 adds
versioned personal playback coordination over the existing JSON sync stream;
legacy unfenced playback messages are ignored by upgraded clients.

`music_dht::playback` provides the shared pure ownership engine and generic
snapshot/command envelopes. Configure startup and failover with `Config`, feed
actual output reports, and persist `Checkpoint` before applying decisions.
`Config::passive()` is intended for always-running gateways. See the workspace
`PLAYBACK_PROTOCOL.md` for invariants, configuration and migration details.

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p music-dht
```

The integration test starts three real nodes in one process (they use Iroh's
public relay infrastructure), so it needs network access.
