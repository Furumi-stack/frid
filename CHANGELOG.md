# Changelog

## Unreleased — podcast discovery (`podcast_dht`)

- `music-dht 0.6.0`: new `music_dht::podcast_dht` overlay for public podcast
  discovery on its own ALPN `furumi-fd/podcast-dht/1` (version 1) and its own
  SQLite cache (schema 2): signed `PodcastCard`s and `PodcastTombstone`s
  (`furumi-podcast-card:v1` / `furumi-podcast-card-tombstone:v1` canonical
  signing input), monotonic revisions with a total tie-break, text and id
  keys, `PodcastDht::{publish, unpublish, sync_shares, republish, search,
  lookup_podcast}` gated by `effective_share`, typed search hits
  (`PodcastSearchHit`, `ClaimedFeedUrl`, `FederatedSearchHit`,
  `SearchHitKind`, `merge_search_results`) and the advisory
  `feed_url_privacy_warning`.
- Replicas accept records only from their publisher, store one payload per
  `(podcast_id, publisher)`, admit placements only for keys they are close to
  and within per-key, per-publisher and total budgets, and never evict
  existing records for newcomers. Search offers only the URL the podcast id
  hashes from; moved-feed URLs are reported as unverified claims.
- `sync_shares` is fail-safe for revocation (per-podcast `failed`) and holds
  back cards with privacy warnings until confirmed (`held_back`,
  `PodcastShareState::privacy_confirmed`).
- `music-dht 0.6.0`: capability manifests can advertise `podcast_dht = 1`
  (`capabilities::PODCAST_DHT_ID`, `CapabilityManifest::with_podcast_dht`,
  `CapabilityManifest::protocol_version`,
  `CapabilityManifest::supports_podcast_dht`). It is opt-in:
  `CapabilityManifest::frid()` does not include it.
- `music-dht 0.6.0`: new `MusicDhtError::PodcastPublication` variant (the enum
  is `#[non_exhaustive]`).
- The music DHT (`music-dht-v5`), `federation-net` and device-sync protocol v4
  are unchanged. See "Public discovery (podcast_dht)" in `PODCASTS.md`.

## Unreleased — podcast contract

- `music-dht 0.6.0` (breaking): new `music_dht::podcast` module with the shared
  podcast contract: canonical `FeedUrl`, `PodcastId`/`EpisodeId` derivation
  (`furumi-podcast:*:v1`), feed aliases, the no-GUID episode identity rule,
  `PodcastMeta`/`EpisodeMeta`/`PodcastEpisodeRef`, and convergent merge rules
  for subscriptions, shares, progress registers and listening sessions.
- `music-dht 0.6.0`: optional `podcast-feed` feature with the shared tolerant
  RSS/Atom parser (`quick-xml`, `encoding_rs`, `url`).
- `music-dht 0.6.0`: device-sync protocol version 4 (ALPN unchanged): five
  `podcast_*` operation kinds (subscription, share, feed aliases, progress,
  listening sessions), bounded podcast snapshot sections,
  `PlaybackTrack.source`, reserved `PlaybackStateWire.playback_rate`,
  `DeviceProfileSet.protocol_version`, optional `InviteWire` protocol version
  (`p`), self-reported vs relayed version helpers, version-gating helpers and
  Jam redaction. New struct fields and enum variants break exhaustive
  construction and matching in clients.
- `furumi-library 0.3.0`: follows `music-dht 0.6.0`, whose types it re-exposes.
- `furumi-library 0.3.0`: new `furumi_library::podcasts::PodcastStore`, the
  SQLite reference store of the podcast contract in its own
  `podcasts.sqlite3`: `user_version` migrations that import the prototype
  podcast database (tables kept as `legacy_*`, unimportable rows in
  `legacy_quarantine`) and refuse newer or foreign files unchanged; podcasts,
  feed aliases, seasons, episodes, subscription/share/progress registers,
  listening sessions and local-only fetch state; local actions that return
  the `podcast_*` operations for the client's sync log; `apply_remote_op`,
  bounded snapshot export and merge; RSS refresh ingestion with stable
  episode ids; UI queries. Enables the `music-dht` `podcast-feed` feature.
- `furumi-library 0.3.0` (breaking): `TrackItem.podcast:
  Option<PodcastEpisodeRef>` and `TrackItem::podcast_episode`/`is_podcast`.
  Exhaustive `TrackItem` literals in clients need the new field; music-only
  code must skip podcast items. Podcast items use the fixed sentinel
  `TrackItem::NOT_A_LIBRARY_ROW` (-1) as `id`, `release_id` and artist id,
  so existing `id < 0` / `release_id < 0` checks already treat them as
  non-library items; the sentinel carries no identity. New
  `TrackItem::key` / `TrackItemKey` (`lib:<id>`, `fed:<owner>:<item_id>`,
  `ep:<episode id>`), the source identity: episodes must be keyed by it (or
  `TrackItem.podcast`), never by `id`; content-first music matching stays
  as it is. New `TrackItem::from_playback_track` / `PlaybackItem` convert
  received playback items without copying wire ids (0 for episodes and Jam
  private placeholders). The library database is unchanged.
- `furumi-library 0.3.0`: prototype import collapses rows of one GUID-less
  item under several enclosure URLs, and the first refresh re-keys imported
  GUID-less episodes without personal state to the ids the current feed
  gives, so they match devices that never had the prototype; when another
  episode (for example one created by a remote operation) already holds
  that id, the imported copy is deleted instead of kept as a duplicate.
  Fetch-state
  writes are atomic across threads and processes.
- See `PODCASTS.md` for the normative contract.

## v0.4.1 — 2026-09-02

- `federation-net 0.3.1`: recover timed-out or failed Mainline-DHT rendezvous clients and expose network health to applications.
- `music-dht 0.4.1`: retain temporarily unreachable contacts and serialize duplicate dial attempts with bounded backoff.
