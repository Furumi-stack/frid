# Changelog

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
  code must skip podcast items. Podcast items use a negative per-episode id
  (`TrackItem::podcast_track_id`, at most `PODCAST_TRACK_ID_MAX`) and
  `release_id`/artist id -1, so existing `id < 0` / `release_id < 0` checks
  already treat them as non-library items. The library database is
  unchanged.
- `furumi-library 0.3.0`: prototype import collapses rows of one GUID-less
  item under several enclosure URLs, and the first refresh re-keys imported
  GUID-less episodes without personal state to the ids the current feed
  gives, so they match devices that never had the prototype. Fetch-state
  writes are atomic across threads and processes.
- See `PODCASTS.md` for the normative contract.

## v0.4.1 — 2026-09-02

- `federation-net 0.3.1`: recover timed-out or failed Mainline-DHT rendezvous clients and expose network health to applications.
- `music-dht 0.4.1`: retain temporarily unreachable contacts and serialize duplicate dial attempts with bounded backoff.
