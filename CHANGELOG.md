# Changelog

## Unreleased — podcast contract

- `music-dht 0.6.0` (breaking): new `music_dht::podcast` module with the shared
  podcast contract: canonical `FeedUrl`, `PodcastId`/`EpisodeId` derivation
  (`furumi-podcast:*:v1`), feed aliases, the no-GUID episode identity rule,
  `PodcastMeta`/`EpisodeMeta`/`PodcastEpisodeRef`, and convergent merge rules
  for subscriptions, shares, progress registers and listening sessions.
- `music-dht 0.6.0`: optional `podcast-feed` feature with the shared tolerant
  RSS/Atom parser (`quick-xml`, `encoding_rs`, `url`).
- `music-dht 0.6.0`: device-sync protocol version 4 (ALPN unchanged): four
  `podcast_*` operation kinds, bounded podcast snapshot sections,
  `PlaybackTrack.source`, reserved `PlaybackStateWire.playback_rate`,
  `DeviceProfileSet.protocol_version`, version-gating helpers and Jam
  redaction. New struct fields and enum variants break exhaustive
  construction and matching in clients.
- `furumi-library 0.3.0`: follows `music-dht 0.6.0`, whose types it re-exposes.
- See `PODCASTS.md` for the normative contract.

## v0.4.1 — 2026-09-02

- `federation-net 0.3.1`: recover timed-out or failed Mainline-DHT rendezvous clients and expose network health to applications.
- `music-dht 0.4.1`: retain temporarily unreachable contacts and serialize duplicate dial attempts with bounded backoff.
