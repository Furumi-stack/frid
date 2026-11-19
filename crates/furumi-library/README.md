# furumi-library

Client-independent persistence foundation for Furumi music players.

The crate owns the shared `library.db` contract: SQLite schema and migrations,
catalog models and queries, local import and metadata extraction, artwork
resolution, playlists, likes, listening history, and similarity index storage.
It contains no UI, playback engine, or application runtime.

During local development clients use a path dependency:

```toml
furumi-library = { path = "../frid/crates/furumi-library" }
```

Adjust the relative path to match the position of the client repository. After
publication, clients can depend on the released crate version instead.

`Library::open(default_db_path()?)` opens the same platform-native Furumi
library used by every client. Connections use WAL, foreign keys, and a busy
timeout so independently running clients can safely share the database.

Version 0.3 follows `music-dht` 0.6, whose device-sync and podcast contract
types it re-exposes.

## Podcasts

`furumi_library::podcasts::PodcastStore` is the SQLite reference store of the
podcast contract (workspace `PODCASTS.md`, section "SQLite reference store").
It lives in its own file, conventionally `podcasts.sqlite3`
(`PODCAST_DB_FILE`) next to `library.db`; the caller passes the path.

- **Migrations** run on `PRAGMA user_version`, each in its own transaction.
  A database of the original podcast prototype (`user_version` 1) is imported:
  its tables are renamed `legacy_podcasts` / `legacy_podcast_episodes` (never
  dropped), feeds become subscriptions with contract ids, rows that cannot be
  imported are listed in `legacy_quarantine`. A database from a newer Furumi,
  or a file that is not a podcast database, is refused without being touched.
- **Local actions** (`subscribe`, `unsubscribe`, `set_shared`,
  `start_session`, `checkpoint`, `seek`, `restart`, `complete_session`,
  `mark_played`, `mark_unplayed`, `record_listen_session`) draw stamps from a
  `StampSource` (usually the client's sync log), apply the operation and
  return `PodcastLocalOp`s to append to the log with exactly those stamps.
- **Sync**: `apply_remote_op` applies the five `podcast_*` operations,
  `fill_snapshot` / `export_snapshot` produce the bounded snapshot sections
  and `apply_snapshot` merges them. Everything goes through the `music-dht`
  materializers and merge functions, so the result does not depend on
  delivery order and never loses newer local state.
- **RSS**: `apply_feed` ingests a document parsed by
  `music_dht::podcast::feed` (the crate enables the `podcast-feed` feature):
  channel metadata, episodes with stable ids and current audio URLs, feed
  aliases from permanent redirects and `itunes:new-feed-url`. It never
  touches subscriptions, progress or history. HTTP, conditional requests and
  scheduling stay in the client; `record_fetch_*`, `fetch_state` and
  `podcasts_due_for_refresh` keep the local-only fetch state, which is never
  synced.
- **Queries**: `list_podcasts`, `seasons`, `episodes` (season, played-state
  and text filters, date or episode-number order), `resume_position_ms`,
  `episode_ref` / `episode_track` for queueing, `listen_history`.

The store is synchronous: methods take `&self` and lock one connection, so
`Arc<PodcastStore>` can be shared; run calls on a blocking pool.

`TrackItem.podcast` marks a queue item as a podcast episode
(`TrackItem::podcast_episode`). Music-only code (likes, playlists, history,
content ids, federation downloads) must check `TrackItem::is_podcast`; the
music library never stores episodes.

Key queue items, the current track and position matching by
`TrackItem::key`, which returns a typed `TrackItemKey` (`lib:<id>`,
`fed:<owner>:<item_id>` or `ep:<episode id>` as a string), not by
`TrackItem::id`: a negative id only means "not a library row". Every podcast
item has the same sentinel `id`, `release_id` and artist id
(`TrackItem::NOT_A_LIBRARY_ROW` = -1). It carries no identity and must not
be decoded; it only keeps music-only code that predates podcasts from taking
an episode for library track, release or artist 0. The episode's identity is
`TrackItem.podcast` alone.
