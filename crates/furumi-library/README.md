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
