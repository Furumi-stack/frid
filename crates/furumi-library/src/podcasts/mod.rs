//! SQLite reference store for podcasts (`podcasts.sqlite3`).
//!
//! [`PodcastStore`] persists the podcast contract of `music_dht::podcast`
//! for SQLite clients: the local episode catalog refreshed from RSS, the
//! synchronized personal registers (subscription, share, progress), the
//! grow-only listening sessions and the local-only fetch state. It owns no
//! networking, HTTP, scheduling or audio: clients fetch feeds, parse them
//! with `music_dht::podcast::feed`, append sync operations to their own log
//! and exchange snapshots. The normative rules live in the workspace
//! `PODCASTS.md` (section "SQLite reference store" describes this schema).
//!
//! # Files and migrations
//!
//! The store lives in its own file, conventionally [`PODCAST_DB_FILE`] next
//! to the library database; the caller passes the path. Connections use
//! WAL, foreign keys and a busy timeout. `PRAGMA user_version` drives the
//! migrations, each applied in its own `BEGIN IMMEDIATE` transaction that
//! re-checks the version, so concurrent openers and interrupted upgrades are
//! safe:
//!
//! | `user_version` | Schema |
//! | --- | --- |
//! | 0 | empty file |
//! | 1 | prototype schema of the original podcast PR (`podcasts`, `podcast_episodes`) |
//! | 2 | contract model; the prototype tables are renamed `legacy_*` and imported |
//!
//! A database written by a newer Furumi ([`PODCAST_SCHEMA_VERSION`] lower
//! than its `user_version`) or a file that is not a podcast database is
//! refused before anything is written ([`PodcastStoreError`]).
//!
//! # Personal state and sync
//!
//! Local actions ([`PodcastStore::subscribe`], [`PodcastStore::seek`], ...)
//! draw operation stamps from a [`StampSource`], build the device-sync
//! operation, apply it through the same code path as
//! [`PodcastStore::apply_remote_op`] (the `SyncOpWire` materializers of
//! `music_dht::device_sync` and the merge functions of `music_dht::podcast`)
//! and return the [`PodcastLocalOp`]s. The caller appends them to its sync
//! log **with exactly these stamps**; applying them again is a no-op.
//! Applying operations and snapshot sections is idempotent and independent
//! of delivery order.
//!
//! RSS refreshes ([`PodcastStore::apply_feed`]) only touch the catalog and
//! feed aliases: they never write a register, progress or a session.
//!
//! ```
//! use furumi_library::podcasts::{ApplyOutcome, OpStamp, PodcastStore, StampSource};
//! use music_dht::podcast::{FeedUrl, PodcastMeta};
//!
//! /// Stand-in for the client's sync log, which reserves seq and hlc.
//! struct Log {
//!     device: String,
//!     seq: i64,
//! }
//!
//! impl StampSource for Log {
//!     fn device_id(&self) -> &str {
//!         &self.device
//!     }
//!     fn next_stamp(&mut self) -> anyhow::Result<OpStamp> {
//!         self.seq += 1;
//!         let op_id = format!("{}:{}", self.device, self.seq);
//!         Ok(OpStamp::new(op_id, self.device.clone(), 1_700_000_000_000 + self.seq))
//!     }
//! }
//!
//! let store = PodcastStore::open_in_memory()?;
//! let mut log = Log { device: "device-a".into(), seq: 0 };
//! let meta = PodcastMeta {
//!     feed_url: FeedUrl::parse("https://example.test/feed.xml")?,
//!     title: "Show".into(),
//!     author: None,
//!     description: None,
//!     link: None,
//!     image_url: None,
//! };
//! let outcome = store.subscribe(&mut log, &meta)?;
//! assert_eq!(outcome.ops.len(), 1);
//! for op in outcome.ops {
//!     // Append to the sync log with this stamp; re-applying is a no-op.
//!     let op = op.into_wire(log.seq);
//!     assert_eq!(store.apply_remote_op(&op)?, ApplyOutcome::Unchanged);
//! }
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! # Thread safety
//!
//! All methods take `&self` and serialize on an internal mutex around one
//! synchronous `rusqlite` connection, so an `Arc<PodcastStore>` can be shared
//! between threads. Calls block on SQLite I/O: run them on a blocking pool
//! (for example `tokio::task::spawn_blocking`), never on an async reactor
//! thread. Several processes may open the same file; SQLite serializes the
//! writers.

mod actions;
mod apply;
mod queries;
mod refresh;
mod schema;

#[cfg(test)]
mod tests;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use anyhow::{Context as _, Result};
use music_dht::device_sync::{SyncOpPayload, SyncOpWire};
use music_dht::podcast::{
    EpisodeId, FeedUrl, ListenSession, PodcastError, PodcastId, PodcastMeta, ProgressRecord,
    ProgressStatus,
};
use rusqlite::Connection;

pub use actions::{CheckpointOutcome, ProgressSession, ProgressWrite, SubscribeOutcome};
pub use apply::{ApplyOutcome, SnapshotApplyStats};
pub use queries::{
    EpisodeListItem, EpisodeQuery, EpisodeSort, LegacyQuarantineEntry, PlayedFilter,
    PodcastListFilter, PodcastListenEntry, PodcastSummary, SeasonFilter, SeasonSummary,
};
pub use refresh::{FeedRefresh, FetchState, RefreshOutcome};
pub use schema::{LEGACY_PROTOTYPE_SCHEMA_VERSION, PODCAST_SCHEMA_VERSION};

/// Conventional file name of the podcast store, next to `library.db`.
pub const PODCAST_DB_FILE: &str = "podcasts.sqlite3";

/// `op_id` prefix of registers imported from the prototype database.
///
/// Imported subscriptions use generation 0, `hlc_ms` = the prototype
/// `added_at` and `op_id` = this prefix followed by the podcast id, so every
/// write made through the contract orders after them.
pub const LEGACY_IMPORT_OP_PREFIX: &str = "legacy-import:";

/// Typed errors of [`PodcastStore`]; other failures are SQLite or I/O errors.
///
/// They are returned inside `anyhow::Error`; use `downcast_ref` to match.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PodcastStoreError {
    /// The database was written by a newer Furumi. It was not modified.
    NewerSchema {
        /// `user_version` found in the file.
        found: i64,
        /// Newest version this build understands.
        supported: i64,
    },
    /// The file is not a podcast database (or has an unexpected shape). It
    /// was not modified.
    UnrecognizedDatabase(String),
    /// The podcast is unknown to this store.
    UnknownPodcast(PodcastId),
    /// The action needs an active subscription.
    NotSubscribed(PodcastId),
    /// Input violates the podcast contract.
    Invalid(String),
}

impl fmt::Display for PodcastStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NewerSchema { found, supported } => write!(
                f,
                "podcast database schema v{found} is newer than this Furumi supports (v{supported}); \
                 update Furumi, the database was left unchanged"
            ),
            Self::UnrecognizedDatabase(reason) => {
                write!(f, "not a Furumi podcast database: {reason}")
            }
            Self::UnknownPodcast(id) => write!(f, "unknown podcast {id}"),
            Self::NotSubscribed(id) => write!(f, "not subscribed to podcast {id}"),
            Self::Invalid(reason) => write!(f, "invalid podcast data: {reason}"),
        }
    }
}

impl std::error::Error for PodcastStoreError {}

impl From<PodcastError> for PodcastStoreError {
    fn from(error: PodcastError) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// Stamp of a device-sync operation: the fields the `SyncOpWire`
/// materializers turn into register stamps.
///
/// `seq` is not part of it: the client's sync log assigns it when it appends
/// the operation ([`PodcastLocalOp::into_wire`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpStamp {
    /// Unique operation id, usually `<origin_device_id>:<seq>`.
    pub op_id: String,
    /// Origin device id (the progress register `device_id`).
    pub origin_device_id: String,
    /// Operation timestamp (register `hlc_ms`).
    pub hlc_ms: i64,
}

impl OpStamp {
    /// Builds a stamp.
    pub fn new(op_id: impl Into<String>, origin_device_id: impl Into<String>, hlc_ms: i64) -> Self {
        Self {
            op_id: op_id.into(),
            origin_device_id: origin_device_id.into(),
            hlc_ms,
        }
    }

    /// The stamp of an existing operation.
    pub fn of(op: &SyncOpWire) -> Self {
        Self::new(op.op_id.clone(), op.origin_device_id.clone(), op.hlc_ms)
    }

    pub(crate) fn wire(&self, payload: SyncOpPayload) -> SyncOpWire {
        SyncOpWire {
            op_id: self.op_id.clone(),
            origin_device_id: self.origin_device_id.clone(),
            seq: 0,
            hlc_ms: self.hlc_ms,
            payload,
        }
    }
}

/// Source of operation stamps for local actions, usually the client's sync
/// log (which reserves the next sequence number and timestamp).
///
/// A stamp is drawn only when an action actually emits an operation.
pub trait StampSource {
    /// The local device id; every stamp must use it as origin.
    fn device_id(&self) -> &str;
    /// Reserves the stamp of the next local operation.
    fn next_stamp(&mut self) -> Result<OpStamp>;
}

/// A local operation produced by a [`PodcastStore`] action.
///
/// The store has already applied it. Append it to the sync log with exactly
/// this stamp: a different `op_id` or `hlc_ms` would make other devices
/// materialize a different register than this one.
#[derive(Debug, Clone, PartialEq)]
pub struct PodcastLocalOp {
    /// Stamp drawn from the [`StampSource`].
    pub stamp: OpStamp,
    /// Operation payload (one of the `podcast_*` kinds).
    pub payload: SyncOpPayload,
}

impl PodcastLocalOp {
    /// The operation with the sequence number assigned by the sync log.
    pub fn into_wire(self, seq: i64) -> SyncOpWire {
        let mut op = self.stamp.wire(self.payload);
        op.seq = seq;
        op
    }
}

/// SQLite store of podcast catalog, personal state and fetch state.
///
/// See the [module documentation](self) for the schema, the sync model and
/// thread safety.
pub struct PodcastStore {
    conn: Mutex<Connection>,
    path: Option<PathBuf>,
}

impl fmt::Debug for PodcastStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PodcastStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl PodcastStore {
    /// Opens (creating and migrating if needed) the podcast store at `path`.
    ///
    /// A prototype database (`user_version` 1) is imported; see the module
    /// documentation. Fails without modifying the file when it was written
    /// by a newer Furumi or is not a podcast database.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening podcast database {}", path.display()))?;
        Self::init(conn, Some(path.to_path_buf()))
    }

    /// Opens a private in-memory store (tests and previews).
    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?, None)
    }

    fn init(mut conn: Connection, path: Option<PathBuf>) -> Result<Self> {
        conn.busy_timeout(Duration::from_secs(5))?;
        // Refuse newer or foreign files before anything (WAL included) is written.
        schema::check_openable(&conn)?;
        if path.is_some() {
            let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        }
        conn.pragma_update(None, "foreign_keys", "ON")?;
        register_norm_function(&conn)?;
        schema::migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            path,
        })
    }

    /// Path of the database file (`None` for in-memory stores).
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Current `PRAGMA user_version` (always [`PODCAST_SCHEMA_VERSION`]
    /// after a successful open).
    pub fn schema_version(&self) -> Result<i64> {
        Ok(self
            .lock()
            .query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn register_norm_function(conn: &Connection) -> Result<()> {
    use rusqlite::functions::FunctionFlags;
    conn.create_scalar_function(
        "norm",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: Option<String> = ctx.get(0)?;
            Ok(music_dht::normalize_name(
                value.as_deref().unwrap_or_default(),
            ))
        },
    )?;
    Ok(())
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Converts a contract generation to the SQLite column value.
///
/// Generations above `i64::MAX` cannot be stored; records carrying one are
/// rejected (no conforming client produces them).
pub(crate) fn generation_to_sql(generation: u64) -> Result<i64, PodcastStoreError> {
    i64::try_from(generation)
        .map_err(|_| PodcastStoreError::Invalid("generation exceeds i64::MAX".to_string()))
}

pub(crate) fn status_to_sql(status: ProgressStatus) -> &'static str {
    match status {
        ProgressStatus::NotStarted => "not_started",
        ProgressStatus::InProgress => "in_progress",
        ProgressStatus::Completed => "completed",
    }
}

pub(crate) fn status_from_sql(value: &str) -> ProgressStatus {
    match value {
        "in_progress" => ProgressStatus::InProgress,
        "completed" => ProgressStatus::Completed,
        _ => ProgressStatus::NotStarted,
    }
}

/// Parses a stored id; stored ids are validated on write.
pub(crate) fn podcast_id_col(value: String) -> rusqlite::Result<PodcastId> {
    PodcastId::try_from(value).map_err(|error| conversion_error(error.to_string()))
}

pub(crate) fn episode_id_col(value: String) -> rusqlite::Result<EpisodeId> {
    EpisodeId::try_from(value).map_err(|error| conversion_error(error.to_string()))
}

pub(crate) fn feed_url_col(value: String) -> rusqlite::Result<FeedUrl> {
    FeedUrl::try_from(value).map_err(|error| conversion_error(error.to_string()))
}

pub(crate) fn podcast_meta_col(value: &str) -> rusqlite::Result<PodcastMeta> {
    serde_json::from_str(value).map_err(|error| conversion_error(error.to_string()))
}

pub(crate) fn conversion_error(message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(PodcastStoreError::Invalid(message)),
    )
}

pub(crate) fn progress_from_row(
    row: &rusqlite::Row<'_>,
    at: usize,
) -> rusqlite::Result<ProgressRecord> {
    let generation: i64 = row.get(at + 4)?;
    Ok(ProgressRecord {
        episode_id: episode_id_col(row.get(at)?)?,
        podcast_id: podcast_id_col(row.get(at + 1)?)?,
        position_ms: row.get(at + 2)?,
        status: status_from_sql(&row.get::<_, String>(at + 3)?),
        generation: u64::try_from(generation).unwrap_or(0),
        session_id: row.get(at + 5)?,
        hlc_ms: row.get(at + 6)?,
        device_id: row.get(at + 7)?,
        op_id: row.get(at + 8)?,
    })
}

/// Column list matching [`progress_from_row`], for table alias `p`.
pub(crate) const PROGRESS_COLUMNS: &str = "p.episode_id, p.podcast_id, p.position_ms, p.status, \
     p.generation, p.session_id, p.hlc_ms, p.device_id, p.op_id";

pub(crate) fn session_from_row(
    row: &rusqlite::Row<'_>,
    at: usize,
) -> rusqlite::Result<ListenSession> {
    Ok(ListenSession {
        session_id: row.get(at)?,
        episode_id: episode_id_col(row.get(at + 1)?)?,
        podcast_id: podcast_id_col(row.get(at + 2)?)?,
        device_id: row.get(at + 3)?,
        started_at_ms: row.get(at + 4)?,
        ended_at_ms: row.get(at + 5)?,
        start_position_ms: row.get(at + 6)?,
        end_position_ms: row.get(at + 7)?,
        listened_ms: row.get(at + 8)?,
        completed: row.get(at + 9)?,
    })
}

/// Column list matching [`session_from_row`], for table alias `s`.
pub(crate) const SESSION_COLUMNS: &str = "s.session_id, s.episode_id, s.podcast_id, s.device_id, \
     s.started_at_ms, s.ended_at_ms, s.start_position_ms, s.end_position_ms, s.listened_ms, \
     s.completed";
