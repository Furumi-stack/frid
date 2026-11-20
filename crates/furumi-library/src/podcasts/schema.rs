//! Schema, migrations and the import of the prototype database.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context as _, Result};
use music_dht::podcast::{
    EpisodeId, EpisodeIdentityHints, EpisodeMeta, EpisodeType, FeedUrl, KnownEpisode,
    MAX_DESCRIPTION_CHARS, MAX_TITLE_CHARS, MAX_URL_BYTES, PodcastId, PodcastMeta,
    SubscriptionRecord, bounded_text, is_http_url, normalize_episode_title, resolve_episode_ids,
    resolve_podcast_id,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use super::apply::{self, EpisodeSource};
use super::{LEGACY_IMPORT_OP_PREFIX, PodcastStoreError, now_ms};

/// Newest schema version (`PRAGMA user_version`) this build writes.
pub const PODCAST_SCHEMA_VERSION: i64 = 2;

/// `user_version` of the prototype database of the original podcast PR.
pub const LEGACY_PROTOTYPE_SCHEMA_VERSION: i64 = 1;

/// Migration 1: the prototype schema of the original podcast PR, verbatim
/// (`furumi-tui` `src/podcasts/store.rs`, `MIGRATIONS[0]`). It versioned
/// itself with `PRAGMA user_version = 1` in the same transaction.
const PROTOTYPE_SCHEMA_V1: &str = "
    CREATE TABLE IF NOT EXISTS podcasts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        feed_url        TEXT NOT NULL UNIQUE,
        title           TEXT NOT NULL,
        author          TEXT,
        description     TEXT,
        link            TEXT,
        image_url       TEXT,
        cover_url       TEXT,
        cover_path      TEXT,
        etag            TEXT,
        last_modified   TEXT,
        added_at        INTEGER NOT NULL,
        last_checked_at INTEGER,
        last_error      TEXT
    );
    CREATE TABLE IF NOT EXISTS podcast_episodes (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        podcast_id       INTEGER NOT NULL REFERENCES podcasts(id) ON DELETE CASCADE,
        guid             TEXT NOT NULL,
        title            TEXT NOT NULL,
        description      TEXT,
        published_at     INTEGER,
        duration_seconds REAL,
        audio_url        TEXT NOT NULL,
        mime_type        TEXT,
        byte_len         INTEGER,
        link             TEXT,
        image_url        TEXT,
        first_seen_at    INTEGER NOT NULL,
        UNIQUE (podcast_id, guid)
    );
    CREATE INDEX IF NOT EXISTS podcast_episodes_by_date
        ON podcast_episodes (podcast_id, published_at DESC, id DESC);
";

#[cfg(test)]
pub(super) const PROTOTYPE_SCHEMA_V1_FOR_TESTS: &str = PROTOTYPE_SCHEMA_V1;

/// Columns every prototype `podcasts` table has.
const PROTOTYPE_PODCAST_COLUMNS: &[&str] = &[
    "id",
    "feed_url",
    "title",
    "author",
    "description",
    "link",
    "image_url",
    "cover_url",
    "cover_path",
    "etag",
    "last_modified",
    "added_at",
    "last_checked_at",
    "last_error",
];

/// Columns every prototype `podcast_episodes` table has.
const PROTOTYPE_EPISODE_COLUMNS: &[&str] = &[
    "id",
    "podcast_id",
    "guid",
    "title",
    "description",
    "published_at",
    "duration_seconds",
    "audio_url",
    "mime_type",
    "byte_len",
    "link",
    "image_url",
    "first_seen_at",
];

/// Migration 2: the contract model. See `PODCASTS.md`, "SQLite reference store".
const SCHEMA_V2: &str = "
    CREATE TABLE IF NOT EXISTS podcasts (
        podcast_id      TEXT PRIMARY KEY,
        feed_url        TEXT NOT NULL,
        title           TEXT NOT NULL,
        author          TEXT,
        description     TEXT,
        link            TEXT,
        image_url       TEXT,
        meta_from_feed  INTEGER NOT NULL DEFAULT 0,
        created_at_ms   INTEGER NOT NULL,
        updated_at_ms   INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS podcast_feed_aliases (
        podcast_id  TEXT NOT NULL,
        feed_url    TEXT NOT NULL,
        PRIMARY KEY (podcast_id, feed_url)
    );
    CREATE INDEX IF NOT EXISTS podcast_feed_aliases_by_url
        ON podcast_feed_aliases (feed_url);
    CREATE TABLE IF NOT EXISTS episodes (
        episode_id       TEXT PRIMARY KEY,
        podcast_id       TEXT NOT NULL REFERENCES podcasts(podcast_id),
        guid             TEXT,
        title            TEXT NOT NULL,
        description      TEXT,
        season           INTEGER,
        episode_number   INTEGER,
        episode_type     TEXT NOT NULL DEFAULT 'full',
        published_at_ms  INTEGER,
        duration_ms      INTEGER,
        audio_url        TEXT NOT NULL,
        mime             TEXT,
        byte_len         INTEGER,
        image_url        TEXT,
        link             TEXT,
        source           TEXT NOT NULL CHECK (source IN ('feed', 'sync')),
        in_feed          INTEGER NOT NULL DEFAULT 0,
        legacy_guid      TEXT,
        first_seen_at_ms INTEGER NOT NULL,
        updated_at_ms    INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS episodes_by_podcast_date
        ON episodes (podcast_id, published_at_ms DESC);
    CREATE INDEX IF NOT EXISTS episodes_by_podcast_season
        ON episodes (podcast_id, season, episode_number);
    CREATE TABLE IF NOT EXISTS podcast_seasons (
        podcast_id            TEXT NOT NULL REFERENCES podcasts(podcast_id),
        season                INTEGER,
        episode_count         INTEGER NOT NULL,
        first_published_at_ms INTEGER,
        last_published_at_ms  INTEGER
    );
    CREATE UNIQUE INDEX IF NOT EXISTS podcast_seasons_key
        ON podcast_seasons (podcast_id, IFNULL(season, -1));
    CREATE TABLE IF NOT EXISTS podcast_subscriptions (
        podcast_id    TEXT PRIMARY KEY,
        subscribed    INTEGER NOT NULL,
        podcast_json  TEXT NOT NULL,
        generation    INTEGER NOT NULL CHECK (generation >= 0),
        hlc_ms        INTEGER NOT NULL,
        op_id         TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS podcast_shares (
        podcast_id  TEXT PRIMARY KEY,
        shared      INTEGER NOT NULL,
        generation  INTEGER NOT NULL CHECK (generation >= 0),
        hlc_ms      INTEGER NOT NULL,
        op_id       TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS episode_progress (
        episode_id   TEXT PRIMARY KEY,
        podcast_id   TEXT NOT NULL,
        position_ms  INTEGER NOT NULL,
        status       TEXT NOT NULL
                     CHECK (status IN ('not_started', 'in_progress', 'completed')),
        generation   INTEGER NOT NULL CHECK (generation >= 0),
        session_id   TEXT NOT NULL,
        hlc_ms       INTEGER NOT NULL,
        device_id    TEXT NOT NULL,
        op_id        TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS episode_progress_by_podcast
        ON episode_progress (podcast_id);
    CREATE TABLE IF NOT EXISTS podcast_listen_sessions (
        session_id         TEXT PRIMARY KEY,
        episode_id         TEXT NOT NULL,
        podcast_id         TEXT NOT NULL,
        device_id          TEXT NOT NULL,
        started_at_ms      INTEGER NOT NULL,
        ended_at_ms        INTEGER NOT NULL,
        start_position_ms  INTEGER NOT NULL,
        end_position_ms    INTEGER NOT NULL,
        listened_ms        INTEGER NOT NULL,
        completed          INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS podcast_listen_sessions_by_episode
        ON podcast_listen_sessions (episode_id);
    CREATE INDEX IF NOT EXISTS podcast_listen_sessions_by_end
        ON podcast_listen_sessions (ended_at_ms DESC);
    CREATE TABLE IF NOT EXISTS podcast_fetch_state (
        podcast_id            TEXT PRIMARY KEY REFERENCES podcasts(podcast_id),
        etag                  TEXT,
        last_modified         TEXT,
        checked_at_ms         INTEGER,
        last_success_at_ms    INTEGER,
        next_check_at_ms      INTEGER,
        last_error            TEXT,
        consecutive_failures  INTEGER NOT NULL DEFAULT 0,
        cover_url             TEXT,
        cover_path            TEXT
    );
    CREATE TABLE IF NOT EXISTS legacy_quarantine (
        legacy_table  TEXT NOT NULL,
        legacy_id     INTEGER NOT NULL,
        reason        TEXT NOT NULL,
        detail        TEXT,
        PRIMARY KEY (legacy_table, legacy_id)
    );
";

/// Tables a version-2 store owns (the `legacy_*` tables included).
const V2_TABLES: &[&str] = &[
    "podcasts",
    "podcast_feed_aliases",
    "episodes",
    "podcast_seasons",
    "podcast_subscriptions",
    "podcast_shares",
    "episode_progress",
    "podcast_listen_sessions",
    "podcast_fetch_state",
    "legacy_quarantine",
    "legacy_podcasts",
    "legacy_podcast_episodes",
];

fn user_version(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA user_version", [], |row| row.get(0))?)
}

fn user_tables(conn: &Connection) -> Result<BTreeSet<String>> {
    let mut statement = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    Ok(names)
}

pub(crate) fn table_columns(conn: &Connection, table: &str) -> Result<BTreeSet<String>> {
    let mut statement = conn.prepare("SELECT name FROM pragma_table_info(?1)")?;
    let names = statement
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    Ok(names)
}

fn unrecognized(reason: impl Into<String>) -> anyhow::Error {
    PodcastStoreError::UnrecognizedDatabase(reason.into()).into()
}

fn has_columns(conn: &Connection, table: &str, required: &[&str]) -> Result<bool> {
    let columns = table_columns(conn, table)?;
    Ok(required.iter().all(|column| columns.contains(*column)))
}

/// Checks the prototype tables that exist have the prototype shape.
fn check_prototype_shape(conn: &Connection, tables: &BTreeSet<String>) -> Result<()> {
    if tables.contains("podcasts") && !has_columns(conn, "podcasts", PROTOTYPE_PODCAST_COLUMNS)? {
        return Err(unrecognized(
            "table `podcasts` does not have the prototype columns",
        ));
    }
    if tables.contains("podcast_episodes")
        && !has_columns(conn, "podcast_episodes", PROTOTYPE_EPISODE_COLUMNS)?
    {
        return Err(unrecognized(
            "table `podcast_episodes` does not have the prototype columns",
        ));
    }
    Ok(())
}

/// Read-only check run before anything is written to the file.
///
/// * `user_version` above [`PODCAST_SCHEMA_VERSION`]: written by a newer
///   Furumi, refused.
/// * 0: must be empty, or hold only the prototype tables (the prototype
///   sets its version in the same transaction, so this is defensive).
/// * 1: the prototype database; its tables must have the prototype shape.
/// * 2: the current schema.
///
/// Any other table means the path points at a different database (for
/// example `library.db`), which is refused instead of being modified.
pub(super) fn check_openable(conn: &Connection) -> Result<()> {
    let version = user_version(conn)?;
    if version > PODCAST_SCHEMA_VERSION {
        return Err(PodcastStoreError::NewerSchema {
            found: version,
            supported: PODCAST_SCHEMA_VERSION,
        }
        .into());
    }
    if version < 0 {
        return Err(unrecognized(format!("negative user_version {version}")));
    }
    let tables = user_tables(conn)?;
    let allowed: &[&str] = if version < PODCAST_SCHEMA_VERSION {
        &["podcasts", "podcast_episodes"]
    } else {
        V2_TABLES
    };
    if let Some(table) = tables
        .iter()
        .find(|table| !allowed.contains(&table.as_str()))
    {
        return Err(unrecognized(format!(
            "unexpected table `{table}` at schema version {version}"
        )));
    }
    if version < PODCAST_SCHEMA_VERSION {
        check_prototype_shape(conn, &tables)?;
    } else if tables.contains("podcasts")
        && !has_columns(conn, "podcasts", &["podcast_id", "feed_url"])?
    {
        return Err(unrecognized("table `podcasts` is not the version 2 table"));
    }
    Ok(())
}

/// Applies every pending migration, each in its own immediate transaction
/// that re-reads the version (another process may have migrated meanwhile).
pub(super) fn migrate(conn: &mut Connection) -> Result<()> {
    for target in 1..=PODCAST_SCHEMA_VERSION {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version = user_version(&tx)?;
        if version > PODCAST_SCHEMA_VERSION {
            return Err(PodcastStoreError::NewerSchema {
                found: version,
                supported: PODCAST_SCHEMA_VERSION,
            }
            .into());
        }
        if version >= target {
            continue;
        }
        match target {
            1 => tx
                .execute_batch(PROTOTYPE_SCHEMA_V1)
                .context("creating the prototype podcast schema (v1)")?,
            2 => migrate_to_v2(&tx).context("migrating the podcast database to v2")?,
            _ => unreachable!("no migration for version {target}"),
        }
        tx.pragma_update(None, "user_version", target)?;
        tx.commit()
            .with_context(|| format!("committing podcast schema v{target}"))?;
    }
    Ok(())
}

fn migrate_to_v2(tx: &Transaction<'_>) -> Result<()> {
    let tables = user_tables(tx)?;
    // Rename, never drop: the prototype data stays available as `legacy_*`.
    // Renaming `podcasts` also rewrites the foreign key of the episode table.
    if tables.contains("podcasts") && !tables.contains("legacy_podcasts") {
        tx.execute_batch("ALTER TABLE podcasts RENAME TO legacy_podcasts")?;
    }
    if tables.contains("podcast_episodes") && !tables.contains("legacy_podcast_episodes") {
        tx.execute_batch("ALTER TABLE podcast_episodes RENAME TO legacy_podcast_episodes")?;
    }
    tx.execute_batch(SCHEMA_V2)?;
    let tables = user_tables(tx)?;
    if tables.contains("legacy_podcasts") {
        import_prototype(tx, tables.contains("legacy_podcast_episodes"))?;
    }
    Ok(())
}

struct LegacyPodcast {
    id: i64,
    feed_url: String,
    title: String,
    author: Option<String>,
    description: Option<String>,
    link: Option<String>,
    image_url: Option<String>,
    cover_url: Option<String>,
    cover_path: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    added_at: i64,
    last_checked_at: Option<i64>,
    last_error: Option<String>,
}

struct LegacyEpisode {
    id: i64,
    guid: String,
    title: String,
    description: Option<String>,
    published_at: Option<i64>,
    duration_seconds: Option<f64>,
    audio_url: String,
    mime_type: Option<String>,
    byte_len: Option<i64>,
    link: Option<String>,
    image_url: Option<String>,
    first_seen_at: i64,
}

fn quarantine(
    tx: &Transaction<'_>,
    table: &str,
    id: i64,
    reason: &str,
    detail: &str,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO legacy_quarantine (legacy_table, legacy_id, reason, detail)
         VALUES (?1, ?2, ?3, ?4)",
        params![table, id, reason, detail],
    )?;
    Ok(())
}

fn secs_to_ms(secs: i64) -> i64 {
    secs.saturating_mul(1_000)
}

/// Imports the prototype tables into the contract model.
///
/// * Every prototype row is an active subscription: it becomes a
///   subscription register with `subscribed = true`, generation 0,
///   `hlc_ms` = `added_at` and `op_id` = [`LEGACY_IMPORT_OP_PREFIX`] + id,
///   so any later contract write orders after it. Other devices learn it
///   through the snapshot sections. Nothing is shared (private by default).
/// * The podcast id is derived from the canonical feed URL
///   ([`FeedUrl::parse`], falling back to [`FeedUrl::from_user_input`]); rows
///   whose URL cannot be canonicalized are recorded in `legacy_quarantine`
///   and stay in `legacy_podcasts` with their episodes. Rows with the same
///   canonical URL merge into one podcast (the first row's subscription and
///   fetch state win; the episodes of all of them are imported).
/// * Episodes get ids from the contract identity rule over the stored rows.
///   The prototype stored the enclosure URL as `guid` when the item had
///   none: such rows are imported as GUID-less items and keep the stored
///   value in `legacy_guid`. Rows of one GUID-less item seen under several
///   enclosure URLs are collapsed into the newest (the others are
///   quarantined as duplicates). Because stale rows can still change which
///   key is unique, these ids are provisional: the first refresh that lists
///   the item re-keys an episode without progress or sessions to the id the
///   current feed gives (see `refresh.rs`), including its GUID id when the
///   stored value turns out to be a real GUID.
/// * ETag, Last-Modified, check time, last error and the cover cache move to
///   the local fetch state. The prototype has no progress or history, so no
///   progress register or session is created.
fn import_prototype(tx: &Transaction<'_>, with_episodes: bool) -> Result<()> {
    let now = now_ms();
    let legacy: Vec<LegacyPodcast> = {
        let mut statement = tx.prepare(
            "SELECT id, feed_url, title, author, description, link, image_url, cover_url,
                    cover_path, etag, last_modified, added_at, last_checked_at, last_error
               FROM legacy_podcasts ORDER BY id",
        )?;
        statement
            .query_map([], |row| {
                Ok(LegacyPodcast {
                    id: row.get(0)?,
                    feed_url: row.get(1)?,
                    title: row.get(2)?,
                    author: row.get(3)?,
                    description: row.get(4)?,
                    link: row.get(5)?,
                    image_url: row.get(6)?,
                    cover_url: row.get(7)?,
                    cover_path: row.get(8)?,
                    etag: row.get(9)?,
                    last_modified: row.get(10)?,
                    added_at: row.get(11)?,
                    last_checked_at: row.get(12)?,
                    last_error: row.get(13)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut known_urls: Vec<(PodcastId, FeedUrl)> = Vec::new();
    for podcast in legacy {
        let url = match FeedUrl::parse(&podcast.feed_url)
            .or_else(|_| FeedUrl::from_user_input(&podcast.feed_url))
        {
            Ok(url) => url,
            Err(error) => {
                quarantine(
                    tx,
                    "legacy_podcasts",
                    podcast.id,
                    "invalid feed URL",
                    &error.to_string(),
                )?;
                continue;
            }
        };
        let podcast_id = resolve_podcast_id(&url, known_urls.iter().map(|(id, url)| (id, url)));
        let duplicate = known_urls.iter().any(|(id, _)| *id == podcast_id);
        known_urls.push((podcast_id.clone(), url.clone()));
        if duplicate {
            // Same canonical feed as an earlier row: its episodes join that
            // podcast; the first row's subscription and fetch state are kept.
            if with_episodes {
                import_prototype_episodes(tx, &podcast, &podcast_id, now)?;
            }
            apply::refresh_seasons(tx, &podcast_id)?;
            continue;
        }
        let meta = PodcastMeta {
            feed_url: url.clone(),
            title: podcast.title.clone(),
            author: podcast.author.clone(),
            description: podcast.description.clone(),
            link: podcast.link.clone(),
            image_url: podcast.image_url.clone(),
        }
        .bounded();
        let added_ms = secs_to_ms(podcast.added_at);
        let record = SubscriptionRecord {
            podcast_id: podcast_id.clone(),
            subscribed: true,
            podcast: meta.clone(),
            feed_aliases: vec![url.clone()],
            generation: 0,
            hlc_ms: added_ms,
            op_id: format!("{LEGACY_IMPORT_OP_PREFIX}{podcast_id}"),
        };
        apply::apply_subscription_record(tx, &record, now)?;
        // The prototype metadata came from the feed itself.
        tx.execute(
            "UPDATE podcasts
                SET title = ?2, author = ?3, description = ?4, link = ?5, image_url = ?6,
                    meta_from_feed = 1, created_at_ms = MIN(created_at_ms, ?7)
              WHERE podcast_id = ?1 AND meta_from_feed = 0",
            params![
                podcast_id.as_str(),
                meta.title,
                meta.author,
                meta.description,
                meta.link,
                meta.image_url,
                added_ms
            ],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO podcast_fetch_state
                 (podcast_id, etag, last_modified, checked_at_ms, last_success_at_ms,
                  next_check_at_ms, last_error, consecutive_failures, cover_url, cover_path)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)",
            params![
                podcast_id.as_str(),
                podcast.etag,
                podcast.last_modified,
                podcast.last_checked_at.map(secs_to_ms),
                podcast
                    .last_error
                    .is_none()
                    .then(|| podcast.last_checked_at.map(secs_to_ms))
                    .flatten(),
                podcast.last_error,
                i64::from(podcast.last_error.is_some()),
                podcast.cover_url,
                podcast.cover_path,
            ],
        )?;
        if with_episodes {
            import_prototype_episodes(tx, &podcast, &podcast_id, now)?;
        }
        apply::refresh_seasons(tx, &podcast_id)?;
    }
    Ok(())
}

fn import_prototype_episodes(
    tx: &Transaction<'_>,
    podcast: &LegacyPodcast,
    podcast_id: &PodcastId,
    now: i64,
) -> Result<()> {
    let rows: Vec<LegacyEpisode> = {
        let mut statement = tx.prepare(
            "SELECT id, guid, title, description, published_at, duration_seconds, audio_url,
                    mime_type, byte_len, link, image_url, first_seen_at
               FROM legacy_podcast_episodes WHERE podcast_id = ?1 ORDER BY id",
        )?;
        statement
            .query_map([podcast.id], |row| {
                Ok(LegacyEpisode {
                    id: row.get(0)?,
                    guid: row.get(1)?,
                    title: row.get(2)?,
                    description: row.get(3)?,
                    published_at: row.get(4)?,
                    duration_seconds: row.get(5)?,
                    audio_url: row.get(6)?,
                    mime_type: row.get(7)?,
                    byte_len: row.get(8)?,
                    link: row.get(9)?,
                    image_url: row.get(10)?,
                    first_seen_at: row.get(11)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut usable = Vec::new();
    for row in rows {
        if !is_http_url(&row.audio_url) {
            quarantine(
                tx,
                "legacy_podcast_episodes",
                row.id,
                "invalid audio URL",
                &row.audio_url,
            )?;
            continue;
        }
        usable.push(row);
    }
    // The prototype used the enclosure URL as GUID when the item had none.
    let real_guid = |row: &LegacyEpisode| {
        let guid = row.guid.trim();
        (!guid.is_empty() && guid != row.audio_url.trim()).then(|| guid.to_string())
    };
    let usable = collapse_moved_guidless_rows(tx, usable, real_guid)?;
    let hints: Vec<EpisodeIdentityHints> = usable
        .iter()
        .map(|row| EpisodeIdentityHints {
            guid: real_guid(row),
            enclosure_url: Some(row.audio_url.clone()),
            link: row.link.clone(),
            published_at_ms: row.published_at.map(secs_to_ms),
            title: Some(row.title.clone()),
        })
        .collect();
    let known: Vec<KnownEpisode> = apply::known_episodes(tx, podcast_id)?;
    let ids = resolve_episode_ids(podcast_id, podcast.link.as_deref(), &hints, &known);
    let mut seen: BTreeMap<EpisodeId, i64> = BTreeMap::new();
    for ((row, hint), id) in usable.iter().zip(&hints).zip(ids) {
        let Some(episode_id) = id else {
            quarantine(
                tx,
                "legacy_podcast_episodes",
                row.id,
                "episode cannot be identified",
                "no usable identity key or duplicate item",
            )?;
            continue;
        };
        if let Some(first) = seen.insert(episode_id.clone(), row.id) {
            quarantine(
                tx,
                "legacy_podcast_episodes",
                row.id,
                "duplicate episode",
                &format!("same identity as legacy episode {first}"),
            )?;
            continue;
        }
        let meta = EpisodeMeta {
            episode_id,
            podcast_id: podcast_id.clone(),
            guid: hint.guid.clone(),
            title: bounded_title(&row.title),
            description: row
                .description
                .as_deref()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(|text| bounded_text(text, MAX_DESCRIPTION_CHARS)),
            season: None,
            episode_number: None,
            episode_type: EpisodeType::Full,
            published_at_ms: row.published_at.map(secs_to_ms),
            duration_ms: row
                .duration_seconds
                .filter(|secs| secs.is_finite() && *secs > 0.0)
                .map(|secs| (secs * 1_000.0).round() as i64),
            audio_url: row.audio_url.clone(),
            mime: row.mime_type.clone().filter(|mime| {
                !mime.is_empty() && mime.len() <= 128 && mime.bytes().all(|b| b.is_ascii_graphic())
            }),
            byte_len: row.byte_len.and_then(|len| u64::try_from(len).ok()),
            image_url: row.image_url.clone().filter(|url| is_http_url(url)),
            link: row.link.clone().filter(|link| link.len() <= MAX_URL_BYTES),
        };
        if let Err(error) = meta.validate() {
            quarantine(
                tx,
                "legacy_podcast_episodes",
                row.id,
                "invalid episode metadata",
                &error.to_string(),
            )?;
            continue;
        }
        let legacy_guid = hint.guid.is_none().then(|| row.guid.clone());
        let exists: bool = tx
            .query_row(
                "SELECT 1 FROM episodes WHERE episode_id = ?1",
                [meta.episode_id.as_str()],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if exists {
            continue;
        }
        apply::insert_episode(
            tx,
            &meta,
            EpisodeSource::Feed,
            true,
            legacy_guid.as_deref(),
            secs_to_ms(row.first_seen_at),
            now,
        )?;
    }
    Ok(())
}

/// Collapses prototype rows that are one GUID-less item seen under several
/// enclosure URLs.
///
/// The prototype keyed GUID-less items by their enclosure URL, so an item
/// whose audio URL changed (new CDN, tracking prefix) left one row per URL.
/// Those stale rows are not part of the current feed; resolved together with
/// it they would make the item's `<link>` and `(date, title)` look
/// non-unique and give it an id a fresh device never derives. Rows with the
/// same publication second, normalized title and `<link>` (or none) are the
/// same item: the newest (`first_seen_at`, then `id`) is imported, the others
/// are quarantined as duplicates. The link must match too, so distinct
/// episodes that only share a title and second are never merged.
fn collapse_moved_guidless_rows(
    tx: &Transaction<'_>,
    rows: Vec<LegacyEpisode>,
    real_guid: impl Fn(&LegacyEpisode) -> Option<String>,
) -> Result<Vec<LegacyEpisode>> {
    type ItemKey = (i64, String, Option<String>);
    let key = |row: &LegacyEpisode| -> Option<ItemKey> {
        if real_guid(row).is_some() {
            return None;
        }
        let title = normalize_episode_title(&row.title);
        let link = row
            .link
            .as_deref()
            .map(str::trim)
            .filter(|link| !link.is_empty())
            .map(str::to_string);
        (!title.is_empty()).then_some((row.published_at?, title, link))
    };
    let mut newest: BTreeMap<ItemKey, (i64, i64)> = BTreeMap::new();
    for row in &rows {
        if let Some(key) = key(row) {
            let candidate = (row.first_seen_at, row.id);
            newest
                .entry(key)
                .and_modify(|best| *best = (*best).max(candidate))
                .or_insert(candidate);
        }
    }
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let keeper = key(&row).and_then(|key| newest.get(&key).map(|(_, id)| *id));
        match keeper {
            Some(keeper) if keeper != row.id => quarantine(
                tx,
                "legacy_podcast_episodes",
                row.id,
                "duplicate episode",
                &format!(
                    "older enclosure URL of the GUID-less item imported from legacy episode {keeper}"
                ),
            )?,
            _ => kept.push(row),
        }
    }
    Ok(kept)
}

fn bounded_title(title: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        "Untitled episode".to_string()
    } else {
        bounded_text(title, MAX_TITLE_CHARS)
    }
}
