//! The single code path that applies podcast records to the store.
//!
//! Remote operations, local actions and snapshot sections all end here.
//! Records come from the `SyncOpWire` materializers of
//! `music_dht::device_sync` and are merged with the functions of
//! `music_dht::podcast`, so applying is idempotent and order-independent.

use anyhow::Result;
use music_dht::device_sync::{SyncOpPayload, SyncOpWire, SyncSnapshot};
use music_dht::podcast::{
    EpisodeIdentityHints, EpisodeMeta, FeedUrl, KnownEpisode, ListenSession, PodcastEpisodeRef,
    PodcastError, PodcastId, ProgressRecord, ShareRecord, SubscriptionRecord, merge_feed_aliases,
    merge_listen_session,
};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};

use super::{
    PROGRESS_COLUMNS, PodcastStore, PodcastStoreError, SESSION_COLUMNS, episode_id_col,
    feed_url_col, generation_to_sql, now_ms, podcast_meta_col, progress_from_row, session_from_row,
    status_to_sql,
};

/// Result of applying one operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ApplyOutcome {
    /// Stored state changed.
    Changed,
    /// The operation was already reflected (duplicate, or older than the
    /// stored register).
    Unchanged,
    /// Not a podcast operation; nothing to do here.
    Ignored,
    /// The operation violates the contract and was skipped. Acknowledge it
    /// anyway so the log keeps moving (`PODCASTS.md`).
    Rejected(String),
}

/// Counters returned by [`PodcastStore::apply_snapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotApplyStats {
    /// Records that changed stored state.
    pub changed: usize,
    /// Valid records already reflected locally.
    pub unchanged: usize,
    /// Records that failed validation and were skipped.
    pub rejected: usize,
    /// Whether the podcast sections were ignored for exceeding the counts.
    pub ignored_over_limits: bool,
}

/// Where an episode row came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpisodeSource {
    /// Parsed from the podcast's feed on this device.
    Feed,
    /// Learned from a `PodcastEpisodeRef` (sync operation or queue item).
    Sync,
}

impl EpisodeSource {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Feed => "feed",
            Self::Sync => "sync",
        }
    }
}

fn invalid(error: impl ToString) -> anyhow::Error {
    PodcastStoreError::Invalid(error.to_string()).into()
}

pub(crate) fn load_subscription_register(
    conn: &Connection,
    podcast_id: &PodcastId,
) -> Result<Option<SubscriptionRecord>> {
    let row = conn
        .query_row(
            "SELECT subscribed, podcast_json, generation, hlc_ms, op_id
               FROM podcast_subscriptions WHERE podcast_id = ?1",
            [podcast_id.as_str()],
            |row| {
                let generation: i64 = row.get(2)?;
                Ok(SubscriptionRecord {
                    podcast_id: podcast_id.clone(),
                    subscribed: row.get(0)?,
                    podcast: podcast_meta_col(&row.get::<_, String>(1)?)?,
                    feed_aliases: Vec::new(),
                    generation: u64::try_from(generation).unwrap_or(0),
                    hlc_ms: row.get(3)?,
                    op_id: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(mut record) = row else {
        return Ok(None);
    };
    record.feed_aliases = load_aliases(conn, podcast_id)?;
    Ok(Some(record))
}

pub(crate) fn load_aliases(conn: &Connection, podcast_id: &PodcastId) -> Result<Vec<FeedUrl>> {
    let mut statement = conn.prepare_cached(
        "SELECT feed_url FROM podcast_feed_aliases WHERE podcast_id = ?1 ORDER BY feed_url",
    )?;
    let urls = statement
        .query_map([podcast_id.as_str()], |row| feed_url_col(row.get(0)?))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(urls)
}

pub(crate) fn load_share(conn: &Connection, podcast_id: &PodcastId) -> Result<Option<ShareRecord>> {
    Ok(conn
        .query_row(
            "SELECT shared, generation, hlc_ms, op_id FROM podcast_shares WHERE podcast_id = ?1",
            [podcast_id.as_str()],
            |row| {
                let generation: i64 = row.get(1)?;
                Ok(ShareRecord {
                    podcast_id: podcast_id.clone(),
                    shared: row.get(0)?,
                    generation: u64::try_from(generation).unwrap_or(0),
                    hlc_ms: row.get(2)?,
                    op_id: row.get(3)?,
                })
            },
        )
        .optional()?)
}

pub(crate) fn load_progress(
    conn: &Connection,
    episode_id: &music_dht::podcast::EpisodeId,
) -> Result<Option<ProgressRecord>> {
    Ok(conn
        .query_row(
            &format!("SELECT {PROGRESS_COLUMNS} FROM episode_progress p WHERE p.episode_id = ?1"),
            [episode_id.as_str()],
            |row| progress_from_row(row, 0),
        )
        .optional()?)
}

pub(crate) fn load_session(conn: &Connection, session_id: &str) -> Result<Option<ListenSession>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {SESSION_COLUMNS} FROM podcast_listen_sessions s WHERE s.session_id = ?1"
            ),
            [session_id],
            |row| session_from_row(row, 0),
        )
        .optional()?)
}

/// Replaces the alias set of a podcast with `merged` when it differs.
fn store_aliases(conn: &Connection, podcast_id: &PodcastId, merged: &[FeedUrl]) -> Result<bool> {
    let current = load_aliases(conn, podcast_id)?;
    if current == merged {
        return Ok(false);
    }
    conn.execute(
        "DELETE FROM podcast_feed_aliases WHERE podcast_id = ?1",
        [podcast_id.as_str()],
    )?;
    let mut insert = conn.prepare_cached(
        "INSERT INTO podcast_feed_aliases (podcast_id, feed_url) VALUES (?1, ?2)",
    )?;
    for url in merged {
        insert.execute(params![podcast_id.as_str(), url.as_str()])?;
    }
    Ok(true)
}

/// Merges feed URLs into the bounded grow-only alias set of a podcast.
///
/// Returns whether the stored set changed.
pub(crate) fn merge_aliases(
    conn: &Connection,
    podcast_id: &PodcastId,
    urls: &[FeedUrl],
) -> Result<bool> {
    let current = load_aliases(conn, podcast_id)?;
    let merged = merge_feed_aliases(podcast_id, &current, urls);
    store_aliases(conn, podcast_id, &merged)
}

/// Inserts the catalog row of a podcast if it does not exist yet.
fn ensure_podcast_row(
    conn: &Connection,
    podcast_id: &PodcastId,
    feed_url: &FeedUrl,
    title: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO podcasts
             (podcast_id, feed_url, title, meta_from_feed, created_at_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, 0, ?4, ?4)",
        params![podcast_id.as_str(), feed_url.as_str(), title, now],
    )?;
    Ok(())
}

/// Applies a subscription register record (operation or snapshot).
///
/// The register is last-writer-wins ([`SubscriptionRecord::supersedes`]);
/// the alias set is a bounded grow-only set merged independently, so the
/// stored aliases are the bounded union of every URL seen (equal to what
/// `merge_subscription` computes). The catalog row is created from the
/// register metadata and follows the winning register until this device has
/// refreshed the feed itself.
pub(crate) fn apply_subscription_record(
    conn: &Connection,
    record: &SubscriptionRecord,
    now: i64,
) -> Result<bool> {
    generation_to_sql(record.generation)?;
    let podcast_id = &record.podcast_id;
    let current = load_subscription_register(conn, podcast_id)?;
    let wins = current
        .as_ref()
        .is_none_or(|current| record.supersedes(current));
    let mut changed = false;
    if wins {
        conn.execute(
            "INSERT INTO podcast_subscriptions
                 (podcast_id, subscribed, podcast_json, generation, hlc_ms, op_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(podcast_id) DO UPDATE SET
                 subscribed = excluded.subscribed, podcast_json = excluded.podcast_json,
                 generation = excluded.generation, hlc_ms = excluded.hlc_ms,
                 op_id = excluded.op_id",
            params![
                podcast_id.as_str(),
                record.subscribed,
                serde_json::to_string(&record.podcast)?,
                generation_to_sql(record.generation)?,
                record.hlc_ms,
                record.op_id,
            ],
        )?;
        changed = true;
    }
    let mut urls = record.feed_aliases.clone();
    urls.push(record.podcast.feed_url.clone());
    changed |= merge_aliases(conn, podcast_id, &urls)?;

    ensure_podcast_row(
        conn,
        podcast_id,
        &record.podcast.feed_url,
        &record.podcast.title,
        now,
    )?;
    if wins {
        let meta = &record.podcast;
        conn.execute(
            "UPDATE podcasts
                SET feed_url = ?2, title = ?3, author = ?4,
                    description = COALESCE(?5, description), link = ?6, image_url = ?7,
                    updated_at_ms = ?8
              WHERE podcast_id = ?1 AND meta_from_feed = 0",
            params![
                podcast_id.as_str(),
                meta.feed_url.as_str(),
                meta.title,
                meta.author,
                meta.description,
                meta.link,
                meta.image_url,
                now
            ],
        )?;
    }
    Ok(changed)
}

/// Applies a share register record.
pub(crate) fn apply_share_record(conn: &Connection, record: &ShareRecord) -> Result<bool> {
    let generation = generation_to_sql(record.generation)?;
    let current = load_share(conn, &record.podcast_id)?;
    if current
        .as_ref()
        .is_some_and(|current| !record.supersedes(current))
    {
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO podcast_shares (podcast_id, shared, generation, hlc_ms, op_id)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(podcast_id) DO UPDATE SET
             shared = excluded.shared, generation = excluded.generation,
             hlc_ms = excluded.hlc_ms, op_id = excluded.op_id",
        params![
            record.podcast_id.as_str(),
            record.shared,
            generation,
            record.hlc_ms,
            record.op_id
        ],
    )?;
    Ok(true)
}

/// Applies a progress register record. Never touches the catalog.
pub(crate) fn apply_progress_record(conn: &Connection, record: &ProgressRecord) -> Result<bool> {
    let generation = generation_to_sql(record.generation)?;
    let current = load_progress(conn, &record.episode_id)?;
    if current
        .as_ref()
        .is_some_and(|current| !record.supersedes(current))
    {
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO episode_progress
             (episode_id, podcast_id, position_ms, status, generation, session_id, hlc_ms,
              device_id, op_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(episode_id) DO UPDATE SET
             podcast_id = excluded.podcast_id, position_ms = excluded.position_ms,
             status = excluded.status, generation = excluded.generation,
             session_id = excluded.session_id, hlc_ms = excluded.hlc_ms,
             device_id = excluded.device_id, op_id = excluded.op_id",
        params![
            record.episode_id.as_str(),
            record.podcast_id.as_str(),
            record.position_ms,
            status_to_sql(record.status),
            generation,
            record.session_id,
            record.hlc_ms,
            record.device_id,
            record.op_id,
        ],
    )?;
    Ok(true)
}

/// Adds or grows a listening session (grow-only set keyed by `session_id`).
pub(crate) fn apply_listen_session(conn: &Connection, session: &ListenSession) -> Result<bool> {
    let current = load_session(conn, &session.session_id)?;
    let merged = match &current {
        Some(current) => merge_listen_session(current, session).map_err(invalid)?,
        None => session.clone(),
    };
    if current.as_ref() == Some(&merged) {
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO podcast_listen_sessions
             (session_id, episode_id, podcast_id, device_id, started_at_ms, ended_at_ms,
              start_position_ms, end_position_ms, listened_ms, completed)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(session_id) DO UPDATE SET
             started_at_ms = excluded.started_at_ms, ended_at_ms = excluded.ended_at_ms,
             start_position_ms = excluded.start_position_ms,
             end_position_ms = excluded.end_position_ms,
             listened_ms = excluded.listened_ms, completed = excluded.completed",
        params![
            merged.session_id,
            merged.episode_id.as_str(),
            merged.podcast_id.as_str(),
            merged.device_id,
            merged.started_at_ms,
            merged.ended_at_ms,
            merged.start_position_ms,
            merged.end_position_ms,
            merged.listened_ms,
            merged.completed,
        ],
    )?;
    Ok(true)
}

/// Inserts an episode row.
pub(crate) fn insert_episode(
    conn: &Connection,
    meta: &EpisodeMeta,
    source: EpisodeSource,
    in_feed: bool,
    legacy_guid: Option<&str>,
    first_seen_at_ms: i64,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO episodes
             (episode_id, podcast_id, guid, title, description, season, episode_number,
              episode_type, published_at_ms, duration_ms, audio_url, mime, byte_len,
              image_url, link, source, in_feed, legacy_guid, first_seen_at_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                 ?18, ?19, ?20)",
        params![
            meta.episode_id.as_str(),
            meta.podcast_id.as_str(),
            meta.guid,
            meta.title,
            meta.description,
            meta.season,
            meta.episode_number,
            episode_type_to_sql(meta.episode_type),
            meta.published_at_ms,
            meta.duration_ms,
            meta.audio_url,
            meta.mime,
            meta.byte_len.and_then(|len| i64::try_from(len).ok()),
            meta.image_url,
            meta.link,
            source.as_sql(),
            in_feed,
            legacy_guid,
            first_seen_at_ms,
            now,
        ],
    )?;
    Ok(())
}

pub(crate) fn episode_type_to_sql(kind: music_dht::podcast::EpisodeType) -> &'static str {
    match kind {
        music_dht::podcast::EpisodeType::Full => "full",
        music_dht::podcast::EpisodeType::Trailer => "trailer",
        music_dht::podcast::EpisodeType::Bonus => "bonus",
    }
}

/// Records an episode learned through sync when the catalog does not know it.
///
/// Feed metadata always wins: an existing row is left untouched. The
/// podcast catalog row is created from the reference when missing (no
/// register is written).
pub(crate) fn ensure_episode_from_ref(
    conn: &Connection,
    episode: &PodcastEpisodeRef,
) -> Result<()> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM episodes WHERE episode_id = ?1",
            [episode.episode_id.as_str()],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        return Ok(());
    }
    let now = now_ms();
    ensure_podcast_row(
        conn,
        &episode.podcast_id,
        &episode.feed_url,
        &episode.podcast_title,
        now,
    )?;
    let meta = EpisodeMeta {
        episode_id: episode.episode_id.clone(),
        podcast_id: episode.podcast_id.clone(),
        guid: None,
        title: episode.episode_title.clone(),
        description: None,
        season: episode.season,
        episode_number: episode.episode_number,
        episode_type: Default::default(),
        published_at_ms: episode.published_at_ms,
        duration_ms: episode.duration_ms,
        audio_url: episode.audio_url.clone(),
        mime: episode.mime.clone(),
        byte_len: None,
        image_url: episode.image_url.clone(),
        link: None,
    };
    insert_episode(conn, &meta, EpisodeSource::Sync, false, None, now, now)?;
    refresh_seasons(conn, &episode.podcast_id)?;
    Ok(())
}

/// Recomputes the derived season rows of a podcast from its episodes.
pub(crate) fn refresh_seasons(conn: &Connection, podcast_id: &PodcastId) -> Result<()> {
    conn.execute(
        "DELETE FROM podcast_seasons WHERE podcast_id = ?1",
        [podcast_id.as_str()],
    )?;
    conn.execute(
        "INSERT INTO podcast_seasons
             (podcast_id, season, episode_count, first_published_at_ms, last_published_at_ms)
         SELECT podcast_id, season, COUNT(*), MIN(published_at_ms), MAX(published_at_ms)
           FROM episodes WHERE podcast_id = ?1 GROUP BY season",
        [podcast_id.as_str()],
    )?;
    Ok(())
}

/// Known episodes of a podcast with the identity hints they were last seen with.
pub(crate) fn known_episodes(
    conn: &Connection,
    podcast_id: &PodcastId,
) -> Result<Vec<KnownEpisode>> {
    let mut statement = conn.prepare_cached(
        "SELECT episode_id, guid, audio_url, link, published_at_ms, title
           FROM episodes WHERE podcast_id = ?1 ORDER BY episode_id",
    )?;
    let known = statement
        .query_map([podcast_id.as_str()], |row| {
            Ok(KnownEpisode {
                episode_id: episode_id_col(row.get(0)?)?,
                hints: EpisodeIdentityHints {
                    guid: row.get(1)?,
                    enclosure_url: Some(row.get(2)?),
                    link: row.get(3)?,
                    published_at_ms: row.get(4)?,
                    title: Some(row.get(5)?),
                },
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(known)
}

/// Applies one podcast operation inside the caller's transaction.
pub(crate) fn apply_op(conn: &Connection, op: &SyncOpWire) -> Result<ApplyOutcome> {
    if !op.payload.is_podcast() {
        return Ok(ApplyOutcome::Ignored);
    }
    if let Err(error) = op.validate_podcast() {
        return Ok(ApplyOutcome::Rejected(error.to_string()));
    }
    let result = match &op.payload {
        SyncOpPayload::PodcastSubscriptionSet { .. } => {
            let record = op.podcast_subscription().expect("subscription payload");
            apply_subscription_record(conn, &record, now_ms())
        }
        SyncOpPayload::PodcastShareSet { .. } => {
            apply_share_record(conn, &op.podcast_share().expect("share payload"))
        }
        SyncOpPayload::PodcastFeedAliasesAdded { .. } => {
            let (podcast_id, urls) = op.podcast_feed_aliases().expect("alias payload");
            merge_aliases(conn, podcast_id, urls)
        }
        SyncOpPayload::PodcastProgressSet { episode, .. } => {
            let record = op.podcast_progress().expect("progress payload");
            if let Err(error) = generation_to_sql(record.generation) {
                return Ok(ApplyOutcome::Rejected(error.to_string()));
            }
            if let Some(episode) = episode {
                ensure_episode_from_ref(conn, episode)?;
            }
            apply_progress_record(conn, &record)
        }
        SyncOpPayload::PodcastListenRecorded { session, episode } => {
            if let Some(current) = load_session(conn, &session.session_id)?
                && let Err(error) = merge_listen_session(&current, session)
            {
                return Ok(ApplyOutcome::Rejected(error.to_string()));
            }
            if let Some(episode) = episode {
                ensure_episode_from_ref(conn, episode)?;
            }
            apply_listen_session(conn, session)
        }
        _ => return Ok(ApplyOutcome::Ignored),
    };
    match result {
        Ok(true) => Ok(ApplyOutcome::Changed),
        Ok(false) => Ok(ApplyOutcome::Unchanged),
        Err(error) => match error.downcast_ref::<PodcastStoreError>() {
            Some(PodcastStoreError::Invalid(reason)) => Ok(ApplyOutcome::Rejected(reason.clone())),
            _ => Err(error),
        },
    }
}

fn record_outcome(stats: &mut SnapshotApplyStats, result: Result<bool>) -> Result<()> {
    match result {
        Ok(true) => stats.changed += 1,
        Ok(false) => stats.unchanged += 1,
        Err(error) => match error.downcast_ref::<PodcastStoreError>() {
            Some(PodcastStoreError::Invalid(_)) => stats.rejected += 1,
            _ => return Err(error),
        },
    }
    Ok(())
}

fn check(result: Result<(), PodcastError>) -> Result<()> {
    result.map_err(invalid)
}

impl PodcastStore {
    /// Applies a device-sync operation received from a trusted device.
    ///
    /// Non-podcast operations return [`ApplyOutcome::Ignored`]. Podcast
    /// operations are validated (`SyncOpWire::validate_podcast`) and merged
    /// with the contract functions in one transaction; applying the same
    /// operations in any order, with duplicates, gives the same state.
    pub fn apply_remote_op(&self, op: &SyncOpWire) -> Result<ApplyOutcome> {
        if !op.payload.is_podcast() {
            return Ok(ApplyOutcome::Ignored);
        }
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = apply_op(&tx, op)?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Merges the podcast sections of a sync snapshot.
    ///
    /// Sections exceeding the contract counts are ignored
    /// (`SyncSnapshot::podcast_state_within_limits`); every record is
    /// validated and merged with the same functions as operations, so a
    /// snapshot never replaces newer local state. One transaction.
    pub fn apply_snapshot(&self, snapshot: &SyncSnapshot) -> Result<SnapshotApplyStats> {
        let mut stats = SnapshotApplyStats::default();
        if !snapshot.podcast_state_within_limits() {
            stats.ignored_over_limits = true;
            return Ok(stats);
        }
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        for record in &snapshot.podcast_subscriptions {
            let mut record = record.clone();
            record.normalize();
            let result = check(record.validate())
                .and_then(|()| apply_subscription_record(&tx, &record, now));
            record_outcome(&mut stats, result)?;
        }
        for record in &snapshot.podcast_shares {
            let result = check(record.validate()).and_then(|()| apply_share_record(&tx, record));
            record_outcome(&mut stats, result)?;
        }
        for record in &snapshot.podcast_progress {
            let result = check(record.validate()).and_then(|()| apply_progress_record(&tx, record));
            record_outcome(&mut stats, result)?;
        }
        for session in &snapshot.podcast_listen_sessions {
            let result =
                check(session.validate()).and_then(|()| apply_listen_session(&tx, session));
            record_outcome(&mut stats, result)?;
        }
        tx.commit()?;
        Ok(stats)
    }

    /// Fills the podcast sections of `snapshot` with this store's registers
    /// and sessions, bounded by `SyncSnapshot::bound_podcast_state_to`.
    ///
    /// `budget_bytes` is usually `MAX_PODCAST_SNAPSHOT_BYTES` or
    /// `device_sync::podcast_snapshot_budget(other_bytes)`. Records are read
    /// newest first and capped by the per-section counts before the byte
    /// budget is applied. Local fetch state and the catalog are never part
    /// of the snapshot. Send it only to v4 peers (`SyncSnapshot::for_peer`).
    pub fn fill_snapshot(&self, snapshot: &mut SyncSnapshot, budget_bytes: usize) -> Result<()> {
        use music_dht::podcast::{
            MAX_SNAPSHOT_LISTEN_SESSIONS, MAX_SNAPSHOT_PROGRESS, MAX_SNAPSHOT_SHARES,
            MAX_SNAPSHOT_SUBSCRIPTIONS,
        };
        let conn = self.lock();
        let ids: Vec<PodcastId> = {
            let mut statement = conn.prepare(
                "SELECT podcast_id FROM podcast_subscriptions
                  ORDER BY generation DESC, hlc_ms DESC, op_id DESC, podcast_id DESC
                  LIMIT ?1",
            )?;
            statement
                .query_map([MAX_SNAPSHOT_SUBSCRIPTIONS as i64], |row| {
                    super::podcast_id_col(row.get(0)?)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut subscriptions = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(record) = load_subscription_register(&conn, id)? {
                subscriptions.push(record);
            }
        }
        let shares = {
            let mut statement = conn.prepare(
                "SELECT podcast_id, shared, generation, hlc_ms, op_id FROM podcast_shares
                  ORDER BY generation DESC, hlc_ms DESC, op_id DESC, podcast_id DESC
                  LIMIT ?1",
            )?;
            statement
                .query_map([MAX_SNAPSHOT_SHARES as i64], |row| {
                    let generation: i64 = row.get(2)?;
                    Ok(ShareRecord {
                        podcast_id: super::podcast_id_col(row.get(0)?)?,
                        shared: row.get(1)?,
                        generation: u64::try_from(generation).unwrap_or(0),
                        hlc_ms: row.get(3)?,
                        op_id: row.get(4)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let progress = {
            let mut statement = conn.prepare(&format!(
                "SELECT {PROGRESS_COLUMNS} FROM episode_progress p
                  ORDER BY p.generation DESC, p.hlc_ms DESC, p.op_id DESC, p.episode_id DESC
                  LIMIT ?1"
            ))?;
            statement
                .query_map([MAX_SNAPSHOT_PROGRESS as i64], |row| {
                    progress_from_row(row, 0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let sessions = {
            let mut statement = conn.prepare(&format!(
                "SELECT {SESSION_COLUMNS} FROM podcast_listen_sessions s
                  ORDER BY s.ended_at_ms DESC, s.session_id DESC
                  LIMIT ?1"
            ))?;
            statement
                .query_map([MAX_SNAPSHOT_LISTEN_SESSIONS as i64], |row| {
                    session_from_row(row, 0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        drop(conn);
        snapshot.podcast_subscriptions = subscriptions;
        snapshot.podcast_shares = shares;
        snapshot.podcast_progress = progress;
        snapshot.podcast_listen_sessions = sessions;
        snapshot.bound_podcast_state_to(budget_bytes);
        Ok(())
    }

    /// A snapshot holding only this store's podcast sections, bounded with
    /// the default budget (`MAX_PODCAST_SNAPSHOT_BYTES`).
    pub fn export_snapshot(&self) -> Result<SyncSnapshot> {
        let mut snapshot = SyncSnapshot::default();
        self.fill_snapshot(
            &mut snapshot,
            music_dht::podcast::MAX_PODCAST_SNAPSHOT_BYTES,
        )?;
        Ok(snapshot)
    }
}
