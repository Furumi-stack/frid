//! Feed refresh ingestion and the local-only fetch state.

use std::collections::BTreeSet;

use anyhow::Result;
use music_dht::device_sync::SyncOpPayload;
use music_dht::podcast::feed::ParsedFeed;
use music_dht::podcast::{
    EpisodeId, EpisodeIdentityHints, EpisodeMeta, FeedUrl, KnownEpisode, PodcastId,
    merge_feed_aliases, resolve_episode_ids,
};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};

use super::apply::{self, EpisodeSource};
use super::{PodcastLocalOp, PodcastStore, PodcastStoreError, StampSource, now_ms};

/// A fetched and parsed feed document for [`PodcastStore::apply_feed`].
#[derive(Debug, Clone, Copy)]
pub struct FeedRefresh<'a> {
    /// Podcast the feed belongs to.
    pub podcast_id: &'a PodcastId,
    /// The parsed document (`music_dht::podcast::feed::parse_feed_with_base`
    /// with `fetched_from` as base).
    pub feed: &'a ParsedFeed,
    /// URL the document was finally served from, after redirects.
    pub fetched_from: &'a FeedUrl,
    /// Whether every redirect leading to `fetched_from` was permanent
    /// (301/308). Temporary redirects do not move the feed.
    pub moved_permanently: bool,
}

/// Result of [`PodcastStore::apply_feed`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RefreshOutcome {
    /// Episodes seen for the first time.
    pub new_episodes: usize,
    /// Known episodes whose metadata changed (title, audio URL, ...).
    pub updated_episodes: usize,
    /// Known episodes without metadata changes.
    pub unchanged_episodes: usize,
    /// Items without identity or with invalid metadata.
    pub skipped_items: usize,
    /// Feed URLs learned from a permanent redirect or `itunes:new-feed-url`.
    pub learned_aliases: Vec<FeedUrl>,
    /// URL the podcast is fetched from after this refresh.
    pub feed_url: Option<FeedUrl>,
    /// `podcast_feed_aliases_added`, only when the learned URLs changed the
    /// stored alias set. Append it to the sync log.
    pub ops: Vec<PodcastLocalOp>,
}

/// Local-only fetch state of a podcast. Never part of sync output.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchState {
    /// HTTP `ETag` validator of the last successful fetch.
    pub etag: Option<String>,
    /// HTTP `Last-Modified` validator of the last successful fetch.
    pub last_modified: Option<String>,
    /// Last fetch attempt, unix milliseconds.
    pub checked_at_ms: Option<i64>,
    /// Last successful fetch (including "not modified").
    pub last_success_at_ms: Option<i64>,
    /// When the scheduler should check again (`None`: due now).
    pub next_check_at_ms: Option<i64>,
    /// Error of the last attempt; `None` after a success.
    pub last_error: Option<String>,
    /// Failed attempts since the last success.
    pub consecutive_failures: u32,
    /// Image URL the cached cover was downloaded from.
    pub cover_url: Option<String>,
    /// Local path of the cached cover.
    pub cover_path: Option<String>,
}

fn podcast_fetch_url(conn: &Connection, podcast_id: &PodcastId) -> Result<Option<FeedUrl>> {
    Ok(conn
        .query_row(
            "SELECT feed_url FROM podcasts WHERE podcast_id = ?1",
            [podcast_id.as_str()],
            |row| super::feed_url_col(row.get(0)?),
        )
        .optional()?)
}

fn fetch_state_in(conn: &Connection, podcast_id: &PodcastId) -> Result<FetchState> {
    Ok(conn
        .query_row(
            "SELECT etag, last_modified, checked_at_ms, last_success_at_ms,
                    next_check_at_ms, last_error, consecutive_failures, cover_url,
                    cover_path
               FROM podcast_fetch_state WHERE podcast_id = ?1",
            [podcast_id.as_str()],
            |row| {
                Ok(FetchState {
                    etag: row.get(0)?,
                    last_modified: row.get(1)?,
                    checked_at_ms: row.get(2)?,
                    last_success_at_ms: row.get(3)?,
                    next_check_at_ms: row.get(4)?,
                    last_error: row.get(5)?,
                    consecutive_failures: u32::try_from(row.get::<_, i64>(6)?).unwrap_or(u32::MAX),
                    cover_url: row.get(7)?,
                    cover_path: row.get(8)?,
                })
            },
        )
        .optional()?
        .unwrap_or_default())
}

fn has_personal_state(conn: &Connection, episode_id: &EpisodeId) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM episode_progress WHERE episode_id = ?1)
                 OR EXISTS (SELECT 1 FROM podcast_listen_sessions WHERE episode_id = ?1)",
        [episode_id.as_str()],
        |row| row.get(0),
    )?)
}

/// Re-keys episodes imported from the prototype whose stored `guid` was the
/// enclosure URL (`legacy_guid`) when the feed shows it is a real GUID.
///
/// Only episodes without progress or sessions move, so no personal state is
/// touched; otherwise the GUID item becomes a separate episode.
fn rekey_legacy_guids(conn: &Connection, podcast_id: &PodcastId, feed: &ParsedFeed) -> Result<()> {
    for item in &feed.episodes {
        let Some(guid) = item.guid.as_deref() else {
            continue;
        };
        let Some(guid_id) = EpisodeId::from_guid(podcast_id, guid) else {
            continue;
        };
        let taken = conn
            .query_row(
                "SELECT 1 FROM episodes WHERE episode_id = ?1",
                [guid_id.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if taken {
            continue;
        }
        let legacy: Option<String> = conn
            .query_row(
                "SELECT episode_id FROM episodes
                  WHERE podcast_id = ?1 AND legacy_guid = ?2 ORDER BY episode_id LIMIT 1",
                params![podcast_id.as_str(), guid],
                |row| row.get(0),
            )
            .optional()?;
        let Some(legacy) = legacy else {
            continue;
        };
        let legacy = super::episode_id_col(legacy)?;
        if has_personal_state(conn, &legacy)? {
            continue;
        }
        conn.execute(
            "UPDATE episodes SET episode_id = ?2, guid = ?3, legacy_guid = NULL
              WHERE episode_id = ?1",
            params![legacy.as_str(), guid_id.as_str(), guid],
        )?;
    }
    Ok(())
}

/// Episodes imported from the prototype without a GUID (`legacy_guid` set)
/// that carry no progress and no session.
///
/// Their ids were resolved over every stored prototype row at once, stale
/// rows included, so they may differ from what a fresh device derives from
/// the current feed. They are provisional until a refresh confirms them.
fn provisional_episode_ids(
    conn: &Connection,
    podcast_id: &PodcastId,
) -> Result<BTreeSet<EpisodeId>> {
    let mut statement = conn.prepare(
        "SELECT e.episode_id FROM episodes e
          WHERE e.podcast_id = ?1 AND e.legacy_guid IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM episode_progress p WHERE p.episode_id = e.episode_id)
            AND NOT EXISTS (SELECT 1 FROM podcast_listen_sessions s
                             WHERE s.episode_id = e.episode_id)",
    )?;
    let ids = statement
        .query_map([podcast_id.as_str()], |row| {
            super::episode_id_col(row.get(0)?)
        })?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    Ok(ids)
}

/// Resolves the ids of the feed items as a device without prototype history
/// would, and moves provisional imported episodes onto them.
///
/// * The ids come from the contract identity rule against every stored
///   episode except the provisional ones (episodes from earlier refreshes,
///   from sync, and imported ones that already carry personal state).
/// * Each provisional episode is matched to the item it was imported from
///   with the same rule (enclosure, then link, then date and title; GUIDs
///   ignored, since the prototype may have stored the enclosure URL as GUID)
///   and re-keyed to that item's id unless another stored episode already
///   holds it. Unmatched provisional episodes stay as they are (not in the
///   feed); the next refresh that lists them re-keys them.
///
/// Provisional episodes have no progress and no session, so no personal
/// state moves. The refresh then clears `legacy_guid` on every episode it
/// lists, which makes their ids final.
fn resolve_and_rekey_provisional(
    conn: &Connection,
    podcast_id: &PodcastId,
    feed: &ParsedFeed,
) -> Result<Vec<Option<EpisodeId>>> {
    let known = apply::known_episodes(conn, podcast_id)?;
    let provisional = provisional_episode_ids(conn, podcast_id)?;
    if provisional.is_empty() {
        return Ok(feed.resolve_episode_ids(podcast_id, &known));
    }
    let (provisional_known, stable_known): (Vec<KnownEpisode>, Vec<KnownEpisode>) = known
        .into_iter()
        .partition(|episode| provisional.contains(&episode.episode_id));
    let ids = feed.resolve_episode_ids(podcast_id, &stable_known);

    let guidless: Vec<EpisodeIdentityHints> = feed
        .identity_hints()
        .into_iter()
        .map(|hints| EpisodeIdentityHints {
            guid: None,
            ..hints
        })
        .collect();
    let matches = resolve_episode_ids(
        podcast_id,
        feed.link.as_deref(),
        &guidless,
        &provisional_known,
    );
    let mut moves: Vec<(EpisodeId, EpisodeId)> = matches
        .into_iter()
        .zip(&ids)
        .filter_map(|(matched, target)| {
            let matched = matched.filter(|id| provisional.contains(id))?;
            let target = target.clone()?;
            (matched != target).then_some((matched, target))
        })
        .collect();
    // A target held by an episode that does not move away blocks the move.
    let occupied: BTreeSet<EpisodeId> = stable_known
        .iter()
        .chain(&provisional_known)
        .map(|episode| episode.episode_id.clone())
        .collect();
    loop {
        let leaving: BTreeSet<&EpisodeId> = moves.iter().map(|(from, _)| from).collect();
        let blocked: BTreeSet<EpisodeId> = moves
            .iter()
            .filter(|(_, to)| occupied.contains(to) && !leaving.contains(to))
            .map(|(from, _)| from.clone())
            .collect();
        if blocked.is_empty() {
            break;
        }
        moves.retain(|(from, _)| !blocked.contains(from));
    }
    // Two phases, so a move may take an id another move vacates.
    for (index, (from, _)) in moves.iter().enumerate() {
        conn.execute(
            "UPDATE episodes SET episode_id = ?2 WHERE episode_id = ?1",
            params![from.as_str(), format!("rekey-pending:{index}")],
        )?;
    }
    for (index, (_, to)) in moves.iter().enumerate() {
        conn.execute(
            "UPDATE episodes SET episode_id = ?2 WHERE episode_id = ?1",
            params![format!("rekey-pending:{index}"), to.as_str()],
        )?;
    }
    Ok(ids)
}

fn upsert_feed_episode(conn: &Connection, meta: &EpisodeMeta, now: i64) -> Result<Option<bool>> {
    let Some(stored) = super::queries::episode_meta_in(conn, &meta.episode_id)? else {
        apply::insert_episode(conn, meta, EpisodeSource::Feed, true, None, now, now)?;
        return Ok(None);
    };
    let changed = stored != *meta;
    conn.execute(
        "UPDATE episodes
            SET podcast_id = ?2, guid = ?3, title = ?4, description = ?5, season = ?6,
                episode_number = ?7, episode_type = ?8, published_at_ms = ?9,
                duration_ms = ?10, audio_url = ?11, mime = ?12, byte_len = ?13,
                image_url = ?14, link = ?15, source = 'feed', in_feed = 1,
                legacy_guid = NULL,
                updated_at_ms = CASE WHEN ?16 THEN ?17 ELSE updated_at_ms END
          WHERE episode_id = ?1",
        params![
            meta.episode_id.as_str(),
            meta.podcast_id.as_str(),
            meta.guid,
            meta.title,
            meta.description,
            meta.season,
            meta.episode_number,
            apply::episode_type_to_sql(meta.episode_type),
            meta.published_at_ms,
            meta.duration_ms,
            meta.audio_url,
            meta.mime,
            meta.byte_len.and_then(|len| i64::try_from(len).ok()),
            meta.image_url,
            meta.link,
            changed,
            now,
        ],
    )?;
    Ok(Some(changed))
}

impl PodcastStore {
    /// Ingests a fetched feed document for a podcast.
    ///
    /// * Channel metadata of the catalog row is replaced by the feed's.
    /// * Episode ids are resolved with the contract identity rule
    ///   (`ParsedFeed::resolve_episode_ids`) against every stored episode of
    ///   the podcast, including episodes learned through sync, so a changed
    ///   title, audio URL or a GUID-less item keeps its id. Known episodes
    ///   get the new metadata (including the audio URL); new ones are added;
    ///   episodes missing from the document are kept (`in_feed = false`).
    /// * A permanent redirect target and `itunes:new-feed-url` become feed
    ///   aliases and the new fetch URL. When they change the stored alias
    ///   set, one `podcast_feed_aliases_added` operation is emitted.
    /// * Subscription, share and progress registers and listening sessions
    ///   are never touched, so a refresh cannot resurrect an unsubscribed
    ///   podcast or move anyone's position.
    ///
    /// Record the HTTP outcome separately with
    /// [`PodcastStore::record_fetch_success`] and friends.
    pub fn apply_feed(
        &self,
        stamps: &mut dyn StampSource,
        refresh: &FeedRefresh<'_>,
    ) -> Result<RefreshOutcome> {
        let podcast_id = refresh.podcast_id;
        let feed = refresh.feed;
        let now = now_ms();
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_url = match podcast_fetch_url(&tx, podcast_id)? {
            Some(url) => url,
            None => {
                let Some(register) = apply::load_subscription_register(&tx, podcast_id)? else {
                    return Err(PodcastStoreError::UnknownPodcast(podcast_id.clone()).into());
                };
                apply::apply_subscription_record(&tx, &register, now)?;
                register.podcast.feed_url
            }
        };

        rekey_legacy_guids(&tx, podcast_id, feed)?;
        let ids = resolve_and_rekey_provisional(&tx, podcast_id, feed)?;
        tx.execute(
            "UPDATE episodes SET in_feed = 0 WHERE podcast_id = ?1",
            [podcast_id.as_str()],
        )?;
        let mut outcome = RefreshOutcome::default();
        for (item, id) in feed.episodes.iter().zip(ids) {
            let Some(id) = id else {
                outcome.skipped_items += 1;
                continue;
            };
            let meta = item.to_meta(podcast_id, id);
            if meta.validate().is_err() {
                outcome.skipped_items += 1;
                continue;
            }
            match upsert_feed_episode(&tx, &meta, now)? {
                None => outcome.new_episodes += 1,
                Some(true) => outcome.updated_episodes += 1,
                Some(false) => outcome.unchanged_episodes += 1,
            }
        }

        let mut fetch_url = current_url.clone();
        let mut learned = BTreeSet::new();
        if refresh.moved_permanently && *refresh.fetched_from != fetch_url {
            learned.insert(fetch_url.clone());
            learned.insert(refresh.fetched_from.clone());
            fetch_url = refresh.fetched_from.clone();
        }
        if let Some(new_url) = feed
            .new_feed_url
            .as_deref()
            .and_then(|url| FeedUrl::parse(url).ok())
            && new_url != fetch_url
        {
            learned.insert(fetch_url.clone());
            learned.insert(new_url.clone());
            fetch_url = new_url;
        }
        let meta = feed.to_podcast_meta(fetch_url.clone());
        tx.execute(
            "UPDATE podcasts
                SET feed_url = ?2, title = ?3, author = ?4, description = ?5, link = ?6,
                    image_url = ?7, meta_from_feed = 1, updated_at_ms = ?8
              WHERE podcast_id = ?1",
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
        if !learned.is_empty() {
            let learned: Vec<FeedUrl> = learned.into_iter().collect();
            let current = apply::load_aliases(&tx, podcast_id)?;
            if merge_feed_aliases(podcast_id, &current, &learned) != current {
                let op = super::actions::emit(&tx, stamps, |_| {
                    SyncOpPayload::PodcastFeedAliasesAdded {
                        podcast_id: podcast_id.clone(),
                        feed_aliases: learned.clone(),
                    }
                })?;
                outcome.ops.push(op);
            }
            outcome.learned_aliases = learned;
        }
        apply::refresh_seasons(&tx, podcast_id)?;
        tx.commit()?;
        outcome.feed_url = Some(fetch_url);
        Ok(outcome)
    }

    /// Local fetch state of a podcast (defaults when never fetched).
    pub fn fetch_state(&self, podcast_id: &PodcastId) -> Result<FetchState> {
        fetch_state_in(&self.lock(), podcast_id)
    }

    /// Read-modify-write of the fetch state under one lock and one immediate
    /// transaction, so concurrent `record_*` / `set_cover_cache` calls (from
    /// this or another process) never lose each other's columns or failure
    /// increments.
    fn update_fetch_state(
        &self,
        podcast_id: &PodcastId,
        update: impl FnOnce(&mut FetchState),
    ) -> Result<FetchState> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if podcast_fetch_url(&tx, podcast_id)?.is_none() {
            return Err(PodcastStoreError::UnknownPodcast(podcast_id.clone()).into());
        }
        let mut state = fetch_state_in(&tx, podcast_id)?;
        update(&mut state);
        tx.execute(
            "INSERT INTO podcast_fetch_state
                 (podcast_id, etag, last_modified, checked_at_ms, last_success_at_ms,
                  next_check_at_ms, last_error, consecutive_failures, cover_url, cover_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(podcast_id) DO UPDATE SET
                 etag = excluded.etag, last_modified = excluded.last_modified,
                 checked_at_ms = excluded.checked_at_ms,
                 last_success_at_ms = excluded.last_success_at_ms,
                 next_check_at_ms = excluded.next_check_at_ms,
                 last_error = excluded.last_error,
                 consecutive_failures = excluded.consecutive_failures,
                 cover_url = excluded.cover_url, cover_path = excluded.cover_path",
            params![
                podcast_id.as_str(),
                state.etag,
                state.last_modified,
                state.checked_at_ms,
                state.last_success_at_ms,
                state.next_check_at_ms,
                state.last_error,
                i64::from(state.consecutive_failures),
                state.cover_url,
                state.cover_path,
            ],
        )?;
        tx.commit()?;
        Ok(state)
    }

    /// Records a successful fetch (HTTP 200) with its new validators.
    pub fn record_fetch_success(
        &self,
        podcast_id: &PodcastId,
        now_ms: i64,
        etag: Option<&str>,
        last_modified: Option<&str>,
        next_check_at_ms: Option<i64>,
    ) -> Result<()> {
        self.update_fetch_state(podcast_id, |state| {
            state.etag = etag.map(str::to_string);
            state.last_modified = last_modified.map(str::to_string);
            state.checked_at_ms = Some(now_ms);
            state.last_success_at_ms = Some(now_ms);
            state.next_check_at_ms = next_check_at_ms;
            state.last_error = None;
            state.consecutive_failures = 0;
        })
        .map(|_| ())
    }

    /// Records a conditional fetch answered with "304 Not Modified".
    pub fn record_fetch_not_modified(
        &self,
        podcast_id: &PodcastId,
        now_ms: i64,
        next_check_at_ms: Option<i64>,
    ) -> Result<()> {
        self.update_fetch_state(podcast_id, |state| {
            state.checked_at_ms = Some(now_ms);
            state.last_success_at_ms = Some(now_ms);
            state.next_check_at_ms = next_check_at_ms;
            state.last_error = None;
            state.consecutive_failures = 0;
        })
        .map(|_| ())
    }

    /// Records a failed fetch; returns the consecutive failure count. The
    /// stored catalog is kept.
    pub fn record_fetch_failure(
        &self,
        podcast_id: &PodcastId,
        now_ms: i64,
        error: &str,
        next_check_at_ms: Option<i64>,
    ) -> Result<u32> {
        self.update_fetch_state(podcast_id, |state| {
            state.checked_at_ms = Some(now_ms);
            state.next_check_at_ms = next_check_at_ms;
            state.last_error = Some(error.to_string());
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        })
        .map(|state| state.consecutive_failures)
    }

    /// Records the locally cached cover image of a podcast.
    pub fn set_cover_cache(
        &self,
        podcast_id: &PodcastId,
        cover_url: Option<&str>,
        cover_path: Option<&str>,
    ) -> Result<()> {
        self.update_fetch_state(podcast_id, |state| {
            state.cover_url = cover_url.map(str::to_string);
            state.cover_path = cover_path.map(str::to_string);
        })
        .map(|_| ())
    }

    /// Subscribed podcasts due for a refresh at `now_ms` (never checked or
    /// `next_check_at_ms <= now_ms`), most overdue first, with their fetch URL.
    pub fn podcasts_due_for_refresh(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<(PodcastId, FeedUrl)>> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT p.podcast_id, p.feed_url
               FROM podcasts p
               JOIN podcast_subscriptions s ON s.podcast_id = p.podcast_id AND s.subscribed = 1
               LEFT JOIN podcast_fetch_state f ON f.podcast_id = p.podcast_id
              WHERE f.next_check_at_ms IS NULL OR f.next_check_at_ms <= ?1
              ORDER BY IFNULL(f.next_check_at_ms, 0), p.podcast_id
              LIMIT ?2",
        )?;
        let due = statement
            .query_map(
                params![now_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| {
                    Ok((
                        super::podcast_id_col(row.get(0)?)?,
                        super::feed_url_col(row.get(1)?)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(due)
    }
}
