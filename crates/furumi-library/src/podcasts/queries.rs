//! Read-only queries for clients' podcast UI.

use anyhow::Result;
use music_dht::podcast::{
    EpisodeId, EpisodeMeta, EpisodeType, FeedUrl, ListenSession, PodcastEpisodeRef, PodcastId,
    PodcastMeta, ProgressRecord, ProgressStatus, ShareRecord, SubscriptionRecord, effective_share,
};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension as _, params, params_from_iter};

use super::apply;
use super::{
    PROGRESS_COLUMNS, PodcastStore, SESSION_COLUMNS, episode_id_col, feed_url_col, podcast_id_col,
    progress_from_row, session_from_row,
};
use crate::TrackItem;

/// Which podcasts [`PodcastStore::list_podcasts`] returns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PodcastListFilter {
    /// Podcasts with an active subscription.
    #[default]
    Subscribed,
    /// Every podcast in the catalog, including unsubscribed ones and
    /// podcasts only known from synced progress or queue items.
    All,
}

/// A podcast with its personal state and episode counters.
#[derive(Debug, Clone, PartialEq)]
pub struct PodcastSummary {
    /// Podcast id.
    pub podcast_id: PodcastId,
    /// Local channel metadata; `feed_url` is the URL this device fetches.
    pub meta: PodcastMeta,
    /// Whether the subscription register says subscribed.
    pub subscribed: bool,
    /// Whether the podcast must currently be published (`effective_share`).
    pub shared: bool,
    /// Episodes in the catalog.
    pub episode_count: u64,
    /// Episodes not completed (no progress, not started or in progress).
    pub unplayed_count: u64,
    /// Episodes in progress.
    pub in_progress_count: u64,
    /// Newest publication time.
    pub latest_published_at_ms: Option<i64>,
    /// Numbered seasons (episodes without a season are not counted).
    pub season_count: u64,
}

/// One season group of a podcast. `season == None` groups the episodes
/// without `itunes:season`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeasonSummary {
    /// Season number; `None` for episodes without a season.
    pub season: Option<u32>,
    /// Episodes in the group.
    pub episode_count: u64,
    /// Episodes not completed.
    pub unplayed_count: u64,
    /// Oldest publication time.
    pub first_published_at_ms: Option<i64>,
    /// Newest publication time.
    pub last_published_at_ms: Option<i64>,
}

/// Season filter of an [`EpisodeQuery`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SeasonFilter {
    /// All episodes.
    #[default]
    Any,
    /// Episodes of one season.
    Season(u32),
    /// Episodes without a season.
    NoSeason,
}

/// Listening-state filter of an [`EpisodeQuery`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlayedFilter {
    /// All episodes.
    #[default]
    Any,
    /// Not completed (not started or in progress).
    Unplayed,
    /// Never started or marked unplayed.
    NotStarted,
    /// In progress.
    InProgress,
    /// Completed.
    Played,
}

/// Episode ordering of an [`EpisodeQuery`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EpisodeSort {
    /// Newest publication first; undated episodes last.
    #[default]
    NewestFirst,
    /// Oldest publication first; undated episodes last.
    OldestFirst,
    /// By season, then episode number, ascending; unnumbered last.
    EpisodeNumber,
    /// By season, then episode number, descending; unnumbered last.
    EpisodeNumberDesc,
}

/// Parameters of [`PodcastStore::episodes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpisodeQuery {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Season filter.
    pub season: SeasonFilter,
    /// Listening-state filter.
    pub played: PlayedFilter,
    /// Text searched in titles and descriptions (Unicode-normalized,
    /// case-insensitive substring match). Blank means no filter.
    pub search: Option<String>,
    /// Ordering.
    pub sort: EpisodeSort,
    /// Maximum rows (`None`: all).
    pub limit: Option<usize>,
    /// Rows to skip.
    pub offset: usize,
}

impl EpisodeQuery {
    /// All episodes of a podcast, newest first.
    pub fn new(podcast_id: PodcastId) -> Self {
        Self {
            podcast_id,
            season: SeasonFilter::Any,
            played: PlayedFilter::Any,
            search: None,
            sort: EpisodeSort::NewestFirst,
            limit: None,
            offset: 0,
        }
    }
}

/// An episode with its personal state.
#[derive(Debug, Clone, PartialEq)]
pub struct EpisodeListItem {
    /// Episode metadata (feed or synced reference).
    pub episode: EpisodeMeta,
    /// Progress register, if any.
    pub progress: Option<ProgressRecord>,
    /// Distinct completed listening sessions (`completed_listen_count`).
    pub completed_count: u64,
    /// Whether the latest refresh of the feed listed the episode.
    pub in_feed: bool,
}

impl EpisodeListItem {
    /// Listening status (`NotStarted` without a register).
    pub fn status(&self) -> ProgressStatus {
        self.progress
            .as_ref()
            .map_or(ProgressStatus::NotStarted, |progress| progress.status)
    }

    /// Position ordinary Play resumes from.
    pub fn resume_position_ms(&self) -> i64 {
        self.progress
            .as_ref()
            .map_or(0, ProgressRecord::resume_position_ms)
    }
}

/// A listening session in history, with display titles when known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodcastListenEntry {
    /// The session.
    pub session: ListenSession,
    /// Episode title from the catalog.
    pub episode_title: Option<String>,
    /// Podcast title from the catalog.
    pub podcast_title: Option<String>,
}

/// A prototype row that could not be imported (kept in its `legacy_*` table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyQuarantineEntry {
    /// `legacy_podcasts` or `legacy_podcast_episodes`.
    pub legacy_table: String,
    /// Row id in that table.
    pub legacy_id: i64,
    /// Short reason.
    pub reason: String,
    /// Details (the offending value or error).
    pub detail: Option<String>,
}

const EPISODE_COLUMNS: &str = "e.episode_id, e.podcast_id, e.guid, e.title, e.description, \
     e.season, e.episode_number, e.episode_type, e.published_at_ms, e.duration_ms, \
     e.audio_url, e.mime, e.byte_len, e.image_url, e.link";
const EPISODE_COLUMN_COUNT: usize = 15;

fn episode_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EpisodeMeta> {
    Ok(EpisodeMeta {
        episode_id: episode_id_col(row.get(0)?)?,
        podcast_id: podcast_id_col(row.get(1)?)?,
        guid: row.get(2)?,
        title: row.get(3)?,
        description: row.get(4)?,
        season: row.get(5)?,
        episode_number: row.get(6)?,
        episode_type: EpisodeType::parse(&row.get::<_, String>(7)?),
        published_at_ms: row.get(8)?,
        duration_ms: row.get(9)?,
        audio_url: row.get(10)?,
        mime: row.get(11)?,
        byte_len: row
            .get::<_, Option<i64>>(12)?
            .and_then(|len| u64::try_from(len).ok()),
        image_url: row.get(13)?,
        link: row.get(14)?,
    })
}

fn optional_progress(
    row: &rusqlite::Row<'_>,
    at: usize,
) -> rusqlite::Result<Option<ProgressRecord>> {
    if row.get::<_, Option<String>>(at)?.is_none() {
        return Ok(None);
    }
    progress_from_row(row, at).map(Some)
}

pub(crate) fn episode_meta_in(
    conn: &Connection,
    episode_id: &EpisodeId,
) -> Result<Option<EpisodeMeta>> {
    Ok(conn
        .query_row(
            &format!("SELECT {EPISODE_COLUMNS} FROM episodes e WHERE e.episode_id = ?1"),
            [episode_id.as_str()],
            episode_from_row,
        )
        .optional()?)
}

pub(crate) fn podcast_meta_in(
    conn: &Connection,
    podcast_id: &PodcastId,
) -> Result<Option<PodcastMeta>> {
    Ok(conn
        .query_row(
            "SELECT feed_url, title, author, description, link, image_url
               FROM podcasts WHERE podcast_id = ?1",
            [podcast_id.as_str()],
            |row| {
                Ok(PodcastMeta {
                    feed_url: feed_url_col(row.get(0)?)?,
                    title: row.get(1)?,
                    author: row.get(2)?,
                    description: row.get(3)?,
                    link: row.get(4)?,
                    image_url: row.get(5)?,
                })
            },
        )
        .optional()?)
}

/// Queue reference of a catalog episode, with `progress_generation` set to
/// the generation of its progress register (handoff hint).
pub(crate) fn episode_ref_in(
    conn: &Connection,
    episode_id: &EpisodeId,
) -> Result<Option<PodcastEpisodeRef>> {
    let Some(episode) = episode_meta_in(conn, episode_id)? else {
        return Ok(None);
    };
    let Some(podcast) = podcast_meta_in(conn, &episode.podcast_id)? else {
        return Ok(None);
    };
    let mut reference = episode.to_ref(&podcast);
    reference.progress_generation =
        apply::load_progress(conn, episode_id)?.map(|progress| progress.generation);
    Ok(Some(reference))
}

const NOT_COMPLETED: &str = "IFNULL(g.status, 'not_started') <> 'completed'";

impl PodcastStore {
    /// Podcasts with subscription state and counters, sorted by title.
    pub fn list_podcasts(&self, filter: PodcastListFilter) -> Result<Vec<PodcastSummary>> {
        let conn = self.lock();
        let join = match filter {
            PodcastListFilter::Subscribed => {
                "JOIN podcast_subscriptions r ON r.podcast_id = p.podcast_id AND r.subscribed = 1"
            }
            PodcastListFilter::All => "",
        };
        let ids: Vec<PodcastId> = {
            let mut statement = conn.prepare(&format!(
                "SELECT p.podcast_id FROM podcasts p {join}
                  ORDER BY p.title COLLATE NOCASE, p.podcast_id"
            ))?;
            statement
                .query_map([], |row| podcast_id_col(row.get(0)?))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(summary) = summary_in(&conn, &id)? {
                out.push(summary);
            }
        }
        Ok(out)
    }

    /// One podcast with subscription state and counters.
    pub fn podcast(&self, podcast_id: &PodcastId) -> Result<Option<PodcastSummary>> {
        summary_in(&self.lock(), podcast_id)
    }

    /// The subscription register (with the stored alias set), if any.
    pub fn subscription(&self, podcast_id: &PodcastId) -> Result<Option<SubscriptionRecord>> {
        apply::load_subscription_register(&self.lock(), podcast_id)
    }

    /// The share register, if any.
    pub fn share(&self, podcast_id: &PodcastId) -> Result<Option<ShareRecord>> {
        apply::load_share(&self.lock(), podcast_id)
    }

    /// The bounded, synced feed alias set of a podcast.
    pub fn feed_aliases(&self, podcast_id: &PodcastId) -> Result<Vec<FeedUrl>> {
        apply::load_aliases(&self.lock(), podcast_id)
    }

    /// Season groups of a podcast: numbered seasons ascending, then the
    /// group of episodes without a season.
    pub fn seasons(&self, podcast_id: &PodcastId) -> Result<Vec<SeasonSummary>> {
        let conn = self.lock();
        let mut statement = conn.prepare(&format!(
            "SELECT s.season, s.episode_count, s.first_published_at_ms, s.last_published_at_ms,
                    (SELECT COUNT(*) FROM episodes e
                       LEFT JOIN episode_progress g ON g.episode_id = e.episode_id
                      WHERE e.podcast_id = s.podcast_id AND e.season IS s.season
                        AND {NOT_COMPLETED})
               FROM podcast_seasons s WHERE s.podcast_id = ?1
              ORDER BY s.season IS NULL, s.season"
        ))?;
        let seasons = statement
            .query_map([podcast_id.as_str()], |row| {
                Ok(SeasonSummary {
                    season: row.get(0)?,
                    episode_count: count(row.get(1)?),
                    first_published_at_ms: row.get(2)?,
                    last_published_at_ms: row.get(3)?,
                    unplayed_count: count(row.get(4)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(seasons)
    }

    /// Episodes of a podcast or season with their progress.
    pub fn episodes(&self, query: &EpisodeQuery) -> Result<Vec<EpisodeListItem>> {
        let mut conditions = vec!["e.podcast_id = ?".to_string()];
        let mut values = vec![Value::Text(query.podcast_id.as_str().to_string())];
        match query.season {
            SeasonFilter::Any => {}
            SeasonFilter::Season(season) => {
                conditions.push("e.season = ?".to_string());
                values.push(Value::Integer(i64::from(season)));
            }
            SeasonFilter::NoSeason => conditions.push("e.season IS NULL".to_string()),
        }
        match query.played {
            PlayedFilter::Any => {}
            PlayedFilter::Unplayed => conditions.push(NOT_COMPLETED.to_string()),
            PlayedFilter::NotStarted => {
                conditions.push("IFNULL(g.status, 'not_started') = 'not_started'".to_string());
            }
            PlayedFilter::InProgress => conditions.push("g.status = 'in_progress'".to_string()),
            PlayedFilter::Played => conditions.push("g.status = 'completed'".to_string()),
        }
        if let Some(search) = query.search.as_deref() {
            let needle = music_dht::normalize_name(search);
            if !needle.is_empty() {
                conditions.push(
                    "(instr(norm(e.title), ?) > 0 OR instr(norm(e.description), ?) > 0)"
                        .to_string(),
                );
                values.push(Value::Text(needle.clone()));
                values.push(Value::Text(needle));
            }
        }
        let order = match query.sort {
            EpisodeSort::NewestFirst => {
                "e.published_at_ms IS NULL, e.published_at_ms DESC, e.episode_id"
            }
            EpisodeSort::OldestFirst => {
                "e.published_at_ms IS NULL, e.published_at_ms, e.episode_id"
            }
            EpisodeSort::EpisodeNumber => {
                "e.season IS NULL, e.season, e.episode_number IS NULL, e.episode_number, \
                 e.published_at_ms IS NULL, e.published_at_ms, e.episode_id"
            }
            EpisodeSort::EpisodeNumberDesc => {
                "e.season IS NULL, e.season DESC, e.episode_number IS NULL, \
                 e.episode_number DESC, e.published_at_ms IS NULL, e.published_at_ms DESC, \
                 e.episode_id"
            }
        };
        values.push(Value::Integer(
            query
                .limit
                .map_or(-1, |limit| i64::try_from(limit).unwrap_or(i64::MAX)),
        ));
        values.push(Value::Integer(
            i64::try_from(query.offset).unwrap_or(i64::MAX),
        ));
        let progress_columns = PROGRESS_COLUMNS.replace("p.", "g.");
        let sql = format!(
            "SELECT {EPISODE_COLUMNS}, e.in_feed, {progress_columns},
                    (SELECT COUNT(*) FROM podcast_listen_sessions s
                      WHERE s.episode_id = e.episode_id AND s.completed = 1)
               FROM episodes e
               LEFT JOIN episode_progress g ON g.episode_id = e.episode_id
              WHERE {}
              ORDER BY {order}
              LIMIT ? OFFSET ?",
            conditions.join(" AND ")
        );
        let conn = self.lock();
        let mut statement = conn.prepare(&sql)?;
        let items = statement
            .query_map(params_from_iter(values), list_item_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(items)
    }

    /// One episode with its progress.
    pub fn episode(&self, episode_id: &EpisodeId) -> Result<Option<EpisodeListItem>> {
        let progress_columns = PROGRESS_COLUMNS.replace("p.", "g.");
        Ok(self
            .lock()
            .query_row(
                &format!(
                    "SELECT {EPISODE_COLUMNS}, e.in_feed, {progress_columns},
                            (SELECT COUNT(*) FROM podcast_listen_sessions s
                              WHERE s.episode_id = e.episode_id AND s.completed = 1)
                       FROM episodes e
                       LEFT JOIN episode_progress g ON g.episode_id = e.episode_id
                      WHERE e.episode_id = ?1"
                ),
                [episode_id.as_str()],
                list_item_from_row,
            )
            .optional()?)
    }

    /// Progress register of an episode (also for episodes the catalog does
    /// not know yet).
    pub fn progress(&self, episode_id: &EpisodeId) -> Result<Option<ProgressRecord>> {
        apply::load_progress(&self.lock(), episode_id)
    }

    /// Position ordinary Play resumes from (`ProgressRecord::resume_position_ms`, 0 without progress).
    pub fn resume_position_ms(&self, episode_id: &EpisodeId) -> Result<i64> {
        Ok(self
            .progress(episode_id)?
            .map_or(0, |progress| progress.resume_position_ms()))
    }

    /// A queue item for an episode (see [`TrackItem::podcast_episode`]).
    pub fn episode_track(&self, episode_id: &EpisodeId) -> Result<Option<TrackItem>> {
        Ok(self
            .episode_ref(episode_id)?
            .map(TrackItem::podcast_episode))
    }

    /// Listening history (sessions that `should_record`), newest first.
    pub fn listen_history(
        &self,
        podcast_id: Option<&PodcastId>,
        limit: usize,
    ) -> Result<Vec<PodcastListenEntry>> {
        let conn = self.lock();
        let mut statement = conn.prepare(&format!(
            "SELECT {SESSION_COLUMNS}, e.title, p.title
               FROM podcast_listen_sessions s
               LEFT JOIN episodes e ON e.episode_id = s.episode_id
               LEFT JOIN podcasts p ON p.podcast_id = s.podcast_id
              WHERE (s.completed = 1 OR s.listened_ms >= ?1)
                AND (?2 IS NULL OR s.podcast_id = ?2)
              ORDER BY s.ended_at_ms DESC, s.session_id DESC
              LIMIT ?3"
        ))?;
        let entries = statement
            .query_map(
                params![
                    music_dht::podcast::MIN_RECORDED_EPISODE_LISTEN_MS,
                    podcast_id.map(PodcastId::as_str),
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |row| {
                    Ok(PodcastListenEntry {
                        session: session_from_row(row, 0)?,
                        episode_title: row.get(10)?,
                        podcast_title: row.get(11)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(entries)
    }

    /// Number of distinct completed sessions of an episode.
    pub fn completed_listen_count(&self, episode_id: &EpisodeId) -> Result<u64> {
        let total: i64 = self.lock().query_row(
            "SELECT COUNT(*) FROM podcast_listen_sessions WHERE episode_id = ?1 AND completed = 1",
            [episode_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(count(total))
    }

    /// Prototype rows that could not be imported.
    pub fn legacy_quarantine(&self) -> Result<Vec<LegacyQuarantineEntry>> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT legacy_table, legacy_id, reason, detail FROM legacy_quarantine
              ORDER BY legacy_table, legacy_id",
        )?;
        let entries = statement
            .query_map([], |row| {
                Ok(LegacyQuarantineEntry {
                    legacy_table: row.get(0)?,
                    legacy_id: row.get(1)?,
                    reason: row.get(2)?,
                    detail: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(entries)
    }
}

fn count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn list_item_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EpisodeListItem> {
    Ok(EpisodeListItem {
        episode: episode_from_row(row)?,
        in_feed: row.get(EPISODE_COLUMN_COUNT)?,
        progress: optional_progress(row, EPISODE_COLUMN_COUNT + 1)?,
        completed_count: count(row.get(EPISODE_COLUMN_COUNT + 10)?),
    })
}

fn summary_in(conn: &Connection, podcast_id: &PodcastId) -> Result<Option<PodcastSummary>> {
    let Some(meta) = podcast_meta_in(conn, podcast_id)? else {
        return Ok(None);
    };
    let (episode_count, unplayed_count, in_progress_count, latest, season_count) = conn.query_row(
        &format!(
            "SELECT COUNT(*),
                        COUNT(*) FILTER (WHERE {NOT_COMPLETED}),
                        COUNT(*) FILTER (WHERE g.status = 'in_progress'),
                        MAX(e.published_at_ms),
                        (SELECT COUNT(*) FROM podcast_seasons s
                          WHERE s.podcast_id = ?1 AND s.season IS NOT NULL)
                   FROM episodes e
                   LEFT JOIN episode_progress g ON g.episode_id = e.episode_id
                  WHERE e.podcast_id = ?1"
        ),
        [podcast_id.as_str()],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        },
    )?;
    let subscription = apply::load_subscription_register(conn, podcast_id)?;
    let share = apply::load_share(conn, podcast_id)?;
    Ok(Some(PodcastSummary {
        podcast_id: podcast_id.clone(),
        meta,
        subscribed: subscription
            .as_ref()
            .is_some_and(|record| record.subscribed),
        shared: effective_share(subscription.as_ref(), share.as_ref()),
        episode_count: count(episode_count),
        unplayed_count: count(unplayed_count),
        in_progress_count: count(in_progress_count),
        latest_published_at_ms: latest,
        season_count: count(season_count),
    }))
}
