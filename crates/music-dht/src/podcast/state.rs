//! Personal podcast state and its convergent merge rules.
//!
//! Every merge function here is commutative, associative and idempotent over
//! records of the same entity, so trusted devices that receive the same set of
//! operations converge to the same state regardless of delivery order and
//! duplication. All ties are broken by a total order over every field.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{
    EpisodeId, FeedUrl, MAX_FEED_ALIASES, PodcastError, PodcastId, PodcastMeta, PodcastResult,
    check_token, merge_feed_aliases,
};

/// Largest tail of an episode that still counts as finished, in milliseconds.
///
/// See [`reaches_completion`].
pub const COMPLETION_TAIL_MAX_MS: i64 = 30_000;

/// Minimum actual listening time for a non-completed session to enter history.
pub const MIN_RECORDED_EPISODE_LISTEN_MS: i64 = 30_000;

/// Shared automatic completion criterion for every client.
///
/// A position completes an episode when it is within the smaller of 30 s and
/// 5 % of the duration from the end. Natural end of stream always completes,
/// whatever the announced duration. Without a known positive duration only the
/// end of stream completes.
pub fn reaches_completion(position_ms: i64, duration_ms: Option<i64>) -> bool {
    let Some(duration) = duration_ms.filter(|duration| *duration > 0) else {
        return false;
    };
    let tail = COMPLETION_TAIL_MAX_MS.min(duration / 20);
    position_ms >= duration - tail
}

/// Listening status of one episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressStatus {
    /// Never started, or explicitly marked unplayed.
    NotStarted,
    /// Started and not finished.
    InProgress,
    /// Finished, automatically or by an explicit "mark played".
    Completed,
}

/// Durable resume register of one episode.
///
/// The winner among records of the same episode is the greatest
/// `(generation, hlc_ms, device_id, op_id)`; remaining fields only break ties
/// between malformed duplicates. Progress is **never** the maximum position:
/// seeking backwards and "start over" are real changes.
///
/// * Explicit user actions (start of a new listening session, seek, "start
///   over", "mark played", "mark unplayed", taking over playback through a
///   handoff) use [`ProgressRecord::explicit_generation`]: one more than the
///   greatest generation the device has seen for the episode. This is a
///   Lamport clock, so wall-clock skew cannot undo an explicit action that was
///   observed. An explicit action during a session becomes the generation of
///   that session.
/// * Periodic checkpoints keep the generation of the latest explicit action of
///   their session. A late checkpoint of an older session therefore loses
///   against any newer explicit action, whatever its `hlc_ms`.
/// * Concurrent explicit actions on offline devices receive the same
///   generation and are ordered by `hlc_ms`, then `device_id`, then `op_id`.
/// * Handoff: only the receiving device writes the explicit action, with
///   `explicit_generation(max(local, carried))` where `carried` is
///   [`PodcastEpisodeRef::progress_generation`](super::PodcastEpisodeRef::progress_generation)
///   of the queue item. The sender writes a final checkpoint at its session
///   generation.
/// * A device that observes a greater generation for the episode it is
///   playing stops writing at its old generation: either playback moved away,
///   or the user keeps listening locally and the device writes its next
///   record as an explicit action.
/// * Outgoing-log coalescing may drop a pending progress operation only when
///   another pending operation of the same episode [supersedes](Self::supersedes)
///   it; dropping by sequence order could discard an explicit action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressRecord {
    /// Episode the register belongs to.
    pub episode_id: EpisodeId,
    /// Owning podcast.
    pub podcast_id: PodcastId,
    /// Resume position in milliseconds.
    pub position_ms: i64,
    /// Listening status.
    pub status: ProgressStatus,
    /// Lamport generation of the latest explicit action (see type docs).
    pub generation: u64,
    /// Listening session (or explicit action) that wrote the record.
    pub session_id: String,
    /// Logical write time, normally the sync operation `hlc_ms`.
    pub hlc_ms: i64,
    /// Writing device, normally the sync operation origin.
    pub device_id: String,
    /// Writing operation id.
    pub op_id: String,
}

impl ProgressRecord {
    /// Generation for an explicit user action, given the greatest generation
    /// already seen for the episode (`None` when none was seen).
    pub fn explicit_generation(max_seen: Option<u64>) -> u64 {
        max_seen.map_or(1, |generation| generation.saturating_add(1))
    }

    /// Primary ordering key.
    pub fn order_key(&self) -> (u64, i64, &str, &str) {
        (
            self.generation,
            self.hlc_ms,
            self.device_id.as_str(),
            self.op_id.as_str(),
        )
    }

    fn total_cmp(&self, other: &Self) -> Ordering {
        self.order_key()
            .cmp(&other.order_key())
            .then_with(|| self.position_ms.cmp(&other.position_ms))
            .then_with(|| self.status.cmp(&other.status))
            .then_with(|| self.session_id.cmp(&other.session_id))
            .then_with(|| self.podcast_id.cmp(&other.podcast_id))
    }

    /// Returns whether this record wins over `other` (same episode).
    pub fn supersedes(&self, other: &Self) -> bool {
        self.total_cmp(other) == Ordering::Greater
    }

    /// Position from which ordinary Play resumes.
    ///
    /// Completed and not-started episodes start from the beginning; use the
    /// explicit "start over" action to restart an in-progress episode.
    pub fn resume_position_ms(&self) -> i64 {
        match self.status {
            ProgressStatus::InProgress => self.position_ms,
            ProgressStatus::NotStarted | ProgressStatus::Completed => 0,
        }
    }

    /// Validates contract bounds.
    pub fn validate(&self) -> PodcastResult<()> {
        if self.position_ms < 0 {
            return Err(PodcastError::InvalidField {
                field: "position_ms",
                reason: "must not be negative",
            });
        }
        check_token("session_id", &self.session_id)?;
        check_token("device_id", &self.device_id)?;
        check_token("op_id", &self.op_id)
    }
}

/// Merges two progress registers of the same episode.
pub fn merge_progress(
    current: &ProgressRecord,
    incoming: &ProgressRecord,
) -> PodcastResult<ProgressRecord> {
    if current.episode_id != incoming.episode_id {
        return Err(PodcastError::Mismatch("episodes"));
    }
    Ok(if incoming.supersedes(current) {
        incoming.clone()
    } else {
        current.clone()
    })
}

/// Personal subscription state of one podcast.
///
/// `subscribed` and `podcast` form one last-writer-wins register ordered by
/// `(generation, hlc_ms, op_id)`. `generation` is a Lamport counter shared by
/// the subscription and share registers of the podcast (see
/// [`next_podcast_generation`]), so an action taken after observing another
/// device's write wins whatever the device clocks say. Unsubscribing writes
/// `subscribed = false`: it is state, not a tombstone, is never compacted, and
/// never deletes progress or history, so subscribing again continues where
/// the user stopped.
///
/// Only explicit user actions (subscribe, unsubscribe) write this register.
/// Background RSS refreshes keep their metadata locally and must not emit a
/// subscription operation; otherwise a stale device could resurrect an
/// unsubscribed podcast. Feed aliases learned by refreshes travel in their
/// own grow-only operation.
///
/// `feed_aliases` is a bounded grow-only set merged independently of the
/// register (see [`merge_feed_aliases`]): the URL the id was derived from
/// plus the [`MAX_FEED_ALIASES`] smallest other URLs. The current
/// `podcast.feed_url` may fall outside that bound, so look podcasts up by
/// [`SubscriptionRecord::feed_urls`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRecord {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Whether the user is subscribed.
    pub subscribed: bool,
    /// Channel metadata at the time of the write.
    pub podcast: PodcastMeta,
    /// Known feed URLs of the podcast.
    #[serde(default)]
    pub feed_aliases: Vec<FeedUrl>,
    /// Lamport generation shared with the share register (see type docs).
    #[serde(default)]
    pub generation: u64,
    /// Logical write time.
    pub hlc_ms: i64,
    /// Writing operation id.
    pub op_id: String,
}

impl SubscriptionRecord {
    /// Register ordering key: `(generation, hlc_ms, op_id)`.
    pub fn order_key(&self) -> (u64, i64, &str) {
        (self.generation, self.hlc_ms, self.op_id.as_str())
    }

    fn total_cmp(&self, other: &Self) -> Ordering {
        self.order_key()
            .cmp(&other.order_key())
            .then_with(|| self.subscribed.cmp(&other.subscribed))
            .then_with(|| self.podcast.cmp(&other.podcast))
    }

    /// Returns whether this record's register wins over `other`'s.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.total_cmp(other) == Ordering::Greater
    }

    /// Merges `podcast.feed_url` into `feed_aliases` and bounds the set.
    ///
    /// The result is sorted and bounded by [`merge_feed_aliases`]; it may not
    /// contain `podcast.feed_url` when many smaller aliases are known.
    pub fn normalize(&mut self) {
        self.feed_aliases = merge_feed_aliases(
            &self.podcast_id,
            &self.feed_aliases,
            std::slice::from_ref(&self.podcast.feed_url),
        );
    }

    /// Adds aliases learned through a `podcast_feed_aliases_added` operation.
    ///
    /// Only the grow-only alias set changes; the register is untouched.
    pub fn add_feed_aliases(&mut self, aliases: &[FeedUrl]) {
        self.feed_aliases = merge_feed_aliases(&self.podcast_id, &self.feed_aliases, aliases);
    }

    /// Every URL that identifies this podcast: the aliases and the current
    /// `podcast.feed_url`, sorted and without duplicates.
    ///
    /// Stores pass these pairs to [`super::resolve_podcast_id`].
    pub fn feed_urls(&self) -> Vec<FeedUrl> {
        let mut urls: BTreeSet<FeedUrl> = self.feed_aliases.iter().cloned().collect();
        urls.insert(self.podcast.feed_url.clone());
        urls.into_iter().collect()
    }

    /// Validates contract bounds.
    pub fn validate(&self) -> PodcastResult<()> {
        self.podcast.validate()?;
        if self.feed_aliases.len() > MAX_FEED_ALIASES + 1 {
            return Err(PodcastError::InvalidField {
                field: "feed_aliases",
                reason: "has too many entries",
            });
        }
        check_token("op_id", &self.op_id)
    }
}

/// Merges two subscription records of the same podcast.
///
/// The result is normalized (see [`SubscriptionRecord::normalize`]).
pub fn merge_subscription(
    current: &SubscriptionRecord,
    incoming: &SubscriptionRecord,
) -> PodcastResult<SubscriptionRecord> {
    if current.podcast_id != incoming.podcast_id {
        return Err(PodcastError::Mismatch("podcasts"));
    }
    let mut merged = if incoming.supersedes(current) {
        incoming.clone()
    } else {
        current.clone()
    };
    let mut left = current.feed_aliases.clone();
    left.push(current.podcast.feed_url.clone());
    let mut right = incoming.feed_aliases.clone();
    right.push(incoming.podcast.feed_url.clone());
    merged.feed_aliases = merge_feed_aliases(&merged.podcast_id, &left, &right);
    Ok(merged)
}

/// Decision to publish a podcast card to the federation.
///
/// Last-writer-wins by `(generation, hlc_ms, op_id)`, with `generation` the
/// per-podcast Lamport counter shared with [`SubscriptionRecord`] (see
/// [`next_podcast_generation`]). `shared = false` revokes the publication.
/// It is privacy-protecting register state, **not** a tombstone: like an
/// unsubscribe it is never compacted, otherwise a stale `shared = true`
/// delivered again would republish the podcast. Unsubscribing must also write
/// `shared = false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareRecord {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Whether the user chose to share the podcast.
    pub shared: bool,
    /// Lamport generation shared with the subscription register.
    #[serde(default)]
    pub generation: u64,
    /// Logical write time.
    pub hlc_ms: i64,
    /// Writing operation id.
    pub op_id: String,
}

impl ShareRecord {
    /// Register ordering key: `(generation, hlc_ms, op_id)`.
    pub fn order_key(&self) -> (u64, i64, &str) {
        (self.generation, self.hlc_ms, self.op_id.as_str())
    }

    fn total_cmp(&self, other: &Self) -> Ordering {
        self.order_key()
            .cmp(&other.order_key())
            .then_with(|| self.shared.cmp(&other.shared))
    }

    /// Returns whether this record wins over `other`.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.total_cmp(other) == Ordering::Greater
    }

    /// Validates contract bounds.
    pub fn validate(&self) -> PodcastResult<()> {
        check_token("op_id", &self.op_id)
    }
}

/// Merges two share records of the same podcast.
pub fn merge_share(current: &ShareRecord, incoming: &ShareRecord) -> PodcastResult<ShareRecord> {
    if current.podcast_id != incoming.podcast_id {
        return Err(PodcastError::Mismatch("podcasts"));
    }
    Ok(if incoming.supersedes(current) {
        incoming.clone()
    } else {
        current.clone()
    })
}

/// Generation for a new write of the subscription or share register.
///
/// One more than the greatest generation of either register of the podcast
/// (`None` when it has none), so a write made after observing another
/// device's subscription, unsubscription or share decision orders after it
/// even when that device's clock runs ahead. Concurrent writes on devices
/// that did not see each other receive the same generation and are ordered
/// by `hlc_ms`, then `op_id`.
pub fn next_podcast_generation(
    subscription: Option<&SubscriptionRecord>,
    share: Option<&ShareRecord>,
) -> u64 {
    subscription
        .map(|record| record.generation)
        .into_iter()
        .chain(share.map(|record| record.generation))
        .max()
        .map_or(1, |generation| generation.saturating_add(1))
}

/// Whether a podcast must currently be published.
///
/// True only when the user is subscribed, chose to share, and the share
/// decision orders after the latest subscription write (by
/// `(generation, hlc_ms, op_id)`). A new subscription (including
/// re-subscribing, or subscribing from someone else's public card) is
/// therefore always private until the user shares it again, and a stale
/// device cannot republish a podcast through an old share decision. A share
/// decision taken after observing the subscription always applies, because it
/// carries a greater generation.
pub fn effective_share(
    subscription: Option<&SubscriptionRecord>,
    share: Option<&ShareRecord>,
) -> bool {
    match (subscription, share) {
        (Some(subscription), Some(share)) => {
            subscription.subscribed
                && share.shared
                && subscription.podcast_id == share.podcast_id
                && share.order_key() > subscription.order_key()
        }
        _ => false,
    }
}

/// One listening session of an episode on one device.
///
/// Sessions form a grow-only set keyed by `session_id`, generated when
/// playback of the episode starts on a device. A device may re-send its
/// session with more listening time (for example after a crash); versions of
/// one session merge by [`merge_listen_session`].
///
/// * A session enters history when it completed or lasted at least
///   [`MIN_RECORDED_EPISODE_LISTEN_MS`] ([`ListenSession::should_record`]).
/// * `completed` is set when the session reached [`reaches_completion`] or the
///   natural end of the stream. Each session counts at most once, so the play
///   count of an episode is the number of distinct completed sessions
///   ([`completed_listen_count`]); duplicate delivery never double counts.
/// * "Mark played" changes [`ProgressRecord::status`] only. It does not create
///   a session and does not invent listening time.
/// * Sessions listened concurrently on offline devices are all kept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenSession {
    /// Unique session id, normally a UUID.
    pub session_id: String,
    /// Episode.
    pub episode_id: EpisodeId,
    /// Owning podcast.
    pub podcast_id: PodcastId,
    /// Device that played the session.
    pub device_id: String,
    /// Session start in unix milliseconds.
    pub started_at_ms: i64,
    /// Latest update of the session in unix milliseconds.
    pub ended_at_ms: i64,
    /// Position when the session started.
    pub start_position_ms: i64,
    /// Position when the session ended (or was last reported).
    pub end_position_ms: i64,
    /// Actual accumulated listening time, excluding pauses and seeks.
    pub listened_ms: i64,
    /// Whether the session completed the episode.
    pub completed: bool,
}

impl ListenSession {
    fn total_cmp(&self, other: &Self) -> Ordering {
        (
            self.completed,
            self.listened_ms,
            self.ended_at_ms,
            self.end_position_ms,
            self.started_at_ms,
            self.start_position_ms,
        )
            .cmp(&(
                other.completed,
                other.listened_ms,
                other.ended_at_ms,
                other.end_position_ms,
                other.started_at_ms,
                other.start_position_ms,
            ))
    }

    /// Whether the session belongs in listening history.
    pub fn should_record(&self) -> bool {
        self.completed || self.listened_ms >= MIN_RECORDED_EPISODE_LISTEN_MS
    }

    /// Validates contract bounds.
    pub fn validate(&self) -> PodcastResult<()> {
        check_token("session_id", &self.session_id)?;
        check_token("device_id", &self.device_id)?;
        if self.started_at_ms < 0 || self.ended_at_ms < self.started_at_ms {
            return Err(PodcastError::InvalidField {
                field: "ended_at_ms",
                reason: "must not precede started_at_ms",
            });
        }
        if self.start_position_ms < 0 || self.end_position_ms < 0 || self.listened_ms < 0 {
            return Err(PodcastError::InvalidField {
                field: "listened_ms",
                reason: "positions and listening time must not be negative",
            });
        }
        Ok(())
    }
}

/// Merges two versions of the same listening session.
///
/// The version with the greatest `(completed, listened_ms, ended_at_ms, ...)`
/// wins, so a session can only grow.
pub fn merge_listen_session(
    current: &ListenSession,
    incoming: &ListenSession,
) -> PodcastResult<ListenSession> {
    if current.session_id != incoming.session_id {
        return Err(PodcastError::Mismatch("sessions"));
    }
    if current.episode_id != incoming.episode_id
        || current.podcast_id != incoming.podcast_id
        || current.device_id != incoming.device_id
    {
        return Err(PodcastError::Mismatch("session identities"));
    }
    Ok(if incoming.total_cmp(current) == Ordering::Greater {
        incoming.clone()
    } else {
        current.clone()
    })
}

/// Number of distinct sessions that completed an episode.
pub fn completed_listen_count<'a>(sessions: impl IntoIterator<Item = &'a ListenSession>) -> usize {
    sessions
        .into_iter()
        .filter(|session| session.completed)
        .map(|session| session.session_id.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
