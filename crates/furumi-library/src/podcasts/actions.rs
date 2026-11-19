//! Local user actions: they build the device-sync operation, apply it through
//! the shared code path and return it for the client's sync log.

use anyhow::Result;
use music_dht::device_sync::SyncOpPayload;
use music_dht::podcast::{
    EpisodeId, FeedUrl, ListenSession, PodcastEpisodeRef, PodcastId, PodcastMeta, ProgressRecord,
    ProgressStatus, effective_share, merge_feed_aliases, merge_listen_session,
    next_podcast_generation,
};
use rusqlite::{Connection, TransactionBehavior};

use super::apply::{self, ApplyOutcome};
use super::{OpStamp, PodcastLocalOp, PodcastStore, PodcastStoreError, StampSource};

/// Result of [`PodcastStore::subscribe`].
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeOutcome {
    /// Podcast the URL resolved to (an existing one when the URL is a known
    /// feed URL or alias).
    pub podcast_id: PodcastId,
    /// The podcast was already subscribed; no operation was emitted.
    pub already_subscribed: bool,
    /// Operations to append to the sync log.
    pub ops: Vec<PodcastLocalOp>,
}

/// A listening session of this device as seen by the progress register.
///
/// Returned by explicit actions; periodic checkpoints of the session use its
/// `generation` ([`PodcastStore::checkpoint`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressSession {
    /// Episode being played.
    pub episode: PodcastEpisodeRef,
    /// Session id (also the `ListenSession::session_id`).
    pub session_id: String,
    /// Generation of the session's latest explicit action.
    pub generation: u64,
}

/// A progress register written by a local action.
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressWrite {
    /// The register as written.
    pub record: ProgressRecord,
    /// The session after the action (its generation for later checkpoints).
    pub session: ProgressSession,
    /// Operation to append to the sync log.
    pub op: PodcastLocalOp,
}

/// Result of [`PodcastStore::checkpoint`].
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointOutcome {
    /// The checkpoint was written.
    Written(Box<ProgressWrite>),
    /// The register holds a greater generation (another device or an
    /// explicit action took over). Nothing was written: stop writing at the
    /// old generation, or continue locally with an explicit action
    /// ([`PodcastStore::start_session`] / [`PodcastStore::seek`]).
    Superseded {
        /// Current register.
        current: ProgressRecord,
    },
    /// This session already completed the episode at its generation; a
    /// checkpoint would turn it back into "in progress". Nothing was written.
    AlreadyCompleted {
        /// Current register.
        current: ProgressRecord,
    },
}

#[derive(Debug, Clone, Copy)]
enum ExplicitAction {
    Start(i64),
    Seek(i64),
    Restart,
    Complete(i64),
    MarkPlayed,
    MarkUnplayed,
}

fn invalid(reason: impl ToString) -> anyhow::Error {
    PodcastStoreError::Invalid(reason.to_string()).into()
}

/// Validates the payload (with a provisional stamp), draws the real stamp,
/// and applies the operation through [`apply::apply_op`].
pub(crate) fn emit(
    conn: &Connection,
    stamps: &mut dyn StampSource,
    make: impl Fn(&OpStamp) -> SyncOpPayload,
) -> Result<PodcastLocalOp> {
    let device_id = stamps.device_id().to_string();
    let provisional = OpStamp::new("provisional", device_id.clone(), 0);
    provisional
        .wire(make(&provisional))
        .validate_podcast()
        .map_err(invalid)?;
    let stamp = stamps.next_stamp()?;
    if stamp.origin_device_id != device_id {
        return Err(invalid("stamp origin differs from the local device id"));
    }
    let payload = make(&stamp);
    let op = stamp.wire(payload.clone());
    match apply::apply_op(conn, &op)? {
        ApplyOutcome::Rejected(reason) => Err(invalid(reason)),
        _ => Ok(PodcastLocalOp { stamp, payload }),
    }
}

/// Resolves a feed URL against every known URL of every podcast.
pub(crate) fn resolve_podcast(conn: &Connection, url: &FeedUrl) -> Result<PodcastId> {
    let mut statement = conn.prepare_cached(
        "SELECT podcast_id FROM podcast_feed_aliases WHERE feed_url = ?1
         UNION SELECT podcast_id FROM podcasts WHERE feed_url = ?1
         UNION SELECT podcast_id FROM podcast_subscriptions
                WHERE json_extract(podcast_json, '$.feed_url') = ?1",
    )?;
    let ids = statement
        .query_map([url.as_str()], |row| super::podcast_id_col(row.get(0)?))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let pairs: Vec<(PodcastId, FeedUrl)> = ids.into_iter().map(|id| (id, url.clone())).collect();
    Ok(music_dht::podcast::resolve_podcast_id(
        url,
        pairs.iter().map(|(id, url)| (id, url)),
    ))
}

fn catalog_meta(conn: &Connection, podcast_id: &PodcastId) -> Result<Option<PodcastMeta>> {
    use rusqlite::OptionalExtension as _;
    Ok(conn
        .query_row(
            "SELECT feed_url, title, author, description, link, image_url
               FROM podcasts WHERE podcast_id = ?1",
            [podcast_id.as_str()],
            |row| {
                Ok(PodcastMeta {
                    feed_url: super::feed_url_col(row.get(0)?)?,
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

fn share_payload(conn: &Connection, podcast_id: &PodcastId, shared: bool) -> Result<SyncOpPayload> {
    let subscription = apply::load_subscription_register(conn, podcast_id)?;
    let share = apply::load_share(conn, podcast_id)?;
    Ok(SyncOpPayload::PodcastShareSet {
        podcast_id: podcast_id.clone(),
        shared,
        generation: next_podcast_generation(subscription.as_ref(), share.as_ref()),
    })
}

/// The reference carried by progress operations: identity and display only.
fn op_ref(episode: &PodcastEpisodeRef) -> PodcastEpisodeRef {
    let mut episode = episode.clone();
    episode.progress_generation = None;
    episode
}

impl PodcastStore {
    /// Subscribes to the podcast of `podcast.feed_url` (explicit user action).
    ///
    /// The URL is resolved against known feed URLs and aliases first, so
    /// subscribing again, through a redirect target or from someone else's
    /// card reuses the existing podcast with its progress and history. When
    /// the podcast is already subscribed nothing is emitted. Otherwise one
    /// `podcast_subscription_set` with generation
    /// `next_podcast_generation(..)` and the known aliases is applied and
    /// returned. The new subscription is private until
    /// [`PodcastStore::set_shared`].
    ///
    /// Pass the channel metadata of the fetched feed
    /// (`ParsedFeed::to_podcast_meta(url)`), then ingest the episodes with
    /// [`PodcastStore::apply_feed`].
    pub fn subscribe(
        &self,
        stamps: &mut dyn StampSource,
        podcast: &PodcastMeta,
    ) -> Result<SubscribeOutcome> {
        let meta = podcast.bounded();
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let podcast_id = resolve_podcast(&tx, &meta.feed_url)?;
        let current = apply::load_subscription_register(&tx, &podcast_id)?;
        if current.as_ref().is_some_and(|record| record.subscribed) {
            return Ok(SubscribeOutcome {
                podcast_id,
                already_subscribed: true,
                ops: Vec::new(),
            });
        }
        let share = apply::load_share(&tx, &podcast_id)?;
        let generation = next_podcast_generation(current.as_ref(), share.as_ref());
        let aliases = merge_feed_aliases(
            &podcast_id,
            &apply::load_aliases(&tx, &podcast_id)?,
            std::slice::from_ref(&meta.feed_url),
        );
        let op = emit(&tx, stamps, |_| SyncOpPayload::PodcastSubscriptionSet {
            podcast_id: podcast_id.clone(),
            subscribed: true,
            podcast: meta.clone(),
            feed_aliases: aliases.clone(),
            generation,
        })?;
        tx.commit()?;
        Ok(SubscribeOutcome {
            podcast_id,
            already_subscribed: false,
            ops: vec![op],
        })
    }

    /// Unsubscribes (explicit user action).
    ///
    /// Emits `podcast_subscription_set { subscribed: false }` followed by
    /// `podcast_share_set { shared: false }`, each with the next podcast
    /// generation. Progress, sessions and the catalog are kept, so
    /// subscribing again continues where the user stopped. Does nothing when
    /// the podcast is not subscribed.
    pub fn unsubscribe(
        &self,
        stamps: &mut dyn StampSource,
        podcast_id: &PodcastId,
    ) -> Result<Vec<PodcastLocalOp>> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(current) = apply::load_subscription_register(&tx, podcast_id)? else {
            return Ok(Vec::new());
        };
        if !current.subscribed {
            return Ok(Vec::new());
        }
        let share = apply::load_share(&tx, podcast_id)?;
        let generation = next_podcast_generation(Some(&current), share.as_ref());
        let mut meta = catalog_meta(&tx, podcast_id)?
            .map(|meta| meta.bounded())
            .unwrap_or_else(|| current.podcast.clone());
        meta.feed_url = current.podcast.feed_url.clone();
        let aliases = current.feed_aliases.clone();
        let unsubscribe = emit(&tx, stamps, |_| SyncOpPayload::PodcastSubscriptionSet {
            podcast_id: podcast_id.clone(),
            subscribed: false,
            podcast: meta.clone(),
            feed_aliases: aliases.clone(),
            generation,
        })?;
        let revoke_payload = share_payload(&tx, podcast_id, false)?;
        let revoke = emit(&tx, stamps, |_| revoke_payload.clone())?;
        tx.commit()?;
        Ok(vec![unsubscribe, revoke])
    }

    /// Sets the decision to publish the podcast to the federation.
    ///
    /// Sharing needs an active subscription and is a no-op when the podcast
    /// is already effectively shared (`effective_share`). Revoking is a no-op
    /// when no share decision says `true`. The write uses the next podcast
    /// generation, so it orders after the subscription it observed.
    pub fn set_shared(
        &self,
        stamps: &mut dyn StampSource,
        podcast_id: &PodcastId,
        shared: bool,
    ) -> Result<Vec<PodcastLocalOp>> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let subscription = apply::load_subscription_register(&tx, podcast_id)?;
        let share = apply::load_share(&tx, podcast_id)?;
        if shared {
            if !subscription
                .as_ref()
                .is_some_and(|record| record.subscribed)
            {
                return Err(PodcastStoreError::NotSubscribed(podcast_id.clone()).into());
            }
            if effective_share(subscription.as_ref(), share.as_ref()) {
                return Ok(Vec::new());
            }
        } else if !share.as_ref().is_some_and(|record| record.shared) {
            return Ok(Vec::new());
        }
        let payload = share_payload(&tx, podcast_id, shared)?;
        let op = emit(&tx, stamps, |_| payload.clone())?;
        tx.commit()?;
        Ok(vec![op])
    }

    fn explicit_progress(
        &self,
        stamps: &mut dyn StampSource,
        episode: &PodcastEpisodeRef,
        session_id: Option<&str>,
        carried_generation: Option<u64>,
        action: ExplicitAction,
    ) -> Result<ProgressWrite> {
        episode.validate().map_err(invalid)?;
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = apply::load_progress(&tx, &episode.episode_id)?;
        let max_seen = current
            .as_ref()
            .map(|record| record.generation)
            .into_iter()
            .chain(carried_generation)
            .max();
        let generation = ProgressRecord::explicit_generation(max_seen);
        let current_position = current.as_ref().map_or(0, |record| record.position_ms);
        let (position_ms, status) = match action {
            ExplicitAction::Start(position) | ExplicitAction::Seek(position) => {
                (position, ProgressStatus::InProgress)
            }
            ExplicitAction::Restart => (0, ProgressStatus::InProgress),
            ExplicitAction::Complete(position) => (position, ProgressStatus::Completed),
            ExplicitAction::MarkPlayed => (current_position, ProgressStatus::Completed),
            ExplicitAction::MarkUnplayed => (0, ProgressStatus::NotStarted),
        };
        let reference = op_ref(episode);
        let op = emit(&tx, stamps, |stamp| SyncOpPayload::PodcastProgressSet {
            episode_id: episode.episode_id.clone(),
            podcast_id: episode.podcast_id.clone(),
            position_ms,
            status,
            generation,
            session_id: session_id.unwrap_or(&stamp.op_id).to_string(),
            episode: Some(reference.clone()),
        })?;
        tx.commit()?;
        let wire = op.stamp.wire(op.payload.clone());
        let record = wire.podcast_progress().expect("progress payload");
        Ok(ProgressWrite {
            session: ProgressSession {
                episode: episode.clone(),
                session_id: record.session_id.clone(),
                generation,
            },
            record,
            op,
        })
    }

    /// Starts a listening session at `position_ms` (explicit action).
    ///
    /// The generation is `explicit_generation(max(local, carried_generation))`;
    /// pass `PodcastEpisodeRef::progress_generation` of a queue item received
    /// through a handoff as `carried_generation`. Ordinary Play resumes from
    /// [`PodcastStore::resume_position_ms`].
    pub fn start_session(
        &self,
        stamps: &mut dyn StampSource,
        episode: &PodcastEpisodeRef,
        session_id: &str,
        position_ms: i64,
        carried_generation: Option<u64>,
    ) -> Result<ProgressWrite> {
        self.explicit_progress(
            stamps,
            episode,
            Some(session_id),
            carried_generation,
            ExplicitAction::Start(position_ms),
        )
    }

    /// Periodic checkpoint (or pause/stop save) of a running session.
    ///
    /// Keeps the session generation. When the register already holds a
    /// greater generation nothing is written ([`CheckpointOutcome::Superseded`]).
    pub fn checkpoint(
        &self,
        stamps: &mut dyn StampSource,
        session: &ProgressSession,
        position_ms: i64,
    ) -> Result<CheckpointOutcome> {
        let episode = &session.episode;
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(current) = apply::load_progress(&tx, &episode.episode_id)? {
            if current.generation > session.generation {
                return Ok(CheckpointOutcome::Superseded { current });
            }
            if current.generation == session.generation
                && current.session_id == session.session_id
                && current.status == ProgressStatus::Completed
            {
                return Ok(CheckpointOutcome::AlreadyCompleted { current });
            }
        }
        let reference = op_ref(episode);
        let op = emit(&tx, stamps, |_| SyncOpPayload::PodcastProgressSet {
            episode_id: episode.episode_id.clone(),
            podcast_id: episode.podcast_id.clone(),
            position_ms,
            status: ProgressStatus::InProgress,
            generation: session.generation,
            session_id: session.session_id.clone(),
            episode: Some(reference.clone()),
        })?;
        tx.commit()?;
        let record = op
            .stamp
            .wire(op.payload.clone())
            .podcast_progress()
            .expect("progress payload");
        Ok(CheckpointOutcome::Written(Box::new(ProgressWrite {
            record,
            session: session.clone(),
            op,
        })))
    }

    /// User seek during a session (explicit action; the returned session
    /// carries the new generation for later checkpoints).
    pub fn seek(
        &self,
        stamps: &mut dyn StampSource,
        session: &ProgressSession,
        position_ms: i64,
    ) -> Result<ProgressWrite> {
        self.explicit_progress(
            stamps,
            &session.episode,
            Some(&session.session_id),
            Some(session.generation),
            ExplicitAction::Seek(position_ms),
        )
    }

    /// The session reached the end (`reaches_completion` or end of stream):
    /// writes `completed` with an explicit generation. Record the session
    /// itself with [`PodcastStore::record_listen_session`].
    pub fn complete_session(
        &self,
        stamps: &mut dyn StampSource,
        session: &ProgressSession,
        position_ms: i64,
    ) -> Result<ProgressWrite> {
        self.explicit_progress(
            stamps,
            &session.episode,
            Some(&session.session_id),
            Some(session.generation),
            ExplicitAction::Complete(position_ms),
        )
    }

    /// "Start over": position 0, in progress, explicit generation.
    pub fn restart(
        &self,
        stamps: &mut dyn StampSource,
        episode: &PodcastEpisodeRef,
        session_id: &str,
    ) -> Result<ProgressWrite> {
        self.explicit_progress(
            stamps,
            episode,
            Some(session_id),
            None,
            ExplicitAction::Restart,
        )
    }

    /// "Mark played": status completed, explicit generation. Creates no
    /// listening session and invents no listening time.
    pub fn mark_played(
        &self,
        stamps: &mut dyn StampSource,
        episode: &PodcastEpisodeRef,
    ) -> Result<ProgressWrite> {
        self.explicit_progress(stamps, episode, None, None, ExplicitAction::MarkPlayed)
    }

    /// "Mark unplayed": status not started at position 0, explicit generation.
    pub fn mark_unplayed(
        &self,
        stamps: &mut dyn StampSource,
        episode: &PodcastEpisodeRef,
    ) -> Result<ProgressWrite> {
        self.explicit_progress(stamps, episode, None, None, ExplicitAction::MarkUnplayed)
    }

    /// Records (or grows) a listening session of this device.
    ///
    /// Sessions below the history threshold (`ListenSession::should_record`)
    /// and versions that do not grow the stored session emit nothing.
    /// `session.device_id` must be the local device id. The episode reference
    /// defaults to the catalog's.
    pub fn record_listen_session(
        &self,
        stamps: &mut dyn StampSource,
        session: &ListenSession,
        episode: Option<&PodcastEpisodeRef>,
    ) -> Result<Option<PodcastLocalOp>> {
        if session.device_id != stamps.device_id() {
            return Err(invalid(
                "session device_id differs from the local device id",
            ));
        }
        session.validate().map_err(invalid)?;
        if !session.should_record() {
            return Ok(None);
        }
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(current) = apply::load_session(&tx, &session.session_id)? {
            let merged = merge_listen_session(&current, session).map_err(invalid)?;
            if merged == current {
                return Ok(None);
            }
        }
        let reference = match episode {
            Some(episode) => Some(op_ref(episode)),
            None => super::queries::episode_ref_in(&tx, &session.episode_id)?.map(|r| op_ref(&r)),
        };
        let op = emit(&tx, stamps, |_| SyncOpPayload::PodcastListenRecorded {
            session: session.clone(),
            episode: reference.clone(),
        })?;
        tx.commit()?;
        Ok(Some(op))
    }

    /// Resolves a feed URL to a podcast id: the existing podcast when the
    /// URL is a known feed URL or alias (`resolve_podcast_id`), otherwise
    /// the id a first subscription through this URL would derive.
    pub fn resolve_podcast_id(&self, url: &FeedUrl) -> Result<PodcastId> {
        resolve_podcast(&self.lock(), url)
    }

    /// The episode reference for queueing or progress actions, or `None`
    /// when the episode is unknown.
    pub fn episode_ref(&self, episode_id: &EpisodeId) -> Result<Option<PodcastEpisodeRef>> {
        super::queries::episode_ref_in(&self.lock(), episode_id)
    }
}
