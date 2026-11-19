//! Wire protocol shared by furumi personal-device synchronization clients.
//!
//! The storage and UI policy live in applications. This module only defines
//! the stable JSON-lines messages spoken over the auxiliary iroh stream ALPN,
//! plus small helpers for opaque `frid://i/...` invites.
//!
//! # Versions
//!
//! | Profile `protocol_version` | Adds |
//! | --- | --- |
//! | 1 | likes, playlists, membership (`furumi/sync/1`) |
//! | 2 | [`SyncOpPayload::ListenRecorded`] (`furumi/sync/2`) |
//! | 3 | fenced personal playback coordination |
//! | 4 | podcasts: four `podcast_*` operations, podcast snapshot sections, [`PlaybackSource`] on queue items, reserved `playback_rate` |
//!
//! The ALPN stays `furumi/sync/2` for v3 and v4. [`SyncOpPayload`] and
//! [`PlaybackSource`] are closed serde enums: a peer that does not know a
//! `kind` fails to deserialize the **whole** message that contains it, not
//! just one operation. Senders must therefore filter by the peer's version
//! with [`op_allowed_for_peer`] and [`playback_allowed_for_peer`]. See the
//! workspace `PODCASTS.md` for the podcast contract.

use std::collections::BTreeMap;
use std::str::FromStr;

use federation_net::{ByteStream, NetworkId, PeerTicket, SecretKey};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{MusicDhtError, Result};
use crate::podcast::{
    EpisodeId, FeedUrl, ListenSession, MAX_PODCAST_SNAPSHOT_BYTES, MAX_SNAPSHOT_LISTEN_SESSIONS,
    MAX_SNAPSHOT_PROGRESS, MAX_SNAPSHOT_SHARES, MAX_SNAPSHOT_SUBSCRIPTIONS, PodcastEpisodeRef,
    PodcastError, PodcastId, PodcastMeta, PodcastResult, ProgressRecord, ProgressStatus,
    ShareRecord, SubscriptionRecord,
};

/// Legacy ALPN used by personal-device sync v1.
pub const SYNC_ALPN_V1: &[u8] = b"furumi/sync/1";

/// Current ALPN used by personal-device sync v2.
///
/// Version 2 adds content-addressed, append-only listening history. Clients
/// should accept both v1 and v2 while older devices remain in a sync group,
/// but must only send [`SyncOpPayload::ListenRecorded`] to v2 peers.
pub const SYNC_ALPN_V2: &[u8] = b"furumi/sync/2";

/// Auxiliary ALPN used by current personal-device sync streams.
pub const SYNC_ALPN: &[u8] = SYNC_ALPN_V2;

/// Version of the personal-device sync wire protocol.
///
/// Version 4 adds podcasts. Always advertise the constant of the running
/// client for **this** device only; for other devices keep the version they
/// reported themselves (see [`profile_protocol_version`]).
pub const DEVICE_SYNC_PROTOCOL_VERSION: u16 = 4;

/// First protocol version that understands podcast operations, podcast
/// snapshot sections and [`PlaybackSource`].
pub const PODCAST_PROTOCOL_VERSION: u16 = 4;

/// Version assumed for a device whose profile never reported one.
///
/// Profiles written by clients before v4 carry no version in
/// [`SyncOpPayload::DeviceProfileSet`]. Treat such a device as the oldest
/// protocol: it will be upgraded as soon as it reports its own version.
pub const UNREPORTED_PROTOCOL_VERSION: u16 = 1;

/// Minimum actual listening time retained for an interrupted listen.
pub const MIN_RECORDED_LISTEN_MS: i64 = 5_000;

/// Maximum listening time required to qualify a listen as a play.
pub const MAX_QUALIFYING_LISTEN_MS: i64 = 4 * 60 * 1_000;

/// Returns whether a peer may receive listening-history operations.
pub const fn supports_listen_history(protocol_version: u16) -> bool {
    protocol_version >= 2
}

/// Returns whether a peer may receive podcast operations, podcast snapshot
/// sections and playback state that contains podcast episodes.
pub const fn supports_podcasts(protocol_version: u16) -> bool {
    protocol_version >= PODCAST_PROTOCOL_VERSION
}

/// Protocol version to store for a device after reading one of its profiles.
///
/// `reported` is the version the device itself put into its
/// [`DeviceProfileWire`] (Hello/pairing) or [`SyncOpPayload::DeviceProfileSet`];
/// `previously_known` is the stored value. Receivers and relays must never
/// substitute their own [`DEVICE_SYNC_PROTOCOL_VERSION`] for another device:
/// doing so would make version gating send podcast state to old clients.
pub fn profile_protocol_version(reported: Option<u16>, previously_known: Option<u16>) -> u16 {
    reported
        .or(previously_known)
        .unwrap_or(UNREPORTED_PROTOCOL_VERSION)
}

/// Default invite lifetime used by applications unless they need a custom TTL.
pub const DEFAULT_INVITE_TTL_MS: i64 = 10 * 60 * 1000;

/// Maximum JSON line accepted by the protocol.
pub const MAX_SYNC_LINE: usize = 8 * 1024 * 1024;

/// Opaque invite payload encoded into `frid://i/<base64url-json>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteWire {
    /// Invite schema version.
    pub v: u16,
    /// Transport ticket for the inviting endpoint.
    #[serde(rename = "t")]
    pub ticket: String,
    /// Device id of the inviter.
    #[serde(rename = "d")]
    pub device_id: String,
    /// Short invite id stored by the inviter.
    #[serde(rename = "i")]
    pub invite_id: String,
    /// Runtime pairing secret.
    #[serde(rename = "s")]
    pub secret: String,
    /// Expiration timestamp in unix milliseconds.
    #[serde(rename = "e")]
    pub expires_at_ms: i64,
}

/// Public device profile exchanged inside a trusted sync group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceProfileWire {
    /// Stable device id.
    pub device_id: String,
    /// User-visible device name.
    pub name: String,
    /// Application version.
    pub client_version: String,
    /// Wire protocol version reported by **this** device.
    ///
    /// When relaying other devices' profiles, send the version stored for
    /// them, never the relay's own constant.
    pub protocol_version: u16,
    /// Transport endpoint id, as string.
    pub endpoint_id: String,
    /// Transport ticket used for direct reconnects.
    pub endpoint_ticket: String,
    /// Whether the device has been revoked.
    #[serde(default)]
    pub revoked: bool,
    /// Last accepted seq from this device after revocation.
    #[serde(default)]
    pub revoke_cutoff_seq: Option<i64>,
    /// Profile update timestamp in unix milliseconds.
    #[serde(default)]
    pub updated_at_ms: i64,
}

/// Portable track reference embedded into sync payloads for unresolved tracks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncedFedTrack {
    /// DHT item id, when known.
    pub item_id: String,
    /// DHT owner endpoint id, when known.
    pub owner: String,
    /// Track title.
    pub title: String,
    /// Main artist names.
    #[serde(default)]
    pub artist_names: Vec<String>,
    /// Featured artist names.
    #[serde(default)]
    pub featured_artist_names: Vec<String>,
    /// Release or track year.
    pub year: Option<i32>,
    /// Duration in seconds.
    pub duration_seconds: Option<i64>,
    /// Stable content id (`b3:<hex>`).
    pub content_id: String,
    /// Release title, when known.
    pub release_title: Option<String>,
    /// Track number.
    pub track_number: Option<i32>,
    /// Disc number.
    pub disc_number: Option<i32>,
}

/// Why a listening session ended.
///
/// This is deliberately factual rather than a client-specific `completed`
/// flag. All clients use [`ListenEvent::qualifies_as_play`] to derive the
/// shared play-count meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenEndReason {
    /// Playback reached the natural end of the track.
    Finished,
    /// The listener explicitly moved to another track.
    Skipped,
    /// Playback was stopped without selecting a replacement.
    Stopped,
    /// Another queue item or playback session replaced this one.
    Replaced,
    /// The client cannot determine why playback ended.
    Unknown,
}

/// Portable metadata kept with a listening event.
///
/// Metadata is a snapshot, not identity: [`ListenEvent::content_id`] is the
/// stable track identity. Keeping the snapshot lets a device display and
/// scrobble a listen even when it does not have that track in its own catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenTrackMetadata {
    /// Track title.
    pub title: String,
    /// Main artist names.
    #[serde(default)]
    pub artist_names: Vec<String>,
    /// Featured artist names.
    #[serde(default)]
    pub featured_artist_names: Vec<String>,
    /// Release title, when known.
    #[serde(default)]
    pub release_title: Option<String>,
}

/// One immutable, content-addressed listening-history event.
///
/// `listen_id` is generated when playback starts and must be reused for every
/// retry of the same report. Receivers materialize it under a unique key, so
/// browser retries and repeated sync delivery never duplicate history.
///
/// Events are append-only and belong to one trusted-device sync group. They
/// must never be published through the public music DHT.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenEvent {
    /// Stable id for this playback session, normally a UUID.
    pub listen_id: String,
    /// Stable audio content id (`b3:<hex>`).
    pub content_id: String,
    /// Playback start time as Unix milliseconds.
    pub started_at_ms: i64,
    /// Actual accumulated listening time in milliseconds.
    pub listened_ms: i64,
    /// Track duration in milliseconds, when known.
    #[serde(default)]
    pub track_duration_ms: Option<i64>,
    /// Factual reason the session ended.
    pub ended_reason: ListenEndReason,
    /// Portable display and scrobbling metadata.
    pub track: ListenTrackMetadata,
}

impl ListenEvent {
    /// Returns whether the event contains the minimum portable identity.
    pub fn is_valid(&self) -> bool {
        !self.listen_id.trim().is_empty()
            && crate::normalize_content_id(&self.content_id).is_some()
            && self.started_at_ms >= 0
            && self.listened_ms >= 0
            && self.track_duration_ms.is_none_or(|duration| duration > 0)
            && !self.track.title.trim().is_empty()
            && self
                .track
                .artist_names
                .iter()
                .any(|artist| !artist.trim().is_empty())
    }

    /// Returns whether this event should be retained in listening history.
    ///
    /// Natural track completion is always retained. Interrupted sessions are
    /// retained after five seconds of actual listening.
    pub fn should_record(&self) -> bool {
        self.is_valid()
            && (self.ended_reason == ListenEndReason::Finished
                || self.listened_ms >= MIN_RECORDED_LISTEN_MS)
    }

    /// Returns whether this event contributes to play counts.
    ///
    /// A natural finish qualifies immediately. Otherwise a listen qualifies
    /// after the smaller of half the known track duration and four minutes.
    /// Without a known duration only the four-minute threshold applies.
    pub fn qualifies_as_play(&self) -> bool {
        if !self.should_record() {
            return false;
        }
        if self.ended_reason == ListenEndReason::Finished {
            return true;
        }
        self.listened_ms >= self.qualifying_threshold_ms()
    }

    /// Returns the shared listening threshold for this event.
    pub fn qualifying_threshold_ms(&self) -> i64 {
        self.track_duration_ms
            .map(|duration| (duration / 2 + duration % 2).min(MAX_QUALIFYING_LISTEN_MS))
            .unwrap_or(MAX_QUALIFYING_LISTEN_MS)
    }
}

/// Explicit source of a queue item (protocol v4).
///
/// Absent on music items from older clients, which are music. Podcast
/// episodes are identified only by this field: never by a negative id, a
/// placeholder release or a URL in `file_path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlaybackSource {
    /// Music track resolved by `content_id` / `fed`.
    Music,
    /// Podcast episode streamed from the publisher (boxed: music items stay small).
    PodcastEpisode(Box<PodcastEpisodeRef>),
    /// Opaque placeholder that replaces a private item in Jam snapshots
    /// (see [`redact_for_jam`]). It is never playable and carries no title,
    /// URL or id.
    Private,
}

/// Display title of [`PlaybackTrack::private_placeholder`].
pub const PRIVATE_ITEM_TITLE: &str = "Private item";

/// Portable playback queue track.
///
/// For podcast items [`PlaybackTrack::source`] is authoritative; the legacy
/// display fields mirror it (see [`PlaybackTrack::podcast_episode`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaybackTrack {
    /// Local or placeholder id on the sending device.
    pub id: i64,
    /// Track title.
    pub title: String,
    /// Track number.
    pub track_number: Option<i32>,
    /// Disc number.
    pub disc_number: Option<i32>,
    /// Duration in seconds.
    pub duration_seconds: f64,
    /// Main artist names.
    #[serde(default)]
    pub artist_names: Vec<String>,
    /// Featured artist names.
    #[serde(default)]
    pub featured_artist_names: Vec<String>,
    /// Local or placeholder release id.
    pub release_id: i64,
    /// Release title.
    pub release_title: String,
    /// Release year.
    pub release_year: Option<i32>,
    /// Legacy compatibility only. Paths are device-local and should be empty.
    #[serde(default)]
    pub file_path: String,
    /// Stable content id.
    pub content_id: Option<String>,
    /// Audio format label.
    pub audio_format: Option<String>,
    /// Audio bitrate.
    pub audio_bitrate: Option<i32>,
    /// Audio sample rate.
    pub audio_sample_rate: Option<i32>,
    /// Audio bit depth.
    pub audio_bit_depth: Option<i32>,
    /// File size in bytes.
    pub file_size_bytes: Option<i64>,
    /// Sender-side play count.
    #[serde(default)]
    pub play_count: i64,
    /// Federation metadata for unresolved tracks.
    #[serde(default)]
    pub fed: Option<SyncedFedTrack>,
    /// Explicit item source (protocol v4). `None` means music.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<PlaybackSource>,
}

impl PlaybackTrack {
    /// Queue item for a podcast episode.
    ///
    /// Legacy display fields are filled for presentation only: `title` is the
    /// episode title, `artist_names` and `release_title` the podcast title,
    /// `id`/`release_id` are `0`, `file_path` is empty and `content_id` is
    /// `None`.
    pub fn podcast_episode(episode: PodcastEpisodeRef) -> Self {
        Self {
            id: 0,
            title: episode.episode_title.clone(),
            track_number: None,
            disc_number: None,
            duration_seconds: episode.duration_ms.unwrap_or(0) as f64 / 1_000.0,
            artist_names: vec![episode.podcast_title.clone()],
            featured_artist_names: Vec::new(),
            release_id: 0,
            release_title: episode.podcast_title.clone(),
            release_year: None,
            file_path: String::new(),
            content_id: None,
            audio_format: episode.mime.clone(),
            audio_bitrate: None,
            audio_sample_rate: None,
            audio_bit_depth: None,
            file_size_bytes: None,
            play_count: 0,
            fed: None,
            source: Some(PlaybackSource::PodcastEpisode(Box::new(episode))),
        }
    }

    /// Opaque, unplayable placeholder for a private item.
    pub fn private_placeholder() -> Self {
        Self {
            id: 0,
            title: PRIVATE_ITEM_TITLE.to_string(),
            track_number: None,
            disc_number: None,
            duration_seconds: 0.0,
            artist_names: Vec::new(),
            featured_artist_names: Vec::new(),
            release_id: 0,
            release_title: String::new(),
            release_year: None,
            file_path: String::new(),
            content_id: None,
            audio_format: None,
            audio_bitrate: None,
            audio_sample_rate: None,
            audio_bit_depth: None,
            file_size_bytes: None,
            play_count: 0,
            fed: None,
            source: Some(PlaybackSource::Private),
        }
    }

    /// The podcast episode of this item, if it is one.
    pub fn podcast_episode_ref(&self) -> Option<&PodcastEpisodeRef> {
        match &self.source {
            Some(PlaybackSource::PodcastEpisode(episode)) => Some(episode),
            _ => None,
        }
    }

    /// Whether this item is a Jam private placeholder.
    pub fn is_private_placeholder(&self) -> bool {
        matches!(self.source, Some(PlaybackSource::Private))
    }

    /// Whether this item needs protocol v4 (podcast episode or placeholder).
    pub fn requires_podcast_support(&self) -> bool {
        matches!(
            self.source,
            Some(PlaybackSource::PodcastEpisode(_) | PlaybackSource::Private)
        )
    }
}

/// Playback repeat mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackRepeat {
    /// Repeat disabled.
    #[default]
    Off,
    /// Repeat current track.
    One,
    /// Repeat the whole queue.
    All,
}

/// Portable playback state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaybackStateWire {
    /// Queue tracks.
    #[serde(default)]
    pub queue: Vec<PlaybackTrack>,
    /// Current queue index.
    #[serde(default)]
    pub queue_pos: usize,
    /// Whether playback is active.
    pub playing: bool,
    /// Whether playback is paused.
    pub paused: bool,
    /// Active device idle timestamp, when paused/stopped.
    #[serde(default)]
    pub idle_since_ms: Option<i64>,
    /// Current position in seconds.
    pub position_secs: f64,
    /// Volume 0..100.
    #[serde(default)]
    pub volume: u8,
    /// Shuffle mode.
    pub shuffle: bool,
    /// Repeat mode.
    pub repeat: PlaybackRepeat,
    /// Reserved playback rate (1.0 = normal speed); no client uses it yet.
    ///
    /// When implemented, position extrapolation and handoff must account for
    /// it. Receivers that do not support rates treat it as 1.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub playback_rate: Option<f32>,
}

/// Playback snapshot with the common coordination envelope.
pub type PlaybackSnapshot = crate::playback::Snapshot<PlaybackStateWire>;
/// Shared command vocabulary with the portable music queue DTO.
pub type PlaybackCommand = crate::playback::Command<PlaybackStateWire>;

/// One operation in the device-sync log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncOpWire {
    /// Unique op id, usually `<origin_device_id>:<seq>`.
    pub op_id: String,
    /// Origin device id.
    pub origin_device_id: String,
    /// Monotonic sequence inside the origin device log.
    pub seq: i64,
    /// Hybrid/logical timestamp in unix milliseconds.
    pub hlc_ms: i64,
    /// Operation payload.
    pub payload: SyncOpPayload,
}

/// Personal-device sync operation payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncOpPayload {
    /// Set like state for a content id.
    TrackLikeSet {
        /// Content id.
        content_id: String,
        /// Desired like state.
        liked: bool,
        /// Optional unresolved federated metadata.
        #[serde(default)]
        fed: Option<SyncedFedTrack>,
    },
    /// Create playlist.
    PlaylistCreated {
        /// Stable playlist sync id.
        playlist_id: String,
        /// Playlist title.
        title: String,
    },
    /// Rename playlist.
    PlaylistRenamed {
        /// Stable playlist sync id.
        playlist_id: String,
        /// Playlist title.
        title: String,
    },
    /// Delete playlist.
    PlaylistDeleted {
        /// Stable playlist sync id.
        playlist_id: String,
    },
    /// Add track to playlist.
    PlaylistTrackAdded {
        /// Stable playlist sync id.
        playlist_id: String,
        /// Content id.
        content_id: String,
        /// Playlist position.
        position: i64,
        /// Optional unresolved federated metadata.
        #[serde(default)]
        fed: Option<SyncedFedTrack>,
    },
    /// Remove track from playlist.
    PlaylistTrackRemoved {
        /// Stable playlist sync id.
        playlist_id: String,
        /// Content id.
        content_id: String,
    },
    /// Update origin device profile.
    DeviceProfileSet {
        /// Display name.
        name: String,
        /// Client version.
        client_version: String,
        /// Endpoint ticket.
        endpoint_ticket: String,
        /// Endpoint id.
        endpoint_id: String,
        /// Protocol version of the origin device (v4+).
        ///
        /// `None` from older clients. Receivers store
        /// [`profile_protocol_version`]`(protocol_version, stored)` and must
        /// never substitute their own constant.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        protocol_version: Option<u16>,
    },
    /// Trust a device.
    DeviceTrusted {
        /// Trusted device id.
        target_device_id: String,
    },
    /// Revoke a device.
    DeviceRevoked {
        /// Revoked device id.
        target_device_id: String,
        /// Max seq seen from the target at revoke time.
        target_max_seq_seen: i64,
    },
    /// Playback command for a target device.
    PlaybackCommand {
        /// Target device id.
        target_device_id: String,
        /// Command.
        command: PlaybackCommand,
        /// Ownership and ordering fence. Legacy commands cannot change v3 playback.
        #[serde(default)]
        authority: Option<crate::playback::CommandStamp>,
    },
    /// Append one immutable listening-history event.
    ///
    /// This operation is valid only on device-sync protocol v2. The event is
    /// deduplicated by `event.listen_id`, independently of transport `op_id`.
    ListenRecorded {
        /// Portable listening event.
        event: ListenEvent,
    },
    /// Subscribe to or unsubscribe from a podcast (protocol v4).
    ///
    /// Last-writer-wins by `(hlc_ms, op_id)` of the operation. Unsubscribing
    /// is state, not a tombstone. Only explicit user actions emit it; RSS
    /// refreshes never do. Materialize with [`SyncOpWire::podcast_subscription`].
    PodcastSubscriptionSet {
        /// Podcast.
        podcast_id: PodcastId,
        /// Desired subscription state.
        subscribed: bool,
        /// Channel metadata, so other devices subscribe without the URL.
        podcast: PodcastMeta,
        /// Known feed URLs (grow-only, bounded).
        #[serde(default)]
        feed_aliases: Vec<FeedUrl>,
    },
    /// Share or stop sharing a podcast card with the federation (protocol v4).
    ///
    /// Last-writer-wins; `shared = false` is a tombstone. Unsubscribing also
    /// emits `shared = false`.
    PodcastShareSet {
        /// Podcast.
        podcast_id: PodcastId,
        /// Desired share state.
        shared: bool,
    },
    /// Write the durable resume register of an episode (protocol v4).
    ///
    /// Ordered by `(generation, hlc_ms, origin_device_id, op_id)`; see
    /// [`ProgressRecord`]. Clients coalesce periodic checkpoints in their
    /// outgoing log: only the latest per episode needs to stay pending.
    PodcastProgressSet {
        /// Episode.
        episode_id: EpisodeId,
        /// Owning podcast.
        podcast_id: PodcastId,
        /// Resume position in milliseconds.
        position_ms: i64,
        /// Listening status.
        status: ProgressStatus,
        /// Lamport generation (see [`ProgressRecord`]).
        generation: u64,
        /// Session or explicit action id.
        session_id: String,
        /// Display reference, so a device without the feed can show and
        /// resume the episode.
        #[serde(default)]
        episode: Option<PodcastEpisodeRef>,
    },
    /// Record or update one listening session (protocol v4).
    ///
    /// Grow-only set keyed by `session.session_id`; see [`ListenSession`].
    /// `session.device_id` must equal the operation origin.
    PodcastListenRecorded {
        /// Listening session.
        session: ListenSession,
        /// Display reference for history on devices without the feed.
        #[serde(default)]
        episode: Option<PodcastEpisodeRef>,
    },
}

impl SyncOpPayload {
    /// Returns true for operations that remove state and may eventually compact.
    ///
    /// `podcast_share_set { shared: false }` is a tombstone. Unsubscribing
    /// (`podcast_subscription_set { subscribed: false }`) is **not**: it is
    /// last-writer-wins state that must keep beating stale subscriptions.
    pub fn is_tombstone(&self) -> bool {
        matches!(
            self,
            SyncOpPayload::TrackLikeSet { liked: false, .. }
                | SyncOpPayload::PlaylistDeleted { .. }
                | SyncOpPayload::PlaylistTrackRemoved { .. }
                | SyncOpPayload::DeviceRevoked { .. }
                | SyncOpPayload::PodcastShareSet { shared: false, .. }
        )
    }

    /// Returns true for the protocol v4 podcast operation kinds.
    pub fn is_podcast(&self) -> bool {
        matches!(
            self,
            SyncOpPayload::PodcastSubscriptionSet { .. }
                | SyncOpPayload::PodcastShareSet { .. }
                | SyncOpPayload::PodcastProgressSet { .. }
                | SyncOpPayload::PodcastListenRecorded { .. }
        )
    }
}

impl SyncOpWire {
    /// Subscription record written by a `podcast_subscription_set` operation.
    ///
    /// The register stamp is the operation's `(hlc_ms, op_id)`.
    pub fn podcast_subscription(&self) -> Option<SubscriptionRecord> {
        let SyncOpPayload::PodcastSubscriptionSet {
            podcast_id,
            subscribed,
            podcast,
            feed_aliases,
        } = &self.payload
        else {
            return None;
        };
        let mut record = SubscriptionRecord {
            podcast_id: podcast_id.clone(),
            subscribed: *subscribed,
            podcast: podcast.clone(),
            feed_aliases: feed_aliases.clone(),
            hlc_ms: self.hlc_ms,
            op_id: self.op_id.clone(),
        };
        record.normalize();
        Some(record)
    }

    /// Share record written by a `podcast_share_set` operation.
    pub fn podcast_share(&self) -> Option<ShareRecord> {
        let SyncOpPayload::PodcastShareSet { podcast_id, shared } = &self.payload else {
            return None;
        };
        Some(ShareRecord {
            podcast_id: podcast_id.clone(),
            shared: *shared,
            hlc_ms: self.hlc_ms,
            op_id: self.op_id.clone(),
        })
    }

    /// Progress register written by a `podcast_progress_set` operation.
    ///
    /// `device_id` is the operation origin.
    pub fn podcast_progress(&self) -> Option<ProgressRecord> {
        let SyncOpPayload::PodcastProgressSet {
            episode_id,
            podcast_id,
            position_ms,
            status,
            generation,
            session_id,
            ..
        } = &self.payload
        else {
            return None;
        };
        Some(ProgressRecord {
            episode_id: episode_id.clone(),
            podcast_id: podcast_id.clone(),
            position_ms: *position_ms,
            status: *status,
            generation: *generation,
            session_id: session_id.clone(),
            hlc_ms: self.hlc_ms,
            device_id: self.origin_device_id.clone(),
            op_id: self.op_id.clone(),
        })
    }

    /// Listening session carried by a `podcast_listen_recorded` operation.
    pub fn podcast_listen_session(&self) -> Option<&ListenSession> {
        match &self.payload {
            SyncOpPayload::PodcastListenRecorded { session, .. } => Some(session),
            _ => None,
        }
    }

    /// Validates a podcast operation against the contract.
    ///
    /// Non-podcast operations are accepted unchanged. Receivers must skip
    /// (but still acknowledge) invalid podcast operations.
    pub fn validate_podcast(&self) -> PodcastResult<()> {
        let check_ref = |episode: &Option<PodcastEpisodeRef>,
                         episode_id: &EpisodeId,
                         podcast_id: &PodcastId| {
            match episode {
                Some(episode)
                    if &episode.episode_id != episode_id || &episode.podcast_id != podcast_id =>
                {
                    Err(PodcastError::Mismatch("episode reference"))
                }
                Some(episode) => episode.validate(),
                None => Ok(()),
            }
        };
        match &self.payload {
            SyncOpPayload::PodcastSubscriptionSet { .. } => self
                .podcast_subscription()
                .expect("subscription payload")
                .validate(),
            SyncOpPayload::PodcastShareSet { .. } => {
                self.podcast_share().expect("share payload").validate()
            }
            SyncOpPayload::PodcastProgressSet {
                episode_id,
                podcast_id,
                episode,
                ..
            } => {
                self.podcast_progress()
                    .expect("progress payload")
                    .validate()?;
                check_ref(episode, episode_id, podcast_id)
            }
            SyncOpPayload::PodcastListenRecorded { session, episode } => {
                session.validate()?;
                if session.device_id != self.origin_device_id {
                    return Err(PodcastError::Mismatch(
                        "session device and operation origin",
                    ));
                }
                check_ref(episode, &session.episode_id, &session.podcast_id)
            }
            _ => Ok(()),
        }
    }
}

/// Returns whether `payload` may be sent to a peer with `peer_version`.
///
/// * `listen_recorded` needs v2;
/// * every `podcast_*` operation needs v4;
/// * a `playback_command` whose state contains podcast items needs v4;
/// * everything else is accepted by every version.
///
/// Gating is mandatory: an older peer cannot deserialize an unknown `kind`
/// and would reject the entire message, stalling all synchronization.
pub fn op_allowed_for_peer(payload: &SyncOpPayload, peer_version: u16) -> bool {
    match payload {
        SyncOpPayload::ListenRecorded { .. } => supports_listen_history(peer_version),
        SyncOpPayload::PlaybackCommand { command, .. } => {
            !playback_command_has_podcasts(command) || supports_podcasts(peer_version)
        }
        payload if payload.is_podcast() => supports_podcasts(peer_version),
        _ => true,
    }
}

/// Returns whether the queue contains podcast episodes or Jam placeholders.
pub fn playback_state_has_podcasts(state: &PlaybackStateWire) -> bool {
    state
        .queue
        .iter()
        .any(PlaybackTrack::requires_podcast_support)
}

/// Returns whether a playback command carries podcast items.
pub fn playback_command_has_podcasts(command: &PlaybackCommand) -> bool {
    match command {
        crate::playback::Command::SetState { state, .. }
        | crate::playback::Command::ActiveChanged { state, .. } => {
            playback_state_has_podcasts(state)
        }
    }
}

/// Returns whether `state` may be sent to (or handed off to) a peer.
///
/// For a peer below v4 with podcast items in the queue, do not send the
/// playback snapshot and refuse the handoff with an "update required" message.
/// Never strip the episodes to make the state fit: an old client would then
/// forward a queue without them and lose them for every device.
pub fn playback_allowed_for_peer(state: &PlaybackStateWire, peer_version: u16) -> bool {
    !playback_state_has_podcasts(state) || supports_podcasts(peer_version)
}

/// Returns whether a `SetState`/`ActiveChanged` command from a peer may be
/// applied to the local playback state.
///
/// While the local queue contains podcast items, commands from peers below
/// v4 are rejected: such a peer does not know the items and its state would
/// silently drop them.
pub fn playback_command_allowed_from_peer(local: &PlaybackStateWire, peer_version: u16) -> bool {
    supports_podcasts(peer_version) || !playback_state_has_podcasts(local)
}

/// Returns a copy of `state` safe to show to Jam participants.
///
/// Every podcast episode (and any existing placeholder) is replaced by
/// [`PlaybackTrack::private_placeholder`]: no title, URL, feed or id leaves
/// the host. Queue order, position and transport state are kept.
pub fn redact_for_jam(mut state: PlaybackStateWire) -> PlaybackStateWire {
    for track in &mut state.queue {
        if track.requires_podcast_support() {
            *track = PlaybackTrack::private_placeholder();
        }
    }
    state
}

/// Returns whether a Jam participant command may be considered at all.
///
/// Commands that contain real podcast episodes are rejected: participants
/// cannot add episodes to the host queue. Placeholders are allowed here and
/// must be mapped back with [`restore_jam_private_items`].
pub fn jam_command_allowed(command: &PlaybackCommand) -> bool {
    match command {
        crate::playback::Command::SetState { state, .. }
        | crate::playback::Command::ActiveChanged { state, .. } => !state
            .queue
            .iter()
            .any(|track| track.podcast_episode_ref().is_some()),
    }
}

/// Maps the placeholders of a participant's state back to the host's items.
///
/// The k-th placeholder of `participant` becomes the k-th private item of
/// `host`. Returns `None` (reject the command) when the participant state
/// contains real podcast episodes or a different number of placeholders, so a
/// participant can reorder music and move placeholders but cannot drop,
/// duplicate or inject private items.
pub fn restore_jam_private_items(
    host: &PlaybackStateWire,
    mut participant: PlaybackStateWire,
) -> Option<PlaybackStateWire> {
    let mut private = host
        .queue
        .iter()
        .filter(|track| track.requires_podcast_support());
    for track in &mut participant.queue {
        if track.podcast_episode_ref().is_some() {
            return None;
        }
        if track.is_private_placeholder() {
            *track = private.next()?.clone();
        }
    }
    private.next().is_none().then_some(participant)
}

/// Materialized sync snapshot sent with every handshake.
///
/// Music listening history is intentionally absent: it is an unbounded
/// append-only log, not last-write-wins state. New devices catch it up from
/// missing [`SyncOpPayload::ListenRecorded`] operations using the existing
/// per-origin vector and bounded operation batches.
///
/// Podcast state (protocol v4) **is** included, bounded by
/// [`SyncSnapshot::bound_podcast_state`]. Operations hidden from a peer while
/// it ran an older version are never re-requested after it upgrades, because
/// sync vectors only grow, so snapshots are the recovery path. Send podcast
/// sections only to v4 peers ([`SyncSnapshot::for_peer`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncSnapshot {
    /// Current likes.
    #[serde(default)]
    pub likes: Vec<SnapshotLike>,
    /// Like tombstones.
    #[serde(default)]
    pub unlikes: Vec<SnapshotLikeTombstone>,
    /// Current playlists.
    #[serde(default)]
    pub playlists: Vec<SnapshotPlaylist>,
    /// Deleted playlists.
    #[serde(default)]
    pub deleted_playlists: Vec<SnapshotPlaylistTombstone>,
    /// Removed playlist items.
    #[serde(default)]
    pub removed_playlist_items: Vec<SnapshotPlaylistItemTombstone>,
    /// Podcast subscription registers, including unsubscribed ones (v4).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub podcast_subscriptions: Vec<SubscriptionRecord>,
    /// Podcast share registers, including `shared = false` tombstones (v4).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub podcast_shares: Vec<ShareRecord>,
    /// Episode progress registers (v4).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub podcast_progress: Vec<ProgressRecord>,
    /// Most recent podcast listening sessions (v4).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub podcast_listen_sessions: Vec<ListenSession>,
}

impl SyncSnapshot {
    /// Whether any podcast section is non-empty.
    pub fn has_podcast_state(&self) -> bool {
        !self.podcast_subscriptions.is_empty()
            || !self.podcast_shares.is_empty()
            || !self.podcast_progress.is_empty()
            || !self.podcast_listen_sessions.is_empty()
    }

    /// Removes all podcast sections.
    pub fn strip_podcast_state(&mut self) {
        self.podcast_subscriptions.clear();
        self.podcast_shares.clear();
        self.podcast_progress.clear();
        self.podcast_listen_sessions.clear();
    }

    /// Copy of the snapshot for a peer: podcast sections only for v4+.
    pub fn for_peer(&self, peer_version: u16) -> Self {
        let mut snapshot = self.clone();
        if !supports_podcasts(peer_version) {
            snapshot.strip_podcast_state();
        }
        snapshot
    }

    /// Sorts and truncates the podcast sections to the snapshot limits.
    ///
    /// Each section is ordered newest first (subscriptions, shares and
    /// progress by `hlc_ms`, sessions by `ended_at_ms`, ties by id) and capped
    /// by its `MAX_SNAPSHOT_*` count. Sections then share
    /// [`MAX_PODCAST_SNAPSHOT_BYTES`] of serialized JSON in priority order:
    /// subscriptions, shares, progress, sessions. Older registers that do not
    /// fit still propagate through operations.
    pub fn bound_podcast_state(&mut self) {
        let mut budget = MAX_PODCAST_SNAPSHOT_BYTES;
        self.podcast_subscriptions.sort_by(|a, b| {
            (b.hlc_ms, &b.op_id, &b.podcast_id).cmp(&(a.hlc_ms, &a.op_id, &a.podcast_id))
        });
        bound_section(
            &mut self.podcast_subscriptions,
            MAX_SNAPSHOT_SUBSCRIPTIONS,
            &mut budget,
        );
        self.podcast_shares.sort_by(|a, b| {
            (b.hlc_ms, &b.op_id, &b.podcast_id).cmp(&(a.hlc_ms, &a.op_id, &a.podcast_id))
        });
        bound_section(&mut self.podcast_shares, MAX_SNAPSHOT_SHARES, &mut budget);
        self.podcast_progress.sort_by(|a, b| {
            (b.hlc_ms, &b.op_id, &b.episode_id).cmp(&(a.hlc_ms, &a.op_id, &a.episode_id))
        });
        bound_section(
            &mut self.podcast_progress,
            MAX_SNAPSHOT_PROGRESS,
            &mut budget,
        );
        self.podcast_listen_sessions
            .sort_by(|a, b| (b.ended_at_ms, &b.session_id).cmp(&(a.ended_at_ms, &a.session_id)));
        bound_section(
            &mut self.podcast_listen_sessions,
            MAX_SNAPSHOT_LISTEN_SESSIONS,
            &mut budget,
        );
    }

    /// Whether the podcast sections respect the per-section count limits.
    ///
    /// Receivers should ignore the podcast sections of a snapshot that
    /// violates them and validate every record before merging it.
    pub fn podcast_state_within_limits(&self) -> bool {
        self.podcast_subscriptions.len() <= MAX_SNAPSHOT_SUBSCRIPTIONS
            && self.podcast_shares.len() <= MAX_SNAPSHOT_SHARES
            && self.podcast_progress.len() <= MAX_SNAPSHOT_PROGRESS
            && self.podcast_listen_sessions.len() <= MAX_SNAPSHOT_LISTEN_SESSIONS
    }
}

fn bound_section<T: Serialize>(items: &mut Vec<T>, max_items: usize, budget: &mut usize) {
    let mut keep = 0;
    for item in items.iter().take(max_items) {
        let size = serde_json::to_vec(item).map_or(usize::MAX, |bytes| bytes.len() + 1);
        if size > *budget {
            break;
        }
        *budget -= size;
        keep += 1;
    }
    items.truncate(keep);
}

/// Snapshot like row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLike {
    /// Content id.
    pub content_id: String,
    /// Last write timestamp.
    pub hlc_ms: i64,
    /// Last write op id.
    pub op_id: String,
    /// Optional unresolved federated metadata.
    #[serde(default)]
    pub fed: Option<SyncedFedTrack>,
}

/// Snapshot unlike tombstone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLikeTombstone {
    /// Content id.
    pub content_id: String,
    /// Last write timestamp.
    pub hlc_ms: i64,
    /// Last write op id.
    pub op_id: String,
}

/// Snapshot playlist.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPlaylist {
    /// Stable playlist sync id.
    pub playlist_id: String,
    /// Title.
    pub title: String,
    /// Last write timestamp.
    pub hlc_ms: i64,
    /// Last write op id.
    pub op_id: String,
    /// Current items.
    #[serde(default)]
    pub items: Vec<SnapshotPlaylistItem>,
}

/// Snapshot deleted playlist tombstone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPlaylistTombstone {
    /// Stable playlist sync id.
    pub playlist_id: String,
    /// Last write timestamp.
    pub hlc_ms: i64,
    /// Last write op id.
    pub op_id: String,
}

/// Snapshot playlist item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPlaylistItem {
    /// Content id.
    pub content_id: String,
    /// Playlist position.
    pub position: i64,
    /// Last write timestamp.
    pub hlc_ms: i64,
    /// Last write op id.
    pub op_id: String,
    /// Optional unresolved federated metadata.
    #[serde(default)]
    pub fed: Option<SyncedFedTrack>,
}

/// Snapshot removed playlist item tombstone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPlaylistItemTombstone {
    /// Stable playlist sync id.
    pub playlist_id: String,
    /// Content id.
    pub content_id: String,
    /// Last write timestamp.
    pub hlc_ms: i64,
    /// Last write op id.
    pub op_id: String,
}

/// Top-level message spoken on [`SYNC_ALPN`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireMessage {
    /// Pairing request sent by the invite consumer.
    PairRequest {
        /// Invite id.
        invite_id: String,
        /// Invite secret.
        secret: String,
        /// Requester device profile.
        profile: DeviceProfileWire,
        /// Requester sync group id.
        #[serde(default)]
        group_id: Option<String>,
        /// Number of active devices in requester group.
        #[serde(default)]
        group_active_devices: usize,
        /// Requester trusted devices.
        #[serde(default)]
        devices: Vec<DeviceProfileWire>,
        /// Requester vector clock.
        vector: BTreeMap<String, i64>,
        /// Requester pending operations.
        ops: Vec<SyncOpWire>,
        /// Requester materialized snapshot.
        snapshot: SyncSnapshot,
        /// Requester playback snapshot.
        #[serde(default)]
        playback: Option<PlaybackSnapshot>,
    },
    /// Pairing response sent by the inviter.
    PairResponse {
        /// Whether pairing was accepted.
        accepted: bool,
        /// Whether the request is waiting for user confirmation.
        #[serde(default)]
        pending: bool,
        /// Optional rejection error.
        #[serde(default)]
        error: Option<String>,
        /// Accepted group id.
        #[serde(default)]
        group_id: Option<String>,
        /// Inviter profile.
        #[serde(default)]
        profile: Option<DeviceProfileWire>,
        /// Inviter trusted devices.
        #[serde(default)]
        devices: Vec<DeviceProfileWire>,
        /// Inviter vector clock.
        #[serde(default)]
        vector: BTreeMap<String, i64>,
        /// Inviter pending operations.
        #[serde(default)]
        ops: Vec<SyncOpWire>,
        /// Inviter materialized snapshot.
        #[serde(default)]
        snapshot: SyncSnapshot,
        /// Inviter playback snapshot.
        #[serde(default)]
        playback: Option<PlaybackSnapshot>,
    },
    /// Regular sync hello.
    Hello {
        /// Sync group id.
        group_id: String,
        /// Sender profile.
        profile: DeviceProfileWire,
        /// Sender trusted devices.
        devices: Vec<DeviceProfileWire>,
        /// Sender vector clock.
        vector: BTreeMap<String, i64>,
        /// Sender pending operations.
        ops: Vec<SyncOpWire>,
        /// Sender materialized snapshot.
        snapshot: SyncSnapshot,
        /// Sender playback snapshot.
        #[serde(default)]
        playback: Option<PlaybackSnapshot>,
    },
    /// Regular sync response.
    SyncResponse {
        /// Whether sync was accepted.
        accepted: bool,
        /// Optional rejection error.
        #[serde(default)]
        error: Option<String>,
        /// Receiver trusted devices.
        #[serde(default)]
        devices: Vec<DeviceProfileWire>,
        /// Receiver vector clock.
        #[serde(default)]
        vector: BTreeMap<String, i64>,
        /// Receiver pending operations.
        #[serde(default)]
        ops: Vec<SyncOpWire>,
        /// Receiver materialized snapshot.
        #[serde(default)]
        snapshot: SyncSnapshot,
        /// Receiver playback snapshot.
        #[serde(default)]
        playback: Option<PlaybackSnapshot>,
    },
}

/// Encodes an invite as a compact `frid://i/...` link.
pub fn encode_invite(invite: &InviteWire) -> Result<String> {
    let bytes = serde_json::to_vec(invite).map_err(protocol_err)?;
    Ok(format!("frid://i/{}", base64url_encode(&bytes)))
}

/// Parses a `frid://i/...` invite.
pub fn parse_invite(value: &str) -> Result<InviteWire> {
    let Some(token) = value.trim().strip_prefix("frid://i/") else {
        return Err(MusicDhtError::Protocol(
            "usage: frid://i/<invite>".to_string(),
        ));
    };
    let bytes = base64url_decode(token)?;
    let invite: InviteWire = serde_json::from_slice(&bytes).map_err(protocol_err)?;
    if invite.v != 1 {
        return Err(MusicDhtError::Protocol(
            "unsupported invite version".to_string(),
        ));
    }
    Ok(invite)
}

/// Extracts the network id carried by an invite's endpoint ticket.
pub fn invite_network_id(value: &str) -> Result<NetworkId> {
    let invite = parse_invite(value)?;
    let ticket = PeerTicket::from_str(&invite.ticket).map_err(protocol_err)?;
    Ok(ticket.network_id)
}

/// Hashes an invite secret for durable storage.
pub fn hash_secret(secret: &str) -> String {
    blake3::hash(secret.as_bytes()).to_hex().to_string()
}

/// Generates a random lowercase hex token.
pub fn random_hex(bytes: usize) -> String {
    let key = SecretKey::generate();
    let mut seed = key.to_bytes().to_vec();
    while seed.len() < bytes {
        seed.extend_from_slice(blake3::hash(&seed).as_bytes());
    }
    hex_encode(&seed[..bytes])
}

/// Extracts endpoint id from a ticket string.
pub fn ticket_endpoint_id(ticket: &str) -> Option<String> {
    let ticket = PeerTicket::from_str(ticket).ok()?;
    Some(ticket.endpoint_id().to_string())
}

/// Writes a JSON-lines sync message.
pub async fn write_msg(stream: &mut ByteStream, message: &WireMessage) -> Result<()> {
    let mut payload = serde_json::to_vec(message).map_err(protocol_err)?;
    payload.push(b'\n');
    stream.send.write_all(&payload).await.map_err(network_err)?;
    Ok(())
}

/// Finishes the sending side.
pub async fn finish_send(stream: &mut ByteStream) -> Result<()> {
    stream.send.finish().map_err(network_err)?;
    Ok(())
}

/// Finishes the response side and waits briefly for peer acknowledgement.
pub async fn finish_response(
    stream: &mut ByteStream,
    drain_timeout: std::time::Duration,
) -> Result<()> {
    stream.send.finish().map_err(network_err)?;
    let _ = tokio::time::timeout(drain_timeout, stream.send.stopped()).await;
    Ok(())
}

/// Reads a JSON-lines sync message.
pub async fn read_msg(stream: &mut ByteStream) -> Result<WireMessage> {
    let line = read_line(&mut stream.recv).await?;
    serde_json::from_slice(&line).map_err(protocol_err)
}

/// Reads one newline-terminated protocol line.
pub async fn read_line<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    read_line_limited(reader, MAX_SYNC_LINE).await
}

/// Reads one newline-terminated protocol line with a custom size limit.
pub async fn read_line_limited<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_line: usize,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let read = reader.read(&mut byte).await.map_err(network_err)?;
        if read == 0 {
            break;
        }
        if byte[0] == b'\n' {
            break;
        }
        out.push(byte[0]);
        if out.len() > max_line {
            return Err(MusicDhtError::Protocol(
                "protocol line is too large".to_string(),
            ));
        }
    }
    Ok(out)
}

/// Reads one sync message from any async reader.
pub async fn read_msg_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<WireMessage> {
    let line = read_line(reader).await?;
    serde_json::from_slice(&line).map_err(protocol_err)
}

fn base64url_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes.get(i + 1).copied().unwrap_or(0);
        let b2 = bytes.get(i + 2).copied().unwrap_or(0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if i + 1 < bytes.len() {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        }
        if i + 2 < bytes.len() {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        }
        i += 3;
    }
    out
}

fn base64url_decode(value: &str) -> Result<Vec<u8>> {
    fn val(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut i = 0;
    while i < bytes.len() {
        let a = val(bytes[i])
            .ok_or_else(|| MusicDhtError::Protocol("invalid base64url invite".to_string()))?;
        let b = val(*bytes
            .get(i + 1)
            .ok_or_else(|| MusicDhtError::Protocol("truncated base64url invite".to_string()))?)
        .ok_or_else(|| MusicDhtError::Protocol("invalid base64url invite".to_string()))?;
        let c = bytes.get(i + 2).and_then(|byte| val(*byte));
        let d = bytes.get(i + 3).and_then(|byte| val(*byte));
        out.push((a << 2) | (b >> 4));
        if let Some(c) = c {
            out.push((b << 4) | (c >> 2));
            if let Some(d) = d {
                out.push((c << 6) | d);
            }
        }
        i += 4;
    }
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn protocol_err(err: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(err.to_string())
}

fn network_err(err: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Network(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listen(
        listened_ms: i64,
        duration_ms: Option<i64>,
        ended_reason: ListenEndReason,
    ) -> ListenEvent {
        ListenEvent {
            listen_id: "018f-test-listen".to_string(),
            content_id: format!("b3:{}", "a".repeat(64)),
            started_at_ms: 1_700_000_000_000,
            listened_ms,
            track_duration_ms: duration_ms,
            ended_reason,
            track: ListenTrackMetadata {
                title: "Teardrop".to_string(),
                artist_names: vec!["Massive Attack".to_string()],
                featured_artist_names: Vec::new(),
                release_title: Some("Mezzanine".to_string()),
            },
        }
    }

    #[test]
    fn finished_track_is_recorded_and_qualifies() {
        let event = listen(1_000, Some(300_000), ListenEndReason::Finished);
        assert!(event.should_record());
        assert!(event.qualifies_as_play());
    }

    #[test]
    fn interrupted_listens_under_five_seconds_are_noise() {
        let event = listen(4_999, Some(300_000), ListenEndReason::Skipped);
        assert!(!event.should_record());
        assert!(!event.qualifies_as_play());
    }

    #[test]
    fn half_duration_qualifies_for_tracks_under_eight_minutes() {
        let before = listen(149_999, Some(300_000), ListenEndReason::Skipped);
        let at = listen(150_000, Some(300_000), ListenEndReason::Skipped);
        assert!(!before.qualifies_as_play());
        assert!(at.qualifies_as_play());
    }

    #[test]
    fn qualifying_threshold_is_capped_at_four_minutes() {
        let event = listen(240_000, Some(900_000), ListenEndReason::Stopped);
        assert_eq!(event.qualifying_threshold_ms(), 240_000);
        assert!(event.qualifies_as_play());
    }

    #[test]
    fn listen_payload_round_trips_with_portable_metadata() {
        let payload = SyncOpPayload::ListenRecorded {
            event: listen(150_000, Some(300_000), ListenEndReason::Replaced),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains(r#""kind":"listen_recorded""#));
        assert_eq!(
            serde_json::from_str::<SyncOpPayload>(&json).unwrap(),
            payload
        );
    }

    #[test]
    fn v1_peers_do_not_support_listening_history() {
        assert!(!supports_listen_history(1));
        assert!(supports_listen_history(2));
    }

    mod podcasts {
        use super::*;
        use crate::playback::Command;
        use crate::podcast::{FeedUrl, MAX_SNAPSHOT_SHARES};

        fn feed() -> FeedUrl {
            FeedUrl::parse("https://private.example.test/feed?token=s3cr3t").unwrap()
        }

        fn podcast_id() -> PodcastId {
            PodcastId::from_feed_url(&feed())
        }

        fn episode_id() -> EpisodeId {
            EpisodeId::from_guid(&podcast_id(), "ep-1").unwrap()
        }

        fn episode() -> PodcastEpisodeRef {
            PodcastEpisodeRef {
                podcast_id: podcast_id(),
                episode_id: episode_id(),
                feed_url: feed(),
                podcast_title: "Secret Show".to_string(),
                episode_title: "Private Episode".to_string(),
                audio_url: "https://cdn.example.test/private.mp3?sig=xyz".to_string(),
                mime: Some("audio/mpeg".to_string()),
                duration_ms: Some(3_600_000),
                published_at_ms: Some(1_700_000_000_000),
                season: Some(1),
                episode_number: Some(3),
                image_url: None,
            }
        }

        fn music(id: i64) -> PlaybackTrack {
            PlaybackTrack {
                id,
                title: format!("Song {id}"),
                track_number: Some(1),
                disc_number: None,
                duration_seconds: 200.0,
                artist_names: vec!["Artist".to_string()],
                featured_artist_names: Vec::new(),
                release_id: 7,
                release_title: "Album".to_string(),
                release_year: Some(2020),
                file_path: String::new(),
                content_id: Some(format!("b3:{}", "a".repeat(64))),
                audio_format: None,
                audio_bitrate: None,
                audio_sample_rate: None,
                audio_bit_depth: None,
                file_size_bytes: None,
                play_count: 0,
                fed: None,
                source: None,
            }
        }

        fn state(queue: Vec<PlaybackTrack>) -> PlaybackStateWire {
            PlaybackStateWire {
                queue,
                queue_pos: 1,
                playing: true,
                paused: false,
                idle_since_ms: None,
                position_secs: 42.5,
                volume: 80,
                shuffle: false,
                repeat: PlaybackRepeat::Off,
                playback_rate: None,
            }
        }

        fn mixed() -> PlaybackStateWire {
            state(vec![
                music(1),
                PlaybackTrack::podcast_episode(episode()),
                music(2),
            ])
        }

        fn op(seq: i64, payload: SyncOpPayload) -> SyncOpWire {
            SyncOpWire {
                op_id: format!("dev-a:{seq}"),
                origin_device_id: "dev-a".to_string(),
                seq,
                hlc_ms: 1_000 + seq,
                payload,
            }
        }

        fn session() -> ListenSession {
            ListenSession {
                session_id: "s-1".to_string(),
                episode_id: episode_id(),
                podcast_id: podcast_id(),
                device_id: "dev-a".to_string(),
                started_at_ms: 10,
                ended_at_ms: 20,
                start_position_ms: 0,
                end_position_ms: 60_000,
                listened_ms: 60_000,
                completed: false,
            }
        }

        fn podcast_ops() -> Vec<SyncOpPayload> {
            vec![
                SyncOpPayload::PodcastSubscriptionSet {
                    podcast_id: podcast_id(),
                    subscribed: true,
                    podcast: PodcastMeta {
                        feed_url: feed(),
                        title: "Secret Show".to_string(),
                        author: None,
                        description: Some("About things".to_string()),
                        link: None,
                        image_url: None,
                    },
                    feed_aliases: vec![feed()],
                },
                SyncOpPayload::PodcastShareSet {
                    podcast_id: podcast_id(),
                    shared: false,
                },
                SyncOpPayload::PodcastProgressSet {
                    episode_id: episode_id(),
                    podcast_id: podcast_id(),
                    position_ms: 120_000,
                    status: ProgressStatus::InProgress,
                    generation: 3,
                    session_id: "s-1".to_string(),
                    episode: Some(episode()),
                },
                SyncOpPayload::PodcastListenRecorded {
                    session: session(),
                    episode: None,
                },
            ]
        }

        #[test]
        fn podcast_ops_round_trip_and_materialize() {
            let kinds = [
                "podcast_subscription_set",
                "podcast_share_set",
                "podcast_progress_set",
                "podcast_listen_recorded",
            ];
            for (seq, (payload, kind)) in podcast_ops().into_iter().zip(kinds).enumerate() {
                let op = op(seq as i64 + 1, payload);
                let json = serde_json::to_string(&op).unwrap();
                assert!(json.contains(&format!(r#""kind":"{kind}""#)), "{json}");
                let back: SyncOpWire = serde_json::from_str(&json).unwrap();
                assert_eq!(back, op);
                assert!(op.payload.is_podcast());
                op.validate_podcast().unwrap();
            }
            let ops: Vec<SyncOpWire> = podcast_ops()
                .into_iter()
                .enumerate()
                .map(|(i, payload)| op(i as i64 + 1, payload))
                .collect();
            let subscription = ops[0].podcast_subscription().unwrap();
            assert_eq!(
                (subscription.hlc_ms, subscription.op_id.as_str()),
                (1_001, "dev-a:1")
            );
            assert!(subscription.subscribed);
            let share = ops[1].podcast_share().unwrap();
            assert!(!share.shared);
            let progress = ops[2].podcast_progress().unwrap();
            assert_eq!(progress.device_id, "dev-a");
            assert_eq!(progress.op_id, "dev-a:3");
            assert_eq!(progress.generation, 3);
            assert_eq!(ops[3].podcast_listen_session(), Some(&session()));
            assert!(ops[0].podcast_share().is_none());
        }

        #[test]
        fn tombstone_rules() {
            let ops = podcast_ops();
            let unsubscribe = SyncOpPayload::PodcastSubscriptionSet {
                podcast_id: podcast_id(),
                subscribed: false,
                podcast: match &ops[0] {
                    SyncOpPayload::PodcastSubscriptionSet { podcast, .. } => podcast.clone(),
                    _ => unreachable!(),
                },
                feed_aliases: Vec::new(),
            };
            assert!(!unsubscribe.is_tombstone(), "unsubscribe is LWW state");
            assert!(ops[1].is_tombstone(), "share=false is a tombstone");
            let share = SyncOpPayload::PodcastShareSet {
                podcast_id: podcast_id(),
                shared: true,
            };
            assert!(!share.is_tombstone());
            assert!(!ops[2].is_tombstone());
            assert!(!ops[3].is_tombstone());
        }

        #[test]
        fn invalid_podcast_ops_are_detected() {
            let mut foreign = session();
            foreign.device_id = "dev-b".to_string();
            let op1 = op(
                1,
                SyncOpPayload::PodcastListenRecorded {
                    session: foreign,
                    episode: None,
                },
            );
            assert!(op1.validate_podcast().is_err());
            let mut wrong = episode();
            wrong.episode_id = EpisodeId::from_guid(&podcast_id(), "other").unwrap();
            let op2 = op(
                2,
                SyncOpPayload::PodcastProgressSet {
                    episode_id: episode_id(),
                    podcast_id: podcast_id(),
                    position_ms: 1,
                    status: ProgressStatus::InProgress,
                    generation: 1,
                    session_id: "s".to_string(),
                    episode: Some(wrong),
                },
            );
            assert!(op2.validate_podcast().is_err());
            let op3 = op(
                3,
                SyncOpPayload::PodcastProgressSet {
                    episode_id: episode_id(),
                    podcast_id: podcast_id(),
                    position_ms: -5,
                    status: ProgressStatus::InProgress,
                    generation: 1,
                    session_id: "s".to_string(),
                    episode: None,
                },
            );
            assert!(op3.validate_podcast().is_err());
            // Music operations are not affected.
            let like = op(
                4,
                SyncOpPayload::TrackLikeSet {
                    content_id: "b3:x".to_string(),
                    liked: true,
                    fed: None,
                },
            );
            like.validate_podcast().unwrap();
        }

        #[test]
        fn legacy_v3_json_still_parses() {
            let track = r#"{"id":5,"title":"T","track_number":null,"disc_number":null,
                "duration_seconds":1.0,"release_id":1,"release_title":"R","release_year":null,
                "content_id":null,"audio_format":null,"audio_bitrate":null,
                "audio_sample_rate":null,"audio_bit_depth":null,"file_size_bytes":null}"#;
            let track: PlaybackTrack = serde_json::from_str(track).unwrap();
            assert_eq!(track.source, None);
            let state = r#"{"queue":[],"playing":false,"paused":true,"position_secs":0.0,
                "shuffle":false,"repeat":"off"}"#;
            let state: PlaybackStateWire = serde_json::from_str(state).unwrap();
            assert_eq!(state.playback_rate, None);
            let profile = r#"{"kind":"device_profile_set","name":"n","client_version":"1",
                "endpoint_ticket":"t","endpoint_id":"e"}"#;
            match serde_json::from_str::<SyncOpPayload>(profile).unwrap() {
                SyncOpPayload::DeviceProfileSet {
                    protocol_version, ..
                } => {
                    assert_eq!(protocol_version, None)
                }
                other => panic!("{other:?}"),
            }
            let snapshot: SyncSnapshot =
                serde_json::from_str(r#"{"likes":[],"playlists":[]}"#).unwrap();
            assert!(!snapshot.has_podcast_state());
        }

        #[test]
        fn music_only_messages_keep_the_v3_shape() {
            let json = serde_json::to_value(state(vec![music(1)])).unwrap();
            assert!(json.get("playback_rate").is_none());
            assert!(json["queue"][0].get("source").is_none());
            let snapshot = serde_json::to_value(SyncSnapshot::default()).unwrap();
            assert!(snapshot.get("podcast_progress").is_none());
            let profile = serde_json::to_value(SyncOpPayload::DeviceProfileSet {
                name: "n".to_string(),
                client_version: "1".to_string(),
                endpoint_ticket: "t".to_string(),
                endpoint_id: "e".to_string(),
                protocol_version: None,
            })
            .unwrap();
            assert!(profile.get("protocol_version").is_none());
        }

        #[test]
        fn podcast_queue_items_round_trip() {
            let mut state = mixed();
            state.playback_rate = Some(1.5);
            let json = serde_json::to_string(&state).unwrap();
            assert!(json.contains(r#""kind":"podcast_episode""#));
            let back: PlaybackStateWire = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
            let item = &back.queue[1];
            assert_eq!(item.podcast_episode_ref(), Some(&episode()));
            assert_eq!(item.file_path, "");
            assert_eq!(item.id, 0);
            assert_eq!(item.content_id, None);
            assert_eq!(item.duration_seconds, 3_600.0);
            let explicit_music = PlaybackTrack {
                source: Some(PlaybackSource::Music),
                ..music(3)
            };
            let json = serde_json::to_string(&explicit_music).unwrap();
            assert!(json.contains(r#""source":{"kind":"music"}"#), "{json}");
            assert_eq!(
                serde_json::from_str::<PlaybackTrack>(&json).unwrap(),
                explicit_music
            );
        }

        /// The v3 operation enum, as compiled into deployed v3 clients.
        mod v3 {
            use serde::Deserialize;

            #[derive(Debug, Deserialize)]
            #[serde(tag = "kind", rename_all = "snake_case")]
            #[allow(dead_code)]
            pub enum SyncOpPayload {
                TrackLikeSet {
                    content_id: String,
                    liked: bool,
                },
                PlaylistCreated {
                    playlist_id: String,
                    title: String,
                },
                PlaylistRenamed {
                    playlist_id: String,
                    title: String,
                },
                PlaylistDeleted {
                    playlist_id: String,
                },
                PlaylistTrackAdded {
                    playlist_id: String,
                    content_id: String,
                    position: i64,
                },
                PlaylistTrackRemoved {
                    playlist_id: String,
                    content_id: String,
                },
                DeviceProfileSet {
                    name: String,
                    client_version: String,
                    endpoint_ticket: String,
                    endpoint_id: String,
                },
                DeviceTrusted {
                    target_device_id: String,
                },
                DeviceRevoked {
                    target_device_id: String,
                    target_max_seq_seen: i64,
                },
                PlaybackCommand {
                    target_device_id: String,
                },
                ListenRecorded {
                    event: serde_json::Value,
                },
            }

            #[derive(Debug, Deserialize)]
            #[allow(dead_code)]
            pub struct SyncOpWire {
                pub op_id: String,
                pub origin_device_id: String,
                pub seq: i64,
                pub hlc_ms: i64,
                pub payload: SyncOpPayload,
            }
        }

        #[test]
        fn v3_peers_cannot_parse_new_kinds_so_gating_is_mandatory() {
            let like = op(
                1,
                SyncOpPayload::TrackLikeSet {
                    content_id: "b3:x".to_string(),
                    liked: true,
                    fed: None,
                },
            );
            let profile = op(
                2,
                SyncOpPayload::DeviceProfileSet {
                    name: "n".to_string(),
                    client_version: "2".to_string(),
                    endpoint_ticket: "t".to_string(),
                    endpoint_id: "e".to_string(),
                    protocol_version: Some(DEVICE_SYNC_PROTOCOL_VERSION),
                },
            );
            // Additive fields are fine for v3.
            for compatible in [&like, &profile] {
                let json = serde_json::to_string(compatible).unwrap();
                serde_json::from_str::<v3::SyncOpWire>(&json).unwrap();
            }
            for (seq, payload) in podcast_ops().into_iter().enumerate() {
                let podcast = op(seq as i64 + 10, payload);
                let json = serde_json::to_string(&podcast).unwrap();
                assert!(serde_json::from_str::<v3::SyncOpWire>(&json).is_err());
                // One unknown op poisons the whole batch of a Hello message.
                let batch = serde_json::to_string(&vec![like.clone(), podcast.clone()]).unwrap();
                assert!(serde_json::from_str::<Vec<v3::SyncOpWire>>(&batch).is_err());
                assert!(!op_allowed_for_peer(&podcast.payload, 3));
                assert!(op_allowed_for_peer(&podcast.payload, 4));
            }
        }

        #[test]
        fn gating_boundaries() {
            assert!(!supports_podcasts(3));
            assert!(supports_podcasts(4));
            assert!(supports_podcasts(5));
            let listen = SyncOpPayload::ListenRecorded {
                event: listen(150_000, Some(300_000), ListenEndReason::Finished),
            };
            assert!(!op_allowed_for_peer(&listen, 1));
            assert!(op_allowed_for_peer(&listen, 2));
            for payload in podcast_ops() {
                for version in 0..=3 {
                    assert!(!op_allowed_for_peer(&payload, version));
                }
                assert!(op_allowed_for_peer(&payload, 4));
            }
            let like = SyncOpPayload::TrackLikeSet {
                content_id: "b3:x".to_string(),
                liked: false,
                fed: None,
            };
            assert!(op_allowed_for_peer(&like, 1));

            let command = |state| SyncOpPayload::PlaybackCommand {
                target_device_id: "dev-b".to_string(),
                command: Command::ActiveChanged {
                    active_device_id: "dev-b".to_string(),
                    active_device_name: "B".to_string(),
                    state,
                },
                authority: None,
            };
            assert!(op_allowed_for_peer(&command(state(vec![music(1)])), 3));
            assert!(!op_allowed_for_peer(&command(mixed()), 3));
            assert!(op_allowed_for_peer(&command(mixed()), 4));

            assert!(playback_allowed_for_peer(&state(vec![music(1)]), 1));
            assert!(!playback_allowed_for_peer(&mixed(), 3));
            assert!(playback_allowed_for_peer(&mixed(), 4));
            let redacted = redact_for_jam(mixed());
            assert!(!playback_allowed_for_peer(&redacted, 3));

            assert!(playback_command_allowed_from_peer(
                &state(vec![music(1)]),
                3
            ));
            assert!(!playback_command_allowed_from_peer(&mixed(), 3));
            assert!(playback_command_allowed_from_peer(&mixed(), 4));
        }

        #[test]
        fn profile_versions_are_never_substituted() {
            assert_eq!(profile_protocol_version(Some(4), Some(3)), 4);
            assert_eq!(profile_protocol_version(None, Some(3)), 3);
            assert_eq!(
                profile_protocol_version(None, None),
                UNREPORTED_PROTOCOL_VERSION
            );
            assert!(!supports_podcasts(profile_protocol_version(None, None)));
        }

        #[test]
        fn jam_redaction_hides_private_items() {
            let original = mixed();
            let redacted = redact_for_jam(original.clone());
            assert_eq!(redacted.queue.len(), 3);
            assert_eq!(redacted.queue[0], original.queue[0]);
            assert_eq!(redacted.queue[2], original.queue[2]);
            assert!(redacted.queue[1].is_private_placeholder());
            assert_eq!(redacted.queue[1].title, PRIVATE_ITEM_TITLE);
            assert_eq!(redacted.queue_pos, original.queue_pos);
            assert_eq!(redacted.position_secs, original.position_secs);
            let json = serde_json::to_string(&redacted).unwrap();
            for secret in [
                "Secret Show",
                "Private Episode",
                "private.mp3",
                "s3cr3t",
                "private.example.test",
                podcast_id().as_str(),
                episode_id().as_str(),
            ] {
                assert!(!json.contains(secret), "{secret} leaked: {json}");
            }
            assert!(json.contains(r#""source":{"kind":"private"}"#));
            // Redaction is idempotent.
            assert_eq!(redact_for_jam(redacted.clone()), redacted);
        }

        #[test]
        fn jam_commands_cannot_inject_or_drop_private_items() {
            let host = mixed();
            let set = |state| Command::SetState { state, seek: false };
            // Participants see placeholders; echoing them back is allowed
            // and maps to the host's episodes.
            let mut seen = redact_for_jam(host.clone());
            seen.paused = true;
            assert!(jam_command_allowed(&set(seen.clone())));
            let restored = restore_jam_private_items(&host, seen.clone()).unwrap();
            assert_eq!(restored.queue, host.queue);
            assert!(restored.paused);
            // Reordering music around the placeholder keeps the episode.
            let mut reordered = seen.clone();
            reordered.queue.swap(0, 2);
            let restored = restore_jam_private_items(&host, reordered).unwrap();
            assert_eq!(restored.queue[1].podcast_episode_ref(), Some(&episode()));
            // Injecting a real episode is rejected.
            let injected = state(vec![music(1), PlaybackTrack::podcast_episode(episode())]);
            assert!(!jam_command_allowed(&set(injected.clone())));
            assert!(restore_jam_private_items(&host, injected).is_none());
            // Dropping or duplicating placeholders is rejected.
            let mut dropped = seen.clone();
            dropped.queue.remove(1);
            assert!(restore_jam_private_items(&host, dropped).is_none());
            let mut duplicated = seen;
            duplicated.queue.push(PlaybackTrack::private_placeholder());
            assert!(restore_jam_private_items(&host, duplicated).is_none());
            // Music-only Jam is unaffected.
            let music_only = state(vec![music(1)]);
            assert!(jam_command_allowed(&set(music_only.clone())));
            assert_eq!(
                restore_jam_private_items(&music_only, music_only.clone()),
                Some(music_only)
            );
        }

        #[test]
        fn snapshot_sections_round_trip_and_are_gated() {
            let ops: Vec<SyncOpWire> = podcast_ops()
                .into_iter()
                .enumerate()
                .map(|(i, payload)| op(i as i64 + 1, payload))
                .collect();
            let snapshot = SyncSnapshot {
                podcast_subscriptions: vec![ops[0].podcast_subscription().unwrap()],
                podcast_shares: vec![ops[1].podcast_share().unwrap()],
                podcast_progress: vec![ops[2].podcast_progress().unwrap()],
                podcast_listen_sessions: vec![session()],
                ..SyncSnapshot::default()
            };
            let message = WireMessage::Hello {
                group_id: "g".to_string(),
                profile: DeviceProfileWire {
                    device_id: "dev-a".to_string(),
                    name: "A".to_string(),
                    client_version: "1".to_string(),
                    protocol_version: DEVICE_SYNC_PROTOCOL_VERSION,
                    endpoint_id: "e".to_string(),
                    endpoint_ticket: "t".to_string(),
                    revoked: false,
                    revoke_cutoff_seq: None,
                    updated_at_ms: 0,
                },
                devices: Vec::new(),
                vector: BTreeMap::new(),
                ops,
                snapshot: snapshot.clone(),
                playback: None,
            };
            let json = serde_json::to_string(&message).unwrap();
            assert_eq!(serde_json::from_str::<WireMessage>(&json).unwrap(), message);
            assert!(snapshot.has_podcast_state());
            assert!(!snapshot.for_peer(3).has_podcast_state());
            assert_eq!(snapshot.for_peer(4), snapshot);
        }

        #[test]
        fn snapshot_bounds_accept_the_limit_and_cut_the_next_record() {
            let shares = |count: usize| -> Vec<ShareRecord> {
                (0..count)
                    .map(|i| ShareRecord {
                        podcast_id: PodcastId::from_feed_url(
                            &FeedUrl::parse(&format!("https://a.test/{i}")).unwrap(),
                        ),
                        shared: i % 2 == 0,
                        hlc_ms: i as i64,
                        op_id: format!("dev:{i}"),
                    })
                    .collect()
            };
            let mut at = SyncSnapshot {
                podcast_shares: shares(MAX_SNAPSHOT_SHARES),
                ..SyncSnapshot::default()
            };
            assert!(at.podcast_state_within_limits());
            at.bound_podcast_state();
            assert_eq!(at.podcast_shares.len(), MAX_SNAPSHOT_SHARES);
            let mut over = SyncSnapshot {
                podcast_shares: shares(MAX_SNAPSHOT_SHARES + 1),
                ..SyncSnapshot::default()
            };
            assert!(!over.podcast_state_within_limits());
            over.bound_podcast_state();
            assert_eq!(over.podcast_shares.len(), MAX_SNAPSHOT_SHARES);
            // Newest first: the oldest record is the one cut.
            assert_eq!(over.podcast_shares[0].hlc_ms, MAX_SNAPSHOT_SHARES as i64);
            assert!(over.podcast_shares.iter().all(|share| share.hlc_ms > 0));
        }

        #[test]
        fn snapshot_byte_budget_is_enforced() {
            let big = "x".repeat(crate::podcast::MAX_DESCRIPTION_CHARS);
            let subscriptions: Vec<SubscriptionRecord> = (0..MAX_SNAPSHOT_SUBSCRIPTIONS)
                .map(|i| {
                    let url = FeedUrl::parse(&format!("https://a.test/{i}")).unwrap();
                    SubscriptionRecord {
                        podcast_id: PodcastId::from_feed_url(&url),
                        subscribed: true,
                        podcast: PodcastMeta {
                            feed_url: url,
                            title: big.chars().take(1_000).collect(),
                            author: None,
                            description: Some(big.clone()),
                            link: None,
                            image_url: None,
                        },
                        feed_aliases: Vec::new(),
                        hlc_ms: i as i64,
                        op_id: format!("dev:{i}"),
                    }
                })
                .collect();
            let mut snapshot = SyncSnapshot {
                podcast_subscriptions: subscriptions,
                podcast_listen_sessions: vec![session()],
                ..SyncSnapshot::default()
            };
            snapshot.bound_podcast_state();
            let bytes: usize = snapshot
                .podcast_subscriptions
                .iter()
                .map(|record| serde_json::to_vec(record).unwrap().len() + 1)
                .sum::<usize>()
                + snapshot
                    .podcast_listen_sessions
                    .iter()
                    .map(|record| serde_json::to_vec(record).unwrap().len() + 1)
                    .sum::<usize>();
            assert!(bytes <= MAX_PODCAST_SNAPSHOT_BYTES);
            assert!(snapshot.podcast_subscriptions.len() < MAX_SNAPSHOT_SUBSCRIPTIONS);
            assert!(!snapshot.podcast_subscriptions.is_empty());
            assert!(serde_json::to_vec(&snapshot).unwrap().len() < MAX_SYNC_LINE);
        }
    }
}
