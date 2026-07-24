//! Wire protocol shared by furumi personal-device synchronization clients.
//!
//! The storage and UI policy live in applications. This module only defines
//! the stable JSON-lines messages spoken over the auxiliary iroh stream ALPN,
//! plus small helpers for opaque `frid://i/...` invites.

use std::collections::BTreeMap;
use std::str::FromStr;

use federation_net::{ByteStream, NetworkId, PeerTicket, SecretKey};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{MusicDhtError, Result};

/// Auxiliary ALPN used for personal-device sync streams.
pub const SYNC_ALPN: &[u8] = b"furumi/sync/1";

/// Version of the personal-device sync wire protocol.
pub const DEVICE_SYNC_PROTOCOL_VERSION: u16 = 1;

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
    /// Wire protocol version.
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

/// Portable playback queue track.
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
}

/// Playback status broadcast by a device during sync.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaybackSnapshot {
    /// Device id.
    pub device_id: String,
    /// Device name.
    pub device_name: String,
    /// Whether this device considers itself active.
    pub active: bool,
    /// Update timestamp in unix milliseconds.
    pub updated_at_ms: i64,
    /// Playback state.
    pub state: PlaybackStateWire,
}

/// Playback command delivered through the sync op log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlaybackCommand {
    /// Replace target state. `seek` means the target should also seek audio.
    SetState {
        /// New state.
        state: PlaybackStateWire,
        /// Whether to seek the audio sink to `state.position_secs`.
        #[serde(default)]
        seek: bool,
    },
    /// Another device became active.
    ActiveChanged {
        /// New active device id.
        active_device_id: String,
        /// New active device name.
        active_device_name: String,
        /// Transferred playback state.
        state: PlaybackStateWire,
    },
}

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
    },
}

impl SyncOpPayload {
    /// Returns true for operations that remove state and may eventually compact.
    pub fn is_tombstone(&self) -> bool {
        matches!(
            self,
            SyncOpPayload::TrackLikeSet { liked: false, .. }
                | SyncOpPayload::PlaylistDeleted { .. }
                | SyncOpPayload::PlaylistTrackRemoved { .. }
                | SyncOpPayload::DeviceRevoked { .. }
        )
    }
}

/// Materialized sync snapshot sent with every handshake.
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
