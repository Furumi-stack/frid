//! Artist records, DHT keys and record lifetime rules.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use federation_net::{EndpointId, NetworkId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ArtistDhtError;
use crate::normalization::{normalize_artist_name, tokenize};

/// Identifier of the peer that owns a record (its `federation-net` endpoint).
pub type PeerId = EndpointId;

/// Maximum artist name length in UTF-8 bytes.
pub const MAX_ARTIST_NAME_BYTES: usize = 512;
/// Maximum number of tokens a single artist name may produce.
pub const MAX_TOKENS_PER_ARTIST: usize = 32;
/// Maximum lifetime of an active DHT record.
pub const ACTIVE_RECORD_TTL: Duration = Duration::from_secs(30 * 60);
/// Maximum lifetime of a tombstone.
pub const TOMBSTONE_TTL: Duration = Duration::from_secs(2 * 60 * 60);

fn fmt_hex(bytes: &[u8; 32], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

/// Stable identifier of one artist record.
///
/// Derived deterministically from the owning peer and a fresh UUID, so two
/// peers adding the same name produce two distinct records.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArtistId([u8; 32]);

impl ArtistId {
    /// Derives an artist id: `BLAKE3("artist-dht:artist:" || owner || uuid)`.
    pub fn derive(owner: &EndpointId, uuid: &Uuid) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"artist-dht:artist:");
        hasher.update(owner.as_bytes());
        hasher.update(uuid.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// Creates an id from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw bytes of this id.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Renders the id as lowercase hex.
    pub fn to_hex(&self) -> String {
        data_encoding::HEXLOWER.encode(&self.0)
    }
}

impl fmt::Display for ArtistId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_hex(&self.0, f)
    }
}

impl fmt::Debug for ArtistId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArtistId(")?;
        fmt_hex(&self.0, f)?;
        write!(f, ")")
    }
}

impl FromStr for ArtistId {
    type Err = ArtistDhtError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::HEXLOWER_PERMISSIVE
            .decode(s.trim().as_bytes())
            .map_err(|_| ArtistDhtError::ArtistNotFound)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ArtistDhtError::ArtistNotFound)?;
        Ok(Self(bytes))
    }
}

/// One artist record as stored by its owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artist {
    /// Stable identifier of this record.
    pub id: ArtistId,
    /// Peer that created (and owns) the record.
    pub owner: PeerId,
    /// Human-readable artist name as entered by the user.
    pub name: String,
    /// Normalized form of the name, used for indexing.
    pub normalized_name: String,
    /// Monotonically increasing revision; bumped on every change.
    pub revision: u64,
    /// `true` if this record is a deletion tombstone.
    pub deleted: bool,
    /// Unix timestamp (milliseconds) of the last modification.
    pub updated_at_ms: u64,
}

impl Artist {
    /// Returns the DHT keys this artist is published under: the exact key
    /// plus one key per unique token.
    pub fn dht_keys(&self, network_id: &NetworkId) -> Vec<DhtKey> {
        let mut keys = vec![DhtKey::exact(network_id, &self.normalized_name)];
        let mut seen = std::collections::HashSet::new();
        for token in tokenize(&self.normalized_name) {
            if seen.insert(token.clone()) {
                keys.push(DhtKey::token(network_id, &token));
            }
        }
        keys
    }
}

/// A replicated DHT entry: an artist plus replication metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredArtistRecord {
    /// The artist record itself.
    pub artist: Artist,
    /// Peer that pushed this replica. Not cryptographically verified in the
    /// PoC: the connection authenticates the direct sender, not the origin
    /// of a replicated record.
    pub publisher: PeerId,
    /// Unix timestamp (milliseconds) after which the replica must be dropped.
    pub expires_at_ms: u64,
}

/// A 256-bit DHT key. Lives in the same key space as [`crate::NodeId`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DhtKey([u8; 32]);

impl DhtKey {
    /// Key for exact-name lookups:
    /// `BLAKE3(NetworkId || "artist:exact:" || normalized_name)`.
    pub fn exact(network_id: &NetworkId, normalized_name: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(b"artist:exact:");
        hasher.update(normalized_name.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// Key for single-token lookups:
    /// `BLAKE3(NetworkId || "artist:token:" || token)`.
    pub fn token(network_id: &NetworkId, token: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(b"artist:token:");
        hasher.update(token.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// Creates a key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw bytes of this key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for DhtKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_hex(&self.0, f)
    }
}

impl fmt::Debug for DhtKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DhtKey(")?;
        fmt_hex(&self.0, f)?;
        write!(f, ")")
    }
}

/// Returns the current Unix time in milliseconds.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Validates a user-supplied artist name and returns its normalized form.
pub(crate) fn validate_name(name: &str) -> Result<String, ArtistDhtError> {
    if name.len() > MAX_ARTIST_NAME_BYTES {
        return Err(ArtistDhtError::ArtistNameTooLong);
    }
    let normalized = normalize_artist_name(name);
    if normalized.is_empty() {
        return Err(ArtistDhtError::InvalidArtistName);
    }
    if tokenize(&normalized).len() > MAX_TOKENS_PER_ARTIST {
        return Err(ArtistDhtError::ArtistNameTooLong);
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn artist_id_is_deterministic() {
        let key = test_peer(7);
        let uuid = Uuid::from_u128(42);
        let a = ArtistId::derive(&key, &uuid);
        let b = ArtistId::derive(&key, &uuid);
        assert_eq!(a, b);
        let other = ArtistId::derive(&key, &Uuid::from_u128(43));
        assert_ne!(a, other);
    }

    #[test]
    fn artist_id_hex_round_trip() {
        let id = ArtistId::from_bytes([0xabu8; 32]);
        let parsed: ArtistId = id.to_hex().parse().expect("parse");
        assert_eq!(parsed, id);
    }

    #[test]
    fn exact_key_is_deterministic_and_distinct() {
        let net = NetworkId::from_name("test");
        let a = DhtKey::exact(&net, "massive attack");
        let b = DhtKey::exact(&net, "massive attack");
        assert_eq!(a, b);
        assert_ne!(a, DhtKey::exact(&net, "portishead"));
        // A different network yields different keys for the same name.
        let other_net = NetworkId::from_name("other");
        assert_ne!(a, DhtKey::exact(&other_net, "massive attack"));
    }

    #[test]
    fn token_key_is_deterministic_and_distinct_from_exact() {
        let net = NetworkId::from_name("test");
        let token = DhtKey::token(&net, "massive");
        assert_eq!(token, DhtKey::token(&net, "massive"));
        assert_ne!(token, DhtKey::exact(&net, "massive"));
    }

    #[test]
    fn dht_keys_cover_exact_and_unique_tokens() {
        let key = test_peer(7);
        let artist = Artist {
            id: ArtistId::from_bytes([1u8; 32]),
            owner: key,
            name: "Attack Attack".into(),
            normalized_name: "attack attack".into(),
            revision: 1,
            deleted: false,
            updated_at_ms: 0,
        };
        let net = NetworkId::from_name("test");
        let keys = artist.dht_keys(&net);
        // One exact key + one deduplicated token key.
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], DhtKey::exact(&net, "attack attack"));
        assert_eq!(keys[1], DhtKey::token(&net, "attack"));
    }

    #[test]
    fn name_validation() {
        assert!(validate_name("Massive Attack").is_ok());
        assert!(matches!(
            validate_name("!!!"),
            Err(ArtistDhtError::InvalidArtistName)
        ));
        assert!(matches!(
            validate_name(&"x".repeat(MAX_ARTIST_NAME_BYTES + 1)),
            Err(ArtistDhtError::ArtistNameTooLong)
        ));
        let many_tokens = (0..MAX_TOKENS_PER_ARTIST + 1)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(matches!(
            validate_name(&many_tokens),
            Err(ArtistDhtError::ArtistNameTooLong)
        ));
    }
}
