//! LibraryItem records, DHT keys and record lifetime rules.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use federation_net::{EndpointId, NetworkId};
use serde::{Deserialize, Serialize};

use crate::error::MusicDhtError;
use crate::normalization::{normalize_name, tokenize};

/// Identifier of the peer that owns a record (its `federation-net` endpoint).
pub type PeerId = EndpointId;

/// Maximum item name length in UTF-8 bytes.
pub const MAX_ITEM_NAME_BYTES: usize = 512;
/// Maximum number of tokens a single item name may produce.
pub const MAX_TOKENS_PER_ITEM: usize = 32;
/// Maximum number of artist names carried by one release/track record.
pub const MAX_ARTISTS_PER_ITEM: usize = 16;
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

/// Kind of a library item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemKind {
    /// A musical artist.
    Artist,
    /// A release (album, EP, single, ...).
    Release,
    /// A single track.
    Track,
}

impl ItemKind {
    /// Stable lowercase name used in id derivation and APIs.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Artist => "artist",
            Self::Release => "release",
            Self::Track => "track",
        }
    }
}

impl fmt::Display for ItemKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ItemKind {
    type Err = MusicDhtError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "artist" => Ok(Self::Artist),
            "release" => Ok(Self::Release),
            "track" => Ok(Self::Track),
            _ => Err(MusicDhtError::InvalidItemName),
        }
    }
}

/// Stable identifier of one item record.
///
/// Derived deterministically from the owning peer, the item kind and an
/// application-chosen local key, so republishing the same library item always
/// yields the same id (letting higher revisions supersede older ones), while
/// two peers publishing the same name still produce distinct records.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ItemId([u8; 32]);

impl ItemId {
    /// Derives an item id:
    /// `BLAKE3("music-dht:item:" || owner || kind || ":" || local_key)`.
    pub fn derive(owner: &EndpointId, kind: ItemKind, local_key: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"music-dht:item:");
        hasher.update(owner.as_bytes());
        hasher.update(kind.as_str().as_bytes());
        hasher.update(b":");
        hasher.update(local_key.as_bytes());
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

impl fmt::Display for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_hex(&self.0, f)
    }
}

impl fmt::Debug for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ItemId(")?;
        fmt_hex(&self.0, f)?;
        write!(f, ")")
    }
}

impl FromStr for ItemId {
    type Err = MusicDhtError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::HEXLOWER_PERMISSIVE
            .decode(s.trim().as_bytes())
            .map_err(|_| MusicDhtError::ItemNotFound)?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| MusicDhtError::ItemNotFound)?;
        Ok(Self(bytes))
    }
}

/// One item record as stored by its owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LibraryItem {
    /// Stable identifier of this record.
    pub id: ItemId,
    /// Peer that created (and owns) the record.
    pub owner: PeerId,
    /// Kind of the item (artist, release or track).
    pub kind: ItemKind,
    /// Human-readable item name or title.
    pub name: String,
    /// Normalized form of the name, used for indexing.
    pub normalized_name: String,
    /// Display names of the item's artists (empty for artist records).
    /// Lets a release or track be found by its artist's name.
    pub artist_names: Vec<String>,
    /// Release/track year, when known.
    pub year: Option<i32>,
    /// Release type (album, ep, single, ...) for release records.
    pub release_type: Option<String>,
    /// Track duration in seconds for track records.
    pub duration_seconds: Option<f64>,
    /// Monotonically increasing revision; bumped on every change.
    pub revision: u64,
    /// `true` if this record is a deletion tombstone.
    pub deleted: bool,
    /// Unix timestamp (milliseconds) of the last modification.
    pub updated_at_ms: u64,
}

impl LibraryItem {
    /// Returns the DHT keys this item is published under: the exact key of
    /// the name plus one key per unique token of the name **and of every
    /// artist name** — so a release or track is also found by searching for
    /// its artist.
    pub fn dht_keys(&self, network_id: &NetworkId) -> Vec<DhtKey> {
        let mut keys = vec![DhtKey::exact(network_id, &self.normalized_name)];
        let mut seen = std::collections::HashSet::new();
        for token in self.search_tokens() {
            if seen.insert(token.clone()) {
                keys.push(DhtKey::token(network_id, &token));
            }
        }
        keys
    }

    /// All tokens this item is searchable by: its own name plus the names of
    /// its artists.
    pub fn search_tokens(&self) -> Vec<String> {
        let mut tokens = tokenize(&self.normalized_name);
        for artist in &self.artist_names {
            tokens.extend(tokenize(&normalize_name(artist)));
        }
        tokens
    }
}

/// A replicated DHT entry: an item plus replication metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredRecord {
    /// The item record itself.
    pub item: LibraryItem,
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
    /// `BLAKE3(NetworkId || "item:exact:" || normalized_name)`.
    pub fn exact(network_id: &NetworkId, normalized_name: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(b"item:exact:");
        hasher.update(normalized_name.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// Key for single-token lookups:
    /// `BLAKE3(NetworkId || "item:token:" || token)`.
    pub fn token(network_id: &NetworkId, token: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(b"item:token:");
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

/// Validates a user-supplied item name and returns its normalized form.
pub(crate) fn validate_name(name: &str) -> Result<String, MusicDhtError> {
    if name.len() > MAX_ITEM_NAME_BYTES {
        return Err(MusicDhtError::ItemNameTooLong);
    }
    let normalized = normalize_name(name);
    if normalized.is_empty() {
        return Err(MusicDhtError::InvalidItemName);
    }
    if tokenize(&normalized).len() > MAX_TOKENS_PER_ITEM {
        return Err(MusicDhtError::ItemNameTooLong);
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
    fn item_id_is_stable_per_owner_kind_and_key() {
        let key = test_peer(7);
        let a = ItemId::derive(&key, ItemKind::Artist, "artist:42");
        let b = ItemId::derive(&key, ItemKind::Artist, "artist:42");
        assert_eq!(a, b);
        // Different local key, kind or owner -> different id.
        assert_ne!(a, ItemId::derive(&key, ItemKind::Artist, "artist:43"));
        assert_ne!(a, ItemId::derive(&key, ItemKind::Release, "artist:42"));
        assert_ne!(
            a,
            ItemId::derive(&test_peer(8), ItemKind::Artist, "artist:42")
        );
    }

    #[test]
    fn artist_id_hex_round_trip() {
        let id = ItemId::from_bytes([0xabu8; 32]);
        let parsed: ItemId = id.to_hex().parse().expect("parse");
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
        let item = LibraryItem {
            id: ItemId::from_bytes([1u8; 32]),
            owner: key,
            kind: ItemKind::Artist,
            name: "Attack Attack".into(),
            normalized_name: "attack attack".into(),
            artist_names: Vec::new(),
            year: None,
            release_type: None,
            duration_seconds: None,
            revision: 1,
            deleted: false,
            updated_at_ms: 0,
        };
        let net = NetworkId::from_name("test");
        let keys = item.dht_keys(&net);
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
            Err(MusicDhtError::InvalidItemName)
        ));
        assert!(matches!(
            validate_name(&"x".repeat(MAX_ITEM_NAME_BYTES + 1)),
            Err(MusicDhtError::ItemNameTooLong)
        ));
        let many_tokens = (0..MAX_TOKENS_PER_ITEM + 1)
            .map(|i| format!("t{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(matches!(
            validate_name(&many_tokens),
            Err(MusicDhtError::ItemNameTooLong)
        ));
    }
}
