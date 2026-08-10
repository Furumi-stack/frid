//! Deterministic, model-neutral LSH routing for federated similarity search.
//!
//! Applications still own embedding generation and exact local search. This
//! module turns normalized embeddings into compact routing signatures and
//! groups them into bounded, signed peer summaries suitable for a DHT.

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use federation_net::{EndpointId, NetworkId, PeerTicket, Signature};
use serde::{Deserialize, Serialize};

use crate::error::{MusicDhtError, Result};

/// ALPN of the schema-independent similarity-routing overlay.
pub const SIMILARITY_DHT_ALPN: &[u8] = b"furumi-fd/similarity-dht/1";
/// Current similarity-routing wire and record version.
pub const SIMILARITY_DHT_PROTOCOL_VERSION: u16 = 1;
/// Stable identifier advertised in capability manifests.
pub const SIMILARITY_DHT_ID: &str = "similarity_dht";
/// Size of the routing SimHash. It is separate from the shorter result
/// signature used only for near-duplicate filtering.
pub const ROUTING_SIGNATURE_BYTES: usize = 32;
/// Number of independent LSH tables published by every peer.
pub const ROUTING_TABLES: usize = 12;
/// Bits in the fine bucket of one table.
pub const ROUTING_BITS: usize = 20;
/// Prefix bits forming the DHT key. The remaining bits are carried compactly
/// inside the peer's signed record.
pub const ROUTING_PRIMARY_BITS: usize = 10;
/// Fine-bucket suffix bits stored inside a primary-bucket record.
pub const ROUTING_SUFFIX_BITS: usize = ROUTING_BITS - ROUTING_PRIMARY_BITS;
/// Query-side primary buckets per table: exact plus the lowest-margin
/// one-bit neighbor.
pub const ROUTING_PRIMARY_PROBES: usize = 2;
/// Query-side fine suffixes checked inside a record: exact plus eight
/// lowest-margin one-bit neighbors.
pub const ROUTING_SUFFIX_PROBES: usize = 9;
/// Maximum profile fingerprint length accepted on the routing wire.
pub const MAX_ROUTING_PROFILE_BYTES: usize = 128;
/// Maximum self-contained owner ticket accepted in a routing record.
pub const MAX_ROUTING_TICKET_BYTES: usize = 16 * 1024;
/// Maximum fine buckets in one peer summary. Ten suffix bits make this a
/// natural hard bound independent of library size.
pub const MAX_ROUTING_ENTRIES: usize = 1 << ROUTING_SUFFIX_BITS;
/// Routing records expire together with ordinary active library records.
pub const ROUTING_RECORD_TTL: Duration = crate::ACTIVE_RECORD_TTL;

const ROUTING_SIGNATURE_BITS: usize = ROUTING_SIGNATURE_BYTES * 8;
const SIGNATURE_DOMAIN: &[u8] = b"frid-similarity-simhash-v1";
const TABLE_DOMAIN: &[u8] = b"frid-similarity-lsh-table-v1\0";
const KEY_DOMAIN: &[u8] = b"frid-similarity-lsh-key-v1\0";
const RECORD_DOMAIN: &[u8] = b"frid-similarity-lsh-record-v1\0";

/// A 256-bit point in the similarity-routing DHT key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimilarityDhtKey([u8; 32]);

impl SimilarityDhtKey {
    /// Derives a network-, profile-, table- and primary-bucket-specific key.
    pub fn derive(
        network_id: &NetworkId,
        profile_id: &str,
        table: u8,
        primary_bucket: u16,
    ) -> Result<Self> {
        validate_profile(profile_id)?;
        if table as usize >= ROUTING_TABLES
            || primary_bucket as usize >= (1 << ROUTING_PRIMARY_BITS)
        {
            return Err(protocol_error("invalid similarity DHT bucket"));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(KEY_DOMAIN);
        hasher.update(network_id.as_bytes());
        hasher.update(&(profile_id.len() as u16).to_le_bytes());
        hasher.update(profile_id.as_bytes());
        hasher.update(&[table]);
        hasher.update(&primary_bucket.to_le_bytes());
        Ok(Self(*hasher.finalize().as_bytes()))
    }

    /// Creates a key from raw bytes received on the wire.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw DHT key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Query-side routing projection. Margins are intentionally local-only: they
/// choose useful neighboring probes but are never sent over the network.
#[derive(Clone)]
pub struct SimilarityRoutingQuery {
    signature: [u8; ROUTING_SIGNATURE_BYTES],
    margins: [f32; ROUTING_SIGNATURE_BITS],
}

impl std::fmt::Debug for SimilarityRoutingQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimilarityRoutingQuery")
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

impl SimilarityRoutingQuery {
    /// Compact routing signature of the normalized query embedding.
    pub fn signature(&self) -> &[u8; ROUTING_SIGNATURE_BYTES] {
        &self.signature
    }

    /// Returns the two coarse DHT buckets and nine fine suffixes to probe for
    /// one table.
    pub fn probes(&self, profile_id: &str, table: u8) -> Result<SimilarityTableProbes> {
        let positions = table_bit_positions(profile_id, table)?;
        let primary_positions = &positions[..ROUTING_PRIMARY_BITS];
        let suffix_positions = &positions[ROUTING_PRIMARY_BITS..];
        let primary = extract_bits(&self.signature, primary_positions);
        let suffix = extract_bits(&self.signature, suffix_positions);

        let mut primary_by_margin: Vec<(usize, f32)> = primary_positions
            .iter()
            .enumerate()
            .map(|(bucket_bit, signature_bit)| (bucket_bit, self.margins[*signature_bit as usize]))
            .collect();
        primary_by_margin.sort_by(|left, right| left.1.total_cmp(&right.1));
        let mut primary_buckets = vec![primary];
        for (bit, _) in primary_by_margin
            .into_iter()
            .take(ROUTING_PRIMARY_PROBES.saturating_sub(1))
        {
            primary_buckets.push(primary ^ (1 << bit));
        }

        let mut suffix_by_margin: Vec<(usize, f32)> = suffix_positions
            .iter()
            .enumerate()
            .map(|(bucket_bit, signature_bit)| (bucket_bit, self.margins[*signature_bit as usize]))
            .collect();
        suffix_by_margin.sort_by(|left, right| left.1.total_cmp(&right.1));
        let mut suffix_buckets = vec![suffix];
        for (bit, _) in suffix_by_margin
            .into_iter()
            .take(ROUTING_SUFFIX_PROBES.saturating_sub(1))
        {
            suffix_buckets.push(suffix ^ (1 << bit));
        }

        Ok(SimilarityTableProbes {
            table,
            primary_buckets,
            suffix_buckets,
        })
    }
}

/// Bounded query probes for one LSH table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimilarityTableProbes {
    /// LSH table number.
    pub table: u8,
    /// Exact and one low-margin neighboring DHT bucket.
    pub primary_buckets: Vec<u16>,
    /// Exact and eight low-margin fine buckets checked locally.
    pub suffix_buckets: Vec<u16>,
}

/// One fine bucket and its anonymous representative routing signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimilarityRouteEntry {
    /// Ten-bit suffix inside the record's primary bucket.
    pub suffix: u16,
    /// A stable representative of tracks in this peer's fine bucket.
    pub representative: [u8; ROUTING_SIGNATURE_BYTES],
}

/// Unsigned local summary built from a peer's embeddings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimilarityRouteSpec {
    /// Exact model/preprocessing compatibility fingerprint.
    pub profile_id: String,
    /// LSH table number.
    pub table: u8,
    /// Coarse ten-bit DHT bucket.
    pub primary_bucket: u16,
    /// Sorted, unique fine-bucket representatives.
    pub entries: Vec<SimilarityRouteEntry>,
}

impl SimilarityRouteSpec {
    /// Derives this summary's DHT key for `network_id`.
    pub fn key(&self, network_id: &NetworkId) -> Result<SimilarityDhtKey> {
        SimilarityDhtKey::derive(
            network_id,
            &self.profile_id,
            self.table,
            self.primary_bucket,
        )
    }
}

/// Signed immutable payload owned by one peer. Forwarders cannot change its
/// profile, buckets, representatives or issue time without invalidating the
/// Ed25519 identity signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimilarityRoutePayload {
    /// Record format version.
    pub version: u16,
    /// Network scope; prevents replay into another federation.
    pub network_id: NetworkId,
    /// Peer that owns and signed the summary.
    pub owner: EndpointId,
    /// Self-contained route to the owner. This is required because a lookup
    /// may discover a peer that is absent from the caller's routing table.
    pub owner_ticket: String,
    /// Exact model/preprocessing compatibility fingerprint.
    pub profile_id: String,
    /// LSH table number.
    pub table: u8,
    /// Coarse ten-bit DHT bucket.
    pub primary_bucket: u16,
    /// Sorted, unique fine-bucket representatives.
    pub entries: Vec<SimilarityRouteEntry>,
    /// Unix time at which the owner issued this version.
    pub issued_at_ms: u64,
}

impl SimilarityRoutePayload {
    /// Builds a payload from a validated local summary.
    pub fn from_spec(
        spec: SimilarityRouteSpec,
        network_id: NetworkId,
        owner: EndpointId,
        owner_ticket: String,
        issued_at_ms: u64,
    ) -> Result<Self> {
        let payload = Self {
            version: SIMILARITY_DHT_PROTOCOL_VERSION,
            network_id,
            owner,
            owner_ticket,
            profile_id: spec.profile_id,
            table: spec.table,
            primary_bucket: spec.primary_bucket,
            entries: spec.entries,
            issued_at_ms,
        };
        payload.validate(&payload.network_id)?;
        Ok(payload)
    }

    /// Derives the DHT key this payload must be stored under.
    pub fn key(&self) -> Result<SimilarityDhtKey> {
        SimilarityDhtKey::derive(
            &self.network_id,
            &self.profile_id,
            self.table,
            self.primary_bucket,
        )
    }

    /// Validates all untrusted bounds and the expected network scope.
    pub fn validate(&self, expected_network: &NetworkId) -> Result<()> {
        if self.version != SIMILARITY_DHT_PROTOCOL_VERSION {
            return Err(protocol_error(
                "unsupported similarity route record version",
            ));
        }
        if &self.network_id != expected_network {
            return Err(protocol_error(
                "similarity route belongs to another network",
            ));
        }
        if self.owner_ticket.len() > MAX_ROUTING_TICKET_BYTES {
            return Err(protocol_error("invalid similarity route owner ticket"));
        }
        let ticket = self
            .owner_ticket
            .parse::<PeerTicket>()
            .map_err(|_| protocol_error("invalid similarity route owner ticket"))?;
        if ticket.endpoint_id() != self.owner || &ticket.network_id != expected_network {
            return Err(protocol_error("similarity route owner ticket mismatch"));
        }
        validate_profile(&self.profile_id)?;
        if self.table as usize >= ROUTING_TABLES
            || self.primary_bucket as usize >= (1 << ROUTING_PRIMARY_BITS)
            || self.entries.is_empty()
            || self.entries.len() > MAX_ROUTING_ENTRIES
            || self.issued_at_ms == 0
        {
            return Err(protocol_error("invalid similarity route bounds"));
        }
        let mut previous = None;
        for entry in &self.entries {
            if entry.suffix as usize >= (1 << ROUTING_SUFFIX_BITS)
                || previous.is_some_and(|value| value >= entry.suffix)
            {
                return Err(protocol_error("invalid similarity route entries"));
            }
            previous = Some(entry.suffix);
        }
        Ok(())
    }
}

/// End-to-end signed routing summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedSimilarityRoute {
    /// Immutable owner payload.
    pub payload: SimilarityRoutePayload,
    /// Ed25519 signature made by `payload.owner`.
    pub signature: Signature,
}

impl SignedSimilarityRoute {
    /// Signs a validated payload using an identity-backed signer.
    pub fn sign(
        payload: SimilarityRoutePayload,
        signer: impl FnOnce(&[u8]) -> Signature,
    ) -> Result<Self> {
        payload.validate(&payload.network_id)?;
        let bytes = signing_bytes(&payload)?;
        Ok(Self {
            payload,
            signature: signer(&bytes),
        })
    }

    /// Verifies payload bounds, network scope and the owner's signature.
    pub fn verify(&self, expected_network: &NetworkId) -> Result<()> {
        self.payload.validate(expected_network)?;
        let bytes = signing_bytes(&self.payload)?;
        self.payload
            .owner
            .verify(&bytes, &self.signature)
            .map_err(|_| protocol_error("invalid similarity route signature"))
    }

    /// DHT key under which this record must be stored.
    pub fn key(&self) -> Result<SimilarityDhtKey> {
        self.payload.key()
    }
}

/// Computes the stable 256-bit routing signature of a normalized embedding.
pub fn routing_signature(vector: &[f32]) -> Result<[u8; ROUTING_SIGNATURE_BYTES]> {
    Ok(routing_query(vector)?.signature)
}

/// Computes a routing signature and local projection margins for multi-probe
/// lookup. No model-specific constants or calibration data are used.
pub fn routing_query(vector: &[f32]) -> Result<SimilarityRoutingQuery> {
    validate_vector(vector)?;
    let signs = hyperplane_signs();
    let mut projections = [0.0f32; ROUTING_SIGNATURE_BITS];
    for (dimension, value) in vector.iter().copied().enumerate() {
        for (projection, sign) in projections.iter_mut().zip(&signs[dimension]) {
            *projection += value * *sign;
        }
    }
    let mut signature = [0u8; ROUTING_SIGNATURE_BYTES];
    let mut margins = [0.0f32; ROUTING_SIGNATURE_BITS];
    for (bit, projection) in projections.into_iter().enumerate() {
        if projection >= 0.0 {
            signature[bit / 8] |= 1 << (bit % 8);
        }
        margins[bit] = projection.abs();
    }
    Ok(SimilarityRoutingQuery { signature, margins })
}

/// Builds one bounded peer summary per occupied `(table, primary bucket)`.
/// Multiple tracks in the same fine bucket collapse to one deterministic
/// representative, so a large library cannot flood a hot DHT key.
pub fn build_route_specs(
    profile_id: &str,
    signatures: &[[u8; ROUTING_SIGNATURE_BYTES]],
) -> Result<Vec<SimilarityRouteSpec>> {
    validate_profile(profile_id)?;
    if signatures.is_empty() {
        return Ok(Vec::new());
    }
    let positions = (0..ROUTING_TABLES)
        .map(|table| table_bit_positions(profile_id, table as u8))
        .collect::<Result<Vec<_>>>()?;
    let mut groups: BTreeMap<(u8, u16), BTreeMap<u16, [u8; ROUTING_SIGNATURE_BYTES]>> =
        BTreeMap::new();
    for signature in signatures {
        for (table, table_positions) in positions.iter().enumerate() {
            let primary = extract_bits(signature, &table_positions[..ROUTING_PRIMARY_BITS]);
            let suffix = extract_bits(signature, &table_positions[ROUTING_PRIMARY_BITS..]);
            groups
                .entry((table as u8, primary))
                .or_default()
                .entry(suffix)
                .and_modify(|representative| {
                    if signature < representative {
                        *representative = *signature;
                    }
                })
                .or_insert(*signature);
        }
    }
    Ok(groups
        .into_iter()
        .map(|((table, primary_bucket), entries)| SimilarityRouteSpec {
            profile_id: profile_id.to_string(),
            table,
            primary_bucket,
            entries: entries
                .into_iter()
                .map(|(suffix, representative)| SimilarityRouteEntry {
                    suffix,
                    representative,
                })
                .collect(),
        })
        .collect())
}

/// Hamming distance between two routing signatures.
pub fn routing_signature_distance(
    left: &[u8; ROUTING_SIGNATURE_BYTES],
    right: &[u8; ROUTING_SIGNATURE_BYTES],
) -> u32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| (left ^ right).count_ones())
        .sum()
}

fn hyperplane_signs() -> &'static Vec<[f32; ROUTING_SIGNATURE_BITS]> {
    static SIGNS: OnceLock<Vec<[f32; ROUTING_SIGNATURE_BITS]>> = OnceLock::new();
    SIGNS.get_or_init(|| {
        (0..crate::similarity::MAX_SIMILARITY_DIMENSIONS)
            .map(|dimension| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(SIGNATURE_DOMAIN);
                hasher.update(&(dimension as u64).to_le_bytes());
                let digest = hasher.finalize();
                std::array::from_fn(|bit| {
                    if digest.as_bytes()[bit / 8] & (1 << (bit % 8)) != 0 {
                        1.0
                    } else {
                        -1.0
                    }
                })
            })
            .collect()
    })
}

fn table_bit_positions(profile_id: &str, table: u8) -> Result<[u8; ROUTING_BITS]> {
    validate_profile(profile_id)?;
    if table as usize >= ROUTING_TABLES {
        return Err(protocol_error("invalid similarity routing table"));
    }
    let mut selected = Vec::with_capacity(ROUTING_BITS);
    let mut seen = HashSet::with_capacity(ROUTING_BITS);
    let mut counter = 0u32;
    while selected.len() < ROUTING_BITS {
        let mut hasher = blake3::Hasher::new();
        hasher.update(TABLE_DOMAIN);
        hasher.update(profile_id.as_bytes());
        hasher.update(&(table as u16).to_le_bytes());
        hasher.update(&counter.to_le_bytes());
        counter += 1;
        for candidate in hasher.finalize().as_bytes() {
            if seen.insert(*candidate) {
                selected.push(*candidate);
                if selected.len() == ROUTING_BITS {
                    break;
                }
            }
        }
    }
    selected
        .try_into()
        .map_err(|_| protocol_error("failed to derive similarity routing table"))
}

fn extract_bits(signature: &[u8; ROUTING_SIGNATURE_BYTES], positions: &[u8]) -> u16 {
    positions
        .iter()
        .enumerate()
        .fold(0u16, |value, (bucket_bit, signature_bit)| {
            let bit = (signature[*signature_bit as usize / 8] >> (*signature_bit as usize % 8)) & 1;
            value | (u16::from(bit) << bucket_bit)
        })
}

fn signing_bytes(payload: &SimilarityRoutePayload) -> Result<Vec<u8>> {
    let encoded = postcard::to_stdvec(payload).map_err(protocol_error)?;
    let mut bytes = Vec::with_capacity(RECORD_DOMAIN.len() + encoded.len());
    bytes.extend_from_slice(RECORD_DOMAIN);
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

fn validate_profile(profile_id: &str) -> Result<()> {
    if profile_id.is_empty() || profile_id.len() > MAX_ROUTING_PROFILE_BYTES {
        Err(protocol_error("invalid similarity routing profile id"))
    } else {
        Ok(())
    }
}

fn validate_vector(vector: &[f32]) -> Result<()> {
    if vector.is_empty()
        || vector.len() > crate::similarity::MAX_SIMILARITY_DIMENSIONS
        || !vector.iter().all(|value| value.is_finite())
    {
        return Err(protocol_error("invalid vector for similarity routing"));
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if !norm.is_finite() || (norm - 1.0).abs() > 0.05 {
        return Err(protocol_error(
            "similarity routing vector is not L2-normalized",
        ));
    }
    Ok(())
}

fn protocol_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(error.to_string())
}

#[cfg(test)]
mod tests {
    use federation_net::{
        EndpointAddr, PROTOCOL_VERSION, PeerTicket, SchemaId, SecretKey, TICKET_VERSION,
    };

    use super::*;

    fn normalized(values: &mut [f32]) {
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        for value in values {
            *value /= norm;
        }
    }

    fn ticket(network: NetworkId, owner: EndpointId) -> String {
        PeerTicket {
            ticket_version: TICKET_VERSION,
            protocol_version: PROTOCOL_VERSION,
            network_id: network,
            schema_id: SchemaId::from_name("similarity-test"),
            endpoint_addr: EndpointAddr::new(owner),
        }
        .to_string()
    }

    #[test]
    fn routing_signature_is_stable_and_extends_result_simhash() {
        let mut vector = vec![0.0; 64];
        for (index, value) in vector.iter_mut().enumerate() {
            *value = index as f32 - 31.5;
        }
        normalized(&mut vector);
        let routing = routing_signature(&vector).unwrap();
        let compact = crate::similarity::embedding_signature(&vector).unwrap();
        assert_eq!(&routing[..compact.len()], &compact);
        assert_eq!(routing, routing_signature(&vector).unwrap());
    }

    #[test]
    fn route_specs_are_bounded_sorted_and_deterministic() {
        let signatures = [[0u8; 32], [0xff; 32], [0u8; 32]];
        let first = build_route_specs("sim1:test", &signatures).unwrap();
        let second = build_route_specs("sim1:test", &signatures).unwrap();
        assert_eq!(first, second);
        assert!(!first.is_empty());
        assert!(first.len() <= ROUTING_TABLES * signatures.len());
        assert!(first.iter().all(|spec| {
            !spec.entries.is_empty()
                && spec.entries.len() <= MAX_ROUTING_ENTRIES
                && spec
                    .entries
                    .windows(2)
                    .all(|pair| pair[0].suffix < pair[1].suffix)
        }));
    }

    #[test]
    fn signed_route_cannot_be_forged_or_replayed_between_networks() {
        let network = NetworkId::from_name("a");
        let other = NetworkId::from_name("b");
        let key = SecretKey::from_bytes(&[7; 32]);
        let spec = build_route_specs("sim1:test", &[[0x55; 32]])
            .unwrap()
            .remove(0);
        let payload = SimilarityRoutePayload::from_spec(
            spec,
            network,
            key.public(),
            ticket(network, key.public()),
            42,
        )
        .unwrap();
        let signed = SignedSimilarityRoute::sign(payload, |bytes| key.sign(bytes)).unwrap();
        signed.verify(&network).unwrap();
        assert!(signed.verify(&other).is_err());

        let mut forged = signed.clone();
        forged.payload.entries[0].representative[0] ^= 1;
        assert!(forged.verify(&network).is_err());
    }

    #[test]
    fn route_payload_rejects_unbounded_wire_fields() {
        let network = NetworkId::from_name("bounds");
        let owner = SecretKey::from_bytes(&[6; 32]).public();
        let spec = build_route_specs("sim1:test", &[[0x33; 32]])
            .unwrap()
            .remove(0);
        let payload =
            SimilarityRoutePayload::from_spec(spec, network, owner, ticket(network, owner), 1)
                .unwrap();

        let mut oversized_profile = payload.clone();
        oversized_profile.profile_id = "x".repeat(MAX_ROUTING_PROFILE_BYTES + 1);
        assert!(oversized_profile.validate(&network).is_err());

        let mut oversized_entries = payload;
        oversized_entries.entries = (0..=MAX_ROUTING_ENTRIES)
            .map(|suffix| SimilarityRouteEntry {
                suffix: suffix as u16,
                representative: [0; ROUTING_SIGNATURE_BYTES],
            })
            .collect();
        assert!(oversized_entries.validate(&network).is_err());

        let mut mismatched_ticket = oversized_profile;
        mismatched_ticket.profile_id = "sim1:test".into();
        mismatched_ticket.owner_ticket = ticket(network, SecretKey::from_bytes(&[5; 32]).public());
        assert!(mismatched_ticket.validate(&network).is_err());
    }

    #[test]
    fn query_probes_are_bounded_and_include_exact_bucket() {
        let query = routing_query(&[0.5; 4]).unwrap();
        let probes = query.probes("sim1:test", 0).unwrap();
        assert_eq!(probes.primary_buckets.len(), ROUTING_PRIMARY_PROBES);
        assert_eq!(probes.suffix_buckets.len(), ROUTING_SUFFIX_PROBES);
        assert!(
            probes
                .primary_buckets
                .iter()
                .all(|bucket| *bucket < (1 << ROUTING_PRIMARY_BITS))
        );
        assert!(
            probes
                .suffix_buckets
                .iter()
                .all(|bucket| *bucket < (1 << ROUTING_SUFFIX_BITS))
        );
    }
}
