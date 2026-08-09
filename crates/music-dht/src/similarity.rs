//! Shared wire contract for federated music-similarity queries.
//!
//! This module deliberately does not define how embeddings are generated,
//! stored, or searched. Applications provide that policy and implementation;
//! Frid only supplies compatible, bounded messages over an authenticated byte
//! stream.

use federation_net::ByteStream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{MusicDhtError, Result};

/// ALPN of the Furumi similarity-search stream protocol.
pub const SIMILARITY_ALPN: &[u8] = b"furumi-fd/similarity/1";
/// Current similarity-search wire version.
pub const SIMILARITY_PROTOCOL_VERSION: u16 = 1;
/// Maximum serialized request size.
pub const MAX_SIMILARITY_REQUEST_BYTES: usize = 96 * 1024;
/// Maximum serialized response size.
pub const MAX_SIMILARITY_RESPONSE_BYTES: usize = 256 * 1024;
/// Maximum embedding dimensions accepted from a peer.
pub const MAX_SIMILARITY_DIMENSIONS: usize = 4096;
/// Maximum results in one peer response.
pub const MAX_SIMILARITY_RESULTS: usize = 50;
/// Maximum UTF-8 bytes in a model/profile compatibility fingerprint.
pub const MAX_SIMILARITY_PROFILE_BYTES: usize = 128;
/// Maximum main or featured artists attached to one hit.
pub const MAX_SIMILARITY_ARTISTS: usize = 32;
/// Maximum UTF-8 bytes in one user-visible metadata field.
pub const MAX_SIMILARITY_TEXT_BYTES: usize = 1024;
/// Size of the compact SimHash used to suppress near-duplicate results.
pub const SIMILARITY_SIGNATURE_BYTES: usize = 16;

/// Normalized embedding query sent to a peer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SimilarityRequest {
    /// Wire protocol version.
    pub version: u16,
    /// Exact model-artifact and preprocessing fingerprint.
    pub profile_id: String,
    /// Number of values in `vector`.
    pub dimensions: usize,
    /// L2-normalized query embedding.
    pub vector: Vec<f32>,
    /// Maximum desired result count.
    pub limit: usize,
}

impl SimilarityRequest {
    /// Builds and validates a request for the current wire version.
    pub fn new(profile_id: impl Into<String>, vector: Vec<f32>, limit: usize) -> Result<Self> {
        let request = Self {
            version: SIMILARITY_PROTOCOL_VERSION,
            profile_id: profile_id.into(),
            dimensions: vector.len(),
            vector,
            limit,
        };
        request.validate()?;
        Ok(request)
    }

    /// Validates compatibility, input bounds, and vector normalization.
    pub fn validate(&self) -> Result<()> {
        if self.version != SIMILARITY_PROTOCOL_VERSION {
            return Err(protocol_error(format!(
                "unsupported similarity protocol {}",
                self.version
            )));
        }
        if self.profile_id.is_empty() || self.profile_id.len() > MAX_SIMILARITY_PROFILE_BYTES {
            return Err(protocol_error("invalid similarity profile id"));
        }
        if self.dimensions == 0
            || self.dimensions > MAX_SIMILARITY_DIMENSIONS
            || self.vector.len() != self.dimensions
        {
            return Err(protocol_error("invalid similarity vector dimensions"));
        }
        if !(1..=MAX_SIMILARITY_RESULTS).contains(&self.limit) {
            return Err(protocol_error("invalid similarity result limit"));
        }
        if !self.vector.iter().all(|value| value.is_finite()) {
            return Err(protocol_error(
                "similarity vector contains a non-finite value",
            ));
        }
        let norm = self
            .vector
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        if !norm.is_finite() || (norm - 1.0).abs() > 0.05 {
            return Err(protocol_error("similarity vector is not L2-normalized"));
        }
        Ok(())
    }
}

/// One track returned by a similarity provider.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SimilarityHit {
    /// Cosine similarity in the inclusive `[-1, 1]` range.
    pub score: f32,
    /// Peer-local DHT track item identifier.
    pub item_id: String,
    /// Display track title.
    pub title: String,
    /// Main artist names.
    pub artist_names: Vec<String>,
    /// Featured artist names.
    pub featured_artist_names: Vec<String>,
    /// Release year when known.
    pub year: Option<i32>,
    /// Rounded duration in seconds when known.
    pub duration_seconds: Option<i64>,
    /// Stable audio content identifier when known.
    pub content_id: Option<String>,
    /// Release title when known.
    pub release_title: Option<String>,
    /// Track number when known.
    pub track_number: Option<i32>,
    /// Disc number when known.
    pub disc_number: Option<i32>,
    /// Compact, model-neutral SimHash of the normalized embedding.
    ///
    /// Older peers may omit it. It is a ranking hint, not a track identity.
    pub embedding_signature: Option<[u8; SIMILARITY_SIGNATURE_BYTES]>,
}

impl SimilarityHit {
    /// Validates one untrusted result and its metadata bounds.
    pub fn validate(&self) -> Result<()> {
        if !self.score.is_finite() || !(-1.001..=1.001).contains(&self.score) {
            return Err(protocol_error("invalid similarity score"));
        }
        if self.item_id.is_empty() || self.item_id.len() > 128 {
            return Err(protocol_error("invalid similarity item id"));
        }
        validate_text(&self.title, false)?;
        validate_names(&self.artist_names)?;
        validate_names(&self.featured_artist_names)?;
        if let Some(content_id) = &self.content_id {
            validate_text(content_id, true)?;
        }
        if let Some(release_title) = &self.release_title {
            validate_text(release_title, true)?;
        }
        Ok(())
    }
}

/// Bounded response returned by a similarity provider.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SimilarityResponse {
    /// Whether the query was accepted.
    pub ok: bool,
    /// Human-readable refusal reason when `ok` is false.
    pub error: Option<String>,
    /// Ranked track matches.
    pub hits: Vec<SimilarityHit>,
}

impl SimilarityResponse {
    /// Creates a successful, validated response.
    pub fn success(hits: Vec<SimilarityHit>) -> Result<Self> {
        let response = Self {
            ok: true,
            error: None,
            hits,
        };
        response.validate()?;
        Ok(response)
    }

    /// Creates a bounded refusal response.
    pub fn refused(error: impl Into<String>) -> Result<Self> {
        let response = Self {
            ok: false,
            error: Some(error.into()),
            hits: Vec::new(),
        };
        response.validate()?;
        Ok(response)
    }

    /// Validates response consistency and every untrusted result.
    pub fn validate(&self) -> Result<()> {
        if self.hits.len() > MAX_SIMILARITY_RESULTS {
            return Err(protocol_error("too many similarity results"));
        }
        if self.ok {
            if self.error.is_some() {
                return Err(protocol_error(
                    "successful similarity response has an error",
                ));
            }
        } else {
            let error = self
                .error
                .as_deref()
                .ok_or_else(|| protocol_error("similarity refusal has no reason"))?;
            validate_text(error, false)?;
            if !self.hits.is_empty() {
                return Err(protocol_error("similarity refusal contains results"));
            }
        }
        for hit in &self.hits {
            hit.validate()?;
        }
        Ok(())
    }
}

/// Writes one validated request without closing the send stream.
pub async fn write_request(stream: &mut ByteStream, request: &SimilarityRequest) -> Result<()> {
    request.validate()?;
    write_json(stream, request, MAX_SIMILARITY_REQUEST_BYTES).await
}

/// Reads and validates one request after the sender closes its stream.
pub async fn read_request(stream: &mut ByteStream) -> Result<SimilarityRequest> {
    read_request_from(&mut stream.recv).await
}

/// Reads and validates one request from an arbitrary async reader.
pub async fn read_request_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<SimilarityRequest> {
    let request: SimilarityRequest = read_json(reader, MAX_SIMILARITY_REQUEST_BYTES).await?;
    request.validate()?;
    Ok(request)
}

/// Writes one validated response without closing the send stream.
pub async fn write_response(stream: &mut ByteStream, response: &SimilarityResponse) -> Result<()> {
    response.validate()?;
    write_json(stream, response, MAX_SIMILARITY_RESPONSE_BYTES).await
}

/// Reads and validates one response after the sender closes its stream.
pub async fn read_response(stream: &mut ByteStream) -> Result<SimilarityResponse> {
    read_response_from(&mut stream.recv).await
}

/// Reads and validates one response from an arbitrary async reader.
pub async fn read_response_from<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<SimilarityResponse> {
    let response: SimilarityResponse = read_json(reader, MAX_SIMILARITY_RESPONSE_BYTES).await?;
    response.validate()?;
    Ok(response)
}

/// Performs one request/response exchange on an already authenticated stream.
pub async fn exchange(
    stream: &mut ByteStream,
    request: &SimilarityRequest,
) -> Result<SimilarityResponse> {
    write_request(stream, request).await?;
    stream.send.finish().map_err(network_error)?;
    read_response(stream).await
}

/// Produces the stable compact signature carried with a similarity hit.
///
/// Hamming distance between signatures approximates angular distance between
/// normalized embeddings without returning every result vector to the caller.
pub fn embedding_signature(vector: &[f32]) -> Result<[u8; SIMILARITY_SIGNATURE_BYTES]> {
    if vector.is_empty()
        || vector.len() > MAX_SIMILARITY_DIMENSIONS
        || !vector.iter().all(|value| value.is_finite())
    {
        return Err(protocol_error("invalid similarity vector for signature"));
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if !norm.is_finite() || (norm - 1.0).abs() > 0.05 {
        return Err(protocol_error("similarity vector is not L2-normalized"));
    }

    let mut projections = [0.0f32; SIMILARITY_SIGNATURE_BYTES * 8];
    for (dimension, value) in vector.iter().copied().enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"frid-similarity-simhash-v1");
        hasher.update(&(dimension as u64).to_le_bytes());
        let random_signs = hasher.finalize();
        for (bit, projection) in projections.iter_mut().enumerate() {
            let positive = random_signs.as_bytes()[bit / 8] & (1 << (bit % 8)) != 0;
            *projection += if positive { value } else { -value };
        }
    }

    let mut signature = [0u8; SIMILARITY_SIGNATURE_BYTES];
    for (bit, projection) in projections.into_iter().enumerate() {
        if projection >= 0.0 {
            signature[bit / 8] |= 1 << (bit % 8);
        }
    }
    Ok(signature)
}

/// Returns the bit distance between two compact embedding signatures.
pub fn signature_distance(
    left: &[u8; SIMILARITY_SIGNATURE_BYTES],
    right: &[u8; SIMILARITY_SIGNATURE_BYTES],
) -> u32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| (left ^ right).count_ones())
        .sum()
}

async fn write_json<T: Serialize>(stream: &mut ByteStream, value: &T, max: usize) -> Result<()> {
    let payload = serde_json::to_vec(value).map_err(protocol_error)?;
    if payload.len() > max {
        return Err(protocol_error("similarity message is too large"));
    }
    stream.send.write_all(&payload).await.map_err(network_error)
}

async fn read_json<R, T>(reader: &mut R, max: usize) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut payload = Vec::new();
    reader
        .take(max as u64 + 1)
        .read_to_end(&mut payload)
        .await
        .map_err(network_error)?;
    if payload.len() > max {
        return Err(protocol_error("similarity message is too large"));
    }
    if payload.is_empty() {
        return Err(protocol_error("similarity message is empty"));
    }
    serde_json::from_slice(&payload).map_err(protocol_error)
}

fn validate_names(names: &[String]) -> Result<()> {
    if names.len() > MAX_SIMILARITY_ARTISTS {
        return Err(protocol_error("too many artists in similarity result"));
    }
    for name in names {
        validate_text(name, false)?;
    }
    Ok(())
}

fn validate_text(value: &str, allow_empty: bool) -> Result<()> {
    if (!allow_empty && value.trim().is_empty()) || value.len() > MAX_SIMILARITY_TEXT_BYTES {
        return Err(protocol_error("invalid similarity metadata bounds"));
    }
    Ok(())
}

fn protocol_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(error.to_string())
}

fn network_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Network(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> SimilarityRequest {
        SimilarityRequest::new("sim1:test", vec![0.5; 4], 10).unwrap()
    }

    fn hit() -> SimilarityHit {
        SimilarityHit {
            score: 0.75,
            item_id: "00aa".to_string(),
            title: "Track".to_string(),
            artist_names: vec!["Artist".to_string()],
            ..SimilarityHit::default()
        }
    }

    #[test]
    fn request_accepts_normalized_bounded_vector() {
        request().validate().unwrap();
    }

    #[test]
    fn request_rejects_incompatible_and_oversized_inputs() {
        let mut invalid = request();
        invalid.version += 1;
        assert!(invalid.validate().is_err());

        let mut invalid = request();
        invalid.vector.push(0.0);
        assert!(invalid.validate().is_err());

        let mut invalid = request();
        invalid.profile_id = "x".repeat(MAX_SIMILARITY_PROFILE_BYTES + 1);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn request_accepts_limits_and_rejects_the_first_value_beyond_them() {
        let component = 1.0 / (MAX_SIMILARITY_DIMENSIONS as f32).sqrt();
        let at_dimension_limit = SimilarityRequest::new(
            "x".repeat(MAX_SIMILARITY_PROFILE_BYTES),
            vec![component; MAX_SIMILARITY_DIMENSIONS],
            MAX_SIMILARITY_RESULTS,
        )
        .unwrap();
        at_dimension_limit.validate().unwrap();

        let over_dimensions = vec![component; MAX_SIMILARITY_DIMENSIONS + 1];
        assert!(SimilarityRequest::new("profile", over_dimensions, 1).is_err());
        assert!(SimilarityRequest::new("profile", vec![1.0], MAX_SIMILARITY_RESULTS + 1).is_err());
    }

    #[test]
    fn response_rejects_invalid_hits_and_first_result_over_limit() {
        let mut invalid = hit();
        invalid.score = 2.0;
        assert!(SimilarityResponse::success(vec![invalid]).is_err());

        let too_many = vec![hit(); MAX_SIMILARITY_RESULTS + 1];
        assert!(SimilarityResponse::success(too_many).is_err());
    }

    #[test]
    fn hit_accepts_metadata_limits_and_rejects_the_first_value_beyond_them() {
        let mut at_limit = hit();
        at_limit.title = "x".repeat(MAX_SIMILARITY_TEXT_BYTES);
        at_limit.artist_names = vec!["Artist".to_string(); MAX_SIMILARITY_ARTISTS];
        at_limit.validate().unwrap();

        let mut over_text = hit();
        over_text.title = "x".repeat(MAX_SIMILARITY_TEXT_BYTES + 1);
        assert!(over_text.validate().is_err());

        let mut over_artists = hit();
        over_artists.artist_names = vec!["Artist".to_string(); MAX_SIMILARITY_ARTISTS + 1];
        assert!(over_artists.validate().is_err());
    }

    #[tokio::test]
    async fn request_reader_enforces_the_byte_limit() {
        let payload = vec![b'x'; MAX_SIMILARITY_REQUEST_BYTES + 1];
        assert!(read_request_from(&mut payload.as_slice()).await.is_err());
    }

    #[tokio::test]
    async fn request_and_response_round_trip_as_json() {
        let request = request();
        let request_json = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            read_request_from(&mut request_json.as_slice())
                .await
                .unwrap(),
            request
        );

        let response = SimilarityResponse::success(vec![hit()]).unwrap();
        let response_json = serde_json::to_vec(&response).unwrap();
        assert_eq!(
            read_response_from(&mut response_json.as_slice())
                .await
                .unwrap(),
            response
        );
    }

    #[test]
    fn compact_signatures_are_stable_and_distance_preserving() {
        let first = vec![0.5; 4];
        let identical = embedding_signature(&first).unwrap();
        let different = embedding_signature(&[0.5, -0.5, 0.5, -0.5]).unwrap();
        assert_eq!(identical, embedding_signature(&first).unwrap());
        assert_eq!(signature_distance(&identical, &identical), 0);
        assert!(signature_distance(&identical, &different) > 0);
    }

    #[test]
    fn old_hit_without_signature_remains_compatible() {
        let json = r#"{
            "score": 0.75,
            "item_id": "00aa",
            "title": "Track",
            "artist_names": ["Artist"]
        }"#;
        let decoded: SimilarityHit = serde_json::from_str(json).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.embedding_signature, None);
    }
}
