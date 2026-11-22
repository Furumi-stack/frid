//! Optional content-addressed lyrics, independent of audio/catalog wire layouts.
//!
//! One bounded JSON request and response per stream; writers finish their send
//! half after writing. Unsupported peers simply lack this ALPN/capability.
use crate::error::{MusicDhtError, Result};
use federation_net::ByteStream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Optional lyrics stream ALPN.
pub const LYRICS_ALPN: &[u8] = b"furumi/lyrics/1";
/// Version of the lyrics envelope.
pub const LYRICS_PROTOCOL_VERSION: u16 = 1;
/// Capability identifier; applications advertise it only when serving lyrics.
pub const LYRICS_ID: &str = "lyrics";
/// Maximum number of alternative texts returned for one content identity.
pub const MAX_LYRICS_VERSIONS: usize = 16;
/// Maximum UTF-8 bytes per plain or timed text.
pub const MAX_LYRICS_TEXT_BYTES: usize = 256 * 1024;
/// Maximum JSON response, including escaping overhead.
pub const MAX_LYRICS_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum request bytes.
pub const MAX_LYRICS_REQUEST_BYTES: usize = 1024;

/// Fetch all available versions for these exact audio bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsRequest {
    /// Envelope version.
    pub version: u16,
    /// Canonical `b3:<hex>` audio content identity, never a title match.
    pub content_id: String,
}
impl LyricsRequest {
    /// Constructs a validated request.
    pub fn new(content_id: impl Into<String>) -> Result<Self> {
        let request = Self {
            version: LYRICS_PROTOCOL_VERSION,
            content_id: content_id.into(),
        };
        request.validate()?;
        Ok(request)
    }
    /// Rejects unsupported versions and noncanonical identities.
    pub fn validate(&self) -> Result<()> {
        if self.version != LYRICS_PROTOCOL_VERSION
            || crate::normalize_content_id(&self.content_id).as_ref() != Some(&self.content_id)
        {
            return Err(invalid(
                "unsupported lyrics version or invalid content identity",
            ));
        }
        Ok(())
    }
}

/// One alternative text; timed and plain representations belong together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsVersion {
    /// Opaque identity scoped to this response's content identity.
    pub id: String,
    /// Human-readable version label.
    pub label: String,
    /// Provenance, for example `lrclib` or `manual`.
    pub source: String,
    /// Stable provider record id, when available.
    pub source_id: Option<String>,
    /// Optional BCP-47 language tag.
    pub language: Option<String>,
    /// Text without synchronization.
    pub plain_text: Option<String>,
    /// LRC text with line timestamps, including optional offset metadata.
    pub synced_lrc: Option<String>,
    /// Explicitly identified as instrumental.
    pub instrumental: bool,
}
impl LyricsVersion {
    /// Validates text bounds and requires actual text or an instrumental marker.
    pub fn validate(&self) -> Result<()> {
        for text in [&self.id, &self.label, &self.source] {
            if text.trim().is_empty() || text.len() > 512 {
                return Err(invalid("invalid lyrics metadata"));
            }
        }
        for text in [&self.source_id, &self.language].into_iter().flatten() {
            if text.len() > 512 {
                return Err(invalid("lyrics metadata too large"));
            }
        }
        for text in [&self.plain_text, &self.synced_lrc].into_iter().flatten() {
            if text.len() > MAX_LYRICS_TEXT_BYTES {
                return Err(invalid("lyrics text too large"));
            }
        }
        let has_text = [&self.plain_text, &self.synced_lrc]
            .into_iter()
            .flatten()
            .any(|v| !v.trim().is_empty());
        if (!has_text && !self.instrumental) || (has_text && self.instrumental) {
            return Err(invalid("lyrics must contain text or be instrumental"));
        }
        Ok(())
    }
}

/// Versions ordered best first (timed before plain); empty means unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsResponse {
    /// Envelope version.
    pub version: u16,
    /// Echo of the requested audio identity.
    pub content_id: String,
    /// Available alternatives, best first.
    pub versions: Vec<LyricsVersion>,
}
impl LyricsResponse {
    /// Validates envelope, content identity, version count and each text.
    pub fn validate(&self) -> Result<()> {
        LyricsRequest {
            version: self.version,
            content_id: self.content_id.clone(),
        }
        .validate()?;
        if self.versions.len() > MAX_LYRICS_VERSIONS {
            return Err(invalid("too many lyrics versions"));
        }
        let mut ids = std::collections::HashSet::new();
        for version in &self.versions {
            version.validate()?;
            if !ids.insert(&version.id) {
                return Err(invalid("duplicate lyrics version"));
            }
        }
        Ok(())
    }
}
/// Writes a request. Caller finishes the send half afterwards.
pub async fn write_request(stream: &mut ByteStream, value: &LyricsRequest) -> Result<()> {
    value.validate()?;
    write_json(stream, value, MAX_LYRICS_REQUEST_BYTES).await
}
/// Writes a response. Caller finishes the send half afterwards.
pub async fn write_response(stream: &mut ByteStream, value: &LyricsResponse) -> Result<()> {
    value.validate()?;
    write_json(stream, value, MAX_LYRICS_RESPONSE_BYTES).await
}
/// Reads a request through EOF with a strict byte bound.
pub async fn read_request_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<LyricsRequest> {
    let value: LyricsRequest = read_json(reader, MAX_LYRICS_REQUEST_BYTES).await?;
    value.validate()?;
    Ok(value)
}
/// Reads a response through EOF with strict byte and semantic bounds.
pub async fn read_response_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<LyricsResponse> {
    let value: LyricsResponse = read_json(reader, MAX_LYRICS_RESPONSE_BYTES).await?;
    value.validate()?;
    Ok(value)
}
async fn read_json<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
    max: usize,
) -> Result<T> {
    let mut bytes = Vec::new();
    reader
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|e| MusicDhtError::Network(e.to_string()))?;
    if bytes.len() > max {
        return Err(invalid("lyrics frame too large"));
    }
    serde_json::from_slice(&bytes).map_err(invalid)
}
async fn write_json<T: Serialize>(stream: &mut ByteStream, value: &T, max: usize) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(invalid)?;
    if bytes.len() > max {
        return Err(invalid("lyrics frame too large"));
    }
    stream
        .send
        .write_all(&bytes)
        .await
        .map_err(|e| MusicDhtError::Network(e.to_string()))
}
fn invalid(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn version() -> LyricsVersion {
        LyricsVersion {
            id: "1".into(),
            label: "Original".into(),
            source: "manual".into(),
            source_id: None,
            language: None,
            plain_text: Some("hello".into()),
            synced_lrc: None,
            instrumental: false,
        }
    }
    fn response() -> LyricsResponse {
        LyricsResponse {
            version: 1,
            content_id: format!("b3:{}", "a".repeat(64)),
            versions: vec![version()],
        }
    }
    #[tokio::test]
    async fn roundtrip_and_reject_incompatible_version() {
        let value = response();
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            read_response_from(&mut bytes.as_slice()).await.unwrap(),
            value
        );
        let mut bad = value;
        bad.version = 2;
        assert!(bad.validate().is_err());
        assert!(LyricsRequest::new("not-a-content-id").is_err());
    }
    #[test]
    fn semantic_limits() {
        let mut value = response();
        value.versions[0].plain_text = Some("x".repeat(MAX_LYRICS_TEXT_BYTES));
        value.validate().unwrap();
        value.versions[0].plain_text.as_mut().unwrap().push('x');
        assert!(value.validate().is_err());
        let mut value = response();
        value.versions = (0..MAX_LYRICS_VERSIONS)
            .map(|i| LyricsVersion {
                id: i.to_string(),
                ..version()
            })
            .collect();
        value.validate().unwrap();
        value.versions.push(LyricsVersion {
            id: "extra".into(),
            ..version()
        });
        assert!(value.validate().is_err());
        assert!(
            LyricsVersion {
                plain_text: None,
                ..version()
            }
            .validate()
            .is_err()
        );
    }
    #[tokio::test]
    async fn bounded_frames_and_empty_results() {
        let bytes = vec![b'x'; MAX_LYRICS_REQUEST_BYTES + 1];
        assert!(read_request_from(&mut bytes.as_slice()).await.is_err());
        let value = LyricsResponse {
            versions: vec![],
            ..response()
        };
        value.validate().unwrap();
    }
}
