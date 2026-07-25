//! Shared wire shapes for Furumi catalog streams.
//!
//! The DHT itself only indexes compact search records. Applications can expose
//! richer, peer-to-peer catalog slices over this protocol when another peer
//! needs a browseable library view.

use serde::{Deserialize, Serialize};

/// ALPN of the Furumi catalog stream protocol.
pub const CATALOG_ALPN: &[u8] = b"furumi-fd/catalog/1";

/// Request sent by a catalog client.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogRequest {
    /// Artist name for full artist-card and image requests.
    #[serde(default)]
    pub artist: String,
    /// Requested resource kind.
    #[serde(default)]
    pub want: Option<String>,
    /// Release title for release-cover requests.
    #[serde(default)]
    pub release: Option<String>,
    /// Opaque pagination cursor for browse slices.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Desired number of entries for browse slices.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response returned by a catalog peer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogResponse {
    /// Whether the request succeeded.
    pub ok: bool,
    /// Human-readable error message when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Full artist card for `catalog` requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist: Option<CatalogArtist>,
    /// Compact artist previews for `artists` slice requests.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artists: Vec<CatalogArtistPreview>,
    /// Optional release card for future direct release requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release: Option<CatalogRelease>,
    /// Next pagination cursor for browse slices.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Compact artist entry used by paginated library browsing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogArtistPreview {
    /// Normalized artist key.
    pub artist_key: String,
    /// Display artist name.
    pub name: String,
    /// Optional image path or peer-local image handle.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_path: Option<String>,
    /// Number of releases where the artist is a primary release artist.
    pub release_count: i64,
    /// Number of tracks where the artist appears.
    pub track_count: i64,
}

/// Full artist card returned by the catalog protocol.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogArtist {
    /// Display artist name.
    pub name: String,
    /// Releases owned by this artist.
    #[serde(default)]
    pub releases: Vec<CatalogRelease>,
    /// Release appearances where this artist is not the primary artist.
    #[serde(default)]
    pub appears_on: Vec<CatalogAppearance>,
}

/// Release appearance used by full artist cards.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogAppearance {
    /// Release title.
    pub release_title: String,
    /// Release type.
    #[serde(default)]
    pub release_type: String,
    /// Release year.
    pub year: Option<i32>,
    /// Track where the artist appears.
    pub track: CatalogTrack,
}

/// Release card returned by the catalog protocol.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogRelease {
    /// Release title.
    pub title: String,
    /// Release type.
    #[serde(default)]
    pub release_type: String,
    /// Release year.
    pub year: Option<i32>,
    /// Tracks on this release.
    #[serde(default)]
    pub tracks: Vec<CatalogTrack>,
}

/// Track entry returned inside catalog cards.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogTrack {
    /// Track title.
    pub title: String,
    /// Main artists.
    #[serde(default)]
    pub artists: Vec<String>,
    /// Featured artists.
    #[serde(default)]
    pub featured_artists: Vec<String>,
    /// Track number.
    pub track_number: Option<i32>,
    /// Disc number.
    pub disc_number: Option<i32>,
    /// Track duration in seconds.
    pub duration_seconds: Option<f64>,
    /// Stable audio content id when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_id: Option<String>,
    /// Peer-local item id used by legacy fetch paths.
    pub item_id: String,
}

/// Header returned by catalog image requests before the image bytes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogImageHeader {
    /// Whether the image request succeeded.
    pub ok: bool,
    /// Human-readable error message when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// MIME type of the returned image.
    pub mime_type: String,
    /// Size of the following image payload in bytes.
    pub size: u64,
}
