//! Data shapes the views render. They mirror what the furumusic API used to
//! return, but every field is now filled from the local SQLite library.

use serde::{Deserialize, Serialize};

/// Which catalog sources a client wants library queries to include.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LibrarySourceMode {
    Local,
    My,
    #[default]
    Global,
}

impl LibrarySourceMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Local => "Local",
            Self::My => "My",
            Self::Global => "Global",
        }
    }

    pub fn includes_network(self) -> bool {
        !matches!(self, Self::Local)
    }

    pub fn includes_global_peers(self) -> bool {
        matches!(self, Self::Global)
    }

    pub fn next(self) -> Self {
        match self {
            Self::Local => Self::My,
            Self::My => Self::Global,
            Self::Global => Self::Local,
        }
    }
}

/// Client-neutral filters accepted by library browse/search operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryFilters {
    #[serde(default)]
    pub hide_featured_only: bool,
    #[serde(default)]
    pub source_mode: LibrarySourceMode,
}

/// Durable metadata for a track known through federation but not necessarily
/// materialized as a local audio file. Networking remains a client concern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederatedTrack {
    pub item_id: String,
    pub owner: String,
    pub own: bool,
    pub title: String,
    pub artist_names: Vec<String>,
    pub featured_artist_names: Vec<String>,
    pub year: Option<i32>,
    pub duration_seconds: Option<i64>,
    pub content_id: Option<String>,
    pub release_title: Option<String>,
    pub track_number: Option<i32>,
    pub disc_number: Option<i32>,
}

impl FederatedTrack {
    pub fn artist_line(&self) -> String {
        let mut line = self.artist_names.join(", ");
        if !self.featured_artist_names.is_empty() {
            if !line.is_empty() {
                line.push_str(" feat. ");
            }
            line.push_str(&self.featured_artist_names.join(", "));
        }
        line
    }

    pub fn owner_short(&self) -> String {
        self.owner.chars().take(10).collect()
    }

    pub fn duration_label(&self) -> String {
        self.duration_seconds.map_or_else(String::new, |total| {
            format!("{}:{:02}", total / 60, total % 60)
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Availability {
    #[default]
    Local,
    Mixed,
    Remote,
}

impl Availability {
    pub fn is_remoteish(self) -> bool {
        matches!(self, Availability::Mixed | Availability::Remote)
    }
}

#[derive(Debug, Clone)]
pub struct ArtistCard {
    pub id: i64,
    pub name: String,
    /// Path to a local image file, if one is set for the artist.
    pub image_path: Option<String>,
    pub release_count: i64,
    pub track_count: i64,
    pub availability: Availability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtistRef {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct TrackItem {
    pub id: i64,
    pub title: String,
    pub track_number: Option<i32>,
    pub disc_number: Option<i32>,
    pub duration_seconds: f64,
    pub artists: Vec<ArtistRef>,
    pub featured_artists: Vec<ArtistRef>,
    pub release_id: i64,
    pub release_title: String,
    pub release_year: Option<i32>,
    /// Absolute path to the local audio file.
    pub file_path: String,
    /// Stable audio content id (`b3:<64 hex>`) when known.
    pub content_id: Option<String>,
    /// Path to a local cover image (the release cover).
    pub cover_path: Option<String>,
    pub audio_format: Option<String>,
    pub audio_bitrate: Option<i32>,
    pub audio_sample_rate: Option<i32>,
    pub audio_bit_depth: Option<i32>,
    pub file_size_bytes: Option<i64>,
    /// Completed local plays, from the history table.
    pub play_count: i64,
    /// Set for federated tracks that are not in the local library (yet):
    /// carries everything needed to download them from the owning peer.
    /// With an empty `file_path` the player resolves the track on demand.
    pub fed: Option<FederatedTrack>,
}

impl TrackItem {
    /// A federated track that still needs downloading before playback.
    pub fn is_fed_pending(&self) -> bool {
        self.fed.is_some() && self.file_path.is_empty()
    }

    pub fn artist_line(&self) -> String {
        let artists = self
            .artists
            .iter()
            .map(|a| a.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let featured = self
            .featured_artists
            .iter()
            .map(|a| a.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match (artists.is_empty(), featured.is_empty()) {
            (false, false) => format!("{artists} feat. {featured}"),
            (false, true) => artists,
            (true, false) => format!("feat. {featured}"),
            (true, true) => String::new(),
        }
    }

    pub fn duration_label(&self) -> String {
        let total = self.duration_seconds.round() as i64;
        format!("{}:{:02}", total / 60, total % 60)
    }

    /// Full tech line for the status bar, including the sample rate.
    pub fn tech_label_full(&self) -> String {
        let mut parts = Vec::new();
        if let Some(format) = &self.audio_format {
            parts.push(format.to_uppercase());
        }
        if let Some(bitrate) = self.audio_bitrate {
            parts.push(format!("{bitrate}kbps"));
        }
        if let Some(rate) = self.audio_sample_rate {
            parts.push(format!("{:.1}kHz", f64::from(rate) / 1000.0));
        }
        if let Some(bytes) = self.file_size_bytes {
            parts.push(format!("{:.1}MB", bytes as f64 / 1_048_576.0));
        }
        parts.join(" · ")
    }
}

#[derive(Debug, Clone)]
pub struct ReleaseCard {
    pub id: i64,
    pub title: String,
    pub release_type: String,
    pub year: Option<i32>,
    pub cover_path: Option<String>,
    pub track_count: i64,
    pub availability: Availability,
}

#[derive(Debug)]
pub struct ArtistDetail {
    #[allow(dead_code, reason = "cache key is held by the caller")]
    pub id: i64,
    pub name: String,
    pub image_path: Option<String>,
    pub total_track_count: i64,
    pub total_play_count: i64,
    pub top_tracks: Vec<TrackItem>,
    pub releases: Vec<ReleaseCard>,
    /// Tracks where this artist is featured (the only content for artists
    /// without own releases).
    pub featured_tracks: Vec<TrackItem>,
}

#[derive(Debug)]
pub struct ReleaseDetail {
    #[allow(dead_code, reason = "cache key is held by the caller")]
    pub id: i64,
    pub title: String,
    pub release_type: String,
    pub year: Option<i32>,
    pub cover_path: Option<String>,
    pub artists: Vec<ArtistRef>,
    pub tracks: Vec<TrackItem>,
}

#[derive(Debug, Clone)]
pub struct PlaylistCard {
    pub id: i64,
    pub title: String,
    pub track_count: i64,
    /// "normal" for user playlists, "likes" for the virtual Likes playlist.
    pub kind: String,
}

#[derive(Debug)]
pub struct PlaylistDetail {
    #[allow(dead_code, reason = "cache key is held by the caller")]
    pub id: i64,
    pub title: String,
    #[allow(dead_code, reason = "shown in a detail header later")]
    pub description: Option<String>,
    pub tracks: Vec<TrackItem>,
}

#[derive(Debug, Default)]
pub struct SearchResults {
    pub artists: Vec<ArtistCard>,
    pub releases: Vec<ReleaseCard>,
    pub tracks: Vec<TrackItem>,
}

impl SearchResults {
    pub fn len(&self) -> usize {
        self.artists.len() + self.releases.len() + self.tracks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.artists.is_empty() && self.releases.is_empty() && self.tracks.is_empty()
    }
}

#[derive(Debug)]
pub struct ArtistsPage {
    pub items: Vec<ArtistCard>,
    pub total: i64,
    pub page: i64,
    pub has_more: bool,
}

/// Edited values submitted from the track edit form. `None` numbers clear
/// the column.
#[derive(Debug, Clone)]
pub struct TrackEdit {
    pub title: String,
    pub artists: Vec<String>,
    pub featured_artists: Vec<String>,
    pub track_number: Option<i32>,
    pub disc_number: Option<i32>,
    /// Cover image path; the cover lives on the track's release (the same
    /// image every view shows for the track). None clears it.
    pub cover_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReleaseEdit {
    pub title: String,
    pub release_type: String,
    pub year: Option<i32>,
    pub artists: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artist(name: &str) -> ArtistRef {
        ArtistRef {
            id: 1,
            name: name.to_string(),
        }
    }

    #[test]
    fn artist_line_formats_featured_artists() {
        let track = TrackItem {
            id: 1,
            title: "Track".into(),
            track_number: None,
            disc_number: None,
            duration_seconds: 1.0,
            artists: vec![artist("Main")],
            featured_artists: vec![artist("Guest"), artist("Other")],
            release_id: 1,
            release_title: "Release".into(),
            release_year: None,
            file_path: "/tmp/track.mp3".into(),
            content_id: None,
            cover_path: None,
            audio_format: None,
            audio_bitrate: None,
            audio_sample_rate: None,
            audio_bit_depth: None,
            file_size_bytes: None,
            play_count: 0,
            fed: None,
        };

        assert_eq!(track.artist_line(), "Main feat. Guest, Other");
    }
}
