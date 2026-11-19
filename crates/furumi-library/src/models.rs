//! Data shapes the views render. They mirror what the furumusic API used to
//! return, but every field is now filled from the local SQLite library.

use music_dht::podcast::{EpisodeId, PodcastEpisodeRef};
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

/// A playable queue item: a library track, a pending federated track or a
/// podcast episode.
///
/// Library tracks have `id >= 0`. Items that are not library rows use
/// negative ids, so code that only handles library tracks can keep skipping
/// `id < 0` (and `release_id < 0`, artist `id < 0`):
///
/// * pending federated tracks count down from -1;
/// * podcast episodes ([`TrackItem::podcast_episode`]) use
///   [`TrackItem::podcast_track_id`], a per-episode id below
///   [`PODCAST_TRACK_ID_MAX`], with `release_id` and artist ids -1.
///
/// Podcast episodes carry their [`PodcastEpisodeRef`] in
/// [`TrackItem::podcast`]: `file_path` is empty, `content_id`, `fed` and
/// `cover_path` are `None`, and the display fields mirror the reference
/// (episode title, podcast title as artist and release). The audio URL is
/// only in the reference: it is never a local path, never content-addressed
/// and never offered for P2P transfer.
///
/// **Music-only code paths must check [`TrackItem::is_podcast`]** (or
/// `podcast.is_none()`) before treating an item as music: likes, playlists,
/// listening history, content-id lookups, federation downloads, similarity
/// and file operations do not apply to episodes. The negative ids are a
/// fail-safe for code that predates podcasts, not a replacement for that
/// check. Library queries never return podcast items; the library database
/// stores none.
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
    /// Set for podcast episodes (see the type documentation); `None` for music.
    pub podcast: Option<PodcastEpisodeRef>,
}

/// Every [`TrackItem::podcast_track_id`] is at most this value
/// (`i64::MIN + 2^62 - 1`), far below the ids of pending federated tracks,
/// which count down from -1.
pub const PODCAST_TRACK_ID_MAX: i64 = i64::MIN + (1 << 62) - 1;

impl TrackItem {
    /// The negative `TrackItem::id` of a podcast episode.
    ///
    /// Derived from the episode id (the low 62 bits of the first 8 bytes of
    /// its digest), so the same episode always gets the same id and different
    /// episodes practically never share one. Always `<=`
    /// [`PODCAST_TRACK_ID_MAX`]; never persisted.
    pub fn podcast_track_id(episode_id: &EpisodeId) -> i64 {
        let digest = episode_id.digest();
        let mut head = [0u8; 8];
        head.copy_from_slice(&digest[..8]);
        let low = u64::from_be_bytes(head) & ((1u64 << 62) - 1);
        i64::MIN + low as i64
    }

    /// Queue item for a podcast episode streamed from the publisher.
    ///
    /// Display fields mirror `PlaybackTrack::podcast_episode` of
    /// `music_dht::device_sync` (the episode title, the podcast title as
    /// artist and release, the announced duration; no track or disc number)
    /// and add the publication year. Season and episode number stay in the
    /// reference. `id` is [`TrackItem::podcast_track_id`]; `release_id` and
    /// the artist id are -1, like pending federated tracks.
    pub fn podcast_episode(episode: PodcastEpisodeRef) -> Self {
        Self {
            id: Self::podcast_track_id(&episode.episode_id),
            title: episode.episode_title.clone(),
            track_number: None,
            disc_number: None,
            duration_seconds: episode
                .duration_ms
                .map_or(0.0, |ms| ms.max(0) as f64 / 1_000.0),
            artists: vec![ArtistRef {
                id: -1,
                name: episode.podcast_title.clone(),
            }],
            featured_artists: Vec::new(),
            release_id: -1,
            release_title: episode.podcast_title.clone(),
            release_year: episode.published_at_ms.map(year_of_unix_ms),
            file_path: String::new(),
            content_id: None,
            cover_path: None,
            audio_format: None,
            audio_bitrate: None,
            audio_sample_rate: None,
            audio_bit_depth: None,
            file_size_bytes: None,
            play_count: 0,
            fed: None,
            podcast: Some(episode),
        }
    }

    /// Whether this item is a podcast episode (not music).
    pub fn is_podcast(&self) -> bool {
        self.podcast.is_some()
    }

    /// The episode reference of a podcast item.
    pub fn podcast_episode_ref(&self) -> Option<&PodcastEpisodeRef> {
        self.podcast.as_ref()
    }

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

/// Proleptic Gregorian year of a unix timestamp in milliseconds (UTC).
fn year_of_unix_ms(ms: i64) -> i32 {
    // Howard Hinnant's civil_from_days.
    let days = ms.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    i32::try_from(year).unwrap_or(if year < 0 { i32::MIN } else { i32::MAX })
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
            podcast: None,
        };

        assert_eq!(track.artist_line(), "Main feat. Guest, Other");
        assert!(!track.is_podcast());
    }

    #[test]
    fn year_of_unix_ms_handles_epoch_and_leap_years() {
        assert_eq!(year_of_unix_ms(0), 1970);
        assert_eq!(year_of_unix_ms(-1), 1969);
        // 2024-02-29T12:00:00Z and 2024-12-31T23:59:59Z.
        assert_eq!(year_of_unix_ms(1_709_208_000_000), 2024);
        assert_eq!(year_of_unix_ms(1_735_689_599_000), 2024);
        assert_eq!(year_of_unix_ms(1_735_689_600_000), 2025);
    }

    #[test]
    fn podcast_track_mirrors_the_episode_reference() {
        use music_dht::podcast::{FeedUrl, PodcastId};
        let feed_url = FeedUrl::parse("https://example.test/feed.xml").unwrap();
        let podcast_id = PodcastId::from_feed_url(&feed_url);
        let episode = PodcastEpisodeRef {
            episode_id: EpisodeId::from_guid(&podcast_id, "e1").unwrap(),
            podcast_id,
            feed_url,
            podcast_title: "Show".into(),
            episode_title: "Pilot".into(),
            audio_url: "https://cdn.example.test/1.mp3".into(),
            mime: Some("audio/mpeg".into()),
            duration_ms: Some(90_500),
            published_at_ms: Some(1_709_208_000_000),
            season: Some(2),
            episode_number: Some(7),
            image_url: None,
            progress_generation: None,
        };
        let track = TrackItem::podcast_episode(episode.clone());
        assert!(track.is_podcast());
        assert_eq!(track.podcast_episode_ref(), Some(&episode));
        // Fail-safe ids: never a library row, release or artist, stable per
        // episode, distinct between episodes and below pending federated ids.
        assert!(track.id <= PODCAST_TRACK_ID_MAX);
        const { assert!(PODCAST_TRACK_ID_MAX < -(1 << 61)) };
        assert_eq!(track.id, TrackItem::podcast_track_id(&episode.episode_id));
        assert_eq!(TrackItem::podcast_episode(episode.clone()).id, track.id);
        assert_eq!(track.release_id, -1);
        assert!(track.artists.iter().all(|artist| artist.id == -1));
        let other = EpisodeId::from_guid(&episode.podcast_id, "e2").unwrap();
        assert_ne!(TrackItem::podcast_track_id(&other), track.id);
        assert!(TrackItem::podcast_track_id(&other) <= PODCAST_TRACK_ID_MAX);
        assert!(track.file_path.is_empty());
        assert!(track.content_id.is_none() && track.fed.is_none());
        assert!(!track.is_fed_pending());
        assert_eq!(track.title, "Pilot");
        assert_eq!(track.artist_line(), "Show");
        assert_eq!(track.release_title, "Show");
        assert_eq!(track.release_year, Some(2024));
        assert_eq!(track.duration_label(), "1:31");
    }
}
