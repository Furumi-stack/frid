//! Data shapes the views render. They mirror what the furumusic API used to
//! return, but every field is now filled from the local SQLite library.

use music_dht::device_sync::{PlaybackSource, PlaybackTrack};
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
/// # Identity
///
/// What an item *is* comes from its explicit source fields, never from the
/// numeric `id` alone; [`TrackItem::key`] returns it as a typed
/// [`TrackItemKey`]:
///
/// * a library track is a library row: `id >= 0`, `fed` and `podcast` are
///   `None`;
/// * a federated track is identified by [`TrackItem::fed`] (owner and item
///   id). While it is not in the library its `id` is a negative,
///   process-local counter value (pending federated tracks count down from
///   -1) and `release_id` / artist ids are -1;
/// * a podcast episode is identified **only** by [`TrackItem::podcast`] (its
///   [`EpisodeId`]). Its `id`, `release_id` and artist id are the fixed
///   sentinel [`TrackItem::NOT_A_LIBRARY_ROW`] (-1), shared by every episode.
///
/// A negative `id` therefore means "not a library row" and nothing else: it
/// carries no identity and must never be decoded, compared across items or
/// used as a key. The -1 sentinel only keeps music-only code that predates
/// podcasts (`id >= 0` / `release_id < 0` checks) from treating an episode
/// as library track, release or artist 0.
///
/// [`TrackItem::key`] is the *source* identity. It does not merge a pending
/// federated item with its imported library copy, which share a content id
/// but have different keys (`fed:` and `lib:`). Clients that key playback
/// items content-first (content id, then federated source, then id) keep
/// doing so for music; what they must add is a podcast branch
/// ([`TrackItem::podcast`], or [`TrackItemKey::PodcastEpisode`]) checked
/// before any fallback to `id`. An episode must never be keyed by `id`: a
/// `local:{id}` key would merge all episodes into one. On the wire the item
/// kind is `music_dht::device_sync::PlaybackSource`; convert received items
/// with [`TrackItem::from_playback_track`], never by copying wire ids.
///
/// # Podcast episodes
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
/// and file operations do not apply to episodes. Library queries never
/// return podcast items; the library database stores none.
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

/// Typed source identity of a [`TrackItem`], from [`TrackItem::key`].
///
/// Two items with equal keys are the same track or episode; the same track
/// may still appear more than once in a queue. The key has no content-id
/// equivalence: a pending federated item and its library copy differ. Use it
/// where the source matters (and always for podcast episodes, which have no
/// other identity); code that matches music by content id keeps doing that
/// and checks the podcast variant first (see [`TrackItem`]).
///
/// The [`Display`](std::fmt::Display) form is stable and suitable as a map
/// key or in logs:
///
/// | Variant | String form |
/// | --- | --- |
/// | `Library(123)` | `lib:123` |
/// | `Federated { owner, item_id }` | `fed:<owner>:<item_id>` |
/// | `PodcastEpisode(id)` | `ep:<episode id>`, e.g. `ep:ep1:...` |
/// | `Transient(-5)` | `tmp:-5` |
///
/// `owner` is a peer endpoint id and contains no `:`, so the federated form
/// is unambiguous. Keys are not persisted by this crate; a `Transient` key is
/// only meaningful inside the process that built the item.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrackItemKey {
    /// A row of the local music library (`TrackItem::id >= 0`).
    Library(i64),
    /// A federated track that is not a library row, identified by its owner
    /// peer and DHT item id ([`FederatedTrack::owner`] /
    /// [`FederatedTrack::item_id`]), not by its process-local negative `id`.
    Federated { owner: String, item_id: String },
    /// A podcast episode, identified by its [`EpisodeId`] only.
    PodcastEpisode(EpisodeId),
    /// A non-library item with no explicit source (negative `id`, no `fed`,
    /// no `podcast`). furumi-library never builds one; clients that do must
    /// keep its `id` unique within the process.
    Transient(i64),
}

/// What a received `music_dht::device_sync::PlaybackTrack` is locally, from
/// [`TrackItem::from_playback_track`].
#[derive(Debug, Clone)]
pub enum PlaybackItem {
    /// A music track. Convert it with the client's music path (content id,
    /// then federated source); its wire ids are the sender's.
    Music,
    /// A podcast episode, built with [`TrackItem::podcast_episode`].
    PodcastEpisode(Box<TrackItem>),
    /// A Jam private placeholder: not playable and without identity. Show it
    /// as `music_dht::device_sync::PRIVATE_ITEM_TITLE` where the queue is
    /// displayed; never insert it as a library or queue track, never key it.
    Private,
}

impl std::fmt::Display for TrackItemKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Library(id) => write!(f, "lib:{id}"),
            Self::Federated { owner, item_id } => write!(f, "fed:{owner}:{item_id}"),
            Self::PodcastEpisode(episode_id) => write!(f, "ep:{}", episode_id.as_str()),
            Self::Transient(id) => write!(f, "tmp:{id}"),
        }
    }
}

impl TrackItem {
    /// The `id`, `release_id` and artist id of items that are not library
    /// rows and have no numeric identity: every podcast episode uses it.
    ///
    /// It is a sentinel, not an identifier: it is shared by all episodes
    /// (and can coincide with the first pending federated track's id), so it
    /// must never be decoded or used as a key. See [`TrackItem::key`].
    pub const NOT_A_LIBRARY_ROW: i64 = -1;

    /// The typed identity of this item; see [`TrackItemKey`].
    ///
    /// Precedence: [`TrackItem::podcast`] makes it a podcast episode; else a
    /// non-negative `id` is a library row; else [`TrackItem::fed`] makes it
    /// a federated track; anything else is [`TrackItemKey::Transient`].
    pub fn key(&self) -> TrackItemKey {
        if let Some(episode) = &self.podcast {
            return TrackItemKey::PodcastEpisode(episode.episode_id.clone());
        }
        if self.id >= 0 {
            return TrackItemKey::Library(self.id);
        }
        match &self.fed {
            Some(fed) => TrackItemKey::Federated {
                owner: fed.owner.clone(),
                item_id: fed.item_id.clone(),
            },
            None => TrackItemKey::Transient(self.id),
        }
    }

    /// Queue item for a podcast episode streamed from the publisher.
    ///
    /// Title, artist and release follow `PlaybackTrack::podcast_episode` of
    /// `music_dht::device_sync` (the episode title, the podcast title as
    /// artist and release, the announced duration; no track or disc number).
    /// The other fields differ from that wire form on purpose:
    ///
    /// * `id`, `release_id` and the artist id are the sentinel
    ///   [`TrackItem::NOT_A_LIBRARY_ROW`] (-1) for every episode, where the
    ///   wire form carries `0`; the item's identity is [`TrackItem::podcast`]
    ///   alone ([`TrackItem::key`]);
    /// * `release_year` is the publication year (`None` on the wire);
    /// * `audio_format` is `None` (the wire form carries the MIME type, which
    ///   stays in the reference).
    ///
    /// Season and episode number stay in the reference.
    pub fn podcast_episode(episode: PodcastEpisodeRef) -> Self {
        Self {
            id: Self::NOT_A_LIBRARY_ROW,
            title: episode.episode_title.clone(),
            track_number: None,
            disc_number: None,
            duration_seconds: episode
                .duration_ms
                .map_or(0.0, |ms| ms.max(0) as f64 / 1_000.0),
            artists: vec![ArtistRef {
                id: Self::NOT_A_LIBRARY_ROW,
                name: episode.podcast_title.clone(),
            }],
            featured_artists: Vec::new(),
            release_id: Self::NOT_A_LIBRARY_ROW,
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

    /// Classifies a received `music_dht::device_sync::PlaybackTrack` and
    /// builds the local queue item for non-music sources.
    ///
    /// The wire `id` and `release_id` of podcast episodes and of Jam private
    /// placeholders are `0`, and those of music items are the sender's local
    /// ids. None of them identifies anything on this device, so they are
    /// never copied:
    ///
    /// * a podcast episode becomes [`TrackItem::podcast_episode`] of its
    ///   [`PlaybackSource::PodcastEpisode`] reference (sentinel ids, key
    ///   `ep:`), whatever the legacy display fields say;
    /// * a private placeholder ([`PlaybackSource::Private`]) is not playable
    ///   and has no identity, so no `TrackItem` exists for it;
    /// * music (no source, or [`PlaybackSource::Music`]) is left to the
    ///   client's music path, which resolves it by content id or federated
    ///   source.
    pub fn from_playback_track(track: &PlaybackTrack) -> PlaybackItem {
        match &track.source {
            None | Some(PlaybackSource::Music) => PlaybackItem::Music,
            Some(PlaybackSource::PodcastEpisode(episode)) => PlaybackItem::PodcastEpisode(
                Box::new(Self::podcast_episode(episode.as_ref().clone())),
            ),
            Some(PlaybackSource::Private) => PlaybackItem::Private,
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

    fn episode_ref(guid: &str) -> PodcastEpisodeRef {
        use music_dht::podcast::{FeedUrl, PodcastId};
        let feed_url = FeedUrl::parse("https://example.test/feed.xml").unwrap();
        let podcast_id = PodcastId::from_feed_url(&feed_url);
        PodcastEpisodeRef {
            episode_id: EpisodeId::from_guid(&podcast_id, guid).unwrap(),
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
        }
    }

    fn library_track(id: i64) -> TrackItem {
        TrackItem {
            id,
            title: "Track".into(),
            track_number: None,
            disc_number: None,
            duration_seconds: 1.0,
            artists: vec![artist("Main")],
            featured_artists: Vec::new(),
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
        }
    }

    fn federated(owner: &str, item_id: &str) -> FederatedTrack {
        FederatedTrack {
            item_id: item_id.into(),
            owner: owner.into(),
            own: false,
            title: "Remote".into(),
            artist_names: vec!["Someone".into()],
            featured_artist_names: Vec::new(),
            year: None,
            duration_seconds: None,
            content_id: None,
            release_title: None,
            track_number: None,
            disc_number: None,
        }
    }

    #[test]
    fn podcast_track_mirrors_the_episode_reference() {
        let episode = episode_ref("e1");
        let track = TrackItem::podcast_episode(episode.clone());
        assert!(track.is_podcast());
        assert_eq!(track.podcast_episode_ref(), Some(&episode));
        // The sentinel marks "not a library row, release or artist" and
        // carries no identity.
        assert_eq!(TrackItem::NOT_A_LIBRARY_ROW, -1);
        assert_eq!(track.id, -1);
        assert_eq!(track.release_id, -1);
        assert!(track.artists.iter().all(|artist| artist.id == -1));
        assert!(track.featured_artists.is_empty());
        assert!(track.file_path.is_empty());
        assert!(track.content_id.is_none() && track.fed.is_none());
        assert!(!track.is_fed_pending());
        assert_eq!(track.title, "Pilot");
        assert_eq!(track.artist_line(), "Show");
        assert_eq!(track.release_title, "Show");
        assert_eq!(track.release_year, Some(2024));
        assert_eq!(track.duration_label(), "1:31");
    }

    #[test]
    fn key_is_typed_by_source() {
        let library = library_track(123);
        assert_eq!(library.key(), TrackItemKey::Library(123));
        assert_eq!(library.key().to_string(), "lib:123");
        assert_eq!(library_track(0).key(), TrackItemKey::Library(0));

        let mut pending = library_track(-5);
        pending.release_id = -1;
        pending.file_path.clear();
        pending.fed = Some(federated("peer1", "item-9"));
        let fed_key = TrackItemKey::Federated {
            owner: "peer1".into(),
            item_id: "item-9".into(),
        };
        assert_eq!(pending.key(), fed_key);
        assert_eq!(pending.key().to_string(), "fed:peer1:item-9");
        // The process-local negative id is not part of a federated identity.
        let mut renumbered = pending.clone();
        renumbered.id = -6;
        assert_eq!(renumbered.key(), pending.key());

        let episode = episode_ref("e1");
        let track = TrackItem::podcast_episode(episode.clone());
        let ep_key = TrackItemKey::PodcastEpisode(episode.episode_id.clone());
        assert_eq!(track.key(), ep_key);
        assert_eq!(
            track.key().to_string(),
            format!("ep:{}", episode.episode_id.as_str())
        );
        assert!(track.key().to_string().starts_with("ep:ep1:"));

        let transient = library_track(-7);
        assert_eq!(transient.key(), TrackItemKey::Transient(-7));
        assert_eq!(transient.key().to_string(), "tmp:-7");

        // The source wins over the numeric id: a podcast item is never keyed
        // as a library row, a federated item never by its counter value.
        let mut odd = TrackItem::podcast_episode(episode);
        odd.id = 42;
        assert_eq!(odd.key(), ep_key);
        let mut pending_minus_one = pending.clone();
        pending_minus_one.id = TrackItem::NOT_A_LIBRARY_ROW;
        assert_eq!(pending_minus_one.key(), fed_key);
        assert_ne!(pending_minus_one.key(), track.key());
    }

    #[test]
    fn received_playback_tracks_never_copy_wire_ids() {
        // F1 wire form of an episode: id 0, release_id 0.
        let episode = episode_ref("e1");
        let wire = PlaybackTrack::podcast_episode(episode.clone());
        assert_eq!((wire.id, wire.release_id), (0, 0));
        let PlaybackItem::PodcastEpisode(item) = TrackItem::from_playback_track(&wire) else {
            panic!("an episode is not music");
        };
        assert_eq!(item.id, TrackItem::NOT_A_LIBRARY_ROW);
        assert_eq!(item.release_id, TrackItem::NOT_A_LIBRARY_ROW);
        assert!(item.artists.iter().all(|artist| artist.id < 0));
        assert_eq!(
            item.key(),
            TrackItemKey::PodcastEpisode(episode.episode_id.clone())
        );
        assert_eq!(item.podcast_episode_ref(), Some(&episode));
        // Legacy display fields an older sender filled in do not matter.
        let mut odd = wire.clone();
        odd.id = 42;
        odd.release_id = 7;
        odd.title = "Something else".into();
        let PlaybackItem::PodcastEpisode(odd) = TrackItem::from_playback_track(&odd) else {
            panic!("the source decides");
        };
        assert_eq!(odd.key(), item.key());
        assert_eq!(odd.id, TrackItem::NOT_A_LIBRARY_ROW);

        // A private placeholder (also id 0) never becomes library row 0.
        let private = PlaybackTrack::private_placeholder();
        assert_eq!(private.id, 0);
        assert!(matches!(
            TrackItem::from_playback_track(&private),
            PlaybackItem::Private
        ));

        // Music is left to the client's music path.
        let mut music = wire;
        music.source = None;
        assert!(matches!(
            TrackItem::from_playback_track(&music),
            PlaybackItem::Music
        ));
        music.source = Some(PlaybackSource::Music);
        assert!(matches!(
            TrackItem::from_playback_track(&music),
            PlaybackItem::Music
        ));
    }

    #[test]
    fn episodes_share_the_sentinel_id_but_not_the_key() {
        let first = TrackItem::podcast_episode(episode_ref("e1"));
        let second = TrackItem::podcast_episode(episode_ref("e2"));
        assert_eq!(first.id, TrackItem::NOT_A_LIBRARY_ROW);
        assert_eq!(second.id, TrackItem::NOT_A_LIBRARY_ROW);
        assert_eq!(first.id, second.id);
        assert_eq!(first.release_id, second.release_id);
        assert_ne!(first.key(), second.key());
        assert_ne!(first.key().to_string(), second.key().to_string());
        assert_eq!(
            TrackItem::podcast_episode(episode_ref("e1")).key(),
            first.key()
        );
        let keys: std::collections::HashSet<TrackItemKey> =
            [first.key(), second.key(), first.key()]
                .into_iter()
                .collect();
        assert_eq!(keys.len(), 2);
        let mut ordered = [second.key(), first.key(), TrackItemKey::Library(1)];
        ordered.sort();
        assert_eq!(ordered[0], TrackItemKey::Library(1));
    }
}
