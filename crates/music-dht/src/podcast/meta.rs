//! Podcast and episode metadata DTOs.

use serde::{Deserialize, Serialize};

use super::{
    EpisodeId, EpisodeIdentityHints, FeedUrl, MAX_DESCRIPTION_CHARS, MAX_GUID_BYTES,
    MAX_MIME_BYTES, MAX_TITLE_CHARS, MAX_URL_BYTES, PodcastError, PodcastId, PodcastResult,
    bounded_text, check_opt_text, check_opt_url, check_text, check_url,
};

/// `itunes:episodeType`.
///
/// Unknown values deserialize (and parse) as [`EpisodeType::Full`], the
/// iTunes default, so publishers cannot break the wire format.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case", from = "String")]
pub enum EpisodeType {
    /// Regular episode.
    #[default]
    Full,
    /// Trailer or teaser.
    Trailer,
    /// Bonus content.
    Bonus,
}

impl EpisodeType {
    /// Parses an `itunes:episodeType` value, case-insensitively.
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "trailer" => Self::Trailer,
            "bonus" => Self::Bonus,
            _ => Self::Full,
        }
    }
}

impl From<String> for EpisodeType {
    fn from(value: String) -> Self {
        Self::parse(&value)
    }
}

/// Channel metadata of a podcast, as last fetched from its feed.
///
/// Synced inside subscription operations so that another trusted device can
/// show and refresh the podcast without the user typing the URL again. It is
/// private user data: never publish it outside the trusted-device group.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PodcastMeta {
    /// Canonical URL the feed is currently fetched from.
    pub feed_url: FeedUrl,
    /// Podcast title.
    pub title: String,
    /// Author.
    #[serde(default)]
    pub author: Option<String>,
    /// Plain-text description, at most [`MAX_DESCRIPTION_CHARS`].
    #[serde(default)]
    pub description: Option<String>,
    /// Publisher web page.
    #[serde(default)]
    pub link: Option<String>,
    /// Artwork URL.
    #[serde(default)]
    pub image_url: Option<String>,
}

impl PodcastMeta {
    /// Validates contract bounds.
    pub fn validate(&self) -> PodcastResult<()> {
        check_text("podcast title", &self.title, MAX_TITLE_CHARS, true)?;
        check_opt_text("podcast author", self.author.as_deref(), MAX_TITLE_CHARS)?;
        check_opt_text(
            "podcast description",
            self.description.as_deref(),
            MAX_DESCRIPTION_CHARS,
        )?;
        if self
            .link
            .as_ref()
            .is_some_and(|link| link.len() > MAX_URL_BYTES)
        {
            return Err(PodcastError::InvalidField {
                field: "podcast link",
                reason: "is too long",
            });
        }
        check_opt_url("podcast image_url", self.image_url.as_deref())
    }

    /// Returns a copy that fits the contract bounds.
    ///
    /// Text is truncated with [`bounded_text`]; invalid optional URLs are
    /// dropped; an empty title becomes `"Untitled podcast"`.
    pub fn bounded(&self) -> Self {
        Self {
            feed_url: self.feed_url.clone(),
            title: bounded_title(&self.title, "Untitled podcast"),
            author: bounded_opt(self.author.as_deref(), MAX_TITLE_CHARS),
            description: bounded_opt(self.description.as_deref(), MAX_DESCRIPTION_CHARS),
            link: self
                .link
                .clone()
                .filter(|link| link.len() <= MAX_URL_BYTES && !link.trim().is_empty()),
            image_url: valid_url(self.image_url.as_deref()),
        }
    }
}

/// Full episode metadata, as stored by clients and refreshed from RSS.
///
/// Publisher metadata never touches personal progress or history: those are
/// separate records keyed by [`EpisodeId`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeMeta {
    /// Stable episode id.
    pub episode_id: EpisodeId,
    /// Owning podcast.
    pub podcast_id: PodcastId,
    /// RSS GUID / Atom id, when present.
    #[serde(default)]
    pub guid: Option<String>,
    /// Episode title.
    pub title: String,
    /// Plain-text description, at most [`MAX_DESCRIPTION_CHARS`].
    #[serde(default)]
    pub description: Option<String>,
    /// `itunes:season`. Episodes without a season form their own group.
    #[serde(default)]
    pub season: Option<u32>,
    /// `itunes:episode`.
    #[serde(default)]
    pub episode_number: Option<u32>,
    /// `itunes:episodeType`.
    #[serde(default)]
    pub episode_type: EpisodeType,
    /// Publication time in unix milliseconds.
    #[serde(default)]
    pub published_at_ms: Option<i64>,
    /// Duration announced by the publisher, in milliseconds.
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// Absolute http(s) audio URL at the publisher.
    pub audio_url: String,
    /// Enclosure MIME type.
    #[serde(default)]
    pub mime: Option<String>,
    /// Enclosure length in bytes.
    #[serde(default)]
    pub byte_len: Option<u64>,
    /// Episode artwork URL.
    #[serde(default)]
    pub image_url: Option<String>,
    /// Episode web page.
    #[serde(default)]
    pub link: Option<String>,
}

impl EpisodeMeta {
    /// Validates contract bounds.
    pub fn validate(&self) -> PodcastResult<()> {
        if self
            .guid
            .as_ref()
            .is_some_and(|guid| guid.len() > MAX_GUID_BYTES)
        {
            return Err(PodcastError::InvalidField {
                field: "episode guid",
                reason: "is too long",
            });
        }
        check_text("episode title", &self.title, MAX_TITLE_CHARS, true)?;
        check_opt_text(
            "episode description",
            self.description.as_deref(),
            MAX_DESCRIPTION_CHARS,
        )?;
        check_duration(self.duration_ms)?;
        check_url("episode audio_url", &self.audio_url)?;
        check_mime(self.mime.as_deref())?;
        check_opt_url("episode image_url", self.image_url.as_deref())?;
        if self
            .link
            .as_ref()
            .is_some_and(|link| link.len() > MAX_URL_BYTES)
        {
            return Err(PodcastError::InvalidField {
                field: "episode link",
                reason: "is too long",
            });
        }
        Ok(())
    }

    /// Identity hints to keep for the no-GUID matching rule.
    pub fn identity_hints(&self) -> EpisodeIdentityHints {
        EpisodeIdentityHints {
            guid: self.guid.clone(),
            enclosure_url: Some(self.audio_url.clone()),
            link: self.link.clone(),
            published_at_ms: self.published_at_ms,
            title: Some(self.title.clone()),
        }
    }

    /// Builds the self-contained queue reference for this episode.
    ///
    /// The episode artwork falls back to the podcast artwork.
    pub fn to_ref(&self, podcast: &PodcastMeta) -> PodcastEpisodeRef {
        PodcastEpisodeRef {
            podcast_id: self.podcast_id.clone(),
            episode_id: self.episode_id.clone(),
            feed_url: podcast.feed_url.clone(),
            podcast_title: bounded_title(&podcast.title, "Untitled podcast"),
            episode_title: bounded_title(&self.title, "Untitled episode"),
            audio_url: self.audio_url.clone(),
            mime: self
                .mime
                .clone()
                .filter(|mime| check_mime(Some(mime)).is_ok()),
            duration_ms: self.duration_ms.filter(|ms| *ms > 0),
            published_at_ms: self.published_at_ms,
            season: self.season,
            episode_number: self.episode_number,
            image_url: valid_url(self.image_url.as_deref())
                .or_else(|| valid_url(podcast.image_url.as_deref())),
            progress_generation: None,
        }
    }
}

/// Self-contained, bounded reference to a podcast episode.
///
/// Carried by playback queues (`PlaybackSource::PodcastEpisode`) and by
/// progress operations, so a receiving trusted device can display, resume and
/// play the episode without being subscribed and without its own copy of the
/// feed. Apart from the optional handoff hint
/// [`progress_generation`](Self::progress_generation) it contains no personal
/// state. It is private: Jam snapshots replace it with an opaque placeholder.
///
/// If the audio URL went stale, refresh the feed at `feed_url` and look the
/// episode up by `episode_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastEpisodeRef {
    /// Owning podcast.
    pub podcast_id: PodcastId,
    /// Episode.
    pub episode_id: EpisodeId,
    /// Canonical feed URL used to refresh the episode.
    pub feed_url: FeedUrl,
    /// Podcast title for display.
    pub podcast_title: String,
    /// Episode title for display.
    pub episode_title: String,
    /// Absolute http(s) audio URL at the publisher.
    pub audio_url: String,
    /// Enclosure MIME type.
    #[serde(default)]
    pub mime: Option<String>,
    /// Announced duration in milliseconds.
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// Publication time in unix milliseconds.
    #[serde(default)]
    pub published_at_ms: Option<i64>,
    /// `itunes:season`.
    #[serde(default)]
    pub season: Option<u32>,
    /// `itunes:episode`.
    #[serde(default)]
    pub episode_number: Option<u32>,
    /// Episode or podcast artwork URL.
    #[serde(default)]
    pub image_url: Option<String>,
    /// Greatest progress generation the sender has seen for this episode.
    ///
    /// Set on queue items of a playback state (in particular on handoff),
    /// because the playback state can arrive before the sender's progress
    /// operations. A device that starts playing the item writes its explicit
    /// action with
    /// [`ProgressRecord::explicit_generation`](super::ProgressRecord::explicit_generation)`(max(local, progress_generation))`.
    /// Ignored in progress and listening operations, whose own generation is
    /// authoritative. Not part of the episode identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_generation: Option<u64>,
}

impl PodcastEpisodeRef {
    /// Validates contract bounds. Receivers must drop invalid references.
    pub fn validate(&self) -> PodcastResult<()> {
        check_text("podcast title", &self.podcast_title, MAX_TITLE_CHARS, true)?;
        check_text("episode title", &self.episode_title, MAX_TITLE_CHARS, true)?;
        check_url("episode audio_url", &self.audio_url)?;
        check_mime(self.mime.as_deref())?;
        check_duration(self.duration_ms)?;
        check_opt_url("episode image_url", self.image_url.as_deref())
    }

    /// Identity hints known from this reference.
    ///
    /// Add them to the known-episode set before resolving ids from a feed so
    /// a GUID-less episode learned through sync keeps its id.
    pub fn identity_hints(&self) -> EpisodeIdentityHints {
        EpisodeIdentityHints {
            guid: None,
            enclosure_url: Some(self.audio_url.clone()),
            link: None,
            published_at_ms: self.published_at_ms,
            title: Some(self.episode_title.clone()),
        }
    }
}

fn check_duration(duration_ms: Option<i64>) -> PodcastResult<()> {
    if duration_ms.is_some_and(|ms| ms < 0) {
        return Err(PodcastError::InvalidField {
            field: "duration_ms",
            reason: "must not be negative",
        });
    }
    Ok(())
}

fn check_mime(mime: Option<&str>) -> PodcastResult<()> {
    let Some(mime) = mime else { return Ok(()) };
    if mime.is_empty() || mime.len() > MAX_MIME_BYTES || !mime.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(PodcastError::InvalidField {
            field: "mime",
            reason: "must be a short printable ASCII token",
        });
    }
    Ok(())
}

fn bounded_title(title: &str, fallback: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        fallback.to_string()
    } else {
        bounded_text(title, MAX_TITLE_CHARS)
    }
}

fn bounded_opt(value: Option<&str>, max_chars: usize) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| bounded_text(value, max_chars))
}

fn valid_url(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| super::is_http_url(value))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn podcast_meta() -> PodcastMeta {
        PodcastMeta {
            feed_url: FeedUrl::parse("https://example.test/feed").unwrap(),
            title: "Show".to_string(),
            author: Some("Host".to_string()),
            description: None,
            link: Some("https://example.test/".to_string()),
            image_url: Some("https://example.test/cover.jpg".to_string()),
        }
    }

    fn episode() -> EpisodeMeta {
        let podcast_id = PodcastId::from_feed_url(&podcast_meta().feed_url);
        EpisodeMeta {
            episode_id: EpisodeId::from_guid(&podcast_id, "g1").unwrap(),
            podcast_id,
            guid: Some("g1".to_string()),
            title: "Pilot".to_string(),
            description: Some("Notes".to_string()),
            season: Some(2),
            episode_number: Some(7),
            episode_type: EpisodeType::Bonus,
            published_at_ms: Some(1_700_000_000_000),
            duration_ms: Some(3_600_000),
            audio_url: "https://cdn.example.test/1.mp3?sig=abc".to_string(),
            mime: Some("audio/mpeg".to_string()),
            byte_len: Some(1234),
            image_url: None,
            link: Some("https://example.test/1".to_string()),
        }
    }

    #[test]
    fn episode_type_is_tolerant() {
        assert_eq!(EpisodeType::parse(" Trailer "), EpisodeType::Trailer);
        assert_eq!(EpisodeType::parse("BONUS"), EpisodeType::Bonus);
        assert_eq!(EpisodeType::parse("full"), EpisodeType::Full);
        assert_eq!(EpisodeType::parse("teaser"), EpisodeType::Full);
        assert_eq!(
            serde_json::to_string(&EpisodeType::Trailer).unwrap(),
            "\"trailer\""
        );
        assert_eq!(
            serde_json::from_str::<EpisodeType>("\"something-new\"").unwrap(),
            EpisodeType::Full
        );
    }

    #[test]
    fn metadata_round_trips_and_validates() {
        let episode = episode();
        episode.validate().unwrap();
        let json = serde_json::to_string(&episode).unwrap();
        assert_eq!(serde_json::from_str::<EpisodeMeta>(&json).unwrap(), episode);
        podcast_meta().validate().unwrap();

        let mut bad = episode.clone();
        bad.audio_url = "file:///tmp/x.mp3".to_string();
        assert!(bad.validate().is_err());
        let mut bad = episode.clone();
        bad.description = Some("x".repeat(MAX_DESCRIPTION_CHARS + 1));
        assert!(bad.validate().is_err());
        let mut bad = episode;
        bad.mime = Some("audio/mpeg; codecs=\"mp3\"".to_string());
        assert!(bad.validate().is_err());
    }

    #[test]
    fn minimal_legacy_json_uses_defaults() {
        let episode = episode();
        let json = format!(
            r#"{{"episode_id":"{}","podcast_id":"{}","title":"T","audio_url":"https://a.test/1.mp3"}}"#,
            episode.episode_id, episode.podcast_id
        );
        let parsed: EpisodeMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.episode_type, EpisodeType::Full);
        assert_eq!(parsed.season, None);
    }

    #[test]
    fn episode_ref_is_self_contained_and_bounded() {
        let podcast = podcast_meta();
        let mut episode = episode();
        episode.title = "x".repeat(MAX_TITLE_CHARS + 10);
        let reference = episode.to_ref(&podcast);
        reference.validate().unwrap();
        assert_eq!(reference.episode_title.chars().count(), MAX_TITLE_CHARS);
        assert_eq!(reference.podcast_title, "Show");
        assert_eq!(
            reference.image_url.as_deref(),
            Some("https://example.test/cover.jpg")
        );
        assert_eq!(reference.feed_url, podcast.feed_url);
        let json = serde_json::to_string(&reference).unwrap();
        assert!(!json.contains("description"));
        assert_eq!(
            serde_json::from_str::<PodcastEpisodeRef>(&json).unwrap(),
            reference
        );

        let mut bad = reference.clone();
        bad.episode_title = " ".to_string();
        assert!(bad.validate().is_err());
        let mut bad = reference.clone();
        bad.audio_url = "javascript:alert(1)".to_string();
        assert!(bad.validate().is_err());
        let mut bad = reference;
        bad.duration_ms = Some(-1);
        assert!(bad.validate().is_err());
    }

    #[test]
    fn refs_reject_invalid_ids_on_deserialization() {
        let json = r#"{"podcast_id":"pod1:bad","episode_id":"ep1:bad","feed_url":"https://a.test/","podcast_title":"P","episode_title":"E","audio_url":"https://a.test/1.mp3"}"#;
        assert!(serde_json::from_str::<PodcastEpisodeRef>(json).is_err());
    }

    #[test]
    fn bounded_podcast_meta_fits_the_contract() {
        let mut meta = podcast_meta();
        meta.title = "  ".to_string();
        meta.description = Some("d".repeat(MAX_DESCRIPTION_CHARS * 2));
        meta.image_url = Some("ftp://x".to_string());
        let bounded = meta.bounded();
        bounded.validate().unwrap();
        assert_eq!(bounded.title, "Untitled podcast");
        assert_eq!(bounded.image_url, None);
    }
}
