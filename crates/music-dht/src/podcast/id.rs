//! Cross-device podcast and episode identifiers.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{FeedUrl, MAX_GUID_BYTES, PodcastError, PodcastResult};

/// BLAKE3 derivation domain of [`PodcastId`]. Changing it changes every id.
pub const PODCAST_ID_DOMAIN: &str = "furumi-podcast:podcast:v1";
/// BLAKE3 derivation domain of [`EpisodeId`]. Changing it changes every id.
pub const EPISODE_ID_DOMAIN: &str = "furumi-podcast:episode:v1";
/// String prefix of a [`PodcastId`].
pub const PODCAST_ID_PREFIX: &str = "pod1:";
/// String prefix of an [`EpisodeId`].
pub const EPISODE_ID_PREFIX: &str = "ep1:";

/// Length of the base32 body: 32 bytes, RFC 4648 lowercase, no padding.
const DIGEST_CHARS: usize = 52;

fn digest(domain: &str, parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain.as_bytes());
    hasher.update(&[0]);
    for part in parts {
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

fn encode(prefix: &str, digest: &[u8; 32]) -> String {
    let mut body = data_encoding::BASE32_NOPAD.encode(digest);
    body.make_ascii_lowercase();
    format!("{prefix}{body}")
}

fn decode(prefix: &str, value: &str) -> Option<[u8; 32]> {
    let body = value.strip_prefix(prefix)?;
    if body.len() != DIGEST_CHARS
        || !body
            .bytes()
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
    {
        return None;
    }
    let bytes = data_encoding::BASE32_NOPAD
        .decode(body.to_ascii_uppercase().as_bytes())
        .ok()?;
    let digest: [u8; 32] = bytes.try_into().ok()?;
    // Reject non-canonical trailing bits so every id has one spelling.
    (encode(prefix, &digest) == value).then_some(digest)
}

macro_rules! string_id {
    ($name:ident, $prefix:expr, $kind:literal) => {
        impl $name {
            /// Parses and validates the canonical string form.
            pub fn parse(value: &str) -> PodcastResult<Self> {
                decode($prefix, value)
                    .map(|_| Self(value.to_string()))
                    .ok_or(PodcastError::InvalidId { kind: $kind })
            }

            /// Canonical string form.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// The 32-byte BLAKE3 digest encoded in the id.
            pub fn digest(&self) -> [u8; 32] {
                decode($prefix, &self.0).expect("validated on construction")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = PodcastError;

            fn from_str(value: &str) -> PodcastResult<Self> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = PodcastError;

            fn try_from(value: String) -> PodcastResult<Self> {
                decode($prefix, &value)
                    .map(|_| Self(value))
                    .ok_or(PodcastError::InvalidId { kind: $kind })
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

/// Stable cross-device podcast identity: `pod1:<52 lowercase base32 chars>`.
///
/// Derived as `BLAKE3("furumi-podcast:podcast:v1" || 0x00 || canonical_feed_url)`
/// from the URL used by the **first** subscription. The id never changes when
/// the feed moves; new URLs are recorded as aliases (see
/// [`super::resolve_podcast_id`]). Deserialization validates the format.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PodcastId(String);

string_id!(PodcastId, PODCAST_ID_PREFIX, "podcast");

impl PodcastId {
    /// Derives the id of a podcast first subscribed through `url`.
    pub fn from_feed_url(url: &FeedUrl) -> Self {
        Self(encode(
            PODCAST_ID_PREFIX,
            &digest(PODCAST_ID_DOMAIN, &[url.as_str().as_bytes()]),
        ))
    }
}

/// Stable cross-device episode identity: `ep1:<52 lowercase base32 chars>`.
///
/// Derived as `BLAKE3("furumi-podcast:episode:v1" || 0x00 || podcast_id || key)`
/// where `key` is the material of an [`EpisodeKey`]. Title, artwork or audio
/// URL changes never change the id of an existing episode.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EpisodeId(String);

string_id!(EpisodeId, EPISODE_ID_PREFIX, "episode");

impl EpisodeId {
    /// Derives an episode id from a key inside `podcast`.
    pub fn derive(podcast: &PodcastId, key: &EpisodeKey) -> Self {
        let material = key.material();
        Self(encode(
            EPISODE_ID_PREFIX,
            &digest(
                EPISODE_ID_DOMAIN,
                &[podcast.as_str().as_bytes(), material.as_bytes()],
            ),
        ))
    }

    /// Derives the id of an episode with an RSS GUID (or Atom `<id>`).
    ///
    /// Returns `None` when the GUID is blank or longer than [`MAX_GUID_BYTES`].
    pub fn from_guid(podcast: &PodcastId, guid: &str) -> Option<Self> {
        EpisodeKey::guid(guid).map(|key| Self::derive(podcast, &key))
    }
}

/// Key material from which an [`EpisodeId`] is derived.
///
/// | Variant | Material |
/// | --- | --- |
/// | `Guid` | `guid:` + GUID trimmed of surrounding whitespace |
/// | `Link` | `link:` + `<link>` trimmed |
/// | `DateTitle` | `date-title:` + unix seconds + `\0` + [`super::normalize_episode_title`] |
/// | `Enclosure` | `enclosure:` + enclosure URL trimmed |
///
/// GUIDs and URLs are otherwise byte-for-byte: GUIDs are case-sensitive.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EpisodeKey {
    /// RSS `<guid>` or Atom `<id>`.
    Guid(String),
    /// Item `<link>`.
    Link(String),
    /// Publication time (whole unix seconds) and normalized title.
    DateTitle {
        /// Publication time in unix seconds.
        published_at_secs: i64,
        /// Title normalized by [`super::normalize_episode_title`].
        normalized_title: String,
    },
    /// Enclosure (audio) URL.
    Enclosure(String),
}

impl EpisodeKey {
    /// GUID key; `None` for a blank or oversized GUID.
    pub fn guid(guid: &str) -> Option<Self> {
        let guid = guid.trim();
        (!guid.is_empty() && guid.len() <= MAX_GUID_BYTES).then(|| Self::Guid(guid.to_string()))
    }

    /// Link key; `None` for a blank or oversized link.
    pub fn link(link: &str) -> Option<Self> {
        let link = link.trim();
        (!link.is_empty() && link.len() <= super::MAX_URL_BYTES)
            .then(|| Self::Link(link.to_string()))
    }

    /// Date + title key; `None` when the title normalizes to nothing.
    pub fn date_title(published_at_ms: i64, title: &str) -> Option<Self> {
        let normalized_title = super::normalize_episode_title(title);
        (!normalized_title.is_empty()).then(|| Self::DateTitle {
            published_at_secs: published_at_ms.div_euclid(1_000),
            normalized_title,
        })
    }

    /// Enclosure key; `None` for a blank or oversized URL.
    pub fn enclosure(url: &str) -> Option<Self> {
        let url = url.trim();
        (!url.is_empty() && url.len() <= super::MAX_URL_BYTES)
            .then(|| Self::Enclosure(url.to_string()))
    }

    /// Exact bytes hashed after the podcast id.
    pub fn material(&self) -> String {
        match self {
            Self::Guid(guid) => format!("guid:{guid}"),
            Self::Link(link) => format!("link:{link}"),
            Self::DateTitle {
                published_at_secs,
                normalized_title,
            } => format!("date-title:{published_at_secs}\0{normalized_title}"),
            Self::Enclosure(url) => format!("enclosure:{url}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn podcast() -> PodcastId {
        PodcastId::from_feed_url(&FeedUrl::parse("https://example.test/feed.xml").unwrap())
    }

    #[test]
    fn podcast_id_is_domain_separated_blake3() {
        let url = FeedUrl::parse("https://example.test/feed.xml").unwrap();
        let mut input = b"furumi-podcast:podcast:v1\0".to_vec();
        input.extend_from_slice(url.as_str().as_bytes());
        let expected = blake3::hash(&input);
        let id = PodcastId::from_feed_url(&url);
        assert_eq!(&id.digest(), expected.as_bytes());
        assert_eq!(id.as_str().len(), PODCAST_ID_PREFIX.len() + DIGEST_CHARS);
        assert!(id.as_str()[5..].bytes().all(|b| !b.is_ascii_uppercase()));
    }

    #[test]
    fn podcast_id_golden_value() {
        // Golden vector: changing it breaks every stored and synced id.
        let id = podcast();
        let mut input = b"furumi-podcast:podcast:v1\0".to_vec();
        input.extend_from_slice(b"https://example.test/feed.xml");
        let mut body = data_encoding::BASE32_NOPAD.encode(blake3::hash(&input).as_bytes());
        body.make_ascii_lowercase();
        assert_eq!(id.as_str(), format!("pod1:{body}"));
    }

    #[test]
    fn episode_id_follows_the_plan_formula() {
        let podcast = podcast();
        let id = EpisodeId::from_guid(&podcast, "  abc-123 ").unwrap();
        let mut input = b"furumi-podcast:episode:v1\0".to_vec();
        input.extend_from_slice(podcast.as_str().as_bytes());
        input.extend_from_slice(b"guid:abc-123");
        assert_eq!(&id.digest(), blake3::hash(&input).as_bytes());
        // GUIDs are case-sensitive and podcast-scoped.
        assert_ne!(EpisodeId::from_guid(&podcast, "ABC-123"), Some(id.clone()));
        let other = PodcastId::from_feed_url(&FeedUrl::parse("https://other.test/").unwrap());
        assert_ne!(EpisodeId::from_guid(&other, "abc-123"), Some(id));
    }

    #[test]
    fn keys_are_distinct_per_kind() {
        let podcast = podcast();
        let value = "https://example.test/e1";
        let link = EpisodeId::derive(&podcast, &EpisodeKey::link(value).unwrap());
        let guid = EpisodeId::derive(&podcast, &EpisodeKey::guid(value).unwrap());
        let enclosure = EpisodeId::derive(&podcast, &EpisodeKey::enclosure(value).unwrap());
        assert_ne!(link, guid);
        assert_ne!(link, enclosure);
        assert_ne!(guid, enclosure);
    }

    #[test]
    fn blank_and_oversized_keys_are_rejected() {
        assert!(EpisodeKey::guid("   ").is_none());
        assert!(EpisodeKey::guid(&"g".repeat(MAX_GUID_BYTES)).is_some());
        assert!(EpisodeKey::guid(&"g".repeat(MAX_GUID_BYTES + 1)).is_none());
        assert!(EpisodeKey::date_title(0, " -- ").is_none());
        assert!(EpisodeKey::link("").is_none());
    }

    #[test]
    fn date_title_uses_whole_seconds_and_normalized_title() {
        let a = EpisodeKey::date_title(1_700_000_000_123, "Hello,  World!").unwrap();
        let b = EpisodeKey::date_title(1_700_000_000_999, "hello world").unwrap();
        assert_eq!(a, b);
        let c = EpisodeKey::date_title(-1, "x").unwrap();
        assert_eq!(
            c,
            EpisodeKey::DateTitle {
                published_at_secs: -1,
                normalized_title: "x".to_string()
            }
        );
    }

    #[test]
    fn parsing_validates_and_round_trips() {
        let id = podcast();
        assert_eq!(PodcastId::parse(id.as_str()).unwrap(), id);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<PodcastId>(&json).unwrap(), id);

        let upper = id.as_str().to_ascii_uppercase().replace("POD1:", "pod1:");
        assert!(PodcastId::parse(&upper).is_err());
        assert!(PodcastId::parse(&id.as_str()[..id.as_str().len() - 1]).is_err());
        assert!(PodcastId::parse(&format!("{id}a")).is_err());
        assert!(PodcastId::parse("pod1:").is_err());
        assert!(PodcastId::parse("").is_err());
        assert!(EpisodeId::parse(id.as_str()).is_err());
        assert!(serde_json::from_str::<PodcastId>("\"pod1:xyz\"").is_err());

        // Non-canonical trailing bits: last char encodes 1 data bit + 4 zero bits.
        let mut chars: Vec<char> = id.as_str().chars().collect();
        let last = chars.len() - 1;
        chars[last] = if chars[last] == 'b' { 'c' } else { 'b' };
        let tampered: String = chars.into_iter().collect();
        assert!(PodcastId::parse(&tampered).is_err());
    }
}
