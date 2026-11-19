//! Shared podcast contract for Furumi clients.
//!
//! This module is pure: it has no SQLite, HTTP, clock, audio or UI
//! dependency. Every Furumi client (TUI, web, desktop) uses it so that
//! podcast identity, normalization, merge rules and wire DTOs are defined
//! exactly once. The normative description lives in the workspace
//! `PODCASTS.md`; this documentation summarizes the API.
//!
//! # Identity
//!
//! * [`FeedUrl`] is a canonical feed URL. [`FeedUrl::parse`] lowercases the
//!   scheme and host, drops a default port and the fragment, and keeps the
//!   path and **every** query parameter byte-for-byte (only non-ASCII bytes
//!   are percent-encoded). Non-`http(s)` URLs, userinfo and whitespace are
//!   rejected. [`FeedUrl::from_user_input`] additionally accepts the
//!   `feed:`/`podcast:`/`pcast:`/`itpc:` forms and a missing scheme.
//! * [`PodcastId`] is `pod1:` followed by the lowercase base32 BLAKE3 hash of
//!   `"furumi-podcast:podcast:v1\0"` and the canonical feed URL. It is
//!   derived once, from the URL of the first subscription; later URLs
//!   (redirects, `itunes:new-feed-url`) become feed aliases resolved by
//!   [`resolve_podcast_id`].
//! * [`EpisodeId`] is `ep1:` followed by the lowercase base32 BLAKE3 hash of
//!   `"furumi-podcast:episode:v1\0"`, the podcast id and a key. The key is
//!   the RSS GUID when present. Without a GUID, [`resolve_episode_ids`] first
//!   reuses the id of a known episode that matches by enclosure URL, `<link>`
//!   or (date, normalized title) and only then derives a new id from the
//!   first available fallback key.
//!
//! # Personal state
//!
//! * [`SubscriptionRecord`]: last-writer-wins by `(hlc_ms, op_id)`;
//!   unsubscribing is state, not a tombstone. Feed aliases merge as a bounded
//!   grow-only set.
//! * [`ShareRecord`]: last-writer-wins; `shared = false` is a tombstone. See
//!   [`effective_share`] for the rule that a (re)subscription is private.
//! * [`ProgressRecord`]: register ordered by
//!   `(generation, hlc_ms, device_id, op_id)`. Explicit user actions bump the
//!   generation to `max_seen + 1`; periodic checkpoints keep the generation of
//!   their session, so a late checkpoint of an old session always loses.
//! * [`ListenSession`]: grow-only set keyed by `session_id`; one completion per
//!   session, so re-delivery never double counts.
//!
//! # Queue items
//!
//! [`PodcastEpisodeRef`] is the bounded, self-contained reference carried by
//! playback queues and progress operations. A receiving device can show and
//! play the episode without being subscribed to the podcast.
//!
//! # Feed parser
//!
//! With the `podcast-feed` cargo feature, `feed` provides the shared tolerant
//! RSS 2.0 / RSS 1.0 / Atom parser that produces the identity hints consumed
//! by [`resolve_episode_ids`].
//!
//! ```
//! use music_dht::podcast::{EpisodeId, FeedUrl, PodcastId};
//!
//! let url = FeedUrl::parse("HTTPS://Example.COM:443/feed.xml?token=Ab#top").unwrap();
//! assert_eq!(url.as_str(), "https://example.com/feed.xml?token=Ab");
//! let podcast = PodcastId::from_feed_url(&url);
//! assert!(podcast.as_str().starts_with("pod1:"));
//! let episode = EpisodeId::from_guid(&podcast, "episode-42").unwrap();
//! assert!(episode.as_str().starts_with("ep1:"));
//! ```

mod feed_url;
mod id;
mod identity;
mod meta;
mod state;

#[cfg(feature = "podcast-feed")]
pub mod feed;

pub use feed_url::{FeedUrl, merge_feed_aliases, resolve_podcast_id};
pub use id::{
    EPISODE_ID_DOMAIN, EPISODE_ID_PREFIX, EpisodeId, EpisodeKey, PODCAST_ID_DOMAIN,
    PODCAST_ID_PREFIX, PodcastId,
};
pub use identity::{
    EpisodeIdentityHints, KnownEpisode, normalize_episode_title, resolve_episode_id,
    resolve_episode_ids,
};
pub use meta::{EpisodeMeta, EpisodeType, PodcastEpisodeRef, PodcastMeta};
pub use state::{
    COMPLETION_TAIL_MAX_MS, ListenSession, MIN_RECORDED_EPISODE_LISTEN_MS, ProgressRecord,
    ProgressStatus, ShareRecord, SubscriptionRecord, completed_listen_count, effective_share,
    merge_listen_session, merge_progress, merge_share, merge_subscription, reaches_completion,
};

/// Maximum characters of a title (podcast, episode, author).
pub const MAX_TITLE_CHARS: usize = 1_000;
/// Maximum characters of a podcast or episode description.
pub const MAX_DESCRIPTION_CHARS: usize = 4_000;
/// Maximum bytes of any URL carried by the contract.
pub const MAX_URL_BYTES: usize = 2_048;
/// Maximum bytes of an RSS GUID. Longer GUIDs are treated as absent.
pub const MAX_GUID_BYTES: usize = 2_048;
/// Maximum bytes of a MIME type.
pub const MAX_MIME_BYTES: usize = 128;
/// Maximum bytes of a device, session or operation id.
pub const MAX_TOKEN_BYTES: usize = 256;
/// Maximum feed aliases kept per podcast, besides the URL the id was derived from.
pub const MAX_FEED_ALIASES: usize = 8;

/// Maximum subscription records in one sync snapshot.
pub const MAX_SNAPSHOT_SUBSCRIPTIONS: usize = 1_000;
/// Maximum share records in one sync snapshot.
pub const MAX_SNAPSHOT_SHARES: usize = 1_000;
/// Maximum progress registers in one sync snapshot.
pub const MAX_SNAPSHOT_PROGRESS: usize = 5_000;
/// Maximum listening sessions in one sync snapshot.
pub const MAX_SNAPSHOT_LISTEN_SESSIONS: usize = 2_000;
/// Serialized JSON budget shared by all podcast snapshot sections.
///
/// It leaves room for music state inside the 8 MiB sync line limit.
pub const MAX_PODCAST_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

/// Errors produced by podcast contract validation and parsing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PodcastError {
    /// A feed URL cannot be canonicalized.
    #[error("invalid feed URL: {0}")]
    InvalidFeedUrl(String),
    /// A string is not a valid podcast or episode id.
    #[error("invalid {kind} id")]
    InvalidId {
        /// `podcast` or `episode`.
        kind: &'static str,
    },
    /// A field violates the contract (missing, too long, malformed).
    #[error("invalid {field}: {reason}")]
    InvalidField {
        /// Field name.
        field: &'static str,
        /// Human-readable reason.
        reason: &'static str,
    },
    /// Two records that must describe the same entity do not.
    #[error("cannot merge records of different {0}")]
    Mismatch(&'static str),
    /// The document is not an RSS or Atom feed.
    #[error("not an RSS or Atom feed{0}")]
    NotAFeed(String),
    /// The feed is malformed and nothing usable could be parsed.
    #[error("malformed feed: {0}")]
    MalformedFeed(String),
}

/// Result alias for this module.
pub type PodcastResult<T> = std::result::Result<T, PodcastError>;

/// Truncates `text` to at most `max_chars` characters, ending with `…` when cut.
///
/// Clients use it to fit publisher metadata into the contract bounds.
pub fn bounded_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    let mut out: String = text.chars().take(max_chars - 1).collect();
    out.push('…');
    out
}

pub(crate) fn check_text(
    field: &'static str,
    value: &str,
    max_chars: usize,
    required: bool,
) -> PodcastResult<()> {
    if required && value.trim().is_empty() {
        return Err(PodcastError::InvalidField {
            field,
            reason: "must not be empty",
        });
    }
    if value.len() > max_chars * 4 || value.chars().count() > max_chars {
        return Err(PodcastError::InvalidField {
            field,
            reason: "is too long",
        });
    }
    Ok(())
}

pub(crate) fn check_opt_text(
    field: &'static str,
    value: Option<&str>,
    max_chars: usize,
) -> PodcastResult<()> {
    value.map_or(Ok(()), |value| check_text(field, value, max_chars, false))
}

pub(crate) fn check_token(field: &'static str, value: &str) -> PodcastResult<()> {
    if value.is_empty() {
        return Err(PodcastError::InvalidField {
            field,
            reason: "must not be empty",
        });
    }
    if value.len() > MAX_TOKEN_BYTES {
        return Err(PodcastError::InvalidField {
            field,
            reason: "is too long",
        });
    }
    if value.chars().any(|c| c.is_control()) {
        return Err(PodcastError::InvalidField {
            field,
            reason: "contains control characters",
        });
    }
    Ok(())
}

/// Returns whether `value` is an absolute `http`/`https` URL with a host,
/// without whitespace or control characters and within [`MAX_URL_BYTES`].
///
/// Media URLs keep the publisher's spelling; only feed URLs are canonicalized.
pub fn is_http_url(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_URL_BYTES
        || value.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return false;
    }
    let lower_prefix = |prefix: &str| {
        value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix)
    };
    let rest = if lower_prefix("http://") {
        &value[7..]
    } else if lower_prefix("https://") {
        &value[8..]
    } else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority.rsplit('@').next().unwrap_or_default();
    !host.is_empty() && !host.starts_with(':')
}

pub(crate) fn check_url(field: &'static str, value: &str) -> PodcastResult<()> {
    if is_http_url(value) {
        Ok(())
    } else {
        Err(PodcastError::InvalidField {
            field,
            reason: "must be an absolute http(s) URL",
        })
    }
}

pub(crate) fn check_opt_url(field: &'static str, value: Option<&str>) -> PodcastResult<()> {
    value.map_or(Ok(()), |value| check_url(field, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_text_cuts_on_char_boundaries() {
        assert_eq!(bounded_text("hello", 5), "hello");
        assert_eq!(bounded_text("hello!", 5), "hell…");
        assert_eq!(bounded_text("привет", 3), "пр…");
        assert_eq!(bounded_text("x", 0), "");
    }

    #[test]
    fn text_limits_accept_the_limit_and_reject_one_more() {
        let at = "я".repeat(MAX_TITLE_CHARS);
        let over = "я".repeat(MAX_TITLE_CHARS + 1);
        assert!(check_text("title", &at, MAX_TITLE_CHARS, true).is_ok());
        assert!(check_text("title", &over, MAX_TITLE_CHARS, true).is_err());
        assert!(check_text("title", "  ", MAX_TITLE_CHARS, true).is_err());
        assert!(check_text("title", "", MAX_TITLE_CHARS, false).is_ok());
    }

    #[test]
    fn token_limits() {
        assert!(check_token("op_id", &"a".repeat(MAX_TOKEN_BYTES)).is_ok());
        assert!(check_token("op_id", &"a".repeat(MAX_TOKEN_BYTES + 1)).is_err());
        assert!(check_token("op_id", "").is_err());
        assert!(check_token("op_id", "a\nb").is_err());
    }

    #[test]
    fn http_urls() {
        assert!(is_http_url("https://cdn.example.test/a.mp3?x=1"));
        assert!(is_http_url("HTTP://cdn.example.test"));
        assert!(!is_http_url("ftp://cdn.example.test/a.mp3"));
        assert!(!is_http_url("file:///etc/passwd"));
        assert!(!is_http_url("https://"));
        assert!(!is_http_url("https:///path"));
        assert!(!is_http_url("https://a.test/my file.mp3"));
        assert!(!is_http_url(""));
        let long = format!("https://a.test/{}", "x".repeat(MAX_URL_BYTES));
        assert!(!is_http_url(&long));
        let at = format!("https://a.test/{}", "x".repeat(MAX_URL_BYTES - 15));
        assert_eq!(at.len(), MAX_URL_BYTES);
        assert!(is_http_url(&at));
    }
}
