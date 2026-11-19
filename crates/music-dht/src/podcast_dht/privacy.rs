//! Advisory detection of feed URLs that look private.
//!
//! Premium and member feeds usually carry a personal secret in the URL. A
//! card publishes its feed URL verbatim, so the UI must ask the user before
//! publishing a URL for which [`feed_url_privacy_warning`] returns a warning.
//! The heuristics are deliberately broad and can produce false positives
//! (and miss secrets); they never block publication by themselves.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::podcast::FeedUrl;

/// Query-parameter name words that usually carry a credential. A name is
/// split into lowercase words at non-alphanumeric characters and camelCase
/// boundaries; any listed word triggers a warning.
pub const SECRET_QUERY_WORDS: &[&str] = &[
    "token",
    "key",
    "auth",
    "secret",
    "sig",
    "signature",
    "uid",
    "user",
    "pass",
    "password",
    "passwd",
    "pwd",
    "session",
    "sess",
    "sid",
    "access",
    "private",
    "credential",
    "credentials",
    "apikey",
    "jwt",
    "hmac",
];

/// Substrings that trigger a warning anywhere in a query-parameter name,
/// for names such as `apitoken` or `privatefeed` that do not split.
const SECRET_QUERY_SUBSTRINGS: &[&str] = &[
    "token",
    "secret",
    "signature",
    "password",
    "passwd",
    "session",
    "private",
    "credential",
    "apikey",
];

/// Hosts (and their subdomains) known to serve personal member feeds.
pub const PRIVATE_FEED_HOSTS: &[&str] = &[
    "patreon.com",
    "supercast.com",
    "supportingcast.fm",
    "glow.fm",
];

/// Minimum length of a query value considered a possible secret.
const MIN_SECRET_VALUE_CHARS: usize = 16;
/// Minimum length of a path segment considered a possible secret.
const MIN_SECRET_SEGMENT_CHARS: usize = 32;
/// Minimum Shannon entropy, in bits per character, of a possible secret.
const MIN_SECRET_ENTROPY_BITS: f64 = 3.0;

/// Why a feed URL may contain a personal secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
#[non_exhaustive]
pub enum FeedUrlPrivacyWarning {
    /// A query parameter whose name suggests a credential.
    SecretQueryParameter {
        /// Parameter name as it appears in the URL.
        name: String,
    },
    /// The host is known to serve personal member feeds.
    PrivateFeedHost {
        /// Lowercase host.
        host: String,
    },
    /// A long, random-looking query value.
    HighEntropyQueryValue {
        /// Name of the parameter carrying the value.
        name: String,
    },
    /// A long, random-looking path segment.
    HighEntropyPathSegment {
        /// Zero-based index of the segment after the host.
        index: usize,
    },
}

impl fmt::Display for FeedUrlPrivacyWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SecretQueryParameter { name } => write!(
                f,
                "the feed URL has a query parameter \"{name}\" that looks like a credential"
            ),
            Self::PrivateFeedHost { host } => {
                write!(f, "{host} usually serves personal member feeds")
            }
            Self::HighEntropyQueryValue { name } => write!(
                f,
                "the value of the query parameter \"{name}\" looks like a secret token"
            ),
            Self::HighEntropyPathSegment { index } => write!(
                f,
                "path segment {} of the feed URL looks like a secret token",
                index + 1
            ),
        }
    }
}

/// Returns why `url` may contain a personal secret, or `None`.
///
/// Advisory only (see the module documentation). Checks, in order: query
/// parameter names ([`SECRET_QUERY_WORDS`]), known member-feed hosts
/// ([`PRIVATE_FEED_HOSTS`]), long high-entropy query values, and long
/// high-entropy path segments.
pub fn feed_url_privacy_warning(url: &FeedUrl) -> Option<FeedUrlPrivacyWarning> {
    let value = url.as_str();
    let rest = value.split_once("://").map_or(value, |(_, rest)| rest);
    let (authority, path_and_query) = match rest.find('/') {
        Some(index) => rest.split_at(index),
        None => (rest, "/"),
    };
    let host = if authority.starts_with('[') {
        authority
            .find(']')
            .map_or(authority, |end| &authority[..=end])
    } else {
        authority.split(':').next().unwrap_or(authority)
    };
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let parameters: Vec<(&str, &str)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect();

    if let Some((name, _)) = parameters
        .iter()
        .find(|(name, _)| is_secret_parameter_name(name))
    {
        return Some(FeedUrlPrivacyWarning::SecretQueryParameter {
            name: (*name).to_string(),
        });
    }
    if PRIVATE_FEED_HOSTS
        .iter()
        .any(|known| host == *known || host.ends_with(&format!(".{known}")))
    {
        return Some(FeedUrlPrivacyWarning::PrivateFeedHost {
            host: host.to_string(),
        });
    }
    if let Some((name, _)) = parameters
        .iter()
        .find(|(_, value)| looks_random(value, MIN_SECRET_VALUE_CHARS))
    {
        return Some(FeedUrlPrivacyWarning::HighEntropyQueryValue {
            name: (*name).to_string(),
        });
    }
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .position(|segment| {
            let stem = segment.rsplit_once('.').map_or(segment, |(stem, _)| stem);
            looks_random(stem, MIN_SECRET_SEGMENT_CHARS)
        })
        .map(|index| FeedUrlPrivacyWarning::HighEntropyPathSegment { index })
}

fn is_secret_parameter_name(name: &str) -> bool {
    let words = split_words(name);
    if words
        .iter()
        .any(|word| SECRET_QUERY_WORDS.contains(&word.as_str()))
    {
        return true;
    }
    let compact: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    SECRET_QUERY_SUBSTRINGS
        .iter()
        .any(|needle| compact.contains(needle))
}

/// Splits at non-alphanumeric characters and lower-to-upper case changes.
fn split_words(name: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut previous_lower = false;
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            previous_lower = false;
            continue;
        }
        if c.is_ascii_uppercase() && previous_lower && !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
        previous_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        current.push(c.to_ascii_lowercase());
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// A long token-alphabet string mixing letters and digits (or both letter
/// cases) with high per-character entropy.
fn looks_random(value: &str, min_chars: usize) -> bool {
    if value.chars().count() < min_chars
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.~%=+".contains(c))
    {
        return false;
    }
    let has_digit = value.chars().any(|c| c.is_ascii_digit());
    let has_lower = value.chars().any(|c| c.is_ascii_lowercase());
    let has_upper = value.chars().any(|c| c.is_ascii_uppercase());
    let classes = [has_digit, has_lower, has_upper]
        .into_iter()
        .filter(|present| *present)
        .count();
    classes >= 2 && shannon_entropy(value) >= MIN_SECRET_ENTROPY_BITS
}

fn shannon_entropy(value: &str) -> f64 {
    let mut counts: HashMap<char, usize> = HashMap::new();
    for c in value.chars() {
        *counts.entry(c).or_default() += 1;
    }
    let total = value.chars().count() as f64;
    counts
        .values()
        .map(|count| {
            let p = *count as f64 / total;
            -p * p.log2()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warn(url: &str) -> Option<FeedUrlPrivacyWarning> {
        feed_url_privacy_warning(&FeedUrl::parse(url).unwrap())
    }

    #[test]
    fn public_feeds_have_no_warning() {
        for url in [
            "https://feeds.example.com/podcast.xml",
            "https://example.com/feed?format=rss&page=2",
            "https://anchor.example/s/abc123de/podcast/rss",
            "https://feeds.example.com/12345/rss",
            "https://example.com/feed?author=jane",
            "https://example.com:8443/shows/my-great-show/feed.xml",
        ] {
            assert_eq!(warn(url), None, "{url}");
        }
    }

    #[test]
    fn secret_query_names_warn() {
        for (url, name) in [
            ("https://example.com/feed?token=abc", "token"),
            ("https://example.com/feed?auth_token=abc", "auth_token"),
            (
                "https://example.com/feed?X-Amz-Signature=abc",
                "X-Amz-Signature",
            ),
            ("https://example.com/feed?accessKey=abc", "accessKey"),
            ("https://example.com/feed?a=1&uid=42", "uid"),
            ("https://example.com/feed?apitoken=x", "apitoken"),
            ("https://example.com/feed?PRIVATE=1", "PRIVATE"),
            ("https://example.com/feed?session_id=1", "session_id"),
        ] {
            assert_eq!(
                warn(url),
                Some(FeedUrlPrivacyWarning::SecretQueryParameter { name: name.into() }),
                "{url}"
            );
        }
    }

    #[test]
    fn known_member_hosts_warn() {
        assert_eq!(
            warn("https://www.patreon.com/rss/show"),
            Some(FeedUrlPrivacyWarning::PrivateFeedHost {
                host: "www.patreon.com".into()
            })
        );
        assert_eq!(warn("https://notpatreon.com/rss"), None);
    }

    #[test]
    fn high_entropy_values_and_segments_warn() {
        assert_eq!(
            warn("https://example.com/feed?c=Zx81kQ2mP0aLr7Vb"),
            Some(FeedUrlPrivacyWarning::HighEntropyQueryValue { name: "c".into() })
        );
        assert_eq!(warn("https://example.com/feed?c=aaaaaaaaaaaaaaaa1"), None);
        assert_eq!(
            warn("https://example.com/feeds/q8Zr2LmK0pXv7TnB4sYc1HdWe9GuJa3F.rss"),
            Some(FeedUrlPrivacyWarning::HighEntropyPathSegment { index: 1 })
        );
        // Below the segment length: an ordinary short id.
        assert_eq!(warn("https://example.com/feeds/q8Zr2LmK0pXv7TnB.rss"), None);
    }

    #[test]
    fn warnings_render_without_the_secret() {
        let warning = warn("https://example.com/feed?c=Zx81kQ2mP0aLr7Vb").unwrap();
        assert!(!warning.to_string().contains("Zx81"));
    }
}
