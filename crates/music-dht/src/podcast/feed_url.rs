//! Feed URL canonicalization and feed aliases.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::{MAX_FEED_ALIASES, MAX_URL_BYTES, PodcastError, PodcastId, PodcastResult};

/// Canonical podcast feed URL.
///
/// Canonicalization ([`FeedUrl::parse`]) is deliberately small so that every
/// client, whatever its URL library, produces the same bytes:
///
/// 1. Only `http://` and `https://` (any case) are accepted; the scheme is
///    lowercased.
/// 2. Userinfo (`user:pass@`), whitespace, control characters and `\` are
///    rejected.
/// 3. The host is lowercased. Non-ASCII hosts are rejected: pass the IDNA
///    (`xn--`) form, as produced by WHATWG URL parsers. IPv6 literals are
///    accepted in brackets.
/// 4. An empty port and the scheme's default port (80, 443) are dropped;
///    other ports lose leading zeros.
/// 5. The fragment is dropped.
/// 6. An empty path becomes `/`. The path and the **whole** query string are
///    otherwise kept byte-for-byte, including parameter order, case and
///    percent-encoding; only non-ASCII bytes are percent-encoded (`%XX`,
///    uppercase hex) so the result is ASCII.
///
/// The canonical form is at most [`MAX_URL_BYTES`] bytes and canonicalizing it
/// again is the identity. Deserialization accepts only canonical strings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct FeedUrl(String);

impl FeedUrl {
    /// Canonicalizes an absolute `http(s)` feed URL.
    pub fn parse(input: &str) -> PodcastResult<Self> {
        canonicalize(input).map(Self)
    }

    /// Canonicalizes a URL typed or pasted by a user.
    ///
    /// Surrounding whitespace is trimmed, `feed:`, `podcast:`, `pcast:` and
    /// `itpc:` prefixes map to `https://` (or to the embedded `http(s)` URL),
    /// and input without a scheme defaults to `https://`.
    pub fn from_user_input(input: &str) -> PodcastResult<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(invalid("feed URL is empty"));
        }
        let lower = trimmed.to_ascii_lowercase();
        let mut candidate = trimmed.to_string();
        for scheme in ["feed:", "podcast:", "pcast:", "itpc:"] {
            if lower.starts_with(scheme) {
                let rest = &trimmed[scheme.len()..];
                let rest_lower = rest.to_ascii_lowercase();
                candidate =
                    if rest_lower.starts_with("http://") || rest_lower.starts_with("https://") {
                        rest.to_string()
                    } else {
                        format!("https://{}", rest.trim_start_matches('/'))
                    };
                break;
            }
        }
        if !has_scheme(&candidate) {
            candidate = format!("https://{candidate}");
        }
        Self::parse(&candidate)
    }

    /// Canonical string form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FeedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for FeedUrl {
    type Error = PodcastError;

    fn try_from(value: String) -> PodcastResult<Self> {
        if canonicalize(&value)? == value {
            Ok(Self(value))
        } else {
            Err(invalid("feed URL is not in canonical form"))
        }
    }
}

impl From<FeedUrl> for String {
    fn from(value: FeedUrl) -> String {
        value.0
    }
}

impl AsRef<str> for FeedUrl {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn invalid(reason: &str) -> PodcastError {
    PodcastError::InvalidFeedUrl(reason.to_string())
}

fn has_scheme(text: &str) -> bool {
    let Some((scheme, rest)) = text.split_once(':') else {
        return false;
    };
    let mut chars = scheme.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        && rest.starts_with("//")
}

fn canonicalize(input: &str) -> PodcastResult<String> {
    if input.len() > MAX_URL_BYTES * 3 {
        return Err(invalid("feed URL is too long"));
    }
    if input.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(invalid("feed URL must not contain whitespace"));
    }
    if input.contains('\\') {
        return Err(invalid("feed URL must not contain '\\'"));
    }
    let (scheme, rest) = input
        .split_once("://")
        .ok_or_else(|| invalid("feed URL must start with http:// or https://"))?;
    let (scheme, default_port) = if scheme.eq_ignore_ascii_case("http") {
        ("http", 80)
    } else if scheme.eq_ignore_ascii_case("https") {
        ("https", 443)
    } else {
        return Err(invalid("only http and https feed URLs are supported"));
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    if authority.contains('@') {
        return Err(invalid("feed URL must not contain credentials"));
    }
    let (host, port) = split_host_port(authority)?;
    let host = canonical_host(host)?;
    let port = match port {
        None | Some("") => None,
        Some(port) => {
            if port.len() > 5 || !port.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid("invalid feed URL port"));
            }
            match port.parse::<u32>() {
                Ok(value) if (1..=65_535).contains(&value) => {
                    (value != default_port).then_some(value)
                }
                _ => return Err(invalid("invalid feed URL port")),
            }
        }
    };
    let tail = tail.split('#').next().unwrap_or_default();
    let (path, query) = match tail.find('?') {
        Some(at) => tail.split_at(at),
        None => (tail, ""),
    };
    let mut out = String::with_capacity(input.len() + 8);
    out.push_str(scheme);
    out.push_str("://");
    out.push_str(&host);
    if let Some(port) = port {
        out.push(':');
        out.push_str(&port.to_string());
    }
    if path.is_empty() {
        out.push('/');
    } else {
        push_ascii(&mut out, path);
    }
    push_ascii(&mut out, query);
    if out.len() > MAX_URL_BYTES {
        return Err(invalid("feed URL is too long"));
    }
    Ok(out)
}

fn split_host_port(authority: &str) -> PodcastResult<(&str, Option<&str>)> {
    if authority.starts_with('[') {
        let close = authority
            .find(']')
            .ok_or_else(|| invalid("unterminated IPv6 host"))?;
        let (host, after) = authority.split_at(close + 1);
        return match after {
            "" => Ok((host, None)),
            _ => after
                .strip_prefix(':')
                .map(|port| (host, Some(port)))
                .ok_or_else(|| invalid("invalid feed URL host")),
        };
    }
    Ok(match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    })
}

fn canonical_host(host: &str) -> PodcastResult<String> {
    if host.is_empty() {
        return Err(invalid("feed URL has no host"));
    }
    if !host.is_ascii() {
        return Err(invalid(
            "feed URL host must be ASCII (use the IDNA xn-- form)",
        ));
    }
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        if inner.is_empty()
            || !inner
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || matches!(b, b':' | b'.'))
        {
            return Err(invalid("invalid IPv6 host"));
        }
        return Ok(host.to_ascii_lowercase());
    }
    if !host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
        || !host.bytes().any(|b| b.is_ascii_alphanumeric())
    {
        return Err(invalid("invalid feed URL host"));
    }
    Ok(host.to_ascii_lowercase())
}

fn push_ascii(out: &mut String, value: &str) {
    for ch in value.chars() {
        if ch.is_ascii() {
            out.push(ch);
        } else {
            let mut buf = [0u8; 4];
            for byte in ch.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{byte:02X}"));
            }
        }
    }
}

/// Resolves the podcast a canonical feed URL belongs to.
///
/// `known` yields `(podcast_id, url)` pairs of every URL already associated
/// with a podcast: the URL of its first subscription, its current fetch URL
/// and its feed aliases. When `url` matches, the existing id is returned (the
/// smallest id if, after a concurrent offline subscription, several podcasts
/// share the URL). Otherwise a new id is derived from `url`.
///
/// Re-subscribing through a redirect target or an `itunes:new-feed-url`
/// therefore reuses the original podcast, its progress and its history.
pub fn resolve_podcast_id<'a>(
    url: &FeedUrl,
    known: impl IntoIterator<Item = (&'a PodcastId, &'a FeedUrl)>,
) -> PodcastId {
    known
        .into_iter()
        .filter(|(_, alias)| *alias == url)
        .map(|(id, _)| id)
        .min()
        .cloned()
        .unwrap_or_else(|| PodcastId::from_feed_url(url))
}

/// Merges two feed alias sets of `podcast` as a bounded grow-only set.
///
/// The result is sorted and duplicate-free. The URL from which `podcast` was
/// derived is always kept; of the other URLs the [`MAX_FEED_ALIASES`]
/// lexicographically smallest are kept. Taking the smallest `k` elements of a
/// union is commutative, associative and idempotent, so devices converge
/// regardless of delivery order.
pub fn merge_feed_aliases(podcast: &PodcastId, a: &[FeedUrl], b: &[FeedUrl]) -> Vec<FeedUrl> {
    let mut origin = None;
    let mut rest = BTreeSet::new();
    for url in a.iter().chain(b) {
        if PodcastId::from_feed_url(url) == *podcast {
            origin = Some(url.clone());
        } else {
            rest.insert(url.clone());
        }
    }
    let mut out: Vec<FeedUrl> = rest.into_iter().take(MAX_FEED_ALIASES).collect();
    if let Some(origin) = origin {
        out.push(origin);
        out.sort();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canon(input: &str) -> String {
        FeedUrl::parse(input).unwrap().as_str().to_string()
    }

    #[test]
    fn canonicalization_rules() {
        let cases = [
            (
                "https://example.com/feed.xml",
                "https://example.com/feed.xml",
            ),
            (
                "HTTPS://Example.COM/Feed.XML",
                "https://example.com/Feed.XML",
            ),
            ("http://example.com:80/rss", "http://example.com/rss"),
            ("https://example.com:443/rss", "https://example.com/rss"),
            ("http://example.com:443/rss", "http://example.com:443/rss"),
            (
                "https://example.com:08080/rss",
                "https://example.com:8080/rss",
            ),
            ("https://example.com:/rss", "https://example.com/rss"),
            ("https://example.com", "https://example.com/"),
            ("https://example.com?x=1", "https://example.com/?x=1"),
            ("https://example.com/rss#latest", "https://example.com/rss"),
            ("https://example.com/rss?#x", "https://example.com/rss?"),
            (
                "https://example.com/a/../b/./c?b=2&a=1&A=%2f&token=XyZ",
                "https://example.com/a/../b/./c?b=2&a=1&A=%2f&token=XyZ",
            ),
            (
                "https://example.com/rss?url=https://other.test/x?y#frag",
                "https://example.com/rss?url=https://other.test/x?y",
            ),
            (
                "https://example.com/подкаст?q=ё",
                "https://example.com/%D0%BF%D0%BE%D0%B4%D0%BA%D0%B0%D1%81%D1%82?q=%D1%91",
            ),
            (
                "https://[2001:DB8::1]:443/feed",
                "https://[2001:db8::1]/feed",
            ),
            ("https://[::1]:8443/feed", "https://[::1]:8443/feed"),
            (
                "https://xn--d1acufc.xn--p1ai/rss",
                "https://xn--d1acufc.xn--p1ai/rss",
            ),
            (
                "https://my_host.example./rss",
                "https://my_host.example./rss",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(canon(input), expected, "{input:?}");
            // Idempotent.
            assert_eq!(canon(expected), expected, "{expected:?}");
        }
    }

    #[test]
    fn rejected_urls() {
        for bad in [
            "",
            "example.com/feed",
            "ftp://example.com/feed",
            "file:///etc/passwd",
            "javascript:alert(1)",
            "https://",
            "https:///path",
            "https://user:pass@example.com/feed",
            "https://token@example.com/feed",
            "https://example.com/my feed",
            " https://example.com/feed",
            "https://example.com/feed\n",
            "https://example.com\\feed",
            "https://пример.рф/rss",
            "https://example.com:0/rss",
            "https://example.com:65536/rss",
            "https://example.com:12a/rss",
            "https://[::1/feed",
            "https://[zz]/feed",
            "https://exa%6dple.com/feed",
            "https://.../feed",
        ] {
            assert!(FeedUrl::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn length_limit_is_inclusive() {
        let prefix = "https://a.test/";
        let at = format!("{prefix}{}", "x".repeat(MAX_URL_BYTES - prefix.len()));
        assert_eq!(canon(&at).len(), MAX_URL_BYTES);
        let over = format!("{at}x");
        assert!(FeedUrl::parse(&over).is_err());
    }

    #[test]
    fn user_input_forms() {
        let ok = [
            (
                "https://podster.test/rss.xml?pid=778",
                "https://podster.test/rss.xml?pid=778",
            ),
            ("  http://a.test/feed  ", "http://a.test/feed"),
            ("a.test/feed.xml", "https://a.test/feed.xml"),
            ("feed://a.test/rss", "https://a.test/rss"),
            ("feed:https://a.test/rss", "https://a.test/rss"),
            ("podcast://a.test/rss", "https://a.test/rss"),
            ("itpc://a.test/rss", "https://a.test/rss"),
            ("HTTPS://A.TEST/Feed", "https://a.test/Feed"),
            ("a.test:8080/rss", "https://a.test:8080/rss"),
            (
                "example.test/rss?src=https://other.test",
                "https://example.test/rss?src=https://other.test",
            ),
            (
                "localhost:8080/feed?u=http://x.test/",
                "https://localhost:8080/feed?u=http://x.test/",
            ),
        ];
        for (input, expected) in ok {
            assert_eq!(
                FeedUrl::from_user_input(input).unwrap().as_str(),
                expected,
                "{input:?}"
            );
        }
        for bad in [
            "",
            "   ",
            "ftp://a.test/feed",
            "https://a.test/my feed",
            "https://",
            "file:///etc/passwd",
            "javascript:alert(1)",
        ] {
            assert!(FeedUrl::from_user_input(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn deserialization_requires_canonical_form() {
        let url = FeedUrl::parse("https://example.com/rss").unwrap();
        let json = serde_json::to_string(&url).unwrap();
        assert_eq!(serde_json::from_str::<FeedUrl>(&json).unwrap(), url);
        assert!(serde_json::from_str::<FeedUrl>("\"HTTPS://example.com/rss\"").is_err());
        assert!(serde_json::from_str::<FeedUrl>("\"https://example.com/rss#x\"").is_err());
        assert!(serde_json::from_str::<FeedUrl>("\"ftp://example.com/rss\"").is_err());
    }

    #[test]
    fn query_parameters_change_identity() {
        let a = PodcastId::from_feed_url(&FeedUrl::parse("https://a.test/rss?id=1").unwrap());
        let b = PodcastId::from_feed_url(&FeedUrl::parse("https://a.test/rss?id=2").unwrap());
        let c = PodcastId::from_feed_url(&FeedUrl::parse("HTTPS://A.test:443/rss?id=1#x").unwrap());
        assert_ne!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn aliases_resolve_to_the_original_podcast() {
        let original = FeedUrl::parse("http://old.test/feed").unwrap();
        let moved = FeedUrl::parse("https://new.test/feed").unwrap();
        let id = PodcastId::from_feed_url(&original);
        let known = [(&id, &original), (&id, &moved)];
        assert_eq!(resolve_podcast_id(&moved, known), id);
        assert_eq!(resolve_podcast_id(&original, known), id);
        let unrelated = FeedUrl::parse("https://other.test/feed").unwrap();
        assert_eq!(
            resolve_podcast_id(&unrelated, known),
            PodcastId::from_feed_url(&unrelated)
        );
        // Ambiguity after concurrent offline subscriptions resolves to the
        // smallest id, independent of iteration order.
        let other = PodcastId::from_feed_url(&moved);
        let ambiguous = [(&id, &moved), (&other, &moved)];
        let reversed = [(&other, &moved), (&id, &moved)];
        let expected = (&id).min(&other).clone();
        assert_eq!(resolve_podcast_id(&moved, ambiguous), expected);
        assert_eq!(resolve_podcast_id(&moved, reversed), expected);
    }

    #[test]
    fn alias_merge_is_a_bounded_semilattice() {
        let origin = FeedUrl::parse("https://zzz.test/origin").unwrap();
        let id = PodcastId::from_feed_url(&origin);
        let urls: Vec<FeedUrl> = (0..(MAX_FEED_ALIASES + 4))
            .map(|i| FeedUrl::parse(&format!("https://a.test/{i:02}")).unwrap())
            .collect();
        let a: Vec<FeedUrl> = urls[..5].to_vec();
        let b: Vec<FeedUrl> = urls[3..].iter().cloned().chain([origin.clone()]).collect();
        let c: Vec<FeedUrl> = urls[7..].to_vec();
        let ab = merge_feed_aliases(&id, &a, &b);
        assert_eq!(ab, merge_feed_aliases(&id, &b, &a));
        assert_eq!(ab, merge_feed_aliases(&id, &ab, &ab));
        let left = merge_feed_aliases(&id, &ab, &c);
        let right = merge_feed_aliases(&id, &a, &merge_feed_aliases(&id, &b, &c));
        assert_eq!(left, right);
        assert_eq!(left.len(), MAX_FEED_ALIASES + 1);
        assert!(left.contains(&origin));
        assert!(left.windows(2).all(|w| w[0] < w[1]));
    }
}
