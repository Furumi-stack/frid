//! Artist name normalization and tokenization.

use unicode_normalization::UnicodeNormalization;

/// Normalizes an artist name for indexing and comparison.
///
/// The algorithm is: Unicode NFKC normalization, lowercasing, replacing every
/// non-alphanumeric character with a space, collapsing repeated spaces and
/// trimming. The result is deterministic for a given input.
///
/// ```
/// use artist_dht::normalize_artist_name;
/// assert_eq!(normalize_artist_name("Massive Attack"), "massive attack");
/// assert_eq!(normalize_artist_name("  MASSIVE   ATTACK "), "massive attack");
/// assert_eq!(normalize_artist_name("Massive-Attack"), "massive attack");
/// assert_eq!(normalize_artist_name("Björk"), "björk");
/// ```
pub fn normalize_artist_name(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut pending_space = false;
    for ch in input.nfkc() {
        if ch.is_alphanumeric() {
            if pending_space && !result.is_empty() {
                result.push(' ');
            }
            pending_space = false;
            for lower in ch.to_lowercase() {
                result.push(lower);
            }
        } else {
            pending_space = true;
        }
    }
    result
}

/// Splits a normalized name into search tokens.
///
/// Empty tokens are ignored.
///
/// ```
/// use artist_dht::tokenize;
/// assert_eq!(tokenize("massive attack"), vec!["massive", "attack"]);
/// ```
pub fn tokenize(normalized: &str) -> Vec<String> {
    normalized
        .split(' ')
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_is_deterministic() {
        let a = normalize_artist_name("Massive Attack");
        let b = normalize_artist_name("Massive Attack");
        assert_eq!(a, b);
        assert_eq!(a, "massive attack");
    }

    #[test]
    fn normalization_handles_case_and_whitespace() {
        assert_eq!(
            normalize_artist_name("  MASSIVE   ATTACK "),
            "massive attack"
        );
        assert_eq!(normalize_artist_name("Massive-Attack"), "massive attack");
        assert_eq!(
            normalize_artist_name("Massive___Attack!!!"),
            "massive attack"
        );
        assert_eq!(normalize_artist_name("Björk"), "björk");
        assert_eq!(normalize_artist_name("   "), "");
        assert_eq!(normalize_artist_name("!!!"), "");
    }

    #[test]
    fn normalization_applies_nfkc() {
        // U+FF21 FULLWIDTH LATIN CAPITAL LETTER A normalizes to 'A' → 'a'.
        assert_eq!(normalize_artist_name("\u{FF21}BBA"), "abba");
    }

    #[test]
    fn tokenize_splits_and_skips_empty() {
        assert_eq!(tokenize("massive attack"), vec!["massive", "attack"]);
        assert_eq!(tokenize(""), Vec::<String>::new());
        assert_eq!(tokenize("solo"), vec!["solo"]);
    }
}
