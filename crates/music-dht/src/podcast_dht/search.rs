//! Podcast search hits and their merge with music search results.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use federation_net::EndpointId;
use serde::{Deserialize, Serialize};

use crate::normalization::{normalize_name, tokenize};
use crate::podcast::PodcastId;
use crate::record::{ItemKind, LibraryItem};

use super::card::{MAX_INDEX_TOKEN_BYTES, PodcastCard, SignedPodcastRecord, compare_records};

/// Maximum distinct query tokens used by a podcast text search.
pub const MAX_PODCAST_QUERY_TOKENS: usize = 8;
/// Maximum publishers reported per hit.
pub const MAX_HIT_PUBLISHERS: usize = 64;

/// Kind of a federated search hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchHitKind {
    /// A musical artist.
    Artist,
    /// A release.
    Release,
    /// A track.
    Track,
    /// A public podcast card.
    Podcast,
}

impl SearchHitKind {
    /// Stable lowercase name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Artist => "artist",
            Self::Release => "release",
            Self::Track => "track",
            Self::Podcast => "podcast",
        }
    }
}

impl fmt::Display for SearchHitKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<ItemKind> for SearchHitKind {
    fn from(kind: ItemKind) -> Self {
        match kind {
            ItemKind::Artist => Self::Artist,
            ItemKind::Release => Self::Release,
            ItemKind::Track => Self::Track,
        }
    }
}

/// One public podcast found in the discovery overlay.
///
/// Cards of the same podcast published by several devices are merged by
/// `podcast_id`. The hit carries no personal state of any publisher.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastSearchHit {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Representative card: the most common `feed_url` among the
    /// publishers' current cards, then the most recently issued card, then
    /// the greatest publisher key.
    pub card: PodcastCard,
    /// Sorted distinct publisher devices with a current valid card, at most
    /// [`MAX_HIT_PUBLISHERS`]. These are devices, not users or subscribers:
    /// one user may publish from several devices.
    pub publishers: Vec<EndpointId>,
}

impl PodcastSearchHit {
    /// Always [`SearchHitKind::Podcast`].
    pub fn kind(&self) -> SearchHitKind {
        SearchHitKind::Podcast
    }
}

/// A music or podcast result of a federated search.
///
/// Serialized as `{"type": "music" | "podcast", "hit": {...}}`; use
/// [`FederatedSearchHit::kind`] for the fine-grained kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "hit", rename_all = "snake_case")]
pub enum FederatedSearchHit {
    /// A music-DHT record (artist, release or track).
    Music(LibraryItem),
    /// A public podcast.
    Podcast(PodcastSearchHit),
}

impl FederatedSearchHit {
    /// Kind of the hit: `artist`, `release`, `track` or `podcast`.
    pub fn kind(&self) -> SearchHitKind {
        match self {
            Self::Music(item) => item.kind.into(),
            Self::Podcast(_) => SearchHitKind::Podcast,
        }
    }

    /// Display name: the item name or the podcast title.
    pub fn title(&self) -> &str {
        match self {
            Self::Music(item) => &item.name,
            Self::Podcast(hit) => &hit.card.title,
        }
    }
}

/// Merges music results (as returned by `MusicDhtService::search_network`)
/// with podcast hits for the same query.
///
/// Results whose normalized name equals the normalized query come first;
/// within each tier music keeps its order and precedes podcasts, which keep
/// theirs. Existing music consumers that only read `SearchOutcome` are
/// unaffected.
pub fn merge_search_results(
    query: &str,
    music: Vec<LibraryItem>,
    podcasts: Vec<PodcastSearchHit>,
) -> Vec<FederatedSearchHit> {
    let normalized = normalize_name(query);
    let mut exact = Vec::new();
    let mut rest = Vec::new();
    let hits = music
        .into_iter()
        .map(FederatedSearchHit::Music)
        .chain(podcasts.into_iter().map(FederatedSearchHit::Podcast));
    for hit in hits {
        let is_exact = !normalized.is_empty() && normalize_name(hit.title()) == normalized;
        if is_exact {
            exact.push(hit);
        } else {
            rest.push(hit);
        }
    }
    exact.extend(rest);
    exact
}

/// Distinct normalized query tokens, at most [`MAX_PODCAST_QUERY_TOKENS`].
pub fn podcast_query_tokens(query: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    tokenize(&normalize_name(query))
        .into_iter()
        .filter(|token| token.len() <= MAX_INDEX_TOKEN_BYTES && seen.insert(token.clone()))
        .take(MAX_PODCAST_QUERY_TOKENS)
        .collect()
}

/// Keeps the winning record per `(podcast_id, publisher)` and groups the
/// cards that are still active by podcast. Records must be verified.
pub(crate) fn aggregate_hits(
    records: impl IntoIterator<Item = SignedPodcastRecord>,
) -> Vec<PodcastSearchHit> {
    let mut winners: HashMap<(PodcastId, EndpointId), SignedPodcastRecord> = HashMap::new();
    for record in records {
        let owner = (record.podcast_id().clone(), record.publisher());
        match winners.get(&owner) {
            Some(current) if compare_records(current, &record).is_ge() => {}
            _ => {
                winners.insert(owner, record);
            }
        }
    }
    let mut by_podcast: BTreeMap<PodcastId, Vec<PodcastCard>> = BTreeMap::new();
    for record in winners.into_values() {
        if let Some(card) = record.card() {
            by_podcast
                .entry(card.podcast_id.clone())
                .or_default()
                .push(card.clone());
        }
    }
    by_podcast
        .into_iter()
        .map(|(podcast_id, cards)| {
            let mut feed_votes: HashMap<&str, usize> = HashMap::new();
            for card in &cards {
                *feed_votes.entry(card.feed_url.as_str()).or_default() += 1;
            }
            let card = cards
                .iter()
                .max_by_key(|card| {
                    (
                        feed_votes[card.feed_url.as_str()],
                        card.issued_at_ms,
                        *card.publisher.as_bytes(),
                    )
                })
                .cloned()
                .expect("a group has at least one card");
            let mut publishers: Vec<EndpointId> = cards.iter().map(|card| card.publisher).collect();
            publishers.sort_by_key(|publisher| *publisher.as_bytes());
            publishers.dedup();
            publishers.truncate(MAX_HIT_PUBLISHERS);
            PodcastSearchHit {
                podcast_id,
                card,
                publishers,
            }
        })
        .collect()
}

/// Keeps hits whose card is indexed under every query token and ranks
/// them: exact normalized title first, then more publishers, then title and
/// id.
pub(crate) fn rank_hits(
    mut hits: Vec<PodcastSearchHit>,
    query: &str,
    tokens: &[String],
) -> Vec<PodcastSearchHit> {
    let normalized_query = normalize_name(query);
    hits.retain(|hit| {
        let indexed = hit.card.index_tokens();
        tokens.iter().all(|token| indexed.contains(token))
    });
    hits.sort_by_cached_key(|hit| {
        let title = normalize_name(&hit.card.title);
        (
            title != normalized_query,
            Reverse(hit.publishers.len()),
            title,
            hit.podcast_id.clone(),
        )
    });
    hits
}
