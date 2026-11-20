//! Podcast search hits and their merge with music search results.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use federation_net::EndpointId;
use serde::{Deserialize, Serialize};

use crate::normalization::{normalize_name, tokenize};
use crate::podcast::{FeedUrl, PodcastId};
use crate::record::{ItemKind, LibraryItem};

use super::card::{MAX_INDEX_TOKEN_BYTES, PodcastCard, SignedPodcastRecord, compare_records};

/// Maximum distinct query tokens used by a podcast text search.
pub const MAX_PODCAST_QUERY_TOKENS: usize = 8;
/// Maximum publishers reported per hit and per claimed feed URL.
pub const MAX_HIT_PUBLISHERS: usize = 64;
/// Maximum claimed moved feed URLs reported per hit.
pub const MAX_HIT_FEED_URL_CLAIMS: usize = 8;

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
///
/// Only [`Self::feed_url`] is bound to the podcast: the id is its hash. Every
/// other field is a claim of some publisher device, and device identities are
/// free, so neither the metadata nor the number of publishers is a trust
/// signal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastSearchHit {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// The URL `podcast_id` is derived from
    /// (`PodcastId::from_feed_url(feed_url) == podcast_id`). Subscribe with
    /// this URL; follow the feed's own HTTP redirects and
    /// `itunes:new-feed-url` from there.
    pub feed_url: FeedUrl,
    /// Representative card for display: a card whose `feed_url` is the
    /// origin URL when one exists, then the title most publishers agree on,
    /// then the smallest publisher key. Its metadata is a publisher's claim.
    pub card: PodcastCard,
    /// Sorted distinct publisher devices with a current valid card, at most
    /// [`MAX_HIT_PUBLISHERS`]. These are devices, not users or subscribers:
    /// one user may publish from several devices, and anyone can mint
    /// devices.
    pub publishers: Vec<EndpointId>,
    /// Feed URLs other than [`Self::feed_url`] that publishers claim the
    /// podcast moved to, most claimed first, at most
    /// [`MAX_HIT_FEED_URL_CLAIMS`]. Unverified: never fetch one as this
    /// podcast or record it as an alias unless the feed at
    /// [`Self::feed_url`] itself redirects there.
    pub claimed_feed_urls: Vec<ClaimedFeedUrl>,
}

impl PodcastSearchHit {
    /// Always [`SearchHitKind::Podcast`].
    pub fn kind(&self) -> SearchHitKind {
        SearchHitKind::Podcast
    }
}

/// An unverified moved-feed URL claimed by some publishers of a podcast.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedFeedUrl {
    /// Claimed current feed URL.
    pub feed_url: FeedUrl,
    /// Sorted distinct publisher devices claiming it, at most
    /// [`MAX_HIT_PUBLISHERS`].
    pub publishers: Vec<EndpointId>,
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
            let mut title_votes: HashMap<String, usize> = HashMap::new();
            for card in &cards {
                *title_votes.entry(normalize_name(&card.title)).or_default() += 1;
            }
            // Publisher-controlled times and revisions never break ties.
            let card = cards
                .iter()
                .max_by_key(|card| {
                    (
                        card.feed_url_is_origin(),
                        title_votes[&normalize_name(&card.title)],
                        Reverse(*card.publisher.as_bytes()),
                    )
                })
                .cloned()
                .expect("a group has at least one card");
            let mut claims: BTreeMap<&FeedUrl, Vec<EndpointId>> = BTreeMap::new();
            for card in cards.iter().filter(|card| !card.feed_url_is_origin()) {
                claims
                    .entry(&card.feed_url)
                    .or_default()
                    .push(card.publisher);
            }
            let mut claimed_feed_urls: Vec<ClaimedFeedUrl> = claims
                .into_iter()
                .map(|(feed_url, publishers)| ClaimedFeedUrl {
                    feed_url: feed_url.clone(),
                    publishers: sorted_publishers(publishers),
                })
                .collect();
            claimed_feed_urls.sort_by(|left, right| {
                right
                    .publishers
                    .len()
                    .cmp(&left.publishers.len())
                    .then_with(|| left.feed_url.cmp(&right.feed_url))
            });
            claimed_feed_urls.truncate(MAX_HIT_FEED_URL_CLAIMS);
            PodcastSearchHit {
                podcast_id,
                feed_url: card.origin_feed_url().clone(),
                publishers: sorted_publishers(cards.iter().map(|card| card.publisher).collect()),
                card,
                claimed_feed_urls,
            }
        })
        .collect()
}

fn sorted_publishers(mut publishers: Vec<EndpointId>) -> Vec<EndpointId> {
    publishers.sort_by_key(|publisher| *publisher.as_bytes());
    publishers.dedup();
    publishers.truncate(MAX_HIT_PUBLISHERS);
    publishers
}

/// Keeps hits whose card is indexed under every query token and ranks
/// them: exact normalized title first, then hits with a card published under
/// the origin feed URL, then title and id. The publisher count is not used:
/// anyone can inflate it with new device identities.
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
            !hit.card.feed_url_is_origin(),
            title,
            hit.podcast_id.clone(),
        )
    });
    hits
}
