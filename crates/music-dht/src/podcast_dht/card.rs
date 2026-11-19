//! Signed public podcast cards, tombstones, DHT keys and their canonical
//! signing encoding.
//!
//! Everything here is pure and deterministic: no clock, no I/O. Callers pass
//! the current time to the validation functions.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::Duration;

use federation_net::{EndpointId, NetworkId, Signature};
use serde::{Deserialize, Serialize};

use crate::error::{MusicDhtError, Result};
use crate::normalization::{normalize_name, tokenize};
use crate::podcast::{
    FeedUrl, MAX_URL_BYTES, PodcastId, SubscriptionRecord, bounded_text, is_http_url,
};

use super::privacy::{FeedUrlPrivacyWarning, feed_url_privacy_warning};

/// Current card, tombstone and overlay wire version.
pub const PODCAST_DHT_PROTOCOL_VERSION: u16 = 1;
/// Signing domain of a [`PodcastCard`]. Changing it invalidates every card.
pub const PODCAST_CARD_SIGNING_DOMAIN: &str = "furumi-podcast-card:v1";
/// Signing domain of a [`PodcastTombstone`].
pub const PODCAST_TOMBSTONE_SIGNING_DOMAIN: &str = "furumi-podcast-card-tombstone:v1";
/// BLAKE3 derivation domain of [`PodcastDhtKey`].
pub const PODCAST_DHT_KEY_DOMAIN: &str = "furumi-podcast-dht:key:v1";

/// Maximum lifetime of a card: the main DHT's active-record TTL.
pub const PODCAST_CARD_TTL: Duration = crate::record::ACTIVE_RECORD_TTL;
/// Maximum lifetime of a tombstone: the main DHT's tombstone TTL. It exceeds
/// [`PODCAST_CARD_TTL`], so a card issued before a tombstone always expires
/// before the tombstone does.
pub const PODCAST_TOMBSTONE_TTL: Duration = crate::record::TOMBSTONE_TTL;
/// Tolerated clock skew for `issued_at_ms` in the future.
pub const PODCAST_DHT_MAX_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);

/// Maximum characters of a card title.
pub const MAX_CARD_TITLE_CHARS: usize = 300;
/// Maximum characters of a card author.
pub const MAX_CARD_AUTHOR_CHARS: usize = 300;
/// Maximum characters of a card description.
pub const MAX_CARD_DESCRIPTION_CHARS: usize = 1_000;
/// Maximum bytes of a card language tag (BCP 47 shape: ASCII letters,
/// digits and `-`).
pub const MAX_CARD_LANGUAGE_BYTES: usize = 35;
/// Maximum categories on a card.
pub const MAX_CARD_CATEGORIES: usize = 8;
/// Maximum characters of one category.
pub const MAX_CARD_CATEGORY_CHARS: usize = 64;
/// Maximum indexed search tokens of a card (title tokens, then author tokens).
pub const MAX_CARD_INDEX_TOKENS: usize = 32;
/// Maximum bytes of one indexed or query token. Longer tokens are not indexed.
pub const MAX_INDEX_TOKEN_BYTES: usize = 64;
/// Maximum DHT keys a record is stored under: the podcast-id key plus one
/// key per indexed token.
pub const MAX_RECORD_DHT_KEYS: usize = MAX_CARD_INDEX_TOKENS + 1;
/// Maximum postcard-encoded size of one signed record.
pub const MAX_PODCAST_RECORD_BYTES: usize = 24 * 1024;

const KEY_KIND_PODCAST: u8 = 1;
const KEY_KIND_TOKEN: u8 = 2;

/// A 256-bit point in the podcast-discovery key space.
///
/// `BLAKE3("furumi-podcast-dht:key:v1" || 0x00 || network_id || kind || material)`
/// with kind `0x01` and the podcast id string for the id key, or kind `0x02`
/// and a normalized token for a text key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PodcastDhtKey([u8; 32]);

impl PodcastDhtKey {
    /// Key under which every card and tombstone of `podcast_id` is stored.
    pub fn podcast(network_id: &NetworkId, podcast_id: &PodcastId) -> Self {
        Self::derive(network_id, KEY_KIND_PODCAST, podcast_id.as_str())
    }

    /// Key of one normalized search token (see [`card_index_tokens`]).
    pub fn token(network_id: &NetworkId, token: &str) -> Self {
        Self::derive(network_id, KEY_KIND_TOKEN, token)
    }

    fn derive(network_id: &NetworkId, kind: u8, material: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(PODCAST_DHT_KEY_DOMAIN.as_bytes());
        hasher.update(&[0]);
        hasher.update(network_id.as_bytes());
        hasher.update(&[kind]);
        hasher.update(material.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// Creates a key from raw bytes received on the wire.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Raw key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Indexed search tokens of a card: distinct [`normalize_name`] tokens of the
/// title, then of the author, in order, skipping tokens longer than
/// [`MAX_INDEX_TOKEN_BYTES`], at most [`MAX_CARD_INDEX_TOKENS`].
pub fn card_index_tokens(title: &str, author: Option<&str>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    let sources = std::iter::once(title).chain(author);
    for source in sources {
        for token in tokenize(&normalize_name(source)) {
            if tokens.len() == MAX_CARD_INDEX_TOKENS {
                return tokens;
            }
            if token.len() <= MAX_INDEX_TOKEN_BYTES && seen.insert(token.clone()) {
                tokens.push(token);
            }
        }
    }
    tokens
}

/// Unsigned public projection of a podcast prepared by its publisher.
///
/// It is built from the user's subscription ([`PodcastCardSpec::from_subscription`])
/// and contains only publisher metadata: never progress, listening history,
/// subscription times, operation ids, device names or episodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastCardSpec {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Canonical URL the feed is currently fetched from.
    pub feed_url: FeedUrl,
    /// URL `podcast_id` was derived from, when it differs from `feed_url`.
    pub id_feed_url: Option<FeedUrl>,
    /// Podcast title.
    pub title: String,
    /// Author.
    pub author: Option<String>,
    /// Plain-text description.
    pub description: Option<String>,
    /// Artwork URL.
    pub image_url: Option<String>,
    /// Publisher web page.
    pub link: Option<String>,
    /// Feed language tag, for example `en` or `pt-BR`.
    pub language: Option<String>,
    /// Feed categories.
    pub categories: Vec<String>,
    /// Number of episodes in the feed, when known.
    pub episode_count: Option<u32>,
    /// Publication time of the newest episode in unix milliseconds.
    pub latest_episode_at_ms: Option<i64>,
}

impl PodcastCardSpec {
    /// Builds the bounded public projection of a subscription.
    ///
    /// Only `podcast_id`, the feed URLs and the channel metadata are read.
    /// `id_feed_url` is the alias that hashes to `podcast_id`
    /// ([`crate::podcast::merge_feed_aliases`] always keeps it). Fails when no
    /// such URL is known. Callers may then set `language`, `categories`,
    /// `episode_count` and `latest_episode_at_ms` from their feed cache.
    pub fn from_subscription(subscription: &SubscriptionRecord) -> Result<Self> {
        let origin = subscription
            .feed_urls()
            .into_iter()
            .find(|url| PodcastId::from_feed_url(url) == subscription.podcast_id)
            .ok_or_else(|| {
                publication_error("the feed URL the podcast id was derived from is unknown")
            })?;
        let feed_url = subscription.podcast.feed_url.clone();
        let meta = &subscription.podcast;
        Ok(Self {
            podcast_id: subscription.podcast_id.clone(),
            id_feed_url: (origin != feed_url).then_some(origin),
            feed_url,
            title: meta.title.clone(),
            author: meta.author.clone(),
            description: meta.description.clone(),
            image_url: meta.image_url.clone(),
            link: meta.link.clone(),
            language: None,
            categories: Vec::new(),
            episode_count: None,
            latest_episode_at_ms: None,
        }
        .bounded())
    }

    /// Returns a copy that fits the card bounds: text is truncated with
    /// [`bounded_text`], control characters are removed, invalid URLs and
    /// language tags are dropped, and categories are trimmed, deduplicated and
    /// bounded. An empty title becomes `"Untitled podcast"`.
    pub fn bounded(&self) -> Self {
        let title = clean_line(&self.title);
        let mut seen = HashSet::new();
        let categories = self
            .categories
            .iter()
            .map(|category| bounded_text(&clean_line(category), MAX_CARD_CATEGORY_CHARS))
            .filter(|category| !category.is_empty() && seen.insert(category.to_lowercase()))
            .take(MAX_CARD_CATEGORIES)
            .collect();
        Self {
            podcast_id: self.podcast_id.clone(),
            feed_url: self.feed_url.clone(),
            id_feed_url: self
                .id_feed_url
                .clone()
                .filter(|origin| *origin != self.feed_url),
            title: if title.is_empty() {
                "Untitled podcast".to_string()
            } else {
                bounded_text(&title, MAX_CARD_TITLE_CHARS)
            },
            author: bounded_line(self.author.as_deref(), MAX_CARD_AUTHOR_CHARS),
            description: self
                .description
                .as_deref()
                .map(|text| bounded_text(clean_block(text).trim(), MAX_CARD_DESCRIPTION_CHARS))
                .filter(|text| !text.is_empty()),
            image_url: self.image_url.clone().filter(|url| is_http_url(url)),
            link: self.link.clone().filter(|url| is_http_url(url)),
            language: self
                .language
                .as_deref()
                .map(str::trim)
                .filter(|language| valid_language(language))
                .map(str::to_string),
            categories,
            episode_count: self.episode_count,
            latest_episode_at_ms: self.latest_episode_at_ms.filter(|at| *at >= 0),
        }
    }

    /// Validates the card bounds without bounding anything.
    pub fn validate(&self) -> Result<()> {
        let id_url = self.id_feed_url.as_ref().unwrap_or(&self.feed_url);
        if PodcastId::from_feed_url(id_url) != self.podcast_id {
            return Err(invalid("podcast id does not match its feed URL"));
        }
        if self.id_feed_url.as_ref() == Some(&self.feed_url) {
            return Err(invalid("id_feed_url must differ from feed_url"));
        }
        check_line("title", &self.title, MAX_CARD_TITLE_CHARS, true)?;
        if let Some(author) = &self.author {
            check_line("author", author, MAX_CARD_AUTHOR_CHARS, false)?;
        }
        if let Some(description) = &self.description {
            check_chars("description", description, MAX_CARD_DESCRIPTION_CHARS)?;
            if description
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t')
            {
                return Err(invalid("description contains control characters"));
            }
        }
        for url in [&self.image_url, &self.link].into_iter().flatten() {
            if !is_http_url(url) {
                return Err(invalid("card URLs must be absolute http(s) URLs"));
            }
        }
        if self
            .language
            .as_deref()
            .is_some_and(|language| !valid_language(language))
        {
            return Err(invalid("invalid language tag"));
        }
        if self.categories.len() > MAX_CARD_CATEGORIES {
            return Err(invalid("too many categories"));
        }
        for category in &self.categories {
            check_line("category", category, MAX_CARD_CATEGORY_CHARS, true)?;
        }
        if self.latest_episode_at_ms.is_some_and(|at| at < 0) {
            return Err(invalid("negative latest_episode_at_ms"));
        }
        Ok(())
    }

    /// Advisory privacy warnings for both feed URLs of the card (see
    /// [`feed_url_privacy_warning`]). The UI must ask before publishing a
    /// card with warnings.
    pub fn privacy_warnings(&self) -> Vec<FeedUrlPrivacyWarning> {
        std::iter::once(&self.feed_url)
            .chain(self.id_feed_url.as_ref())
            .filter_map(feed_url_privacy_warning)
            .collect()
    }

    /// Indexed search tokens of this card.
    pub fn index_tokens(&self) -> Vec<String> {
        card_index_tokens(&self.title, self.author.as_deref())
    }
}

/// A signed public podcast card: one publisher device's projection of one
/// podcast. See `PODCASTS.md`, "Public discovery (podcast_dht)".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastCard {
    /// Record format version ([`PODCAST_DHT_PROTOCOL_VERSION`]).
    pub version: u16,
    /// Network scope; prevents replay into another federation.
    pub network_id: NetworkId,
    /// Publisher device (its persistent transport identity); signs the card.
    pub publisher: EndpointId,
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Canonical URL the feed is currently fetched from.
    pub feed_url: FeedUrl,
    /// URL `podcast_id` was derived from, when it differs from `feed_url`.
    pub id_feed_url: Option<FeedUrl>,
    /// Podcast title.
    pub title: String,
    /// Author.
    pub author: Option<String>,
    /// Plain-text description.
    pub description: Option<String>,
    /// Artwork URL.
    pub image_url: Option<String>,
    /// Publisher web page.
    pub link: Option<String>,
    /// Feed language tag.
    pub language: Option<String>,
    /// Feed categories.
    pub categories: Vec<String>,
    /// Number of episodes in the feed, when known.
    pub episode_count: Option<u32>,
    /// Publication time of the newest episode in unix milliseconds.
    pub latest_episode_at_ms: Option<i64>,
    /// Monotonic per `(podcast_id, publisher)` revision.
    pub revision: u64,
    /// Unix time at which this version was signed.
    pub issued_at_ms: u64,
    /// Unix time after which the card must be discarded.
    pub expires_at_ms: u64,
}

impl PodcastCard {
    /// Builds a card from a spec. `expires_at_ms` is
    /// `issued_at_ms + PODCAST_CARD_TTL`.
    pub fn from_spec(
        spec: PodcastCardSpec,
        network_id: NetworkId,
        publisher: EndpointId,
        revision: u64,
        issued_at_ms: u64,
    ) -> Self {
        Self {
            version: PODCAST_DHT_PROTOCOL_VERSION,
            network_id,
            publisher,
            podcast_id: spec.podcast_id,
            feed_url: spec.feed_url,
            id_feed_url: spec.id_feed_url,
            title: spec.title,
            author: spec.author,
            description: spec.description,
            image_url: spec.image_url,
            link: spec.link,
            language: spec.language,
            categories: spec.categories,
            episode_count: spec.episode_count,
            latest_episode_at_ms: spec.latest_episode_at_ms,
            revision,
            issued_at_ms,
            expires_at_ms: issued_at_ms.saturating_add(PODCAST_CARD_TTL.as_millis() as u64),
        }
    }

    /// The publisher-provided part of the card.
    pub fn spec(&self) -> PodcastCardSpec {
        PodcastCardSpec {
            podcast_id: self.podcast_id.clone(),
            feed_url: self.feed_url.clone(),
            id_feed_url: self.id_feed_url.clone(),
            title: self.title.clone(),
            author: self.author.clone(),
            description: self.description.clone(),
            image_url: self.image_url.clone(),
            link: self.link.clone(),
            language: self.language.clone(),
            categories: self.categories.clone(),
            episode_count: self.episode_count,
            latest_episode_at_ms: self.latest_episode_at_ms,
        }
    }

    /// Indexed search tokens.
    pub fn index_tokens(&self) -> Vec<String> {
        card_index_tokens(&self.title, self.author.as_deref())
    }

    /// Canonical signing input (see `PODCASTS.md`).
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut out = Canonical::new(PODCAST_CARD_SIGNING_DOMAIN);
        out.u16(self.version);
        out.raw(self.network_id.as_bytes());
        out.raw(self.publisher.as_bytes());
        out.str(self.podcast_id.as_str());
        out.str(self.feed_url.as_str());
        out.opt_str(self.id_feed_url.as_ref().map(FeedUrl::as_str));
        out.str(&self.title);
        out.opt_str(self.author.as_deref());
        out.opt_str(self.description.as_deref());
        out.opt_str(self.image_url.as_deref());
        out.opt_str(self.link.as_deref());
        out.opt_str(self.language.as_deref());
        out.u32(self.categories.len() as u32);
        for category in &self.categories {
            out.str(category);
        }
        match self.episode_count {
            Some(count) => {
                out.u8(1);
                out.u32(count);
            }
            None => out.u8(0),
        }
        match self.latest_episode_at_ms {
            Some(at) => {
                out.u8(1);
                out.i64(at);
            }
            None => out.u8(0),
        }
        out.u64(self.revision);
        out.u64(self.issued_at_ms);
        out.u64(self.expires_at_ms);
        out.finish()
    }

    fn validate(&self, expected_network: &NetworkId) -> Result<()> {
        check_header(self.version, &self.network_id, expected_network)?;
        self.spec().validate()?;
        check_lifetime(
            self.revision,
            self.issued_at_ms,
            self.expires_at_ms,
            PODCAST_CARD_TTL,
        )
    }
}

/// A signed retraction of one publisher's card of one podcast.
///
/// It supersedes every card of the same `(podcast_id, publisher)` with a
/// revision lower than or equal to its own and is retained until it expires.
/// `index_keys` lists the text keys of the retracted cards so the tombstone
/// can be stored next to them; it carries hashes, not the tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastTombstone {
    /// Record format version ([`PODCAST_DHT_PROTOCOL_VERSION`]).
    pub version: u16,
    /// Network scope.
    pub network_id: NetworkId,
    /// Publisher device; signs the tombstone.
    pub publisher: EndpointId,
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Revision; greater than that of every retracted card.
    pub revision: u64,
    /// Unix time at which the tombstone was signed.
    pub issued_at_ms: u64,
    /// Unix time after which the tombstone may be discarded.
    pub expires_at_ms: u64,
    /// Sorted, distinct text keys besides the podcast-id key, at most
    /// [`MAX_CARD_INDEX_TOKENS`].
    pub index_keys: Vec<PodcastDhtKey>,
}

impl PodcastTombstone {
    /// Builds a tombstone. `expires_at_ms` is `issued_at_ms + PODCAST_TOMBSTONE_TTL`;
    /// `index_keys` is sorted, deduplicated and stripped of the podcast-id key.
    pub fn new(
        network_id: NetworkId,
        publisher: EndpointId,
        podcast_id: PodcastId,
        revision: u64,
        issued_at_ms: u64,
        index_keys: impl IntoIterator<Item = PodcastDhtKey>,
    ) -> Self {
        let id_key = PodcastDhtKey::podcast(&network_id, &podcast_id);
        let mut index_keys: Vec<_> = index_keys
            .into_iter()
            .filter(|key| *key != id_key)
            .collect();
        index_keys.sort();
        index_keys.dedup();
        index_keys.truncate(MAX_CARD_INDEX_TOKENS);
        Self {
            version: PODCAST_DHT_PROTOCOL_VERSION,
            network_id,
            publisher,
            podcast_id,
            revision,
            issued_at_ms,
            expires_at_ms: issued_at_ms.saturating_add(PODCAST_TOMBSTONE_TTL.as_millis() as u64),
            index_keys,
        }
    }

    /// Canonical signing input (see `PODCASTS.md`).
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut out = Canonical::new(PODCAST_TOMBSTONE_SIGNING_DOMAIN);
        out.u16(self.version);
        out.raw(self.network_id.as_bytes());
        out.raw(self.publisher.as_bytes());
        out.str(self.podcast_id.as_str());
        out.u64(self.revision);
        out.u64(self.issued_at_ms);
        out.u64(self.expires_at_ms);
        out.u32(self.index_keys.len() as u32);
        for key in &self.index_keys {
            out.raw(key.as_bytes());
        }
        out.finish()
    }

    fn validate(&self, expected_network: &NetworkId) -> Result<()> {
        check_header(self.version, &self.network_id, expected_network)?;
        if self.index_keys.len() > MAX_CARD_INDEX_TOKENS {
            return Err(invalid("tombstone has too many keys"));
        }
        let id_key = PodcastDhtKey::podcast(&self.network_id, &self.podcast_id);
        if self.index_keys.contains(&id_key)
            || self.index_keys.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("tombstone keys are not canonical"));
        }
        check_lifetime(
            self.revision,
            self.issued_at_ms,
            self.expires_at_ms,
            PODCAST_TOMBSTONE_TTL,
        )
    }
}

/// Body of a signed podcast-discovery record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodcastRecordBody {
    /// An active public card.
    Card(Box<PodcastCard>),
    /// A retraction.
    Tombstone(PodcastTombstone),
}

/// End-to-end signed podcast-discovery record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedPodcastRecord {
    /// Signed body.
    pub body: PodcastRecordBody,
    /// Ed25519 signature by `publisher` over [`Self::signing_bytes`].
    pub signature: Signature,
}

impl SignedPodcastRecord {
    /// Validates and signs a card.
    pub fn sign_card(card: PodcastCard, signer: impl FnOnce(&[u8]) -> Signature) -> Result<Self> {
        card.validate(&card.network_id)?;
        let signature = signer(&card.signing_bytes());
        let record = Self {
            body: PodcastRecordBody::Card(Box::new(card)),
            signature,
        };
        record.check_size()?;
        Ok(record)
    }

    /// Validates and signs a tombstone.
    pub fn sign_tombstone(
        tombstone: PodcastTombstone,
        signer: impl FnOnce(&[u8]) -> Signature,
    ) -> Result<Self> {
        tombstone.validate(&tombstone.network_id)?;
        let signature = signer(&tombstone.signing_bytes());
        let record = Self {
            body: PodcastRecordBody::Tombstone(tombstone),
            signature,
        };
        record.check_size()?;
        Ok(record)
    }

    /// Canonical signing input of the body.
    pub fn signing_bytes(&self) -> Vec<u8> {
        match &self.body {
            PodcastRecordBody::Card(card) => card.signing_bytes(),
            PodcastRecordBody::Tombstone(tombstone) => tombstone.signing_bytes(),
        }
    }

    /// Validates bounds, network, lifetime shape, encoded size and the
    /// publisher's signature. Does not look at the clock; see
    /// [`Self::verify_at`].
    pub fn verify(&self, expected_network: &NetworkId) -> Result<()> {
        match &self.body {
            PodcastRecordBody::Card(card) => card.validate(expected_network)?,
            PodcastRecordBody::Tombstone(tombstone) => tombstone.validate(expected_network)?,
        }
        self.check_size()?;
        self.publisher()
            .verify(&self.signing_bytes(), &self.signature)
            .map_err(|_| invalid("invalid podcast record signature"))
    }

    /// [`Self::verify`] plus time checks: not issued more than
    /// [`PODCAST_DHT_MAX_CLOCK_SKEW`] in the future and not expired at `now_ms`.
    pub fn verify_at(&self, expected_network: &NetworkId, now_ms: u64) -> Result<()> {
        self.verify(expected_network)?;
        let future_limit = now_ms.saturating_add(PODCAST_DHT_MAX_CLOCK_SKEW.as_millis() as u64);
        if self.issued_at_ms() > future_limit || self.expires_at_ms() <= now_ms {
            return Err(invalid("podcast record is expired or from the future"));
        }
        Ok(())
    }

    /// The card, when this is not a tombstone.
    pub fn card(&self) -> Option<&PodcastCard> {
        match &self.body {
            PodcastRecordBody::Card(card) => Some(card.as_ref()),
            PodcastRecordBody::Tombstone(_) => None,
        }
    }

    /// Whether this is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        matches!(self.body, PodcastRecordBody::Tombstone(_))
    }

    /// Podcast.
    pub fn podcast_id(&self) -> &PodcastId {
        match &self.body {
            PodcastRecordBody::Card(card) => &card.podcast_id,
            PodcastRecordBody::Tombstone(tombstone) => &tombstone.podcast_id,
        }
    }

    /// Publisher device.
    pub fn publisher(&self) -> EndpointId {
        match &self.body {
            PodcastRecordBody::Card(card) => card.publisher,
            PodcastRecordBody::Tombstone(tombstone) => tombstone.publisher,
        }
    }

    /// Network scope.
    pub fn network_id(&self) -> NetworkId {
        match &self.body {
            PodcastRecordBody::Card(card) => card.network_id,
            PodcastRecordBody::Tombstone(tombstone) => tombstone.network_id,
        }
    }

    /// Revision.
    pub fn revision(&self) -> u64 {
        match &self.body {
            PodcastRecordBody::Card(card) => card.revision,
            PodcastRecordBody::Tombstone(tombstone) => tombstone.revision,
        }
    }

    /// Signing time.
    pub fn issued_at_ms(&self) -> u64 {
        match &self.body {
            PodcastRecordBody::Card(card) => card.issued_at_ms,
            PodcastRecordBody::Tombstone(tombstone) => tombstone.issued_at_ms,
        }
    }

    /// Expiry time.
    pub fn expires_at_ms(&self) -> u64 {
        match &self.body {
            PodcastRecordBody::Card(card) => card.expires_at_ms,
            PodcastRecordBody::Tombstone(tombstone) => tombstone.expires_at_ms,
        }
    }

    /// Every DHT key this record belongs under: the podcast-id key, then the
    /// text keys (card tokens or the tombstone's `index_keys`).
    pub fn dht_keys(&self) -> Vec<PodcastDhtKey> {
        let network_id = self.network_id();
        let mut keys = vec![PodcastDhtKey::podcast(&network_id, self.podcast_id())];
        match &self.body {
            PodcastRecordBody::Card(card) => keys.extend(
                card.index_tokens()
                    .iter()
                    .map(|token| PodcastDhtKey::token(&network_id, token)),
            ),
            PodcastRecordBody::Tombstone(tombstone) => {
                keys.extend(tombstone.index_keys.iter().copied());
            }
        }
        keys
    }

    /// Whether the record may be stored under `key`.
    pub fn belongs_under(&self, key: &PodcastDhtKey) -> bool {
        self.dht_keys().contains(key)
    }

    /// postcard-encoded size.
    pub fn encoded_len(&self) -> usize {
        postcard::to_stdvec(self).map_or(usize::MAX, |bytes| bytes.len())
    }

    fn check_size(&self) -> Result<()> {
        if self.encoded_len() > MAX_PODCAST_RECORD_BYTES {
            Err(invalid("podcast record is too large"))
        } else {
            Ok(())
        }
    }
}

/// Total order of two records of the same `(podcast_id, publisher)`.
///
/// Greater wins: higher `revision`; at an equal revision a tombstone beats a
/// card; then later `issued_at_ms` (a republish); then later
/// `expires_at_ms`; then the lexicographically greater canonical signing
/// input. Every node therefore keeps the same winner whatever the delivery
/// order.
pub fn compare_records(left: &SignedPodcastRecord, right: &SignedPodcastRecord) -> Ordering {
    left.revision()
        .cmp(&right.revision())
        .then_with(|| left.is_tombstone().cmp(&right.is_tombstone()))
        .then_with(|| left.issued_at_ms().cmp(&right.issued_at_ms()))
        .then_with(|| left.expires_at_ms().cmp(&right.expires_at_ms()))
        .then_with(|| left.signing_bytes().cmp(&right.signing_bytes()))
}

/// Whether `winner` makes `other` obsolete everywhere, not just under one key:
/// `other` is a card of a lower revision, or `winner` is a tombstone of an
/// equal or higher revision.
pub(crate) fn supersedes_everywhere(
    winner: &SignedPodcastRecord,
    other: &SignedPodcastRecord,
) -> bool {
    !other.is_tombstone()
        && (other.revision() < winner.revision()
            || (winner.is_tombstone() && other.revision() <= winner.revision()))
}

struct Canonical(Vec<u8>);

impl Canonical {
    fn new(domain: &str) -> Self {
        let mut bytes = Vec::with_capacity(512);
        bytes.extend_from_slice(domain.as_bytes());
        bytes.push(0);
        Self(bytes)
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.raw(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.raw(&value.to_le_bytes());
    }

    fn str(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.raw(value.as_bytes());
    }

    fn opt_str(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.str(value);
            }
            None => self.u8(0),
        }
    }

    fn finish(self) -> Vec<u8> {
        self.0
    }
}

fn check_header(version: u16, network_id: &NetworkId, expected: &NetworkId) -> Result<()> {
    if version != PODCAST_DHT_PROTOCOL_VERSION {
        return Err(invalid("unsupported podcast record version"));
    }
    if network_id != expected {
        return Err(invalid("podcast record belongs to another network"));
    }
    Ok(())
}

fn check_lifetime(
    revision: u64,
    issued_at_ms: u64,
    expires_at_ms: u64,
    ttl: Duration,
) -> Result<()> {
    if revision == 0
        || issued_at_ms == 0
        || expires_at_ms <= issued_at_ms
        || expires_at_ms - issued_at_ms > ttl.as_millis() as u64
    {
        return Err(invalid("invalid podcast record revision or lifetime"));
    }
    Ok(())
}

fn check_chars(field: &'static str, value: &str, max_chars: usize) -> Result<()> {
    if value.len() > max_chars * 4 || value.chars().count() > max_chars {
        return Err(MusicDhtError::Protocol(format!("card {field} is too long")));
    }
    Ok(())
}

fn check_line(field: &'static str, value: &str, max_chars: usize, required: bool) -> Result<()> {
    if required && value.trim().is_empty() {
        return Err(MusicDhtError::Protocol(format!("card {field} is empty")));
    }
    check_chars(field, value, max_chars)?;
    if value.chars().any(char::is_control) {
        return Err(MusicDhtError::Protocol(format!(
            "card {field} contains control characters"
        )));
    }
    Ok(())
}

fn valid_language(language: &str) -> bool {
    !language.is_empty()
        && language.len() <= MAX_CARD_LANGUAGE_BYTES
        && language
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn clean_line(text: &str) -> String {
    text.split(char::is_control)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn clean_block(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .collect()
}

fn bounded_line(text: Option<&str>, max_chars: usize) -> Option<String> {
    text.map(clean_line)
        .filter(|text| !text.is_empty())
        .map(|text| bounded_text(&text, max_chars))
}

fn invalid(reason: &str) -> MusicDhtError {
    MusicDhtError::Protocol(reason.to_string())
}

pub(crate) fn publication_error(reason: &str) -> MusicDhtError {
    MusicDhtError::PodcastPublication(reason.to_string())
}

// Statically bound the worst-case card so `MAX_PODCAST_RECORD_BYTES` is a
// defensive limit, not the one users hit first.
const _: () = assert!(
    MAX_CARD_TITLE_CHARS * 4
        + MAX_CARD_AUTHOR_CHARS * 4
        + MAX_CARD_DESCRIPTION_CHARS * 4
        + MAX_CARD_CATEGORIES * MAX_CARD_CATEGORY_CHARS * 4
        + MAX_CARD_LANGUAGE_BYTES
        + 4 * MAX_URL_BYTES
        + 1024
        <= MAX_PODCAST_RECORD_BYTES
);
