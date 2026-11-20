//! Episode identity resolution, including the documented no-GUID rule.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{EpisodeId, EpisodeKey, PodcastId};

/// Normalizes an episode title for the `(date, title)` identity key.
///
/// This is [`crate::normalize_name`]: NFKC, lowercase, non-alphanumeric runs
/// collapsed to one space, trimmed. It is part of the episode-id derivation.
pub fn normalize_episode_title(title: &str) -> String {
    crate::normalize_name(title)
}

/// Fields of a feed item (or of a stored episode) that identify it.
///
/// The parser in `podcast::feed` produces these hints; stores keep them for
/// known episodes so that a later refresh can recognize items without GUID.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeIdentityHints {
    /// RSS `<guid>` / Atom `<id>`, when present.
    #[serde(default)]
    pub guid: Option<String>,
    /// Enclosure (audio) URL.
    #[serde(default)]
    pub enclosure_url: Option<String>,
    /// Item `<link>`.
    #[serde(default)]
    pub link: Option<String>,
    /// Publication time in unix milliseconds.
    #[serde(default)]
    pub published_at_ms: Option<i64>,
    /// Item title.
    #[serde(default)]
    pub title: Option<String>,
}

impl EpisodeIdentityHints {
    fn guid_key(&self) -> Option<EpisodeKey> {
        self.guid.as_deref().and_then(EpisodeKey::guid)
    }

    fn enclosure(&self) -> Option<&str> {
        trimmed(self.enclosure_url.as_deref())
    }

    fn link(&self) -> Option<&str> {
        trimmed(self.link.as_deref())
    }

    fn date_title(&self) -> Option<(i64, String)> {
        let published = self.published_at_ms?.div_euclid(1_000);
        let title = normalize_episode_title(self.title.as_deref()?);
        (!title.is_empty()).then_some((published, title))
    }
}

/// One matching stage of the no-GUID rule: does `known` match `item`?
type StageMatcher<'f> = &'f dyn Fn(&Usable<'_>, &EpisodeIdentityHints) -> bool;

struct Usable<'a> {
    link: Option<&'a str>,
    enclosure: Option<&'a str>,
    date_title: Option<(i64, String)>,
}

fn trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// An episode already stored for the podcast, with the hints it was last seen with.
///
/// Include episodes learned through sync (progress operations, queue items)
/// as well as episodes from earlier RSS refreshes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownEpisode {
    /// Existing stable id.
    pub episode_id: EpisodeId,
    /// Identity hints recorded for it.
    pub hints: EpisodeIdentityHints,
}

/// Resolves the id of a single feed item. See [`resolve_episode_ids`].
///
/// A single item has no feed context, so its `<link>` is only used when it
/// differs from `channel_link`. Prefer [`resolve_episode_ids`] for a whole
/// feed refresh.
pub fn resolve_episode_id(
    podcast: &PodcastId,
    channel_link: Option<&str>,
    item: &EpisodeIdentityHints,
    known: &[KnownEpisode],
) -> Option<EpisodeId> {
    resolve_episode_ids(podcast, channel_link, std::slice::from_ref(item), known)
        .pop()
        .flatten()
}

/// Resolves stable ids for the items of one feed document.
///
/// Returns one entry per item, in order; `None` marks an item that cannot be
/// identified (no usable key) or that duplicates an earlier item of the same
/// document. The rule (normative, see `PODCASTS.md`):
///
/// 1. An item with a GUID gets `EpisodeId::from_guid`. A GUID never matches
///    a different known episode, so the id is independent of local history.
/// 2. Items without a GUID then look for known episodes in three stages run
///    over the whole document: same enclosure URL, then same `<link>`, then
///    same publication second and [`normalize_episode_title`]. A stage
///    assigns matches for all remaining items before the next stage starts;
///    among candidates the smallest id is reused. A known episode already
///    claimed (by a GUID item or an earlier stage) is not a candidate, so the
///    result does not depend on item order.
/// 3. Otherwise a new id is derived from the first available key: `<link>`,
///    then `(date, title)`, then enclosure URL.
///
/// A `<link>` is only identifying when it is unique within the document and
/// differs from the channel link; many feeds repeat the show homepage in every
/// item. The same applies to enclosure URLs and `(date, title)` pairs shared
/// by several GUID-less items.
pub fn resolve_episode_ids(
    podcast: &PodcastId,
    channel_link: Option<&str>,
    items: &[EpisodeIdentityHints],
    known: &[KnownEpisode],
) -> Vec<Option<EpisodeId>> {
    let channel_link = trimmed(channel_link);
    let mut link_counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut enclosure_counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut date_title_counts: BTreeMap<(i64, String), usize> = BTreeMap::new();
    for item in items {
        if let Some(link) = item.link() {
            *link_counts.entry(link).or_default() += 1;
        }
        if item.guid_key().is_none() {
            if let Some(url) = item.enclosure() {
                *enclosure_counts.entry(url).or_default() += 1;
            }
            if let Some(pair) = item.date_title() {
                *date_title_counts.entry(pair).or_default() += 1;
            }
        }
    }
    let usable: Vec<Usable<'_>> = items
        .iter()
        .map(|item| Usable {
            link: item
                .link()
                .filter(|link| Some(*link) != channel_link && link_counts.get(link) == Some(&1)),
            enclosure: item
                .enclosure()
                .filter(|url| enclosure_counts.get(url) == Some(&1)),
            date_title: item
                .date_title()
                .filter(|pair| date_title_counts.get(pair) == Some(&1)),
        })
        .collect();

    let mut claimed: BTreeSet<EpisodeId> = BTreeSet::new();
    let mut out: Vec<Option<EpisodeId>> = vec![None; items.len()];

    // Pass 1: GUID items claim their derived ids first.
    for (index, item) in items.iter().enumerate() {
        if let Some(key) = item.guid_key() {
            let id = EpisodeId::derive(podcast, &key);
            if claimed.insert(id.clone()) {
                out[index] = Some(id);
            }
        }
    }

    // Pass 2: GUID-less items reuse a matching known episode. Stages run
    // across the whole document: every enclosure match is assigned before any
    // link match, and every link match before any (date, title) match, so a
    // weak match of an earlier item cannot steal the known episode that a
    // later item matches strongly. Within one stage an item's key is unique in
    // the document, so two items never compete for the same known episode
    // and the result does not depend on item order.
    let mut pending: Vec<usize> = (0..items.len())
        .filter(|index| items[*index].guid_key().is_none())
        .collect();
    let stages: [StageMatcher<'_>; 3] = [
        &|own, candidate| {
            own.enclosure
                .is_some_and(|url| candidate.enclosure() == Some(url))
        },
        &|own, candidate| own.link.is_some_and(|link| candidate.link() == Some(link)),
        &|own, candidate| {
            own.date_title
                .as_ref()
                .is_some_and(|pair| candidate.date_title().as_ref() == Some(pair))
        },
    ];
    for matches in stages {
        let mut unmatched = Vec::with_capacity(pending.len());
        for index in pending {
            let own = &usable[index];
            let reused = known
                .iter()
                .filter(|known| !claimed.contains(&known.episode_id) && matches(own, &known.hints))
                .map(|known| &known.episode_id)
                .min()
                .cloned();
            match reused {
                Some(id) => {
                    claimed.insert(id.clone());
                    out[index] = Some(id);
                }
                None => unmatched.push(index),
            }
        }
        pending = unmatched;
    }

    // Pass 3: derive new ids from the first available fallback key.
    for index in pending {
        let own = &usable[index];
        let key = own
            .link
            .and_then(EpisodeKey::link)
            .or_else(|| {
                own.date_title
                    .as_ref()
                    .map(|(secs, title)| EpisodeKey::DateTitle {
                        published_at_secs: *secs,
                        normalized_title: title.clone(),
                    })
            })
            .or_else(|| own.enclosure.and_then(EpisodeKey::enclosure));
        if let Some(key) = key {
            let id = EpisodeId::derive(podcast, &key);
            if claimed.insert(id.clone()) {
                out[index] = Some(id);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::podcast::FeedUrl;

    fn podcast() -> PodcastId {
        PodcastId::from_feed_url(&FeedUrl::parse("https://example.test/feed").unwrap())
    }

    fn hints(
        guid: Option<&str>,
        enclosure: Option<&str>,
        link: Option<&str>,
        date: Option<i64>,
        title: Option<&str>,
    ) -> EpisodeIdentityHints {
        EpisodeIdentityHints {
            guid: guid.map(str::to_string),
            enclosure_url: enclosure.map(str::to_string),
            link: link.map(str::to_string),
            published_at_ms: date,
            title: title.map(str::to_string),
        }
    }

    fn derived(key: EpisodeKey) -> Option<EpisodeId> {
        Some(EpisodeId::derive(&podcast(), &key))
    }

    #[test]
    fn guid_wins_and_ignores_history() {
        let item = hints(Some("g1"), Some("https://cdn/1.mp3"), None, None, Some("T"));
        let known = [KnownEpisode {
            episode_id: EpisodeId::from_guid(&podcast(), "other").unwrap(),
            hints: hints(None, Some("https://cdn/1.mp3"), None, None, None),
        }];
        assert_eq!(
            resolve_episode_id(&podcast(), None, &item, &known),
            derived(EpisodeKey::guid("g1").unwrap())
        );
    }

    #[test]
    fn title_cover_and_audio_url_changes_keep_the_guid_id() {
        let before = hints(
            Some("g1"),
            Some("https://cdn/a.mp3"),
            None,
            Some(1),
            Some("Old"),
        );
        let after = hints(
            Some("g1"),
            Some("https://cdn/b.mp3"),
            None,
            Some(2),
            Some("New"),
        );
        assert_eq!(
            resolve_episode_id(&podcast(), None, &before, &[]),
            resolve_episode_id(&podcast(), None, &after, &[])
        );
    }

    #[test]
    fn fallback_key_order_for_new_items() {
        let p = podcast();
        let with_link = hints(
            None,
            Some("https://cdn/1.mp3"),
            Some("https://e/1"),
            Some(5_000),
            Some("A"),
        );
        assert_eq!(
            resolve_episode_id(&p, None, &with_link, &[]),
            derived(EpisodeKey::link("https://e/1").unwrap())
        );
        let date_title = hints(
            None,
            Some("https://cdn/1.mp3"),
            None,
            Some(5_000),
            Some("A"),
        );
        assert_eq!(
            resolve_episode_id(&p, None, &date_title, &[]),
            derived(EpisodeKey::date_title(5_000, "A").unwrap())
        );
        let enclosure_only = hints(None, Some("https://cdn/1.mp3"), None, None, Some("A"));
        assert_eq!(
            resolve_episode_id(&p, None, &enclosure_only, &[]),
            derived(EpisodeKey::enclosure("https://cdn/1.mp3").unwrap())
        );
        assert_eq!(
            resolve_episode_id(&p, None, &EpisodeIdentityHints::default(), &[]),
            None
        );
    }

    #[test]
    fn guidless_episode_keeps_its_id_after_title_and_url_changes() {
        let p = podcast();
        // First seen: no link, so the id comes from (date, title).
        let first = hints(
            None,
            Some("https://cdn/a.mp3"),
            None,
            Some(9_000),
            Some("Pilot"),
        );
        let id = resolve_episode_id(&p, None, &first, &[]).unwrap();
        let known = [KnownEpisode {
            episode_id: id.clone(),
            hints: first.clone(),
        }];
        // Title changed: matched by enclosure.
        let retitled = hints(
            None,
            Some("https://cdn/a.mp3"),
            None,
            Some(9_000),
            Some("Pilot (remastered)"),
        );
        assert_eq!(
            resolve_episode_id(&p, None, &retitled, &known),
            Some(id.clone())
        );
        // Audio URL changed: matched by (date, title).
        let moved = hints(
            None,
            Some("https://cdn2/a.mp3"),
            None,
            Some(9_999),
            Some("PILOT!"),
        );
        assert_eq!(
            resolve_episode_id(&p, None, &moved, &known),
            Some(id.clone())
        );
        // Link appeared later: the enclosure still matches first.
        let linked = hints(
            None,
            Some("https://cdn/a.mp3"),
            Some("https://e/pilot"),
            None,
            None,
        );
        assert_eq!(resolve_episode_id(&p, None, &linked, &known), Some(id));
    }

    #[test]
    fn shared_channel_links_are_not_identifying() {
        let p = podcast();
        let home = "https://show.test/";
        let items = [
            hints(None, Some("https://cdn/1.mp3"), Some(home), None, None),
            hints(None, Some("https://cdn/2.mp3"), Some(home), None, None),
        ];
        let ids = resolve_episode_ids(&p, Some(home), &items, &[]);
        assert_eq!(
            ids[0],
            derived(EpisodeKey::enclosure("https://cdn/1.mp3").unwrap())
        );
        assert_eq!(
            ids[1],
            derived(EpisodeKey::enclosure("https://cdn/2.mp3").unwrap())
        );
        // The same holds without the channel link: the link repeats.
        assert_eq!(resolve_episode_ids(&p, None, &items, &[]), ids);
        // A single item whose link equals the channel link.
        assert_eq!(resolve_episode_id(&p, Some(home), &items[0], &[]), ids[0]);
    }

    #[test]
    fn duplicates_in_one_document_are_dropped() {
        let p = podcast();
        let items = [
            hints(Some("g"), Some("https://cdn/1.mp3"), None, None, None),
            hints(Some("g"), Some("https://cdn/2.mp3"), None, None, None),
            hints(None, Some("https://cdn/3.mp3"), None, None, None),
            hints(None, Some("https://cdn/3.mp3"), None, None, None),
        ];
        let ids = resolve_episode_ids(&p, None, &items, &[]);
        assert!(ids[0].is_some());
        assert_eq!(ids[1], None);
        // The shared enclosure is not identifying: both GUID-less items have
        // no usable key.
        assert_eq!(ids[2], None);
        assert_eq!(ids[3], None);
    }

    #[test]
    fn a_known_episode_is_claimed_once() {
        let p = podcast();
        let known_id = derived(EpisodeKey::enclosure("https://cdn/old.mp3").unwrap()).unwrap();
        let known = [KnownEpisode {
            episode_id: known_id.clone(),
            hints: hints(
                None,
                Some("https://cdn/old.mp3"),
                None,
                Some(1_000),
                Some("Same"),
            ),
        }];
        // Two new items both match by (date, title) only if unique; here
        // they share it, so neither matches; the enclosure one matches.
        let items = [
            hints(
                None,
                Some("https://cdn/x.mp3"),
                None,
                Some(1_000),
                Some("Same"),
            ),
            hints(
                None,
                Some("https://cdn/old.mp3"),
                None,
                Some(1_000),
                Some("Same"),
            ),
        ];
        let ids = resolve_episode_ids(&p, None, &items, &known);
        assert_eq!(ids[1], Some(known_id.clone()));
        assert_eq!(
            ids[0],
            derived(EpisodeKey::enclosure("https://cdn/x.mp3").unwrap())
        );
        assert_ne!(ids[0], ids[1]);
    }

    #[test]
    fn ambiguous_known_matches_pick_the_smallest_id_in_any_order() {
        let p = podcast();
        let a = EpisodeId::from_guid(&p, "a").unwrap();
        let b = EpisodeId::from_guid(&p, "b").unwrap();
        let same = hints(None, Some("https://cdn/1.mp3"), None, None, None);
        let k1 = KnownEpisode {
            episode_id: a.clone(),
            hints: same.clone(),
        };
        let k2 = KnownEpisode {
            episode_id: b.clone(),
            hints: same.clone(),
        };
        let expected = Some(a.clone().min(b.clone()));
        assert_eq!(
            resolve_episode_id(&p, None, &same, &[k1.clone(), k2.clone()]),
            expected
        );
        assert_eq!(resolve_episode_id(&p, None, &same, &[k2, k1]), expected);
    }

    #[test]
    fn result_is_independent_of_known_order() {
        let p = podcast();
        let items: Vec<EpisodeIdentityHints> = (0..6)
            .map(|i| {
                hints(
                    None,
                    Some(&format!("https://cdn/{i}.mp3")),
                    Some(&format!("https://e/{i}")),
                    Some(i * 1_000),
                    Some(&format!("Ep {i}")),
                )
            })
            .collect();
        let mut known: Vec<KnownEpisode> = items
            .iter()
            .enumerate()
            .map(|(i, h)| KnownEpisode {
                episode_id: EpisodeId::from_guid(&p, &format!("legacy-{i}")).unwrap(),
                hints: h.clone(),
            })
            .collect();
        let forward = resolve_episode_ids(&p, None, &items, &known);
        known.reverse();
        assert_eq!(resolve_episode_ids(&p, None, &items, &known), forward);
        assert!(forward.iter().all(Option::is_some));
    }

    #[test]
    fn strong_matches_beat_earlier_weak_matches_in_any_item_order() {
        let p = podcast();
        let k1 = EpisodeId::from_guid(&p, "legacy-k1").unwrap();
        let known = [KnownEpisode {
            episode_id: k1.clone(),
            hints: hints(
                None,
                Some("https://cdn/e1.mp3"),
                None,
                Some(5_000),
                Some("Same"),
            ),
        }];
        // A new item that only matches K1 by (date, title), and the real K1
        // (same enclosure) renamed and re-dated by the publisher.
        let weak = hints(
            None,
            Some("https://cdn/new.mp3"),
            None,
            Some(5_000),
            Some("Same"),
        );
        let strong = hints(
            None,
            Some("https://cdn/e1.mp3"),
            None,
            Some(9_000),
            Some("Renamed"),
        );
        let forward = resolve_episode_ids(&p, None, &[weak.clone(), strong.clone()], &known);
        assert_eq!(forward[1].as_ref(), Some(&k1), "enclosure match keeps K1");
        assert_ne!(forward[0].as_ref(), Some(&k1));
        let reverse = resolve_episode_ids(&p, None, &[strong, weak], &known);
        assert_eq!(reverse, vec![forward[1].clone(), forward[0].clone()]);
    }

    #[test]
    fn result_is_independent_of_item_order() {
        let p = podcast();
        let items: Vec<EpisodeIdentityHints> = (0..5)
            .map(|i| {
                hints(
                    None,
                    Some(&format!("https://cdn/{i}.mp3")),
                    (i % 2 == 0).then(|| format!("https://e/{i}")).as_deref(),
                    Some((i % 3) * 1_000),
                    Some(&format!("Ep {}", i % 2)),
                )
            })
            .collect();
        // Known episodes whose hints overlap different items at different
        // stages.
        let known: Vec<KnownEpisode> = (0..5)
            .map(|i| KnownEpisode {
                episode_id: EpisodeId::from_guid(&p, &format!("legacy-{i}")).unwrap(),
                hints: hints(
                    None,
                    Some(&format!("https://cdn/{}.mp3", (i + 2) % 7)),
                    Some(&format!("https://e/{}", (i + 1) % 5)),
                    Some((i % 3) * 1_000),
                    Some(&format!("Ep {}", i % 2)),
                ),
            })
            .collect();
        let reference = resolve_episode_ids(&p, None, &items, &known);
        let mut order: Vec<usize> = (0..items.len()).collect();
        for _ in 0..20 {
            order.rotate_left(1);
            let middle = order.len() / 2;
            order.swap(0, middle);
            let shuffled: Vec<EpisodeIdentityHints> =
                order.iter().map(|i| items[*i].clone()).collect();
            let result = resolve_episode_ids(&p, None, &shuffled, &known);
            for (position, index) in order.iter().enumerate() {
                assert_eq!(result[position], reference[*index], "order {order:?}");
            }
        }
    }
}
