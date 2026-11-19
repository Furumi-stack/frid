use std::collections::BTreeMap;

use super::*;
use crate::podcast::{EpisodeIdentityHints, EpisodeMeta, EpisodeType, resolve_episode_ids};

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for index in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(index);
        for mut tail in permutations(&rest) {
            tail.insert(0, head.clone());
            out.push(tail);
        }
    }
    out
}

fn feed_url(path: &str) -> FeedUrl {
    FeedUrl::parse(&format!("https://example.test/{path}")).unwrap()
}

fn podcast_id() -> PodcastId {
    PodcastId::from_feed_url(&feed_url("feed"))
}

fn episode_id(guid: &str) -> EpisodeId {
    EpisodeId::from_guid(&podcast_id(), guid).unwrap()
}

fn progress(
    generation: u64,
    hlc_ms: i64,
    device: &str,
    op: &str,
    position_ms: i64,
    status: ProgressStatus,
    session: &str,
) -> ProgressRecord {
    ProgressRecord {
        episode_id: episode_id("e1"),
        podcast_id: podcast_id(),
        position_ms,
        status,
        generation,
        session_id: session.to_string(),
        hlc_ms,
        device_id: device.to_string(),
        op_id: op.to_string(),
    }
}

fn fold<T: Clone>(items: &[T], merge: impl Fn(&T, &T) -> PodcastResult<T>) -> T {
    let mut iter = items.iter();
    let first = iter.next().expect("non-empty").clone();
    iter.fold(first, |acc, item| merge(&acc, item).unwrap())
}

fn assert_converges<T: Clone + PartialEq + std::fmt::Debug>(
    items: &[T],
    merge: impl Fn(&T, &T) -> PodcastResult<T> + Copy,
) -> T {
    let reference = fold(items, merge);
    for order in permutations(items) {
        assert_eq!(fold(&order, merge), reference, "order: {order:?}");
    }
    // Merge laws on every pair and triple.
    for a in items {
        assert_eq!(
            merge(&merge(a, a).unwrap(), a).unwrap(),
            merge(a, a).unwrap()
        );
        for b in items {
            assert_eq!(merge(a, b).unwrap(), merge(b, a).unwrap());
            for c in items {
                assert_eq!(
                    merge(&merge(a, b).unwrap(), c).unwrap(),
                    merge(a, &merge(b, c).unwrap()).unwrap()
                );
            }
        }
    }
    reference
}

#[test]
fn progress_converges_for_every_permutation() {
    use ProgressStatus::*;
    let ops = [
        progress(1, 100, "dev-a", "dev-a:1", 10_000, InProgress, "s1"),
        progress(1, 160, "dev-a", "dev-a:2", 70_000, InProgress, "s1"),
        progress(2, 150, "dev-b", "dev-b:1", 0, InProgress, "s2"),
        progress(2, 150, "dev-c", "dev-c:1", 5_000, InProgress, "s3"),
        progress(2, 140, "dev-b", "dev-b:2", 0, Completed, "s2"),
        // Duplicate delivery of the same operation.
        progress(1, 160, "dev-a", "dev-a:2", 70_000, InProgress, "s1"),
    ];
    let winner = assert_converges(&ops, merge_progress);
    assert_eq!(winner.op_id, "dev-c:1");
}

#[test]
fn late_old_generation_checkpoint_loses() {
    use ProgressStatus::*;
    // Device A keeps checkpointing session s1 (generation 3) with a clock far
    // ahead; device B explicitly seeks back (generation 4) with a slow clock.
    let seek_back = progress(4, 1_000, "dev-b", "dev-b:9", 60_000, InProgress, "s2");
    let late = progress(
        3, 9_000_000, "dev-a", "dev-a:50", 3_000_000, InProgress, "s1",
    );
    for order in [[&seek_back, &late], [&late, &seek_back]] {
        let merged = merge_progress(order[0], order[1]).unwrap();
        assert_eq!(merged, seek_back);
    }
    assert!(seek_back.supersedes(&late));
    assert!(!late.supersedes(&seek_back));
}

#[test]
fn restart_beats_later_checkpoint_of_the_old_session() {
    use ProgressStatus::*;
    let checkpoint = progress(2, 100, "dev-a", "dev-a:1", 2_400_000, InProgress, "s1");
    let generation = ProgressRecord::explicit_generation(Some(checkpoint.generation));
    assert_eq!(generation, 3);
    let restart = progress(generation, 110, "dev-a", "dev-a:2", 0, InProgress, "s2");
    // A checkpoint of s1 that was queued before the restart and delivered
    // later, even stamped after the restart.
    let late = progress(2, 120, "dev-a", "dev-a:3", 2_460_000, InProgress, "s1");
    let merged = assert_converges(&[checkpoint, restart.clone(), late], merge_progress);
    assert_eq!(merged, restart);
    assert_eq!(merged.resume_position_ms(), 0);
}

#[test]
fn progress_is_not_the_maximum_position() {
    use ProgressStatus::*;
    let ahead = progress(1, 100, "dev-a", "dev-a:1", 3_000_000, InProgress, "s1");
    let back = progress(2, 101, "dev-a", "dev-a:2", 60_000, InProgress, "s1");
    assert_eq!(merge_progress(&ahead, &back).unwrap().position_ms, 60_000);
}

#[test]
fn concurrent_offline_sessions_converge_and_keep_both_sessions() {
    use ProgressStatus::*;
    // Both devices last saw generation 5 and start listening offline.
    let generation = ProgressRecord::explicit_generation(Some(5));
    let a_start = progress(
        generation, 200, "dev-a", "dev-a:1", 300_000, InProgress, "sa",
    );
    let a_cp = progress(
        generation, 300, "dev-a", "dev-a:2", 900_000, InProgress, "sa",
    );
    let b_start = progress(
        generation, 210, "dev-b", "dev-b:1", 300_000, InProgress, "sb",
    );
    let b_cp = progress(
        generation, 280, "dev-b", "dev-b:2", 1_500_000, InProgress, "sb",
    );
    let merged = assert_converges(&[a_start, a_cp.clone(), b_start, b_cp], merge_progress);
    // Deterministic: the latest write of the shared generation wins.
    assert_eq!(merged, a_cp);

    let session = |id: &str, device: &str, listened: i64, completed: bool| ListenSession {
        session_id: id.to_string(),
        episode_id: episode_id("e1"),
        podcast_id: podcast_id(),
        device_id: device.to_string(),
        started_at_ms: 200,
        ended_at_ms: 300,
        start_position_ms: 300_000,
        end_position_ms: 900_000,
        listened_ms: listened,
        completed,
    };
    let sessions = [
        session("sa", "dev-a", 600_000, false),
        session("sb", "dev-b", 1_200_000, true),
        session("sb", "dev-b", 1_200_000, true),
    ];
    let mut set: BTreeMap<String, ListenSession> = BTreeMap::new();
    for order in permutations(&sessions) {
        set.clear();
        for item in order {
            let merged = match set.get(&item.session_id) {
                Some(current) => merge_listen_session(current, &item).unwrap(),
                None => item.clone(),
            };
            set.insert(item.session_id.clone(), merged);
        }
        assert_eq!(set.len(), 2);
        assert_eq!(completed_listen_count(set.values()), 1);
    }
}

#[test]
fn listen_session_versions_only_grow() {
    let base = ListenSession {
        session_id: "s".to_string(),
        episode_id: episode_id("e1"),
        podcast_id: podcast_id(),
        device_id: "dev-a".to_string(),
        started_at_ms: 1_000,
        ended_at_ms: 2_000,
        start_position_ms: 0,
        end_position_ms: 60_000,
        listened_ms: 60_000,
        completed: false,
    };
    let longer = ListenSession {
        ended_at_ms: 3_000,
        end_position_ms: 120_000,
        listened_ms: 120_000,
        ..base.clone()
    };
    let finished = ListenSession {
        ended_at_ms: 3_500,
        end_position_ms: 3_600_000,
        listened_ms: 125_000,
        completed: true,
        ..base.clone()
    };
    let merged = assert_converges(
        &[base.clone(), longer, finished.clone(), base.clone()],
        merge_listen_session,
    );
    assert_eq!(merged, finished);
    assert!(merged.should_record());
    assert_eq!(completed_listen_count([&merged, &merged, &finished]), 1);

    let mut other_episode = base.clone();
    other_episode.episode_id = episode_id("e2");
    assert!(merge_listen_session(&base, &other_episode).is_err());
    let short = ListenSession {
        listened_ms: MIN_RECORDED_EPISODE_LISTEN_MS - 1,
        ..base.clone()
    };
    assert!(!short.should_record());
    assert!(
        ListenSession {
            listened_ms: MIN_RECORDED_EPISODE_LISTEN_MS,
            ..base
        }
        .should_record()
    );
}

#[test]
fn completion_criterion() {
    // 60 min episode: tail is 30 s.
    assert!(!reaches_completion(3_569_999, Some(3_600_000)));
    assert!(reaches_completion(3_570_000, Some(3_600_000)));
    // 5 min episode: tail is 5 % = 15 s.
    assert!(!reaches_completion(284_999, Some(300_000)));
    assert!(reaches_completion(285_000, Some(300_000)));
    assert!(!reaches_completion(1_000_000, None));
    assert!(!reaches_completion(1_000_000, Some(0)));
}

fn podcast_meta(title: &str, url: &str) -> PodcastMeta {
    PodcastMeta {
        feed_url: feed_url(url),
        title: title.to_string(),
        author: None,
        description: None,
        link: None,
        image_url: None,
    }
}

fn subscription(
    subscribed: bool,
    hlc_ms: i64,
    op: &str,
    title: &str,
    url: &str,
) -> SubscriptionRecord {
    let mut record = SubscriptionRecord {
        podcast_id: podcast_id(),
        subscribed,
        podcast: podcast_meta(title, url),
        feed_aliases: Vec::new(),
        hlc_ms,
        op_id: op.to_string(),
    };
    record.normalize();
    record
}

#[test]
fn subscriptions_converge_for_every_permutation() {
    let ops = [
        // Same podcast added on two devices.
        subscription(true, 100, "dev-a:1", "Show", "feed"),
        subscription(true, 101, "dev-b:1", "Show (B)", "feed"),
        // Unsubscribed on A while B was offline.
        subscription(false, 200, "dev-a:2", "Show", "feed"),
        // B comes back and re-delivers its old state after the feed moved.
        subscription(true, 150, "dev-b:2", "Show", "moved"),
        subscription(true, 101, "dev-b:1", "Show (B)", "feed"),
    ];
    let merged = assert_converges(&ops, merge_subscription);
    assert!(
        !merged.subscribed,
        "unsubscribe survives the offline device"
    );
    assert_eq!(merged.op_id, "dev-a:2");
    assert!(merged.feed_aliases.contains(&feed_url("moved")));
    assert!(merged.feed_aliases.contains(&feed_url("feed")));
    merged.validate().unwrap();
}

#[test]
fn resubscribing_keeps_progress_and_history() {
    use ProgressStatus::*;
    let mut progress_register = BTreeMap::new();
    let saved = progress(4, 500, "dev-a", "dev-a:5", 1_234_000, InProgress, "s1");
    progress_register.insert(saved.episode_id.clone(), saved.clone());
    let subscribed = subscription(true, 100, "dev-a:1", "Show", "feed");
    let unsubscribed = subscription(false, 600, "dev-a:6", "Show", "feed");
    let again = subscription(true, 700, "dev-b:1", "Show", "feed");
    let merged = fold(&[subscribed, unsubscribed, again], merge_subscription);
    assert!(merged.subscribed);
    assert_eq!(progress_register.get(&saved.episode_id), Some(&saved));
}

fn share(shared: bool, hlc_ms: i64, op: &str) -> ShareRecord {
    ShareRecord {
        podcast_id: podcast_id(),
        shared,
        hlc_ms,
        op_id: op.to_string(),
    }
}

#[test]
fn shares_converge_and_false_is_a_tombstone() {
    let ops = [
        share(true, 100, "dev-a:1"),
        share(false, 200, "dev-a:2"),
        share(true, 150, "dev-b:1"),
        share(false, 200, "dev-a:2"),
    ];
    let merged = assert_converges(&ops, merge_share);
    assert!(!merged.shared, "late stale share cannot republish");
}

#[test]
fn effective_share_rules() {
    let subscribed = subscription(true, 100, "dev-a:1", "Show", "feed");
    let shared = share(true, 150, "dev-a:2");
    assert!(effective_share(Some(&subscribed), Some(&shared)));
    assert!(!effective_share(Some(&subscribed), None));
    assert!(!effective_share(None, Some(&shared)));
    // New subscriptions are private: a share decision older than the
    // latest subscription write does not apply.
    let resubscribed = subscription(true, 300, "dev-a:4", "Show", "feed");
    assert!(!effective_share(Some(&resubscribed), Some(&shared)));
    // Unsubscribing revokes publication even if a stale share wins.
    let unsubscribed = subscription(false, 200, "dev-a:3", "Show", "feed");
    let stale = share(true, 250, "dev-b:1");
    assert!(!effective_share(Some(&unsubscribed), Some(&stale)));
}

/// Minimal client store used to show that whole operation sets converge and
/// that publisher metadata never reaches personal progress.
#[derive(Debug, Default, Clone, PartialEq)]
struct Store {
    subscriptions: BTreeMap<PodcastId, SubscriptionRecord>,
    shares: BTreeMap<PodcastId, ShareRecord>,
    progress: BTreeMap<EpisodeId, ProgressRecord>,
    sessions: BTreeMap<String, ListenSession>,
    catalog: BTreeMap<EpisodeId, EpisodeMeta>,
}

#[derive(Debug, Clone)]
enum Op {
    Subscription(SubscriptionRecord),
    Share(ShareRecord),
    Progress(ProgressRecord),
    Session(ListenSession),
    /// Local RSS refresh: title and audio URL changed at the publisher.
    Rss {
        title: &'static str,
        audio_url: &'static str,
    },
}

fn upsert<K: Ord + Clone, V: Clone>(
    map: &mut BTreeMap<K, V>,
    key: K,
    value: &V,
    merge: impl Fn(&V, &V) -> PodcastResult<V>,
) {
    let merged = match map.get(&key) {
        Some(current) => merge(current, value).unwrap(),
        None => value.clone(),
    };
    map.insert(key, merged);
}

impl Store {
    fn apply(&mut self, op: &Op) {
        match op {
            Op::Subscription(record) => upsert(
                &mut self.subscriptions,
                record.podcast_id.clone(),
                record,
                merge_subscription,
            ),
            Op::Share(record) => upsert(
                &mut self.shares,
                record.podcast_id.clone(),
                record,
                merge_share,
            ),
            Op::Progress(record) => upsert(
                &mut self.progress,
                record.episode_id.clone(),
                record,
                merge_progress,
            ),
            Op::Session(session) => upsert(
                &mut self.sessions,
                session.session_id.clone(),
                session,
                merge_listen_session,
            ),
            Op::Rss { title, audio_url } => {
                let hints = EpisodeIdentityHints {
                    guid: Some("e1".to_string()),
                    enclosure_url: Some(audio_url.to_string()),
                    link: None,
                    published_at_ms: Some(1_000),
                    title: Some(title.to_string()),
                };
                let id = resolve_episode_ids(&podcast_id(), None, &[hints], &[])[0]
                    .clone()
                    .unwrap();
                // Metadata only: last refresh on this device wins locally.
                let meta = EpisodeMeta {
                    episode_id: id.clone(),
                    podcast_id: podcast_id(),
                    guid: Some("e1".to_string()),
                    title: title.to_string(),
                    description: None,
                    season: None,
                    episode_number: None,
                    episode_type: EpisodeType::Full,
                    published_at_ms: Some(1_000),
                    duration_ms: None,
                    audio_url: audio_url.to_string(),
                    mime: None,
                    byte_len: None,
                    image_url: None,
                    link: None,
                };
                self.catalog.insert(id, meta);
            }
        }
    }

    fn personal(&self) -> Store {
        Store {
            catalog: BTreeMap::new(),
            ..self.clone()
        }
    }
}

#[test]
fn whole_operation_sets_converge_and_rss_never_touches_progress() {
    use ProgressStatus::*;
    let session = ListenSession {
        session_id: "s2".to_string(),
        episode_id: episode_id("e1"),
        podcast_id: podcast_id(),
        device_id: "dev-b".to_string(),
        started_at_ms: 10,
        ended_at_ms: 20,
        start_position_ms: 0,
        end_position_ms: 400_000,
        listened_ms: 400_000,
        completed: false,
    };
    let ops = [
        Op::Subscription(subscription(true, 100, "dev-a:1", "Show", "feed")),
        Op::Share(share(true, 120, "dev-a:2")),
        Op::Progress(progress(
            1, 130, "dev-a", "dev-a:3", 900_000, InProgress, "s1",
        )),
        Op::Progress(progress(
            2, 125, "dev-b", "dev-b:1", 400_000, InProgress, "s2",
        )),
        Op::Session(session),
        Op::Rss {
            title: "Old title",
            audio_url: "https://cdn.test/old.mp3",
        },
        Op::Rss {
            title: "New title",
            audio_url: "https://cdn.test/new.mp3",
        },
    ];
    let mut reference: Option<Store> = None;
    for order in permutations(&ops) {
        let mut store = Store::default();
        for op in &order {
            store.apply(op);
        }
        // One episode id across both metadata versions.
        assert_eq!(store.catalog.len(), 1);
        let progress = store.progress.values().next().unwrap();
        assert_eq!(progress.op_id, "dev-b:1");
        assert_eq!(progress.position_ms, 400_000);
        let personal = store.personal();
        match &reference {
            Some(reference) => assert_eq!(&personal, reference),
            None => reference = Some(personal),
        }
    }
}

#[test]
fn validation_bounds() {
    use ProgressStatus::*;
    let ok = progress(1, 1, "dev-a", "dev-a:1", 0, NotStarted, "s");
    ok.validate().unwrap();
    let mut bad = ok.clone();
    bad.position_ms = -1;
    assert!(bad.validate().is_err());
    let mut bad = ok.clone();
    bad.session_id = String::new();
    assert!(bad.validate().is_err());
    let mut bad = ok;
    bad.device_id = "d".repeat(crate::podcast::MAX_TOKEN_BYTES + 1);
    assert!(bad.validate().is_err());

    let mut record = subscription(true, 1, "dev-a:1", "Show", "feed");
    record.feed_aliases = (0..=crate::podcast::MAX_FEED_ALIASES + 1)
        .map(|i| feed_url(&format!("alias-{i}")))
        .collect();
    assert!(record.validate().is_err());
    record.normalize();
    record.validate().unwrap();
}

#[test]
fn progress_json_shape() {
    let record = progress(
        3,
        42,
        "dev-a",
        "dev-a:7",
        1_000,
        ProgressStatus::InProgress,
        "s",
    );
    let json = serde_json::to_value(&record).unwrap();
    assert_eq!(json["status"], "in_progress");
    assert_eq!(json["generation"], 3);
    assert_eq!(
        serde_json::from_value::<ProgressRecord>(json).unwrap(),
        record
    );
}
