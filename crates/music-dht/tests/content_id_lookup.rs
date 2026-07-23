//! Focused integration test: a track published by one node must be
//! resolvable by content id from another node (the share-link flow).

use std::path::Path;
use std::time::Duration;

use music_dht::{
    ItemKind, ItemSpec, MusicDhtConfig, MusicDhtEventReceiver, MusicDhtService, NetworkId,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(240);

const CONTENT_ID: &str = "b3:661eb31d76ab7cef89a19bb8e978c0eb357ae62395a73fa5539a0efd35110dd9";

type Started = (MusicDhtService, MusicDhtEventReceiver);

async fn start(dir: &Path, network: &str) -> Started {
    let config = MusicDhtConfig::builder()
        .data_dir(dir)
        .network_id(NetworkId::from_name(network))
        .republish_interval(Duration::from_secs(5))
        .expire_interval(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(5))
        .lookup_timeout(Duration::from_secs(10))
        .build()
        .expect("valid config");
    MusicDhtService::start(config)
        .await
        .expect("service starts")
}

#[tokio::test]
async fn share_link_content_id_lookup() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");

        let (a, _events_a) = start(dir_a.path(), "content-id-test").await;
        let ticket_a = a.ticket().await.expect("ticket");
        let (b, _events_b) = start(dir_b.path(), "content-id-test").await;
        b.connect(ticket_a).await.expect("b connects to a");

        // B publishes one track with a content id and many featured artists
        // (mirrors the real-world failing share link).
        let spec = ItemSpec {
            local_key: "track:1".into(),
            kind: ItemKind::Track,
            name: "Ежемесячные".into(),
            artist_names: vec!["Основной Артист".into()],
            featured_artist_names: vec![
                "Pyrokinesis".into(),
                "Блёв МС".into(),
                "Артем Татищевский".into(),
                "WormGanger".into(),
                "Рудбой".into(),
                "АнальгиН-56 школа".into(),
            ],
            year: Some(2024),
            release_type: Some("album".into()),
            release_title: Some("Тестовый релиз".into()),
            track_number: Some(1),
            disc_number: Some(1),
            duration_seconds: Some(200.0),
            content_id: Some(CONTENT_ID.into()),
        };
        let stats = b.sync_library(vec![spec]).await.expect("sync");
        assert_eq!(stats.added, 1, "track is published");
        assert_eq!(stats.failed, 0);

        // A resolves the share link by content id.
        let started = std::time::Instant::now();
        let found = loop {
            let outcome = a
                .search_content_id(CONTENT_ID)
                .await
                .expect("content id search");
            let hit = outcome
                .network_results
                .iter()
                .find(|item| item.kind == ItemKind::Track);
            if let Some(hit) = hit {
                break hit.clone();
            }
            assert!(
                started.elapsed() < Duration::from_secs(45),
                "timed out resolving the content id from the other node"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        assert_eq!(found.content_id.as_deref(), Some(CONTENT_ID));
        assert_eq!(found.owner, b.endpoint_id());

        a.shutdown().await.expect("shutdown a");
        b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}

/// Regression test for the original share-link failure: with a large
/// library, every track — including the tail of the publish order — must be
/// resolvable by content id. The old per-key-lookup publisher could not keep
/// a big library alive within the record TTL.
#[tokio::test]
async fn large_library_tail_is_resolvable() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        const TRACKS: usize = 300;
        let content_id_of = |index: usize| format!("b3:{index:064x}");

        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let (a, _events_a) = start(dir_a.path(), "content-id-bulk-test").await;
        let ticket_a = a.ticket().await.expect("ticket");
        let (b, _events_b) = start(dir_b.path(), "content-id-bulk-test").await;
        b.connect(ticket_a).await.expect("b connects to a");

        // Crosses both the publish-wave and the store-batch boundaries.
        let library: Vec<ItemSpec> = (0..TRACKS)
            .map(|index| ItemSpec {
                local_key: format!("track:{index}"),
                kind: ItemKind::Track,
                name: format!("Track {index}"),
                artist_names: vec![format!("Artist {}", index % 7)],
                featured_artist_names: Vec::new(),
                year: Some(2024),
                release_type: Some("album".into()),
                release_title: Some(format!("Album {}", index % 23)),
                track_number: Some((index % 12) as i32 + 1),
                disc_number: None,
                duration_seconds: Some(200.0),
                content_id: Some(content_id_of(index)),
            })
            .collect();
        let stats = b.sync_library(library).await.expect("sync");
        assert_eq!(stats.added, TRACKS);
        assert_eq!(stats.failed, 0);

        // The last item of the publish order must be findable, not only the
        // head of the library.
        for index in [0, TRACKS / 2, TRACKS - 1] {
            let wanted = content_id_of(index);
            let started = std::time::Instant::now();
            loop {
                let outcome = a.search_content_id(&wanted).await.expect("search");
                if outcome
                    .network_results
                    .iter()
                    .any(|item| item.content_id.as_deref() == Some(wanted.as_str()))
                {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(45),
                    "timed out resolving track {index} by content id"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        a.shutdown().await.expect("shutdown a");
        b.shutdown().await.expect("shutdown b");
    })
    .await
    .expect("test timed out");
}
