//! Lyrics is an optional ALPN: compatible peers exchange texts, old peers keep
//! their DHT protocol and simply cannot serve the new stream.
use music_dht::lyrics::{self, LYRICS_ALPN, LyricsRequest, LyricsResponse, LyricsVersion};
use music_dht::{MusicDhtConfig, MusicDhtService, NetworkId};
use std::time::Duration;

#[tokio::test]
async fn optional_lyrics_stream_roundtrip_and_legacy_peer() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let network = NetworkId::from_name(&server_dir.path().to_string_lossy());
        let config = MusicDhtConfig::builder()
            .data_dir(server_dir.path())
            .network_id(network)
            .schema_independent_stream_protocol(LYRICS_ALPN)
            .build()
            .unwrap();
        let (server, _server_events) = MusicDhtService::start(config).await.unwrap();
        let config = MusicDhtConfig::builder()
            .data_dir(client_dir.path())
            .network_id(network)
            .build()
            .unwrap();
        let (legacy, _client_events) = MusicDhtService::start(config).await.unwrap();
        legacy
            .connect(server.ticket().await.unwrap())
            .await
            .unwrap();
        let mut acceptor = server.stream_acceptor(LYRICS_ALPN).unwrap();
        let task = tokio::spawn(async move {
            let mut stream = acceptor.accept().await.unwrap();
            let request = lyrics::read_request_from(&mut stream.recv).await.unwrap();
            let response = LyricsResponse {
                version: 1,
                content_id: request.content_id,
                versions: vec![LyricsVersion {
                    id: "original".into(),
                    label: "Original".into(),
                    source: "manual".into(),
                    source_id: None,
                    language: Some("en".into()),
                    plain_text: Some("Words".into()),
                    synced_lrc: Some("[00:01]Words".into()),
                    instrumental: false,
                }],
            };
            lyrics::write_response(&mut stream, &response)
                .await
                .unwrap();
            stream.send.finish().unwrap();
            let _ = stream.send.stopped().await;
        });
        let request = LyricsRequest::new(format!("b3:{}", "a".repeat(64))).unwrap();
        let mut stream = legacy
            .open_stream(server.endpoint_id(), LYRICS_ALPN)
            .await
            .unwrap();
        lyrics::write_request(&mut stream, &request).await.unwrap();
        stream.send.finish().unwrap();
        let response = lyrics::read_response_from(&mut stream.recv).await.unwrap();
        assert_eq!(response.content_id, request.content_id);
        assert_eq!(
            response.versions[0].synced_lrc.as_deref(),
            Some("[00:01]Words")
        );
        task.await.unwrap();
        assert!(
            server
                .open_stream(legacy.endpoint_id(), LYRICS_ALPN)
                .await
                .is_err()
        );
        assert!(
            !music_dht::capabilities::CapabilityManifest::frid("legacy", "1")
                .protocols
                .contains_key(lyrics::LYRICS_ID)
        );
        legacy.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    })
    .await
    .expect("lyrics integration timed out");
}
