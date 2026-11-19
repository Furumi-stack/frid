use super::*;

const ITUNES_NS: &str = "http://www.itunes.com/dtds/podcast-1.0.dtd";

fn rss(channel_extra: &str, items: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="{ITUNES_NS}" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
  <title>Test Cast</title>
  <link>https://example.test/cast</link>
  {channel_extra}
  {items}
</channel>
</rss>"#
    )
}

fn item(guid: &str, title: &str, date: &str) -> String {
    format!(
        r#"<item>
  <title>{title}</title>
  <guid isPermaLink="false">{guid}</guid>
  <pubDate>{date}</pubDate>
  <enclosure url="https://cdn.example.test/{guid}.mp3" length="1234" type="audio/mpeg"/>
</item>"#
    )
}

#[test]
fn rss_with_itunes_tags_cdata_and_fallbacks() {
    let xml = rss(
        r#"<description><![CDATA[<p>Weekly <b>talk</b> show.</p><script>evil()</script>]]></description>
  <itunes:author>Jane Host</itunes:author>
  <itunes:image href="https://example.test/cover.jpg"/>
  <image><url>https://example.test/small.png</url><title>ignored</title></image>"#,
        r#"<item>
  <title>  Episode   one </title>
  <itunes:title>One</itunes:title>
  <description>&lt;p&gt;First &amp;amp; best&lt;/p&gt;</description>
  <guid>ep-1</guid>
  <pubDate>Tue, 02 Jan 2024 10:00:00 GMT</pubDate>
  <itunes:duration>1:02:03</itunes:duration>
  <itunes:image href="https://example.test/ep1.jpg"/>
  <link>https://example.test/ep1</link>
  <enclosure url="https://cdn.example.test/1.mp3" length="5000" type="audio/mpeg"/>
</item>
<item>
  <title></title>
  <itunes:title>Only itunes title</itunes:title>
  <content:encoded><![CDATA[<div>Long <i>show</i> notes</div>]]></content:encoded>
  <guid>ep-2</guid>
  <itunes:duration>754</itunes:duration>
  <enclosure url="https://cdn.example.test/2.m4a" type="audio/x-m4a"/>
</item>
<item>
  <itunes:summary>Summary only</itunes:summary>
  <guid>ep-3</guid>
  <enclosure url="https://cdn.example.test/3.mp3"/>
</item>"#,
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.title, "Test Cast");
    assert_eq!(feed.author.as_deref(), Some("Jane Host"));
    assert_eq!(feed.description.as_deref(), Some("Weekly talk show."));
    assert_eq!(feed.link.as_deref(), Some("https://example.test/cast"));
    assert_eq!(
        feed.image_url.as_deref(),
        Some("https://example.test/cover.jpg")
    );
    assert_eq!(feed.episodes.len(), 3);

    let first = &feed.episodes[0];
    assert_eq!(first.guid.as_deref(), Some("ep-1"));
    assert_eq!(first.title, "Episode one");
    assert_eq!(first.description.as_deref(), Some("First & best"));
    assert_eq!(first.published_at_ms, Some(1_704_189_600_000));
    assert_eq!(first.duration_ms, Some(3_723_000));
    assert_eq!(first.audio_url, "https://cdn.example.test/1.mp3");
    assert_eq!(first.mime_type.as_deref(), Some("audio/mpeg"));
    assert_eq!(first.byte_len, Some(5000));
    assert_eq!(first.link.as_deref(), Some("https://example.test/ep1"));
    assert_eq!(
        first.image_url.as_deref(),
        Some("https://example.test/ep1.jpg")
    );

    let second = &feed.episodes[1];
    assert_eq!(second.title, "Only itunes title");
    assert_eq!(second.description.as_deref(), Some("Long show notes"));
    assert_eq!(second.duration_ms, Some(754_000));
    assert_eq!(second.byte_len, None);
    assert_eq!(second.published_at_ms, None);

    let third = &feed.episodes[2];
    assert_eq!(third.title, "Untitled episode");
    assert_eq!(third.description.as_deref(), Some("Summary only"));
    assert_eq!(third.mime_type, None);
}

#[test]
fn entities_numeric_refs_and_unknown_html_entities() {
    let xml = rss(
        "",
        r#"<item>
  <title>A&nbsp;&amp;&nbsp;B &#8212; &#x41;&#1055; &mdash; &bogus; R&amp;D &copy;</title>
  <description>x &lt; y &amp;&amp; z</description>
  <guid>e</guid>
  <enclosure url="https://cdn.example.test/a.mp3?x=1&amp;y=2" type="audio/mpeg"/>
</item>"#,
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    let episode = &feed.episodes[0];
    assert_eq!(episode.title, "A & B — AП — &bogus; R&D ©");
    assert_eq!(episode.description.as_deref(), Some("x < y && z"));
    assert_eq!(episode.audio_url, "https://cdn.example.test/a.mp3?x=1&y=2");
}

#[test]
fn atom_feed() {
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title type="text">Atom Cast</title>
  <subtitle>About &lt;b&gt;things&lt;/b&gt;</subtitle>
  <link rel="self" href="https://example.test/atom.xml"/>
  <link rel="alternate" href="https://example.test/"/>
  <logo>https://example.test/logo.png</logo>
  <author><name>Atom Author</name></author>
  <entry>
<title>Entry one</title>
<id>urn:uuid:1</id>
<published>2024-03-01T12:30:00+02:00</published>
<updated>2024-03-05T00:00:00Z</updated>
<summary type="html">&lt;p&gt;Entry summary&lt;/p&gt;</summary>
<link rel="alternate" href="https://example.test/e1"/>
<link rel="enclosure" href="https://cdn.example.test/e1.ogg" type="audio/ogg" length="42"/>
  </entry>
  <entry>
<title>Entry two</title>
<id>urn:uuid:2</id>
<updated>2024-03-06T00:00:00Z</updated>
<content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml"><p>Inline</p><p>xhtml</p></div></content>
<link rel="enclosure" href="https://cdn.example.test/e2.mp3"/>
  </entry>
  <entry>
<title>No audio</title>
<id>urn:uuid:3</id>
<link rel="alternate" href="https://example.test/e3"/>
  </entry>
</feed>"#;
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.title, "Atom Cast");
    assert_eq!(feed.description.as_deref(), Some("About things"));
    assert_eq!(feed.link.as_deref(), Some("https://example.test/"));
    assert_eq!(
        feed.image_url.as_deref(),
        Some("https://example.test/logo.png")
    );
    assert_eq!(feed.author.as_deref(), Some("Atom Author"));
    assert_eq!(feed.episodes.len(), 2);
    let first = &feed.episodes[0];
    assert_eq!(first.guid.as_deref(), Some("urn:uuid:1"));
    assert_eq!(first.published_at_ms, Some(1_709_289_000_000));
    assert_eq!(first.description.as_deref(), Some("Entry summary"));
    assert_eq!(first.link.as_deref(), Some("https://example.test/e1"));
    assert_eq!(first.audio_url, "https://cdn.example.test/e1.ogg");
    assert_eq!(first.byte_len, Some(42));
    let second = &feed.episodes[1];
    assert_eq!(second.published_at_ms, Some(1_709_683_200_000));
    assert_eq!(second.description.as_deref(), Some("Inline xhtml"));
}

#[test]
fn items_without_audio_are_skipped_and_media_content_is_a_fallback() {
    let xml = r#"<rss xmlns:media="http://search.yahoo.com/mrss/"><channel><title>T</title>
<item><title>No enclosure</title><guid>a</guid></item>
<item><title>Image enclosure</title><guid>b</guid><enclosure url="https://x.test/b.jpg" type="image/jpeg"/></item>
<item><title>Empty url</title><guid>c</guid><enclosure url="" type="audio/mpeg"/></item>
<item><title>Media</title><guid>d</guid>
  <media:content url="https://x.test/d.png" medium="image"/>
  <media:group><media:content url="https://x.test/d.mp3" type="audio/mpeg" fileSize="77"/></media:group>
  <media:thumbnail url="https://x.test/d-thumb.jpg"/>
</item>
<item><title>Video</title><guid>e</guid><enclosure url="https://x.test/e.mp4" type="video/mp4"/></item>
</channel></rss>"#;
    let feed = parse_feed(xml.as_bytes()).unwrap();
    let titles: Vec<_> = feed.episodes.iter().map(|e| e.title.as_str()).collect();
    assert_eq!(titles, ["Media", "Video"]);
    assert_eq!(feed.episodes[0].audio_url, "https://x.test/d.mp3");
    assert_eq!(feed.episodes[0].byte_len, Some(77));
    assert_eq!(
        feed.episodes[0].image_url.as_deref(),
        Some("https://x.test/d-thumb.jpg")
    );
}

#[test]
fn duplicate_guids_keep_the_first_and_missing_guids_stay_absent() {
    let items = [
        item("dup", "First", "Mon, 01 Jan 2024 00:00:00 +0000"),
        item("dup", "Second", "Tue, 02 Jan 2024 00:00:00 +0000"),
        r#"<item><title>No guid</title><enclosure url="https://cdn.example.test/noguid.mp3" type="audio/mpeg"/></item>"#.to_string(),
        r#"<item><title>Blank guid</title><guid>  </guid><enclosure url="https://cdn.example.test/blank.mp3" type="audio/mpeg"/></item>"#.to_string(),
    ]
    .concat();
    let feed = parse_feed(rss("", &items).as_bytes()).unwrap();
    let pairs: Vec<_> = feed
        .episodes
        .iter()
        .map(|e| (e.guid.as_deref(), e.title.as_str(), e.audio_url.as_str()))
        .collect();
    assert_eq!(
        pairs,
        [
            (Some("dup"), "First", "https://cdn.example.test/dup.mp3"),
            (None, "No guid", "https://cdn.example.test/noguid.mp3"),
            (None, "Blank guid", "https://cdn.example.test/blank.mp3"),
        ]
    );
}

#[test]
fn durations() {
    let cases = [
        ("01:02:03", Some(3723.0)),
        ("1:02:03", Some(3723.0)),
        ("62:03", Some(3723.0)),
        ("3723", Some(3723.0)),
        ("3723.5", Some(3723.5)),
        ("00:00:01.25", Some(1.25)),
        (" 45:00 ", Some(2700.0)),
        ("0", None),
        ("00:00:00", None),
        ("", None),
        ("abc", None),
        ("1:2:3:4", None),
        ("-5", None),
        ("1e3", None),
        ("NaN", None),
        ("12:", None),
    ];
    for (input, expected) in cases {
        assert_eq!(parse_duration(input), expected, "{input:?}");
    }
}

#[test]
fn dates() {
    let reference = Some(1_704_189_600); // 2024-01-02 10:00:00 UTC
    let cases = [
        ("Tue, 02 Jan 2024 10:00:00 GMT", reference),
        ("Tue, 02 Jan 2024 10:00:00 +0000", reference),
        ("02 Jan 2024 10:00:00 UT", reference),
        ("Tue, 2 Jan 2024 13:00:00 +0300", reference),
        ("Tue,  02  Jan  2024   05:00:00   EST", reference),
        ("Tue, 02 Jan 2024 02:00:00 PST", reference),
        ("Tuesday, 02 January 2024 10:00 GMT", reference),
        ("Tue, 02 Jan 24 10:00:00 GMT", reference),
        ("Tue, 02 Jan 2024 13:00:00 +03:00", reference),
        ("Tue, 02 Jan 2024 10:00:00", reference),
        ("Вт, 02 янв 2024 13:00:00 +0300", reference),
        ("2024-01-02T10:00:00Z", reference),
        ("2024-01-02T13:00:00+03:00", reference),
        ("2024-01-02T10:00:00.123Z", reference),
        ("2024-01-02 10:00:00", reference),
        ("2024-01-02", Some(1_704_153_600)),
        ("not a date", None),
        ("Tue, 31 Feb 2024 10:00:00 GMT", None),
        ("Tue, 02 Foo 2024 10:00:00 GMT", None),
        ("2024-13-02T10:00:00Z", None),
        ("", None),
    ];
    for (input, expected) in cases {
        assert_eq!(parse_date(input), expected, "{input:?}");
    }
}

#[test]
fn non_ascii_zones_do_not_panic() {
    for input in [
        "Mon, 07 Nov 2022 10:00:00 +й0",
        "Mon, 07 Nov 2022 10:00:00 -йй",
        "Mon, 07 Nov 2022 10:00+й0",
        "Mon, 07 Nov 2022 10:00:00 +0й00",
        "2022-11-07T10:00:00+0й0",
        "2022-11-07T10:00:00+й",
        "2022-11-07T10:00:00-€:00",
        "2022-11-07T10:00:00+日本",
    ] {
        assert_eq!(parse_date(input), None, "{input:?}");
    }
    // Exhaustive-ish: every short suffix of mixed ASCII / multi-byte
    // characters after a sign.
    let alphabet = ['0', '9', ':', 'й', '€', '日', '+', '-'];
    for a in alphabet {
        for b in alphabet {
            for c in alphabet {
                for prefix in ["Mon, 07 Nov 2022 10:00:00 ", "2022-11-07T10:00:00"] {
                    for sign in ['+', '-'] {
                        let _ = parse_date(&format!("{prefix}{sign}{a}{b}{c}"));
                    }
                }
            }
        }
    }
    // In a feed, the bad date only loses the date.
    let xml = rss("", &item("z", "Zone", "Mon, 07 Nov 2022 10:00:00 +й0"));
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.episodes.len(), 1);
    assert_eq!(feed.episodes[0].published_at_ms, None);
}

#[test]
fn media_urls_are_resolved_and_must_be_http() {
    let xml = rss(
        r#"<itunes:image href="/img/cover.jpg"/>"#,
        r#"<item><guid>rel</guid><title>Relative</title>
  <itunes:image href="ep.png"/>
  <enclosure url="/audio/1.mp3" type="audio/mpeg"/></item>
<item><guid>proto</guid><title>Protocol relative</title>
  <enclosure url="//cdn.example.test/2.mp3" type="audio/mpeg"/></item>
<item><guid>file</guid><title>Local file</title>
  <enclosure url="file:///etc/passwd" type="audio/mpeg"/></item>
<item><guid>ftp</guid><title>FTP</title>
  <enclosure url="ftp://example.test/3.mp3" type="audio/mpeg"/></item>
<item><guid>fallback</guid><title>Media fallback</title>
  <enclosure url="file:///tmp/x.mp3" type="audio/mpeg"/>
  <media:content xmlns:media="http://search.yahoo.com/mrss/" url="https://cdn.example.test/4.mp3" type="audio/mpeg"/></item>
<item><guid>abs</guid><title>Absolute</title>
  <enclosure url="HTTPS://cdn.example.test/5.mp3" type="audio/mpeg"/></item>"#,
    );
    let feed =
        parse_feed_with_base(xml.as_bytes(), Some("https://host.test/feeds/rss.xml")).unwrap();
    let urls: Vec<(&str, &str)> = feed
        .episodes
        .iter()
        .map(|e| (e.guid.as_deref().unwrap_or_default(), e.audio_url.as_str()))
        .collect();
    assert_eq!(
        urls,
        [
            ("rel", "https://host.test/audio/1.mp3"),
            ("proto", "https://cdn.example.test/2.mp3"),
            ("fallback", "https://cdn.example.test/4.mp3"),
            ("abs", "HTTPS://cdn.example.test/5.mp3"),
        ]
    );
    assert_eq!(
        feed.image_url.as_deref(),
        Some("https://host.test/img/cover.jpg")
    );
    assert_eq!(
        feed.episodes[0].image_url.as_deref(),
        Some("https://host.test/feeds/ep.png")
    );
    for episode in &feed.episodes {
        assert!(is_remote_url(&episode.audio_url), "{}", episode.audio_url);
    }

    // Without a base, relative media cannot be resolved and is dropped;
    // protocol-relative URLs default to https.
    let feed = parse_feed(xml.as_bytes()).unwrap();
    let guids: Vec<&str> = feed
        .episodes
        .iter()
        .map(|e| e.guid.as_deref().unwrap_or_default())
        .collect();
    assert_eq!(guids, ["proto", "fallback", "abs"]);
    assert_eq!(feed.image_url, None);
}

#[test]
fn rss_with_an_unrelated_default_namespace() {
    for ns in ["http://example.test/x", "http://www.w3.org/2005/Atom"] {
        let xml = format!(
            r#"<rss version="2.0" xmlns="{ns}" xmlns:atom="http://www.w3.org/2005/Atom"><channel><title>T</title>
<atom:link href="https://a.test/feed" rel="self"/>
<item><guid>1</guid><title>One</title><enclosure url="https://a.test/1.mp3" type="audio/mpeg"/></item>
</channel></rss>"#
        );
        let feed = parse_feed(xml.as_bytes()).unwrap();
        assert_eq!(feed.title, "T", "{ns}");
        assert_eq!(feed.episodes.len(), 1, "{ns}");
        assert_eq!(feed.episodes[0].title, "One");
    }
}

#[test]
fn html_pages_and_garbage_are_rejected() {
    let html = br#"<!DOCTYPE html><html><head><title>Vivat</title></head><body><p>Hi &nbsp; there</p></body></html>"#;
    let err = parse_feed(html).unwrap_err().to_string();
    assert!(err.contains("not an RSS or Atom feed"), "{err}");
    for garbage in [
        &b""[..],
        b"hello world",
        b"\x00\xff\xfe garbage",
        b"{\"json\": true}",
        b"<",
        b"<<<>>>",
    ] {
        let err = parse_feed(garbage).unwrap_err().to_string();
        assert!(
            err.contains("not an RSS or Atom feed"),
            "{garbage:?}: {err}"
        );
    }
}

#[test]
fn truncated_and_malformed_xml_never_panics() {
    let items: String = (0..5)
        .map(|i| item(&format!("g{i}"), &format!("T {i} &amp; &nbsp;"), "bad date"))
        .collect();
    let xml = rss(
        "<description><![CDATA[cdata]]></description><itunes:image href=\"x\"/>",
        &items,
    );
    let bytes = xml.as_bytes();
    for len in 0..=bytes.len() {
        let _ = parse_feed(&bytes[..len]);
    }
    // Mismatched end tags are tolerated.
    let sloppy = r#"<rss><channel><title>S</title><item><title>A</b></title><guid>1</guid><enclosure url="https://x.test/1.mp3"/></item></channel></rss>"#;
    let feed = parse_feed(sloppy.as_bytes()).unwrap();
    assert_eq!(feed.episodes.len(), 1);
    assert_eq!(feed.episodes[0].title, "A");
    // Truncation after complete items keeps them.
    let third = xml.find("<title>T 3").unwrap();
    let cut = &bytes[..xml[..third].rfind("<item>").unwrap()];
    let feed = parse_feed(cut).unwrap();
    assert_eq!(feed.episodes.len(), 3);
}

#[test]
fn deep_nesting_does_not_overflow() {
    let depth = 100_000;
    let mut xml = String::from("<rss><channel><title>Deep</title>");
    xml.push_str(&"<x>".repeat(depth));
    xml.push_str(&"</x>".repeat(depth));
    xml.push_str(&item("after", "After", ""));
    xml.push_str("</channel></rss>");
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.episodes.len(), 1);
}

#[test]
fn cyrillic_titles_in_utf8_and_windows_1251() {
    let xml = rss(
        "<itunes:author>Иван Петров</itunes:author>",
        &item(
            "1",
            "Выпуск №1: «Привет»",
            "Mon, 01 Jan 2024 00:00:00 +0300",
        ),
    )
    .replace("Test Cast", "Подкаст");
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.title, "Подкаст");
    assert_eq!(feed.author.as_deref(), Some("Иван Петров"));
    assert_eq!(feed.episodes[0].title, "Выпуск №1: «Привет»");

    let cp1251 = xml.replace("encoding=\"UTF-8\"", "encoding=\"windows-1251\"");
    let (encoded, _, unmappable) = encoding_rs::WINDOWS_1251.encode(&cp1251);
    assert!(!unmappable);
    let feed = parse_feed(&encoded).unwrap();
    assert_eq!(feed.title, "Подкаст");
    assert_eq!(feed.episodes[0].title, "Выпуск №1: «Привет»");

    // Invalid UTF-8 without a declaration is decoded lossily.
    let mut broken = xml.into_bytes();
    let at = broken
        .windows(7)
        .position(|w| w == b"Test Ca")
        .unwrap_or(10);
    broken.insert(at, 0xff);
    assert!(parse_feed(&broken).is_ok());
}

#[test]
fn namespace_prefixes_are_matched_tolerantly() {
    // Undeclared `itunes:` prefix and a custom prefix bound to the
    // iTunes namespace.
    let xml = format!(
        r#"<rss><channel><title>P</title><itunes:author>Undeclared</itunes:author>
<item xmlns:it="{ITUNES_NS}"><guid>1</guid><it:title>Custom prefix</it:title><it:duration>90</it:duration>
<enclosure url="https://x.test/1.mp3" type="audio/mpeg"/></item></channel></rss>"#
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.author.as_deref(), Some("Undeclared"));
    assert_eq!(feed.episodes[0].title, "Custom prefix");
    assert_eq!(feed.episodes[0].duration_ms, Some(90_000));
}

#[test]
fn long_descriptions_are_truncated() {
    let long = "word ".repeat(2000);
    let text = html_to_text(&long, 4000);
    assert!(text.chars().count() <= 4000);
    assert!(text.ends_with('…'));
    let xml = rss(&format!("<description>{long}</description>"), "");
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert!(feed.description.unwrap().chars().count() <= 4000);
}

#[test]
fn itunes_season_episode_and_type() {
    let xml = rss(
        "",
        r#"<item><guid>a</guid><title>A</title>
  <itunes:season>2</itunes:season><itunes:episode> 7 </itunes:episode>
  <itunes:episodeType>Trailer</itunes:episodeType>
  <enclosure url="https://cdn.example.test/a.mp3" type="audio/mpeg"/></item>
<item><guid>b</guid><title>B</title>
  <itunes:season>two</itunes:season><itunes:episode>-1</itunes:episode>
  <itunes:episodeType>mystery</itunes:episodeType>
  <enclosure url="https://cdn.example.test/b.mp3" type="audio/mpeg"/></item>
<item><guid>c</guid><title>C</title><itunes:episodeType>bonus</itunes:episodeType>
  <itunes:episode>99999999999</itunes:episode>
  <enclosure url="https://cdn.example.test/c.mp3" type="audio/mpeg"/></item>"#,
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    let a = &feed.episodes[0];
    assert_eq!((a.season, a.episode_number), (Some(2), Some(7)));
    assert_eq!(a.episode_type, EpisodeType::Trailer);
    let b = &feed.episodes[1];
    assert_eq!((b.season, b.episode_number), (None, None));
    assert_eq!(b.episode_type, EpisodeType::Full);
    let c = &feed.episodes[2];
    assert_eq!((c.season, c.episode_number), (None, None));
    assert_eq!(c.episode_type, EpisodeType::Bonus);
}

#[test]
fn guid_permalink_flag() {
    let xml = rss(
        "",
        r#"<item><guid isPermaLink="false">x</guid><enclosure url="https://c.test/x.mp3"/></item>
<item><guid>https://e.test/y</guid><enclosure url="https://c.test/y.mp3"/></item>
<item><guid isPermaLink=" FALSE ">z</guid><enclosure url="https://c.test/z.mp3"/></item>
<item><enclosure url="https://c.test/n.mp3"/></item>"#,
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    let flags: Vec<Option<bool>> = feed.episodes.iter().map(|e| e.guid_is_permalink).collect();
    assert_eq!(flags, [Some(false), Some(true), Some(false), None]);
}

#[test]
fn new_feed_url_is_reported() {
    let xml = rss(
        "<itunes:new-feed-url> https://new.example.test/feed?x=1 </itunes:new-feed-url>",
        "",
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(
        feed.new_feed_url.as_deref(),
        Some("https://new.example.test/feed?x=1")
    );
    let alias = FeedUrl::parse(feed.new_feed_url.as_deref().unwrap()).unwrap();
    assert_eq!(alias.as_str(), "https://new.example.test/feed?x=1");
}

#[test]
fn oversized_guid_is_treated_as_absent() {
    let long = "g".repeat(MAX_GUID_BYTES + 1);
    let xml = rss(
        "",
        &format!(r#"<item><guid>{long}</guid><enclosure url="https://c.test/1.mp3"/></item>"#),
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.episodes[0].guid, None);
}

#[test]
fn media_urls_with_spaces_are_percent_encoded() {
    let xml = rss(
        "",
        r#"<item><guid>s</guid><enclosure url="https://c.test/my file.mp3" type="audio/mpeg"/></item>"#,
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert_eq!(feed.episodes[0].audio_url, "https://c.test/my%20file.mp3");
}

#[test]
fn parsed_feed_maps_to_valid_contract_metadata() {
    let base = FeedUrl::parse("https://example.test/feed.xml").unwrap();
    let items = [
        item("g1", "One", "Tue, 02 Jan 2024 10:00:00 GMT"),
        r#"<item><title>No guid</title><link>https://example.test/no-guid</link>
  <pubDate>Wed, 03 Jan 2024 10:00:00 GMT</pubDate>
  <enclosure url="https://cdn.example.test/ng.mp3" type="audio/mpeg"/></item>"#
            .to_string(),
        // Same page as the channel link: not identifying.
        r#"<item><title>Home link</title><link>https://example.test/cast</link>
  <enclosure url="https://cdn.example.test/home.mp3" type="audio/mpeg"/></item>"#
            .to_string(),
    ]
    .concat();
    let feed = parse_feed_with_base(rss("", &items).as_bytes(), Some(base.as_str())).unwrap();
    let podcast_id = PodcastId::from_feed_url(&base);
    let ids = feed.resolve_episode_ids(&podcast_id, &[]);
    assert_eq!(ids[0], EpisodeId::from_guid(&podcast_id, "g1"));
    assert_eq!(
        ids[1],
        Some(EpisodeId::derive(
            &podcast_id,
            &crate::podcast::EpisodeKey::link("https://example.test/no-guid").unwrap()
        ))
    );
    assert_eq!(
        ids[2],
        Some(EpisodeId::derive(
            &podcast_id,
            &crate::podcast::EpisodeKey::enclosure("https://cdn.example.test/home.mp3").unwrap()
        ))
    );

    let podcast = feed.to_podcast_meta(base.clone());
    podcast.validate().unwrap();
    for (episode, id) in feed.episodes.iter().zip(ids) {
        let meta = episode.to_meta(&podcast_id, id.unwrap());
        meta.validate().unwrap();
        meta.to_ref(&podcast).validate().unwrap();
    }

    // A later refresh with a changed title and audio URL keeps every id,
    // using the stored hints of the previous refresh.
    let known: Vec<KnownEpisode> = feed
        .episodes
        .iter()
        .zip(feed.resolve_episode_ids(&podcast_id, &[]))
        .map(|(episode, id)| KnownEpisode {
            episode_id: id.unwrap(),
            hints: episode.identity_hints(),
        })
        .collect();
    let refreshed = items
        .replace("Home link", "Home link (edited)")
        .replace("cdn.example.test/ng.mp3", "cdn2.example.test/ng.mp3");
    let feed2 = parse_feed_with_base(rss("", &refreshed).as_bytes(), Some(base.as_str())).unwrap();
    let ids2 = feed2.resolve_episode_ids(&podcast_id, &known);
    let expected: Vec<Option<EpisodeId>> =
        known.iter().map(|k| Some(k.episode_id.clone())).collect();
    assert_eq!(ids2, expected);
}

#[test]
fn hostile_sizes_stay_bounded() {
    let huge_title = "t".repeat(MAX_CAPTURE_BYTES * 2);
    let xml = rss(
        "",
        &format!(
            r#"<item><guid>h</guid><title>{huge_title}</title><enclosure url="https://c.test/h.mp3"/></item>"#
        ),
    );
    let feed = parse_feed(xml.as_bytes()).unwrap();
    assert!(feed.episodes[0].title.chars().count() <= MAX_TITLE_CHARS);
    let duration = rss(
        "",
        r#"<item><guid>d</guid><itunes:duration>99999999999999999999</itunes:duration><enclosure url="https://c.test/d.mp3"/></item>"#,
    );
    assert_eq!(
        parse_feed(duration.as_bytes()).unwrap().episodes[0].duration_ms,
        None
    );
}
