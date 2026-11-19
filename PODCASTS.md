# Podcast contract

Status: normative for `music-dht` 0.6 and device-sync protocol v4.

Podcasts are a separate content type shared by every Furumi client. This
document defines what all clients must agree on: identity, normalization,
private state and its merge rules, trusted-device sync operations, snapshot
recovery, queue items, version gating and Jam privacy. The Rust definitions
live in `music_dht::podcast` (pure types and functions) and
`music_dht::device_sync` (wire DTOs and gating helpers). Clients must use
them instead of local copies.

Public discovery (signed podcast cards in a separate DHT overlay) is not part
of this version of the contract; it will be specified with the overlay.

## Responsibilities

| Layer | Owns |
| --- | --- |
| `music_dht::podcast` | ids, URL canonicalization, no-GUID rule, metadata DTOs and bounds, merge functions, RSS/Atom parser (feature `podcast-feed`) |
| `music_dht::device_sync` | sync operation kinds, snapshot sections, `PlaybackSource`, version gating, Jam redaction |
| Client storage (`furumi-library`, web PostgreSQL) | persistence and migrations implementing the rules below |
| Client runtime | HTTP fetching, conditional requests, scheduling, audio buffering and decoding, UI |

Audio is always fetched from the publisher. It is never imported into the
music library, never content-addressed with a BLAKE3 content id and never
offered for P2P transfer. RSS refreshes fetch metadata only.

## Data classes

| Data | Trusted devices | Public federation | Local only |
| --- | --- | --- | --- |
| Subscription state, channel metadata, feed aliases | `podcast_subscription_set` + snapshot | never | |
| Share decision | `podcast_share_set` + snapshot | only the published card (future overlay) | |
| Episode progress | `podcast_progress_set` + snapshot | never | |
| Listening sessions, play counts | `podcast_listen_recorded` + snapshot | never | |
| Episode catalog | each device parses RSS itself; `PodcastEpisodeRef` travels with progress and queue items | never | |
| ETag/Last-Modified, last check time, fetch errors, artwork cache, audio buffer | | | yes |

Synchronizing to a trusted device is not permission to publish. Private
podcasts, their URLs, descriptions and episodes must not reach DHT records,
public catalogs, public search results or Jam participants.

## Identity

### Feed URL canonicalization

`FeedUrl::parse` produces the canonical form. Every client must use it (or
an implementation that passes the same tests) before deriving ids or
comparing URLs.

1. Only `http://` and `https://` are accepted (case-insensitive) and the
   scheme is lowercased.
2. Userinfo (`user@`, `user:pass@`), whitespace, control characters and `\`
   are rejected.
3. The host is lowercased. Non-ASCII hosts are rejected; pass the IDNA
   (`xn--`) form as produced by WHATWG URL parsers. Bracketed IPv6 literals
   are accepted.
4. An empty port and the default port (80 for http, 443 for https) are
   removed; other ports lose leading zeros.
5. The fragment is removed.
6. An empty path becomes `/`. The rest of the path and the **entire** query
   string are kept byte-for-byte: parameter order, case, percent-encoding and
   dot segments are preserved. Query parameters can be part of the source
   identity (for example a per-user token) and must never be dropped. Only
   non-ASCII characters are percent-encoded (UTF-8, uppercase hex).
7. The result is at most 2048 bytes. Canonicalization is idempotent;
   deserialization accepts only canonical strings.

| Input | Canonical |
| --- | --- |
| `HTTPS://Example.COM:443/Feed.xml?b=2&a=1#top` | `https://example.com/Feed.xml?b=2&a=1` |
| `http://example.com:80` | `http://example.com/` |
| `https://example.com/rss?token=XyZ&x=%2f` | unchanged |
| `https://user@example.com/rss` | rejected |

`FeedUrl::from_user_input` is for typed or pasted input only: it trims, maps
`feed:`, `podcast:`, `pcast:`, `itpc:` to `https://` and adds `https://` when
the scheme is missing, then canonicalizes.

### Podcast id

```text
podcast_id = "pod1:" + base32_lower_nopad(BLAKE3("furumi-podcast:podcast:v1" || 0x00 || canonical_feed_url))
```

The id is derived **once**, from the canonical URL of the first subscription.
It never changes afterwards.

### Feed aliases

Redirect targets (permanent redirects), `itunes:new-feed-url` and URLs used to
subscribe again are feed aliases of an existing podcast:

* the subscription record carries `feed_aliases`, a grow-only set merged
  independently of the last-writer-wins register (`merge_feed_aliases`). The
  URL that derives the podcast id is always kept, plus the 8 smallest other
  URLs; this bounded union is commutative, associative and idempotent;
* `podcast.feed_url` is the URL currently fetched and is always added to the
  aliases;
* before deriving a new id for a URL, clients call `resolve_podcast_id` with
  every known `(podcast_id, url)` pair. A match reuses the existing podcast,
  its progress and its history. With several matches the smallest id wins.

Limitation: two devices that subscribe offline, through different URLs of
the same feed, before either learns the alias, create two podcast ids. The
contract does not merge podcast ids automatically in this version.

### Episode id

```text
episode_id = "ep1:" + base32_lower_nopad(BLAKE3("furumi-podcast:episode:v1" || 0x00 || podcast_id || key))
```

| Key | Material |
| --- | --- |
| GUID | `guid:` + RSS `<guid>` / Atom `<id>` trimmed, case-sensitive |
| link | `link:` + item `<link>` trimmed |
| date + title | `date-title:` + publication unix seconds + `\0` + normalized title |
| enclosure | `enclosure:` + enclosure URL trimmed |

The normalized title is `music_dht::normalize_name` (NFKC, lowercase,
non-alphanumeric runs collapsed). GUIDs longer than 2048 bytes are treated as
absent. Changing the title, artwork, description or audio URL of an episode
never changes its id and never resets its progress.

### No-GUID rule

`resolve_episode_ids(podcast_id, channel_link, items, known)` is normative.
For one parsed feed document:

1. Items with a GUID get the GUID id. A GUID never matches another known
   episode, so the result does not depend on local history.
2. An item without GUID first searches the known episodes of the podcast,
   stage by stage: same enclosure URL, then same `<link>`, then same
   publication second and normalized title. The first stage with candidates
   wins and the smallest candidate id is reused. A known episode can be
   claimed by only one item of the document.
3. Otherwise a new id is derived from the first available key: `<link>`,
   then date + title, then enclosure URL.

A `<link>` is identifying only if it is unique in the document and differs
from the channel `<link>` (many feeds repeat the homepage in every item).
Enclosure URLs and date + title pairs shared by several GUID-less items are
not identifying either. Items without any usable key, and duplicates of an
earlier item of the same document, get no id and are skipped.

"Known episodes" include episodes from earlier refreshes **and** episodes
learned through sync (`PodcastEpisodeRef::identity_hints`). Feed GUID-less
episodes into it so a device that sees an episode first through sync keeps
the id used by the other devices.

Limitation: for GUID-less episodes the id can depend on what a device saw
first. If a publisher changes several identifying fields of a GUID-less
episode at once, a device that never saw the old version can derive a
different id.

## Metadata and bounds

| Field | Bound |
| --- | --- |
| titles, author | 1000 characters, non-empty titles |
| descriptions | 4000 characters (plain text) |
| URLs | 2048 bytes, audio and artwork URLs absolute `http(s)` |
| GUID | 2048 bytes |
| MIME | 128 printable ASCII bytes |
| device, session, operation ids | 1..256 bytes, no control characters |
| feed aliases | 8 plus the origin URL |

`PodcastMeta` (channel), `EpisodeMeta` (episode, including season, episode
number, `itunes:episodeType`, publication time and duration in milliseconds,
audio URL, MIME, byte length, artwork and link) and `PodcastEpisodeRef`
(queue item) validate these bounds. `PodcastMeta::bounded` and
`EpisodeMeta::to_ref` truncate publisher data to fit. Receivers drop records
that fail validation.

Episodes without a season are shown in their own group. Unknown
`itunes:episodeType` values are `full`.

## Personal state and merge rules

All merge functions are commutative, associative and idempotent per entity;
ties are broken by a total order over all fields. The same set of operations
therefore yields the same state in any delivery order, with duplicates.
`hlc_ms` is the sync operation timestamp; it is not a true hybrid logical
clock, so ordering of user intent on progress uses a Lamport generation.

### Subscription

* Register: `subscribed` + `podcast` metadata, last-writer-wins by
  `(hlc_ms, op_id)`.
* Unsubscribing writes `subscribed = false`. It is state, not a tombstone,
  and is never compacted, so it beats a stale subscription from a device that
  returns after being offline.
* Unsubscribing stops monitoring but keeps progress and history.
  Subscribing again continues where the user stopped. Erasing history is a
  separate explicit action (not part of this version).
* Only explicit subscribe and unsubscribe actions write the register. RSS
  refreshes keep their metadata locally; a background refresh that wrote the
  register could resurrect an unsubscribed podcast.
* Subscribing to a podcast already subscribed locally must not emit a new
  operation.

### Share

* `ShareRecord`, last-writer-wins by `(hlc_ms, op_id)`. `shared = false` is a
  tombstone that revokes the publication.
* Unsubscribing also writes `shared = false`.
* `effective_share(subscription, share)` decides whether to publish: the user
  is subscribed, chose to share, **and** the share decision is newer than the
  latest subscription write. Every new subscription, including subscribing
  again or subscribing from someone else's public card, is private until the
  user shares it again, and a stale share decision cannot republish.

### Progress

`ProgressRecord { episode_id, podcast_id, position_ms, status, generation,
session_id, hlc_ms, device_id, op_id }`, with `status` one of `not_started`,
`in_progress`, `completed`. The winner is the greatest
`(generation, hlc_ms, device_id, op_id)`. Progress is never the maximum
position: seeking back and "start over" are real changes.

* Explicit actions use `generation = max_seen + 1`
  (`ProgressRecord::explicit_generation`): starting a listening session,
  seeking, "start over", "mark played", "mark unplayed", handing playback to
  another device. `max_seen` is the generation of the current register,
  including received records.
* Periodic checkpoints and pause/stop saves within a session keep the
  generation of that session's latest explicit action.
* Write progress periodically (every 15–30 s) and on pause, seek, stop,
  episode change and handoff, in the background. Coalesce pending checkpoints
  so the outgoing log keeps only the latest per episode.

| Events | Result |
| --- | --- |
| A checkpoints session s1 at g3; B seeks back at g4 with a slower clock; A's late g3 checkpoint arrives | B's seek (g4) wins |
| A restarts (g3, position 0); a queued g2 checkpoint of the old session arrives later | restart wins |
| A and B both saw g5 and listen offline; both start sessions at g6 | the later `hlc_ms` (then `device_id`, `op_id`) wins on every device; both sessions are kept |

Ordinary Play resumes from `resume_position_ms()` (the position when in
progress, otherwise 0). "Start over" is a separate explicit action. Publisher
metadata changes never touch progress; if the duration or audio changed, the
client keeps the stored position and lets the user decide.

### Listening sessions and counts

`ListenSession { session_id, episode_id, podcast_id, device_id,
started_at_ms, ended_at_ms, start_position_ms, end_position_ms, listened_ms,
completed }` forms a grow-only set keyed by `session_id` (generated at
playback start). A device may resend its session with more listening time;
versions merge to the greatest `(completed, listened_ms, ended_at_ms, ...)`.

* A session is recorded in history when `completed` or
  `listened_ms >= 30 000` (`should_record`).
* `completed` is set when the session reaches the end of the stream or
  `reaches_completion(position_ms, duration_ms)`: within the smaller of 30 s
  and 5 % of the duration from the end. Completing also writes progress with
  status `completed` (explicit generation).
* The play count of an episode is the number of distinct completed sessions
  (`completed_listen_count`). Each session counts at most once, so
  re-delivery never double counts, and listening again is a new session.
* "Mark played" only writes progress; it does not create a session or invent
  listening time.
* Concurrent offline sessions are all kept; there is no promise of a single
  global session during a partition.

## Trusted-device sync (protocol v4)

The ALPN remains `furumi/sync/2`; device profiles advertise
`protocol_version = 4`. New operation kinds:

| Kind | Payload | Materialization | Tombstone |
| --- | --- | --- | --- |
| `podcast_subscription_set` | `podcast_id`, `subscribed`, `podcast`, `feed_aliases` | `SyncOpWire::podcast_subscription` | no |
| `podcast_share_set` | `podcast_id`, `shared` | `SyncOpWire::podcast_share` | `shared = false` |
| `podcast_progress_set` | `episode_id`, `podcast_id`, `position_ms`, `status`, `generation`, `session_id`, optional `episode` ref | `SyncOpWire::podcast_progress` | no |
| `podcast_listen_recorded` | `session`, optional `episode` ref | `SyncOpWire::podcast_listen_session` | no |

Register stamps come from the operation: `hlc_ms`, `op_id`, and for progress
`device_id = origin_device_id`. A session's `device_id` must equal the
operation origin. `SyncOpWire::validate_podcast` checks this and the bounds;
receivers skip invalid podcast operations but still acknowledge them, so the
log keeps moving.

`DeviceProfileSet` gains an optional `protocol_version`. Each device reports
its own version. Receivers store
`profile_protocol_version(reported, previously_known)` and relays forward the
stored version of other devices in `DeviceProfileWire.protocol_version`. They
must never substitute their own constant: version gating would then send
podcast state to an old client. A device that never reported a version is
treated as version 1.

### Snapshot recovery

Sync vectors only grow, so operations hidden from a peer while it ran an
older version are never re-requested after it upgrades. Podcast state is
therefore part of `SyncSnapshot`:

| Section | Content | Limit |
| --- | --- | --- |
| `podcast_subscriptions` | all subscription registers, including unsubscribed | 1000 |
| `podcast_shares` | all share registers, including `shared = false` | 1000 |
| `podcast_progress` | progress registers | 5000 |
| `podcast_listen_sessions` | most recent sessions | 2000 |

`SyncSnapshot::bound_podcast_state` orders each section newest first, caps it
and fits all sections into a 4 MiB JSON budget in the priority order above,
leaving room for music state inside the 8 MiB line limit. Records that do not
fit still propagate through operations. Receivers ignore podcast sections
that exceed the counts (`podcast_state_within_limits`), validate each record
and merge it with the same functions as operations. A new or long-absent
device thus converges from one snapshot.

Empty sections are not serialized, so snapshots without podcast state are
byte-identical to v3.

## Queue and playback

`PlaybackTrack.source: Option<PlaybackSource>` makes the item source
explicit: absent or `music` for music, `podcast_episode` with a
`PodcastEpisodeRef`, or `private` for a Jam placeholder. Negative ids,
placeholder releases and URLs in `file_path` are not part of the contract.
`PlaybackTrack::podcast_episode` fills the legacy display fields (episode
title, podcast title as artist and release, `id = 0`, empty `file_path`, no
`content_id`); receivers read `source`.

`PodcastEpisodeRef` is self-contained: podcast and episode ids, canonical
feed URL, titles, audio URL, MIME, duration, publication time, season,
episode number, artwork. A device without the subscription can display,
resume and play the item; if the audio URL is stale it refreshes the feed and
looks the episode up by id. Mixed queues keep order, position and identity of
every item.

Handoff follows `PLAYBACK_PROTOCOL.md`: record position and play/pause
intent, transfer the queue with its sources, the receiver opens the HTTP
source and seeks, and ownership moves only through the coordination claim.

`PlaybackStateWire.playback_rate: Option<f32>` is reserved. No client
implements variable speed yet; receivers treat a missing or unsupported value
as 1.0. When implemented, position extrapolation and handoff must use it.

## Version gating for peers below v4

`SyncOpPayload` and `PlaybackSource` are closed serde enums. A v3 client that
receives an unknown `kind` fails to parse the **whole** message, not one
operation, which would stall synchronization. Additive `serde(default)`
fields alone do not make this compatible. Therefore:

1. Never send podcast operations to a peer below v4
   (`op_allowed_for_peer`).
2. Never send podcast snapshot sections (`SyncSnapshot::for_peer`) or a
   playback snapshot whose queue contains podcast items
   (`playback_allowed_for_peer`) to a peer below v4.
3. Refuse a handoff to a device below v4 while the queue contains podcast
   items and tell the user that the target must be updated. Never strip the
   episodes to make the queue fit.
4. Reject `SetState`/`ActiveChanged` from a peer below v4 while the local
   queue contains podcast items (`playback_command_allowed_from_peer`): its
   state would silently drop them.
5. Music operations and music-only playback keep working with v1–v3 peers.

## Jam privacy

Jam participants are outside the trusted-device group.

* Hosts send `jam::redact_jam_snapshot` (or `redact_for_jam` on the state):
  every podcast item becomes a `private` placeholder titled "Private item"
  without URL, feed, ids, titles or duration; music items, order and position
  are kept.
* Commands containing real podcast episodes are rejected
  (`jam_command_allowed`): participants cannot add episodes.
* Before applying a participant's state, hosts call
  `restore_jam_private_items`: the k-th placeholder maps to the host's k-th
  private item. States that drop, duplicate or inject private items are
  rejected. Participants may still pause, seek and reorder music.

## Feed parser

With the `podcast-feed` feature, `music_dht::podcast::feed` parses RSS 2.0,
RSS 1.0 and Atom tolerantly: BOM and declared single-byte encodings, HTML
entities, CDATA, mismatched end tags, undeclared prefixes, depth and size
limits, lenient dates and durations, relative media URLs resolved against
the fetch URL. Items without an absolute `http(s)` audio URL are dropped. It
extracts `itunes:season`, `itunes:episode`, `itunes:episodeType`,
`guid isPermaLink` and `itunes:new-feed-url`, and produces identity hints for
`resolve_episode_ids`. It performs no I/O.

## Versioning

* `music-dht` 0.6.0 and `furumi-library` 0.3.0: new public types, enum
  variants and struct fields are breaking for 0.x.
* Device-sync protocol 4, ALPN unchanged. Podcast payloads are JSON; no
  postcard layout changes.
* Id domains `furumi-podcast:podcast:v1` and `furumi-podcast:episode:v1` and
  the `pod1:`/`ep1:` prefixes are frozen. A different derivation needs new
  domains, new prefixes, a migration and a new protocol version.
* Adding an operation kind or a `PlaybackSource` variant requires a new
  protocol version and gating as above.
* Public podcast cards will use a separate signed overlay with its own ALPN
  and version; the music DHT schema and `federation-net` do not change.
