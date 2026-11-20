# Podcast contract

Status: normative for `music-dht` 0.6 and device-sync protocol v4.

Podcasts are a separate content type shared by every Furumi client. This
document defines what all clients must agree on: identity, normalization,
private state and its merge rules, trusted-device sync operations, snapshot
recovery, queue items, version gating and Jam privacy. The Rust definitions
live in `music_dht::podcast` (pure types and functions) and
`music_dht::device_sync` (wire DTOs and gating helpers). Clients must use
them instead of local copies.

Public discovery of podcasts a user explicitly shared uses signed podcast
cards in a separate DHT overlay, `music_dht::podcast_dht`; see
[Public discovery (podcast_dht)](#public-discovery-podcast_dht).

## Responsibilities

| Layer | Owns |
| --- | --- |
| `music_dht::podcast` | ids, URL canonicalization, no-GUID rule, metadata DTOs and bounds, merge functions, RSS/Atom parser (feature `podcast-feed`) |
| `music_dht::device_sync` | sync operation kinds, snapshot sections, `PlaybackSource`, version gating, Jam redaction |
| `music_dht::podcast_dht` | public podcast cards and tombstones, their signatures, replication, cache and search, privacy warnings for feed URLs |
| Client storage (`furumi_library::podcasts::PodcastStore`, web PostgreSQL) | persistence and migrations implementing the rules below (SQLite reference: [SQLite reference store](#sqlite-reference-store)) |
| Client runtime | HTTP fetching, conditional requests, scheduling, audio buffering and decoding, UI |

Audio is always fetched from the publisher. It is never imported into the
music library, never content-addressed with a BLAKE3 content id and never
offered for P2P transfer. RSS refreshes fetch metadata only.

## Data classes

| Data | Trusted devices | Public federation | Local only |
| --- | --- | --- | --- |
| Subscription state, channel metadata | `podcast_subscription_set` + snapshot | never | |
| Feed aliases | `podcast_feed_aliases_added`, `podcast_subscription_set` + snapshot | never | |
| Share decision | `podcast_share_set` + snapshot | only the signed card while `effective_share` holds (`podcast_dht`) | |
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

* aliases form a grow-only set per podcast, merged independently of the
  subscription register (`merge_feed_aliases`). The URL that derives the
  podcast id is always kept, plus the 8 lexicographically smallest other
  URLs; this bounded union is commutative, associative and idempotent;
* aliases travel in their own operation, `podcast_feed_aliases_added`. A
  background refresh that learns a URL emits it, but only when merging the
  URL changes the stored set (a URL outside the bound is not sent again on
  every refresh). The operation never touches the subscription or share
  register, so it cannot resurrect a subscription or reset a share decision.
  Subscribe and unsubscribe operations also carry the aliases known at that
  time;
* `podcast.feed_url` of the subscription register is the URL used at the last
  explicit subscribe or unsubscribe. It can fall outside the bounded alias
  set, so the URLs that identify a podcast are
  `SubscriptionRecord::feed_urls()`: the aliases plus `podcast.feed_url`;
* before deriving a new id for a URL, clients call `resolve_podcast_id` with
  every known `(podcast_id, url)` pair, including `feed_urls()` and the URL
  each device currently fetches. A match reuses the existing podcast, its
  progress and its history. With several matches the smallest id wins.

Limitations: two devices that subscribe offline, through different URLs of
the same feed, before either learns the alias, create two podcast ids; the
contract does not merge podcast ids automatically in this version. A podcast
with more than 8 aliases can lose a larger URL from the synced set; a device
that only knows that URL derives a new id.

### Episode id

```text
episode_id = "ep1:" + base32_lower_nopad(BLAKE3("furumi-podcast:episode:v1" || 0x00 || podcast_id || key))
```

| Key | Material |
| --- | --- |
| GUID | `guid:` + RSS `<guid>` / Atom `<id>` trimmed, case-sensitive |
| link | `link:` + item `<link>` text (XML-unescaped once) or Atom `href`, trimmed of surrounding whitespace; no other cleanup |
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
2. Items without GUID then search the known episodes of the podcast in three
   stages, each run over the **whole document** before the next one starts:
   same enclosure URL, then same `<link>`, then same publication second and
   normalized title. Among candidates the smallest id is reused. A known
   episode can be claimed by only one item. Because a weak match of an
   earlier item cannot take a known episode that a later item matches by
   enclosure, the result does not depend on the order of items.
3. Otherwise a new id is derived from the first available key: `<link>`,
   then date + title, then enclosure URL.

A `<link>` is identifying only if it is unique in the document and differs
from the channel `<link>` (many feeds repeat the homepage in every item).
Enclosure URLs and date + title pairs shared by several GUID-less items are
not identifying either. Items without any usable key, and duplicates of an
earlier item of the same document, get no id and are skipped.

The identity hints (GUID, enclosure, link, publication time, title) are the
ones `music_dht::podcast::feed` produces: which enclosure or `media:content`
is chosen, how relative URLs are resolved, which date field is used and how
it is parsed, how the title is cleaned. They are part of the rule; clients
must use that parser (or an implementation that passes its tests). Relative
media URLs are resolved against the URL the document was fetched from.

"Known episodes" include episodes from earlier refreshes **and** episodes
learned through sync (`PodcastEpisodeRef::identity_hints`). Feed GUID-less
episodes into it so a device that sees an episode first through sync keeps
the id used by the other devices.

Limitations, GUID-less episodes only:

* The id can depend on what a device saw first. If the publisher changes the
  field the id was derived from (the `<link>`, or the title or date for a
  date + title id), a device that saw the old version keeps the old id
  through the enclosure match, while a device that never saw it derives a
  new id. One changed field is enough.
* Feeds with relative enclosure URLs fetched through different aliases give
  different enclosure URLs, so the enclosure match and enclosure-derived ids
  can differ between such devices.

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
`hlc_ms` is the sync operation timestamp; clients advance it only past their
own operations, so it is not a hybrid logical clock and a device with a fast
clock stamps every write "later". Every register therefore orders by a
Lamport `generation` first and uses `hlc_ms` only between writes that did not
observe each other.

### Subscription

* Register: `subscribed` + `podcast` metadata, last-writer-wins by
  `(generation, hlc_ms, op_id)`.
* `generation` is a per-podcast Lamport counter shared with the share
  register: every subscribe, unsubscribe and share write uses
  `next_podcast_generation(subscription, share)` = one more than the greatest
  generation of either register (1 when there is none). An action taken after
  observing another device's write orders after it whatever the clocks say.
* Unsubscribing writes `subscribed = false`. It is state, not a tombstone,
  and is never compacted, so it beats a stale subscription from a device that
  returns after being offline.
* Unsubscribing stops monitoring but keeps progress and history.
  Subscribing again continues where the user stopped. Erasing history is a
  separate explicit action (not part of this version).
* Only explicit subscribe and unsubscribe actions write the register. RSS
  refreshes keep their metadata locally; a background refresh that wrote the
  register could resurrect an unsubscribed podcast. Aliases learned by a
  refresh use `podcast_feed_aliases_added`.
* Subscribing to a podcast already subscribed locally must not emit a new
  operation.

| Events | Result |
| --- | --- |
| A (clock 10 min ahead) subscribes at g1; B sees it and unsubscribes at g2 | unsubscribed on every device |
| A and B, both offline after g1, unsubscribe and subscribe at g2 | the later `hlc_ms` (then `op_id`) wins |

### Share

* `ShareRecord`, last-writer-wins by `(generation, hlc_ms, op_id)`, with the
  generation from `next_podcast_generation`.
* `shared = false` revokes the publication. Like an unsubscribe it is
  privacy-protecting state, **not** a tombstone: it is never compacted and
  its row is never deleted. Otherwise a stale `shared = true` still in some
  device's log, or delivered again to a new device, would republish the
  podcast. The register is one record per podcast, so keeping it is cheap.
* Unsubscribing also writes `shared = false`.
* `effective_share(subscription, share)` decides whether to publish: the user
  is subscribed, chose to share, **and** the share decision orders after the
  latest subscription write by `(generation, hlc_ms, op_id)`. Every new
  subscription, including subscribing again or subscribing from someone
  else's public card, is private until the user shares it again; a stale
  share decision cannot republish; a share decision taken after observing the
  subscription always applies.

### Progress

`ProgressRecord { episode_id, podcast_id, position_ms, status, generation,
session_id, hlc_ms, device_id, op_id }`, with `status` one of `not_started`,
`in_progress`, `completed`. The winner is the greatest
`(generation, hlc_ms, device_id, op_id)`. Progress is never the maximum
position: seeking back and "start over" are real changes.

* Explicit actions use `generation = max_seen + 1`
  (`ProgressRecord::explicit_generation`): starting a listening session,
  seeking, "start over", "mark played", "mark unplayed", taking over playback
  through a handoff. `max_seen` is the generation of the current register,
  including received records. An explicit action during a session becomes
  the generation of that session: its later checkpoints use it.
* Periodic checkpoints and pause/stop saves within a session keep the
  generation of that session's latest explicit action.
* Handoff: queue items carry `PodcastEpisodeRef.progress_generation`, the
  greatest generation the sender has seen for the episode, because the
  playback state can arrive before the sender's progress operations. The
  sender writes a final checkpoint at its session generation and does not
  bump. Only the receiver writes the explicit action, with
  `explicit_generation(max(local, progress_generation))`, so its writes order
  after the sender's whatever the clocks say.
* A device that observes a greater generation for the episode it is playing
  stops writing at its old generation: either playback moved away (the
  coordination claim says so), or the user keeps listening locally and the
  device writes its next record as an explicit action
  (`explicit_generation(max_seen)`).
* Write progress periodically (every 15–30 s) and on pause, seek, stop,
  episode change and handoff, in the background. The outgoing log may drop a
  pending progress operation only when another pending operation of the same
  episode `supersedes` it (greater `(generation, hlc_ms, device_id, op_id)`).
  Never coalesce by sequence order: a checkpoint of an older session queued
  after "mark played" must not replace it.

| Events | Result |
| --- | --- |
| A checkpoints session s1 at g3; B seeks back at g4 with a slower clock; A's late g3 checkpoint arrives | B's seek (g4) wins |
| A restarts (g3, position 0); a queued g2 checkpoint of the old session arrives later | restart wins |
| A and B both saw g5 and listen offline; both start sessions at g6 | the later `hlc_ms` (then `device_id`, `op_id`) wins on every device; both sessions are kept |
| A plays at g5 with a fast clock and hands off to B, which has only seen g2 | B starts at g6 (`max(2, 5) + 1`) and its checkpoints win |
| A marks played (g5), then a queued g3 checkpoint of its session is written | the outgoing log keeps g5; g3 is dominated |

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
`protocol_version = 4`. New operation kinds (none of them is a tombstone):

| Kind | Payload | Materialization |
| --- | --- | --- |
| `podcast_subscription_set` | `podcast_id`, `subscribed`, `podcast`, `feed_aliases`, `generation` | `SyncOpWire::podcast_subscription` |
| `podcast_share_set` | `podcast_id`, `shared`, `generation` | `SyncOpWire::podcast_share` |
| `podcast_feed_aliases_added` | `podcast_id`, `feed_aliases` (1..9 URLs) | `SyncOpWire::podcast_feed_aliases` |
| `podcast_progress_set` | `episode_id`, `podcast_id`, `position_ms`, `status`, `generation`, `session_id`, optional `episode` ref | `SyncOpWire::podcast_progress` |
| `podcast_listen_recorded` | `session`, optional `episode` ref | `SyncOpWire::podcast_listen_session` |

Register stamps come from the operation: `hlc_ms`, `op_id`, and for progress
`device_id = origin_device_id`. A session's `device_id` must equal the
operation origin. `SyncOpWire::validate_podcast` checks this and the bounds;
receivers skip operations that fail it but still acknowledge them, so the
log keeps moving.

Ids (`PodcastId`, `EpisodeId`) and feed URLs (`FeedUrl`) are validated while
the message is deserialized: a malformed id or a non-canonical feed URL is a
schema violation, like a wrong JSON type, and the **whole message** fails to
parse. Conforming clients cannot produce one, because these types serialize
only canonical values, and their rules are frozen with the `v1` derivation
domains (changing them needs a new protocol version, see Versioning).

### Peer versions

Each device reports its own version: in the `profile` of its `Hello`,
`PairRequest` and `PairResponse`, in its own entry of the `devices` list (a
`SyncResponse` has no `profile`, so an initiator learns the responder's
version from the entry whose `device_id` is the responder), and in
`DeviceProfileSet.protocol_version` of operations it originates. A client
emits a `DeviceProfileSet` with `protocol_version` on the first start of a
new protocol version.

* A self-report replaces the stored version:
  `profile_protocol_version(reported, stored)` (`None` keeps the stored
  value).
* Profiles of other devices relayed in `devices` only raise it:
  `relayed_protocol_version(relayed, stored)` = `max(stored, relayed)`, or
  `relayed` when nothing is stored. Deployed v1–v3 clients relay every
  device with their own constant, so a relayed 3 must not downgrade a v4
  peer.
* Relays forward the stored version of other devices in
  `DeviceProfileWire.protocol_version` and must never substitute their own
  constant: version gating would then send podcast state to an old client.
* A device that never reported a version is treated as version 1.
  Downgrading a device below v4 is not supported while relays still hold
  v4 for it.

### Pairing

The invite consumer sends its `PairRequest` (operations, snapshot, playback)
before it hears from the inviter. Invites created by v4 clients carry the
inviter's version in `InviteWire.protocol_version` (JSON key `p`, omitted by
older clients). The consumer gates the whole request by
`InviteWire::inviter_protocol_version()`, which is version 1 for invites
without it: no podcast operations, `snapshot.for_peer(version)`, and no
playback snapshot with podcast items. Podcast state then catches up through
the snapshot of the next regular sync. The inviter gates its `PairResponse`
by the version in the request's `profile`.

### Snapshot recovery

Sync vectors only grow, so operations hidden from a peer while it ran an
older version are never re-requested after it upgrades. Podcast state is
therefore part of `SyncSnapshot`:

| Section | Content | Limit | Budget share |
| --- | --- | --- | --- |
| `podcast_subscriptions` | subscription registers, including unsubscribed, with feed aliases; `podcast.description` is omitted | 1000 | 25 % |
| `podcast_shares` | share registers, including `shared = false` | 1000 | 5 % |
| `podcast_progress` | progress registers | 5000 | 45 % |
| `podcast_listen_sessions` | most recent sessions | 2000 | 25 % |

`SyncSnapshot::bound_podcast_state_to(budget)` orders each section newest
first (registers by `(generation, hlc_ms)`, sessions by `ended_at_ms`), caps
it by count and by its share of the JSON byte budget; unused budget passes on
to the following sections in table order, so large subscription records
cannot crowd out progress. `bound_podcast_state()` uses the default 4 MiB
budget. Senders whose other message parts are large pass
`podcast_snapshot_budget(other_bytes)`, which keeps the whole line under the
8 MiB limit. Channel descriptions are not snapshotted: every device refreshes
channel metadata from the feed.

Receivers ignore podcast sections that exceed the counts
(`podcast_state_within_limits`), validate each record and merge it with the
same functions as operations.

Limitations: records cut by the bounds reach a peer only through operations
it has not acknowledged yet. A peer that upgrades from v3, or that received
operations through a v3 relay, already skipped the podcast operations: it
recovers only the newest records that fit, so older progress registers and
listening sessions are missing on that device and its play counts
(`completed_listen_count`) can differ from other devices.

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

`SyncOpPayload` is a closed serde enum. A v3 client that receives an unknown
operation `kind` fails to parse the **whole** message, not one operation,
which would stall synchronization. A v3 client does not know
`PlaybackTrack.source` and ignores it: it would treat an episode as a broken
music item (id 0, no content id) and forward the queue that way, losing the
episode for every device. Additive `serde(default)` fields alone therefore do
not make podcasts compatible. The peer version comes from "Peer versions"
above, and from the invite while pairing. Therefore:

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

`PlaybackSource` is closed for v4 clients as well: a v4 client fails to parse
a message with an unknown source `kind`.

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

## Public discovery (podcast_dht)

`music_dht::podcast_dht` publishes podcasts the user explicitly shared so
that peers outside the user's trusted-device group can find and subscribe to
them. It is a separate, schema-independent Kademlia overlay built like
`similarity_dht`: its own ALPN, version, routing contacts and SQLite cache.
The music DHT (`music-dht-v5`) and `federation-net` are unchanged. Every
device of a user publishes under its own identity; search merges the cards
of one podcast by `podcast_id`.

| Item | Value |
| --- | --- |
| ALPN | `furumi-fd/podcast-dht/1` (schema-independent stream protocol) |
| Wire and record version | `PODCAST_DHT_PROTOCOL_VERSION = 1` |
| Capability manifest | `podcast_dht = 1`, opt-in (`CapabilityManifest::with_podcast_dht`, `supports_podcast_dht`) |
| Cache | separate SQLite file, `PRAGMA user_version = 2`; a newer schema is refused, the pre-release version 1 replica table is dropped |
| Card TTL | `ACTIVE_RECORD_TTL` of the music DHT (12 h) |
| Tombstone TTL | `TOMBSTONE_TTL` of the music DHT (24 h) |
| Republish | `DEFAULT_REPUBLISH_INTERVAL` (10 min), expiry sweep `DEFAULT_EXPIRE_INTERVAL` (1 min) |
| Clock skew | `issued_at_ms` at most 5 min in the future |

### Card

A card is the public projection of one podcast by one publisher device. It
is built from the subscription with `PodcastCardSpec::from_subscription`,
which reads only the podcast id, the feed URLs and the channel metadata. It
never contains progress, listening history, play counts, subscription or
share registers (`generation`, `hlc_ms`, `op_id`), device names, local paths,
HTTP validators or episodes.

| Field | Bound |
| --- | --- |
| `version`, `network_id`, `publisher` | 1, the federation network, the device's iroh endpoint key |
| `podcast_id` | must equal `PodcastId::from_feed_url(id_feed_url or feed_url)` |
| `feed_url` | canonical `FeedUrl` the publisher fetches the feed from |
| `id_feed_url` | the URL the id was derived from, present only when it differs from `feed_url` (feed moved) |
| `title` | 1..300 characters, no control characters |
| `author` | optional, ≤ 300 characters, no control characters |
| `description` | optional, ≤ 1 000 characters, only `\n`/`\t` controls |
| `image_url`, `link` | optional absolute `http(s)` URLs ≤ 2 048 bytes |
| `language` | optional, ≤ 35 bytes of ASCII letters, digits and `-` |
| `categories` | ≤ 8, each 1..64 characters, no control characters |
| `episode_count`, `latest_episode_at_ms` | optional; the time is ≥ 0 |
| `revision` | ≥ 1, monotonic per `(podcast_id, publisher)` |
| `issued_at_ms`, `expires_at_ms` | `0 < issued < expires ≤ issued + 12 h` |

`PodcastCardSpec::bounded` truncates publisher data to these bounds. The
whole signed record is at most 24 KiB postcard-encoded; the field bounds keep
every valid card below it.

Only the **origin URL** (`PodcastCard::origin_feed_url`: `id_feed_url`, or
`feed_url` when the feed has not moved) is bound to the podcast, because the
id is its hash. When `id_feed_url` is present, `feed_url` is an unverified
claim of the publisher that the feed moved: anyone can sign a card that
carries an existing podcast's origin URL and an arbitrary `feed_url`. Search
therefore never offers a claimed URL as the podcast's feed (see Search), and
clients must not fetch it as that podcast or record it as an alias unless the
feed at the origin URL itself redirects there.

### Tombstone

`(version, network_id, publisher, podcast_id, revision, issued_at_ms,
expires_at_ms, index_keys)` with `expires_at_ms ≤ issued_at_ms + 24 h` and
`index_keys` the sorted, distinct text keys of the retracted card (at most
32, without the podcast-id key). It carries key hashes, not the title. A
tombstone supersedes every card of the same `(podcast_id, publisher)` with a
lower or equal revision and is itself retained and served until it expires.
Because the tombstone TTL exceeds the card TTL, every card issued before a
tombstone expires before it.

`index_keys` is self-asserted, so it never creates a placement by itself: a
replica indexes a tombstone under a text key only in place of a record of the
same `(podcast_id, publisher)` it already holds there (see Replica storage).

### Signing input

Both records are signed with the publisher's Ed25519 iroh identity over a
canonical, language-independent encoding: the domain string, a `0x00` byte,
then the fields in the order listed. Integers are little-endian (`u16`,
`u32`, `u64`, `i64`); strings are a `u32` byte length followed by UTF-8;
`Option` is `0x00`, or `0x01` followed by the value; lists are a `u32` count
followed by the items; `network_id`, `publisher` and keys are their raw 32
bytes.

* Card, domain `furumi-podcast-card:v1`: `version u16, network_id, publisher,
  podcast_id str, feed_url str, id_feed_url opt str, title str, author opt
  str, description opt str, image_url opt str, link opt str, language opt
  str, categories list of str, episode_count opt u32, latest_episode_at_ms
  opt i64, revision u64, issued_at_ms u64, expires_at_ms u64`.
* Tombstone, domain `furumi-podcast-card-tombstone:v1`: `version u16,
  network_id, publisher, podcast_id str, revision u64, issued_at_ms u64,
  expires_at_ms u64, index_keys list of 32-byte keys`.

Unit tests pin BLAKE3 vectors of both encodings. On the wire, records are
postcard `SignedPodcastRecord { body: Card | Tombstone, signature }`.

### Keys and placement

`key = BLAKE3("furumi-podcast-dht:key:v1" || 0x00 || network_id || kind ||
material)` with kind `0x01` and the podcast id string for the id key, or
kind `0x02` and a token for a text key. Tokens are the distinct
`normalize_name` tokens of the title, then of the author, skipping tokens
longer than 64 bytes, at most 32. A card belongs under the id key and its
text keys (at most 33); a tombstone under the id key and its `index_keys`.
The publisher sends each record to the `K` (8) overlay nodes closest to each
of its keys, one `StoreBatch` per node listing the keys that node should hold.

### Ordering

For one `(podcast_id, publisher)` the greater record wins: higher
`revision`; at an equal revision a tombstone beats a card; then later
`issued_at_ms` (a republish); then later `expires_at_ms`; then the
lexicographically greater signing input. This is a total order, so every
replica and every lookup converge whatever the delivery order. A replica
keeps only the winning record of each `(podcast_id, publisher)`; a replayed
old revision is ignored. These fields only order one publisher's own records:
they never rank or select records of different publishers.

Revisions: a changed card gets `max(previous + 1, now_ms)` (time-seeded, so
revisions stay monotonic even if the local cache is lost); an unchanged card
is re-signed with the same revision and a fresh lifetime on every republish;
unpublishing writes a tombstone with `previous + 1`; publishing again later
gets `max(tombstone + 1, now_ms)`.

### Wire protocol

One request per bidirectional stream on `furumi-fd/podcast-dht/1`: the
requester writes one postcard `PodcastDhtRequest` and finishes its send side;
the responder writes one postcard `PodcastDhtResponse` and finishes. Messages
are not length-prefixed; the stream end delimits them. Requests above
512 KiB and responses above 4 MiB are refused.

```text
PodcastDhtRequest  { version: u16, requester: NodeContact, operation: PodcastDhtOperation }
PodcastDhtOperation = 0: FindNode   { target: [u8; 32] }
                    | 1: FindValue  { key: PodcastDhtKey, filter_tokens: Vec<String> }
                    | 2: StoreBatch { records: Vec<PodcastStoreEntry> }
PodcastStoreEntry  { keys: Vec<PodcastDhtKey>, record: SignedPodcastRecord }
PodcastDhtResponse { version: u16, ok: bool, error: Option<String>,
                     nodes: Vec<NodeContact>, records: Vec<SignedPodcastRecord>, stored: u32 }
NodeContact        { node_id: [u8; 32], peer_id: EndpointId, ticket: String, last_seen_ms: u64 }
PodcastRecordBody  = 0: Card(PodcastCard) | 1: Tombstone(PodcastTombstone)
```

Fields are in postcard order. `PodcastDhtKey` is 32 raw bytes.

* `version` must be 1 in both directions. `requester.peer_id` must equal the
  authenticated transport peer, `node_id` must be `NodeId::from_endpoint`
  of it and its ticket must name the same endpoint and network; otherwise the
  request is refused. The responder learns the requester as an overlay
  contact (it is authenticated).
* `nodes` in a response are unverified suggestions. A requester treats them
  as lookup or bootstrap candidates only and adds one to its routing table
  (and persisted contacts) only after it answered a request itself.
* `FindNode` returns the `K` closest overlay contacts (the responder
  included).
* `FindValue` returns the records under `key` (see Replica storage) and the
  `K` closest contacts. `filter_tokens` (≤ 8, each 1..64 bytes) are
  normalized tokens every returned card must be indexed under; tombstones are
  returned regardless. Requesters verify every record again and drop records
  that do not belong under `key` or miss a filter token.
* `StoreBatch` carries 1..16 entries, each with 1..33 distinct keys. Every
  record must be published by the requester itself (replicas never forward
  records), pass `verify_at` (bounds, network, lifetime, clock skew,
  signature, size) and belong under each listed key; otherwise the whole
  batch is refused. Keys the responder is not responsible for are skipped.
  `stored` counts the `(key, record)` placements written. A publisher
  offers the placements of a batch answered with `stored = 0` (or that
  failed) once to the next closest node of each key.
* A refused request gets `ok = false` with an `error` of at most 1 KiB and
  empty `nodes`, `records` and `stored = 0`; an accepted one has `ok = true`
  and no `error`. Requesters reject responses that violate this or exceed
  `K` nodes or 128 records.

### Replica storage

* A replica stores one payload per `(podcast_id, publisher)` (the winning
  record) and indexes it under the keys it holds. A newer version replaces
  it in place; placements the new version no longer belongs under (an old
  title's tokens, keys a tombstone does not list) are dropped, so on one
  replica a tombstone supersedes the card under every key it held.
* A node accepts a placement only when fewer than `2 * K` of its verified
  overlay contacts that are not backed off are closer to the key than
  itself. Contacts only enter the routing table by answering a request or
  by sending one, so a peer cannot make a replica refuse keys by returning
  made-up contacts. A contact that fails three times in a row (each after
  its 30 s backoff) is removed.
* A key holds at most 128 records, at most 32 of them tombstones: exactly
  what one `FindValue` response carries, so every admitted record is served.
  A tombstone is indexed under a text key only in place of its owner's
  record there, and such an in-place conversion counts against the
  tombstone budget like a new tombstone.
* Full key: a new placement evicts a random placement of another owner from
  the younger half of the key (by first indexing time; only tombstones when
  the tombstone budget is full). The older half is never evicted for a
  newcomer, so records placed before a flood keep their place and their
  publishers refresh them in place. Capacity is never first-come-forever:
  a newcomer always gets a place, though placements in the younger half can
  be evicted again by later newcomers.
* A replica holds at most 256 records per publisher (a publisher over it is
  refused), 4 096 records and 32 MiB of payload. A full cache admits a new
  record by evicting random records of other owners from its younger half,
  at most 16; when that is not enough room, nothing is evicted and the record
  is refused. At most 1 024 overlay contacts are persisted.
* Device identities cost nothing, so these rules bound flooding but cannot
  prevent it: an attacker with many identities can keep part of the younger
  half of a key or cache busy, and a newcomer's place there can be evicted
  again until its publisher's next republish (every 10 min) puts it back.
  Protocol 1 has no proof of work or other identity cost.
* `FindValue` answers with at most 128 records: at most 32 tombstones and
  the cards indexed longest first. Every record is verified again on every
  read.

### Publication

* `PodcastDht::publish(subscription, share, card)` refuses
  (`MusicDhtError::PodcastPublication`) unless `effective_share` holds, so a
  new or resumed subscription is never published without a fresh share
  decision. The UI calls it only after asking the user about any privacy
  warning, so the call is the user's consent.
* `PodcastDht::sync_shares(states)` is declarative and idempotent: podcasts
  for which `effective_share` holds are published (when changed, new or past
  half their lifetime), every other podcast this device published is
  unpublished. Clients call it after applying share or subscription
  operations, so unsharing or unsubscribing on any trusted device revokes
  every device's own card once the registers converge, and a stale device
  cannot republish (its registers lose to the newer share decision).
* Revocation is fail-safe: each podcast is handled on its own. A shared
  podcast whose card cannot be built or signed is reported in
  `PodcastDhtSyncStats::failed` and treated as not shared (an earlier
  publication is tombstoned); it never prevents other podcasts from being
  unpublished.
* A card whose `PodcastCardSpec::privacy_warnings` is not empty is held back
  (`PodcastDhtSyncStats::held_back`, with the warnings) unless
  `PodcastShareState::privacy_confirmed` is set or this device already
  publishes the same `feed_url` and `id_feed_url`. A feed move to a URL that
  looks private therefore asks again; an earlier publication of a held-back
  podcast stays as it was.
* SQLite clients build each `PodcastShareState` from
  `PodcastStore::subscription` (the merged register with its alias set, which
  always contains the origin URL) and `PodcastStore::share`; its `is_shared`
  equals `PodcastSummary::shared`. A card for the URL the device now fetches
  sets `feed_url` from the catalog and `id_feed_url` to the origin URL. The
  store does not persist `privacy_confirmed`; the client keeps that
  confirmation. See [SQLite reference store](#sqlite-reference-store).
* `PodcastDht::unpublish(podcast_id)` writes the tombstone. Nothing is ever
  sent for a podcast this device never published: no tombstone, no id.
* `PodcastDht::republish` (run by `maintenance` every 10 minutes and after
  a bootstrap that found new peers) re-signs active cards and re-sends
  unexpired tombstones.
* Own cards persisted by an earlier run are not sent again until the client
  calls `sync_shares` in this process: before that, `republish` sends only
  tombstones and cards published with `publish` in this process, and
  `FindValue` answers leave those cards out (they stay in the cache). A share
  revoked on another trusted device while this one was offline is therefore
  never re-sent on startup from the stale cache. Clients call `sync_shares`
  on start, with the registers they hold, and again after device sync
  applies newer operations; the first call also re-sends the unchanged cards
  as they are (`PodcastDhtSyncStats::unchanged`, counted in `push`).
  `sync_shares` itself still trusts the registers it is given: an unchanged
  card is only re-signed when it used half its lifetime, so clients that may
  hold stale registers on start lose nothing by reconciling again after
  device sync.
* The privacy heuristics (`feed_url_privacy_warning`) are advisory: query
  parameter names containing words such as `token`, `key`, `auth`, `secret`,
  `sig`/`signature`, `uid`, `user`, `pass`, `session`, `access`, `private`;
  known member-feed hosts; and long high-entropy query values (≥ 16
  characters) or path segments (≥ 32 characters). They check `feed_url` and
  `id_feed_url`, not artwork or web-page links. False positives and misses
  are possible.

### Search

* `PodcastDht::search(query, limit)` normalizes the query
  (`podcast_query_tokens`: distinct tokens, at most 8), looks up the text
  keys of the three longest tokens and asks every replica to return only
  cards indexed under **all** query tokens (tombstones are always returned).
  Results are verified again, the winning record per publisher is kept,
  tombstoned publishers are dropped and cards are merged by `podcast_id`
  into `PodcastSearchHit { podcast_id, feed_url, card, publishers,
  claimed_feed_urls }`.
* `PodcastDht::lookup_podcast(podcast_id)` reads the id key.
* `feed_url` is the origin URL; it is the only URL a client subscribes with.
  `claimed_feed_urls` lists other `feed_url`s that publishers claim (at most
  8, with their publishers), for display only.
* The representative `card` is a card whose `feed_url` is the origin URL
  when one exists, then the title most publishers agree on, then the
  smallest publisher key. Publisher-controlled times and revisions never
  break ties. Its metadata remains a publisher's claim.
* `publishers` lists distinct publisher **devices**. It is not a subscriber
  count (one user may publish from several devices, and subscribers who did
  not share are invisible) and not a trust signal: device identities are
  free. No subscriber count is offered and ranking ignores it.
* Ranking: exact normalized title first, then hits that have a card under
  the origin URL, then title and id.
* `FederatedSearchHit` (`{"type": "music" | "podcast", "hit": ...}`),
  `SearchHitKind` (`artist`, `release`, `track`, `podcast`) and
  `merge_search_results` merge podcast hits with `SearchOutcome` music
  results; `SearchOutcome` itself is unchanged.
* Lookups query the 8 closest overlay nodes (α = 3, at most 32 requests,
  15 s deadline) and return partial results when peers fail. They keep at
  most 512 `(podcast_id, publisher)` owners; each response (and the local
  cache) may add at most 64 new owners (`512 / K`), so the responders that
  answer first cannot fill a lookup and crowd out the rest. A newer record
  of a known owner always replaces its winner. A peer that fails a request
  is skipped for 30 s.

Subscribing from a hit creates the user's own private subscription with the
hit's `feed_url` (the origin URL): resolve the id with `resolve_podcast_id`,
which yields the hit's `podcast_id` unless the user already knows that URL,
fetch the feed from that URL and follow its own HTTP redirects and
`itunes:new-feed-url`, recording those as aliases as usual. A URL from
`claimed_feed_urls` becomes an alias only when that fetch leads there. The new
subscription is private (see `effective_share`), the publisher's availability
is not needed afterwards, and the episode catalog comes from RSS.

### Limits

| Limit | Value |
| --- | --- |
| Request / response | 512 KiB / 4 MiB postcard |
| Store batch | 1..16 entries, each 1..33 distinct keys, records of the requester only |
| Records per `FindValue` response | 128, at most 32 of them tombstones |
| Filter tokens | ≤ 8, each 1..64 bytes |
| Records per key | 128, at most 32 of them tombstones; random eviction from the younger half |
| Records per replica | 256 per publisher (refused beyond), 4 096 in all, 32 MiB of payload (random eviction from the younger half, at most 16 per record) |
| Store responsibility | fewer than `2 * K` = 16 verified, not backed-off contacts closer to the key |
| Records kept per lookup | 512 `(podcast_id, publisher)`, at most 64 new ones per response |
| Persisted overlay contacts | 1 024 |
| Inbound requests | 64 concurrent, 8 per peer (more are dropped); 10 s each |
| Error message | ≤ 1 KiB |
| Contact ticket | ≤ 16 KiB |

### Privacy guarantees

* Only podcasts with `effective_share` are published; never-shared podcasts
  produce no network traffic at all.
* After a restart nothing is re-sent from the persisted cards until the
  client reconciled its registers with `sync_shares` (see Publication).
* Cards carry the public projection only (see Card); tombstones carry key
  hashes only.
* Records are signed by the publisher device and verified on every receive
  and on every cache read; a forwarder cannot alter or impersonate them,
  only the publisher can store them, and records from another network are
  rejected.
* The card's `publisher` is the device's persistent iroh endpoint key, which
  is also its overlay and music-DHT identity. It is **linkable**: anyone can
  look the key up in the overlay (`FindNode` for
  `NodeId::from_endpoint(publisher)`) and obtain the device's contact ticket
  with its relay and direct IP addresses, because every overlay request
  carries the requester's ticket. The same key owns the device's music-DHT
  records, so a device's shared podcasts can be linked to its public music
  library and its network address. Subscribing from a card never contacts
  the publisher.
* Devices of one user are not linked by the overlay, but publishing the same
  podcasts from several devices makes them correlatable.
* Unpublishing is eventually consistent: replicas that missed the tombstone
  keep serving the old card until it expires (at most 12 h), including
  replicas that only held an older title's text keys.
* Capability manifests are informational. A peer without `podcast_dht` never
  enters the overlay routing table (it cannot answer on the ALPN), and
  `PodcastDht::note_peer_capabilities` evicts a peer whose manifest lacks it
  and stops dialing it. Applications advertise the overlay only when they
  run it (`with_podcast_dht`).

## Feed parser

With the `podcast-feed` feature, `music_dht::podcast::feed` parses RSS 2.0,
RSS 1.0 and Atom tolerantly: BOM and declared single-byte encodings, HTML
entities, CDATA, mismatched end tags, undeclared prefixes, depth and size
limits, lenient dates and durations, relative media URLs resolved against
the fetch URL. Items without an absolute `http(s)` audio URL are dropped. It
extracts `itunes:season`, `itunes:episode`, `itunes:episodeType`,
`guid isPermaLink` and `itunes:new-feed-url`, and produces identity hints for
`resolve_episode_ids`; those hints are normative (see the no-GUID rule).
Links are kept verbatim (trimmed only) because they are identity material.
Text cleanup is linear in the input size. It performs no I/O.

## SQLite reference store

`furumi_library::podcasts::PodcastStore` (`furumi-library` 0.3) implements
this contract for SQLite clients. Other stores (the web client's PostgreSQL)
must keep the same invariants.

### File and migrations

* Own file, conventionally `podcasts.sqlite3` next to `library.db`; the caller
  passes the path. WAL, foreign keys, 5 s busy timeout.
* `PRAGMA user_version` drives migrations. Each runs in a `BEGIN IMMEDIATE`
  transaction that re-reads the version, so concurrent openers and
  interrupted upgrades are safe and reopening is idempotent.

| Version | Content |
| --- | --- |
| 0 | empty file |
| 1 | prototype schema of the original podcast PR (`podcasts`, `podcast_episodes`), verbatim |
| 2 | the model below; the prototype tables are renamed and imported |

* Before writing anything (including the WAL switch), the store refuses a
  file with a `user_version` above its own, and a file whose tables are not
  the expected ones for its version (for example `library.db` passed by
  mistake). The file is left byte-for-byte unchanged.
* Prototype import (migration 2). Tables are renamed `legacy_podcasts` and
  `legacy_podcast_episodes`, never dropped.
  * Each feed becomes a podcast with `podcast_id` from the canonical feed URL
    (`FeedUrl::parse`, then `FeedUrl::from_user_input`) and a subscription
    register `subscribed = true`, generation 0, `hlc_ms` = `added_at`,
    `op_id` = `legacy-import:<podcast_id>`, not shared. Every contract write
    orders after it; other devices learn it through the snapshot sections.
    Rows with the same canonical URL merge (first row wins).
  * URLs that cannot be canonicalized, episodes with a non-`http(s)` audio URL,
    invalid metadata or no identity are listed in `legacy_quarantine`; the
    rows stay in the `legacy_*` tables.
  * Episode ids come from `resolve_episode_ids` over the stored items. The
    prototype stored the enclosure URL as `guid` when an item had none: such
    rows are imported as GUID-less items and keep the value in
    `episodes.legacy_guid`. The prototype kept one row per enclosure URL, so
    rows with the same publication second, normalized title and `<link>`
    (or none) are one item whose audio URL moved: the newest row
    (`first_seen_at`, then `id`) is imported, the others are quarantined as
    duplicates.
  * Imported GUID-less episodes are **provisional**: stale rows can still
    change which key is unique among the stored items, so their ids may
    differ from what a device without the prototype derives. On a refresh,
    item ids are resolved against the stored episodes except provisional
    ones without progress or sessions; each such provisional episode is
    matched to its item (enclosure, then link, then date and title, GUIDs
    ignored) and re-keyed to that item's id. When another episode that
    stays already holds that id (for example one a remote operation created
    before this device's first refresh), it is the item's episode and the
    provisional one is a duplicate: it is deleted rather than kept as a
    second copy. The same applies when `legacy_guid` turns out to be the
    item's real GUID. Listing an episode clears `legacy_guid`, which makes
    its id final. Provisional episodes carry no personal state, so nothing
    personal moves or is lost; an imported episode played before its first
    refresh is not provisional and keeps its id (the feed item then becomes
    a separate episode if the ids differ).
  * ETag, Last-Modified, check time, last error and the cover cache move to the
    fetch state. The prototype stores no progress or history, so none is
    created.

### Tables (version 2)

| Table | Key | Content | Synced |
| --- | --- | --- | --- |
| `podcasts` | `podcast_id` | local channel metadata, `feed_url` = URL this device fetches, `meta_from_feed` | catalog, no |
| `podcast_feed_aliases` | `(podcast_id, feed_url)` | bounded grow-only alias set (`merge_feed_aliases`) | yes, inside subscription records and `podcast_feed_aliases_added` |
| `episodes` | `episode_id` | `EpisodeMeta` columns (season, number, type, published, duration, audio URL, MIME, length, artwork, link), `source` (`feed`/`sync`), `in_feed`, `legacy_guid` | catalog, no |
| `podcast_seasons` | `(podcast_id, season)` | derived from `episodes`: episode count, first/last publication; `season` NULL groups episodes without a season | no |
| `podcast_subscriptions` | `podcast_id` | subscription register: `subscribed`, `PodcastMeta` JSON, `generation`, `hlc_ms`, `op_id` | yes |
| `podcast_shares` | `podcast_id` | share register | yes |
| `episode_progress` | `episode_id` | progress register (`ProgressRecord`) | yes |
| `podcast_listen_sessions` | `session_id` | grow-only set of `ListenSession` | yes |
| `podcast_fetch_state` | `podcast_id` | ETag, Last-Modified, checked / last success / next check times, last error, consecutive failures, cover cache | **never** |
| `legacy_quarantine` | `(legacy_table, legacy_id)` | prototype rows that could not be imported | no |

### Invariants

* Registers and sessions have no foreign keys to the catalog: operations for
  podcasts or episodes the device has not fetched are stored as they arrive,
  in any order. An operation's `PodcastEpisodeRef` adds a `source = 'sync'`
  catalog row only when the episode is unknown; feed metadata always wins.
* One apply path. Local actions validate the payload, draw a stamp from the
  client's sync log, build the `SyncOpWire`, and apply it like a received
  operation: `SyncOpWire::podcast_*` materializers, then `merge_*`. The
  returned operation must be appended with exactly that stamp. Snapshot
  records go through the same functions after validation. The result is
  therefore commutative, associative and idempotent, and a snapshot never
  replaces newer local state.
* Invalid operations (`validate_podcast`, a generation above `i64::MAX`, a
  session whose identity differs from the stored one) are skipped and
  reported as rejected, so the client can still acknowledge them.
* Only `subscribe`/`unsubscribe` write the subscription register and only
  `set_shared`/`unsubscribe` the share register, with
  `next_podcast_generation`. No-op actions draw no stamp and emit nothing.
  `unsubscribe` emits `subscribed = false` and `shared = false` and keeps
  progress, sessions and the catalog. The store only records the decision;
  publishing and revoking the public card is `PodcastDht::sync_shares` (see
  [Public discovery](#public-discovery-podcast_dht)).
* Explicit progress actions use `explicit_generation(max(local, carried))`;
  checkpoints keep the session generation and are refused (`Superseded`)
  once the register holds a greater one, or after the session completed.
* RSS refreshes update the catalog (channel metadata, episode metadata and
  audio URLs, `in_feed`) and the alias set only; episodes missing from the
  feed are kept. Permanent redirects and `itunes:new-feed-url` become the
  fetch URL and aliases, and emit `podcast_feed_aliases_added` only when the
  stored alias set changes.
* Snapshot export reads each section newest first up to its count, then
  applies `bound_podcast_state_to(budget)`. Fetch state and the catalog
  (descriptions, episodes) are never exported.
* Feed URLs resolve through `resolve_podcast_id` over the alias set, the
  catalog fetch URL and the register's `podcast.feed_url`.
* Fetch-state writes (`record_fetch_*`, `set_cover_cache`) read and write
  the row in one immediate transaction, so concurrent writers on one or
  several connections never lose a column or a failure increment.
* `TrackItem::podcast_episode` builds queue items whose identity is the
  explicit source `TrackItem.podcast` alone. Their `id`, `release_id` and
  artist id are the fixed sentinel `TrackItem::NOT_A_LIBRARY_ROW` (-1),
  shared by every episode, the same "not a library row" convention as
  pending federated tracks. The sentinel carries no identity and is never
  decoded; it only keeps music-only code that predates podcasts
  (`id >= 0`, `release_id < 0` checks) from taking an episode for library
  track, release or artist 0. There is no reserved range of negative ids.
* `TrackItem::key` (a typed `TrackItemKey`; string form `lib:<id>`,
  `fed:<owner>:<item_id>`, `ep:<episode id>`) is the source identity of a
  queue item; on the wire it is `PlaybackSource`. An episode is identified
  by `TrackItem.podcast` / `ep:` only and must never be keyed by its numeric
  id: keying by id (for example a `local:{id}` string) would merge all
  episodes into one. `TrackItemKey` has no content-id equivalence (a pending
  federated item and its imported library copy differ), so clients that
  match music content-first keep doing that and add the podcast branch
  before any fallback to the id.
* Wire ids of podcast items and of Jam private placeholders are `0`
  (`PlaybackTrack::podcast_episode`, `PlaybackTrack::private_placeholder`)
  and must not be copied into a `TrackItem`.
  `TrackItem::from_playback_track` classifies a received item as
  `PlaybackItem::Music` (the client's music path), `PodcastEpisode` (built
  with `TrackItem::podcast_episode`, sentinel ids) or `Private` (not
  playable, no `TrackItem`). Besides the ids, `TrackItem::podcast_episode`
  also differs from the wire form in `release_year` (publication year) and
  `audio_format` (`None`; the MIME type stays in the reference).

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
* Feed URL canonicalization, the identity bounds (`MAX_URL_BYTES`,
  `MAX_GUID_BYTES`) and the no-GUID rule are frozen with the `v1` domains:
  receivers reject non-canonical ids and URLs while parsing.
* Public podcast cards use the separate signed overlay `podcast_dht` with
  its own ALPN (`furumi-fd/podcast-dht/1`), wire/record version 1, signing
  domains `furumi-podcast-card:v1` / `furumi-podcast-card-tombstone:v1`, key
  domain `furumi-podcast-dht:key:v1`, all frozen, and cache schema 2; the music
  DHT schema and `federation-net` do not change.
