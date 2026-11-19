# Personal playback coordination

`music_dht::playback` owns the personal-device state machine, policy parameters,
snapshot/command envelopes and conflict rules. It has no networking, database,
clock, UI or audio dependency. The existing JSON-lines device-sync transport
continues to carry snapshots and logged commands. Jam is a separate authority
scope; its capability-based protocol is not a personal ownership claim.

## Contract

Each logical device identity must have one coordinator and one selected output
adapter. Independent live installations must not reuse a device identity or
checkpoint. The web hub is a single authority for its user's browser outputs;
running multiple independent hubs for that same identity requires an external
actor/leader arrangement, not two copies of this in-memory adapter.

An owner is selected by an immutable `Claim { counter, issuer, owner }`.
Claims are ordered lexicographically in that order. A locally issued claim
increments the greatest counter observed so far. Persist the winning claim;
never reset it because a peer disappears from an online list. Controllers
gossip the winning claim as well. Concurrent startup, transfer or failover
claims therefore converge to the same winner regardless of delivery order.

Only the winning output may render personal audio. A losing player stops its
audio even if it was playing; it cannot reject a higher claim on that basis.
This is eventual exclusivity, not a quorum lock: partitions can temporarily
produce two outputs. Convergence assumes eventual delivery between trusted
devices, functioning polling/event loops, and no continuing stream of explicit
conflicting user transfers. A disconnected browser cannot be forcibly silenced.

Availability, actual unpaused playback, ownership and network connectivity are
different inputs. Only new reports from the owner's audio endpoint renew its
liveness. Duplicate snapshots and relayed claims do not. Gateway timers must
not turn cached browser state into new output heartbeats. The engine reserves
heartbeat sequence ranges in its checkpoint so a restart cannot roll them back
and every UI tick does not require a database write.

Commands carry `CommandStamp { version, claim, sequence }`. A command can carry
its existing claim ahead of the corresponding announcement. An explicit
handoff must name that claim's owner and originate from its issuer. In a term,
commands are applied in increasing sequence per sender; old, duplicate and
incompatible envelopes are ignored. The command-sender map is bounded to 128
entries per term. Concurrent controllers are serialized by the selected
output, whose snapshots distribute the resulting queue and transport state.
This does not claim exactly-once audio effects across process crashes.

## Adapter obligations

1. Authenticate the peer and trusted group using the existing transport and
   operation-log membership checks. Bind snapshot `device_id` to the actual
   sender. Names and snapshot wall-clock timestamps do not grant ownership.
2. Restore `Checkpoint` only for its matching group. Construct `Engine` with a
   stable device id, local `Config` and a monotonic clock. Do not restore wall
   clock liveness deadlines.
3. Feed real output availability and activity. Use `heartbeat` for a local
   output or `output_report` with an increasing source-report id for a gateway.
4. Feed announcements, tick the engine, and route explicit transfers through
   `transfer`. Ordinary controls only stamp a command in the current term.
5. Persist changed durable state before publishing decisions or executing
   commands. Serialize transitions with checkpoint commits and restore the
   previous engine if persistence fails. Recheck queued command fences before
   their effects reach the output.
6. Apply the resolved owner to the existing UI/audio adapter. Gossip its claim
   even while controlling another output. Online lists are presentation only.

Startup has a discovery grace period. If a restored/relayed owner has not yet
reported, keep the startup decision pending until its heartbeat or timeout.
Discovery is necessarily bounded: an entirely unknown peer across a partition
cannot prevent a local startup claim. Once the peers communicate, normal claim
ordering resolves the conflict.

## Configuration

The complete serde `Config` accepts these fields (unknown fields are errors):

| Field | Default | Meaning |
| --- | --- | --- |
| `claim_on_startup` | `true` | One startup intent for this application instance |
| `automatic_failover` | `true` | Claim when the owner stops reporting |
| `take_paused_on_startup` | `true` | Startup may take a paused/idle output |
| `discovery_ms` | `3000` | Initial discovery grace |
| `owner_timeout_ms` | `120000` | Missing fresh output reports before failover |

`Config::passive()` disables application-startup claims and automatic failover.
Furumusic uses this for the always-on server. A real browser startup supplies a
separate one-shot `request_startup`; explicit selection supplies a transfer.
These events are not generated by server polling. Local browser selection uses
the same `Config::should_claim` policy, with automatic cross-federation claims
disabled. Browser presence expiry preserves the current owner and queue.

TUI exposes the configuration as the `playback` section of its existing settings
file and preserves it when saving other settings. The web adapter reads optional
`playback_config_json` on the user's `furumusic__fed_device_identity` row at
session initialization; null selects the passive profile. Audio availability
is a runtime input, not a persisted setting.

## Versioning and rollout

The library-sync JSON ALPN remains `furumi/sync/2`; catalog, pairing and library
data do not require a new transport. Device profiles advertised protocol
version 3 when coordination was introduced (version 4 adds podcast queue items,
see below). Coordination envelopes independently carry version 1. Snapshot coordination
and operation authority are additive optional JSON fields so library-sync
records from older clients still deserialize. Upgraded clients ignore legacy
personal playback snapshots/commands without the coordination envelope.
The exclusivity guarantee applies after all audio clients have upgraded.

The shared generic `Snapshot<S>` and `Command<S>` keep state conversion in the
applications while preventing divergent control message definitions. These
envelopes use JSON, not a backwards-compatible promise for postcard layouts.
Do not change Jam's independent authority by interpreting personal stamps there.

## Protocol v4: podcast queue items

Device profiles advertise protocol version 4. The JSON ALPN stays
`furumi/sync/2` and the coordination envelope stays version 1. Version 4 adds
to the portable queue DTO:

- `PlaybackTrack.source`: `music`, `podcast_episode` with a self-contained
  `PodcastEpisodeRef`, or `private` (Jam placeholder). It is omitted for
  music, so music-only state serializes exactly as in v3. Podcast episodes are
  never encoded as negative ids or URLs in `file_path`.
- `PlaybackStateWire.playback_rate`: reserved, omitted when unset. Treat a
  missing or unsupported value as 1.0.

`PlaybackSource` is a closed enum: a v3 client fails to parse any message
with a podcast item. Adapters therefore apply the shared helpers from
`music_dht::device_sync`:

1. Send a playback snapshot to a peer only when
   `playback_allowed_for_peer(state, peer_version)` holds; otherwise send
   `None`.
2. Refuse an explicit transfer to a device below v4 while the queue contains
   podcast items and show that the target needs an update. The decision uses
   the version that device reported itself (`profile_protocol_version`),
   never the local constant. Do not strip episodes from the queue.
3. Ignore `SetState`/`ActiveChanged` from peers below v4 while the local
   queue contains podcast items (`playback_command_allowed_from_peer`).
4. A stamped `playback_command` operation with podcast items is itself gated
   by `op_allowed_for_peer`.

A handoff of a podcast episode records the position and the play/pause intent
first, transfers the queue with its sources, and lets the receiver open the
publisher's HTTP source and seek. Ownership still changes only through the
claim, so a late response cannot make the previous owner resume audio. The
durable episode progress is a separate register; see `PODCASTS.md`.

Jam hosts send `music_dht::jam::redact_jam_snapshot`, which replaces podcast
items with opaque `private` placeholders and drops the personal coordination
envelope. They reject participant commands with real episodes
(`jam_command_allowed`) and map placeholders back with
`restore_jam_private_items` before applying a participant's state.

For local integration checks, patch the three frid crates with Cargo's external
`--config` mechanism. Keep released dependency declarations and lockfiles in
the applications; publish frid and then bump client dependencies for rollout.
