# Frid architecture

Frid separates decentralized applications into a transport layer and a
domain-specific overlay. The transport answers **how peers find and talk to
each other**; the overlay answers **what data is routed, replicated, and
queried**.

```text
application
    ├── UI, playback, runtime, policy
    │
furumi-library
    ├── local catalog, import, migrations
    ├── playlists, likes, history, similarity persistence
    │
music-dht
    ├── Kademlia routing and iterative lookup
    ├── replicated records, revisions, TTLs, tombstones
    ├── catalog/content discovery
    │
federation-net
    ├── authenticated iroh connections
    ├── typed bounded messages
    ├── application ALPN byte streams
    └── optional Mainline-DHT rendezvous
```

No layer assigns a permanent server role to a peer.

`furumi-library` is client-independent local persistence. It may reuse stable
music-domain identifiers and device-sync DTOs from `music-dht`, but it does
not start networking, own an application runtime, or depend on a particular
UI or playback engine. Dependency direction remains one-way:
`federation-net <- music-dht <- furumi-library <- applications`.

## Local library: furumi-library

All Furumi clients share one platform-native `library.db` contract. The crate
owns its SQLite schema and migrations, local catalog models and queries,
metadata import, artwork resolution, playlists, likes, listening history, and
similarity index storage. Connections enable foreign keys, WAL, and a bounded
busy timeout so independently running clients can safely share the database.

Filesystem scanning schedules, playback, federation lifecycle, and UI state
remain application responsibilities. Database and metadata work must stay off
interactive and async reactor threads.

## Transport: federation-net

### Identity and authentication

Every engine owns a persistent iroh secret key. Its public endpoint ID is the
peer identity used by connections, tickets, events, and higher-level routing.
Authentication comes from the iroh connection; address or ticket payloads are
never treated as proof of peer identity.

Keeping transport identity stable across restarts lets overlays derive stable
node IDs and retain routing knowledge without a separate account system.

### Network and schema isolation

The transport ALPN identifies the federation-net protocol. An application
handshake then verifies:

- protocol version;
- `NetworkId`, which selects an independent deployment;
- `SchemaId`, which identifies the typed message format.

This is intentional double isolation: the ALPN selects the transport protocol,
while network and schema IDs prevent unrelated deployments or incompatible
applications from exchanging domain payloads.

Changing serialized messages incompatibly requires a new schema ID. Changing
the transport handshake incompatibly requires a protocol-version/ALPN plan.
The music DHT currently derives its schema id from `music-dht-v5`; the former
proof-of-concept name is no longer part of the production protocol identity.

### Discovery is not authorization

Automatic discovery publishes short-lived endpoint records into a
network-specific BEP44 mutable record in the public BitTorrent Mainline DHT.
The signing key is derived from the network ID, allowing equal peers to update
the shared rendezvous set without a dedicated bootstrap service.

Knowing a network ID allows discovery. It does not establish application
authorization. Private membership, ACLs, or trusted-device pairing belong in a
higher-level protocol.

Tickets provide explicit discovery and carry enough connection information to
dial a peer directly. They remain useful when public rendezvous is disabled or
unavailable.

Jam control uses its own auxiliary ALPN and an opaque `frid://j/...`
capability. It reuses the portable playback state and command vocabulary from
personal-device sync, but never carries trusted membership, likes, playlists,
or listening history. Possession of the runtime capability authorizes control
of that host player only.

### Two data paths

Typed messages and raw byte streams serve different workloads:

- the message channel carries bounded, serde/postcard application messages and
  emits them through a bounded event queue;
- registered ALPN streams carry protocol-specific request/response or bulk
  data directly between authenticated peers.

This prevents catalogs, media, and synchronization payloads from inflating the
control/event channel. Higher layers can evolve their stream protocols without
adding domain logic to federation-net.

## Overlay: music-dht

### Routing

Each peer derives a stable 256-bit `NodeId` from its endpoint identity. Routing
uses XOR distance and Kademlia-style buckets. Iterative lookups query a bounded
number of the closest known peers in parallel and learn additional contacts
from responses.

The overlay is not broadcast-based. Query cost follows the routing graph, and
contacts are dialed on demand using stored peer tickets.

### Records and indexing

`LibraryItem` is the published domain record. An item is indexed under several
BLAKE3-derived DHT keys:

- the complete normalized name;
- tokens from names, artists, featured artists, and release titles;
- an exact content key when a content ID is available.

Multiple index keys point to the same stable item identity. This makes search
and exact-content resolution different queries over one record model rather
than separate databases.

Publishers declare their complete desired library through `sync_library`.
The service diffs that declaration against owned state, republishes changes,
and tombstones removed items. Callers do not manually maintain every DHT index
entry.

### Replication and convergence

Records are stored on the `K` known nodes closest to each key. Owners advance
revisions and periodically republish active state. Replicas expire after a TTL
if their owner disappears.

Deletion uses revisioned tombstones. A tombstone wins over an active record at
the same or an older revision, preventing delayed replication from immediately
resurrecting deleted content. Tombstones have a longer lifetime than ordinary
records so the removal has time to propagate.

This model provides eventual availability and convergence without consensus.
The DHT is a distributed directory, not a transactional global database.

### Persistence abstraction

`MusicDhtStorage` separates protocol behavior from storage ownership. The
default backend stores local items, replicas, routing contacts, and publication
state in SQLite. Applications may supply another durable implementation while
preserving the same service semantics.

Database work is executed outside async reactor threads. Corrupt or expired
replica payloads are isolated from healthy records rather than crashing the
node.

### Extension protocols

Music discovery identifies an owner and content; it does not force all domain
traffic through DHT messages. `music-dht` exposes the underlying authenticated
stream capability and defines shared wire models for Furumi catalog and device
sync protocols. Audio transfer, rich catalog exchange, and synchronization can
therefore use dedicated ALPNs while sharing identity and connectivity.

Music similarity is another shared extension protocol. `music-dht` owns its
versioned ALPN, bounded request/response models, validation, and byte-stream
framing. Applications own model selection, user consent, audio preprocessing,
embedding generation and storage, nearest-neighbor search, participation
policy, and result presentation. Compatible clients can therefore use
different local implementations while exchanging vectors only when their exact
profile fingerprints match.

Similarity peer selection uses a second, schema-independent Kademlia overlay.
It derives a model-neutral 256-bit SimHash, maps fixed subsets into twelve
two-level LSH tables, and publishes one summary per owner and occupied coarse
bucket. The coarse key is routed through the overlay; compact fine-bucket
suffixes, one anonymous representative signature, and the owner's self-contained
connection ticket are carried in the value. This bounds a peer's presence in a
hot bucket independently of how many tracks it owns, makes multi-hop results
dialable, and avoids a canonical calibration file.

Routing summaries are signed by the owner's persistent transport identity.
Replicas validate the owner signature, network, profile, derived key, bucket
bounds, issue time and TTL before storing or forwarding a record. The routing
cache uses a separate SQLite database because it is replaceable federation
state, not application embedding storage. Its ALPN is additive: peers that do
not implement it continue to use the unchanged `music-dht-v5` catalog network,
while upgraded peers bootstrap and gossip their own routing contacts.

Capability discovery is the narrow exception to schema isolation. The bounded,
self-versioned `furumi/capabilities/1` stream still validates the federation
transport version and network id, but remains reachable across application
schema upgrades. A peer publishes the versions it actually supports; consumers
retain the highest versions observed during the current run and may recommend a
manual application update. Capability data is informational only and never
authorizes a peer or triggers an automatic update.

## Failure model

Frid assumes normal distributed-system failures:

- discovered peers may be offline before they are dialed;
- connections may change between direct and relay paths;
- rendezvous records may be stale, malformed, or concurrently updated;
- messages may time out after a peer disconnects;
- routing tables may contain dead contacts;
- replicas may expire before an owner returns;
- event consumers may be slower than producers.

The response is bounded degradation, not global failure. Dial backoff limits
repeated failures, lookup budgets guarantee termination, stale records expire,
per-connection errors become events, and bounded channels apply backpressure.

## Resource boundaries

Untrusted network input is bounded before allocation or fan-out:

- maximum typed-message frame size;
- ticket and rendezvous record sizes;
- event channel capacity;
- concurrent streams per peer;
- pending request count and lookup budget;
- peer-exchange contacts;
- records per response and store batch;
- item names, artist lists, tokens, and content IDs.

New protocols must define equivalent limits. A valid peer identity does not
make its payloads trusted.

## Compatibility rules

Frid is consumed by multiple applications, so compatibility is an
architectural constraint:

1. Do not change existing ALPN bytes, ticket formats, protocol versions,
   derivation domains, or serialized wire layouts accidentally.
2. Additive serialized fields require an explicit backward-compatibility
   strategy; postcard formats do not become extensible automatically.
3. Changes to normalization or key derivation alter where records live and
   require a migration/protocol plan.
4. SQLite schema changes must preserve existing identities, owned records, and
   routing state.
5. Public re-exports are part of the library API even when their definitions
   live in another crate.
6. A refactor is not behavior-preserving until unit, integration, and doctests
   pass with real peer connections.

## Architectural invariants

- No Frid-operated service is required for peer communication.
- All peers can initiate, route, store, and query according to the same rules.
- Discovery mechanisms do not silently become authorization mechanisms.
- Domain policy stays above federation-net.
- DHT operations remain bounded and terminate in the presence of dead peers.
- Local durable state survives ordinary peer and network failure.
- Additional stream protocols reuse authenticated connectivity without
  coupling bulk data to the typed control channel.
