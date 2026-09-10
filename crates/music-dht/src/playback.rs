//! Transport-independent personal playback coordination.
//!
//! This is an eventually consistent register, not a distributed lock. During a
//! partition multiple outputs may play. Once announcements are delivered, all
//! participants adopt the greatest claim and only its owner may keep playing.
//! Applications authenticate membership, persist [`DurableState`] before acting,
//! feed monotonic time and actual output heartbeats, and execute ownership changes.
//! Relaying an announcement must never manufacture an output heartbeat.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Version of the additive playback coordination envelope.
pub const VERSION: u16 = 1;
/// Maximum command senders retained within one ownership term.
pub const MAX_COMMAND_SENDERS: usize = 128;

/// Local choices; these do not weaken conflict resolution or command fencing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Claim idle playback once after discovery at application startup.
    pub claim_on_startup: bool,
    /// Become the output when its owner stops producing heartbeats.
    pub automatic_failover: bool,
    /// A newly started output may take over a paused owner.
    pub take_paused_on_startup: bool,
    /// Wait for initial peer discovery before considering a startup claim.
    pub discovery_ms: u64,
    /// Missing output heartbeats, not server connectivity, trigger failover.
    pub owner_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            claim_on_startup: true,
            automatic_failover: true,
            take_paused_on_startup: true,
            discovery_ms: 3_000,
            owner_timeout_ms: 120_000,
        }
    }
}

impl Config {
    /// Shared candidate policy, also used when a gateway selects a browser.
    /// `owner` is `(actually_playing, milliseconds_since_fresh_output_report)`.
    /// Startup intent is a one-shot event, never inferred from reconnection.
    pub fn should_claim(&self, startup: bool, owner: Option<(bool, u64)>) -> bool {
        let vacant = owner.is_none();
        let expired = owner.is_some_and(|(_, age)| age >= self.owner_timeout_ms.max(1));
        let idle = owner.is_some_and(|(playing, _)| !playing);
        (startup && (vacant || expired || (self.take_paused_on_startup && idle)))
            || (self.automatic_failover && (vacant || expired))
    }

    /// An always-running gateway only transfers ownership on user intent.
    pub fn passive() -> Self {
        Self {
            claim_on_startup: false,
            automatic_failover: false,
            ..Self::default()
        }
    }
}

/// A Lamport term with a deterministic tie-breaker. No wall-clock ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Claim {
    /// Greater than every term the issuer has observed.
    pub counter: u64,
    /// Authenticated device issuing the claim.
    pub issuer: String,
    /// Device allowed to render personal audio in this term.
    pub owner: String,
}

impl Claim {
    fn valid(&self) -> bool {
        self.counter > 0
            && [&self.issuer, &self.owner]
                .into_iter()
                .all(|id| !id.is_empty() && id.len() <= 256)
    }
}

/// Persist atomically before publishing a new claim or applying a command.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DurableState {
    /// Winning claim, retained even after the owner becomes unavailable.
    pub claim: Option<Claim>,
    /// Last accepted command sequence per sender in the winning term.
    pub commands: BTreeMap<String, u64>,
    /// Locally authored command sequence; never reset on ownership changes.
    pub command_sequence: u64,
    /// Locally authored output heartbeat; retained across process restarts.
    pub heartbeat_sequence: u64,
}

/// Gossip carried beside a playback snapshot, including by controllers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Announcement {
    /// Envelope version; unknown versions are ignored.
    pub version: u16,
    /// Greatest observed ownership claim.
    pub claim: Option<Claim>,
    /// Advances only when the sender's actual output reports activity.
    pub heartbeat: u64,
    /// Sender can currently render audio; a server alone cannot.
    pub available: bool,
    /// Sender is actually playing unpaused audio.
    pub playing: bool,
}

/// Commands are scoped to an ownership term and ordered per sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandStamp {
    /// Coordination envelope version.
    pub version: u16,
    /// Ownership term in which the command is valid.
    pub claim: Claim,
    /// Increasing sequence of the authenticated operation origin.
    pub sequence: u64,
}

/// Application checkpoint scoped to a trusted-device group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Application-defined membership scope. Do not reuse state across groups.
    pub scope: String,
    /// Durable coordinator state in that scope.
    pub state: DurableState,
}

/// Shared snapshot envelope; applications supply their portable queue DTO.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot<S> {
    /// Authenticated sender, checked against the transport by the adapter.
    pub device_id: String,
    /// Display name only; never an identity or ordering key.
    pub device_name: String,
    /// Compatibility/UI projection. Ownership comes only from coordination.
    pub active: bool,
    /// Position extrapolation timestamp, never used for claim ordering.
    pub updated_at_ms: i64,
    /// Output state or the controller's cached presentation state.
    pub state: S,
    /// Absent on older clients and on the independently scoped Jam protocol.
    #[serde(default)]
    pub coordination: Option<Announcement>,
}

/// Shared personal/Jam command vocabulary. Personal commands additionally
/// require [`CommandStamp`]; Jam keeps its own capability and authority scope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Command<S> {
    /// Update the current owner's output state.
    SetState {
        /// Queue and transport state.
        state: S,
        /// Also seek the actual audio output.
        #[serde(default)]
        seek: bool,
    },
    /// Explicit transfer with state for the newly selected output.
    ActiveChanged {
        /// Selected output identity.
        active_device_id: String,
        /// Selected output display name.
        active_device_name: String,
        /// State to render after transfer.
        state: S,
    },
}

/// Pure engine. All times are monotonic milliseconds supplied by the adapter.
#[derive(Debug, Clone)]
pub struct Engine {
    id: String,
    config: Config,
    durable: DurableState,
    started_at: u64,
    startup_pending: bool,
    owner_seen_at: u64,
    owner_heartbeat: u64,
    owner_playing: bool,
    available: bool,
    playing: bool,
    output_report: u64,
    heartbeat_sequence: u64,
}

impl Engine {
    /// Restore durable state, but never restore persisted liveness timestamps.
    pub fn new(id: String, config: Config, durable: DurableState, now: u64) -> Self {
        let heartbeat_sequence = durable.heartbeat_sequence;
        let owner_playing = durable.claim.is_some();
        Self {
            id,
            startup_pending: config.claim_on_startup,
            config,
            durable,
            started_at: now,
            owner_seen_at: now,
            owner_heartbeat: 0,
            owner_playing,
            available: false,
            playing: false,
            output_report: 0,
            heartbeat_sequence,
        }
    }

    /// Durable checkpoint. Save it before executing decisions.
    pub fn durable(&self) -> &DurableState {
        &self.durable
    }

    /// Winning output, whether currently reachable or not.
    pub fn owner(&self) -> Option<&str> {
        self.durable
            .claim
            .as_ref()
            .map(|claim| claim.owner.as_str())
    }

    /// Whether this device currently owns personal playback.
    pub fn is_owner(&self) -> bool {
        self.owner() == Some(self.id.as_str())
    }

    /// Update local capabilities without interpreting connectivity as playback.
    pub fn set_output(&mut self, available: bool, playing: bool) {
        if self.available && !available {
            self.advance_heartbeat();
        }
        self.available = available;
        self.playing = available && playing;
    }

    /// Feed one fresh report from the actual local output, even if paused.
    pub fn heartbeat(&mut self, now: u64) {
        if self.available {
            self.advance_heartbeat();
            if self.is_owner() {
                self.owner_seen_at = now;
                self.owner_playing = self.playing;
            }
        }
    }

    fn advance_heartbeat(&mut self) {
        self.heartbeat_sequence = self.heartbeat_sequence.saturating_add(1);
        // Reserve a range before publishing. Restart skips unused values.
        if self.heartbeat_sequence > self.durable.heartbeat_sequence {
            self.durable.heartbeat_sequence = self.heartbeat_sequence.saturating_add(1024);
        }
    }

    /// Merge gossip received from an authenticated trusted device.
    /// Returns false for unsupported or malformed envelopes.
    pub fn observe(&mut self, sender: &str, message: &Announcement, now: u64) -> bool {
        if message.version != VERSION || message.claim.as_ref().is_some_and(|c| !c.valid()) {
            return false;
        }
        if let Some(claim) = &message.claim {
            self.merge(claim, now);
        }
        if self.owner() == Some(sender)
            && self.durable.claim == message.claim
            && message.heartbeat > self.owner_heartbeat
        {
            self.owner_heartbeat = message.heartbeat;
            self.owner_playing = message.available && message.playing;
            if message.available {
                self.owner_seen_at = now;
            }
        }
        true
    }

    fn merge(&mut self, claim: &Claim, now: u64) {
        if self
            .durable
            .claim
            .as_ref()
            .is_none_or(|current| claim > current)
        {
            self.durable.claim = Some(claim.clone());
            self.durable.commands.clear();
            self.owner_seen_at = now;
            self.owner_heartbeat = 0;
            // A transferred owner gets a full grace period before failover.
            self.owner_playing = true;
        }
    }

    /// Apply explicit user intent. Target eligibility is checked by the adapter.
    /// Returns false rather than wrapping an exhausted Lamport counter.
    pub fn transfer(&mut self, target: &str, now: u64) -> bool {
        if target.is_empty() || target.len() > 256 {
            return false;
        }
        let Some(counter) = self
            .durable
            .claim
            .as_ref()
            .map_or(Some(1), |c| c.counter.checked_add(1))
        else {
            return false;
        };
        let claim = Claim {
            counter,
            issuer: self.id.clone(),
            owner: target.into(),
        };
        self.merge(&claim, now);
        self.startup_pending = false;
        true
    }

    /// A real browser/application startup, never a server timer or reconnect.
    pub fn request_startup(&mut self) {
        self.startup_pending = true;
    }

    /// Feed a monotonically increasing output-report identifier. Polling the
    /// same cached report cannot renew owner liveness.
    pub fn output_report(&mut self, report: u64, now: u64) {
        if report > self.output_report {
            self.output_report = report;
            self.heartbeat(now);
        }
    }

    /// Fence an effect again immediately before executing it in an event loop.
    pub fn command_is_current(&self, sender: &str, stamp: &CommandStamp) -> bool {
        self.durable.claim.as_ref() == Some(&stamp.claim)
            && self.durable.commands.get(sender) == Some(&stamp.sequence)
    }

    /// Whether an owner snapshot is at least as recent as accepted liveness.
    pub fn announcement_is_current(&self, sender: &str, message: &Announcement) -> bool {
        self.owner() == Some(sender)
            && self.durable.claim == message.claim
            && message.heartbeat >= self.owner_heartbeat
    }

    /// Evaluate startup/failover. A losing node cannot immediately reclaim:
    /// the winning term always receives an entire heartbeat grace period.
    pub fn tick(&mut self, now: u64) -> bool {
        if now.saturating_sub(self.started_at) < self.config.discovery_ms {
            return false;
        }
        // A restored or relayed owner has unknown activity until it reports.
        // Keep startup intent pending through discovery, rather than treating
        // an unknown playing state as an idle output.
        if self.startup_pending
            && self.owner().is_some()
            && !self.is_owner()
            && self.owner_heartbeat == 0
            && now.saturating_sub(self.owner_seen_at) < self.config.owner_timeout_ms.max(1)
        {
            return false;
        }
        let startup = self.startup_pending;
        self.startup_pending = false;
        let eligible = self.available && !self.is_owner();
        let owner = self
            .owner()
            .map(|_| (self.owner_playing, now.saturating_sub(self.owner_seen_at)));
        if eligible && self.config.should_claim(startup, owner) {
            return self.transfer(&self.id.clone(), now);
        }
        false
    }

    /// Current gossip. Controllers repeat the claim, never the owner's heartbeat.
    pub fn announcement(&self) -> Announcement {
        Announcement {
            version: VERSION,
            claim: self.durable.claim.clone(),
            heartbeat: self.heartbeat_sequence,
            available: self.available,
            playing: self.playing,
        }
    }

    /// Stamp a command with the current ownership term.
    pub fn stamp(&mut self) -> Option<CommandStamp> {
        let claim = self.durable.claim.clone()?;
        self.durable.command_sequence = self.durable.command_sequence.checked_add(1)?;
        Some(CommandStamp {
            version: VERSION,
            claim,
            sequence: self.durable.command_sequence,
        })
    }

    /// Merge the referenced term and fence a command. Commands may carry a
    /// previously issued claim ahead of its gossip (transport can reorder).
    /// Creating claims remains exclusive to transfer/startup/failover.
    /// Sender identity comes from the authenticated operation log.
    pub fn accept_command(
        &mut self,
        sender: &str,
        stamp: &CommandStamp,
        handoff: bool,
        now: u64,
    ) -> bool {
        if stamp.version != VERSION
            || !stamp.claim.valid()
            || sender.is_empty()
            || sender.len() > 256
            || stamp.sequence == 0
        {
            return false;
        }
        if handoff && stamp.claim.issuer != sender {
            return false;
        }
        self.merge(&stamp.claim, now);
        if self.durable.claim.as_ref() != Some(&stamp.claim) {
            return false;
        }
        if !self.durable.commands.contains_key(sender)
            && self.durable.commands.len() >= MAX_COMMAND_SENDERS
        {
            return false;
        }
        let previous = self.durable.commands.entry(sender.to_string()).or_default();
        if stamp.sequence <= *previous {
            return false;
        }
        *previous = stamp.sequence;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(id: &str, config: Config) -> Engine {
        let mut e = Engine::new(id.into(), config, DurableState::default(), 0);
        e.set_output(true, false);
        e
    }
    #[test]
    fn partition_converges_without_reclaim_loop() {
        let mut a = node("a", Config::default());
        let mut b = node("b", Config::default());
        assert!(a.tick(3_000));
        assert!(b.tick(3_000));
        a.set_output(true, true);
        b.set_output(true, true);
        a.heartbeat(3_001);
        b.heartbeat(3_001);
        let aa = a.announcement();
        let bb = b.announcement();
        a.observe("b", &bb, 3_002);
        b.observe("a", &aa, 3_002);
        assert_eq!(a.owner(), b.owner());
        assert!(!a.is_owner());
        assert!(b.is_owner());
        assert!(!a.tick(3_003));
        a.observe("a", &aa, 3_004);
        assert_eq!(a.owner(), Some("b"));
    }
    #[test]
    fn startup_respects_playing_but_can_take_paused() {
        for playing in [true, false] {
            let mut owner = node("a", Config::default());
            owner.tick(3_000);
            owner.set_output(true, playing);
            owner.heartbeat(3_001);
            let mut newcomer = node("b", Config::default());
            newcomer.observe("a", &owner.announcement(), 2_000);
            assert_eq!(newcomer.tick(3_000), !playing);
        }
    }
    #[test]
    fn passive_gateway_never_claims_without_intent() {
        let mut web = node("web", Config::passive());
        assert!(!web.tick(3_000));
        assert!(!web.tick(1_000_000));
        assert!(web.transfer("web", 1_000_001));
        assert!(web.is_owner());
    }
    #[test]
    fn duplicates_and_relays_do_not_extend_liveness() {
        let mut a = node("a", Config::default());
        a.tick(3_000);
        a.set_output(true, true);
        a.heartbeat(3_001);
        let mut b = node("b", Config::default());
        b.observe("a", &a.announcement(), 3_002);
        b.tick(3_003);
        b.observe("a", &a.announcement(), 100_000);
        b.observe("relay", &a.announcement(), 122_000);
        assert!(b.tick(123_003));
    }
    #[test]
    fn commands_are_fenced_ordered_and_survive_restore() {
        let mut a = node("a", Config::default());
        a.tick(3_000);
        let old = a.stamp().unwrap();
        assert!(a.accept_command("a", &old, false, 3_001));
        let mut restored = Engine::new("a".into(), Config::default(), a.durable().clone(), 4_000);
        assert!(!restored.accept_command("a", &old, false, 4_001));
        restored.transfer("b", 4_002);
        assert!(!restored.accept_command("a", &old, false, 4_003));
        let forged = restored.stamp().unwrap();
        assert!(!restored.accept_command("spoof", &forged, true, 4_004));
    }

    #[test]
    fn three_outputs_converge_in_every_delivery_order() {
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let mut nodes = [
                node("a", Config::default()),
                node("b", Config::default()),
                node("c", Config::default()),
            ];
            for node in &mut nodes {
                node.tick(3_000);
                node.set_output(true, true);
                node.heartbeat(3_001);
            }
            let messages = nodes.each_ref().map(Engine::announcement);
            for node in &mut nodes {
                for index in order {
                    node.observe(["a", "b", "c"][index], &messages[index], 4_000);
                }
                assert_eq!(node.owner(), Some("c"));
                assert!(!node.tick(4_001));
            }
            assert_eq!(nodes.iter().filter(|node| node.is_owner()).count(), 1);
        }
    }

    #[test]
    fn cached_browser_reports_do_not_keep_a_suspended_output_alive() {
        let mut web = node("web", Config::passive());
        web.transfer("web", 0);
        web.set_output(true, true);
        web.output_report(1, 1);
        let mut tui = node("tui", Config::default());
        tui.observe("web", &web.announcement(), 2);
        tui.tick(3_000);
        for now in [30_000, 60_000, 90_000, 120_003] {
            web.output_report(1, now);
            tui.observe("web", &web.announcement(), now);
        }
        assert!(tui.tick(120_003));
        web.observe("tui", &tui.announcement(), 120_004);
        assert!(!web.is_owner());
    }

    #[test]
    fn heartbeat_reservation_survives_restart() {
        let mut a = node("a", Config::default());
        a.tick(3_000);
        a.heartbeat(3_001);
        let before = a.announcement();
        let mut restored = Engine::new("a".into(), Config::default(), a.durable().clone(), 0);
        restored.set_output(true, false);
        restored.heartbeat(1);
        assert!(restored.announcement().heartbeat > before.heartbeat);
    }

    #[test]
    fn command_can_arrive_before_handoff_and_older_state_cannot_overwrite_it() {
        let mut a = node("a", Config::default());
        a.transfer("b", 1);
        let handoff = a.stamp().unwrap();
        let play = a.stamp().unwrap();
        let mut b = node("b", Config::default());
        assert!(b.accept_command("a", &play, false, 2));
        assert!(b.is_owner());
        assert!(!b.accept_command("a", &handoff, true, 3));
        assert!(b.command_is_current("a", &play));
        let mut incompatible = a.stamp().unwrap();
        incompatible.version += 1;
        assert!(!b.accept_command("a", &incompatible, false, 4));
    }

    #[test]
    fn disabled_automatic_policy_and_unavailable_outputs_never_claim() {
        let mut disabled = node(
            "a",
            Config {
                claim_on_startup: false,
                automatic_failover: false,
                ..Config::default()
            },
        );
        assert!(!disabled.tick(1_000_000));
        let mut unavailable = node("b", Config::default());
        unavailable.set_output(false, false);
        assert!(!unavailable.tick(1_000_000));
    }

    #[test]
    fn command_sender_memory_is_bounded() {
        let mut a = node("a", Config::default());
        a.tick(3_000);
        let stamp = a.stamp().unwrap();
        for index in 0..MAX_COMMAND_SENDERS {
            assert!(a.accept_command(&format!("sender{index}"), &stamp, false, 3_001));
        }
        assert!(!a.accept_command("overflow", &stamp, false, 3_002));
        assert_eq!(a.durable().commands.len(), MAX_COMMAND_SENDERS);
    }
}
