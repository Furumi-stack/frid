//! Point-in-time health information for the transport and rendezvous loop.

use std::fmt;
use std::time::Duration;

/// Coarse health state of a running network engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkHealthState {
    /// The engine is starting or has no discovery mechanism configured.
    Discovering,
    /// Transport and rendezvous maintenance are operating normally.
    Healthy,
    /// A recent rendezvous operation failed and is being retried.
    Recovering,
    /// Repeated rendezvous failures indicate that the engine should be restarted.
    Degraded,
}

impl NetworkHealthState {
    /// Returns a compact stable label for logs and user interfaces.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovering => "discovering",
            Self::Healthy => "healthy",
            Self::Recovering => "recovering",
            Self::Degraded => "degraded",
        }
    }
}

impl fmt::Display for NetworkHealthState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Point-in-time health snapshot of a running network engine.
///
/// Zero connected peers is not itself an error: a network may legitimately
/// contain only one online node. [`Self::restart_recommended`] is therefore
/// based on repeated discovery failures as well as the absence of peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkHealthSnapshot {
    /// Coarse state suitable for logs and user interfaces.
    pub state: NetworkHealthState,
    /// Number of currently connected peers.
    pub connected_peers: usize,
    /// Whether Mainline-DHT rendezvous discovery is enabled.
    pub rendezvous_enabled: bool,
    /// Consecutive failed or timed-out rendezvous rounds.
    pub consecutive_rendezvous_failures: u32,
    /// Consecutive rendezvous rounds in which every discovered peer dial failed.
    pub consecutive_peer_dial_failures: u32,
    /// Number of times the rendezvous client was rebuilt after failures.
    pub rendezvous_restarts: u64,
    /// Time elapsed since the most recent successful rendezvous round.
    pub last_rendezvous_success_ago: Option<Duration>,
    /// Most recent rendezvous error, if a later successful round has not cleared it.
    pub last_rendezvous_error: Option<String>,
    /// Whether an application-owned full service restart is advisable.
    pub restart_recommended: bool,
}
