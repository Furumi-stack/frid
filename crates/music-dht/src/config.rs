//! Service configuration.

use std::path::PathBuf;
use std::time::Duration;

use federation_net::{NetworkId, RendezvousConfig};

use crate::error::{MusicDhtError, Result};

/// Default interval between republish rounds.
pub const DEFAULT_REPUBLISH_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Default interval between expired-record sweeps.
pub const DEFAULT_EXPIRE_INTERVAL: Duration = Duration::from_secs(60);
/// Default timeout of a single DHT request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Default timeout of a whole iterative lookup.
pub const DEFAULT_LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);
/// Default timeout for transport operations (dialing, handshakes, sends).
pub const DEFAULT_TRANSPORT_TIMEOUT: Duration = Duration::from_secs(15);
/// Default timeout for on-demand dials during DHT operations.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for an [`crate::MusicDhtService`].
///
/// Use [`MusicDhtConfig::builder`] to construct a validated instance.
#[derive(Debug, Clone)]
pub struct MusicDhtConfig {
    /// Directory for the peer identity and the SQLite database.
    pub data_dir: PathBuf,
    /// Network this peer participates in.
    pub network_id: NetworkId,
    /// Interval between automatic republish rounds.
    pub republish_interval: Duration,
    /// Interval between sweeps of expired DHT records.
    pub expire_interval: Duration,
    /// Timeout of a single DHT request.
    pub request_timeout: Duration,
    /// Timeout of a whole iterative lookup.
    pub lookup_timeout: Duration,
    /// Timeout for transport operations: dialing a peer, handshakes and
    /// message delivery. Kept separate from `request_timeout` because
    /// establishing a connection through relays can take much longer than a
    /// request over an existing one.
    pub transport_timeout: Duration,
    /// Timeout for dialing a contact **on demand during DHT operations**
    /// (lookups, publishes). Deliberately shorter than `transport_timeout`:
    /// a dead contact must not stall a whole lookup, and a peer that needs
    /// longer than this to dial will still be reached by the periodic
    /// rendezvous/republish machinery.
    pub dial_timeout: Duration,
    /// Automatic peer discovery over the mainline DHT: peers of the same
    /// network find each other knowing nothing but the network id. `None`
    /// disables it; peers are then connected via tickets only.
    pub rendezvous: Option<RendezvousConfig>,
    /// Auxiliary ALPN protocols on which this peer accepts raw byte streams
    /// (see [`crate::MusicDhtService::stream_acceptor`]).
    pub stream_protocols: Vec<Vec<u8>>,
    /// Auxiliary stream protocols that remain reachable across DHT schema
    /// upgrades, such as bounded capability discovery.
    pub schema_independent_stream_protocols: Vec<Vec<u8>>,
}

impl MusicDhtConfig {
    /// Returns a new [`MusicDhtConfigBuilder`].
    pub fn builder() -> MusicDhtConfigBuilder {
        MusicDhtConfigBuilder::default()
    }
}

/// Builder for [`MusicDhtConfig`].
///
/// `data_dir` and `network_id` are required; the timers default to the
/// production values and are configurable mainly for tests.
#[derive(Debug, Default, Clone)]
pub struct MusicDhtConfigBuilder {
    data_dir: Option<PathBuf>,
    network_id: Option<NetworkId>,
    republish_interval: Option<Duration>,
    expire_interval: Option<Duration>,
    request_timeout: Option<Duration>,
    lookup_timeout: Option<Duration>,
    transport_timeout: Option<Duration>,
    dial_timeout: Option<Duration>,
    rendezvous: Option<RendezvousConfig>,
    stream_protocols: Vec<Vec<u8>>,
    schema_independent_stream_protocols: Vec<Vec<u8>>,
}

impl MusicDhtConfigBuilder {
    /// Sets the data directory.
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }

    /// Sets the network identifier.
    pub fn network_id(mut self, network_id: NetworkId) -> Self {
        self.network_id = Some(network_id);
        self
    }

    /// Sets the republish interval.
    pub fn republish_interval(mut self, interval: Duration) -> Self {
        self.republish_interval = Some(interval);
        self
    }

    /// Sets the expired-record sweep interval.
    pub fn expire_interval(mut self, interval: Duration) -> Self {
        self.expire_interval = Some(interval);
        self
    }

    /// Sets the timeout of a single DHT request.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Sets the timeout of a whole iterative lookup.
    pub fn lookup_timeout(mut self, timeout: Duration) -> Self {
        self.lookup_timeout = Some(timeout);
        self
    }

    /// Sets the timeout for transport operations (dialing, handshakes).
    pub fn transport_timeout(mut self, timeout: Duration) -> Self {
        self.transport_timeout = Some(timeout);
        self
    }

    /// Sets the timeout for on-demand dials during DHT operations.
    pub fn dial_timeout(mut self, timeout: Duration) -> Self {
        self.dial_timeout = Some(timeout);
        self
    }

    /// Enables automatic peer discovery over the mainline DHT.
    pub fn rendezvous(mut self, rendezvous: RendezvousConfig) -> Self {
        self.rendezvous = Some(rendezvous);
        self
    }

    /// Declares an auxiliary ALPN protocol on which this peer accepts raw
    /// byte streams (see [`crate::MusicDhtService::stream_acceptor`]). May
    /// be called multiple times, once per protocol. The ALPNs are validated
    /// by the underlying network engine on service start.
    pub fn stream_protocol(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.stream_protocols.push(alpn.into());
        self
    }

    /// Declares a self-versioned auxiliary protocol that does not require the
    /// local and remote DHT schema ids to match.
    pub fn schema_independent_stream_protocol(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.schema_independent_stream_protocols.push(alpn.into());
        self
    }

    /// Validates and builds the configuration.
    pub fn build(self) -> Result<MusicDhtConfig> {
        let data_dir = self
            .data_dir
            .ok_or_else(|| MusicDhtError::Database("data_dir is required".into()))?;
        if data_dir.as_os_str().is_empty() {
            return Err(MusicDhtError::Database("data_dir must not be empty".into()));
        }
        let network_id = self
            .network_id
            .ok_or_else(|| MusicDhtError::Network("network_id is required".into()))?;

        let config = MusicDhtConfig {
            data_dir,
            network_id,
            republish_interval: self
                .republish_interval
                .unwrap_or(DEFAULT_REPUBLISH_INTERVAL),
            expire_interval: self.expire_interval.unwrap_or(DEFAULT_EXPIRE_INTERVAL),
            request_timeout: self.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT),
            lookup_timeout: self.lookup_timeout.unwrap_or(DEFAULT_LOOKUP_TIMEOUT),
            transport_timeout: self.transport_timeout.unwrap_or(DEFAULT_TRANSPORT_TIMEOUT),
            dial_timeout: self.dial_timeout.unwrap_or(DEFAULT_DIAL_TIMEOUT),
            rendezvous: self.rendezvous,
            stream_protocols: self.stream_protocols,
            schema_independent_stream_protocols: self.schema_independent_stream_protocols,
        };
        for (name, value) in [
            ("republish_interval", config.republish_interval),
            ("expire_interval", config.expire_interval),
            ("request_timeout", config.request_timeout),
            ("lookup_timeout", config.lookup_timeout),
            ("transport_timeout", config.transport_timeout),
            ("dial_timeout", config.dial_timeout),
        ] {
            if value.is_zero() {
                return Err(MusicDhtError::Database(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_applies_defaults() {
        let config = MusicDhtConfig::builder()
            .data_dir("./dir")
            .network_id(NetworkId::from_name("test"))
            .build()
            .expect("valid");
        assert_eq!(config.republish_interval, DEFAULT_REPUBLISH_INTERVAL);
        assert_eq!(config.expire_interval, DEFAULT_EXPIRE_INTERVAL);
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(config.lookup_timeout, DEFAULT_LOOKUP_TIMEOUT);
    }

    #[test]
    fn builder_rejects_missing_or_invalid() {
        assert!(MusicDhtConfig::builder().build().is_err());
        assert!(
            MusicDhtConfig::builder()
                .data_dir("./dir")
                .network_id(NetworkId::from_name("test"))
                .request_timeout(Duration::ZERO)
                .build()
                .is_err()
        );
    }
}
