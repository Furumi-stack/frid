//! Service configuration.

use std::path::PathBuf;
use std::time::Duration;

use federation_net::NetworkId;

use crate::error::{ArtistDhtError, Result};

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

/// Configuration for an [`crate::ArtistDhtService`].
///
/// Use [`ArtistDhtConfig::builder`] to construct a validated instance.
#[derive(Debug, Clone)]
pub struct ArtistDhtConfig {
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
}

impl ArtistDhtConfig {
    /// Returns a new [`ArtistDhtConfigBuilder`].
    pub fn builder() -> ArtistDhtConfigBuilder {
        ArtistDhtConfigBuilder::default()
    }
}

/// Builder for [`ArtistDhtConfig`].
///
/// `data_dir` and `network_id` are required; the timers default to the
/// production values and are configurable mainly for tests.
#[derive(Debug, Default, Clone)]
pub struct ArtistDhtConfigBuilder {
    data_dir: Option<PathBuf>,
    network_id: Option<NetworkId>,
    republish_interval: Option<Duration>,
    expire_interval: Option<Duration>,
    request_timeout: Option<Duration>,
    lookup_timeout: Option<Duration>,
    transport_timeout: Option<Duration>,
}

impl ArtistDhtConfigBuilder {
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

    /// Validates and builds the configuration.
    pub fn build(self) -> Result<ArtistDhtConfig> {
        let data_dir = self
            .data_dir
            .ok_or_else(|| ArtistDhtError::Database("data_dir is required".into()))?;
        if data_dir.as_os_str().is_empty() {
            return Err(ArtistDhtError::Database(
                "data_dir must not be empty".into(),
            ));
        }
        let network_id = self
            .network_id
            .ok_or_else(|| ArtistDhtError::Network("network_id is required".into()))?;

        let config = ArtistDhtConfig {
            data_dir,
            network_id,
            republish_interval: self
                .republish_interval
                .unwrap_or(DEFAULT_REPUBLISH_INTERVAL),
            expire_interval: self.expire_interval.unwrap_or(DEFAULT_EXPIRE_INTERVAL),
            request_timeout: self.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT),
            lookup_timeout: self.lookup_timeout.unwrap_or(DEFAULT_LOOKUP_TIMEOUT),
            transport_timeout: self.transport_timeout.unwrap_or(DEFAULT_TRANSPORT_TIMEOUT),
        };
        for (name, value) in [
            ("republish_interval", config.republish_interval),
            ("expire_interval", config.expire_interval),
            ("request_timeout", config.request_timeout),
            ("lookup_timeout", config.lookup_timeout),
            ("transport_timeout", config.transport_timeout),
        ] {
            if value.is_zero() {
                return Err(ArtistDhtError::Database(format!(
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
        let config = ArtistDhtConfig::builder()
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
        assert!(ArtistDhtConfig::builder().build().is_err());
        assert!(
            ArtistDhtConfig::builder()
                .data_dir("./dir")
                .network_id(NetworkId::from_name("test"))
                .request_timeout(Duration::ZERO)
                .build()
                .is_err()
        );
    }
}
