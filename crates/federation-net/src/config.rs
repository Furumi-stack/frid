//! Engine configuration.

use std::path::PathBuf;
use std::time::Duration;

use crate::error::{NetworkError, Result};
use crate::protocol::{NetworkId, SchemaId};
use crate::rendezvous::RendezvousConfig;

/// Default maximum size of an encoded domain message (256 KiB).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 256 * 1024;
/// Default timeout applied to handshakes and message round-trips.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Default capacity of the event channel.
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 1024;
/// Default limit of concurrently processed streams per peer.
pub const DEFAULT_MAX_CONCURRENT_STREAMS_PER_PEER: usize = 64;

/// Configuration for a [`crate::NetworkEngine`].
///
/// Use [`NetworkConfig::builder`] to construct a validated instance.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Directory where the persistent peer identity is stored.
    pub data_dir: PathBuf,
    /// Identifier of the network this peer participates in.
    pub network_id: NetworkId,
    /// Identifier of the domain message schema.
    pub schema_id: SchemaId,
    /// Maximum size in bytes of a single encoded domain message.
    pub max_message_size: usize,
    /// Timeout applied to handshakes and message round-trips.
    pub request_timeout: Duration,
    /// Capacity of the bounded event channel.
    pub event_channel_capacity: usize,
    /// Maximum number of concurrently processed streams per peer.
    pub max_concurrent_streams_per_peer: usize,
    /// Automatic peer discovery over the mainline DHT; `None` disables it and
    /// peers are connected via tickets only.
    pub rendezvous: Option<RendezvousConfig>,
}

impl NetworkConfig {
    /// Returns a new [`NetworkConfigBuilder`].
    pub fn builder() -> NetworkConfigBuilder {
        NetworkConfigBuilder::default()
    }
}

/// Builder for [`NetworkConfig`].
///
/// `data_dir`, `network_id` and `schema_id` are required; everything else has
/// sensible defaults.
#[derive(Debug, Default, Clone)]
pub struct NetworkConfigBuilder {
    data_dir: Option<PathBuf>,
    network_id: Option<NetworkId>,
    schema_id: Option<SchemaId>,
    max_message_size: Option<usize>,
    request_timeout: Option<Duration>,
    event_channel_capacity: Option<usize>,
    max_concurrent_streams_per_peer: Option<usize>,
    rendezvous: Option<RendezvousConfig>,
}

impl NetworkConfigBuilder {
    /// Sets the directory where the persistent peer identity is stored.
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }

    /// Sets the network identifier.
    pub fn network_id(mut self, network_id: NetworkId) -> Self {
        self.network_id = Some(network_id);
        self
    }

    /// Sets the schema identifier.
    pub fn schema_id(mut self, schema_id: SchemaId) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Sets the maximum size in bytes of a single encoded domain message.
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Sets the timeout applied to handshakes and message round-trips.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Sets the capacity of the bounded event channel.
    pub fn event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = Some(capacity);
        self
    }

    /// Sets the maximum number of concurrently processed streams per peer.
    pub fn max_concurrent_streams_per_peer(mut self, limit: usize) -> Self {
        self.max_concurrent_streams_per_peer = Some(limit);
        self
    }

    /// Enables automatic peer discovery over the mainline DHT.
    pub fn rendezvous(mut self, rendezvous: RendezvousConfig) -> Self {
        self.rendezvous = Some(rendezvous);
        self
    }

    /// Validates the configuration and builds a [`NetworkConfig`].
    pub fn build(self) -> Result<NetworkConfig> {
        let data_dir = self
            .data_dir
            .ok_or_else(|| NetworkError::InvalidConfig("data_dir is required".into()))?;
        let network_id = self
            .network_id
            .ok_or_else(|| NetworkError::InvalidConfig("network_id is required".into()))?;
        let schema_id = self
            .schema_id
            .ok_or_else(|| NetworkError::InvalidConfig("schema_id is required".into()))?;

        if data_dir.as_os_str().is_empty() {
            return Err(NetworkError::InvalidConfig(
                "data_dir must not be empty".into(),
            ));
        }

        let max_message_size = self.max_message_size.unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        if max_message_size == 0 {
            return Err(NetworkError::InvalidConfig(
                "max_message_size must be greater than zero".into(),
            ));
        }
        if max_message_size > u32::MAX as usize {
            return Err(NetworkError::InvalidConfig(
                "max_message_size must fit into a 32-bit length prefix".into(),
            ));
        }

        let request_timeout = self.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        if request_timeout.is_zero() {
            return Err(NetworkError::InvalidConfig(
                "request_timeout must be greater than zero".into(),
            ));
        }

        let event_channel_capacity = self
            .event_channel_capacity
            .unwrap_or(DEFAULT_EVENT_CHANNEL_CAPACITY);
        if event_channel_capacity == 0 {
            return Err(NetworkError::InvalidConfig(
                "event_channel_capacity must be greater than zero".into(),
            ));
        }

        let max_concurrent_streams_per_peer = self
            .max_concurrent_streams_per_peer
            .unwrap_or(DEFAULT_MAX_CONCURRENT_STREAMS_PER_PEER);
        if max_concurrent_streams_per_peer == 0 {
            return Err(NetworkError::InvalidConfig(
                "max_concurrent_streams_per_peer must be greater than zero".into(),
            ));
        }

        if let Some(rendezvous) = &self.rendezvous {
            if rendezvous.interval.is_zero() {
                return Err(NetworkError::InvalidConfig(
                    "rendezvous interval must be greater than zero".into(),
                ));
            }
            if rendezvous.entry_ttl.is_zero() {
                return Err(NetworkError::InvalidConfig(
                    "rendezvous entry_ttl must be greater than zero".into(),
                ));
            }
        }

        Ok(NetworkConfig {
            data_dir,
            network_id,
            schema_id,
            max_message_size,
            request_timeout,
            event_channel_capacity,
            max_concurrent_streams_per_peer,
            rendezvous: self.rendezvous,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_builder() -> NetworkConfigBuilder {
        NetworkConfig::builder()
            .data_dir("./some-dir")
            .network_id(NetworkId::from_name("test-network"))
            .schema_id(SchemaId::from_name("test-schema"))
    }

    #[test]
    fn builder_applies_defaults() {
        let config = base_builder().build().expect("valid config");
        assert_eq!(config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(
            config.event_channel_capacity,
            DEFAULT_EVENT_CHANNEL_CAPACITY
        );
        assert_eq!(
            config.max_concurrent_streams_per_peer,
            DEFAULT_MAX_CONCURRENT_STREAMS_PER_PEER
        );
    }

    #[test]
    fn builder_rejects_missing_required_fields() {
        assert!(NetworkConfig::builder().build().is_err());
        assert!(
            NetworkConfig::builder()
                .data_dir("./dir")
                .network_id(NetworkId::from_name("n"))
                .build()
                .is_err()
        );
    }

    #[test]
    fn builder_rejects_invalid_values() {
        assert!(base_builder().max_message_size(0).build().is_err());
        assert!(
            base_builder()
                .request_timeout(Duration::ZERO)
                .build()
                .is_err()
        );
        assert!(base_builder().event_channel_capacity(0).build().is_err());
        assert!(
            base_builder()
                .max_concurrent_streams_per_peer(0)
                .build()
                .is_err()
        );
    }

    #[test]
    fn rendezvous_is_disabled_by_default_and_validated_when_set() {
        let config = base_builder().build().expect("valid config");
        assert!(config.rendezvous.is_none());

        let config = base_builder()
            .rendezvous(RendezvousConfig::default())
            .build()
            .expect("valid config");
        assert!(config.rendezvous.is_some());

        let zero_interval = RendezvousConfig {
            interval: Duration::ZERO,
            ..RendezvousConfig::default()
        };
        assert!(base_builder().rendezvous(zero_interval).build().is_err());

        let zero_ttl = RendezvousConfig {
            entry_ttl: Duration::ZERO,
            ..RendezvousConfig::default()
        };
        assert!(base_builder().rendezvous(zero_ttl).build().is_err());
    }
}
