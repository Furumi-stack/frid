//! Runtime publication of application and protocol versions.
//!
//! Capability manifests are informational. They let applications explain
//! interoperability problems and suggest an update, but do not authorize a
//! peer or trigger any update action.

use std::collections::BTreeMap;

use federation_net::ByteStream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{MusicDhtError, Result};

/// Auxiliary ALPN used to query a peer's version manifest.
pub const CAPABILITIES_ALPN: &[u8] = b"furumi/capabilities/1";
/// Capability protocol and manifest envelope version.
pub const CAPABILITIES_PROTOCOL_VERSION: u16 = 1;
/// Maximum accepted capability JSON line.
pub const MAX_CAPABILITIES_LINE: usize = 64 * 1024;
/// Maximum protocol entries accepted from one peer.
pub const MAX_PROTOCOL_ENTRIES: usize = 64;

/// Stable protocol identifier for federation-net.
pub const FEDERATION_NET_ID: &str = "federation_net";
/// Stable protocol identifier for endpoint tickets.
pub const TICKET_ID: &str = "ticket";
/// Stable protocol identifier for Mainline-DHT rendezvous records.
pub const RENDEZVOUS_ID: &str = "rendezvous";
/// Stable protocol identifier for music-dht.
pub const MUSIC_DHT_ID: &str = "music_dht";
/// Stable protocol identifier for rich catalog streams.
pub const CATALOG_ID: &str = "catalog";
/// Stable protocol identifier for music-similarity streams.
pub const SIMILARITY_ID: &str = "similarity";
/// Stable protocol identifier for DHT-routed similarity discovery.
pub const SIMILARITY_DHT_ID: &str = "similarity_dht";
/// Stable protocol identifier for the public podcast-discovery overlay.
pub const PODCAST_DHT_ID: &str = "podcast_dht";
/// Stable protocol identifier for personal-device synchronization.
pub const DEVICE_SYNC_ID: &str = "device_sync";
/// Stable protocol identifier for Jam playback control.
pub const JAM_ID: &str = "jam";

/// Application and protocol versions published by one peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    /// Manifest schema version.
    pub manifest_version: u16,
    /// Application family, for example `furumi`.
    pub application: String,
    /// User-visible application release.
    pub application_version: String,
    /// Supported/current protocol version by stable identifier.
    pub protocols: BTreeMap<String, u16>,
}

impl CapabilityManifest {
    /// Creates a manifest containing every protocol owned by Frid that a
    /// music-DHT application serves.
    ///
    /// The public podcast-discovery overlay is opt-in: applications that run
    /// [`crate::podcast_dht::PodcastDht`] add it with
    /// [`Self::with_podcast_dht`], so an application that does not serve the
    /// overlay never advertises it.
    pub fn frid(application: impl Into<String>, application_version: impl Into<String>) -> Self {
        let mut protocols = BTreeMap::new();
        protocols.insert(
            FEDERATION_NET_ID.to_string(),
            federation_net::PROTOCOL_VERSION,
        );
        protocols.insert(TICKET_ID.to_string(), federation_net::TICKET_VERSION);
        protocols.insert(
            RENDEZVOUS_ID.to_string(),
            federation_net::RENDEZVOUS_RECORD_VERSION,
        );
        protocols.insert(MUSIC_DHT_ID.to_string(), crate::DHT_PROTOCOL_VERSION);
        protocols.insert(
            CATALOG_ID.to_string(),
            crate::catalog::CATALOG_PROTOCOL_VERSION,
        );
        protocols.insert(
            SIMILARITY_ID.to_string(),
            crate::similarity::SIMILARITY_PROTOCOL_VERSION,
        );
        protocols.insert(
            SIMILARITY_DHT_ID.to_string(),
            crate::similarity_lsh::SIMILARITY_DHT_PROTOCOL_VERSION,
        );
        protocols.insert(
            DEVICE_SYNC_ID.to_string(),
            crate::device_sync::DEVICE_SYNC_PROTOCOL_VERSION,
        );
        protocols.insert(JAM_ID.to_string(), crate::jam::JAM_PROTOCOL_VERSION);
        Self {
            manifest_version: CAPABILITIES_PROTOCOL_VERSION,
            application: application.into(),
            application_version: application_version.into(),
            protocols,
        }
    }

    /// Adds an application-owned protocol to the manifest.
    pub fn with_protocol(mut self, id: impl Into<String>, version: u16) -> Self {
        self.protocols.insert(id.into(), version);
        self
    }

    /// Removes a shared protocol that this particular application does not
    /// expose, while retaining the canonical versions for the others.
    pub fn without_protocol(mut self, id: &str) -> Self {
        self.protocols.remove(id);
        self
    }

    /// Advertises the public podcast-discovery overlay
    /// ([`PODCAST_DHT_ID`] at [`crate::podcast_dht::PODCAST_DHT_PROTOCOL_VERSION`]).
    /// Call it only when the application serves
    /// [`crate::podcast_dht::PODCAST_DHT_ALPN`].
    pub fn with_podcast_dht(self) -> Self {
        self.with_protocol(
            PODCAST_DHT_ID,
            crate::podcast_dht::PODCAST_DHT_PROTOCOL_VERSION,
        )
    }

    /// Version of protocol `id` advertised by the peer, if any.
    pub fn protocol_version(&self, id: &str) -> Option<u16> {
        self.protocols.get(id).copied()
    }

    /// Whether the peer advertises the public podcast-discovery overlay at
    /// the version this crate speaks. Peers without it are never queried.
    pub fn supports_podcast_dht(&self) -> bool {
        self.protocol_version(PODCAST_DHT_ID)
            == Some(crate::podcast_dht::PODCAST_DHT_PROTOCOL_VERSION)
    }

    /// Validates bounds and identifiers received from a peer.
    pub fn validate(&self) -> Result<()> {
        if self.manifest_version != CAPABILITIES_PROTOCOL_VERSION {
            return Err(protocol_error(format!(
                "unsupported capability manifest version {}",
                self.manifest_version
            )));
        }
        if self.application.trim().is_empty()
            || self.application.len() > 64
            || self.application_version.len() > 64
            || self.protocols.len() > MAX_PROTOCOL_ENTRIES
        {
            return Err(protocol_error("invalid capability manifest bounds"));
        }
        for (id, version) in &self.protocols {
            if id.is_empty()
                || id.len() > 64
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                || *version == 0
            {
                return Err(protocol_error("invalid capability protocol entry"));
            }
        }
        Ok(())
    }
}

/// One request/response message on [`CAPABILITIES_ALPN`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CapabilityMessage {
    /// Requests the peer's current manifest.
    Get {
        /// Request protocol version.
        version: u16,
    },
    /// Returns a manifest.
    Manifest {
        /// Published version manifest.
        manifest: CapabilityManifest,
    },
    /// Refuses a malformed or unsupported request.
    Error {
        /// Human-readable diagnostic.
        message: String,
    },
}

/// Writes one bounded JSON-lines capability message.
pub async fn write_message(stream: &mut ByteStream, message: &CapabilityMessage) -> Result<()> {
    let mut bytes = serde_json::to_vec(message).map_err(protocol_error)?;
    if bytes.len() > MAX_CAPABILITIES_LINE {
        return Err(protocol_error("capability message is too large"));
    }
    bytes.push(b'\n');
    stream.send.write_all(&bytes).await.map_err(network_error)?;
    Ok(())
}

/// Reads one bounded JSON-lines capability message.
pub async fn read_message(stream: &mut ByteStream) -> Result<CapabilityMessage> {
    read_message_from(&mut stream.recv).await
}

/// Reads one bounded JSON-lines capability message from an async reader.
pub async fn read_message_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<CapabilityMessage> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = reader.read(&mut byte).await.map_err(network_error)?;
        if read == 0 || byte[0] == b'\n' {
            break;
        }
        bytes.push(byte[0]);
        if bytes.len() > MAX_CAPABILITIES_LINE {
            return Err(protocol_error("capability message is too large"));
        }
    }
    if bytes.is_empty() {
        return Err(protocol_error("capability message is empty"));
    }
    let message: CapabilityMessage = serde_json::from_slice(&bytes).map_err(protocol_error)?;
    if let CapabilityMessage::Manifest { manifest } = &message {
        manifest.validate()?;
    }
    Ok(message)
}

fn protocol_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(error.to_string())
}

fn network_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Network(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frid_manifest_contains_every_shared_protocol() {
        let manifest = CapabilityManifest::frid("furumi", "1.2.3");
        for id in [
            FEDERATION_NET_ID,
            TICKET_ID,
            RENDEZVOUS_ID,
            MUSIC_DHT_ID,
            CATALOG_ID,
            SIMILARITY_ID,
            SIMILARITY_DHT_ID,
            DEVICE_SYNC_ID,
            JAM_ID,
        ] {
            assert!(manifest.protocols.contains_key(id), "missing {id}");
        }
        manifest.validate().unwrap();
    }

    #[test]
    fn podcast_dht_capability_is_opt_in_and_removable() {
        let plain = CapabilityManifest::frid("furumi", "1.2.3");
        assert_eq!(plain.protocol_version(PODCAST_DHT_ID), None);
        assert!(!plain.supports_podcast_dht());
        let manifest = plain.with_podcast_dht();
        assert_eq!(
            manifest.protocol_version(PODCAST_DHT_ID),
            Some(crate::podcast_dht::PODCAST_DHT_PROTOCOL_VERSION)
        );
        assert!(manifest.supports_podcast_dht());
        manifest.validate().unwrap();
        let without = manifest.clone().without_protocol(PODCAST_DHT_ID);
        assert!(!without.supports_podcast_dht());
        let other_version = manifest.with_protocol(PODCAST_DHT_ID, 2);
        assert!(!other_version.supports_podcast_dht());
    }

    #[test]
    fn application_protocol_is_additive() {
        let manifest = CapabilityManifest::frid("furumi", "1.2.3").with_protocol("audio", 1);
        assert_eq!(manifest.protocols.get("audio"), Some(&1));
    }

    #[test]
    fn invalid_protocol_identifier_is_rejected() {
        let manifest = CapabilityManifest::frid("furumi", "1.2.3").with_protocol("Audio Stream", 1);
        assert!(manifest.validate().is_err());
    }
}
