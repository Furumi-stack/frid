//! Peer tickets: self-contained connection invitations.

use std::fmt;
use std::str::FromStr;

use iroh::{EndpointAddr, EndpointId};
use iroh_tickets::{ParseError, Ticket};
use serde::{Deserialize, Serialize};

use crate::protocol::{NetworkId, SchemaId};

/// Current version of the ticket wire format.
pub const TICKET_VERSION: u16 = 1;

/// Maximum size of a decoded ticket in bytes.
const MAX_TICKET_BYTES: usize = 8 * 1024;
/// Maximum length of a ticket string accepted by [`PeerTicket::from_str`].
const MAX_TICKET_STRING_LEN: usize = 16 * 1024;

/// A shareable invitation to connect to a peer.
///
/// The ticket contains everything needed to dial the peer over Iroh, plus the
/// network and schema identifiers so incompatibility is detected before any
/// domain message is exchanged.
///
/// Tickets serialize to a string with the `fnet` prefix via [`fmt::Display`]
/// and parse back via [`FromStr`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerTicket {
    /// Version of the ticket format itself.
    pub ticket_version: u16,
    /// Version of the application-level wire protocol the peer speaks.
    pub protocol_version: u16,
    /// Network the issuing peer participates in.
    pub network_id: NetworkId,
    /// Message schema the issuing peer uses.
    pub schema_id: SchemaId,
    /// Iroh address of the issuing peer.
    pub endpoint_addr: EndpointAddr,
}

impl PeerTicket {
    /// Returns the endpoint id of the peer this ticket points to.
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint_addr.id
    }
}

/// Versioned body of the ticket; everything after the leading version number.
#[derive(Serialize, Deserialize)]
struct TicketBody {
    protocol_version: u16,
    network_id: NetworkId,
    schema_id: SchemaId,
    endpoint_addr: EndpointAddr,
}

impl Ticket for PeerTicket {
    const KIND: &'static str = "fnet";

    fn encode_bytes(&self) -> Vec<u8> {
        let body = TicketBody {
            protocol_version: self.protocol_version,
            network_id: self.network_id,
            schema_id: self.schema_id,
            endpoint_addr: self.endpoint_addr.clone(),
        };
        // Serializing plain owned data into a growable Vec is infallible;
        // postcard only errors on unsupported types or fixed-size buffers.
        postcard::to_stdvec(&(self.ticket_version, body))
            .expect("postcard serialization of a ticket into a Vec cannot fail")
    }

    fn decode_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        if bytes.len() > MAX_TICKET_BYTES {
            return Err(ParseError::verification_failed(
                "ticket exceeds the maximum allowed size",
            ));
        }
        let (ticket_version, rest) = postcard::take_from_bytes::<u16>(bytes)?;
        if ticket_version != TICKET_VERSION {
            return Err(ParseError::verification_failed(
                "unsupported ticket version",
            ));
        }
        let body: TicketBody = postcard::from_bytes(rest)?;
        Ok(Self {
            ticket_version,
            protocol_version: body.protocol_version,
            network_id: body.network_id,
            schema_id: body.schema_id,
            endpoint_addr: body.endpoint_addr,
        })
    }
}

impl fmt::Display for PeerTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&Ticket::encode_string(self))
    }
}

impl FromStr for PeerTicket {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > MAX_TICKET_STRING_LEN {
            return Err(ParseError::verification_failed(
                "ticket string exceeds the maximum allowed length",
            ));
        }
        Ticket::decode_string(s)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use iroh::SecretKey;

    use super::*;

    fn sample_ticket() -> PeerTicket {
        let endpoint_id = SecretKey::generate().public();
        let addr = EndpointAddr::new(endpoint_id)
            .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 4242)));
        PeerTicket {
            ticket_version: TICKET_VERSION,
            protocol_version: 1,
            network_id: NetworkId::from_name("test-network"),
            schema_id: SchemaId::from_name("test-schema"),
            endpoint_addr: addr,
        }
    }

    #[test]
    fn ticket_string_round_trip() {
        let ticket = sample_ticket();
        let encoded = ticket.to_string();
        assert!(encoded.starts_with("fnet"));
        let decoded: PeerTicket = encoded.parse().expect("parse ticket");
        assert_eq!(decoded, ticket);
        // The canonical string form must be stable across round-trips.
        assert_eq!(decoded.to_string(), encoded);
    }

    #[test]
    fn corrupted_ticket_is_rejected() {
        let ticket = sample_ticket();
        let mut encoded = ticket.to_string();
        // Truncate the payload; the result must not parse.
        encoded.truncate(encoded.len() - 10);
        assert!(encoded.parse::<PeerTicket>().is_err());

        // Corrupt the alphabet: '!' is not valid base32.
        let corrupted = format!("fnet!{}", &ticket.to_string()[5..]);
        assert!(corrupted.parse::<PeerTicket>().is_err());
    }

    #[test]
    fn wrong_prefix_is_rejected() {
        let ticket = sample_ticket();
        let encoded = ticket.to_string();
        let renamed = format!("blob{}", &encoded[4..]);
        assert!(renamed.parse::<PeerTicket>().is_err());
    }

    #[test]
    fn unknown_ticket_version_is_rejected() {
        let ticket = sample_ticket();
        let body = TicketBody {
            protocol_version: ticket.protocol_version,
            network_id: ticket.network_id,
            schema_id: ticket.schema_id,
            endpoint_addr: ticket.endpoint_addr.clone(),
        };
        let bytes = postcard::to_stdvec(&(99u16, body)).expect("encode");
        let mut encoded = String::from("fnet");
        data_encoding::BASE32_NOPAD.encode_append(&bytes, &mut encoded);
        encoded.make_ascii_lowercase();
        assert!(encoded.parse::<PeerTicket>().is_err());
    }

    #[test]
    fn oversized_ticket_is_rejected() {
        let bytes = vec![0u8; MAX_TICKET_BYTES + 1];
        assert!(PeerTicket::decode_bytes(&bytes).is_err());

        let huge = "fnet".to_string() + &"a".repeat(MAX_TICKET_STRING_LEN);
        assert!(huge.parse::<PeerTicket>().is_err());
    }
}
