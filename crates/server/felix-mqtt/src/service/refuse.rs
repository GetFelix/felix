//! What a connected client may do besides publish: ping and disconnect.
//! SUBSCRIBE is refused until the subscribe path exists.

use felix_mqtt_wire::{Packet, ReasonCode, SubAck, Subscribe, UnsubAck, Unsubscribe, Version};
use tokio::io::{AsyncRead, AsyncWrite};

use super::Shared;
use super::connect::Session;
use super::connection::{Connection, End};
use super::publish::Publishing;
use crate::metrics;

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Answer one packet from a connected client.
    pub(super) fn handle(
        &mut self,
        shared: &Shared,
        session: &Session,
        publishing: &mut Publishing<'_>,
        packet: Packet,
    ) -> Result<(), End> {
        let version = session.version;
        match packet {
            Packet::PingReq => self.queue(&Packet::PingResp),
            Packet::Disconnect(_) => Err(End::Gone),
            Packet::Publish(publish) => self.handle_publish(shared, session, publishing, publish),
            Packet::Subscribe(subscribe) => self.refuse_subscribe(&subscribe, version),
            Packet::Unsubscribe(unsubscribe) => self.answer_unsubscribe(&unsubscribe, version),
            // A second CONNECT, an AUTH nobody invited, an acknowledgement of
            // a message this listener never sent, or a packet that only a
            // server sends.
            _ => Err(End::Disconnect(
                ReasonCode::ProtocolError,
                Some("protocol_error"),
            )),
        }
    }

    fn refuse_subscribe(&mut self, subscribe: &Subscribe, version: Version) -> Result<(), End> {
        metrics::refused("subscribe_unsupported");
        let reason = match version {
            Version::V5 => ReasonCode::NotAuthorized,
            Version::V311 => ReasonCode::UnspecifiedError,
        };
        self.queue(&Packet::SubAck(SubAck {
            packet_id: subscribe.packet_id,
            reasons: vec![reason; subscribe.filters.len()],
            properties: Default::default(),
        }))
    }

    /// No subscription can exist yet, so there is nothing to remove.
    fn answer_unsubscribe(
        &mut self,
        unsubscribe: &Unsubscribe,
        version: Version,
    ) -> Result<(), End> {
        let reasons = match version {
            Version::V5 => vec![ReasonCode::NoSubscriptionExisted; unsubscribe.filters.len()],
            Version::V311 => Vec::new(),
        };
        self.queue(&Packet::UnsubAck(UnsubAck {
            packet_id: unsubscribe.packet_id,
            reasons,
            properties: Default::default(),
        }))
    }
}
