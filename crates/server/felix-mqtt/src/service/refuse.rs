//! What a connected client may do today: ping and disconnect. Everything
//! that would move a message is refused, visibly, until the publish and
//! subscribe paths exist.

use felix_mqtt_wire::{
    Ack, Packet, Publish, QoS, ReasonCode, SubAck, Subscribe, UnsubAck, Unsubscribe, Version,
};
use tokio::io::{AsyncRead, AsyncWrite};

use super::connection::{Connection, End};
use crate::metrics;

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Answer one packet from a connected client.
    pub(super) fn handle(&mut self, packet: Packet, version: Version) -> Result<(), End> {
        match packet {
            Packet::PingReq => self.queue(&Packet::PingResp),
            Packet::Disconnect(_) => Err(End::Gone),
            Packet::Publish(publish) => self.refuse_publish(&publish, version),
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

    /// A 5.0 client is told per message with a reason on the acknowledgement;
    /// 3.1.1 has no way to say no, so that connection ends, as the
    /// specification asks of a server that cannot accept a `PUBLISH`.
    fn refuse_publish(&mut self, publish: &Publish, version: Version) -> Result<(), End> {
        metrics::refused("publish_unsupported");
        let (Version::V5, Some(packet_id)) = (version, publish.packet_id) else {
            return Err(End::Disconnect(ReasonCode::NotAuthorized, None));
        };
        let mut ack = Ack::new(packet_id);
        ack.reason = ReasonCode::NotAuthorized;
        match publish.qos {
            QoS::AtLeastOnce => self.queue(&Packet::PubAck(ack)),
            QoS::ExactlyOnce => self.queue(&Packet::PubRec(ack)),
            QoS::AtMostOnce => Err(End::Disconnect(ReasonCode::NotAuthorized, None)),
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
