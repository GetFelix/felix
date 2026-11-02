//! What a connected client may do today: ping and disconnect. A PUBLISH is
//! resolved to its stream and then refused, visibly, until the publish path
//! exists; SUBSCRIBE is refused until the subscribe path does.

use felix_mqtt_wire::{
    Ack, Packet, Publish, QoS, ReasonCode, SubAck, Subscribe, UnsubAck, Unsubscribe, Version,
};
use tokio::io::{AsyncRead, AsyncWrite};

use super::Shared;
use super::connect::Session;
use super::connection::{Connection, End};
use crate::metrics;
use crate::topic::check_topic_name;

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Answer one packet from a connected client.
    pub(super) fn handle(
        &mut self,
        shared: &Shared,
        session: &Session,
        packet: Packet,
    ) -> Result<(), End> {
        let version = session.version;
        match packet {
            Packet::PingReq => self.queue(&Packet::PingResp),
            Packet::Disconnect(_) => Err(End::Gone),
            Packet::Publish(publish) => {
                self.handle_publish(shared, session.principal.tenant_id(), &publish, version)
            }
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

    /// Resolve the topic, then refuse: a topic under no mount is the
    /// client's mistake, and one under a mount waits for the publish path.
    fn handle_publish(
        &mut self,
        shared: &Shared,
        tenant_id: &str,
        publish: &Publish,
        version: Version,
    ) -> Result<(), End> {
        let topic = publish.topic.as_str();
        if check_topic_name(topic).is_err() {
            return Err(End::Disconnect(
                ReasonCode::TopicNameInvalid,
                Some("invalid_topic_name"),
            ));
        }
        match shared.mounts.resolve(tenant_id, topic) {
            Some(_) => self.refuse_publish(publish, version),
            None => self.refuse_unmounted(publish, version),
        }
    }

    /// A topic under no mount has nowhere to go. A 5.0 client with an
    /// acknowledgement to carry it is told `TopicNameInvalid` and keeps its
    /// connection; one without (QoS 0) is disconnected with it, and 3.1.1 has
    /// no way to say no, so that connection ends.
    fn refuse_unmounted(&mut self, publish: &Publish, version: Version) -> Result<(), End> {
        let (Version::V5, Some(packet_id)) = (version, publish.packet_id) else {
            return Err(End::Disconnect(
                ReasonCode::TopicNameInvalid,
                Some("unmounted"),
            ));
        };
        metrics::refused("unmounted");
        let mut ack = Ack::new(packet_id);
        ack.reason = ReasonCode::TopicNameInvalid;
        match publish.qos {
            QoS::AtLeastOnce => self.queue(&Packet::PubAck(ack)),
            QoS::ExactlyOnce => self.queue(&Packet::PubRec(ack)),
            QoS::AtMostOnce => Err(End::Disconnect(ReasonCode::TopicNameInvalid, None)),
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
