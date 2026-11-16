//! What a connected client may do: the packet dispatch. A SUBSCRIBE is
//! refused when the broker has no reads to serve it from.

use felix_mqtt_wire::{Packet, ReasonCode, SubAck, Subscribe, Version};
use tokio::io::{AsyncRead, AsyncWrite};

use super::Shared;
use super::connect::Session;
use super::connection::{Connection, End};
use super::publish::Publishing;
use super::subscribe::Subscriptions;
use crate::metrics;

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Answer one packet from a connected client.
    pub(super) async fn handle(
        &mut self,
        shared: &Shared,
        session: &Session,
        publishing: &mut Publishing<'_>,
        subs: &mut Subscriptions,
        packet: Packet,
    ) -> Result<(), End> {
        let version = session.version;
        match packet {
            Packet::PingReq => self.queue(&Packet::PingResp),
            Packet::Disconnect(_) => Err(End::Gone),
            Packet::Publish(publish) => self.handle_publish(shared, session, publishing, publish),
            Packet::Subscribe(subscribe) if !shared.reads.available() => {
                self.refuse_subscribe(&subscribe, version)
            }
            Packet::Subscribe(subscribe) => {
                self.handle_subscribe(shared, session, subs, subscribe)
                    .await
            }
            Packet::Unsubscribe(unsubscribe) => {
                self.handle_unsubscribe(subs, version, &unsubscribe)
            }
            Packet::PubAck(ack) => self.handle_puback(subs, ack),
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
}
