//! Raft messages over the broker transport: a [`RaftTransport`] that sends
//! through the peer pool, and the handler a peer listener answers them with.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use felix_wire::internal::{InternalMessage, PeerCapabilities, RaftCall};

use super::network::{RaftReply, RaftRequest, RaftTransport, Refused};
use super::runtime::GroupRuntime;
use super::wire;
use crate::peer::{PeerRequester, RaftHandler};

/// Where each broker's internal listener is.
pub trait PeerAddresses: Send + Sync + 'static {
    /// `None` for a broker this one does not know, which is not sent to.
    fn address(&self, broker: &str) -> Option<SocketAddr>;
}

impl<F> PeerAddresses for F
where
    F: Fn(&str) -> Option<SocketAddr> + Send + Sync + 'static,
{
    fn address(&self, broker: &str) -> Option<SocketAddr> {
        self(broker)
    }
}

/// Sends Raft messages to peer brokers over their internal listeners.
///
/// One per broker, shared by every group. A message goes only to a broker
/// that offered [`PeerCapabilities::RAFT`] in its handshake; for any other
/// the answer is [`Refused::Unreachable`] and nothing is sent.
pub struct PeerTransport<R> {
    requester: R,
    addresses: Arc<dyn PeerAddresses>,
}

impl<R> PeerTransport<R> {
    /// `requester` is normally the broker's `Arc<PeerPool>`.
    pub fn new(requester: R, addresses: Arc<dyn PeerAddresses>) -> Self {
        Self {
            requester,
            addresses,
        }
    }
}

#[async_trait]
impl<R> RaftTransport for PeerTransport<R>
where
    R: PeerRequester + Send + Sync + 'static,
{
    async fn send(&self, mut request: RaftRequest) -> Result<RaftReply, Refused> {
        let broker = Arc::clone(&request.to().broker);
        let unreachable =
            |detail: String| Refused::Unreachable(format!("broker {broker}: {detail}"));
        let addr = self
            .addresses
            .address(&broker)
            .ok_or_else(|| unreachable("no address is known".into()))?;
        // Checked here as well as in the pool, so a requester that does not
        // check cannot send one either.
        let offered = self
            .requester
            .capabilities(&broker, addr)
            .await
            .map_err(|err| unreachable(err.to_string()))?;
        if !offered.contains(PeerCapabilities::RAFT) {
            return Err(unreachable("it has not offered Raft".into()));
        }
        wire::fit(&mut request);
        let body = wire::encode_request(&request).map_err(|err| unreachable(err.to_string()))?;
        let call = InternalMessage::Raft(RaftCall {
            correlation_id: 0,
            body,
        });
        match self.requester.request(&broker, addr, call).await {
            Ok(InternalMessage::RaftAnswer(answer)) => wire::decode_reply(answer.body)
                .map_err(|err| unreachable(format!("its answer does not read: {err}")))?,
            // Overloaded, or a refusal from a build that does not know the
            // kind. Raft sends again later either way.
            Ok(other) => Err(unreachable(format!("it answered {:?}", other.kind()))),
            Err(err) => Err(unreachable(err.to_string())),
        }
    }
}

#[async_trait]
impl RaftHandler for GroupRuntime {
    async fn answer(&self, body: Bytes) -> Bytes {
        let reply = match wire::decode_request(body) {
            Ok(request) => self.handle(request).await,
            Err(err) => Err(Refused::Unreachable(format!(
                "the message does not read: {err}"
            ))),
        };
        wire::encode_reply(&reply)
    }
}

#[cfg(test)]
mod tests;
