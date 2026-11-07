//! Routing a peer request to the half of this broker that answers it.
//!
//! Two halves, because a broker plays two roles at once and they are not the
//! same role for the same shard: it is the leader of some shards and a follower
//! of others. A forwarded publish is a request to *order* a record, and only a
//! leader may answer it; a replication batch is a request to *store* one
//! already ordered, and only a follower should. Keeping them apart is what
//! stops either check standing in for the other.
use std::sync::Arc;

use async_trait::async_trait;
use felix_wire::internal::{
    ErrorCode, ForwardCacheError, ForwardPublishError, InternalMessage, PeerCapabilities,
};
use tokio_util::sync::CancellationToken;

use crate::serving::forward::owner::ForwardingHandler;
use felix_replication::peer::server::PeerRequestHandler;
use felix_replication::replica::ReplicaHandler;

/// Answers every request a peer may send this broker.
pub(crate) struct BrokerPeerHandler {
    forwarding: ForwardingHandler,
    replica: ReplicaHandler,
    /// Until cancelled, a forwarded client request is refused as
    /// `Unavailable`, which the forwarder retries. Set only on a broker that
    /// started from its control-plane view: it serves no client until it has
    /// registered, as its own listeners do not.
    clients: Option<CancellationToken>,
}

impl BrokerPeerHandler {
    pub(crate) fn new(forwarding: ForwardingHandler, replica: ReplicaHandler) -> Self {
        Self {
            forwarding,
            replica,
            clients: None,
        }
    }

    /// Refuse forwarded client requests until `clients` is cancelled.
    pub(crate) fn serving_clients_after(mut self, clients: CancellationToken) -> Self {
        self.clients = Some(clients);
        self
    }

    /// Whether a forwarded client request must wait for registration.
    fn refusing_clients(&self) -> bool {
        self.clients
            .as_ref()
            .is_some_and(|clients| !clients.is_cancelled())
    }
}

/// Why a forwarded client request is refused before registration.
const NOT_REGISTERED: &str = "this broker has not registered with the control plane yet";

#[async_trait]
impl PeerRequestHandler for BrokerPeerHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        self.handle_from(None, PeerCapabilities::NONE, request)
            .await
    }

    async fn handle_from(
        &self,
        peer: Option<Arc<str>>,
        caller: PeerCapabilities,
        request: InternalMessage,
    ) -> InternalMessage {
        // The replica's ballot names a leader by this; forwarding does not
        // care who asked.
        let sender = peer.as_deref();
        match request {
            InternalMessage::ForwardPublish(publish) if self.refusing_clients() => {
                InternalMessage::ForwardPublishError(ForwardPublishError {
                    correlation_id: publish.correlation_id,
                    code: ErrorCode::Unavailable,
                    detail: NOT_REGISTERED.to_string(),
                })
            }
            InternalMessage::ForwardCacheOp(op) | InternalMessage::LeaseForwardCacheOp(op)
                if self.refusing_clients() =>
            {
                InternalMessage::ForwardCacheError(ForwardCacheError {
                    correlation_id: op.correlation_id,
                    code: ErrorCode::Unavailable,
                    detail: NOT_REGISTERED.to_string(),
                })
            }
            InternalMessage::ForwardPublish(publish) => self.forwarding.apply(publish).await,
            InternalMessage::ForwardCacheOp(op) => self.forwarding.apply_cache_op(op).await,
            InternalMessage::LeaseForwardCacheOp(op) => {
                self.forwarding.apply_lease_cache_op(op).await
            }
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                self.replica
                    .apply(sender, batch, felix_broker::LogKind::Stream)
                    .await
            }
            InternalMessage::ReplicateCacheRecords(batch) => {
                self.replica
                    .apply(sender, batch, felix_broker::LogKind::Cache)
                    .await
            }
            InternalMessage::ReplicateGroupRecords(batch) => {
                self.replica
                    .apply(sender, batch, felix_broker::LogKind::GroupCursors)
                    .await
            }
            InternalMessage::ReplicateDeadLetterRecords(batch) => {
                self.replica
                    .apply(sender, batch, felix_broker::LogKind::GroupDeadLetters)
                    .await
            }
            InternalMessage::ReplicateCounterRecords(batch) => {
                self.replica
                    .apply(sender, batch, felix_broker::LogKind::Counters)
                    .await
            }
            InternalMessage::ReplicateBootstrap(request) => {
                self.replica
                    .bootstrap(sender, request, felix_broker::LogKind::Stream)
                    .await
            }
            InternalMessage::ReplicateCacheBootstrap(request) => {
                self.replica
                    .bootstrap(sender, request, felix_broker::LogKind::Cache)
                    .await
            }
            InternalMessage::ReplicateGroupBootstrap(request) => {
                self.replica
                    .bootstrap(sender, request, felix_broker::LogKind::GroupCursors)
                    .await
            }
            InternalMessage::ReplicateDeadLetterBootstrap(request) => {
                self.replica
                    .bootstrap(sender, request, felix_broker::LogKind::GroupDeadLetters)
                    .await
            }
            InternalMessage::ReplicateCounterBootstrap(request) => {
                self.replica
                    .bootstrap(sender, request, felix_broker::LogKind::Counters)
                    .await
            }
            InternalMessage::ReplicateRebuild(request) => {
                self.replica.rebuild(sender, request).await
            }
            InternalMessage::Fence(request) => {
                self.replica.fence_from(sender, caller, request).await
            }
            InternalMessage::ReplicateFetch(request) => self.replica.fetch(sender, request).await,
            InternalMessage::PreVote(request) => self.replica.pre_vote(request).await,
            // Responses have no business arriving as requests, and a broker that
            // answered one would be inventing a request that was never made.
            other => InternalMessage::ForwardPublishError(ForwardPublishError {
                correlation_id: other.correlation_id(),
                code: ErrorCode::Malformed,
                detail: format!("{:?} is not a request", other.kind()),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
