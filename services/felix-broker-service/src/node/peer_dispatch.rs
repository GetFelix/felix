//! Routing a peer request to the half of this broker that answers it.
//!
//! Two halves (a forwarding handler and the replication engine), because a
//! broker plays two roles at once and they are not the same role for the same
//! shard: it is the leader of some shards and a follower of others. A forwarded publish is a request to *order* a record, and only a
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
use felix_replication::engine::Engine;
use felix_replication::peer::server::PeerRequestHandler;

/// Answers every request a peer may send this broker.
pub(crate) struct BrokerPeerHandler<E> {
    forwarding: ForwardingHandler,
    /// Answers the replication requests; the rest are forwarded writes.
    engine: Arc<E>,
    /// Until cancelled, a forwarded client request is refused as
    /// `Unavailable`, which the forwarder retries. Set only on a broker that
    /// started from its control-plane view: it serves no client until it has
    /// registered, as its own listeners do not.
    clients: Option<CancellationToken>,
    /// Takes peers' store-liveness heartbeats, on a broker that runs it.
    liveness: super::store_liveness::StoreLiveness,
}

impl<E> BrokerPeerHandler<E> {
    pub(crate) fn new(forwarding: ForwardingHandler, engine: Arc<E>) -> Self {
        Self {
            forwarding,
            engine,
            clients: None,
            liveness: Default::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn engine(&self) -> &Arc<E> {
        &self.engine
    }

    #[cfg(test)]
    pub(crate) fn forwarding(&self) -> &ForwardingHandler {
        &self.forwarding
    }

    /// Refuse forwarded client requests until `clients` is cancelled.
    pub(crate) fn serving_clients_after(mut self, clients: CancellationToken) -> Self {
        self.clients = Some(clients);
        self
    }

    /// Hand peers' store-liveness heartbeats to `liveness`.
    pub(crate) fn with_store_liveness(
        mut self,
        liveness: super::store_liveness::StoreLiveness,
    ) -> Self {
        self.liveness = liveness;
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
impl<E: Engine + 'static> PeerRequestHandler for BrokerPeerHandler<E> {
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
            request @ (InternalMessage::ReplicateRecords(_)
            | InternalMessage::ReplicateMarkedRecords(_)
            | InternalMessage::ReplicateCacheRecords(_)
            | InternalMessage::ReplicateGroupRecords(_)
            | InternalMessage::ReplicateDeadLetterRecords(_)
            | InternalMessage::ReplicateCounterRecords(_)
            | InternalMessage::ReplicateBootstrap(_)
            | InternalMessage::ReplicateCacheBootstrap(_)
            | InternalMessage::ReplicateGroupBootstrap(_)
            | InternalMessage::ReplicateDeadLetterBootstrap(_)
            | InternalMessage::ReplicateCounterBootstrap(_)
            | InternalMessage::ReplicateRebuild(_)
            | InternalMessage::Fence(_)
            | InternalMessage::ReplicateFetch(_)
            | InternalMessage::PreVote(_)) => self.engine.handle(sender, caller, request).await,
            // Shadow mode: recorded and answered, and nothing else reads it.
            InternalMessage::StoreLiveness(beat) => {
                let correlation_id = beat.correlation_id;
                self.liveness.heard(sender, beat).unwrap_or_else(|| {
                    InternalMessage::ForwardPublishError(ForwardPublishError {
                        correlation_id,
                        code: ErrorCode::UnsupportedKind,
                        detail: "this broker does not run store liveness".to_string(),
                    })
                })
            }
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
