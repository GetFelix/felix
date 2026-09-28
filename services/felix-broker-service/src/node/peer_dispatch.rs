//! Routing a peer request to the half of this broker that answers it.
//!
//! Two halves, because a broker plays two roles at once and they are not the
//! same role for the same shard: it is the leader of some shards and a follower
//! of others. A forwarded publish is a request to *order* a record, and only a
//! leader may answer it; a replication batch is a request to *store* one
//! already ordered, and only a follower should. Keeping them apart is what
//! stops either check standing in for the other.
use async_trait::async_trait;
use felix_wire::internal::{ErrorCode, ForwardPublishError, InternalMessage};

use crate::serving::forward::owner::ForwardingHandler;
use felix_replication::peer::server::PeerRequestHandler;
use felix_replication::replica::ReplicaHandler;

/// Answers every request a peer may send this broker.
pub(crate) struct BrokerPeerHandler {
    forwarding: ForwardingHandler,
    replica: ReplicaHandler,
}

impl BrokerPeerHandler {
    pub(crate) fn new(forwarding: ForwardingHandler, replica: ReplicaHandler) -> Self {
        Self {
            forwarding,
            replica,
        }
    }
}

#[async_trait]
impl PeerRequestHandler for BrokerPeerHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        match request {
            InternalMessage::ForwardPublish(publish) => self.forwarding.apply(publish).await,
            InternalMessage::ForwardCacheOp(op) => self.forwarding.apply_cache_op(op).await,
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                self.replica
                    .apply(batch, felix_broker::LogKind::Stream)
                    .await
            }
            InternalMessage::ReplicateCacheRecords(batch) => {
                self.replica
                    .apply(batch, felix_broker::LogKind::Cache)
                    .await
            }
            InternalMessage::ReplicateGroupRecords(batch) => {
                self.replica
                    .apply(batch, felix_broker::LogKind::GroupCursors)
                    .await
            }
            InternalMessage::ReplicateDeadLetterRecords(batch) => {
                self.replica
                    .apply(batch, felix_broker::LogKind::GroupDeadLetters)
                    .await
            }
            InternalMessage::ReplicateCounterRecords(batch) => {
                self.replica
                    .apply(batch, felix_broker::LogKind::Counters)
                    .await
            }
            InternalMessage::ReplicateBootstrap(request) => {
                self.replica
                    .bootstrap(request, felix_broker::LogKind::Stream)
                    .await
            }
            InternalMessage::ReplicateCacheBootstrap(request) => {
                self.replica
                    .bootstrap(request, felix_broker::LogKind::Cache)
                    .await
            }
            InternalMessage::ReplicateGroupBootstrap(request) => {
                self.replica
                    .bootstrap(request, felix_broker::LogKind::GroupCursors)
                    .await
            }
            InternalMessage::ReplicateDeadLetterBootstrap(request) => {
                self.replica
                    .bootstrap(request, felix_broker::LogKind::GroupDeadLetters)
                    .await
            }
            InternalMessage::ReplicateCounterBootstrap(request) => {
                self.replica
                    .bootstrap(request, felix_broker::LogKind::Counters)
                    .await
            }
            InternalMessage::ReplicateRebuild(request) => self.replica.rebuild(request).await,
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
