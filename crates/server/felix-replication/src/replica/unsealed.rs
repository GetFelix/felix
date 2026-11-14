//! A build without Raft takes part in no seal: it refuses the seal's
//! messages, and no copy of it ever holds a promise.

use felix_wire::internal::{ErrorCode, InternalMessage, SealAck, SealFence, ShardRef};

use super::{ReplicaHandler, refused};

impl ReplicaHandler {
    pub(crate) async fn seal_fence(&self, _: Option<&str>, request: SealFence) -> InternalMessage {
        unsealed(request.fence.correlation_id)
    }

    pub(crate) async fn seal_ack(&self, _: Option<&str>, request: SealAck) -> InternalMessage {
        unsealed(request.correlation_id)
    }

    pub(crate) async fn sealed_refusal(
        &self,
        _: u64,
        _: &ShardRef,
        _: Option<felix_storage::StorageId>,
        _: SealedWrite,
    ) -> Option<InternalMessage> {
        None
    }
}

/// What a sealed copy is asked to store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SealedWrite {
    Batch,
    Rebuild,
    Ballot,
}

pub(crate) fn seal_refusal(correlation_id: u64, _: &std::convert::Infallible) -> InternalMessage {
    unsealed(correlation_id)
}

fn unsealed(correlation_id: u64) -> InternalMessage {
    refused(
        correlation_id,
        ErrorCode::UnsupportedKind,
        0,
        "this build does not take part in the switch to Raft".to_string(),
    )
}
