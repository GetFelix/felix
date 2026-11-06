//! Publishing without a transport: resolve a target, then admit, append and
//! wait for the quorum.
//!
//! The publish worker runs the same tail ([`finish_claimed`]) the in-process
//! callers here do, so a simulated client and a QUIC one meet the same fence,
//! the same append ordering and the same quorum rules.

use std::time::Duration;

use bytes::Bytes;
use felix_broker::{Broker, ClaimedPublish, ConsistencyLevel, PublishOutcome, StreamHandle};
use felix_replication::quorum::QuorumMarks;

use super::route::{Authority, PublishRoute, publish_shard_at, resolve_route, shard_key_at};
use super::{PublishResult, StreamHandleCache};
use crate::cluster::lease::LeaseState;
use crate::serving::forward::ForwardTarget;
use crate::shards::ShardKey;
use crate::shards::lifecycle::fence::{self, FenceGuard};
use crate::shards::routing::IngressRouter;

/// A stream this broker serves, at the generation it was admitted under.
pub struct LocalTarget {
    pub handle: StreamHandle,
    /// `None` on a single-node broker, which has no replica set.
    pub shard: Option<ShardKey>,
    pub generation: u64,
    /// The write's place in the shard's fence, if routing entered it.
    pub fenced: Option<FenceGuard>,
}

/// Where a publish goes.
pub enum Resolution {
    /// This broker owns the shard.
    Local(LocalTarget),
    /// Another broker owns it; `owner.node_id` says which.
    Forward(ForwardTarget),
}

/// A publish refused before it reached the log.
#[derive(Debug, Clone)]
pub struct Refusal {
    pub code: felix_wire::ErrorCode,
    pub retry: felix_wire::RetryClass,
    pub message: String,
}

/// Why a publish did not return an offset.
#[derive(Debug)]
pub enum DirectPublishError {
    /// The fence refused the publish, or claiming its offsets failed, so no
    /// offset was consumed and it is safe to send again. A claim that failed
    /// on a disk write is the exception when the writer could not rewind
    /// the file either: bytes may remain. The simulator has no I/O faults, so
    /// it never sees that case.
    NotWritten(anyhow::Error),
    /// The batch was claimed, so it may be on disk and may yet reach a
    /// majority. A quorum timeout lands here, as do a flush failure and a lost
    /// leadership.
    OutcomeUnknown(anyhow::Error),
}

/// Resolve where a publish for `stream` goes, as the control stream does.
///
/// `named` is an explicit shard; otherwise `key` picks one. A stream cache is
/// not kept between calls.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_publish(
    broker: &Broker,
    ingress: Option<&IngressRouter>,
    lease: Option<&LeaseState>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    key: Option<&[u8]>,
    named: Option<u32>,
) -> Result<Resolution, Refusal> {
    let refuse = |err: super::super::super::client_error::ClientError| Refusal {
        code: err.code().clone(),
        retry: err.retry(),
        message: err.message().to_string(),
    };
    let shard =
        publish_shard_at(ingress, tenant_id, namespace, stream, key, named).map_err(refuse)?;
    let mut cache = StreamHandleCache::default();
    let mut scratch = String::new();
    let route = resolve_route(
        broker,
        Authority { ingress, lease },
        &mut cache,
        &mut scratch,
        tenant_id,
        namespace,
        stream,
        shard,
    )
    .await;
    match route {
        PublishRoute::Local {
            handle,
            generation,
            fenced,
        } => Ok(Resolution::Local(LocalTarget {
            handle,
            shard: shard_key_at(ingress, tenant_id, namespace, stream, shard),
            generation,
            fenced,
        })),
        PublishRoute::Forward(owner) => Ok(Resolution::Forward(owner)),
        PublishRoute::Refused(err) => Err(refuse(err)),
    }
}

/// Enter the fence, claim the batch's offsets and wait for it to be durable
/// and, on a `Quorum` stream, replicated. Returns the first record's offset.
///
/// The same steps the publish worker runs for a durable publish, in the same
/// order: the offsets are consumed after the fence and before any wait.
pub async fn publish_direct(
    broker: &Broker,
    ingress: Option<&IngressRouter>,
    marks: Option<&QuorumMarks>,
    target: LocalTarget,
    payloads: &[Bytes],
    publisher: Option<&Bytes>,
    quorum_timeout: Duration,
) -> Result<Option<u64>, DirectPublishError> {
    let LocalTarget {
        handle,
        shard,
        generation,
        fenced,
    } = target;
    let mut held = fenced;
    let fences = fence::enter_or_keep(&mut held, ingress, shard.as_ref(), generation)
        .map_err(|refused| DirectPublishError::NotWritten(refused.into()))?;
    let claimed = broker
        .claim_publish(&handle, payloads, publisher, None)
        .await
        .map_err(|err| DirectPublishError::NotWritten(err.into()))?;
    let mut answer = None;
    finish_claimed(
        broker,
        &handle,
        shard.as_ref(),
        claimed,
        fences,
        marks,
        ingress,
        quorum_timeout,
        |result| answer = Some(result),
    )
    .await;
    match answer {
        Some(Ok(first)) => Ok(first),
        Some(Err(err)) => Err(DirectPublishError::OutcomeUnknown(err)),
        None => Err(DirectPublishError::OutcomeUnknown(anyhow::anyhow!(
            "the publish finished without an answer"
        ))),
    }
}

/// Complete a claimed publish and hand its result to `answer`.
///
/// Anything but `Quorum` answers before the fanout, so the ack does not queue
/// behind waking every subscriber. A `Quorum` stream releases the fence once
/// the batch is committed, then waits for a majority before it answers.
/// `fences` is held until the batch is durable and fanned out.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finish_claimed(
    broker: &Broker,
    handle: &StreamHandle,
    shard: Option<&ShardKey>,
    claimed: ClaimedPublish,
    fences: impl Send,
    marks: Option<&QuorumMarks>,
    ingress: Option<&IngressRouter>,
    quorum_timeout: Duration,
    answer: impl FnOnce(PublishResult),
) {
    if handle.consistency() != ConsistencyLevel::Quorum {
        broker
            .complete_publish_answering(claimed, |completed| {
                answer(
                    completed
                        .map(|outcome| first_offset(&outcome))
                        .map_err(Into::into),
                );
            })
            .await;
        drop(fences);
        return;
    }
    let completed = broker.complete_publish(claimed).await;
    drop(fences);
    let result = match completed {
        // One wait for the last offset: the mark is monotonic, so it covers
        // every record in the claim.
        Ok(outcome) => felix_replication::quorum::await_quorum(
            handle,
            shard,
            &outcome,
            marks,
            ingress,
            quorum_timeout,
        )
        .await
        .map(|()| first_offset(&outcome)),
        Err(err) => Err(err.into()),
    };
    answer(result);
}

/// The offset a successful publish reports: its first record's, when the
/// stream has a log.
pub(super) fn first_offset(outcome: &PublishOutcome) -> Option<u64> {
    outcome.offsets.map(|(first, _)| first)
}
