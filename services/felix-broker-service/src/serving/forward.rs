//! Forwarding a publish to the broker that owns its shard.
//!
//! # What may be retried, and what may not
//!
//! The rule is one question: *could the owner already have applied this batch?*
//! If it could, a retry is a duplicate rather than a repair, and this broker has
//! no way to tell the two apart.
//!
//! - **Nothing was sent** — the peer was shed, in backoff, or unreachable.
//!   Retryable.
//! - **The owner refused** — `NotLeader`, `StaleRoute`, `Unavailable`,
//!   `Overload`. The refusal *is* the evidence nothing was applied. Retryable,
//!   and `NotLeader` carries where to go instead.
//! - **The answer was lost** — the connection dropped, or the request timed out.
//!   The batch may be on the owner's disk. **Never retried here**, for a stream
//!   of any delivery guarantee.
//!
//! That last rule is stricter than `AtLeastOnce` requires. It is deliberate: a
//! duplicate produced inside the broker is invisible to the client, which holds
//! the `request_id` and is the only layer that could deduplicate. A client that
//! wants a retry can reissue and know it did.
//!
//! Retries are bounded by `MAX_ATTEMPTS` so a shard being reassigned converges
//! or fails explicitly, rather than chasing `NotLeader` around a cluster.
//!
//! This is the requesting side; [`owner`] is the side that answers.

pub mod owner;

pub use owner::ForwardingHandler;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_wire::internal::{
    AckMode, CacheOpKind, ErrorCode, ForwardCacheOp, ForwardPublish, InternalMessage,
    PeerCapabilities, ShardRef,
};

use felix_replication::peer::PeerRequester;
use felix_replication::peer::metrics;
use felix_replication::peer::pool::PeerError;

/// Total attempts for one publish, across every owner it is redirected to.
///
/// Three covers the case this exists for — a reassignment observed mid-publish,
/// which costs one redirect — with one spare. Beyond that the routing view is
/// churning faster than a publish can complete, and failing explicitly beats
/// chasing it.
const MAX_ATTEMPTS: u32 = 3;

/// Where a publish is being sent, as this broker currently resolves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardTarget {
    pub node_id: String,
    pub advertise_addr: SocketAddr,
    pub generation: u64,
}

/// Which shard the batch belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
}

/// Why a forward did not succeed.
#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    /// The owner refused, or could not be reached, and retrying is allowed but
    /// exhausted. Nothing was applied.
    #[error("could not forward to the owner of {stream}: {detail}")]
    Refused {
        stream: String,
        detail: String,
        /// The owner's last typed refusal, if it gave one. `None` means it
        /// could not be reached or never answered with a code.
        code: Option<ErrorCode>,
    },
    /// The batch was sent and its answer never arrived. It may or may not have
    /// been applied, and this broker cannot tell.
    #[error("forwarded publish to {node_id} was not acknowledged: {detail}")]
    Indeterminate { node_id: String, detail: String },
}

/// Forward one batch and wait for the owner's answer.
///
/// Returns the log offsets the owner assigned, when the stream has a log and
/// the owner is recent enough to say so.
///
/// `budget` bounds the whole loop, retries included. Without it the attempt
/// budget is the only bound, and it is far larger than the client's patience:
/// the waiter gives up first and replaces whatever this concluded with
/// "publish commit timeout", which is both less informative and less
/// actionable than every answer below. See `BrokerConfig::forward_budget`.
pub async fn forward_publish(
    pool: &impl PeerRequester,
    target: &ForwardTarget,
    key: &ForwardKey,
    ack: AckMode,
    credential: &str,
    payloads: Vec<Bytes>,
    budget: Duration,
) -> Result<Option<(u64, u64)>, ForwardError> {
    let deadline = Instant::now() + budget;
    let mut target = target.clone();
    let mut last = String::new();
    let mut last_code = None;
    // Set once an owner has said it does not know the credentialed kind: it
    // predates it, and the only frame it can serve is the legacy one. That
    // owner checks nothing either way, so falling back costs no protection
    // this broker could have had -- it only keeps a rolling upgrade forwarding.
    let mut legacy = false;
    // Every (node, generation) this batch has already been sent to. A redirect
    // back to one of them is a loop and is refused; a redirect to a new one is
    // followed even when the generation has not advanced — on a freshly formed
    // cluster the correct owner is at generation 0, and a forwarder whose
    // routing snapshot has not yet converged legitimately points at the wrong
    // node there. Refusing every same-generation redirect made that transient
    // misroute fatal instead of self-correcting.
    let mut tried: Vec<(String, u64)> = vec![(target.node_id.clone(), target.generation)];

    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            metrics::record_forward_retry();
        }
        // Nothing has been sent on this attempt yet, so running out here is
        // still "nothing was sent" and the caller may re-send.
        let Some(remaining) = remaining(deadline) else {
            metrics::record_forward(metrics::OUTCOME_EXHAUSTED);
            return Err(ForwardError::Refused {
                stream: key.stream.clone(),
                code: last_code,
                detail: budget_spent(&target.node_id, budget, attempt, &last),
            });
        };
        let request = InternalMessage::ForwardPublish(ForwardPublish {
            // The pool assigns the real id; it owns the connection this lands on.
            correlation_id: 0,
            shard: ShardRef {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
                generation: target.generation,
            },
            ack,
            payloads: payloads.clone(),
            credential: if legacy {
                String::new()
            } else {
                credential.to_string()
            },
        });

        let answer = tokio::time::timeout(
            remaining,
            PeerRequester::request(pool, &target.node_id, target.advertise_addr, request),
        )
        .await;
        let Ok(answer) = answer else {
            // The request went out and the budget ran out waiting for it. The
            // owner may have applied the batch, so this is not a refusal.
            metrics::record_forward(metrics::OUTCOME_INDETERMINATE);
            return Err(ForwardError::Indeterminate {
                node_id: target.node_id,
                detail: format!("no answer within the publish budget of {budget:?}"),
            });
        };
        match answer {
            Ok(InternalMessage::ForwardPublishOk(ok)) => {
                metrics::record_forward(metrics::OUTCOME_OK);
                // An owner that predates the capability answered a stream with
                // no log with `0..=0`, so its offsets cannot be told from a
                // real one and are not passed on.
                let placed = pool
                    .recorded_capabilities(&target.node_id)
                    .is_some_and(|offered| offered.contains(PeerCapabilities::FORWARD_OFFSETS));
                return Ok(ok.offsets().filter(|_| placed));
            }
            Ok(InternalMessage::NotLeader(moved)) => {
                // The owner refused, so nothing was applied, and it named where
                // to go. Following that beats waiting for this broker's watch to
                // catch up — but only within the attempt budget, so a shard
                // being reassigned repeatedly fails rather than loops.
                last = format!(
                    "shard moved to {} at generation {}",
                    moved.node_id, moved.generation
                );
                let Ok(advertise_addr) = moved.advertise_addr.parse() else {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        code: None,
                        detail: format!(
                            "owner {} advertised an unusable address {}",
                            moved.node_id, moved.advertise_addr
                        ),
                    });
                };
                // A redirect back to a node this batch has already been sent to
                // at the same generation is a loop; following it would bounce
                // between two brokers that disagree. A redirect to a node not
                // yet tried is a correction, even at the same generation.
                if tried.contains(&(moved.node_id.clone(), moved.generation)) {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        code: None,
                        detail: format!(
                            "owner {} redirected in a loop at generation {}",
                            moved.node_id, moved.generation
                        ),
                    });
                }
                tried.push((moved.node_id.clone(), moved.generation));
                target = ForwardTarget {
                    node_id: moved.node_id,
                    advertise_addr,
                    generation: moved.generation,
                };
            }
            Ok(InternalMessage::ForwardPublishError(err))
                if err.code == ErrorCode::UnsupportedKind && !legacy =>
            {
                // An owner from before credentialed forwards. Nothing was
                // applied; send what it can read.
                last = format!("{:?}: {}", err.code, err.detail);
                last_code = Some(err.code);
                legacy = true;
            }
            Ok(InternalMessage::ForwardPublishError(err)) => {
                last = format!("{:?}: {}", err.code, err.detail);
                last_code = Some(err.code);
                if !err.code.is_retryable() {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        code: last_code,
                        detail: last,
                    });
                }
                // `StorageFailed` is not retryable and lands above; every code
                // that reaches here refused before writing.
                sleep_within(retry_delay(attempt), deadline).await;
            }
            Ok(other) => {
                metrics::record_forward(metrics::OUTCOME_REFUSED);
                return Err(ForwardError::Refused {
                    stream: key.stream.clone(),
                    code: None,
                    detail: format!("owner answered with an unexpected {:?}", other.kind()),
                });
            }
            Err(err) if err.is_retryable() => {
                last = err.to_string();
                last_code = None;
                sleep_within(retry_delay(attempt), deadline).await;
            }
            Err(err @ (PeerError::Disconnected { .. } | PeerError::Timeout { .. })) => {
                // The batch is out there. Retrying would duplicate it, and this
                // broker cannot tell whether it landed.
                metrics::record_forward(metrics::OUTCOME_INDETERMINATE);
                return Err(ForwardError::Indeterminate {
                    node_id: target.node_id,
                    detail: err.to_string(),
                });
            }
            Err(err) => {
                metrics::record_forward(metrics::OUTCOME_REFUSED);
                return Err(ForwardError::Refused {
                    stream: key.stream.clone(),
                    code: last_code,
                    detail: err.to_string(),
                });
            }
        }
    }

    metrics::record_forward(metrics::OUTCOME_EXHAUSTED);
    Err(ForwardError::Refused {
        stream: key.stream.clone(),
        code: last_code,
        detail: format!("gave up after {MAX_ATTEMPTS} attempts: {last}"),
    })
}

/// What a cache operation asks the owner to do.
///
/// A separate type from the wire's `CacheOpKind` so callers do not have to
/// build a `Bytes` and a TTL for a read.
#[derive(Debug, Clone)]
pub enum CacheRequest {
    Put {
        value: Bytes,
        ttl_ms: u64,
    },
    Get,
    Delete,
    /// Apply a signed delta to the counter of this key. The delta rides the
    /// value bytes as eight big-endian bytes, so the envelope is unchanged and
    /// an old peer refuses the op rather than misparsing the body.
    CounterAdd {
        delta: i64,
    },
    /// Read the counter's sum; answered in the value bytes the same way.
    CounterGet,
}

impl CacheRequest {
    fn parts(&self) -> (CacheOpKind, Bytes, u64) {
        match self {
            Self::Put { value, ttl_ms } => (CacheOpKind::Put, value.clone(), *ttl_ms),
            Self::Get => (CacheOpKind::Get, Bytes::new(), 0),
            Self::Delete => (CacheOpKind::Delete, Bytes::new(), 0),
            Self::CounterAdd { delta } => (
                CacheOpKind::CounterAdd,
                felix_storage::counter_log::encode_sum(*delta),
                0,
            ),
            Self::CounterGet => (CacheOpKind::CounterGet, Bytes::new(), 0),
        }
    }

    /// Whether re-sending this operation after an indeterminate answer is safe.
    ///
    /// A `Get` is a read and a `Delete` removes a named key, so re-sending
    /// either lands on the same state. A `Put` with a TTL does not: the second
    /// attempt restarts the clock. Treating a write as indeterminate rather
    /// than retrying it keeps the caller in charge of that decision.
    fn is_idempotent(&self) -> bool {
        matches!(self, Self::Get | Self::Delete)
    }
}

/// Forward one cache operation and wait for the owner's answer.
///
/// Returns the value the owner reported: what a `Get` found, what a `Delete`
/// removed, and `None` for a `Put` or a miss.
pub async fn forward_cache_op(
    pool: &impl PeerRequester,
    target: &ForwardTarget,
    key: &ForwardKey,
    cache_key: &str,
    credential: &str,
    request: &CacheRequest,
) -> Result<Option<Bytes>, ForwardError> {
    let mut target = target.clone();
    let mut last = String::new();
    let mut last_code = None;
    let (op, value, ttl_ms) = request.parts();
    // See `forward_publish`: an owner that predates the credentialed kind gets
    // the legacy one, which is all it can read.
    let mut legacy = false;
    // See `forward_publish`: a redirect to a node not yet tried is followed even
    // at the same generation, and only a loop back to a tried (node, generation)
    // is refused.
    let mut tried: Vec<(String, u64)> = vec![(target.node_id.clone(), target.generation)];

    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            metrics::record_forward_retry();
        }
        let message = InternalMessage::ForwardCacheOp(ForwardCacheOp {
            correlation_id: 0,
            shard: ShardRef {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
                generation: target.generation,
            },
            op,
            key: cache_key.to_string(),
            value: value.clone(),
            ttl_ms,
            credential: if legacy {
                String::new()
            } else {
                credential.to_string()
            },
        });

        match PeerRequester::request(pool, &target.node_id, target.advertise_addr, message).await {
            Ok(InternalMessage::ForwardCacheOk(ok)) => {
                metrics::record_forward(metrics::OUTCOME_OK);
                return Ok(ok.value);
            }
            Ok(InternalMessage::NotLeader(moved)) => {
                last = format!(
                    "cache shard moved to {} at generation {}",
                    moved.node_id, moved.generation
                );
                let Ok(advertise_addr) = moved.advertise_addr.parse() else {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        code: None,
                        detail: format!(
                            "owner {} advertised an unusable address {}",
                            moved.node_id, moved.advertise_addr
                        ),
                    });
                };
                // Same rule as a forwarded publish: refuse only a loop back to a
                // (node, generation) already tried; follow a correction to a new
                // node even at the same generation.
                if tried.contains(&(moved.node_id.clone(), moved.generation)) {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        code: None,
                        detail: format!(
                            "owner {} redirected in a loop at generation {}",
                            moved.node_id, moved.generation
                        ),
                    });
                }
                tried.push((moved.node_id.clone(), moved.generation));
                target = ForwardTarget {
                    node_id: moved.node_id,
                    advertise_addr,
                    generation: moved.generation,
                };
            }
            // An unknown kind is refused before the responder knows which
            // request it was, so the answer arrives in the publish error's
            // shape whatever was asked.
            Ok(InternalMessage::ForwardPublishError(err))
                if err.code == ErrorCode::UnsupportedKind && !legacy =>
            {
                last = format!("{:?}: {}", err.code, err.detail);
                last_code = Some(err.code);
                legacy = true;
            }
            Ok(InternalMessage::ForwardCacheError(err)) => {
                last = format!("{:?}: {}", err.code, err.detail);
                last_code = Some(err.code);
                if !err.code.is_retryable() {
                    metrics::record_forward(metrics::OUTCOME_REFUSED);
                    return Err(ForwardError::Refused {
                        stream: key.stream.clone(),
                        code: last_code,
                        detail: last,
                    });
                }
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Ok(other) => {
                metrics::record_forward(metrics::OUTCOME_REFUSED);
                return Err(ForwardError::Refused {
                    stream: key.stream.clone(),
                    code: None,
                    detail: format!("owner answered with an unexpected {:?}", other.kind()),
                });
            }
            Err(err) if err.is_retryable() => {
                last = err.to_string();
                last_code = None;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(err @ (PeerError::Disconnected { .. } | PeerError::Timeout { .. }))
                if !request.is_idempotent() =>
            {
                metrics::record_forward(metrics::OUTCOME_INDETERMINATE);
                return Err(ForwardError::Indeterminate {
                    node_id: target.node_id,
                    detail: err.to_string(),
                });
            }
            Err(err) => {
                last = err.to_string();
                last_code = None;
                tokio::time::sleep(retry_delay(attempt)).await;
            }
        }
    }

    metrics::record_forward(metrics::OUTCOME_REFUSED);
    Err(ForwardError::Refused {
        stream: key.stream.clone(),
        code: last_code,
        detail: last,
    })
}

/// A short pause between attempts, so a shard mid-reassignment is not retried
/// before anything can have changed. Deliberately small: the client is waiting,
/// and the attempt budget is the real bound.
fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(5 << attempt.min(4))
}

/// What is left of the budget, or `None` once it is spent.
fn remaining(deadline: Instant) -> Option<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    (!left.is_zero()).then_some(left)
}

/// Pause between attempts without sleeping past the deadline — the next attempt
/// checks what is left, and a sleep that overshot would make that check the
/// thing that fails rather than the request it was waiting to retry.
async fn sleep_within(delay: Duration, deadline: Instant) {
    tokio::time::sleep(delay.min(deadline.saturating_duration_since(Instant::now()))).await;
}

fn budget_spent(node_id: &str, budget: Duration, attempts: u32, last: &str) -> String {
    format!(
        "owner {node_id} did not accept the batch within {budget:?} ({attempts} attempts): {last}"
    )
}

#[cfg(test)]
mod tests;
