//! Ingress: worker sharding, the bounded enqueue path, and in-flight depth accounting.

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "perf_debug")]
use std::time::Instant;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felix_broker::StreamHandle;
use tokio::sync::{mpsc, watch};

use crate::observability::tenants;
use crate::serving::quic::GLOBAL_INGRESS_DEPTH;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::ack::EnqueuePolicy;
use crate::serving::quic::handlers::publish::admission::AdmissionPermit;
use crate::serving::quic::handlers::publish::{PublishContext, PublishJob};
use crate::serving::quic::telemetry::{t_counter, t_gauge};
use crate::shards::lifecycle::fence::FenceGuard;

pub(crate) enum PublishTarget {
    Resolved {
        handle: StreamHandle,
        /// The shard this publish resolved to, when this broker is in a
        /// cluster. `None` on a single-node broker, which has no replica set
        /// and so nothing to wait for.
        shard: Option<crate::shards::ShardKey>,
        /// The generation it was admitted at, which the fence checks at the
        /// claim.
        generation: u64,
        /// Its place in the fence, when routing entered it. Moved to the job
        /// when it is queued.
        fenced: Option<FenceGuard>,
    },
    /// This broker leads the shard and the batch names its producer: appended
    /// once however many times it arrives. Never forwarded, because only the
    /// leader holds the sequences a re-send is checked against.
    Idempotent {
        handle: StreamHandle,
        shard: Option<crate::shards::ShardKey>,
        generation: u64,
        fenced: Option<FenceGuard>,
        producer_id: u64,
        sequence: u64,
        /// What a different batch under a sequence already held gets.
        reuse: felix_broker::SequenceReuse,
    },
    /// Another broker owns the shard. The batch is sent there and its answer
    /// relayed, from the same worker a local write would have used, so the ack
    /// path is identical either way.
    Forward {
        target: crate::serving::forward::ForwardTarget,
        key: crate::serving::forward::ForwardKey,
        ack: felix_wire::internal::AckMode,
        /// The publisher's token, for the owner to verify again.
        credential: String,
    },
    #[cfg(test)]
    Named {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
}

impl PublishTarget {
    /// The fence place routing entered for this write, if it did.
    fn take_fence(&mut self) -> Option<FenceGuard> {
        match self {
            Self::Resolved { fenced, .. } | Self::Idempotent { fenced, .. } => fenced.take(),
            _ => None,
        }
    }
}

/// Deterministically map (tenant, namespace, stream) to a publish worker index.
///
/// Goal: keep ordering locality and cache locality for a given stream by always hashing to
/// the same worker, while distributing streams across workers reasonably well.
///
/// This *must* be stable across processes for predictable performance; it does not need to be
/// cryptographically secure.
pub(crate) fn publish_worker_index(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    worker_count: usize,
) -> usize {
    if worker_count == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    tenant_id.hash(&mut hasher);
    namespace.hash(&mut hasher);
    stream.hash(&mut hasher);
    (hasher.finish() as usize) % worker_count
}

/// Enqueue a publish job into the appropriate worker queue with explicit overload semantics.
///
/// Return value:
/// - `Ok(true)`  → job enqueued
/// - `Ok(false)` → job intentionally dropped (policy = Drop)
/// - `Err(...)`  → failure to enqueue (policy = Fail, closed queue, timeout, cancellation)
///
/// `cancel` is the connection's teardown signal. It is what bounds
/// [`EnqueuePolicy::Backpressure`], which has no timer; `None` means the caller has
/// no teardown signal to offer (the uni-stream paths), and such a wait ends only
/// when capacity frees or the worker queue closes.
///
/// Implementation detail:
/// - We try `try_send` first to keep the common path allocation-free and to make overload observable
///   (`Full` vs `Closed`).
/// - Any path that successfully enqueues must increment both local and global depth **exactly once**.
pub(crate) async fn enqueue_publish(
    publish_ctx: &PublishContext,
    mut job: PublishJob,
    policy: EnqueuePolicy,
    mut cancel: Option<watch::Receiver<bool>>,
) -> Result<bool> {
    let worker_index = match &job.target {
        PublishTarget::Resolved { handle, .. } | PublishTarget::Idempotent { handle, .. } => {
            handle.id() as usize % publish_ctx.worker_count.max(1)
        }
        // Hashed by name, because there is no local handle to hash. Same
        // function the named path uses, so one stream's forwards stay on one
        // worker and keep their order.
        PublishTarget::Forward { key, .. } => publish_worker_index(
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            publish_ctx.worker_count,
        ),
        #[cfg(test)]
        PublishTarget::Named {
            tenant_id,
            namespace,
            stream,
        } => publish_worker_index(tenant_id, namespace, stream, publish_ctx.worker_count),
    };
    let worker = publish_ctx
        .workers
        .get(worker_index)
        .ok_or_else(|| anyhow!("publish worker index out of range"))?;

    // Byte-based admission gate, independent of the item-count queue depth: bounds total bytes
    // queued-or-processing so a handful of large payloads/batches can't blow past the intended
    // ingress memory budget. Two gates are applied: the connection's own share
    // (`conn_admission`) first, then the shared process-wide budget (`admission`). Gating on the
    // per-connection budget first means one connection maxing out its own share can't consume
    // global-budget accounting cycles meant for other connections. Both permits travel with the
    // job and are released together once the job is done (or dropped without ever being
    // enqueued).
    let job_bytes: usize = job.payloads.iter().map(Bytes::len).sum();
    // One deadline for the whole enqueue, not one per stage. Admission and the
    // queue send used to get a full `wait_timeout` each, so a publish could block
    // for twice the configured budget while the knob read as a single bound — and
    // because this runs inline in the control-stream read loop, that doubled the
    // head-of-line stall on the connection too.
    let deadline = tokio::time::Instant::now() + publish_ctx.wait_timeout;
    let acquire_both = async {
        let conn_permit = publish_ctx.conn_admission.acquire(job_bytes).await?;
        let permit = publish_ctx.admission.acquire(job_bytes).await?;
        Ok::<_, tokio::sync::AcquireError>((conn_permit, permit))
    };
    let (conn_permit, permit) = match policy {
        EnqueuePolicy::Wait => match tokio::time::timeout_at(deadline, acquire_both).await {
            Ok(Ok(permits)) => permits,
            Ok(Err(_)) => return Err(anyhow!("publish admission closed")),
            Err(_) => return Err(anyhow!("publish admission timed out")),
        },
        EnqueuePolicy::Backpressure => match until_cancelled(acquire_both, &mut cancel).await {
            Some(Ok(permits)) => permits,
            Some(Err(_)) => return Err(anyhow!("publish admission closed")),
            None => {
                t_counter!("felix_broker_ingress_backpressure_cancelled_total").increment(1);
                return Err(anyhow!(
                    "publish cancelled while waiting for ingress capacity"
                ));
            }
        },
        EnqueuePolicy::Drop | EnqueuePolicy::Fail => {
            let conn_permit = match publish_ctx.conn_admission.try_acquire(job_bytes) {
                Ok(permit) => permit,
                Err(_) => {
                    t_counter!("felix_broker_ingress_conn_bytes_full_total").increment(1);
                    return match policy {
                        EnqueuePolicy::Drop => {
                            t_counter!("felix_broker_ingress_dropped_total").increment(1);
                            Ok(false)
                        }
                        EnqueuePolicy::Fail => {
                            t_counter!("felix_broker_ingress_rejected_total").increment(1);
                            Err(anyhow!(
                                "publish ingress per-connection byte budget exhausted"
                            ))
                        }
                        EnqueuePolicy::Wait | EnqueuePolicy::Backpressure => {
                            unreachable!("waiting policies handled above")
                        }
                    };
                }
            };
            match publish_ctx.admission.try_acquire(job_bytes) {
                Ok(permit) => (conn_permit, permit),
                Err(_) => {
                    t_counter!("felix_broker_ingress_bytes_full_total").increment(1);
                    return match policy {
                        EnqueuePolicy::Drop => {
                            t_counter!("felix_broker_ingress_dropped_total").increment(1);
                            Ok(false)
                        }
                        EnqueuePolicy::Fail => {
                            t_counter!("felix_broker_ingress_rejected_total").increment(1);
                            Err(anyhow!("publish ingress byte budget exhausted"))
                        }
                        EnqueuePolicy::Wait | EnqueuePolicy::Backpressure => {
                            unreachable!("waiting policies handled above")
                        }
                    };
                }
            }
        }
    };
    job.admission_permit = Some(AdmissionPermit {
        _conn: conn_permit,
        _global: permit,
    });
    // A write routed on a cluster member entered the fence when it was
    // routed, and keeps that place until it is written.
    if job.fenced.is_none() {
        job.fenced = job.target.take_fence();
    }
    // Nobody waits on this job, so its ack goes out before the write and a
    // refusal at the claim would reach no one. It enters the fence now and
    // keeps the guard until it is written; a move then waits for it instead.
    if job.response.is_none() && job.fenced.is_none() {
        job.fenced = fence_now(publish_ctx, &job.target)?;
    }

    #[cfg(feature = "perf_debug")]
    let enqueue_wait_start = Instant::now();
    // We use try_send first to keep the fast path allocation-free and to make overload observable
    // (Full vs Closed). Only the Wait policy performs an async send with a timeout.
    // IMPORTANT: Any code path that successfully enqueues MUST increment depth counters exactly once.
    // Best-effort enqueue with metrics and optional backpressure/err.
    match worker.try_send(job) {
        Ok(()) => {
            let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
            let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
            t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
            #[cfg(feature = "perf_debug")]
            {
                let wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
                metrics::histogram!(
                    "felix_perf_publish_enqueue_wait_ns",
                    "worker" => worker_index.to_string()
                )
                .record(wait_ns as f64);
                metrics::counter!(
                    "felix_perf_publish_enqueue_ok_total",
                    "worker" => worker_index.to_string()
                )
                .increment(1);
            }
            Ok(true)
        }
        Err(mpsc::error::TrySendError::Full(job)) => {
            t_counter!("felix_broker_ingress_queue_full_total").increment(1);
            match policy {
                EnqueuePolicy::Drop => {
                    t_counter!("felix_broker_ingress_dropped_total").increment(1);
                    Ok(false)
                }
                EnqueuePolicy::Fail => {
                    t_counter!("felix_broker_ingress_rejected_total").increment(1);
                    Err(anyhow!("publish queue full"))
                }
                EnqueuePolicy::Wait | EnqueuePolicy::Backpressure => {
                    // Acked publishes with ack_on_commit use Wait; unacked publishes
                    // with `pub_ingress_wait` use Backpressure.
                    t_counter!("felix_broker_ingress_waited_total").increment(1);
                    let send_result = match policy {
                        // Shares the deadline computed above with the admission
                        // stage, so the two together cannot exceed `wait_timeout`.
                        EnqueuePolicy::Wait => tokio::time::timeout_at(deadline, worker.send(job))
                            .await
                            .map_err(|_| anyhow!("publish enqueue timed out"))?,
                        _ => match until_cancelled(worker.send(job), &mut cancel).await {
                            Some(result) => result,
                            None => {
                                t_counter!("felix_broker_ingress_backpressure_cancelled_total")
                                    .increment(1);
                                return Err(anyhow!(
                                    "publish cancelled while waiting for ingress queue"
                                ));
                            }
                        },
                    };
                    send_result.map_err(|_| anyhow!("publish queue closed"))?;
                    // Local depth is per publish context; global depth is used for cross-connection observability.
                    let _local = publish_ctx.depth.fetch_add(1, Ordering::Relaxed) + 1;
                    let global = GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
                    t_gauge!("felix_broker_ingress_queue_depth").set(global as f64);
                    #[cfg(feature = "perf_debug")]
                    {
                        let wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
                        metrics::histogram!(
                            "felix_perf_publish_enqueue_wait_ns",
                            "worker" => worker_index.to_string()
                        )
                        .record(wait_ns as f64);
                        metrics::counter!(
                            "felix_perf_publish_enqueue_wait_total",
                            "worker" => worker_index.to_string()
                        )
                        .increment(1);
                    }
                    Ok(true)
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("publish queue closed")),
    }
}

/// [`enqueue_publish`] for a client's publish on behalf of `tenant`, after the
/// tenant's publish quota admits it, counting what was queued against the
/// tenant.
///
/// The quota is checked before any byte budget is taken, so a tenant over its
/// quota holds none of the shared budget other tenants need. Over quota:
///
/// - an acked publish (`Fail`/`Wait`) is refused as `overloaded` with the wait
///   in `retry_after_ms`, and nothing is queued;
/// - a fire-and-forget publish under `Drop` is shed, like any other overload;
/// - under `Backpressure` it waits for the quota, which slows the publisher
///   through QUIC flow control instead of losing its messages.
pub(crate) async fn enqueue_tenant_publish(
    publish_ctx: &PublishContext,
    tenant: &str,
    job: PublishJob,
    policy: EnqueuePolicy,
    mut cancel: Option<watch::Receiver<bool>>,
) -> Result<bool> {
    let messages = job.payloads.len() as u64;
    let bytes = job.payloads.iter().map(Bytes::len).sum::<usize>() as u64;
    let rates = &publish_ctx.tenant_rates;
    if let Err(wait) = rates.try_admit(tenant, messages, bytes) {
        match policy {
            EnqueuePolicy::Drop => {
                tenants::record_throttled(tenant, tenants::THROTTLE_DROPPED);
                t_counter!("felix_broker_ingress_dropped_total").increment(1);
                return Ok(false);
            }
            EnqueuePolicy::Fail | EnqueuePolicy::Wait => {
                tenants::record_throttled(tenant, tenants::THROTTLE_REFUSED);
                return Err(anyhow::Error::new(ClientError::tenant_quota(wait)));
            }
            EnqueuePolicy::Backpressure => {
                tenants::record_throttled(tenant, tenants::THROTTLE_DELAYED);
                let mut wait = wait;
                loop {
                    if until_cancelled(tokio::time::sleep(wait), &mut cancel)
                        .await
                        .is_none()
                    {
                        return Err(anyhow!("publish cancelled while waiting for tenant quota"));
                    }
                    match rates.try_admit(tenant, messages, bytes) {
                        Ok(()) => break,
                        Err(next) => wait = next,
                    }
                }
            }
        }
    }
    let enqueued = enqueue_publish(publish_ctx, job, policy, cancel).await?;
    if enqueued {
        tenants::record_published(tenant, messages, bytes);
    }
    Ok(enqueued)
}

// Adjust queue depth gauges safely when send fails or work completes.
pub(crate) fn decrement_depth(
    depth: &Arc<AtomicUsize>,
    global: &AtomicUsize,
    gauge: &'static str,
) -> Option<(usize, usize)> {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    // Depth tracking is intentionally best-effort: we avoid panicking on underflow and tolerate drift.
    // Drift can occur if a task exits unexpectedly or if multiple teardown paths reset counters.
    // We record drift metrics and rely on `reset_local_depth_only` to reconcile on teardown.
    if let Ok(prev) = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        if value == 0 { None } else { Some(value - 1) }
    }) {
        let cur = prev.saturating_sub(1);
        let updated = match global.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            if value == 0 { None } else { Some(value - 1) }
        }) {
            Ok(value) => value - 1,
            Err(_) => {
                t_counter!("felix_queue_depth_drift_total", "queue" => gauge).increment(1);
                global.load(Ordering::Relaxed)
            }
        };
        t_gauge!(gauge).set(updated as f64);
        return Some((prev, cur));
    }
    None
}

pub(crate) fn reset_local_depth_only(
    depth: &Arc<AtomicUsize>,
    global: &AtomicUsize,
    gauge: &'static str,
) {
    #[cfg(not(feature = "telemetry"))]
    let _ = gauge;
    let remaining = depth.swap(0, Ordering::Relaxed);
    if remaining == 0 {
        return;
    }
    let mut prev = global.load(Ordering::Relaxed);
    loop {
        let next = prev.saturating_sub(remaining);
        match global.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                t_gauge!(gauge).set(next as f64);
                break;
            }
            Err(updated) => prev = updated,
        }
    }
}

/// Enter the write fence for a local write at the generation it was admitted
/// at. `Ok(None)` for a forward, which the owner fences, and on a single-node
/// broker.
fn fence_now(
    publish_ctx: &PublishContext,
    target: &PublishTarget,
) -> Result<Option<crate::shards::lifecycle::fence::FenceGuard>> {
    match target {
        PublishTarget::Resolved {
            shard, generation, ..
        }
        | PublishTarget::Idempotent {
            shard, generation, ..
        } => Ok(crate::shards::lifecycle::fence::enter(
            publish_ctx.ingress.as_deref(),
            shard.as_ref(),
            *generation,
        )?),
        _ => Ok(None),
    }
}

/// Await `fut` unless the connection is cancelled first.
///
/// `None` means cancellation won. A dropped sender counts as cancelled: the owner
/// of the control stream is gone, so there is nobody left to deliver for.
async fn until_cancelled<F: Future>(
    fut: F,
    cancel: &mut Option<watch::Receiver<bool>>,
) -> Option<F::Output> {
    let Some(rx) = cancel else {
        return Some(fut.await);
    };
    // Pinned so a watch wake-up that turns out not to be a cancellation can go
    // back to waiting on the same future instead of restarting it.
    tokio::pin!(fut);
    loop {
        if *rx.borrow() {
            return None;
        }
        tokio::select! {
            out = &mut fut => return Some(out),
            changed = rx.changed() => {
                // A dropped sender means the connection is gone; anything else
                // loops round to re-read the flag.
                if changed.is_err() {
                    return None;
                }
            }
        }
    }
}
