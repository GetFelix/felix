//! The process-wide publish worker pool, and the publish context every
//! connection derives its own from.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use tokio::sync::{mpsc, oneshot};

use super::{
    PublishAdmission, PublishContext, PublishJob, PublishTarget, SubscriptionLimiter,
    decrement_depth,
};
use crate::config::BrokerConfig;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::subscribe::WriterLaneManager;
use crate::serving::quic::{ClusterContext, GLOBAL_INGRESS_DEPTH};
use crate::shards::ShardKey;
use crate::shards::lifecycle::fence;

pub(crate) fn build_publish_context(
    broker: Arc<Broker>,
    config: &BrokerConfig,
    cluster: ClusterContext,
) -> PublishContext {
    let ClusterContext {
        ingress,
        peers,
        lease,
        marks,
        client_endpoints,
    } = cluster;
    let quorum_timeout = std::time::Duration::from_millis(config.publish_quorum_timeout_ms.max(1));
    // Copied per worker below. A forward that outlasts this is cut off with its
    // own answer rather than the waiter's, which is the whole point -- see
    // `BrokerConfig::forward_budget`.
    let forward_budget = config.forward_budget();
    // NOTE: This is intentionally global for the process (not per-connection).
    // With per-connection worker pools, adding more publisher connections multiplied
    // concurrent broker.publish_batch callers and caused lock contention on shared broker state.
    let shards = crate::serving::core_shards::global_shards(config);
    // With core shards enabled, run exactly one publish worker per shard so
    // each stream's state has a single owning core (worker i lives on shard i,
    // and `publish_worker_index` maps handle id -> worker == shard).
    let worker_count = match &shards {
        Some(shards) => shards.len(),
        None => config.pub_workers_per_conn.max(1),
    };
    let publish_queue_depth = config.pub_queue_depth.max(1);
    let admission = Arc::new(PublishAdmission::new(config.pub_inflight_bytes));
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut worker_txs = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        #[cfg(not(feature = "perf_debug"))]
        let _ = worker_id;
        #[cfg(feature = "perf_debug")]
        let worker_label = worker_id.to_string();
        let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(publish_queue_depth);
        let queue_depth_worker = Arc::clone(&queue_depth);
        let broker_for_worker = Arc::clone(&broker);
        let peers_for_worker = peers.clone();
        let marks_for_worker = marks.clone();
        let ingress_for_worker = ingress.clone();
        // How many durable publishes this worker may have awaiting their device
        // flush at once. The worker claims offsets serially -- that is what
        // keeps disk order equal to arrival order -- and then lets the flushes
        // overlap, because group commit only coalesces what is concurrently in
        // `ensure_durable`. At a fan-in of one the ceiling was one batch per
        // flush (#535).
        //
        // Bounded, not unbounded: the reason this pool is process-wide in the
        // first place is that unbounded concurrent `publish_batch` callers
        // contended on shared broker state (see the note above). This buys the
        // flush overlap without going back to that.
        let flush_slots = Arc::new(tokio::sync::Semaphore::new(
            config.pub_flush_concurrency.max(1),
        ));
        let worker_task = async move {
            while let Some(mut job) = publish_rx.recv().await {
                let mut held = job.fenced.take();
                #[cfg(feature = "perf_debug")]
                metrics::counter!(
                    "felix_perf_publish_worker_wakeups_total",
                    "worker" => worker_label.clone()
                )
                .increment(1);
                let _ = decrement_depth(
                    &queue_depth_worker,
                    &GLOBAL_INGRESS_DEPTH,
                    "felix_broker_ingress_queue_depth",
                );
                #[cfg(feature = "perf_debug")]
                let worker_start = std::time::Instant::now();

                // Durable local publishes take the split path: claim here, in
                // queue order, then complete off-worker so the device flushes
                // overlap. Everything else -- forwards, idempotent sequences,
                // the single-node local target -- stays inline, because none of
                // them is waiting on a flush this worker could be sharing.
                if let PublishTarget::Resolved {
                    handle,
                    shard,
                    generation,
                    ..
                } = &job.target
                    && handle.is_durable()
                {
                    // Held until the publish is durable and fanned out, so
                    // a drained report cannot go out while it is landing.
                    // Entering is also the lease check, against the clock:
                    // see `fence`.
                    let fenced = match fence::enter_or_keep(
                        &mut held,
                        ingress_for_worker.as_deref(),
                        shard.as_ref(),
                        *generation,
                    ) {
                        Ok(fenced) => fenced,
                        Err(refused) => {
                            settle(
                                job.response,
                                job.acked_on_enqueue,
                                shard.as_ref(),
                                Err(refused.into()),
                            );
                            continue;
                        }
                    };
                    // Serial, and the only ordered part: offsets are
                    // consumed here, so the order these return in is the
                    // order records land on disk.
                    match broker_for_worker.claim_publish(handle, &job.payloads).await {
                        Ok(claimed) => {
                            let permit = Arc::clone(&flush_slots)
                                .acquire_owned()
                                .await
                                .expect("flush slots are never closed");
                            let broker = Arc::clone(&broker_for_worker);
                            let handle = handle.clone();
                            let shard = shard.clone();
                            let marks = marks_for_worker.clone();
                            let ingress = ingress_for_worker.clone();
                            let response = job.response;
                            let acked_on_enqueue = job.acked_on_enqueue;
                            tokio::spawn(async move {
                                let completed = broker.complete_publish(claimed).await;
                                drop(fenced);
                                let result = match completed {
                                    Ok(outcome) => {
                                        crate::replication::quorum::await_quorum(
                                            &handle,
                                            shard.as_ref(),
                                            &outcome,
                                            marks.as_deref(),
                                            ingress.as_deref(),
                                            quorum_timeout,
                                        )
                                        .await
                                    }
                                    Err(err) => Err(err.into()),
                                };
                                settle(response, acked_on_enqueue, shard.as_ref(), result);
                                drop(permit);
                            });
                            continue;
                        }
                        Err(err) => {
                            settle(
                                job.response,
                                job.acked_on_enqueue,
                                shard.as_ref(),
                                Err(err.into()),
                            );
                            continue;
                        }
                    }
                }

                let result: Result<(), anyhow::Error> = match &job.target {
                    PublishTarget::Resolved {
                        handle,
                        shard,
                        generation,
                        ..
                    } => {
                        // The commit fence, and the authoritative lease check.
                        // Everything between admission and here can take
                        // arbitrarily long -- a full queue, a slow fsync, a
                        // suspended process -- so a lease that was valid on the
                        // way in may have lapsed, and a broker that writes after
                        // losing it is writing a shard someone else may lead.
                        match fence::enter_or_keep(
                            &mut held,
                            ingress_for_worker.as_deref(),
                            shard.as_ref(),
                            *generation,
                        ) {
                            Err(refused) => Err(refused.into()),
                            Ok(fenced) => {
                                let published = broker_for_worker
                                    .publish_batch_with_outcome(handle, &job.payloads)
                                    .await;
                                drop(fenced);
                                match published {
                                    Ok(outcome) => {
                                        crate::replication::quorum::await_quorum(
                                            handle,
                                            shard.as_ref(),
                                            &outcome,
                                            marks_for_worker.as_deref(),
                                            ingress_for_worker.as_deref(),
                                            quorum_timeout,
                                        )
                                        .await
                                    }
                                    Err(err) => Err(err.into()),
                                }
                            }
                        }
                    }
                    PublishTarget::Idempotent {
                        handle,
                        shard,
                        generation,
                        producer_id,
                        sequence,
                        ..
                    } => match fence::enter_or_keep(
                        // The same commit fence as a plain publish: see above.
                        &mut held,
                        ingress_for_worker.as_deref(),
                        shard.as_ref(),
                        *generation,
                    ) {
                        Err(refused) => Err(refused.into()),
                        Ok(fenced) => {
                            let published = broker_for_worker
                                .publish_batch_idempotent(
                                    handle,
                                    *producer_id,
                                    *sequence,
                                    &job.payloads,
                                )
                                .await;
                            drop(fenced);
                            match published {
                                // A duplicate waits on the same quorum the
                                // original did: its offsets are the original's,
                                // and the answer must mean the same thing.
                                Ok(idempotent) => {
                                    crate::replication::quorum::await_quorum(
                                        handle,
                                        shard.as_ref(),
                                        &idempotent.outcome,
                                        marks_for_worker.as_deref(),
                                        ingress_for_worker.as_deref(),
                                        quorum_timeout,
                                    )
                                    .await
                                }
                                Err(err) => Err(err.into()),
                            }
                        }
                    },
                    #[cfg(test)]
                    PublishTarget::Named {
                        tenant_id,
                        namespace,
                        stream,
                    } => broker_for_worker
                        .publish_batch(tenant_id, namespace, stream, 0, &job.payloads)
                        .await
                        .map(|_| ())
                        .map_err(Into::into),
                    PublishTarget::Forward {
                        target,
                        key,
                        ack,
                        credential,
                    } => {
                        // Forwarding runs on the publish worker, not inline on
                        // the read loop, so a slow peer backs up the same queue
                        // a slow disk would and the existing backpressure and
                        // ack plumbing apply unchanged.
                        match &peers_for_worker {
                            Some(pool) => crate::serving::forward::forward_publish(
                                pool,
                                target,
                                key,
                                *ack,
                                credential,
                                job.payloads.clone(),
                                forward_budget,
                            )
                            .await
                            .map(|_| ())
                            .map_err(anyhow::Error::from),
                            // The route said forward and there is nothing to
                            // forward with. Refusing beats writing another
                            // broker's shard locally.
                            None => Err(ClientError::internal(format!(
                                "no peer transport: this broker cannot forward to {}",
                                target.node_id
                            ))
                            .with_retry(felix_wire::RetryClass::Retry)
                            .into()),
                        }
                    }
                };
                #[cfg(feature = "perf_debug")]
                {
                    let ns = worker_start.elapsed().as_nanos() as u64;
                    metrics::histogram!("felix_perf_pub_worker_ns", "worker" => worker_label.clone())
                        .record(ns as f64);
                    metrics::counter!(
                        "felix_perf_publish_worker_jobs_total",
                        "worker" => worker_label.clone()
                    )
                    .increment(1);
                }
                let shard = match &job.target {
                    PublishTarget::Resolved { shard, .. }
                    | PublishTarget::Idempotent { shard, .. } => shard.as_ref(),
                    _ => None,
                };
                settle(job.response, job.acked_on_enqueue, shard, result);
            }
        };
        match &shards {
            Some(shards) => {
                shards.handle_for(worker_id as u64).spawn(worker_task);
            }
            None => {
                tokio::spawn(worker_task);
            }
        }
        worker_txs.push(publish_tx);
    }
    PublishContext {
        ingress,
        peers,
        lease,
        lease_headroom: lease_headroom(config),
        client_endpoints,
        marks,
        quorum_timeout,
        workers: Arc::new(worker_txs),
        worker_count,
        depth: queue_depth,
        wait_timeout: Duration::from_millis(config.publish_queue_wait_timeout_ms),
        admission,
        // Placeholder; `handle_connection` replaces this (and `subscriptions`/`lane_manager`)
        // with fresh per-connection instances before this context is used by any stream on
        // that connection. These template values are never themselves shared across
        // connections.
        conn_admission: Arc::new(PublishAdmission::new(config.pub_conn_inflight_bytes)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: WriterLaneManager::new(config),
        ingress_wait: config.pub_ingress_wait,
    }
}

/// Hand a job's outcome to whoever is waiting for it.
///
/// A job acknowledged on enqueue has nobody waiting, so a failure here means
/// the client holds an ack for a record that was not written. This is the only
/// place that is visible, so it is counted and logged rather than dropped.
fn settle(
    response: Option<oneshot::Sender<anyhow::Result<()>>>,
    acked_on_enqueue: bool,
    shard: Option<&ShardKey>,
    result: anyhow::Result<()>,
) {
    match (response, result) {
        (Some(response), result) => {
            let _ = response.send(result);
        }
        (None, Err(err)) if acked_on_enqueue => {
            // `fenced`: the broker lost the shard or its lease first, so nothing
            // was written. `failed`: the write itself went wrong.
            let reason = match ClientError::from_anyhow(&err).code() {
                felix_wire::ErrorCode::ShardUnavailable => "fenced",
                _ => "failed",
            };
            metrics::counter!(ACKED_PUBLISHES_DROPPED_TOTAL, "reason" => reason).increment(1);
            tracing::warn!(
                shard = ?shard,
                reason,
                error = %err,
                "a publish acknowledged on enqueue was not written",
            );
        }
        (None, _) => {}
    }
}

/// Publishes the client was told succeeded when they were queued and that the
/// worker then could not write, by `reason`.
const ACKED_PUBLISHES_DROPPED_TOTAL: &str = "felix_broker_acked_publishes_dropped_total";

/// How much lease a publish needs left to be acknowledged before it is
/// written: long enough to wait out a full queue and then commit. Derived from
/// the two waits the broker already bounds rather than configured apart from
/// them. A `Leader` publish does not wait for a quorum, so the raw ack wait is
/// the commit allowance, not `ack_wait_timeout()`.
fn lease_headroom(config: &BrokerConfig) -> Duration {
    Duration::from_millis(
        config
            .publish_queue_wait_timeout_ms
            .saturating_add(config.ack_wait_timeout_ms),
    )
}

#[cfg(test)]
mod tests;
