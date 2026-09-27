//! What a publish does once the scheduler hands it to an executor, and the
//! publish context every connection derives its own from.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::task::TaskTracker;

use super::scheduler::{LaneGuard, PublishScheduler};
use super::{PublishAdmission, PublishContext, PublishJob, PublishTarget, SubscriptionLimiter};
use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::subscribe::WriterLaneManager;
use crate::serving::quic::preauth::PreAuthGate;
use crate::shards::ShardKey;
use crate::shards::lifecycle::fence::{self, FenceGuard};

/// The publish context with untracked executors, for tests that do not drain.
#[cfg(test)]
pub(crate) fn build_publish_context(
    broker: Arc<Broker>,
    config: &BrokerConfig,
    cluster: ClusterContext,
) -> PublishContext {
    build_tracked_publish_context(broker, config, cluster, &TaskTracker::new())
}

/// Start the publish scheduler and build the context that feeds it, with the
/// executors and every task they hand work to tracked by `work`.
///
/// The scheduler closes once every context using it is gone, and its
/// executors exit when what is queued has run, so once the connections have
/// ended, closing `work` and waiting on it waits for every queued publish to
/// be settled. A publish acknowledged on enqueue is only safe on a clean stop
/// if the drain waits for it.
pub(crate) fn build_tracked_publish_context(
    broker: Arc<Broker>,
    config: &BrokerConfig,
    cluster: ClusterContext,
    work: &TaskTracker,
) -> PublishContext {
    let ClusterContext {
        ingress,
        peers,
        lease,
        marks,
        client_endpoints,
    } = cluster;
    let quorum_timeout = std::time::Duration::from_millis(config.publish_quorum_timeout_ms.max(1));
    // Process-wide, not per connection: executors bound how many publishes
    // are inside broker state at once, and one pool per connection turned
    // more publisher connections into lock contention there.
    let shards = crate::serving::core_shards::global_shards(config);
    let executors = config.pub_workers_per_conn.max(1);
    let share = config.pub_queue_depth.max(1);
    let scheduler = Arc::new(PublishScheduler::new(
        shards.as_ref().map_or(1, |shards| shards.len()),
        // `pub_queue_depth` per executor in all, and one executor's worth
        // guaranteed to every tenant.
        share.saturating_mul(executors),
        share,
    ));
    let lanes = Arc::new(LaneWork {
        broker,
        peers: peers.clone(),
        marks: marks.clone(),
        ingress: ingress.clone(),
        quorum_timeout,
        // A forward that outlasts this is cut off with its own answer rather
        // than the waiter's -- see `BrokerConfig::forward_budget`.
        forward_budget: config.forward_budget(),
        flush_concurrency: config.pub_flush_concurrency.max(1),
        flush_slots: Mutex::new(FlushSlots::default()),
        work: work.clone(),
    });
    for (index, partition) in scheduler.partitions().iter().enumerate() {
        let runtime = shards
            .as_ref()
            .map(|shards| shards.handle_for(index as u64).clone());
        for executor in 0..executors {
            let partition = Arc::clone(partition);
            let lanes = Arc::clone(&lanes);
            let make_executor = move || {
                let partition = Arc::clone(&partition);
                let lanes = Arc::clone(&lanes);
                async move {
                    while let Some((job, lane)) = partition.next().await {
                        lanes.run(job, lane).await;
                    }
                }
            };
            work.spawn(supervise(
                index * executors + executor,
                runtime.clone(),
                make_executor,
            ));
        }
    }
    PublishContext {
        ingress,
        peers,
        lease,
        lease_headroom: lease_headroom(config),
        client_endpoints,
        marks,
        quorum_timeout,
        scheduler,
        wait_timeout: Duration::from_millis(config.publish_queue_wait_timeout_ms),
        admission: Arc::new(PublishAdmission::new(config.pub_inflight_bytes)),
        // Placeholder; `handle_connection` replaces this (and `subscriptions`/`lane_manager`)
        // with fresh per-connection instances before this context is used by any stream on
        // that connection. These template values are never themselves shared across
        // connections.
        conn_admission: Arc::new(PublishAdmission::new(config.pub_conn_inflight_bytes)),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: WriterLaneManager::new(config),
        ingress_wait: config.pub_ingress_wait,
        preauth: Arc::new(PreAuthGate::new(config)),
        // The accept loop swaps in the broker-wide instance.
        tenant_rates: Arc::new(crate::serving::limits::TenantRates::new(&config.limits)),
    }
}

/// What a job does on its lane, and everything it needs to do it.
///
/// Each kind of job holds its lane only as long as order requires, and an
/// executor only as long as that takes without waiting on anything slow.
/// The rest is handed to a task, so the lane's next job, and every other
/// lane, go on without it.
struct LaneWork {
    broker: Arc<Broker>,
    peers: Option<Arc<crate::peer::PeerPool>>,
    marks: Option<Arc<crate::replication::quorum::QuorumMarks>>,
    ingress: Option<Arc<crate::shards::routing::IngressRouter>>,
    quorum_timeout: Duration,
    forward_budget: Duration,
    flush_concurrency: usize,
    flush_slots: Mutex<FlushSlots>,
    work: TaskTracker,
}

impl LaneWork {
    async fn run(self: &Arc<Self>, mut job: PublishJob, lane: LaneGuard) {
        let held = job.fenced.take();
        match &job.target {
            PublishTarget::Resolved { handle, .. } if handle.is_durable() => {
                let slots = self.flush_slots(handle.id());
                match Arc::clone(&slots).try_acquire_owned() {
                    Ok(slot) => self.claim(job, held, slot, lane).await,
                    // This shard has as many flushes outstanding as it may.
                    // Wait for one off the executor; the lane stays held,
                    // because the claim still has to go in order.
                    Err(_) => {
                        let this = Arc::clone(self);
                        self.work.spawn(async move {
                            let slot = slots
                                .acquire_owned()
                                .await
                                .expect("flush slots are never closed");
                            this.claim(job, held, slot, lane).await;
                        });
                    }
                }
            }
            PublishTarget::Resolved { .. } => self.publish_in_memory(job, held, lane).await,
            // Held for the whole write: the sequence check and the append are
            // one ordered step, and that can include a flush, so it runs off
            // the executor.
            PublishTarget::Idempotent { .. } => {
                let this = Arc::clone(self);
                self.work
                    .spawn(async move { this.publish_idempotent(job, held, lane).await });
            }
            // Held for the whole round trip. A forward can be retried or
            // redirected, so a later batch sent before this one is answered
            // could land ahead of it. Only this shard's later publishes wait.
            PublishTarget::Forward { target, .. } => match &self.peers {
                Some(pool) => {
                    let pool = Arc::clone(pool);
                    let budget = self.forward_budget;
                    self.work.spawn(async move {
                        let result = forward(&pool, &job, budget).await;
                        drop(lane);
                        settle(job.response, job.acked_on_enqueue, None, result);
                    });
                }
                // The route said forward and there is nothing to forward
                // with. Refusing beats writing another broker's shard locally.
                None => {
                    let refused = ClientError::internal(format!(
                        "no peer transport: this broker cannot forward to {}",
                        target.node_id
                    ))
                    .with_retry(felix_wire::RetryClass::Retry);
                    drop(lane);
                    settle(
                        job.response,
                        job.acked_on_enqueue,
                        None,
                        Err(refused.into()),
                    );
                }
            },
            #[cfg(test)]
            PublishTarget::Named { stream, .. } if stream == tests::PANICKING_STREAM => {
                panic!("injected publish worker panic")
            }
            #[cfg(test)]
            PublishTarget::Named {
                tenant_id,
                namespace,
                stream,
            } => {
                let result = self
                    .broker
                    .publish_batch(tenant_id, namespace, stream, 0, &job.payloads)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
                drop(lane);
                settle(job.response, job.acked_on_enqueue, None, result);
            }
        }
    }

    /// Claim a durable publish's offsets on its lane, then let the lane go and
    /// finish the flush, fanout and quorum wait on a task of its own, so the
    /// flushes overlap and group commit has something to coalesce (#535).
    async fn claim(
        &self,
        job: PublishJob,
        mut held: Option<FenceGuard>,
        slot: OwnedSemaphorePermit,
        lane: LaneGuard,
    ) {
        let PublishTarget::Resolved {
            handle,
            shard,
            generation,
            ..
        } = &job.target
        else {
            unreachable!("only durable local publishes are claimed")
        };
        // Held until the publish is durable and fanned out, so a drained
        // report cannot go out while it is landing. Entering is also the
        // lease check, against the clock: see `fence`.
        let fenced = match fence::enter_or_keep(
            &mut held,
            self.ingress.as_deref(),
            shard.as_ref(),
            *generation,
        ) {
            Ok(fenced) => fenced,
            Err(refused) => {
                drop(lane);
                settle(
                    job.response,
                    job.acked_on_enqueue,
                    shard.as_ref(),
                    Err(refused.into()),
                );
                return;
            }
        };
        // The only ordered part: offsets are consumed here, so the order
        // claims return in is the order records land on disk.
        let claimed = self.broker.claim_publish(handle, &job.payloads).await;
        drop(lane);
        let claimed = match claimed {
            Ok(claimed) => claimed,
            Err(err) => {
                settle(
                    job.response,
                    job.acked_on_enqueue,
                    shard.as_ref(),
                    Err(err.into()),
                );
                return;
            }
        };
        let broker = Arc::clone(&self.broker);
        let handle = handle.clone();
        let shard = shard.clone();
        let marks = self.marks.clone();
        let ingress = self.ingress.clone();
        let quorum_timeout = self.quorum_timeout;
        let response = job.response;
        let acked_on_enqueue = job.acked_on_enqueue;
        self.work.spawn(async move {
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
            drop(slot);
        });
    }

    /// An ephemeral stream's publish has no flush to wait for, so it runs on
    /// the executor; only a quorum wait is handed off.
    async fn publish_in_memory(
        &self,
        job: PublishJob,
        mut held: Option<FenceGuard>,
        lane: LaneGuard,
    ) {
        let PublishTarget::Resolved {
            handle,
            shard,
            generation,
            ..
        } = &job.target
        else {
            unreachable!("only local publishes are written here")
        };
        // The commit fence, and the authoritative lease check. Everything
        // between admission and here can take arbitrarily long -- a full
        // queue, a slow fsync, a suspended process -- so a lease that was
        // valid on the way in may have lapsed, and a broker that writes after
        // losing it is writing a shard someone else may lead.
        let published = match fence::enter_or_keep(
            &mut held,
            self.ingress.as_deref(),
            shard.as_ref(),
            *generation,
        ) {
            Err(refused) => Err(refused.into()),
            Ok(fenced) => {
                let published = self
                    .broker
                    .publish_batch_with_outcome(handle, &job.payloads)
                    .await;
                drop(fenced);
                published.map_err(anyhow::Error::from)
            }
        };
        drop(lane);
        match published {
            Ok(outcome) if handle.consistency() == felix_broker::ConsistencyLevel::Quorum => {
                let handle = handle.clone();
                let shard = shard.clone();
                let marks = self.marks.clone();
                let ingress = self.ingress.clone();
                let quorum_timeout = self.quorum_timeout;
                self.work.spawn(async move {
                    let result = crate::replication::quorum::await_quorum(
                        &handle,
                        shard.as_ref(),
                        &outcome,
                        marks.as_deref(),
                        ingress.as_deref(),
                        quorum_timeout,
                    )
                    .await;
                    settle(job.response, job.acked_on_enqueue, shard.as_ref(), result);
                });
            }
            Ok(_) => settle(job.response, job.acked_on_enqueue, shard.as_ref(), Ok(())),
            Err(err) => settle(job.response, job.acked_on_enqueue, shard.as_ref(), Err(err)),
        }
    }

    async fn publish_idempotent(
        &self,
        job: PublishJob,
        mut held: Option<FenceGuard>,
        lane: LaneGuard,
    ) {
        let PublishTarget::Idempotent {
            handle,
            shard,
            generation,
            producer_id,
            sequence,
            reuse,
            ..
        } = &job.target
        else {
            unreachable!("only idempotent publishes are written here")
        };
        // The same commit fence as a plain publish: see above.
        let published = match fence::enter_or_keep(
            &mut held,
            self.ingress.as_deref(),
            shard.as_ref(),
            *generation,
        ) {
            Err(refused) => Err(refused.into()),
            Ok(fenced) => {
                let published = self
                    .broker
                    .publish_batch_idempotent(
                        handle,
                        *producer_id,
                        *sequence,
                        &job.payloads,
                        *reuse,
                    )
                    .await;
                drop(fenced);
                published.map_err(anyhow::Error::from)
            }
        };
        drop(lane);
        let result = match published {
            // A duplicate waits on the same quorum the original did: its
            // offsets are the original's, and the answer must mean the same
            // thing.
            Ok(idempotent) => {
                crate::replication::quorum::await_quorum(
                    handle,
                    shard.as_ref(),
                    &idempotent.outcome,
                    self.marks.as_deref(),
                    self.ingress.as_deref(),
                    self.quorum_timeout,
                )
                .await
            }
            Err(err) => Err(err),
        };
        settle(job.response, job.acked_on_enqueue, shard.as_ref(), result);
    }

    /// The flush slots of the shard with this handle id.
    fn flush_slots(&self, handle_id: u64) -> Arc<Semaphore> {
        self.flush_slots
            .lock()
            .get_or_insert(handle_id, self.flush_concurrency)
    }
}

/// Per-shard semaphores bounding durable publishes awaiting their flush.
///
/// Per shard rather than per executor, so one shard with a slow device uses
/// up its own slots and nobody else's.
#[derive(Default)]
struct FlushSlots {
    slots: HashMap<u64, Arc<Semaphore>>,
    /// Map size at which to next sweep out shards with nothing in flight.
    prune_at: usize,
}

impl FlushSlots {
    fn get_or_insert(&mut self, handle_id: u64, permits: usize) -> Arc<Semaphore> {
        if let Some(slots) = self.slots.get(&handle_id) {
            return Arc::clone(slots);
        }
        // Kept while a shard has flushes in flight, so the bound holds across
        // a lane that empties and refills; swept in bulk so the map tracks
        // busy shards, not every shard ever written.
        if self.slots.len() >= self.prune_at {
            self.slots
                .retain(|_, slots| slots.available_permits() < permits);
            self.prune_at = (self.slots.len() * 2).max(FLUSH_SLOTS_PRUNE_MIN);
        }
        let slots = Arc::new(Semaphore::new(permits));
        self.slots.insert(handle_id, Arc::clone(&slots));
        slots
    }
}

/// Below this many shards the flush-slot map is not worth sweeping.
const FLUSH_SLOTS_PRUNE_MIN: usize = 1024;

/// Send a forwarded batch and wait for the owner's answer.
async fn forward(
    pool: &crate::peer::PeerPool,
    job: &PublishJob,
    budget: Duration,
) -> anyhow::Result<()> {
    let PublishTarget::Forward {
        target,
        key,
        ack,
        credential,
    } = &job.target
    else {
        unreachable!("only forwards are forwarded")
    };
    crate::serving::forward::forward_publish(
        pool,
        target,
        key,
        *ack,
        credential,
        job.payloads.clone(),
        budget,
    )
    .await
    .map(|_| ())
    .map_err(anyhow::Error::from)
}

/// Run a publish executor, starting a fresh one on the same queue whenever it
/// panics.
///
/// An executor that died unsupervised would take its share of the broker's
/// publish throughput with it while `/ready` stayed green. The job it was
/// running is lost -- its waiter sees the response dropped, and its lane guard
/// frees the lane -- but the rest of the queue is served.
async fn supervise<F, Fut>(executor: usize, runtime: Option<tokio::runtime::Handle>, make_worker: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    loop {
        let worker = make_worker();
        let joined = match &runtime {
            Some(runtime) => runtime.spawn(worker).await,
            None => tokio::spawn(worker).await,
        };
        match joined {
            // Every sender is gone and the queue is drained.
            Ok(()) => return,
            Err(err) if err.is_panic() => {
                metrics::counter!(PUBLISH_WORKER_RESTARTS_TOTAL).increment(1);
                tracing::error!(
                    executor,
                    "publish executor panicked; starting a replacement"
                );
            }
            // Cancelled: the runtime is shutting down.
            Err(_) => return,
        }
    }
}

/// Publish executors restarted after a panic.
const PUBLISH_WORKER_RESTARTS_TOTAL: &str = "felix_broker_publish_worker_restarts_total";

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

/// Publishes the client was told succeeded when they were queued and that
/// could not then be written, by `reason`.
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
