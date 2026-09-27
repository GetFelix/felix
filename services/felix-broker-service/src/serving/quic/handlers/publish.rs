//! Publish path (ingress) helpers for the QUIC transport.
//!
//! This module is the “publish ingestion glue” between QUIC stream handlers and the broker core.
//! It owns:
//! - **Ingress enqueue policy** (Drop/Fail/Wait/Backpressure) into the publish worker queues.
//! - **Worker sharding** (deterministic hashing of tenant/namespace/stream to pick a worker).
//! - **Ack semantics + backpressure** for control-stream publishes (including commit-ack waiting).
//! - **Depth tracking** for ingress and outbound-ack queues (local + global gauges).
//!
//! Publish arrives in two main shapes:
//! - **Control stream publish** (bi-directional): publish messages may request an ack (`ack != None`)
//!   and can be configured as either enqueue-ack or commit-ack (`ack_on_commit`).
//! - **Uni-directional publish stream** (ingress-only): fire-and-forget publishes with **no acks**.
//!
//! Ack meaning depends on configuration:
//! - `ack_on_commit = false` → **enqueue-ack**: an ack means “accepted into the ingress queue”.
//!   Lowest latency, but does not guarantee the publish ultimately commits.
//! - `ack_on_commit = true` → **commit-ack**: an ack means “the publish job completed/committed”.
//!   Higher latency; bounded by `ack_waiters` and `ack_waiter_tx` to avoid unbounded in-flight acks.
//!
//! Backpressure strategy:
//! - Ingress queue uses `EnqueuePolicy` (Drop/Fail/Wait/Backpressure) to shed load, wait within
//!   a single bounded budget, or apply true unbounded-but-cancellable backpressure.
//! - Outbound ack queue maintains a high-water throttle signal (`ack_throttle_tx`) and records
//!   enqueue failures/timeouts to decide when to cooperatively cancel the control stream.
//! - Depth counters are tracked both per-stream and globally to support observability and tuning.
//!
//! Submodules:
//! - `admission`: byte-budget admission control and the subscription cap.
//! - `ingress`: worker sharding, bounded enqueue, in-flight depth accounting.
//! - `ack`: ack envelopes, waiter protocol, and the ack timeout window.
//! - `route`: whether a publish is served here, forwarded, or refused.
//! - `stream_cache`: the per-connection cache of resolved stream handles.
//! - `control`: acked publish handlers on the bi-directional control stream.
//! - `uni`: fire-and-forget publish handlers on uni-directional streams.
//! - `worker`: the process-wide publish worker pool.
//!
//! The connection and stream layers address these through the re-exports below,
//! so `handlers::publish::<name>` stays the stable path for the whole transport.

mod ack;
mod admission;
mod control;
mod ingress;
mod route;
mod stream_cache;
mod uni;
mod worker;

pub(crate) use ack::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, AckWaiterResult, Outgoing,
    handle_ack_enqueue_result, send_outgoing_best_effort, send_outgoing_critical,
};
pub(crate) use admission::{PublishAdmission, SubscriptionLimiter};
pub(crate) use control::{
    handle_acked_binary_publish_batch_control, handle_binary_publish_batch_control,
    handle_publish_batch_message, handle_publish_message,
};
pub(crate) use ingress::{PublishTarget, decrement_depth, reset_local_depth_only};
pub(crate) use stream_cache::StreamHandleCache;
pub(crate) use uni::{
    handle_binary_publish_batch_uni, handle_publish_batch_message_uni, handle_publish_message_uni,
};
pub(crate) use worker::build_publish_context;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::subscribe::WriterLaneManager;
use crate::serving::quic::preauth::PreAuthGate;
use crate::shards::routing::IngressRouter;
use ack::EnqueuePolicy;
use admission::AdmissionPermit;
use route::Authority;

/// Shared publish-ingress configuration and worker queue handles.
///
/// - `workers`: per-worker `mpsc::Sender<PublishJob>` queues.
/// - `worker_count`: cached length for fast hashing.
/// - `depth`: best-effort local depth tracking for this publish queue set.
/// - `wait_timeout`: the *total* budget for one `EnqueuePolicy::Wait` enqueue, spanning
///   admission and the queue send together. Not used by `Backpressure`, which has no timer.
/// - `admission`: shared in-flight-byte budget across all workers (see [`PublishAdmission`]).
/// - `conn_admission`: this connection's slice of `admission`. `workers`/`admission`/`depth` are
///   intentionally process-wide (see `build_publish_context`'s note on avoiding per-connection
///   worker pools), but that means nothing bounds how much of the shared budget one connection
///   can occupy. `conn_admission` is constructed fresh per connection
///   (`handle_connection`) and closes that gap without touching the shared worker pool.
#[derive(Clone)]
pub(crate) struct PublishContext {
    /// Cluster ownership, when this broker is a member.
    ///
    /// `None` on a single-node broker, which is the default: there is nothing
    /// to resolve against, and the gate costs one null check.
    pub(crate) ingress: Option<Arc<IngressRouter>>,
    /// Connections to peer brokers, when this broker is in a cluster. Present so
    /// the handlers can tell "forwardable" from "cannot forward" before
    /// enqueueing rather than after.
    pub(crate) peers: Option<Arc<crate::peer::PeerPool>>,
    /// This broker's authority to serve the shards it leads.
    ///
    /// `None` on a single-node broker, which leads by construction and has
    /// nobody to lose a shard to.
    pub(crate) lease: Option<Arc<crate::cluster::lease::LeaseState>>,
    /// Lease a publish must have left to be acknowledged on enqueue. See
    /// [`PublishContext::must_wait_for_write`].
    pub(crate) lease_headroom: Duration,
    /// Where a client may connect, for answering `Topology` on the control
    /// stream. Nothing on the publish path reads it; it rides here because this
    /// is the per-connection bundle of what the cluster makes available, beside
    /// `ingress` and `peers`.
    pub(crate) client_endpoints: Option<Arc<crate::cluster::client_endpoints::ClientEndpoints>>,
    /// How far a majority of each shard's replica set has got, for a write
    /// that must not be acknowledged before it does. `None` off a cluster.
    pub(crate) marks: Option<Arc<crate::replication::quorum::QuorumMarks>>,
    /// How long such a write waits for its majority before saying it cannot
    /// confirm one.
    pub(crate) quorum_timeout: Duration,
    pub(crate) workers: Arc<Vec<mpsc::Sender<PublishJob>>>,
    pub(crate) worker_count: usize,
    pub(crate) depth: Arc<AtomicUsize>,
    pub(crate) wait_timeout: Duration,
    pub(crate) admission: Arc<PublishAdmission>,
    pub(crate) conn_admission: Arc<PublishAdmission>,
    /// This connection's subscription-count limiter (see [`SubscriptionLimiter`]). Bundled here
    /// because `PublishContext` is already the per-connection context threaded down to the
    /// control-stream loop that handles `Subscribe` messages.
    pub(crate) subscriptions: Arc<SubscriptionLimiter>,
    /// This connection's writer-lane manager for subscription delivery (see
    /// [`WriterLaneManager`]). One instance per connection, constructed fresh in
    /// `handle_connection` — see that type's doc comment for why it's no longer a
    /// process-wide cache.
    pub(crate) lane_manager: Arc<WriterLaneManager>,
    /// When true, un-acked publishes wait for ingress capacity instead of being
    /// shed. Production keeps this off so fire-and-forget load sheds visibly under
    /// overload; benchmarks and lossless pipelines turn it on so backpressure
    /// propagates through QUIC flow control to the publisher.
    ///
    /// The wait is [`EnqueuePolicy::Backpressure`]: unbounded but cancellable. It
    /// deliberately has no timeout, because an unacked publish has no channel on
    /// which to report one — a bounded wait here could only end in a silent drop,
    /// which is the opposite of what enabling this is asking for.
    pub(crate) ingress_wait: bool,
    /// What this connection may cost before it authenticates. Rides here for
    /// the same reason as `subscriptions`: this is the per-connection bundle
    /// every stream loop already has.
    pub(crate) preauth: Arc<PreAuthGate>,
}

impl PublishContext {
    /// Derive this connection's context from the process-wide one.
    ///
    /// Only the per-connection limits are fresh: its slice of the publish byte
    /// budget, its subscription limiter, and its writer lanes. Everything else —
    /// the worker queues, the shared budget, and **the cluster view** — is
    /// carried through.
    ///
    /// The cluster view is the part worth stating. `ingress`, `peers`, and
    /// `lease` decide whether a publish is served here, refused, or forwarded,
    /// and dropping any of them here disables shard ownership for every client
    /// connection: the gate keeps passing its own tests while no broker ever
    /// refuses or forwards a shard it does not own.
    pub(crate) fn for_connection(&self, config: &crate::config::BrokerConfig) -> Self {
        Self {
            conn_admission: Arc::new(PublishAdmission::new(config.pub_conn_inflight_bytes)),
            subscriptions: Arc::new(SubscriptionLimiter::new()),
            lane_manager: WriterLaneManager::new(config),
            preauth: Arc::new(PreAuthGate::new(config)),
            ..self.clone()
        }
    }

    /// Overflow policy for publishes that carry no ack (fire-and-forget).
    /// Unacked publishes get `Backpressure`, never `Wait`: with no ack there is no
    /// channel on which to report a timeout, so a bounded wait could only end in a
    /// silent drop. See [`EnqueuePolicy`].
    pub(crate) fn overflow_policy(&self) -> EnqueuePolicy {
        if self.ingress_wait {
            EnqueuePolicy::Backpressure
        } else {
            EnqueuePolicy::Drop
        }
    }

    /// True when a publish that would be acknowledged on enqueue should wait
    /// for its write instead, because the lease may run out first.
    ///
    /// A job whose lease lapses in the queue is refused at the worker and must
    /// be: another broker may lead the shard by then. An ack already sent for
    /// it would be a lie, so near the end of the lease the client waits and
    /// hears the refusal. The headroom is capped at half the usable lease so
    /// that, at any sensible heartbeat cadence, a broker whose renewals are
    /// landing never trips it.
    ///
    /// Reads the clock, not the cached flag: the flag can be a refresh
    /// interval behind, which is the window this exists to close. Callers ask
    /// only after every cheaper reason to wait has said no.
    pub(crate) fn must_wait_for_write(&self) -> bool {
        self.lease
            .as_ref()
            .is_some_and(|lease| lease.remaining() < self.lease_headroom.min(lease.usable() / 2))
    }

    pub(crate) fn authority(&self) -> Authority<'_> {
        Authority {
            ingress: self.ingress.as_deref(),
            lease: self.lease.as_deref(),
        }
    }
}

/// Work item consumed by publish workers.
///
/// A publish job is the unit the broker’s ingress pipeline processes:
/// - It identifies the target stream with a resolved handle.
/// - It carries one or more payloads (single publish or batch).
/// - `response` is **only** used when the publish was received on the control stream and the
///   client requested an ack in commit-ack mode (`ack_on_commit = true`).
///
/// For uni-stream publishes and enqueue-ack mode, `response` is `None`.
pub(crate) struct PublishJob {
    pub(crate) target: PublishTarget,
    pub(crate) payloads: Vec<Bytes>,
    pub(crate) response: Option<oneshot::Sender<Result<()>>>,
    /// The client was told this job succeeded when it was queued. If the
    /// worker then cannot write it, nobody hears, so the worker counts it.
    pub(crate) acked_on_enqueue: bool,
    /// Held from `enqueue_publish` admission until this job finishes processing (or is dropped
    /// without ever being enqueued). See [`PublishAdmission`].
    pub(crate) admission_permit: Option<AdmissionPermit>,
    /// The shard's write fence, entered at admission when the publish is
    /// acknowledged before it is written. See `enqueue_publish`.
    pub(crate) fenced: Option<crate::shards::lifecycle::fence::FenceGuard>,
}

/// Count a publish that arrived on the JSON encoding.
///
/// The data path is binary as of 0.5.0 and no Felix client emits a JSON publish
/// unless it is talking to a broker that never advertised the binary frame. The
/// arm cannot be removed on that reasoning alone, though: `ORIGINAL_V1_FLAGS` is
/// frozen, so a client older than the flags is entitled to keep sending JSON
/// forever. This counter is what turns "nothing should be sending these" into
/// "nothing in this deployment is", which is the precondition for ever dropping
/// it. `frame` separates the single publish from the batch, because a client
/// still on the single form is a different (older) client.
pub(crate) fn record_json_publish(frame: &'static str) {
    metrics::counter!("felix_broker_json_publishes_total", "frame" => frame).increment(1);
}

#[cfg(test)]
mod tests;
