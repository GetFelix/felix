//! Logical clients, and the record of what they saw.
//!
//! A [`SimClient`] stands in for an SDK client of the cluster. It publishes,
//! puts, gets and deletes through the same in-process entries the broker's
//! request handlers use (`resolve_publish` and `publish_direct`, and
//! `resolve_cache` and `apply_direct`), against whichever broker it picks. It
//! follows a redirect to the broker the router names, waits and retries when a
//! broker refuses, and gives up on a request after a deadline, as an SDK does.
//!
//! Every operation lands in the [`Recorder`], which builds the
//! [`History`] the checker reads:
//!
//! | Answer | Recorded as |
//! |---|---|
//! | An offset | the append succeeded at that offset |
//! | `NotWritten`, or never reached a broker that would take it | the append failed, definitely |
//! | `OutcomeUnknown`, a timeout, or the broker crashing meanwhile | the append is indeterminate |
//!
//! A cache put or delete that errors is indeterminate, as the checker's model
//! says: the cache has no answer that means "nothing was applied".
//!
//! Each stream shard is its own list, so a list's positions are that shard's
//! log offsets. Reads page through `read_committed` to the end of the shard on
//! the broker that serves it, which is what a subscriber replays from; since a
//! read stops only at the log's tail, it cannot drop its newest records, and
//! the history sets `exact_tails`.
//!
//! A leader that was replaced without knowing it still serves its readers up
//! to its own committed mark, which by design stops short of what its
//! successor has acknowledged ("Readers and group sessions without the
//! lease" in `docs/replication-design.md`). Such a read is not the log's tail,
//! so a read is recorded only once a round after it (or the lease, where the
//! fleet reads by lease) confirms the broker still leads the shard at the
//! generation it read at, as a state read is.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_broker_service::config::BrokerConfig;
use felix_broker_service::serving::direct_cache::{
    CacheOp, CacheResolution, apply_direct, resolve_cache,
};
use felix_broker_service::serving::direct_publish::{
    DirectPublishError, Resolution, publish_direct, resolve_publish,
};
use felix_broker_service::serving::direct_read::check_readable;
use felix_cluster::history::model::{
    Action, AppendOutcome, Consistency, Element, FOREIGN_PAYLOAD, FaultEvent, History, ListSpec, Op,
};
use felix_cluster::history::register::{RegisterAction, RegisterOp};
use felix_cluster::history::rng::Rng;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::cluster::{CACHE, NAMESPACE, NodeHandles, STREAM, SimCluster, TENANT};

/// How long a client keeps trying to find a broker that will take a request.
const FIND_WITHIN: Duration = Duration::from_secs(8);
/// How much of a shard one read page asks for.
const PAGE_BYTES: usize = 1 << 20;
/// A read that has not finished by now is given up on and not recorded.
const READ_WITHIN: Duration = Duration::from_secs(15);

/// The list a stream shard is recorded as.
pub fn list_name(shard: u32) -> String {
    format!("{STREAM}/{shard}")
}

/// The history under construction, shared by every client and the nemesis.
pub struct Recorder {
    started: Instant,
    history: Mutex<History>,
    values: AtomicU64,
    /// When an operator abandoned each list's log, on the history's clock.
    abandoned: Mutex<BTreeMap<String, u64>>,
}

impl Recorder {
    /// A recorder whose clock starts now, with one list per stream shard.
    pub fn new(shards: u32) -> Arc<Self> {
        let lists: BTreeMap<String, ListSpec> = (0..shards)
            .map(|shard| {
                (
                    list_name(shard),
                    ListSpec {
                        consistency: Consistency::Quorum,
                        base: 0,
                    },
                )
            })
            .collect();
        Arc::new(Self {
            started: Instant::now(),
            history: Mutex::new(History {
                lists,
                // Reads page to the log's tail; see the module docs.
                exact_tails: true,
                ..History::default()
            }),
            values: AtomicU64::new(0),
            abandoned: Mutex::new(BTreeMap::new()),
        })
    }

    /// Nanoseconds since the recorder was made: the history's one clock.
    pub fn now(&self) -> u64 {
        self.started.elapsed().as_nanos() as u64
    }

    /// A value no other operation uses.
    pub fn next_value(&self) -> u64 {
        self.values.fetch_add(1, Ordering::SeqCst)
    }

    pub fn fault(&self, what: impl Into<String>) {
        let at = self.now();
        self.history.lock().faults.push(FaultEvent {
            at,
            what: what.into(),
        });
    }

    fn push(&self, op: Op) {
        self.history.lock().ops.push(op);
    }

    fn push_register(&self, op: RegisterOp) {
        self.history.lock().registers.push(op);
    }

    /// Record what a final read of `shard` found.
    pub fn set_final_read(&self, shard: u32, elements: Vec<Element>) {
        self.history
            .lock()
            .final_reads
            .insert(list_name(shard), elements);
    }

    /// An operator abandoned `shard`'s log just now.
    pub fn abandoned(&self, shard: u32) {
        let at = self.now();
        self.abandoned.lock().insert(list_name(shard), at);
    }

    /// The lists whose log an operator abandoned, with when.
    pub fn abandoned_lists(&self) -> BTreeMap<String, u64> {
        self.abandoned.lock().clone()
    }

    /// The history so far, with what each abandon gave up forgotten (see
    /// [`forget_abandoned`]). Lists with no final read are left declared; use
    /// [`History::lists`] to drop them before checking.
    pub fn history(&self) -> History {
        let mut history = self.history.lock().clone();
        forget_abandoned(&mut history, &self.abandoned.lock());
        history
    }
}

/// An abandoned log loses what it acknowledged by design, so for each list in
/// `abandoned` (list, when) an append acknowledged before the abandon becomes
/// unknown (it may or may not be on the new log) and a read started before it
/// is dropped (it may have read the log that was given up). An append still
/// retrying at the abandon can only be acknowledged by the new leader, since
/// the old one is stopped first, so it is held to the new log. Everything started after
/// the abandon is checked as usual, so a write the new log loses still fails.
pub fn forget_abandoned(history: &mut History, abandoned: &BTreeMap<String, u64>) {
    history.ops.retain_mut(|op| {
        let (invoke, complete) = (op.invoke, op.complete);
        match &mut op.action {
            Action::Append { list, outcome, .. } => {
                if abandoned.get(list).is_some_and(|at| complete < *at)
                    && matches!(outcome, AppendOutcome::Ok { .. })
                {
                    *outcome = AppendOutcome::Info;
                }
                true
            }
            Action::Read { list, .. } => abandoned.get(list).is_none_or(|at| invoke >= *at),
        }
    });
}

/// A seeded logical client.
pub struct SimClient {
    id: usize,
    cluster: Arc<SimCluster>,
    recorder: Arc<Recorder>,
    rng: Rng,
    /// The broker that last took a request for each shard, tried first.
    hints: BTreeMap<u32, usize>,
}

/// What looking for a broker to take a request found out.
enum Step<T> {
    Local(T),
    /// Another broker owns it.
    Forward(String),
    Refused,
}

/// How a cache operation went.
enum Sent {
    /// No broker took it, so it did nothing.
    Nowhere,
    /// It was sent and failed or timed out, so it may have applied.
    Unknown,
    /// What it returned: a get's value, or what a delete removed.
    Answered(Option<Bytes>),
}

impl SimClient {
    pub fn new(id: usize, cluster: Arc<SimCluster>, recorder: Arc<Recorder>, rng: Rng) -> Self {
        Self {
            id,
            cluster,
            recorder,
            rng,
            hints: BTreeMap::new(),
        }
    }

    /// Run until `stop`: mostly appends, with cache puts, gets and deletes
    /// mixed in.
    pub async fn run(mut self, stop: CancellationToken) {
        loop {
            let think = self
                .rng
                .between(Duration::from_millis(30), Duration::from_millis(200));
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(think) => {}
            }
            match self.rng.below(100) {
                0..=59 => {
                    let shard = self.rng.below(u64::from(self.cluster.config.shards)) as u32;
                    self.append(shard).await;
                }
                60..=79 => self.cache_put().await,
                80..=94 => self.cache_get().await,
                _ => self.cache_delete().await,
            }
        }
    }

    /// Read random shards until `stop`, recording each full read.
    pub async fn run_reader(mut self, stop: CancellationToken) {
        loop {
            let pause = self
                .rng
                .between(Duration::from_millis(300), Duration::from_millis(1200));
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(pause) => {}
            }
            let shard = self.rng.below(u64::from(self.cluster.config.shards)) as u32;
            let invoke = self.recorder.now();
            if let Some(observed) = self.read_shard(shard).await {
                self.recorder.push(Op {
                    process: self.id,
                    invoke,
                    complete: self.recorder.now(),
                    action: Action::Read {
                        list: list_name(shard),
                        observed,
                    },
                });
            }
        }
    }

    /// Append one record to `shard` and record how it ended.
    pub async fn append(&mut self, shard: u32) {
        let value = self.recorder.next_value();
        let invoke = self.recorder.now();
        let outcome = self.try_append(shard, value).await;
        self.recorder.push(Op {
            process: self.id,
            invoke,
            complete: self.recorder.now(),
            action: Action::Append {
                list: list_name(shard),
                value,
                outcome,
            },
        });
    }

    async fn try_append(&mut self, shard: u32, value: u64) -> AppendOutcome {
        let found = self
            .find(self.hints.get(&shard).copied(), |h| async move {
                match resolve_publish(
                    &h.broker,
                    Some(&h.ingress),
                    Some(&h.lease),
                    TENANT,
                    NAMESPACE,
                    STREAM,
                    None,
                    Some(shard),
                )
                .await
                {
                    Ok(Resolution::Local(target)) => Step::Local(target),
                    Ok(Resolution::Forward(owner)) => Step::Forward(owner.node_id),
                    Err(_) => Step::Refused,
                }
            })
            .await;
        // Never reached a broker that would take it: nothing was written.
        let Some((handles, target)) = found else {
            return AppendOutcome::Fail;
        };
        self.hints.insert(shard, handles.index);
        tracing::debug!(client = self.id, shard, value, node = %handles.node_id, "append sent");
        let payloads = [payload(value, self.cluster.config.power_loss)];
        let quorum_timeout = quorum_timeout();
        let task = spawn_on(&handles.crashed, {
            let handles = handles.clone();
            async move {
                publish_direct(
                    &handles.broker,
                    Some(&handles.ingress),
                    Some(&handles.marks),
                    target,
                    &payloads,
                    None,
                    quorum_timeout,
                )
                .await
            }
        });
        let result = tokio::time::timeout(ack_wait(), task).await;
        tracing::debug!(client = self.id, shard, value, node = %handles.node_id, ?result, "append answered");
        match result {
            Ok(Ok(Some(Ok(offset)))) => AppendOutcome::Ok { offset },
            Ok(Ok(Some(Err(DirectPublishError::NotWritten(_))))) => AppendOutcome::Fail,
            _ => AppendOutcome::Info,
        }
    }

    async fn cache_put(&mut self) {
        let key = self.cache_key();
        let value = self.recorder.next_value();
        let invoke = self.recorder.now();
        let op = CacheOp::Put {
            value: Bytes::copy_from_slice(&value.to_le_bytes()),
        };
        let acknowledged = match self.cache_op(&key, op).await {
            Sent::Nowhere => return,
            Sent::Unknown => false,
            Sent::Answered(_) => true,
        };
        self.recorder.push_register(RegisterOp {
            process: self.id,
            invoke,
            complete: self.recorder.now(),
            key,
            action: RegisterAction::Put {
                value,
                acknowledged,
            },
        });
    }

    async fn cache_get(&mut self) {
        let key = self.cache_key();
        self.get(key).await;
    }

    /// Get every cache key once, for the end of a run.
    pub async fn get_every_key(&mut self) {
        for key in 0..self.cluster.config.cache_keys {
            self.get(format!("k{key}")).await;
        }
    }

    async fn get(&mut self, key: String) {
        let invoke = self.recorder.now();
        let Sent::Answered(found) = self.cache_op(&key, CacheOp::Get).await else {
            // A get that failed is not recorded.
            return;
        };
        self.recorder.push_register(RegisterOp {
            process: self.id,
            invoke,
            complete: self.recorder.now(),
            key,
            action: RegisterAction::Get {
                value: found.as_deref().map(decode),
            },
        });
    }

    async fn cache_delete(&mut self) {
        let key = self.cache_key();
        let invoke = self.recorder.now();
        let removed = match self.cache_op(&key, CacheOp::Delete).await {
            Sent::Nowhere => return,
            Sent::Unknown => None,
            Sent::Answered(removed) => Some(removed.as_deref().map(decode)),
        };
        self.recorder.push_register(RegisterOp {
            process: self.id,
            invoke,
            complete: self.recorder.now(),
            key,
            action: RegisterAction::Delete { removed },
        });
    }

    fn cache_key(&mut self) -> String {
        format!("k{}", self.rng.below(self.cluster.config.cache_keys as u64))
    }

    /// Send one cache operation to the broker that owns `key`.
    async fn cache_op(&mut self, key: &str, op: CacheOp) -> Sent {
        let found = self
            .find(None, |h| {
                let key = key.to_string();
                async move {
                    match resolve_cache(Some(&h.ingress), TENANT, NAMESPACE, CACHE, &key).await {
                        Ok(CacheResolution::Local(target)) => Step::Local(target),
                        Ok(CacheResolution::Forward(owner)) => Step::Forward(owner.node_id),
                        Err(_) => Step::Refused,
                    }
                }
            })
            .await;
        let Some((handles, target)) = found else {
            return Sent::Nowhere;
        };
        let key = key.to_string();
        let quorum_timeout = quorum_timeout();
        let task = spawn_on(&handles.crashed, {
            let handles = handles.clone();
            async move {
                apply_direct(
                    &handles.broker,
                    Some(&handles.ingress),
                    Some(&handles.marks),
                    quorum_timeout,
                    TENANT,
                    NAMESPACE,
                    CACHE,
                    &key,
                    target,
                    op,
                )
                .await
            }
        });
        match tokio::time::timeout(ack_wait(), task).await {
            Ok(Ok(Some(Ok(answer)))) => Sent::Answered(answer),
            _ => Sent::Unknown,
        }
    }

    /// Read every committed record of `shard` from the broker that serves it.
    /// `None` if no broker would, the read did not finish, or the broker could
    /// not confirm afterwards that it still led the shard.
    pub async fn read_shard(&mut self, shard: u32) -> Option<Vec<Element>> {
        let (handles, generation) = self
            .find(self.hints.get(&shard).copied(), |h| async move {
                match resolve_publish(
                    &h.broker,
                    Some(&h.ingress),
                    Some(&h.lease),
                    TENANT,
                    NAMESPACE,
                    STREAM,
                    None,
                    Some(shard),
                )
                .await
                {
                    // Only the broker is wanted; the fence entry is released.
                    // A subscription is also refused by a broker that has
                    // seen a newer generation, so ask that too.
                    Ok(Resolution::Local(target)) => {
                        let key = crate::cluster::stream_key(shard);
                        match check_readable(Some(&h.ingress), &key) {
                            Ok(()) => Step::Local(target.generation),
                            Err(_) => Step::Refused,
                        }
                    }
                    Ok(Resolution::Forward(owner)) => Step::Forward(owner.node_id),
                    Err(_) => Step::Refused,
                }
            })
            .await?;
        self.hints.insert(shard, handles.index);
        let task = spawn_on(&handles.crashed, {
            let handles = handles.clone();
            async move {
                let elements = read_all(&handles, shard).await?;
                confirm_leader(&handles, shard, generation)
                    .await
                    .then_some(elements)
            }
        });
        let result = tokio::time::timeout(READ_WITHIN, task).await;
        if tracing::enabled!(tracing::Level::DEBUG) {
            let seen = match &result {
                Ok(Ok(Some(Some(elements)))) => {
                    format!("{} records, last {:?}", elements.len(), elements.last())
                }
                other => format!("no read: {other:?}"),
            };
            let at = self
                .cluster
                .snapshot(handles.index, &crate::cluster::stream_key(shard))
                .await;
            tracing::debug!(client = self.id, shard, node = %handles.node_id, seen, ?at, "read");
        }
        match result {
            Ok(Ok(Some(Some(elements)))) => Some(elements),
            _ => None,
        }
    }

    /// Find the broker that owns what `resolve` asks about, following
    /// redirects and backing off when brokers refuse, until [`FIND_WITHIN`]
    /// runs out.
    async fn find<T, F, Fut>(&mut self, hint: Option<usize>, resolve: F) -> Option<(NodeHandles, T)>
    where
        F: Fn(NodeHandles) -> Fut,
        Fut: Future<Output = Step<T>>,
    {
        let give_up = Instant::now() + FIND_WITHIN;
        let mut target = hint.unwrap_or_else(|| self.random_node());
        let mut hops = 0;
        while Instant::now() < give_up {
            let Some(handles) = self.cluster.handles(target) else {
                self.backoff().await;
                target = self.random_node();
                continue;
            };
            let step = tokio::select! {
                step = resolve(handles.clone()) => step,
                () = handles.crashed.cancelled() => Step::Refused,
            };
            match step {
                Step::Local(found) => return Some((handles, found)),
                Step::Forward(node_id) if hops < 3 => {
                    hops += 1;
                    target = self.cluster.index_of(&node_id).unwrap_or(target);
                }
                Step::Forward(_) | Step::Refused => {
                    hops = 0;
                    self.backoff().await;
                    target = self.random_node();
                }
            }
        }
        None
    }

    fn random_node(&mut self) -> usize {
        self.rng.below(self.cluster.broker_count() as u64) as usize
    }

    async fn backoff(&mut self) {
        let wait = self
            .rng
            .between(Duration::from_millis(50), Duration::from_millis(250));
        tokio::time::sleep(wait).await;
    }
}

/// Spawn `work` as a task of its own, as the broker's request worker would
/// run it: it finishes even if the client stops waiting, and ends only if the
/// broker crashes, when it gives `None`.
fn spawn_on<T: Send + 'static>(
    crashed: &CancellationToken,
    work: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<Option<T>> {
    let crashed = crashed.clone();
    tokio::spawn(async move {
        // A crash wins over an answer ready in the same poll: a power cut
        // stops the broker inside a write, and what it would answer after
        // that never reached a client.
        tokio::select! {
            biased;
            () = crashed.cancelled() => None,
            out = work => Some(out),
        }
    })
}

/// Read `shard` on this broker from its first record to its tail, the way a
/// subscriber does: stopping at the committed mark.
async fn read_all(handles: &NodeHandles, shard: u32) -> Option<Vec<Element>> {
    read_pages(handles, shard, true).await
}

/// Whether this broker still leads `shard` at `generation`, confirmed by a
/// round (or the lease, where the fleet reads by lease) after a read took its
/// value. See the module docs for why.
async fn confirm_leader(handles: &NodeHandles, shard: u32, generation: u64) -> bool {
    let key = crate::cluster::stream_key(shard);
    let confirmed = felix_replication::quorum::confirm_state_read(
        &key,
        Some(&handles.marks),
        Some(&*handles.ingress),
    )
    .await;
    let now = handles.ingress.generation(&key);
    let led = confirmed.is_ok() && now == Some(generation);
    if !led {
        // A dropped read leaves no trace in the history, so say so here.
        tracing::debug!(
            shard,
            generation,
            ?now,
            ?confirmed,
            "read dropped: leadership not confirmed"
        );
    }
    led
}

/// Every client record in this broker's copy of `shard`, committed or not, or
/// `None` if the broker does not serve it, the read took over [`READ_WITHIN`]
/// or the broker crashed meanwhile.
pub(crate) async fn read_log(handles: &NodeHandles, shard: u32) -> Option<Vec<Element>> {
    let task = spawn_on(&handles.crashed, {
        let handles = handles.clone();
        async move { read_pages(&handles, shard, false).await }
    });
    tokio::time::timeout(READ_WITHIN, task).await.ok()?.ok()??
}

async fn read_pages(handles: &NodeHandles, shard: u32, committed: bool) -> Option<Vec<Element>> {
    let mut elements = Vec::new();
    let mut next = 0;
    loop {
        let page = if committed {
            handles
                .broker
                .read_committed(TENANT, NAMESPACE, STREAM, shard, next, PAGE_BYTES)
                .await
        } else {
            handles
                .broker
                .read_durable(TENANT, NAMESPACE, STREAM, shard, next, PAGE_BYTES)
                .await
        }
        .ok()?;
        if page.is_empty() {
            return Some(elements);
        }
        for record in page {
            elements.push(Element {
                offset: record.offset,
                value: decode(&record.payload),
                // Offsets the read passed over are generation-start records.
                skipped_before: record.offset.saturating_sub(next),
            });
            next = record.offset + 1;
        }
    }
}

/// The payload of an append of `value`: the value, and in a power-loss run
/// padding to 1 to 4 KiB, so an append spans several sectors and a cut can
/// lose one sector of it before another that arrived.
fn payload(value: u64, padded: bool) -> Bytes {
    let mut out = value.to_le_bytes().to_vec();
    if padded {
        out.extend(padding(value));
    }
    Bytes::from(out)
}

/// The padding after `value`, a function of the value alone, so a read can
/// tell an intact record from one with damaged bytes.
fn padding(value: u64) -> impl Iterator<Item = u8> {
    let mut mix = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mix = (mix ^ (mix >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mix ^= mix >> 31;
    let len = 1024 - 8 + (mix % 3073) as usize;
    (0..len).map(move |i| (mix >> (8 * (i % 8))) as u8 ^ i as u8)
}

fn decode(payload: &[u8]) -> u64 {
    let Some(value) = payload
        .get(..8)
        .and_then(|head| head.try_into().ok())
        .map(u64::from_le_bytes)
    else {
        return FOREIGN_PAYLOAD;
    };
    let rest = &payload[8..];
    if rest.is_empty() || rest.iter().copied().eq(padding(value)) {
        value
    } else {
        FOREIGN_PAYLOAD
    }
}

fn quorum_timeout() -> Duration {
    Duration::from_millis(BrokerConfig::default().publish_quorum_timeout_ms.max(1))
}

/// How long a client waits for an acknowledgement, as the SDK's publish does.
fn ack_wait() -> Duration {
    BrokerConfig::default().ack_wait_timeout()
}

#[cfg(test)]
mod tests;
