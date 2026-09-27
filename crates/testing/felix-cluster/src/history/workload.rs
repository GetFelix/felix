//! The clients: concurrent appenders and readers that record every operation
//! they run, and the reads the campaign uses to take the final state.
//!
//! Half the clients publish plainly and never re-send a publish that may have
//! landed; the other half use idempotent producers and re-send until answered.
//! The plain ones are what exercise rule 6, since only they ever record a
//! definite failure: an idempotent producer's error may follow an earlier
//! attempt that landed, so every error it returns is recorded as unknown.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use felix_client::{BrokerError, ClusterClient, IdempotentProducer, NotLeaderError, RetryClass};
use felix_wire::AckMode;

use super::model::{Action, AppendOutcome, Element, FOREIGN_PAYLOAD, FaultEvent, Op};
use super::rng::Rng;
use crate::client;

/// Every workload payload starts with this, so a read can tell a value from
/// anything else that reached the stream.
const PAYLOAD_PREFIX: &str = "felix-history-";

/// Percent of operations that are reads.
const READ_PERCENT: u64 = 20;

/// How long a client may take to reach a broker before trying again.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Consecutive failed operations after which a client reconnects from the
/// current address book: a restarted broker listens on new ports, so the
/// addresses a client started with go stale.
const FAILURES_BEFORE_RECONNECT: u32 = 3;

/// Sends of one idempotent batch before its value is recorded as unknown.
const IDEMPOTENT_ATTEMPTS: u32 = 3;

/// How long a read with no known tail waits for one more record.
const READ_IDLE: Duration = Duration::from_millis(500);

/// Percent of reads that start at the list's base rather than near its tail.
/// The rest start at most `RECENT_WINDOW` records back, so a long campaign's
/// reads do not each replay, and the checker hold, the whole list.
const FULL_READ_PERCENT: u64 = 25;
const RECENT_WINDOW: u64 = 256;

/// How a client publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClientKind {
    /// `ClusterClient::publish`: never re-sent once it may have landed.
    Plain,
    /// An idempotent producer, re-sent under the same sequence.
    Idempotent,
}

/// What every client shares: where the cluster is, and the record.
pub(super) struct Workload {
    tenant_id: String,
    namespace: String,
    token: String,
    lists: Vec<String>,
    /// Records below a list's base were there before the run.
    bases: RwLock<Vec<(String, u64)>>,
    op_timeout: Duration,
    /// Brokers' client addresses, refreshed by the nemesis after a restart.
    addrs: RwLock<Vec<SocketAddr>>,
    recorder: Recorder,
    /// The furthest tail any read has reported, per list, in `lists` order.
    tails: Vec<AtomicU64>,
    stop: AtomicBool,
    next_value: AtomicU64,
}

impl Workload {
    pub(super) fn new(
        tenant_id: String,
        namespace: String,
        token: String,
        lists: Vec<String>,
        op_timeout: Duration,
        addrs: Vec<SocketAddr>,
    ) -> Self {
        Self {
            tenant_id,
            namespace,
            token,
            bases: RwLock::new(Vec::new()),
            tails: lists.iter().map(|_| AtomicU64::new(0)).collect(),
            lists,
            op_timeout,
            addrs: RwLock::new(addrs),
            recorder: Recorder::new(),
            stop: AtomicBool::new(false),
            next_value: AtomicU64::new(0),
        }
    }

    pub(super) fn set_addrs(&self, addrs: Vec<SocketAddr>) {
        *self.addrs.write().expect("address book lock") = addrs;
    }

    /// Ignore everything below `base` in `list` from now on.
    pub(super) fn set_base(&self, list: &str, base: u64) {
        let mut bases = self.bases.write().expect("base lock");
        bases.retain(|(name, _)| name != list);
        bases.push((list.to_string(), base));
    }

    pub(super) fn now(&self) -> u64 {
        self.recorder.now()
    }

    pub(super) fn fault(&self, what: String) {
        self.recorder.fault(what);
    }

    pub(super) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Everything recorded, in completion order.
    pub(super) fn take(&self) -> (Vec<Op>, Vec<FaultEvent>) {
        self.recorder.take()
    }

    /// Run one client until [`Self::stop`].
    pub(super) async fn run_client(self: Arc<Self>, process: usize, kind: ClientKind, rng: Rng) {
        let mut client = Client {
            workload: &self,
            process,
            kind,
            rng,
        };
        client.run().await;
    }

    /// Read `list` from `addr`, from offset `from` (or its first record) to
    /// the tail the broker reported when the subscription was registered.
    ///
    /// `complete` is true only when the read reached that tail, which is what
    /// a final read has to be: anything short of it is a read with a hole.
    pub(super) async fn read(
        &self,
        addr: SocketAddr,
        list: &str,
        from: Option<u64>,
        budget: Duration,
    ) -> Result<Read> {
        let deadline = tokio::time::Instant::now() + budget;
        let base = self.base(list);
        let start = match from {
            Some(offset) => felix_client::StartPosition::Offset(offset),
            None => felix_client::StartPosition::Earliest,
        };
        let mut subscription = match self.subscribe(addr, list, start, deadline).await {
            Err(err) => match err.downcast_ref::<NotLeaderError>() {
                // One hop, the way a cluster client would follow it.
                Some(NotLeaderError {
                    addr: Some(leader), ..
                }) => {
                    let leader: SocketAddr = leader.parse().context("leader address")?;
                    self.subscribe(leader, list, start, deadline).await?
                }
                _ => return Err(err),
            },
            Ok(subscription) => subscription,
        };
        let tail = subscription.1.live_offset();
        if let (Some(tail), Some(index)) = (tail, self.lists.iter().position(|l| l == list)) {
            self.tails[index].fetch_max(tail, Ordering::Relaxed);
        }

        let mut elements = Vec::new();
        let mut next = from.unwrap_or(0);
        loop {
            if let Some(tail) = tail
                && next >= tail
            {
                return Ok(Read {
                    elements,
                    complete: true,
                    tail: Some(tail),
                });
            }
            // A known tail bounds the read by the deadline alone; without one,
            // a quiet stream is the only sign it has all been delivered.
            let wait = match tail {
                Some(_) => deadline.saturating_duration_since(tokio::time::Instant::now()),
                None => READ_IDLE,
            };
            let event = match tokio::time::timeout(wait, subscription.1.next_event()).await {
                Ok(Ok(Some(event))) => event,
                _ => break,
            };
            let offset = event
                .offset
                .ok_or_else(|| anyhow!("{list} delivered a record without an offset"))?;
            next = offset + 1;
            if offset < base {
                continue;
            }
            elements.push(Element {
                offset,
                value: decode(&event.payload),
            });
        }
        Ok(Read {
            elements,
            complete: tail.is_none(),
            tail,
        })
    }

    fn base(&self, list: &str) -> u64 {
        self.bases
            .read()
            .expect("base lock")
            .iter()
            .find(|(name, _)| name == list)
            .map_or(0, |(_, base)| *base)
    }

    fn addrs(&self) -> Vec<SocketAddr> {
        self.addrs.read().expect("address book lock").clone()
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    async fn subscribe(
        &self,
        addr: SocketAddr,
        list: &str,
        start: felix_client::StartPosition,
        deadline: tokio::time::Instant,
    ) -> Result<(felix_client::Client, felix_client::Subscription)> {
        let client = tokio::time::timeout_at(
            deadline,
            client::connect(addr, &self.tenant_id, &self.token),
        )
        .await
        .context("connect timed out")??;
        let subscription = tokio::time::timeout_at(
            deadline,
            client.subscribe_shard(&self.tenant_id, &self.namespace, list, 0, Some(start)),
        )
        .await
        .context("subscribe timed out")??;
        // The client is returned too: dropping it closes the connection the
        // subscription is delivered on.
        Ok((client, subscription))
    }
}

/// What one read returned.
#[derive(Debug, Clone, Default)]
pub(super) struct Read {
    pub(super) elements: Vec<Element>,
    /// Whether every record below `tail` was delivered.
    pub(super) complete: bool,
    pub(super) tail: Option<u64>,
}

/// The operations and faults of one run, on one clock.
struct Recorder {
    start: Instant,
    ops: Mutex<Vec<Op>>,
    faults: Mutex<Vec<FaultEvent>>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            ops: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
        }
    }

    /// Nanoseconds since the run began. The monotonic clock is shared by
    /// every thread, so one client's reading before another's is real-time
    /// order.
    fn now(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    fn record(&self, op: Op) {
        self.ops.lock().expect("history lock").push(op);
    }

    fn fault(&self, what: String) {
        let at = self.now();
        self.faults
            .lock()
            .expect("history lock")
            .push(FaultEvent { at, what });
    }

    fn take(&self) -> (Vec<Op>, Vec<FaultEvent>) {
        (
            std::mem::take(&mut *self.ops.lock().expect("history lock")),
            std::mem::take(&mut *self.faults.lock().expect("history lock")),
        )
    }
}

/// One client's loop.
struct Client<'w> {
    workload: &'w Workload,
    process: usize,
    kind: ClientKind,
    rng: Rng,
}

impl Client<'_> {
    async fn run(&mut self) {
        let w = self.workload;
        while !w.stopped() {
            let addrs = w.addrs();
            let connected = tokio::time::timeout(
                CONNECT_TIMEOUT,
                client::connect_cluster(&addrs, &w.tenant_id, &w.token),
            )
            .await;
            let Ok(Ok(cluster)) = connected else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            self.run_connected(&cluster).await;
        }
    }

    /// Run operations over one connection until it looks dead or the run ends.
    async fn run_connected(&mut self, cluster: &ClusterClient) {
        let w = self.workload;
        let mut producer: Option<IdempotentProducer<'_>> = None;
        let mut failures = 0;
        while !w.stopped() && failures < FAILURES_BEFORE_RECONNECT {
            let ok = if self.rng.percent(READ_PERCENT) {
                self.read().await
            } else {
                match self.kind {
                    ClientKind::Plain => self.append_plain(cluster).await,
                    ClientKind::Idempotent => self.append_idempotent(cluster, &mut producer).await,
                }
            };
            failures = if ok { 0 } else { failures + 1 };
            let think = self.rng.between(Duration::ZERO, Duration::from_millis(20));
            tokio::time::sleep(think).await;
        }
    }

    async fn append_plain(&mut self, cluster: &ClusterClient) -> bool {
        let w = self.workload;
        let list = self.rng.pick(&w.lists).clone();
        let value = w.next_value.fetch_add(1, Ordering::Relaxed);
        let invoke = w.now();
        let published = tokio::time::timeout(
            w.op_timeout,
            cluster.publish(
                &w.tenant_id,
                &w.namespace,
                &list,
                encode(value),
                AckMode::PerMessage,
            ),
        )
        .await;
        let outcome = match published {
            Ok(Ok(())) => AppendOutcome::Ok { offset: None },
            Ok(Err(err)) => classify(&err),
            // Dropping a publish after it reached the writer does not stop it.
            Err(_) => AppendOutcome::Info,
        };
        self.record_append(invoke, list, value, outcome)
    }

    async fn append_idempotent<'c>(
        &mut self,
        cluster: &'c ClusterClient,
        producer: &mut Option<IdempotentProducer<'c>>,
    ) -> bool {
        let w = self.workload;
        if producer.is_none() {
            // Nothing has been sent yet, so a producer that cannot be had
            // costs no operation.
            match tokio::time::timeout(CONNECT_TIMEOUT, cluster.idempotent_producer()).await {
                Ok(Ok(fresh)) => *producer = Some(fresh),
                _ => return false,
            }
        }
        let Some(current) = producer.as_ref() else {
            return false;
        };
        let list = self.rng.pick(&w.lists).clone();
        let value = w.next_value.fetch_add(1, Ordering::Relaxed);
        let invoke = w.now();
        let mut outcome = AppendOutcome::Info;
        for _ in 0..IDEMPOTENT_ATTEMPTS {
            let sent = tokio::time::timeout(
                w.op_timeout,
                current.publish(&w.tenant_id, &w.namespace, &list, encode(value)),
            )
            .await;
            match sent {
                Ok(Ok(())) => {
                    outcome = AppendOutcome::Ok { offset: None };
                    break;
                }
                // The same batch under the same sequence cannot land twice.
                Ok(Err(_)) => continue,
                // A cancelled publish leaves the producer refusing everything.
                Err(_) => break,
            }
        }
        if outcome == AppendOutcome::Info {
            // Its sequence is in doubt, and only this value may be sent
            // under it; a fresh producer is the way on to the next one.
            *producer = None;
        }
        self.record_append(invoke, list, value, outcome)
    }

    async fn read(&mut self) -> bool {
        let w = self.workload;
        let list = self.rng.pick(&w.lists).clone();
        let addrs = w.addrs();
        let addr = *self.rng.pick(&addrs);
        let invoke = w.now();
        let from = self.read_from(&list);
        let read = w.read(addr, &list, from, w.op_timeout).await;
        let complete = w.now();
        let Ok(read) = read else {
            return false;
        };
        w.recorder.record(Op {
            process: self.process,
            invoke,
            complete,
            action: Action::Read {
                list,
                observed: read.elements,
            },
        });
        true
    }

    /// Where a read starts: mostly a little behind the furthest tail seen,
    /// sometimes the whole list.
    fn read_from(&mut self, list: &str) -> Option<u64> {
        let w = self.workload;
        let index = w.lists.iter().position(|l| l == list)?;
        let tail = w.tails[index].load(Ordering::Relaxed);
        let base = w.base(list);
        if tail <= base || self.rng.percent(FULL_READ_PERCENT) {
            return None;
        }
        // Below the tail, since starting at it could be refused as a future
        // offset by a leader whose tail is a moment behind.
        let low = tail.saturating_sub(RECENT_WINDOW).max(base);
        Some(low + self.rng.below(tail - low))
    }

    fn record_append(&self, invoke: u64, list: String, value: u64, outcome: AppendOutcome) -> bool {
        let w = self.workload;
        w.recorder.record(Op {
            process: self.process,
            invoke,
            complete: w.now(),
            action: Action::Append {
                list,
                value,
                outcome,
            },
        });
        matches!(outcome, AppendOutcome::Ok { .. })
    }
}

/// Whether a failed publish definitely applied nothing.
///
/// Only the broker's own retry class says so. Anything else, a timeout or a
/// dropped connection included, may have landed.
fn classify(err: &anyhow::Error) -> AppendOutcome {
    match err.downcast_ref::<BrokerError>() {
        Some(broker)
            if matches!(
                broker.retry,
                RetryClass::Retry | RetryClass::RetryAfter | RetryClass::Redirect
            ) =>
        {
            AppendOutcome::Fail
        }
        _ => AppendOutcome::Info,
    }
}

fn encode(value: u64) -> Vec<u8> {
    format!("{PAYLOAD_PREFIX}{value}").into_bytes()
}

fn decode(payload: &[u8]) -> u64 {
    std::str::from_utf8(payload)
        .ok()
        .and_then(|text| text.strip_prefix(PAYLOAD_PREFIX))
        .and_then(|value| value.parse().ok())
        .unwrap_or(FOREIGN_PAYLOAD)
}
