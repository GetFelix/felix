//! Aggregate ingest throughput across many publishers.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use felix_client::{Client, ClusterClient, Publisher};
use felix_wire::AckMode;
use felix_wire::routing::{ShardRouting, shard_for_routing};
use tokio::task::JoinSet;

use super::connect::{client, cluster};
use super::framing::{SEND_TIME, stamp_send_time};
use super::{Common, is_retriable_transient, start_instant};
use crate::stats::{Samples, fmt_us, report};

/// The `ingest`-only flags.
#[derive(Default)]
pub struct IngestOptions {
    /// Distinct routing keys; 0 publishes unkeyed.
    pub keys: usize,
    /// The stream's shard count. Absent, the broker is asked.
    pub shards: Option<u32>,
    /// Acked batches each publisher keeps outstanding; 0 is fire-and-forget.
    pub in_flight: usize,
    /// Publish for this long instead of a fixed `--total`.
    pub duration: Option<Duration>,
    /// Start publishing at this wall-clock time, so generators on separate
    /// machines overlap for the whole run instead of starting seconds apart.
    pub start_at: Option<SystemTime>,
    /// Put the wall-clock send time, Unix micros, in each payload's first 8
    /// bytes, so a `subscribe` run in another process can report delivery
    /// latency.
    pub stamp_send_time: bool,
    /// Publish through one shared `ClusterClient` per generator, which sends
    /// each shard's records straight to the shard's owner, instead of one
    /// plain connection per publisher whose batches a non-owner forwards.
    pub cluster: bool,
}

/// Where an ingest publisher writes.
#[derive(Clone)]
struct Target {
    tenant: String,
    namespace: String,
    stream: String,
}

/// What one publisher got through before it stopped.
#[derive(Default)]
struct Tally {
    /// Records in sends that were started and not cut off by the deadline.
    published: usize,
    /// Records whose send call returned. Fire-and-forget, that means written
    /// to the connection, not appended.
    completed: usize,
    /// Records the broker acknowledged. Zero when fire-and-forget.
    acked: usize,
    /// Records in sends abandoned at the deadline.
    cut_off: usize,
    retries: u64,
    /// Records in sends that failed outright (a dead broker, a refusal that
    /// is not retried). They may or may not have landed.
    failed_sends: usize,
    /// Times a publisher replaced a broken connection, or the cluster client
    /// reported that it did.
    reconnects: u64,
    /// Records the broker acknowledged, by shard.
    per_shard: BTreeMap<u32, usize>,
    /// Acked sends (batches), to set the client's forwarded count against.
    acked_sends: usize,
    samples: Samples,
}

/// Aggregate ingest ceiling — the write throughput measured the way a
/// multi-partition system quotes it. `concurrency` publishers, each on its own
/// connection spread across the brokers, hammer `stream` (give it as many
/// shards as brokers, or more) with fire-and-forget **binary** batches and no
/// subscribers, so nothing on the delivery side can false-bottleneck the
/// number. Reports aggregate msg/s and MB/s, and the per-publisher figure so
/// scaling is visible. A single publisher on one shard is the least-parallel
/// configuration possible; this is the opposite, and it is what actually
/// stresses the brokers.
///
/// `keys` spreads batches over that many routing keys. At 0 the batches are
/// unkeyed, and a record with no key resolves to shard 0 -- so a 12-shard
/// stream is exercised as a single log, which is how every multi-shard
/// measurement before this one was really a single-shard one. When the shard
/// count is known the key names are chosen so they cover the shards evenly;
/// see [`spread_keys`].
///
/// With a duration, every send is bounded by the deadline. A fire-and-forget
/// send can otherwise wait in client admission until the broker drains its
/// backlog, long after the run should have ended.
///
/// `in_flight` above 0 switches to acknowledged batches: each publisher keeps
/// up to that many `PerBatch` publishes unanswered on its connection, which is
/// what the broker's publish window allows, and the ack latency is reported.
/// On a durable stream with ack-on-commit this is the rate a client that waits
/// for durability gets, not the broker's enqueue rate.
pub(crate) async fn ingest(
    common: &Common,
    stream: &str,
    opts: &IngestOptions,
) -> Result<serde_json::Value> {
    let IngestOptions {
        in_flight,
        stamp_send_time: stamp,
        cluster: use_cluster,
        ..
    } = *opts;
    let publishers = common.concurrency.max(1);
    let per = (common.total / publishers).max(1);
    let batch = common.batch.max(1);
    let payload_bytes = common.payload_bytes.max(if stamp { SEND_TIME } else { 1 });

    // Readiness pre-flight on one connection, so a cold routing snapshot does
    // not land inside the measured window (same discipline as `pubsub`).
    let plan = {
        let pf = client(common, common.brokers[0]).await?;
        let publisher = pf.publisher().await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive = 0;
        while consecutive < 50 {
            match publisher
                .publish(
                    &common.tenant,
                    &common.namespace,
                    stream,
                    vec![0u8; payload_bytes],
                    AckMode::PerMessage,
                )
                .await
            {
                Ok(_) => consecutive += 1,
                Err(_) if Instant::now() < deadline => {
                    consecutive = 0;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(err) => return Err(err.context("cluster never became ready to ingest")),
            }
        }
        KeyPlan::learn(&pf, common, stream, opts).await
    };
    let plan = Arc::new(plan);

    // One cluster client for every publisher of this generator, so a shard's
    // owner is learned once and its connection is shared.
    let shared = if use_cluster {
        let client = Arc::new(cluster(common).await?);
        let dialled = Arc::new(std::sync::Mutex::new(Some(client.client().await.dialled())));
        Some((client, dialled))
    } else {
        None
    };

    let forwarded_before = felix_client::publishes_forwarded();
    let started = start_instant(opts.start_at, "ingest");
    let deadline = opts.duration.map(|d| started + d);
    let mut tasks = Vec::new();
    for p in 0..publishers {
        let target = Target {
            tenant: common.tenant.clone(),
            namespace: common.namespace.clone(),
            stream: stream.to_string(),
        };
        let token = common.token.clone();
        let plan = Arc::clone(&plan);
        let brokers = common.brokers.clone();
        let shared = shared.clone();
        tasks.push(tokio::spawn(async move {
            // Each plain publisher owns a connection to a different broker in
            // round-robin, so the write load is spread across the fleet, not
            // funnelled through one ingress.
            let mut conn = match shared {
                Some((cluster, dialled)) => Conn {
                    sink: Sink::Cluster(cluster),
                    broker: 0,
                    generation: 0,
                    dialled,
                },
                None => {
                    let (index, publisher) =
                        connect_publisher(&brokers, p % brokers.len(), &target.tenant, &token)
                            .await?;
                    Conn {
                        sink: Sink::Single(publisher),
                        broker: index,
                        generation: 0,
                        dialled: Arc::default(),
                    }
                }
            };
            tokio::time::sleep_until(started.into()).await;
            let template = vec![0u8; payload_bytes];
            let mut tally = Tally::default();
            let mut batches = 0usize;
            // Records in acked sends not yet answered, which the deadline
            // cuts off if it arrives first.
            let mut outstanding = 0usize;
            let mut pending: JoinSet<Done> = JoinSet::new();
            let mut last_success = Instant::now();
            while match deadline {
                Some(deadline) => Instant::now() < deadline,
                None => tally.published < per,
            } {
                let this = match deadline {
                    Some(_) => batch,
                    None => batch.min(per - tally.published),
                };
                // The key decides the shard, so cycling keys spreads the load
                // across logs instead of piling it on shard 0.
                let slot =
                    (!plan.names.is_empty()).then(|| (p * 1_000_003 + batches) % plan.names.len());
                let key = slot.map(|i| plan.names[i].clone());
                let shard = slot.map_or_else(|| plan.shard_of(None), |i| plan.shards[i]);
                batches += 1;
                if in_flight == 0 {
                    let send = send(
                        &conn.sink,
                        &target,
                        key,
                        &template,
                        this,
                        AckMode::None,
                        stamp,
                    );
                    match until(deadline, send).await {
                        Some(Ok(sent)) => {
                            tally.retries += sent.retries;
                            tally.published += this;
                            tally.completed += this;
                            last_success = Instant::now();
                        }
                        Some(Err(err)) => {
                            tally.failed_sends += this;
                            let failed = failure(&err, conn.generation);
                            conn.recover(
                                &failed,
                                &brokers,
                                &target,
                                &token,
                                &mut tally,
                                last_success,
                            )
                            .await?;
                        }
                        None => {
                            tally.cut_off += this;
                            break;
                        }
                    }
                    continue;
                }
                if pending.len() >= in_flight {
                    match until(deadline, pending.join_next()).await {
                        Some(done) => {
                            let done = done.expect("pending").context("ingest send task")?;
                            outstanding -= done.records;
                            if let Err(failed) = settle(&mut tally, done, &mut last_success) {
                                conn.recover(
                                    &failed,
                                    &brokers,
                                    &target,
                                    &token,
                                    &mut tally,
                                    last_success,
                                )
                                .await?;
                            }
                        }
                        None => break,
                    }
                }
                tally.published += this;
                outstanding += this;
                let (sink, target, template) =
                    (conn.sink.clone(), target.clone(), template.clone());
                let generation = conn.generation;
                pending.spawn(async move {
                    let started = Instant::now();
                    let result = send(
                        &sink,
                        &target,
                        key,
                        &template,
                        this,
                        AckMode::PerBatch,
                        stamp,
                    )
                    .await;
                    Done {
                        result,
                        took: started.elapsed(),
                        records: this,
                        shard,
                        generation,
                    }
                });
            }
            loop {
                match until(deadline, pending.join_next()).await {
                    Some(Some(done)) => {
                        let done = done.context("ingest send task")?;
                        outstanding -= done.records;
                        let _ = settle(&mut tally, done, &mut last_success);
                    }
                    Some(None) => break,
                    None => {
                        // Abandoned sends are not counted as published.
                        pending.abort_all();
                        tally.published -= outstanding;
                        tally.cut_off += outstanding;
                        break;
                    }
                }
            }
            Ok::<Tally, anyhow::Error>(tally)
        }));
    }

    let mut total = Tally::default();
    for task in tasks {
        let t = task.await.context("publisher task")??;
        total.merge(t);
    }
    if let Some((shared, _)) = &shared {
        // Fire-and-forget sends return once queued. Flushed inside the
        // measured time, so the rate is for records written, not queued.
        if let Err(err) = shared.finish().await {
            eprintln!("ingest: flushing the cluster client: {err:#}");
        }
    }
    let Tally {
        published,
        completed,
        acked,
        cut_off,
        retries,
        failed_sends,
        reconnects,
        per_shard,
        acked_sends,
        samples: mut acks,
    } = total;
    let elapsed = started.elapsed();
    let secs = elapsed.as_secs_f64();
    let msg_s = published as f64 / secs;
    let mb_s = msg_s * payload_bytes as f64 / 1_000_000.0;
    // Fire-and-forget has no acked count: a completed send there is one the
    // client wrote, which says nothing about what the broker appended.
    let acked_records = (in_flight > 0).then_some(acked);
    let acked_msg_s = acked_records.map(|n| n as f64 / secs);
    report!(
        common,
        "ingest: publishers = {publishers}, in_flight = {in_flight}, cluster = {use_cluster}, published = {published} in {secs:.1} s, {msg_s:.0} msg/s, {mb_s:.1} MB/s, publish_retries = {retries}, cut_off = {cut_off}, failed_sends = {failed_sends}, reconnects = {reconnects}"
    );
    match acked_msg_s {
        Some(rate) => report!(common, "  acked = {acked} ({rate:.0} msg/s)"),
        None => report!(
            common,
            "  fire-and-forget: published counts sends written, not records appended"
        ),
    }
    let ack = (!acks.is_empty()).then(|| acks.percentiles());
    if let Some(ack) = ack {
        report!(
            common,
            "  batch ack: p50 = {}, p99 = {}, p999 = {}, max = {}",
            fmt_us(ack.p50_us),
            fmt_us(ack.p99_us),
            fmt_us(ack.p999_us),
            fmt_us(ack.max_us)
        );
    }
    // The client counts a send as forwarded when its ack names another broker
    // as the shard's owner (`felix_client::publishes_forwarded`). A send that
    // asks for no ack gets no such answer, so fire-and-forget cannot be
    // counted from here; use the brokers' own forward counters for that.
    let forwarded_sends = (in_flight > 0)
        .then(|| felix_client::publishes_forwarded().saturating_sub(forwarded_before));
    let forwarded_share = forwarded_sends
        .filter(|_| acked_sends > 0)
        .map(|n| n as f64 / acked_sends as f64);
    let per_shard_acked: BTreeMap<String, usize> = per_shard
        .iter()
        .map(|(shard, n)| (shard.to_string(), *n))
        .collect();
    Ok(serde_json::json!({
        "scenario": "ingest",
        "environment": common.environment,
        "stream": stream,
        "publishers": publishers,
        "batch": batch,
        "payload_bytes": payload_bytes,
        "published": published,
        "completed_records": completed,
        "acked_records": acked_records,
        "acked_throughput_msg_s": acked_msg_s,
        "acked_throughput_mb_s": acked_msg_s.map(|r| r * payload_bytes as f64 / 1_000_000.0),
        "cut_off_records": cut_off,
        "publish_retries": retries,
        "failed_sends": failed_sends,
        "reconnects": reconnects,
        "per_shard_acked": per_shard_acked,
        "acked_sends": acked_sends,
        "forwarded_sends": forwarded_sends,
        "forwarded_share": forwarded_share,
        "cluster": use_cluster,
        "throughput_msg_s": msg_s,
        "throughput_mb_s": mb_s,
        "per_publisher_msg_s": msg_s / publishers as f64,
        "in_flight": in_flight,
        "stamp_send_time": stamp,
        "duration_s": secs,
        "batch_ack_latency_us": ack.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
        })),
    }))
}

/// Where a publisher's sends go.
#[derive(Clone)]
enum Sink {
    /// One connection to one broker; a batch for a shard it does not lead is
    /// forwarded.
    Single(Publisher),
    /// The generator's shared cluster client, which sends each shard's records
    /// to the shard's owner.
    Cluster(Arc<ClusterClient>),
}

/// What an acknowledged or queued send came back with.
struct Sent {
    retries: u64,
}

/// One acked send's result, with what is needed to tally it.
struct Done {
    result: Result<Sent>,
    took: Duration,
    records: usize,
    shard: u32,
    generation: u64,
}

/// A send that failed, and what it says about the connection.
struct Failure {
    /// The connection broke or timed out. A coded refusal from a broker
    /// (stale width, stream not found, overloaded) is not that: the
    /// connection is fine, and replacing it would fix nothing.
    transport: bool,
    /// The connection generation the send ran on, so many in-flight sends
    /// failing together replace the connection once.
    generation: u64,
}

fn failure(err: &anyhow::Error, generation: u64) -> Failure {
    Failure {
        transport: err.downcast_ref::<felix_client::BrokerError>().is_none(),
        generation,
    }
}

/// Tally an acked send. `Err` says what failed; the records are already
/// counted as failed.
fn settle(tally: &mut Tally, done: Done, last_success: &mut Instant) -> Result<(), Failure> {
    match done.result {
        Ok(sent) => {
            tally.acked_one(sent.retries, done.took, done.records, done.shard);
            *last_success = Instant::now();
            Ok(())
        }
        Err(err) => {
            tally.failed_sends += done.records;
            Err(failure(&err, done.generation))
        }
    }
}

/// How long a publisher keeps trying to get a connection back before it
/// gives up and fails the run. One broker dying must not end a run; every
/// broker being gone for this long is not a failover.
const RECONNECT_BUDGET: Duration = Duration::from_secs(30);

/// Try `brokers` in order, starting after `failed`, until one connects or
/// `budget` runs out. Returns the index that connected and what it gave.
async fn reconnect_with<T, F, Fut>(
    brokers: &[SocketAddr],
    failed: usize,
    budget: Duration,
    pause: Duration,
    mut connect: F,
) -> Result<(usize, T)>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let until = Instant::now() + budget;
    let mut next = failed;
    loop {
        next = (next + 1) % brokers.len();
        match connect(brokers[next]).await {
            Ok(connected) => return Ok((next, connected)),
            Err(err) if Instant::now() >= until => {
                return Err(
                    err.context(format!("no broker accepted a connection within {budget:?}"))
                );
            }
            Err(_) => tokio::time::sleep(pause).await,
        }
    }
}

/// Connect a plain publisher, starting at `first` and moving on to the next
/// broker if it does not answer.
async fn connect_publisher(
    brokers: &[SocketAddr],
    first: usize,
    tenant: &str,
    token: &str,
) -> Result<(usize, Publisher)> {
    let failed = (first + brokers.len() - 1) % brokers.len();
    reconnect_with(
        brokers,
        failed,
        RECONNECT_BUDGET,
        Duration::from_millis(200),
        |addr| async move {
            let config = crate::tls::client_config(tenant, token)?;
            let client = Client::connect(addr, "localhost", config)
                .await
                .with_context(|| format!("connect to {addr}"))?;
            client.publisher().await
        },
    )
    .await
}

/// A publisher's sink and the state to replace it.
struct Conn {
    sink: Sink,
    /// Index in the broker list of the broker a plain publisher is on.
    broker: usize,
    /// Bumped each time the connection is replaced.
    generation: u64,
    /// The broker the shared cluster client last dialled, to notice it moved.
    dialled: Arc<std::sync::Mutex<Option<SocketAddr>>>,
}

impl Conn {
    /// Get a working sink back after `failed`, giving up only when nothing
    /// has succeeded for [`RECONNECT_BUDGET`] in total. A cluster client
    /// reconnects by itself, so that is only observed; a plain publisher
    /// moves to the next broker, once per broken connection.
    async fn recover(
        &mut self,
        failed: &Failure,
        brokers: &[SocketAddr],
        target: &Target,
        token: &str,
        tally: &mut Tally,
        last_success: Instant,
    ) -> Result<()> {
        let left = RECONNECT_BUDGET.saturating_sub(last_success.elapsed());
        if left.is_zero() {
            anyhow::bail!("no publish succeeded for {RECONNECT_BUDGET:?}");
        }
        // Not the connection's fault; a pause keeps a refusing broker from
        // being hammered.
        if !failed.transport {
            tokio::time::sleep(Duration::from_millis(20)).await;
            return Ok(());
        }
        match &self.sink {
            Sink::Cluster(cluster) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let now = cluster.client().await.dialled();
                let mut last = self.dialled.lock().unwrap();
                if last.replace(now).is_some_and(|before| before != now) {
                    tally.reconnects += 1;
                }
            }
            Sink::Single(_) if failed.generation != self.generation => {}
            Sink::Single(_) => {
                let (index, publisher) = reconnect_with(
                    brokers,
                    self.broker,
                    left,
                    Duration::from_millis(200),
                    |addr| async move {
                        let config = crate::tls::client_config(&target.tenant, token)?;
                        let client = Client::connect(addr, "localhost", config).await?;
                        client.publisher().await
                    },
                )
                .await?;
                self.broker = index;
                self.sink = Sink::Single(publisher);
                self.generation += 1;
                tally.reconnects += 1;
            }
        }
        Ok(())
    }
}

/// Publish a batch of `count` copies of `template`, retrying the transient
/// refusals [`is_retriable_transient`] names. With `stamp`, each attempt
/// carries the time it was sent, not the time the first attempt was.
async fn send(
    sink: &Sink,
    target: &Target,
    key: Option<String>,
    template: &[u8],
    count: usize,
    ack: AckMode,
    stamp: bool,
) -> Result<Sent> {
    let mut retries = 0u64;
    loop {
        let payloads: Vec<Vec<u8>> = std::iter::repeat_with(|| {
            let mut body = template.to_vec();
            if stamp {
                stamp_send_time(&mut body);
            }
            body
        })
        .take(count)
        .collect();
        let key = key
            .as_ref()
            .map(|key| bytes::Bytes::from(key.clone().into_bytes()));
        let result = match (sink, key) {
            (Sink::Single(publisher), Some(key)) => {
                publisher
                    .publish_batch_keyed(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        key,
                        payloads,
                        ack,
                    )
                    .await
            }
            (Sink::Single(publisher), None) => {
                publisher
                    .publish_batch(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        payloads,
                        ack,
                    )
                    .await
            }
            // A single record keeps the per-record call, so batch 1 measures
            // what an application publishing one record at a time gets.
            (Sink::Cluster(cluster), Some(key)) if count == 1 => {
                let payload = payloads.into_iter().next().expect("one payload");
                cluster
                    .publish_keyed(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        payload,
                        key,
                        ack,
                    )
                    .await
            }
            (Sink::Cluster(cluster), Some(key)) => {
                cluster
                    .publish_batch_keyed(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        key,
                        payloads,
                        ack,
                    )
                    .await
            }
            (Sink::Cluster(cluster), None) => {
                cluster
                    .publish_batch(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        payloads,
                        ack,
                    )
                    .await
            }
        };
        match result {
            Ok(_) => return Ok(Sent { retries }),
            Err(err) if is_retriable_transient(&err) => {
                retries += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => return Err(err.context("ingest publish")),
        }
    }
}

impl Tally {
    fn acked_one(&mut self, retries: u64, took: Duration, records: usize, shard: u32) {
        self.retries += retries;
        self.completed += records;
        self.acked += records;
        *self.per_shard.entry(shard).or_default() += records;
        self.acked_sends += 1;
        self.samples.record(took);
    }

    fn merge(&mut self, other: Tally) {
        self.published += other.published;
        self.completed += other.completed;
        self.acked += other.acked;
        self.cut_off += other.cut_off;
        self.retries += other.retries;
        self.failed_sends += other.failed_sends;
        self.reconnects += other.reconnects;
        for (shard, n) in other.per_shard {
            *self.per_shard.entry(shard).or_default() += n;
        }
        self.acked_sends += other.acked_sends;
        self.samples.merge(other.samples);
    }
}

/// Run `f` to completion, or until `deadline`. `None` means the deadline won.
async fn until<F: Future>(deadline: Option<Instant>, f: F) -> Option<F::Output> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline.into(), f).await.ok(),
        None => Some(f.await),
    }
}

/// The routing keys an ingest run cycles through, and the shard each lands on.
struct KeyPlan {
    names: Vec<String>,
    /// `shards[i]` is the shard `names[i]` routes to.
    shards: Vec<u32>,
    count: u32,
    routing: ShardRouting,
}

impl KeyPlan {
    fn shard_of(&self, key: Option<&str>) -> u32 {
        shard_for_routing(self.routing, self.count, key.map(str::as_bytes))
    }

    /// Spread across the stream's shards when the shard count is known, else
    /// `k0..k{n}` on what is assumed to be one shard.
    async fn learn(client: &Client, common: &Common, stream: &str, opts: &IngestOptions) -> Self {
        let (names, count, routing) = key_names(client, common, stream, opts).await;
        let mut plan = Self {
            names,
            shards: Vec::new(),
            count: count.max(1),
            routing,
        };
        plan.shards = plan
            .names
            .iter()
            .map(|name| plan.shard_of(Some(name)))
            .collect();
        plan
    }
}

/// The routing key names an ingest run cycles through. Spread across the
/// stream's shards when the shard count is known, else `k0..k{n}`.
async fn key_names(
    client: &Client,
    common: &Common,
    stream: &str,
    opts: &IngestOptions,
) -> (Vec<String>, u32, ShardRouting) {
    if opts.keys == 0 {
        return (
            Vec::new(),
            opts.shards.unwrap_or(1),
            ShardRouting::default(),
        );
    }
    let learned = if client.supports_stream_shards() {
        client
            .stream_routing(&common.tenant, &common.namespace, stream)
            .await
            .ok()
    } else {
        None
    };
    if let (Some(given), Some((asked, _))) = (opts.shards, learned)
        && given != asked
    {
        eprintln!(
            "ingest: --shards {given}, but the broker says {stream} has {asked}; using {given}"
        );
    }
    let routing = learned.map(|(_, r)| r).unwrap_or_default();
    match opts.shards.or(learned.map(|(s, _)| s)) {
        Some(shards) if shards > 1 => (spread_keys(opts.keys, shards, routing), shards, routing),
        _ => (
            (0..opts.keys).map(|i| format!("k{i}")).collect(),
            1,
            routing,
        ),
    }
}

/// `keys` key names that land on the shards as evenly as possible: every
/// shard gets one before any gets two, and fewer keys than shards land on
/// distinct shards. Found by trying `k0, k1, ...` against the stream's own
/// routing, since sequential names hash unevenly: 12 of them on 12 shards
/// reach only 8. Returned round-robin by shard, so consecutive keys hit
/// different shards.
pub(crate) fn spread_keys(keys: usize, shards: u32, routing: ShardRouting) -> Vec<String> {
    let n = shards as usize;
    let want = |shard: usize| keys / n + usize::from(shard < keys % n);
    let mut by_shard: Vec<Vec<String>> = vec![Vec::new(); n];
    let mut found = 0;
    let mut candidate = 0u64;
    while found < keys {
        let name = format!("k{candidate}");
        let shard = shard_for_routing(routing, shards, Some(name.as_bytes())) as usize;
        if by_shard[shard].len() < want(shard) {
            by_shard[shard].push(name);
            found += 1;
        }
        candidate += 1;
    }
    let rounds = keys.div_ceil(n);
    (0..rounds)
        .flat_map(|round| by_shard.iter().filter_map(move |names| names.get(round)))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
