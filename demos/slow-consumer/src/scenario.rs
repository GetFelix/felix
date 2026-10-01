//! The workload: one telemetry stream, N dashboard subscribers, one of which stalls.
//!
//! Everything here is policy-agnostic. The same scenario runs twice — once under
//! `drop_new` and once under `block` — and the contrast between the two runs is the
//! point of the demo.

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::{Broker, StreamMetadata, SubQueuePolicy};
use felix_client::{Client, ClientConfig, ClientSubQueuePolicy};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub const TENANT: &str = "t1";
pub const NAMESPACE: &str = "default";
pub const STREAM: &str = "telemetry";

/// Every event carries its own sequence and publish timestamp.
///
/// `felix_client::Event` exposes only tenant/namespace/stream/payload — no sequence
/// number and no timestamp — so a subscriber cannot otherwise tell that it missed
/// anything. Encoding both in the payload is what makes the *gap count* observable,
/// and the gap count is the honest measure of what at-most-once delivery costs: those
/// events are gone, with no replay and no redelivery.
const HEADER_LEN: usize = 16;

fn encode(seq: u64, publish_ns: u64, payload_bytes: usize) -> Bytes {
    let mut buf = Vec::with_capacity(payload_bytes.max(HEADER_LEN));
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&publish_ns.to_be_bytes());
    buf.resize(payload_bytes.max(HEADER_LEN), 0xAB);
    Bytes::from(buf)
}

fn decode(payload: &[u8]) -> Option<(u64, u64)> {
    if payload.len() < HEADER_LEN {
        return None;
    }
    let seq = u64::from_be_bytes(payload[0..8].try_into().ok()?);
    let publish_ns = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    Some((seq, publish_ns))
}

fn now_ns() -> u64 {
    // Monotonic within a process, which is all we need: publisher and subscriber
    // share this process, so wall-clock skew is not a factor.
    static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let origin = ORIGIN.get_or_init(Instant::now);
    origin.elapsed().as_nanos() as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    DropNew,
    Block,
}

impl Policy {
    pub fn label(self) -> &'static str {
        match self {
            Policy::DropNew => "drop_new",
            Policy::Block => "block",
        }
    }

    /// One-line statement of what this policy chooses to sacrifice.
    pub fn tradeoff(self) -> &'static str {
        match self {
            Policy::DropNew => "publishers never slow down; the slow consumer loses events",
            Policy::Block => "nothing is lost; one slow consumer throttles everyone",
        }
    }
}

/// Which phase of the scenario is running. The stall is what the demo is about;
/// baseline and recovery exist so the viewer can see the before and after.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Baseline,
    Degraded,
    Recovered,
}

impl Phase {
    /// The victim is whichever subscriber is last, so its name depends on
    /// `--subscribers` and cannot be baked into the string.
    pub fn label(self, victim: &str) -> String {
        match self {
            Phase::Baseline => "baseline — all consumers healthy".to_string(),
            Phase::Degraded => format!("DEGRADED — {victim} has stopped draining"),
            Phase::Recovered => format!("recovered — {victim} draining again"),
        }
    }
}

/// Live counters for one subscriber. Read by the renderer on every tick.
#[derive(Debug)]
pub struct SubscriberStats {
    pub name: String,
    /// True while this subscriber is deliberately not draining its queue.
    pub stalled: AtomicBool,
    pub received: AtomicU64,
    /// Events that were published but never arrived, detected via sequence breaks.
    pub gaps: AtomicU64,
    last_seq: AtomicU64,
    /// Latency samples for the current render window, drained each tick.
    window: Mutex<Vec<u64>>,
}

impl SubscriberStats {
    fn new(name: String) -> Self {
        Self {
            name,
            stalled: AtomicBool::new(false),
            received: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            last_seq: AtomicU64::new(u64::MAX),
            window: Mutex::new(Vec::new()),
        }
    }

    /// Drain the window and return (p50, p99) in microseconds.
    pub async fn take_percentiles(&self) -> Option<(u64, u64)> {
        let mut samples = {
            let mut guard = self.window.lock().await;
            if guard.is_empty() {
                return None;
            }
            std::mem::take(&mut *guard)
        };
        samples.sort_unstable();
        let pick = |q: f64| -> u64 {
            let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
            samples[idx] / 1_000
        };
        Some((pick(0.50), pick(0.99)))
    }
}

/// Publisher-side counters. The headline number: does the publisher slow down?
#[derive(Debug, Default)]
pub struct PublisherStats {
    pub published: AtomicU64,
    pub errors: AtomicU64,
}

/// Everything the renderer needs, shared across tasks.
#[derive(Debug)]
pub struct LiveState {
    pub policy: Policy,
    /// Name of the subscriber that stalls, for labelling.
    pub victim: String,
    pub phase: std::sync::Mutex<Phase>,
    pub subscribers: Vec<Arc<SubscriberStats>>,
    pub publisher: PublisherStats,
    pub target_rate: u64,
    /// Broker-side drop counters, scraped from the Prometheus recorder.
    pub broker_drops: AtomicU64,
}

impl LiveState {
    pub fn phase(&self) -> Phase {
        *self.phase.lock().expect("phase mutex")
    }
}

/// What one policy run produced, kept for the side-by-side comparison at the end.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub policy: Policy,
    pub target_rate: u64,
    pub achieved_rate: u64,
    /// Per-subscriber (name, received, gaps, was_stalled).
    pub subscribers: Vec<(String, u64, u64, bool)>,
    pub broker_drops: u64,
    /// p99 across the healthy subscribers during the degraded phase, in microseconds.
    pub healthy_p99_us_degraded: Option<u64>,
}

impl RunOutcome {
    pub fn healthy_gaps(&self) -> u64 {
        self.subscribers
            .iter()
            .filter(|(_, _, _, stalled)| !stalled)
            .map(|(_, _, gaps, _)| *gaps)
            .sum()
    }

    /// Events successfully received by the consumers that never stalled.
    pub fn healthy_received(&self) -> u64 {
        self.subscribers
            .iter()
            .filter(|(_, _, _, stalled)| !stalled)
            .map(|(_, received, _, _)| *received)
            .sum()
    }

    pub fn stalled_gaps(&self) -> u64 {
        self.subscribers
            .iter()
            .filter(|(_, _, _, stalled)| *stalled)
            .map(|(_, _, gaps, _)| *gaps)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// Transport fixtures
// ---------------------------------------------------------------------------

// Duplicated from the other demos and the soak harness rather than refactored: this
// pair appears verbatim in 10+ places already, and unifying it is a separate change.
fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

/// Build a client config whose subscription queue matches the broker-side policy.
///
/// This has to be set explicitly or the demo measures the wrong thing. The client
/// keeps its *own* bounded subscription queue (`client_sub_queue_policy`, default
/// `drop_new`, capacity 256), which sits downstream of everything the broker does.
/// An application that stops calling `next_event` fills that queue first, so with
/// the client left on `drop_new` the loss happens client-side and the broker's
/// policy never comes into play — the `block` arm would look identical to
/// `drop_new`, which is exactly what happened before this was fixed.
///
/// Backpressure only reaches the publisher when the whole chain is configured to
/// propagate it: client queue blocks -> QUIC flow control -> broker subscriber
/// queue blocks -> fanout stalls -> publish worker -> ingress (`pub_ingress_wait`).
fn client_config(
    cert: &CertificateDer<'static>,
    auth: &felix_broker_service::serving::auth::demo::DemoAuth,
    policy: Policy,
    queue_capacity: usize,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    config.client_sub_queue_policy = match policy {
        Policy::DropNew => ClientSubQueuePolicy::DropNew,
        Policy::Block => ClientSubQueuePolicy::Block,
    };
    config.client_sub_queue_capacity = queue_capacity;
    // Keep the transport buffer small enough for a short demo run to reach the
    // broker-side queue. The production default (64 MiB per stream) can absorb
    // the entire synthetic stall and hide block-policy backpressure.
    config.event_stream_recv_window = 256 * 1024;
    Ok(config)
}

struct Harness {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    shutdown: tokio_util_shim::Token,
    accept_task: tokio::task::JoinHandle<()>,
    _server: Arc<QuicServer>,
}

/// Minimal stand-in so this crate does not need tokio-util directly just to stop
/// the accept loop between the two policy runs.
mod tokio_util_shim {
    use tokio::sync::watch;

    #[derive(Clone)]
    pub struct Token(watch::Sender<bool>);

    impl Token {
        pub fn new() -> Self {
            Self(watch::channel(false).0)
        }
        pub fn cancel(&self) {
            let _ = self.0.send(true);
        }
        pub async fn cancelled(&self) {
            let mut rx = self.0.subscribe();
            if *rx.borrow() {
                return;
            }
            let _ = rx.changed().await;
        }
    }

    impl std::fmt::Debug for Token {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Token")
        }
    }
}

async fn start_broker(
    auth: &felix_broker_service::serving::auth::demo::DemoAuth,
    policy: Policy,
    queue_capacity: usize,
    phase_secs: u64,
) -> Result<Harness> {
    let core = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(queue_capacity)
        .context("configure subscriber queue depth")?
        .with_subscriber_queue_policy(match policy {
            Policy::DropNew => SubQueuePolicy::DropNew,
            Policy::Block => SubQueuePolicy::Block,
        });
    let core = Arc::new(core);
    core.register_tenant(TENANT).await?;
    core.register_namespace(TENANT, NAMESPACE).await?;
    core.register_stream(TENANT, NAMESPACE, STREAM, StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let broker_policy = match policy {
        Policy::DropNew => SubQueuePolicy::DropNew,
        Policy::Block => SubQueuePolicy::Block,
    };
    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.subscriber_queue_capacity = queue_capacity;
    config.subscriber_queue_policy = broker_policy;
    // Checkpoint 4 waits in both arms so ingress shedding cannot be mistaken for
    // coupling between consumers. With the default
    // `pub_ingress_wait = false` the broker's ingress queue *sheds* fire-and-forget
    // publishes. Under `drop_new`, downstream shedding still isolates the publisher;
    // under `block`, waiting propagates backpressure instead of moving loss upstream.
    // See docs-site/docs/development/internals-concurrency.md:77-88.
    config.pub_ingress_wait = true;
    // The degraded phase lasts two phase intervals. Keep the bounded ingress wait
    // beyond that interval so the lossless arm does not time out and close the
    // fire-and-forget publish stream before the consumer recovers.
    config.publish_queue_wait_timeout_ms = phase_secs.saturating_mul(2_000).saturating_add(5_000);
    // Checkpoint 6, the writer lane, has its own policy and its own default of
    // `drop_new` at depth 64. It sits *downstream* of the subscriber queue, so
    // leaving it on drop_new means the lane sheds before checkpoint 5 ever blocks
    // and the `block` arm again looks identical to `drop_new`. The docs are explicit
    // that these are independent: "A subscriber can be perfectly healthy at
    // checkpoint 5 and still get shed at checkpoint 6."
    config.subscriber_lane_queue_policy = broker_policy;

    let shutdown = tokio_util_shim::Token::new();
    let accept_task = {
        let server = Arc::clone(&server);
        let auth = Arc::clone(&auth.auth);
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                result = felix_broker_service::serving::quic::serve(server, core, config, auth) => {
                    if let Err(err) = result {
                        eprintln!("accept loop exited: {err}");
                    }
                }
            }
        })
    };

    Ok(Harness {
        addr,
        cert,
        shutdown,
        accept_task,
        _server: server,
    })
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// Snapshot of every broker drop counter, so a run can say *which* checkpoint shed
/// rather than only that something did.
pub fn scrape_drop_counters(rendered: &str) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for line in rendered.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((key, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = key.split('{').next().unwrap_or(key).trim();
        if !(name.contains("dropped") || name.contains("rejected")) {
            continue;
        }
        if let Ok(parsed) = value.trim().parse::<f64>()
            && parsed > 0.0
        {
            out.push((name.to_string(), parsed as u64));
        }
    }
    out.sort();
    out.dedup_by(|a, b| a.0 == b.0);
    out
}

pub struct RunConfig {
    pub policy: Policy,
    pub subscribers: usize,
    pub target_rate: u64,
    pub payload_bytes: usize,
    pub queue_capacity: usize,
    pub phase_secs: u64,
}

/// Run the scenario once under one policy, calling `on_tick` about twice a second so
/// a renderer can draw. Returns the outcome for the final comparison.
pub async fn run_once<F>(config: RunConfig, mut on_tick: F) -> Result<RunOutcome>
where
    F: FnMut(&LiveState) -> Result<bool>,
{
    let auth = felix_broker_service::serving::auth::demo::demo_auth_for_tenant(TENANT)?;
    let harness = start_broker(
        &auth,
        config.policy,
        config.queue_capacity,
        config.phase_secs,
    )
    .await?;

    let stats: Vec<Arc<SubscriberStats>> = (1..=config.subscribers)
        .map(|i| Arc::new(SubscriberStats::new(format!("dash-{i}"))))
        .collect();
    // The last subscriber is the one that stalls.
    let victim = Arc::clone(stats.last().expect("at least one subscriber"));

    let state = Arc::new(LiveState {
        policy: config.policy,
        victim: victim.name.clone(),
        phase: std::sync::Mutex::new(Phase::Baseline),
        subscribers: stats.clone(),
        publisher: PublisherStats::default(),
        target_rate: config.target_rate,
        broker_drops: AtomicU64::new(0),
    });

    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();

    for stat in &stats {
        let client_cfg = client_config(&harness.cert, &auth, config.policy, config.queue_capacity)?;
        let addr = harness.addr;
        let stat = Arc::clone(stat);
        let stop = Arc::clone(&stop);
        tasks.push(tokio::spawn(async move {
            let client = match Client::connect(addr, "localhost", client_cfg).await {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("subscriber connect failed: {err}");
                    return;
                }
            };
            let mut subscription = match client.subscribe(TENANT, NAMESPACE, STREAM).await {
                Ok(sub) => sub,
                Err(err) => {
                    eprintln!("subscribe failed: {err}");
                    return;
                }
            };
            while !stop.load(Ordering::Relaxed) {
                if stat.stalled.load(Ordering::Relaxed) {
                    // The stall: hold the subscription open but stop reading. This is
                    // what a blocked render loop or a degraded link looks like to the
                    // broker, and it is what fills the subscriber's queue.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                match tokio::time::timeout(Duration::from_millis(200), subscription.next_event())
                    .await
                {
                    Ok(Ok(Some(event))) => {
                        if let Some((seq, publish_ns)) = decode(&event.payload) {
                            let previous = stat.last_seq.swap(seq, Ordering::Relaxed);
                            if previous != u64::MAX && seq > previous.wrapping_add(1) {
                                stat.gaps.fetch_add(seq - previous - 1, Ordering::Relaxed);
                            }
                            stat.received.fetch_add(1, Ordering::Relaxed);
                            let latency = now_ns().saturating_sub(publish_ns);
                            let mut window = stat.window.lock().await;
                            if window.len() < 200_000 {
                                window.push(latency);
                            }
                        }
                    }
                    Ok(Ok(None)) | Ok(Err(_)) => break,
                    Err(_) => {}
                }
            }
        }));
    }

    // Let the subscriptions register before publishing, so the baseline is real.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher_task = {
        let client_cfg = client_config(&harness.cert, &auth, config.policy, config.queue_capacity)?;
        let addr = harness.addr;
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        let payload_bytes = config.payload_bytes;
        let target_rate = config.target_rate;
        tokio::spawn(async move {
            let client = match Client::connect(addr, "localhost", client_cfg).await {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("publisher connect failed: {err}");
                    return;
                }
            };
            let publisher = match client.publisher().await {
                Ok(publisher) => publisher,
                Err(err) => {
                    eprintln!("publisher init failed: {err}");
                    return;
                }
            };
            // Pace in 1 ms slices: per-message sleeps cannot resolve 50 µs, and a
            // free-running loop would measure the machine rather than the policy.
            // Pace by batching per tick, with a tick sized to the target rate.
            // `rate / 1000` against a fixed 1 ms tick truncates to zero for any
            // rate below 1000/s and then clamps to 1, which silently published at
            // 1000/s no matter what was asked for — the documented 400/s default
            // was really 1000/s.
            let (tick, per_tick) = if target_rate >= 1000 {
                (Duration::from_millis(1), target_rate / 1000)
            } else {
                (Duration::from_micros(1_000_000 / target_rate.max(1)), 1)
            };
            let mut ticker = tokio::time::interval(tick);
            // Backpressure represents publish opportunities that were missed, not
            // work to replay in an unbounded recovery burst.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut seq = 0u64;
            while !stop.load(Ordering::Relaxed) {
                ticker.tick().await;
                for _ in 0..per_tick {
                    let body = encode(seq, now_ns(), payload_bytes);
                    // AckMode::None is the fire-and-forget path: the publisher never
                    // waits on subscriber progress. Under `block` the backpressure
                    // still reaches it, through the ingress queue rather than an ack.
                    match publisher
                        .publish(TENANT, NAMESPACE, STREAM, body.to_vec(), AckMode::None)
                        .await
                    {
                        Ok(_) => {
                            state.publisher.published.fetch_add(1, Ordering::Relaxed);
                            seq += 1;
                        }
                        Err(_) => {
                            state.publisher.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        })
    };

    // Phase schedule: baseline, then the stall, then recovery.
    let started = Instant::now();
    let phase_for = |elapsed: Duration| -> Phase {
        let secs = elapsed.as_secs();
        if secs < config.phase_secs {
            Phase::Baseline
        } else if secs < config.phase_secs * 3 {
            Phase::Degraded
        } else {
            Phase::Recovered
        }
    };
    let total = Duration::from_secs(config.phase_secs * 4);

    let mut healthy_p99_degraded: Vec<u64> = Vec::new();
    let mut last_published = 0u64;
    let mut rate_samples: Vec<u64> = Vec::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(500));

    while started.elapsed() < total {
        ticker.tick().await;
        let phase = phase_for(started.elapsed());
        *state.phase.lock().expect("phase mutex") = phase;
        victim
            .stalled
            .store(matches!(phase, Phase::Degraded), Ordering::Relaxed);

        let published = state.publisher.published.load(Ordering::Relaxed);
        let delta = published.saturating_sub(last_published);
        last_published = published;
        rate_samples.push(delta * 2); // per 500 ms sample -> per second

        if matches!(phase, Phase::Degraded) {
            for stat in &stats {
                if !stat.stalled.load(Ordering::Relaxed)
                    && let Some((_, p99)) = stat.take_percentiles().await
                {
                    healthy_p99_degraded.push(p99);
                }
            }
        }

        if !on_tick(&state)? {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), publisher_task).await;
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    harness.shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), harness.accept_task).await;

    // Steady-state rate, ignoring the ramp at either end.
    let achieved_rate = if rate_samples.len() > 4 {
        let body = &rate_samples[2..rate_samples.len() - 2];
        body.iter().sum::<u64>() / body.len().max(1) as u64
    } else {
        rate_samples.iter().copied().max().unwrap_or(0)
    };

    healthy_p99_degraded.sort_unstable();

    Ok(RunOutcome {
        policy: config.policy,
        target_rate: config.target_rate,
        achieved_rate,
        subscribers: stats
            .iter()
            .map(|s| {
                (
                    s.name.clone(),
                    s.received.load(Ordering::Relaxed),
                    s.gaps.load(Ordering::Relaxed),
                    // The victim is the last subscriber; report which one stalled.
                    Arc::ptr_eq(s, &victim),
                )
            })
            .collect(),
        broker_drops: state.broker_drops.load(Ordering::Relaxed),
        healthy_p99_us_degraded: healthy_p99_degraded
            .get(healthy_p99_degraded.len() / 2)
            .copied(),
    })
}
