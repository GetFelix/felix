//! A config keyspace distributed as a change stream, and what happens to consumers
//! that try to hold a local copy of it.
//!
//! The publisher owns the authoritative values. Consumers apply changes as they
//! arrive and maintain their own copy. At the end everything quiesces and each
//! consumer's copy is diffed against the authority.

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::{Broker, StreamMetadata, SubQueuePolicy};
use felix_client::{ClientConfig, ClientSubQueuePolicy, Connection};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};

pub const TENANT: &str = "t1";
pub const NAMESPACE: &str = "default";
pub const STREAM: &str = "config-changes";

/// Named keys for the readable head of the keyspace; the rest are generated.
///
/// The size matters. An earlier version of this demo used twelve keys at 20k
/// changes/sec and measured *zero* permanent divergence, because at that ratio every
/// key is rewritten thousands of times a second and any missed update is corrected
/// almost immediately. That is a real effect worth knowing — churn heals divergence
/// — but it is not what a control-plane config feed looks like. Real configuration
/// is thousands of keys changing a few hundred times a second, where most keys are
/// touched rarely and a missed update can stand for a very long time.
const NAMED_KEYS: &[&str] = &[
    "cluster/membership",
    "routing/table",
    "policy/authz",
    "policy/residency",
    "quota/tenant-limits",
    "cert/rotation",
    "flag/new-checkout",
    "flag/beta-ui",
    "service/discovery",
    "config/log-level",
];

/// Human-readable name for a key index.
pub fn key_name(idx: usize) -> String {
    match NAMED_KEYS.get(idx) {
        Some(name) => (*name).to_string(),
        None => format!("config/key-{idx:04}"),
    }
}

/// Deterministic PRNG so a run is reproducible and needs no rand dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Zipf-ish: key i chosen with weight proportional to 1/(i+1).
    fn weighted_key(&mut self, table: &[usize]) -> usize {
        table[(self.next() % table.len() as u64) as usize]
    }
}

/// Zipf-ish weights: a handful of keys churn constantly, the long tail rarely
/// changes. Uniform access would make divergence look far less serious than it is,
/// because every key would be rewritten — and therefore repaired — at the same rate.
fn weight_table(keys: usize) -> Vec<usize> {
    let mut table = Vec::new();
    for idx in 0..keys {
        let weight = (keys / (idx + 1)).clamp(1, 64);
        for _ in 0..weight {
            table.push(idx);
        }
    }
    table
}

// Payload: key index, version, value. The client API surfaces no sequence number
// or timestamp, so a consumer can only reason about what it received from what the
// producer chose to put in the bytes.
const HEADER_LEN: usize = 18;

fn encode(key: u16, version: u64, value: u64, payload_bytes: usize) -> Bytes {
    let mut buf = Vec::with_capacity(payload_bytes.max(HEADER_LEN));
    buf.extend_from_slice(&key.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&value.to_be_bytes());
    buf.resize(payload_bytes.max(HEADER_LEN), 0);
    Bytes::from(buf)
}

fn decode(payload: &[u8]) -> Option<(usize, u64, u64)> {
    if payload.len() < HEADER_LEN {
        return None;
    }
    let key = u16::from_be_bytes(payload[0..2].try_into().ok()?) as usize;
    let version = u64::from_be_bytes(payload[2..10].try_into().ok()?);
    let value = u64::from_be_bytes(payload[10..18].try_into().ok()?);
    Some((key, version, value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Production defaults: shed under pressure.
    Lossy,
    /// Every checkpoint configured to block instead.
    Lossless,
}

impl Mode {
    pub fn label(self) -> &'static str {
        match self {
            Mode::Lossy => "at-most-once (production defaults)",
            Mode::Lossless => "lossless (block at every checkpoint)",
        }
    }
    pub fn short(self) -> &'static str {
        match self {
            Mode::Lossy => "lossy",
            Mode::Lossless => "lossless",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Converged,
    Stalled,
    Recovering,
    Quiesced,
}

impl Phase {
    /// The victim is whichever consumer is last, so its name depends on
    /// `--consumers` and cannot be baked into the string.
    pub fn label(self, victim: &str) -> String {
        match self {
            Phase::Converged => "converged — every consumer matches the authority".to_string(),
            Phase::Stalled => format!("STALLED — {victim} has stopped applying changes"),
            Phase::Recovering => format!("recovering — {victim} is reading again"),
            Phase::Quiesced => "quiesced — publishing stopped, state has settled".to_string(),
        }
    }
}

/// One consumer's local copy of the keyspace.
#[derive(Debug)]
pub struct ConsumerState {
    pub name: String,
    pub stalled: AtomicBool,
    pub applied: AtomicU64,
    /// Set if the subscription ended before the run did. A consumer that stopped
    /// consuming looks identical to one that lost events, unless we record it:
    /// both simply end up short. Distinguishing them matters because one is a
    /// delivery-semantics result and the other is a broken connection.
    pub ended_early: Mutex<Option<String>>,
    keys: usize,
    /// value per key, and the version it came from. `None` = never seen.
    local: Mutex<Vec<Option<(u64, u64)>>>,
}

impl ConsumerState {
    fn new(name: String, keys: usize) -> Self {
        Self {
            name,
            stalled: AtomicBool::new(false),
            applied: AtomicU64::new(0),
            ended_early: Mutex::new(None),
            keys,
            local: Mutex::new(vec![None; keys]),
        }
    }

    async fn apply(&self, key: usize, version: u64, value: u64) {
        let mut guard = self.local.lock().await;
        // Last-write-wins on version, which is how a consumer maintaining a local
        // copy would actually handle out-of-order or duplicate delivery.
        match guard[key] {
            Some((existing_version, _)) if existing_version >= version => {}
            _ => guard[key] = Some((version, value)),
        }
    }

    /// Keys whose value differs from the authority right now.
    ///
    /// A key the authority has never written is not divergence: with a keyspace
    /// larger than the number of changes in a run, most keys are simply untouched,
    /// and counting "never written" as wrong reported hundreds of false positives
    /// against consumers that had in fact applied every single change.
    pub async fn wrong_keys(&self, authority: &[Option<u64>]) -> Vec<usize> {
        let local = self.local.lock().await;
        (0..self.keys)
            .filter(|&idx| match (local[idx], authority[idx]) {
                (_, None) => false,
                (Some((_, value)), Some(truth)) => value != truth,
                (None, Some(_)) => true,
            })
            .collect()
    }
}

/// The publisher's authoritative copy.
#[derive(Debug, Default)]
pub struct Authority {
    values: Mutex<Vec<Option<u64>>>,
    pub version: AtomicU64,
}

impl Authority {
    fn new(keys: usize) -> Self {
        Self {
            values: Mutex::new(vec![None; keys]),
            version: AtomicU64::new(0),
        }
    }
    pub async fn values(&self) -> Vec<Option<u64>> {
        self.values.lock().await.clone()
    }
}

#[derive(Debug)]
pub struct LiveState {
    pub mode: Mode,
    /// Name of the consumer that stalls, for labelling.
    pub victim: String,
    pub keys: usize,
    pub phase: std::sync::Mutex<Phase>,
    pub consumers: Vec<Arc<ConsumerState>>,
    pub published: AtomicU64,
    /// Wrong-key counts from the most recent sample, one per consumer.
    pub wrong_now: Mutex<Vec<usize>>,
}

impl LiveState {
    pub fn phase(&self) -> Phase {
        *self.phase.lock().expect("phase mutex")
    }
}

/// What one mode produced.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub mode: Mode,
    pub keys: usize,
    pub published: u64,
    pub consumers: Vec<ConsumerOutcome>,
}

#[derive(Debug, Clone)]
pub struct ConsumerOutcome {
    pub name: String,
    pub applied: u64,
    /// Keys whose value differs from the authority once everything settled.
    pub wrong: Vec<usize>,
    pub stalled: bool,
    /// Why this consumer stopped consuming early, if it did.
    pub ended_early: Option<String>,
}

impl Outcome {
    pub fn total_wrong(&self) -> usize {
        self.consumers.iter().map(|c| c.wrong.len()).sum()
    }
    pub fn stalled_wrong(&self) -> usize {
        self.consumers
            .iter()
            .filter(|c| c.stalled)
            .map(|c| c.wrong.len())
            .sum()
    }
    pub fn healthy_wrong(&self) -> usize {
        self.consumers
            .iter()
            .filter(|c| !c.stalled)
            .map(|c| c.wrong.len())
            .sum()
    }
    /// Consumers whose subscription ended before the run did.
    pub fn ended_early(&self) -> Vec<(String, String)> {
        self.consumers
            .iter()
            .filter_map(|c| c.ended_early.clone().map(|why| (c.name.clone(), why)))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Fixtures — duplicated from the other demos, as they are everywhere in this repo.
// ---------------------------------------------------------------------------

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok((
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?,
        cert_der,
    ))
}

fn client_config(
    cert: &CertificateDer<'static>,
    auth: &felix_broker_service::serving::auth::demo::DemoAuth,
    mode: Mode,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    // The client keeps its own bounded subscription queue. Left on the default it
    // sheds before anything the broker is configured to do can matter.
    config.client_sub_queue_policy = match mode {
        Mode::Lossy => ClientSubQueuePolicy::DropNew,
        Mode::Lossless => ClientSubQueuePolicy::Block,
    };
    Ok(config)
}

struct Harness {
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    accept_task: tokio::task::JoinHandle<()>,
    _server: Arc<QuicServer>,
}

async fn start_broker(
    auth: &felix_broker_service::serving::auth::demo::DemoAuth,
    mode: Mode,
    queue_capacity: usize,
) -> Result<Harness> {
    let policy = match mode {
        Mode::Lossy => SubQueuePolicy::DropNew,
        Mode::Lossless => SubQueuePolicy::Block,
    };
    let core = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(queue_capacity)
            .context("configure subscriber queue depth")?
            .with_subscriber_queue_policy(policy),
    );
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

    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.subscriber_queue_capacity = queue_capacity;
    // All four checkpoints must agree. Any one of them left on a shedding default
    // becomes the place loss happens, and the ones downstream never come into play.
    config.subscriber_queue_policy = policy;
    config.subscriber_lane_queue_policy = policy;
    config.pub_ingress_wait = matches!(mode, Mode::Lossless);

    let accept_task = {
        let server = Arc::clone(&server);
        let auth = Arc::clone(&auth.auth);
        tokio::spawn(async move {
            if let Err(err) =
                felix_broker_service::serving::quic::serve(server, core, config, auth).await
            {
                eprintln!("accept loop exited: {err}");
            }
        })
    };

    Ok(Harness {
        addr,
        cert,
        accept_task,
        _server: server,
    })
}

pub struct RunConfig {
    pub mode: Mode,
    pub keys: usize,
    pub consumers: usize,
    pub rate: u64,
    pub payload_bytes: usize,
    pub queue_capacity: usize,
    pub phase_secs: u64,
}

/// Run the scenario once. `on_tick` is called about twice a second for rendering.
pub async fn run_once<F>(config: RunConfig, mut on_tick: F) -> Result<Outcome>
where
    F: FnMut(&LiveState) -> Result<bool>,
{
    let auth = felix_broker_service::serving::auth::demo::demo_auth_for_tenant(TENANT)?;
    let harness = start_broker(&auth, config.mode, config.queue_capacity).await?;

    let consumers: Vec<Arc<ConsumerState>> = (1..=config.consumers)
        .map(|i| Arc::new(ConsumerState::new(format!("consumer-{i}"), config.keys)))
        .collect();
    let victim = Arc::clone(consumers.last().expect("at least one consumer"));
    let authority = Arc::new(Authority::new(config.keys));

    let state = Arc::new(LiveState {
        mode: config.mode,
        victim: victim.name.clone(),
        keys: config.keys,
        phase: std::sync::Mutex::new(Phase::Converged),
        consumers: consumers.clone(),
        published: AtomicU64::new(0),
        wrong_now: Mutex::new(vec![0; config.consumers]),
    });

    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    let (ready_tx, mut ready_rx) = mpsc::channel(config.consumers);

    for consumer in &consumers {
        let client_cfg = client_config(&harness.cert, &auth, config.mode)?;
        let addr = harness.addr;
        let consumer = Arc::clone(consumer);
        let stop = Arc::clone(&stop);
        let ready_tx = ready_tx.clone();
        let keys = config.keys;
        tasks.push(tokio::spawn(async move {
            let client = match Connection::connect(addr, "localhost", client_cfg).await {
                Ok(client) => client,
                Err(err) => {
                    let _ = ready_tx
                        .send(Err(
                            err.context(format!("{} failed to connect", consumer.name))
                        ))
                        .await;
                    return;
                }
            };
            let mut subscription = match client.subscribe(TENANT, NAMESPACE, STREAM).await {
                Ok(sub) => sub,
                Err(err) => {
                    let _ = ready_tx
                        .send(Err(
                            err.context(format!("{} failed to subscribe", consumer.name))
                        ))
                        .await;
                    return;
                }
            };
            if ready_tx.send(Ok(())).await.is_err() {
                return;
            }
            while !stop.load(Ordering::Relaxed) {
                if consumer.stalled.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                match tokio::time::timeout(Duration::from_millis(200), subscription.next_event())
                    .await
                {
                    Ok(Ok(Some(event))) => {
                        if let Some((key, version, value)) = decode(&event.payload)
                            && key < keys
                        {
                            consumer.apply(key, version, value).await;
                            consumer.applied.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(Ok(None)) => {
                        *consumer.ended_early.lock().await =
                            Some("subscription closed".to_string());
                        break;
                    }
                    Ok(Err(err)) => {
                        *consumer.ended_early.lock().await =
                            Some(format!("subscription error: {err}"));
                        break;
                    }
                    Err(_) => {}
                }
            }
        }));
    }

    drop(ready_tx);
    for _ in 0..consumers.len() {
        tokio::time::timeout(Duration::from_secs(10), ready_rx.recv())
            .await
            .context("timed out waiting for consumers to subscribe")?
            .context("consumer readiness channel closed")??;
    }

    let publish_stop = Arc::new(AtomicBool::new(false));
    let publisher_task = {
        let client_cfg = client_config(&harness.cert, &auth, config.mode)?;
        let addr = harness.addr;
        let state = Arc::clone(&state);
        let authority = Arc::clone(&authority);
        let publish_stop = Arc::clone(&publish_stop);
        let payload_bytes = config.payload_bytes;
        let rate = config.rate;
        let keys = config.keys;
        tokio::spawn(async move {
            let client = match Connection::connect(addr, "localhost", client_cfg).await {
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
            let table = weight_table(keys);
            let mut rng = Rng::new(0x5EED);
            // Pace by batching per tick, with a tick sized to the target rate.
            // `rate / 1000` against a fixed 1 ms tick truncates to zero for any
            // rate below 1000/s and then clamps to 1, which silently published at
            // 1000/s no matter what was asked for — the documented 400/s default
            // was really 1000/s.
            let (tick, per_tick) = if rate >= 1000 {
                (Duration::from_millis(1), rate / 1000)
            } else {
                (Duration::from_micros(1_000_000 / rate.max(1)), 1)
            };
            let mut ticker = tokio::time::interval(tick);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

            while !publish_stop.load(Ordering::Relaxed) {
                ticker.tick().await;
                for _ in 0..per_tick {
                    let key = rng.weighted_key(&table);
                    let version = authority.version.fetch_add(1, Ordering::Relaxed) + 1;
                    let value = rng.next();
                    // The authority is updated first: it is the source of truth, and
                    // whether the change reaches anyone is a separate question.
                    {
                        let mut values = authority.values.lock().await;
                        values[key] = Some(value);
                    }
                    let body = encode(key as u16, version, value, payload_bytes);
                    if publisher
                        .publish(TENANT, NAMESPACE, STREAM, body.to_vec(), AckMode::None)
                        .await
                        .is_ok()
                    {
                        state.published.fetch_add(1, Ordering::Relaxed);
                    }
                    if publish_stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        })
    };

    // Phase schedule. Publishing stops the moment the stall ends, and this is the
    // load-bearing detail: while changes keep flowing, every key is eventually
    // rewritten and a consumer's mistakes are silently repaired. That masks the
    // problem entirely. Real config feeds are bursty — a deploy, then quiet — and
    // it is the quiet that makes a missed update permanent. So the stall is
    // followed by silence, and the consumer is given every chance to catch up on
    // whatever is still queued for it before anything is measured.
    let secs = config.phase_secs;
    let schedule = |elapsed: u64| -> Phase {
        if elapsed < secs {
            Phase::Converged
        } else if elapsed < secs * 3 {
            Phase::Stalled
        } else if elapsed < secs * 4 {
            Phase::Recovering
        } else {
            Phase::Quiesced
        }
    };
    let total = secs * 5;

    let started = std::time::Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    while started.elapsed().as_secs() < total {
        ticker.tick().await;
        let elapsed = started.elapsed().as_secs();
        let phase = schedule(elapsed);
        *state.phase.lock().expect("phase mutex") = phase;
        victim
            .stalled
            .store(matches!(phase, Phase::Stalled), Ordering::Relaxed);
        // Silence begins when the stall ends.
        if matches!(phase, Phase::Recovering | Phase::Quiesced) {
            publish_stop.store(true, Ordering::Relaxed);
        }

        let authoritative = authority.values().await;
        let mut wrong = Vec::with_capacity(consumers.len());
        for consumer in &consumers {
            wrong.push(consumer.wrong_keys(&authoritative).await.len());
        }
        *state.wrong_now.lock().await = wrong;

        if !on_tick(&state)? {
            break;
        }
    }

    publish_stop.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(5), publisher_task).await;
    // Give delivery a moment to fully settle before the final diff, so anything
    // still in flight is not miscounted as lost.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let authoritative = authority.values().await;
    let mut results = Vec::new();
    for consumer in &consumers {
        let wrong = consumer.wrong_keys(&authoritative).await;
        results.push(ConsumerOutcome {
            name: consumer.name.clone(),
            applied: consumer.applied.load(Ordering::Relaxed),
            wrong,
            stalled: Arc::ptr_eq(consumer, &victim),
            ended_early: consumer.ended_early.lock().await.clone(),
        });
    }

    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    harness.accept_task.abort();
    let _ = harness.accept_task.await;

    Ok(Outcome {
        mode: config.mode,
        keys: config.keys,
        published: state.published.load(Ordering::Relaxed),
        consumers: results,
    })
}
