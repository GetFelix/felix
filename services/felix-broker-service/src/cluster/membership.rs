//! Broker-side cluster membership.
//!
//! The broker claims a stable identity on boot, reports health on an interval,
//! and says goodbye on the way out. The control plane turns that into the
//! catalog placement reads.
//!
//! The distinction this module exists to preserve: a broker that deregisters is
//! `left`, and one that simply stops is found `down` by heartbeat expiry. Both
//! remove it from placement, but only the first is intentional, so only the
//! first should be silent in the logs.
//!
//! Registration happens *after* the broker can serve. Registering earlier
//! advertises a node that placement may immediately use and that cannot yet
//! answer.
pub mod metrics;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use felix_common::fleet::FleetGate;
use felix_common::membership::NodeLifecycle;
use felix_replication::controlplane_link::ControlPlaneLink;
use felix_storage::StorageId;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::cluster::membership::metrics as mm;
use crate::config::MembershipConfig;

/// Ceiling on heartbeat retry backoff.
///
/// Bounded so a broker that was unreachable for an hour resumes reporting
/// within one ceiling of the control plane coming back, rather than after a
/// doubling interval measured in hours.
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Retries during an outage are also capped at this fraction of the lease.
///
/// The control plane expires a node one full window after its last accepted
/// heartbeat, so once it answers again the broker has to get a heartbeat in
/// quickly. A fifth, not a quarter, so that jitter cannot push a retry past a
/// quarter of the lease.
const LEASE_RETRY_FRACTION: u32 = 5;

/// A single heartbeat is abandoned after this fraction of the lease. Retried
/// on the capped backoff above, a stalled control plane still gets at least
/// two attempts per lease.
const LEASE_REQUEST_FRACTION: u32 = 4;

/// Fraction of a delay that jitter may add.
///
/// Restarting a cluster puts every broker on the same schedule; without jitter
/// they heartbeat in lockstep and the control plane sees the whole fleet at
/// once, every interval.
const JITTER_FRACTION: f64 = 0.2;

#[derive(Debug, Serialize)]
struct RegistrationRequest<'a> {
    node_id: &'a str,
    advertise_addr: &'a str,
    /// Omitted entirely when unset, so a broker that offers clients no address
    /// registers exactly the body it did before this field existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    client_addr: Option<&'a str>,
    /// Only a broker running the Kafka listener sends one.
    #[serde(skip_serializing_if = "Option::is_none")]
    kafka_addr: Option<&'a str>,
    region: &'a str,
    /// Omitted when unset, so a broker without a zone registers the body it
    /// always did.
    #[serde(skip_serializing_if = "Option::is_none")]
    zone: Option<&'a str>,
    /// Omitted when empty, so a build implementing no fleet features sends
    /// the body it always did.
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    features: &'a BTreeSet<String>,
    /// The data directory's identity, as 32 lowercase hex digits. Omitted
    /// when unset, so the body is the one a broker sent before storage ids.
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegistrationResponse {
    node: NodeView,
    heartbeat_interval_ms: u64,
    /// Absent from a control plane that predates fleet features, which
    /// enables none.
    #[serde(default)]
    fleet_features: BTreeSet<String>,
}

/// `GET /v1/nodes/{node_id}`.
#[derive(Debug, Deserialize)]
struct NodeDetail {
    node: NodeView,
}

/// The longest wait between asks at startup, where nothing is served until
/// the control plane answers.
const STARTUP_RETRY_CAP: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
struct NodeView {
    status: NodeStatusView,
}

#[derive(Debug, Deserialize)]
struct NodeStatusView {
    incarnation: u64,
    lifecycle: String,
    /// Absent from a broker that registered before storage ids.
    #[serde(default)]
    storage_id: Option<String>,
}

/// What the control plane holds from a node's last registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    pub incarnation: u64,
    /// The data directory's identity it registered with, as 32 lowercase
    /// hex digits. `None` from a broker that predates storage ids.
    pub storage_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct HeartbeatRequest {
    incarnation: u64,
    /// Leaders of shards this broker follows that have stopped answering it.
    /// Left out when there are none, so the request is what an older control
    /// plane always read.
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    suspects: BTreeSet<String>,
    /// Set while the lease waits on the routes, which then have to reach the
    /// answer's `assignment_seq`. Left out otherwise, as before.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    want_assignment_seq: bool,
}

#[derive(Debug, Deserialize)]
struct HeartbeatResponse {
    /// An enum, not a string: a typo in a comparison is a silent false, and a
    /// state added later would read as one too. `Unknown` covers the latter,
    /// and is not placeable — a broker that cannot understand its own status
    /// must not assume it may keep leading shards.
    lifecycle: NodeLifecycle,
    heartbeat_interval_ms: u64,
    /// How long the control plane will wait before declaring this node down.
    /// The broker's lease is derived from it, so the two ends cannot disagree
    /// about when authority to serve ends.
    ///
    /// Defaulted rather than required: a control plane predating this field
    /// leaves the broker on its conservative initial lease instead of failing to
    /// parse the response and losing membership entirely.
    #[serde(default)]
    expiry_timeout_ms: Option<u64>,
    #[serde(default)]
    fleet_features: BTreeSet<String>,
    /// The assignment feed's head as of the accept, when asked for. Absent
    /// from a control plane that predates it.
    #[serde(default)]
    assignment_seq: Option<u64>,
}

/// A registered identity, and what the control plane told us about it.
#[derive(Debug, Clone)]
pub struct Registration {
    pub node_id: String,
    /// Carried so the heartbeat loop does not need the whole config.
    ///
    /// The shared holder, not a copy: heartbeats run for the life of the
    /// process and outlast any one access token.
    pub token: crate::cluster::credential::NodeCredential,
    /// This process's incarnation. Sent with every heartbeat so one delayed
    /// past a restart is rejected instead of counted for its successor.
    pub incarnation: u64,
    pub heartbeat_interval_ms: u64,
    /// The fleet features enabled as of this registration.
    pub fleet_features: BTreeSet<String>,
}

/// Why a membership call did not succeed.
///
/// The split decides whether to retry, and it is also what the metrics report:
/// a control plane that is still starting will accept the same request in a
/// moment, while one that refused the identity will refuse it forever.
/// Collapsing the two makes a misconfigured broker look like a flaky network.
#[derive(Debug)]
pub enum MembershipError {
    /// The control plane answered and said no. Terminal.
    Rejected(String),
    /// Nothing answered, or it failed internally.
    Unavailable(anyhow::Error),
}

impl MembershipError {
    /// Metric label for this failure. Bounded to two values.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Rejected(_) => mm::KIND_REJECTED,
            Self::Unavailable(_) => mm::KIND_UNAVAILABLE,
        }
    }

    /// Classify an HTTP response. A 4xx is the server refusing; anything else
    /// may simply be a control plane still coming up.
    fn from_status(status: reqwest::StatusCode, message: String) -> Self {
        if status.is_client_error() {
            Self::Rejected(message)
        } else {
            Self::Unavailable(anyhow!(message))
        }
    }
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) => write!(f, "{message}"),
            Self::Unavailable(err) => write!(f, "{err}"),
        }
    }
}

/// Claim this broker's identity, reporting the identity of its data
/// directory with it when `storage_id` is set.
///
/// Fails loudly. A broker that cannot register is not a cluster member, and
/// carrying on as if it were means publishing to a node no one will route to.
pub async fn register(
    link: &impl ControlPlaneLink,
    config: &MembershipConfig,
    credential: &crate::cluster::credential::NodeCredential,
    storage_id: Option<StorageId>,
) -> std::result::Result<Registration, MembershipError> {
    let response = link
        .register(
            &credential.bearer(),
            &RegistrationRequest {
                node_id: &config.node_id,
                advertise_addr: &config.advertise_addr,
                client_addr: config.client_advertise_addr.as_deref(),
                kafka_addr: config.kafka_advertise_addr.as_deref(),
                region: &config.region,
                zone: config.zone.as_deref(),
                features: &config.features,
                storage_id: storage_id.map(|id| id.to_hex()),
            },
        )
        .await
        .with_context(|| format!("register node {} with {}", config.node_id, link.endpoint()))
        .map_err(MembershipError::Unavailable)?;

    let status = response.status;
    if !status.is_success() {
        let body = response.text();
        return Err(MembershipError::from_status(
            status,
            format!(
                "control plane rejected registration of {} ({status}): {body}",
                config.node_id
            ),
        ));
    }

    let registered: RegistrationResponse = response
        .json()
        .context("decode node registration response")
        .map_err(MembershipError::Unavailable)?;

    mm::record_registration("registered");
    mm::record_membership_live(true);
    tracing::info!(
        node_id = %config.node_id,
        advertise_addr = %config.advertise_addr,
        incarnation = registered.node.status.incarnation,
        lifecycle = %registered.node.status.lifecycle,
        "registered with the control plane",
    );

    Ok(Registration {
        node_id: config.node_id.clone(),
        token: credential.clone(),
        incarnation: registered.node.status.incarnation,
        heartbeat_interval_ms: registered.heartbeat_interval_ms,
        fleet_features: registered.fleet_features,
    })
}

/// The control plane's record of `node_id`'s last registration, or `None`
/// when it has never registered it.
pub async fn registered(
    link: &impl ControlPlaneLink,
    node_id: &str,
    credential: &crate::cluster::credential::NodeCredential,
) -> std::result::Result<Option<Registered>, MembershipError> {
    // A refreshed credential can still come in, so a 401 is waited out.
    lookup(link, node_id, credential)
        .await
        .map_err(|failure| match failure {
            LookupFailure::Down(err)
            | LookupFailure::Unauthorized(err)
            | LookupFailure::Undecodable(err) => MembershipError::Unavailable(err),
            LookupFailure::Rejected(message) => MembershipError::Rejected(message),
        })
}

/// Why one lookup of a node's registration got no answer.
///
/// A start without the control plane needs it down, not refusing: a 401
/// means it is up and has not accepted this credential, which waiting can
/// fix and starting from the view must not paper over.
#[derive(Debug)]
pub(crate) enum LookupFailure {
    /// Nothing answered, or it answered with a 5xx.
    Down(anyhow::Error),
    /// It answered 401.
    Unauthorized(anyhow::Error),
    /// It answered 200 with a body this build cannot read: up, so not down.
    Undecodable(anyhow::Error),
    /// It answered with another 4xx. Final.
    Rejected(String),
}

async fn lookup(
    link: &impl ControlPlaneLink,
    node_id: &str,
    credential: &crate::cluster::credential::NodeCredential,
) -> std::result::Result<Option<Registered>, LookupFailure> {
    let response = link
        .node(&credential.bearer(), node_id)
        .await
        .with_context(|| format!("look up node {node_id} at {}", link.endpoint()))
        .map_err(LookupFailure::Down)?;
    let status = response.status;
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        let body = response.text();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(LookupFailure::Unauthorized(anyhow!(
                "control plane did not accept the credential looking up {node_id} ({status}): {body}"
            )));
        }
        let message = format!("control plane refused to look up {node_id} ({status}): {body}");
        return Err(if status.is_client_error() {
            LookupFailure::Rejected(message)
        } else {
            LookupFailure::Down(anyhow!(message))
        });
    }
    let detail: NodeDetail = response
        .json()
        .context("decode node lookup response")
        .map_err(LookupFailure::Undecodable)?;
    Ok(Some(Registered {
        incarnation: detail.node.status.incarnation,
        storage_id: detail.node.status.storage_id,
    }))
}

/// What the startup lookup got within its bound.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Bounded {
    Answered(Option<Registered>),
    /// Every attempt found the control plane down: no answer, a timeout or a
    /// 5xx.
    Down,
    /// It answered and refused, did not accept the credential, or sent an
    /// answer that does not decode, at least once.
    Refusing,
}

/// [`registered`], asked for about `wait`, telling a control plane that is
/// down from one that refuses. One attempt is made even with no wait.
pub(crate) async fn registered_within(
    link: &impl ControlPlaneLink,
    node_id: &str,
    credential: &crate::cluster::credential::NodeCredential,
    wait: Duration,
) -> Bounded {
    let deadline = tokio::time::Instant::now() + wait;
    let mut refused = false;
    let mut attempt = 0u64;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        // An attempt that has not answered in time counts as down. Each gets
        // at least a second, so a short wait still asks once properly.
        let bound = remaining.max(Duration::from_secs(1));
        match tokio::time::timeout(bound, lookup(link, node_id, credential)).await {
            Ok(Ok(registered)) => return Bounded::Answered(registered),
            Ok(Err(LookupFailure::Rejected(message))) => {
                tracing::warn!(node_id, error = %message, "the control plane refused the startup lookup");
                return Bounded::Refusing;
            }
            Ok(Err(LookupFailure::Unauthorized(err))) => {
                refused = true;
                tracing::warn!(node_id, error = %err, "the control plane did not accept this node's credential at startup");
            }
            Ok(Err(LookupFailure::Undecodable(err))) => {
                refused = true;
                tracing::warn!(node_id, error = %err, "the control plane's answer to the startup lookup does not decode");
            }
            Ok(Err(LookupFailure::Down(err))) => {
                tracing::warn!(node_id, error = %err, "the control plane did not answer the startup lookup");
            }
            Err(_) => {
                tracing::warn!(node_id, "the startup lookup timed out");
            }
        }
        attempt += 1;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let delay = backoff(Duration::from_millis(500), attempt).min(STARTUP_RETRY_CAP);
        tokio::time::sleep(jittered(delay).min(remaining)).await;
    }
    if refused {
        Bounded::Refusing
    } else {
        Bounded::Down
    }
}

/// [`registered`], asked until the control plane answers. Fails only when
/// it refuses. The broker announces no storage id until this returns, so an
/// outage holds startup rather than letting a directory older than the
/// node's last registration be counted on.
pub async fn await_registered(
    link: &impl ControlPlaneLink,
    node_id: &str,
    credential: &crate::cluster::credential::NodeCredential,
) -> Result<Option<Registered>> {
    let mut attempt = 0u64;
    loop {
        match registered(link, node_id, credential).await {
            Ok(registered) => return Ok(registered),
            Err(MembershipError::Rejected(message)) => return Err(anyhow!(message)),
            Err(MembershipError::Unavailable(err)) => {
                attempt += 1;
                tracing::warn!(
                    node_id,
                    attempt,
                    error = %err,
                    "could not ask the control plane for this node's incarnation; the data \
                     directory's storage id is not announced until it answers; retrying",
                );
                let delay = backoff(Duration::from_millis(500), attempt).min(STARTUP_RETRY_CAP);
                tokio::time::sleep(jittered(delay)).await;
            }
        }
    }
}

/// Why [`run_heartbeat`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatEnd {
    /// `shutdown` fired.
    Shutdown,
    /// The control plane answered that this node is `down`. The registration
    /// is spent: the broker has to register again, under a new incarnation,
    /// before placement will use it.
    MarkedDown,
}

/// Report health until `shutdown` fires or the control plane says this node is
/// down.
///
/// A failed heartbeat is retried with bounded exponential backoff. It is never
/// fatal: the control plane being briefly unreachable must not take down a
/// broker that is otherwise serving fine. If it stays unreachable past the
/// expiry timeout the control plane marks this node down on its own, and the
/// next answer says so.
#[allow(clippy::too_many_arguments)]
pub async fn run_heartbeat(
    link: impl ControlPlaneLink,
    registration: Registration,
    shutdown: CancellationToken,
    consecutive_failures: Arc<AtomicU64>,
    lease: Arc<crate::cluster::lease::LeaseState>,
    fleet: Arc<FleetGate>,
    suspects: Arc<felix_replication::suspicion::Suspects>,
) -> HeartbeatEnd {
    run_heartbeat_checked(
        link,
        registration,
        shutdown,
        consecutive_failures,
        lease,
        fleet,
        suspects,
        None,
    )
    .await
}

/// [`run_heartbeat`], skipping every beat while `pulse` says the broker's
/// main runtime has stopped.
#[allow(clippy::too_many_arguments)]
async fn run_heartbeat_checked(
    link: impl ControlPlaneLink,
    registration: Registration,
    shutdown: CancellationToken,
    consecutive_failures: Arc<AtomicU64>,
    lease: Arc<crate::cluster::lease::LeaseState>,
    fleet: Arc<FleetGate>,
    suspects: Arc<felix_replication::suspicion::Suspects>,
    pulse: Option<Arc<RuntimePulse>>,
) -> HeartbeatEnd {
    let mut suspected = suspects.subscribe();
    let mut interval = Duration::from_millis(registration.heartbeat_interval_ms.max(1));
    let mut last_success = std::time::Instant::now();

    loop {
        let failures = consecutive_failures.load(Ordering::Acquire);
        let delay = if failures == 0 {
            interval
        } else {
            backoff(interval, failures).min(retry_cap(lease.usable()))
        };

        tokio::select! {
            _ = shutdown.cancelled() => return HeartbeatEnd::Shutdown,
            _ = tokio::time::sleep(jittered(delay)) => {}
            // A new suspicion goes now rather than an interval later: the
            // control plane promotes on it. Not while backing off.
            Ok(()) = suspected.changed(), if failures == 0 => {}
        }
        let suspects = suspected.borrow_and_update().clone();
        if let Some(silent) = pulse.as_ref().and_then(|pulse| pulse.hung()) {
            // Renewing here would keep the shards this broker leads on a
            // process that serves nothing. Letting the lease run out hands
            // them to a live copy.
            tracing::error!(
                node_id = %registration.node_id,
                silent_ms = silent.as_millis() as u64,
                "the broker's runtime has stopped; not renewing the lease",
            );
            continue;
        }

        // Before the request, not after the answer: see `LeaseState::renew_at`.
        let sent = lease.now();
        // Read per heartbeat, so a refresh between beats is picked up without
        // this loop knowing refresh exists.
        match send_heartbeat(
            &link,
            &registration.node_id,
            &registration.token.bearer(),
            HeartbeatRequest {
                incarnation: registration.incarnation,
                suspects,
                want_assignment_seq: lease.awaits_routes(),
            },
            lease.usable() / LEASE_REQUEST_FRACTION,
        )
        .await
        {
            Ok(response) => {
                consecutive_failures.store(0, Ordering::Release);
                last_success = std::time::Instant::now();
                // The heartbeat *is* the lease renewal. Renewed only on an
                // accepted response, so a control plane that answers "you are
                // not live" does not extend the authority to serve.
                if response.lifecycle.is_placeable() {
                    if let Some(expiry) = response.expiry_timeout_ms {
                        lease.adopt(Duration::from_millis(expiry.max(1)));
                    }
                    lease.renew_at(sent, response.assignment_seq);
                }
                mm::record_heartbeat_success();
                for feature in fleet.observe(response.fleet_features.iter().map(String::as_str)) {
                    tracing::info!(feature, "an operator finalized this fleet feature; enabled");
                    mm::record_fleet_feature(feature, true);
                }
                // The control plane owns the cadence, so a change to it takes
                // effect without touching broker configuration.
                interval = Duration::from_millis(response.heartbeat_interval_ms.max(1));

                let placeable = response.lifecycle.is_placeable();
                mm::record_membership_live(placeable);
                if !placeable {
                    // Told outright that it is not a member. Waiting out the
                    // remaining margin would serve a shard the control plane may
                    // already have reassigned.
                    lease.surrender();
                    tracing::warn!(
                        node_id = %registration.node_id,
                        lifecycle = %response.lifecycle,
                        "the control plane no longer considers this broker live; \
                         surrendering the lease",
                    );
                }
                // A heartbeat never revives a down node, so carrying on here
                // would keep a healthy broker out of the cluster for good.
                // `left` is not handed back: that is a deregistration someone
                // asked for, and registering over it would undo it.
                if response.lifecycle == NodeLifecycle::Down {
                    return HeartbeatEnd::MarkedDown;
                }
            }
            Err(err) => {
                let failures = consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
                mm::record_heartbeat_failure(err.kind());
                // Published on failure too: this is the number that keeps rising
                // while the control plane is unreachable, and the only warning a
                // broker gets that it is about to be declared down.
                mm::record_heartbeat_age(last_success.elapsed());
                tracing::warn!(
                    node_id = %registration.node_id,
                    consecutive_failures = failures,
                    error = %err,
                    "heartbeat failed; retrying with backoff",
                );
            }
        }
    }
}

/// Stop receiving new placement, without stopping service.
pub async fn drain(link: &impl ControlPlaneLink, node_id: &str, token: &str) -> Result<()> {
    post_lifecycle(link, node_id, token, "drain").await
}

/// Leave the cluster on purpose, so this is not mistaken for a crash.
pub async fn deregister(link: &impl ControlPlaneLink, node_id: &str, token: &str) -> Result<()> {
    post_lifecycle(link, node_id, token, "deregister").await
}

/// Everything the broker needs to keep its membership current.
pub struct MembershipTask {
    /// Ends when the broker stops.
    pub handle: tokio::task::JoinHandle<()>,
    /// Cancelled when registration was refused outright, so the process can
    /// exit with the reason rather than run on as a non-member.
    pub fatal: CancellationToken,
    /// Consecutive heartbeat failures, exposed so shutdown and metrics can see
    /// whether membership is currently healthy.
    pub consecutive_failures: Arc<AtomicU64>,
    /// This broker's authority to serve the shards it leads. Renewed by the
    /// heartbeat below; read by the publish path.
    pub lease: Arc<crate::cluster::lease::LeaseState>,
    /// Which fleet features an operator has enabled, kept current by
    /// registration and heartbeats.
    pub fleet: Arc<FleetGate>,
}

/// Register once the broker can serve, then report health until shutdown.
///
/// `serving` gates registration: advertising a node before it can answer means
/// placement may route to it and get nothing. A registration refused with a 4xx
/// cancels `fatal` instead of retrying, because a wrong identity stays wrong.
///
/// A broker the control plane has marked down registers again, which is the
/// only way back into placement. Without that, any outage longer than the
/// expiry window would leave every broker running but out of the cluster.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    link: impl ControlPlaneLink + Clone,
    config: MembershipConfig,
    credential: crate::cluster::credential::NodeCredential,
    serving: CancellationToken,
    shutdown: CancellationToken,
    lease: Arc<crate::cluster::lease::LeaseState>,
    fleet: Arc<FleetGate>,
    suspects: Arc<felix_replication::suspicion::Suspects>,
    incarnation: Arc<AtomicU64>,
    registered: tokio::sync::watch::Sender<Option<u64>>,
    storage_id: Option<StorageId>,
    record_in: Option<std::path::PathBuf>,
    restore_check: Option<CancellationToken>,
) -> MembershipTask {
    spawn_with(
        link,
        config,
        credential,
        serving,
        shutdown,
        lease,
        fleet,
        suspects,
        incarnation,
        registered,
        storage_id,
        record_in.clone(),
        record_in.map(crate::cluster::controlplane_view::ViewFile::new),
        restore_check,
        true,
    )
}

/// [`spawn`], on a thread of its own when `own_thread`, else on the current
/// runtime. The simulator wants the latter: its virtual clock and network live
/// on one runtime, which a heartbeat on a real-time runtime could not reach.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_with(
    link: impl ControlPlaneLink + Clone,
    config: MembershipConfig,
    credential: crate::cluster::credential::NodeCredential,
    serving: CancellationToken,
    shutdown: CancellationToken,
    lease: Arc<crate::cluster::lease::LeaseState>,
    fleet: Arc<FleetGate>,
    suspects: Arc<felix_replication::suspicion::Suspects>,
    incarnation: Arc<AtomicU64>,
    registered: tokio::sync::watch::Sender<Option<u64>>,
    storage_id: Option<StorageId>,
    record_in: Option<std::path::PathBuf>,
    view: Option<crate::cluster::controlplane_view::ViewFile>,
    restore_check: Option<CancellationToken>,
    own_thread: bool,
) -> MembershipTask {
    let fatal = CancellationToken::new();
    let consecutive_failures = Arc::new(AtomicU64::new(0));
    // On the current runtime the heartbeat cannot outlive a hung runtime, so
    // there is nothing for the pulse to prove.
    let pulse = own_thread.then(|| RuntimePulse::start(RUNTIME_HUNG_AFTER, shutdown.clone()));
    let task = {
        let fatal = fatal.clone();
        let consecutive_failures = Arc::clone(&consecutive_failures);
        let lease = Arc::clone(&lease);
        let fleet = Arc::clone(&fleet);
        async move {
            // A start from the control-plane view skipped the restore check
            // (#1334), so it runs first, before anything else the control
            // plane says is applied or this node registers.
            if let Some(checked) = restore_check {
                match deferred_restore_check(
                    &link,
                    &config.node_id,
                    &credential,
                    record_in.as_deref(),
                    storage_id,
                    &shutdown,
                )
                .await
                {
                    Some(true) => checked.cancel(),
                    Some(false) => {
                        mm::record_membership_live(false);
                        forget_view(view.as_ref()).await;
                        fatal.cancel();
                        return;
                    }
                    None => return,
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = serving.cancelled() => {}
            }

            loop {
                let mut attempt = 0u64;
                let registration = loop {
                    match register(&link, &config, &credential, storage_id).await {
                        Ok(registration) => break registration,
                        Err(MembershipError::Rejected(message)) => {
                            mm::record_registration(mm::KIND_REJECTED);
                            mm::record_membership_live(false);
                            tracing::error!(
                                node_id = %config.node_id,
                                error = %message,
                                "the control plane refused this identity; the broker is not a cluster member",
                            );
                            forget_view(view.as_ref()).await;
                            fatal.cancel();
                            return;
                        }
                        Err(MembershipError::Unavailable(err)) => {
                            attempt += 1;
                            mm::record_registration(mm::KIND_UNAVAILABLE);
                            tracing::warn!(
                                node_id = %config.node_id,
                                attempt,
                                error = %err,
                                "could not reach the control plane to register; retrying",
                            );
                            let delay = backoff(Duration::from_millis(500), attempt)
                                .min(retry_cap(lease.usable()));
                            tokio::select! {
                                _ = shutdown.cancelled() => return,
                                _ = tokio::time::sleep(jittered(delay)) => {}
                            }
                        }
                    }
                };
                if let Some(reason) =
                    join_refusal(record_in.as_deref(), &registration.fleet_features).await
                {
                    mm::record_registration(mm::KIND_REJECTED);
                    mm::record_membership_live(false);
                    tracing::error!(
                        node_id = %config.node_id,
                        error = %reason,
                        "this broker's data directory needs a fleet feature the fleet has not finalized",
                    );
                    // Registered already, so placement could pick it; leave
                    // before stopping rather than wait for the expiry.
                    if let Err(err) =
                        deregister(&link, &config.node_id, &registration.token.bearer()).await
                    {
                        tracing::warn!(error = %err, "could not deregister after refusing to join");
                    }
                    forget_view(view.as_ref()).await;
                    fatal.cancel();
                    return;
                }
                if let Some(root) = &record_in {
                    record_registration(root, registration.incarnation).await;
                }
                consecutive_failures.store(0, Ordering::Release);
                adopt_fleet(&fleet, &registration.fleet_features);
                incarnation.store(registration.incarnation, Ordering::Release);
                registered.send_replace(Some(registration.incarnation));

                match run_heartbeat_checked(
                    link.clone(),
                    registration,
                    shutdown.clone(),
                    Arc::clone(&consecutive_failures),
                    Arc::clone(&lease),
                    Arc::clone(&fleet),
                    Arc::clone(&suspects),
                    pulse.clone(),
                )
                .await
                {
                    HeartbeatEnd::Shutdown => return,
                    HeartbeatEnd::MarkedDown => {
                        tracing::warn!(
                            node_id = %config.node_id,
                            "the control plane marked this broker down; registering again",
                        );
                    }
                }
            }
        }
    };
    let handle = if own_thread {
        spawn_on_own_thread(task)
    } else {
        tokio::spawn(task)
    };

    MembershipTask {
        handle,
        fatal,
        consecutive_failures,
        lease,
        fleet,
    }
}

/// Remove the control-plane view durably, before a broker stops because the
/// control plane refused it or found its directory replaced, so a restart
/// during an outage does not vote from it. Through the writer's own file, so
/// the writer does not put it back before the stop.
async fn forget_view(view: Option<&crate::cluster::controlplane_view::ViewFile>) {
    if let Some(view) = view {
        view.forget_now().await;
    }
}

/// What the control plane's record of this node says about a data directory
/// a start from the view opened without asking it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Directory {
    /// The directory is the one the node last registered with.
    Current,
    /// The control plane registered the node at an incarnation the directory
    /// never recorded: it is a restored or copied snapshot. Its storage id
    /// was rotated to `rotated`, as a start that asked first would have.
    Restored { rotated: StorageId },
    /// The control plane last registered the node under another storage id.
    Replaced,
}

/// Judge the directory at `root`, running under `id`, against `registered`,
/// and record what a start that asked first would have recorded: the
/// replaced-storage marker, and for a restored copy a new storage id.
pub(crate) fn judge_directory(
    root: &std::path::Path,
    id: StorageId,
    registered: Option<&Registered>,
) -> Result<Directory> {
    let recorded = felix_storage::run_state::read_registration(root)
        .with_context(|| format!("read the registration record under {}", root.display()))?;
    // The marker names the id the directory will register next and the
    // control plane's incarnation now, so only a view from a registration
    // after this one starts this directory again.
    let after = registered.map_or(0, |registered| registered.incarnation);
    let replaced = |under: StorageId| {
        felix_storage::run_state::record_replaced(root, under, after)
            .with_context(|| format!("record a replaced storage id under {}", root.display()))
    };
    if recorded.behind(registered.map(|registered| registered.incarnation)) {
        let rotated = crate::node::storage::rotate_storage_id_after(root, replaced)?;
        return Ok(Directory::Restored { rotated });
    }
    if registered
        .and_then(|registered| registered.storage_id.as_deref())
        .is_some_and(|prior| prior != id.to_hex())
    {
        replaced(id)?;
        return Ok(Directory::Replaced);
    }
    Ok(Directory::Current)
}

/// Ask for this node's registration until the control plane answers, then
/// [`judge_directory`]. Returns whether the node may register, or `None` if
/// the broker stopped first.
async fn deferred_restore_check(
    link: &impl ControlPlaneLink,
    node_id: &str,
    credential: &crate::cluster::credential::NodeCredential,
    root: Option<&std::path::Path>,
    storage_id: Option<StorageId>,
    shutdown: &CancellationToken,
) -> Option<bool> {
    let mut attempt = 0u64;
    let answer = loop {
        let asked = tokio::select! {
            _ = shutdown.cancelled() => return None,
            asked = lookup(link, node_id, credential) => asked,
        };
        match asked {
            Ok(answer) => break answer,
            Err(LookupFailure::Rejected(message)) => {
                tracing::error!(node_id, error = %message, "the control plane refused to look up this node");
                return Some(false);
            }
            // Up but refusing is not an outage an operator can wait out
            // unseen: a credential or version problem needs fixing.
            Err(LookupFailure::Unauthorized(err)) => {
                tracing::warn!(node_id, error = %err, "the control plane did not accept this node's credential for the restore check; retrying");
            }
            Err(LookupFailure::Undecodable(err)) => {
                tracing::warn!(node_id, error = %err, "the control plane's answer to the restore check does not decode; retrying");
            }
            Err(LookupFailure::Down(err)) => {
                tracing::debug!(node_id, error = %err, "the control plane is still down");
            }
        }
        attempt += 1;
        let delay = backoff(Duration::from_millis(500), attempt).min(STARTUP_RETRY_CAP);
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(jittered(delay)) => {}
        }
    };
    let (Some(root), Some(id)) = (root, storage_id) else {
        return Some(true);
    };
    let root = root.to_path_buf();
    let judged = tokio::task::spawn_blocking(move || judge_directory(&root, id, answer.as_ref()))
        .await
        .map_err(anyhow::Error::from)
        .and_then(|judged| judged);
    match judged {
        Ok(Directory::Current) => {
            tracing::info!(
                node_id,
                "the control plane answered; this data directory is the one it registered"
            );
            Some(true)
        }
        Ok(Directory::Restored { rotated }) => {
            mm::record_restored_directory();
            tracing::error!(
                node_id,
                previous = %id,
                storage_id = %rotated,
                "this data directory is older than the node's last registration, as a restored \
                 snapshot or a copy is, and it ran from its control-plane view while the control \
                 plane was down; gave it a new storage id and stopping, so it starts again as a \
                 copy peers rebuild. A restore must delete storage-id or use \
                 `felix-broker restore-point`"
            );
            Some(false)
        }
        Ok(Directory::Replaced) => {
            tracing::error!(
                node_id,
                storage_id = %id,
                "the control plane last registered this node under another storage id; \
                 stopping so the next start joins as a replaced directory"
            );
            Some(false)
        }
        Err(err) => {
            tracing::error!(node_id, error = %err, "could not check this data directory against the control plane; stopping");
            Some(false)
        }
    }
}

/// Run `task` on a thread of its own, with a runtime of its own.
///
/// The heartbeat is the lease renewal, so it must not share workers with
/// anything that can block on disk: a broker whose every runtime worker waits
/// on a stalled filesystem journal would otherwise stop renewing and lose its
/// lease while it is otherwise healthy. The returned handle joins the task.
/// Aborting it stops the task at its next await; a request already in flight
/// finishes first.
fn spawn_on_own_thread<F>(task: F) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let stop = CancellationToken::new();
    let (done, finished) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("felix-membership".to_string())
        .spawn({
            let stop = stop.clone();
            move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build the membership runtime");
                let ended = runtime.block_on(async move {
                    let task = tokio::spawn(task);
                    let abort = task.abort_handle();
                    tokio::select! {
                        ended = task => ended,
                        _ = stop.cancelled() => {
                            abort.abort();
                            Ok(())
                        }
                    }
                });
                let _ = done.send(ended);
            }
        })
        .expect("start the membership thread");
    tokio::spawn(async move {
        // Dropped with this task, so aborting the handle stops the heartbeat.
        let _stop = stop.drop_guard();
        match finished.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            Ok(Err(_)) => {}
            Err(_) => panic!("the membership thread stopped without finishing"),
        }
    })
}

/// How long the broker's main runtime may go without running a task before
/// the heartbeat stops renewing the lease. Well past a filesystem journal
/// stall (seconds), so only a runtime that is stuck for good loses its shards.
const RUNTIME_HUNG_AFTER: Duration = Duration::from_secs(10);

/// Proof that the broker's main runtime still runs tasks, for the heartbeat on
/// its own thread. Without it a deadlocked broker would keep renewing a lease
/// it cannot serve under, and the shards it leads would never fail over.
struct RuntimePulse {
    started: std::time::Instant,
    /// Milliseconds after `started` the main runtime last ran the ticker.
    last: AtomicU64,
    hung_after: Duration,
}

impl RuntimePulse {
    /// Tick from the current runtime until `shutdown`.
    fn start(hung_after: Duration, shutdown: CancellationToken) -> Arc<Self> {
        let pulse = Arc::new(Self::new(hung_after));
        let ticking = Arc::clone(&pulse);
        tokio::spawn(async move {
            loop {
                ticking.tick();
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        });
        pulse
    }

    fn new(hung_after: Duration) -> Self {
        Self {
            started: std::time::Instant::now(),
            last: AtomicU64::new(0),
            hung_after,
        }
    }

    fn tick(&self) {
        let now = self.started.elapsed().as_millis() as u64;
        self.last.store(now, Ordering::Release);
    }

    /// How long the runtime has been silent, when that is past `hung_after`.
    fn hung(&self) -> Option<Duration> {
        let last = Duration::from_millis(self.last.load(Ordering::Acquire));
        let silent = self.started.elapsed().saturating_sub(last);
        (silent > self.hung_after).then_some(silent)
    }
}

/// Note in the data directory under `root` that the node registered at
/// `incarnation`, so a later start can tell this directory from an older
/// copy of it. A failure is logged and not retried: the next start then reads
/// the directory as older and rotates its id, which is safe.
async fn record_registration(root: &std::path::Path, incarnation: u64) {
    let root = root.to_path_buf();
    let written = felix_common::blocking::spawn(move || {
        felix_storage::run_state::record_registration(&root, incarnation)
    })
    .await;
    match written {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(
            incarnation,
            error = %err,
            "could not record the registration in the data directory; its next start will \
             give it a new storage id"
        ),
        Err(err) => {
            tracing::error!(incarnation, error = %err, "recording the registration panicked")
        }
    }
}

/// Why this broker must not serve in a fleet whose enabled features are
/// `enabled`, or `None` when it may.
///
/// A data directory under `root` that took a head trim, or stored a record
/// with headers, holds records an older broker cannot decode: an older leader
/// promoted over it would fail to read them and stall that shard, and an
/// older follower would be held at the first. So once a log here has done
/// either (`felix_storage::run_state::head_trimmed`, `has_record_headers`),
/// the fleet must have finalized `head_trim`, `record_headers`. A marker that
/// cannot be read counts as present.
pub(crate) async fn join_refusal(
    root: Option<&std::path::Path>,
    enabled: &BTreeSet<String>,
) -> Option<String> {
    let root = root?;
    type Marker = fn(&std::path::Path) -> felix_storage::Result<bool>;
    let markers: [(felix_common::fleet::FleetFeature, &str, Marker); 2] = [
        (
            felix_common::fleet::HEAD_TRIM,
            "has taken a head trim",
            felix_storage::run_state::head_trimmed,
        ),
        (
            felix_common::fleet::RECORD_HEADERS,
            "has stored a record with headers",
            felix_storage::run_state::has_record_headers,
        ),
    ];
    let shown = root.display().to_string();
    for (feature, did, marker) in markers {
        if enabled.contains(feature.name()) {
            continue;
        }
        let root = root.to_path_buf();
        let found =
            felix_common::blocking::spawn(move || marker(&root).map_err(|err| err.to_string()))
                .await
                .unwrap_or_else(|err| Err(err.to_string()));
        let name = feature.name();
        match found {
            Ok(false) => {}
            Ok(true) => {
                return Some(format!(
                    "a log in {shown} {did}, which brokers older than {name} cannot read; \
                     finalize {name} for the fleet before this broker joins it"
                ));
            }
            Err(err) => {
                return Some(format!(
                    "could not tell whether a log in {shown} {did} ({err}); \
                     finalize {name} for the fleet before this broker joins it"
                ));
            }
        }
    }
    None
}

/// Start the gate over from a registration's answer.
fn adopt_fleet(fleet: &FleetGate, answer: &BTreeSet<String>) {
    for feature in fleet.restart(answer.iter().map(String::as_str)) {
        // Only when the control plane lost its state: a finalize is never
        // undone.
        tracing::warn!(
            feature,
            "the fleet no longer has this feature enabled; disabled"
        );
        mm::record_fleet_feature(feature, false);
    }
    for feature in fleet.enabled() {
        mm::record_fleet_feature(feature, true);
    }
    for feature in fleet.reported() {
        if !fleet.supports_name(&feature) {
            mm::record_fleet_feature(&feature, false);
        }
    }
}

/// Leave the cluster on the way out.
///
/// Drain first so nothing new is placed here while in-flight work finishes,
/// then deregister so this reads as intentional rather than as a crash.
/// Neither failure is worth aborting a shutdown over: if the control plane
/// cannot be told, heartbeat expiry reaches the same conclusion a timeout later.
///
/// Returns whether the control plane may have deregistered the node: false
/// only when the request never reached it.
pub async fn shutdown_membership(link: &impl ControlPlaneLink, node_id: &str, token: &str) -> bool {
    if let Err(err) = drain(link, node_id, token).await {
        tracing::warn!(node_id, error = %err, "could not mark this broker draining");
    }
    match deregister(link, node_id, token).await {
        Ok(()) => {
            tracing::info!(node_id, "deregistered from the control plane");
            true
        }
        Err(err) => {
            tracing::warn!(
                node_id,
                error = %err,
                "could not deregister; the control plane will expire this node instead",
            );
            !never_sent(&err)
        }
    }
}

/// Whether `err` is a request that failed before the connection was made,
/// so the control plane cannot have acted on it. A timeout or an answer may
/// follow a request it applied.
fn never_sent(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<reqwest::Error>())
        .any(reqwest::Error::is_connect)
}

async fn send_heartbeat(
    link: &impl ControlPlaneLink,
    node_id: &str,
    token: &str,
    request: HeartbeatRequest,
    timeout: Duration,
) -> std::result::Result<HeartbeatResponse, MembershipError> {
    let response = link
        .heartbeat(token, node_id, &request, timeout)
        .await
        .context("send heartbeat")
        .map_err(MembershipError::Unavailable)?;

    let status = response.status;
    if !status.is_success() {
        let body = response.text();
        return Err(MembershipError::from_status(
            status,
            format!("heartbeat rejected ({status}): {body}"),
        ));
    }
    response
        .json()
        .context("decode heartbeat response")
        .map_err(MembershipError::Unavailable)
}

async fn post_lifecycle(
    link: &impl ControlPlaneLink,
    node_id: &str,
    token: &str,
    action: &str,
) -> Result<()> {
    let response = link
        .lifecycle(token, node_id, action)
        .await
        .with_context(|| format!("{action} node {node_id}"))?;

    let status = response.status;
    if !status.is_success() {
        return Err(anyhow!("{action} rejected ({status}): {}", response.text()));
    }
    Ok(())
}

/// Exponential backoff, capped.
///
/// `failures` is the count so far, so the first retry waits one interval rather
/// than doubling immediately.
fn backoff(interval: Duration, failures: u64) -> Duration {
    let shift = failures.saturating_sub(1).min(16) as u32;
    interval
        .saturating_mul(2u32.saturating_pow(shift))
        .min(MAX_RETRY_BACKOFF)
}

/// The longest a retry may wait under a lease of `usable`. Never zero, so a
/// degenerate lease cannot turn the retry loop into a spin.
fn retry_cap(usable: Duration) -> Duration {
    (usable / LEASE_RETRY_FRACTION)
        .min(MAX_RETRY_BACKOFF)
        .max(Duration::from_millis(10))
}

/// Spread a delay so a restarted fleet does not report in lockstep.
fn jittered(delay: Duration) -> Duration {
    let spread = delay.as_secs_f64() * JITTER_FRACTION;
    if spread <= 0.0 {
        return delay;
    }
    let fraction = felix_common::jitter::up_to(u64::from(u32::MAX)) as f64 / f64::from(u32::MAX);
    delay + Duration::from_secs_f64(spread * fraction)
}

#[cfg(test)]
mod tests;
