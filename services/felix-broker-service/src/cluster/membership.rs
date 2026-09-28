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

#[derive(Debug, Deserialize)]
struct NodeView {
    status: NodeStatusView,
}

#[derive(Debug, Deserialize)]
struct NodeStatusView {
    incarnation: u64,
    lifecycle: String,
}

#[derive(Debug, Serialize)]
struct HeartbeatRequest {
    incarnation: u64,
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

/// Claim this broker's identity.
///
/// Fails loudly. A broker that cannot register is not a cluster member, and
/// carrying on as if it were means publishing to a node no one will route to.
pub async fn register(
    client: &reqwest::Client,
    base_url: &str,
    config: &MembershipConfig,
    credential: &crate::cluster::credential::NodeCredential,
) -> std::result::Result<Registration, MembershipError> {
    let response = client
        .post(format!("{}/v1/nodes", base_url.trim_end_matches('/')))
        // Proves this broker may claim `node_id`. The control plane authorises
        // the identity in the body against it, so a broker cannot register
        // under a name its credential does not cover.
        .bearer_auth(credential.bearer())
        .json(&RegistrationRequest {
            node_id: &config.node_id,
            advertise_addr: &config.advertise_addr,
            client_addr: config.client_advertise_addr.as_deref(),
            kafka_addr: config.kafka_advertise_addr.as_deref(),
            region: &config.region,
            zone: config.zone.as_deref(),
            features: &config.features,
        })
        .send()
        .await
        .with_context(|| format!("register node {} with {base_url}", config.node_id))
        .map_err(MembershipError::Unavailable)?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
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
        .await
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
pub async fn run_heartbeat(
    client: reqwest::Client,
    base_url: String,
    registration: Registration,
    shutdown: CancellationToken,
    consecutive_failures: Arc<AtomicU64>,
    lease: Arc<crate::cluster::lease::LeaseState>,
    fleet: Arc<FleetGate>,
) -> HeartbeatEnd {
    let base_url = base_url.trim_end_matches('/').to_string();
    let url = format!("{base_url}/v1/nodes/{}/heartbeat", registration.node_id);
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
        }

        // Before the request, not after the answer: see `LeaseState::renew_at`.
        let sent = lease.now();
        // Read per heartbeat, so a refresh between beats is picked up without
        // this loop knowing refresh exists.
        match send_heartbeat(
            &client,
            &url,
            &registration.token.bearer(),
            registration.incarnation,
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
                    lease.renew_at(sent);
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
pub async fn drain(
    client: &reqwest::Client,
    base_url: &str,
    node_id: &str,
    token: &str,
) -> Result<()> {
    post_lifecycle(client, base_url, node_id, token, "drain").await
}

/// Leave the cluster on purpose, so this is not mistaken for a crash.
pub async fn deregister(
    client: &reqwest::Client,
    base_url: &str,
    node_id: &str,
    token: &str,
) -> Result<()> {
    post_lifecycle(client, base_url, node_id, token, "deregister").await
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
    client: reqwest::Client,
    base_url: String,
    config: MembershipConfig,
    credential: crate::cluster::credential::NodeCredential,
    serving: CancellationToken,
    shutdown: CancellationToken,
    lease: Arc<crate::cluster::lease::LeaseState>,
    fleet: Arc<FleetGate>,
) -> MembershipTask {
    let fatal = CancellationToken::new();
    let consecutive_failures = Arc::new(AtomicU64::new(0));
    let handle = tokio::spawn({
        let fatal = fatal.clone();
        let consecutive_failures = Arc::clone(&consecutive_failures);
        let lease = Arc::clone(&lease);
        let fleet = Arc::clone(&fleet);
        async move {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = serving.cancelled() => {}
            }

            loop {
                let mut attempt = 0u64;
                let registration = loop {
                    match register(&client, &base_url, &config, &credential).await {
                        Ok(registration) => break registration,
                        Err(MembershipError::Rejected(message)) => {
                            mm::record_registration(mm::KIND_REJECTED);
                            mm::record_membership_live(false);
                            tracing::error!(
                                node_id = %config.node_id,
                                error = %message,
                                "the control plane refused this identity; the broker is not a cluster member",
                            );
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
                consecutive_failures.store(0, Ordering::Release);
                adopt_fleet(&fleet, &registration.fleet_features);

                match run_heartbeat(
                    client.clone(),
                    base_url.clone(),
                    registration,
                    shutdown.clone(),
                    Arc::clone(&consecutive_failures),
                    Arc::clone(&lease),
                    Arc::clone(&fleet),
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
    });

    MembershipTask {
        handle,
        fatal,
        consecutive_failures,
        lease,
        fleet,
    }
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
pub async fn shutdown_membership(
    client: &reqwest::Client,
    base_url: &str,
    node_id: &str,
    token: &str,
) {
    if let Err(err) = drain(client, base_url, node_id, token).await {
        tracing::warn!(node_id, error = %err, "could not mark this broker draining");
    }
    if let Err(err) = deregister(client, base_url, node_id, token).await {
        tracing::warn!(
            node_id,
            error = %err,
            "could not deregister; the control plane will expire this node instead",
        );
    } else {
        tracing::info!(node_id, "deregistered from the control plane");
    }
}

async fn send_heartbeat(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    incarnation: u64,
    timeout: Duration,
) -> std::result::Result<HeartbeatResponse, MembershipError> {
    let response = client
        .post(url)
        .timeout(timeout)
        .bearer_auth(token)
        .json(&HeartbeatRequest { incarnation })
        .send()
        .await
        .context("send heartbeat")
        .map_err(MembershipError::Unavailable)?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(MembershipError::from_status(
            status,
            format!("heartbeat rejected ({status}): {body}"),
        ));
    }
    response
        .json()
        .await
        .context("decode heartbeat response")
        .map_err(MembershipError::Unavailable)
}

async fn post_lifecycle(
    client: &reqwest::Client,
    base_url: &str,
    node_id: &str,
    token: &str,
    action: &str,
) -> Result<()> {
    let response = client
        .post(format!(
            "{}/v1/nodes/{node_id}/{action}",
            base_url.trim_end_matches('/')
        ))
        .bearer_auth(token)
        .send()
        .await
        .with_context(|| format!("{action} node {node_id}"))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("{action} rejected ({status}): {body}"));
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
    // Cheap and adequate: this only needs to decorrelate brokers, not resist
    // prediction, so it avoids pulling in an RNG on the shutdown path.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let fraction = f64::from(nanos) / f64::from(u32::MAX);
    delay + Duration::from_secs_f64(spread * fraction)
}

#[cfg(test)]
mod tests;
