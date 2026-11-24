//! Faults a client suite can ask the fixture for.
//!
//! A suite in another language has no handle on the cluster, so the scenarios
//! about what a client sees during a move or a lost quorum could not be written
//! at all. This serves a few POSTs on localhost that put the cluster into those
//! states and hold it there:
//!
//! - `/fence` stops the movable stream's shard halfway through a move, with
//!   its owner fenced, and answers once that owner refuses publishes. On Raft
//!   no move happens: every broker is cut off from the others, and it answers
//!   once the owner refuses proposals as unwritten.
//! - `/partition` cuts the quorum stream's leader off from its followers and
//!   answers at once. The first publish through it is proposed, and times out
//!   without a majority.
//! - `/link` breaks the connections through the link interposer, with a body
//!   of `{"fault": "drop" | "reset" | "stall", "hold_ms": N}`. It answers at
//!   once; a drop or a stall heals itself after `hold_ms`.
//! - `/heal` undoes all of them. After a Raft `/fence` it also waits for the
//!   movable stream to take a write, so no probe left with an unknown outcome
//!   lands in it afterwards.
//! - `POST /tokens` mints a token for the fixture tenant with the same grants
//!   as `token`, with a body of `{"ttl_secs": N, "forged": false}`, and
//!   answers `{"token", "ttl_secs"}`. A suite uses a short life to make a
//!   client refresh mid-run. A `forged` token is signed with a key no broker
//!   trusts, so it looks live to the client and every broker refuses it. The
//!   brokers allow 60 s of clock skew, so "expires" is the client's view of
//!   the token's `exp`, not a refusal at the broker.
//! - `GET /no-commit/requests` answers how many commits, state reads, head trims and
//!   named-shard publishes the broker at `no_commit_addrs` has been sent (a
//!   plain integer).
//!
//! `/fence`, `/partition` and `/heal` answer `{"node_id", "addr"}` for the
//! broker to publish through directly. Going through another broker would add a forward, and what the
//! forward says about a refusal is a different question.
//!
//! The faults are not scoped to one stream: fencing drains the owner, and a
//! partition isolates the leader for everything it serves. A suite runs these
//! against a fixture of its own.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use felix_cluster::Cluster;
use felix_conformance::link::{Interposer, LinkFault};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// Long enough for an assignment to reach a broker, or for a quorum wait to
/// run out, several times over.
const TAKES_EFFECT_WITHIN: Duration = Duration::from_secs(60);

pub(crate) struct Control {
    cluster: Arc<Cluster>,
    /// Set while a fault is held. The fixture's placement timer skips its pass,
    /// because the next pass would finish the move this is holding open.
    hold: Arc<AtomicBool>,
    /// Held by the placement timer for each pass, and here while `hold` is
    /// set, so setting it waits out a pass already in flight.
    placement: Arc<Mutex<()>>,
    link: Arc<Interposer>,
    no_commit_requests: Arc<AtomicUsize>,
    movable_stream: String,
    quorum_stream: String,
    drained: Mutex<Option<String>>,
    /// Set once `/fence` on Raft has sent probes whose outcome may have been
    /// unknown, for `/heal` to settle.
    probed: AtomicBool,
}

#[derive(Deserialize)]
struct LinkRequest {
    fault: LinkFault,
    #[serde(default)]
    hold_ms: u64,
}

#[derive(Deserialize)]
struct TokenRequest {
    ttl_secs: u64,
    #[serde(default)]
    forged: bool,
}

#[derive(Serialize)]
struct Minted {
    token: String,
    ttl_secs: u64,
}

/// A token a suite asks for lives at least a second and at most the harness's
/// own tokens' hour.
const MAX_TOKEN_TTL_SECS: u64 = 3600;

#[derive(Serialize)]
struct Target {
    node_id: String,
    addr: String,
}

type Answer = Result<Json<Target>, (StatusCode, String)>;

impl Control {
    pub(crate) fn new(
        cluster: Arc<Cluster>,
        hold: Arc<AtomicBool>,
        placement: Arc<Mutex<()>>,
        link: Arc<Interposer>,
        no_commit_requests: Arc<AtomicUsize>,
        movable_stream: &str,
        quorum_stream: &str,
    ) -> Self {
        Self {
            cluster,
            hold,
            placement,
            link,
            no_commit_requests,
            movable_stream: movable_stream.to_string(),
            quorum_stream: quorum_stream.to_string(),
            drained: Mutex::new(None),
            probed: AtomicBool::new(false),
        }
    }

    /// Serve on an ephemeral localhost port, returning its base URL and the
    /// task serving it. The task holds the cluster, so it has to end before
    /// the cluster can be torn down.
    pub(crate) async fn serve(self) -> Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let router = Router::new()
            .route("/fence", post(fence))
            .route("/partition", post(partition))
            .route("/link", post(link))
            .route("/heal", post(heal))
            .route("/tokens", post(tokens))
            .route("/no-commit/requests", get(no_commit_requests))
            .with_state(Arc::new(self));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok((url, task))
    }

    fn target(&self, node_id: &str) -> Target {
        let addr = self
            .cluster
            .node(node_id)
            .map(|node| node.client_addr.to_string())
            .unwrap_or_default();
        Target {
            node_id: node_id.to_string(),
            addr,
        }
    }
}

async fn tokens(
    State(control): State<Arc<Control>>,
    Json(request): Json<TokenRequest>,
) -> Result<Json<Minted>, (StatusCode, String)> {
    if !(1..=MAX_TOKEN_TTL_SECS).contains(&request.ttl_secs) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("ttl_secs must be 1 to {MAX_TOKEN_TTL_SECS}"),
        ));
    }
    let ttl = Duration::from_secs(request.ttl_secs);
    let credentials = control.cluster.credentials();
    let token = if request.forged {
        credentials.forged_client_token(ttl).map_err(failed)?
    } else {
        credentials.client_token_for(ttl)
    };
    Ok(Json(Minted {
        token,
        ttl_secs: request.ttl_secs,
    }))
}

async fn no_commit_requests(State(control): State<Arc<Control>>) -> String {
    control
        .no_commit_requests
        .load(Ordering::SeqCst)
        .to_string()
}

async fn fence(State(control): State<Arc<Control>>) -> Answer {
    if control.cluster.nodes.len() < 2 {
        return Err(refused("a move needs a second broker to move to"));
    }
    {
        let _pass = control.placement.lock().await;
        control.hold.store(true, Ordering::SeqCst);
    }
    if felix_cluster::suite_runs_on_raft() {
        return fence_raft(&control).await;
    }
    let owner = control
        .cluster
        .fence_shard(&control.movable_stream)
        .await
        .map_err(failed)?;
    *control.drained.lock().await = Some(owner.clone());
    // The owner learns of the fence from its assignment feed, a moment after
    // the control plane wrote it.
    until_refused(&control.cluster, &owner, &control.movable_stream)
        .await
        .map_err(failed)?;
    Ok(Json(control.target(&owner)))
}

/// A Raft shard has no fenced state to hold open: its leader moves by an
/// election. What a client sees while nobody serves it is a group with no
/// majority anywhere, so every broker is cut off from the others. The old
/// owner stops taking proposals once one has waited past the quorum timeout,
/// and nothing it refuses then is written. Cutting off the owner alone would
/// let the other two elect a leader within seconds and end the fence.
async fn fence_raft(control: &Control) -> Answer {
    let owner = control
        .cluster
        .owner(&control.movable_stream)
        .await
        .map_err(failed)?;
    for node in &control.cluster.nodes {
        tokio::task::block_in_place(|| control.cluster.partition_node(&node.node_id))
            .map_err(failed)?;
    }
    control.probed.store(true, Ordering::SeqCst);
    // The first probe may be proposed before the owner notices it has no
    // majority, and then times out with its outcome unknown. Only the
    // broker's own retryable answer says nothing was written: two of those
    // in a row mean it stopped taking writes.
    let deadline = Instant::now() + TAKES_EFFECT_WITHIN;
    let mut refused_in_a_row = 0;
    loop {
        let probe = control
            .cluster
            .publish_via(&owner, &control.movable_stream, b"fixture-probe".to_vec())
            .await;
        refused_in_a_row = if probe.as_ref().is_err_and(refused_unwritten) {
            refused_in_a_row + 1
        } else {
            0
        };
        if refused_in_a_row == 2 {
            return Ok(Json(control.target(&owner)));
        }
        if Instant::now() >= deadline {
            return Err(failed(anyhow::anyhow!(
                "{owner} kept taking publishes to {}",
                control.movable_stream
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn partition(State(control): State<Arc<Control>>) -> Answer {
    if control.cluster.nodes.len() < 2 {
        return Err(refused("a lost quorum needs followers to lose"));
    }
    // A move left running here would be frozen by the hold with its leader
    // fenced, and publishes would fail as "moving" instead of unknown.
    {
        let _pass = control.placement.lock().await;
        // An earlier /fence may be holding a move open on purpose.
        if !control.hold.load(Ordering::SeqCst) {
            settle_moves(&control.cluster).await.map_err(failed)?;
        }
        control.hold.store(true, Ordering::SeqCst);
    }
    let leader = control
        .cluster
        .owner(&control.quorum_stream)
        .await
        .map_err(failed)?;
    // No probe publish here: the cut is in force once this returns, and a
    // probe batch the leader accepted and never settled would make it refuse
    // the caller's own publish before the log, as retryable, on Raft.
    tokio::task::block_in_place(|| control.cluster.partition_node(&leader)).map_err(failed)?;
    Ok(Json(control.target(&leader)))
}

async fn link(
    State(control): State<Arc<Control>>,
    Json(request): Json<LinkRequest>,
) -> Json<serde_json::Value> {
    control
        .link
        .inject(request.fault, Duration::from_millis(request.hold_ms));
    Json(serde_json::json!({ "fault": request.fault, "hold_ms": request.hold_ms }))
}

async fn heal(State(control): State<Arc<Control>>) -> Answer {
    control.link.heal();
    tokio::task::block_in_place(|| control.cluster.heal_partitions()).map_err(failed)?;
    let drained = control.drained.lock().await.take();
    if let Some(node_id) = &drained {
        control
            .cluster
            .undrain_node(node_id)
            .await
            .map_err(failed)?;
    }
    if control.probed.swap(false, Ordering::SeqCst) {
        settle_probes(&control.cluster, &control.movable_stream)
            .await
            .map_err(failed)?;
    }
    // Undraining invites a rebalance back onto the broker. Finish it now so the
    // timer does not start one a later request would freeze.
    {
        let _pass = control.placement.lock().await;
        let settled = settle_moves(&control.cluster).await;
        // Release the hold even on failure, or the fixture stays wedged.
        control.hold.store(false, Ordering::SeqCst);
        settled.map_err(failed)?;
    }
    Ok(Json(control.target(drained.as_deref().unwrap_or_default())))
}

/// Wait until the group accepts a write again. A probe `/fence` sent on Raft
/// may have been proposed with its outcome unknown, and commits or is
/// dropped once the group has a majority. The group decides every entry
/// ahead of a new one first, so this one's ack means no stray probe is left
/// to land in a later scenario's stream.
async fn settle_probes(cluster: &Cluster, stream: &str) -> Result<()> {
    let deadline = Instant::now() + TAKES_EFFECT_WITHIN;
    loop {
        for node in &cluster.nodes {
            if cluster
                .publish_via(&node.node_id, stream, b"fixture-settle".to_vec())
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            bail!("{stream} took no write after the partition healed");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Step placement until no shard is mid-move. The caller holds the placement
/// lock, so the timer cannot start a move between the last pass and the hold.
async fn settle_moves(cluster: &Cluster) -> Result<()> {
    let deadline = Instant::now() + TAKES_EFFECT_WITHIN;
    loop {
        cluster.place_shards().await;
        let moving = cluster.moving_shards().await?;
        if moving.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "shard moves did not settle, still moving: {}",
                moving.join(", ")
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn until_refused(cluster: &Cluster, node_id: &str, stream: &str) -> Result<()> {
    let deadline = Instant::now() + TAKES_EFFECT_WITHIN;
    loop {
        if cluster
            .publish_via(node_id, stream, b"fixture-probe".to_vec())
            .await
            .is_err()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("{node_id} kept accepting publishes to {stream}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Whether a failed publish definitely wrote nothing, by the broker's own
/// retry class. A timeout or an outcome-unknown answer may still commit.
fn refused_unwritten(err: &anyhow::Error) -> bool {
    use felix_client::{BrokerError, RetryClass};
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<BrokerError>())
        .any(|broker| {
            matches!(
                broker.retry,
                RetryClass::Retry | RetryClass::RetryAfter | RetryClass::Redirect
            )
        })
}

fn refused(why: &str) -> (StatusCode, String) {
    (StatusCode::CONFLICT, why.to_string())
}

fn failed(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
}
