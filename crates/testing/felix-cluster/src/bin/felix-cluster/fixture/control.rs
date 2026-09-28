//! Faults a client suite can ask the fixture for.
//!
//! A suite in another language has no handle on the cluster, so the scenarios
//! about what a client sees during a move or a lost quorum could not be written
//! at all. This serves a few POSTs on localhost that put the cluster into those
//! states and hold it there:
//!
//! - `/fence` stops the movable stream's shard halfway through a move, with
//!   its owner fenced, and answers once that owner refuses publishes.
//! - `/partition` cuts the quorum stream's leader off from its followers, and
//!   answers once a publish through it fails.
//! - `/link` breaks the connections through the link interposer, with a body
//!   of `{"fault": "drop" | "reset" | "stall", "hold_ms": N}`. It answers at
//!   once; a drop or a stall heals itself after `hold_ms`.
//! - `/heal` undoes all of them.
//!
//! `/fence`, `/partition` and `/heal` answer `{"node_id", "addr"}` for the
//! broker to publish through directly. Going through another broker would add a forward, and what the
//! forward says about a refusal is a different question.
//!
//! The faults are not scoped to one stream: fencing drains the owner, and a
//! partition isolates the leader for everything it serves. A suite runs these
//! against a fixture of its own.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
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
    link: Arc<Interposer>,
    movable_stream: String,
    quorum_stream: String,
    drained: Mutex<Option<String>>,
}

#[derive(Deserialize)]
struct LinkRequest {
    fault: LinkFault,
    #[serde(default)]
    hold_ms: u64,
}

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
        link: Arc<Interposer>,
        movable_stream: &str,
        quorum_stream: &str,
    ) -> Self {
        Self {
            cluster,
            hold,
            link,
            movable_stream: movable_stream.to_string(),
            quorum_stream: quorum_stream.to_string(),
            drained: Mutex::new(None),
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

async fn fence(State(control): State<Arc<Control>>) -> Answer {
    if control.cluster.nodes.len() < 2 {
        return Err(refused("a move needs a second broker to move to"));
    }
    control.hold.store(true, Ordering::SeqCst);
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

async fn partition(State(control): State<Arc<Control>>) -> Answer {
    if control.cluster.nodes.len() < 2 {
        return Err(refused("a lost quorum needs followers to lose"));
    }
    control.hold.store(true, Ordering::SeqCst);
    let leader = control
        .cluster
        .owner(&control.quorum_stream)
        .await
        .map_err(failed)?;
    tokio::task::block_in_place(|| control.cluster.partition_node(&leader)).map_err(failed)?;
    until_refused(&control.cluster, &leader, &control.quorum_stream)
        .await
        .map_err(failed)?;
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
    control.hold.store(false, Ordering::SeqCst);
    Ok(Json(control.target(drained.as_deref().unwrap_or_default())))
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

fn refused(why: &str) -> (StatusCode, String) {
    (StatusCode::CONFLICT, why.to_string())
}

fn failed(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
}
