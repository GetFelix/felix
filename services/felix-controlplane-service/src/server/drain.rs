//! The shutdown half of [`super::run`].
//!
//! Order matters: readiness flips first (in `run`), the listener keeps serving
//! through the pre-drain hold-off, then every subsystem drains against one
//! shared deadline, and the metrics endpoint goes last so the drain stays
//! observable to the end.
use std::time::Duration;

use felix_common::lifecycle::DrainBudget;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::ControlPlaneConfig;
use crate::raft::RaftHandle;

/// Everything `run` started that a drain has to stop.
pub(super) struct Running {
    pub(super) config: ControlPlaneConfig,
    pub(super) api_shutdown: CancellationToken,
    pub(super) metrics_shutdown: CancellationToken,
    pub(super) api_task: JoinHandle<std::io::Result<()>>,
    pub(super) reconcile_task: JoinHandle<()>,
    pub(super) expiry_task: JoinHandle<()>,
    pub(super) bootstrap_task: Option<JoinHandle<()>>,
    pub(super) raft_metrics_task: Option<JoinHandle<()>>,
    pub(super) raft_handle: Option<RaftHandle>,
    pub(super) raft_peer_task: Option<JoinHandle<()>>,
    pub(super) peer_shutdown: CancellationToken,
    pub(super) metrics_task: JoinHandle<std::io::Result<()>>,
    /// Resolves on a termination signal after the first.
    pub(super) second_signal: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}

impl Running {
    /// Drain after readiness has already been flipped to draining.
    pub(super) async fn drain(self) -> anyhow::Result<()> {
        let Running {
            config,
            api_shutdown,
            metrics_shutdown,
            mut api_task,
            reconcile_task,
            expiry_task,
            bootstrap_task,
            raft_metrics_task,
            raft_handle,
            raft_peer_task,
            peer_shutdown,
            metrics_task,
            second_signal,
        } = self;

        // Step 1b: keep serving while load balancers notice. Without this the
        // listener closes in the same breath as the readiness flip, and anything
        // still routed here is refused at the socket.
        if config.shutdown_predrain_ms > 0 {
            let hold_off = Duration::from_millis(config.shutdown_predrain_ms);
            tracing::info!(
                hold_off_ms = config.shutdown_predrain_ms,
                "serving while unready so load balancers can drop this instance"
            );
            tokio::select! {
                _ = tokio::time::sleep(hold_off) => {}
                // An operator who signals twice is asking to skip the wait.
                _ = second_signal => {
                    tracing::info!("second termination signal; ending hold-off early");
                }
            }
        }

        // Step 2: stop admitting, then drain in-flight requests against one shared
        // deadline covering every subsystem.
        let mut budget = DrainBudget::new(Duration::from_millis(config.shutdown_drain_timeout_ms));
        tracing::info!(
            deadline_ms = config.shutdown_drain_timeout_ms,
            "draining in-flight requests"
        );
        api_shutdown.cancel();

        if !budget
            .drain("api_server", async {
                let _ = (&mut api_task).await;
            })
            .await
        {
            api_task.abort();
        }

        let mut reconcile_task = reconcile_task;
        if !budget
            .drain("shard_reconciler", async {
                let _ = (&mut reconcile_task).await;
            })
            .await
        {
            reconcile_task.abort();
        }

        let mut expiry_task = expiry_task;
        if !budget
            .drain("node_expiry_sweep", async {
                let _ = (&mut expiry_task).await;
            })
            .await
        {
            expiry_task.abort();
        }

        let mut bootstrap_task = bootstrap_task;
        if let Some(task) = &mut bootstrap_task
            && !budget
                .drain("bootstrap_server", async {
                    let _ = (&mut *task).await;
                })
                .await
        {
            task.abort();
        }

        if let Some(task) = raft_metrics_task {
            task.abort();
        }
        if let Some(handle) = &raft_handle {
            // After the API stops (no new proposals), before metrics: a member
            // that leaves without shutting down looks like a failure to the
            // group and costs it an election timeout. Known fact, pre-0.10
            // openraft: there is no leadership-transfer API, so when the member
            // being deployed *is* the leader, the group pauses writes for one
            // election timeout before a successor takes over. Bounded (~1s at
            // defaults), recorded in docs/metadata-raft-design.md, and closed by
            // the 0.10 upgrade the seam exists to contain.
            if !budget
                .drain("raft_node", async {
                    let _ = handle.shutdown().await;
                })
                .await
            {
                tracing::warn!("raft node did not shut down within the drain budget");
            }
        }
        peer_shutdown.cancel();
        if let Some(mut task) = raft_peer_task
            && !budget
                .drain("raft_peer_server", async {
                    let _ = (&mut task).await;
                })
                .await
        {
            task.abort();
        }

        // Step 3: metrics last, so `/ready` keeps reporting "draining" and `/metrics`
        // stays scrapeable for the whole drain.
        metrics_shutdown.cancel();
        let mut metrics_task = metrics_task;
        if !budget
            .drain("metrics_server", async {
                let _ = (&mut metrics_task).await;
            })
            .await
        {
            metrics_task.abort();
        }

        budget.report();
        tracing::info!("control plane stopped");
        Ok(())
    }
}
