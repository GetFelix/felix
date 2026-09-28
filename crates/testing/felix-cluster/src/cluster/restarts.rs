//! Stopping a broker the way an orchestrator does, and starting it again.

use std::process::ExitStatus;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::{Cluster, READY_TIMEOUT};
use crate::node::spawn_broker;
use crate::wait;

impl Cluster {
    /// Send a broker SIGTERM and return at once, leaving it to shut down.
    ///
    /// What Kubernetes and systemd send, so this is the start of a rolling
    /// restart. Pair it with [`Self::wait_for_exit`].
    #[cfg(unix)]
    pub fn terminate_node(&self, node_id: &str) -> Result<()> {
        self.signal(node_id, libc::SIGTERM, "terminate")
    }

    /// How a broker exited, once it has; `None` while it is still running.
    ///
    /// For a test that has other things to drive while it waits.
    pub fn exit_status(&mut self, node_id: &str) -> Result<Option<ExitStatus>> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let status = node.exited();
        if status.is_some() {
            node.take_process();
        }
        Ok(status)
    }

    /// Wait for a broker to exit on its own, and say how it exited.
    ///
    /// Fails, and kills the broker, if it is still running after `timeout`: a
    /// shutdown that does not end is the thing being measured, so it is not
    /// scaled by the harness's slow-machine factor.
    pub async fn wait_for_exit(&mut self, node_id: &str, timeout: Duration) -> Result<ExitStatus> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = node.exited() {
                node.take_process();
                return Ok(status);
            }
            if Instant::now() >= deadline {
                if let Some(mut process) = node.take_process() {
                    let _ = process.kill();
                    let _ = process.wait();
                }
                bail!("{node_id} was still running {timeout:?} after it was told to stop");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Set one environment variable for `node_id`'s next start, over the
    /// cluster-wide ones: restarting it under a different
    /// `FELIX_TEST_FLEET_FEATURES` is how a test upgrades one broker.
    pub fn set_node_env(&mut self, node_id: &str, key: &str, value: &str) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        if self.config.node_env.len() <= index {
            self.config.node_env.resize(index + 1, Vec::new());
        }
        let env = &mut self.config.node_env[index];
        env.retain(|(existing, _)| existing != key);
        env.push((key.to_string(), value.to_string()));
        Ok(())
    }

    /// Start a stopped broker again under the same identity and data
    /// directory, and wait until the control plane can place on it.
    ///
    /// Its ports are new; a broker re-registers its addresses on every start.
    pub async fn restart_node(&mut self, node_id: &str) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        if self.nodes[index].is_running() {
            bail!("{node_id} is still running");
        }
        // Kept beside the new one: the old run is usually what a failure is about.
        let log = self.nodes[index].data_dir.join("broker.log");
        let _ = std::fs::rename(&log, log.with_extension("log.previous"));
        let control_plane = self
            .control_plane
            .as_ref()
            .ok_or_else(|| anyhow!("control plane is gone"))?;
        let node = spawn_broker(
            &self.binary,
            control_plane,
            &self.config,
            self._root.path(),
            index,
            self.links.as_ref(),
        )
        .with_context(|| format!("restart {node_id}"))?;
        self.nodes[index] = node;
        self.await_placeable(index).await
    }

    /// Wait for the broker at `index` to report ready and the control plane
    /// to count it placeable.
    pub(super) async fn await_placeable(&mut self, index: usize) -> Result<()> {
        let node_id = self.nodes[index].node_id.clone();
        let deadline = Instant::now() + wait::budget(READY_TIMEOUT);
        loop {
            let url = format!("http://{}/ready", self.nodes[index].metrics_addr);
            if let Some(status) = self.nodes[index].exited() {
                let reason = self.nodes[index].failure_reason();
                bail!("{node_id} exited before becoming ready ({status}){reason}");
            }
            let ok = matches!(
                self.http.get(&url).send().await,
                Ok(response) if response.status().is_success()
            );
            if ok {
                break;
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for {node_id} to be ready");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let this = &*self;
        wait::until(
            READY_TIMEOUT,
            &format!("{node_id} to be placeable"),
            || {
                let expected = node_id.clone();
                async move {
                    matches!(this.placeable_nodes().await, Ok(live) if live.contains(&expected))
                }
            },
        )
        .await
    }
}
