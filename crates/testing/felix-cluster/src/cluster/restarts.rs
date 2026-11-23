//! Stopping a broker the way an orchestrator does, and starting it again.

use std::process::ExitStatus;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::kept_logs::set_aside_log;
use super::startup::MAX_SPAWN_ATTEMPTS;
use super::{Cluster, READY_TIMEOUT};
use crate::wait;

/// How long a broker pinned to its old ports waits before trying them again.
const PINNED_PORT_BACKOFF: Duration = Duration::from_millis(500);

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
    /// Also waits for that new registration: the old one's fresh heartbeat
    /// already satisfies "placeable".
    pub async fn restart_node(&mut self, node_id: &str) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        if self.nodes[index].is_running() {
            bail!("{node_id} is still running");
        }
        // Unreadable (no record yet, control plane down) means there is nothing to outwait.
        let prior_incarnation = match &self.control_plane {
            Some(control_plane) => control_plane
                .registered_node(node_id)
                .await
                .ok()
                .map(|node| node.status.incarnation),
            None => None,
        };
        // Kept beside the new one: the old run is usually what a failure is about.
        set_aside_log(&self.nodes[index].data_dir);
        self.respawn(index)
            .with_context(|| format!("restart {node_id}"))?;
        self.await_placeable_retrying(index).await?;
        let Some(prior) = prior_incarnation else {
            return Ok(());
        };
        let this = &*self;
        wait::until(
            READY_TIMEOUT,
            &format!("{node_id} to register a new incarnation"),
            || async move {
                matches!(
                    this.registered_node(node_id).await,
                    Ok(node) if node.status.incarnation > prior
                )
            },
        )
        .await
    }

    /// Start a stopped broker again and return at once, without waiting for it
    /// to be ready. Works while the control plane is crashed, which is the
    /// point: a broker started then waits for the control plane before it
    /// opens its storage. Pair it with [`Self::await_node_ready`].
    pub fn start_node_unready(&mut self, node_id: &str) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        if self.nodes[index].is_running() {
            bail!("{node_id} is still running");
        }
        set_aside_log(&self.nodes[index].data_dir);
        self.respawn(index)
    }

    /// [`Self::start_node_unready`] on the ports it had, as a broker with a
    /// fixed address restarts. Peers whose view of the cluster the control
    /// plane has not refreshed reach it at its old address only.
    pub fn start_node_unready_in_place(&mut self, node_id: &str) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        if self.nodes[index].is_running() {
            bail!("{node_id} is still running");
        }
        let ports = (
            self.nodes[index].client_addr,
            self.nodes[index].internal_addr,
        );
        set_aside_log(&self.nodes[index].data_dir);
        self.respawn_on(index, Some(ports))
    }

    /// Wait for a broker started with [`Self::start_node_unready`] to report
    /// ready and the control plane to count it placeable.
    pub async fn await_node_ready(&mut self, node_id: &str) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        self.await_placeable_retrying(index).await
    }

    /// The control plane's address, whether it is running or crashed.
    pub(super) fn control_plane_base_url(&self) -> Result<String> {
        match (&self.control_plane, &self.crashed_control_plane) {
            (Some(control_plane), _) => Ok(control_plane.base_url.clone()),
            (None, Some(stopped)) => Ok(stopped.base_url()),
            (None, None) => bail!("control plane is gone"),
        }
    }

    /// [`Self::await_placeable`], starting the broker again if it exited
    /// because it lost a port race.
    ///
    /// Ports are probed and released before the broker binds them, and
    /// another process (a peer's outbound socket, a parallel test) can take one
    /// in between. A broker on pinned ports waits first, since the holder is
    /// usually transient. Any other exit, such as a broker that cannot recover
    /// its storage, is returned: a real bind problem must still fail the test.
    pub(super) async fn await_placeable_retrying(&mut self, index: usize) -> Result<()> {
        let mut attempts = 1;
        loop {
            match self.await_placeable(index).await {
                Err(_)
                    if attempts < MAX_SPAWN_ATTEMPTS
                        && self.nodes[index].exited().is_some()
                        && self.nodes[index].lost_port_race() =>
                {
                    attempts += 1;
                    let node_id = self.nodes[index].node_id.clone();
                    tracing::warn!(%node_id, "broker lost a port; starting it again");
                    let pinned = self.nodes[index].pinned_ports();
                    if pinned.is_some() {
                        tokio::time::sleep(PINNED_PORT_BACKOFF).await;
                    }
                    self.respawn_on(index, pinned)?;
                }
                result => return result,
            }
        }
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
