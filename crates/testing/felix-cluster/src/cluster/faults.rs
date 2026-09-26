//! Faults a test can inject: stopping the control plane, killing, pausing
//! and partitioning brokers.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};

use super::{Cluster, READY_TIMEOUT};
use crate::node::partition_file;
use crate::wait;

impl Cluster {
    /// Stop the control plane, leaving the brokers running.
    ///
    /// A failure primitive rather than a teardown: brokers keep serving on the
    /// authority they already hold, and lose it when their leases lapse. That is
    /// the partition this cluster can produce without touching the network.
    pub async fn stop_control_plane(&mut self) {
        if let Some(control_plane) = self.control_plane.take() {
            control_plane.shutdown().await;
        }
    }

    /// Take the control plane down for `downtime`, then bring it back on the
    /// same address with the same metadata, the way a redeploy or a crash and
    /// restart would.
    pub async fn restart_control_plane(&mut self, downtime: Duration) -> Result<()> {
        let control_plane = self
            .control_plane
            .take()
            .ok_or_else(|| anyhow!("the control plane is not running"))?;
        self.control_plane = Some(control_plane.restart(downtime).await?);
        Ok(())
    }

    /// Whether the control plane is still running. `false` once
    /// [`Self::stop_control_plane`] has been called, after which the assignment
    /// and node endpoints are unreachable.
    pub fn control_plane_running(&self) -> bool {
        self.control_plane.is_some()
    }

    /// Stop one broker, and wait until the control plane agrees it is gone.
    ///
    /// The wait is the useful half: a test that kills a broker and immediately
    /// asserts is racing the expiry sweep.
    pub async fn stop_node(&mut self, node_id: &str) -> Result<()> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let Some(mut process) = node.take_process() else {
            return Ok(());
        };
        let _ = process.kill();
        let _ = process.wait();

        let node_id = node_id.to_string();
        wait::until(
            READY_TIMEOUT,
            &format!("control plane to notice {node_id} is gone"),
            || {
                let this = &*self;
                let node_id = node_id.clone();
                async move {
                    match this.placeable_nodes().await {
                        Ok(live) => !live.contains(&node_id),
                        Err(_) => false,
                    }
                }
            },
        )
        .await
    }

    /// Kill a broker and return immediately.
    ///
    /// Unlike [`Cluster::stop_node`], this does not wait for the control plane
    /// to notice. A test measuring how long failover takes has to start its
    /// clock at the kill, not after the cluster has already reacted to it.
    pub fn kill_node(&mut self, node_id: &str) -> Result<()> {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let Some(mut process) = node.take_process() else {
            return Ok(());
        };
        let _ = process.kill();
        // Reaped so the process does not linger as a zombie for the rest of the
        // test; the kill itself has already happened, so this does not wait on
        // anything the caller is timing.
        let _ = process.wait();
        Ok(())
    }

    /// Cut `node_id` off from every other broker, both ways.
    ///
    /// Distinct from pausing: the broker keeps running and keeps heartbeating,
    /// so the control plane still believes it is healthy. That is the state no
    /// other fault produces, and the one a replication design is most likely to
    /// get wrong.
    ///
    /// Written on both sides, because a partition is symmetric and a broker
    /// still reachable inbound would not be isolated.
    pub fn partition_node(&self, node_id: &str) -> Result<()> {
        let others: Vec<String> = self
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .filter(|id| id != node_id)
            .collect();
        for node in &self.nodes {
            let listed = if node.node_id == node_id {
                others.clone()
            } else {
                vec![node_id.to_string()]
            };
            std::fs::write(partition_file(&node.data_dir), listed.join("\n"))
                .with_context(|| format!("write the partition file for {}", node.node_id))?;
        }
        await_partition_reread();
        Ok(())
    }

    /// Reconnect everything.
    pub fn heal_partitions(&self) -> Result<()> {
        for node in &self.nodes {
            let path = partition_file(&node.data_dir);
            if path.exists() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("heal the partition for {}", node.node_id))?;
            }
        }
        await_partition_reread();
        Ok(())
    }

    /// Suspend a broker without stopping it.
    ///
    /// The process stays alive and keeps every lease and connection it holds,
    /// and answers nothing. That is the fault a kill cannot produce, and it is
    /// the one the commit-boundary lease check exists for: a broker suspended
    /// past its lease expiry must refuse the write it was in the middle of when
    /// it wakes, rather than committing to a shard someone else now leads.
    ///
    /// Unix only. Elsewhere there is no equivalent that leaves the process
    /// holding its state, and a test that quietly did something weaker would be
    /// worse than one that does not run.
    #[cfg(unix)]
    pub fn pause_node(&self, node_id: &str) -> Result<()> {
        self.signal(node_id, libc::SIGSTOP, "pause")?;
        // `kill` returns when the signal is queued, not when the process has
        // stopped. A test that probes straight afterwards can still be answered
        // by a broker that has not been descheduled yet -- which reads as "the
        // fault did not happen" and is this harness's fault, not the broker's.
        // Waiting for the kernel to say it is stopped is what makes `pause`
        // mean paused by the time it returns.
        self.await_stopped(node_id)
    }

    /// Whether the kernel currently reports this broker as stopped.
    ///
    /// Exposed so a test can check the fault is in effect rather than infer it
    /// from the broker failing to answer, which is the thing under test.
    #[cfg(unix)]
    pub fn is_paused(&self, node_id: &str) -> bool {
        self.node(node_id)
            .and_then(|node| node.pid())
            .is_some_and(process_is_stopped)
    }

    /// Let a suspended broker run again.
    #[cfg(unix)]
    pub fn resume_node(&self, node_id: &str) -> Result<()> {
        self.signal(node_id, libc::SIGCONT, "resume")
    }

    /// Wait until the kernel reports the broker as stopped.
    ///
    /// Polled rather than waited on: `waitpid` with `WUNTRACED` would reap the
    /// stop notification that `Child` relies on, and this harness needs the
    /// process handle to stay usable for the resume.
    #[cfg(unix)]
    fn await_stopped(&self, node_id: &str) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let pid = node
            .pid()
            .ok_or_else(|| anyhow!("cannot pause {node_id}: it is not running"))?;

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if process_is_stopped(pid) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(anyhow!(
            "{node_id} did not stop within 5s of being sent SIGSTOP"
        ))
    }

    #[cfg(unix)]
    pub(super) fn signal(&self, node_id: &str, signal: libc::c_int, what: &str) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let pid = node
            .pid()
            .ok_or_else(|| anyhow!("cannot {what} {node_id}: it is not running"))?
            as libc::pid_t;
        // Safety: `pid` came from a child this harness spawned and has not
        // reaped, so it names that child or nothing. `kill` reports an error
        // rather than misbehaving if the process is already gone.
        let sent = unsafe { libc::kill(pid, signal) };
        if sent != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("{what} {node_id} (pid {pid})"));
        }
        Ok(())
    }
}

/// Whether the kernel reports `pid` as stopped.
///
/// Read through `ps` rather than `/proc`, which does not exist on macOS, and
/// the harness runs on developer machines as well as on Linux CI. The state
/// letter is `T` for a job-control stop on both; anything after it (`T+`, and
/// the extra flag letters macOS appends) is not part of the state.
#[cfg(unix)]
fn process_is_stopped(pid: u32) -> bool {
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "state=", "-p", &pid.to_string()])
        .output()
    else {
        return false;
    };
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .starts_with('T')
}

/// Wait until every broker has re-read its partition file.
///
/// A broker caches its reading briefly rather than stat-ing a file on every
/// forwarded publish, so writing the file does not sever anything until that
/// cache expires. Returning before then would hand a caller a fault that is not
/// yet in effect, and the test would go on to prove nothing. `pause_node` waits
/// for the stop for the same reason.
///
/// Generous against the broker's window rather than equal to it: this runs once
/// per fault, and a test that races the injector fails for a reason that has
/// nothing to do with what it is testing.
fn await_partition_reread() {
    std::thread::sleep(std::time::Duration::from_millis(400));
}
