//! Faults a test can inject: the composable [`Fault`] values, and the
//! primitives under them (stopping the control plane, killing, pausing and
//! partitioning brokers).
//!
//! Every fault a broker has to cooperate with is a file the broker was
//! pointed at when it started (partition, clock, storage), so injecting is a
//! write and healing a delete. Link faults need no cooperation at all: they
//! are rules in the harness's own proxies.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::{Cluster, READY_TIMEOUT};
use crate::fault::{ClockFault, Endpoint, Fault, FsyncFault, WriteFault};
use crate::node::{clock_fault_file, partition_file, storage_fault_file};
use crate::proxy::Links;
use crate::wait;

/// How long after a clock file is written before every reader has seen it.
/// The readers re-read at most every 50ms, on their next clock reading, and
/// a broker's lease refresh reads the clock about that often.
const CLOCK_SETTLE: Duration = Duration::from_millis(250);

/// The same for the storage fault file, which a broker re-reads on its next
/// flush or segment write once 50ms have passed.
const STORAGE_SETTLE: Duration = Duration::from_millis(150);

/// How long a broker may go on using its last reading of the partition file.
/// See [`await_partition_reread`].
const PARTITION_REREAD: Duration = Duration::from_millis(400);

impl Cluster {
    /// Inject `fault`, returning once it is in effect.
    ///
    /// Faults compose: inject as many as the scenario needs, of any family,
    /// and undo each with [`Self::heal`] or all of them with
    /// [`Self::heal_all`]. A fault naming a node the cluster does not have is
    /// an error, never a silent no-op.
    pub async fn inject(&self, fault: &Fault) -> Result<()> {
        match fault {
            Fault::Drop { from, to } => {
                self.links_between(from, to)?
                    .rules()
                    .set_dropped(from, to, true);
            }
            Fault::Delay { from, to, by } => {
                self.links_between(from, to)?
                    .rules()
                    .set_delay(from, to, *by);
            }
            Fault::Refuse { node, peers } => {
                self.require_node(node)?;
                for peer in peers {
                    self.require_node(peer)?;
                }
                {
                    let mut injected = self.injected();
                    injected
                        .refused
                        .entry(node.clone())
                        .or_default()
                        .extend(peers.iter().cloned());
                    self.write_refusals(&injected.refused)?;
                }
                tokio::time::sleep(PARTITION_REREAD).await;
            }
            Fault::Suspend { node } => self.suspend(node)?,
            Fault::Clock { process, fault } => {
                check_clock_fault(process, fault)?;
                let path = self.clock_file(process)?;
                let content = {
                    let mut injected = self.injected();
                    let clock = injected.clocks.entry(process.clone()).or_default();
                    match fault {
                        ClockFault::StepMillis(by) => {
                            clock.offset_ms = clock.offset_ms.saturating_add(*by)
                        }
                        ClockFault::Rate(rate) => clock.rate = *rate,
                    }
                    clock.file_body()
                };
                std::fs::write(&path, content)
                    .with_context(|| format!("write the clock fault for {process:?}"))?;
                if *process == Endpoint::ControlPlane {
                    self.follow_clock(&path)?;
                }
                tokio::time::sleep(CLOCK_SETTLE).await;
            }
            Fault::Fsync { node, fault } => {
                self.require_node(node)?;
                {
                    let mut injected = self.injected();
                    injected.disk_generation += 1;
                    let generation = injected.disk_generation;
                    let disk = injected.disks.entry(node.clone()).or_default();
                    match fault {
                        FsyncFault::Delay(delay) => disk.delay = *delay,
                        FsyncFault::Fail | FsyncFault::FailOnce => {
                            disk.failure = Some(*fault);
                            disk.generation = generation;
                        }
                    }
                    self.write_disk(node, disk)?;
                }
                tokio::time::sleep(STORAGE_SETTLE).await;
            }
            Fault::Write { node, fault } => {
                self.require_node(node)?;
                {
                    let mut injected = self.injected();
                    injected.disk_generation += 1;
                    let generation = injected.disk_generation;
                    let disk = injected.disks.entry(node.clone()).or_default();
                    disk.write = Some(*fault);
                    disk.write_generation = generation;
                    self.write_disk(node, disk)?;
                }
                tokio::time::sleep(STORAGE_SETTLE).await;
            }
        }
        self.injected().active.push(fault.clone());
        Ok(())
    }

    /// Undo `fault`. Healing one that is not in effect is not an error.
    ///
    /// Healing a clock fault puts that process back on the true clock, which
    /// is a step of its own; healing a failed fsync does not un-poison a log
    /// that already refused to go on, which is the point of the fault.
    pub async fn heal(&self, fault: &Fault) -> Result<()> {
        match fault {
            Fault::Drop { from, to } => {
                let links = self.links_between(from, to)?;
                links.rules().set_dropped(from, to, false);
                warn_if_unattributed(links, fault);
            }
            Fault::Delay { from, to, .. } => {
                let links = self.links_between(from, to)?;
                links.rules().set_delay(from, to, Duration::ZERO);
                warn_if_unattributed(links, fault);
            }
            Fault::Refuse { node, peers } => {
                {
                    let mut injected = self.injected();
                    if let Some(refused) = injected.refused.get_mut(node) {
                        for peer in peers {
                            refused.remove(peer);
                        }
                    }
                    self.write_refusals(&injected.refused)?;
                }
                tokio::time::sleep(PARTITION_REREAD).await;
            }
            Fault::Suspend { node } => self.unsuspend(node)?,
            Fault::Clock { process, fault } => {
                let path = self.clock_file(process)?;
                match process {
                    Endpoint::ControlPlane => {
                        self.injected().clocks.remove(process);
                        remove_if_present(&path)?;
                    }
                    // A broker's lease clock is boottime, which never runs
                    // back, so healing only stops the fault from getting
                    // worse: the rate returns to 1x with its drift kept, and
                    // a forward step stays taken.
                    Endpoint::Node(_) => {
                        let content = {
                            let mut injected = self.injected();
                            match injected.clocks.get_mut(process) {
                                Some(clock) if matches!(fault, ClockFault::Rate(_)) => {
                                    clock.rate = 1.0;
                                    Some(clock.file_body())
                                }
                                _ => None,
                            }
                        };
                        if let Some(content) = content {
                            std::fs::write(&path, content).with_context(|| {
                                format!("write the clock fault for {process:?}")
                            })?;
                        }
                    }
                }
                tokio::time::sleep(CLOCK_SETTLE).await;
            }
            Fault::Fsync { node, fault } => {
                {
                    let mut injected = self.injected();
                    let disk = injected.disks.entry(node.clone()).or_default();
                    match fault {
                        FsyncFault::Delay(_) => disk.delay = Duration::ZERO,
                        FsyncFault::Fail | FsyncFault::FailOnce => disk.failure = None,
                    }
                    self.write_disk(node, disk)?;
                }
                tokio::time::sleep(STORAGE_SETTLE).await;
            }
            Fault::Write { node, .. } => {
                {
                    let mut injected = self.injected();
                    let disk = injected.disks.entry(node.clone()).or_default();
                    disk.write = None;
                    self.write_disk(node, disk)?;
                }
                tokio::time::sleep(STORAGE_SETTLE).await;
            }
        }
        let mut injected = self.injected();
        if let Some(index) = injected.active.iter().position(|active| active == fault) {
            injected.active.remove(index);
        }
        Ok(())
    }

    /// Heal every fault injected through [`Self::inject`] and not yet healed,
    /// newest first.
    pub async fn heal_all(&self) -> Result<()> {
        let active = std::mem::take(&mut self.injected().active);
        for fault in active.iter().rev() {
            self.heal(fault).await?;
        }
        Ok(())
    }

    /// The faults in effect, in the order they were injected.
    pub fn active_faults(&self) -> Vec<Fault> {
        self.injected().active.clone()
    }

    /// Datagrams the peer proxies passed while a link fault was in effect
    /// without knowing which broker sent them. Each one skipped whatever
    /// [`Fault::Drop`] or [`Fault::Delay`] applied to its sender, so a test
    /// that depends on a link fault holding can assert this is zero.
    pub fn unattributed_datagrams(&self) -> u64 {
        self.links
            .as_ref()
            .map_or(0, |links| links.rules().unattributed())
    }

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

    /// Stop the control plane the way a crash does, keeping its state for
    /// [`Self::recover_control_plane`]. Brokers keep serving on the authority
    /// they hold until their leases lapse, and nothing is placed meanwhile.
    pub async fn crash_control_plane(&mut self) -> Result<()> {
        let control_plane = self
            .control_plane
            .take()
            .ok_or_else(|| anyhow!("the control plane is not running"))?;
        self.crashed_control_plane = Some(control_plane.stop().await?);
        Ok(())
    }

    /// Bring back a control plane [`Self::crash_control_plane`] stopped, on the
    /// same address over the same store, with placement running again if it
    /// was. Does nothing if it was not crashed.
    pub async fn recover_control_plane(&mut self) -> Result<()> {
        if let Some(stopped) = self.crashed_control_plane.take() {
            self.control_plane = Some(stopped.start().await?);
        }
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
    ///
    /// The partition-file fault ([`Fault::Refuse`]) both ways, rather than
    /// dropped packets: see [`Fault::Drop`] for the proxied kind.
    pub fn partition_node(&self, node_id: &str) -> Result<()> {
        self.require_node(node_id)?;
        {
            let mut injected = self.injected();
            for node in &self.nodes {
                if node.node_id == node_id {
                    let others = self
                        .nodes
                        .iter()
                        .map(|other| other.node_id.clone())
                        .filter(|id| id != node_id);
                    injected
                        .refused
                        .entry(node_id.to_string())
                        .or_default()
                        .extend(others);
                } else {
                    injected
                        .refused
                        .entry(node.node_id.clone())
                        .or_default()
                        .insert(node_id.to_string());
                }
            }
            self.write_refusals(&injected.refused)?;
        }
        await_partition_reread();
        Ok(())
    }

    /// Reconnect everything the partition file severed.
    pub fn heal_partitions(&self) -> Result<()> {
        {
            let mut injected = self.injected();
            injected.refused.clear();
            injected
                .active
                .retain(|fault| !matches!(fault, Fault::Refuse { .. }));
            self.write_refusals(&injected.refused)?;
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

    /// The record of what has been injected. Never held across an await.
    fn injected(&self) -> std::sync::MutexGuard<'_, Injected> {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn require_node(&self, node_id: &str) -> Result<()> {
        self.node(node_id)
            .map(|_| ())
            .ok_or_else(|| anyhow!("unknown node {node_id}"))
    }

    /// The proxies a fault on `from -> to` acts through.
    fn links_between(&self, from: &Endpoint, to: &Endpoint) -> Result<&Links> {
        if from == to {
            bail!("a link needs two different ends, not {from:?} twice");
        }
        for end in [from, to] {
            if let Endpoint::Node(node_id) = end {
                self.require_node(node_id)?;
            }
        }
        self.links.as_ref().ok_or_else(|| {
            anyhow!(
                "link faults need a cluster started with `ClusterConfig {{ proxy_links: true, .. }}`"
            )
        })
    }

    #[cfg(unix)]
    fn suspend(&self, node_id: &str) -> Result<()> {
        self.pause_node(node_id)
    }

    #[cfg(unix)]
    fn unsuspend(&self, node_id: &str) -> Result<()> {
        self.resume_node(node_id)
    }

    #[cfg(not(unix))]
    fn suspend(&self, node_id: &str) -> Result<()> {
        bail!("cannot suspend {node_id}: SIGSTOP is Unix only")
    }

    #[cfg(not(unix))]
    fn unsuspend(&self, node_id: &str) -> Result<()> {
        bail!("cannot resume {node_id}: SIGCONT is Unix only")
    }

    /// Where `process` reads its clock skew from.
    fn clock_file(&self, process: &Endpoint) -> Result<PathBuf> {
        match process {
            Endpoint::Node(node_id) => self
                .node(node_id)
                .map(|node| clock_fault_file(&node.data_dir))
                .ok_or_else(|| anyhow!("unknown node {node_id}")),
            Endpoint::ControlPlane => Ok(self._root.path().join("controlplane-clock-fault")),
        }
    }

    /// Point this process's clock, which the in-process control plane reads,
    /// at `path`. Once per cluster: following again would restart the skew.
    #[cfg(any(debug_assertions, feature = "fault-injection"))]
    fn follow_clock(&self, path: &Path) -> Result<()> {
        let mut injected = self.injected();
        if injected.control_plane_clock.as_deref() != Some(path) {
            // Skews the whole test process, not just this cluster. Safe only
            // because cluster tests are `#[serial]`.
            felix_common::clock::fault::follow(path);
            injected.control_plane_clock = Some(path.to_path_buf());
        }
        Ok(())
    }

    #[cfg(not(any(debug_assertions, feature = "fault-injection")))]
    fn follow_clock(&self, _path: &Path) -> Result<()> {
        bail!("control-plane clock faults need a debug build or the `fault-injection` feature")
    }

    /// Put this process back on the true clock, if this cluster moved it.
    pub(super) fn stop_following_clock(&self) {
        #[cfg(any(debug_assertions, feature = "fault-injection"))]
        if let Some(path) = self.injected().control_plane_clock.take() {
            felix_common::clock::fault::stop_following(&path);
        }
    }

    /// Write every broker's partition file from `refused`, removing the ones
    /// that list nobody.
    fn write_refusals(&self, refused: &HashMap<String, BTreeSet<String>>) -> Result<()> {
        for node in &self.nodes {
            let path = partition_file(&node.data_dir);
            match refused.get(&node.node_id).filter(|peers| !peers.is_empty()) {
                Some(peers) => {
                    let listed: Vec<&str> = peers.iter().map(String::as_str).collect();
                    std::fs::write(&path, listed.join("\n"))
                        .with_context(|| format!("write the partition file for {}", node.node_id))?
                }
                None => remove_if_present(&path)?,
            }
        }
        Ok(())
    }

    fn write_disk(&self, node_id: &str, disk: &DiskFault) -> Result<()> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let path = storage_fault_file(&node.data_dir);
        if disk.delay.is_zero() && disk.failure.is_none() && disk.write.is_none() {
            return remove_if_present(&path);
        }
        let failure = match disk.failure {
            Some(FsyncFault::Fail) => "fail",
            Some(FsyncFault::FailOnce) => "fail_once",
            _ => "ok",
        };
        let write = match disk.write {
            Some(WriteFault::NoSpace) => "enospc",
            Some(WriteFault::Io) => "eio",
            Some(WriteFault::IoOnce) => "eio_once",
            None => "ok",
        };
        let body = format!(
            "fsync_delay_ms={}\nfsync={failure}\ngeneration={}\n\
             write={write}\nwrite_generation={}\n",
            disk.delay.as_millis(),
            disk.generation,
            disk.write_generation,
        );
        std::fs::write(&path, body)
            .with_context(|| format!("write the storage fault for {node_id}"))
    }
}

/// What a cluster has injected and not yet healed, so a heal can undo
/// exactly its own part of a file several faults share.
#[derive(Default)]
pub(super) struct Injected {
    active: Vec<Fault>,
    /// Per broker, the peers its partition file lists.
    refused: HashMap<String, BTreeSet<String>>,
    clocks: HashMap<Endpoint, ClockSkew>,
    disks: HashMap<String, DiskFault>,
    /// Bumped per disk failure injected, so a second `FailOnce` or `IoOnce`
    /// fires again.
    disk_generation: u64,
    /// The file this process's clock follows for the control plane, once a
    /// test has skewed it.
    #[cfg(any(debug_assertions, feature = "fault-injection"))]
    control_plane_clock: Option<PathBuf>,
}

/// One process's clock, as its fault file describes it.
struct ClockSkew {
    offset_ms: i64,
    rate: f64,
}

impl Default for ClockSkew {
    fn default() -> Self {
        Self {
            offset_ms: 0,
            rate: 1.0,
        }
    }
}

impl ClockSkew {
    /// The fault file's contents, in `felix_common::clock::fault`'s format.
    fn file_body(&self) -> String {
        format!("offset_ms={}\nrate={}\n", self.offset_ms, self.rate)
    }
}

/// One broker's disk, as its fault file describes it.
#[derive(Default)]
struct DiskFault {
    delay: Duration,
    failure: Option<FsyncFault>,
    generation: u64,
    write: Option<WriteFault>,
    write_generation: u64,
}

/// Say so when a link fault may not have held: the proxy passed datagrams it
/// could not attribute to a broker while faults were in effect.
fn warn_if_unattributed(links: &Links, fault: &Fault) {
    let unattributed = links.rules().unattributed();
    if unattributed > 0 {
        tracing::warn!(
            unattributed,
            ?fault,
            "datagrams bypassed link faults unattributed; this fault may not have held",
        );
    }
}

/// Refuse a clock fault the process could never see for real.
///
/// A broker's lease runs on `CLOCK_BOOTTIME`, which never goes backwards, so
/// stepping it back would stretch its lease and report unsafety no real
/// machine can produce. The control plane stamps with the wall clock, which
/// can be stepped either way.
fn check_clock_fault(process: &Endpoint, fault: &ClockFault) -> Result<()> {
    match (process, fault) {
        (Endpoint::Node(node), ClockFault::StepMillis(by)) if *by < 0 => {
            bail!(
                "cannot step {node}'s clock back: its lease clock is boottime, which never goes backwards"
            )
        }
        (_, ClockFault::Rate(rate)) if !rate.is_finite() || *rate < 0.0 => {
            bail!("a clock rate must be finite and not negative, not {rate}")
        }
        _ => Ok(()),
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
    std::thread::sleep(PARTITION_REREAD);
}

fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}

#[cfg(test)]
mod tests;
