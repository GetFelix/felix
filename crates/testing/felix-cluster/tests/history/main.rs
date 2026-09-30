//! A fault campaign against a real three-broker cluster, checked afterwards
//! with the list-append history checker.
//!
//! Short and seeded by default, so it runs on every PR and a failure replays
//! the same fault schedule. The nightly workflow runs it for longer with a
//! random seed and every fault family. See `docs/history-checker.md`.
//!
//! Unless `FELIX_HISTORY_MODE` says otherwise, the main campaign runs
//! lease-free and the every-family one on the lease, so each PR covers both.
//! The adversarial one runs lease-free on four brokers.
//!
//! ```text
//! FELIX_HISTORY_SEED=1234 FELIX_HISTORY_DURATION_SECS=300 FELIX_HISTORY_MODE=lease-free \
//!     FELIX_HISTORY_NEMESIS=adversarial \
//!     cargo test -p felix-cluster --test history -- --nocapture
//! ```
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use felix_cluster::history::model::{Action, AppendOutcome};
use felix_cluster::history::nemesis::ClusterView;
use felix_cluster::history::register::RegisterAction;
use felix_cluster::history::rng::Rng;
use felix_cluster::history::{
    self, Campaign, Fault, FaultFamily, FaultKind, History, Mode, Nemesis, RandomNemesis,
};
use serial_test::serial;

/// The per-PR seed. Any seed should pass; a fixed one keeps a red run
/// reproducible.
const SEED: u64 = 0x5eed_0001;

/// Long enough for several faults of each kind, short enough for the PR job.
const DURATION: Duration = Duration::from_secs(45);

/// Long enough for [`RoundRobin::every_family`] to go once round the families, faults
/// that restart a broker included.
const EVERY_FAMILY_DURATION: Duration = Duration::from_secs(75);

/// Long enough for [`RoundRobin`] to go once round the adversarial kinds,
/// each followed by the wait for every shard to serve again.
const ADVERSARIAL_DURATION: Duration = Duration::from_secs(150);

/// **Acknowledged `Quorum` appends survive kills, pauses and partitions**,
/// nothing is duplicated or reordered, reads are prefixes of the final log,
/// no read sees a value that was never written or was refused, and no
/// `Quorum` cache get is stale. Lease-free unless `FELIX_HISTORY_MODE` says
/// otherwise.
///
/// A run given `FELIX_HISTORY_DURATION_SECS`, as the nightly one is, also
/// cuts links, skews clocks, fails fsyncs, moves shards and drains brokers
/// ([`RandomNemesis::all_faults`]), or with `FELIX_HISTORY_NEMESIS=adversarial`
/// injects the compound faults on four brokers
/// ([`RandomNemesis::adversarial`]).
/// The per-PR run keeps [`RandomNemesis::process_faults`], so its fixed seed
/// replays the schedule it always has.
///
/// After every heal each shard must serve again within a bound, or the run
/// fails naming the shards that are stuck.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fault_campaign_keeps_quorum_histories_valid() {
    let mut campaign =
        Campaign::from_env(SEED, DURATION, Mode::LeaseFree).expect("campaign settings");
    let long = std::env::var_os(history::campaign::DURATION_VAR).is_some();
    let nemesis_var = std::env::var(history::campaign::NEMESIS_VAR).ok();
    let (mut nemesis, faults) = match nemesis_var.as_deref() {
        Some("adversarial") => {
            campaign = campaign.with_spare_broker();
            (RandomNemesis::adversarial(), "adversarial faults")
        }
        Some("all") | None if long => (RandomNemesis::all_faults(), "every fault family"),
        None => (RandomNemesis::process_faults(), "process faults"),
        Some(other) => panic!(
            "{}={other:?} is not `all` or `adversarial`",
            history::campaign::NEMESIS_VAR
        ),
    };
    let (seed, mode) = (campaign.seed, campaign.mode);
    println!(
        "history campaign: seed {seed}, mode {mode}, {:?}, {faults}; rerun with \
         FELIX_HISTORY_SEED={seed} FELIX_HISTORY_MODE={mode}",
        campaign.duration
    );
    let history = run_checked(&campaign, &mut nemesis).await;
    assert!(
        history.faults.iter().any(|f| f.what.starts_with("healed")),
        "seed {seed}: no fault was injected and healed"
    );
}

/// **Every fault family runs through the campaign and heals.** Link, clock,
/// disk and assignment faults, one after another with the process faults,
/// leave a
/// valid history, and at least one fault of each family is injected and
/// healed. What the long random schedule relies on, checked on every PR, on
/// the lease unless `FELIX_HISTORY_MODE` says otherwise.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_fault_family_is_injected_and_healed_in_a_campaign() {
    let mut campaign =
        Campaign::from_env(SEED, EVERY_FAMILY_DURATION, Mode::Lease).expect("campaign settings");
    // Fixed: the nightly's long random run already covers these faults.
    campaign.duration = EVERY_FAMILY_DURATION;
    let (seed, mode) = (campaign.seed, campaign.mode);
    println!(
        "every-family campaign: seed {seed}, mode {mode}; rerun with \
         FELIX_HISTORY_SEED={seed} FELIX_HISTORY_MODE={mode}"
    );
    let mut nemesis = RoundRobin::every_family();
    let history = run_checked(&campaign, &mut nemesis).await;

    let healed: BTreeSet<FaultFamily> = nemesis
        .picked
        .iter()
        .filter(|fault| {
            let line = format!("healed {fault}");
            history.faults.iter().any(|f| f.what == line)
        })
        .map(Fault::family)
        .collect();
    for family in [
        FaultFamily::Process,
        FaultFamily::Link,
        FaultFamily::Clock,
        FaultFamily::Disk,
        FaultFamily::Assignment,
    ] {
        assert!(
            healed.contains(&family),
            "seed {seed}: no {family:?} fault was injected and healed; picked {:?}",
            nemesis.picked,
        );
    }
}

/// **The adversarial faults run through the campaign, and the cluster
/// recovers from each.** Two brokers killed at once, a leader isolated
/// until it fails over, a partition beside a delayed link, two random faults
/// together, a move cut short by a kill, a restart loop, a torn write, a
/// drain that replaces follower copies, a power loss on every broker (Linux
/// only; elsewhere it is logged as not injected) and a control plane crash
/// with work in flight, one after another on four brokers
/// with the clients writing throughout. The history stays valid, and after
/// each heal every shard serves again within the liveness bound.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adversarial_faults_heal_and_every_shard_serves_again() {
    let mut campaign = Campaign::from_env(SEED, ADVERSARIAL_DURATION, Mode::LeaseFree)
        .expect("campaign settings")
        .with_spare_broker();
    campaign.duration = ADVERSARIAL_DURATION;
    let (seed, mode) = (campaign.seed, campaign.mode);
    println!(
        "adversarial campaign: seed {seed}, mode {mode}; rerun with \
         FELIX_HISTORY_SEED={seed} FELIX_HISTORY_MODE={mode}"
    );
    let mut nemesis = RoundRobin::new(RandomNemesis::adversarial().kinds().to_vec());
    let history = run_checked(&campaign, &mut nemesis).await;

    let healed: BTreeSet<String> = nemesis
        .picked
        .iter()
        .filter(|fault| {
            let line = format!("healed {fault}");
            history.faults.iter().any(|f| f.what == line)
        })
        .map(|fault| format!("{:?}", fault.kind()))
        .collect();
    // An isolation that forced no failover is logged as one that could not
    // be injected, so a healed one is a partition that caused a failover.
    for kind in [
        FaultKind::Isolate,
        FaultKind::KillTwo,
        FaultKind::RestartLoop,
    ] {
        assert!(
            healed.contains(&format!("{kind:?}")),
            "seed {seed}: no {kind:?} was injected and healed; picked {:?}",
            nemesis.picked,
        );
    }
}

/// Long enough for [`RoundRobin`] to go round a power loss and a control
/// plane crash at least once each.
const OUTAGE_DURATION: Duration = Duration::from_secs(60);

/// **Nothing acknowledged is lost when every broker loses power at once, or
/// when the control plane crashes with work in flight.** A power loss keeps
/// only what each broker flushed, so an acknowledged append or cache put
/// missing afterwards is a write acknowledged before it was durable. The
/// power loss needs Linux; elsewhere it is logged as not injected and only
/// the control plane crash is checked.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_power_loss_or_a_control_plane_crash_loses_nothing_acknowledged() {
    let mut campaign =
        Campaign::from_env(SEED, OUTAGE_DURATION, Mode::LeaseFree).expect("campaign settings");
    campaign.duration = OUTAGE_DURATION;
    let (seed, mode) = (campaign.seed, campaign.mode);
    println!(
        "outage campaign: seed {seed}, mode {mode}; rerun with \
         FELIX_HISTORY_SEED={seed} FELIX_HISTORY_MODE={mode}"
    );
    let mut nemesis = RoundRobin::new(vec![FaultKind::PowerLoss, FaultKind::ControlPlaneCrash]);
    let history = run_checked(&campaign, &mut nemesis).await;

    let healed: BTreeSet<String> = nemesis
        .picked
        .iter()
        .filter(|fault| {
            let line = format!("healed {fault}");
            history.faults.iter().any(|f| f.what == line)
        })
        .map(|fault| format!("{:?}", fault.kind()))
        .collect();
    let mut expected = vec![FaultKind::ControlPlaneCrash];
    if cfg!(target_os = "linux") {
        expected.push(FaultKind::PowerLoss);
    }
    for kind in expected {
        assert!(
            healed.contains(&format!("{kind:?}")),
            "seed {seed}: no {kind:?} was injected and healed; picked {:?}",
            nemesis.picked,
        );
    }
}

/// Start a cluster for `nemesis`, run `campaign`, and check the history,
/// failing on any violation and on a campaign too quiet to prove anything.
async fn run_checked(campaign: &Campaign, nemesis: &mut impl Nemesis) -> History {
    let seed = campaign.seed;
    let started = Instant::now();
    let mut cluster = campaign
        .start(&*nemesis)
        .await
        .unwrap_or_else(|err| panic!("seed {seed}: start the cluster: {err:#}"));
    let history = campaign
        .run(&mut cluster, nemesis)
        .await
        .unwrap_or_else(|err| panic!("seed {seed}: the campaign could not finish: {err:#}"));
    println!("{}", history.summary());

    let report = history::check(&history);
    println!("{report}");
    println!("campaign took {:?}", started.elapsed());
    if !report.is_valid() {
        println!("{}", campaign.state_dump(&cluster).await);
    }
    assert!(
        report.is_valid(),
        "seed {seed}: the history broke the rules; rerun with FELIX_HISTORY_SEED={seed}\n{report}"
    );

    // A campaign where nothing happened proves nothing, however valid.
    assert!(
        history.acknowledged() >= 50,
        "seed {seed}: only {} appends were acknowledged",
        history.acknowledged()
    );
    let (puts, gets) = history
        .registers
        .iter()
        .fold((0, 0), |(puts, gets), op| match op.action {
            RegisterAction::Put {
                acknowledged: true, ..
            } => (puts + 1, gets),
            RegisterAction::Put { .. } => (puts, gets),
            RegisterAction::Get { .. } => (puts, gets + 1),
        });
    assert!(
        puts >= 10 && gets >= 10,
        "seed {seed}: only {puts} cache puts were acknowledged and {gets} gets answered"
    );
    // The rule that an append sits where its acknowledgement said can only
    // fire for appends acknowledged with an offset, which today are commits.
    let at_known_offsets = history
        .ops
        .iter()
        .filter(|op| {
            matches!(
                op.action,
                Action::Append {
                    outcome: AppendOutcome::Ok { offset: Some(_) },
                    ..
                }
            )
        })
        .count();
    assert!(
        at_known_offsets >= 5,
        "seed {seed}: only {at_known_offsets} appends were acknowledged with an offset"
    );
    cluster.shutdown().await;
    history
}

impl RoundRobin {
    /// Goes round the single fault kinds in a fixed order that visits every
    /// family in its first five picks, so a short run is sure to see each
    /// one.
    fn every_family() -> Self {
        Self::new(vec![
            FaultKind::DropOutbound,
            FaultKind::ClockRate,
            FaultKind::FsyncFailOnce,
            FaultKind::MoveShard,
            FaultKind::Kill,
            FaultKind::Drain,
            FaultKind::DelayOutbound,
            FaultKind::ControlPlaneClockStep,
            FaultKind::SlowFsync,
            FaultKind::Pause,
            FaultKind::DropControlPlaneReplies,
            FaultKind::Partition,
        ])
    }
}

/// Goes round `kinds` in order, one fault of each, drawing targets from the
/// seed.
struct RoundRobin {
    kinds: Vec<RandomNemesis>,
    next: usize,
    picked: Vec<Fault>,
}

impl RoundRobin {
    fn new(order: Vec<FaultKind>) -> Self {
        Self {
            kinds: order
                .into_iter()
                .map(|kind| RandomNemesis::new(vec![kind]))
                .collect(),
            next: 0,
            picked: Vec::new(),
        }
    }
}

impl Nemesis for RoundRobin {
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
        let slot = self.next % self.kinds.len();
        let fault = self.kinds[slot].next_fault(rng, view)?;
        self.next += 1;
        self.picked.push(fault.clone());
        Some(fault)
    }

    fn needs_proxy_links(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_proxy_links)
    }

    fn needs_fsync_on_commit(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_fsync_on_commit)
    }

    fn needs_power_loss(&self) -> bool {
        self.kinds.iter().any(Nemesis::needs_power_loss)
    }
}
