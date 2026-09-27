//! A fault campaign against a real three-broker cluster, checked afterwards
//! with the list-append history checker.
//!
//! Short and seeded by default, so it runs on every PR and a failure replays
//! the same fault schedule. The nightly workflow runs it for longer with a
//! random seed and every fault family. See `docs/history-checker.md`.
//!
//! ```text
//! FELIX_HISTORY_SEED=1234 FELIX_HISTORY_DURATION_SECS=300 \
//!     cargo test -p felix-cluster --test history -- --nocapture
//! ```
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use felix_cluster::Cluster;
use felix_cluster::history::nemesis::ClusterView;
use felix_cluster::history::rng::Rng;
use felix_cluster::history::{
    self, Campaign, Fault, FaultFamily, FaultKind, History, Nemesis, RandomNemesis,
};
use serial_test::serial;

/// The per-PR seed. Any seed should pass; a fixed one keeps a red run
/// reproducible.
const SEED: u64 = 0x5eed_0001;

/// Long enough for several faults of each kind, short enough for the PR job.
const DURATION: Duration = Duration::from_secs(45);

/// Long enough for [`EveryFamily`] to go once round the families, faults
/// that restart a broker included.
const EVERY_FAMILY_DURATION: Duration = Duration::from_secs(75);

/// **Acknowledged `Quorum` appends survive kills, pauses and partitions**,
/// nothing is duplicated or reordered, reads are prefixes of the final log,
/// and no read sees a value that was never written or was refused.
///
/// A run given `FELIX_HISTORY_DURATION_SECS`, as the nightly one is, also
/// cuts links, skews clocks and fails fsyncs ([`RandomNemesis::all_faults`]).
/// The per-PR run keeps [`RandomNemesis::process_faults`], so its fixed seed
/// replays the schedule it always has.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fault_campaign_keeps_quorum_histories_valid() {
    let campaign = Campaign::from_env(SEED, DURATION).expect("campaign settings");
    let (mut nemesis, faults) = if std::env::var_os(history::campaign::DURATION_VAR).is_some() {
        (RandomNemesis::all_faults(), "every fault family")
    } else {
        (RandomNemesis::process_faults(), "process faults")
    };
    let seed = campaign.seed;
    println!(
        "history campaign: seed {seed}, {:?}, {faults}; rerun with FELIX_HISTORY_SEED={seed}",
        campaign.duration
    );
    let history = run_checked(&campaign, &mut nemesis).await;
    assert!(
        history.faults.iter().any(|f| f.what.starts_with("healed")),
        "seed {seed}: no fault was injected and healed"
    );
}

/// **Every fault family runs through the campaign and heals.** Link, clock
/// and disk faults, one after another with the process faults, leave a
/// valid history, and at least one fault of each family is injected and
/// healed. What the long random schedule relies on, checked on every PR.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_fault_family_is_injected_and_healed_in_a_campaign() {
    let mut campaign = Campaign::from_env(SEED, EVERY_FAMILY_DURATION).expect("campaign settings");
    // Fixed: the nightly's long random run already covers these faults.
    campaign.duration = EVERY_FAMILY_DURATION;
    let seed = campaign.seed;
    println!("every-family campaign: seed {seed}; rerun with FELIX_HISTORY_SEED={seed}");
    let mut nemesis = EveryFamily::new();
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
    ] {
        assert!(
            healed.contains(&family),
            "seed {seed}: no {family:?} fault was injected and healed; picked {:?}",
            nemesis.picked,
        );
    }
}

/// Start a cluster for `nemesis`, run `campaign`, and check the history,
/// failing on any violation and on a campaign too quiet to prove anything.
async fn run_checked(campaign: &Campaign, nemesis: &mut impl Nemesis) -> History {
    let seed = campaign.seed;
    let started = Instant::now();
    let mut cluster = Cluster::start(campaign.cluster_config(&*nemesis))
        .await
        .expect("start cluster");
    let history = campaign
        .run(&mut cluster, nemesis)
        .await
        .unwrap_or_else(|err| panic!("seed {seed}: the campaign could not finish: {err:#}"));
    println!("{}", history.summary());

    let report = history::check(&history);
    println!("{report}");
    println!("campaign took {:?}", started.elapsed());
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
    cluster.shutdown().await;
    history
}

/// Goes round the fault kinds in a fixed order that visits every family in
/// its first four picks, so a short run is sure to see each one. Targets are
/// still drawn from the seed.
struct EveryFamily {
    kinds: Vec<RandomNemesis>,
    next: usize,
    picked: Vec<Fault>,
}

impl EveryFamily {
    fn new() -> Self {
        let order = [
            FaultKind::DropOutbound,
            FaultKind::ClockRate,
            FaultKind::FsyncFailOnce,
            FaultKind::Kill,
            FaultKind::DelayOutbound,
            FaultKind::ControlPlaneClockStep,
            FaultKind::SlowFsync,
            FaultKind::Pause,
            FaultKind::DropControlPlaneReplies,
            FaultKind::Partition,
        ];
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

impl Nemesis for EveryFamily {
    fn next_fault(&mut self, rng: &mut Rng, view: &ClusterView) -> Option<Fault> {
        let fault = self.kinds[self.next % self.kinds.len()].next_fault(rng, view)?;
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
}
