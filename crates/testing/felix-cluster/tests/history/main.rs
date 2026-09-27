//! A fault campaign against a real three-broker cluster, checked afterwards
//! with the list-append history checker.
//!
//! Short and seeded by default, so it runs on every PR and a failure replays
//! the same fault schedule. The nightly workflow runs it for longer with a
//! random seed. See `docs/history-checker.md`.
//!
//! ```text
//! FELIX_HISTORY_SEED=1234 FELIX_HISTORY_DURATION_SECS=300 \
//!     cargo test -p felix-cluster --test history -- --nocapture
//! ```
use std::time::{Duration, Instant};

use felix_cluster::Cluster;
use felix_cluster::history::{self, Campaign, RandomNemesis};
use serial_test::serial;

/// The per-PR seed. Any seed should pass; a fixed one keeps a red run
/// reproducible.
const SEED: u64 = 0x5eed_0001;

/// Long enough for several faults of each kind, short enough for the PR job.
const DURATION: Duration = Duration::from_secs(45);

/// **Acknowledged `Quorum` appends survive kills, pauses and partitions**,
/// nothing is duplicated or reordered, reads are prefixes of the final log,
/// and no read sees a value that was never written or was refused.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fault_campaign_keeps_quorum_histories_valid() {
    let campaign = Campaign::from_env(SEED, DURATION).expect("campaign settings");
    let seed = campaign.seed;
    println!(
        "history campaign: seed {seed}, {:?}; rerun with FELIX_HISTORY_SEED={seed}",
        campaign.duration
    );
    let started = Instant::now();
    let mut cluster = Cluster::start(campaign.cluster_config())
        .await
        .expect("start cluster");
    let history = campaign
        .run(&mut cluster, &mut RandomNemesis::process_faults())
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
    assert!(
        history.faults.iter().any(|f| f.what.starts_with("healed")),
        "seed {seed}: no fault was injected and healed"
    );
    cluster.shutdown().await;
}
