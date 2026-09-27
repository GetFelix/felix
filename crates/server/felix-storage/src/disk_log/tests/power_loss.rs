//! Recovery after a power loss, not a process crash.
//!
//! A workload runs against a real log while `io::power_loss` remembers what
//! each flush made durable. At intervals the test builds the tree a reboot
//! would find -- unsynced pages dropped, torn or zeroed, unflushed directory
//! changes undone -- and opens a log on that copy. Three things must hold for
//! every crash:
//!
//! - recovery succeeds: an honest crash is never reported as corruption;
//! - every record acknowledged as durable before the crash is there, intact;
//! - what recovery kept is a gap-free prefix of what was appended, and the
//!   repaired log takes appends and reopens.
//!
//! Every failure names its seed. The workload races a background roll against
//! appends and flushes, so a seed replays the same crashes but not always the
//! same interleaving; that is why each test runs many seeds.

use std::path::Path;

use super::*;

use crate::io::power_loss::{PowerLoss, SplitMix64, Writeback};

/// Crashes built from each workload checkpoint.
const CRASHES_PER_CHECKPOINT: u64 = 6;
/// Appends between checkpoints.
const STEPS_PER_CHECKPOINT: u64 = 7;
const CHECKPOINTS: u64 = 12;
/// Workload seeds per test, from `FELIX_POWER_LOSS_SEED` upwards. Eight keeps
/// the suite to a few seconds; `FELIX_POWER_LOSS_SEEDS` runs more.
const DEFAULT_SEEDS: u64 = 8;
/// The first workload seed: the one CI failed on, crash seed `0x5_5eed_0001`
/// (checkpoint 5, trial 0), when a lost-race preparation came back mid-chain.
const BASE_SEED: u64 = 0x5eed_0001;

#[derive(Debug, Clone, Copy)]
struct Scenario {
    fsync_mode: FsyncMode,
    /// Roll in the background before the segment is full, rather than inline.
    background_roll: bool,
    writeback: Writeback,
    seed: u64,
}

impl Scenario {
    fn config(&self) -> LogConfig {
        LogConfig {
            // Several pages per segment, so a crash can land inside, between
            // and across segments, and rollover happens every few checkpoints.
            segment_size_bytes: 3 * crate::io::power_loss::PAGE as u64 + 700,
            index_spacing_bytes: 512,
            fsync_mode: self.fsync_mode,
            preallocate_segments: true,
            rollover_threshold_percent: if self.background_roll { 60 } else { 100 },
            max_overshoot_percent: 200,
            ..LogConfig::default()
        }
    }
}

/// The payload at `offset`, so the check needs no record of what was written.
/// Lengths vary so records straddle pages and sectors.
fn payload(seed: u64, offset: Offset) -> Vec<u8> {
    let mut rng = SplitMix64::new(seed ^ offset.wrapping_mul(0x2545_F491_4F6C_DD1D));
    let len = 1 + rng.below(900) as usize;
    let mut out = format!("{seed}:{offset}:").into_bytes();
    out.extend((0..len).map(|_| b'a' + rng.below(26) as u8));
    out
}

async fn run(scenario: Scenario) {
    let dir = tempdir().expect("dir");
    let root = dir.path().join("log");
    std::fs::create_dir_all(&root).expect("log dir");
    let observer = PowerLoss::install(&root).expect("install the power-loss observer");
    let config = scenario.config();
    let log = DiskLog::open(&root, "t/ns/s/0", config.clone()).expect("open");

    let mut rng = SplitMix64::new(scenario.seed);
    let mut next: Offset = 0;
    let mut crashes = 0;
    for checkpoint in 0..CHECKPOINTS {
        for _ in 0..STEPS_PER_CHECKPOINT {
            let batch = 1 + rng.below(4);
            let records: Vec<AppendRecord> = (next..next + batch)
                .map(|offset| AppendRecord {
                    payload: Bytes::from(payload(scenario.seed, offset)),
                    timestamp_micros: 1_700_000_000 + offset,
                    mark: Default::default(),
                })
                .collect();
            let appended = log.append(&records).await.expect("append");
            assert_eq!(appended.first_offset, next);
            next += batch;
            if matches!(scenario.fsync_mode, FsyncMode::OnCommit) {
                assert!(
                    log.durable_offset() >= next,
                    "OnCommit acked before durable"
                );
            }
            match rng.below(6) {
                0 => log.sync().await.expect("sync"),
                1 => tokio::time::sleep(Duration::from_millis(3)).await,
                _ => {}
            }
        }

        // Everything the log has said is durable. A crash may keep more, and
        // must never keep less.
        let acknowledged = log.durable_offset();
        for trial in 0..CRASHES_PER_CHECKPOINT {
            let seed = scenario.seed ^ (checkpoint << 32) ^ trial;
            let image = tempdir().expect("image dir");
            observer
                .crash(seed, scenario.writeback, image.path())
                .expect("build the crash image");
            verify(image.path(), &config, scenario, seed, acknowledged, next).await;
            crashes += 1;
        }
    }
    assert!(
        log.segments().len() > 2,
        "the workload never rolled a segment"
    );
    assert!(crashes > 0);
    log.shutdown().await.expect("shutdown");

    let counts = observer.counts();
    assert!(
        counts.data + counts.uring > 0 && counts.dir > 0,
        "the observer saw no flushes, so every crash was a copy: {counts:?}",
    );
}

async fn verify(
    image: &Path,
    config: &LogConfig,
    scenario: Scenario,
    seed: u64,
    acknowledged: Offset,
    appended: Offset,
) {
    let replay = format!("{scenario:?}, crash seed {seed:#x}");
    let log = match DiskLog::open(image, "t/ns/s/0", config.clone()) {
        Ok(log) => log,
        Err(err) => panic!("recovery refused an honest crash ({replay}): {err}"),
    };
    let records = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap_or_else(|err| panic!("read after recovery ({replay}): {err}"));

    assert!(
        records.len() as u64 >= acknowledged,
        "lost acknowledged records ({replay}): recovered {} of {acknowledged} durable",
        records.len(),
    );
    assert!(
        records.len() as u64 <= appended,
        "invented records ({replay})"
    );
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, index as u64, "gap or reorder ({replay})");
        assert_eq!(
            record.payload.as_ref(),
            payload(scenario.seed, record.offset).as_slice(),
            "record {} came back different ({replay})",
            record.offset,
        );
    }

    // Recovery repaired what it found; the log it left must take a write and
    // come back with it.
    let tail = records.len() as u64;
    let more = AppendRecord {
        payload: Bytes::from_static(b"after the crash"),
        timestamp_micros: 1,
        mark: Default::default(),
    };
    let appended = log
        .append(std::slice::from_ref(&more))
        .await
        .unwrap_or_else(|err| panic!("append after recovery ({replay}): {err}"));
    assert_eq!(appended.first_offset, tail, "{replay}");
    log.sync().await.expect("sync after recovery");
    log.shutdown().await.expect("shutdown after recovery");
    drop(log);
    let reopened = DiskLog::open(image, "t/ns/s/0", config.clone())
        .unwrap_or_else(|err| panic!("reopen after repair ({replay}): {err}"));
    assert_eq!(
        reopened.tail_offset().await.expect("tail"),
        tail + 1,
        "{replay}"
    );
    reopened.shutdown().await.expect("shutdown");
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    match value.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => value.parse().ok(),
    }
}

/// Run the scenario once per seed, sequentially: every run is a real log with
/// its own observer, and interleaving them would only blur which seed failed.
async fn run_seeds(fsync_mode: FsyncMode, background_roll: bool, writeback: Writeback) {
    let base = env_u64("FELIX_POWER_LOSS_SEED").unwrap_or(BASE_SEED);
    let count = env_u64("FELIX_POWER_LOSS_SEEDS")
        .unwrap_or(DEFAULT_SEEDS)
        .max(1);
    for seed in base..base.saturating_add(count) {
        run(Scenario {
            fsync_mode,
            background_roll,
            writeback,
            seed,
        })
        .await;
    }
}

const PERIODIC: FsyncMode = FsyncMode::Periodic {
    interval: Duration::from_millis(2),
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_commit_survives_in_order_writeback() {
    run_seeds(FsyncMode::OnCommit, false, Writeback::InOrder).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_commit_survives_any_writeback() {
    run_seeds(FsyncMode::OnCommit, false, Writeback::AnySubset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_commit_with_background_roll_survives_any_writeback() {
    run_seeds(FsyncMode::OnCommit, true, Writeback::AnySubset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_survives_in_order_writeback() {
    run_seeds(PERIODIC, true, Writeback::InOrder).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_survives_any_writeback() {
    run_seeds(PERIODIC, false, Writeback::AnySubset).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_with_background_roll_survives_any_writeback() {
    run_seeds(PERIODIC, true, Writeback::AnySubset).await;
}

/// `None` acknowledges without flushing; only an explicit `sync` makes
/// anything durable, and that is what the test holds it to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_fsync_keeps_what_explicit_syncs_covered() {
    run_seeds(FsyncMode::None, false, Writeback::AnySubset).await;
}

/// The same guarantee when flushes go through `io_uring`: the ring is its own
/// path to the device, and the observer has to see it for the model to hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_commit_through_io_uring_survives_any_writeback() {
    if !crate::io::uring_fsync::available() {
        eprintln!("io_uring is unavailable here; the blocking path is covered above");
        return;
    }
    let _ring = crate::io::uring_fsync::ForceForTests::hold();
    run_seeds(FsyncMode::OnCommit, true, Writeback::AnySubset).await;
}

/// Keeps the test above honest about which path ran: with the ring forced,
/// segment flushes reach the observer through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_uring_path_is_observed() {
    if !crate::io::uring_fsync::available() {
        return;
    }
    let _ring = crate::io::uring_fsync::ForceForTests::hold();
    let dir = tempdir().expect("dir");
    let observer = PowerLoss::install(dir.path()).expect("install");
    let log = DiskLog::open(dir.path(), "t/ns/s/0", config(FsyncMode::OnCommit)).expect("open");
    log.append(&records(&["a"])).await.expect("append");
    assert!(observer.counts().uring > 0, "{:?}", observer.counts());
}
