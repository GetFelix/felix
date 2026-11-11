//! A failed flush at every flush site of a log, on the simulator's
//! filesystem.
//!
//! Each operation is run again and again, failing its first flush, then its
//! second, and so on until it runs with none failing. A flush here is any
//! `fsync`, `fdatasync` or directory sync. After each failure the log must be
//! poisoned and refuse the next append with [`StorageError::Poisoned`], the
//! power is cut, and the reopened log must hold every record that was
//! acknowledged.
//!
//! Built only with the `sim` feature; CI runs it in the coverage job
//! (`--all-features`).

use std::future::Future;
use std::path::PathBuf;

use super::*;
use crate::disk_log::{Member, Membership, ReplicaSet};
use crate::sim_fs;

const KEY: &str = "t/ns/s/0";
/// Records acknowledged before the operation under test.
const BEFORE: u64 = 6;

fn config() -> LogConfig {
    LogConfig {
        // Small, so a handful of appends rolls the segment.
        segment_size_bytes: 4096,
        index_spacing_bytes: 512,
        fsync_mode: FsyncMode::OnCommit,
        rollover_threshold_percent: 100,
        max_overshoot_percent: 200,
        // Retention runs only when a test asks.
        retention_check_interval: std::time::Duration::from_secs(3600),
        ..LogConfig::default()
    }
}

fn payload(offset: Offset) -> Vec<u8> {
    format!("{offset}:{}", "x".repeat(700)).into_bytes()
}

fn numbered(offset: Offset) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from(payload(offset)),
        timestamp_micros: 1_700_000_000 + offset,
        mark: Default::default(),
        publisher: None,
        headers: None,
    }
}

struct Sim(PathBuf);

impl Sim {
    fn new(name: &str, skip: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "felix-simfs-flushfail-{}-{name}-{skip}",
            std::process::id()
        ));
        sim_fs::register_root(&root);
        Self(root)
    }

    fn dir(&self) -> PathBuf {
        self.0.join("log")
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.0);
    }
}

/// Append records one at a time and return how many were acknowledged.
async fn append_until_refused(log: &DiskLog, from: Offset, count: u64) -> (u64, Result<()>) {
    for offset in from..from + count {
        if let Err(err) = log.append(&[numbered(offset)]).await {
            return (offset, Err(err));
        }
    }
    (from + count, Ok(()))
}

/// A background rollover fails after the append that started it returned.
async fn poisoned_soon(log: &DiskLog) -> bool {
    for _ in 0..200 {
        if log.is_poisoned() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    false
}

/// Fail each flush of `operation` in turn. `operation` returns the tail it
/// was acknowledged up to and its result; `reopened` checks the state the
/// log comes back with. Returns how many flush sites were failed.
async fn every_flush_site<Op, Fut>(name: &str, operation: Op, reopened: impl Fn(&DiskLog)) -> u64
where
    Op: Fn(DiskLog) -> Fut,
    Fut: Future<Output = (DiskLog, u64, Result<()>)>,
{
    every_flush_site_after(name, async |_| {}, operation, reopened).await
}

/// [`every_flush_site`], with `prepare` run before the fault is armed.
async fn every_flush_site_after<Op, Fut>(
    name: &str,
    prepare: impl AsyncFn(&DiskLog),
    operation: Op,
    reopened: impl Fn(&DiskLog),
) -> u64
where
    Op: Fn(DiskLog) -> Fut,
    Fut: Future<Output = (DiskLog, u64, Result<()>)>,
{
    for skip in 0.. {
        let sim = Sim::new(name, skip);
        let log = DiskLog::open(sim.dir(), KEY, config()).expect("open");
        let (acknowledged, written) = append_until_refused(&log, 0, BEFORE).await;
        written.expect("append before the fault");
        assert_eq!(acknowledged, BEFORE);
        prepare(&log).await;

        sim_fs::fail_flush(&sim.0, skip).expect("arm");
        let (log, acknowledged, outcome) = operation(log).await;
        // A background rollover the operation started may still be flushing.
        let poisoned = poisoned_soon(&log).await;
        if !sim_fs::flush_failed(&sim.0).expect("disarm") {
            outcome.unwrap_or_else(|err| panic!("{name}: failed with no flush failing: {err}"));
            assert!(!log.is_poisoned(), "{name}: poisoned with no flush failing");
            return skip;
        }

        assert!(
            poisoned,
            "{name}: flush {skip} failed and the log carried on (returned {outcome:?})"
        );
        let refused = log.append(&[numbered(acknowledged)]).await;
        assert!(
            matches!(refused, Err(StorageError::Poisoned { .. })),
            "{name}: flush {skip} failed and a later append got {refused:?}"
        );
        assert!(
            matches!(log.sync().await, Err(StorageError::Poisoned { .. })),
            "{name}: flush {skip} failed and a later sync was not refused"
        );

        // The kernel may have dropped what the failed flush was asked to
        // keep. Reopening reads back what reached the disk.
        drop(log);
        sim_fs::crash(&sim.0, skip).expect("crash");
        let log = DiskLog::open(sim.dir(), KEY, config())
            .unwrap_or_else(|err| panic!("{name}: reopen after flush {skip} failed: {err}"));
        assert!(!log.is_poisoned());
        // From the oldest record retention left.
        let base = log.readable_base();
        let records = log
            .read_range(ReadRange {
                start: base,
                max_bytes: usize::MAX,
            })
            .await
            .expect("read")
            .records;
        let kept: Vec<_> = records
            .iter()
            .filter(|record| record.offset < acknowledged)
            .collect();
        assert_eq!(
            kept.len() as u64,
            acknowledged.saturating_sub(base),
            "{name}: flush {skip} failed and the acknowledged records from {base} to \
             {acknowledged} did not all come back"
        );
        for (record, offset) in kept.iter().zip(base..) {
            assert_eq!(record.offset, offset, "{name}: gap or reorder");
            assert_eq!(record.payload.as_ref(), payload(offset).as_slice());
        }
        reopened(&log);
        log.append(&[numbered(acknowledged)])
            .await
            .expect("append after reopen");
    }
    unreachable!()
}

/// A state write that hit the failed flush says so itself.
fn state_write(outcome: Result<()>) -> Result<()> {
    assert!(
        matches!(outcome, Ok(()) | Err(StorageError::SyncFailed(_))),
        "{outcome:?}"
    );
    outcome
}

fn set(generation: u64) -> ReplicaSet {
    ReplicaSet {
        current: Membership {
            generation,
            members: ["a", "b", "c"]
                .into_iter()
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .collect(),
        },
        leaving: None,
        earlier: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_an_append_poisons_the_log() {
    let sites = every_flush_site(
        "append",
        |log| async move {
            let (acknowledged, outcome) = append_until_refused(&log, BEFORE, 1).await;
            (log, acknowledged, outcome)
        },
        |_| {},
    )
    .await;
    assert!(sites >= 1, "an acknowledged append flushes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_during_a_rollover_poisons_the_log() {
    let sites = every_flush_site(
        "rollover",
        |log| async move {
            // Enough to roll the segment more than once.
            let (acknowledged, outcome) = append_until_refused(&log, BEFORE, 14).await;
            (log, acknowledged, outcome)
        },
        |_| {},
    )
    .await;
    // The appends' own flushes, and for each roll the seal, the index, the
    // new segment and the directory.
    assert!(sites > 14, "only {sites} flushes in two rollovers");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_the_replica_set_poisons_the_log() {
    let sites = every_flush_site(
        "set",
        |log| async move {
            let outcome = log.accept_set(set(3)).await.map(|_| ());
            (log, BEFORE, state_write(outcome))
        },
        // The set file is the old one or the new one.
        |log| assert!(matches!(log.set_generation(), 0 | 3)),
    )
    .await;
    assert_eq!(sites, 2, "the temporary and the directory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_a_vote_poisons_the_log() {
    let sites = every_flush_site(
        "vote",
        |log| async move {
            let outcome = log.accept_generation(4, Some("a")).await.map(|_| ());
            (log, BEFORE, state_write(outcome))
        },
        |log| match log.accepted_generation() {
            0 => {}
            4 => assert!(matches!(log.accepted_leader().as_deref(), None | Some("a"))),
            other => panic!("reopened at generation {other}"),
        },
    )
    .await;
    assert_eq!(sites, 4, "the ballot and the replica state");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_the_commit_offset_poisons_the_log() {
    let sites = every_flush_site(
        "commit-offset",
        |log| async move {
            // Written behind, so the call itself does not fail.
            let outcome = log.advance_commit_offset(BEFORE).await;
            (log, BEFORE, outcome)
        },
        |log| assert!(log.commit_offset() <= BEFORE),
    )
    .await;
    assert_eq!(sites, 2, "the temporary and the directory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_the_generation_history_poisons_the_log() {
    let sites = every_flush_site(
        "generations",
        |log| async move {
            let outcome = log.record_generation(2, BEFORE).map(|_| ());
            (log, BEFORE, state_write(outcome))
        },
        |_| {},
    )
    .await;
    assert_eq!(sites, 2, "the temporary and the directory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_during_a_truncation_poisons_the_log() {
    let sites = every_flush_site(
        "truncate",
        |log| async move {
            // Nothing below the commit offset is cut, so every record stays
            // acknowledged only up to the cut.
            let outcome = log.truncate(BEFORE - 2).await;
            (log, BEFORE - 2, outcome)
        },
        |_| {},
    )
    .await;
    assert!(sites >= 2, "only {sites} flushes in a truncation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_during_a_seal_poisons_the_log() {
    let sites = every_flush_site(
        "seal",
        |log| async move {
            let outcome = log.seal().await.map(|_| ());
            (log, BEFORE, outcome)
        },
        |_| {},
    )
    .await;
    // The segment, its index, the segment after it and the directory.
    assert!(sites >= 3, "only {sites} flushes in a seal");
}

/// Enough records for several sealed segments, with every roll finished.
async fn fill_segments(log: &DiskLog) -> u64 {
    let (tail, written) = append_until_refused(log, BEFORE, 18).await;
    written.expect("append");
    log.sync().await.expect("sync");
    assert!(log.segments().len() >= 4, "{:?}", log.segments());
    tail
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_a_retention_removal_poisons_the_log() {
    let sites = every_flush_site_after(
        "retention",
        async |log| {
            fill_segments(log).await;
            log.set_retention(crate::log::Retention {
                bytes: Some(2 * 4096),
                age: None,
            })
            .expect("bounds");
        },
        |log| async move {
            let outcome = log.enforce_retention_now().await;
            if let Ok(outcome) = &outcome {
                assert!(outcome.segments_deleted > 0, "{outcome:?}");
            }
            (log, BEFORE + 18, outcome.map(|_| ()))
        },
        |_| {},
    )
    .await;
    assert!(sites >= 1, "a removal syncs the directory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_of_a_removal_below_the_floor_poisons_the_log() {
    // The trim is the last record, so everything before it is numbered.
    const TRIM_AT: u64 = BEFORE + 18;
    let sites = every_flush_site_after(
        "floor",
        async |log| {
            fill_segments(log).await;
            let trim = AppendRecord {
                mark: RecordMark::Trim { floor: 12 },
                ..numbered(TRIM_AT)
            };
            log.append(&[trim]).await.expect("trim");
            assert_eq!(log.readable_base(), 12);
        },
        |log| async move {
            let outcome = log.enforce_retention_now().await;
            if let Ok(outcome) = &outcome {
                assert!(outcome.segments_deleted > 0, "{outcome:?}");
            }
            (log, TRIM_AT, outcome.map(|_| ()))
        },
        |_| {},
    )
    .await;
    assert!(sites >= 1, "a removal syncs the directory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poisoned_log_refuses_state_writes() {
    let sim = Sim::new("state-writes", 0);
    let log = DiskLog::open(sim.dir(), KEY, config()).expect("open");
    sim_fs::fail_flush(&sim.0, 0).expect("arm");
    let failed = log.append(&[numbered(0)]).await;
    assert!(
        matches!(failed, Err(StorageError::SyncFailed(_))),
        "{failed:?}"
    );

    // The flush works again, and nothing may trust it.
    assert!(matches!(
        log.accept_set(set(3)).await,
        Err(StorageError::Poisoned { .. })
    ));
    assert!(matches!(
        log.accept_generation(4, Some("a")).await,
        Err(StorageError::Poisoned { .. })
    ));
    assert!(matches!(
        log.record_generation(2, 0),
        Err(StorageError::Poisoned { .. })
    ));
    assert_eq!(log.set_generation(), 0);
    assert_eq!(log.accepted_generation(), 0);
}

/// What a readiness probe reports: the shard and what stopped it, which for
/// a truncation that failed without a flush failing is not a flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poisoned_log_is_listed_with_its_cause() {
    let sim = Sim::new("listed", 0);
    let flushed = "t/ns/listed/0";
    let log = DiskLog::open(sim.dir(), flushed, config()).expect("open");
    sim_fs::fail_flush(&sim.0, 0).expect("arm");
    log.append(&[numbered(0)]).await.expect_err("flush");
    let listed = crate::disk_log::poisoned_logs();
    let entry = listed
        .iter()
        .find(|log| log.shard == flushed)
        .expect("listed");
    assert!(entry.reason.contains("sync"), "{}", entry.reason);

    let rewound = "t/ns/listed/1";
    let log = DiskLog::open(sim.0.join("rewound"), rewound, config()).expect("open");
    let failed: Result<()> = Err(StorageError::Io(std::io::Error::other("unlink refused")));
    log.inner.poison_after_rewind(failed).expect_err("rewind");
    let listed = crate::disk_log::poisoned_logs();
    let entry = listed
        .iter()
        .find(|log| log.shard == rewound)
        .expect("listed");
    assert_eq!(
        entry.reason,
        "a truncation or reset failed: io error: unlink refused"
    );
}
