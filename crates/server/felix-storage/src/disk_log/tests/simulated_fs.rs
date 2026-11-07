//! Recovery on the simulator's in-memory filesystem: the contract the real
//! power-loss tests check on disk, without touching it.
//!
//! A log is written on a registered root, the power is cut at a seed, and a
//! second log opens on what is left. Acknowledged records must be there and
//! intact, what survives must be a gap-free prefix, and a torn tail must be
//! repaired so the log takes appends again.

use std::path::{Path, PathBuf};

use super::*;
use crate::io::writeback::SplitMix64;
use crate::sim_fs;

const KEY: &str = "t/ns/s/0";

fn config(fsync_mode: FsyncMode) -> LogConfig {
    LogConfig {
        // A few pages per segment, so a crash lands inside and between
        // segments and the workload rolls several times.
        segment_size_bytes: 3 * 4096 + 700,
        index_spacing_bytes: 512,
        fsync_mode,
        preallocate_segments: true,
        rollover_threshold_percent: 100,
        max_overshoot_percent: 200,
        ..LogConfig::default()
    }
}

fn payload(offset: Offset) -> Vec<u8> {
    let mut rng = SplitMix64::new(offset.wrapping_mul(0x2545_F491_4F6C_DD1D));
    let len = 1 + rng.below(900) as usize;
    let mut out = format!("{offset}:").into_bytes();
    out.extend((0..len).map(|_| b'a' + rng.below(26) as u8));
    out
}

fn record(offset: Offset) -> AppendRecord {
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
    fn new(seed: u64) -> Self {
        let root =
            std::env::temp_dir().join(format!("felix-simfs-log-{}-{seed}", std::process::id()));
        sim_fs::register_root(&root);
        Self(root)
    }

    fn log_dir(&self) -> PathBuf {
        self.0.join("log")
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.0);
    }
}

/// Append `count` records from `from`, one batch at a time.
async fn append(log: &DiskLog, from: Offset, count: u64) {
    for offset in from..from + count {
        log.append(&[record(offset)]).await.expect("append");
    }
}

/// Open a log on what the crash left and check it against the contract.
/// Returns how many records came back.
async fn recover(dir: &Path, fsync_mode: FsyncMode, acknowledged: u64, appended: u64) -> u64 {
    let log =
        DiskLog::open(dir, KEY, config(fsync_mode)).expect("recovery refused an honest crash");
    let records = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read after recovery")
        .records;
    let kept = records.len() as u64;
    assert!(
        kept >= acknowledged,
        "lost acknowledged records: {kept} of {acknowledged}"
    );
    assert!(kept <= appended, "invented records: {kept} of {appended}");
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, index as u64, "gap or reorder");
        assert_eq!(record.payload.as_ref(), payload(record.offset).as_slice());
    }

    // The repaired log takes a write and keeps it across a reopen.
    log.append(&[record(kept)])
        .await
        .expect("append after recovery");
    log.sync().await.expect("sync after recovery");
    log.shutdown().await.expect("shutdown");
    drop(log);
    let reopened = DiskLog::open(dir, KEY, config(fsync_mode)).expect("reopen");
    assert_eq!(reopened.tail_offset().await.expect("tail"), kept + 1);
    reopened.shutdown().await.expect("shutdown");
    kept
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_commit_acknowledged_records_survive_every_crash() {
    for seed in 0..24 {
        let sim = Sim::new(seed);
        let log = DiskLog::open(sim.log_dir(), KEY, config(FsyncMode::OnCommit)).expect("open");
        append(&log, 0, 80).await;
        let acknowledged = log.durable_offset();
        assert_eq!(
            acknowledged, 80,
            "OnCommit acknowledged before it was durable"
        );
        assert!(
            log.segments().len() > 2,
            "the workload never rolled a segment"
        );

        sim_fs::crash(&sim.0, seed).expect("crash");
        let kept = recover(&sim.log_dir(), FsyncMode::OnCommit, acknowledged, 80).await;
        assert_eq!(kept, 80, "seed {seed}");
        log.shutdown().await.expect("shutdown the cut-off log");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsynced_tail_is_repaired_and_the_synced_prefix_survives() {
    let mut short = 0;
    let mut whole = 0;
    for seed in 0..48 {
        let sim = Sim::new(1_000 + seed);
        let log = DiskLog::open(sim.log_dir(), KEY, config(FsyncMode::None)).expect("open");
        append(&log, 0, 20).await;
        log.sync().await.expect("sync");
        let acknowledged = log.durable_offset();
        assert_eq!(acknowledged, 20);
        append(&log, 20, 20).await;

        sim_fs::crash(&sim.0, seed).expect("crash");
        let kept = recover(&sim.log_dir(), FsyncMode::None, acknowledged, 40).await;
        if kept < 40 {
            short += 1;
        } else {
            whole += 1;
        }
        log.shutdown().await.expect("shutdown the cut-off log");
    }
    assert!(short > 0, "no crash ever lost the unsynced tail");
    let _ = whole;
}

/// A power cut inside an append under `OnCommit` leaves that append short or
/// torn, after earlier batches the group commit had not synced yet; recovery
/// repairs that segment's tail and keeps everything acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_power_cut_during_an_append_is_repaired() {
    for seed in 0..32 {
        let sim = Sim::new(2_000 + seed);
        let log = DiskLog::open(sim.log_dir(), KEY, config(FsyncMode::OnCommit)).expect("open");
        append(&log, 0, 20).await;
        let acknowledged = log.durable_offset();
        let torn = std::sync::Arc::new(std::sync::Mutex::new(None));
        let slot = std::sync::Arc::clone(&torn);
        sim_fs::arm_power_cut(&sim.0, seed, move |path, _| {
            *slot.lock().unwrap() = Some(path.to_path_buf());
        })
        .expect("arm");
        // The process outlives the cut here, so this append may even say it
        // was synced; the device kept what it held at the cut.
        let _ = log.append(&[record(20)]).await;
        assert!(
            sim_fs::reboot(&sim.0).expect("reboot"),
            "seed {seed}: no cut"
        );
        let torn = torn.lock().unwrap().clone().expect("the torn segment");
        let before = sim_fs::torn_tails_repaired(&sim.0).expect("repairs").len();
        let kept = recover(&sim.log_dir(), FsyncMode::OnCommit, acknowledged, 20).await;
        assert_eq!(kept, 20, "seed {seed}");
        let repaired = sim_fs::torn_tails_repaired(&sim.0).expect("repairs");
        assert_eq!(
            &repaired[before..],
            [torn],
            "seed {seed}: the torn append was not repaired"
        );
        log.shutdown().await.expect("shutdown the cut-off log");
    }
}

/// A segment recovery removes as unfinished counts as dealt with, as a
/// repaired one does: a power cut can leave the segment it tore that way.
#[tokio::test]
async fn a_segment_recovery_removes_is_recorded() {
    let sim = Sim::new(3_000);
    let dir = sim.log_dir();
    crate::fs::create_dir_all(&dir).expect("dir");
    let blank = dir.join(crate::segment::segment_file_name(0));
    crate::fs::write(
        &blank,
        vec![0u8; crate::segment::SEGMENT_HEADER_LEN as usize],
    )
    .expect("blank segment");
    let log = DiskLog::open(&dir, KEY, config(FsyncMode::OnCommit)).expect("open");
    let repaired = sim_fs::torn_tails_repaired(&sim.0).expect("repairs");
    assert_eq!(
        repaired,
        [PathBuf::from("log").join(crate::segment::segment_file_name(0))]
    );
    log.shutdown().await.expect("shutdown");
}
