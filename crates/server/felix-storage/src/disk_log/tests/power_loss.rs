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
use std::sync::Arc;

use super::*;

use crate::io::power_loss::{PowerLoss, SplitMix64, Writeback};
use crate::log::SegmentId;

/// Crashes built from each workload checkpoint.
const CRASHES_PER_CHECKPOINT: u64 = 6;
/// Appends between checkpoints.
const STEPS_PER_CHECKPOINT: u64 = 7;
const CHECKPOINTS: u64 = 12;
/// Workload seeds per test, from `FELIX_POWER_LOSS_SEED` upwards. Eight keeps
/// the suite to a few seconds; `FELIX_POWER_LOSS_SEEDS` runs more, and the
/// nightly `power-loss-nightly.yml` sweep does.
const DEFAULT_SEEDS: u64 = 8;
/// The first workload seed: the one CI failed on, crash seed `0x5_5eed_0001`
/// (checkpoint 5, trial 0), when a lost-race preparation came back mid-chain.
const BASE_SEED: u64 = 0x5eed_0001;
/// Seeds every run covers on top of the range, each one known to catch a bug
/// the default range misses. `0x5eed_0009` is the first seed that fails with
/// the directory sync after writing `durable.mark` removed.
const PINNED_SEEDS: &[u64] = &[0x5eed_0009];

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
    let range = base..base.saturating_add(count);
    let pinned = PINNED_SEEDS
        .iter()
        .copied()
        .filter(|seed| !range.contains(seed));
    for seed in range.clone().chain(pinned) {
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

/// A log whose next background roll is parked before it seals the segment it
/// retired, so a crash can be taken inside that window every time.
struct ParkedSeal {
    _dir: tempfile::TempDir,
    observer: Arc<PowerLoss>,
    log: DiskLog,
    config: LogConfig,
    scenario: Scenario,
    /// Sending, or dropping it (a failed assertion does), lets the seal go.
    release: std::sync::mpsc::Sender<()>,
    next: Offset,
}

impl ParkedSeal {
    /// No flush but the one explicit sync, so the retired segment keeps a
    /// synced prefix and an unsynced tail until its seal runs.
    async fn open() -> Self {
        let scenario = Scenario {
            fsync_mode: FsyncMode::None,
            background_roll: true,
            writeback: Writeback::InOrder,
            seed: BASE_SEED,
        };
        let config = scenario.config();
        let dir = tempdir().expect("dir");
        let root = dir.path().join("log");
        std::fs::create_dir_all(&root).expect("log dir");
        let observer = PowerLoss::install(&root).expect("install the power-loss observer");
        let log = DiskLog::open(&root, "t/ns/s/0", config.clone()).expect("open");
        let (release, held) = std::sync::mpsc::channel();
        *log.inner.hold_next_seal.lock() = Some(held);
        let mut this = Self {
            _dir: dir,
            observer,
            log,
            config,
            scenario,
            release,
            next: 0,
        };

        while this.log.inner.roll_state.load(Ordering::Acquire) == RollState::Idle as u8 {
            this.append().await;
            if this.next == 4 {
                this.log.sync().await.expect("sync");
            }
        }
        // The roll has started; with nothing appending, it installs its
        // segment and reaches the seal.
        for _ in 0..5_000 {
            if this.log.inner.hold_next_seal.lock().is_none()
                && this.log.inner.roll_state.load(Ordering::Acquire) == RollState::Sealing as u8
            {
                return this;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the background roll never reached its seal");
    }

    async fn append(&mut self) {
        let record = AppendRecord {
            payload: Bytes::from(payload(self.scenario.seed, self.next)),
            timestamp_micros: 1_700_000_000 + self.next,
            mark: Default::default(),
        };
        self.log.append(&[record]).await.expect("append");
        self.next += 1;
    }

    /// Crash with every seed in `seeds` and check what each recovers to.
    /// Returns how many images match `shape`.
    async fn crash_all(&self, seeds: std::ops::Range<u64>, shape: impl Fn(&Path) -> bool) -> usize {
        let acknowledged = self.log.durable_offset();
        let mut matched = 0;
        for seed in seeds {
            let image = tempdir().expect("image dir");
            self.observer
                .crash(seed, self.scenario.writeback, image.path())
                .expect("build the crash image");
            if shape(image.path()) {
                matched += 1;
            }
            verify(
                image.path(),
                &self.config,
                self.scenario,
                seed,
                acknowledged,
                self.next,
            )
            .await;
        }
        matched
    }

    async fn finish(self) {
        self.release.send(()).expect("release the seal");
        self.log.shutdown().await.expect("shutdown");
    }
}

/// The CI failure, pinned: power goes while a background roll is sealing.
/// The retired segment's unsynced tail and the new segment's records reach
/// the device independently, so a crash can keep the new segment and cut the
/// old one back to its last sync -- an offset gap between them. Nothing past
/// that sync was ever reported durable, so recovery must take the log back to
/// it rather than refuse.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_while_sealing_can_cut_the_retired_segment_short() {
    let mut parked = ParkedSeal::open().await;
    let segments = parked.log.segments();
    let [.., retired, active] = segments.as_slice() else {
        panic!("the roll installed no segment");
    };
    let (retired, active, active_base) = (retired.id, active.id, active.base_offset);
    for _ in 0..3 {
        parked.append().await;
    }
    assert!(
        parked.log.durable_offset() < active_base,
        "the retired segment's tail must be unsynced"
    );

    let config = parked.config.clone();
    let cut_short = |image: &Path| retired_ends_before(image, &config, retired, active);
    let matched = parked.crash_all(0..64, cut_short).await;
    assert!(matched > 0, "no crash image cut the retired segment short");
    parked.finish().await;
}

/// The segment the parked roll retired and the one it installed.
fn retired_and_installed(log: &DiskLog) -> (SegmentId, SegmentId) {
    let segments = log.segments();
    let [.., retired, installed] = segments.as_slice() else {
        panic!("the roll installed no segment");
    };
    (retired.id, installed.id)
}

/// An inline roll while a background seal is parked seals the newer segment.
/// It has to sync the retired one first: otherwise a crash can keep the newer
/// segment whole and cut the older one short, and a gap in front of a sealed
/// segment is indistinguishable from lost records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inline_roll_while_sealing_syncs_the_retired_segment_first() {
    let mut parked = ParkedSeal::open().await;
    let (retired, installed) = retired_and_installed(&parked.log);
    // Past the overshoot ceiling, so the next append has to roll inline.
    while parked.log.segments().len() == 2 {
        parked.append().await;
    }

    let config = parked.config.clone();
    let cut_short = |image: &Path| retired_ends_before(image, &config, retired, installed);
    let matched = parked.crash_all(0..64, cut_short).await;
    assert_eq!(
        matched, 0,
        "a crash kept the sealed segment and cut the one before it"
    );
    parked.finish().await;
}

/// `seal` seals the active segment too, so it owes the same sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealing_while_a_background_seal_is_parked_syncs_the_retired_segment_first() {
    let mut parked = ParkedSeal::open().await;
    let (retired, installed) = retired_and_installed(&parked.log);
    for _ in 0..3 {
        parked.append().await;
    }
    parked.log.seal().await.expect("seal");

    let config = parked.config.clone();
    let cut_short = |image: &Path| retired_ends_before(image, &config, retired, installed);
    let matched = parked.crash_all(0..64, cut_short).await;
    assert_eq!(
        matched, 0,
        "a crash kept the sealed segment and cut the one before it"
    );
    parked.finish().await;
}

/// While a background seal runs, the segment it installed may overshoot its
/// size up to the ceiling. Rolling at the plain size instead would seal the
/// new segment ahead of the retired one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_overshoots_rather_than_rolls_while_a_seal_is_parked() {
    let mut parked = ParkedSeal::open().await;
    let size = parked.config.segment_size_bytes;
    loop {
        let segments = parked.log.segments();
        assert_eq!(
            segments.len(),
            2,
            "rolled inline below the overshoot ceiling"
        );
        if segments[1].size_bytes > size + 1_000 {
            break;
        }
        parked.append().await;
    }
    parked.finish().await;
}

/// Whether segment `retired` in `image` is intact but ends before segment
/// `active`, which holds records, begins.
fn retired_ends_before(
    image: &Path,
    config: &LogConfig,
    retired: SegmentId,
    active: SegmentId,
) -> bool {
    use crate::segment::{ScanStart, scan_segment, segment_file_name};
    let scan = |id| {
        scan_segment(
            &image.join(segment_file_name(id)),
            id,
            "t/ns/s/0",
            config.index_spacing_bytes,
            ScanStart::Full,
            false,
        )
    };
    match (scan(retired), scan(active)) {
        (Ok(retired), Ok(active)) => {
            retired.torn_tail.is_none()
                && active.record_count > 0
                && active.header.base_offset > retired.next_offset
        }
        _ => false,
    }
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

/// Retention's unlinks survive a power loss as a prefix of the chain. If the
/// device could keep a newer unlink and lose an older one, the recovered log
/// would have a gap, which recovery refuses as corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_power_loss_after_retention_leaves_no_gap() {
    let dir = tempdir().expect("dir");
    let root = dir.path().join("log");
    std::fs::create_dir_all(&root).expect("log dir");
    let observer = PowerLoss::install(&root).expect("install the power-loss observer");
    let config = LogConfig {
        retention_bytes: Some(crate::segment::SEGMENT_HEADER_LEN + 120),
        retention_check_interval: Duration::from_secs(3600),
        ..super::config(FsyncMode::None)
    };
    let log = DiskLog::open(&root, "t/ns/s/0", config.clone()).expect("open");
    let payloads: Vec<String> = (0..24).map(|i| format!("record-{i:02}")).collect();
    for payload in &payloads {
        log.append(&records(&[payload])).await.expect("append");
    }
    log.sync().await.expect("make every record durable");

    let outcome = log.enforce_retention_now().await.expect("retention");
    assert!(
        outcome.segments_deleted >= 4,
        "too few segments deleted to leave a gap: {outcome:?}"
    );
    let trimmed_to = log.base_offset();

    for seed in 0x7e7e_0000..0x7e7e_0020u64 {
        for writeback in [Writeback::AnySubset, Writeback::InOrder] {
            let image = tempdir().expect("image dir");
            observer
                .crash(seed, writeback, image.path())
                .expect("build the crash image");
            let recovered = DiskLog::open(image.path(), "t/ns/s/0", config.clone())
                .unwrap_or_else(|err| panic!("seed {seed:#x} ({writeback:?}): {err}"));
            let base = recovered.base_offset();
            assert!(
                base <= trimmed_to,
                "seed {seed:#x}: base {base} is past retention's {trimmed_to}"
            );
            assert_eq!(
                read_all(&recovered, base).await,
                payloads[base as usize..],
                "seed {seed:#x} ({writeback:?}): records lost above the base",
            );
            recovered.shutdown().await.expect("shutdown");
        }
    }
    log.shutdown().await.expect("shutdown");
}

/// Crash images built at each stop of a truncation or reset.
const IMAGES_PER_STOP: u64 = 8;

/// A log of 24 records in small segments under `FsyncMode::None`, made
/// durable, and watched for a power loss. Returns what was written.
async fn watched_log(root: &Path, config: &LogConfig) -> (Arc<PowerLoss>, DiskLog, Vec<String>) {
    std::fs::create_dir_all(root).expect("log dir");
    let observer = PowerLoss::install(root).expect("install the power-loss observer");
    let log = DiskLog::open(root, "t/ns/s/0", config.clone()).expect("open");
    let payloads: Vec<String> = (0..24).map(|i| format!("record-{i:02}")).collect();
    for payload in &payloads {
        log.append(&records(&[payload])).await.expect("append");
    }
    log.sync().await.expect("make every record durable");
    (observer, log, payloads)
}

/// Every crash image the observer can build now opens, and passes `check`.
async fn check_images<F>(observer: &PowerLoss, config: &LogConfig, seed: &mut u64, check: F)
where
    F: AsyncFn(&DiskLog, &str),
{
    for _ in 0..IMAGES_PER_STOP {
        *seed += 1;
        for writeback in [Writeback::AnySubset, Writeback::InOrder] {
            let image = tempdir().expect("image dir");
            observer
                .crash(*seed, writeback, image.path())
                .expect("build the crash image");
            let when = format!("seed {:#x} ({writeback:?})", *seed);
            let recovered = DiskLog::open(image.path(), "t/ns/s/0", config.clone())
                .unwrap_or_else(|err| panic!("{when}: {err}"));
            check(&recovered, &when).await;
            recovered.shutdown().await.expect("shutdown");
        }
    }
}

/// A truncation that drops several whole segments leaves, after a power loss
/// at any point in it, a longer log rather than a gap. Truncation removes a
/// suffix, so its unlinks go newest first, each synced: an older unlink that
/// stuck while a newer one was undone would leave survivors that do not meet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_power_loss_during_truncation_leaves_no_gap() {
    let config = config(FsyncMode::None);
    let cut = 3;
    let mut seed = 0x7a0c_0000u64;
    let mut stops = 0..;
    loop {
        let stop = stops.next().expect("unbounded");
        let dir = tempdir().expect("dir");
        let root = dir.path().join("log");
        let (observer, log, payloads) = watched_log(&root, &config).await;
        let segments = log.segments().len();
        assert!(segments >= 5, "only {segments} segments; too few for a gap");

        crate::disk_log::segments::stop_after_unlinks(&root, stop);
        let finished = log.truncate(cut).await.is_ok();
        check_images(&observer, &config, &mut seed, async |recovered, when| {
            assert_eq!(recovered.base_offset(), 0, "{when}: the head moved");
            let tail = recovered.tail_offset().await.expect("tail");
            assert!(tail >= cut, "{when}: cut to {tail}, below {cut}");
            assert_eq!(
                read_all(recovered, 0).await,
                payloads[..tail as usize],
                "{when}, stopped after {stop} unlinks: not a prefix of what was written",
            );
        })
        .await;
        log.shutdown().await.ok();
        if finished {
            assert!(
                stop >= 4,
                "the truncation finished after only {stop} unlinks"
            );
            break;
        }
    }
}

/// A reset discards the whole log. After a power loss at any point in it the
/// directory holds a prefix of the old log or the new empty one, never old
/// segments beside the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_power_loss_during_a_reset_leaves_old_or_new() {
    let config = config(FsyncMode::None);
    let new_base = 1_000;
    let mut seed = 0x7a0d_0000u64;
    let mut stops = 0..;
    loop {
        let stop = stops.next().expect("unbounded");
        let dir = tempdir().expect("dir");
        let root = dir.path().join("log");
        let (observer, log, payloads) = watched_log(&root, &config).await;

        crate::disk_log::segments::stop_after_unlinks(&root, stop);
        let finished = log.reset_to(new_base).await.is_ok();
        check_images(&observer, &config, &mut seed, async |recovered, when| {
            let base = recovered.base_offset();
            let tail = recovered.tail_offset().await.expect("tail");
            if base == new_base {
                assert_eq!(tail, new_base, "{when}: records in the new log");
            } else {
                assert_eq!(base, 0, "{when}: neither the old base nor the new");
                assert_eq!(
                    read_all(recovered, 0).await,
                    payloads[..tail as usize],
                    "{when}, stopped after {stop} unlinks: not a prefix of the old log",
                );
            }
        })
        .await;
        log.shutdown().await.ok();
        if finished {
            break;
        }
    }
}
