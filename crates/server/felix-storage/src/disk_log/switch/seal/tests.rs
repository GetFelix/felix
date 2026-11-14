//! The seal on the hybrid side: the promise, the leader's claim and the
//! followers' acknowledgement, each refusal that keeps a copy out of the
//! base, and a crash between the claim's steps.

use std::path::Path;

use bytes::Bytes;
use tempfile::tempdir;

use super::*;
use crate::disk_log::switch::{RangeDigest, SideDir, start_voter};
use crate::log::{AppendRecord, LogConfig};
use crate::raft_log::{SnapshotId, StoredSnapshot};

const LABEL: &str = "t/ns/s/0";
const SEAL: u64 = 5;
const LEADER: &str = "a";

fn sealer() -> StorageId {
    StorageId::from_bytes([1; 16])
}

fn other() -> StorageId {
    StorageId::from_bytes([2; 16])
}

fn config(fsync_mode: FsyncMode) -> LogConfig {
    let config = LogConfig {
        fsync_mode,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    config.generations.open_when(|| true);
    config
}

fn on_commit() -> LogConfig {
    config(FsyncMode::OnCommit)
}

fn record(payload: &str) -> AppendRecord {
    AppendRecord {
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        timestamp_micros: 7,
        mark: RecordMark::None,
        publisher: None,
        headers: None,
    }
}

/// Append `payloads` at the tail as records generation 1 wrote.
async fn write(log: &DiskLog, payloads: &[&str]) {
    let tail = log.tail_offset().await.expect("tail");
    let writers = WrittenBy::Writers(vec![Epoch {
        generation: 1,
        start_offset: tail,
    }]);
    let records: Vec<_> = payloads.iter().map(|p| record(p)).collect();
    let pending = log
        .append_pending_at(tail, &records, writers)
        .await
        .expect("append")
        .expect("at the tail");
    log.commit(&pending).await.expect("commit");
}

/// A copy: the shard's log and its two side logs.
struct Copy {
    log: DiskLog,
    dead_letters: DiskLog,
    cursors: DiskLog,
}

impl Copy {
    fn open(dir: &Path, config: LogConfig) -> Self {
        let open =
            |kind: &str| DiskLog::open(&dir.join(kind), LABEL, config.clone()).expect("open");
        Self {
            log: open("log"),
            dead_letters: open("dead-letters"),
            cursors: open("cursors"),
        }
    }

    /// Written at generation 1 from `first` on.
    async fn with(dir: &Path, config: LogConfig, first: Offset, payloads: &[&str]) -> Self {
        let copy = Self::open(dir, config);
        if first > 0 {
            copy.log.reset_to(first).await.expect("reset");
        }
        write(&copy.log, payloads).await;
        write(&copy.dead_letters, &["dead"]).await;
        write(&copy.cursors, &["cursor"]).await;
        copy
    }

    async fn promise(&self, generation: u64, sealer: StorageId) -> GenerationCheck {
        self.log
            .promise_seal(generation, LEADER, sealer)
            .await
            .expect("promise")
    }

    async fn claim(&self, start: Offset) -> std::result::Result<SwitchBase, SealRefused> {
        self.log
            .claim_seal(sealer(), start, (&self.dead_letters, 0), (&self.cursors, 0))
            .await
            .expect("claim")
    }

    async fn ack(
        &self,
        seal: &LogRecord,
        sender: StorageId,
    ) -> std::result::Result<(), SealRefused> {
        self.log
            .ack_seal(seal, sender, &self.dead_letters, &self.cursors)
            .await
            .expect("ack")
    }

    async fn record_at(&self, offset: Offset) -> LogRecord {
        self.log
            .read_range_with_trims(ReadRange {
                start: offset,
                max_bytes: 1,
            })
            .await
            .expect("read")
            .remove(0)
    }

    async fn shutdown(self) {
        for log in [self.log, self.dead_letters, self.cursors] {
            log.shutdown().await.expect("shutdown");
        }
    }
}

/// A leader and a follower holding `a b c d`, both promised to the seal, and
/// the leader's claimed seal record.
async fn sealed(root: &Path) -> (Copy, Copy, LogRecord) {
    let leader = Copy::with(&root.join("leader"), on_commit(), 0, &["a", "b", "c", "d"]).await;
    let follower = Copy::with(
        &root.join("follower"),
        on_commit(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    for copy in [&leader, &follower] {
        assert_eq!(copy.promise(SEAL, sealer()).await, GenerationCheck::Raised);
    }
    let base = leader.claim(0).await.expect("claimed");
    let seal = leader.record_at(base.offset).await;
    (leader, follower, seal)
}

#[tokio::test]
async fn a_follower_that_holds_the_leaders_records_acknowledges_and_starts_as_a_voter() {
    let root = tempdir().expect("tempdir");
    let (leader, follower, seal) = sealed(root.path()).await;
    let base = SwitchBase::from_record(&seal).expect("a seal record");
    assert_eq!((base.generation, base.offset, base.start), (SEAL, 4, 0));
    assert!(leader.log.durable_offset() > base.offset);

    assert_eq!(follower.ack(&seal, sealer()).await, Ok(()));
    // On disk before the acknowledgement, and the same record the leader
    // holds, header generation included.
    assert!(follower.log.durable_offset() > base.offset);
    let held = follower.record_at(base.offset).await;
    assert_eq!(held.payload, seal.payload);
    assert_eq!(held.generation, Some(SEAL));
    // A retried acknowledgement answers again and writes nothing.
    assert_eq!(follower.ack(&seal, sealer()).await, Ok(()));
    assert_eq!(
        follower.log.tail_offset().await.expect("tail"),
        base.offset + 1
    );

    // Both copies hold the base the record names, so both can vote.
    let dir = root.path().join("follower");
    follower.shutdown().await;
    leader.shutdown().await;
    let side = |kind: &str| SideDir {
        dir: Box::leak(dir.join(kind).into_boxed_path()),
        label: LABEL,
        config: on_commit(),
    };
    let started = start_voter(
        &dir.join("log"),
        LABEL,
        on_commit(),
        &base,
        [side("dead-letters"), side("cursors")],
        "b#1",
        StoredSnapshot {
            last: Some(SnapshotId {
                term: base.term().expect("term"),
                index: base.offset,
            }),
            meta: Bytes::from_static(b"{}"),
            data: Bytes::new(),
        },
    )
    .await
    .expect("start");
    assert_eq!(started, Ok(()));
}

#[tokio::test]
async fn a_follower_whose_records_differ_does_not_acknowledge() {
    let root = tempdir().expect("tempdir");
    let leader = Copy::with(
        &root.path().join("leader"),
        on_commit(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    let follower = Copy::with(
        &root.path().join("follower"),
        on_commit(),
        0,
        &["a", "x", "c", "d"],
    )
    .await;
    leader.promise(SEAL, sealer()).await;
    follower.promise(SEAL, sealer()).await;
    let base = leader.claim(0).await.expect("claimed");
    let seal = leader.record_at(base.offset).await;
    assert_eq!(
        follower.ack(&seal, sealer()).await,
        Err(SealRefused::DigestDiffers)
    );
    assert_eq!(follower.log.tail_offset().await.expect("tail"), base.offset);
}

#[tokio::test]
async fn a_follower_that_starts_above_the_seals_start_does_not_acknowledge() {
    let root = tempdir().expect("tempdir");
    let leader = Copy::with(
        &root.path().join("leader"),
        on_commit(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    // Its own records match the leader's from where it starts, but it lacks
    // the ones below that the seal's start covers.
    let follower = Copy::with(&root.path().join("follower"), on_commit(), 2, &["c", "d"]).await;
    leader.promise(SEAL, sealer()).await;
    follower.promise(SEAL, sealer()).await;
    let base = leader.claim(0).await.expect("claimed");
    let seal = leader.record_at(base.offset).await;
    assert_eq!(
        follower.ack(&seal, sealer()).await,
        Err(SealRefused::StartsAbove { first: 2, start: 0 })
    );
}

#[tokio::test]
async fn a_follower_whose_side_logs_differ_does_not_acknowledge() {
    // A dead letter the leader's log does not hold.
    let root = tempdir().expect("tempdir");
    let (_leader, follower, seal) = sealed(root.path()).await;
    write(&follower.dead_letters, &["extra"]).await;
    assert_eq!(
        follower.ack(&seal, sealer()).await,
        Err(SealRefused::SideDiffers {
            label: LABEL.to_string()
        })
    );

    // A cursor that differs in content.
    let root = tempdir().expect("tempdir");
    let leader = Copy::with(&root.path().join("leader"), on_commit(), 0, &["a"]).await;
    let follower = Copy::open(&root.path().join("follower"), on_commit());
    write(&follower.log, &["a"]).await;
    write(&follower.dead_letters, &["dead"]).await;
    write(&follower.cursors, &["moved"]).await;
    leader.promise(SEAL, sealer()).await;
    follower.promise(SEAL, sealer()).await;
    let base = leader.claim(0).await.expect("claimed");
    let seal = leader.record_at(base.offset).await;
    assert!(matches!(
        follower.ack(&seal, sealer()).await,
        Err(SealRefused::SideDiffers { .. })
    ));
}

#[tokio::test]
async fn a_follower_that_lags_or_runs_ahead_does_not_acknowledge() {
    let root = tempdir().expect("tempdir");
    let leader = Copy::with(
        &root.path().join("leader"),
        on_commit(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    let lags = Copy::with(&root.path().join("lags"), on_commit(), 0, &["a", "b", "c"]).await;
    let ahead = Copy::with(
        &root.path().join("ahead"),
        on_commit(),
        0,
        &["a", "b", "c", "d", "e"],
    )
    .await;
    for copy in [&leader, &lags, &ahead] {
        copy.promise(SEAL, sealer()).await;
    }
    let base = leader.claim(0).await.expect("claimed");
    let seal = leader.record_at(base.offset).await;
    assert_eq!(
        lags.ack(&seal, sealer()).await,
        Err(SealRefused::EndsAt { tail: 3, offset: 4 })
    );
    assert_eq!(
        ahead.ack(&seal, sealer()).await,
        Err(SealRefused::EndsAt { tail: 5, offset: 4 })
    );
}

#[tokio::test]
async fn only_the_sealing_copy_is_acknowledged() {
    let root = tempdir().expect("tempdir");
    let (_leader, follower, seal) = sealed(root.path()).await;
    // The same leader under a new copy id, as after a wipe.
    assert_eq!(
        follower.ack(&seal, other()).await,
        Err(SealRefused::PromisedOther {
            generation: SEAL,
            sealer: sealer()
        })
    );
    // And a copy that never took the seal's fence acknowledges nothing.
    let unsealed = Copy::with(
        &root.path().join("unsealed"),
        on_commit(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    assert_eq!(
        unsealed.ack(&seal, sealer()).await,
        Err(SealRefused::NoPromise)
    );
    let promise = follower.log.seal_promise().expect("promise");
    assert!(promise.admits_batch(sealer()));
    assert!(!promise.admits_batch(other()));
}

#[tokio::test]
async fn a_copy_that_is_not_fsync_on_commit_takes_no_part() {
    let root = tempdir().expect("tempdir");
    let periodic = config(FsyncMode::Periodic {
        interval: std::time::Duration::from_secs(3600),
    });
    let leader = Copy::with(
        &root.path().join("leader"),
        on_commit(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    let follower = Copy::with(
        &root.path().join("follower"),
        periodic.clone(),
        0,
        &["a", "b", "c", "d"],
    )
    .await;
    follower.log.sync().await.expect("sync");
    leader.promise(SEAL, sealer()).await;
    follower.promise(SEAL, sealer()).await;
    let base = leader.claim(0).await.expect("claimed");
    let seal = leader.record_at(base.offset).await;
    assert!(matches!(
        follower.ack(&seal, sealer()).await,
        Err(SealRefused::NotOnCommit { .. })
    ));

    let periodic_leader =
        Copy::with(&root.path().join("periodic-leader"), periodic, 0, &["a"]).await;
    periodic_leader.promise(SEAL, sealer()).await;
    assert!(matches!(
        periodic_leader.claim(0).await,
        Err(SealRefused::NotOnCommit { .. })
    ));
}

#[tokio::test]
async fn the_promise_survives_a_restart_and_only_a_later_generation_ends_it() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("copy");
    let copy = Copy::with(&dir, on_commit(), 0, &["a"]).await;
    copy.promise(SEAL, sealer()).await;
    copy.shutdown().await;

    let copy = Copy::open(&dir, on_commit());
    let promise = SealPromise {
        generation: SEAL,
        sealer: sealer(),
        claimed: false,
    };
    assert_eq!(copy.log.seal_promise(), Some(promise));
    // A promise to an older seal, or to another leader at this one, is
    // refused and writes nothing.
    assert_eq!(
        copy.promise(SEAL - 1, other()).await,
        GenerationCheck::Superseded { accepted: SEAL }
    );
    assert_eq!(
        copy.log
            .promise_seal(SEAL, "b", other())
            .await
            .expect("promise"),
        GenerationCheck::Promised {
            leader: LEADER.to_string()
        }
    );
    assert_eq!(copy.log.seal_promise(), Some(promise));
    // An unsealed promotion at a later generation reopens the shard.
    copy.log
        .accept_generation(SEAL + 1, Some("b"))
        .await
        .expect("accept");
    assert_eq!(copy.log.seal_promise(), None);
    copy.shutdown().await;
    assert_eq!(Copy::open(&dir, on_commit()).log.seal_promise(), None);
}

#[tokio::test]
async fn a_sealer_claims_its_record_once_per_generation() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("leader");
    let leader = Copy::with(&dir, on_commit(), 0, &["a", "b"]).await;
    leader.promise(SEAL, sealer()).await;
    assert_eq!(leader.claim(0).await.map(|base| base.offset), Ok(2));
    assert_eq!(
        leader.claim(0).await,
        Err(SealRefused::Claimed { generation: SEAL })
    );
    leader.shutdown().await;
    // Nor after a restart, whatever its tail kept.
    let leader = Copy::open(&dir, on_commit());
    assert_eq!(
        leader.claim(0).await,
        Err(SealRefused::Claimed { generation: SEAL })
    );
    // A copy that did not seal claims nothing.
    let follower = Copy::with(&root.path().join("follower"), on_commit(), 0, &["a"]).await;
    follower.promise(SEAL, other()).await;
    assert!(matches!(
        follower.claim(0).await,
        Err(SealRefused::PromisedOther { .. })
    ));
}

#[tokio::test]
async fn a_crash_after_the_claims_mark_leaves_no_second_record_at_the_generation() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("leader");
    let leader = Copy::with(&dir, on_commit(), 0, &["a", "b"]).await;
    leader.promise(SEAL, sealer()).await;
    CRASH_AFTER.with(|at| at.set(1));
    let crashed = leader
        .log
        .claim_seal(sealer(), 0, (&leader.dead_letters, 0), (&leader.cursors, 0))
        .await;
    CRASH_AFTER.with(|at| at.set(0));
    assert!(crashed.is_err());
    assert_eq!(leader.log.tail_offset().await.expect("tail"), 2);
    leader.shutdown().await;

    let leader = Copy::open(&dir, on_commit());
    assert_eq!(
        leader.log.seal_promise().map(|promise| promise.claimed),
        Some(true)
    );
    assert_eq!(
        leader.claim(0).await,
        Err(SealRefused::Claimed { generation: SEAL })
    );
}

#[test]
fn a_damaged_promise_does_not_decode() {
    let promise = SealPromise {
        generation: SEAL,
        sealer: sealer(),
        claimed: true,
    };
    let bytes = promise.encode();
    assert_eq!(SealPromise::decode(&bytes), Some(promise));
    for at in 0..bytes.len() {
        let mut damaged = bytes;
        damaged[at] ^= 0x40;
        assert_eq!(SealPromise::decode(&damaged), None, "byte {at}");
    }
}

#[test]
fn a_seal_record_names_the_base() {
    let base = SwitchBase {
        generation: SEAL,
        offset: 9,
        start: 3,
        digest: RangeDigest([4; 32]),
        dead_letters: SideRange {
            start: 1,
            end: 2,
            digest: RangeDigest([5; 32]),
        },
        cursors: SideRange {
            start: 6,
            end: 7,
            digest: RangeDigest([8; 32]),
        },
    };
    let appended = base.record(11);
    let record = LogRecord {
        offset: 9,
        timestamp_micros: 11,
        checksum: 0,
        payload: appended.payload.clone(),
        mark: appended.mark,
        publisher: None,
        headers: None,
        generation: Some(SEAL),
    };
    assert_eq!(SwitchBase::from_record(&record), Some(base));
    // Its payload opens with the generation, as every start record's does.
    assert_eq!(&record.payload[..8], &SEAL.to_be_bytes());
    // An ordinary start record, or one stamped with another generation, is
    // not a seal.
    let plain = LogRecord {
        payload: Bytes::copy_from_slice(&SEAL.to_be_bytes()),
        ..record.clone()
    };
    assert_eq!(SwitchBase::from_record(&plain), None);
    let restamped = LogRecord {
        generation: Some(SEAL + 1),
        ..record
    };
    assert_eq!(SwitchBase::from_record(&restamped), None);
}
