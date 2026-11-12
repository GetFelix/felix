//! Power loss at every step of each write to a metadata store, on the
//! simulator's filesystem.
//!
//! A write is stopped before each step of its state-file writes (in the
//! store, in the temporary a new store is built in, and in the log
//! directory) and after each unlink, then the power is cut. What is left
//! must open as a store, with the vote not lower than it was, the purged id
//! not past the stored snapshot, no gap above it, and every flushed entry
//! above the snapshot still there. It then takes the next entry.
//!
//! Built only with the `sim` feature; CI runs it in the coverage job
//! (`--all-features`).

use std::future::Future;

use openraft::type_config::alias::VoteOf;

use super::*;
use crate::log::LogConfig;
use crate::metadata_store::MetadataStore;
use crate::raft_log::tests::writes::{append, append_flushed};
use crate::raft_log::tests::{
    Codec, LogReads, LogWrites, TestEntry, Types, all, blank, log_id, normal,
};
use crate::raft_log::{RaftLog, SnapshotId, StoredSnapshot};
use crate::sim_fs;

pub(super) type Store = MetadataStore<Types, Codec>;

const SEEDS: u64 = 3;
/// More steps than any one sequence here has.
const MOST_STEPS: usize = 64;

pub(super) fn config() -> LogConfig {
    LogConfig {
        // A few pages per segment, so a purge and a cut have files to unlink.
        segment_size_bytes: 2 * 4096 + 300,
        index_spacing_bytes: 512,
        preallocate_segments: true,
        ..LogConfig::default()
    }
}

/// The entry at `index` as the first leader wrote it: ten to a term.
pub(super) fn entry(index: u64) -> TestEntry {
    entry_of(1 + index / 10, index)
}

/// The entry at `index` as a leader of `term` wrote it.
pub(super) fn entry_of(term: u64, index: u64) -> TestEntry {
    match index % 10 {
        0 => blank(term, index),
        _ => normal(
            term,
            index,
            &format!(
                "{term}:{index}:{}",
                "x".repeat(1 + (index as usize * 97) % 600)
            ),
        ),
    }
}

fn entries(range: std::ops::Range<u64>) -> Vec<TestEntry> {
    range.map(entry).collect()
}

pub(super) fn snapshot(term: u64, index: u64) -> StoredSnapshot {
    StoredSnapshot {
        last: Some(SnapshotId { term, index }),
        meta: bytes::Bytes::from_static(b"meta"),
        data: bytes::Bytes::from(format!("state through {index}").repeat(40)),
    }
}

/// A data directory on the simulator's filesystem.
pub(super) struct Sim(pub(super) PathBuf);

impl Sim {
    pub(super) fn new(name: &str, seed: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "felix-simfs-store-{}-{name}-{seed}",
            std::process::id()
        ));
        sim_fs::register_root(&root);
        Self(root)
    }

    pub(super) async fn open(&self) -> io::Result<Store> {
        Store::open_configured(
            self.0.clone(),
            CLUSTER,
            Codec,
            "meta/raft".to_string(),
            config(),
        )
        .await
    }

    fn dir(&self, of: Dir) -> PathBuf {
        match of {
            Dir::Store => self.0.join(STORE_DIR),
            Dir::Building => self.0.join(format!("{STORE_DIR}.tmp")),
            Dir::Log => self.0.join(STORE_DIR).join(LOG_DIR),
        }
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.0);
    }
}

/// A directory whose state-file writes are counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dir {
    Store,
    /// `store.tmp`, where a new store is built.
    Building,
    Log,
}

/// Where a sequence is stopped.
#[derive(Clone, Copy, Debug)]
enum Stop {
    /// Before this step of the state-file writes in one directory.
    FileStep(Dir, usize),
    /// After this many unlinks in the log.
    Unlink(usize),
}

impl Stop {
    fn arm(self, sim: &Sim, log: Option<&RaftLog<Types, Codec>>) {
        match (self, log) {
            (Self::FileStep(dir, step), _) => crash_point::watch(&sim.dir(dir), Some(step)),
            (Self::Unlink(unlinks), Some(log)) => log.disk_log().stop_after_unlinks(unlinks),
            (Self::Unlink(_), None) => {}
        }
    }

    fn disarm(self, sim: &Sim) {
        if let Self::FileStep(dir, _) = self {
            crash_point::unwatch(&sim.dir(dir));
        }
    }
}

/// Every stop of each kind, until `run` reports that its sequence ran to
/// the end without reaching the stop it was given. Returns how many stops
/// of each kind were reached.
async fn at_every_stop<F>(mut run: impl FnMut(Stop, u64) -> F) -> [usize; 4]
where
    F: Future<Output = bool>,
{
    let kinds: [fn(usize) -> Stop; 4] = [
        |step| Stop::FileStep(Dir::Store, step),
        |step| Stop::FileStep(Dir::Building, step),
        |step| Stop::FileStep(Dir::Log, step),
        Stop::Unlink,
    ];
    let mut reached = [0; 4];
    for (kind, stop_at) in kinds.into_iter().enumerate() {
        for step in 0..MOST_STEPS {
            let mut finished = false;
            for seed in 0..SEEDS {
                finished = run(stop_at(step), seed).await;
            }
            if finished {
                break;
            }
            reached[kind] += 1;
            assert!(step + 1 < MOST_STEPS, "never ran out of steps");
        }
    }
    reached
}

/// What must hold of the store a cut left.
#[derive(Default)]
struct Kept {
    /// The vote on disk before the sequence.
    vote: Option<VoteOf<Types>>,
    /// The snapshot stored before the sequence.
    snapshot: Option<u64>,
    /// Entries whose flush was confirmed. Those above the snapshot found
    /// must still be in the log.
    flushed: Vec<TestEntry>,
    /// Entries that may be found beside the flushed ones.
    possible: Vec<TestEntry>,
}

/// Open what the cut left and check it against `kept`. Returns the store
/// and its entries.
async fn reopen(sim: &Sim, kept: &Kept, at: &str) -> (Store, Vec<TestEntry>) {
    let store = sim.open().await.unwrap_or_else(|err| panic!("{at}: {err}"));
    let mut log = store.log();
    let vote = store.reader().read_vote().await.expect("vote");
    assert!(vote >= kept.vote, "{at}: the vote went down to {vote:?}");
    let covered = store.snapshots().covers().map(|last| last.index);
    assert!(covered >= kept.snapshot, "{at}: the snapshot went back");
    let state = log.log_state().await.expect("state");
    let purged = state.last_purged_log_id.map(|id| id.index);
    assert!(
        purged.is_none() || purged <= covered,
        "{at}: the log starts after its snapshot: purged {purged:?}, snapshot {covered:?}"
    );
    let found = all(&log).await.expect("read");
    let first = purged.map_or(0, |purged| purged + 1);
    for (at_index, entry) in (first..).zip(&found) {
        assert_eq!(entry.log_id.index, at_index, "{at}: a gap");
        assert!(
            kept.flushed.contains(entry) || kept.possible.contains(entry),
            "{at}: {} was never written",
            entry.log_id
        );
    }
    for entry in &kept.flushed {
        if Some(entry.log_id.index) > covered {
            assert!(
                found.contains(entry),
                "{at}: flushed entry {} is gone",
                entry.log_id
            );
        }
    }
    // The committed id names an entry that is here, or the purged id.
    let committed = log.read_committed().await.expect("committed");
    assert!(
        committed.map(|id| id.index) <= state.last_log_id.map(|id| id.index),
        "{at}: committed {committed:?} past the log"
    );
    (store, found)
}

/// The store takes the entry after its last, and still has it after a
/// clean restart.
async fn takes_the_next(sim: &Sim, store: Store, at: &str) {
    let mut log = store.log();
    let last = log.log_state().await.expect("state").last_log_id;
    let next = normal(1_000, last.map_or(0, |id| id.index + 1), "after the cut");
    append_flushed(&mut log, std::slice::from_ref(&next)).await;
    store.shutdown().await.expect("shutdown");
    let store = sim.open().await.unwrap_or_else(|err| panic!("{at}: {err}"));
    assert_eq!(
        all(&store.log()).await.expect("read").last(),
        Some(&next),
        "{at}"
    );
    store.shutdown().await.expect("shutdown");
}

/// Let a committed id that is being written behind its call reach the
/// disk, or fail at the stop. The process a cut kills writes nothing
/// afterwards; in a test its tasks live on, and a late write would land on
/// the store that reopened.
pub(super) async fn settle(log: &RaftLog<Types, Codec>) {
    let persisted = log.disk_log().commit_offset_persisted();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), persisted).await;
}

fn vote(term: u64, node: u64) -> VoteOf<Types> {
    VoteOf::<Types>::new(term, node)
}

/// A new member's first start: the store is built, then started. Stopped at
/// every step, in `store.tmp` too. What is left is no store or a whole one,
/// and a store whose open returned is still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_while_a_store_is_made_leaves_none_or_a_whole_one() {
    let reached = at_every_stop(|stop, seed| async move {
        let sim = Sim::new(&format!("create-{stop:?}"), seed);
        let at = format!("{stop:?}, seed {seed}");
        stop.arm(&sim, None);
        let mut opened = false;
        let started = async {
            let store = sim.open().await?;
            opened = true;
            store.mark_started().await?;
            store.shutdown().await
        }
        .await;
        stop.disarm(&sim);
        sim_fs::crash(&sim.0, seed).expect("crash");

        match decide_start(&sim.0).unwrap_or_else(|err| panic!("{at}: {err}")) {
            Start::New => assert!(!opened, "{at}: an opened store is gone"),
            Start::Existing(found) => {
                assert_eq!(found.cluster_id, CLUSTER, "{at}");
                assert!(started.is_err() || found.started, "{at}: the mark is gone");
            }
        }
        let (store, found) = reopen(&sim, &Kept::default(), &at).await;
        assert!(found.is_empty() && !store.has_raft_state(), "{at}");
        takes_the_next(&sim, store, &at).await;
        started.is_ok()
    })
    .await;
    // The build's own steps and the started mark; the member file's write
    // inside `store.tmp`.
    assert_eq!(reached[..2], [7, 4]);
}

/// The started mark and the withheld vote, written through the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_in_a_member_file_write_keeps_a_change_that_returned() {
    let reached = at_every_stop(|stop, seed| async move {
        let sim = Sim::new(&format!("member-{stop:?}"), seed);
        let at = format!("{stop:?}, seed {seed}");
        let store = sim.open().await.expect("open");
        stop.arm(&sim, Some(&store.log()));
        let withheld = store.member_file().set_vote_withheld(true).await;
        let started = store.mark_started().await;
        stop.disarm(&sim);
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(store);

        let (store, _) = reopen(&sim, &Kept::default(), &at).await;
        let member = store.member_file().member();
        assert!(withheld.is_err() || member.vote_withheld, "{at}");
        assert!(started.is_err() || member.started, "{at}");
        assert!(
            !member.started || member.vote_withheld,
            "{at}: out of order"
        );
        takes_the_next(&sim, store, &at).await;
        started.is_ok()
    })
    .await;
    assert_eq!(reached[0], 8);
}

/// A vote, then a higher one. The vote found is never below one whose save
/// returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_in_a_vote_save_never_lowers_the_vote() {
    let reached = at_every_stop(|stop, seed| async move {
        let sim = Sim::new(&format!("vote-{stop:?}"), seed);
        let at = format!("{stop:?}, seed {seed}");
        let store = sim.open().await.expect("open");
        let mut log = store.log();
        log.save_vote(&vote(2, 1)).await.expect("vote");
        let written = entries(0..6);
        append_flushed(&mut log, &written).await;

        stop.arm(&sim, Some(&log));
        let saved = async {
            log.save_vote(&vote(5, 3)).await?;
            log.save_vote(&vote(6, 2)).await
        }
        .await;
        stop.disarm(&sim);
        settle(&log).await;
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(store);

        let kept = Kept {
            vote: Some(if saved.is_ok() {
                vote(6, 2)
            } else {
                vote(2, 1)
            }),
            flushed: written,
            ..Kept::default()
        };
        let (store, _) = reopen(&sim, &kept, &at).await;
        takes_the_next(&sim, store, &at).await;
        saved.is_ok()
    })
    .await;
    assert!(reached[2] >= 4, "only {reached:?} stops");
}

/// An append cut before its flush and after its callback, and a committed
/// id saved over it. Flushed entries stay; the committed id read back is
/// never past the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_around_an_append_and_a_committed_id_keeps_what_was_flushed() {
    let written = entries(0..26);
    for flush in [false, true] {
        let written = &written;
        at_every_stop(|stop, seed| async move {
            let sim = Sim::new(&format!("append-{flush}-{stop:?}"), seed);
            let at = format!("flushed {flush}, {stop:?}, seed {seed}");
            let store = sim.open().await.expect("open");
            let mut log = store.log();
            append_flushed(&mut log, &written[..20]).await;

            let disk = log.disk_log().clone();
            let held = (!flush).then_some(disk.hold_flushes().await);
            stop.arm(&sim, Some(&log));
            let saved = async {
                let flushed = append(&mut log, &written[20..]).await?;
                if flush {
                    flushed.await?;
                }
                // openraft saves an id before its entry is flushed here.
                log.save_committed(Some(written[22].log_id)).await?;
                log.save_committed(Some(written[25].log_id)).await
            }
            .await;
            stop.disarm(&sim);
            settle(&log).await;
            sim_fs::crash(&sim.0, seed).expect("crash");
            drop(held);
            drop(store);

            let confirmed = if flush && saved.is_ok() { 26 } else { 20 };
            let kept = Kept {
                flushed: written[..confirmed].to_vec(),
                possible: written.clone(),
                ..Kept::default()
            };
            let (store, _) = reopen(&sim, &kept, &at).await;
            takes_the_next(&sim, store, &at).await;
            saved.is_ok()
        })
        .await;
    }
}

/// A truncation stopped at each step. What is left is never shorter than
/// it was asked to keep; once it returned, the cut entries stay gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_in_a_truncation_leaves_no_gap() {
    const KEEP: usize = 12;
    let written = entries(0..60);
    let written = &written;
    let reached = at_every_stop(|stop, seed| async move {
        let sim = Sim::new(&format!("truncate-{stop:?}"), seed);
        let at = format!("{stop:?}, seed {seed}");
        let store = sim.open().await.expect("open");
        let mut log = store.log();
        append_flushed(&mut log, written).await;
        log.save_committed(Some(written[KEEP - 2].log_id))
            .await
            .expect("save");
        log.disk_log().commit_offset_persisted().await;

        stop.arm(&sim, Some(&log));
        let cut = log.truncate_after(Some(written[KEEP].log_id)).await;
        stop.disarm(&sim);
        settle(&log).await;
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(store);

        let kept = Kept {
            flushed: written[..=KEEP].to_vec(),
            possible: written.clone(),
            ..Kept::default()
        };
        let (store, found) = reopen(&sim, &kept, &at).await;
        if cut.is_ok() {
            assert_eq!(found.len(), KEEP + 1, "{at}: cut entries came back");
        }
        takes_the_next(&sim, store, &at).await;
        cut.is_ok()
    })
    .await;
    assert!(reached[3] >= 2, "only {reached:?} stops");
}

/// A snapshot is built and the log purged up to it, then purged past it,
/// which only hides, then a later snapshot lets the next append remove the
/// hidden part. Stopped at every step: the purged id on disk never passes
/// the snapshot on disk, and no entry above the snapshot is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_in_a_snapshot_build_and_a_purge_never_starts_the_log_after_its_snapshot() {
    let written = entries(0..62);
    let written = &written;
    let reached = at_every_stop(|stop, seed| async move {
        let sim = Sim::new(&format!("purge-{stop:?}"), seed);
        let at = format!("{stop:?}, seed {seed}");
        let store = sim.open().await.expect("open");
        let mut log = store.log();
        append_flushed(&mut log, &written[..60]).await;
        log.save_committed(Some(written[59].log_id))
            .await
            .expect("save");

        stop.arm(&sim, Some(&log));
        let mut hidden = false;
        let done = async {
            // A build, and the purge that follows it.
            store.snapshots().install(snapshot(3, 25)).await?;
            log.purge(written[25].log_id).await?;
            // Past the snapshot: up to it on disk, the rest hidden.
            log.purge(written[45].log_id).await?;
            hidden = true;
            // Covered now: the append removes the hidden part.
            store.snapshots().install(snapshot(5, 45)).await?;
            let flushed = append(&mut log, &written[60..]).await?;
            flushed.await
        }
        .await;
        if hidden {
            let state = log.log_state().await.expect("state");
            assert_eq!(state.last_purged_log_id, Some(written[45].log_id), "{at}");
        }
        stop.disarm(&sim);
        settle(&log).await;
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(store);

        let kept = Kept {
            flushed: written[..if done.is_ok() { 62 } else { 60 }].to_vec(),
            possible: written.clone(),
            ..Kept::default()
        };
        let (store, found) = reopen(&sim, &kept, &at).await;
        if done.is_ok() {
            assert_eq!(found, written[46..], "{at}");
        }
        takes_the_next(&sim, store, &at).await;
        done.is_ok()
    })
    .await;
    assert!(
        reached[0] >= 8 && reached[2] >= 8 && reached[3] >= 1,
        "only {reached:?} stops"
    );
}

/// A follower with a tail no leader confirmed installs a snapshot, then
/// takes the leader's entries and hears they are committed. `inside` puts
/// the snapshot inside the follower's log, at an index where its tail
/// holds another term's entry; otherwise the snapshot is past the log.
///
/// Stopped at every step. No entry of the old tail is ever found between
/// the snapshot and the committed id, where a restart would apply it.
async fn a_follower_installs(inside: bool) -> [usize; 4] {
    const COMMITTED: usize = 40;
    let written = entries(0..60);
    let at_snapshot: u64 = if inside { 50 } else { 75 };
    // What the leader holds from its snapshot on, in a term the follower
    // never wrote in.
    let leaders: Vec<_> = (at_snapshot + 1..at_snapshot + 8)
        .map(|index| entry_of(9, index))
        .collect();
    let (written, leaders) = (&written, &leaders);
    at_every_stop(|stop, seed| async move {
        let sim = Sim::new(&format!("install-{inside}-{stop:?}"), seed);
        let at = format!("inside {inside}, {stop:?}, seed {seed}");
        let store = sim.open().await.expect("open");
        let mut log = store.log();
        log.save_vote(&vote(9, 2)).await.expect("vote");
        append_flushed(&mut log, written).await;
        store
            .snapshots()
            .install(snapshot(2, 15))
            .await
            .expect("snapshot");
        log.purge(written[15].log_id).await.expect("purge");
        log.save_committed(Some(written[COMMITTED].log_id))
            .await
            .expect("save");
        log.disk_log().commit_offset_persisted().await;

        stop.arm(&sim, Some(&log));
        let installed = async {
            let last = log_id(9, at_snapshot);
            // openraft's order: cut what is not committed, store the
            // snapshot, purge up to it.
            log.truncate_after(Some(written[COMMITTED].log_id)).await?;
            store.snapshots().install(snapshot(9, at_snapshot)).await?;
            log.purge(last).await?;
            log.save_committed(Some(last)).await?;
            let flushed = append(&mut log, leaders).await?;
            flushed.await?;
            log.save_committed(leaders.last().map(|entry| entry.log_id))
                .await
        }
        .await;
        stop.disarm(&sim);
        settle(&log).await;
        sim_fs::crash(&sim.0, seed).expect("crash");
        drop(store);

        let kept = Kept {
            vote: Some(vote(9, 2)),
            snapshot: Some(15),
            flushed: written[..=COMMITTED].to_vec(),
            possible: written.iter().chain(leaders).cloned().collect(),
        };
        let (store, found) = reopen(&sim, &kept, &at).await;
        let covered = store.snapshots().covers().map(|last| last.index);
        let committed = store
            .log()
            .read_committed()
            .await
            .expect("committed")
            .map(|id| id.index);
        for entry in &found {
            let index = Some(entry.log_id.index);
            let applies = index > covered && index <= committed;
            assert!(
                !applies || !written[COMMITTED + 1..].contains(entry),
                "{at}: a restart would apply {} of the unconfirmed tail",
                entry.log_id
            );
        }
        if installed.is_ok() {
            assert_eq!(&found, leaders, "{at}");
        }
        takes_the_next(&sim, store, &at).await;
        installed.is_ok()
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_cut_in_an_install_past_its_log_never_applies_its_unconfirmed_tail() {
    let reached = a_follower_installs(false).await;
    assert!(
        reached[0] >= 4 && reached[2] >= 4 && reached[3] >= 2,
        "only {reached:?} stops"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_cut_in_an_install_inside_its_log_never_applies_its_unconfirmed_tail() {
    let reached = a_follower_installs(true).await;
    assert!(
        reached[0] >= 4 && reached[2] >= 4 && reached[3] >= 2,
        "only {reached:?} stops"
    );
}

/// On the simulator's filesystem there is no file to lock, so the store is
/// claimed by path in this process: one opener at a time there too.
#[tokio::test]
async fn a_store_in_memory_is_held_by_one_opener() {
    let sim = Sim::new("claim", 0);
    let store = sim.open().await.expect("open");
    let err = sim.open().await.err().expect("second open");
    assert_eq!(err.kind(), io::ErrorKind::ResourceBusy, "{err}");
    assert!(
        !crate::fs::exists(sim.dir(Dir::Store).join("lock")),
        "a lock file in memory locks nothing"
    );
    store.shutdown().await.expect("shutdown");
    sim.open().await.expect("released");
}
