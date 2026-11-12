//! Power loss around a purge, a snapshot write and a follower's snapshot
//! install, on the simulator's filesystem.
//!
//! openraft installs a snapshot in three calls: `truncate_after` when the
//! log conflicts with the snapshot, the state machine's `install_snapshot`,
//! then `purge` up to the snapshot's id. Each call is stopped at every step
//! it has (each step of a state-file write, each unlink), the power is cut,
//! and what is left must open: the purged id never past the stored
//! snapshot, the snapshot one whole snapshot, the log gap-free above the
//! purged id, and openraft's own startup (`get_initial_state`) accepting
//! it. The install is then finished and the log takes the next entry.
//!
//! Built only with the `sim` feature; CI runs it in the coverage job
//! (`--all-features`).

use openraft::storage::{RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine, StorageHelper};
use openraft::type_config::alias::SnapshotMetaOf;

use super::crash_points::{Sim, entries};
use super::machine::{Machine, State};
use super::writes::append_flushed;
use super::*;
use crate::disk_log::crash_point;
use crate::raft_log::{SnapshotId, SnapshotStore, StoredSnapshot};
use crate::sim_fs;

const SEEDS: u64 = 4;
/// More steps than any one sequence here has.
const MOST_STEPS: usize = 64;

/// Where a sequence is stopped.
#[derive(Clone, Copy, Debug)]
enum Stop {
    /// Before this step of the state-file writes, counted over the sequence.
    FileStep(usize),
    /// After this many unlinks.
    Unlink(usize),
}

impl Stop {
    fn arm(self, raft: &RaftLog<Types, Codec>) {
        match self {
            Self::FileStep(step) => crash_point::watch(raft.disk_log().dir(), Some(step)),
            Self::Unlink(unlinks) => raft.disk_log().stop_after_unlinks(unlinks),
        }
    }

    fn disarm(self, raft: &RaftLog<Types, Codec>) {
        if let Self::FileStep(_) = self {
            crash_point::unwatch(raft.disk_log().dir());
        }
    }
}

/// Every stop of each kind, until `run` reports that a sequence ran to its
/// end without reaching the stop it was given.
async fn at_every_stop<F>(mut run: impl FnMut(Stop, u64) -> F) -> usize
where
    F: Future<Output = bool>,
{
    let mut stopped = 0;
    for kind in [Stop::FileStep as fn(usize) -> Stop, Stop::Unlink] {
        for step in 0..MOST_STEPS {
            let mut finished = false;
            for seed in 0..SEEDS {
                finished = run(kind(step), seed).await;
            }
            if finished {
                break;
            }
            stopped += 1;
            assert!(step + 1 < MOST_STEPS, "{:?} never ran out", kind(step));
        }
    }
    stopped
}

fn machine(sim: &Sim) -> Machine {
    Machine::open(&sim.0.join("log"))
}

/// The state of a machine that applied `written[..=last]`.
fn applied(written: &[TestEntry], last: usize) -> State {
    State {
        last_applied: Some(written[last].log_id),
        commands: vec![format!("applied through {last}")],
        ..State::default()
    }
}

fn meta(state: &State) -> SnapshotMetaOf<Types> {
    SnapshotMetaOf::<Types> {
        last_log_id: state.last_applied,
        last_membership: state.membership.clone(),
    }
}

/// Open what a cut left and check it. Returns the log, the state machine as
/// openraft's startup left it, and the entries the log holds.
async fn reopen(
    sim: &Sim,
    written: &[TestEntry],
    at: &str,
) -> (RaftLog<Types, Codec>, Machine, Vec<TestEntry>) {
    let mut raft = sim.open();
    let mut machine = machine(sim);
    raft.purge_within(&machine.snapshots)
        .unwrap_or_else(|err| panic!("{at}: {err}"));
    let snapshot = machine.snapshots.covers().map(|last| last.index);

    let state = raft.log_state().await.expect("state");
    let purged = state.last_purged_log_id.map(|id| id.index);
    assert!(
        purged <= snapshot,
        "{at}: purged {purged:?} past {snapshot:?}"
    );
    let found = all(&raft).await.expect("read after the cut");
    let first = purged.map_or(0, |purged| purged + 1) as usize;
    for (at_index, entry) in (first..).zip(&found) {
        assert_eq!(entry.log_id.index, at_index as u64, "{at}: a gap");
        if let Some(original) = written.get(at_index) {
            assert_eq!(entry, original, "{at}");
        }
    }
    assert_eq!(
        state.last_log_id.map(|id| id.index),
        found.last().map(|entry| entry.log_id.index).or(purged),
        "{at}"
    );

    StorageHelper::new(&mut raft, &mut machine)
        .with_id(1)
        .get_initial_state()
        .await
        .unwrap_or_else(|err| panic!("{at}: openraft refuses to start: {err}"));
    let applied = machine.state.last_applied.map(|id| id.index);
    assert!(applied >= snapshot, "{at}: applied {applied:?}");
    (raft, machine, found)
}

/// A purge inside the log, stopped at every step. The log reopens starting
/// at its old first entry or right after the purged id, never in between
/// and never with a gap, and once the purge returned it stays purged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_at_each_step_of_a_purge_opens() {
    let written = entries(0..90);
    let stopped = at_every_stop(|stop, seed| {
        let written = &written;
        async move {
            let sim = Sim::new(&format!("purge-{stop:?}"), seed);
            let mut raft = sim.open();
            append_flushed(&mut raft, written).await;
            let mut state_machine = machine(&sim);
            state_machine.state = applied(written, 60);
            state_machine
                .get_snapshot_builder()
                .await
                .build_snapshot()
                .await
                .expect("snapshot");

            stop.arm(&raft);
            let purge = raft.purge(written[60].log_id).await;
            stop.disarm(&raft);
            sim_fs::crash(&sim.0, seed).expect("crash");
            drop(raft);

            let at = format!("{stop:?}, seed {seed}");
            let (mut raft, _, found) = reopen(&sim, written, &at).await;
            let purged = raft.log_state().await.expect("state").last_purged_log_id;
            match purged {
                None => assert_eq!(found, written[..], "{at}"),
                Some(id) => {
                    assert_eq!(id, written[60].log_id, "{at}");
                    assert_eq!(found, written[61..], "{at}");
                }
            }
            if purge.is_ok() {
                assert!(purged.is_some(), "{at}: a finished purge came undone");
            }
            raft.purge(written[60].log_id).await.expect("purge again");
            let next = normal(1_000, 90, "after the cut");
            append_flushed(&mut raft, std::slice::from_ref(&next)).await;
            assert_eq!(all(&raft).await.expect("read").last(), Some(&next), "{at}");
            raft.disk_log().shutdown().await.expect("shutdown");
            purge.is_ok()
        }
    })
    .await;
    assert!(stopped >= 6, "only {stopped} stops");
}

/// A snapshot replaced by another, stopped at every step of the write.
/// What reopens is one of the two, whole: never one's data under the
/// other's id, never neither. Once the install returned it is the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_at_each_step_of_a_snapshot_write_leaves_one_whole_snapshot() {
    let snapshot = |index: u64, data: &str| StoredSnapshot {
        last: Some(SnapshotId { term: 2, index }),
        meta: Bytes::from(format!("membership as of {index}")),
        data: Bytes::from(data.repeat(700)),
    };
    let old = snapshot(10, "old");
    let new = snapshot(40, "newer");
    let mut stopped = 0;
    for step in 0..MOST_STEPS {
        let mut finished = false;
        for seed in 0..SEEDS {
            let sim = Sim::new(&format!("snapshot-{step}"), seed);
            crate::fs::create_dir_all(&sim.0).expect("dir");
            let store = SnapshotStore::open(&sim.0).expect("open");
            assert!(store.install(old.clone()).await.expect("install"));

            crash_point::watch(&sim.0, Some(step));
            let installed = store.install(new.clone()).await;
            crash_point::unwatch(&sim.0);
            sim_fs::crash(&sim.0, seed).expect("crash");
            drop(store);

            let store = SnapshotStore::open(&sim.0).expect("open after the cut");
            let found = store.current().await.expect("read").expect("a snapshot");
            assert!(found == old || found == new, "step {step}, seed {seed}");
            if installed.is_ok() {
                assert_eq!(found, new, "step {step}, seed {seed}: came undone");
            }
            assert_eq!(store.covers(), found.last);
            finished = installed.is_ok();
        }
        if finished {
            break;
        }
        stopped += 1;
    }
    assert!(stopped >= 4, "only {stopped} stops");
}

/// A follower installs a leader's snapshot that is past its log and
/// conflicts with it: truncate, install, purge. The power is cut at every
/// step of the three. What is left opens, openraft starts on it, and the
/// install can be finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_at_each_step_of_a_snapshot_install_opens() {
    let written = entries(0..60);
    // What the leader sends: a state machine that applied index 75 of a
    // term this follower never saw.
    let leader = State {
        last_applied: Some(log_id(9, 75)),
        commands: vec!["the leader's state".to_string()],
        ..State::default()
    };
    let data = serde_json::to_vec(&leader).expect("json");

    let install = |raft: &mut RaftLog<Types, Codec>, machine: &mut Machine| {
        let (leader, data, committed) = (leader.clone(), data.clone(), written[40].log_id);
        let mut raft = raft.clone();
        let snapshots = machine.snapshots.clone();
        async move {
            let mut machine = Machine {
                snapshots,
                state: State::default(),
                unstored: None,
            };
            raft.truncate_after(Some(committed)).await?;
            machine.install_snapshot(&meta(&leader), data).await?;
            raft.purge(log_id(9, 75)).await?;
            raft.save_committed(Some(log_id(9, 75))).await
        }
    };

    let stopped = at_every_stop(|stop, seed| {
        let (written, leader, install) = (&written, &leader, &install);
        async move {
            let sim = Sim::new(&format!("install-{stop:?}"), seed);
            let mut raft = sim.open();
            append_flushed(&mut raft, written).await;
            let mut state_machine = machine(&sim);
            state_machine.state = applied(written, 20);
            state_machine
                .get_snapshot_builder()
                .await
                .build_snapshot()
                .await
                .expect("snapshot");
            raft.purge(written[20].log_id).await.expect("purge");
            raft.save_committed(Some(written[40].log_id))
                .await
                .expect("save");
            raft.disk_log().commit_offset_persisted().await;

            stop.arm(&raft);
            let installed = install(&mut raft, &mut state_machine).await;
            stop.disarm(&raft);
            sim_fs::crash(&sim.0, seed).expect("crash");
            drop(raft);

            let at = format!("{stop:?}, seed {seed}");
            let (mut raft, mut state_machine, found) = reopen(&sim, written, &at).await;
            let applied = state_machine.state.last_applied;
            if applied == leader.last_applied {
                // The snapshot is in. openraft's startup purged up to it if
                // the cut came before the purge.
                assert_eq!(state_machine.state.commands, leader.commands, "{at}");
                assert_eq!(all(&raft).await.expect("read"), [], "{at}");
            } else {
                // Still on the old snapshot: every committed entry is here.
                assert!(installed.is_err(), "{at}: a finished install came undone");
                assert_eq!(found.first(), Some(&written[21]), "{at}");
                assert!(found.len() >= 20, "{at}: {} entries", found.len());
                install(&mut raft, &mut state_machine)
                    .await
                    .unwrap_or_else(|err| panic!("{at}: the install cannot be finished: {err}"));
            }
            let state = raft.log_state().await.expect("state");
            assert_eq!(state.last_purged_log_id, Some(log_id(9, 75)), "{at}");
            assert_eq!(state.last_log_id, Some(log_id(9, 75)), "{at}");

            let next = normal(9, 76, "after the snapshot");
            append_flushed(&mut raft, std::slice::from_ref(&next)).await;
            raft.disk_log().shutdown().await.expect("shutdown");
            drop(raft);
            let (raft, state_machine, found) = reopen(&sim, written, &at).await;
            assert_eq!(found, [next], "{at}");
            assert_eq!(state_machine.state.commands, leader.commands, "{at}");
            raft.disk_log().shutdown().await.expect("shutdown");
            installed.is_ok()
        }
    })
    .await;
    assert!(stopped >= 10, "only {stopped} stops");
}

/// The same install when the snapshot ends inside the follower's log and
/// agrees with it: nothing is cut, and the purge deletes head segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_at_each_step_of_an_install_inside_the_log_opens() {
    let written = entries(0..90);
    let leader = applied(&written, 70);
    let data = serde_json::to_vec(&leader).expect("json");
    let stopped = at_every_stop(|stop, seed| {
        let (written, leader, data) = (&written, &leader, data.clone());
        async move {
            let sim = Sim::new(&format!("install-in-{stop:?}"), seed);
            let mut raft = sim.open();
            append_flushed(&mut raft, written).await;
            let mut state_machine = machine(&sim);

            stop.arm(&raft);
            let installed = async {
                state_machine.install_snapshot(&meta(leader), data).await?;
                raft.purge(written[70].log_id).await
            }
            .await;
            stop.disarm(&raft);
            sim_fs::crash(&sim.0, seed).expect("crash");
            drop(raft);

            let at = format!("{stop:?}, seed {seed}");
            let (mut raft, state_machine, found) = reopen(&sim, written, &at).await;
            assert_eq!(
                found.last(),
                written.last(),
                "{at}: flushed entries are gone"
            );
            if installed.is_ok() {
                assert_eq!(
                    state_machine.snapshots.covers().map(|last| last.index),
                    Some(70)
                );
                assert_eq!(found, written[71..], "{at}");
            }
            let next = normal(1_000, 90, "after the cut");
            append_flushed(&mut raft, std::slice::from_ref(&next)).await;
            raft.disk_log().shutdown().await.expect("shutdown");
            installed.is_ok()
        }
    })
    .await;
    assert!(stopped >= 6, "only {stopped} stops");
}
