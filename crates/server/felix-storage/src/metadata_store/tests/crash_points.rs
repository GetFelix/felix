//! Power loss at every step of a member-file write, on the simulator's
//! filesystem.
//!
//! A write has four steps: the temporary is written, fsynced, renamed over
//! the file, and the directory synced. The writer is stopped before each,
//! the power is cut, and what is left must open as the old member or the
//! new one, whole. A change that returned must still be there.
//!
//! Built only with the `sim` feature; CI runs it in the coverage job
//! (`--all-features`).

use std::future::Future;
use std::path::PathBuf;

use super::*;
use crate::sim_fs;

const SEEDS: u64 = 8;
/// More steps than any sequence here has.
const MOST_STEPS: usize = 32;

const CLUSTER: &str = "prod-eu";

/// A data directory on the simulator's filesystem.
struct Sim(PathBuf);

impl Sim {
    /// Empty: a new member's.
    fn empty(name: &str, seed: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "felix-simfs-member-{}-{name}-{seed}",
            std::process::id()
        ));
        sim_fs::register_root(&root);
        Self(root)
    }

    /// As a conversion leaves it: a converted store beside a stub
    /// `raft.redb`, all on disk.
    async fn converted(name: &str, seed: u64) -> Self {
        let sim = Self::empty(name, seed);
        let file = MemberFile::open(sim.store(), CLUSTER).expect("store");
        file.set_converted(true).await.expect("converted");
        let stub = crate::fs::File::create(sim.0.join(REDB_FILE)).expect("stub");
        crate::io::sync_all(&stub).expect("sync");
        crate::io::sync_dir(&sim.0).expect("sync");
        sim
    }

    fn store(&self) -> PathBuf {
        self.0.join(STORE_DIR)
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.0);
    }
}

/// Run `prepare`, then `change` stopped at each step in turn with the power
/// cut after it, until a run reaches no stop. `check` gets the member that
/// reopened, the one before the change, and whether the change returned.
async fn at_every_step<P, C>(
    name: &str,
    prepare: impl Fn(MemberFile) -> P,
    change: impl Fn(MemberFile) -> C,
    check: impl Fn(&Member, &Member, bool, &str),
) -> usize
where
    P: Future<Output = ()>,
    C: Future<Output = io::Result<()>>,
{
    let mut stopped = 0;
    for step in 0..MOST_STEPS {
        let mut finished = false;
        for seed in 0..SEEDS {
            let sim = Sim::converted(&format!("{name}-{step}"), seed).await;
            let file = MemberFile::open(sim.store(), CLUSTER).expect("open");
            prepare(file.clone()).await;
            let before = file.member();

            crash_point::watch(&sim.store(), Some(step));
            let changed = change(file.clone()).await;
            crash_point::unwatch(&sim.store());
            sim_fs::crash(&sim.0, seed).expect("crash");
            drop(file);

            let at = format!("{name}: step {step}, seed {seed}");
            // Decided before the open, which may delete a temporary.
            let decided = decide_start(&sim.0);
            let reopened =
                MemberFile::open(sim.store(), CLUSTER).unwrap_or_else(|err| panic!("{at}: {err}"));
            let found = reopened.member();
            check(&found, &before, changed.is_ok(), &at);
            // Beside the stub, the service starts exactly when the mark is set.
            match decided {
                Ok(start) => assert!(start == Start::Existing(found.clone()), "{at}"),
                Err(err) => assert!(!found.converted, "{at}: {err}"),
            }
            // The store still takes a change.
            reopened
                .set_vote_withheld(!reopened.member().vote_withheld)
                .await
                .unwrap_or_else(|err| panic!("{at}: {err}"));
            finished = changed.is_ok();
        }
        if finished {
            break;
        }
        stopped += 1;
        assert!(step + 1 < MOST_STEPS, "{name} never ran out of steps");
    }
    stopped
}

/// The started mark over an existing file. Once `mark_started` has
/// returned, the mark survives a power cut: that is what lets the Raft node
/// be made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_started_mark_that_returned_survives_a_cut() {
    let stopped = at_every_step(
        "started",
        |_| async {},
        |file| async move { file.mark_started().await.map(drop) },
        |found, before, returned, at| {
            let started = Member {
                started: true,
                ..before.clone()
            };
            assert!(found == before || *found == started, "{at}: {found:?}");
            if returned {
                assert!(found.started, "{at}: a node could exist without its mark");
            }
        },
    )
    .await;
    assert_eq!(stopped, 4);
}

/// The withheld vote: durable before the member acts on it, in both
/// directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vote_withheld_or_given_back_survives_a_cut_once_it_returned() {
    for withheld in [true, false] {
        let stopped = at_every_step(
            &format!("withheld-{withheld}"),
            |file| async move {
                file.set_vote_withheld(!withheld).await.expect("set");
                file.mark_started().await.expect("start");
            },
            |file| async move { file.set_vote_withheld(withheld).await },
            |found, before, returned, at| {
                let after = Member {
                    vote_withheld: withheld,
                    ..before.clone()
                };
                assert!(found == before || *found == after, "{at}: {found:?}");
                if returned {
                    assert_eq!(found.vote_withheld, withheld, "{at}: came undone");
                }
            },
        )
        .await;
        assert_eq!(stopped, 4);
    }
}

/// Clearing the converted mark, as a restore does before it puts the redb
/// file back. The started mark is never lost on the way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cleared_converted_mark_survives_a_cut_once_it_returned() {
    let stopped = at_every_step(
        "cleared",
        |file| async move {
            file.mark_started().await.expect("start");
        },
        |file| async move { file.set_converted(false).await },
        |found, before, returned, at| {
            assert!(found.started, "{at}: the started mark is gone");
            assert_eq!(found.cluster_id, before.cluster_id, "{at}");
            if returned {
                assert!(!found.converted, "{at}: came undone");
            }
        },
    )
    .await;
    assert_eq!(stopped, 4);
}

/// A member's first start on an empty data directory: the open makes the
/// store, then the vote is withheld and the started mark set, stopped at
/// every step with the power cut after it. What is left is no store, or a
/// whole one holding a prefix of the writes. A store whose open returned
/// survives, and so does a started mark that returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_during_a_first_start_leaves_no_store_or_a_prefix_of_its_writes() {
    let mut stopped = 0;
    for step in 0..MOST_STEPS {
        let mut finished = false;
        for seed in 0..SEEDS {
            let sim = Sim::empty(&format!("first-start-{step}"), seed);
            let at = format!("first start: step {step}, seed {seed}");

            crash_point::watch(&sim.store(), Some(step));
            let mut opened = false;
            let started = async {
                let file = MemberFile::open(sim.store(), CLUSTER)?;
                opened = true;
                file.set_vote_withheld(true).await?;
                file.mark_started().await
            }
            .await;
            crash_point::unwatch(&sim.store());
            sim_fs::crash(&sim.0, seed).expect("crash");

            let decided = decide_start(&sim.0).unwrap_or_else(|err| panic!("{at}: {err}"));
            match &decided {
                // The log is opened once the open returns, so the store
                // must be on disk by then.
                Start::New => assert!(!opened, "{at}: an opened store is gone"),
                Start::Existing(found) => {
                    assert_eq!(found.cluster_id, CLUSTER, "{at}");
                    if found.started {
                        assert!(found.vote_withheld, "{at}: started with its vote");
                    }
                    if started.is_ok() {
                        assert!(found.started, "{at}: a node could exist without its mark");
                    }
                }
            }
            // Whatever was left opens, and takes the rest of the start.
            let file =
                MemberFile::open(sim.store(), CLUSTER).unwrap_or_else(|err| panic!("{at}: {err}"));
            assert!(
                crate::fs::is_dir(file.log_dir()),
                "{at}: a store without its log"
            );
            file.mark_started()
                .await
                .unwrap_or_else(|err| panic!("{at}: {err}"));
            finished = started.is_ok();
        }
        if finished {
            break;
        }
        stopped += 1;
        assert!(step + 1 < MOST_STEPS, "never ran out of steps");
    }
    // Three in the store's build, four in each of the two writes.
    assert_eq!(stopped, 11);
}
