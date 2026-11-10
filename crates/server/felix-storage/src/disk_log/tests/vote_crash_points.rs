//! A crash at every step of a vote save, on the simulator's filesystem.
//!
//! A save writes the ballot (with the committed mark) and then, on a raise,
//! `replica`, each through a fsynced temporary, a rename and a directory
//! sync. The writer is stopped before each of those steps in turn, the power
//! is cut with several seeds, and the shard is reopened. It must come back
//! with the vote it had or the one being saved, never a mix: in particular
//! never a committed mark on a vote that was not durably saved.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::*;
use crate::disk_log::crash_point;
use crate::sim_fs;

const KEY: &str = "t/ns/s/0";
const SEEDS: u64 = 8;

/// The generation, leader and mark a shard opens with.
type Vote = (u64, Option<Arc<str>>, bool);

fn vote(generation: u64, leader: Option<&str>, committed: bool) -> Vote {
    (generation, leader.map(Arc::from), committed)
}

#[derive(Clone, Copy, Debug)]
enum Save {
    Accept(u64, Option<&'static str>),
    Commit(u64, &'static str),
}

impl Save {
    async fn run(self, log: &DiskLog) -> Result<GenerationCheck> {
        match self {
            Self::Accept(generation, leader) => log.accept_generation(generation, leader).await,
            Self::Commit(generation, leader) => log.commit_vote_for_tests(generation, leader).await,
        }
    }
}

struct Sim(PathBuf);

impl Sim {
    fn new(name: &str, stop: usize, seed: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "felix-simfs-vote-{}-{name}-{stop}-{seed}",
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

fn open_at(dir: &Path) -> DiskLog {
    DiskLog::open(dir, KEY, LogConfig::default()).expect("open")
}

fn held(log: &DiskLog) -> Vote {
    (
        log.accepted_generation(),
        log.accepted_leader(),
        log.accepted_vote_committed(),
    )
}

/// Stop `save` before each of its steps after `before`, cut the power, and
/// check what the shard reopens with.
async fn every_crash_point(name: &str, before: &[Save], save: Save, old: Vote, new: Vote) {
    // How many steps the save takes, with nothing stopped.
    let steps = {
        let sim = Sim::new(name, usize::MAX, 0);
        let log = open_at(&sim.dir());
        for earlier in before {
            earlier.run(&log).await.expect("earlier save");
        }
        assert_eq!(held(&log), old, "{name}: the state before the save");
        crash_point::watch(&sim.dir(), None);
        save.run(&log).await.expect("save");
        let steps = crash_point::unwatch(&sim.dir());
        assert_eq!(held(&log), new, "{name}: the state after the save");
        log.shutdown().await.expect("shutdown");
        steps
    };
    assert!(steps >= 4, "{name}: only {steps} steps");

    for stop in 0..=steps {
        for seed in 0..SEEDS {
            let sim = Sim::new(name, stop, seed);
            let log = open_at(&sim.dir());
            for earlier in before {
                earlier.run(&log).await.expect("earlier save");
            }
            crash_point::watch(&sim.dir(), Some(stop));
            let saved = save.run(&log).await.is_ok();
            crash_point::unwatch(&sim.dir());
            assert_eq!(saved, stop == steps, "{name}: stop {stop}");

            sim_fs::crash(&sim.0, seed).expect("crash");
            let reopened = open_at(&sim.dir());
            let found = held(&reopened);
            let at = format!("{name}: stopped before step {stop} of {steps}, seed {seed}");
            if saved {
                assert_eq!(found, new, "{at}: a finished save was lost");
            } else {
                assert!(found == old || found == new, "{at}: opened with {found:?}");
            }
            reopened.shutdown().await.expect("shutdown");
            drop(log);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committing_the_vote_already_accepted() {
    every_crash_point(
        "commit-same",
        &[Save::Accept(5, Some("a"))],
        Save::Commit(5, "a"),
        vote(5, Some("a"), false),
        vote(5, Some("a"), true),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committing_a_vote_at_a_raised_generation() {
    every_crash_point(
        "commit-raise",
        &[Save::Commit(4, "a")],
        Save::Commit(5, "b"),
        vote(4, Some("a"), true),
        vote(5, Some("b"), true),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepting_a_new_leader_after_a_committed_vote() {
    every_crash_point(
        "accept-leader",
        &[Save::Commit(4, "a")],
        Save::Accept(5, Some("b")),
        vote(4, Some("a"), true),
        vote(5, Some("b"), false),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raising_without_a_leader_after_a_committed_vote() {
    // Only `replica` is written; the ballot left behind still marks
    // generation 4, and the open must not carry that mark to generation 5.
    every_crash_point(
        "accept-bare",
        &[Save::Commit(4, "a")],
        Save::Accept(5, None),
        vote(4, Some("a"), true),
        vote(5, None, false),
    )
    .await;
}
