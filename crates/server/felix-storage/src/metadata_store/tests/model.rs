//! The store against a small reference, under random writes and power cuts.
//!
//! The reference keeps what the rules say a restart must find: the entries
//! on disk, the purged id on disk and the hidden one, the snapshot, the
//! vote and the committed id. Each seed draws writes openraft could send,
//! a few it never would, clean restarts and power cuts, and after each one
//! the store must read exactly as the reference does. The reference also
//! holds `PurgedCoveredBySnapshot` from `specs/FelixRaft.tla`: the purged
//! id on disk is never past the snapshot.
//!
//! A failure names its seed and step; `FELIX_STORE_MODEL_SEED` replays one
//! seed. Built only with the `sim` feature.

use std::collections::BTreeMap;

use openraft::type_config::alias::VoteOf;

use super::store_crash_points::{Sim, Store, entry_of, settle, snapshot};
use crate::io::writeback::SplitMix64;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{LogReads, LogWrites, Types, all, log_id};
use crate::sim_fs;

const SEEDS: u64 = 24;
const STEPS: usize = 70;

/// An entry's index and term.
type Id = (u64, u64);

#[derive(Debug, Clone, Default, PartialEq)]
struct Model {
    /// Every entry on disk above `purged`, hidden or not.
    log: BTreeMap<u64, u64>,
    purged: Option<Id>,
    /// Past `purged`, and forgotten by a restart.
    hidden: Option<Id>,
    snapshot: Option<Id>,
    vote: Option<(u64, u64)>,
    committed: Option<Id>,
    term: u64,
}

impl Model {
    /// The purged id as the running store shows it.
    fn seen_purged(&self) -> Option<Id> {
        self.hidden.or(self.purged)
    }

    fn seen_last(&self) -> Option<Id> {
        let purged = self.seen_purged();
        match self.log.last_key_value() {
            Some((index, term)) if Some(*index) > purged.map(|id| id.0) => Some((*index, *term)),
            _ => purged,
        }
    }

    fn seen_entries(&self) -> Vec<Id> {
        let first = self.seen_purged().map_or(0, |id| id.0 + 1);
        self.log.range(first..).map(|(i, t)| (*i, *t)).collect()
    }

    fn seen_committed(&self) -> Option<Id> {
        match (self.committed, self.seen_purged()) {
            (Some(committed), Some(purged)) if committed.0 <= purged.0 => Some(purged),
            (None, purged) => purged,
            (committed, _) => committed,
        }
    }

    fn id_at(&self, index: u64) -> Option<Id> {
        match self.log.get(&index) {
            Some(term) => Some((index, *term)),
            None => self.seen_purged().filter(|id| id.0 == index),
        }
    }

    /// Remove through `id` on disk.
    fn remove(&mut self, id: Id) {
        self.purged = Some(id);
        self.log = self.log.split_off(&(id.0 + 1));
        if self.hidden.is_some_and(|hidden| hidden.0 <= id.0) {
            self.hidden = None;
        }
    }

    /// A purge: on disk up to the snapshot, hidden past it.
    fn purge(&mut self, asked: Id) {
        let target = match self.seen_purged() {
            Some(purged) if purged.0 >= asked.0 => purged,
            _ => asked,
        };
        let removable = match self.snapshot {
            Some(stored) if stored.0 < target.0 => Some(stored),
            Some(_) => Some(target),
            None => None,
        };
        if let Some(removable) = removable
            && self.purged.is_none_or(|purged| purged.0 < removable.0)
        {
            self.remove(removable);
        }
        self.hidden = self
            .purged
            .is_none_or(|purged| purged.0 < target.0)
            .then_some(target);
    }

    /// An append that begins at `first`: written, or refused with nothing
    /// changed. On disk it must continue the entries there, or start right
    /// after the purged id or the snapshot when there are none, and it
    /// must never be written while the entries below it are only hidden.
    fn append(&mut self, first: u64, terms: &[u64]) -> bool {
        let mut next = self.clone();
        if let Some(hidden) = next.hidden
            && next.snapshot.is_some_and(|stored| stored.0 >= hidden.0)
        {
            next.remove(hidden);
        }
        let last = next.log.last_key_value().map(|(index, _)| *index);
        let follows = |id: Option<Id>| id.is_some_and(|id| id.0 + 1 == first);
        let admitted = match (next.hidden, last) {
            (Some(hidden), Some(last)) => first == last + 1 && first > hidden.0,
            (Some(_), None) => false,
            (None, Some(last)) => first == last + 1,
            (None, None) if follows(next.purged) => true,
            (None, None) if follows(next.snapshot) && next.snapshot > next.purged => {
                next.remove(next.snapshot.expect("follows it"));
                true
            }
            (None, None) => next.purged.is_none() && next.snapshot.is_none(),
        };
        if admitted {
            next.log.extend((first..).zip(terms.iter().copied()));
            *self = next;
        }
        admitted
    }

    /// What a restart finds: nothing hidden, and a log the snapshot covers
    /// emptied to start right after it.
    fn restart(&mut self) {
        self.hidden = None;
        if let Some(stored) = self.snapshot
            && self.purged.is_none_or(|purged| purged.0 < stored.0)
            && self
                .log
                .last_key_value()
                .is_none_or(|(last, _)| *last <= stored.0)
        {
            self.remove(stored);
        }
        assert!(
            self.purged.map(|id| id.0) <= self.snapshot.map(|id| id.0) || self.purged.is_none(),
            "the reference broke PurgedCoveredBySnapshot: {self:?}"
        );
    }
}

fn id((index, term): Id) -> openraft::type_config::alias::LogIdOf<Types> {
    log_id(term, index)
}

/// The store reads exactly as the reference does.
async fn same(store: &Store, model: &Model, at: &str) {
    let mut log = store.log();
    let state = log.log_state().await.expect("state");
    let pair = |found: Option<openraft::type_config::alias::LogIdOf<Types>>| {
        found.map(|found| (found.index, *found.leader_id))
    };
    assert_eq!(
        pair(state.last_purged_log_id),
        model.seen_purged(),
        "{at}: purged"
    );
    assert_eq!(pair(state.last_log_id), model.seen_last(), "{at}: last");
    let found = all(&log).await.expect("read");
    let expected: Vec<_> = model
        .seen_entries()
        .into_iter()
        .map(|(index, term)| entry_of(term, index))
        .collect();
    assert_eq!(found, expected, "{at}: entries");
    let vote = store.reader().read_vote().await.expect("vote");
    assert_eq!(
        vote.map(|vote| (vote.leader_id.term, vote.leader_id.voted_for)),
        model.vote,
        "{at}: vote"
    );
    assert_eq!(
        store
            .snapshots()
            .covers()
            .map(|last| (last.index, last.term)),
        model.snapshot,
        "{at}: snapshot"
    );
    assert_eq!(
        pair(log.read_committed().await.expect("committed")),
        model.seen_committed(),
        "{at}: committed"
    );
}

/// Restart on what is on disk. `unflushed` are entries a power cut may
/// have kept any prefix of.
async fn restart(sim: &Sim, model: &mut Model, first: u64, unflushed: &[u64], at: &str) -> Store {
    let store = sim.open().await.unwrap_or_else(|err| panic!("{at}: {err}"));
    let found = all(&store.log()).await.expect("read").len();
    // A committed id is written behind the call, so a cut may give back an
    // older one. Never a newer one.
    let committed = store
        .log()
        .read_committed()
        .await
        .expect("committed")
        .map(|id| (id.index, *id.leader_id));
    let fits = (0..=unflushed.len()).any(|kept| {
        let mut next = model.clone();
        next.log
            .extend((first..).zip(unflushed[..kept].iter().copied()));
        next.restart();
        if next.seen_entries().len() != found {
            return false;
        }
        assert!(
            committed <= next.seen_committed(),
            "{at}: committed {committed:?}, saved {:?}",
            next.committed
        );
        if committed < next.seen_committed() {
            next.committed = committed;
        }
        *model = next;
        true
    });
    assert!(
        fits,
        "{at}: {found} entries fit no prefix of the unflushed ones"
    );
    same(&store, model, at).await;
    store
}

async fn run(seed: u64) {
    let mut rng = SplitMix64::new(seed ^ 0x5EED_0000_2230);
    let sim = Sim::new("model", seed);
    let mut model = Model {
        term: 1,
        ..Model::default()
    };
    let mut store = sim.open().await.expect("open");
    for step in 0..STEPS {
        let mut log = store.log();
        let op = rng.below(20);
        let at = format!("seed {seed}, step {step}, op {op}, before it {model:?}");
        let last = model.seen_last();
        let next = last.map_or(0, |id| id.0 + 1);
        match op {
            // An append where openraft would send it, or now and then
            // somewhere else.
            0..=6 => {
                // A log with nothing in or under it starts wherever its
                // first append does; openraft starts it at 0.
                let untouched = last.is_none() && model.snapshot.is_none();
                let first = match rng.below(8) {
                    0 if !untouched => next + 1 + rng.below(3),
                    1 => next.saturating_sub(1 + rng.below(2)),
                    _ => next,
                };
                let terms = vec![model.term; 1 + rng.below(6) as usize];
                let entries: Vec<_> = (first..)
                    .zip(&terms)
                    .map(|(index, term)| entry_of(*term, index))
                    .collect();
                let admitted = model.append(first, &terms);
                match append(&mut log, &entries).await {
                    Ok(flushed) => {
                        assert!(admitted, "{at}: an append at {first} was let in");
                        flushed.await.expect("flush");
                    }
                    Err(err) => assert!(!admitted, "{at}: an append at {first}: {err}"),
                }
            }
            // A new term's vote.
            7 => {
                model.term += 1 + rng.below(2);
                let node = rng.below(3);
                log.save_vote(&VoteOf::<Types>::new(model.term, node))
                    .await
                    .expect("vote");
                model.vote = Some((model.term, node));
            }
            // More of the log is committed.
            8..=10 => {
                let floor = model.seen_committed().map_or(0, |id| id.0 + 1);
                let Some(committed) = last
                    .filter(|last| last.0 >= floor)
                    .and_then(|last| model.id_at(floor + rng.below(last.0 - floor + 1)))
                else {
                    continue;
                };
                log.save_committed(Some(id(committed))).await.expect("save");
                model.committed = Some(committed);
            }
            // A cut of entries that are not committed.
            11 => {
                let floor = model.seen_committed();
                let Some(last) = last else { continue };
                let from = floor.map_or(0, |id| id.0);
                let keep = match model.id_at(from + rng.below(last.0 - from + 1)) {
                    Some(keep) => Some(keep),
                    None if floor.is_none() => None,
                    None => continue,
                };
                log.truncate_after(keep.map(id))
                    .await
                    .unwrap_or_else(|err| panic!("{at}: {err}"));
                drop(model.log.split_off(&keep.map_or(0, |keep| keep.0 + 1)));
            }
            // A snapshot of committed state: at a committed entry, or at a
            // hidden id, which the state machine had applied.
            12..=13 => {
                let from = model.snapshot.map_or(0, |id| id.0 + 1);
                let built = match (model.hidden, model.seen_committed()) {
                    (Some(hidden), _) if rng.below(2) == 0 => hidden,
                    (_, Some(through)) if through.0 >= from => {
                        let index = from + rng.below(through.0 - from + 1);
                        let Some(term) = model.log.get(&index) else {
                            continue;
                        };
                        (index, *term)
                    }
                    _ => continue,
                };
                if model.snapshot.is_some_and(|stored| stored.0 >= built.0) {
                    continue;
                }
                store
                    .snapshots()
                    .install(snapshot(built.1, built.0))
                    .await
                    .expect("snapshot");
                model.snapshot = Some(built);
            }
            // A purge of committed entries, past the snapshot or not.
            14..=15 => {
                let from = model.seen_purged().map_or(0, |id| id.0 + 1);
                let Some(through) = model.committed.map(|id| id.0).filter(|c| *c >= from) else {
                    continue;
                };
                let Some(asked) = model.id_at(from + rng.below(through - from + 1)) else {
                    continue;
                };
                log.purge(id(asked))
                    .await
                    .unwrap_or_else(|err| panic!("{at}: {err}"));
                model.purge(asked);
            }
            // A purge past the log's end, as openraft's startup asks for
            // when the state machine is ahead of a fully committed log.
            16 => {
                if model.seen_committed() != last {
                    continue;
                }
                let asked = (next + rng.below(3), model.term);
                log.purge(id(asked))
                    .await
                    .unwrap_or_else(|err| panic!("{at}: {err}"));
                model.purge(asked);
            }
            // A leader's snapshot past the log: vote, cut, install, purge.
            17 => {
                model.term += 1;
                let leader = (next + 1 + rng.below(5), model.term);
                log.save_vote(&VoteOf::<Types>::new(model.term, 2))
                    .await
                    .expect("vote");
                model.vote = Some((model.term, 2));
                let keep = model.seen_committed();
                log.truncate_after(keep.map(id))
                    .await
                    .unwrap_or_else(|err| panic!("{at}: {err}"));
                drop(model.log.split_off(&keep.map_or(0, |keep| keep.0 + 1)));
                store
                    .snapshots()
                    .install(snapshot(leader.1, leader.0))
                    .await
                    .expect("install");
                model.snapshot = Some(leader);
                log.purge(id(leader))
                    .await
                    .unwrap_or_else(|err| panic!("{at}: {err}"));
                model.purge(leader);
                log.save_committed(Some(id(leader))).await.expect("save");
                model.committed = Some(leader);
            }
            // A clean restart.
            18 => {
                store.shutdown().await.expect("shutdown");
                store = restart(&sim, &mut model, next, &[], &at).await;
                continue;
            }
            // A power cut, half the time with an append not yet flushed.
            _ => {
                let mut unflushed = Vec::new();
                let disk = log.disk_log().clone();
                let held = disk.hold_flushes().await;
                let mut after = model.clone();
                let terms = vec![model.term; 1 + rng.below(4) as usize];
                if rng.below(2) == 0 && after.append(next, &terms) {
                    let entries: Vec<_> = (next..)
                        .zip(&terms)
                        .map(|(index, term)| entry_of(*term, index))
                        .collect();
                    drop(append(&mut log, &entries).await.expect("append"));
                    // What the append removed on its way in is on disk.
                    drop(after.log.split_off(&next));
                    model = after;
                    unflushed = terms;
                }
                settle(&log).await;
                sim_fs::crash(&sim.0, rng.next_u64()).expect("crash");
                drop(held);
                drop(store);
                drop(log);
                drop(disk);
                store = restart(&sim, &mut model, next, &unflushed, &at).await;
                continue;
            }
        }
        same(&store, &model, &at).await;
    }
    store.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_store_matches_the_reference_under_random_writes_and_power_cuts() {
    let seeds = match std::env::var("FELIX_STORE_MODEL_SEED") {
        Ok(seed) => {
            let seed = seed.parse().expect("a seed");
            seed..seed + 1
        }
        Err(_) => 0..SEEDS,
    };
    for seed in seeds {
        run(seed).await;
    }
}
