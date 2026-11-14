//! The membership decision, one test per guard of
//! `docs/formal/FelixRaftPlacement.tla`. Each fails with its guard removed.

use std::collections::BTreeSet;

use crate::raft::MemberId;
use crate::raft::membership::{Config, Step, Target, TargetVersion, View, Wait, next_step};

fn m(broker: &str) -> MemberId {
    MemberId::new(broker, 0)
}

fn set(brokers: &[&str]) -> BTreeSet<MemberId> {
    brokers.iter().map(|broker| m(broker)).collect()
}

fn v(counter: u64) -> TargetVersion {
    TargetVersion {
        incarnation: 0,
        counter,
    }
}

fn target(brokers: &[&str], counter: u64) -> Target {
    Target {
        voters: set(brokers),
        version: v(counter),
    }
}

/// Leader `a` of a settled group with `voters` at version 1, every other
/// member holding its whole log.
fn leader(voters: &[&str], learners: &[&str]) -> View {
    let last_index = 10;
    View {
        me: m("a"),
        config: Config {
            old: set(voters),
            new: set(voters),
            learners: set(learners),
            version: v(1),
        },
        config_committed: true,
        own_term_committed: true,
        transferring: false,
        acked_incarnation: 0,
        last_index,
        commit_index: last_index,
        matched: set(voters)
            .into_iter()
            .chain(set(learners))
            .map(|member| (member, last_index))
            .collect(),
        lag_threshold: 2,
    }
}

fn joint(old: &[&str], new: &[&str]) -> View {
    let mut view = leader(old, new);
    view.config.new = set(new);
    view.config.learners.clear();
    view
}

#[test]
fn a_new_member_joins_as_a_learner_first() {
    let view = leader(&["a", "b", "c"], &[]);
    let step = next_step(&view, Some(&target(&["a", "b", "d"], 2)));
    assert_eq!(step, Step::AddLearner(m("d")));
}

#[test]
fn a_caught_up_learner_is_promoted_and_the_dropped_voter_leaves() {
    let view = leader(&["a", "b", "c"], &["d"]);
    let step = next_step(&view, Some(&target(&["a", "b", "d"], 2)));
    assert_eq!(
        step,
        Step::Change {
            voters: set(&["a", "b", "d"]),
            version: v(2)
        }
    );
}

#[test]
fn a_learner_behind_is_not_promoted() {
    let mut view = leader(&["a", "b", "c"], &["d"]);
    view.matched.insert(m("d"), view.last_index - 3);
    let goal = target(&["a", "b", "d"], 2);
    assert_eq!(next_step(&view, Some(&goal)), Step::Wait(Wait::CatchingUp));
    // Past the commit index but too far from the end is behind as well.
    view.commit_index = view.last_index - 5;
    assert_eq!(next_step(&view, Some(&goal)), Step::Wait(Wait::CatchingUp));
    view.commit_index = view.last_index;
    // Within the threshold but short of the commit index is still behind.
    view.matched.insert(m("d"), view.last_index - 1);
    assert_eq!(next_step(&view, Some(&goal)), Step::Wait(Wait::CatchingUp));
    view.commit_index = view.last_index - 1;
    assert!(matches!(next_step(&view, Some(&goal)), Step::Change { .. }));
}

#[test]
fn a_target_older_than_the_membership_is_refused() {
    let mut view = leader(&["b", "c", "a"], &[]);
    view.config.version = v(2);
    // The stale target would give `d` a seat the newer one took away.
    let step = next_step(&view, Some(&target(&["a", "b", "d"], 1)));
    assert_eq!(step, Step::Wait(Wait::StaleTarget));
}

#[test]
fn a_target_from_before_a_restore_is_refused() {
    let mut view = leader(&["a", "b", "c"], &[]);
    let goal = target(&["a", "b", "d"], 9);
    view.acked_incarnation = 1;
    assert_eq!(next_step(&view, Some(&goal)), Step::Wait(Wait::StaleTarget));
    // An incarnation only the log has seen refuses it too.
    view.acked_incarnation = 0;
    view.config.version = TargetVersion {
        incarnation: 1,
        counter: 0,
    };
    assert_eq!(next_step(&view, Some(&goal)), Step::Wait(Wait::StaleTarget));
}

#[test]
fn a_restore_incarnation_outranks_the_counter() {
    let mut view = leader(&["a", "b", "c"], &[]);
    view.config.version = v(7);
    let goal = Target {
        voters: set(&["a", "b", "d"]),
        version: TargetVersion {
            incarnation: 1,
            counter: 0,
        },
    };
    view.acked_incarnation = 1;
    assert_eq!(next_step(&view, Some(&goal)), Step::AddLearner(m("d")));
}

#[test]
fn no_step_before_an_entry_of_this_term_commits() {
    let mut view = leader(&["a", "b", "c"], &[]);
    view.own_term_committed = false;
    let step = next_step(&view, Some(&target(&["a", "b", "d"], 2)));
    assert_eq!(step, Step::Wait(Wait::Barrier));
    let mut view = joint(&["a", "b", "c"], &["a", "b", "d"]);
    view.own_term_committed = false;
    assert_eq!(next_step(&view, None), Step::Wait(Wait::Barrier));
}

#[test]
fn no_step_on_an_uncommitted_membership() {
    let mut view = leader(&["a", "b", "c"], &[]);
    view.config_committed = false;
    let step = next_step(&view, Some(&target(&["a", "b", "d"], 2)));
    assert_eq!(step, Step::Wait(Wait::Uncommitted));
    let mut view = joint(&["a", "b", "c"], &["a", "b", "d"]);
    view.config_committed = false;
    assert_eq!(next_step(&view, None), Step::Wait(Wait::Uncommitted));
}

#[test]
fn no_step_during_a_transfer() {
    let mut view = leader(&["a", "b", "c"], &[]);
    view.transferring = true;
    let step = next_step(&view, Some(&target(&["a", "b", "d"], 2)));
    assert_eq!(step, Step::Wait(Wait::Transferring));
}

#[test]
fn a_joint_membership_is_left_before_a_newer_target() {
    let view = joint(&["a", "b", "c"], &["a", "b", "d"]);
    let step = next_step(&view, Some(&target(&["a", "b", "e"], 3)));
    assert_eq!(step, Step::LeaveJoint);
    // With no target at all, too: the joint came from a real one.
    assert_eq!(next_step(&view, None), Step::LeaveJoint);
}

#[test]
fn a_leader_the_joint_leaves_out_transfers_instead_of_flattening() {
    let view = joint(&["a", "b", "c"], &["b", "c", "d"]);
    assert_eq!(next_step(&view, None), Step::Transfer(m("b")));
}

#[test]
fn a_leader_the_target_leaves_out_grows_then_transfers() {
    // The leader's seat moves to `d`: never a change without itself.
    let goal = target(&["b", "c", "d"], 2);
    let view = leader(&["a", "b", "c"], &["d"]);
    assert_eq!(
        next_step(&view, Some(&goal)),
        Step::Change {
            voters: set(&["a", "b", "c", "d"]),
            // Not the target's voters, so not the target's version.
            version: v(1)
        }
    );
    let view = leader(&["a", "b", "c", "d"], &[]);
    assert_eq!(next_step(&view, Some(&goal)), Step::Transfer(m("b")));
}

#[test]
fn a_group_of_one_moves_by_growing_first() {
    let goal = target(&["b"], 2);
    let view = leader(&["a"], &[]);
    assert_eq!(next_step(&view, Some(&goal)), Step::AddLearner(m("b")));
    let view = leader(&["a"], &["b"]);
    assert_eq!(
        next_step(&view, Some(&goal)),
        Step::Change {
            voters: set(&["a", "b"]),
            version: v(1)
        }
    );
    let view = leader(&["a", "b"], &[]);
    assert_eq!(next_step(&view, Some(&goal)), Step::Transfer(m("b")));
}

#[test]
fn a_transfer_goes_only_to_a_voter_holding_the_whole_log() {
    let goal = target(&["b", "c", "d"], 2);
    let mut view = leader(&["a", "b", "c", "d"], &[]);
    view.matched.insert(m("b"), view.last_index - 1);
    assert_eq!(next_step(&view, Some(&goal)), Step::Transfer(m("c")));
    for behind in ["c", "d"] {
        view.matched.insert(m(behind), view.last_index - 1);
    }
    assert_eq!(next_step(&view, Some(&goal)), Step::Wait(Wait::NoSuccessor));
    // A voter only the old set has is no successor.
    let mut view = joint(&["a", "b", "c"], &["d", "e", "f"]);
    view.matched.clear();
    view.matched.insert(m("b"), view.last_index);
    assert_eq!(next_step(&view, None), Step::Wait(Wait::NoSuccessor));
}

#[test]
fn the_same_voters_at_a_newer_version_are_stamped() {
    // `d` was placed and dropped again before it was promoted.
    let view = leader(&["a", "b", "c"], &["d"]);
    assert_eq!(
        next_step(&view, Some(&target(&["a", "b", "c"], 4))),
        Step::Stamp {
            learners: BTreeSet::new(),
            version: v(4)
        }
    );
    assert_eq!(
        next_step(&view, Some(&target(&["a", "b", "c"], 1))),
        Step::Wait(Wait::Settled)
    );
}

#[test]
fn no_target_no_step() {
    let view = leader(&["a", "b", "c"], &["d"]);
    assert_eq!(next_step(&view, None), Step::Wait(Wait::NoTarget));
}

/// No step from any view here writes a membership without the leader.
#[test]
fn no_change_ever_leaves_the_leader_out() {
    let all = ["a", "b", "c", "d"];
    let subsets = |min: usize| {
        (1u32..16)
            .map(|bits| {
                let picked: Vec<&str> = (0..4)
                    .filter(|i| bits >> i & 1 == 1)
                    .map(|i| all[i])
                    .collect();
                picked
            })
            .filter(move |picked| picked.len() >= min)
            .collect::<Vec<_>>()
    };
    for voters in subsets(1)
        .into_iter()
        .filter(|voters| voters.contains(&"a"))
    {
        for goal in subsets(1) {
            let learners: Vec<&str> = goal
                .iter()
                .copied()
                .filter(|g| !voters.contains(g))
                .collect();
            let view = leader(&voters, &learners);
            if let Step::Change { voters, .. } = next_step(&view, Some(&target(&goal, 2))) {
                assert!(voters.contains(&m("a")), "{goal:?} from {view:?}");
            }
        }
    }
}
