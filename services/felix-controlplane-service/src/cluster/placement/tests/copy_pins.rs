//! The copy pins a failover writes (`fence_storage_ids`) outlive a rename
//! that keeps the promoted leader until it reports serving, so its next fence
//! still counts each member only from the copy the failover's report named
//! (#2046). A member's pin goes once it is admitted under new storage.
use felix_common::membership::RefusedGeneration;

use super::super::moves::keep_copy_pins;
use super::*;

const NOW: u64 = 10_000_000_000;

/// Reports for a shard at generation 3 led by broker-a.
#[derive(Default)]
struct Reports {
    refused: Option<RefusedGeneration>,
    opened: bool,
    stuck: bool,
    readmittable: Option<&'static str>,
    holds_start: Option<&'static str>,
}

impl CaughtUp for Reports {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }

    fn leader_refusal(
        &self,
        _key: &ShardKey,
        generation: u64,
        leader: &str,
    ) -> Option<RefusedGeneration> {
        self.refused
            .clone()
            .filter(|_| generation == 3 && leader == "broker-a")
    }

    fn leader_opened(&self, _key: &ShardKey, generation: u64, leader: &str) -> bool {
        self.opened && generation == 3 && leader == "broker-a"
    }

    fn stuck_leader(&self, _key: &ShardKey, generation: u64) -> Option<StuckLeader> {
        (self.stuck && generation == 3).then_some(StuckLeader {
            since_millis: 0,
            reason: StuckReason::NotCommitting,
            recent_renames: 0,
            fleet_stalled: false,
        })
    }

    fn readmittable(&self, _key: &ShardKey, _generation: u64, _leader: &str) -> Option<String> {
        self.readmittable.map(str::to_string)
    }

    fn holds_start(&self, _key: &ShardKey, generation: u64, leader: &str, node: &str) -> bool {
        generation == 3 && leader == "broker-a" && self.holds_start == Some(node)
    }
}

fn sid(n: char) -> String {
    n.to_string().repeat(32)
}

/// broker-a promoted at generation 3, its fence pinned to each member's copy.
fn promoted() -> ShardAssignment {
    let mut assignment = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    assignment.fence_storage_ids = [
        ("broker-a".to_string(), sid('a')),
        ("broker-b".to_string(), sid('b')),
        ("broker-c".to_string(), sid('c')),
    ]
    .into();
    assignment
}

fn promise() -> RefusedGeneration {
    RefusedGeneration {
        generation: 3,
        promised_to: "broker-d".to_string(),
        ballot: 0,
    }
}

fn decide_with(existing: ShardAssignment, reports: &Reports, joint_fence: bool) -> Decision {
    let streams = [Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let policy = MovePolicy {
        stuck_leader_millis: Some(1_000),
        joint_fence,
        ..MovePolicy::default()
    };
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    plan_with(&streams, &[], &nodes, &[existing], reports, policy)
        .shards
        .remove(0)
        .decision
}

fn renamed(decision: Decision) -> (MoveStep, ShardAssignment) {
    let Decision::Move(step, next) = decision else {
        panic!("not a rename: {decision:?}");
    };
    assert_eq!(next.leader, "broker-a", "{next:?}");
    (step, next)
}

/// **A refusal's rename keeps the pins** while the leader has not opened.
#[test]
fn a_refusal_rename_keeps_the_pins_until_the_leader_opens() {
    let reports = Reports {
        refused: Some(promise()),
        ..Reports::default()
    };
    let (step, next) = renamed(decide_with(promoted(), &reports, false));
    assert_eq!(step, MoveStep::ElectionRefused);
    assert_eq!(next.fence_storage_ids, promoted().fence_storage_ids);

    let reports = Reports {
        refused: Some(RefusedGeneration::above(3, 5)),
        ..Reports::default()
    };
    let (step, next) = renamed(decide_with(promoted(), &reports, false));
    assert!(matches!(step, MoveStep::SetUnknown { .. }), "{step:?}");
    assert_eq!(next.fence_storage_ids, promoted().fence_storage_ids);

    // Once the leader has reported serving at the pinned generation, the
    // next rename names no pins.
    let reports = Reports {
        refused: Some(promise()),
        opened: true,
        ..Reports::default()
    };
    let (_, next) = renamed(decide_with(promoted(), &reports, false));
    assert!(next.fence_storage_ids.is_empty(), "{next:?}");
}

/// **A refusal that drops a staged move keeps them too.**
#[test]
fn undoing_a_staged_move_keeps_the_pins() {
    let staged = ShardAssignment {
        successor: Some("broker-c".to_string()),
        move_started_at_millis: Some(NOW),
        move_reason: Some(crate::model::MoveReason::Balance),
        ..promoted()
    };
    let reports = Reports {
        refused: Some(promise()),
        ..Reports::default()
    };
    let (step, next) = renamed(decide_with(staged, &reports, false));
    assert_eq!(step, MoveStep::ElectionRefused);
    assert_eq!(next.successor, None);
    assert_eq!(next.fence_storage_ids, promoted().fence_storage_ids);
}

/// **A stuck leader's rename keeps them.**
#[test]
fn a_stuck_rename_keeps_the_pins() {
    let reports = Reports {
        stuck: true,
        ..Reports::default()
    };
    let (step, next) = renamed(decide_with(promoted(), &reports, false));
    assert!(matches!(step, MoveStep::Stuck { .. }), "{step:?}");
    assert_eq!(next.fence_storage_ids, promoted().fence_storage_ids);
}

/// **An admitted member loses its pin**: the copy it names was replaced.
#[test]
fn an_admitted_member_loses_its_pin() {
    let without_c = {
        let mut pins = promoted().fence_storage_ids;
        pins.remove("broker-c");
        pins
    };

    let reports = Reports {
        readmittable: Some("broker-c"),
        ..Reports::default()
    };
    let (step, next) = renamed(decide_with(promoted(), &reports, true));
    assert!(matches!(step, MoveStep::Readmit { .. }), "{step:?}");
    assert_eq!(next.fence_storage_ids, without_c);

    let learning = ShardAssignment {
        learners: ["broker-c".to_string()].into(),
        ..promoted()
    };
    let reports = Reports {
        refused: Some(promise()),
        holds_start: Some("broker-c"),
        ..Reports::default()
    };
    let (_, next) = renamed(decide_with(learning, &reports, true));
    assert_eq!(next.fence_storage_ids, without_c);
}

/// **A new leader gets none**: its fence needs pins of its own.
#[test]
fn a_new_leader_gets_no_pins() {
    let existing = promoted();
    let mut next = ShardAssignment {
        leader: "broker-b".to_string(),
        ..existing.renamed()
    };
    keep_copy_pins(&existing, &mut next, &Reports::default(), None);
    assert!(next.fence_storage_ids.is_empty());
}
