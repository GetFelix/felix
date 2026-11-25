use std::collections::BTreeMap;
use std::time::Duration;

use felix_cluster::history::model::{Action, AppendOutcome, Op};
use felix_cluster::history::register::{RegisterAction, RegisterOp};

use super::settle;
use crate::client::{Recorder, list_name};
use crate::cluster::DiskEvent;

fn acked(recorder: &Recorder, value: u64, complete: u64) {
    recorder.push(Op {
        process: 0,
        invoke: complete.saturating_sub(1),
        complete,
        action: Action::Append {
            list: list_name(0),
            value,
            outcome: AppendOutcome::Ok {
                offset: Some(value),
            },
        },
    });
}

fn put(recorder: &Recorder, value: u64, complete: u64) {
    recorder.push_register(RegisterOp {
        process: 1,
        invoke: complete.saturating_sub(1),
        complete,
        key: "k".into(),
        action: RegisterAction::Put {
            value,
            acknowledged: true,
        },
    });
}

fn event(excused: BTreeMap<u32, Vec<u64>>) -> DiskEvent {
    DiskEvent {
        at: Duration::ZERO,
        what: "wipe".into(),
        excused,
        unexcused: Vec::new(),
        held_before: Vec::new(),
    }
}

fn outcome(recorder: &Recorder, of: u64) -> AppendOutcome {
    recorder
        .history()
        .ops
        .iter()
        .find_map(|op| match &op.action {
            Action::Append { value, outcome, .. } if *value == of => Some(*outcome),
            _ => None,
        })
        .unwrap()
}

/// Only the records the fault took become unknown; the rest of the shard's
/// acknowledgements are still held to the log.
#[tokio::test(start_paused = true)]
async fn an_allowed_loss_forgets_only_the_records_it_took() {
    let recorder = Recorder::new(1);
    acked(&recorder, 1, 1);
    acked(&recorder, 2, 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle(&recorder, &event(BTreeMap::from([(0, vec![1])])), 2, true);
    assert_eq!(outcome(&recorder, 1), AppendOutcome::Info);
    assert!(matches!(outcome(&recorder, 2), AppendOutcome::Ok { .. }));
}

/// A fault that may have lost cache writes forgets the writes that finished
/// before it, not the ones after.
#[tokio::test(start_paused = true)]
async fn a_cache_loss_forgets_only_writes_before_the_fault() {
    let recorder = Recorder::new(1);
    put(&recorder, 1, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle(&recorder, &event(BTreeMap::from([(0, vec![9])])), 2, true);
    put(&recorder, 2, recorder.now() + 1);
    let puts: Vec<(u64, bool)> = recorder
        .history()
        .registers
        .iter()
        .filter_map(|op| match op.action {
            RegisterAction::Put {
                value,
                acknowledged,
            } => Some((value, acknowledged)),
            _ => None,
        })
        .collect();
    assert_eq!(puts, vec![(1, false), (2, true)]);
}

fn disk_plan(seed: u64, brokers: usize, window: Duration, cut: bool, wipes: bool) -> super::Plan {
    super::Plan::default().with_disk_faults(seed, brokers, window, cut, wipes)
}

/// **A seed replays the same disk faults**, both kinds off adds none, and
/// the schedule is in time order with nothing starting after the window.
#[test]
fn disk_faults_are_seeded_ordered_and_inside_the_window() {
    let window = Duration::from_secs(180);
    assert!(disk_plan(7, 5, window, false, false).steps.is_empty());
    for seed in 0..50 {
        let plan = disk_plan(seed, 5, window, true, true);
        assert_eq!(plan, disk_plan(seed, 5, window, true, true), "seed {seed}");
        assert!(!plan.steps.is_empty(), "seed {seed}");
        assert!(plan.steps.windows(2).all(|w| w[0].at <= w[1].at));
        assert!(plan.steps.iter().all(|step| step.at < window));
    }
}

/// **A cut takes two brokers or a majority, a wipe one or two, never the
/// same broker twice and never one that does not exist**; each stays down
/// between 5 and 20 seconds.
#[test]
fn disk_faults_pick_distinct_existing_brokers() {
    let brokers = 5;
    let mut cuts = 0;
    let mut wipes = 0;
    for seed in 0..100 {
        for step in disk_plan(seed, brokers, Duration::from_secs(300), true, true).steps {
            let (nodes, down_for, allowed): (Vec<usize>, Duration, &[usize]) = match step.fault {
                super::Fault::MajorityCut { nodes, down_for } => {
                    cuts += 1;
                    (nodes, down_for, &[2, brokers / 2 + 1])
                }
                super::Fault::Wipe {
                    nodes, down_for, ..
                } => {
                    wipes += 1;
                    (nodes, down_for, &[1, 2])
                }
                other => panic!("not a disk fault: {other:?}"),
            };
            assert!(allowed.contains(&nodes.len()), "{nodes:?}");
            assert!(
                nodes.windows(2).all(|w| w[0] < w[1]),
                "sorted, distinct: {nodes:?}"
            );
            assert!(nodes.iter().all(|&n| n < brokers), "{nodes:?}");
            assert!(
                (Duration::from_secs(5)..=Duration::from_secs(20)).contains(&down_for),
                "{down_for:?}"
            );
        }
    }
    assert!(cuts > 0 && wipes > 0, "cuts {cuts}, wipes {wipes}");
}

/// **A window shorter than the first fault's start schedules nothing**:
/// every disk fault waits at least ten seconds into the run.
#[test]
fn a_short_window_has_no_disk_faults() {
    for seed in 0..20 {
        assert!(
            disk_plan(seed, 3, Duration::from_secs(10), true, true)
                .steps
                .is_empty()
        );
    }
}
