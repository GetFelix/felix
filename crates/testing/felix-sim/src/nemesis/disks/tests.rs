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
