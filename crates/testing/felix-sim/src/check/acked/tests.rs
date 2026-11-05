use felix_cluster::history::model::Op;

use super::*;

fn held(records: &[(u64, u64)]) -> Vec<Element> {
    records
        .iter()
        .map(|&(offset, value)| Element {
            offset,
            value,
            skipped_before: 0,
        })
        .collect()
}

fn acked(offset: Option<u64>, value: u64) -> Acked {
    Acked { offset, value }
}

#[test]
fn a_record_missing_or_at_another_offset_is_not_held() {
    let log = held(&[(0, 10), (1, 11), (3, 13)]);
    let lost = unheld(
        &[
            acked(Some(1), 11),
            acked(Some(2), 12),
            // Held, but at offset 3, not the offset the client was told.
            acked(Some(4), 13),
            acked(Some(1), 99),
        ],
        &log,
    );
    assert_eq!(
        lost,
        vec![acked(Some(2), 12), acked(Some(4), 13), acked(Some(1), 99)]
    );
}

#[test]
fn records_the_leader_holds_satisfy_the_checker() {
    let log = held(&[(0, 10), (1, 11), (2, 12)]);
    assert!(unheld(&[acked(Some(0), 10), acked(Some(2), 12)], &log).is_empty());
    // No offset given: the value anywhere will do.
    assert!(unheld(&[acked(None, 11)], &log).is_empty());
    assert_eq!(unheld(&[acked(None, 55)], &log), vec![acked(None, 55)]);
    assert!(unheld(&[], &[]).is_empty());
}

fn append(list: &str, value: u64, outcome: AppendOutcome) -> Op {
    Op {
        process: 0,
        invoke: 0,
        complete: 1,
        action: Action::Append {
            list: list.to_string(),
            value,
            outcome,
        },
    }
}

#[test]
fn only_acknowledged_appends_to_stream_shards_are_expected() {
    let history = History {
        ops: vec![
            append(&list_name(0), 1, AppendOutcome::Ok { offset: Some(0) }),
            append(&list_name(0), 2, AppendOutcome::Info),
            append(&list_name(1), 3, AppendOutcome::Fail),
            append(&list_name(1), 4, AppendOutcome::Ok { offset: None }),
            append("elsewhere", 5, AppendOutcome::Ok { offset: Some(0) }),
        ],
        ..History::default()
    };
    let by_shard = acked_by_shard(&history, 2);
    assert_eq!(by_shard[&0], vec![acked(Some(0), 1)]);
    assert_eq!(by_shard[&1], vec![acked(None, 4)]);
    assert_eq!(by_shard.len(), 2);
}
