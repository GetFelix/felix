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

/// A broker serving at `generation` whose quorum mark is at its log's tail.
fn served(name: &str, generation: u64, records: &[(u64, u64)]) -> Served {
    let tail = records.last().map(|(offset, _)| offset + 1);
    served_to(name, generation, tail, records)
}

fn served_to(
    name: &str,
    generation: u64,
    committed: Option<u64>,
    records: &[(u64, u64)],
) -> Served {
    Served {
        name: name.to_string(),
        generation: Some(generation),
        committed,
        held: held(records),
    }
}

#[test]
fn a_deposed_leader_missing_an_acknowledged_record_is_reported() {
    let told = [acked(Some(0), 10), acked(Some(1), 11), acked(Some(3), 13)];
    // b1 is current and has all; b2 serves at an older generation and has a
    // hole at offset 1 inside its log.
    let both = [
        served("b1", 2, &[(0, 10), (1, 11), (2, 12), (3, 13)]),
        served("b2", 1, &[(0, 10), (2, 12)]),
    ];
    let found = missing_by_server(&told, &both, Some(2));
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].0.name, "b2");
    assert_eq!(found[0].1, vec![acked(Some(1), 11)]);
}

#[test]
fn a_deposed_leader_is_not_held_to_what_its_successor_acknowledged() {
    // Offset 3 was acknowledged by the generation-2 leader; b2 stopped at 1.
    let told = [acked(Some(0), 10), acked(Some(3), 13)];
    let both = [
        served("b1", 2, &[(0, 10), (1, 12), (3, 13)]),
        served("b2", 1, &[(0, 10), (1, 12)]),
    ];
    assert!(missing_by_server(&told, &both, Some(2)).is_empty());
}

#[test]
fn the_newest_leader_must_hold_every_acknowledged_record() {
    let told = [acked(Some(0), 10), acked(Some(3), 13), acked(None, 14)];
    let alone = [served("b1", 2, &[(0, 10), (1, 12)])];
    let found = missing_by_server(&told, &alone, Some(2));
    assert_eq!(found[0].1, vec![acked(Some(3), 13), acked(None, 14)]);
    // A newer generation exists that nobody serves: b1 is deposed, and its
    // log is complete up to its end.
    assert!(missing_by_server(&told, &alone, Some(3)).is_empty());
}

#[test]
fn a_deposed_leader_is_not_held_past_its_quorum_mark() {
    // b2 wrote value 22 at offset 1 at generation 1 but no majority took it;
    // b1 started generation 2 at offset 1 and acknowledged value 11 there.
    let told = [acked(Some(0), 10), acked(Some(1), 11)];
    let both = [
        served("b1", 2, &[(0, 10), (1, 11)]),
        served_to("b2", 1, Some(1), &[(0, 10), (1, 22)]),
    ];
    assert!(missing_by_server(&told, &both, Some(2)).is_empty());
    // Below its mark the same difference is a lost acknowledgement.
    let both = [served_to("b2", 1, Some(2), &[(0, 10), (1, 22)])];
    let found = missing_by_server(&told, &both, Some(2));
    assert_eq!(found[0].1, vec![acked(Some(1), 11)]);
}

#[test]
fn after_the_heal_a_lone_deposed_server_is_waited_for_then_held_to_everything() {
    use crate::check::Point;
    let alone = [served("b1", 2, &[(0, 10)])];
    assert_eq!(newest_held(Point::Quiet, &alone, Some(3)), Some(Some(3)));
    assert_eq!(newest_held(Point::Healed, &alone, Some(3)), None);
    assert_eq!(newest_held(Point::End, &alone, Some(3)), Some(None));
    assert_eq!(newest_held(Point::End, &alone, Some(2)), Some(Some(2)));
    // At the end b1 is held to the record it lacks at the tail.
    let told = [acked(Some(0), 10), acked(Some(1), 11)];
    let newest = newest_held(Point::End, &alone, Some(3)).expect("checked");
    let found = missing_by_server(&told, &alone, newest);
    assert_eq!(found[0].1, vec![acked(Some(1), 11)]);
}
