use reqwest::StatusCode;

use super::*;
use crate::shards::ShardKind;
use crate::test_support::fake_link::FakeLink;

mod control_plane;

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn at(term: u64, index: u64) -> LogPoint {
    LogPoint { term, index }
}

/// What each sent request carried, as (stream, incarnation, index).
fn sent(link: &FakeLink) -> Vec<Vec<(String, u64, u64)>> {
    link.floors()
        .iter()
        .map(|batch| {
            batch
                .iter()
                .map(|floor| (floor.key.stream.clone(), floor.incarnation, floor.index))
                .collect()
        })
        .collect()
}

fn accepting() -> FakeLink {
    FakeLink::default().reply("report_raft_floors", serde_json::json!({ "raised": 0 }))
}

#[tokio::test]
async fn one_request_carries_every_moved_group_and_none_goes_when_nothing_moved() {
    let link = accepting();
    let mut floors = Floors::default();
    let led = || vec![(key("a"), 1, at(1, 5)), (key("b"), 1, at(1, 9))];

    tick(&mut floors, led(), &link, "n1", None).await;
    tick(&mut floors, led(), &link, "n1", None).await;

    assert_eq!(sent(&link), [vec![("a".into(), 1, 5), ("b".into(), 1, 9)]]);
}

#[tokio::test]
async fn a_refused_report_is_sent_again_and_cleared_by_a_200() {
    let link = accepting().status("report_raft_floors", StatusCode::CONFLICT);
    let mut floors = Floors::default();
    // Idle: neither group commits anything across the ticks.
    let led = || vec![(key("a"), 1, at(1, 5)), (key("b"), 1, at(1, 9))];

    tick(&mut floors, led(), &link, "n1", None).await;
    tick(&mut floors, led(), &link, "n1", None).await;
    link.clear_status("report_raft_floors");
    tick(&mut floors, led(), &link, "n1", None).await;
    tick(&mut floors, led(), &link, "n1", None).await;

    let both = vec![("a".into(), 1, 5), ("b".into(), 1, 9)];
    assert_eq!(sent(&link), [both.clone(), both.clone(), both]);
}

#[tokio::test]
async fn an_unreachable_control_plane_leaves_the_floors_owed() {
    // No reply set: the fake answers 503.
    let link = FakeLink::default();
    let mut floors = Floors::default();

    tick(
        &mut floors,
        vec![(key("a"), 1, at(1, 5))],
        &link,
        "n1",
        None,
    )
    .await;
    tick(
        &mut floors,
        vec![(key("a"), 1, at(1, 5))],
        &link,
        "n1",
        None,
    )
    .await;

    assert_eq!(sent(&link).len(), 2);
}

#[tokio::test]
async fn only_the_groups_that_moved_are_sent_again() {
    let link = accepting();
    let mut floors = Floors::default();

    tick(
        &mut floors,
        vec![(key("a"), 1, at(1, 5)), (key("b"), 1, at(1, 9))],
        &link,
        "n1",
        None,
    )
    .await;
    tick(
        &mut floors,
        vec![(key("a"), 1, at(1, 5)), (key("b"), 1, at(2, 12))],
        &link,
        "n1",
        None,
    )
    .await;

    assert_eq!(
        sent(&link),
        [
            vec![("a".into(), 1, 5), ("b".into(), 1, 9)],
            vec![("b".into(), 1, 12)],
        ]
    );
}

#[test]
fn a_group_that_moved_after_its_batch_was_cut_stays_owed() {
    let mut floors = Floors::default();
    floors.observe([(key("a"), 1, at(1, 5))]);
    let batch = floors.batch();
    floors.observe([(key("a"), 1, at(1, 6))]);

    floors.accepted(&batch);

    let owed: Vec<_> = floors.batch().iter().map(|floor| floor.index).collect();
    assert_eq!(owed, [6]);
}

#[test]
fn each_incarnation_of_a_shard_is_its_own_entry() {
    let mut floors = Floors::default();
    floors.observe([(key("a"), 1, at(1, 5)), (key("a"), 2, at(1, 3))]);

    let owed: Vec<_> = floors
        .batch()
        .iter()
        .map(|floor| (floor.incarnation, floor.index))
        .collect();

    assert_eq!(owed, [(1, 5), (2, 3)]);
}

#[test]
fn a_stopped_group_drops_what_it_owed() {
    let mut floors = Floors::default();
    floors.observe([(key("a"), 1, at(1, 5)), (key("b"), 1, at(1, 9))]);

    // The shard came back at a new incarnation, and "b" stopped here.
    floors.observe([(key("a"), 2, at(1, 1))]);

    let owed: Vec<_> = floors
        .batch()
        .iter()
        .map(|floor| (floor.key.stream.clone(), floor.incarnation))
        .collect();
    assert_eq!(owed, [("a".to_string(), 2)]);
}

#[test]
fn a_group_led_here_again_is_owed_once_more() {
    let mut floors = Floors::default();
    floors.observe([(key("a"), 1, at(1, 5))]);
    let batch = floors.batch();
    floors.accepted(&batch);
    assert!(floors.batch().is_empty());

    floors.observe([]);
    floors.observe([(key("a"), 1, at(1, 5))]);

    assert_eq!(floors.batch().len(), 1);
}

#[test]
fn a_batch_is_bounded() {
    let mut floors = Floors::default();
    floors.observe((0..MAX_BATCH as u64 + 3).map(|i| (key(&format!("s{i:05}")), 1, at(1, i))));

    let first = floors.batch();
    floors.accepted(&first);

    assert_eq!(first.len(), MAX_BATCH);
    assert_eq!(floors.batch().len(), 3);
}
