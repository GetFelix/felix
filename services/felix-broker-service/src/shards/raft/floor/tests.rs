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

/// A group at epoch 0, as every group is until it re-forms.
impl From<(ShardKey, u64, LogPoint)> for Led {
    fn from((key, incarnation, at): (ShardKey, u64, LogPoint)) -> Self {
        Led {
            key,
            incarnation,
            epoch: 0,
            at,
        }
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

    floors.observe(Vec::<Led>::new());
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

/// More groups than one batch holds, all writing every tick: each is still
/// sent within ceil(n / MAX_BATCH) ticks rather than only the first ones.
#[test]
fn every_group_is_sent_in_turn_when_more_move_than_one_batch_holds() {
    let groups = MAX_BATCH as u64 + 476;
    let ticks = groups.div_ceil(MAX_BATCH as u64);
    let mut floors = Floors::default();
    let mut sent = std::collections::BTreeSet::new();
    for tick in 1..=ticks {
        floors.observe((0..groups).map(|i| (key(&format!("s{i:05}")), 1, at(1, tick))));
        let batch = floors.batch();
        floors.accepted(&batch);
        sent.extend(batch.into_iter().map(|floor| floor.key.stream));
    }
    assert_eq!(sent.len() as u64, groups);
}

#[test]
fn a_floor_carries_the_group_epoch_and_a_new_epoch_is_owed_again() {
    let mut floors = Floors::default();
    let led = |epoch| Led {
        key: key("a"),
        incarnation: 1,
        epoch,
        at: at(1, 5),
    };
    floors.observe([led(2)]);
    let batch = floors.batch();
    assert_eq!(
        batch.iter().map(|floor| floor.epoch).collect::<Vec<_>>(),
        [2]
    );
    floors.accepted(&batch);
    floors.observe([led(2)]);
    assert!(floors.batch().is_empty());

    // Same log id, next epoch: the control plane has not seen this group.
    floors.observe([led(3)]);
    assert_eq!(
        floors
            .batch()
            .iter()
            .map(|floor| floor.epoch)
            .collect::<Vec<_>>(),
        [3]
    );
}
