use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::{Reach, Refusals};
use crate::raft::types::GroupId;

fn shard(n: u32) -> GroupId {
    GroupId::new(
        crate::ShardKey {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
            shard: n,
            kind: crate::ShardKind::Stream,
        },
        0,
    )
}

fn back(reach: &Reach, broker: &str) -> bool {
    reach
        .peers()
        .get(broker)
        .is_some_and(|peer| peer.take_back())
}

/// A broker is down only once its calls keep failing for the streak, and
/// back once, on its first answer after that (#2543).
#[tokio::test(start_paused = true)]
async fn a_broker_is_down_after_a_streak_and_back_on_its_first_answer() {
    let reach = Reach::new(Instant::now(), Duration::from_millis(200));
    let b: Arc<str> = "b".into();
    reach.answered(&b);
    assert!(!back(&reach, &b), "never failed");

    // One slow call, then an answer: not an outage.
    reach.lost(&b, reach.now());
    tokio::time::advance(Duration::from_millis(300)).await;
    reach.answered(&b);
    reach.lost(&b, reach.now());
    assert!(!reach.peers()[&b].is_down());
    reach.answered(&b);
    assert!(!back(&reach, &b), "was never down");

    // Calls sent together that all fail late are one failure.
    let sent = reach.now();
    tokio::time::advance(Duration::from_millis(300)).await;
    reach.lost(&b, sent);
    reach.lost(&b, sent);
    assert!(
        !reach.peers()[&b].is_down(),
        "sent before the failures began"
    );

    tokio::time::advance(Duration::from_millis(100)).await;
    reach.lost(&b, reach.now());
    assert!(!reach.peers()[&b].is_down(), "shorter than the streak");
    tokio::time::advance(Duration::from_millis(100)).await;
    reach.lost(&b, reach.now());
    assert!(reach.peers()[&b].is_down());
    reach.answered(&b);
    reach.answered(&b);
    assert!(back(&reach, &b));
    assert!(!back(&reach, &b), "back once");
}

/// A member that refused is retried on its own first answer, at most once
/// per backoff ceiling, and a broker that never failed holds no state.
#[tokio::test(start_paused = true)]
async fn a_member_that_refused_is_owed_a_retry_on_its_own_answer() {
    let reach = Reach::new(Instant::now(), Duration::from_millis(200));
    let refusals = Refusals::new(shard(1));
    let b: Arc<str> = "b".into();
    assert!(!refusals.answered(&b, reach.now()));
    refusals.refused(&b);
    assert!(refusals.holds(&b));
    assert!(!refusals.holds("c"));
    assert!(!refusals.answered("c", reach.now()), "another broker");
    assert!(refusals.answered(&b, reach.now()));
    assert!(!refusals.holds(&b));
    assert!(!refusals.answered(&b, reach.now()), "once");

    // Refused and answering again within the ceiling: no second retry.
    tokio::time::advance(Duration::from_millis(500)).await;
    refusals.refused(&b);
    assert!(!refusals.answered(&b, reach.now()));
    tokio::time::advance(Duration::from_millis(600)).await;
    refusals.refused(&b);
    assert!(refusals.answered(&b, reach.now()));

    reach.opened(&b, &refusals.group);
    assert_eq!(reach.peers()[&b].take_opened(), vec![shard(1)]);
}

/// A broker nothing has failed to for a while is forgotten.
#[tokio::test(start_paused = true)]
async fn an_idle_broker_is_forgotten() {
    let reach = Reach::new(Instant::now(), Duration::from_millis(200));
    let b: Arc<str> = "b".into();
    reach.lost(&b, reach.now());
    reach.forget_idle();
    assert!(reach.peers().contains_key(&b));
    tokio::time::advance(Duration::from_secs(31)).await;
    reach.forget_idle();
    assert!(reach.peers().is_empty());
}

/// A member's retry delays are openraft's own until its broker is down, then
/// keep doubling to `WHILE_DOWN`, and are openraft's again once it answers
/// (#2567).
#[tokio::test(start_paused = true)]
async fn retries_to_a_broker_that_is_down_wait_longer() {
    use super::{WHILE_DOWN, Waits};
    let reach = Arc::new(Reach::new(Instant::now(), Duration::from_millis(200)));
    let b: Arc<str> = "b".into();
    let usual = openraft::network::Backoff::new(std::iter::repeat(Duration::from_secs(1)));
    let mut waits = Waits::new(usual, Arc::clone(&reach), Arc::clone(&b));
    assert_eq!(waits.next(), Some(Duration::from_secs(1)), "never failed");

    reach.lost(&b, reach.now());
    tokio::time::advance(Duration::from_millis(200)).await;
    reach.lost(&b, reach.now());
    assert!(reach.down(&b));
    let down: Vec<_> = waits.by_ref().take(5).collect();
    assert_eq!(
        down,
        [2, 4, 8, 10, 10].map(Duration::from_secs),
        "doubling to {WHILE_DOWN:?}"
    );

    reach.answered(&b);
    assert_eq!(waits.next(), Some(Duration::from_secs(1)), "back");
}
