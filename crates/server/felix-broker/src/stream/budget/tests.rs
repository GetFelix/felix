use super::*;

fn bytes(per_subscriber: usize, broker: usize) -> QueueBytes {
    QueueBytes {
        per_subscriber,
        broker: ByteBudget::new(broker),
    }
}

#[test]
fn a_zero_bound_holds_nothing_back() {
    let sub = bytes(0, 0).for_subscriber();
    assert_eq!(sub.own_available(), None);
    let _held: Vec<_> = (0..100)
        .map(|_| sub.try_hold(1 << 20).expect("unbounded"))
        .collect();
}

#[test]
fn the_subscriber_bound_turns_away_what_does_not_fit_and_takes_it_back_on_drop() {
    let sub = bytes(100, 0).for_subscriber();
    let first = sub.try_hold(60).expect("fits");
    assert_eq!(sub.own_available(), Some(40));
    assert_eq!(sub.try_hold(41).unwrap_err(), ByteBound::Subscriber);
    let second = sub.try_hold(40).expect("fits exactly");
    assert_eq!(sub.own_available(), Some(0));
    drop(first);
    drop(second);
    assert_eq!(sub.own_available(), Some(100));
}

#[test]
fn a_batch_larger_than_the_bound_goes_through_an_empty_queue_only() {
    let sub = bytes(100, 0).for_subscriber();
    let small = sub.try_hold(1).expect("fits");
    assert_eq!(sub.try_hold(1_000).unwrap_err(), ByteBound::Subscriber);
    drop(small);
    let big = sub.try_hold(1_000).expect("empty queue takes it");
    assert_eq!(sub.own_available(), Some(0));
    assert_eq!(sub.try_hold(1).unwrap_err(), ByteBound::Subscriber);
    drop(big);
    assert_eq!(sub.own_available(), Some(100));
}

/// The broker budget is shared, so one subscriber holding it turns another
/// away, and a refusal there gives back what the subscriber's own budget
/// had already granted.
#[test]
fn the_broker_budget_is_shared_and_a_refusal_holds_nothing() {
    let queues = bytes(100, 150);
    let slow = queues.for_subscriber();
    let fast = queues.for_subscriber();
    let _slow_held = slow.try_hold(100).expect("fits both");
    assert_eq!(fast.try_hold(60).unwrap_err(), ByteBound::Broker);
    assert_eq!(fast.own_available(), Some(100));
    let _fast_held = fast.try_hold(50).expect("what is left of the broker's");
}

#[tokio::test]
async fn hold_waits_for_room() {
    let sub = bytes(10, 0).for_subscriber();
    let first = sub.try_hold(10).expect("fits");
    let waiter = {
        let sub = sub.clone();
        tokio::spawn(async move { sub.hold(5).await })
    };
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(first);
    let _held = waiter.await.expect("join");
    assert_eq!(sub.own_available(), Some(5));
}
