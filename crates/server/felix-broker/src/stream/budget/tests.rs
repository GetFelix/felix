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

#[test]
fn a_budget_above_four_gib_is_not_cut_to_four_gib() {
    const GIB: usize = 1 << 30;
    let sub = bytes(0, 10 * GIB + 1).for_subscriber();
    let broker = sub.broker.as_ref().expect("broker budget");
    assert_eq!(broker.available(), 10 * GIB);
    let held: Vec<_> = (0..9)
        .map(|_| sub.try_hold(GIB).expect("nine GiB fit in ten"))
        .collect();
    assert_eq!(broker.available(), GIB);
    let tenth = sub.try_hold(GIB).expect("the tenth GiB fits");
    assert_eq!(sub.try_hold(GIB).err(), Some(ByteBound::Broker));
    drop(held);
    assert_eq!(broker.available(), 9 * GIB);
    drop(tenth);
}

#[test]
fn a_large_budget_rounds_each_batch_up_to_a_whole_unit() {
    let sub = bytes(0, 8 << 30).for_subscriber();
    let broker = sub.broker.as_ref().expect("broker budget");
    let held = sub.try_hold(1).expect("room");
    assert_eq!(broker.available(), (8 << 30) - 4);
    drop(held);
    assert_eq!(broker.available(), 8 << 30);
}

#[tokio::test]
async fn both_bounds_above_four_gib_hold_and_wait_in_bytes() {
    const GIB: usize = 1 << 30;
    let sub = bytes(6 * GIB, 12 * GIB).for_subscriber();
    assert_eq!(sub.own_available(), Some(6 * GIB));
    let first = sub.hold(5 * GIB).await;
    assert_eq!(sub.try_hold(2 * GIB).err(), Some(ByteBound::Subscriber));
    let waiting = tokio::spawn({
        let sub = sub.clone();
        async move { sub.hold(2 * GIB).await }
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "waits on its own bound");
    drop(first);
    let second = waiting.await.expect("hold");
    assert_eq!(sub.own_available(), Some(4 * GIB));
    drop(second);
    // A batch over the whole bound still goes through an empty queue.
    let oversize = sub.try_hold(20 * GIB).expect("empty queue");
    assert_eq!(sub.own_available(), Some(0));
    drop(oversize);
    assert_eq!(sub.own_available(), Some(6 * GIB));
}
