use std::time::Duration;

use super::*;

#[test]
fn each_subscription_gets_an_equal_share() {
    let budget = Arc::new(EgressBudget::new(100));
    let a = budget.join();
    let b = budget.join();
    let held = a.try_hold(50).expect("within the share");
    assert!(a.try_hold(1).is_none(), "the share is full");
    let other = b.try_hold(50).expect("b's share is its own");
    drop(held);
    assert!(a.try_hold(50).is_some());
    drop(other);
}

#[test]
fn a_frame_larger_than_the_share_goes_once_nothing_else_waits() {
    let budget = Arc::new(EgressBudget::new(100));
    let share = budget.join();
    let small = share.try_hold(1).expect("fits");
    assert!(share.try_hold(1_000).is_none());
    drop(small);
    let big = share.try_hold(1_000).expect("alone it goes");
    assert!(share.try_hold(1).is_none());
    drop(big);
}

#[test]
fn the_share_grows_when_a_subscription_leaves() {
    let budget = Arc::new(EgressBudget::new(100));
    let a = budget.join();
    let b = budget.join();
    let held = a.try_hold(40).expect("fits");
    assert!(a.try_hold(40).is_none());
    drop(b);
    let more = a.try_hold(40).expect("the whole budget is a's now");
    drop((held, more));
}

#[test]
fn zero_is_unbounded() {
    let budget = Arc::new(EgressBudget::new(0));
    let share = budget.join();
    let _held: Vec<_> = (0..10)
        .map(|_| share.try_hold(1 << 30).expect("unbounded"))
        .collect();
}

#[tokio::test]
async fn hold_waits_for_the_subscriptions_own_frames() {
    let budget = Arc::new(EgressBudget::new(100));
    let share = budget.join();
    let mut held = share.hold(60).await;
    held.absorb(share.try_hold(30).expect("fits"));
    let waiting = tokio::time::timeout(Duration::from_millis(20), share.hold(20)).await;
    assert!(waiting.is_err(), "no room yet");
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(held);
    });
    let _next = tokio::time::timeout(Duration::from_secs(5), share.hold(20))
        .await
        .expect("woken when the frame was written");
    release.await.expect("release");
}
