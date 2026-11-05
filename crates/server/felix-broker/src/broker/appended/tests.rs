use super::*;

#[test]
fn a_marked_shard_is_taken_once() {
    let appended = AppendedShards::default();
    let slot = AppendedShards::slot("t", "ns", "orders", 3);
    assert_eq!(slot, AppendedShards::slot("t", "ns", "orders", 3));
    appended.mark(slot);
    assert!(appended.take().contains(slot));
    assert!(!appended.take().contains(slot));
}

#[test]
fn an_unmarked_shard_is_not_taken() {
    let appended = AppendedShards::default();
    let marked = AppendedShards::slot("t", "ns", "orders", 0);
    let other = (0..u32::MAX)
        .map(|shard| AppendedShards::slot("t", "ns", "orders", shard))
        .find(|slot| *slot != marked)
        .expect("another bit");
    appended.mark(marked);
    let taken = appended.take();
    assert!(taken.contains(marked));
    assert!(!taken.contains(other));
}

#[tokio::test]
async fn a_mark_wakes_a_waiter_that_came_later() {
    let appended = AppendedShards::default();
    appended.mark(7);
    // The permit `notify_one` left.
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        appended.wake().notified(),
    )
    .await
    .expect("woken");
}

#[tokio::test]
async fn only_the_mark_that_sets_a_bit_wakes() {
    let appended = AppendedShards::default();
    appended.mark(7);
    appended.wake().notified().await;
    // Still set, so its first mark's wake covers it: no second permit.
    appended.mark(7);
    let again = appended.wake();
    let notified = again.notified();
    tokio::pin!(notified);
    assert!(!notified.as_mut().enable(), "an already set bit woke again");
    assert!(appended.take().contains(7));
    // Cleared by the take, so the next mark wakes.
    appended.mark(7);
    assert!(notified.as_mut().enable());
}

#[test]
fn slots_lists_every_marked_bit_once() {
    let appended = AppendedShards::default();
    for slot in [0, 63, 64, 700, 4095] {
        appended.mark(slot);
    }
    appended.mark(700);
    let taken: Vec<u16> = appended.take().slots().collect();
    assert_eq!(taken, vec![0, 63, 64, 700, 4095]);
    assert_eq!(appended.take().slots().count(), 0);
}
