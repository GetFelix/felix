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
