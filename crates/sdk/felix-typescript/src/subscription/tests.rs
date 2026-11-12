use super::Progress;

#[test]
fn a_new_subscription_reports_no_granted_capacity() {
    assert_eq!(Progress::default().queue_capacity(), None);
}

#[test]
fn capacity_follows_the_current_subscription_after_a_move_or_reconnect() {
    let progress = Progress::default();
    progress.store(Some(8), 0, 0, false);
    assert_eq!(progress.queue_capacity(), Some(8));
    // The resubscribe after a move was granted a different queue.
    progress.store(Some(32), 1, 0, false);
    assert_eq!(progress.queue_capacity(), Some(32));
    // And a broker that grants none puts it back to the default.
    progress.store(None, 1, 1, false);
    assert_eq!(progress.queue_capacity(), None);
    assert_eq!(progress.moves.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        progress
            .reconnects
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}
