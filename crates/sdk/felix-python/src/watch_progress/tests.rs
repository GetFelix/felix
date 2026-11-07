use super::SubscriptionProgress;

#[test]
fn a_new_progress_reads_as_nothing_happened() {
    let progress = SubscriptionProgress::default();
    progress.store(0, 0, false, None);
    assert_eq!(
        (
            progress.moves(),
            progress.reconnects(),
            progress.stream_deleted()
        ),
        (0, 0, false)
    );
    assert_eq!(progress.queue_capacity(), None);
}

#[test]
fn each_read_replaces_what_the_last_one_stored() {
    let progress = SubscriptionProgress::default();
    progress.store(0, 0, false, Some(8));
    assert_eq!(progress.queue_capacity(), Some(8));

    progress.store(1, 2, true, Some(3));
    assert_eq!(
        (
            progress.moves(),
            progress.reconnects(),
            progress.stream_deleted()
        ),
        (1, 2, true)
    );
    // A resubscribe the broker granted nothing for must not keep the old one.
    assert_eq!(progress.queue_capacity(), Some(3));
    progress.store(1, 3, true, None);
    assert_eq!(progress.queue_capacity(), None);
    assert_eq!(progress.reconnects(), 3);
}

#[test]
fn the_largest_capacity_is_not_mistaken_for_none() {
    let progress = SubscriptionProgress::default();
    progress.store(0, 0, false, Some(u32::MAX));
    assert_eq!(progress.queue_capacity(), Some(u32::MAX));
}
