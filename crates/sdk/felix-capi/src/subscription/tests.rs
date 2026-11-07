use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;
use std::ptr;

#[test]
fn start_kinds_map_and_unknown_is_refused() {
    assert!(matches!(
        start_position(FELIX_START_LATEST, 9),
        Ok(StartPosition::Latest)
    ));
    assert!(matches!(
        start_position(FELIX_START_EARLIEST, 9),
        Ok(StartPosition::Earliest)
    ));
    assert!(matches!(
        start_position(FELIX_START_OFFSET, 9),
        Ok(StartPosition::Offset(9))
    ));
    let failure = start_position(7, 0).unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[test]
fn a_deleted_stream_is_told_from_one_that_ended() {
    assert_eq!(close_reason(true), FELIX_CLOSE_STREAM_DELETED);
    assert_eq!(close_reason(false), FELIX_CLOSE_ENDED);
}

#[test]
fn a_lease_dropped_without_its_subscription_closes_the_slot_with_its_reason() {
    let slot = Arc::new(Mutex::new(Slot::Reading));
    drop(Lease {
        slot: Arc::clone(&slot),
        subscription: None,
        closed: FELIX_CLOSE_STREAM_DELETED,
    });
    assert!(matches!(
        *lock(&slot),
        Slot::Closed(FELIX_CLOSE_STREAM_DELETED)
    ));
}

fn handle(stats: Arc<Stats>) -> FelixSubscription {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let shared = Arc::new(Shared::new(runtime));
    FelixSubscription {
        slot: Arc::new(Mutex::new(Slot::Closed(FELIX_CLOSE_ENDED))),
        start_offset: None,
        live_offset: None,
        stats,
        cancel: shared.cancel_token(),
        shared,
    }
}

fn stats(moves: u64, reconnects: u64, deleted: bool, capacity: Option<u32>) -> Arc<Stats> {
    let stats = Arc::new(Stats {
        moves: AtomicU64::new(0),
        reconnects: AtomicU64::new(0),
        stream_deleted: AtomicBool::new(false),
        queue_capacity: AtomicU64::new(NO_CAPACITY),
    });
    stats.set(moves, reconnects, deleted, capacity);
    stats
}

#[test]
fn the_accessors_report_what_the_last_read_recorded() {
    let sub = handle(stats(3, 2, true, Some(64)));
    let (mut moves, mut reconnects, mut deleted) = (0u64, 0u64, false);
    let (mut capacity, mut has) = (0u32, false);
    // SAFETY: the handle is live and each out-pointer is writable.
    unsafe {
        assert_eq!(felix_subscription_moves(&sub, &mut moves), FELIX_STATUS_OK);
        assert_eq!(
            felix_subscription_reconnects(&sub, &mut reconnects),
            FELIX_STATUS_OK
        );
        assert_eq!(
            felix_subscription_stream_deleted(&sub, &mut deleted),
            FELIX_STATUS_OK
        );
        assert_eq!(
            felix_subscription_queue_capacity(&sub, &mut capacity, &mut has),
            FELIX_STATUS_OK
        );
    }
    assert_eq!((moves, reconnects, deleted), (3, 2, true));
    assert_eq!((capacity, has), (64, true));

    sub.stats.set(4, 2, true, None);
    // SAFETY: as above.
    unsafe {
        felix_subscription_queue_capacity(&sub, &mut capacity, &mut has);
        felix_subscription_moves(&sub, &mut moves);
    }
    assert_eq!((capacity, has, moves), (0, false, 4));
}

#[test]
fn a_capacity_of_zero_is_a_capacity() {
    let sub = handle(stats(0, 0, false, Some(0)));
    let (mut capacity, mut has) = (9u32, false);
    // SAFETY: as above.
    unsafe { felix_subscription_queue_capacity(&sub, &mut capacity, &mut has) };
    assert_eq!((capacity, has), (0, true));
}

#[test]
fn the_accessors_check_their_pointers() {
    let sub = handle(stats(0, 0, false, None));
    let (mut n, mut flag, mut cap) = (0u64, false, 0u32);
    // SAFETY: null pointers are the case; none is read or written.
    unsafe {
        assert_eq!(
            felix_subscription_moves(ptr::null(), &mut n),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_subscription_moves(&sub, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_subscription_reconnects(ptr::null(), &mut n),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_subscription_reconnects(&sub, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_subscription_stream_deleted(ptr::null(), &mut flag),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_subscription_stream_deleted(&sub, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            felix_subscription_queue_capacity(ptr::null(), &mut cap, ptr::null_mut()),
            FELIX_STATUS_INVALID_ARGUMENT
        );
        // Both out-pointers are optional.
        assert_eq!(
            felix_subscription_queue_capacity(&sub, ptr::null_mut(), ptr::null_mut()),
            FELIX_STATUS_OK
        );
    }
}
