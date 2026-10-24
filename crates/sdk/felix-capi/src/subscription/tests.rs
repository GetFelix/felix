use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

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
