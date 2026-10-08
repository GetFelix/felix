//! One delivered record, and what C can read from it.

use bytes::Bytes;
use felix_client::Event;

use crate::boundary::{Failure, guard, guard_void, write_optional};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// One record delivered to a subscription. Owns its payload until freed with
/// `felix_event_free`.
pub struct FelixEvent {
    payload: Bytes,
    offset: Option<u64>,
    skipped_before: u64,
}

impl From<Event> for FelixEvent {
    fn from(event: Event) -> Self {
        Self {
            payload: event.payload,
            offset: event.offset,
            skipped_before: event.skipped_before,
        }
    }
}

/// The record's bytes, exactly as published.
///
/// `*out_data` stays valid until the event is freed. It may be null when
/// `*out_len` is zero.
///
/// # Safety
///
/// `event` is a live event, and both out-pointers are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_event_payload(
    event: *const FelixEvent,
    out_data: *mut *const u8,
    out_len: *mut usize,
) -> FelixStatus {
    guard(|| {
        if out_data.is_null() || out_len.is_null() {
            return Err(Failure::invalid("out_data and out_len must not be null"));
        }
        // SAFETY: null or a live event, per the contract above.
        let event = unsafe { live(event) }?;
        // SAFETY: both checked non-null; the caller promises they are
        // writable.
        unsafe {
            out_data.write(event.payload.as_ptr());
            out_len.write(event.payload.len());
        }
        Ok(FELIX_STATUS_OK)
    })
}

/// The record's log offset.
///
/// `*out_has_offset` is false on a stream without a log, and from a broker
/// that did not negotiate offsets. Either out-pointer may be null.
///
/// # Safety
///
/// `event` is a live event, and the out-pointers are null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_event_offset(
    event: *const FelixEvent,
    out_offset: *mut u64,
    out_has_offset: *mut bool,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live event, per the contract above.
        let event = unsafe { live(event) }?;
        // SAFETY: each is null or writable, per the contract above.
        unsafe {
            write_optional(out_has_offset, event.offset.is_some());
            write_optional(out_offset, event.offset.unwrap_or(0));
        }
        Ok(FELIX_STATUS_OK)
    })
}

/// How many offsets just before this one hold no event, such as a new
/// leader's generation-start record. A larger jump between consecutive
/// offsets than this explains is a drop.
///
/// # Safety
///
/// `event` is a live event, and `out_skipped` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_event_skipped_before(
    event: *const FelixEvent,
    out_skipped: *mut u64,
) -> FelixStatus {
    guard(|| {
        if out_skipped.is_null() {
            return Err(Failure::invalid("out_skipped is null"));
        }
        // SAFETY: null or a live event, per the contract above.
        let event = unsafe { live(event) }?;
        // SAFETY: checked non-null; the caller promises it is writable.
        unsafe { out_skipped.write(event.skipped_before) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Free an event and its payload. Null is ignored.
///
/// # Safety
///
/// `event` is null or a pointer from `felix_subscription_next_event` that has
/// not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_event_free(event: *mut FelixEvent) {
    guard_void(|| {
        if !event.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in
            // `felix_subscription_next_event` and is freed exactly once.
            drop(unsafe { Box::from_raw(event) });
        }
    });
}

/// # Safety
///
/// `event` is null or a live event that outlives `'a`.
unsafe fn live<'a>(event: *const FelixEvent) -> Result<&'a FelixEvent, Failure> {
    // SAFETY: the caller's contract, passed through.
    unsafe { event.as_ref() }.ok_or_else(|| Failure::invalid("event is null"))
}
