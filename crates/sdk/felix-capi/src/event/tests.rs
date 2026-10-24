use std::ptr;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::Event;

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

fn event(publisher: Option<&str>, timestamp_micros: Option<u64>) -> FelixEvent {
    FelixEvent::from(Event {
        tenant_id: Arc::from("t"),
        namespace: Arc::from("n"),
        stream: Arc::from("s"),
        payload: Bytes::from_static(b"x"),
        offset: Some(4),
        skipped_before: 0,
        publisher: publisher.map(Arc::from),
        timestamp_micros,
    })
}

#[test]
fn a_publisher_and_time_reach_c_when_the_broker_sent_them() {
    let ev = event(Some("alice"), Some(1_700_000_000_123_456));
    let (mut data, mut len) = (ptr::null(), 0usize);
    // SAFETY: a live event and writable out-pointers.
    let status = unsafe { felix_event_publisher(&ev, &mut data, &mut len) };
    assert_eq!(status, FELIX_STATUS_OK);
    // SAFETY: `len` bytes valid while `ev` lives.
    assert_eq!(unsafe { std::slice::from_raw_parts(data, len) }, b"alice");

    let (mut micros, mut has) = (0u64, false);
    // SAFETY: as above.
    let status = unsafe { felix_event_timestamp(&ev, &mut micros, &mut has) };
    assert_eq!(status, FELIX_STATUS_OK);
    assert!(has);
    assert_eq!(micros, 1_700_000_000_123_456);
}

/// A broker that did not negotiate either is not an error.
#[test]
fn an_absent_publisher_and_time_read_as_absent() {
    let ev = event(None, None);
    let (mut data, mut len) = (std::ptr::dangling::<u8>(), 9usize);
    // SAFETY: a live event and writable out-pointers.
    let status = unsafe { felix_event_publisher(&ev, &mut data, &mut len) };
    assert_eq!(status, FELIX_STATUS_OK);
    assert!(data.is_null());
    assert_eq!(len, 0);

    let (mut micros, mut has) = (7u64, true);
    // SAFETY: as above.
    let status = unsafe { felix_event_timestamp(&ev, &mut micros, &mut has) };
    assert_eq!(status, FELIX_STATUS_OK);
    assert!(!has);
    assert_eq!(micros, 0);
}

#[test]
fn accessors_refuse_null_arguments() {
    let ev = event(None, None);
    let mut len = 0usize;
    // SAFETY: the out-pointer under test is null on purpose.
    let status = unsafe { felix_event_publisher(&ev, ptr::null_mut(), &mut len) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: a null event is the case under test.
    let status = unsafe { felix_event_timestamp(ptr::null(), ptr::null_mut(), ptr::null_mut()) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}
