use std::ptr;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{Event, RecordHeaders};

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
        headers: None,
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

/// Null means absent, so an empty principal must read as absent too.
#[test]
fn an_empty_publisher_reads_as_absent() {
    let ev = event(Some(""), None);
    let (mut data, mut len) = (std::ptr::dangling::<u8>(), 9usize);
    // SAFETY: a live event and writable out-pointers.
    let status = unsafe { felix_event_publisher(&ev, &mut data, &mut len) };
    assert_eq!(status, FELIX_STATUS_OK);
    assert!(data.is_null());
    assert_eq!(len, 0);
}

fn with_headers(headers: Option<RecordHeaders>) -> FelixEvent {
    let mut ev = event(None, None);
    ev.headers = headers;
    ev
}

#[test]
fn a_key_and_headers_reach_c_in_order() {
    let ev = with_headers(Some(RecordHeaders {
        key: Some(Bytes::from_static(b"k")),
        headers: vec![
            ("a".to_string(), Bytes::from_static(b"1")),
            ("a".to_string(), Bytes::new()),
        ],
    }));
    let (mut data, mut len) = (ptr::null(), 0usize);
    // SAFETY: a live event and writable out-pointers.
    assert_eq!(
        unsafe { felix_event_key(&ev, &mut data, &mut len) },
        FELIX_STATUS_OK
    );
    // SAFETY: `len` bytes valid while `ev` lives.
    assert_eq!(unsafe { std::slice::from_raw_parts(data, len) }, b"k");

    let mut count = 0usize;
    // SAFETY: as above.
    assert_eq!(
        unsafe { felix_event_header_count(&ev, &mut count) },
        FELIX_STATUS_OK
    );
    assert_eq!(count, 2);

    let (mut n, mut nl, mut v, mut vl) = (ptr::null(), 0usize, ptr::null(), 0usize);
    // SAFETY: as above.
    let status = unsafe { felix_event_header(&ev, 0, &mut n, &mut nl, &mut v, &mut vl) };
    assert_eq!(status, FELIX_STATUS_OK);
    // SAFETY: valid while `ev` lives.
    unsafe {
        assert_eq!(std::slice::from_raw_parts(n, nl), b"a");
        assert_eq!(std::slice::from_raw_parts(v, vl), b"1");
    }
    // SAFETY: as above.
    let status = unsafe { felix_event_header(&ev, 1, &mut n, &mut nl, &mut v, &mut vl) };
    assert_eq!(status, FELIX_STATUS_OK);
    assert!(v.is_null());
    assert_eq!(vl, 0);

    // SAFETY: as above; the index is out of range.
    let status = unsafe { felix_event_header(&ev, 2, &mut n, &mut nl, &mut v, &mut vl) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[test]
fn an_event_without_headers_reads_as_plain() {
    let ev = with_headers(None);
    let (mut data, mut len) = (std::ptr::dangling::<u8>(), 5usize);
    // SAFETY: a live event and writable out-pointers.
    assert_eq!(
        unsafe { felix_event_key(&ev, &mut data, &mut len) },
        FELIX_STATUS_OK
    );
    assert!(data.is_null());
    assert_eq!(len, 0);
    let mut count = 7usize;
    // SAFETY: as above.
    assert_eq!(
        unsafe { felix_event_header_count(&ev, &mut count) },
        FELIX_STATUS_OK
    );
    assert_eq!(count, 0);
}
