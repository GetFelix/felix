use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use bytes::Bytes;
use felix_client::StreamRecord;

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

type Seen = Mutex<Vec<(FelixStatus, Vec<(u64, Vec<u8>, Option<String>, u64)>, u64)>>;

unsafe extern "C" fn page_cb(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    records: *const FelixStreamRecord,
    count: usize,
    next_offset: u64,
) {
    // SAFETY: the test passes a live `Seen`.
    let seen = unsafe { &*user_data.cast::<Seen>() };
    let rows = (0..count)
        .map(|i| {
            // SAFETY: `records` holds `count` entries while the callback runs.
            let r = unsafe { &*records.add(i) };
            let payload = if r.payload.is_null() {
                Vec::new()
            } else {
                // SAFETY: `payload_len` bytes, valid while the callback runs.
                unsafe { std::slice::from_raw_parts(r.payload, r.payload_len) }.to_vec()
            };
            let publisher = (!r.publisher.is_null()).then(|| {
                // SAFETY: `publisher_len` UTF-8 bytes.
                String::from_utf8(
                    unsafe {
                        std::slice::from_raw_parts(r.publisher.cast::<u8>(), r.publisher_len)
                    }
                    .to_vec(),
                )
                .unwrap()
            });
            (r.offset, payload, publisher, r.timestamp_micros)
        })
        .collect();
    seen.lock().unwrap().push((status, rows, next_offset));
}

#[test]
fn a_page_reaches_c_with_its_publishers_times_and_next_offset() {
    let seen: Seen = Mutex::new(Vec::new());
    let page = StreamPage {
        records: vec![
            StreamRecord {
                offset: 4,
                payload: Bytes::from_static(b"a"),
                publisher: Some("alice".into()),
                timestamp_micros: 11,
            },
            StreamRecord {
                offset: 6,
                payload: Bytes::new(),
                publisher: None,
                timestamp_micros: 12,
            },
        ],
        next_offset: 7,
    };
    // SAFETY: the callback's contract; `seen` outlives the call.
    unsafe {
        Page::deliver(
            Some(page_cb),
            ptr::from_ref(&seen).cast_mut().cast(),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Page(page)),
        );
    }
    let seen = seen.lock().unwrap();
    assert_eq!(
        seen[0],
        (
            FELIX_STATUS_OK,
            vec![
                (4, b"a".to_vec(), Some("alice".to_string()), 11),
                (6, Vec::new(), None, 12)
            ],
            7
        )
    );
}

#[test]
fn a_failed_read_has_no_records() {
    let seen: Seen = Mutex::new(Vec::new());
    // SAFETY: as above.
    unsafe {
        Page::deliver(
            Some(page_cb),
            ptr::from_ref(&seen).cast_mut().cast(),
            FELIX_STATUS_INVALID_ARGUMENT,
            ptr::null(),
            None,
        );
    }
    assert_eq!(
        seen.lock().unwrap()[0],
        (FELIX_STATUS_INVALID_ARGUMENT, vec![], 0)
    );
}

#[test]
fn a_read_without_a_client_is_refused() {
    let s: &CStr = c"x";
    // SAFETY: NUL-terminated strings; the null client is the case.
    let status = unsafe {
        felix_client_read(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            s.as_ptr(),
            0,
            0,
            false,
            0,
            0,
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}
