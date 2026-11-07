use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

fn record(payload: &[u8], key: &[u8], headers: &[FelixHeader]) -> FelixRecord {
    FelixRecord {
        size: size_of::<FelixRecord>(),
        payload: payload.as_ptr(),
        payload_len: payload.len(),
        key: if key.is_empty() {
            ptr::null()
        } else {
            key.as_ptr()
        },
        key_len: key.len(),
        headers: if headers.is_empty() {
            ptr::null()
        } else {
            headers.as_ptr()
        },
        header_count: headers.len(),
    }
}

fn header(name: &'static str, value: &'static [u8]) -> FelixHeader {
    FelixHeader {
        name: name.as_ptr(),
        name_len: name.len(),
        value: if value.is_empty() {
            ptr::null()
        } else {
            value.as_ptr()
        },
        value_len: value.len(),
    }
}

#[test]
fn a_record_keeps_its_key_headers_and_their_order() {
    let headers = [
        header("trace", b"abc"),
        header("trace", b""),
        header("kind", b"x"),
    ];
    let rec = record(b"body", b"k1", &headers);
    // SAFETY: a live record whose pointers are live.
    let parsed = unsafe { parse_record(&rec, rec.size) }.expect("parsed");
    assert_eq!(parsed.payload, b"body");
    assert_eq!(parsed.headers.key.as_deref(), Some(&b"k1"[..]));
    let names: Vec<_> = parsed
        .headers
        .headers
        .iter()
        .map(|(n, _)| n.as_str())
        .collect();
    assert_eq!(names, ["trace", "trace", "kind"]);
    assert_eq!(&parsed.headers.headers[0].1[..], b"abc");
    assert!(parsed.headers.headers[1].1.is_empty());
}

#[test]
fn a_plain_record_has_empty_headers() {
    let rec = record(b"body", b"", &[]);
    // SAFETY: as above.
    let parsed = unsafe { parse_record(&rec, rec.size) }.expect("parsed");
    assert!(parsed.headers.is_empty());
}

#[test]
fn a_record_that_cannot_be_read_is_refused() {
    let rec = record(b"x", b"", &[]);
    // SAFETY: the size is smaller than the struct, which is the case.
    let small = unsafe { parse_record(&rec, size_of::<usize>()) }
        .err()
        .unwrap();
    assert_eq!(small.status, FELIX_STATUS_INVALID_ARGUMENT);

    let counted = FelixRecord {
        header_count: 2,
        ..record(b"x", b"", &[])
    };
    // SAFETY: the null headers with a count is the case.
    let failure = unsafe { parse_record(&counted, counted.size) }
        .err()
        .unwrap();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(
        failure.message.contains("header_count"),
        "{}",
        failure.message
    );

    let name = [0xffu8, 0xfe];
    let bad = [FelixHeader {
        name: name.as_ptr(),
        name_len: 2,
        value: ptr::null(),
        value_len: 0,
    }];
    let rec = record(b"x", b"", &bad);
    // SAFETY: live pointers; the name is not UTF-8.
    let failure = unsafe { parse_record(&rec, rec.size) }.err().unwrap();
    assert!(failure.message.contains("UTF-8"), "{}", failure.message);
}

/// A caller built against a later header has a larger struct. The reader
/// reads the fields it knows and steps by the caller's size.
#[test]
fn a_larger_record_than_this_library_knows_still_reads() {
    #[repr(C)]
    struct Later {
        base: FelixRecord,
        extra: u64,
    }
    let mut first = record(b"one", b"", &[]);
    first.size = size_of::<Later>();
    let mut second = record(b"two", b"", &[]);
    second.size = size_of::<Later>();
    let rows = [
        Later {
            base: first,
            extra: 9,
        },
        Later {
            base: second,
            extra: 9,
        },
    ];
    // SAFETY: two records of this stride, whose pointers are live.
    let batch = unsafe { parse_batch(ptr::from_ref(&rows).cast(), 2) }.expect("parsed");
    let payloads: Vec<_> = batch.into_iter().map(|(_, payload)| payload).collect();
    assert_eq!(payloads, [b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn a_batch_is_checked_before_it_is_read() {
    // SAFETY: nothing is read for a count of zero.
    assert!(unsafe { parse_batch(ptr::null(), 0) }.unwrap().is_empty());
    // SAFETY: a null array with a count is the case.
    let failure = unsafe { parse_batch(ptr::null(), 2) }.err().unwrap();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    let mut small = record(b"x", b"", &[]);
    small.size = 8;
    // SAFETY: a record whose size is below the first version is the case.
    let failure = unsafe { parse_batch(&small, 1) }.err().unwrap();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[test]
fn the_record_layout_is_the_headers() {
    assert_eq!(size_of::<FelixHeader>(), 32);
    assert_eq!(size_of::<FelixRecord>(), 56);
    assert_eq!(offset_of!(FelixRecord, key), 24);
    assert_eq!(offset_of!(FelixRecord, headers), 40);
    assert_eq!(offset_of!(FelixRecord, header_count), 48);
}

#[test]
fn publishing_without_a_client_is_refused() {
    let s = c"x";
    let rec = record(b"x", b"", &[]);
    // SAFETY: NUL-terminated strings and a live record; the null client is the case.
    let status = unsafe {
        felix_client_publish_with_headers(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            s.as_ptr(),
            &rec,
            1,
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let status = unsafe {
        felix_client_publish_batch_with_headers(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            s.as_ptr(),
            &rec,
            1,
            1,
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}
