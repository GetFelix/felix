use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use super::*;
use crate::status::{FELIX_STATUS_ERROR, FELIX_STATUS_INVALID_ARGUMENT};

#[test]
fn condition_kinds_map_and_unknown_is_refused() {
    assert_eq!(
        condition(FELIX_IF_ABSENT, 9).unwrap(),
        CacheCondition::Absent
    );
    assert_eq!(
        condition(FELIX_IF_VERSION, 9).unwrap(),
        CacheCondition::Version(9)
    );
    let failure = condition(7, 0).unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains('7'), "{}", failure.message);
}

#[test]
fn zero_ttl_means_no_expiry() {
    assert_eq!(ttl(0), None);
    assert_eq!(ttl(1500), Some(1500));
}

#[test]
fn a_value_may_be_empty_with_a_null_pointer_but_not_missing() {
    // SAFETY: nothing is read for a length of zero.
    let empty = unsafe { copy_value(ptr::null(), 0) }.expect("empty");
    assert!(empty.is_empty());

    // SAFETY: a null pointer with a length is rejected before it is read.
    let failure = unsafe { copy_value(ptr::null(), 3) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);

    let data = *b"abc";
    // SAFETY: points at three live bytes.
    let copied = unsafe { copy_value(data.as_ptr(), 3) }.expect("copied");
    assert_eq!(&copied[..], b"abc");
    assert_ne!(copied.as_ptr(), data.as_ptr(), "the value is copied");
}

#[test]
fn a_call_without_a_client_is_refused_before_anything_starts() {
    let key = c"k";
    // SAFETY: the strings are NUL-terminated; the null client is the case.
    let status = unsafe {
        felix_client_cache_get(
            ptr::null(),
            key.as_ptr(),
            key.as_ptr(),
            key.as_ptr(),
            key.as_ptr(),
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[derive(Debug, PartialEq)]
enum Seen {
    Value(FelixStatus, bool, Option<Vec<u8>>),
    Versioned(bool, Option<Vec<u8>>, u64),
    Condition(bool, bool, u64),
    Counter(bool, i64),
}

fn log(user_data: *mut c_void) -> &'static Mutex<Vec<(Seen, Option<String>)>> {
    // SAFETY: tests pass a pointer to a live `Mutex` that outlives the call.
    unsafe { &*user_data.cast() }
}

unsafe fn text(message: *const c_char) -> Option<String> {
    // SAFETY: null or NUL-terminated, per the callbacks' contract.
    (!message.is_null()).then(|| {
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    })
}

unsafe fn bytes_of(at: *const u8, len: usize) -> Option<Vec<u8>> {
    // SAFETY: `len` readable bytes when non-null.
    (!at.is_null()).then(|| unsafe { std::slice::from_raw_parts(at, len) }.to_vec())
}

unsafe extern "C" fn on_value(
    user_data: *mut c_void,
    status: FelixStatus,
    message: *const c_char,
    found: bool,
    value: *const u8,
    len: usize,
) {
    // SAFETY: the arguments are the callback's own.
    let (message, value) = unsafe { (text(message), bytes_of(value, len)) };
    log(user_data)
        .lock()
        .unwrap()
        .push((Seen::Value(status, found, value), message));
}

unsafe extern "C" fn on_versioned(
    user_data: *mut c_void,
    _status: FelixStatus,
    message: *const c_char,
    found: bool,
    value: *const u8,
    len: usize,
    version: u64,
) {
    // SAFETY: the arguments are the callback's own.
    let (message, value) = unsafe { (text(message), bytes_of(value, len)) };
    assert_eq!(found, value.is_some() || len == 0 && found);
    log(user_data)
        .lock()
        .unwrap()
        .push((Seen::Versioned(found, value, version), message));
}

unsafe extern "C" fn on_condition(
    user_data: *mut c_void,
    _status: FelixStatus,
    message: *const c_char,
    applied: bool,
    has_version: bool,
    version: u64,
) {
    // SAFETY: the arguments are the callback's own.
    let message = unsafe { text(message) };
    log(user_data)
        .lock()
        .unwrap()
        .push((Seen::Condition(applied, has_version, version), message));
}

unsafe extern "C" fn on_counter(
    user_data: *mut c_void,
    _status: FelixStatus,
    message: *const c_char,
    has_value: bool,
    value: i64,
) {
    // SAFETY: the argument is the callback's own.
    let message = unsafe { text(message) };
    log(user_data)
        .lock()
        .unwrap()
        .push((Seen::Counter(has_value, value), message));
}

fn user(log: &Mutex<Vec<(Seen, Option<String>)>>) -> *mut c_void {
    ptr::from_ref(log).cast_mut().cast()
}

#[test]
fn values_are_found_empty_or_missing() {
    let seen = Mutex::new(Vec::new());
    for value in [
        Some(Value(Some(Bytes::from_static(b"hello")))),
        Some(Value(Some(Bytes::new()))),
        Some(Value(None)),
        None,
    ] {
        // SAFETY: the callback is the one this type delivers to.
        unsafe {
            Value::deliver(
                Some(on_value),
                user(&seen),
                FELIX_STATUS_OK,
                ptr::null(),
                value,
            );
        }
    }
    let seen: Vec<_> = seen
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|s| s.0)
        .collect();
    assert_eq!(
        seen,
        [
            Seen::Value(FELIX_STATUS_OK, true, Some(b"hello".to_vec())),
            // A found, empty value has a null pointer and a length of zero.
            Seen::Value(FELIX_STATUS_OK, true, None),
            Seen::Value(FELIX_STATUS_OK, false, None),
            Seen::Value(FELIX_STATUS_OK, false, None),
        ]
    );
}

#[test]
fn a_failed_read_reports_its_message_and_nothing_found() {
    let seen = Mutex::new(Vec::new());
    // SAFETY: as above.
    unsafe {
        Value::deliver(
            Some(on_value),
            user(&seen),
            FELIX_STATUS_ERROR,
            c"boom".as_ptr(),
            None,
        );
    }
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen[0].0, Seen::Value(FELIX_STATUS_ERROR, false, None));
    assert_eq!(seen[0].1.as_deref(), Some("boom"));
}

#[test]
fn versioned_values_carry_their_version() {
    let seen = Mutex::new(Vec::new());
    let entry = VersionedValue {
        value: Bytes::from_static(b"v"),
        version: 42,
    };
    // SAFETY: as above.
    unsafe {
        Versioned::deliver(
            Some(on_versioned),
            user(&seen),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Versioned(Some(entry))),
        );
        Versioned::deliver(
            Some(on_versioned),
            user(&seen),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Versioned(None)),
        );
    }
    let seen: Vec<_> = seen
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|s| s.0)
        .collect();
    assert_eq!(
        seen,
        [
            Seen::Versioned(true, Some(b"v".to_vec()), 42),
            Seen::Versioned(false, None, 0),
        ]
    );
}

#[test]
fn a_refused_condition_is_not_an_error() {
    let seen = Mutex::new(Vec::new());
    let results = [
        CacheConditionResult {
            applied: true,
            version: Some(5),
        },
        CacheConditionResult {
            applied: false,
            version: Some(6),
        },
        CacheConditionResult {
            applied: false,
            version: None,
        },
    ];
    for result in results {
        // SAFETY: as above.
        unsafe {
            Condition::deliver(
                Some(on_condition),
                user(&seen),
                FELIX_STATUS_OK,
                ptr::null(),
                Some(Condition(result)),
            );
        }
    }
    let seen: Vec<_> = seen
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|s| s.0)
        .collect();
    assert_eq!(
        seen,
        [
            Seen::Condition(true, true, 5),
            Seen::Condition(false, true, 6),
            Seen::Condition(false, false, 0),
        ]
    );
}

#[test]
fn a_missing_counter_has_no_value_and_zero_is_a_value() {
    let seen = Mutex::new(Vec::new());
    for counter in [Counter(Some(-3)), Counter(Some(0)), Counter(None)] {
        // SAFETY: as above.
        unsafe {
            Counter::deliver(
                Some(on_counter),
                user(&seen),
                FELIX_STATUS_OK,
                ptr::null(),
                Some(counter),
            );
        }
    }
    let seen: Vec<_> = seen
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|s| s.0)
        .collect();
    assert_eq!(
        seen,
        [
            Seen::Counter(true, -3),
            Seen::Counter(true, 0),
            Seen::Counter(false, 0),
        ]
    );
}
