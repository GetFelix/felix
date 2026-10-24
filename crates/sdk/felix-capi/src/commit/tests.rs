use std::ffi::CStr;
use std::sync::Mutex;

use super::*;
use crate::status::{FELIX_STATUS_ERROR, FELIX_STATUS_INVALID_ARGUMENT};

fn op(
    kind: FelixCommitOpKind,
    stream: &CStr,
    key: Option<&CStr>,
    data: &'static [u8],
) -> FelixCommitOp {
    FelixCommitOp {
        kind,
        stream: stream.as_ptr(),
        key: key.map_or(ptr::null(), CStr::as_ptr),
        data: if data.is_empty() {
            ptr::null()
        } else {
            data.as_ptr()
        },
        data_len: data.len(),
    }
}

fn read(ops: &[FelixCommitOp]) -> Result<Vec<CommitOp>, Failure> {
    // SAFETY: `ops` and the strings and payloads in it are live.
    unsafe { read_ops(ops.as_ptr(), ops.len()) }
}

#[test]
fn operations_are_read_by_kind() {
    let parsed = read(&[
        op(FELIX_COMMIT_PUBLISH, c"s", None, b"event"),
        op(FELIX_COMMIT_PUT, c"s", Some(c"k"), b"value"),
        op(FELIX_COMMIT_DELETE, c"s", Some(c"gone"), b""),
    ])
    .expect("a valid commit");
    assert_eq!(
        parsed,
        [
            CommitOp::publish("s", Bytes::from_static(b"event")),
            CommitOp::put("s", "k", Bytes::from_static(b"value")),
            CommitOp::delete("s", "gone"),
        ]
    );
    let queued = read(&[op(FELIX_COMMIT_ENQUEUE, c"q", None, b"")]).expect("enqueue");
    assert_eq!(queued, [CommitOp::enqueue("q", Bytes::new())]);
}

fn refused(ops: &[FelixCommitOp]) -> String {
    let failure = read(ops).expect_err("must be refused");
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    failure.message
}

#[test]
fn a_commit_needs_exactly_one_event() {
    assert!(refused(&[]).contains("exactly one event"));
    assert!(refused(&[op(FELIX_COMMIT_PUT, c"s", Some(c"k"), b"v")]).contains("has 0"));
    let two = refused(&[
        op(FELIX_COMMIT_PUBLISH, c"s", None, b"a"),
        op(FELIX_COMMIT_ENQUEUE, c"s", None, b"b"),
    ]);
    assert!(two.contains("has 2"), "{two}");
}

#[test]
fn an_operation_on_another_stream_is_refused() {
    let message = refused(&[
        op(FELIX_COMMIT_PUBLISH, c"orders", None, b"a"),
        op(FELIX_COMMIT_PUT, c"other", Some(c"k"), b"v"),
    ]);
    assert!(
        message.contains("operation 1") && message.contains("other"),
        "{message}"
    );
}

#[test]
fn bad_operations_are_named() {
    let message = refused(&[op(9, c"s", None, b"")]);
    assert!(message.contains("ops[0].kind"), "{message}");

    let message = refused(&[
        op(FELIX_COMMIT_PUBLISH, c"s", None, b"a"),
        op(FELIX_COMMIT_PUT, c"s", None, b"v"),
    ]);
    assert!(message.contains("ops[1].key"), "{message}");

    let no_stream = FelixCommitOp {
        stream: ptr::null(),
        ..op(FELIX_COMMIT_PUBLISH, c"s", None, b"a")
    };
    assert!(refused(&[no_stream]).contains("ops[0].stream"));

    let bad_utf8 = FelixCommitOp {
        stream: c"\xff".as_ptr(),
        ..op(FELIX_COMMIT_PUBLISH, c"s", None, b"a")
    };
    assert!(refused(&[bad_utf8]).contains("UTF-8"));

    let lying = FelixCommitOp {
        data: ptr::null(),
        data_len: 3,
        ..op(FELIX_COMMIT_PUBLISH, c"s", None, b"")
    };
    assert!(refused(&[lying]).contains("ops[0].data"));
}

#[test]
fn fields_a_kind_ignores_are_not_read() {
    // Null with a length would be refused if it were read.
    let garbage = |kind| FelixCommitOp {
        data: ptr::null(),
        data_len: 7,
        ..op(kind, c"s", Some(c"k"), b"")
    };
    let parsed = read(&[
        op(FELIX_COMMIT_PUBLISH, c"s", None, b"a"),
        garbage(FELIX_COMMIT_DELETE),
    ])
    .expect("a delete's data is ignored");
    assert_eq!(parsed[1], CommitOp::delete("s", "k"));

    let message = refused(&[garbage(9)]);
    assert!(message.contains("ops[0].kind"), "{message}");
}

#[test]
fn a_null_array_with_a_count_is_refused() {
    // SAFETY: a null array is rejected before it is read.
    let failure = unsafe { read_ops(ptr::null(), 2) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[test]
fn operations_are_copied() {
    let payload = *b"abc";
    let one = FelixCommitOp {
        data: payload.as_ptr(),
        data_len: 3,
        ..op(FELIX_COMMIT_PUBLISH, c"s", None, b"")
    };
    let parsed = read(&[one]).expect("valid");
    let CommitOp::Publish { payload: copy, .. } = &parsed[0] else {
        panic!("not a publish");
    };
    assert_eq!(&copy[..], b"abc");
    assert_ne!(copy.as_ptr(), payload.as_ptr(), "the payload is copied");
}

#[test]
fn a_conditional_publish_needs_a_record() {
    // SAFETY: nothing is read for a count of zero.
    let failure = unsafe { payload_batch(ptr::null(), ptr::null(), 0) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);

    let lens = [1usize, 2];
    // SAFETY: three live bytes for the two lengths.
    let batch = unsafe { payload_batch(b"abc".as_ptr(), lens.as_ptr(), 2) }.expect("batch");
    assert_eq!(batch, [b"a".to_vec(), b"bc".to_vec()]);
}

#[test]
fn calls_without_a_client_are_refused_before_anything_starts() {
    let s = c"s";
    // SAFETY: the strings are NUL-terminated; the null client is the case.
    let status = unsafe {
        felix_client_commit(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let status = unsafe {
        felix_client_state_get_on_shard(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            s.as_ptr(),
            0,
            s.as_ptr(),
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[derive(Debug, PartialEq)]
enum Seen {
    Commit(u64),
    Conditional(bool, u64),
    State(Option<StateSeen>),
}

#[derive(Debug, PartialEq)]
struct StateSeen {
    value: Option<Vec<u8>>,
    version: Option<u64>,
    as_of: Option<u64>,
}

type Log = Mutex<Vec<(FelixStatus, Seen)>>;

fn log(user_data: *mut c_void) -> &'static Log {
    // SAFETY: tests pass a pointer to a live `Mutex` that outlives the call.
    unsafe { &*user_data.cast() }
}

fn user(log: &Log) -> *mut c_void {
    ptr::from_ref(log).cast_mut().cast()
}

unsafe extern "C" fn on_commit(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    offset: u64,
) {
    log(user_data)
        .lock()
        .unwrap()
        .push((status, Seen::Commit(offset)));
}

unsafe extern "C" fn on_conditional(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    written: bool,
    offset: u64,
) {
    log(user_data)
        .lock()
        .unwrap()
        .push((status, Seen::Conditional(written, offset)));
}

unsafe extern "C" fn on_state(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    state: *const FelixStateValue,
) {
    // SAFETY: null or valid while the callback runs.
    let seen = unsafe { state.as_ref() }.map(|state| StateSeen {
        value: state.has_value.then(|| {
            if state.value.is_null() {
                assert_eq!(state.value_len, 0);
                Vec::new()
            } else {
                // SAFETY: `value_len` readable bytes.
                unsafe { std::slice::from_raw_parts(state.value, state.value_len) }.to_vec()
            }
        }),
        version: state.has_version.then_some(state.version),
        as_of: state.has_as_of.then_some(state.as_of),
    });
    log(user_data)
        .lock()
        .unwrap()
        .push((status, Seen::State(seen)));
}

#[test]
fn a_commit_reports_its_offset() {
    let seen = Mutex::new(Vec::new());
    // SAFETY: the callback is the one this type delivers to.
    unsafe {
        Committed::deliver(
            Some(on_commit),
            user(&seen),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Committed(41)),
        );
        Committed::deliver(
            Some(on_commit),
            user(&seen),
            FELIX_STATUS_ERROR,
            ptr::null(),
            None,
        );
    }
    assert_eq!(
        seen.into_inner().unwrap(),
        [
            (FELIX_STATUS_OK, Seen::Commit(41)),
            (FELIX_STATUS_ERROR, Seen::Commit(0)),
        ]
    );
}

#[test]
fn a_refused_conditional_write_is_an_answer_with_the_tail() {
    let seen = Mutex::new(Vec::new());
    for write in [
        ConditionalWrite::Written { offset: 7 },
        ConditionalWrite::Refused { tail: 9 },
    ] {
        // SAFETY: as above.
        unsafe {
            Conditional::deliver(
                Some(on_conditional),
                user(&seen),
                FELIX_STATUS_OK,
                ptr::null(),
                Some(Conditional(write)),
            );
        }
    }
    assert_eq!(
        seen.into_inner().unwrap(),
        [
            (FELIX_STATUS_OK, Seen::Conditional(true, 7)),
            (FELIX_STATUS_OK, Seen::Conditional(false, 9)),
        ]
    );
}

#[test]
fn state_is_a_borrowed_view_and_null_on_failure() {
    let seen = Mutex::new(Vec::new());
    let states = [
        Some(State(StateValue {
            value: Some(Bytes::from_static(b"placed")),
            version: Some(5),
            as_of: Some(8),
        })),
        Some(State(StateValue {
            value: Some(Bytes::new()),
            version: Some(6),
            as_of: None,
        })),
        Some(State(StateValue {
            value: None,
            version: None,
            as_of: None,
        })),
        None,
    ];
    for state in states {
        // SAFETY: as above.
        unsafe {
            State::deliver(
                Some(on_state),
                user(&seen),
                FELIX_STATUS_OK,
                ptr::null(),
                state,
            );
        }
    }
    let seen: Vec<_> = seen
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|s| s.1)
        .collect();
    assert_eq!(
        seen,
        [
            Seen::State(Some(StateSeen {
                value: Some(b"placed".to_vec()),
                version: Some(5),
                as_of: Some(8),
            })),
            // A key set to an empty value is present, with a null pointer.
            Seen::State(Some(StateSeen {
                value: Some(Vec::new()),
                version: Some(6),
                as_of: None,
            })),
            Seen::State(Some(StateSeen {
                value: None,
                version: None,
                as_of: None,
            })),
            Seen::State(None),
        ]
    );
}
