use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use felix_client::Seed;

use super::{Endpoints, parse_addrs, resolve_names};
use crate::boundary::Failure;
use crate::completion::Completion;
use crate::status::{FELIX_STATUS_AUTH, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_OK};

/// Names reach the client as names, so it can resolve them again later.
#[test]
fn names_are_kept_as_names() {
    let seeds = parse_addrs("127.0.0.1:5000, localhost:5000").expect("parse");
    assert_eq!(
        seeds,
        [
            Seed::Addr("127.0.0.1:5000".parse().unwrap()),
            Seed::Name("localhost:5000".to_string()),
        ]
    );
}

#[test]
fn an_address_without_a_port_is_an_invalid_argument() {
    let failure = parse_addrs("localhost").unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

/// A name that does not resolve is still the caller's bad argument.
#[tokio::test]
async fn a_name_that_does_not_resolve_is_an_invalid_argument() {
    let seeds = parse_addrs("felix.invalid:5000").expect("parse");
    let failure = resolve_names(&seeds).await.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(
        failure.message.contains("could not resolve"),
        "{}",
        failure.message
    );
}

type Seen = (i32, Option<Vec<String>>);

unsafe extern "C" fn listed(
    user_data: *mut c_void,
    status: i32,
    _message: *const c_char,
    addrs: *const *const c_char,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    let addrs = (!addrs.is_null()).then(|| {
        // SAFETY: `count` NUL-terminated strings, valid while this runs.
        unsafe { std::slice::from_raw_parts(addrs, count) }
            .iter()
            .map(|&addr| {
                unsafe { CStr::from_ptr(addr) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    });
    calls.lock().unwrap().push((status, addrs));
}

/// The addresses reach C as strings in order, and a failure carries none.
#[test]
fn endpoints_are_handed_over_as_strings() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    Completion::<Endpoints>::new(Some(listed), data)
        .unwrap()
        .ok(Endpoints::new(vec![
            "127.0.0.1:5000".to_string(),
            "[::1]:5001".to_string(),
        ]));
    Completion::<Endpoints>::new(Some(listed), data)
        .unwrap()
        .fail(Failure {
            status: FELIX_STATUS_AUTH,
            message: "denied".to_string(),
        });
    let calls = calls.into_inner().unwrap();
    assert_eq!(
        calls[0],
        (
            FELIX_STATUS_OK,
            Some(vec!["127.0.0.1:5000".to_string(), "[::1]:5001".to_string()])
        )
    );
    assert_eq!(calls[1], (FELIX_STATUS_AUTH, None));
}

#[test]
fn endpoints_refuses_a_null_client_and_a_null_callback() {
    // SAFETY: null is the case under test.
    let status = unsafe {
        super::felix_client_endpoints(
            std::ptr::null(),
            std::ptr::null(),
            Some(listed),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}
