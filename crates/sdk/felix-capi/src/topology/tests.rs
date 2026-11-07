use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::Mutex;

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

type Seen = Mutex<Vec<(FelixStatus, Vec<(String, usize)>)>>;

unsafe extern "C" fn connections_cb(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    addrs: *const *const c_char,
    counts: *const usize,
    len: usize,
) {
    // SAFETY: the test passes a live `Seen`.
    let seen = unsafe { &*user_data.cast::<Seen>() };
    let rows = (0..len)
        .map(|i| {
            // SAFETY: `len` entries in each array while the callback runs.
            unsafe {
                (
                    CStr::from_ptr(*addrs.add(i)).to_str().unwrap().to_string(),
                    *counts.add(i),
                )
            }
        })
        .collect();
    seen.lock().unwrap().push((status, rows));
}

#[test]
fn connections_reach_c_as_addresses_and_counts() {
    let seen: Seen = Mutex::new(Vec::new());
    let rows = vec![
        (CString::new("10.0.0.1:4433").unwrap(), 1),
        (CString::new("10.0.0.2:4433").unwrap(), 3),
    ];
    // SAFETY: the callback's contract; `seen` outlives the call.
    unsafe {
        Connections::deliver(
            Some(connections_cb),
            ptr::from_ref(&seen).cast_mut().cast(),
            FELIX_STATUS_OK,
            ptr::null(),
            Some(Connections(rows)),
        );
    }
    assert_eq!(
        seen.lock().unwrap()[0],
        (
            FELIX_STATUS_OK,
            vec![
                ("10.0.0.1:4433".to_string(), 1),
                ("10.0.0.2:4433".to_string(), 3)
            ]
        )
    );
}

#[test]
fn calls_without_a_client_are_refused() {
    // SAFETY: the null client is the case.
    let status =
        unsafe { felix_client_refresh_topology(ptr::null(), ptr::null(), None, ptr::null_mut()) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let status = unsafe {
        felix_client_connections_per_node(ptr::null(), ptr::null(), None, ptr::null_mut())
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}
