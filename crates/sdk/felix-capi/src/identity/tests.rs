use std::ptr;

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

#[test]
fn an_identity_needs_a_client_and_a_token() {
    let s = c"x";
    // SAFETY: NUL-terminated strings; the null client is the case.
    let status = unsafe {
        felix_client_with_identity(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

unsafe extern "C" fn never_fetch(_: *mut c_void, _: *mut crate::token::FelixTokenReply) {
    panic!("a refused call must not fetch");
}

unsafe extern "C" fn count_release(data: *mut c_void) {
    // SAFETY: the test passes a live counter.
    unsafe { &*data.cast::<std::sync::atomic::AtomicUsize>() }
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

#[test]
fn a_refused_provider_identity_never_releases_the_callers_data() {
    let releases = std::sync::atomic::AtomicUsize::new(0);
    let s = c"x";
    // SAFETY: NUL-terminated strings and a live counter; the null client is the case.
    let status = unsafe {
        felix_client_with_identity_provider(
            ptr::null(),
            s.as_ptr(),
            ptr::null(),
            Some(never_fetch),
            ptr::from_ref(&releases).cast_mut().cast(),
            Some(count_release),
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
    assert_eq!(releases.load(std::sync::atomic::Ordering::SeqCst), 0);
}
