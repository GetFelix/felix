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
