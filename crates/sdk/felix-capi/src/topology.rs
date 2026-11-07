//! Re-learning the cluster's brokers, and how many connections the client
//! holds to each.

use std::ffi::{CString, c_char, c_void};
use std::ptr;
use std::sync::Arc;

use crate::boundary::{Failure, guard};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::{Completion, Deliver};
use crate::groups::spawn_read;
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// Called once when `felix_client_refresh_topology` finishes. On
/// `FELIX_STATUS_OK`, `count` is how many brokers the cluster named; zero is a
/// legitimate answer for a broker with no cluster behind it. Zero on failure.
pub type FelixCountCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        count: usize,
    ),
>;

/// Called once when `felix_client_connections_per_node` finishes. On
/// `FELIX_STATUS_OK`, `addrs[i]` is a NUL-terminated `host:port` and
/// `counts[i]` is the live connections to it, for `len` brokers. Both arrays
/// and the strings are valid only while the callback runs; both are null on
/// failure or when there are none.
pub type FelixConnectionsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        addrs: *const *const c_char,
        counts: *const usize,
        len: usize,
    ),
>;

pub(crate) struct Count(usize);

impl Deliver for Count {
    type Callback = FelixCountCallback;

    fn is_null(callback: Self::Callback) -> bool {
        callback.is_none()
    }

    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        value: Option<Self>,
    ) {
        if let Some(callback) = callback {
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, value.map_or(0, |c| c.0)) };
        }
    }
}

pub(crate) struct Connections(Vec<(CString, usize)>);

impl Deliver for Connections {
    type Callback = FelixConnectionsCallback;

    fn is_null(callback: Self::Callback) -> bool {
        callback.is_none()
    }

    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        value: Option<Self>,
    ) {
        if let Some(callback) = callback {
            let rows = value.map(|c| c.0).unwrap_or_default();
            let addrs: Vec<*const c_char> = rows.iter().map(|(a, _)| a.as_ptr()).collect();
            let counts: Vec<usize> = rows.iter().map(|(_, c)| *c).collect();
            let (a, c) = if rows.is_empty() {
                (ptr::null(), ptr::null())
            } else {
                (addrs.as_ptr(), counts.as_ptr())
            };
            // SAFETY: the callback's contract is the header's; the arrays live
            // until it returns.
            unsafe { callback(user_data, status, message, a, c, rows.len()) };
        }
    }
}

/// Ask a broker which brokers a client may connect to and add them to the
/// endpoints this client tries. Configured seeds are kept whatever the answer
/// was; a broker that has left the cluster stops being tried.
///
/// A broker that predates discovery completes with an error, not a count of
/// zero.
///
/// # Safety
///
/// `client` is a live client, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_refresh_topology(
    client: *const FelixClient,
    cancel: *const FelixCancelToken,
    callback: FelixCountCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Count>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move { cluster.refresh_topology().await };
        spawn_read(client, user, done, ask, Count);
        Ok(FELIX_STATUS_OK)
    })
}

/// List the live connections the client holds to each broker. A broker starts
/// at one connection, shared by all traffic, and gains more only when its
/// streams are saturated.
///
/// # Safety
///
/// As for `felix_client_refresh_topology`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_connections_per_node(
    client: *const FelixClient,
    cancel: *const FelixCancelToken,
    callback: FelixConnectionsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Connections>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move { Ok(cluster.connections_per_node().await) };
        spawn_read(client, user, done, ask, |nodes| {
            Connections(
                nodes
                    .into_iter()
                    .filter_map(|(addr, count)| {
                        CString::new(addr.to_string()).ok().map(|a| (a, count))
                    })
                    .collect(),
            )
        });
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
