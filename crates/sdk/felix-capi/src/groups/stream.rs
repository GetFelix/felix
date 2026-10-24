//! Group administration across every shard of a stream.
//!
//! Each call is one request per shard, in shard order, so a failure partway
//! leaves the earlier shards changed; every call is safe to repeat.

use std::ffi::{c_char, c_void};
use std::ptr;

use felix_client::{GroupInfo, GroupPosition};

use super::{Deleted, FelixDeletedCallback, FelixGroupInfo, group_target, info_row, spawn_read};
use crate::boundary::Failure;
use crate::boundary::guard;
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::{Completion, Deliver};
use crate::publish::spawn_publish;
use crate::status::{FELIX_STATUS_OK, FelixStatus};
use crate::subscription::{FelixStartKind, start_position};

/// Where a group resumes on one shard.
#[repr(C)]
pub struct FelixGroupPosition {
    /// The offset the group resumes from.
    pub offset: u64,
    /// False when `felix_client_group_create_stream` found the group already
    /// on this shard and left it alone.
    pub moved: bool,
}

/// Called once when `felix_client_group_create_stream` or
/// `felix_client_group_seek_stream` finishes. On `FELIX_STATUS_OK`,
/// `positions` holds `count` entries indexed by shard. The array is valid
/// only while the callback runs, and null on failure.
pub type FelixGroupPositionsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        positions: *const FelixGroupPosition,
        count: usize,
    ),
>;

/// Called once when `felix_client_group_describe_stream` finishes. On
/// `FELIX_STATUS_OK`, `infos` holds `count` entries indexed by shard. The
/// array is valid only while the callback runs, and null on failure.
pub type FelixGroupInfosCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        infos: *const FelixGroupInfo,
        count: usize,
    ),
>;

pub(crate) struct Positions(Vec<GroupPosition>);

impl Deliver for Positions {
    type Callback = FelixGroupPositionsCallback;

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
        let Some(callback) = callback else {
            return;
        };
        let rows = value.map(|positions| position_rows(&positions.0));
        let (at, count) = rows
            .as_deref()
            .map_or((ptr::null(), 0), |rows| (rows.as_ptr(), rows.len()));
        // SAFETY: the callback's contract is the header's; `rows` lives until
        // it returns.
        unsafe { callback(user_data, status, message, at, count) };
    }
}

fn position_rows(positions: &[GroupPosition]) -> Vec<FelixGroupPosition> {
    positions
        .iter()
        .map(|p| FelixGroupPosition {
            offset: p.offset,
            moved: p.moved,
        })
        .collect()
}

pub(crate) struct Infos(Vec<GroupInfo>);

impl Deliver for Infos {
    type Callback = FelixGroupInfosCallback;

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
        let Some(callback) = callback else {
            return;
        };
        let rows: Option<Vec<FelixGroupInfo>> =
            value.map(|infos| infos.0.iter().map(info_row).collect());
        let (at, count) = rows
            .as_deref()
            .map_or((ptr::null(), 0), |rows| (rows.as_ptr(), rows.len()));
        // SAFETY: as above.
        unsafe { callback(user_data, status, message, at, count) };
    }
}

/// Create a group on every shard of a stream at `start`, leaving a shard
/// where it already exists alone (its `moved` is then false).
///
/// `start_offset` is refused for a stream with more than one shard, since an
/// offset means something different on each; use `FELIX_START_EARLIEST` or
/// `FELIX_START_LATEST` there. This is one request per shard, so a failure
/// partway leaves the earlier shards created; the call is safe to repeat.
///
/// A write: cancelled after it may have been sent it completes with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated,
/// `cancel` is null or a live token, and `callback` is safe to call with
/// `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_create_stream(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    group: *const c_char,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixGroupPositionsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        move_stream(
            true,
            (client, tenant_id, ns, stream, group),
            (start, start_offset),
            (cancel, callback, user_data),
        )
    }
}

/// Move a group's cursor on every shard of a stream, as
/// `felix_client_group_create_stream` does for creation. `FELIX_START_LATEST`
/// is each shard's tail when its own request lands, not one cut across them.
///
/// A write; see `felix_client_group_create_stream`. Safe to repeat.
///
/// # Safety
///
/// As for `felix_client_group_create_stream`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_seek_stream(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    group: *const c_char,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixGroupPositionsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        move_stream(
            false,
            (client, tenant_id, ns, stream, group),
            (start, start_offset),
            (cancel, callback, user_data),
        )
    }
}

/// Where a group stands on every shard of a stream, indexed by shard. A read:
/// cancelled, it completes with `FELIX_STATUS_CANCELLED`.
///
/// # Safety
///
/// As for `felix_client_group_create_stream`, with a `FelixGroupInfosCallback`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_describe_stream(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    group: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixGroupInfosCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Infos>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .group_describe_stream(t, n, s, &target.group)
                .await
        };
        spawn_read(client, user, done, ask, Infos);
        Ok(FELIX_STATUS_OK)
    })
}

/// Delete a group, with its cursor and dead letters, on every shard of a
/// stream. The callback says whether any shard had it.
///
/// A write; see `felix_client_group_create_stream`. A failure partway leaves
/// the earlier shards deleted; the call is safe to repeat.
///
/// # Safety
///
/// As for `felix_client_group_create_stream`, with a `FelixDeletedCallback`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_delete_stream(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    group: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixDeletedCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Deleted>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .group_delete_stream(t, n, s, &target.group)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Deleted).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// # Safety
///
/// As for `felix_client_group_create_stream`.
unsafe fn move_stream(
    create: bool,
    (client, tenant_id, ns, stream, group): (
        *const FelixClient,
        *const c_char,
        *const c_char,
        *const c_char,
        *const c_char,
    ),
    (start, start_offset): (FelixStartKind, u64),
    (cancel, callback, user_data): (
        *const FelixCancelToken,
        FelixGroupPositionsCallback,
        *mut c_void,
    ),
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        let start = start_position(start, start_offset)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Positions>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            let (inner, group) = (&target.inner, &target.group);
            if create {
                inner.group_create_stream(t, n, s, group, start).await
            } else {
                inner.group_seek_stream(t, n, s, group, start).await
            }
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Positions).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
