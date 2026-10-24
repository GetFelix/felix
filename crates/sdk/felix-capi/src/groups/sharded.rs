//! A consumer group read across every shard of a stream.
//!
//! Settling a record needs nothing new: the per-shard calls
//! (`felix_client_group_ack` and the rest) already take a shard, and a
//! record from here names the one it came from.

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use felix_client::{ShardedGroup, ShardedGroupRecord};
use tokio_util::sync::CancellationToken;

use super::{FelixGroupRecord, group_target, record_row, spawn_read, spawn_read_on};
use crate::boundary::{Failure, guard, guard_void, write_optional};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// A consumer group over every shard of a stream, from
/// `felix_client_group_sharded`.
///
/// Safe to use from several threads at once, but not while another thread
/// frees it. Freeing it cancels a poll still pending on it.
pub struct FelixShardedGroup {
    /// Taken in `Drop`, so the last reference to the client is released
    /// inside the runtime.
    group: Option<Arc<ShardedGroup>>,
    shards: u32,
    /// Cancelled when this handle is freed, which ends a pending poll.
    cancel: CancellationToken,
    shared: Arc<Shared>,
}

impl Drop for FelixShardedGroup {
    fn drop(&mut self) {
        self.cancel.cancel();
        let _guard = self.shared.handle().enter();
        drop(self.group.take());
    }
}

/// One record from a sharded group, with the shard it was claimed from.
///
/// Settle it with `felix_client_group_ack`, `_nack`, `_extend` or
/// `_dead_letter`, passing `shard` and the record's `offset` and `attempts`:
/// an offset means nothing without its shard.
#[repr(C)]
pub struct FelixShardedGroupRecord {
    pub shard: u32,
    /// Borrowed, as in `FelixGroupPollCallback`.
    pub record: FelixGroupRecord,
}

/// Called once when `felix_client_group_sharded` finishes. On
/// `FELIX_STATUS_OK`, `group` is a new group the callee owns; otherwise it is
/// null.
pub type FelixShardedGroupCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        group: *mut FelixShardedGroup,
    ),
>;

/// Called once when `felix_sharded_group_poll` finishes.
///
/// `records` holds `count` entries and is valid, with every payload in it,
/// only while the callback runs; copy what you keep. An empty poll is
/// `FELIX_STATUS_OK` with a count of zero. `records` is null on failure.
pub type FelixShardedGroupPollCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        records: *const FelixShardedGroupRecord,
        count: usize,
    ),
>;

impl Deliver for Box<FelixShardedGroup> {
    type Callback = FelixShardedGroupCallback;

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
            let group = value.map_or(ptr::null_mut(), Box::into_raw);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, group) };
        }
    }
}

pub(crate) struct ShardedPolled(Vec<ShardedGroupRecord>);

impl Deliver for ShardedPolled {
    type Callback = FelixShardedGroupPollCallback;

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
        let Some(ShardedPolled(records)) = value else {
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, ptr::null(), 0) };
            return;
        };
        let rows: Vec<FelixShardedGroupRecord> = records
            .iter()
            .map(|r| FelixShardedGroupRecord {
                shard: r.shard,
                record: record_row(&r.record),
            })
            .collect();
        // SAFETY: as above; `rows` points into `records`, which live until
        // the callback returns.
        unsafe { callback(user_data, status, message, rows.as_ptr(), rows.len()) };
    }
}

/// Open a consumer group across every shard of a stream. The shard count is
/// asked of the broker, so an unknown stream fails here. This creates no
/// group: use `felix_client_group_create_stream` for that.
///
/// A read: cancelled, it completes with `FELIX_STATUS_CANCELLED`.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated,
/// `cancel` is null or a live token, and `callback` is safe to call with
/// `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_sharded(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    group: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixShardedGroupCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Box<FelixShardedGroup>>::new(callback, user_data)?;
        // The new handle keeps the runtime alive, so the task holds the
        // shared state rather than only the client.
        let shared = Arc::clone(&client.shared);
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target.inner.group_sharded(t, n, s, &target.group).await
        };
        spawn_read(client, user, done, ask, move |group| {
            Box::new(FelixShardedGroup {
                shards: group.shards(),
                group: Some(Arc::new(group)),
                cancel: shared.cancel_token(),
                shared,
            })
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// How many shards the group spans.
///
/// # Safety
///
/// `group` is a live sharded group and `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_group_shards(
    group: *const FelixShardedGroup,
    out: *mut u32,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live group, per the contract above.
        let group = unsafe { group.as_ref() }.ok_or_else(|| Failure::invalid("group is null"))?;
        if out.is_null() {
            return Err(Failure::invalid("out is null"));
        }
        // SAFETY: both checked non-null; `out` is writable per the contract.
        unsafe { write_optional(out, group.shards) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Take up to `max_records` (at least one) from the group's shards, visiting
/// them in turn from where the last poll left off, and returning the first
/// non-empty batch. An empty answer means no shard had anything.
///
/// Order holds within a shard and nowhere across shards. A shard that cannot
/// be polled fails the whole poll, since skipping it would look exactly like
/// a shard with nothing to deliver. Settle each record on its own `shard`
/// with the per-shard group calls.
///
/// A read: cancelled, or with the group freed, it completes with
/// `FELIX_STATUS_CANCELLED`, and anything it claimed in the meantime comes
/// back after the visibility timeout.
///
/// # Safety
///
/// `group` is a live sharded group, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_group_poll(
    group: *const FelixShardedGroup,
    max_records: u32,
    cancel: *const FelixCancelToken,
    callback: FelixShardedGroupPollCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live group, per the contract above.
        let handle = unsafe { group.as_ref() }.ok_or_else(|| Failure::invalid("group is null"))?;
        if max_records == 0 {
            return Err(Failure::invalid("max_records is zero"));
        }
        let inner = handle
            .group
            .clone()
            .ok_or_else(|| Failure::invalid("group is closed"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<ShardedPolled>::new(callback, user_data)?;
        let ask = async move { inner.poll(max_records).await };
        spawn_read_on(
            &handle.shared,
            handle.cancel.clone(),
            user,
            done,
            ask,
            ShardedPolled,
        );
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a sharded group. Null is ignored.
///
/// A poll still pending on it completes with `FELIX_STATUS_CANCELLED`,
/// possibly after this returns.
///
/// # Safety
///
/// `group` is null or a pointer from a `felix_client_group_sharded` callback
/// that has not been freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_group_free(group: *mut FelixShardedGroup) {
    guard_void(|| {
        if !group.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the callback
            // and is freed exactly once.
            drop(unsafe { Box::from_raw(group) });
        }
    });
}

#[cfg(test)]
mod tests;
