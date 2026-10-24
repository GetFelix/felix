//! Subscribing to every shard of a stream at once, and reading the merged
//! feed one item at a time.

use std::ffi::{c_char, c_void};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use felix_client::{ShardEvent, ShardOffsets, ShardedSubscription};
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, guard_void, required_str, write_optional};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver, cancelled, is_cancelled};
use crate::event::FelixEvent;
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_CLOSED, FELIX_STATUS_OK, FELIX_STATUS_TIMEOUT, FelixStatus,
};
use crate::subscription::{FelixStartKind, start_position};

/// What a sharded read delivered.
pub type FelixShardItemKind = i32;

/// No item: the read failed, timed out or was cancelled, which the status says.
pub const FELIX_SHARD_NONE: FelixShardItemKind = 0;
/// A record, in `event`. The callee owns it and frees it with
/// `felix_event_free`.
pub const FELIX_SHARD_RECORD: FelixShardItemKind = 1;
/// The shard's owner stopped answering and the shard is being re-established;
/// the other shards are unaffected. The message holds the error text, and the
/// status is still `FELIX_STATUS_OK`.
pub const FELIX_SHARD_LOST: FelixShardItemKind = 2;
/// The shard is delivering again, resuming after the last offset seen from
/// it.
pub const FELIX_SHARD_RECOVERED: FelixShardItemKind = 3;
/// The shard moved to another broker and is being followed there.
pub const FELIX_SHARD_MOVED: FelixShardItemKind = 4;
/// The stream was deleted. This shard's feed has ended and every other shard
/// ends the same way.
pub const FELIX_SHARD_STREAM_DELETED: FelixShardItemKind = 5;

/// Called once when a sharded subscribe finishes. On `FELIX_STATUS_OK`,
/// `subscription` is a new subscription the callee owns; otherwise it is
/// null.
pub type FelixShardedSubscribeCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        subscription: *mut FelixShardedSubscription,
    ),
>;

/// Called once when `felix_sharded_next` finishes.
///
/// On `FELIX_STATUS_OK`, `kind` says what arrived and `shard` which shard it
/// concerns. `event` is non-null only for `FELIX_SHARD_RECORD`. `message` is
/// valid only while the callback runs; it is set for `FELIX_SHARD_LOST`.
/// With any other status, `kind` is `FELIX_SHARD_NONE`.
pub type FelixShardItemCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        kind: FelixShardItemKind,
        shard: u32,
        event: *mut FelixEvent,
    ),
>;

/// A live subscription to every shard of a stream, merged into one feed.
///
/// Order holds within a shard and nowhere across shards. Losing a shard is
/// an item, not an error: a caller that ignores `FELIX_SHARD_LOST` reads an
/// incomplete stream. One read may be pending at a time. It must not be freed
/// while another thread is calling it.
pub struct FelixShardedSubscription {
    slot: Arc<Mutex<Slot>>,
    /// What the caller has been handed, per shard. Updated by the read that
    /// hands a record out, so it is never ahead of what the caller has been handed and can be read
    /// while another read is pending.
    positions: Arc<Mutex<ShardOffsets>>,
    shards: u32,
    /// Cancelled when this handle is freed, which ends a pending read.
    cancel: CancellationToken,
    shared: Arc<Shared>,
}

enum Slot {
    Idle(Box<ShardedSubscription>),
    /// A read has the subscription.
    Reading,
    /// Every shard's feed has ended.
    Closed,
}

impl Drop for FelixShardedSubscription {
    fn drop(&mut self) {
        self.cancel.cancel();
        // Its shard tasks belong to the client's runtime, so it is dropped
        // inside that runtime. A pending read drops it on its own.
        let _guard = self.shared.handle().enter();
        let idle = std::mem::replace(&mut *lock(&self.slot), Slot::Closed);
        drop(idle);
    }
}

/// A read's hold on the subscription. Dropping it puts the subscription
/// back, so a read that panics or is cancelled leaves it readable.
struct Lease {
    slot: Arc<Mutex<Slot>>,
    /// `None` once every shard's feed has ended.
    subscription: Option<Box<ShardedSubscription>>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let next = self.subscription.take().map_or(Slot::Closed, Slot::Idle);
        *lock(&self.slot) = next;
    }
}

/// One item of the merged feed, as C sees it.
pub(crate) struct ShardItem {
    kind: FelixShardItemKind,
    shard: u32,
    message: Option<String>,
    event: Option<Box<FelixEvent>>,
}

impl ShardItem {
    fn new(kind: FelixShardItemKind, shard: u32) -> Self {
        Self {
            kind,
            shard,
            message: None,
            event: None,
        }
    }

    /// `None` for an item this library does not know yet. `ShardEvent` can
    /// grow, and a caller reading a variant it cannot name is better served
    /// by the next item than by a made-up kind.
    fn from_event(item: ShardEvent) -> Option<Self> {
        Some(match item {
            ShardEvent::Record { shard, event } => Self {
                event: Some(Box::new(FelixEvent::from(event))),
                ..Self::new(FELIX_SHARD_RECORD, shard)
            },
            ShardEvent::ShardLost { shard, error } => Self {
                message: Some(error),
                ..Self::new(FELIX_SHARD_LOST, shard)
            },
            ShardEvent::ShardRecovered { shard } => Self::new(FELIX_SHARD_RECOVERED, shard),
            ShardEvent::ShardMoved { shard, .. } => Self::new(FELIX_SHARD_MOVED, shard),
            ShardEvent::StreamDeleted { shard } => Self::new(FELIX_SHARD_STREAM_DELETED, shard),
            _ => return None,
        })
    }
}

impl Deliver for Box<FelixShardedSubscription> {
    type Callback = FelixShardedSubscribeCallback;

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
            let subscription = value.map_or(std::ptr::null_mut(), Box::into_raw);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, subscription) };
        }
    }
}

impl Deliver for ShardItem {
    type Callback = FelixShardItemCallback;

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
            let item = value.unwrap_or_else(|| Self::new(FELIX_SHARD_NONE, 0));
            let event = item.event.map_or(std::ptr::null_mut(), Box::into_raw);
            // SAFETY: as above.
            unsafe { callback(user_data, status, message, item.kind, item.shard, event) };
        }
    }
}

/// Subscribe to every shard of a stream.
///
/// The shard count is asked of the broker. The call fails, and no
/// subscription is handed out, if any shard cannot be opened, so a
/// subscription never quietly covers fewer shards than the stream has.
/// `start` is one of the `FELIX_START_*` values and applies to every shard;
/// `start_offset` is read only for `FELIX_START_OFFSET`.
///
/// `callback` runs once, unless this returns something other than
/// `FELIX_STATUS_OK`. A cancelled subscribe never hands out a subscription.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated,
/// `cancel` is null or a live token, and `callback` is safe to call with
/// `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_subscribe_sharded(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixShardedSubscribeCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the caller's contract, passed through.
        unsafe {
            open(
                client,
                (tenant_id, ns, stream),
                None,
                (start, start_offset),
                cancel,
                callback,
                user_data,
            )
        }
    })
}

/// Resume a sharded subscription from where each shard had reached.
///
/// `shards` and `offsets` are `count` parallel entries, as
/// `felix_sharded_positions` writes them. Each listed shard resumes at its
/// offset plus one; a shard not listed starts at `start`, which is what a
/// shard that had delivered nothing should do. A shard listed twice is
/// `FELIX_STATUS_INVALID_ARGUMENT`. Otherwise this is
/// `felix_client_subscribe_sharded`.
///
/// # Safety
///
/// As `felix_client_subscribe_sharded`, and `shards` and `offsets` are each
/// null or point to `count` readable values (null only when `count` is
/// zero).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_resubscribe_sharded(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shards: *const u32,
    offsets: *const u64,
    count: usize,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixShardedSubscribeCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the caller's contract, passed through.
        let resume = unsafe { shard_offsets(shards, offsets, count) }?;
        // SAFETY: as above.
        unsafe {
            open(
                client,
                (tenant_id, ns, stream),
                Some(resume),
                (start, start_offset),
                cancel,
                callback,
                user_data,
            )
        }
    })
}

/// # Safety
///
/// The contract of `felix_client_subscribe_sharded`.
unsafe fn open(
    client: *const FelixClient,
    target: (*const c_char, *const c_char, *const c_char),
    resume: Option<ShardOffsets>,
    start: (FelixStartKind, u64),
    cancel: *const FelixCancelToken,
    callback: FelixShardedSubscribeCallback,
    user_data: *mut c_void,
) -> Result<FelixStatus, Failure> {
    // SAFETY: null or a live client, per the contract.
    let client = unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
    // SAFETY: each is null or a NUL-terminated string.
    let (tenant_id, ns, stream) = unsafe {
        (
            required_str(target.0, "tenant_id")?.to_string(),
            required_str(target.1, "ns")?.to_string(),
            required_str(target.2, "stream")?.to_string(),
        )
    };
    let start = start_position(start.0, start.1)?;
    // SAFETY: null or a live token, per the contract.
    let user = unsafe { user_token(cancel) };

    // The new subscription keeps the runtime alive, so this task holds the
    // shared state rather than only the client.
    let shared = Arc::clone(&client.shared);
    let handle = client.cancel.clone();
    let done = Completion::<Box<FelixShardedSubscription>>::new(callback, user_data)?;
    client.shared.spawn(done, move |done| async move {
        let cluster = shared.client();
        let opened = async {
            match resume {
                Some(offsets) => {
                    cluster
                        .resubscribe_sharded(&tenant_id, &ns, &stream, offsets, Some(start))
                        .await
                }
                None => {
                    cluster
                        .subscribe_sharded(&tenant_id, &ns, &stream, Some(start))
                        .await
                }
            }
        };
        let outcome = tokio::select! {
            biased;
            () = cancelled(&handle, user.as_ref()) => None,
            outcome = opened => Some(outcome),
        };
        // Dropped here, inside the runtime, when the caller cancelled as it
        // arrived.
        match outcome {
            Some(Ok(subscription)) if !is_cancelled(&handle, user.as_ref()) => {
                done.ok(Box::new(FelixShardedSubscription {
                    positions: Arc::new(Mutex::new(subscription.positions())),
                    shards: subscription.shards(),
                    slot: Arc::new(Mutex::new(Slot::Idle(Box::new(subscription)))),
                    cancel: shared.cancel_token(),
                    shared,
                }));
            }
            Some(Err(err)) if !is_cancelled(&handle, user.as_ref()) => {
                done.fail(err.into());
            }
            _ => done.status(FELIX_STATUS_CANCELLED),
        }
    });
    Ok(FELIX_STATUS_OK)
}

/// Read the next item from any shard.
///
/// `callback` runs once: with `FELIX_STATUS_OK` and an item (see
/// `FelixShardItemCallback`); `FELIX_STATUS_TIMEOUT` when `timeout_ms` passed
/// with nothing (the subscription stays usable); `FELIX_STATUS_CLOSED` once
/// every shard's feed has ended; or `FELIX_STATUS_CANCELLED`. A cancelled or
/// timed-out read loses nothing; the next read gets the item.
///
/// A negative `timeout_ms` waits indefinitely; zero only takes an item that
/// is already waiting. Only one read may be pending: a second returns
/// `FELIX_STATUS_INVALID_ARGUMENT`. The callback may start the next read.
///
/// # Safety
///
/// `subscription` is a live sharded subscription, `cancel` is null or a live
/// token, and `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_next(
    subscription: *const FelixShardedSubscription,
    timeout_ms: i64,
    cancel: *const FelixCancelToken,
    callback: FelixShardItemCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live subscription, per the contract above.
        let subscription = unsafe { subscription.as_ref() }
            .ok_or_else(|| Failure::invalid("subscription is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let wait = u64::try_from(timeout_ms).ok().map(Duration::from_millis);
        let handle = subscription.cancel.clone();
        let positions = Arc::clone(&subscription.positions);

        let mut slot = lock(&subscription.slot);
        let lease = match std::mem::replace(&mut *slot, Slot::Reading) {
            Slot::Idle(inner) => Some(Lease {
                slot: Arc::clone(&subscription.slot),
                subscription: Some(inner),
            }),
            Slot::Reading => {
                return Err(Failure::invalid(
                    "a read is already pending on this subscription",
                ));
            }
            Slot::Closed => {
                *slot = Slot::Closed;
                None
            }
        };
        drop(slot);
        let done = match Completion::<ShardItem>::new(callback, user_data) {
            Ok(done) => done,
            Err(failure) => {
                // Hand the subscription back before refusing.
                drop(lease);
                return Err(failure);
            }
        };
        subscription.shared.spawn(done, move |done| async move {
            let Some(mut lease) = lease else {
                return done.status(FELIX_STATUS_CLOSED);
            };
            let inner = lease
                .subscription
                .as_mut()
                .expect("a lease holds the subscription");
            let timeout = async {
                match wait {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => std::future::pending().await,
                }
            };
            // `next` is cancel-safe, so losing either race loses no item. An
            // item this library cannot name is skipped, and only that.
            let read = async {
                loop {
                    let item = inner.next().await?;
                    if let Some(item) = ShardItem::from_event(item) {
                        return Some(item);
                    }
                }
            };
            let next = tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => Err(FELIX_STATUS_CANCELLED),
                next = read => Ok(next),
                () = timeout => Err(FELIX_STATUS_TIMEOUT),
            };
            let outcome = match next {
                Ok(Some(item)) => {
                    if item.kind == FELIX_SHARD_RECORD {
                        *lock(&positions) = inner.positions();
                    }
                    Ok(item)
                }
                Ok(None) => {
                    lease.subscription = None;
                    Err(FELIX_STATUS_CLOSED)
                }
                Err(status) => Err(status),
            };
            // Give the subscription back before calling out, so the callback
            // can start the next read.
            drop(lease);
            match outcome {
                Ok(mut item) => {
                    let message = item.message.take();
                    done.with_status(FELIX_STATUS_OK, message, item);
                }
                Err(status) => done.status(status),
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// How many shards the subscription covers.
///
/// # Safety
///
/// `subscription` is a live sharded subscription and `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_shards(
    subscription: *const FelixShardedSubscription,
    out: *mut u32,
) -> FelixStatus {
    guard(|| {
        if out.is_null() {
            return Err(Failure::invalid("out is null"));
        }
        // SAFETY: null or a live subscription, per the contract above.
        let subscription = unsafe { subscription.as_ref() }
            .ok_or_else(|| Failure::invalid("subscription is null"))?;
        // SAFETY: checked non-null; the caller promises it is writable.
        unsafe { out.write(subscription.shards) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Where each shard has reached, for `felix_client_resubscribe_sharded`.
///
/// Only shards that have delivered a record to the caller appear, with the
/// offset of the last one. `*out_count` is the total number of entries; at
/// most `cap` are written to `shards` and `offsets`, in ascending shard
/// order, so a caller whose buffers were too small calls again with a larger
/// one. It may be called while a read is pending.
///
/// # Safety
///
/// `subscription` is a live sharded subscription. `shards` and `offsets` are
/// each null or writable for `cap` values (null only when `cap` is zero), and
/// `out_count` is null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_positions(
    subscription: *const FelixShardedSubscription,
    shards: *mut u32,
    offsets: *mut u64,
    cap: usize,
    out_count: *mut usize,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live subscription, per the contract above.
        let subscription = unsafe { subscription.as_ref() }
            .ok_or_else(|| Failure::invalid("subscription is null"))?;
        if cap > 0 && (shards.is_null() || offsets.is_null()) {
            return Err(Failure::invalid("shards and offsets must not be null"));
        }
        let positions = lock(&subscription.positions);
        for (i, (shard, offset)) in positions.iter().take(cap).enumerate() {
            // SAFETY: `i < cap`, and both are writable for `cap` values.
            unsafe {
                shards.add(i).write(*shard);
                offsets.add(i).write(*offset);
            }
        }
        // SAFETY: null or writable, per the contract above.
        unsafe { write_optional(out_count, positions.len()) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a sharded subscription, which stops every shard. Null is ignored.
///
/// A pending read completes with `FELIX_STATUS_CANCELLED`, possibly after
/// this returns.
///
/// # Safety
///
/// `subscription` is null or a pointer from a sharded subscribe callback that
/// has not been freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_sharded_subscription_free(
    subscription: *mut FelixShardedSubscription,
) {
    guard_void(|| {
        if !subscription.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the subscribe
            // callback and is freed exactly once.
            drop(unsafe { Box::from_raw(subscription) });
        }
    });
}

/// The parallel arrays of `felix_client_resubscribe_sharded` as a map.
///
/// # Safety
///
/// `shards` and `offsets` are each null or point to `count` readable values.
unsafe fn shard_offsets(
    shards: *const u32,
    offsets: *const u64,
    count: usize,
) -> Result<ShardOffsets, Failure> {
    if count == 0 {
        return Ok(ShardOffsets::new());
    }
    if shards.is_null() || offsets.is_null() {
        return Err(Failure::invalid("shards and offsets must not be null"));
    }
    // SAFETY: non-null, and the caller promises `count` readable values.
    let (shards, offsets) = unsafe {
        (
            std::slice::from_raw_parts(shards, count),
            std::slice::from_raw_parts(offsets, count),
        )
    };
    let mut resume = ShardOffsets::new();
    for (shard, offset) in shards.iter().zip(offsets) {
        if resume.insert(*shard, *offset).is_some() {
            return Err(Failure::invalid(format!("shard {shard} is listed twice")));
        }
    }
    Ok(resume)
}

/// A poisoned lock means a panic while the value was held. It is still
/// whole, so carry on with it.
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    match value.lock() {
        Ok(value) => value,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests;
