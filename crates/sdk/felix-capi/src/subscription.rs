//! Subscribing, and reading a subscription one event at a time.

use std::ffi::{c_char, c_void};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use felix_client::{ClusterSubscription, StartPosition};
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, guard_void, required_str, write_optional};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver, cancelled, is_cancelled};
use crate::event::FelixEvent;
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_CLOSED, FELIX_STATUS_OK, FELIX_STATUS_TIMEOUT, FelixStatus,
};

/// Where a subscription starts reading.
pub type FelixStartKind = i32;

/// Only records published after the subscription is made.
pub const FELIX_START_LATEST: FelixStartKind = 0;
/// The oldest record the stream still retains.
pub const FELIX_START_EARLIEST: FelixStartKind = 1;
/// The record at `start_offset`: the first one the caller has not seen, so a
/// resuming caller passes the offset it last handled plus one.
pub const FELIX_START_OFFSET: FelixStartKind = 2;

/// Why a subscription ended, or that it has not.
pub type FelixCloseReason = i32;

/// The subscription is live, or ended only because it was freed.
pub const FELIX_CLOSE_OPEN: FelixCloseReason = 0;
/// The broker ended the stream: the event stream closed cleanly, and the
/// stream was not deleted.
pub const FELIX_CLOSE_ENDED: FelixCloseReason = 1;
/// The stream was deleted. There is nothing to resubscribe to.
pub const FELIX_CLOSE_STREAM_DELETED: FelixCloseReason = 2;

/// Called once when `felix_client_subscribe` finishes. On `FELIX_STATUS_OK`,
/// `subscription` is a new subscription the callee owns; otherwise it is
/// null.
pub type FelixSubscribeCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        subscription: *mut FelixSubscription,
    ),
>;

/// Called once when `felix_subscription_next` finishes. On
/// `FELIX_STATUS_OK`, `event` is an event the callee owns; otherwise it is
/// null.
pub type FelixEventCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        event: *mut FelixEvent,
    ),
>;

/// A live subscription to one shard of a stream.
///
/// It follows the shard to whichever broker owns it, as the Rust client
/// does, and resubscribes after the last event if it falls behind, so a lag
/// is not reported. One read may be pending at a time. It must not be freed while
/// another thread is calling it.
pub struct FelixSubscription {
    slot: Arc<Mutex<Slot>>,
    /// Where the subscription began, kept from when it was made.
    start_offset: Option<u64>,
    /// Cancelled when this handle is freed, which ends a pending read.
    cancel: CancellationToken,
    shared: Arc<Shared>,
}

enum Slot {
    Idle(Box<ClusterSubscription>),
    /// A read has the subscription.
    Reading,
    /// The stream ended, for this reason.
    Closed(FelixCloseReason),
}

impl Drop for FelixSubscription {
    fn drop(&mut self) {
        self.cancel.cancel();
        // Its background tasks belong to the client's runtime, so it is
        // dropped inside that runtime. A pending read drops it on its own.
        let _guard = self.shared.handle().enter();
        let idle = std::mem::replace(&mut *lock(&self.slot), Slot::Closed(FELIX_CLOSE_ENDED));
        drop(idle);
    }
}

/// A read's hold on the subscription. Dropping it puts the subscription
/// back, so a read that panics or is cancelled leaves it readable.
struct Lease {
    slot: Arc<Mutex<Slot>>,
    /// `None` once the broker ended the stream.
    subscription: Option<Box<ClusterSubscription>>,
    /// Why, once `subscription` is `None`.
    closed: FelixCloseReason,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let next = self
            .subscription
            .take()
            .map_or(Slot::Closed(self.closed), Slot::Idle);
        *lock(&self.slot) = next;
    }
}

/// Called once when `felix_client_offset_for_time` finishes. On
/// `FELIX_STATUS_OK`, `has_offset` says whether any record is that recent,
/// and `offset` holds the first one when it does.
pub type FelixOffsetCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        has_offset: bool,
        offset: u64,
    ),
>;

/// An `offset_for_time` answer: the offset, when a record is that recent.
pub(crate) struct OffsetAt(Option<u64>);

impl Deliver for OffsetAt {
    type Callback = FelixOffsetCallback;

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
            let offset = value.and_then(|at| at.0);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe {
                callback(
                    user_data,
                    status,
                    message,
                    offset.is_some(),
                    offset.unwrap_or(0),
                )
            };
        }
    }
}

impl Deliver for Box<FelixSubscription> {
    type Callback = FelixSubscribeCallback;

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

impl Deliver for Box<FelixEvent> {
    type Callback = FelixEventCallback;

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
            let event = value.map_or(std::ptr::null_mut(), Box::into_raw);
            // SAFETY: as above.
            unsafe { callback(user_data, status, message, event) };
        }
    }
}

/// Subscribe to one shard of a stream.
///
/// A `shard` past the stream's width is refused by the broker, so the
/// callback gets an error. `start` is one of the `FELIX_START_*` values;
/// `start_offset` is read only for `FELIX_START_OFFSET`. `callback` runs once
/// with the new subscription, unless this returns something other than
/// `FELIX_STATUS_OK`. A cancelled subscribe never hands one out.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated,
/// `cancel` is null or a live token, and `callback` is safe to call with
/// `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_subscribe(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixSubscribeCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, stream) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(stream, "stream")?.to_string(),
            )
        };
        let start = start_position(start, start_offset)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };

        // The new subscription keeps the runtime alive, so this task holds
        // the shared state rather than only the client.
        let shared = Arc::clone(&client.shared);
        let handle = client.cancel.clone();
        let done = Completion::<Box<FelixSubscription>>::new(callback, user_data)?;
        client.shared.spawn(done, move |done| async move {
            let subscribe =
                shared
                    .client()
                    .subscribe_shard(&tenant_id, &ns, &stream, shard, Some(start));
            let outcome = tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => None,
                outcome = subscribe => Some(outcome),
            };
            // Dropped here, inside the runtime, when the caller cancelled
            // as it arrived.
            match outcome {
                Some(Ok(subscription)) if !is_cancelled(&handle, user.as_ref()) => {
                    let cancel = shared.cancel_token();
                    done.ok(Box::new(FelixSubscription {
                        start_offset: subscription.start_offset(),
                        slot: Arc::new(Mutex::new(Slot::Idle(Box::new(subscription)))),
                        cancel,
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
    })
}

/// Read the next event.
///
/// `callback` runs once: with `FELIX_STATUS_OK` and an event to free with
/// `felix_event_free`; `FELIX_STATUS_TIMEOUT` when `timeout_ms` passed with
/// nothing (the subscription stays usable); `FELIX_STATUS_CLOSED` once the
/// broker has ended the stream; or `FELIX_STATUS_CANCELLED`. A cancelled or
/// timed-out read loses nothing; the next read gets the event.
///
/// A negative `timeout_ms` waits indefinitely; zero only takes an event that
/// is already waiting. Only one read may be pending: a second returns
/// `FELIX_STATUS_INVALID_ARGUMENT`. The callback may start the next read.
///
/// # Safety
///
/// `subscription` is a live subscription, `cancel` is null or a live token,
/// and `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_subscription_next(
    subscription: *const FelixSubscription,
    timeout_ms: i64,
    cancel: *const FelixCancelToken,
    callback: FelixEventCallback,
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

        let mut slot = lock(&subscription.slot);
        let lease = match std::mem::replace(&mut *slot, Slot::Reading) {
            Slot::Idle(inner) => Some(Lease {
                slot: Arc::clone(&subscription.slot),
                subscription: Some(inner),
                closed: FELIX_CLOSE_ENDED,
            }),
            Slot::Reading => {
                return Err(Failure::invalid(
                    "a read is already pending on this subscription",
                ));
            }
            Slot::Closed(reason) => {
                *slot = Slot::Closed(reason);
                None
            }
        };
        drop(slot);
        let done = match Completion::<Box<FelixEvent>>::new(callback, user_data) {
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
            // `next_event` is cancel-safe, so losing either race loses no event.
            let next = tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => Err(FELIX_STATUS_CANCELLED),
                next = inner.next_event() => Ok(next),
                () = timeout => Err(FELIX_STATUS_TIMEOUT),
            };
            let outcome = match next {
                Ok(Ok(Some(event))) => Ok(Box::new(FelixEvent::from(event))),
                Ok(Ok(None)) => {
                    lease.closed = close_reason(inner.stream_deleted());
                    lease.subscription = None;
                    Err(Ok(FELIX_STATUS_CLOSED))
                }
                Ok(Err(err)) => Err(Err(Failure::from(err))),
                Err(status) => Err(Ok(status)),
            };
            // Give the subscription back before calling out, so the callback
            // can start the next read.
            drop(lease);
            match outcome {
                Ok(event) => done.ok(event),
                Err(Ok(status)) => done.status(status),
                Err(Err(failure)) => done.fail(failure),
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Why the subscription ended: `FELIX_CLOSE_OPEN` while it is live, else
/// `FELIX_CLOSE_ENDED` or `FELIX_CLOSE_STREAM_DELETED`. A read that returned
/// `FELIX_STATUS_CLOSED` has already set it.
///
/// # Safety
///
/// `subscription` is a live subscription and `out_reason` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_subscription_close_reason(
    subscription: *const FelixSubscription,
    out_reason: *mut FelixCloseReason,
) -> FelixStatus {
    guard(|| {
        if out_reason.is_null() {
            return Err(Failure::invalid("out_reason is null"));
        }
        // SAFETY: null or a live subscription, per the contract above.
        let subscription = unsafe { subscription.as_ref() }
            .ok_or_else(|| Failure::invalid("subscription is null"))?;
        let reason = match *lock(&subscription.slot) {
            Slot::Closed(reason) => reason,
            Slot::Idle(_) | Slot::Reading => FELIX_CLOSE_OPEN,
        };
        // SAFETY: checked non-null; the caller promises it is writable.
        unsafe { out_reason.write(reason) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Where the subscription began: no event below it is delivered.
///
/// `*out_has` is false on a stream without a log, and from a broker that did
/// not negotiate offsets. Count the drops before the first event as
/// `offset - start_offset - skipped_before`; the start can be 0. Either
/// out-pointer may be null.
///
/// # Safety
///
/// `subscription` is a live subscription, and the out-pointers are null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_subscription_start_offset(
    subscription: *const FelixSubscription,
    out_offset: *mut u64,
    out_has: *mut bool,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live subscription, per the contract above.
        let subscription = unsafe { subscription.as_ref() }
            .ok_or_else(|| Failure::invalid("subscription is null"))?;
        // SAFETY: each is null or writable, per the contract above.
        unsafe {
            write_optional(out_has, subscription.start_offset.is_some());
            write_optional(out_offset, subscription.start_offset.unwrap_or(0));
        }
        Ok(FELIX_STATUS_OK)
    })
}

/// The first offset on one shard whose record was appended at or after
/// `at_micros` (microseconds since the Unix epoch), as the leading broker's
/// clock saw it. Subscribe with `FELIX_START_OFFSET` at it to replay from a
/// time.
///
/// `callback` runs once with `has_offset` false when nothing is that recent
/// yet; subscribe with `FELIX_START_LATEST` to wait for it. A time older
/// than every record answers with the oldest. A cancelled call completes
/// with `FELIX_STATUS_CANCELLED`: it only reads, so nothing is left unknown.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated,
/// `cancel` is null or a live token, and `callback` is safe to call with
/// `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_offset_for_time(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    at_micros: u64,
    cancel: *const FelixCancelToken,
    callback: FelixOffsetCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, stream) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(stream, "stream")?.to_string(),
            )
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let shared = Arc::clone(&client.shared);
        let handle = client.cancel.clone();
        let done = Completion::<OffsetAt>::new(callback, user_data)?;
        client.shared.spawn(done, move |done| async move {
            let ask = shared
                .client()
                .offset_for_time(&tenant_id, &ns, &stream, shard, at_micros);
            let outcome = tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => None,
                outcome = ask => Some(outcome),
            };
            match outcome {
                Some(Ok(offset)) if !is_cancelled(&handle, user.as_ref()) => {
                    done.ok(OffsetAt(offset));
                }
                Some(Err(err)) if !is_cancelled(&handle, user.as_ref()) => {
                    done.fail(err.into());
                }
                _ => done.status(FELIX_STATUS_CANCELLED),
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a subscription, which stops it. Null is ignored.
///
/// A pending read completes with `FELIX_STATUS_CANCELLED`, possibly after
/// this returns.
///
/// # Safety
///
/// `subscription` is null or a pointer from a subscribe callback that has not
/// been freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_subscription_free(subscription: *mut FelixSubscription) {
    guard_void(|| {
        if !subscription.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the subscribe
            // callback and is freed exactly once.
            drop(unsafe { Box::from_raw(subscription) });
        }
    });
}

pub(crate) fn start_position(
    start: FelixStartKind,
    start_offset: u64,
) -> Result<StartPosition, Failure> {
    match start {
        FELIX_START_LATEST => Ok(StartPosition::Latest),
        FELIX_START_EARLIEST => Ok(StartPosition::Earliest),
        FELIX_START_OFFSET => Ok(StartPosition::Offset(start_offset)),
        other => Err(Failure::invalid(format!(
            "start {other} is not a FELIX_START_* value"
        ))),
    }
}

fn close_reason(stream_deleted: bool) -> FelixCloseReason {
    if stream_deleted {
        FELIX_CLOSE_STREAM_DELETED
    } else {
        FELIX_CLOSE_ENDED
    }
}

/// A poisoned lock means a panic while the slot was held. The slot is still
/// whole, so carry on with it.
fn lock(slot: &Mutex<Slot>) -> MutexGuard<'_, Slot> {
    match slot.lock() {
        Ok(slot) => slot,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests;
