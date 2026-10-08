//! Subscribing, and polling a subscription for its next event.

use std::ffi::c_char;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use felix_client::{ClusterSubscription, StartPosition};

use crate::boundary::{Failure, guard, guard_void, required_str};
use crate::client::{FelixClient, Shared};
use crate::event::FelixEvent;
use crate::status::{FELIX_STATUS_CLOSED, FELIX_STATUS_OK, FELIX_STATUS_TIMEOUT, FelixStatus};

/// Where a subscription starts reading.
pub type FelixStartKind = i32;

/// Only records published after the subscription is made.
pub const FELIX_START_LATEST: FelixStartKind = 0;
/// The oldest record the stream still retains.
pub const FELIX_START_EARLIEST: FelixStartKind = 1;
/// The record at `start_offset`: the first one the caller has not seen, so a
/// resuming caller passes the offset it last handled plus one.
pub const FELIX_START_OFFSET: FelixStartKind = 2;

/// A live subscription to one shard of a stream.
///
/// It follows the shard to whichever broker owns it, as the Rust client
/// does. Poll it from one thread at a time; concurrent polls take turns.
pub struct FelixSubscription {
    /// `None` once the broker has ended the stream.
    inner: Mutex<Option<ClusterSubscription>>,
    shared: Arc<Shared>,
}

impl Drop for FelixSubscription {
    fn drop(&mut self) {
        // Its background tasks belong to the client's runtime, so it is
        // dropped inside that runtime.
        let _guard = self.shared.runtime().enter();
        let inner = match self.inner.get_mut() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.take();
    }
}

/// Subscribe to shard 0 of a stream.
///
/// `start` is one of the `FELIX_START_*` values; `start_offset` is read only
/// for `FELIX_START_OFFSET`. On success `*out_subscription` holds a
/// subscription to free with `felix_subscription_free`.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated, and
/// `out_subscription` points to writable storage for one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_subscribe(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    start: FelixStartKind,
    start_offset: u64,
    out_subscription: *mut *mut FelixSubscription,
) -> FelixStatus {
    guard(|| {
        if out_subscription.is_null() {
            return Err(Failure::invalid("out_subscription is null"));
        }
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, stream) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?,
                required_str(ns, "ns")?,
                required_str(stream, "stream")?,
            )
        };
        let start = match start {
            FELIX_START_LATEST => StartPosition::Latest,
            FELIX_START_EARLIEST => StartPosition::Earliest,
            FELIX_START_OFFSET => StartPosition::Offset(start_offset),
            other => {
                return Err(Failure::invalid(format!(
                    "start {other} is not a FELIX_START_* value"
                )));
            }
        };

        let shared = Arc::clone(&client.shared);
        let subscription = shared.runtime().block_on(shared.client().subscribe_from(
            tenant_id,
            ns,
            stream,
            Some(start),
        ))?;
        let handle = Box::new(FelixSubscription {
            inner: Mutex::new(Some(subscription)),
            shared,
        });
        // SAFETY: checked non-null above, and the caller promises it is
        // writable.
        unsafe { out_subscription.write(Box::into_raw(handle)) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Wait for the next event.
///
/// Returns `FELIX_STATUS_OK` with `*out_event` set to an event to free with
/// `felix_event_free`; `FELIX_STATUS_TIMEOUT` when `timeout_ms` passed with
/// nothing (the subscription stays usable); or `FELIX_STATUS_CLOSED` once the
/// broker has ended the stream. `*out_event` is null for anything but OK.
///
/// A negative `timeout_ms` waits indefinitely; zero only takes an event that
/// is already waiting.
///
/// # Safety
///
/// `subscription` is a live subscription, and `out_event` points to writable
/// storage for one pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_subscription_next_event(
    subscription: *const FelixSubscription,
    timeout_ms: i64,
    out_event: *mut *mut FelixEvent,
) -> FelixStatus {
    guard(|| {
        if out_event.is_null() {
            return Err(Failure::invalid("out_event is null"));
        }
        // SAFETY: checked non-null; the caller promises it is writable.
        unsafe { out_event.write(std::ptr::null_mut()) };
        // SAFETY: null or a live subscription, per the contract above.
        let subscription = unsafe { subscription.as_ref() }
            .ok_or_else(|| Failure::invalid("subscription is null"))?;

        // A poisoned lock means an earlier poll panicked mid-read. The
        // subscription itself is still whole, so carry on with it.
        let mut slot = match subscription.inner.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(inner) = slot.as_mut() else {
            return Ok(FELIX_STATUS_CLOSED);
        };
        let runtime = subscription.shared.runtime();
        let next = if timeout_ms < 0 {
            runtime.block_on(inner.next_event())?
        } else {
            let wait = Duration::from_millis(timeout_ms.unsigned_abs());
            match runtime.block_on(async { tokio::time::timeout(wait, inner.next_event()).await }) {
                Ok(next) => next?,
                Err(_elapsed) => return Ok(FELIX_STATUS_TIMEOUT),
            }
        };
        let Some(event) = next else {
            slot.take();
            return Ok(FELIX_STATUS_CLOSED);
        };
        let event = Box::new(FelixEvent::from(event));
        // SAFETY: checked non-null and writable above.
        unsafe { out_event.write(Box::into_raw(event)) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a subscription, which stops it. Null is ignored.
///
/// # Safety
///
/// `subscription` is null or a pointer from `felix_client_subscribe` that
/// has not been freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_subscription_free(subscription: *mut FelixSubscription) {
    guard_void(|| {
        if !subscription.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in
            // `felix_client_subscribe` and is freed exactly once.
            drop(unsafe { Box::from_raw(subscription) });
        }
    });
}
