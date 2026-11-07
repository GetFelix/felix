//! Hand-offs: a producer call that reports once the producer's task has the
//! batch, and a handle for the leader's answer to wait on later.
//!
//! `felix_producer_publish` waits for the answer before it reports. A hand-off
//! reports earlier, so a caller can queue the next batch, in order, without
//! waiting for each answer. The batch is sent whether or not its answer is
//! ever read.

use std::ffi::{c_char, c_void};
use std::future::Future;
use std::sync::{Arc, Mutex};

use felix_client::{Handed, IdempotentProducer};
use tokio_util::sync::CancellationToken;

use super::{Lease, check_sizes, payloads, sizes, spawn, split, target, unknown};
use crate::boundary::{Failure, guard, guard_void};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::Shared;
use crate::completion::{Completion, Deliver, cancelled};
use crate::groups::{Done, FelixDoneCallback};
use crate::producer::FelixProducer;
use crate::publish::{FelixPublishCallback, Published, routing_key};
use crate::sharded::lock;
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// Called once when a `felix_producer_hand_off_*` call finishes. On
/// `FELIX_STATUS_OK`, `handed` is a handle the callee owns and frees with
/// `felix_handed_free`; otherwise it is null.
pub type FelixHandedCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        handed: *mut FelixHanded,
    ),
>;

/// A call the producer's task has been handed and not yet answered.
///
/// Its answer is read once, by `felix_handed_offset` or `felix_handed_done`.
/// Freeing the handle does not stop the call.
pub struct FelixHanded {
    answer: Once<Handed>,
    /// Cancelled when the producer's runtime is going away or the owning
    /// producer is freed, which ends a pending wait.
    cancel: CancellationToken,
    shared: Arc<Shared>,
}

impl FelixHanded {
    fn new(handed: Handed, shared: Arc<Shared>) -> Self {
        Self {
            answer: Once::new(handed),
            cancel: shared.cancel_token(),
            shared,
        }
    }
}

impl Drop for FelixHanded {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// A value that can be taken out one time.
struct Once<T>(Mutex<Option<T>>);

impl<T> Once<T> {
    fn new(value: T) -> Self {
        Self(Mutex::new(Some(value)))
    }

    /// The value the first time; a refusal after.
    fn take(&self) -> Result<T, Failure> {
        lock(&self.0)
            .take()
            .ok_or_else(|| Failure::invalid("this hand-off's answer was already read"))
    }
}

impl Deliver for Box<FelixHanded> {
    type Callback = FelixHandedCallback;

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
            let handed = value.map_or(std::ptr::null_mut(), Box::into_raw);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, handed) };
        }
    }
}

/// Queue a hand-off and report the handle once the producer's task has it.
fn hand_off<H, HF>(
    producer: &FelixProducer,
    user: Option<CancellationToken>,
    done: Completion<Box<FelixHanded>>,
    hand: H,
) where
    H: FnOnce(Lease<IdempotentProducer>) -> HF + Send + 'static,
    HF: Future<Output = anyhow::Result<Handed>> + Send + 'static,
{
    let shared = Arc::clone(&producer.shared);
    spawn(
        producer,
        user,
        done,
        hand,
        |handed| async move { Ok(handed) },
        move |done, outcome| done.result(outcome.map(|h| Box::new(FelixHanded::new(h, shared)))),
    );
}

/// Hand a batch of `count` records (at least one) to the producer without
/// waiting for the leader.
///
/// The batch, its key and its sequence are as for `felix_producer_publish`,
/// which this is the first half of: `callback` runs once the producer's task
/// has the batch, with a handle whose `felix_handed_offset` is the leader's
/// answer. Calls handed off one after another are sent in that order. The
/// batch is sent whether or not the answer is read; a failure that leaves it
/// in doubt is settled as `felix_producer_publish` describes.
///
/// A cancel before the task has the batch stops it and reports
/// `FELIX_STATUS_CANCELLED`; after, the batch is still sent and the call
/// reports `FELIX_STATUS_OUTCOME_UNKNOWN`. A closed producer completes with
/// `FELIX_STATUS_CLOSED`.
///
/// # Safety
///
/// As for `felix_producer_publish`, with a `FelixHandedCallback`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_hand_off_batch(
    producer: *const FelixProducer,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    data: *const u8,
    lens: *const usize,
    count: usize,
    key: *const u8,
    key_len: usize,
    cancel: *const FelixCancelToken,
    callback: FelixHandedCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (producer, stream) = unsafe { target(producer, tenant_id, ns, stream) }?;
        // SAFETY: as above.
        let payloads = unsafe { payloads(data, lens, count) }?;
        // SAFETY: as above.
        let key = unsafe { routing_key(key, key_len) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Box<FelixHanded>>::new(callback, user_data)?;
        let hand = move |lease: Lease<IdempotentProducer>| async move {
            let (t, n, s) = (&stream.0, &stream.1, &stream.2);
            lease.hand_off_batch(t, n, s, key, payloads).await
        };
        hand_off(producer, user, done, hand);
        Ok(FELIX_STATUS_OK)
    })
}

/// Hand several batches to the producer without waiting for the leader.
///
/// The records and `batch_sizes` are as for `felix_producer_publish_batches`;
/// the answer is `felix_handed_done`. The ordering, cancel and in-doubt rules
/// of `felix_producer_hand_off_batch` apply.
///
/// # Safety
///
/// As for `felix_producer_publish_batches`, with a `FelixHandedCallback`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_hand_off_batches(
    producer: *const FelixProducer,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    data: *const u8,
    lens: *const usize,
    count: usize,
    batch_sizes: *const usize,
    batch_count: usize,
    cancel: *const FelixCancelToken,
    callback: FelixHandedCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (producer, stream) = unsafe { target(producer, tenant_id, ns, stream) }?;
        // SAFETY: as above.
        let sizes = unsafe { sizes(batch_sizes, batch_count) }?;
        check_sizes(&sizes, count)?;
        // SAFETY: as above.
        let payloads = unsafe { payloads(data, lens, count) }?;
        let batches = split(payloads, &sizes);
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Box<FelixHanded>>::new(callback, user_data)?;
        let hand = move |lease: Lease<IdempotentProducer>| async move {
            let (t, n, s) = (&stream.0, &stream.1, &stream.2);
            lease.hand_off_batches(t, n, s, batches).await
        };
        hand_off(producer, user, done, hand);
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_producer_abandon_in_doubt` up to the hand-off. The answer is
/// `felix_handed_done`. **A batch given up may land twice**, as for the
/// call this is the first half of.
///
/// # Safety
///
/// As for `felix_producer_abandon_in_doubt`, with a `FelixHandedCallback`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_hand_off_abandon(
    producer: *const FelixProducer,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixHandedCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (producer, stream) = unsafe { target(producer, tenant_id, ns, stream) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Box<FelixHanded>>::new(callback, user_data)?;
        let hand = move |lease: Lease<IdempotentProducer>| async move {
            let (t, n, s) = (&stream.0, &stream.1, &stream.2);
            lease.hand_off_abandon(t, n, s).await
        };
        hand_off(producer, user, done, hand);
        Ok(FELIX_STATUS_OK)
    })
}

/// Wait for the leader's answer to a `felix_producer_hand_off_batch`: the
/// offset of the batch's first record, as `felix_producer_publish` reports it.
///
/// The answer is read once, here or by `felix_handed_done`; a second read
/// returns `FELIX_STATUS_INVALID_ARGUMENT` and calls nothing back. A cancel
/// while waiting reports `FELIX_STATUS_OUTCOME_UNKNOWN`, since the batch is
/// sent regardless and its answer is lost with the wait. Any other failure
/// leaves the batch in doubt as `felix_producer_publish` describes.
///
/// # Safety
///
/// `handed` is a live handle, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_handed_offset(
    handed: *const FelixHanded,
    cancel: *const FelixCancelToken,
    callback: FelixPublishCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live handle, per the contract above.
        let handed =
            unsafe { handed.as_ref() }.ok_or_else(|| Failure::invalid("handed is null"))?;
        // Checked before the answer is taken, so a refusal does not use it up.
        if Published::is_null(callback) {
            return Err(Failure::invalid("callback is null"));
        }
        let answer = handed.answer.take()?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Published>::new(callback, user_data)?;
        let handle = handed.cancel.clone();
        handed.shared.spawn(done, move |done| {
            await_answer(
                done,
                answer,
                handle,
                user,
                |h| async move { h.offset().await.map_err(Failure::from) },
                |done, outcome| done.result(outcome.map(Published)),
            )
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Wait for the leader's answer to a `felix_producer_hand_off_batches` or
/// `felix_producer_hand_off_abandon`. As for `felix_handed_offset`, the answer
/// is read once, and a cancel while waiting reports
/// `FELIX_STATUS_OUTCOME_UNKNOWN`.
///
/// # Safety
///
/// `handed` is a live handle, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_handed_done(
    handed: *const FelixHanded,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live handle, per the contract above.
        let handed =
            unsafe { handed.as_ref() }.ok_or_else(|| Failure::invalid("handed is null"))?;
        if Done::is_null(callback) {
            return Err(Failure::invalid("callback is null"));
        }
        let answer = handed.answer.take()?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Done>::new(callback, user_data)?;
        let handle = handed.cancel.clone();
        handed.shared.spawn(done, move |done| {
            await_answer(
                done,
                answer,
                handle,
                user,
                |h| async move { h.done().await.map_err(Failure::from) },
                |done, outcome| done.result(outcome.map(|()| Done)),
            )
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a hand-off handle. Null is ignored.
///
/// This does not stop the batch, which is sent whether or not its answer was
/// read. An answer wait still pending completes with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`, possibly after this returns.
///
/// # Safety
///
/// `handed` is null or a pointer from a hand-off callback that has not been
/// freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_handed_free(handed: *mut FelixHanded) {
    guard_void(|| {
        if !handed.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the hand-off
            // callback and is freed exactly once.
            drop(unsafe { Box::from_raw(handed) });
        }
    });
}

/// Wait for a handed-off call's answer and report it. A cancel leaves the
/// call sent, so it reports an unknown outcome.
pub(super) async fn await_answer<V, H, T, A, AF>(
    done: Completion<V>,
    handed: H,
    handle: CancellationToken,
    user: Option<CancellationToken>,
    answer: A,
    report: impl FnOnce(Completion<V>, Result<T, Failure>),
) where
    V: Deliver,
    A: FnOnce(H) -> AF,
    AF: Future<Output = Result<T, Failure>>,
{
    tokio::select! {
        biased;
        () = cancelled(&handle, user.as_ref()) => done.fail(unknown()),
        outcome = answer(handed) => report(done, outcome),
    }
}

#[cfg(test)]
mod tests;
