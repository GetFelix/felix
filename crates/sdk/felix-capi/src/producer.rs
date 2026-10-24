//! Idempotent producers: publishes that land once however many times they
//! are sent.
//!
//! The producer keeps one sequence per shard and a task that sends in call
//! order, so the contract is `felix_client::IdempotentProducer`'s; this module
//! only gives it a handle. Every call that sends is a write, so all of them
//! start through [`spawn_write`].

use std::ffi::{c_char, c_void};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_client::IdempotentProducer;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, guard_void, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver, cancelled, is_cancelled};
use crate::groups::{Done, FelixDoneCallback};
use crate::publish::{FelixPublishCallback, Published, laid_end_to_end, routing_key, spawn_write};
use crate::sharded::lock;
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_CLOSED, FELIX_STATUS_OK, FELIX_STATUS_TIMEOUT, FelixStatus,
};

/// Called once when `felix_client_idempotent_producer` finishes. On
/// `FELIX_STATUS_OK`, `producer` is a new producer the callee owns and frees
/// with `felix_producer_free`; otherwise it is null.
pub type FelixProducerCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        producer: *mut FelixProducer,
    ),
>;

/// A producer whose publishes are numbered, so a batch the caller never got
/// an answer for can be sent again without landing twice.
///
/// Calls on one producer are sent in the order they were made. It must not be
/// freed while another thread is calling it.
pub struct FelixProducer {
    slot: Arc<Slot<IdempotentProducer>>,
    /// Cancelled when this handle is freed, which ends its pending calls.
    cancel: CancellationToken,
    shared: Arc<Shared>,
}

impl Drop for FelixProducer {
    fn drop(&mut self) {
        self.cancel.cancel();
        // The producer's task belongs to the client's runtime, so what is
        // left is dropped inside it. Batches already handed to the task are
        // still sent; `felix_producer_close` is what waits for them.
        let _guard = self.shared.handle().enter();
        drop(self.slot.take());
    }
}

/// What a handle holds until it is closed or freed.
///
/// A call takes a [`Lease`] and runs on it; close takes the value out and
/// waits for the leases to end, because closing consumes the producer.
struct Slot<T> {
    held: Mutex<Option<Arc<T>>>,
    /// Signalled whenever a lease ends.
    idle: Notify,
}

impl<T> Slot<T> {
    fn new(value: T) -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::new(Some(Arc::new(value))),
            idle: Notify::new(),
        })
    }

    /// Fails with `FELIX_STATUS_CLOSED` once the value has been taken.
    fn lease(self: &Arc<Self>) -> Result<Lease<T>, Failure> {
        let value = lock(&self.held).clone().ok_or_else(closed)?;
        Ok(Lease {
            slot: Arc::clone(self),
            value: Some(value),
        })
    }

    fn take(&self) -> Option<Arc<T>> {
        lock(&self.held).take()
    }

    /// The value once no lease holds it. A lease ending stores a permit, so
    /// one that ends between the check and the wait is not missed.
    async fn sole(&self, mut value: Arc<T>) -> T {
        loop {
            match Arc::try_unwrap(value) {
                Ok(value) => return value,
                Err(shared) => {
                    value = shared;
                    self.idle.notified().await;
                }
            }
        }
    }
}

/// A call's use of the value in a [`Slot`].
struct Lease<T> {
    slot: Arc<Slot<T>>,
    value: Option<Arc<T>>,
}

impl<T> Deref for Lease<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.value.as_ref().expect("a lease holds its value")
    }
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        // Release the value before waking the waiter, or it would look again
        // and still find it shared.
        self.value = None;
        self.slot.idle.notify_one();
    }
}

fn closed() -> Failure {
    Failure {
        status: FELIX_STATUS_CLOSED,
        message: "the producer is closed".to_string(),
    }
}

impl Deliver for Box<FelixProducer> {
    type Callback = FelixProducerCallback;

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
            let producer = value.map_or(std::ptr::null_mut(), Box::into_raw);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, producer) };
        }
    }
}

/// Ask the broker for a producer id and make a producer under it.
///
/// A producer sends its batches under that id, numbered per shard, so the
/// leader can tell a batch it already holds from a new one. Read the
/// guarantees on `felix_producer_publish` before relying on them.
///
/// `callback` runs once, unless this returns something other than
/// `FELIX_STATUS_OK`. A cancelled call never hands out a producer; an id the
/// broker had already assigned is simply never used.
///
/// # Safety
///
/// `client` is a live client, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_idempotent_producer(
    client: *const FelixClient,
    cancel: *const FelixCancelToken,
    callback: FelixProducerCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        // The producer keeps the runtime alive, so this task holds the shared
        // state rather than only the client.
        let shared = Arc::clone(&client.shared);
        let handle = client.cancel.clone();
        let done = Completion::<Box<FelixProducer>>::new(callback, user_data)?;
        client.shared.spawn(done, move |done| async move {
            let created = shared.client().idempotent_producer();
            let outcome = tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => None,
                outcome = created => Some(outcome),
            };
            match outcome {
                Some(Ok(producer)) if !is_cancelled(&handle, user.as_ref()) => {
                    done.ok(Box::new(FelixProducer {
                        slot: Slot::new(producer),
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
    })
}

/// Publish a batch of `count` records (at least one) exactly once.
///
/// The payloads lie end to end in `data`, the i-th `lens[i]` bytes long, and
/// are copied before this returns. The batch takes one sequence whatever its
/// size. A null `key` sends it to shard 0; otherwise to the shard the key
/// routes to, which needs a broker that reports the stream's shards. The
/// producer numbers each shard separately, so keys that share a shard share
/// its sequence.
///
/// `callback` runs once the leader has acknowledged the batch, and
/// `has_offset` and `offset` report the batch's first record (not set when
/// the leader reported none). It can fail in a way that leaves the batch in
/// doubt: any status but a refusal. The sequence has not advanced then and
/// the batch may have landed. **To settle it, call again with the same
/// payloads and key**: the leader answers a batch it holds without
/// appending it, so it cannot land twice. A different batch is refused
/// without being sent until then. A cancel after the batch was handed to the
/// producer reports `FELIX_STATUS_OUTCOME_UNKNOWN`; the batch is still sent,
/// so settle it the same way and do not treat it as lost.
///
/// A refusal by the broker (for example, the shard no longer knows this
/// producer) ends the producer on that stream: every later call on it fails
/// with the same reason, and its message says why. Start a new producer, or
/// call `felix_producer_abandon_in_doubt`, after deciding whether the last
/// batch landed.
///
/// A closed producer completes with `FELIX_STATUS_CLOSED`.
///
/// # Safety
///
/// `producer` is a live producer and the strings are NUL-terminated. `lens`
/// is valid for `count` lengths and `data` for their sum; `key` is null or
/// valid for `key_len` bytes. `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_publish(
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
    callback: FelixPublishCallback,
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
        let done = Completion::<Published>::new(callback, user_data)?;
        let lease = producer.slot.lease();
        let write = async move {
            let lease = lease?;
            let (t, n, s) = (&stream.0, &stream.1, &stream.2);
            let sent = match key {
                None => lease.publish_batch(t, n, s, payloads).await,
                Some(key) => lease.publish_batch_keyed(t, n, s, key, payloads).await,
            };
            sent.map_err(Failure::from)
        };
        spawn(producer, user, done, write, |done, outcome| {
            done.result(outcome.map(Published));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Publish several batches exactly once each, under consecutive sequences.
///
/// The `count` payloads lie end to end in `data` as for
/// `felix_producer_publish`, and `batch_sizes` splits them in order into
/// `batch_count` batches. The sizes must each be at least one and add up to
/// `count`. All go to shard 0. Against a broker that pipelines publishes the
/// batches are in flight together.
///
/// On a failure the batches already acknowledged stay acknowledged and the
/// rest are in doubt together. Call again with the same batches to settle
/// them: only the unacknowledged ones are sent. The call may also start with
/// those batches and carry more after them. Anything else is refused without
/// sending. The cancel and refusal rules of `felix_producer_publish` apply.
///
/// # Safety
///
/// As for `felix_producer_publish`, and `batch_sizes` is valid for
/// `batch_count` values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_publish_batches(
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
    callback: FelixDoneCallback,
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
        let done = Completion::<Done>::new(callback, user_data)?;
        let lease = producer.slot.lease();
        let write = async move {
            let lease = lease?;
            let (t, n, s) = (&stream.0, &stream.1, &stream.2);
            lease
                .publish_batches(t, n, s, batches)
                .await
                .map_err(Failure::from)
        };
        spawn(producer, user, done, write, |done, outcome| {
            done.result(outcome.map(|()| Done));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Give up on the stream's batches in doubt and number the stream from zero
/// under a fresh producer id.
///
/// For a stream that cannot settle them: one deleted and created again at
/// another width while a batch was in doubt is refused every re-send.
/// **A batch given up may land twice**: it may have been appended before, and
/// sent again it is a new record under the fresh id. Every sequence on the
/// stream starts again, not only the ones in doubt.
///
/// # Safety
///
/// `producer` is a live producer, the strings are NUL-terminated, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_abandon_in_doubt(
    producer: *const FelixProducer,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (producer, stream) = unsafe { target(producer, tenant_id, ns, stream) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Done>::new(callback, user_data)?;
        let lease = producer.slot.lease();
        let write = async move {
            let lease = lease?;
            let (t, n, s) = (&stream.0, &stream.1, &stream.2);
            lease.abandon_in_doubt(t, n, s).await.map_err(Failure::from)
        };
        spawn(producer, user, done, write, |done, outcome| {
            done.result(outcome.map(|()| Done));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// The producer id the broker assigned, which its batches go out under
/// unless a stream has been given one of its own.
///
/// `FELIX_STATUS_CLOSED` once the producer has been closed.
///
/// # Safety
///
/// `producer` is a live producer and `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_id(
    producer: *const FelixProducer,
    out: *mut u64,
) -> FelixStatus {
    guard(|| {
        if out.is_null() {
            return Err(Failure::invalid("out is null"));
        }
        // SAFETY: null or a live producer, per the contract above.
        let producer =
            unsafe { producer.as_ref() }.ok_or_else(|| Failure::invalid("producer is null"))?;
        let id = producer.slot.lease()?.producer_id();
        // SAFETY: checked non-null; the caller promises it is writable.
        unsafe { out.write(id) };
        Ok(FELIX_STATUS_OK)
    })
}

/// The id a stream's batches go out under. The producer's own id, unless the
/// stream was created again at another width or given up with
/// `felix_producer_abandon_in_doubt`: it is then numbered afresh under an id
/// of its own.
///
/// `FELIX_STATUS_CLOSED` once the producer has been closed.
///
/// # Safety
///
/// `producer` is a live producer, the strings are NUL-terminated and `out` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_id_for(
    producer: *const FelixProducer,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    out: *mut u64,
) -> FelixStatus {
    guard(|| {
        if out.is_null() {
            return Err(Failure::invalid("out is null"));
        }
        // SAFETY: the pointers meet the contract above.
        let (producer, stream) = unsafe { target(producer, tenant_id, ns, stream) }?;
        let id = producer
            .slot
            .lease()?
            .producer_id_for(&stream.0, &stream.1, &stream.2);
        // SAFETY: checked non-null; the caller promises it is writable.
        unsafe { out.write(id) };
        Ok(FELIX_STATUS_OK)
    })
}

/// Wait for every publish already handed to the producer to be answered,
/// then stop it.
///
/// The producer leaves the handle as soon as this returns `FELIX_STATUS_OK`:
/// later calls on the handle complete, or return, `FELIX_STATUS_CLOSED`. A
/// negative `timeout_ms` waits as long as it takes. Otherwise a producer
/// that has not finished by then is stopped where it is, with a failure
/// status: nothing more is sent or re-sent, and a batch it had already sent
/// may or may not land.
///
/// A cancelled close completes with `FELIX_STATUS_CANCELLED`. The producer
/// is closed all the same and its task finishes on its own. The handle is
/// still freed with `felix_producer_free`.
///
/// # Safety
///
/// `producer` is a live producer, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_close(
    producer: *const FelixProducer,
    timeout_ms: i64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live producer, per the contract above.
        let producer =
            unsafe { producer.as_ref() }.ok_or_else(|| Failure::invalid("producer is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let limit = u64::try_from(timeout_ms).ok().map(Duration::from_millis);
        let done = Completion::<Done>::new(callback, user_data)?;
        // Taken here, not in the task, so a call made after this returns is
        // refused whenever the task gets to run.
        let taken = producer.slot.take();
        let slot = Arc::clone(&producer.slot);
        let handle = producer.cancel.clone();
        producer.shared.spawn(done, move |done| async move {
            let Some(taken) = taken else {
                return done.fail(closed());
            };
            let finish = async {
                let Some(limit) = limit else {
                    let held = slot.sole(taken).await;
                    return held.close().await.map_err(Failure::from);
                };
                let started = Instant::now();
                let held = tokio::time::timeout(limit, slot.sole(taken))
                    .await
                    .map_err(|_| Failure {
                        status: FELIX_STATUS_TIMEOUT,
                        message: format!(
                            "the producer still had calls pending after {} ms, so it was stopped",
                            limit.as_millis()
                        ),
                    })?;
                held.close_within(limit.saturating_sub(started.elapsed()))
                    .await
                    .map_err(Failure::from)
            };
            // The producer is dropped with this future on a cancel, inside
            // the runtime; its task then ends on its own.
            tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => done.status(FELIX_STATUS_CANCELLED),
                outcome = finish => done.result(outcome.map(|()| Done)),
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Free a producer. Null is ignored.
///
/// This does not wait for anything: calls pending on the handle complete
/// with `FELIX_STATUS_CANCELLED` (`FELIX_STATUS_OUTCOME_UNKNOWN` for a send
/// that may already be out), possibly after this returns, and batches
/// already handed to the producer may still be sent. To wait for them, call
/// `felix_producer_close` first.
///
/// # Safety
///
/// `producer` is null or a pointer from a producer callback that has not been
/// freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_producer_free(producer: *mut FelixProducer) {
    guard_void(|| {
        if !producer.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the producer
            // callback and is freed exactly once.
            drop(unsafe { Box::from_raw(producer) });
        }
    });
}

/// The tenant, namespace and stream a call names.
type StreamName = (String, String, String);

/// # Safety
///
/// `producer` is null or live, and each string null or NUL-terminated.
unsafe fn target<'a>(
    producer: *const FelixProducer,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
) -> Result<(&'a FelixProducer, StreamName), Failure> {
    // SAFETY: the caller's contract, passed through.
    let producer =
        unsafe { producer.as_ref() }.ok_or_else(|| Failure::invalid("producer is null"))?;
    // SAFETY: as above.
    let name = unsafe {
        (
            required_str(tenant_id, "tenant_id")?.to_string(),
            required_str(ns, "ns")?.to_string(),
            required_str(stream, "stream")?.to_string(),
        )
    };
    Ok((producer, name))
}

fn spawn<V, T, F>(
    producer: &FelixProducer,
    user: Option<CancellationToken>,
    done: Completion<V>,
    write: F,
    report: impl FnOnce(Completion<V>, T) + Send + 'static,
) where
    V: Deliver,
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    spawn_write(
        &producer.shared,
        producer.cancel.clone(),
        user,
        done,
        write,
        report,
    );
}

/// A batch's payloads. An empty batch has no sequence to take, so the call
/// refuses it.
///
/// # Safety
///
/// As for `laid_end_to_end`.
unsafe fn payloads(
    data: *const u8,
    lens: *const usize,
    count: usize,
) -> Result<Vec<Vec<u8>>, Failure> {
    if count == 0 {
        return Err(Failure::invalid("a publish needs at least one record"));
    }
    // SAFETY: the caller's contract, passed through.
    unsafe { laid_end_to_end(data, lens, count, "data", "lens") }
}

/// # Safety
///
/// `sizes` is null or valid for `count` values.
unsafe fn sizes(sizes: *const usize, count: usize) -> Result<Vec<usize>, Failure> {
    if count == 0 {
        return Err(Failure::invalid("a publish needs at least one batch"));
    }
    if sizes.is_null() {
        return Err(Failure::invalid("batch_sizes is null"));
    }
    if count > isize::MAX as usize / size_of::<usize>() {
        return Err(Failure::invalid("batch_count is too large"));
    }
    // SAFETY: non-null, within `isize::MAX` bytes, and the caller promises
    // `count` values.
    Ok(unsafe { std::slice::from_raw_parts(sizes, count) }.to_vec())
}

/// Every batch holds a record, and together they hold all `count`.
fn check_sizes(sizes: &[usize], count: usize) -> Result<(), Failure> {
    if let Some(at) = sizes.iter().position(|&size| size == 0) {
        return Err(Failure::invalid(format!("batch {at} has no records")));
    }
    let total = sizes
        .iter()
        .try_fold(0usize, |total, &size| total.checked_add(size));
    if total != Some(count) {
        return Err(Failure::invalid(format!(
            "the batch sizes do not add up to the {count} records"
        )));
    }
    Ok(())
}

/// The payloads cut into batches of the given sizes, in order.
fn split(payloads: Vec<Vec<u8>>, sizes: &[usize]) -> Vec<Vec<Vec<u8>>> {
    let mut rest = payloads.into_iter();
    sizes
        .iter()
        .map(|&size| rest.by_ref().take(size).collect())
        .collect()
}

#[cfg(test)]
mod tests;
