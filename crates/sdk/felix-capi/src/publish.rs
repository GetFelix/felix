//! Publishing: one record, a batch to one shard, or records that each carry
//! their own routing key.
//!
//! Every publish is a write that is not cancel-safe, so all of them start
//! through [`spawn_publish`].

use std::ffi::{c_char, c_void};
use std::future::Future;
use std::ptr;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{AckMode, BatchReport, ClusterClient};
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver, c_message, cancelled, is_cancelled};
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_ERROR, FELIX_STATUS_OK, FELIX_STATUS_OUTCOME_UNKNOWN,
    FelixStatus,
};

/// What the broker must have done before a publish completes.
pub type FelixAckMode = i32;

/// Fire and forget: complete once the record is sent.
pub const FELIX_ACK_NONE: FelixAckMode = 0;
/// Wait for the broker to acknowledge this record.
pub const FELIX_ACK_PER_MESSAGE: FelixAckMode = 1;
/// Wait for the broker to acknowledge the batch the record went out in.
pub const FELIX_ACK_PER_BATCH: FelixAckMode = 2;

/// What a publish does when the connection is lost after the record may have
/// been written.
pub type FelixDelivery = i32;

/// Report the failure and do not send the record again, so it is never
/// written twice. A record a broker refused without applying it is still
/// sent once more, to the shard's current owner.
pub const FELIX_DELIVERY_AT_MOST_ONCE: FelixDelivery = 0;
/// Reconnect and send it again, until it is acknowledged or the client's
/// reconnect policy gives up. A record can be written twice.
pub const FELIX_DELIVERY_AT_LEAST_ONCE: FelixDelivery = 1;

/// Called once when `felix_client_publish` finishes. On `FELIX_STATUS_OK`,
/// `has_offset` says whether the broker reported the record's log offset,
/// and `offset` holds it when it did.
pub type FelixPublishCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        has_offset: bool,
        offset: u64,
    ),
>;

/// How one record of a batch fared.
#[repr(C)]
pub struct FelixRecordResult {
    /// `FELIX_STATUS_OK` when the record was accepted.
    pub status: FelixStatus,
    /// Whether `offset` holds the record's log offset.
    pub has_offset: bool,
    /// The record's log offset, when `has_offset` is set.
    pub offset: u64,
    /// Why the record failed, or null. Records that failed together share
    /// one message. Valid only while the callback runs.
    pub message: *const c_char,
}

/// Called once when a batch publish finishes.
///
/// `results` holds `count` entries, one per record in the order they were
/// passed, and is valid only while the callback runs. `status` is
/// `FELIX_STATUS_OK` when every record was accepted, else the status of the
/// first record that failed, with its message.
///
/// `results` is null, and `count` zero, when no record has an outcome of its
/// own: the batch was empty, or the call was cancelled or panicked, which
/// `status` says.
pub type FelixBatchCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        results: *const FelixRecordResult,
        count: usize,
    ),
>;

/// A publish's result: the record's offset, when the broker reported one.
pub(crate) struct Published(pub(crate) Option<u64>);

impl Deliver for Published {
    type Callback = FelixPublishCallback;

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
            let offset = value.and_then(|published| published.0);
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

/// A batch's result, as the pieces it was sent in.
pub(crate) struct Recorded {
    count: usize,
    pieces: Vec<Piece>,
}

/// Records sent together and accepted or refused as a unit. The broker
/// appends a piece contiguously, so the record at `records[i]` has the
/// piece's first offset plus `i`.
struct Piece {
    /// Positions in the caller's batch, ascending.
    records: Vec<usize>,
    outcome: Result<Option<u64>, Failure>,
}

/// Reported for a record no piece accounts for. The client guarantees every
/// record is in one; this keeps a broken guarantee from reading as success.
const NO_OUTCOME: &std::ffi::CStr = c"felix reported no outcome for this record";

impl Recorded {
    /// A batch sent as one piece.
    fn whole(count: usize, outcome: Result<Option<u64>, Failure>) -> Self {
        let pieces = if count == 0 {
            Vec::new()
        } else {
            vec![Piece {
                records: (0..count).collect(),
                outcome,
            }]
        };
        Self { count, pieces }
    }

    fn from_report(count: usize, report: BatchReport) -> Self {
        let pieces = report
            .shards
            .into_iter()
            .map(|shard| Piece {
                records: shard.records,
                outcome: shard.outcome.map_err(Failure::from),
            })
            .collect();
        Self { count, pieces }
    }

    /// `FELIX_STATUS_OK` when every record was accepted, else the status and
    /// message of the first record that failed.
    fn summary(&self) -> (FelixStatus, Option<String>) {
        let first_failed = self
            .pieces
            .iter()
            .filter_map(|piece| match &piece.outcome {
                Err(failure) => piece.records.first().map(|&first| (first, failure)),
                Ok(_) => None,
            })
            .min_by_key(|(first, _)| *first);
        if let Some((_, failure)) = first_failed {
            return (failure.status, Some(failure.message.clone()));
        }
        let covered: usize = self.pieces.iter().map(|piece| piece.records.len()).sum();
        if covered < self.count {
            return (
                FELIX_STATUS_ERROR,
                Some(NO_OUTCOME.to_string_lossy().into_owned()),
            );
        }
        (FELIX_STATUS_OK, None)
    }

    fn report(self, done: Completion<Self>) {
        let (status, message) = self.summary();
        done.with_status(status, message, self);
    }
}

impl Deliver for Recorded {
    type Callback = FelixBatchCallback;

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
        let Some(recorded) = value.filter(|recorded| recorded.count > 0) else {
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, ptr::null(), 0) };
            return;
        };
        let messages: Vec<_> = recorded
            .pieces
            .iter()
            .map(|piece| {
                piece
                    .outcome
                    .as_ref()
                    .err()
                    .map(|failure| c_message(failure.message.clone()))
            })
            .collect();
        let mut results: Vec<FelixRecordResult> = (0..recorded.count)
            .map(|_| FelixRecordResult {
                status: FELIX_STATUS_ERROR,
                has_offset: false,
                offset: 0,
                message: NO_OUTCOME.as_ptr(),
            })
            .collect();
        for (piece, piece_message) in recorded.pieces.iter().zip(&messages) {
            for (index, &position) in piece.records.iter().enumerate() {
                let Some(result) = results.get_mut(position) else {
                    continue;
                };
                *result = match &piece.outcome {
                    Ok(first) => {
                        let offset = first.and_then(|first| first.checked_add(index as u64));
                        FelixRecordResult {
                            status: FELIX_STATUS_OK,
                            has_offset: offset.is_some(),
                            offset: offset.unwrap_or(0),
                            message: ptr::null(),
                        }
                    }
                    Err(failure) => FelixRecordResult {
                        status: failure.status,
                        has_offset: false,
                        offset: 0,
                        message: piece_message.as_ref().map_or(ptr::null(), |m| m.as_ptr()),
                    },
                };
            }
        }
        // SAFETY: as above; `results` and the messages it points into live
        // until the callback returns.
        unsafe { callback(user_data, status, message, results.as_ptr(), results.len()) };
    }
}

/// Publish one record.
///
/// `payload` may be null when `payload_len` is zero; it is copied before this
/// returns, as is the key. `key` routes the record: records sharing a key
/// share a shard and stay in order. Null means no key, and the record goes to
/// shard 0; a non-null `key` with `key_len` zero is the empty key, which is a
/// key like any other.
///
/// `ack` is one of the `FELIX_ACK_*` values, `delivery` one of the
/// `FELIX_DELIVERY_*` values. There is no offset when `ack` is
/// `FELIX_ACK_NONE`, the stream has no log, or the broker acknowledged before
/// writing.
///
/// Publishes in flight at the same time may reach the broker in any order.
/// To keep records in order, start each one from the previous one's
/// callback.
///
/// A publish cancelled before it starts completes with
/// `FELIX_STATUS_CANCELLED`; after it may have been sent, with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`.
///
/// # Safety
///
/// `client` is a live client. The strings are null or NUL-terminated.
/// `payload` is null or valid for `payload_len` bytes, and `key` for
/// `key_len`. `cancel` is null or a live token, and `callback` is safe to
/// call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    payload: *const u8,
    payload_len: usize,
    key: *const u8,
    key_len: usize,
    ack: FelixAckMode,
    delivery: FelixDelivery,
    cancel: *const FelixCancelToken,
    callback: FelixPublishCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { target(client, tenant_id, ns, stream) }?;
        // SAFETY: as above. Copied, because the client owns what it sends.
        let payload = unsafe { bytes(payload, payload_len, "payload") }?.to_vec();
        // SAFETY: as above.
        let key = unsafe { routing_key(key, key_len) }?;
        let ack = parse_ack(ack)?;
        let at_least_once = parse_delivery(delivery)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Published>::new(callback, user_data)?;
        let Target {
            inner,
            tenant_id,
            ns,
            stream,
        } = target;
        let publish = async move {
            let (t, n, s) = (tenant_id.as_str(), ns.as_str(), stream.as_str());
            match (key, at_least_once) {
                (None, false) => inner.publish(t, n, s, payload, ack).await,
                (Some(key), false) => inner.publish_keyed(t, n, s, payload, key, ack).await,
                (None, true) => inner.publish_at_least_once(t, n, s, payload, ack).await,
                // The client resends keyed records only as a batch; this is
                // a batch of one.
                (Some(key), true) => {
                    let report = inner
                        .publish_records_at_least_once(t, n, s, vec![(key, payload)], ack)
                        .await;
                    report
                        .shards
                        .into_iter()
                        .find(|shard| shard.records.contains(&0))
                        .map_or_else(|| Err(anyhow::anyhow!("no outcome")), |shard| shard.outcome)
                }
            }
        };
        spawn_publish(client, user, done, publish, |done, published| {
            done.result(published.map(Published).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Publish a batch to one shard, as one frame that the broker appends
/// contiguously and accepts or refuses as a unit.
///
/// The `count` payloads lie end to end in `data`, the i-th `lens[i]` bytes
/// long; both are copied before this returns. `key` routes the batch as it
/// does a single record for `felix_client_publish`: null is shard 0, and
/// otherwise every record goes to the key's shard. A batch the broker may
/// have written is not sent again; for that, use
/// `felix_client_publish_records` with `FELIX_DELIVERY_AT_LEAST_ONCE`.
///
/// Every record gets the batch's outcome in `callback`, and on success its
/// own offset, when the broker reported one.
///
/// # Safety
///
/// As for `felix_client_publish`. `lens` is null or valid for `count`
/// lengths, and `data` is null or valid for their sum. Either may be null
/// when the sum is zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_batch(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    data: *const u8,
    lens: *const usize,
    count: usize,
    key: *const u8,
    key_len: usize,
    ack: FelixAckMode,
    cancel: *const FelixCancelToken,
    callback: FelixBatchCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { target(client, tenant_id, ns, stream) }?;
        // SAFETY: as above.
        let payloads = unsafe { laid_end_to_end(data, lens, count, "data", "lens") }?;
        // SAFETY: as above.
        let key = unsafe { routing_key(key, key_len) }?;
        let ack = parse_ack(ack)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Recorded>::new(callback, user_data)?;
        let publish = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            match key {
                None => target.inner.publish_batch(t, n, s, payloads, ack).await,
                Some(key) => {
                    let inner = &target.inner;
                    inner.publish_batch_keyed(t, n, s, key, payloads, ack).await
                }
            }
        };
        spawn_publish(client, user, done, publish, move |done, outcome| {
            Recorded::whole(count, outcome.map_err(Failure::from)).report(done);
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Publish a batch to `shard` of a stream, rather than the shard a key would
/// hash to, for a producer that does its own partitioning.
///
/// Laid out, sent and reported as for `felix_client_publish_batch`. A shard
/// the stream does not have is refused by the broker and nothing is written.
/// Fails without sending anything against a broker too old to take a shard
/// number, which would otherwise write to shard 0.
///
/// # Safety
///
/// As for `felix_client_publish_batch`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_on_shard(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    data: *const u8,
    lens: *const usize,
    count: usize,
    ack: FelixAckMode,
    cancel: *const FelixCancelToken,
    callback: FelixBatchCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { target(client, tenant_id, ns, stream) }?;
        // SAFETY: as above.
        let payloads = unsafe { laid_end_to_end(data, lens, count, "data", "lens") }?;
        let ack = parse_ack(ack)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Recorded>::new(callback, user_data)?;
        let publish = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .publish_batch_on_shard(t, n, s, shard, payloads, ack)
                .await
        };
        spawn_publish(client, user, done, publish, move |done, outcome| {
            Recorded::whole(count, outcome.map_err(Failure::from)).report(done);
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Publish records that each carry their own routing key.
///
/// The records are split by shard, and each shard's records go to its owner
/// as one batch, in the order given; there is no order between shards. A
/// shard's batch is accepted or refused as a unit and does not stop the
/// others, so `callback` can report some records accepted and others failed.
///
/// The payloads lie end to end in `data` as for `felix_client_publish_batch`,
/// and the keys the same way in `keys`, the i-th `key_lens[i]` bytes long.
/// Every record has a key; a zero length is the empty key.
///
/// With `FELIX_DELIVERY_AT_LEAST_ONCE` a shard's batch whose outcome is in
/// doubt is sent again, which can write it twice, and a resent batch lands
/// after records sent later. Under `FELIX_ACK_NONE` a record counts as
/// accepted once queued.
///
/// # Safety
///
/// As for `felix_client_publish_batch`, and `key_lens` and `keys` as `lens`
/// and `data`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_records(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    data: *const u8,
    lens: *const usize,
    keys: *const u8,
    key_lens: *const usize,
    count: usize,
    ack: FelixAckMode,
    delivery: FelixDelivery,
    cancel: *const FelixCancelToken,
    callback: FelixBatchCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { target(client, tenant_id, ns, stream) }?;
        // SAFETY: as above.
        let payloads = unsafe { laid_end_to_end(data, lens, count, "data", "lens") }?;
        // SAFETY: as above.
        let keys = unsafe { laid_end_to_end(keys, key_lens, count, "keys", "key_lens") }?;
        let ack = parse_ack(ack)?;
        let at_least_once = parse_delivery(delivery)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Recorded>::new(callback, user_data)?;
        let records: Vec<(Bytes, Vec<u8>)> =
            keys.into_iter().map(Bytes::from).zip(payloads).collect();
        let publish = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            if at_least_once {
                target
                    .inner
                    .publish_records_at_least_once(t, n, s, records, ack)
                    .await
            } else {
                target.inner.publish_records(t, n, s, records, ack).await
            }
        };
        spawn_publish(client, user, done, publish, move |done, report| {
            Recorded::from_report(count, report).report(done);
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// The client and stream a publish goes to, owned so the task can hold them.
struct Target {
    inner: Arc<ClusterClient>,
    tenant_id: String,
    ns: String,
    stream: String,
}

/// # Safety
///
/// `client` is null or live, and each string null or NUL-terminated.
unsafe fn target<'a>(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
) -> Result<(&'a FelixClient, Target), Failure> {
    // SAFETY: the caller's contract, passed through.
    let client = unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
    // SAFETY: as above.
    let (tenant_id, ns, stream) = unsafe {
        (
            required_str(tenant_id, "tenant_id")?.to_string(),
            required_str(ns, "ns")?.to_string(),
            required_str(stream, "stream")?.to_string(),
        )
    };
    let inner = Arc::clone(client.shared.client());
    Ok((
        client,
        Target {
            inner,
            tenant_id,
            ns,
            stream,
        },
    ))
}

/// Run a publish, or any write, on the client's runtime and hand its outcome
/// to `report`.
///
/// A write is not cancel-safe: once started the record may land anyway.
/// So a publish cancelled before it starts reports `FELIX_STATUS_CANCELLED`,
/// and one cancelled after, `FELIX_STATUS_OUTCOME_UNKNOWN`.
pub(crate) fn spawn_publish<V, T, F>(
    client: &FelixClient,
    user: Option<CancellationToken>,
    done: Completion<V>,
    publish: F,
    report: impl FnOnce(Completion<V>, T) + Send + 'static,
) where
    V: Deliver,
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    spawn_write(
        &client.shared,
        client.cancel.clone(),
        user,
        done,
        publish,
        report,
    );
}

/// [`spawn_publish`] for a handle other than the client, which brings its own
/// cancel token.
pub(crate) fn spawn_write<V, T, F>(
    shared: &Shared,
    handle: CancellationToken,
    user: Option<CancellationToken>,
    done: Completion<V>,
    write: F,
    report: impl FnOnce(Completion<V>, T) + Send + 'static,
) where
    V: Deliver,
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    shared.spawn(done, move |done| async move {
        if is_cancelled(&handle, user.as_ref()) {
            return done.status(FELIX_STATUS_CANCELLED);
        }
        tokio::select! {
            biased;
            () = cancelled(&handle, user.as_ref()) => done.fail(Failure {
                status: FELIX_STATUS_OUTCOME_UNKNOWN,
                message: "the write was cancelled after it may have been sent".to_string(),
            }),
            outcome = write => report(done, outcome),
        }
    });
}

/// `len` bytes at `ptr`, which may be null only when `len` is zero.
///
/// # Safety
///
/// `ptr` is null or valid for `len` bytes that outlive `'a`.
pub(crate) unsafe fn bytes<'a>(
    ptr: *const u8,
    len: usize,
    name: &str,
) -> Result<&'a [u8], Failure> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Failure::invalid(format!(
            "{name} is null but {name}_len is not zero"
        )));
    }
    if len > isize::MAX as usize {
        return Err(Failure::invalid(format!("{name}_len is too large")));
    }
    // SAFETY: non-null, within `isize::MAX`, and the caller promises `len`
    // readable bytes.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// The routing key, or `None` for a null `key`. Copied.
///
/// # Safety
///
/// `key` is null or valid for `len` bytes.
pub(crate) unsafe fn routing_key(key: *const u8, len: usize) -> Result<Option<Bytes>, Failure> {
    if key.is_null() {
        if len != 0 {
            return Err(Failure::invalid("key is null but key_len is not zero"));
        }
        return Ok(None);
    }
    // SAFETY: the caller's contract, passed through.
    let key = unsafe { bytes(key, len, "key") }?;
    Ok(Some(Bytes::copy_from_slice(key)))
}

/// `count` byte strings laid end to end at `data`, the i-th `lens[i]` bytes
/// long, copied. One buffer and one array of lengths is what a caller in a
/// managed language can pin and pass without a pointer per record.
///
/// # Safety
///
/// `lens` is null or valid for `count` lengths, and `data` null or valid for
/// their sum.
pub(crate) unsafe fn laid_end_to_end(
    data: *const u8,
    lens: *const usize,
    count: usize,
    name: &str,
    lens_name: &str,
) -> Result<Vec<Vec<u8>>, Failure> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if lens.is_null() {
        return Err(Failure::invalid(format!("{lens_name} is null")));
    }
    if count > isize::MAX as usize / size_of::<usize>() {
        return Err(Failure::invalid("count is too large"));
    }
    // SAFETY: non-null, within `isize::MAX` bytes, and the caller promises
    // `count` lengths.
    let lens = unsafe { std::slice::from_raw_parts(lens, count) };
    let total = lens
        .iter()
        .try_fold(0usize, |total, &len| total.checked_add(len))
        .ok_or_else(|| Failure::invalid(format!("the {lens_name} add up past usize")))?;
    // SAFETY: the caller promises `total` readable bytes at `data`.
    let all = unsafe { bytes(data, total, name) }.map_err(|failure| Failure {
        message: format!("{name} is null but its lengths add up to {total}"),
        ..failure
    })?;
    let mut at = 0;
    Ok(lens
        .iter()
        .map(|&len| {
            let one = all[at..at + len].to_vec();
            at += len;
            one
        })
        .collect())
}

fn parse_ack(ack: FelixAckMode) -> Result<AckMode, Failure> {
    match ack {
        FELIX_ACK_NONE => Ok(AckMode::None),
        FELIX_ACK_PER_MESSAGE => Ok(AckMode::PerMessage),
        FELIX_ACK_PER_BATCH => Ok(AckMode::PerBatch),
        other => Err(Failure::invalid(format!(
            "ack {other} is not a FELIX_ACK_* value"
        ))),
    }
}

/// Whether the publish is sent again when its outcome is in doubt.
fn parse_delivery(delivery: FelixDelivery) -> Result<bool, Failure> {
    match delivery {
        FELIX_DELIVERY_AT_MOST_ONCE => Ok(false),
        FELIX_DELIVERY_AT_LEAST_ONCE => Ok(true),
        other => Err(Failure::invalid(format!(
            "delivery {other} is not a FELIX_DELIVERY_* value"
        ))),
    }
}

#[cfg(test)]
mod tests;
