//! Atomic commits, conditional publishes and state reads.
//!
//! A commit is an array of [`FelixCommitOp`] that the library checks the way
//! `felix-client` does, before anything is sent: exactly one event, and every
//! operation on that event's stream. Strings and payloads are copied before a
//! call returns. All the writes start through [`spawn_publish`], so a write
//! cancelled after it may have been sent reports
//! `FELIX_STATUS_OUTCOME_UNKNOWN`; a state read reports `FELIX_STATUS_CANCELLED`.

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{ClusterClient, CommitError, CommitOp, ConditionalWrite, StateValue};

use crate::boundary::{Failure, guard, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::{Completion, Deliver};
use crate::groups::spawn_read;
use crate::publish::{bytes, laid_end_to_end, routing_key, spawn_publish};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// What one operation of a commit does.
pub type FelixCommitOpKind = i32;

/// The commit's event, delivered to the stream's subscribers. Reads `stream`
/// and `data`.
pub const FELIX_COMMIT_PUBLISH: FelixCommitOpKind = 0;
/// The commit's event, for the stream's consumer groups. `stream` is the
/// queue. Reads `stream` and `data`.
pub const FELIX_COMMIT_ENQUEUE: FelixCommitOpKind = 1;
/// Set a key in the stream's state. Reads `stream`, `key` and `data`.
pub const FELIX_COMMIT_PUT: FelixCommitOpKind = 2;
/// Remove a key from the stream's state. Reads `stream` and `key`.
pub const FELIX_COMMIT_DELETE: FelixCommitOpKind = 3;

/// One operation of an atomic commit. Fields an operation does not read are
/// ignored and may be null. `data` may be null when `data_len` is zero.
#[repr(C)]
pub struct FelixCommitOp {
    /// A `FELIX_COMMIT_*` value.
    pub kind: FelixCommitOpKind,
    /// The stream (or queue) the operation is on, NUL-terminated.
    pub stream: *const c_char,
    /// The state key, NUL-terminated.
    pub key: *const c_char,
    /// The event payload, or the state value.
    pub data: *const u8,
    pub data_len: usize,
}

/// Called once when `felix_client_commit` or `felix_client_commit_on_shard`
/// finishes. `offset` is where the commit's event is read and the version of
/// every key it wrote; it is zero unless the status is `FELIX_STATUS_OK`.
pub type FelixCommitCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        offset: u64,
    ),
>;

/// Called once when a conditional commit or publish finishes. A write the
/// shard refused because its next offset was not the expected one is
/// `FELIX_STATUS_OK` with `written` false, and `offset` is that next offset
/// (the tail): nothing was written. When `written` is true, `offset` is where
/// the write landed.
pub type FelixConditionalCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        written: bool,
        offset: u64,
    ),
>;

/// A key in a stream shard's state.
#[repr(C)]
pub struct FelixStateValue {
    /// False when the key was never written or was deleted.
    pub has_value: bool,
    /// `value_len` bytes, null when empty.
    pub value: *const u8,
    pub value_len: usize,
    /// Whether `version` is set: the offset of the commit that wrote the
    /// value.
    pub has_version: bool,
    pub version: u64,
    /// Whether `as_of` is set: the offset of the last commit the answer
    /// reflects.
    pub has_as_of: bool,
    pub as_of: u64,
}

/// Called once when `felix_client_state_get` or
/// `felix_client_state_get_on_shard` finishes. `state` and the bytes it points
/// to are valid only while the callback runs, and `state` is null unless the
/// status is `FELIX_STATUS_OK`.
pub type FelixStateCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        state: *const FelixStateValue,
    ),
>;

pub(crate) struct Committed(u64);

impl Deliver for Committed {
    type Callback = FelixCommitCallback;

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
            unsafe { callback(user_data, status, message, value.map_or(0, |v| v.0)) };
        }
    }
}

pub(crate) struct Conditional(ConditionalWrite);

impl Deliver for Conditional {
    type Callback = FelixConditionalCallback;

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
            let (written, offset) = match value.map(|v| v.0) {
                Some(ConditionalWrite::Written { offset }) => (true, offset),
                Some(ConditionalWrite::Refused { tail }) => (false, tail),
                None => (false, 0),
            };
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, written, offset) };
        }
    }
}

pub(crate) struct State(StateValue);

impl Deliver for State {
    type Callback = FelixStateCallback;

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
        let Some(callback) = callback else { return };
        let Some(State(state)) = value else {
            // SAFETY: the callback's contract is the header's.
            return unsafe { callback(user_data, status, message, ptr::null()) };
        };
        let (at, len) = state.value.as_deref().map_or((ptr::null(), 0), |value| {
            if value.is_empty() {
                (ptr::null(), 0)
            } else {
                (value.as_ptr(), value.len())
            }
        });
        let view = FelixStateValue {
            has_value: state.value.is_some(),
            value: at,
            value_len: len,
            has_version: state.version.is_some(),
            version: state.version.unwrap_or(0),
            has_as_of: state.as_of.is_some(),
            as_of: state.as_of.unwrap_or(0),
        };
        // SAFETY: `view` and the bytes it points into live until the callback
        // returns.
        unsafe { callback(user_data, status, message, &raw const view) };
    }
}

/// The tenant and namespace of a call, with the client it goes through.
struct Scope {
    inner: Arc<ClusterClient>,
    tenant_id: String,
    ns: String,
}

/// # Safety
///
/// `client` is null or live, and each string null or NUL-terminated.
unsafe fn scope<'a>(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
) -> Result<(&'a FelixClient, Scope), Failure> {
    // SAFETY: null or a live client, per the contract.
    let client = unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
    // SAFETY: each is null or a NUL-terminated string.
    let (tenant_id, ns) = unsafe {
        (
            required_str(tenant_id, "tenant_id")?.to_string(),
            required_str(ns, "ns")?.to_string(),
        )
    };
    let inner = Arc::clone(client.shared.client());
    Ok((
        client,
        Scope {
            inner,
            tenant_id,
            ns,
        },
    ))
}

/// The entity key: `len` bytes, null only when empty. Copied.
///
/// # Safety
///
/// `key` is null or valid for `len` bytes.
unsafe fn entity_key(key: *const u8, len: usize) -> Result<Bytes, Failure> {
    // SAFETY: the caller's contract, passed through.
    let key = unsafe { bytes(key, len, "entity_key") }?;
    Ok(Bytes::copy_from_slice(key))
}

/// The operations of a commit, copied and checked.
///
/// # Safety
///
/// `ops` is null or valid for `count` operations, whose pointers meet the
/// contract of [`FelixCommitOp`].
unsafe fn read_ops(ops: *const FelixCommitOp, count: usize) -> Result<Vec<CommitOp>, Failure> {
    let raw: &[FelixCommitOp] = if count == 0 {
        &[]
    } else if ops.is_null() {
        return Err(Failure::invalid("ops is null but count is not zero"));
    } else if count > isize::MAX as usize / size_of::<FelixCommitOp>() {
        return Err(Failure::invalid("count is too large"));
    } else {
        // SAFETY: non-null, within `isize::MAX` bytes, and the caller
        // promises `count` operations.
        unsafe { std::slice::from_raw_parts(ops, count) }
    };
    let mut parsed = Vec::with_capacity(raw.len());
    for (index, op) in raw.iter().enumerate() {
        // SAFETY: the strings and payload are the caller's, per the contract.
        parsed.push(unsafe { read_op(index, op) }?);
    }
    check(&parsed)?;
    Ok(parsed)
}

/// # Safety
///
/// As for [`read_ops`], for one operation.
unsafe fn read_op(index: usize, op: &FelixCommitOp) -> Result<CommitOp, Failure> {
    let name = |field: &str| format!("ops[{index}].{field}");
    // The kind decides which fields the caller filled in, so it is checked
    // first and nothing it says to ignore is dereferenced.
    if !matches!(
        op.kind,
        FELIX_COMMIT_PUBLISH | FELIX_COMMIT_ENQUEUE | FELIX_COMMIT_PUT | FELIX_COMMIT_DELETE
    ) {
        return Err(Failure::invalid(format!(
            "ops[{index}].kind {} is not a FELIX_COMMIT_* value",
            op.kind
        )));
    }
    // SAFETY: the pointers the kind uses are the caller's, per the contract.
    unsafe {
        let stream = required_str(op.stream, &name("stream"))?.to_string();
        let payload = || -> Result<Bytes, Failure> {
            Ok(Bytes::copy_from_slice(bytes(
                op.data,
                op.data_len,
                &name("data"),
            )?))
        };
        Ok(match op.kind {
            FELIX_COMMIT_PUBLISH => CommitOp::publish(stream, payload()?),
            FELIX_COMMIT_ENQUEUE => CommitOp::enqueue(stream, payload()?),
            FELIX_COMMIT_PUT => {
                let key = required_str(op.key, &name("key"))?;
                CommitOp::put(stream, key, payload()?)
            }
            _ => CommitOp::delete(stream, required_str(op.key, &name("key"))?),
        })
    }
}

/// The checks `felix-client` makes before it sends a commit, made here so a
/// bad commit is refused at the call instead of in a callback.
fn check(ops: &[CommitOp]) -> Result<(), Failure> {
    let stream_of = |op: &CommitOp| match op {
        CommitOp::Publish { stream, .. }
        | CommitOp::Put { stream, .. }
        | CommitOp::Delete { stream, .. } => stream.clone(),
        CommitOp::Enqueue { queue, .. } => queue.clone(),
    };
    let is_event =
        |op: &&CommitOp| matches!(op, CommitOp::Publish { .. } | CommitOp::Enqueue { .. });
    let events = ops.iter().filter(is_event).count();
    if events != 1 {
        return Err(Failure::invalid(
            CommitError::EventCount(events).to_string(),
        ));
    }
    let owner = ops.iter().find(is_event).map(stream_of).unwrap_or_default();
    if let Some((index, op)) = ops
        .iter()
        .enumerate()
        .find(|(_, op)| stream_of(op) != owner)
    {
        let refusal = CommitError::NotOnOwningShard {
            index,
            stream: stream_of(op),
            owner,
        };
        return Err(Failure::invalid(refusal.to_string()));
    }
    Ok(())
}

/// Commit `ops` on the shard `entity_key` routes to, as one record: every
/// reader sees all of it or none of it.
///
/// `ops` must hold exactly one event (`FELIX_COMMIT_PUBLISH` or
/// `FELIX_COMMIT_ENQUEUE`) and every operation must name that event's stream;
/// anything else is `FELIX_STATUS_INVALID_ARGUMENT` at the call, and the
/// callback never runs. Everything is copied before this returns. A broker
/// that does not serve commits completes the call with
/// `FELIX_STATUS_UNSUPPORTED`.
///
/// A write: cancelled after it may have been sent, it completes with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`. A commit that may have been applied is not
/// sent again.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `entity_key` is
/// null or valid for `entity_key_len` bytes, `ops` is null or valid for
/// `count` operations whose pointers are valid for the call, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_commit(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    entity_key: *const u8,
    entity_key_len: usize,
    ops: *const FelixCommitOp,
    count: usize,
    cancel: *const FelixCancelToken,
    callback: FelixCommitCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let entity = unsafe { self::entity_key(entity_key, entity_key_len) }?;
        // SAFETY: as above.
        let ops = unsafe { read_ops(ops, count) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Committed>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner.commit(&tenant_id, &ns, &entity, ops).await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(
                outcome
                    .map(|receipt| Committed(receipt.offset))
                    .map_err(Failure::from),
            );
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_client_commit` to `shard` of the stream, rather than the shard
/// `entity_key` hashes to, for a writer that owns its partitioning.
///
/// `entity_key` still travels with the commit but does not choose the shard.
/// A shard the stream does not have is refused by the broker and nothing is
/// written. A broker too old to take a shard number completes the call with
/// an error without sending anything. Read the state back with
/// `felix_client_state_get_on_shard`.
///
/// # Safety
///
/// As for `felix_client_commit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_commit_on_shard(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    shard: u32,
    entity_key: *const u8,
    entity_key_len: usize,
    ops: *const FelixCommitOp,
    count: usize,
    cancel: *const FelixCancelToken,
    callback: FelixCommitCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let entity = unsafe { self::entity_key(entity_key, entity_key_len) }?;
        // SAFETY: as above.
        let ops = unsafe { read_ops(ops, count) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Committed>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .commit_on_shard(&tenant_id, &ns, shard, &entity, ops)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(
                outcome
                    .map(|receipt| Committed(receipt.offset))
                    .map_err(Failure::from),
            );
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_client_commit`, only if the commit would land at exactly
/// `expected_offset`, the shard's next offset: nothing has been appended
/// since the caller saw the tail. A compare-and-set on the whole shard.
///
/// A refusal is an answer, not an error: the callback gets
/// `FELIX_STATUS_OK`, `written` false and the shard's tail, and nothing was
/// written. A broker that does not support conditional writes completes the
/// call with an error without sending anything.
///
/// # Safety
///
/// As for `felix_client_commit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_commit_if(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    entity_key: *const u8,
    entity_key_len: usize,
    ops: *const FelixCommitOp,
    count: usize,
    expected_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixConditionalCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let entity = unsafe { self::entity_key(entity_key, entity_key_len) }?;
        // SAFETY: as above.
        let ops = unsafe { read_ops(ops, count) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Conditional>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .commit_if(&tenant_id, &ns, &entity, ops, expected_offset)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Conditional).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_client_commit_if` to `shard`; see `felix_client_commit_on_shard`.
/// `expected_offset` is that shard's next offset.
///
/// # Safety
///
/// As for `felix_client_commit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_commit_if_on_shard(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    shard: u32,
    entity_key: *const u8,
    entity_key_len: usize,
    ops: *const FelixCommitOp,
    count: usize,
    expected_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixConditionalCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let entity = unsafe { self::entity_key(entity_key, entity_key_len) }?;
        // SAFETY: as above.
        let ops = unsafe { read_ops(ops, count) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Conditional>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .commit_if_on_shard(&tenant_id, &ns, shard, &entity, ops, expected_offset)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Conditional).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Publish a batch to one shard, only if it would land at exactly
/// `expected_offset`, the shard's next offset.
///
/// The `count` payloads lie end to end in `data`, the i-th `lens[i]` bytes
/// long, as for `felix_client_publish_batch`; `count` must not be zero. `key`
/// routes the batch: null is shard 0, otherwise the key's shard. The batch is
/// written whole or not at all. Answers as `felix_client_commit_if` does: a
/// refusal is `FELIX_STATUS_OK` with `written` false and the shard's tail.
///
/// A write: cancelled after it may have been sent, it completes with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`.
///
/// # Safety
///
/// As for `felix_client_publish_batch`, with `stream` a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_if(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    key: *const u8,
    key_len: usize,
    data: *const u8,
    lens: *const usize,
    count: usize,
    expected_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixConditionalCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let stream = unsafe { required_str(stream, "stream") }?.to_string();
        // SAFETY: as above.
        let payloads = unsafe { payload_batch(data, lens, count) }?;
        // SAFETY: as above.
        let key = unsafe { routing_key(key, key_len) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Conditional>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .publish_if(
                    &tenant_id,
                    &ns,
                    &stream,
                    key.as_deref(),
                    payloads,
                    expected_offset,
                )
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Conditional).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_client_publish_if` to `shard`, rather than the shard a key would
/// hash to. `expected_offset` is that shard's next offset.
///
/// # Safety
///
/// As for `felix_client_publish_if`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_if_on_shard(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    data: *const u8,
    lens: *const usize,
    count: usize,
    expected_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixConditionalCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let stream = unsafe { required_str(stream, "stream") }?.to_string();
        // SAFETY: as above.
        let payloads = unsafe { payload_batch(data, lens, count) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Conditional>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .publish_if_on_shard(&tenant_id, &ns, &stream, shard, payloads, expected_offset)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Conditional).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// The payloads of a conditional publish; the broker refuses an empty one, so
/// the call does.
///
/// # Safety
///
/// As for `laid_end_to_end`.
unsafe fn payload_batch(
    data: *const u8,
    lens: *const usize,
    count: usize,
) -> Result<Vec<Vec<u8>>, Failure> {
    if count == 0 {
        return Err(Failure::invalid(
            "a conditional publish needs at least one record",
        ));
    }
    // SAFETY: the caller's contract, passed through.
    unsafe { laid_end_to_end(data, lens, count, "data", "lens") }
}

/// Read the state under `key` on the shard `entity_key` routes to.
///
/// `entity_key` is what the commits that wrote the key were routed by. A key
/// that was never written, or was deleted, is `FELIX_STATUS_OK` with
/// `has_value` false. `version` is the offset of the commit that wrote the
/// value; `as_of` is the offset of the last commit the answer reflects.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `entity_key` is
/// null or valid for `entity_key_len` bytes, `cancel` is null or a live
/// token, and `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_state_get(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    entity_key: *const u8,
    entity_key_len: usize,
    key: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixStateCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let (stream, key) = unsafe {
            (
                required_str(stream, "stream")?.to_string(),
                required_str(key, "key")?.to_string(),
            )
        };
        // SAFETY: as above.
        let entity = unsafe { self::entity_key(entity_key, entity_key_len) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<State>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .state_get(&tenant_id, &ns, &stream, &entity, &key)
                .await
        };
        spawn_read(client, user, done, ask, State);
        Ok(FELIX_STATUS_OK)
    })
}

/// `felix_client_state_get` on `shard`, for state written with
/// `felix_client_commit_on_shard`.
///
/// # Safety
///
/// As for `felix_client_state_get`, without an entity key.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_state_get_on_shard(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    key: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixStateCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let (stream, key) = unsafe {
            (
                required_str(stream, "stream")?.to_string(),
                required_str(key, "key")?.to_string(),
            )
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<State>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .state_get_on_shard(&tenant_id, &ns, &stream, shard, &key)
                .await
        };
        spawn_read(client, user, done, ask, State);
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
