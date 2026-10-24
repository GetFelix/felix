//! Caches and counters: single-key reads and writes.
//!
//! Keys are UTF-8 C strings. Values are a pointer and a length, copied before
//! a call returns; a null pointer with length zero is the empty value.
//! Values handed back are valid only while the callback runs.
//!
//! Reads report `FELIX_STATUS_CANCELLED` when cancelled. Writes start through
//! [`spawn_publish`], which reports `FELIX_STATUS_OUTCOME_UNKNOWN` once a
//! write may have been sent.

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use bytes::Bytes;
use felix_client::{CacheCondition, CacheConditionResult, ClusterClient, VersionedValue};

use crate::boundary::{Failure, guard, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::{Completion, Deliver};
use crate::groups::{Done, FelixDoneCallback, spawn_read};
use crate::publish::{bytes, spawn_publish};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// What a conditional write requires of the key.
pub type FelixCacheCondition = i32;

/// The key has no live entry: never written, deleted, or expired.
pub const FELIX_IF_ABSENT: FelixCacheCondition = 0;
/// The key's live entry has exactly the given version.
pub const FELIX_IF_VERSION: FelixCacheCondition = 1;

/// Called once when `felix_client_cache_get` or `felix_client_cache_delete`
/// finishes. On `FELIX_STATUS_OK`, `found` says whether there was a value
/// (for a delete, the one that was removed), and `value` holds `len` bytes of
/// it. `value` is valid only while the callback runs, and may be null when
/// `len` is zero.
pub type FelixValueCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        found: bool,
        value: *const u8,
        len: usize,
    ),
>;

/// Called once when `felix_client_cache_get_versioned` finishes. As
/// `FelixValueCallback`, with the entry's `version`, which a conditional
/// write can name.
pub type FelixVersionedCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        found: bool,
        value: *const u8,
        len: usize,
        version: u64,
    ),
>;

/// Called once when `felix_client_cache_put_if` or
/// `felix_client_cache_delete_if` finishes. A condition that did not hold is
/// `FELIX_STATUS_OK` with `applied` false. `has_version` says whether the
/// broker reported the key's version: the new one when applied, and the
/// current one when not.
pub type FelixConditionCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        applied: bool,
        has_version: bool,
        version: u64,
    ),
>;

/// Called once when `felix_client_counter_add` or `felix_client_counter_get`
/// finishes. `has_value` is false for a counter that does not exist, which
/// only a get can report; an add always has the new value.
pub type FelixCounterCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        has_value: bool,
        value: i64,
    ),
>;

pub(crate) struct Value(Option<Bytes>);

impl Deliver for Value {
    type Callback = FelixValueCallback;

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
            let found = value.and_then(|value| value.0);
            let (at, len) = found.as_deref().map_or((ptr::null(), 0), slice_parts);
            // SAFETY: the callback's contract is the header's; `found` lives
            // until it returns.
            unsafe { callback(user_data, status, message, found.is_some(), at, len) };
        }
    }
}

pub(crate) struct Versioned(Option<VersionedValue>);

impl Deliver for Versioned {
    type Callback = FelixVersionedCallback;

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
            let found = value.and_then(|value| value.0);
            let (at, len) = found
                .as_ref()
                .map_or((ptr::null(), 0), |found| slice_parts(&found.value));
            let version = found.as_ref().map_or(0, |found| found.version);
            // SAFETY: as above.
            unsafe {
                callback(
                    user_data,
                    status,
                    message,
                    found.is_some(),
                    at,
                    len,
                    version,
                )
            };
        }
    }
}

pub(crate) struct Condition(CacheConditionResult);

impl Deliver for Condition {
    type Callback = FelixConditionCallback;

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
            let (applied, version) =
                value.map_or((false, None), |value| (value.0.applied, value.0.version));
            // SAFETY: the callback's contract is the header's.
            unsafe {
                callback(
                    user_data,
                    status,
                    message,
                    applied,
                    version.is_some(),
                    version.unwrap_or(0),
                );
            }
        }
    }
}

pub(crate) struct Counter(Option<i64>);

impl Deliver for Counter {
    type Callback = FelixCounterCallback;

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
            let counter = value.and_then(|value| value.0);
            // SAFETY: the callback's contract is the header's.
            unsafe {
                callback(
                    user_data,
                    status,
                    message,
                    counter.is_some(),
                    counter.unwrap_or(0),
                );
            }
        }
    }
}

/// A cache named by a call, with the client it goes through.
pub(crate) struct Names {
    pub(crate) inner: Arc<ClusterClient>,
    pub(crate) tenant_id: String,
    pub(crate) ns: String,
    pub(crate) cache: String,
}

/// A cache and one key in it.
struct Target {
    names: Names,
    key: String,
}

/// # Safety
///
/// `client` is null or live, and each string null or NUL-terminated.
pub(crate) unsafe fn cache_names<'a>(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
) -> Result<(&'a FelixClient, Names), Failure> {
    // SAFETY: null or a live client, per the contract.
    let client = unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
    // SAFETY: each is null or a NUL-terminated string.
    let (tenant_id, ns, cache) = unsafe {
        (
            required_str(tenant_id, "tenant_id")?.to_string(),
            required_str(ns, "ns")?.to_string(),
            required_str(cache, "cache")?.to_string(),
        )
    };
    let names = Names {
        inner: Arc::clone(client.shared.client()),
        tenant_id,
        ns,
        cache,
    };
    Ok((client, names))
}

/// # Safety
///
/// As `cache_names`, and `key` is null or NUL-terminated.
unsafe fn key_target<'a>(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
) -> Result<(&'a FelixClient, Target), Failure> {
    // SAFETY: the caller's contract, passed through.
    let (client, names) = unsafe { cache_names(client, tenant_id, ns, cache) }?;
    // SAFETY: as above.
    let key = unsafe { required_str(key, "key") }?.to_string();
    Ok((client, Target { names, key }))
}

/// The value argument, copied: a pointer and a length, null only when empty.
///
/// # Safety
///
/// `value` is null or valid for `len` bytes.
unsafe fn copy_value(value: *const u8, len: usize) -> Result<Bytes, Failure> {
    // SAFETY: the caller's contract, passed through.
    let value = unsafe { bytes(value, len, "value") }?;
    Ok(Bytes::copy_from_slice(value))
}

fn slice_parts(value: &[u8]) -> (*const u8, usize) {
    if value.is_empty() {
        (ptr::null(), 0)
    } else {
        (value.as_ptr(), value.len())
    }
}

fn ttl(ttl_ms: u64) -> Option<u64> {
    (ttl_ms != 0).then_some(ttl_ms)
}

/// A condition kind and the version it may carry, checked.
fn condition(kind: FelixCacheCondition, version: u64) -> Result<CacheCondition, Failure> {
    match kind {
        FELIX_IF_ABSENT => Ok(CacheCondition::Absent),
        FELIX_IF_VERSION => Ok(CacheCondition::Version(version)),
        other => Err(Failure::invalid(format!("unknown condition kind {other}"))),
    }
}

/// Store `value` under `key`. `ttl_ms` is how long it lives; zero is for
/// ever.
///
/// A write: cancelled after it may have been sent, it completes with
/// `FELIX_STATUS_OUTCOME_UNKNOWN`, and a put that may have been applied is
/// not sent again. A put is safe to repeat.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `value` is null
/// or valid for `len` bytes, `cancel` is null or a live token, and `callback`
/// is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_put(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    value: *const u8,
    len: usize,
    ttl_ms: u64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: as above.
        let value = unsafe { copy_value(value, len) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Done>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names
                .inner
                .cache_put(t, n, c, &key, value, ttl(ttl_ms))
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(|()| Done).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Read the value under `key`. A missing or expired key is
/// `FELIX_STATUS_OK` with `found` false.
///
/// # Safety
///
/// As for `felix_client_cache_put`, without a value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_get(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixValueCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Value>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names.inner.cache_get(t, n, c, &key).await
        };
        spawn_read(client, user, done, ask, Value);
        Ok(FELIX_STATUS_OK)
    })
}

/// Remove `key`. On `FELIX_STATUS_OK`, `found` is whether there was a value
/// to remove, and `value` is that value.
///
/// A write; see `felix_client_cache_put` for cancellation.
///
/// # Safety
///
/// As for `felix_client_cache_get`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_delete(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixValueCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Value>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names.inner.cache_delete(t, n, c, &key).await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Value).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Read the value under `key` with the version a conditional write can name.
///
/// # Safety
///
/// As for `felix_client_cache_get`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_get_versioned(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixVersionedCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Versioned>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names.inner.cache_get_versioned(t, n, c, &key).await
        };
        spawn_read(client, user, done, ask, Versioned);
        Ok(FELIX_STATUS_OK)
    })
}

/// Store `value` under `key` only if `kind` holds: `FELIX_IF_ABSENT`, or
/// `FELIX_IF_VERSION` with the entry's `version`. `version` is read only for
/// `FELIX_IF_VERSION`; any other kind is `FELIX_STATUS_INVALID_ARGUMENT`.
///
/// A write that was refused because the condition did not hold is not an
/// error; see `FelixConditionCallback`. Cancellation as for
/// `felix_client_cache_put`.
///
/// # Safety
///
/// As for `felix_client_cache_put`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_put_if(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    value: *const u8,
    len: usize,
    ttl_ms: u64,
    kind: FelixCacheCondition,
    version: u64,
    cancel: *const FelixCancelToken,
    callback: FelixConditionCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: as above.
        let value = unsafe { copy_value(value, len) }?;
        let condition = condition(kind, version)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Condition>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names
                .inner
                .cache_put_if(t, n, c, &key, value, ttl(ttl_ms), condition)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Condition).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Remove `key` only if its entry has exactly `version`.
///
/// A write; see `felix_client_cache_put_if`.
///
/// # Safety
///
/// As for `felix_client_cache_get`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_delete_if(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    version: u64,
    cancel: *const FelixCancelToken,
    callback: FelixConditionCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Condition>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names.inner.cache_delete_if(t, n, c, &key, version).await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Condition).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Add `delta` to the counter under `key`, creating it at zero, and report
/// the new value.
///
/// A write that is not idempotent: cancelled after it may have been sent it
/// completes with `FELIX_STATUS_OUTCOME_UNKNOWN`, and the add may or may not
/// have counted. Read the counter before repeating it.
///
/// # Safety
///
/// As for `felix_client_cache_get`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_counter_add(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    delta: i64,
    cancel: *const FelixCancelToken,
    callback: FelixCounterCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Counter>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names.inner.counter_add(t, n, c, &key, delta).await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(
                outcome
                    .map(|value| Counter(Some(value)))
                    .map_err(Failure::from),
            );
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Read the counter under `key`. A counter that does not exist is
/// `FELIX_STATUS_OK` with `has_value` false.
///
/// # Safety
///
/// As for `felix_client_cache_get`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_counter_get(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    key: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixCounterCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { key_target(client, tenant_id, ns, cache, key) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Counter>::new(callback, user_data)?;
        let ask = async move {
            let Target { names, key } = target;
            let (t, n, c) = (&names.tenant_id, &names.ns, &names.cache);
            names.inner.counter_get(t, n, c, &key).await
        };
        spawn_read(client, user, done, ask, Counter);
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
