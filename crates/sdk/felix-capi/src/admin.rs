//! What a broker reports about itself and the cluster: which questions it
//! answers, one shard's state, the subscriptions it serves, its peers and who
//! owns each shard.
//!
//! The structured answers have many nested fields, so they reach C as one
//! NUL-terminated JSON object or array with the wire's field names, and the
//! caller parses the part it needs. Single numbers and flags come as plain
//! arguments. Each call asks the broker the client is connected to, which is
//! what an operator compares: ask each broker for its own view.

use std::ffi::{CString, c_char, c_void};
use std::ptr;
use std::sync::Arc;

use felix_client::ShardRouting;
use felix_client::{ShardKind, SubscriptionCursor, SubscriptionFilter};
use serde::Serialize;

use crate::boundary::{Failure, guard, optional_str, read_field, read_flag, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::{Completion, Deliver};
use crate::groups::spawn_read;
use crate::status::{FELIX_STATUS_OK, FelixStatus};
use crate::topology::{Count, FelixCountCallback};

/// Whether a shard belongs to a stream or a cache.
pub type FelixShardKind = i32;

/// A stream's shard.
pub const FELIX_KIND_STREAM: FelixShardKind = 0;
/// A cache's shard.
pub const FELIX_KIND_CACHE: FelixShardKind = 1;

/// How a stream maps routing keys onto its shards.
pub type FelixShardRouting = i32;

/// `hash(key) % shards`. What a stream created before routing modes has.
pub const FELIX_ROUTING_MODULO: FelixShardRouting = 0;
/// Jump consistent hash of the same key hash.
pub const FELIX_ROUTING_JUMP_HASH: FelixShardRouting = 1;

/// The broker answers `felix_client_inspect_shard` and
/// `felix_client_list_subscriptions`.
pub const FELIX_SUPPORTS_INSPECT: u32 = 1 << 0;
/// The broker answers `felix_client_topology`.
pub const FELIX_SUPPORTS_TOPOLOGY: u32 = 1 << 1;
/// The broker honours `FelixConnectOptions.ack_on_commit`.
pub const FELIX_SUPPORTS_ACK_ON_COMMIT: u32 = 1 << 2;
/// The broker answers `felix_client_stream_routing`.
pub const FELIX_SUPPORTS_STREAM_SHARDS: u32 = 1 << 3;
/// The broker answers `felix_client_cache_shards`.
pub const FELIX_SUPPORTS_CACHE_SHARDS: u32 = 1 << 4;
/// The broker answers `felix_client_shard_owners`.
pub const FELIX_SUPPORTS_SHARD_OWNERS: u32 = 1 << 5;

/// Called once when `felix_client_inspect_shard`, `felix_client_topology` or
/// `felix_client_shard_owners` finishes. On `FELIX_STATUS_OK`, `json` is a
/// NUL-terminated JSON value with the wire's field names, valid only while
/// the callback runs; null on failure.
pub type FelixJsonCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        json: *const c_char,
    ),
>;

/// Called once when `felix_client_list_subscriptions` finishes. On
/// `FELIX_STATUS_OK`, `json` is the page as a JSON object
/// `{"node_id", "subscriptions", "next_cursor"}`, and `next_cursor` is the
/// cursor of the next page as a JSON string to pass back to
/// `felix_client_list_subscriptions`, or null on the last page. Both strings
/// are valid only while the callback runs; both are null on failure.
pub type FelixSubscriptionsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        json: *const c_char,
        next_cursor: *const c_char,
    ),
>;

/// Called once when `felix_client_supports` finishes. On `FELIX_STATUS_OK`,
/// `flags` has a `FELIX_SUPPORTS_*` bit for each question the broker in use
/// answers; zero on failure.
pub type FelixSupportsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        flags: u32,
    ),
>;

/// Called once when `felix_client_stream_routing` finishes. On
/// `FELIX_STATUS_OK`, the stream has `shards` shards mapped by `routing`, a
/// `FELIX_ROUTING_*` value; both are zero on failure.
pub type FelixRoutingCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        shards: u32,
        routing: FelixShardRouting,
    ),
>;

/// Which subscriptions `felix_client_list_subscriptions` returns. Every field
/// set narrows the list; none set lists them all.
///
/// Set `size` to `sizeof(FelixSubscriptionFilter)`; a field is read only when
/// `size` covers it.
#[repr(C)]
pub struct FelixSubscriptionFilter {
    /// `sizeof(FelixSubscriptionFilter)` as the caller compiled it.
    pub size: usize,
    /// Only this tenant's subscriptions. Null for any.
    pub tenant_id: *const c_char,
    /// Only this namespace. Null for any.
    pub ns: *const c_char,
    /// Only this stream. Null for any.
    pub stream: *const c_char,
    /// Only subscriptions made under this principal (the token's subject).
    /// Null for any.
    pub principal: *const c_char,
    /// Only this shard, when `has_shard` is true.
    pub shard: u32,
    /// Whether `shard` narrows the list.
    pub has_shard: bool,
    /// Only subscriptions whose queue has dropped records.
    pub dropping: bool,
}

/// A JSON answer, ready to hand to C.
pub(crate) struct Json(CString);

impl Json {
    fn of<T: Serialize>(value: &T) -> anyhow::Result<Self> {
        Ok(Self(text(value)?))
    }
}

/// `value` as JSON text. Serialized JSON escapes control characters, so it
/// never holds a NUL.
fn text<T: Serialize>(value: &T) -> anyhow::Result<CString> {
    let json = serde_json::to_string(value)?;
    CString::new(json).map_err(|_| anyhow::anyhow!("the answer held a NUL byte"))
}

impl Deliver for Json {
    type Callback = FelixJsonCallback;

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
            let json = value.as_ref().map_or(ptr::null(), |j| j.0.as_ptr());
            // SAFETY: the callback's contract is the header's; `value` lives
            // until it returns.
            unsafe { callback(user_data, status, message, json) };
        }
    }
}

/// A page of subscriptions and the cursor of the next.
pub(crate) struct Page {
    json: CString,
    next: Option<CString>,
}

impl Deliver for Page {
    type Callback = FelixSubscriptionsCallback;

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
            let json = value.as_ref().map_or(ptr::null(), |p| p.json.as_ptr());
            let next = value
                .as_ref()
                .and_then(|p| p.next.as_ref())
                .map_or(ptr::null(), |n| n.as_ptr());
            // SAFETY: the callback's contract is the header's; `value` lives
            // until it returns.
            unsafe { callback(user_data, status, message, json, next) };
        }
    }
}

pub(crate) struct Supports(u32);

impl Deliver for Supports {
    type Callback = FelixSupportsCallback;

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
            unsafe { callback(user_data, status, message, value.map_or(0, |s| s.0)) };
        }
    }
}

pub(crate) struct Routing(u32, FelixShardRouting);

impl Deliver for Routing {
    type Callback = FelixRoutingCallback;

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
            let (shards, routing) = value.map_or((0, FELIX_ROUTING_MODULO), |r| (r.0, r.1));
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, shards, routing) };
        }
    }
}

fn shard_kind(kind: FelixShardKind) -> Result<ShardKind, Failure> {
    match kind {
        FELIX_KIND_STREAM => Ok(ShardKind::Stream),
        FELIX_KIND_CACHE => Ok(ShardKind::Cache),
        other => Err(Failure::invalid(format!(
            "kind {other} is not a FELIX_KIND_* value"
        ))),
    }
}

fn routing_code(routing: ShardRouting) -> FelixShardRouting {
    match routing {
        ShardRouting::Modulo => FELIX_ROUTING_MODULO,
        ShardRouting::JumpHash => FELIX_ROUTING_JUMP_HASH,
    }
}

/// The `FELIX_SUPPORTS_*` bits for what a connection answers.
fn support_flags(
    inspect: bool,
    topology: bool,
    ack_on_commit: bool,
    stream_shards: bool,
    cache_shards: bool,
    shard_owners: bool,
) -> u32 {
    [
        (inspect, FELIX_SUPPORTS_INSPECT),
        (topology, FELIX_SUPPORTS_TOPOLOGY),
        (ack_on_commit, FELIX_SUPPORTS_ACK_ON_COMMIT),
        (stream_shards, FELIX_SUPPORTS_STREAM_SHARDS),
        (cache_shards, FELIX_SUPPORTS_CACHE_SHARDS),
        (shard_owners, FELIX_SUPPORTS_SHARD_OWNERS),
    ]
    .into_iter()
    .filter(|(on, _)| *on)
    .fold(0, |bits, (_, bit)| bits | bit)
}

/// The filter a caller's struct names; the default when it passed none.
///
/// # Safety
///
/// `filter` is null or readable up to its `size`, and its strings are null or
/// NUL-terminated.
unsafe fn read_filter(
    filter: *const FelixSubscriptionFilter,
) -> Result<SubscriptionFilter, Failure> {
    if filter.is_null() {
        return Ok(SubscriptionFilter::default());
    }
    // SAFETY: readable up to its own `size`, which starts with `size`.
    let size = unsafe { ptr::addr_of!((*filter).size).read() };
    if size < size_of::<usize>() {
        return Err(Failure::invalid(
            "filter.size is smaller than the size field itself",
        ));
    }
    let base = filter.cast::<u8>();
    let text = |offset: usize, name: &str| -> Result<Option<String>, Failure> {
        // SAFETY: each is inside the `size` bytes the caller promised
        // readable, and a string is null or NUL-terminated.
        let pointer = unsafe { read_field::<*const c_char>(base, size, offset) };
        match pointer {
            None => Ok(None),
            Some(p) => Ok(unsafe { optional_str(p, name) }?.map(str::to_string)),
        }
    };
    let shard = std::mem::offset_of!(FelixSubscriptionFilter, shard);
    let has_shard = std::mem::offset_of!(FelixSubscriptionFilter, has_shard);
    // SAFETY: as above.
    let (shard, has_shard, dropping) = unsafe {
        (
            read_field::<u32>(base, size, shard),
            read_flag(base, size, has_shard),
            read_flag(
                base,
                size,
                std::mem::offset_of!(FelixSubscriptionFilter, dropping),
            ),
        )
    };
    Ok(SubscriptionFilter {
        tenant_id: text(
            std::mem::offset_of!(FelixSubscriptionFilter, tenant_id),
            "tenant_id",
        )?,
        namespace: text(std::mem::offset_of!(FelixSubscriptionFilter, ns), "ns")?,
        stream: text(
            std::mem::offset_of!(FelixSubscriptionFilter, stream),
            "stream",
        )?,
        shard: shard.filter(|_| has_shard),
        principal: text(
            std::mem::offset_of!(FelixSubscriptionFilter, principal),
            "principal",
        )?,
        dropping,
    })
}

/// The cursor a previous page returned, parsed back from its JSON.
fn parse_cursor(json: &str) -> Result<SubscriptionCursor, Failure> {
    serde_json::from_str(json).map_err(|err| {
        Failure::invalid(format!(
            "cursor must be the next_cursor string list_subscriptions returned: {err}"
        ))
    })
}

/// Ask which questions the broker in use answers. Older brokers answer fewer,
/// and a call this reports unsupported fails with `FELIX_STATUS_UNSUPPORTED`
/// or an error instead of being sent.
///
/// # Safety
///
/// `client` is a live client, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_supports(
    client: *const FelixClient,
    cancel: *const FelixCancelToken,
    callback: FelixSupportsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Supports>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            let broker = cluster.client().await;
            Ok(support_flags(
                broker.supports_inspect(),
                broker.supports_topology(),
                broker.supports_ack_on_commit(),
                broker.supports_stream_shards(),
                broker.supports_cache_shards(),
                broker.supports_shard_owners(),
            ))
        };
        spawn_read(client, user, done, ask, Supports);
        Ok(FELIX_STATUS_OK)
    })
}

/// The broker's own view of one shard: its phase and generation, the fence,
/// its lease and tail, and where it leads, every replica's position. `kind`
/// is a `FELIX_KIND_*` value; `name` is the stream or cache.
///
/// Needs `node.view:cluster:*`, and the broker must advertise inspect (see
/// `felix_client_supports`). It is not forwarded: ask each broker for its
/// own.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_inspect_shard(
    client: *const FelixClient,
    kind: FelixShardKind,
    tenant_id: *const c_char,
    ns: *const c_char,
    name: *const c_char,
    shard: u32,
    cancel: *const FelixCancelToken,
    callback: FelixJsonCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        let kind = shard_kind(kind)?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, name) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(name, "name")?.to_string(),
            )
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Json>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            let view = cluster
                .client()
                .await
                .inspect_shard(kind, &tenant_id, &ns, &name, shard)
                .await?;
            Json::of(&view)
        };
        spawn_read(client, user, done, ask, |json| json);
        Ok(FELIX_STATUS_OK)
    })
}

/// One page of the subscriptions the broker serves: each one's queue depth
/// and capacity, overflow policy, records dropped, position against the
/// shard's tail, connection and principal.
///
/// `filter` is null for every subscription, or narrows the list. `limit` is
/// the most to return; zero is the broker's default, 100, and it caps a page
/// at 1000. `cursor` is null for the first page, or the `next_cursor` string
/// the previous page's callback received. Needs `node.view:cluster:*`, and
/// the broker must advertise inspect. Not forwarded: ask each broker for its
/// own.
///
/// # Safety
///
/// `client` is a live client, `filter` is null or readable up to its `size`,
/// the strings are null or NUL-terminated, `cancel` is null or a live token,
/// and `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_list_subscriptions(
    client: *const FelixClient,
    filter: *const FelixSubscriptionFilter,
    limit: u32,
    cursor: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixSubscriptionsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or readable up to its size, per the contract above.
        let filter = unsafe { read_filter(filter) }?;
        // SAFETY: null or a NUL-terminated string.
        let cursor = unsafe { optional_str(cursor, "cursor") }?
            .map(parse_cursor)
            .transpose()?;
        let limit = (limit != 0).then_some(limit);
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Page>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            let page = cluster
                .client()
                .await
                .list_subscriptions(filter, limit, cursor)
                .await?;
            Ok(Page {
                json: text(&serde_json::json!({
                    "node_id": page.node_id,
                    "subscriptions": page.subscriptions,
                    "next_cursor": page.next_cursor,
                }))?,
                next: page.next_cursor.as_ref().map(text).transpose()?,
            })
        };
        spawn_read(client, user, done, ask, |page| page);
        Ok(FELIX_STATUS_OK)
    })
}

/// The brokers a client may connect to, as a JSON array of endpoints with the
/// wire's field names. Needs a broker that advertises topology (see
/// `felix_client_supports`). `felix_client_refresh_topology` is what adds them
/// to the endpoints the client tries.
///
/// # Safety
///
/// `client` is a live client, `cancel` is null or a live token, and
/// `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_topology(
    client: *const FelixClient,
    cancel: *const FelixCancelToken,
    callback: FelixJsonCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Json>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            let brokers = cluster.client().await.topology().await?;
            Json::of(&brokers)
        };
        spawn_read(client, user, done, ask, |json| json);
        Ok(FELIX_STATUS_OK)
    })
}

/// How many shards a stream has and how it maps routing keys onto them.
/// Needs a broker that reports stream shards.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_stream_routing(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixRoutingCallback,
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
        let done = Completion::<Routing>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            cluster
                .client()
                .await
                .stream_routing(&tenant_id, &ns, &stream)
                .await
        };
        spawn_read(client, user, done, ask, |(shards, routing)| {
            Routing(shards, routing_code(routing))
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Which broker serves each shard of a stream or cache, as a JSON array of
/// owners with the wire's field names. A shard nobody can serve has no node
/// and says why. The answer is a snapshot and can be stale; a request sent on
/// it is still routed correctly. Needs a broker that reports shard owners.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_shard_owners(
    client: *const FelixClient,
    kind: FelixShardKind,
    tenant_id: *const c_char,
    ns: *const c_char,
    name: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixJsonCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        let kind = shard_kind(kind)?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, name) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(name, "name")?.to_string(),
            )
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Json>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            let owners = cluster
                .client()
                .await
                .shard_owners(&tenant_id, &ns, &name, kind)
                .await?;
            Json::of(&owners)
        };
        spawn_read(client, user, done, ask, |json| json);
        Ok(FELIX_STATUS_OK)
    })
}

/// How many shards a cache has. Needs a broker that reports cache shards.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_cache_shards(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixCountCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: each is null or a NUL-terminated string.
        let (tenant_id, ns, cache) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(cache, "cache")?.to_string(),
            )
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Count>::new(callback, user_data)?;
        let cluster = Arc::clone(client.cluster());
        let ask = async move {
            cluster
                .client()
                .await
                .cache_shards(&tenant_id, &ns, &cache)
                .await
        };
        spawn_read(client, user, done, ask, |shards| Count(shards as usize));
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
