//! Watching a cache for changes, on one key or prefix of one shard or across
//! every shard, and reading the feed one item at a time.

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use felix_client::{
    CacheWatchFilter, CacheWatchItem, ClusterCacheWatch, ShardOffsets, ShardedCacheWatch,
    ShardedCacheWatchItem,
};
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, guard_void, required_str, write_optional};
use crate::cache::{Names, cache_names};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver, cancelled, is_cancelled};
use crate::sharded::{lock, shard_offsets};
use crate::status::{
    FELIX_STATUS_CANCELLED, FELIX_STATUS_CLOSED, FELIX_STATUS_OK, FELIX_STATUS_TIMEOUT, FelixStatus,
};

/// What a single watch matches.
pub type FelixWatchFilter = i32;

/// Changes to exactly one key.
pub const FELIX_WATCH_KEY: FelixWatchFilter = 0;
/// Changes to every key that starts with the filter. A single watch reads one
/// shard, so on a cache with several shards use `felix_client_watch_cache_sharded`.
pub const FELIX_WATCH_PREFIX: FelixWatchFilter = 1;

/// What a watch read delivered.
pub type FelixWatchItemKind = i32;

/// A key took a new value or was deleted.
pub const FELIX_WATCH_CHANGE: FelixWatchItemKind = 1;
/// The watch fell behind and the broker ended it. `resume_from` is where a
/// new watch picks up without a gap; the next read is
/// `FELIX_STATUS_CLOSED`.
pub const FELIX_WATCH_LAGGED: FelixWatchItemKind = 2;
/// The shard moved to another broker. The watch follows it by itself;
/// `resume_from` is set when the old owner knew where it stopped.
pub const FELIX_WATCH_SHARD_MOVED: FelixWatchItemKind = 3;
/// Every shard's retained state has been delivered, and what follows is live
/// changes. Only a sharded retained watch reports it.
pub const FELIX_WATCH_STATE_COMPLETE: FelixWatchItemKind = 4;
/// One shard's feed has ended and delivers no more. Other shards go on.
pub const FELIX_WATCH_SHARD_CLOSED: FelixWatchItemKind = 5;

/// One item of a watch.
///
/// Everything in it is valid only while the callback runs. `key` is
/// `key_len` bytes of UTF-8, not NUL-terminated, and null when `key_len` is
/// zero.
#[repr(C)]
pub struct FelixWatchItem {
    /// One of the `FELIX_WATCH_*` item kinds.
    pub kind: FelixWatchItemKind,
    /// Whether `shard` names a shard. False on a single watch, and for
    /// `FELIX_WATCH_STATE_COMPLETE`.
    pub has_shard: bool,
    pub shard: u32,
    /// The changed key, for `FELIX_WATCH_CHANGE`.
    pub key: *const c_char,
    pub key_len: usize,
    /// False when the change deleted the key; `value` is then null.
    pub has_value: bool,
    /// The new value, `value_len` bytes; null when the length is zero.
    pub value: *const u8,
    pub value_len: usize,
    /// The change's offset in the cache's log. A single watch's offsets are
    /// sparse, because other keys' changes consume them, so a gap is not a
    /// drop; `FELIX_WATCH_LAGGED` is.
    pub offset: u64,
    /// Unix milliseconds the value expires at; zero is never.
    pub expires_at_millis: u64,
    /// Whether `resume_from` holds an offset.
    pub has_resume_from: bool,
    pub resume_from: u64,
}

/// Called once when a watch opens. On `FELIX_STATUS_OK`, `watch` is a new
/// watch the callee owns; otherwise it is null.
pub type FelixCacheWatchCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        watch: *mut FelixCacheWatch,
    ),
>;

/// Called once when `felix_cache_watch_next` finishes. On `FELIX_STATUS_OK`,
/// `item` is valid only while the callback runs; otherwise it is null.
pub type FelixWatchItemCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        item: *const FelixWatchItem,
    ),
>;

/// A live cache watch, single or sharded.
///
/// One read may be pending at a time. It must not be freed while another
/// thread is calling it.
pub struct FelixCacheWatch {
    slot: Arc<Mutex<Slot<Inner>>>,
    /// What the caller has been handed, per shard, for a sharded watch. Set by
    /// the read that hands an item out, so it is never ahead of what the
    /// caller saw and can be read while another read is pending.
    positions: Option<Arc<Mutex<ShardOffsets>>>,
    retained_count: Option<u64>,
    /// Cancelled when this handle is freed, which ends a pending read.
    cancel: CancellationToken,
    shared: Arc<Shared>,
}

enum Inner {
    Single(Box<ClusterCacheWatch>),
    Sharded(Box<ShardedCacheWatch>),
}

enum Slot<T> {
    Idle(T),
    /// A read has the watch.
    Reading,
    /// The feed has ended.
    Closed,
}

impl Drop for FelixCacheWatch {
    fn drop(&mut self) {
        self.cancel.cancel();
        // Its tasks belong to the client's runtime, so it is dropped inside
        // that runtime. A pending read drops it on its own.
        let _guard = self.shared.handle().enter();
        let idle = std::mem::replace(&mut *lock(&self.slot), Slot::Closed);
        drop(idle);
    }
}

/// A read's hold on the watch. Dropping it puts the watch back, so a read
/// that panics or is cancelled leaves it readable.
struct Lease<T> {
    slot: Arc<Mutex<Slot<T>>>,
    /// `None` once the feed has ended.
    watch: Option<T>,
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        let next = self.watch.take().map_or(Slot::Closed, Slot::Idle);
        *lock(&self.slot) = next;
    }
}

impl Inner {
    fn retained_count(&self) -> Option<u64> {
        match self {
            Self::Single(watch) => watch.retained_count(),
            Self::Sharded(watch) => watch.retained_count(),
        }
    }

    /// Cancel-safe, as the watches' own `recv` are. `positions` is set before
    /// the item is returned, so it never runs ahead of what is handed out.
    async fn recv(&mut self, positions: Option<&Mutex<ShardOffsets>>) -> Option<Item> {
        match self {
            Self::Single(watch) => watch.recv().await.map(Item::from_single),
            Self::Sharded(watch) => {
                let item = watch.recv().await?;
                if let Some(positions) = positions {
                    *lock(positions) = watch.resume_offsets();
                }
                Some(Item::from_sharded(item))
            }
        }
    }
}

/// One item of a watch, owned until it is handed to C.
pub(crate) struct Item {
    kind: FelixWatchItemKind,
    shard: Option<u32>,
    key: String,
    value: Option<Bytes>,
    offset: u64,
    expires_at_millis: u64,
    resume_from: Option<u64>,
}

impl Item {
    fn new(kind: FelixWatchItemKind, shard: Option<u32>) -> Self {
        Self {
            kind,
            shard,
            key: String::new(),
            value: None,
            offset: 0,
            expires_at_millis: 0,
            resume_from: None,
        }
    }

    fn change(shard: Option<u32>, change: felix_client::CacheChange) -> Self {
        Self {
            key: change.key,
            value: change.value,
            offset: change.offset,
            expires_at_millis: change.expires_at_millis,
            ..Self::new(FELIX_WATCH_CHANGE, shard)
        }
    }

    fn from_single(item: CacheWatchItem) -> Self {
        match item {
            CacheWatchItem::Change(change) => Self::change(None, change),
            CacheWatchItem::Lagged { resume_from } => Self {
                resume_from: Some(resume_from),
                ..Self::new(FELIX_WATCH_LAGGED, None)
            },
            CacheWatchItem::ShardMoved(moved) => Self {
                resume_from: moved.resume_from,
                ..Self::new(FELIX_WATCH_SHARD_MOVED, None)
            },
        }
    }

    fn from_sharded(item: ShardedCacheWatchItem) -> Self {
        match item {
            ShardedCacheWatchItem::Change { shard, change } => Self::change(Some(shard), change),
            ShardedCacheWatchItem::StateComplete => Self::new(FELIX_WATCH_STATE_COMPLETE, None),
            ShardedCacheWatchItem::Lagged { shard, resume_from } => Self {
                resume_from: Some(resume_from),
                ..Self::new(FELIX_WATCH_LAGGED, Some(shard))
            },
            ShardedCacheWatchItem::ShardMoved { shard, moved } => Self {
                resume_from: moved.resume_from,
                ..Self::new(FELIX_WATCH_SHARD_MOVED, Some(shard))
            },
            ShardedCacheWatchItem::ShardClosed { shard } => {
                Self::new(FELIX_WATCH_SHARD_CLOSED, Some(shard))
            }
        }
    }

    /// The C view. Borrows from `self`.
    fn row(&self) -> FelixWatchItem {
        let (key, key_len) = parts(self.key.as_bytes());
        let (value, value_len) = self.value.as_deref().map_or((ptr::null(), 0), parts);
        FelixWatchItem {
            kind: self.kind,
            has_shard: self.shard.is_some(),
            shard: self.shard.unwrap_or(0),
            key: key.cast(),
            key_len,
            has_value: self.value.is_some(),
            value,
            value_len,
            offset: self.offset,
            expires_at_millis: self.expires_at_millis,
            has_resume_from: self.resume_from.is_some(),
            resume_from: self.resume_from.unwrap_or(0),
        }
    }
}

fn parts(bytes: &[u8]) -> (*const u8, usize) {
    if bytes.is_empty() {
        (ptr::null(), 0)
    } else {
        (bytes.as_ptr(), bytes.len())
    }
}

impl Deliver for Box<FelixCacheWatch> {
    type Callback = FelixCacheWatchCallback;

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
            let watch = value.map_or(ptr::null_mut(), Box::into_raw);
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message, watch) };
        }
    }
}

impl Deliver for Item {
    type Callback = FelixWatchItemCallback;

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
            let row = value.as_ref().map(Item::row);
            let at = row.as_ref().map_or(ptr::null(), ptr::from_ref);
            // SAFETY: the callback's contract is the header's; `row` points
            // into `value`, and both live until it returns.
            unsafe { callback(user_data, status, message, at) };
        }
    }
}

/// How a watch is to be opened, checked.
enum Open {
    Single {
        filter: CacheWatchFilter,
        from_offset: Option<u64>,
        retained: bool,
    },
    Sharded {
        prefix: String,
        resume: Option<ShardOffsets>,
        retained: bool,
    },
}

/// A single watch's filter from its C form.
fn watch_filter(kind: FelixWatchFilter, filter: &str) -> Result<CacheWatchFilter, Failure> {
    match kind {
        FELIX_WATCH_KEY => Ok(CacheWatchFilter::Key(filter.to_string())),
        FELIX_WATCH_PREFIX => Ok(CacheWatchFilter::Prefix(filter.to_string())),
        other => Err(Failure::invalid(format!("unknown filter kind {other}"))),
    }
}

/// Watch one key or prefix on one shard.
///
/// `retained` first delivers the entries already stored (as changes), then
/// `FELIX_WATCH_CHANGE` items for later ones; it cannot be combined with
/// `has_from_offset`. `from_offset` resumes a watch from a change's offset
/// plus one. `kind` is `FELIX_WATCH_KEY` or `FELIX_WATCH_PREFIX`; any other
/// is `FELIX_STATUS_INVALID_ARGUMENT`. A key is routed to its own shard; a
/// prefix watch reads one shard, so use `felix_client_watch_cache_sharded`
/// when the cache has several.
///
/// `callback` runs once, unless this returns something other than
/// `FELIX_STATUS_OK`. A cancelled open never hands out a watch.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is
/// null or a live token, and `callback` is safe to call with `user_data` from
/// any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_watch_cache(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    kind: FelixWatchFilter,
    filter: *const c_char,
    retained: bool,
    has_from_offset: bool,
    from_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixCacheWatchCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, names) = unsafe { cache_names(client, tenant_id, ns, cache) }?;
        // SAFETY: as above.
        let filter = watch_filter(kind, unsafe { required_str(filter, "filter") }?)?;
        if retained && has_from_offset {
            return Err(Failure::invalid(
                "a retained watch starts from the stored state and takes no from_offset",
            ));
        }
        let how = Open::Single {
            filter,
            from_offset: has_from_offset.then_some(from_offset),
            retained,
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        open(client, names, how, user, callback, user_data)
    })
}

/// Watch every shard of a cache for keys that start with `prefix`.
///
/// Fails, and hands out no watch, if any shard cannot be opened, so a watch
/// never quietly covers fewer shards than the cache has. `count` entries of
/// `shards` and `offsets` resume each listed shard at its offset, as
/// `felix_cache_watch_resume_offsets` writes them; zero starts from now. A
/// shard listed twice is `FELIX_STATUS_INVALID_ARGUMENT`. `retained` first
/// delivers each shard's stored entries, then `FELIX_WATCH_STATE_COMPLETE`;
/// it cannot be combined with resume offsets.
///
/// # Safety
///
/// As for `felix_client_watch_cache`, and `shards` and `offsets` are each
/// null or point to `count` readable values (null only when `count` is
/// zero).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_watch_cache_sharded(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    cache: *const c_char,
    prefix: *const c_char,
    retained: bool,
    shards: *const u32,
    offsets: *const u64,
    count: usize,
    cancel: *const FelixCancelToken,
    callback: FelixCacheWatchCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, names) = unsafe { cache_names(client, tenant_id, ns, cache) }?;
        // SAFETY: as above.
        let prefix = unsafe { required_str(prefix, "prefix") }?.to_string();
        if retained && count > 0 {
            return Err(Failure::invalid(
                "a retained watch starts from the stored state and takes no resume offsets",
            ));
        }
        // SAFETY: as above.
        let resume = unsafe { shard_offsets(shards, offsets, count) }?;
        let how = Open::Sharded {
            prefix,
            resume: (count > 0).then_some(resume),
            retained,
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        open(client, names, how, user, callback, user_data)
    })
}

fn open(
    client: &FelixClient,
    names: Names,
    how: Open,
    user: Option<CancellationToken>,
    callback: FelixCacheWatchCallback,
    user_data: *mut c_void,
) -> Result<FelixStatus, Failure> {
    // The new watch keeps the runtime alive, so this task holds the shared
    // state rather than only the client.
    let shared = Arc::clone(&client.shared);
    let handle = client.cancel.clone();
    let done = Completion::<Box<FelixCacheWatch>>::new(callback, user_data)?;
    client.shared.spawn(done, move |done| async move {
        let Names {
            inner: cluster,
            tenant_id,
            ns,
            cache,
        } = names;
        let opened = async {
            match how {
                Open::Single {
                    filter,
                    from_offset: _,
                    retained: true,
                } => cluster
                    .watch_cache_retained(&tenant_id, &ns, &cache, filter)
                    .await
                    .map(|watch| Inner::Single(Box::new(watch))),
                Open::Single {
                    filter,
                    from_offset,
                    retained: false,
                } => cluster
                    .watch_cache(&tenant_id, &ns, &cache, filter, from_offset)
                    .await
                    .map(|watch| Inner::Single(Box::new(watch))),
                Open::Sharded {
                    prefix,
                    resume: _,
                    retained: true,
                } => cluster
                    .watch_cache_sharded_retained(&tenant_id, &ns, &cache, &prefix)
                    .await
                    .map(|watch| Inner::Sharded(Box::new(watch))),
                Open::Sharded {
                    prefix,
                    resume,
                    retained: false,
                } => cluster
                    .watch_cache_sharded(&tenant_id, &ns, &cache, &prefix, resume)
                    .await
                    .map(|watch| Inner::Sharded(Box::new(watch))),
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
            Some(Ok(inner)) if !is_cancelled(&handle, user.as_ref()) => {
                done.ok(Box::new(FelixCacheWatch::new(inner, shared)));
            }
            Some(Err(err)) if !is_cancelled(&handle, user.as_ref()) => {
                done.fail(err.into());
            }
            _ => done.status(FELIX_STATUS_CANCELLED),
        }
    });
    Ok(FELIX_STATUS_OK)
}

impl FelixCacheWatch {
    fn new(inner: Inner, shared: Arc<Shared>) -> Self {
        let positions = match &inner {
            Inner::Single(_) => None,
            Inner::Sharded(watch) => Some(Arc::new(Mutex::new(watch.resume_offsets()))),
        };
        Self {
            retained_count: inner.retained_count(),
            positions,
            slot: Arc::new(Mutex::new(Slot::Idle(inner))),
            cancel: shared.cancel_token(),
            shared,
        }
    }
}

/// Read the next item.
///
/// `callback` runs once: with `FELIX_STATUS_OK` and an item;
/// `FELIX_STATUS_TIMEOUT` when `timeout_ms` passed with nothing (the watch
/// stays usable); `FELIX_STATUS_CLOSED` once the feed has ended, as after
/// `FELIX_WATCH_LAGGED`; or `FELIX_STATUS_CANCELLED`. A cancelled or
/// timed-out read loses nothing; the next read gets the item.
///
/// A negative `timeout_ms` waits indefinitely; zero only takes an item that
/// is already waiting. Only one read may be pending: a second returns
/// `FELIX_STATUS_INVALID_ARGUMENT`. The callback may start the next read.
///
/// # Safety
///
/// `watch` is a live watch, `cancel` is null or a live token, and `callback`
/// is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_cache_watch_next(
    watch: *const FelixCacheWatch,
    timeout_ms: i64,
    cancel: *const FelixCancelToken,
    callback: FelixWatchItemCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live watch, per the contract above.
        let watch = unsafe { watch.as_ref() }.ok_or_else(|| Failure::invalid("watch is null"))?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let wait = u64::try_from(timeout_ms).ok().map(Duration::from_millis);
        let handle = watch.cancel.clone();
        let positions = watch.positions.clone();

        let mut slot = lock(&watch.slot);
        let lease = match std::mem::replace(&mut *slot, Slot::Reading) {
            Slot::Idle(inner) => Some(Lease {
                slot: Arc::clone(&watch.slot),
                watch: Some(inner),
            }),
            Slot::Reading => {
                return Err(Failure::invalid("a read is already pending on this watch"));
            }
            Slot::Closed => {
                *slot = Slot::Closed;
                None
            }
        };
        drop(slot);
        let done = match Completion::<Item>::new(callback, user_data) {
            Ok(done) => done,
            Err(failure) => {
                // Hand the watch back before refusing.
                drop(lease);
                return Err(failure);
            }
        };
        watch.shared.spawn(done, move |done| async move {
            let Some(mut lease) = lease else {
                return done.status(FELIX_STATUS_CLOSED);
            };
            let inner = lease.watch.as_mut().expect("a lease holds the watch");
            let timeout = async {
                match wait {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => std::future::pending().await,
                }
            };
            let next = tokio::select! {
                biased;
                () = cancelled(&handle, user.as_ref()) => Err(FELIX_STATUS_CANCELLED),
                next = inner.recv(positions.as_deref()) => Ok(next),
                () = timeout => Err(FELIX_STATUS_TIMEOUT),
            };
            let outcome = match next {
                Ok(Some(item)) => Ok(item),
                Ok(None) => {
                    lease.watch = None;
                    Err(FELIX_STATUS_CLOSED)
                }
                Err(status) => Err(status),
            };
            // Give the watch back before calling out, so the callback can
            // start the next read.
            drop(lease);
            match outcome {
                Ok(item) => done.ok(item),
                Err(status) => done.status(status),
            }
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// How many entries a retained watch started from, for sizing a cache that is
/// loaded from its watch.
///
/// Writes `*out_has` false for a watch that is not retained. Either out
/// pointer may be null.
///
/// # Safety
///
/// `watch` is a live watch, and `out` and `out_has` are each null or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_cache_watch_retained_count(
    watch: *const FelixCacheWatch,
    out: *mut u64,
    out_has: *mut bool,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live watch, per the contract above.
        let watch = unsafe { watch.as_ref() }.ok_or_else(|| Failure::invalid("watch is null"))?;
        // SAFETY: null or writable, per the contract above.
        unsafe {
            write_optional(out, watch.retained_count.unwrap_or(0));
            write_optional(out_has, watch.retained_count.is_some());
        }
        Ok(FELIX_STATUS_OK)
    })
}

/// Where each shard has reached, for `felix_client_watch_cache_sharded`.
///
/// Only a sharded watch has these; a single watch is
/// `FELIX_STATUS_INVALID_ARGUMENT`. Every shard appears, with the offset to
/// resume from, never ahead of what reads have handed out. `*out_count` is the
/// total number of entries; at most `cap` are written to `shards` and
/// `offsets`, in ascending shard order, so a caller whose buffers were too
/// small calls again with a larger one. It may be called while a read is
/// pending.
///
/// # Safety
///
/// `watch` is a live watch. `shards` and `offsets` are each null or writable
/// for `cap` values (null only when `cap` is zero), and `out_count` is null
/// or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_cache_watch_resume_offsets(
    watch: *const FelixCacheWatch,
    shards: *mut u32,
    offsets: *mut u64,
    cap: usize,
    out_count: *mut usize,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live watch, per the contract above.
        let watch = unsafe { watch.as_ref() }.ok_or_else(|| Failure::invalid("watch is null"))?;
        let positions = watch
            .positions
            .as_ref()
            .ok_or_else(|| Failure::invalid("only a sharded watch has resume offsets"))?;
        if cap > 0 && (shards.is_null() || offsets.is_null()) {
            return Err(Failure::invalid("shards and offsets must not be null"));
        }
        let positions = lock(positions);
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

/// Free a watch, which stops every shard. Null is ignored.
///
/// A pending read completes with `FELIX_STATUS_CANCELLED`, possibly after
/// this returns.
///
/// # Safety
///
/// `watch` is null or a pointer from a watch callback that has not been
/// freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_cache_watch_free(watch: *mut FelixCacheWatch) {
    guard_void(|| {
        if !watch.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in the watch
            // callback and is freed exactly once.
            drop(unsafe { Box::from_raw(watch) });
        }
    });
}

#[cfg(test)]
mod tests;
