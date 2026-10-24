//! Consumer groups: polling a shard's records, settling them, and managing the
//! group itself.
//!
//! Every call names one shard and goes to the broker that leads it, following
//! a move. Records are passed back by value (offset and attempt count), so a
//! caller keeps a plain struct and nothing needs freeing.
//!
//! Reads report `FELIX_STATUS_CANCELLED` when cancelled. Writes start through
//! [`spawn_publish`], which reports `FELIX_STATUS_OUTCOME_UNKNOWN` once a
//! write may have been sent.

use std::ffi::{c_char, c_void};
use std::future::Future;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_client::{
    ClusterClient, GroupInfo, GroupMember, GroupPollOptions, GroupPosition, GroupRecord,
};
use tokio_util::sync::CancellationToken;

use crate::boundary::{Failure, guard, optional_str, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::{FelixClient, Shared};
use crate::completion::{Completion, Deliver, cancelled, is_cancelled};
use crate::publish::spawn_publish;
use crate::status::{FELIX_STATUS_CANCELLED, FELIX_STATUS_OK, FelixStatus};
use crate::subscription::{FelixStartKind, start_position};

/// The longest consumer name the broker accepts, in bytes.
const MAX_CONSUMER_LEN: usize = 128;

/// Called once when a group call that returns nothing finishes.
pub type FelixDoneCallback = Option<
    unsafe extern "C" fn(user_data: *mut c_void, status: FelixStatus, message: *const c_char),
>;

/// One record handed to a consumer by `felix_client_group_poll`.
#[repr(C)]
pub struct FelixGroupRecord {
    /// The offset to pass to ack, nack, extend and the other settles.
    pub offset: u64,
    /// How many times the record has been handed out, this one included. `1`
    /// is a first attempt. `0` means the broker did not report it.
    pub attempts: u32,
    /// How many offsets directly below this one the broker settled without
    /// delivering. Zero from a broker that does not report it.
    pub skipped_before: u64,
    /// The record's payload, `payload_len` bytes. Valid only while the
    /// callback runs; may be null when the length is zero.
    pub payload: *const u8,
    pub payload_len: usize,
    /// The principal that published the record, `publisher_len` bytes of
    /// UTF-8 that are not NUL terminated; valid only while the callback runs.
    /// Null unless the client was connected with
    /// `FelixConnectOptions.publishers` and the broker recorded one.
    pub publisher: *const c_char,
    pub publisher_len: usize,
    /// Whether `timestamp_micros` holds a time. False unless the client was
    /// connected with `FelixConnectOptions.timestamps` and the broker
    /// supplied one.
    pub has_timestamp: bool,
    /// When the broker appended the record, in microseconds since the Unix
    /// epoch, by the clock of the broker that led the shard.
    pub timestamp_micros: u64,
}

/// Called once when `felix_client_group_poll` finishes.
///
/// `records` holds `count` entries and is valid, with every payload in it,
/// only while the callback runs; copy what you keep. An empty poll is
/// `FELIX_STATUS_OK` with a count of zero. `records` is null on failure.
pub type FelixGroupPollCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        records: *const FelixGroupRecord,
        count: usize,
    ),
>;

/// Called once when `felix_client_group_extend` finishes. On
/// `FELIX_STATUS_OK`, `visible_ms` is how long the claim now stands, from the
/// broker's clock; zero otherwise.
pub type FelixExtendCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        visible_ms: u64,
    ),
>;

/// Called once when `felix_client_group_dead_letters` finishes. `offsets`
/// holds `count` entries and is valid only while the callback runs; it is
/// null on failure or when there are none.
pub type FelixOffsetsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        offsets: *const u64,
        count: usize,
    ),
>;

/// Called once when `felix_client_group_seek` or `felix_client_group_create`
/// finishes. On `FELIX_STATUS_OK`, `offset` is where the group resumes from
/// and `moved` is false when `felix_client_group_create` found the group
/// already there and left it alone.
pub type FelixGroupPositionCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        offset: u64,
        moved: bool,
    ),
>;

/// Where a group stands on one shard.
#[repr(C)]
pub struct FelixGroupInfo {
    /// Whether `committed` holds an offset. False for a group with no cursor
    /// on the shard, which starts at the beginning of the log when polled.
    pub has_committed: bool,
    /// Everything below this is finished.
    pub committed: u64,
    /// The shard's committed tail.
    pub tail: u64,
    /// Records handed out and not yet settled. Leader memory: it restarts
    /// from zero when the leader changes.
    pub in_flight: u64,
    /// Records owed again after a nack or a lapsed claim. Leader memory.
    pub owed: u64,
    /// Records the group gave up on.
    pub dead_letters: u64,
}

/// Called once when `felix_client_group_describe` finishes. `info` is valid
/// only while the callback runs, and null on failure.
pub type FelixGroupInfoCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        info: *const FelixGroupInfo,
    ),
>;

/// Called once when `felix_client_group_delete` finishes. On
/// `FELIX_STATUS_OK`, `existed` says whether the group was there to delete.
pub type FelixDeletedCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        existed: bool,
    ),
>;

/// Called once when `felix_client_stream_shards` finishes. On
/// `FELIX_STATUS_OK`, `shards` is the stream's shard count, and zero means
/// the broker does not know the stream.
pub type FelixShardsCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        shards: u32,
    ),
>;

/// A call that returns nothing.
pub(crate) struct Done;

impl Deliver for Done {
    type Callback = FelixDoneCallback;

    fn is_null(callback: Self::Callback) -> bool {
        callback.is_none()
    }

    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        _value: Option<Self>,
    ) {
        if let Some(callback) = callback {
            // SAFETY: the callback's contract is the header's; the arguments
            // match it.
            unsafe { callback(user_data, status, message) };
        }
    }
}

pub(crate) struct Polled(Vec<GroupRecord>);

impl Deliver for Polled {
    type Callback = FelixGroupPollCallback;

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
        let Some(Polled(records)) = value else {
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, ptr::null(), 0) };
            return;
        };
        let rows: Vec<FelixGroupRecord> = records.iter().map(record_row).collect();
        // SAFETY: as above; `rows` points into `records`, which live until
        // the callback returns.
        unsafe { callback(user_data, status, message, rows.as_ptr(), rows.len()) };
    }
}

pub(crate) fn record_row(record: &GroupRecord) -> FelixGroupRecord {
    FelixGroupRecord {
        offset: record.offset,
        attempts: record.attempts,
        skipped_before: record.skipped_before,
        payload: if record.payload.is_empty() {
            ptr::null()
        } else {
            record.payload.as_ptr()
        },
        payload_len: record.payload.len(),
        publisher: record
            .publisher
            .as_deref()
            .map_or(ptr::null(), |p| p.as_ptr().cast()),
        publisher_len: record.publisher.as_deref().map_or(0, str::len),
        has_timestamp: record.timestamp_micros.is_some(),
        timestamp_micros: record.timestamp_micros.unwrap_or(0),
    }
}

pub(crate) struct Extended(Duration);

impl Deliver for Extended {
    type Callback = FelixExtendCallback;

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
            let visible = value.map_or(0, |extended| millis(extended.0));
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, visible) };
        }
    }
}

pub(crate) struct DeadLetters(Vec<u64>);

impl Deliver for DeadLetters {
    type Callback = FelixOffsetsCallback;

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
            let offsets = value.map(|letters| letters.0).unwrap_or_default();
            let at = if offsets.is_empty() {
                ptr::null()
            } else {
                offsets.as_ptr()
            };
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, at, offsets.len()) };
        }
    }
}

pub(crate) struct Positioned(GroupPosition);

impl Deliver for Positioned {
    type Callback = FelixGroupPositionCallback;

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
            let (offset, moved) = value.map_or((0, false), |p| (p.0.offset, p.0.moved));
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, offset, moved) };
        }
    }
}

pub(crate) struct Described(GroupInfo);

impl Described {
    fn row(&self) -> FelixGroupInfo {
        info_row(&self.0)
    }
}

pub(crate) fn info_row(info: &GroupInfo) -> FelixGroupInfo {
    FelixGroupInfo {
        has_committed: info.committed.is_some(),
        committed: info.committed.unwrap_or(0),
        tail: info.tail,
        in_flight: info.in_flight,
        owed: info.owed,
        dead_letters: info.dead_letters,
    }
}

impl Deliver for Described {
    type Callback = FelixGroupInfoCallback;

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
            let row = value.map(|described| described.row());
            let info = row.as_ref().map_or(ptr::null(), ptr::from_ref);
            // SAFETY: the callback's contract is the header's; `row` lives
            // until it returns.
            unsafe { callback(user_data, status, message, info) };
        }
    }
}

pub(crate) struct Deleted(pub(crate) bool);

impl Deliver for Deleted {
    type Callback = FelixDeletedCallback;

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
            let existed = value.is_some_and(|deleted| deleted.0);
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, existed) };
        }
    }
}

pub(crate) struct ShardCount(u32);

impl Deliver for ShardCount {
    type Callback = FelixShardsCallback;

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
            let shards = value.map_or(0, |count| count.0);
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, shards) };
        }
    }
}

/// Take up to `max_records` records of a group on one shard.
///
/// `wait_ms` is how long the broker may hold the poll open for work; the
/// broker caps it. `visibility_ms` is how long this poll's claims stand
/// before the records are owed again; zero is the broker's own timeout, and a
/// broker that cannot honour another value refuses it. `consumer` names the
/// polling member (1 to 128 bytes, stable across restarts) or is null for one
/// that does not; `reclaim` takes back what that member holds from older
/// connections and needs a `consumer`. `max_records` must be at least one.
///
/// A record neither settled nor handed back is redelivered after its claim
/// lapses. A poll cancelled while it waits completes with
/// `FELIX_STATUS_CANCELLED`; anything it claimed in the meantime comes back
/// after the visibility timeout.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated (only
/// `consumer` may be null), `cancel` is null or a live token, and `callback`
/// is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_poll(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    max_records: u32,
    wait_ms: u64,
    visibility_ms: u64,
    consumer: *const c_char,
    reclaim: bool,
    cancel: *const FelixCancelToken,
    callback: FelixGroupPollCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: as above.
        let consumer = unsafe { optional_str(consumer, "consumer") }?;
        let options = poll_options(consumer, reclaim, wait_ms, visibility_ms)?;
        if max_records == 0 {
            return Err(Failure::invalid("max_records is zero"));
        }
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Polled>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            let group = &target.group;
            target
                .inner
                .group_poll_with(t, n, s, shard, group, max_records, &options)
                .await
        };
        spawn_read(client, user, done, ask, Polled);
        Ok(FELIX_STATUS_OK)
    })
}

/// Finish a record: everything below the group's cursor stays finished.
///
/// A settle is a write. Cancelled after it may have been sent it completes
/// with `FELIX_STATUS_OUTCOME_UNKNOWN`; settling a record again is refused or
/// harmless, so the caller can look at the group and retry.
///
/// # Safety
///
/// As for `felix_client_group_poll`, without the optional `consumer`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_ack(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        settle(
            Settle::Ack,
            (client, tenant_id, ns, stream, group),
            shard,
            offset,
            (cancel, callback, user_data),
        )
    }
}

/// Hand a record back for another attempt, after `delay_ms` (zero is at
/// once). `attempts` is the record's count from the poll: the nack names that
/// delivery, so it is refused once the claim has lapsed or the record was
/// handed out again. A broker without claim control takes only a zero delay.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_ack`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_nack(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    offset: u64,
    attempts: u32,
    delay_ms: u64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    let settle_as = Settle::Nack {
        attempts,
        delay: Duration::from_millis(delay_ms),
    };
    // SAFETY: the caller's contract, passed through.
    unsafe {
        settle(
            settle_as,
            (client, tenant_id, ns, stream, group),
            shard,
            offset,
            (cancel, callback, user_data),
        )
    }
}

/// Give up on a record: list it as a dead letter of the group and finish it.
/// `felix_client_group_redrive` puts it back. Refused for a record already
/// finished.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_ack`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_dead_letter(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        settle(
            Settle::DeadLetter,
            (client, tenant_id, ns, stream, group),
            shard,
            offset,
            (cancel, callback, user_data),
        )
    }
}

/// Drop a dead letter from the group's list without delivering it again.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_ack`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_discard(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        settle(
            Settle::Discard,
            (client, tenant_id, ns, stream, group),
            shard,
            offset,
            (cancel, callback, user_data),
        )
    }
}

/// Put a dead letter back, to be delivered again by a later poll.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_ack`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_redrive(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixDoneCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        settle(
            Settle::Redrive,
            (client, tenant_id, ns, stream, group),
            shard,
            offset,
            (cancel, callback, user_data),
        )
    }
}

/// Lengthen a record's claim by `extend_ms` (at least one), for a consumer
/// that needs longer than the visibility timeout. `attempts` is the record's
/// count from the poll; the extend is refused once the claim has lapsed or
/// the record was handed out again. The callback reports how long the claim
/// now stands.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_ack`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_extend(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    offset: u64,
    attempts: u32,
    extend_ms: u64,
    cancel: *const FelixCancelToken,
    callback: FelixExtendCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        if extend_ms == 0 {
            return Err(Failure::invalid("extend_ms is zero"));
        }
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Extended>::new(callback, user_data)?;
        let record = claim(offset, attempts);
        let extend = Duration::from_millis(extend_ms);
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            let inner = &target.inner;
            inner
                .group_extend(t, n, s, shard, &target.group, &record, extend)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Extended).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// The offsets of the group's dead letters on one shard, oldest first as the
/// broker lists them. A read: cancelled, it completes with
/// `FELIX_STATUS_CANCELLED`.
///
/// # Safety
///
/// As for `felix_client_group_ack`, without `offset`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_dead_letters(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixOffsetsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<DeadLetters>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .group_dead_letters(t, n, s, shard, &target.group)
                .await
        };
        spawn_read(client, user, done, ask, DeadLetters);
        Ok(FELIX_STATUS_OK)
    })
}

/// Move a group's cursor on one shard: to the oldest retained record, past
/// everything now there, or to `start_offset`, as `start` says (the
/// `FELIX_START_*` values). The callback reports where the group resumes.
///
/// A write; see `felix_client_group_ack` for cancellation. Safe to repeat.
///
/// # Safety
///
/// As for `felix_client_group_ack`, without `offset`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_seek(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixGroupPositionCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        position(
            false,
            (client, tenant_id, ns, stream, group),
            shard,
            (start, start_offset),
            (cancel, callback, user_data),
        )
    }
}

/// Create a group on one shard at `start`, leaving it alone when it is
/// already there: the callback's `moved` is then false and `offset` is where
/// it stands.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_seek`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_create(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    start: FelixStartKind,
    start_offset: u64,
    cancel: *const FelixCancelToken,
    callback: FelixGroupPositionCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    // SAFETY: the caller's contract, passed through.
    unsafe {
        position(
            true,
            (client, tenant_id, ns, stream, group),
            shard,
            (start, start_offset),
            (cancel, callback, user_data),
        )
    }
}

/// Where a group stands on one shard. A read: cancelled, it completes with
/// `FELIX_STATUS_CANCELLED`.
///
/// # Safety
///
/// As for `felix_client_group_ack`, without `offset`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_describe(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixGroupInfoCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Described>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .group_describe(t, n, s, shard, &target.group)
                .await
        };
        spawn_read(client, user, done, ask, Described);
        Ok(FELIX_STATUS_OK)
    })
}

/// Delete a group on one shard, with its cursor and dead letters. The
/// callback says whether it existed.
///
/// A write; see `felix_client_group_ack` for cancellation.
///
/// # Safety
///
/// As for `felix_client_group_ack`, without `offset`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_group_delete(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    group: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixDeletedCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Deleted>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .group_delete(t, n, s, shard, &target.group)
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Deleted).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// How many shards a stream was placed with, so a caller can run one group
/// per shard. Zero means the broker does not know the stream. A read:
/// cancelled, it completes with `FELIX_STATUS_CANCELLED`.
///
/// # Safety
///
/// `client` is a live client, the strings are null or NUL-terminated,
/// `cancel` is null or a live token, and `callback` is safe to call with
/// `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_stream_shards(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    cancel: *const FelixCancelToken,
    callback: FelixShardsCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { stream_target(client, tenant_id, ns, stream) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<ShardCount>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target.inner.stream_shards(t, n, s).await
        };
        spawn_read(client, user, done, ask, ShardCount);
        Ok(FELIX_STATUS_OK)
    })
}

/// The client and group a call goes to, owned so the task can hold them.
struct Target {
    inner: Arc<ClusterClient>,
    tenant_id: String,
    ns: String,
    stream: String,
    /// Empty for a call that names none.
    group: String,
}

/// # Safety
///
/// `client` is null or live, and each string null or NUL-terminated.
unsafe fn group_target<'a>(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    group: *const c_char,
) -> Result<(&'a FelixClient, Target), Failure> {
    // SAFETY: the caller's contract, passed through.
    let (client, mut target) = unsafe { stream_target(client, tenant_id, ns, stream) }?;
    // SAFETY: as above.
    target.group = unsafe { required_str(group, "group") }?.to_string();
    Ok((client, target))
}

/// # Safety
///
/// `client` is null or live, and each string null or NUL-terminated.
unsafe fn stream_target<'a>(
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
    Ok((
        client,
        Target {
            inner: Arc::clone(client.shared.client()),
            tenant_id,
            ns,
            stream,
            group: String::new(),
        },
    ))
}

/// A settle that names a record.
enum Settle {
    Ack,
    Nack { attempts: u32, delay: Duration },
    DeadLetter,
    Discard,
    Redrive,
}

/// The pointers every group call starts with.
type Names = (
    *const FelixClient,
    *const c_char,
    *const c_char,
    *const c_char,
    *const c_char,
);

/// # Safety
///
/// As for `felix_client_group_ack`.
unsafe fn settle(
    how: Settle,
    names: Names,
    shard: u32,
    offset: u64,
    (cancel, callback, user_data): (*const FelixCancelToken, FelixDoneCallback, *mut c_void),
) -> FelixStatus {
    guard(|| {
        let (client, tenant_id, ns, stream, group) = names;
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Done>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            let (inner, group) = (&target.inner, &target.group);
            match how {
                Settle::Ack => inner.group_ack(t, n, s, shard, group, offset).await,
                Settle::Nack { attempts, delay } => {
                    let record = claim(offset, attempts);
                    inner
                        .group_nack_after(t, n, s, shard, group, &record, delay)
                        .await
                }
                Settle::DeadLetter => inner.group_dead_letter(t, n, s, shard, group, offset).await,
                Settle::Discard => inner.group_discard(t, n, s, shard, group, offset).await,
                Settle::Redrive => inner.group_redrive(t, n, s, shard, group, offset).await,
            }
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(|()| Done).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// # Safety
///
/// As for `felix_client_group_seek`.
unsafe fn position(
    create: bool,
    names: Names,
    shard: u32,
    (start, start_offset): (FelixStartKind, u64),
    (cancel, callback, user_data): (
        *const FelixCancelToken,
        FelixGroupPositionCallback,
        *mut c_void,
    ),
) -> FelixStatus {
    guard(|| {
        let (client, tenant_id, ns, stream, group) = names;
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { group_target(client, tenant_id, ns, stream, group) }?;
        let start = start_position(start, start_offset)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Positioned>::new(callback, user_data)?;
        let ask = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            let (inner, group) = (&target.inner, &target.group);
            if create {
                inner.group_create(t, n, s, shard, group, start).await
            } else {
                inner.group_seek(t, n, s, shard, group, start).await
            }
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Positioned).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Run a read on the client's runtime and report its value through `wrap`.
/// A read leaves nothing behind when cancelled, so any cancel, before or
/// during the call, reports `FELIX_STATUS_CANCELLED`.
pub(crate) fn spawn_read<V, T, F>(
    client: &FelixClient,
    user: Option<CancellationToken>,
    done: Completion<V>,
    ask: F,
    wrap: impl FnOnce(T) -> V + Send + 'static,
) where
    V: Deliver,
    T: Send + 'static,
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
{
    spawn_read_on(&client.shared, client.cancel.clone(), user, done, ask, wrap);
}

/// [`spawn_read`] for a handle other than the client, which brings its own
/// cancel token.
pub(crate) fn spawn_read_on<V, T, F>(
    shared: &Shared,
    handle: CancellationToken,
    user: Option<CancellationToken>,
    done: Completion<V>,
    ask: F,
    wrap: impl FnOnce(T) -> V + Send + 'static,
) where
    V: Deliver,
    T: Send + 'static,
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
{
    shared.spawn(done, move |done| async move {
        let outcome = tokio::select! {
            biased;
            () = cancelled(&handle, user.as_ref()) => None,
            outcome = ask => Some(outcome),
        };
        match outcome {
            Some(Ok(value)) if !is_cancelled(&handle, user.as_ref()) => done.ok(wrap(value)),
            Some(Err(err)) if !is_cancelled(&handle, user.as_ref()) => done.fail(err.into()),
            _ => done.status(FELIX_STATUS_CANCELLED),
        }
    });
}

/// The options for a poll, checked: `reclaim` needs a named consumer.
fn poll_options(
    consumer: Option<&str>,
    reclaim: bool,
    wait_ms: u64,
    visibility_ms: u64,
) -> Result<GroupPollOptions, Failure> {
    let member = match consumer {
        Some(consumer) => {
            if consumer.is_empty() || consumer.len() > MAX_CONSUMER_LEN {
                return Err(Failure::invalid(format!(
                    "consumer must be 1 to {MAX_CONSUMER_LEN} bytes"
                )));
            }
            Some(GroupMember {
                consumer: consumer.to_string(),
                reclaim,
            })
        }
        None if reclaim => return Err(Failure::invalid("reclaim needs a consumer")),
        None => None,
    };
    Ok(GroupPollOptions {
        member,
        wait: Duration::from_millis(wait_ms),
        visibility: (visibility_ms != 0).then(|| Duration::from_millis(visibility_ms)),
    })
}

/// The record a settle names: the broker checks the offset and the attempt
/// count, so nothing else needs to travel.
fn claim(offset: u64, attempts: u32) -> GroupRecord {
    GroupRecord {
        offset,
        payload: Bytes::new(),
        attempts,
        skipped_before: 0,
        publisher: None,
        timestamp_micros: None,
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

mod sharded;
mod stream;

pub use sharded::{
    FelixShardedGroup, FelixShardedGroupCallback, FelixShardedGroupPollCallback,
    FelixShardedGroupRecord, felix_client_group_sharded, felix_sharded_group_free,
    felix_sharded_group_poll, felix_sharded_group_shards,
};
pub use stream::{
    FelixGroupInfosCallback, FelixGroupPosition, FelixGroupPositionsCallback,
    felix_client_group_create_stream, felix_client_group_delete_stream,
    felix_client_group_describe_stream, felix_client_group_seek_stream,
};

#[cfg(test)]
mod tests;
