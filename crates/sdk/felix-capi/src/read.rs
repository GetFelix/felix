//! Reading a page of a stream shard by offset range, without subscribing.

use std::ffi::{c_char, c_void};
use std::ptr;

use felix_client::StreamPage;

use crate::boundary::{Failure, guard, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::{Completion, Deliver};
use crate::groups::spawn_read;
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// One committed record from `felix_client_read`.
#[repr(C)]
pub struct FelixStreamRecord {
    pub offset: u64,
    /// The record's bytes, `payload_len` of them. Valid only while the
    /// callback runs; may be null when the length is zero.
    pub payload: *const u8,
    pub payload_len: usize,
    /// The principal that published the record, `publisher_len` bytes of
    /// UTF-8 that are not NUL terminated, or null when the broker recorded
    /// none. Valid only while the callback runs.
    pub publisher: *const c_char,
    pub publisher_len: usize,
    /// When the broker appended the record, in microseconds since the Unix
    /// epoch.
    pub timestamp_micros: u64,
}

/// Called once when `felix_client_read` finishes.
///
/// `records` holds `count` entries and is valid, with every payload in it,
/// only while the callback runs; copy what you keep. Read the next page from
/// `next_offset`: offsets that hold no record are passed over, so it is not
/// always the last offset plus one. A page that reaches what is committed
/// comes back short or empty rather than waiting. `records` is null and
/// `next_offset` zero on failure.
pub type FelixPageCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        records: *const FelixStreamRecord,
        count: usize,
        next_offset: u64,
    ),
>;

pub(crate) struct Page(StreamPage);

impl Deliver for Page {
    type Callback = FelixPageCallback;

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
        let Some(Page(page)) = value else {
            // SAFETY: the callback's contract is the header's.
            unsafe { callback(user_data, status, message, ptr::null(), 0, 0) };
            return;
        };
        let rows: Vec<FelixStreamRecord> = page
            .records
            .iter()
            .map(|record| FelixStreamRecord {
                offset: record.offset,
                payload: if record.payload.is_empty() {
                    ptr::null()
                } else {
                    record.payload.as_ptr()
                },
                payload_len: record.payload.len(),
                publisher: record
                    .publisher
                    .as_deref()
                    .filter(|p| !p.is_empty())
                    .map_or(ptr::null(), |p| p.as_ptr().cast()),
                publisher_len: record.publisher.as_deref().map_or(0, str::len),
                timestamp_micros: record.timestamp_micros,
            })
            .collect();
        let at = if rows.is_empty() {
            ptr::null()
        } else {
            rows.as_ptr()
        };
        // SAFETY: the callback's contract is the header's; `page` and `rows`
        // live until it returns.
        unsafe { callback(user_data, status, message, at, rows.len(), page.next_offset) };
    }
}

/// Read one page of a durable stream shard's committed records, from `from`
/// and stopping before `end` when `has_end` is set, without subscribing.
///
/// `max_records` of zero takes the broker's cap, and the broker also caps a
/// page's size. A `from` below what retention kept, or past the tail,
/// completes with `FELIX_STATUS_CURSOR` and details in
/// `felix_last_cursor_error`. Needs the stream's subscribe permission, and
/// only the shard's leader answers; the client follows it.
///
/// A read leaves nothing behind when cancelled.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `cancel` is null
/// or a live token, and `callback` is safe to call with `user_data` from any
/// thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_read(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    from: u64,
    has_end: bool,
    end: u64,
    max_records: u32,
    cancel: *const FelixCancelToken,
    callback: FelixPageCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: null or a live client, per the contract above.
        let client =
            unsafe { client.as_ref() }.ok_or_else(|| Failure::invalid("client is null"))?;
        // SAFETY: NUL-terminated strings, per the contract above.
        let (tenant_id, ns, stream) = unsafe {
            (
                required_str(tenant_id, "tenant_id")?.to_string(),
                required_str(ns, "ns")?.to_string(),
                required_str(stream, "stream")?.to_string(),
            )
        };
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Page>::new(callback, user_data)?;
        let cluster = std::sync::Arc::clone(client.cluster());
        let end = has_end.then_some(end);
        let ask = async move {
            cluster
                .read(&tenant_id, &ns, &stream, shard, from, end, max_records)
                .await
        };
        spawn_read(client, user, done, ask, Page);
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
