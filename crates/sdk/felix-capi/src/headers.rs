//! Publishing records that carry a key and headers.
//!
//! A record's key and headers are stored with it and come back on the
//! delivered event (`felix_event_key`, `felix_event_header`). The key here
//! does not route: the record goes where an unkeyed publish goes.

use std::ffi::{c_char, c_void};
use std::mem::{offset_of, size_of};
use std::ptr;

use bytes::Bytes;
use felix_client::RecordHeaders;

use crate::boundary::{Failure, guard};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::completion::Completion;
use crate::publish::{
    FelixAckMode, FelixBatchCallback, FelixPublishCallback, Published, Recorded, Target, bytes,
    parse_ack, spawn_publish, target,
};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// One header: a name and a value, both byte strings.
///
/// The name is UTF-8, not NUL terminated, non-empty, and must not start with
/// `felix.`, which the broker keeps for itself. The value may be empty. A name
/// may repeat within a record; order is kept.
#[repr(C)]
pub struct FelixHeader {
    pub name: *const u8,
    pub name_len: usize,
    pub value: *const u8,
    pub value_len: usize,
}

/// A record to publish with headers.
///
/// Set `size` to `sizeof(FelixRecord)`. A function that takes an array of
/// records steps through it by the first record's `size`, so a caller built
/// against an older header keeps working when fields are added.
#[repr(C)]
pub struct FelixRecord {
    /// `sizeof(FelixRecord)` as the caller compiled it.
    pub size: usize,
    /// The record's bytes, `payload_len` of them. May be null when the length
    /// is zero.
    pub payload: *const u8,
    pub payload_len: usize,
    /// The record's own key, stored with it and read back by `felix_event_key`.
    /// It does not choose the shard. Null with length zero is no key.
    pub key: *const u8,
    pub key_len: usize,
    /// `header_count` headers. May be null when the count is zero. A record
    /// with no key and no headers is published plain.
    pub headers: *const FelixHeader,
    pub header_count: usize,
}

/// The smallest `size` a caller can pass: through the last field of the
/// first version of the struct.
const MIN_RECORD_SIZE: usize = offset_of!(FelixRecord, header_count) + size_of::<usize>();

/// A record read out of C: its headers and payload, copied.
struct Parsed {
    headers: RecordHeaders,
    payload: Vec<u8>,
}

/// # Safety
///
/// `record` points to a record readable up to `size` bytes, whose pointers
/// meet the contract on [`FelixRecord`] and [`FelixHeader`].
unsafe fn parse_record(record: *const FelixRecord, size: usize) -> Result<Parsed, Failure> {
    if size < MIN_RECORD_SIZE {
        return Err(Failure::invalid(
            "record.size is smaller than the first version of FelixRecord",
        ));
    }
    // SAFETY: readable up to `size`, which covers every field of the first
    // version. Read unaligned: nothing checks the caller's alignment.
    let (payload, payload_len, key, key_len, headers, header_count) = unsafe {
        (
            ptr::addr_of!((*record).payload).read_unaligned(),
            ptr::addr_of!((*record).payload_len).read_unaligned(),
            ptr::addr_of!((*record).key).read_unaligned(),
            ptr::addr_of!((*record).key_len).read_unaligned(),
            ptr::addr_of!((*record).headers).read_unaligned(),
            ptr::addr_of!((*record).header_count).read_unaligned(),
        )
    };
    // SAFETY: the caller's contract, passed through.
    let payload = unsafe { bytes(payload, payload_len, "payload") }?.to_vec();
    // SAFETY: as above.
    let key = unsafe { bytes(key, key_len, "key") }?;
    let mut parsed = Vec::new();
    if header_count > 0 {
        if headers.is_null() {
            return Err(Failure::invalid(
                "headers is null but header_count is not zero",
            ));
        }
        if header_count > isize::MAX as usize / size_of::<FelixHeader>() {
            return Err(Failure::invalid("header_count is too large"));
        }
        // SAFETY: non-null, within `isize::MAX` bytes, and the caller promises
        // `header_count` headers.
        let list = unsafe { std::slice::from_raw_parts(headers, header_count) };
        for header in list {
            // SAFETY: the caller's contract, passed through.
            let name = unsafe { bytes(header.name, header.name_len, "header name") }?;
            let name = std::str::from_utf8(name)
                .map_err(|_| Failure::invalid("a header name is not valid UTF-8"))?;
            // SAFETY: as above.
            let value = unsafe { bytes(header.value, header.value_len, "header value") }?;
            parsed.push((name.to_string(), Bytes::copy_from_slice(value)));
        }
    }
    Ok(Parsed {
        headers: RecordHeaders {
            key: (!key.is_empty()).then(|| Bytes::copy_from_slice(key)),
            headers: parsed,
        },
        payload,
    })
}

/// `count` records at `records`, stepped by the first record's `size`.
///
/// # Safety
///
/// `records` is null or valid for `count` records of that stride.
unsafe fn parse_batch(
    records: *const FelixRecord,
    count: usize,
) -> Result<Vec<(RecordHeaders, Vec<u8>)>, Failure> {
    let mut batch = Vec::new();
    if count == 0 {
        return Ok(batch);
    }
    if records.is_null() {
        return Err(Failure::invalid("records is null but count is not zero"));
    }
    // SAFETY: non-null and readable up to its `size`, which starts it.
    let stride = unsafe { ptr::addr_of!((*records).size).read_unaligned() };
    if stride < MIN_RECORD_SIZE {
        return Err(Failure::invalid(
            "record.size is smaller than the first version of FelixRecord",
        ));
    }
    // The array must fit the address space for the stride to step it.
    if stride
        .checked_mul(count)
        .is_none_or(|total| total > isize::MAX as usize)
    {
        return Err(Failure::invalid("count is too large"));
    }
    for index in 0..count {
        // SAFETY: the caller promises `count` records of this stride.
        let record = unsafe { records.cast::<u8>().add(index * stride) }.cast();
        // SAFETY: as above.
        let parsed = unsafe { parse_record(record, stride) }?;
        batch.push((parsed.headers, parsed.payload));
    }
    Ok(batch)
}

/// Publish one record with a key and headers.
///
/// The key does not route; the record goes where `felix_client_publish`
/// without a key sends it. Returns the record's log offset when there is one.
/// Fails with `FELIX_STATUS_INVALID_ARGUMENT` and sends nothing when a header
/// name is empty or starts with `felix.`, and with `FELIX_STATUS_UNSUPPORTED`
/// when the broker did not accept record headers or the client was connected
/// with `FelixConnectOptions.disable_record_headers`.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `record` is
/// readable up to its `size` and its pointers are valid, `cancel` is null or a
/// live token, and `callback` is safe to call with `user_data` from any
/// thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_with_headers(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    record: *const FelixRecord,
    ack: FelixAckMode,
    cancel: *const FelixCancelToken,
    callback: FelixPublishCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { target(client, tenant_id, ns, stream) }?;
        if record.is_null() {
            return Err(Failure::invalid("record is null"));
        }
        // SAFETY: non-null and readable up to its `size`, which starts it.
        let size = unsafe { ptr::addr_of!((*record).size).read_unaligned() };
        // SAFETY: as above.
        let parsed = unsafe { parse_record(record, size) }?;
        let ack = parse_ack(ack)?;
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
            inner
                .publish_with_headers(
                    &tenant_id,
                    &ns,
                    &stream,
                    parsed.headers,
                    parsed.payload,
                    ack,
                )
                .await
        };
        spawn_publish(client, user, done, publish, |done, published| {
            done.result(published.map(Published).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

/// Publish a batch whose records carry keys and headers.
///
/// `records` is `count` records in an array; the stride is the first
/// record's `size`. The batch goes to shard 0's owner as one frame, as
/// `felix_client_publish_batch` without a key does. A record with neither key
/// nor headers is published plain. Errors are those of
/// `felix_client_publish_with_headers`, and a refused header fails the whole
/// batch before anything is sent.
///
/// # Safety
///
/// As for `felix_client_publish_with_headers`, with `records` valid for
/// `count` records of that stride.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_publish_batch_with_headers(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    records: *const FelixRecord,
    count: usize,
    ack: FelixAckMode,
    cancel: *const FelixCancelToken,
    callback: FelixBatchCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, target) = unsafe { target(client, tenant_id, ns, stream) }?;
        // SAFETY: the caller's contract, passed through.
        let batch = unsafe { parse_batch(records, count) }?;
        let ack = parse_ack(ack)?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Recorded>::new(callback, user_data)?;
        let publish = async move {
            let (t, n, s) = (&target.tenant_id, &target.ns, &target.stream);
            target
                .inner
                .publish_batch_with_headers(t, n, s, batch, ack)
                .await
        };
        spawn_publish(client, user, done, publish, move |done, outcome| {
            Recorded::whole(count, outcome.map_err(Failure::from)).report(done);
        });
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;
