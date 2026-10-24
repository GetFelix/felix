use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use bytes::Bytes;

use super::*;
use crate::groups::claim;
use crate::status::FELIX_STATUS_AUTH;

type Row = (u32, u64, Vec<u8>);
type Seen = (FelixStatus, Option<String>, Option<Vec<Row>>);

unsafe extern "C" fn polled(
    user_data: *mut c_void,
    status: FelixStatus,
    message: *const c_char,
    records: *const FelixShardedGroupRecord,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    let rows = (!records.is_null()).then(|| {
        // SAFETY: `count` entries, valid while the callback runs.
        unsafe { std::slice::from_raw_parts(records, count) }
            .iter()
            .map(|r| {
                let payload = if r.record.payload.is_null() {
                    Vec::new()
                } else {
                    // SAFETY: `payload_len` bytes, valid during the callback.
                    unsafe { std::slice::from_raw_parts(r.record.payload, r.record.payload_len) }
                        .to_vec()
                };
                (r.shard, r.record.offset, payload)
            })
            .collect()
    });
    let message = (!message.is_null()).then(|| {
        // SAFETY: NUL-terminated, valid during the callback.
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    });
    calls.lock().unwrap().push((status, message, rows));
}

fn record(shard: u32, offset: u64, payload: &'static [u8]) -> ShardedGroupRecord {
    ShardedGroupRecord {
        shard,
        record: felix_client::GroupRecord {
            payload: Bytes::from_static(payload),
            ..claim(offset, 1)
        },
    }
}

/// The same offset on two shards is two records; the shard is what tells
/// them apart.
#[test]
fn every_record_names_its_shard() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    Completion::<ShardedPolled>::new(Some(polled), data)
        .unwrap()
        .ok(ShardedPolled(vec![record(2, 7, b"a"), record(3, 7, b"b")]));
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls[0].0, FELIX_STATUS_OK);
    assert_eq!(
        calls[0].2,
        Some(vec![(2, 7, b"a".to_vec()), (3, 7, b"b".to_vec())])
    );
}

#[test]
fn an_empty_poll_is_ok_and_a_failure_has_no_records() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    Completion::<ShardedPolled>::new(Some(polled), data)
        .unwrap()
        .ok(ShardedPolled(Vec::new()));
    Completion::<ShardedPolled>::new(Some(polled), data)
        .unwrap()
        .fail(Failure {
            status: FELIX_STATUS_AUTH,
            message: "denied".to_string(),
        });
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls[0].2, Some(Vec::new()));
    assert_eq!(calls[1].0, FELIX_STATUS_AUTH);
    assert_eq!(calls[1].2, None);
}

#[test]
fn a_null_group_and_a_zero_max_are_refused() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    // SAFETY: null is the case under test.
    let status = unsafe {
        felix_sharded_group_poll(std::ptr::null(), 1, std::ptr::null(), Some(polled), data)
    };
    assert_eq!(status, crate::status::FELIX_STATUS_INVALID_ARGUMENT);
    // SAFETY: as above.
    let status = unsafe { felix_sharded_group_shards(std::ptr::null(), std::ptr::null_mut()) };
    assert_eq!(status, crate::status::FELIX_STATUS_INVALID_ARGUMENT);
    assert!(calls.into_inner().unwrap().is_empty());
}
