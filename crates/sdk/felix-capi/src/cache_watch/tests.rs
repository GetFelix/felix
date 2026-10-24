use std::ffi::{c_char, c_void};

use felix_client::{CacheChange, ShardMoved};

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

fn change(key: &str, value: Option<&'static [u8]>, offset: u64) -> CacheChange {
    CacheChange {
        key: key.to_string(),
        value: value.map(Bytes::from_static),
        offset,
        expires_at_millis: 1234,
    }
}

fn moved(resume_from: Option<u64>) -> ShardMoved {
    ShardMoved {
        resume_from,
        node_id: None,
        addr: None,
        generation: 3,
    }
}

#[test]
fn a_single_change_carries_key_value_and_expiry() {
    let item = Item::from_single(CacheWatchItem::Change(change("a", Some(b"v"), 7)));
    assert_eq!(item.kind, FELIX_WATCH_CHANGE);
    assert_eq!(item.shard, None);
    assert_eq!((item.key.as_str(), item.offset), ("a", 7));
    assert_eq!(item.value.as_deref(), Some(&b"v"[..]));
    assert_eq!(item.expires_at_millis, 1234);
    assert_eq!(item.resume_from, None);
}

#[test]
fn a_deleted_key_has_no_value_which_differs_from_an_empty_one() {
    let deleted = Item::from_single(CacheWatchItem::Change(change("a", None, 1))).row();
    assert!(!deleted.has_value && deleted.value.is_null());

    let empty = Item::from_single(CacheWatchItem::Change(change("a", Some(b""), 2))).row();
    assert!(empty.has_value && empty.value.is_null() && empty.value_len == 0);
}

#[test]
fn lag_and_moves_carry_where_to_resume() {
    let lagged = Item::from_single(CacheWatchItem::Lagged { resume_from: 40 });
    assert_eq!(lagged.kind, FELIX_WATCH_LAGGED);
    assert_eq!(lagged.resume_from, Some(40));

    let row = Item::from_single(CacheWatchItem::ShardMoved(moved(Some(9)))).row();
    assert_eq!(row.kind, FELIX_WATCH_SHARD_MOVED);
    assert!(row.has_resume_from && row.resume_from == 9);

    let row = Item::from_single(CacheWatchItem::ShardMoved(moved(None))).row();
    assert!(!row.has_resume_from);
}

#[test]
fn sharded_items_name_their_shard_except_state_complete() {
    let item = Item::from_sharded(ShardedCacheWatchItem::Change {
        shard: 2,
        change: change("k", Some(b"v"), 5),
    });
    assert_eq!((item.kind, item.shard), (FELIX_WATCH_CHANGE, Some(2)));

    let complete = Item::from_sharded(ShardedCacheWatchItem::StateComplete).row();
    assert_eq!(complete.kind, FELIX_WATCH_STATE_COMPLETE);
    assert!(!complete.has_shard);

    let closed = Item::from_sharded(ShardedCacheWatchItem::ShardClosed { shard: 3 }).row();
    assert_eq!(closed.kind, FELIX_WATCH_SHARD_CLOSED);
    assert!(closed.has_shard && closed.shard == 3);

    let lagged = Item::from_sharded(ShardedCacheWatchItem::Lagged {
        shard: 1,
        resume_from: 11,
    });
    assert_eq!((lagged.kind, lagged.shard), (FELIX_WATCH_LAGGED, Some(1)));
    assert_eq!(lagged.resume_from, Some(11));

    let moved = Item::from_sharded(ShardedCacheWatchItem::ShardMoved {
        shard: 0,
        moved: moved(Some(4)),
    });
    assert_eq!(
        (moved.kind, moved.shard),
        (FELIX_WATCH_SHARD_MOVED, Some(0))
    );
    assert_eq!(moved.resume_from, Some(4));
}

#[test]
fn the_row_points_at_the_key_and_value_bytes() {
    let item = Item::from_single(CacheWatchItem::Change(change("user:1", Some(b"xyz"), 3)));
    let row = item.row();
    // SAFETY: the pointers and lengths come from `item`, which is alive.
    let (key, value) = unsafe {
        (
            std::slice::from_raw_parts(row.key.cast::<u8>(), row.key_len),
            std::slice::from_raw_parts(row.value, row.value_len),
        )
    };
    assert_eq!((key, value), (&b"user:1"[..], &b"xyz"[..]));
}

#[test]
fn filter_kinds_map_and_unknown_is_refused() {
    assert_eq!(
        watch_filter(FELIX_WATCH_KEY, "k").unwrap(),
        CacheWatchFilter::Key("k".into())
    );
    assert_eq!(
        watch_filter(FELIX_WATCH_PREFIX, "p").unwrap(),
        CacheWatchFilter::Prefix("p".into())
    );
    assert_eq!(
        watch_filter(9, "k").unwrap_err().status,
        FELIX_STATUS_INVALID_ARGUMENT
    );
}

#[test]
fn a_lease_returns_the_watch_to_the_slot() {
    let slot = Arc::new(Mutex::new(Slot::Reading));
    drop(Lease {
        slot: Arc::clone(&slot),
        watch: Some(5u32),
    });
    assert!(matches!(*lock(&slot), Slot::Idle(5)));
}

#[test]
fn a_lease_dropped_without_its_watch_closes_the_slot() {
    let slot = Arc::new(Mutex::new(Slot::<u32>::Reading));
    drop(Lease {
        slot: Arc::clone(&slot),
        watch: None,
    });
    assert!(matches!(*lock(&slot), Slot::Closed));
}

#[test]
fn a_lease_dropped_by_a_panic_leaves_the_watch_readable() {
    let slot = Arc::new(Mutex::new(Slot::Reading));
    let lease = Lease {
        slot: Arc::clone(&slot),
        watch: Some(8u32),
    };
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _lease = lease;
        panic!("a read panicked");
    }));
    assert!(unwound.is_err());
    assert!(matches!(*lock(&slot), Slot::Idle(8)));
}

/// An ordinary call refuses these before a callback is armed.
#[test]
fn watch_arguments_are_checked_at_the_call() {
    // SAFETY: a null client is rejected before anything is read.
    let status = unsafe {
        felix_client_watch_cache(
            ptr::null(),
            c"t".as_ptr(),
            c"n".as_ptr(),
            c"c".as_ptr(),
            FELIX_WATCH_KEY,
            c"k".as_ptr(),
            false,
            false,
            0,
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);

    // SAFETY: a null watch is rejected before anything is read.
    let status =
        unsafe { felix_cache_watch_next(ptr::null(), 0, ptr::null(), None, ptr::null_mut()) };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);

    // SAFETY: as above.
    let status = unsafe {
        felix_cache_watch_resume_offsets(
            ptr::null(),
            ptr::null_mut(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

type Row = (FelixStatus, Option<(FelixWatchItemKind, String)>);

unsafe extern "C" fn on_item(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    item: *const FelixWatchItem,
) {
    // SAFETY: tests pass a live `Mutex`; `item` is null or live.
    let rows = unsafe { &*user_data.cast::<Mutex<Vec<Row>>>() };
    let item = unsafe { item.as_ref() }.map(|item| {
        // SAFETY: `key` is `key_len` bytes while the callback runs.
        let key = unsafe { std::slice::from_raw_parts(item.key.cast::<u8>(), item.key_len) };
        (item.kind, String::from_utf8_lossy(key).into_owned())
    });
    rows.lock().unwrap().push((status, item));
}

#[test]
fn an_item_reaches_the_callback_and_a_failure_hands_none() {
    let rows = Mutex::new(Vec::<Row>::new());
    let user_data = ptr::from_ref(&rows).cast_mut().cast();
    let item = Item::from_single(CacheWatchItem::Change(change("k", Some(b"v"), 1)));
    // SAFETY: the callback is the one this type delivers to.
    unsafe {
        Item::deliver(
            Some(on_item),
            user_data,
            FELIX_STATUS_OK,
            ptr::null(),
            Some(item),
        );
        Item::deliver(
            Some(on_item),
            user_data,
            FELIX_STATUS_TIMEOUT,
            ptr::null(),
            None,
        );
    }
    let rows = rows.into_inner().unwrap();
    assert_eq!(
        rows[0],
        (FELIX_STATUS_OK, Some((FELIX_WATCH_CHANGE, "k".into())))
    );
    assert_eq!(rows[1], (FELIX_STATUS_TIMEOUT, None));
}
