use felix_client::Event;

use super::*;
use crate::status::FELIX_STATUS_INVALID_ARGUMENT;

fn event(offset: u64) -> Event {
    Event {
        tenant_id: "t".into(),
        namespace: "n".into(),
        stream: "s".into(),
        publisher: None,
        timestamp_micros: None,
        payload: bytes::Bytes::from_static(b"x"),
        offset: Some(offset),
        skipped_before: 0,
    }
}

#[test]
fn shard_events_map_to_their_kinds() {
    let record = ShardItem::from_event(ShardEvent::Record {
        shard: 2,
        event: event(7),
    })
    .expect("a record");
    assert_eq!((record.kind, record.shard), (FELIX_SHARD_RECORD, 2));
    assert!(record.event.is_some() && record.message.is_none());

    let lost = ShardItem::from_event(ShardEvent::ShardLost {
        shard: 1,
        error: "owner gone".into(),
    })
    .expect("a loss");
    assert_eq!((lost.kind, lost.shard), (FELIX_SHARD_LOST, 1));
    assert_eq!(lost.message.as_deref(), Some("owner gone"));
    assert!(lost.event.is_none());

    let kinds = [
        (
            ShardEvent::ShardRecovered { shard: 3 },
            FELIX_SHARD_RECOVERED,
        ),
        (
            ShardEvent::StreamDeleted { shard: 3 },
            FELIX_SHARD_STREAM_DELETED,
        ),
    ];
    for (item, kind) in kinds {
        let item = ShardItem::from_event(item).expect("an item");
        assert_eq!((item.kind, item.shard), (kind, 3));
    }
}

#[test]
fn resume_offsets_are_paired_by_index() {
    let shards = [3u32, 0, 2];
    let offsets = [30u64, 5, 20];
    // SAFETY: both point at live arrays of three.
    let map = unsafe { shard_offsets(shards.as_ptr(), offsets.as_ptr(), 3) }.expect("map");
    assert_eq!(
        map.into_iter().collect::<Vec<_>>(),
        [(0, 5), (2, 20), (3, 30)]
    );
}

#[test]
fn no_resume_offsets_need_no_buffers() {
    // SAFETY: nothing is read for a count of zero.
    let map = unsafe { shard_offsets(std::ptr::null(), std::ptr::null(), 0) }.expect("empty");
    assert!(map.is_empty());
}

#[test]
fn a_shard_listed_twice_or_a_missing_array_is_refused() {
    let shards = [1u32, 1];
    let offsets = [4u64, 9];
    // SAFETY: both point at live arrays of two.
    let failure = unsafe { shard_offsets(shards.as_ptr(), offsets.as_ptr(), 2) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("shard 1"), "{}", failure.message);

    // SAFETY: a null array is rejected before it is read.
    let failure = unsafe { shard_offsets(std::ptr::null(), offsets.as_ptr(), 2) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[test]
fn a_lease_dropped_without_its_subscription_closes_the_slot() {
    let slot = Arc::new(Mutex::new(Slot::Reading));
    drop(Lease {
        slot: Arc::clone(&slot),
        subscription: None,
    });
    assert!(matches!(*lock(&slot), Slot::Closed));
}
