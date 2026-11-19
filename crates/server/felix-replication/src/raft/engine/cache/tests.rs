use super::*;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "n".into(),
        stream: "c".into(),
        shard,
        kind: crate::ShardKind::Cache,
    }
}

/// A write's floor stays at the lowest sequence of its shard still
/// waiting, and moves past one only once it is answered or dropped.
#[test]
fn a_floor_waits_for_the_lowest_unanswered_write_of_its_shard() {
    let anonymous = Anonymous::new();
    let (one, two) = (key(1), key(2));
    let (first, held) = anonymous.take(&one);
    assert_eq!((first.sequence, first.floor), (1, 1));
    let (second, answered) = anonymous.take(&one);
    assert_eq!((second.sequence, second.floor), (2, 1));
    // Another shard's floor is not held by this one's write.
    let (other, _other) = anonymous.take(&two);
    assert_eq!((other.sequence, other.floor), (3, 3));
    drop(answered);
    let (third, _third) = anonymous.take(&one);
    assert_eq!((third.sequence, third.floor), (4, 1));
    // Dropped as a cancelled call drops it.
    drop(held);
    let (fourth, _fourth) = anonymous.take(&one);
    assert_eq!((fourth.sequence, fourth.floor), (5, 4));
}

/// A TTL too long for the stamp's microseconds saturates instead of
/// wrapping to a short one.
#[test]
fn a_ttl_past_the_stamps_range_never_wraps() {
    let request = Request {
        producer: 1,
        sequence: 1,
        floor: 1,
    };
    let put = |ttl| CacheOp::Put {
        key: "k".into(),
        value: bytes::Bytes::new(),
        ttl: Some(ttl),
        condition: CacheCondition::None,
    };
    let (_, ttl) = command(put(Duration::from_millis(u64::MAX)), request);
    assert_eq!(ttl, u64::MAX);
    let (_, ttl) = command(put(Duration::from_nanos(1)), request);
    assert_eq!(ttl, 1);
}
