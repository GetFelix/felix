//! The fence on its own: who gets in, and when a closed shard is quiet.
use super::*;
use crate::shards::ShardKind;
use crate::shards::routing::IngressRouter;

fn key(kind: ShardKind) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind,
    }
}

#[test]
fn an_open_fence_admits_writes_at_its_generation_only() {
    let fence = ShardFence::default();
    let stream = key(ShardKind::Stream);
    assert!(fence.enter(&stream, 1).is_none(), "never opened");

    fence.open(&stream, 2);
    assert!(fence.enter(&stream, 2).is_some());
    assert!(
        fence.enter(&stream, 1).is_none(),
        "admitted under an older generation"
    );
    assert!(
        fence.enter(&key(ShardKind::Cache), 2).is_none(),
        "a cache of the same name is another shard"
    );
}

/// The property the drained report rests on: a write that got in before the
/// close holds the fence open until it is done, and one that did not is
/// refused.
#[test]
fn a_closed_fence_refuses_and_is_quiet_once_writes_already_in_finish() {
    let fence = ShardFence::default();
    let stream = key(ShardKind::Stream);
    fence.open(&stream, 1);
    let inside = fence.enter(&stream, 1).expect("open");
    assert!(!fence.quiesced(&stream), "open");

    fence.close(&stream);
    assert!(fence.enter(&stream, 1).is_none(), "entered after the close");
    assert!(!fence.quiesced(&stream), "a write is still in flight");

    drop(inside);
    assert!(fence.quiesced(&stream));
}

#[test]
fn a_shard_never_opened_here_is_quiet() {
    assert!(ShardFence::default().quiesced(&key(ShardKind::Stream)));
}

#[test]
fn reopening_at_a_new_generation_admits_again() {
    let fence = ShardFence::default();
    let stream = key(ShardKind::Stream);
    fence.open(&stream, 1);
    fence.close(&stream);
    fence.open(&stream, 3);
    assert!(fence.enter(&stream, 3).is_some());
    assert!(fence.enter(&stream, 1).is_none());
}

#[tokio::test]
async fn quiesce_waits_for_the_last_write_in_flight() {
    let fence = Arc::new(ShardFence::default());
    let stream = key(ShardKind::Stream);
    fence.open(&stream, 1);
    let inside = fence.enter(&stream, 1).expect("open");
    fence.close(&stream);

    let waiting = tokio::spawn({
        let fence = Arc::clone(&fence);
        let stream = stream.clone();
        async move { fence.quiesce(&stream).await }
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "a write is still in flight");

    drop(inside);
    tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .expect("quiesced once the write left")
        .expect("task");
}

/// The fence is the lease's commit gate: every write path enters it, so a
/// lapsed lease must refuse at the fence, not only where a path asked.
#[tokio::test]
async fn a_lapsed_lease_refuses_every_write_until_it_is_renewed() {
    let fence = ShardFence::default();
    let stream = key(ShardKind::Stream);
    fence.open(&stream, 1);
    let lease = Arc::new(LeaseState::new(std::time::Duration::from_secs(60)));
    fence.bind_lease(Arc::clone(&lease));

    assert_eq!(
        fence.admit(&stream, 1).err(),
        Some(Fenced::LeaseLapsed),
        "a lease never granted"
    );
    lease.renew();
    let inside = fence.admit(&stream, 1).expect("leased and open");

    lease.surrender();
    assert_eq!(fence.admit(&stream, 1).err(), Some(Fenced::LeaseLapsed));
    assert_eq!(
        fence.recheck(&inside),
        Err(Fenced::LeaseLapsed),
        "a write admitted before the lapse must not commit after it"
    );
    assert_eq!(
        fence.admit(&stream, 2).err(),
        Some(Fenced::NotServing),
        "the generation is checked too"
    );

    lease.renew();
    assert!(fence.admit(&stream, 1).is_ok(), "renewed");
}

/// A write that took its place in the fence at admission keeps it through the
/// queue, but the lease is read again when it claims its offsets.
#[tokio::test]
async fn a_held_place_is_rechecked_against_the_lease_at_the_claim() {
    let fence = Arc::new(ShardFence::default());
    let stream = key(ShardKind::Stream);
    fence.open(&stream, 1);
    let lease = Arc::new(LeaseState::new(std::time::Duration::from_secs(60)));
    lease.renew();
    fence.bind_lease(Arc::clone(&lease));
    let ingress = IngressRouter::new(
        Arc::new(felix_router::ShardRouter::new(
            "broker-a",
            "us-west-2",
            felix_router::RegionRouter::new("us-west-2".to_string()),
        )),
        Arc::clone(&fence),
    );

    let mut held = Some(fence.admit(&stream, 1).expect("admitted"));
    lease.surrender();
    assert_eq!(
        enter_or_keep(&mut held, Some(&ingress), Some(&stream), 1).err(),
        Some(Fenced::LeaseLapsed)
    );
}
