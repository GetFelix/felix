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

/// A shard whose followers decide its acknowledgements takes writes at that
/// generation without the lease, and at no other. Group state still asks for
/// the lease.
#[tokio::test]
async fn a_shard_acknowledging_by_its_followers_admits_without_the_lease() {
    use felix_replication::driver::WriteFence;

    let fence = ShardFence::default();
    let stream = key(ShardKind::Stream);
    fence.open(&stream, 1);
    let lease = Arc::new(LeaseState::new(std::time::Duration::from_secs(60)));
    fence.bind_lease(Arc::clone(&lease));
    assert_eq!(fence.admit(&stream, 1).err(), Some(Fenced::LeaseLapsed));

    fence.serve_without_lease(&stream, 1);
    let inside = fence.admit(&stream, 1).expect("no lease needed");
    assert_eq!(fence.recheck(&inside), Ok(()));
    assert_eq!(fence.require_lease(), Err(Fenced::LeaseLapsed));

    // A new generation starts back on the lease until the driver says so.
    fence.open(&stream, 2);
    assert_eq!(fence.admit(&stream, 2).err(), Some(Fenced::LeaseLapsed));
    let cache = key(ShardKind::Cache);
    fence.open(&cache, 1);
    assert_eq!(fence.admit(&cache, 1).err(), Some(Fenced::LeaseLapsed));
}

/// Sessions go on without the lease at the generation the driver named, until
/// this broker learns it was deposed there. The first deposal at a generation
/// is queued once.
#[tokio::test]
async fn sessions_leave_the_lease_until_the_shard_is_deposed() {
    use felix_replication::driver::WriteFence;

    let fence = ShardFence::default();
    let stream = key(ShardKind::Stream);
    assert!(!fence.sessions_lease_free(&stream, 1), "never opened");
    fence.open(&stream, 1);
    assert!(!fence.sessions_lease_free(&stream, 1), "not named yet");

    fence.sessions_without_lease(&stream, 1);
    assert!(fence.sessions_lease_free(&stream, 1));
    assert!(!fence.sessions_lease_free(&stream, 2), "another generation");

    WriteFence::deposed(&fence, &stream, 1);
    WriteFence::deposed(&fence, &stream, 1);
    assert!(!fence.sessions_lease_free(&stream, 1));
    assert!(
        fence.confirms_by_round(&stream, 1),
        "group writes still go to the round, which refuses them"
    );
    fence.deposed().await;
    assert_eq!(fence.take_deposals(), vec![(stream.clone(), 1)]);
    assert!(fence.take_deposals().is_empty(), "queued once");

    // A deposal at the old generation says nothing about the next one.
    fence.open(&stream, 2);
    fence.sessions_without_lease(&stream, 2);
    assert!(fence.sessions_lease_free(&stream, 2));
}
