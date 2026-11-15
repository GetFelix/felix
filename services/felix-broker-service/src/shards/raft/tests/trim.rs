//! Head trims on a Raft shard.

use bytes::Bytes;
use felix_broker::{BrokerError, StartPosition, TrimFailed, TrimOutcome};
use felix_storage::log::AppendOnlyLog;

use super::*;

/// Trim `orders` on `member` at `term`, as the control stream does.
async fn trim(
    broker: &Arc<Broker>,
    engine: &crate::shards::routing::StreamEngine,
    term: u64,
    before: u64,
    expected: u64,
) -> Result<TrimOutcome, String> {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let proposed = engine
        .trim_at_on_raft(
            broker,
            &handle,
            &key("orders", 0),
            term,
            before,
            expected,
            Bytes::from_static(b"holder"),
            Duration::from_secs(10),
        )
        .await
        .expect("a Raft shard")
        .map_err(|refused| refused.to_string())?;
    proposed.complete().await.map_err(|failed| match failed {
        TrimFailed::Engine(err) => err.to_string(),
        TrimFailed::Broker(err) => err.to_string(),
    })
}

/// Publish `payload` to `orders` at `term` and return its offset.
async fn publish(
    broker: &Arc<Broker>,
    engine: &crate::shards::routing::StreamEngine,
    term: u64,
    payload: &'static [u8],
) -> u64 {
    use crate::serving::direct_publish::publish_direct;
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    publish_direct(
        broker,
        None,
        engine,
        target(handle, &key("orders", 0), term),
        &[Bytes::from_static(payload)],
        None,
        Duration::from_secs(10),
    )
    .await
    .expect("published")
    .expect("an offset")
}

/// Whether a resume from `offset` on `broker` is refused as too old.
async fn too_old(broker: &Broker, offset: u64) -> bool {
    matches!(
        broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(offset))
            .await,
        Err(BrokerError::CursorTooOld { .. })
    )
}

/// **A trim is written as an entry and hides the shard below its floor on
/// every member**, each of which marks its data root as holding a trim. A stale one is refused with the tail and nothing
/// written, and one at or below the floor writes nothing and answers with
/// the floor, as on the hybrid.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trim_hides_the_shard_below_its_floor_on_every_member() {
    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    lead.broker.allow_raft_head_trims_when(|| true);
    let first = lead.publish(term, b"r0").await;
    let floor = lead.publish(term, b"r1").await;
    let tail = lead.publish(term, b"r2").await + 1;

    let trimmed = trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    assert_eq!(
        trimmed,
        TrimOutcome {
            offset: Some(tail),
            oldest: floor,
        }
    );
    assert!(too_old(&lead.broker, first).await, "read below the floor");
    for member in &members {
        let log = member.shards.log_of(&orders).await.expect("log");
        until("the floor reaches the member", || {
            log.readable_base() == floor
        })
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !too_old(&member.broker, first).await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} served a resume below the floor",
                member.name
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let resumed = member
            .broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Earliest)
            .await
            .expect("a resume from the start");
        assert!(
            resumed.backlog.iter().all(|record| record.offset >= floor),
            "{} served the ring below the floor",
            member.name
        );
        // Every member stores the entry, so every one must refuse a fleet
        // that has not finalized `head_trim`.
        assert!(
            felix_storage::run_state::head_trimmed(member.root.path()).expect("marker"),
            "{} holds a trim without the head-trim marker",
            member.name
        );
    }

    let stale = trim(&lead.broker, &lead.engine, term, tail, tail).await;
    let next = tail + 1;
    let refused = BrokerError::OffsetMismatch {
        expected: tail,
        tail: next,
    };
    assert_eq!(stale, Err(refused.to_string()), "the hybrid's refusal");
    assert_eq!(
        lead.shards
            .log_of(&orders)
            .await
            .expect("log")
            .tail_offset()
            .await
            .expect("tail"),
        next,
        "a stale trim wrote nothing"
    );
    let noop = trim(&lead.broker, &lead.engine, term, first, next)
        .await
        .expect("a no-op");
    assert_eq!(
        noop,
        TrimOutcome {
            offset: None,
            oldest: floor,
        }
    );
    stop(&members).await;
}

/// **The floor holds after a restart.** The log rebuilds it from the trim
/// record, which no snapshot carries.
#[tokio::test]
async fn a_trim_holds_after_a_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let floor = {
        let broker = broker(root.path()).await;
        broker.allow_raft_head_trims_when(|| true);
        let shards = shards(&broker);
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
        until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = engine(&shards);
        let mut tail = 0;
        for payload in [b"r0", b"r1", b"r2"] {
            tail = publish(&broker, &engine, term, payload).await + 1;
        }
        let floor = tail - 1;
        trim(&broker, &engine, term, floor, tail)
            .await
            .expect("trimmed");
        EngineShards::shutdown(&*shards).await;
        floor
    };
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let log = shards.log_of(&orders).await.expect("log");
    until("the floor is back", || log.readable_base() == floor).await;
    assert!(too_old(&broker, floor - 1).await, "read below the floor");
    EngineShards::shutdown(&*shards).await;
}

/// **A trim waits for `raft_head_trim`.** A member that predates trim
/// entries refuses one and replicates nothing past it, so until the node
/// allows Raft trims one is refused by name with nothing written.
#[tokio::test]
async fn a_trim_is_refused_until_raft_head_trim_is_finalized() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let broker = broker(root.path()).await;
    let finalized = Arc::new(AtomicBool::new(false));
    let gate = Arc::clone(&finalized);
    broker.allow_raft_head_trims_when(move || gate.load(Ordering::Acquire));
    let shards = shards(&broker);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let engine = engine(&shards);
    publish(&broker, &engine, term, b"r0").await;
    let tail = publish(&broker, &engine, term, b"r1").await + 1;

    let log = shards.log_of(&orders).await.expect("log");
    let base = log.readable_base();
    let refused = trim(&broker, &engine, term, tail - 1, tail).await;
    let not_enabled = BrokerError::HeadTrimNotEnabled {
        feature: felix_common::fleet::RAFT_HEAD_TRIM.name(),
    };
    assert_eq!(refused, Err(not_enabled.to_string()));
    assert_eq!(
        log.tail_offset().await.expect("tail"),
        tail,
        "nothing written"
    );
    assert_eq!(log.readable_base(), base, "no floor");

    finalized.store(true, Ordering::Release);
    let trimmed = trim(&broker, &engine, term, tail - 1, tail)
        .await
        .expect("trimmed once finalized");
    assert_eq!(trimmed.oldest, tail - 1);
    EngineShards::shutdown(&*shards).await;
}

/// **A follower holding the trim is not read below its floor after
/// `trim_ok`**, though it never hears the commit. On the hybrid only the
/// leader serves readers, so no reader is served below a floor once it is
/// answered; a Raft follower waits for its own commit to pass the trim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_holding_the_trim_is_not_read_below_it_after_the_answer() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let holder = &members[(leader + 1) % 3];
    lead.broker.allow_raft_head_trims_when(|| true);
    let first = lead.publish(term, b"r0").await;
    let floor = lead.publish(term, b"r1").await;
    let tail = lead.publish(term, b"r2").await + 1;
    let log = holder.shards.log_of(&orders).await.expect("log");
    until("the holder has the publishes", || {
        log.commit_offset() == tail
    })
    .await;

    // The holder gets the trim's entry and nothing after it, so it never
    // hears the trim committed. The third member keeps the leader's quorum.
    net.cut_after_entries.lock().insert(holder.name.to_string());
    trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    // The append to the holder may still be in flight.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while log.tail_offset().await.expect("tail") <= tail {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the holder has the trim"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        log.commit_offset() <= tail,
        "the holder never heard the commit"
    );
    let resumed = holder
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await;
    assert!(
        resumed.is_err(),
        "the holder served a resume below the floor after trim_ok"
    );

    net.cut.lock().clear();
    until("the holder hears the commit", || {
        log.readable_base() == floor
    })
    .await;
    assert!(too_old(&holder.broker, first).await, "read below the floor");
    stop(&members).await;
}
