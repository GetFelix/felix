//! A copy a storage error stopped is reported, and stops its shard alone.

use super::short_placement::Gauges;
use super::*;

/// **A poisoned Raft log stops its own shard and says why.** The broker
/// counts the stopped copy, `shard_inspect` names the term and the error, a
/// request for the shard is refused as stopped rather than "still opening",
/// and another shard on the same broker keeps serving.
#[tokio::test]
async fn a_poisoned_log_stops_its_shard_alone_and_is_reported() {
    use crate::serving::direct_publish::publish_direct;

    let gauges = Gauges::default();
    let _recorder = metrics::set_default_local_recorder(&gauges);
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    broker
        .register_stream(
            "t1",
            "ns",
            "payments",
            StreamMetadata {
                durable: true,
                incarnation: INCARNATION,
                replication_factor: Some(1),
                consistency: ConsistencyLevel::Leader,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("payments");
    let shards = shards(&broker);
    let engine = engine(&shards);
    let orders = key("orders", 0);
    let payments = key("payments", 0);
    let both = || {
        assignments([
            assigned(orders.clone(), INCARNATION),
            assigned(payments.clone(), INCARNATION),
        ])
    };
    shards.take(&mut both()).await;
    let stored_both = || {
        assignments([
            stored(assigned(orders.clone(), INCARNATION), &[&shards]),
            stored(assigned(payments.clone(), INCARNATION), &[&shards]),
        ])
    };
    let mut fed = stored_both();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while shards.leading(&orders).is_none() || shards.leading(&payments).is_none() {
        assert!(tokio::time::Instant::now() < deadline, "the groups formed");
        shards.take(&mut fed).await;
        fed = stored_both();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shards.take(&mut both()).await;
    let timeout = Duration::from_secs(10);
    let publish = |key: &ShardKey, payload: &'static [u8]| {
        let (broker, engine, shards, key) = (&broker, &engine, &shards, key.clone());
        async move {
            let term = shards.leading(&key)?;
            let handle = broker
                .resolve_stream_handle("t1", "ns", &key.stream, 0)
                .await
                .ok()?;
            publish_direct(
                broker,
                None,
                engine,
                target(handle, &key, term),
                &[bytes::Bytes::from_static(payload)],
                None,
                timeout,
            )
            .await
            .ok()
            .flatten()
        }
    };
    publish(&orders, b"before").await.expect("orders published");
    let first = publish(&payments, b"before")
        .await
        .expect("payments published");
    let term = shards.runtime.status(&orders).expect("status").term;

    shards
        .log_of(&orders)
        .await
        .expect("orders log")
        .fail_next_flush();
    let deadline = tokio::time::Instant::now() + timeout;
    while shards.runtime.storage_failure(&orders).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the member never stopped"
        );
        let _ = publish(&orders, b"refused").await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shards.take(&mut both()).await;

    let stopped = shards.stopped_copy(&orders).expect("reported stopped");
    assert_eq!(stopped.term, term);
    assert!(stopped.error.contains("fsync failure"), "{}", stopped.error);
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(1.0));
    let view = engine.raft_view(&orders).expect("a raft view");
    assert_eq!(
        view.stopped,
        Some(felix_wire::InspectedStop {
            term,
            error: stopped.error.clone(),
        })
    );
    assert_refused(&shards, &orders, &stopped.error).await;

    assert_eq!(shards.leading(&orders), None);
    // Still reported on a later pass: the copy reopened rotated, with no
    // other copy to catch up from, and serves nothing.
    shards.take(&mut both()).await;
    assert_eq!(shards.stopped_copy(&orders), Some(stopped.clone()));
    assert_eq!(shards.leading(&orders), None);
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(1.0));
    assert!(shards.runtime.holds(&orders), "the copy reopened");
    assert_refused(&shards, &orders, &stopped.error).await;

    // The other shard on this broker is untouched.
    assert_eq!(shards.stopped_copy(&payments), None);
    assert!(engine.raft_view(&payments).expect("view").stopped.is_none());
    let after = publish(&payments, b"after")
        .await
        .expect("payments still serves");
    assert!(after > first);

    // A shard that leaves this broker is no longer counted.
    shards
        .take(&mut assignments([assigned(payments.clone(), INCARNATION)]))
        .await;
    assert_eq!(shards.stopped_copy(&orders), None);
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(0.0));
    EngineShards::shutdown(&*shards).await;
}

/// A write and a subscribe for a stopped copy are refused at once, as
/// retryable, and the refusal does not carry the storage error: it can name
/// the broker's data paths. A write is not held for a takeover either.
async fn assert_refused(shards: &Arc<RaftShards>, key: &ShardKey, error: &str) {
    assert_eq!(
        route(shards, key),
        Some(Dispatch::Unavailable(Reason::CopyStopped))
    );
    assert_eq!(
        Reason::CopyStopped.wire_name(),
        felix_wire::shard_unavailable_reason::NOT_READY
    );
    assert_eq!(
        Reason::CopyStopped.to_string(),
        "this broker's copy of the shard was stopped by a storage error"
    );
    let ingress = ingress(shards);
    let (dispatch, fenced) =
        tokio::time::timeout(Duration::from_millis(100), ingress.dispatch_write(key))
            .await
            .expect("the write was held");
    assert_eq!(dispatch, Dispatch::Unavailable(Reason::CopyStopped));
    assert!(fenced.is_none());
    let refusal = crate::serving::quic::handlers::redirect::redirect_for(
        Some(&ingress),
        None,
        &key.tenant_id,
        &key.namespace,
        &key.stream,
        key.shard,
        key.kind,
        0,
    )
    .expect("the subscribe was refused");
    let felix_wire::Message::Error { message, .. } = &refusal else {
        panic!("not an error: {refusal:?}");
    };
    assert!(
        message.contains("this broker's copy of the shard was stopped by a storage error"),
        "{message}"
    );
    assert!(!message.contains(error), "{message}");
}

/// **A copy that reopens and leads again is no longer reported.** A failed
/// mark log keeps the copy id, so the one copy of the group leads again
/// once it reopens, and a leader with no other voter hears itself.
#[tokio::test]
async fn a_copy_that_reopens_and_leads_is_no_longer_reported() {
    let gauges = Gauges::default();
    let _recorder = metrics::set_default_local_recorder(&gauges);
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let engine = engine(&shards);
    let orders = key("orders", 0);
    take_again(&shards, &orders).await;
    take_formed(&shards, assigned(orders.clone(), INCARNATION)).await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let copy = shards.runtime.status(&orders).expect("status").member;

    let marks = shards.root.get().expect("root").mark_log().clone();
    marks.fail_writes(true);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while shards.runtime.storage_failure(&orders).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the member never stopped"
        );
        if let Some(term) = shards.leading(&orders) {
            let _ = write_or_refuse(&broker, &engine, &orders, term).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    take_again(&shards, &orders).await;
    assert!(shards.stopped_copy(&orders).is_some());
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(1.0));

    marks.fail_writes(false);
    take_again(&shards, &orders).await;
    until("the reopened copy leads", || {
        shards.leading(&orders).is_some()
    })
    .await;
    assert_eq!(
        shards.runtime.status(&orders).expect("status").member,
        copy,
        "the copy kept its id"
    );
    take_again(&shards, &orders).await;
    assert_eq!(shards.stopped_copy(&orders), None);
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(0.0));
    assert!(engine.raft_view(&orders).expect("view").stopped.is_none());
    EngineShards::shutdown(&*shards).await;
}

/// **A follower's copy that reopens under a new copy id is reported until
/// the group takes it in and it hears the leader.** Until then it is no
/// voter, and nothing reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotated_follower_is_reported_until_it_hears_the_leader() {
    use crate::cluster::lease::LeaseState;
    use crate::shards::watch::{RaftTarget, RaftTargetVersion};

    let gauges = Gauges::default();
    let _recorder = metrics::set_default_local_recorder(&gauges);
    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let leases: Vec<Arc<LeaseState>> = members
        .iter()
        .map(|member| {
            let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(10)));
            member.shards.follow_lease(Arc::clone(&lease));
            lease
        })
        .collect();
    let term = members[leader].shards.leading(&orders).expect("term");
    let follower = &members[(leader + 1) % 3];
    let old = follower
        .shards
        .runtime
        .status(&orders)
        .expect("status")
        .member;

    follower
        .shards
        .log_of(&orders)
        .await
        .expect("follower log")
        .fail_next_flush();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while follower.shards.runtime.storage_failure(&orders).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower never stopped"
        );
        members[leader].publish(term, b"x").await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let pass = || async {
        follower.shards.take(&mut assignments([replicated()])).await;
    };
    pass().await;
    pass().await;
    let rotated = follower
        .shards
        .runtime
        .status(&orders)
        .expect("reopened")
        .member;
    assert_ne!(rotated.copy, old.copy, "a poisoned log rotates");
    assert!(follower.shards.stopped_copy(&orders).is_some());
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(1.0));

    // The control plane stores the target with the new copy in place of
    // the old one, and the leader acts on it.
    let mut voters = members[leader]
        .shards
        .runtime
        .voters(&orders)
        .expect("voters");
    assert!(voters.remove(&old));
    voters.insert(rotated.clone());
    let mut fed = replicated();
    fed.target = Some(RaftTarget {
        version: RaftTargetVersion {
            incarnation: 1,
            counter: 1,
        },
        members: voters
            .iter()
            .map(|voter| (voter.broker.to_string(), voter.copy))
            .collect(),
    });
    for (member, lease) in members.iter().zip(&leases) {
        member.shards.take(&mut assignments([fed.clone()])).await;
        lease.record_head(tokio::time::Instant::now(), 10);
        member
            .shards
            .answered(Some(tokio::time::Instant::now()), Some(10));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while follower.shards.stopped_copy(&orders).is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the rotated copy stayed reported"
        );
        follower.shards.take(&mut assignments([fed.clone()])).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(follower.shards.runtime.hears_leader(&orders), Some(true));
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(0.0));
    assert!(
        follower
            .engine
            .raft_view(&orders)
            .expect("view")
            .stopped
            .is_none()
    );
    stop(&members).await;
}

/// Publish one record to `key` at `term`, answered or not.
async fn write_or_refuse(
    broker: &Arc<Broker>,
    engine: &crate::shards::routing::StreamEngine,
    key: &ShardKey,
    term: u64,
) -> Option<u64> {
    let handle = broker
        .resolve_stream_handle("t1", "ns", &key.stream, 0)
        .await
        .ok()?;
    crate::serving::direct_publish::publish_direct(
        broker,
        None,
        engine,
        target(handle, key, term),
        &[bytes::Bytes::from_static(b"x")],
        None,
        Duration::from_secs(2),
    )
    .await
    .ok()
    .flatten()
}
