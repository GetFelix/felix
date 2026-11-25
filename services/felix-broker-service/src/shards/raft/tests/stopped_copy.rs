//! A copy a storage error stopped is reported, and stops its shard alone.

use super::short_placement::Gauges;
use super::*;

/// **A poisoned Raft log stops its own shard and says why.** The broker
/// counts the stopped copy, `shard_inspect` names the term and the error, a
/// request for the shard is refused with that error rather than "still
/// opening", and another shard on the same broker keeps serving.
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
    let Some(Dispatch::Unavailable(reason)) = route(&shards, &orders) else {
        panic!("a stopped copy was routed to");
    };
    assert_eq!(reason, Reason::CopyStopped(stopped.error.clone()));
    assert_eq!(
        reason.wire_name(),
        felix_wire::shard_unavailable_reason::NOT_READY
    );
    assert!(
        reason
            .to_string()
            .contains("copy of the shard was stopped by a storage error"),
        "{reason}"
    );

    assert_eq!(shards.leading(&orders), None);
    // Still reported on a later pass: the copy reopened rotated, with no
    // other copy to catch up from, and serves nothing.
    shards.take(&mut both()).await;
    assert_eq!(shards.stopped_copy(&orders), Some(stopped.clone()));
    assert_eq!(shards.leading(&orders), None);
    assert_eq!(gauges.get(STOPPED_ON_STORAGE_ERROR), Some(1.0));

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
