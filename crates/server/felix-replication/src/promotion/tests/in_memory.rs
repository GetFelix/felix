//! A promoted shard of an in-memory stream has no log to fence.

use super::*;

/// **Its fence opens no log and asks no replica**, and the shard opens on
/// the lease. The fence used to open the leader's log and have each replica
/// open its own, which left the stream with a log on every broker (#3136).
#[tokio::test]
async fn an_in_memory_stream_is_not_fenced() {
    let replicas = Replicas::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = broker_on(dir.path());
    leader.register_tenant(TENANT).await.expect("tenant");
    leader
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    leader
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: false,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    // Without the lease to fall back on, as in a fleet that acknowledges on
    // its followers: a fence there never opens on the lease for a durable log.
    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key_of(ShardKind::Stream),
        &route_of(ShardKind::Stream),
        false,
        None,
    )
    .await;

    assert!(matches!(outcome, Outcome::Lease { .. }), "{outcome:?}");
    assert_eq!(replicas.sent(Kind::Fence), Vec::<String>::new());
    assert!(
        !leader
            .durable_storage()
            .expect("storage")
            .stream_on_disk(TENANT, NAMESPACE, STREAM, 0),
        "the fence opened a log for the stream"
    );
}
