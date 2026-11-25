//! What a client does against a standalone durable broker, through the
//! serving paths that route a write by its term: conditional publish, trim,
//! consumer groups and history. Each runs on a group of one.

use bytes::Bytes;
use felix_broker::{StartPosition, StreamHandle};

use super::*;
use crate::config::BrokerConfig;
use crate::serving::commit_ops::{self, NotWritten};
use crate::serving::group_ops::{self, Settle};
use crate::serving::quic::ClusterContext;
use crate::serving::quic::handlers::publish::{PublishContext, build_publish_context};

fn context(broker: &Arc<Broker>, engine: &Arc<StreamEngine>) -> PublishContext {
    build_publish_context(
        Arc::clone(broker),
        &BrokerConfig::default(),
        ClusterContext {
            engine: Arc::clone(engine),
            ..ClusterContext::default()
        },
    )
}

async fn publish_at(
    broker: &Broker,
    ctx: &PublishContext,
    payload: &'static [u8],
    expected: u64,
) -> Result<u64, NotWritten> {
    commit_ops::publish_at(
        broker,
        ctx,
        "t1",
        "ns",
        "orders",
        None,
        Some(0),
        vec![Bytes::from_static(payload)],
        expected,
        None,
    )
    .await
}

async fn handle(broker: &Broker) -> StreamHandle {
    broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle")
}

/// **A conditional publish lands at the expected offset and is refused,
/// naming the tail, at any other**, with no ingress router: the write is
/// routed at the term the standalone group leads.
#[tokio::test]
async fn a_conditional_publish_checks_the_offset() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (shards, engine, stop) = standalone(&broker);
    led(&shards).await;
    let ctx = context(&broker, &engine);

    let first = publish(&broker, &engine, &[b"zero"]).await;
    let orders = key("orders", 0);
    until("the first record commits", || {
        matches!(
            engine.committed_bound(&orders),
            felix_broker::ReadBound::Committed(bound) if bound > first
        )
    })
    .await;
    let felix_broker::ReadBound::Committed(tail) = engine.committed_bound(&orders) else {
        panic!("no committed bound");
    };
    let landed = publish_at(&broker, &ctx, b"one", tail)
        .await
        .expect("written at the tail");
    assert!(landed >= tail, "landed at {landed} below the tail {tail}");
    match publish_at(&broker, &ctx, b"two", tail).await {
        Err(NotWritten::OffsetMismatch { tail: now }) => assert!(now > tail, "{now}"),
        other => panic!("a stale offset was not refused with the tail: {other:?}"),
    }
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **A head trim on a standalone broker hides the records below its floor**,
/// and a resume below it is refused as too old.
#[tokio::test]
async fn a_trim_hides_what_is_below_its_floor() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    broker.allow_raft_head_trims_when(|| true);
    let (shards, engine, stop) = standalone(&broker);
    led(&shards).await;
    let ctx = context(&broker, &engine);

    let first = publish(&broker, &engine, &[b"r0", b"r1", b"r2"]).await;
    let tail = first + 3;
    let trimmed = commit_ops::trim_at(
        &broker,
        &ctx,
        "t1",
        "ns",
        "orders",
        0,
        first + 2,
        tail,
        Bytes::new(),
    )
    .await
    .map_err(|_| "refused")
    .expect("trimmed");
    assert_eq!(trimmed.oldest, first + 2);
    assert!(matches!(
        broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
            .await,
        Err(felix_broker::BrokerError::CursorTooOld { .. })
    ));
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **A consumer group on a standalone broker hands out each record once and
/// stops handing out what was acknowledged.**
#[tokio::test]
async fn a_consumer_group_claims_and_acknowledges() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let (shards, engine, stop) = standalone(&broker);
    led(&shards).await;
    let ctx = context(&broker, &engine);
    let first = publish(&broker, &engine, &[b"a", b"b"]).await;
    until("the batch commits", || {
        engine.committed_bound(&key("orders", 0)) == felix_broker::ReadBound::Committed(first + 2)
    })
    .await;
    let consumer = felix_broker::GroupConsumer::new("p", "c", 1, false);
    let poll = || {
        group_ops::poll(
            &broker,
            &ctx,
            None,
            "t1",
            "ns",
            "orders",
            0,
            "workers",
            10,
            Duration::from_millis(200),
            Some(&consumer),
            None,
        )
    };
    let claimed = poll().await.expect("poll");
    let offsets: Vec<u64> = claimed.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, vec![first, first + 1]);
    for offset in offsets {
        group_ops::settle(
            &broker,
            &ctx,
            None,
            "t1",
            "ns",
            "orders",
            0,
            "workers",
            offset,
            Settle::Ack,
        )
        .await
        .expect("ack");
    }
    assert!(poll().await.expect("poll").is_empty(), "an acked record");
    let _ = handle(&broker).await;
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **A standalone broker serves its history after a restart, and repairs a
/// torn tail instead of refusing to start.** Bytes of a record that was
/// never acknowledged are cut off and the acknowledged ones are all read.
#[tokio::test]
async fn history_survives_a_restart_with_a_torn_tail() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let end = {
        let broker = broker(root.path()).await;
        let (shards, engine, stop) = standalone(&broker);
        led(&shards).await;
        let first = publish(&broker, &engine, &[b"one", b"two", b"three"]).await;
        until("the batch commits", || {
            engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(first + 3)
        })
        .await;
        stop.cancel();
        assert!(EngineShards::shutdown(&*shards).await);
        first + 3
    };
    let mut newest = None;
    let mut walk = vec![root.path().to_path_buf()];
    while let Some(dir) = walk.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk.push(path);
            } else if path.extension().is_some_and(|ext| ext == "log") {
                let modified = entry.metadata().and_then(|m| m.modified()).expect("mtime");
                if newest.as_ref().is_none_or(|(at, _)| modified >= *at) {
                    newest = Some((modified, path));
                }
            }
        }
    }
    let (_, segment) = newest.expect("a segment file");
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment)
            .expect("open");
        file.write_all(&[0xAB; 7]).expect("tear");
    }

    let broker = broker(root.path()).await;
    let (shards, engine, stop) = standalone(&broker);
    led(&shards).await;
    until("the records are readable again", || {
        matches!(
            engine.committed_bound(&orders),
            felix_broker::ReadBound::Committed(bound) if bound >= end
        )
    })
    .await;
    let records = broker
        .read_committed("t1", "ns", "orders", 0, 0, 1 << 20)
        .await
        .expect("history");
    let payloads: Vec<&[u8]> = records
        .iter()
        .map(|record| record.payload.as_ref())
        .filter(|payload| [&b"one"[..], b"two", b"three"].contains(payload))
        .collect();
    assert_eq!(payloads, vec![&b"one"[..], b"two", b"three"]);
    let next = publish(&broker, &engine, &[b"four"]).await;
    assert!(next >= end, "offset {next} reused below {end}");
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// The entries directly under `dir`, sorted; empty when it does not exist.
fn entries(dir: &std::path::Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .map(|read| {
            read.filter_map(|entry| Some(entry.ok()?.file_name()))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// **A consumer-group seek or commit before the shard's member opens is
/// refused as not ready**, and neither the broker's stream store nor the
/// hybrid's group stores make a directory for it: what they wrote would be
/// served by nothing, and the directory would keep the group from claiming
/// the shard.
#[tokio::test]
async fn a_group_write_before_the_member_opens_is_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = || felix_storage::log::LogConfig {
        fsync_mode: felix_storage::log::FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    let groups =
        felix_broker::ConsumerGroups::open(root.path().join("groups"), config()).expect("groups");
    let dead = felix_broker::DeadLetters::open(root.path().join("dead-letters"), config())
        .expect("dead letters");
    let broker = Broker::new(felix_storage::EphemeralCache::new().into()).with_consumer_groups(
        Arc::new(groups),
        Arc::new(dead),
        Duration::from_secs(30),
        5,
    );
    let broker = super::super::super::tests::registered(
        broker,
        root.path(),
        1,
        super::super::super::tests::INCARNATION,
        felix_broker::ConsistencyLevel::Leader,
        config(),
    )
    .await;
    crate::shards::routing::leave_stores_to_engines(&broker);
    // The feed is not started, so no member opens.
    let runtime =
        GroupRuntime::start(LOCAL_MEMBER, Default::default(), Arc::new(NoPeers)).expect("runtime");
    let shards = RaftShards::new(
        Arc::clone(&broker),
        runtime,
        LOCAL_MEMBER.to_string(),
        Arc::new(fleet_of_one()),
    );
    let engine = Arc::new(StreamEngine::default());
    engine.set_raft(Arc::clone(&shards));
    let ctx = context(&broker, &engine);
    let before = [
        entries(root.path()),
        entries(&root.path().join("groups")),
        entries(&root.path().join("dead-letters")),
    ];

    let not_ready = |err: crate::serving::quic::client_error::ClientError, what: &str| {
        assert_eq!(
            err.retry(),
            felix_wire::RetryClass::Retry,
            "{what}: {err:?}"
        );
        assert_eq!(
            *err.code(),
            felix_wire::ErrorCode::ShardUnavailable,
            "{what}: {err:?}"
        );
    };
    let seek = group_ops::seek(
        &broker,
        &ctx,
        None,
        "t1",
        "ns",
        "orders",
        0,
        "workers",
        felix_wire::StartPosition::Earliest,
        false,
    )
    .await
    .expect_err("a seek went to the hybrid's group store");
    not_ready(seek, "seek");
    let commit = group_ops::settle(
        &broker,
        &ctx,
        None,
        "t1",
        "ns",
        "orders",
        0,
        "workers",
        0,
        Settle::Ack,
    )
    .await
    .expect_err("an ack went to the hybrid's group store");
    not_ready(commit, "ack");

    let after = [
        entries(root.path()),
        entries(&root.path().join("groups")),
        entries(&root.path().join("dead-letters")),
    ];
    assert_eq!(after, before, "a store made a directory for the shard");
}

/// **An in-memory stream keeps its group state on a Raft broker**: no engine
/// serves it, so the hybrid stores still open its group and dead-letter logs,
/// while a durable stream's stay refused. Stores left to engines first, as a
/// broker starts.
#[tokio::test]
async fn an_in_memory_streams_group_state_opens_beside_the_engine() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = || felix_storage::log::LogConfig {
        fsync_mode: felix_storage::log::FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    let groups = Arc::new(
        felix_broker::ConsumerGroups::open(root.path().join("groups"), config()).expect("groups"),
    );
    let dead = Arc::new(
        felix_broker::DeadLetters::open(root.path().join("dead-letters"), config())
            .expect("dead letters"),
    );
    let storage =
        felix_broker::DurableStorage::open(root.path().join("streams"), config()).expect("storage");
    let broker = Arc::new(
        Broker::new(felix_storage::EphemeralCache::new().into())
            .with_consumer_groups(
                Arc::clone(&groups),
                Arc::clone(&dead),
                Duration::from_secs(30),
                5,
            )
            .with_durable_storage(storage),
    );
    broker.require_registered_streams();
    crate::shards::routing::leave_stores_to_engines(&broker);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    let incarnation = super::super::super::tests::INCARNATION;
    for (stream, durable) in [("presence", false), ("orders", true)] {
        broker
            .register_stream(
                "t1",
                "ns",
                stream,
                felix_broker::StreamMetadata {
                    durable,
                    incarnation,
                    replication_factor: Some(1),
                    ..felix_broker::StreamMetadata::default()
                },
            )
            .await
            .expect(stream);
    }

    // What the hybrid lifecycle opens before it serves the shard.
    for kind in crate::shards::lifecycle::GROUP_LOGS {
        let log = broker
            .try_shard_log_off_runtime(kind, "t1", "ns", "presence", 0, None, Some(incarnation))
            .await
            .unwrap_or_else(|err| panic!("{kind:?}: {err}"));
        assert!(
            log.is_some(),
            "{kind:?} log of the in-memory stream refused"
        );
    }
    let key = |stream: &str| felix_broker::GroupKey {
        tenant_id: "t1".into(),
        namespace: "ns".into(),
        stream: stream.into(),
        shard: 0,
        group: "workers".into(),
    };
    assert_eq!(
        groups
            .commit("t1", "ns", "presence", 0, "workers", 3)
            .await
            .expect("commit"),
        3
    );
    assert_eq!(
        groups
            .committed("t1", "ns", "presence", 0, "workers")
            .await
            .expect("committed"),
        Some(3)
    );
    dead.record(&key("presence"), 1).await.expect("dead letter");
    assert_eq!(dead.list(&key("presence")).await.expect("list"), vec![1]);

    let before = [
        entries(&root.path().join("groups")),
        entries(&root.path().join("dead-letters")),
    ];
    groups
        .commit("t1", "ns", "orders", 0, "workers", 3)
        .await
        .expect_err("the hybrid store took a durable stream's commit");
    dead.record(&key("orders"), 1)
        .await
        .expect_err("the hybrid store took a durable stream's dead letter");
    for kind in crate::shards::lifecycle::GROUP_LOGS {
        let opened = broker
            .try_shard_log_off_runtime(kind, "t1", "ns", "orders", 0, None, Some(incarnation))
            .await;
        assert!(
            !matches!(opened, Ok(Some(_))),
            "{kind:?} log of the durable stream opened"
        );
    }
    let after = [
        entries(&root.path().join("groups")),
        entries(&root.path().join("dead-letters")),
    ];
    assert_eq!(
        after, before,
        "a store made a directory for the durable shard"
    );
}
