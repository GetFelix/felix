//! A follower takes a stream shard's records, fence or bootstrap only for the
//! incarnation it holds. A broker restarting while its catalog is slow to seed
//! must not take the new stream's records into a log it will not read, and
//! one whose catalog still has the deleted stream must not take them into the
//! deleted stream's log.

use felix_broker::StreamMetadata;
use felix_wire::internal::{Fence, ReplicaLog};

use super::*;

/// A broker as `node` starts it: requiring registration when its data shows
/// streams here carry incarnations, and otherwise not.
fn following_broker(dir: &std::path::Path) -> Arc<Broker> {
    let broker = broker_on(dir);
    broker
        .require_registration_if_in_play()
        .expect("read what is in play");
    broker
}

async fn register(broker: &Broker, incarnation: u64) {
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                incarnation,
                ..Default::default()
            },
        )
        .await
        .expect("register");
}

fn incarnated(
    generation: u64,
    first_offset: u64,
    values: &[&str],
    incarnation: Option<u64>,
) -> ReplicateRecords {
    let mut records = batch(generation, first_offset, values);
    records.shard.incarnation = incarnation;
    records
}

fn shard_dir(dir: &std::path::Path, incarnation: u64) -> std::path::PathBuf {
    let key = felix_storage::log::ShardKey {
        tenant: TENANT.into(),
        namespace: NAMESPACE.into(),
        stream: STREAM.into(),
        shard: 0,
    };
    felix_storage::disk_log::layout::incarnation_dir(dir, &key, incarnation)
}

/// Restarted in a fleet whose streams carry incarnations, before the catalog
/// has seeded: the leader's records wait for the registration.
#[tokio::test]
async fn a_restarted_follower_takes_nothing_before_the_catalog_registers_the_stream() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        // The incarnation this broker held before it stopped.
        let broker = following_broker(dir.path());
        register(&broker, 7).await;
        broker
            .durable_storage()
            .expect("storage")
            .shutdown()
            .await
            .expect("shutdown");
    }

    let broker = following_broker(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    for sent in [Some(7), None] {
        let answer = handler
            .apply(
                SENDER,
                incarnated(4, 0, &["a"], sent),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute, "{sent:?}");
    }
    let fence = Fence {
        correlation_id: 9,
        shard: incarnated(4, 0, &[], Some(7)).shard,
        log: ReplicaLog::Stream,
    };
    assert_eq!(
        refusal(&handler.fence(SENDER, fence).await).code,
        ErrorCode::StaleRoute
    );
    assert!(
        !shard_dir(dir.path(), 0).exists(),
        "took records into incarnation 0"
    );

    // The catalog arrives.
    register(&broker, 7).await;
    let answer = handler
        .apply(
            SENDER,
            incarnated(4, 0, &["a"], Some(7)),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{:?}",
        answer.kind()
    );
    assert!(!shard_dir(dir.path(), 0).exists());
}

/// The catalog here still has the deleted stream; the leader ships the one
/// created after it.
#[tokio::test]
async fn a_follower_refuses_an_incarnation_it_does_not_hold() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following_broker(dir.path());
    register(&broker, 6).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));

    let answer = handler
        .apply(
            SENDER,
            incarnated(4, 0, &["new"], Some(7)),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);

    register(&broker, 7).await;
    let answer = handler
        .apply(
            SENDER,
            incarnated(4, 0, &["new"], Some(7)),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{:?}",
        answer.kind()
    );
}

/// Before incarnations exist every stream is 0 and no request names one, so
/// a registered stream takes records as it always did.
#[tokio::test]
async fn a_stream_at_incarnation_zero_replicates_as_before() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following_broker(dir.path());
    register(&broker, 0).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let answer = handler
        .apply(
            SENDER,
            incarnated(4, 0, &["a"], None),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{:?}",
        answer.kind()
    );
}

/// A fleet that never finalized incarnations replicates as before: a follower
/// whose catalog lags takes a new stream's records rather than holding up the
/// leader's `Quorum` acknowledgements.
#[tokio::test]
async fn before_incarnations_a_follower_takes_an_unregistered_streams_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following_broker(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let answer = handler
        .apply(
            SENDER,
            incarnated(4, 0, &["a"], None),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{:?}",
        answer.kind()
    );
}

/// The first request naming an incarnation is proof they are in play: it is
/// refused until the catalog registers the stream, and a restart requires
/// registration from the start.
#[tokio::test]
async fn a_request_naming_an_incarnation_turns_the_requirement_on_for_good() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let broker = following_broker(dir.path());
        assert!(!broker.registration_required());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
        let answer = handler
            .apply(
                SENDER,
                incarnated(4, 0, &["a"], Some(7)),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);
        assert!(!shard_dir(dir.path(), 0).exists());
    }
    let broker = following_broker(dir.path());
    assert!(
        broker.registration_required(),
        "a restart forgot incarnations are in play"
    );
}

/// The stream is deleted and created again between the incarnation check and
/// the open: the bootstrap's base, the records and the fence must not reach
/// the new stream's log.
#[tokio::test]
async fn a_recreate_between_the_check_and_the_open_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = following_broker(dir.path());
    register(&broker, 7).await;
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4));
    let recreated = Arc::clone(&broker);
    *handler.incarnation_checked.lock() = Some(Box::new(move || {
        recreated.set_stream_incarnation(TENANT, NAMESPACE, STREAM, Some(8));
    }));

    let bootstrap = ReplicateBootstrap {
        correlation_id: 2,
        shard: incarnated(4, 0, &[], Some(7)).shard,
        base_offset: 5,
    };
    assert_eq!(
        refusal(
            &handler
                .bootstrap(SENDER, bootstrap, felix_broker::LogKind::Stream)
                .await
        )
        .code,
        ErrorCode::StaleRoute
    );
    let answer = handler
        .apply(
            SENDER,
            incarnated(4, 0, &["old"], Some(7)),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::StaleRoute);
    let fence = Fence {
        correlation_id: 9,
        shard: incarnated(4, 0, &[], Some(7)).shard,
        log: ReplicaLog::Stream,
    };
    assert_eq!(
        refusal(&handler.fence(SENDER, fence).await).code,
        ErrorCode::StaleRoute
    );

    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("the new stream's log");
    assert_eq!(log.base_offset(), 0, "placed at the old stream's base");
    assert_eq!(
        log.tail_offset().await.expect("tail"),
        0,
        "took the old stream's records"
    );
    assert_eq!(log.accepted_generation(), 0, "took the old stream's fence");
}
