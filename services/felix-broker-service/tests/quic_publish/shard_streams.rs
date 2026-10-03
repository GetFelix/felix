//! A stalled shard holding up only itself: the client gives each shard its
//! own publish stream, so the request order a pipelining stream is answered
//! in never puts one shard's answers behind another's.

use super::*;
use felix_broker_service::shards::routing::{IngressRouter, routing_table_from};
use felix_broker_service::shards::watch::ShardAssignment;
use felix_broker_service::shards::{ShardKey, ShardKind};
use felix_router::{RegionRouter, ShardRouter};

const NODE: &str = "broker-a";

fn shard_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

/// An ownership view in which this broker leads both shards of `orders`. A
/// broker routes a keyed publish by shard only when it has one; without it
/// every publish is shard 0.
fn leading_both_shards() -> Arc<IngressRouter> {
    let router = Arc::new(ShardRouter::new(
        NODE,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = Arc::new(IngressRouter::new(Arc::clone(&router), Arc::default()));
    let assignments: std::collections::HashMap<ShardKey, ShardAssignment> = (0..2)
        .map(|shard| {
            (
                shard_key(shard),
                ShardAssignment {
                    key: shard_key(shard),
                    leader: NODE.to_string(),
                    replicas: Vec::new(),
                    generation: 1,
                    state: "active".to_string(),
                    successor: None,
                    routing: Default::default(),
                },
            )
        })
        .collect();
    let nodes = std::collections::HashMap::new();
    router.publish(routing_table_from(&assignments, &nodes), &nodes);
    for shard in 0..2 {
        ingress.fence().open(&shard_key(shard), 1);
    }
    ingress.publish_servable((0..2).map(|shard| (shard_key(shard), 1)).collect());
    ingress
}

/// **A stalled shard holds up only itself.** A pipelining stream is answered
/// in request order, so a shard whose publishes cannot commit would hold back
/// every other shard sharing its stream. Each shard gets a stream of its own,
/// so the healthy shard of the same stream keeps being answered.
#[tokio::test]
#[serial]
async fn a_stalled_shard_does_not_hold_up_another_shard_of_the_stream() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 2,
                ..Default::default()
            },
        )
        .await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let mut config = felix_broker_service::config::BrokerConfig::from_env()?;
    config.ack_on_commit = true;
    let auth = auth_fixture("t1", vec!["stream.publish:stream:t1/*/*".to_string()]);
    let limit =
        felix_broker_service::serving::quic::ConnectionLimit::new(config.max_client_connections);
    let limits = felix_broker_service::serving::limits::ListenerLimits::from_config(&config);
    tokio::spawn(felix_broker_service::serving::quic::serve_with_shutdown(
        server,
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.auth),
        tokio_util::sync::CancellationToken::new(),
        tokio_util::task::TaskTracker::new(),
        felix_broker_service::serving::quic::ClusterContext {
            ingress: Some(leading_both_shards()),
            ..Default::default()
        },
        limit,
        limits,
    ));

    let cluster = Arc::new(
        felix_client::ClusterClient::connect(
            &[addr],
            "localhost",
            build_client_config(cert, &auth)?,
        )
        .await?,
    );
    let (shards, routing) = cluster
        .client()
        .await
        .stream_routing("t1", "default", "orders")
        .await?;
    assert_eq!(shards, 2);
    let key_for = |shard: u32| {
        (0..)
            .map(|i| bytes::Bytes::from(format!("key-{i}")))
            .find(|key| felix_wire::routing::shard_for_routing(routing, shards, Some(key)) == shard)
            .expect("a key for the shard")
    };
    let (stalled_key, healthy_key) = (key_for(1), key_for(0));
    let publish = |key: bytes::Bytes| {
        let cluster = Arc::clone(&cluster);
        async move {
            cluster
                .publish_keyed(
                    "t1",
                    "default",
                    "orders",
                    b"record".to_vec(),
                    key,
                    AckMode::PerMessage,
                )
                .await
        }
    };
    // Both shards answer before the stall.
    publish(stalled_key.clone()).await?;
    publish(healthy_key.clone()).await?;

    // A claim on shard 1's commit order that is not completed: every later
    // publish to shard 1 waits behind it, as behind a quorum that never comes.
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 1)
        .await?;
    let held = broker
        .claim_publish(&handle, &[bytes::Bytes::from_static(b"held")])
        .await?;
    let stalled = tokio::spawn(publish(stalled_key));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stalled.is_finished(), "shard 1 was not stalled");

    for _ in 0..5 {
        timeout(Duration::from_secs(2), publish(healthy_key.clone()))
            .await
            .context("shard 0 was held up behind the stalled shard 1")??;
    }

    broker.complete_publish(held).await?;
    timeout(Duration::from_secs(5), stalled)
        .await
        .context("shard 1 did not recover")???;
    Ok(())
}
