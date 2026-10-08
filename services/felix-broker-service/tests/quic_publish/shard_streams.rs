//! A stalled shard holding up only itself: the client gives each shard its
//! own publish stream, so the request order a pipelining stream is answered
//! in never puts one shard's answers behind another's. And what that must
//! not cost: each shard's records still land in the order one client sent
//! them.

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

/// A broker leading both shards of the durable stream `orders`.
struct TwoShards {
    broker: Arc<Broker>,
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: AuthFixture,
    _dir: tempfile::TempDir,
}

impl TwoShards {
    async fn start() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let storage = felix_broker::DurableStorage::open(
            dir.path(),
            felix_storage::log::LogConfig {
                fsync_mode: felix_storage::log::FsyncMode::None,
                preallocate_segments: false,
                ..Default::default()
            },
        )?;
        let broker =
            Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
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
        let limit = felix_broker_service::serving::quic::ConnectionLimit::new(
            config.max_client_connections,
        );
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
        Ok(Self {
            broker,
            addr,
            cert,
            auth,
            _dir: dir,
        })
    }

    fn client_config(&self) -> Result<ClientConfig> {
        build_client_config(self.cert.clone(), &self.auth)
    }

    /// A key that routes to `shard`, the `nth` such key.
    fn key_for(&self, shard: u32, nth: usize) -> bytes::Bytes {
        (0..)
            .map(|i| bytes::Bytes::from(format!("key-{i}")))
            .filter(|key| {
                felix_wire::routing::shard_for_routing(Default::default(), 2, Some(key)) == shard
            })
            .nth(nth)
            .expect("a key for the shard")
    }

    /// Stall shard 1: a claim on its commit order that is not completed, so
    /// every later publish to it waits, as behind a quorum that never comes.
    async fn stall_shard_one(&self) -> Result<felix_broker::ClaimedPublish> {
        let handle = self
            .broker
            .resolve_stream_handle("t1", "default", "orders", 1)
            .await?;
        Ok(self
            .broker
            .claim_publish(&handle, &[bytes::Bytes::from_static(b"held")], None)
            .await?)
    }
}

/// **A stalled shard holds up only itself.** A pipelining stream is answered
/// in request order, so a shard whose publishes cannot commit would hold back
/// every other shard sharing its stream. Each shard gets a stream of its own,
/// so the healthy shard of the same stream keeps being answered.
#[tokio::test]
#[serial]
async fn a_stalled_shard_does_not_hold_up_another_shard_of_the_stream() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let cluster = Arc::new(
        felix_client::ClusterClient::connect(
            &[fixture.addr],
            "localhost",
            fixture.client_config()?,
        )
        .await?,
    );
    let (shards, routing) = cluster
        .client()
        .await
        .stream_routing("t1", "default", "orders")
        .await?;
    assert_eq!((shards, routing), (2, Default::default()));
    let (stalled_key, healthy_key) = (fixture.key_for(1, 0), fixture.key_for(0, 0));
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

    let held = fixture.stall_shard_one().await?;
    let stalled = tokio::spawn(publish(stalled_key));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stalled.is_finished(), "shard 1 was not stalled");

    for _ in 0..5 {
        timeout(Duration::from_secs(2), publish(healthy_key.clone()))
            .await
            .context("shard 0 was held up behind the stalled shard 1")??;
    }

    fixture.broker.complete_publish(held).await?;
    timeout(Duration::from_secs(5), stalled)
        .await
        .context("shard 1 did not recover")???;
    Ok(())
}

/// **A plain `Client` puts one stream's shards on separate streams too.** It
/// learns the stream's width on the first keyed publish, so a hot stream's
/// shards spread over its connections instead of sharing one stream. A
/// stalled shard holding up the other is what sharing one would look like.
#[tokio::test]
#[serial]
async fn a_plain_clients_keyed_publishes_go_on_their_shards_own_streams() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let client = Client::connect(fixture.addr, "localhost", fixture.client_config()?).await?;
    let publisher = Arc::new(client.publisher().await?);
    let (stalled_key, healthy_key) = (fixture.key_for(1, 0), fixture.key_for(0, 0));
    let publish = |key: bytes::Bytes| {
        let publisher = Arc::clone(&publisher);
        async move {
            publisher
                .publish_keyed(
                    "t1",
                    "default",
                    "orders",
                    key,
                    b"record".to_vec(),
                    AckMode::PerMessage,
                )
                .await
        }
    };
    publish(stalled_key.clone()).await?;
    publish(healthy_key.clone()).await?;

    let held = fixture.stall_shard_one().await?;
    let stalled = tokio::spawn(publish(stalled_key));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stalled.is_finished(), "shard 1 was not stalled");

    for _ in 0..5 {
        timeout(Duration::from_secs(2), publish(healthy_key.clone()))
            .await
            .context("shard 0 shares a publish stream with the stalled shard 1")??;
    }

    fixture.broker.complete_publish(held).await?;
    timeout(Duration::from_secs(5), stalled)
        .await
        .context("shard 1 did not recover")???;
    Ok(())
}

/// **Spreading a stream's shards keeps each shard in the order it was
/// sent.** One task publishes a run of records across both shards without
/// waiting for any answer, so nothing but the routing keeps them in order.
/// Each shard's log must hold its records in exactly the order they were
/// issued: every publish to one shard goes through one writer and one
/// QUIC stream, whichever connection that stream is on.
#[tokio::test]
#[serial]
async fn spreading_a_stream_keeps_each_shards_records_in_publish_order() -> Result<()> {
    const RECORDS: usize = 400;
    let fixture = TwoShards::start().await?;
    let client = Client::connect(fixture.addr, "localhost", fixture.client_config()?).await?;
    let publisher = client.publisher().await?;
    // Several keys per shard, so keys sharing a shard interleave too.
    let keys: Vec<(u32, bytes::Bytes)> = (0..4)
        .flat_map(|nth| [(0, fixture.key_for(0, nth)), (1, fixture.key_for(1, nth))])
        .collect();
    let mut sent: [Vec<String>; 2] = Default::default();
    for i in 0..RECORDS {
        let (shard, key) = &keys[i % keys.len()];
        let payload = format!("{i:05}");
        publisher
            .publish_keyed(
                "t1",
                "default",
                "orders",
                key.clone(),
                payload.clone().into_bytes(),
                AckMode::None,
            )
            .await?;
        sent[*shard as usize].push(payload);
    }
    // An acked publish per shard, answered only after everything sent before
    // it on that shard's stream.
    for (shard, key) in keys.iter().take(2) {
        publisher
            .publish_keyed(
                "t1",
                "default",
                "orders",
                key.clone(),
                b"last".to_vec(),
                AckMode::PerMessage,
            )
            .await?;
        sent[*shard as usize].push("last".to_string());
    }

    for shard in 0..2u32 {
        let records = fixture
            .broker
            .read_durable("t1", "default", "orders", shard, 0, 16 << 20)
            .await?;
        let landed: Vec<String> = records
            .iter()
            .map(|record| String::from_utf8_lossy(&record.payload).into_owned())
            .collect();
        assert_eq!(
            landed, sent[shard as usize],
            "shard {shard} holds its records out of publish order"
        );
    }
    Ok(())
}
