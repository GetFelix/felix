//! A stalled shard holding up only itself: the client gives each shard its
//! own publish stream, and a broker that answers plain publishes as they
//! complete does not put one shard's answers behind another's on a shared
//! stream either. And what that must not cost: each shard's records still
//! land in the order one client sent them.

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
                    incarnation: 0,
                    fence_storage_ids: Default::default(),
                    member_storage_ids: Default::default(),
                    pending_set: None,
                    previous_generation: None,
                    readmitted: Default::default(),
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
pub(super) struct TwoShards {
    pub(super) broker: Arc<Broker>,
    pub(super) addr: std::net::SocketAddr,
    pub(super) cert: CertificateDer<'static>,
    pub(super) auth: AuthFixture,
    _dir: tempfile::TempDir,
}

impl TwoShards {
    pub(super) async fn start() -> Result<Self> {
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
        // Without a waiting reader a full shard queue sheds unacked publishes,
        // and the order test expects all 400 to land. The shallow queue makes
        // a missing wait fail every run.
        config.pub_ingress_wait = true;
        config.pub_queue_depth = 1;
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
                engine: std::sync::Arc::new(
                    felix_broker_service::shards::routing::StreamEngine::new(
                        None,
                        Some(leading_both_shards()),
                    ),
                ),
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

    pub(super) fn client_config(&self) -> Result<ClientConfig> {
        build_client_config(self.cert.clone(), &self.auth)
    }

    /// A key that routes to `shard`, the `nth` such key.
    pub(super) fn key_for(&self, shard: u32, nth: usize) -> bytes::Bytes {
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
            .claim_publish(&handle, &[bytes::Bytes::from_static(b"held")], None, None)
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
        felix_client::Client::connect(&[fixture.addr], "localhost", fixture.client_config()?)
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

/// **A plain `Connection` puts one stream's shards on separate streams too.** It
/// learns the stream's width on the first keyed publish, so a hot stream's
/// shards spread over its connections instead of sharing one stream. A
/// stalled shard holding up the other is what sharing one would look like.
#[tokio::test]
#[serial]
async fn a_plain_clients_keyed_publishes_go_on_their_shards_own_streams() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let client = Connection::connect(fixture.addr, "localhost", fixture.client_config()?).await?;
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
    let client = Connection::connect(fixture.addr, "localhost", fixture.client_config()?).await?;
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

/// Authenticate a raw pipelining stream, offering answers on completion when
/// `on_completion`, and return whether the broker answered that bit.
async fn pipelining_stream(
    fixture: &TwoShards,
    on_completion: bool,
) -> Result<(
    felix_transport::QuicConnection,
    quinn::SendStream,
    quinn::RecvStream,
    bool,
)> {
    let (connection, mut send, mut recv) =
        super::pipeline::raw_stream(fixture.addr, fixture.cert.clone()).await?;
    let (client_features, client_features_hi) = felix_wire::offer_features(
        felix_wire::FEATURE_PUBLISH_PIPELINE,
        match on_completion {
            true => felix_wire::FEATURE_PUBLISH_ANSWER_ON_COMPLETION,
            false => 0,
        },
    );
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: fixture.auth.tenant_id.clone(),
            token: fixture.auth.token.clone(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
            client_features: Some(client_features),
            client_features_hi,
            client_flags_ext: None,
        },
    )
    .await?;
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let answer =
        felix_broker_service::serving::quic::read_message_limited(&mut recv, 1 << 20, &mut scratch)
            .await?;
    let Some(Message::AuthOk {
        server_features,
        server_features_hi,
        publish_window: Some(_),
        ..
    }) = answer
    else {
        anyhow::bail!("not granted a window: {answer:?}");
    };
    let granted = felix_wire::supports_feature(
        felix_wire::peer_features_hi(server_features.unwrap_or(0), server_features_hi),
        felix_wire::FEATURE_PUBLISH_ANSWER_ON_COMPLETION,
    );
    Ok((connection, send, recv, granted))
}

fn keyed_publish(key: &bytes::Bytes, request_id: u64) -> Message {
    Message::Publish {
        shard: None,
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        payload: format!("record-{request_id}").into_bytes(),
        key: Some(key.clone()),
        request_id: Some(request_id),
        ack: Some(AckMode::PerMessage),
    }
}

async fn next_answer(recv: &mut quinn::RecvStream, wait: Duration) -> Result<u64> {
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    match timeout(
        wait,
        felix_broker_service::serving::quic::read_message_limited(recv, 1 << 20, &mut scratch),
    )
    .await
    .context("no answer in time")??
    {
        Some(Message::PublishOk { request_id, .. }) => Ok(request_id),
        other => anyhow::bail!("expected publish_ok, got {other:?}"),
    }
}

/// **A plain publish is answered as it completes.** A client that offered
/// `FEATURE_PUBLISH_ANSWER_ON_COMPLETION` sends a publish to a stalled shard
/// and then one to a healthy shard on the same stream. The second is
/// answered while the first still waits. A client that did not offer the
/// bit is not told about it.
#[tokio::test]
#[serial]
async fn a_plain_publish_is_answered_ahead_of_a_stalled_one_on_its_stream() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let (_old, _, _, granted) = pipelining_stream(&fixture, false).await?;
    assert!(!granted, "answered a bit the client did not offer");

    let (_connection, mut send, mut recv, granted) = pipelining_stream(&fixture, true).await?;
    assert!(
        granted,
        "the bit was offered with a window and not answered"
    );
    let (stalled_key, healthy_key) = (fixture.key_for(1, 0), fixture.key_for(0, 0));
    let held = fixture.stall_shard_one().await?;
    felix_broker_service::serving::quic::write_message(&mut send, keyed_publish(&stalled_key, 1))
        .await?;
    felix_broker_service::serving::quic::write_message(&mut send, keyed_publish(&healthy_key, 2))
        .await?;
    let first = next_answer(&mut recv, Duration::from_secs(2))
        .await
        .context("the healthy shard's answer was held behind the stalled one")?;
    assert_eq!(first, 2);

    fixture.broker.complete_publish(held).await?;
    assert_eq!(next_answer(&mut recv, Duration::from_secs(5)).await?, 1);
    Ok(())
}

/// **One client stream, a stalled shard, and the others keep going.** Every
/// shard shares one publish stream here. The writer keeps sending while the
/// stalled shard's answer is out, and the broker answers the others as they
/// complete, so they are not held behind it.
#[tokio::test]
#[serial]
async fn a_stalled_shard_does_not_hold_up_its_stream_mates_on_one_client_stream() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let mut config = fixture.client_config()?;
    config.publish_conn_pool = 1;
    config.publish_streams_per_conn = 1;
    config.publish_shard_streams = 0;
    let client = Connection::connect(fixture.addr, "localhost", config).await?;
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
        let round = futures::future::try_join_all((0..8).map(|_| publish(healthy_key.clone())));
        timeout(Duration::from_secs(2), round)
            .await
            .context("shard 0 was held up behind the stalled shard 1")??;
    }

    fixture.broker.complete_publish(held).await?;
    timeout(Duration::from_secs(5), stalled)
        .await
        .context("shard 1 did not recover")???;
    Ok(())
}

/// Read one publish answer, binary or JSON: its request id and whether it
/// was a success. `scratch` may hold the start of the next frame, so a
/// stream's reads share one.
async fn next_any_answer(
    recv: &mut quinn::RecvStream,
    scratch: &mut felix_broker_service::serving::quic::FrameScratch,
) -> Result<(u64, bool)> {
    let frame =
        felix_broker_service::serving::quic::read_frame_limited_into(recv, 1 << 20, scratch)
            .await?
            .context("the broker closed the stream")?;
    if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACK != 0 {
        let ack = felix_wire::binary::decode_publish_ack(&frame)?;
        return Ok((ack.request_id, ack.error.is_none()));
    }
    match Message::decode(frame)? {
        Message::PublishOk { request_id, .. } => Ok((request_id, true)),
        Message::PublishError { request_id, .. } | Message::PublishRefused { request_id, .. } => {
            Ok((request_id, false))
        }
        other => anyhow::bail!("expected a publish answer, got {other:?}"),
    }
}

/// **Idempotent batches keep request order on a stream that answers on
/// completion.** A producer must see batch n fail before a `sequence_gap` for
/// batch n+1, so a binary idempotent batch to a healthy shard waits for the
/// one sent before it to a stalled shard.
#[tokio::test]
#[serial]
async fn idempotent_batches_keep_request_order_on_a_stream_that_answers_on_completion() -> Result<()>
{
    let fixture = TwoShards::start().await?;
    let (_connection, mut send, mut recv, granted) = pipelining_stream(&fixture, true).await?;
    assert!(granted);
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let mut producers = Vec::new();
    for request_id in [100, 101] {
        felix_broker_service::serving::quic::write_message(
            &mut send,
            Message::ProducerInit { request_id },
        )
        .await?;
        match felix_broker_service::serving::quic::read_message_limited(
            &mut recv,
            1 << 20,
            &mut scratch,
        )
        .await?
        {
            Some(Message::ProducerInitOk { producer_id, .. }) => producers.push(producer_id),
            other => anyhow::bail!("expected producer_init_ok, got {other:?}"),
        }
    }
    let batch = |request_id: u64, key: &bytes::Bytes| {
        felix_wire::binary::encode_idempotent_publish_batch_bytes(
            request_id,
            felix_wire::binary::ProducerSequence {
                producer_id: producers[(request_id - 1) as usize],
                sequence: 0,
            },
            Some(key),
            "t1",
            "default",
            "orders",
            &[format!("record-{request_id}").into_bytes()],
        )
    };
    let (stalled_key, healthy_key) = (fixture.key_for(1, 0), fixture.key_for(0, 0));
    let held = fixture.stall_shard_one().await?;
    send.write_all(&batch(1, &stalled_key)?).await?;
    send.write_all(&batch(2, &healthy_key)?).await?;
    // Batch 2 commits at once, but its answer waits for batch 1's.
    tokio::time::sleep(Duration::from_millis(300)).await;
    fixture.broker.complete_publish(held).await?;
    let first = timeout(
        Duration::from_secs(5),
        next_any_answer(&mut recv, &mut scratch),
    )
    .await??;
    let second = timeout(
        Duration::from_secs(5),
        next_any_answer(&mut recv, &mut scratch),
    )
    .await??;
    assert_eq!((first.0, second.0), (1, 2));
    assert!(first.1 && second.1, "{first:?} {second:?}");
    Ok(())
}
