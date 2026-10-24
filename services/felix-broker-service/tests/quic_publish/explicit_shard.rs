//! A write that names its shard lands on that shard, whatever its key would
//! hash to; a shard the stream does not have is refused; and a client that
//! never asked for the feature sees the broker it always did.

use super::shard_streams::TwoShards;
use super::*;

const TENANT: &str = "t1";
const NAMESPACE: &str = "default";
const STREAM: &str = "orders";

async fn payloads_on(fixture: &TwoShards, shard: u32) -> Result<Vec<String>> {
    Ok(fixture
        .broker
        .read_durable(TENANT, NAMESPACE, STREAM, shard, 0, 16 << 20)
        .await?
        .iter()
        .map(|record| String::from_utf8_lossy(&record.payload).into_owned())
        .collect())
}

/// **A named shard is where the record lands.** Through a `Client`, a
/// `ClusterClient` and a conditional publish, each to the shard named.
#[tokio::test]
#[serial]
async fn a_publish_to_a_named_shard_lands_there() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let client = Client::connect(fixture.addr, "localhost", fixture.client_config()?).await?;
    let publisher = client.publisher().await?;
    let first = publisher
        .publish_batch_on_shard(
            TENANT,
            NAMESPACE,
            STREAM,
            1,
            vec![b"one".to_vec(), b"two".to_vec()],
            AckMode::PerBatch,
        )
        .await?;
    publisher
        .publish_on_shard(
            TENANT,
            NAMESPACE,
            STREAM,
            0,
            b"zero".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    // The conditional publish counts the shard's own tail: two records.
    let written = client
        .publish_if_on_shard(TENANT, NAMESPACE, STREAM, 1, vec![b"three".to_vec()], 2)
        .await?;
    assert_eq!(
        written,
        felix_client::ConditionalWrite::Written { offset: 2 }
    );
    let stale = client
        .publish_if_on_shard(TENANT, NAMESPACE, STREAM, 1, vec![b"late".to_vec()], 2)
        .await?;
    assert_eq!(stale, felix_client::ConditionalWrite::Refused { tail: 3 });

    let cluster = felix_client::ClusterClient::connect(
        &[fixture.addr],
        "localhost",
        fixture.client_config()?,
    )
    .await?;
    cluster
        .publish_on_shard(
            TENANT,
            NAMESPACE,
            STREAM,
            1,
            b"four".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    assert_eq!(first, Some(0));
    assert_eq!(
        payloads_on(&fixture, 1).await?,
        ["one", "two", "three", "four"]
    );
    assert_eq!(payloads_on(&fixture, 0).await?, ["zero"]);
    Ok(())
}

/// **A shard the stream does not have is refused**, with `invalid_request`,
/// and nothing is written to any shard.
#[tokio::test]
#[serial]
async fn a_shard_out_of_range_is_refused() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let client = Client::connect(fixture.addr, "localhost", fixture.client_config()?).await?;
    let publisher = client.publisher().await?;
    let invalid = |err: anyhow::Error| {
        err.downcast_ref::<felix_client::BrokerError>()
            .map(|err| err.code.clone())
    };
    let refused = publisher
        .publish_on_shard(
            TENANT,
            NAMESPACE,
            STREAM,
            2,
            b"lost".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("shard 2 of a two-shard stream");
    assert_eq!(
        invalid(refused),
        Some(felix_wire::ErrorCode::InvalidRequest)
    );
    let refused = client
        .publish_if_on_shard(TENANT, NAMESPACE, STREAM, 7, vec![b"lost".to_vec()], 0)
        .await
        .expect_err("shard 7 of a two-shard stream");
    assert_eq!(
        invalid(refused),
        Some(felix_wire::ErrorCode::InvalidRequest)
    );
    assert!(payloads_on(&fixture, 0).await?.is_empty());
    assert!(payloads_on(&fixture, 1).await?.is_empty());
    Ok(())
}

/// **A client that did not offer the extended features is unaffected.** It
/// is not told about the bit, and its keyed publishes route by the key as
/// they always did. A raw idempotent producer naming shards keeps one
/// sequence per named shard.
#[tokio::test]
#[serial]
async fn an_old_client_is_unaffected_and_sequences_follow_the_named_shard() -> Result<()> {
    let fixture = TwoShards::start().await?;
    let quic = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(fixture.cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = quic.connect(fixture.addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let mut exchange = async |message: Message| -> Result<Option<Message>> {
        felix_broker_service::serving::quic::write_message(&mut send, message).await?;
        felix_broker_service::serving::quic::read_message_limited(&mut recv, 1 << 20, &mut scratch)
            .await
    };

    match exchange(Message::Auth {
        tenant_id: fixture.auth.tenant_id.clone(),
        token: fixture.auth.token.clone(),
        client_flags: Some(felix_wire::ORIGINAL_V1_FLAGS),
        client_features: Some(felix_wire::FEATURE_IDEMPOTENT_PRODUCER),
        client_features_hi: None,
    })
    .await?
    {
        Some(Message::AuthOk {
            server_features_hi: None,
            ..
        }) => {}
        other => anyhow::bail!("expected an AuthOk with no extended word, got {other:?}"),
    }

    // Keyed, with no shard: the frame an older client sends, routed by hash.
    let keyed = Message::PublishBatch {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        payloads: vec![b"hashed".to_vec()],
        key: Some(fixture.key_for(1, 0)),
        shard: None,
        request_id: Some(1),
        ack: Some(AckMode::PerBatch),
    };
    let json = serde_json::to_string(&keyed)?;
    assert!(!json.contains("shard"), "{json}");
    match exchange(keyed).await? {
        Some(Message::PublishOk { request_id: 1, .. }) => {}
        other => anyhow::bail!("keyed publish: {other:?}"),
    }

    let producer_id = match exchange(Message::ProducerInit { request_id: 2 }).await? {
        Some(Message::ProducerInitOk { producer_id, .. }) => producer_id,
        other => anyhow::bail!("producer_init: {other:?}"),
    };
    // Sequence 0 on each named shard: each shard numbers its own batches, so
    // neither is a duplicate of the other. The key would hash to shard 1.
    for (request_id, shard) in [(3, 0), (4, 1)] {
        let answer = exchange(Message::PublishIdempotent {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            payloads: vec![format!("idempotent-{shard}").into_bytes()],
            key: Some(fixture.key_for(1, 0)),
            shard: Some(shard),
            request_id,
            producer_id,
            sequence: 0,
        })
        .await?;
        anyhow::ensure!(
            matches!(answer, Some(Message::PublishOk { request_id: id, .. }) if id == request_id),
            "idempotent publish to shard {shard}: {answer:?}"
        );
    }

    assert_eq!(payloads_on(&fixture, 0).await?, ["idempotent-0"]);
    assert_eq!(payloads_on(&fixture, 1).await?, ["hashed", "idempotent-1"]);
    Ok(())
}
