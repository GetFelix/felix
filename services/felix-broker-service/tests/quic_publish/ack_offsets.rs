//! Publish acks that carry the offset the batch landed at, and only to a
//! client that offered `FLAG_BINARY_PUBLISH_ACK_OFFSET`.

use super::pipeline::{raw_stream, serve_orders};
use super::*;
use felix_storage::log::FsyncMode;
use felix_wire::Frame;

fn commit_acked(config: &mut felix_broker_service::config::BrokerConfig) {
    config.ack_on_commit = true;
}

/// Authenticate one raw stream offering `client_flags`, returning it with the
/// connection that keeps it open.
async fn authed_stream(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
    client_flags: Option<u16>,
) -> Result<(
    felix_transport::QuicConnection,
    quinn::SendStream,
    quinn::RecvStream,
)> {
    let (connection, mut send, mut recv) = raw_stream(addr, cert).await?;
    felix_broker_service::serving::quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags,
            client_features: None,
        },
    )
    .await?;
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let answer =
        felix_broker_service::serving::quic::read_message_limited(&mut recv, 1 << 20, &mut scratch)
            .await?;
    anyhow::ensure!(
        matches!(answer, Some(Message::Ok | Message::AuthOk { .. })),
        "auth: {answer:?}"
    );
    Ok((connection, send, recv))
}

/// Publish `payload` over JSON and return the answer's frame payload as sent.
async fn json_publish(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    request_id: u64,
) -> Result<bytes::Bytes> {
    felix_broker_service::serving::quic::write_message(
        send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            payload: b"json".to_vec(),
            key: None,
            request_id: Some(request_id),
            ack: Some(AckMode::PerMessage),
        },
    )
    .await?;
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    let frame =
        felix_broker_service::serving::quic::read_frame_limited_into(recv, 1 << 20, &mut scratch)
            .await?
            .context("no answer to publish")?;
    Ok(frame.payload)
}

/// Publish one record over the acked binary frame and return the ack frame.
async fn binary_publish(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    request_id: u64,
) -> Result<Frame> {
    let bytes = felix_wire::binary::encode_acked_publish_batch_bytes(
        request_id,
        AckMode::PerBatch,
        "t1",
        "default",
        "orders",
        &[b"binary".to_vec()],
    )?;
    send.write_all(&bytes).await?;
    let mut scratch = felix_broker_service::serving::quic::FrameScratch::new();
    felix_broker_service::serving::quic::read_frame_limited_into(recv, 1 << 20, &mut scratch)
        .await?
        .context("no ack frame")
}

/// **Each commit-acked publish reports where it landed.** Singles land one
/// after another, a batch reports its first record's offset, and the next
/// publish starts right after the batch.
#[tokio::test]
#[serial]
async fn commit_acked_publishes_report_consecutive_offsets() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_broker, addr, cert, auth, _config) =
        serve_orders(dir.path(), FsyncMode::None, commit_acked).await?;
    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let publisher = client.publisher().await?;

    let mut offsets = Vec::new();
    for payload in [b"a", b"b", b"c"] {
        let offset = publisher
            .publish(
                "t1",
                "default",
                "orders",
                payload.to_vec(),
                AckMode::PerMessage,
            )
            .await?;
        offsets.push(offset.context("a commit-acked publish reported no offset")?);
    }
    let first = offsets[0];
    assert_eq!(offsets, vec![first, first + 1, first + 2]);

    let batch = publisher
        .publish_batch(
            "t1",
            "default",
            "orders",
            vec![b"d".to_vec(), b"e".to_vec(), b"f".to_vec()],
            AckMode::PerBatch,
        )
        .await?;
    assert_eq!(batch, Some(first + 3), "a batch reports its first record");
    let next = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"g".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(next, Some(first + 6), "the next publish follows the batch");
    Ok(())
}

/// **Only a client that offered the bit is sent an offset**, over JSON and
/// binary alike. The others get exactly the frames they always got.
#[tokio::test]
#[serial]
async fn an_ack_offset_is_sent_only_to_a_client_that_offered_the_bit() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_broker, addr, cert, auth, _config) =
        serve_orders(dir.path(), FsyncMode::None, commit_acked).await?;

    let (_offered, mut send, mut recv) =
        authed_stream(addr, cert.clone(), &auth, Some(felix_wire::KNOWN_FLAGS)).await?;
    let json = json_publish(&mut send, &mut recv, 1).await?;
    let Message::PublishOk {
        request_id: 1,
        offset: Some(json_offset),
    } = Message::decode(Frame::new(0, json.clone())?)?
    else {
        anyhow::bail!("expected publish_ok with an offset: {json:?}");
    };
    let frame = binary_publish(&mut send, &mut recv, 2).await?;
    assert_eq!(
        frame.header.flags,
        felix_wire::FLAG_BINARY_PUBLISH_ACK | felix_wire::FLAG_BINARY_PUBLISH_ACK_OFFSET
    );
    let ack = felix_wire::binary::decode_publish_ack(&frame)?;
    assert_eq!((ack.request_id, ack.error), (2, None));
    assert_eq!(ack.offset, Some(json_offset + 1));

    let (_v1, mut send, mut recv) = authed_stream(
        addr,
        cert.clone(),
        &auth,
        Some(felix_wire::ORIGINAL_V1_FLAGS),
    )
    .await?;
    let json = json_publish(&mut send, &mut recv, 3).await?;
    let text = std::str::from_utf8(&json)?;
    assert!(!text.contains("offset"), "{text}");
    assert_eq!(
        Message::decode(Frame::new(0, json)?)?,
        Message::PublishOk {
            request_id: 3,
            offset: None
        }
    );

    let (_legacy, mut send, mut recv) = authed_stream(addr, cert, &auth, None).await?;
    let frame = binary_publish(&mut send, &mut recv, 4).await?;
    assert_eq!(frame.header.flags, felix_wire::FLAG_BINARY_PUBLISH_ACK);
    let ack = felix_wire::binary::decode_publish_ack(&frame)?;
    assert_eq!((ack.request_id, ack.error, ack.offset), (4, None, None));
    Ok(())
}

/// **A re-sent idempotent batch reports where the original landed**, not
/// where the log ends now. The client records that offset as its batch's
/// position, so any other answer points it at someone else's records.
#[tokio::test]
#[serial]
async fn a_re_sent_idempotent_batch_reports_the_original_offset() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (broker, addr, cert, auth, _config) =
        serve_orders(dir.path(), FsyncMode::None, commit_acked).await?;
    let client = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let producer_id = client.idempotent_producer().await?.producer_id();
    let publisher = client.publisher().await?;
    let batch = || vec![b"a".to_vec(), b"b".to_vec()];

    let original = publisher
        .publish_idempotent_batch("t1", "default", "orders", batch(), producer_id, 0)
        .await?
        .context("an idempotent publish reported no offset")?;
    let between = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"x".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(between, Some(original + 2));

    let again = publisher
        .publish_idempotent_batch("t1", "default", "orders", batch(), producer_id, 0)
        .await?;
    assert_eq!(
        again,
        Some(original),
        "a re-send reported an offset other than the original's"
    );
    assert_eq!(
        stored_orders(&broker).await?,
        vec![b"a".to_vec(), b"b".to_vec(), b"x".to_vec()]
    );
    Ok(())
}

/// **A client can ask for commit acks on a broker that does not give them to
/// everyone.** With `ClientConfig::ack_on_commit` its publishes report where
/// they landed; a client on the same broker that did not ask is answered at
/// enqueue, with no offset, as before.
#[tokio::test]
#[serial]
async fn a_client_can_ask_for_commit_acks_on_its_own_connection() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let (_broker, addr, cert, auth, _config) =
        serve_orders(dir.path(), FsyncMode::None, |_| {}).await?;

    let mut asking = build_client_config(cert.clone(), &auth)?;
    asking.ack_on_commit = true;
    let client = Client::connect(addr, "localhost", asking).await?;
    assert!(client.supports_ack_on_commit());
    let publisher = client.publisher().await?;
    let first = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"a".to_vec(),
            AckMode::PerMessage,
        )
        .await?
        .context("a client that asked for commit acks got no offset")?;
    let second = publisher
        .publish(
            "t1",
            "default",
            "orders",
            b"b".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(second, Some(first + 1));

    let plain = Client::connect(addr, "localhost", build_client_config(cert, &auth)?).await?;
    let offset = plain
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            "orders",
            b"c".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(
        offset, None,
        "a client that did not ask was acked at enqueue"
    );
    Ok(())
}
