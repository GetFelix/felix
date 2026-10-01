//! Publish acks that say where the batch landed
//! (`FLAG_BINARY_PUBLISH_ACK_OFFSET`), by hand-built frames.
//!
//! The scenarios are the `ack.*offset*` entries in `scenarios.toml`. Only an
//! ack sent after the write has an offset to report, so the checks run on a
//! broker of their own that acks on commit. The suite's broker acks plain
//! publishes on enqueue, and is checked for reporting none there.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::BrokerConfig;
use felix_broker_service::serving::quic;
use felix_storage::EphemeralCache;
use felix_storage::log::LogConfig;
use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use felix_wire::binary::{self, ProducerSequence};
use felix_wire::{AckMode, FLAG_BINARY_PUBLISH_ACK_OFFSET, Frame, KNOWN_FLAGS, Message};
use quinn::{RecvStream, SendStream};

use super::MAX_TEST_FRAME_BYTES;
use super::checks::{
    ensure_flag_advertised, ensure_offset_follows, ensure_same_frame, publish_ack_offset,
};
use super::commit::COMMIT_STREAM;
use super::fixture::{AuthFixture, build_quinn_client_config, build_server_config};
use super::frames::read_frame;

/// `suite` is the suite's own connection, to a broker that acks on commit
/// only if `suite_acks_on_commit`.
pub(crate) async fn run_ack_offsets(
    suite: &QuicConnection,
    auth: &AuthFixture,
    suite_acks_on_commit: bool,
) -> Result<()> {
    println!("Running publish ack offset checks...");
    if !suite_acks_on_commit {
        // Acked on enqueue there is no offset yet, and none may be made up.
        let mut stream = Stream::open(suite, auth, KNOWN_FLAGS).await?;
        let offset = stream.acked_binary(b"early".to_vec()).await?;
        if offset.is_some() {
            return Err(anyhow!(
                "a publish acked on enqueue reported offset {offset:?}"
            ));
        }
        stream.send.finish()?;
    }

    let (connection, server_task, _storage) = serve_commit_acked(auth).await?;
    let result = async {
        offered(&connection, auth).await?;
        not_offered(&connection, auth).await
    }
    .await;
    server_task.abort();
    result
}

/// A durable broker that answers every acked publish after writing it.
async fn serve_commit_acked(
    auth: &AuthFixture,
) -> Result<(
    QuicConnection,
    tokio::task::JoinHandle<Result<()>>,
    tempfile::TempDir,
)> {
    let storage_dir = tempfile::tempdir().context("create a storage directory")?;
    let storage = DurableStorage::open(storage_dir.path(), LogConfig::default())?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant(&auth.tenant_id).await?;
    broker
        .register_namespace(&auth.tenant_id, "default")
        .await?;
    broker
        .register_stream(
            &auth.tenant_id,
            "default",
            COMMIT_STREAM,
            StreamMetadata {
                durable: true,
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
    let config = BrokerConfig {
        ack_on_commit: true,
        ..BrokerConfig::from_env()?
    };
    let server_task = tokio::spawn(quic::serve(
        server,
        broker,
        config,
        Arc::clone(&auth.broker_auth),
    ));
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    Ok((connection, server_task, storage_dir))
}

/// A client that offered the bit gets the offset, and the offsets agree with
/// each other and with the log.
async fn offered(connection: &QuicConnection, auth: &AuthFixture) -> Result<()> {
    let mut stream = Stream::open(connection, auth, KNOWN_FLAGS).await?;
    // ack.offset_negotiated
    ensure_flag_advertised(stream.auth_response.take(), FLAG_BINARY_PUBLISH_ACK_OFFSET)?;

    // ack.carries_the_offset: a two-record batch, then one more right behind
    // it, as a binary ack and as a JSON publish_ok.
    let first = stream
        .acked_binary_batch(&[b"a".to_vec(), b"b".to_vec()])
        .await?
        .ok_or_else(|| anyhow!("a publish acked on commit had no offset"))?;
    let next = ensure_offset_follows(
        "the publish after a two-record batch",
        first,
        2,
        stream.acked_json(b"c".to_vec()).await?,
    )?;

    // An idempotent batch is answered after the write whatever the broker's
    // ack policy.
    let producer_id = stream.producer_init().await?;
    let sequenced = ensure_offset_follows(
        "an idempotent batch",
        next,
        1,
        stream
            .idempotent_binary(producer_id, 0, &[b"d".to_vec(), b"e".to_vec()])
            .await?,
    )?;
    ensure_offset_follows(
        "a JSON idempotent batch",
        sequenced,
        2,
        stream
            .idempotent_json(producer_id, 1, b"f".to_vec())
            .await?,
    )?;
    // ack.duplicate_reports_the_original_offset
    let again = stream
        .idempotent_binary(producer_id, 0, &[b"d".to_vec(), b"e".to_vec()])
        .await?;
    if again != Some(sequenced) {
        return Err(anyhow!(
            "a re-sent batch reported offset {again:?}, not {sequenced} where it landed"
        ));
    }
    stream.send.finish()?;
    Ok(())
}

/// ack.offset_negotiated: a client that did not offer the bit gets the frames
/// it always did, byte for byte.
async fn not_offered(connection: &QuicConnection, auth: &AuthFixture) -> Result<()> {
    let mut stream = Stream::open(
        connection,
        auth,
        KNOWN_FLAGS & !FLAG_BINARY_PUBLISH_ACK_OFFSET,
    )
    .await?;
    let (request_id, frame) = stream.acked_binary_frame(&[b"old".to_vec()]).await?;
    let expected = Frame::decode(binary::encode_publish_ack_bytes(request_id, None)?)?;
    ensure_same_frame(&frame, &expected, "binary ack without the offset flag")?;

    let (request_id, frame) = stream.acked_json_frame(b"old".to_vec()).await?;
    let plain_ok = |request_id| {
        Message::PublishOk {
            request_id,
            offset: None,
        }
        .encode()
    };
    ensure_same_frame(
        &frame,
        &plain_ok(request_id)?,
        "publish_ok without the offset flag",
    )?;

    let producer_id = stream.producer_init().await?;
    let request_id = stream.next_request_id();
    stream
        .write_bytes(binary::encode_idempotent_publish_batch_bytes(
            request_id,
            ProducerSequence {
                producer_id,
                sequence: 0,
            },
            None,
            &stream.tenant,
            "default",
            COMMIT_STREAM,
            &[b"old".to_vec()],
        )?)
        .await?;
    let frame = stream.read().await?;
    ensure_same_frame(
        &frame,
        &plain_ok(request_id)?,
        "idempotent ack without the offset flag",
    )?;
    stream.send.finish()?;
    Ok(())
}

/// One authenticated bi stream and the request ids sent on it.
struct Stream {
    tenant: String,
    send: SendStream,
    recv: RecvStream,
    auth_response: Option<Message>,
    request_id: u64,
}

impl Stream {
    async fn open(
        connection: &QuicConnection,
        auth: &AuthFixture,
        client_flags: u16,
    ) -> Result<Self> {
        let (mut send, mut recv) = connection.open_bi().await?;
        quic::write_message(
            &mut send,
            Message::Auth {
                tenant_id: auth.tenant_id.clone(),
                token: auth.token.clone(),
                client_flags: Some(client_flags),
                client_features: None,
            },
        )
        .await?;
        let mut scratch = quic::FrameScratch::new();
        let auth_response =
            quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await?;
        if !matches!(auth_response, Some(Message::AuthOk { .. })) {
            return Err(anyhow!(
                "auth was not answered with auth_ok: {auth_response:?}"
            ));
        }
        Ok(Self {
            tenant: auth.tenant_id.clone(),
            send,
            recv,
            auth_response,
            request_id: 0,
        })
    }

    fn next_request_id(&mut self) -> u64 {
        self.request_id += 1;
        self.request_id
    }

    async fn write(&mut self, message: Message) -> Result<()> {
        quic::write_message(&mut self.send, message).await
    }

    async fn write_bytes(&mut self, bytes: bytes::Bytes) -> Result<()> {
        self.send.write_all(&bytes).await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<Frame> {
        read_frame(&mut self.recv)
            .await?
            .ok_or_else(|| anyhow!("the stream ended before the answer"))
    }

    async fn producer_init(&mut self) -> Result<u64> {
        let request_id = self.next_request_id();
        self.write(Message::ProducerInit { request_id }).await?;
        match Message::decode(self.read().await?)? {
            Message::ProducerInitOk {
                request_id: answered,
                producer_id,
            } if answered == request_id => Ok(producer_id),
            other => Err(anyhow!("producer_init was not answered: {other:?}")),
        }
    }

    async fn idempotent_binary(
        &mut self,
        producer_id: u64,
        sequence: u64,
        payloads: &[Vec<u8>],
    ) -> Result<Option<u64>> {
        let request_id = self.next_request_id();
        self.write_bytes(binary::encode_idempotent_publish_batch_bytes(
            request_id,
            ProducerSequence {
                producer_id,
                sequence,
            },
            None,
            &self.tenant,
            "default",
            COMMIT_STREAM,
            payloads,
        )?)
        .await?;
        publish_ack_offset(self.read().await?, request_id)
    }

    async fn idempotent_json(
        &mut self,
        producer_id: u64,
        sequence: u64,
        payload: Vec<u8>,
    ) -> Result<Option<u64>> {
        let request_id = self.next_request_id();
        self.write(Message::PublishIdempotent {
            tenant_id: self.tenant.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            payloads: vec![payload],
            key: None,
            request_id,
            producer_id,
            sequence,
        })
        .await?;
        publish_ack_offset(self.read().await?, request_id)
    }

    async fn acked_binary(&mut self, payload: Vec<u8>) -> Result<Option<u64>> {
        self.acked_binary_batch(&[payload]).await
    }

    async fn acked_binary_batch(&mut self, payloads: &[Vec<u8>]) -> Result<Option<u64>> {
        let (request_id, frame) = self.acked_binary_frame(payloads).await?;
        publish_ack_offset(frame, request_id)
    }

    async fn acked_binary_frame(&mut self, payloads: &[Vec<u8>]) -> Result<(u64, Frame)> {
        let request_id = self.next_request_id();
        self.write_bytes(binary::encode_acked_publish_batch_bytes_keyed(
            request_id,
            AckMode::PerBatch,
            None,
            &self.tenant,
            "default",
            COMMIT_STREAM,
            payloads,
        )?)
        .await?;
        Ok((request_id, self.read().await?))
    }

    async fn acked_json(&mut self, payload: Vec<u8>) -> Result<Option<u64>> {
        let (request_id, frame) = self.acked_json_frame(payload).await?;
        publish_ack_offset(frame, request_id)
    }

    async fn acked_json_frame(&mut self, payload: Vec<u8>) -> Result<(u64, Frame)> {
        let request_id = self.next_request_id();
        self.write(Message::Publish {
            tenant_id: self.tenant.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            payload,
            key: None,
            request_id: Some(request_id),
            ack: Some(AckMode::PerMessage),
        })
        .await?;
        Ok((request_id, self.read().await?))
    }
}
