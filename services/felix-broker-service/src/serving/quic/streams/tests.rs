//! QUIC stream unit/integration tests for the broker transport.
//!
//! Cover the control/uni loops, writer/ack waiter branches, and telemetry paths
//! to ensure protocol handling and backpressure behaviors remain correct.
//!
//! - QUIC streams preserve ordering per stream and per connection.
//! - Ack/writer loops respect backpressure and timeouts.
//! - Felix tokens are EdDSA and verified via JWKS.
//!
//! - Test keys and tokens are fixtures only; do not log secrets in production.
//! - RSA is not used for Felix tokens; EdDSA is enforced.
//!
//! - Tests use local loopback and serial sections to avoid races.
//! - Many tests spawn background tasks; ordering is validated explicitly.
//!
//! Run with `cargo test -p felix-broker-service --lib serving::quic::streams`, or narrow with
//! individual test names.

mod ack_waiter;
mod control_auth;
mod control_cache;
mod control_lifecycle;
mod end_to_end;
mod error_codes;
mod frame_source;
mod idempotent_producer;
mod uni;
mod writer;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::{Bytes, BytesMut};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use felix_wire::{Frame, Message};
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio::time::timeout;

use super::ack_waiter::run_ack_waiter_loop;
use super::control::run_control_loop;
use super::frame_source::{DelayFrameSource, FrameSource, TestFrameSource};
use super::hooks::test_hooks;
use super::uni::{UniLoopArgs, run_uni_loop};
use super::writer::run_writer_loop;
use super::{handle_stream, handle_uni_stream};
use crate::config::BrokerConfig;
use crate::observability::timings;
use crate::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use crate::serving::quic::handlers::publish::{
    AckEncoding, AckTimeoutState, AckWaiterMessage, Outgoing, PublishAdmission, PublishContext,
    PublishJob, PublishTarget, SubscriptionLimiter,
};
use crate::serving::quic::handlers::subscribe::WriterLaneManager;
use crate::serving::quic::telemetry;
use crate::serving::quic::{ACK_HI_WATER, ACK_LO_WATER};

const TEST_PRIVATE_KEY: [u8; 32] = [13u8; 32];

struct AuthFixture {
    tenant_id: String,
    token: String,
    auth: Arc<BrokerAuth>,
}

fn auth_fixture(tenant_id: &str, perms: Vec<String>) -> AuthFixture {
    // Build a deterministic Ed25519 keypair and JWKS for repeatable auth tests.
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = jwks_from_public_key(&public_key, "k1");
    let mut key_materials = HashMap::new();
    key_materials.insert(
        tenant_id.to_string(),
        TenantKeyMaterial {
            kid: "k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: TEST_PRIVATE_KEY,
            public_key,
            jwks: jwks.clone(),
        },
    );
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(key_materials),
    );
    // Mint a Felix token to authenticate QUIC clients in tests.
    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:test", perms)
        .expect("mint token");

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    // Inject JWKS directly to avoid network calls in tests.
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));
    AuthFixture {
        tenant_id: tenant_id.to_string(),
        token,
        auth,
    }
}

fn jwks_from_public_key(public_key: &[u8], kid: &str) -> Jwks {
    // Encode Ed25519 public key as JWK `x` component (base64url).
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    }
}

fn default_perms() -> Vec<String> {
    // Broad permissions simplify transport tests without exercising RBAC.
    vec![
        "stream.publish:stream:t1/*/*".to_string(),
        "stream.subscribe:stream:t1/*/*".to_string(),
        "cache.read:cache:t1/*/*".to_string(),
        "cache.write:cache:t1/*/*".to_string(),
    ]
}

fn auth_message(fixture: &AuthFixture) -> Message {
    // Auth messages always carry tenant id + token.
    Message::Auth {
        tenant_id: fixture.tenant_id.clone(),
        token: fixture.token.clone(),
        // Legacy handshake: no capabilities offered, so the broker
        // answers with a plain `Ok`.
        client_flags: None,
        client_features: None,
    }
}

async fn open_authenticated_bi(
    connection: &QuicConnection,
    auth: &AuthFixture,
    max_frame_bytes: usize,
    frame_scratch: &mut BytesMut,
) -> Result<(quinn::SendStream, quinn::RecvStream)> {
    let (mut send, mut recv) = connection.open_bi().await?;
    crate::serving::quic::write_message(&mut send, auth_message(auth)).await?;
    let response =
        crate::serving::quic::read_message_limited(&mut recv, max_frame_bytes, frame_scratch)
            .await?;
    match response {
        Some(Message::Ok) => Ok((send, recv)),
        other => Err(anyhow::anyhow!("auth failed: {other:?}")),
    }
}

async fn build_publish_context(broker: Arc<Broker>) -> PublishContext {
    let (tx, mut rx) = mpsc::channel::<PublishJob>(8);
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let result = match &job.target {
                // This harness drives local publishes only; a forward would
                // need a peer transport it does not build.
                PublishTarget::Forward { .. } => unreachable!("no peers in this test"),
                PublishTarget::Resolved { handle, .. } => {
                    broker.publish_batch_to_handle(handle, &job.payloads).await
                }
                PublishTarget::Idempotent {
                    handle,
                    producer_id,
                    sequence,
                    ..
                } => broker
                    .publish_batch_idempotent(handle, *producer_id, *sequence, &job.payloads)
                    .await
                    .map(|idempotent| idempotent.outcome.subscribers),
                PublishTarget::Named {
                    tenant_id,
                    namespace,
                    stream,
                } => {
                    broker
                        .publish_batch(tenant_id, namespace, stream, 0, &job.payloads)
                        .await
                }
            }
            .map(|_| ())
            .map_err(anyhow::Error::from);
            if let Some(response) = job.response {
                let _ = response.send(result);
            }
        }
    });
    PublishContext {
        ingress: None,
        client_endpoints: None,
        peers: None,
        lease: None,
        lease_headroom: std::time::Duration::ZERO,
        marks: None,
        quorum_timeout: Duration::from_secs(1),
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(50),
        admission: Arc::new(PublishAdmission::unlimited()),
        conn_admission: Arc::new(PublishAdmission::unlimited()),
        subscriptions: Arc::new(SubscriptionLimiter::new()),
        lane_manager: WriterLaneManager::new(&BrokerConfig::default()),
        ingress_wait: false,
        preauth: std::sync::Arc::new(crate::serving::quic::preauth::PreAuthGate::new(
            &crate::config::BrokerConfig::default(),
        )),
        tenant_rates: std::sync::Arc::new(crate::serving::limits::TenantRates::unlimited()),
    }
}

fn frame_from_message(message: Message) -> Frame {
    message.encode().expect("encode message")
}

fn invalid_json_frame() -> Frame {
    Frame::new(0, Bytes::from_static(b"not-json")).expect("frame")
}

fn binary_publish_batch_frame(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Frame {
    let payloads_vec: Vec<Vec<u8>> = payloads.iter().map(|payload| payload.to_vec()).collect();
    let bytes =
        felix_wire::binary::encode_publish_batch_bytes(tenant_id, namespace, stream, &payloads_vec)
            .expect("encode binary batch");
    Frame::decode(bytes).expect("decode frame")
}

async fn run_control_loop_with_frames(
    broker: Arc<Broker>,
    auth: Arc<BrokerAuth>,
    frames: Vec<Result<Option<Frame>>>,
    config: BrokerConfig,
) -> Result<(bool, Vec<Outgoing>)> {
    run_control_loop_with_codes(broker, auth, frames, config, Default::default()).await
}

/// As `run_control_loop_with_frames`, sharing `error_codes` with the loop the
/// way the writer does, so a test can shape what it got as the writer would.
async fn run_control_loop_with_codes(
    broker: Arc<Broker>,
    auth: Arc<BrokerAuth>,
    frames: Vec<Result<Option<Frame>>>,
    config: BrokerConfig,
    error_codes: Arc<crate::serving::quic::client_error::ErrorCodeSupport>,
) -> Result<(bool, Vec<Outgoing>)> {
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        config,
        auth,
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
        error_codes,
    )
    .await?;

    let mut messages = Vec::new();
    while let Ok(message) = out_ack_rx.try_recv() {
        messages.push(message);
    }

    server_task.await.context("server task")??;
    Ok((result, messages))
}

struct PendingFrameSource {
    ready: Arc<AtomicBool>,
}

impl FrameSource for PendingFrameSource {
    fn next_frame<'a>(
        &'a mut self,
        _max_frame_bytes: usize,
        _scratch: &'a mut BytesMut,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + 'a>>
    {
        let ready = Arc::clone(&self.ready);
        Box::pin(async move {
            while !ready.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            Ok(None)
        })
    }
}

fn spawn_ack_waiter_with_closed_out_ack(
    ack_wait_timeout: Duration,
) -> (
    mpsc::Sender<AckWaiterMessage>,
    Arc<Semaphore>,
    tokio::task::JoinHandle<()>,
) {
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let handle = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        ack_wait_timeout,
    ));
    (ack_waiter_tx, Arc::new(Semaphore::new(1)), handle)
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    Ok(quinn)
}

fn build_client_config(
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<felix_client::ClientConfig> {
    let quinn = build_quinn_client_config(cert)?;
    let mut config = felix_client::ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}
