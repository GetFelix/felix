use std::time::Duration;

use anyhow::Context;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;

use super::*;
use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::handlers::publish::{AckTimeoutState, build_publish_context};
use crate::test_support::leader::{self, CACHE, Leader, NAMESPACE, TENANT};

/// A connected pair; the handler is given the server side.
async fn connection() -> Result<(
    QuicClient,
    felix_transport::QuicConnection,
    felix_transport::QuicConnection,
)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let accept = tokio::spawn(async move { server.accept().await });
    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let client_config = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        client_config,
        TransportConfig::default(),
    )?;
    let client_conn = client.connect(addr, "localhost").await?;
    Ok((client, client_conn, accept.await??))
}

/// **A watch that lands after a shard's watches were ended is refused.** The
/// lifecycle closes the fence and ends the watches before this broker's
/// routes catch up, so for a moment the routes still admit a watch. One
/// registered then was never ended: it sat on a broker that no longer applies
/// the cache's writes, which looks exactly like a quiet key.
#[tokio::test]
async fn a_watch_after_the_watches_were_ended_is_refused() -> Result<()> {
    let mut fixture = Leader::start().await;
    fixture.fence_move(&leader::cache_key());
    let hub = Arc::clone(fixture.broker.cache_watches().expect("a log-backed cache"));
    hub.end_shard(TENANT, NAMESPACE, CACHE, 0, None);
    // The window: the routes still say the shard is served here.
    assert!(
        super::super::redirect::redirect_for(
            Some(&fixture.ingress),
            None,
            TENANT,
            NAMESPACE,
            CACHE,
            0,
            crate::shards::ShardKind::Cache,
            0,
        )
        .is_none()
    );

    let (_client, _client_conn, connection) = connection().await?;
    let publish_ctx = build_publish_context(
        Arc::clone(&fixture.broker),
        &BrokerConfig::default(),
        ClusterContext {
            ingress: Some(Arc::clone(&fixture.ingress)),
            ..ClusterContext::default()
        },
    );
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(parking_lot::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    handle_cache_watch_message(
        Arc::clone(&fixture.broker),
        connection,
        BrokerConfig::default(),
        &publish_ctx,
        WatchResponder {
            out_ack_tx: &out_ack_tx,
            out_ack_depth: &out_ack_depth,
            ack_throttle_tx: &ack_throttle_tx,
            ack_timeout_state: &ack_timeout_state,
            cancel_tx: &cancel_tx,
        },
        WatchRequest {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            cache: CACHE.to_string(),
            key: Some("session-1".to_string()),
            prefix: None,
            shard: None,
            from_offset: None,
            retained: false,
            subscription_id: Some(13),
        },
        0,
    )
    .await?;

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    let Outgoing::Message(Message::Error { code, .. }) = ack else {
        panic!("expected a refusal, got {ack:?}");
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::ShardUnavailable));
    assert_eq!(
        hub.registered_watchers(TENANT, NAMESPACE, CACHE, 0),
        0,
        "the refused watch left a registration behind"
    );
    Ok(())
}
