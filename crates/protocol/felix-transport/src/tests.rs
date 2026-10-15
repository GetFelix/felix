//! End-to-end: real endpoints on loopback, with a self-signed certificate.

mod idle_reset;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use quinn::{ClientConfig, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

use crate::{QuicClient, QuicServer, TransportConfig};

fn make_server_config() -> Result<(ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])
        .context("generate self-signed cert")?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config = ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
        .context("build server config")?;
    Ok((server_config, cert_der))
}

fn make_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert).context("add root cert")?;
    Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
}

/// **A connection with nothing to say must stay up.**
///
/// A subscription to a quiet stream sends nothing in either direction, so
/// without a keep-alive QUIC closes it on the idle timer and the subscriber
/// is disconnected while perfectly healthy. Run against a deliberately tiny
/// idle window so the test is seconds rather than a minute.
#[tokio::test]
async fn an_idle_connection_survives_with_a_keep_alive() -> Result<()> {
    let idle = Duration::from_millis(600);
    let transport = TransportConfig {
        max_idle_timeout: Some(idle),
        keep_alive_interval: Some(idle / 4),
        ..TransportConfig::default()
    };

    let (server_config, cert) = make_server_config()?;
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let accepted = tokio::spawn(async move { server.accept().await });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let server_side = accepted.await??;

    // Several idle windows with no traffic at all.
    tokio::time::sleep(idle * 5).await;

    assert!(
        connection.close_reason().is_none(),
        "an idle connection was closed despite a keep-alive: {:?}",
        connection.close_reason(),
    );
    assert!(server_side.close_reason().is_none());
    Ok(())
}

/// The same connection without a keep-alive, to show the first test is
/// asserting something. This is the behaviour every Felix client had.
#[tokio::test]
async fn an_idle_connection_dies_without_a_keep_alive() -> Result<()> {
    let idle = Duration::from_millis(600);
    let transport = TransportConfig {
        max_idle_timeout: Some(idle),
        keep_alive_interval: None,
        ..TransportConfig::default()
    };

    let (server_config, cert) = make_server_config()?;
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let accepted = tokio::spawn(async move { server.accept().await });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let _server_side = accepted.await??;

    tokio::time::sleep(idle * 5).await;

    assert!(
        connection.close_reason().is_some(),
        "expected the idle timeout to close this connection",
    );
    Ok(())
}

#[tokio::test]
async fn quic_smoke_test() -> Result<()> {
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let buf = recv.read_to_end(1024).await?;
        send.write_all(&buf).await?;
        send.finish()?;
        send.stopped().await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    assert_eq!(connection.info().peer_addr, addr);
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(b"ping").await?;
    send.finish()?;
    let response = recv.read_to_end(1024).await?;
    assert_eq!(response, b"ping");

    server_task.await.context("server task join")??;
    Ok(())
}

/// Minimal reproduction of the delivery pattern Felix's subscription path
/// uses: a sender writing many small chunks to one uni stream, and a
/// reader colocated with the connection's drivers forwarding each item
/// through a bounded channel. Isolating quinn's drivers onto a dedicated
/// runtime lost a wakeup here on Linux — the receiver ACKed bytes it never
/// delivered to the application — so this covers that path without the
/// broker or client in the picture.
#[tokio::test]
async fn many_small_frames_reach_a_colocated_reader() -> Result<()> {
    const N: u64 = 400;
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut send = connection.open_uni().await?;
        for i in 0..N {
            send.write_all(&i.to_be_bytes()).await?;
            // One write per item, as the delivery writer does.
            tokio::task::yield_now().await;
        }
        send.finish()?;
        let _ = send.stopped().await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let mut recv = connection.accept_uni().await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);
    connection.spawn_pump(async move {
        let mut buf = [0u8; 8];
        loop {
            if recv.read_exact(&mut buf).await.is_err() {
                break;
            }
            if tx.send(u64::from_be_bytes(buf)).await.is_err() {
                break;
            }
        }
    });

    for expected in 0..N {
        let value = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .with_context(|| format!("delivery stalled waiting for item {expected}"))?
            .with_context(|| format!("stream ended early at item {expected}"))?;
        assert_eq!(value, expected);
    }

    server_task.await.context("server task join")??;
    Ok(())
}

/// Same delivery pattern, but the reader runs on the application runtime
/// while the connection's drivers run on the dedicated I/O runtime — the
/// arrangement every broker-side stream reader uses. This is the
/// configuration that stalls on Linux.
#[tokio::test]
async fn many_small_frames_reach_an_app_runtime_reader() -> Result<()> {
    const N: u64 = 400;
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut send = connection.open_uni().await?;
        for i in 0..N {
            send.write_all(&i.to_be_bytes()).await?;
            tokio::task::yield_now().await;
        }
        send.finish()?;
        let _ = send.stopped().await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let mut recv = connection.accept_uni().await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);
    // Plain spawn: application runtime, not the connection's I/O runtime.
    tokio::spawn(async move {
        let mut buf = [0u8; 8];
        loop {
            if recv.read_exact(&mut buf).await.is_err() {
                break;
            }
            if tx.send(u64::from_be_bytes(buf)).await.is_err() {
                break;
            }
        }
    });

    for expected in 0..N {
        let value = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .with_context(|| format!("delivery stalled waiting for item {expected}"))?
            .with_context(|| format!("stream ended early at item {expected}"))?;
        assert_eq!(value, expected);
    }

    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn quic_uni_stream_smoke() -> Result<()> {
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut recv = connection.accept_uni().await?;
        let buf = recv.read_to_end(1024).await?;
        Result::<Vec<u8>>::Ok(buf)
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let mut send = connection.open_uni().await?;
    send.write_all(b"uni").await?;
    send.finish()?;

    let received = server_task.await.context("server task join")??;
    assert_eq!(received, b"uni");
    Ok(())
}

#[tokio::test]
async fn quic_server_local_addr() -> Result<()> {
    let (server_config, _cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport)?;
    let addr = server.local_addr()?;
    assert_eq!(addr.ip().to_string(), "127.0.0.1");
    assert!(addr.port() > 0);
    Ok(())
}
