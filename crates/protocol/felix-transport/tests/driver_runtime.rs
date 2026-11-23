//! Where a connection's quinn driver runs when drivers share the app runtime.
//!
//! Its own binary: the I/O runtime pool is per process and is built on the
//! first endpoint, so the switch has to be set before any other test binds.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use quinn::{ClientConfig, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;

/// **A connection is driven on the runtime its endpoint was bound on.** quinn
/// spawns the driver from whichever task dials, so a dial from a Raft group
/// thread would otherwise leave that thread carrying the connection's I/O.
/// Here the dialling thread blocks after connecting; the connection must
/// still carry a round trip from the endpoint's runtime.
#[test]
fn a_connection_is_driven_by_its_endpoints_runtime() -> Result<()> {
    // SAFETY: set before any thread or endpoint exists in this binary.
    unsafe { std::env::set_var("FELIX_IO_RUNTIME_THREADS", "0") };
    let app = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config = ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let client_config = ClientConfig::with_root_certificates(Arc::new(roots))?;

    let (server, client) = app.block_on(async {
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let client = QuicClient::bind(
            "127.0.0.1:0".parse()?,
            client_config,
            TransportConfig::default(),
        )?;
        anyhow::Ok((server, client))
    })?;
    let addr = server.local_addr()?;
    let echo = app.spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let body = recv.read_to_end(64).await?;
        send.write_all(&body).await?;
        send.finish()?;
        connection.closed().await;
        anyhow::Ok(())
    });

    let (connected_tx, connected_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let dialler = std::thread::Builder::new()
        .name("felix-raft-group-test".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let connected = runtime.block_on(client.connect(addr, "localhost"));
            let _ = connected_tx.send(connected);
            // The runtime stays alive but is never polled again.
            let _ = release_rx.recv();
            drop(client);
        })?;
    let connection = connected_rx.recv()?.context("connect")?;

    let echoed = app.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut send, mut recv) = connection.open_bi().await?;
            send.write_all(b"ping").await?;
            send.finish()?;
            anyhow::Ok(recv.read_to_end(64).await?)
        })
        .await
    });
    release_tx.send(())?;
    dialler.join().expect("dialler");
    let echoed = echoed
        .context("no round trip while the dialling thread was blocked")?
        .context("round trip")?;
    assert_eq!(echoed, b"ping");
    connection.close(0u32.into(), b"done");
    echo.abort();
    Ok(())
}
