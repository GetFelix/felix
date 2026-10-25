//! The token scenarios that need a real broker and a token that expires.
//!
//! The scenarios are the `token.*` entries in `scenarios.toml`. The rest are
//! covered by the provider's own tests in `felix-client` (`auth/tests.rs`),
//! which drive its clock and a provider that hangs without waiting 30 s.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail, ensure};
use felix_client::{Client, RefreshingToken};
use rustls::pki_types::CertificateDer;

use super::fixture::{AuthFixture, build_client_config};

pub(crate) async fn run_client_token(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running client token checks...");
    let calls = Arc::new(AtomicUsize::new(0));
    let failing = Arc::new(AtomicBool::new(false));

    // token.expiry_refresh: a token minted for 6 s is replaced once two thirds
    // of it have passed, and the replacement is held.
    let short = auth.token_living(Duration::from_secs(6))?;
    let client = connect(addr, &cert, auth, short, &calls, &failing).await?;
    open_stream(&client).await?;
    ensure!(
        calls.load(Ordering::SeqCst) == 0,
        "the provider ran while the token had most of its life"
    );
    tokio::time::sleep(Duration::from_millis(4500)).await;
    open_stream(&client).await?;
    ensure!(
        calls.load(Ordering::SeqCst) == 1,
        "a stream opened past two thirds of the token's life made {} provider calls",
        calls.load(Ordering::SeqCst)
    );
    open_stream(&client).await?;
    ensure!(
        calls.load(Ordering::SeqCst) == 1,
        "the fresh token was not held"
    );
    drop(client);

    // token.keep_valid_on_failure: a refresh that fails while the token is
    // still valid keeps it, and is tried again later, not on every open.
    calls.store(0, Ordering::SeqCst);
    failing.store(true, Ordering::SeqCst);
    let medium = auth.token_living(Duration::from_secs(9))?;
    let client = connect(addr, &cert, auth, medium, &calls, &failing).await?;
    tokio::time::sleep(Duration::from_millis(6500)).await;
    let failed_at = Instant::now();
    open_stream(&client).await?;
    open_stream(&client).await?;
    ensure!(
        calls.load(Ordering::SeqCst) == 1,
        "a failed refresh was retried on every open: {} calls",
        calls.load(Ordering::SeqCst)
    );
    failing.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(9500).saturating_sub(failed_at.elapsed())).await;
    open_stream(&client).await?;
    ensure!(
        calls.load(Ordering::SeqCst) == 2,
        "the provider was not asked again after the retry delay: {} calls",
        calls.load(Ordering::SeqCst)
    );
    Ok(())
}

/// A client holding `initial`, whose provider returns the fixture's long-lived
/// token, or fails while `failing` is set, and counts its calls.
async fn connect(
    addr: std::net::SocketAddr,
    cert: &CertificateDer<'static>,
    auth: &AuthFixture,
    initial: String,
    calls: &Arc<AtomicUsize>,
    failing: &Arc<AtomicBool>,
) -> Result<Client> {
    let fresh = auth.token.clone();
    let (calls, failing) = (Arc::clone(calls), Arc::clone(failing));
    let mut config = build_client_config(cert.clone(), auth)?;
    config.auth_token = None;
    config.token_provider = Some(Arc::new(RefreshingToken::with_initial(
        initial,
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            let result = if failing.load(Ordering::SeqCst) {
                Err(anyhow::anyhow!("idp is down"))
            } else {
                Ok(fresh.clone())
            };
            async move { result }
        },
    )));
    Client::connect(addr, "localhost", config).await
}

/// A stream that authenticates, opened and dropped.
async fn open_stream(client: &Client) -> Result<()> {
    match tokio::time::timeout(
        Duration::from_secs(15),
        client.subscribe("t1", "default", "conformance"),
    )
    .await
    {
        Ok(subscription) => {
            drop(subscription?);
            Ok(())
        }
        Err(_) => bail!("a stream did not open within 15 s"),
    }
}
