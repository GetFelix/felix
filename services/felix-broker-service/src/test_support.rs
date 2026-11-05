//! Test HTTP helpers to avoid hangs:
//! - strict client timeouts and no_proxy to prevent localhost hijacking
//! - readiness polling instead of sleep
//! - graceful shutdown so servers don't linger between tests

pub(crate) mod fake_link;
pub(crate) mod leader;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::{Client, Response, redirect::Policy};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub(crate) fn build_test_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(1))
        .no_proxy()
        .redirect(Policy::none())
        .build()
        .context("build test http client")
}

pub(crate) async fn wait_for_listen(addr: SocketAddr) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!("server not ready at {addr}: {err}"));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

pub(crate) fn spawn_axum_with_shutdown(
    listener: TcpListener,
    router: axum::Router,
) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let serve = axum::serve(listener, router.into_make_service());
        let _ = serve
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    (shutdown_tx, handle)
}

#[allow(dead_code)]
pub(crate) async fn get_with_context(client: &Client, url: &str, phase: &str) -> Result<Response> {
    client
        .get(url)
        .send()
        .await
        .with_context(|| format!("{phase} GET {url}"))
}

#[cfg(test)]
mod tests;
