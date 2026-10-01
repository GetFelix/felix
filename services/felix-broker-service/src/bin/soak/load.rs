//! Load generators: publishers, subscribers and connection churn.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use felix_client::Client;
use felix_wire::AckMode;

use crate::fixture::{AuthFixture, BrokerHarness, NAMESPACE, STREAM, TENANT, client_config};

#[derive(Default)]
pub(crate) struct LoadStats {
    pub(crate) published: AtomicU64,
    pub(crate) publish_errors: AtomicU64,
    pub(crate) received: AtomicU64,
    pub(crate) connect_errors: AtomicU64,
}

/// Sustained publish load from `count` independent client connections.
pub(crate) async fn spawn_publishers(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    stats: Arc<LoadStats>,
    stop: Arc<AtomicBool>,
    count: usize,
    payload_bytes: usize,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let config = client_config(&harness.cert, auth)?;
        let addr = harness.addr;
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let client = match Client::connect(addr, "localhost", config).await {
                Ok(client) => client,
                Err(_) => {
                    stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let publisher = match client.publisher().await {
                Ok(publisher) => publisher,
                Err(_) => return,
            };
            let payload = vec![0xABu8; payload_bytes];
            while !stop.load(Ordering::Relaxed) {
                match publisher
                    .publish(TENANT, NAMESPACE, STREAM, payload.clone(), AckMode::None)
                    .await
                {
                    Ok(_) => {
                        stats.published.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.publish_errors.fetch_add(1, Ordering::Relaxed);
                        // Back off rather than spin on a broken connection.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                // Yield so a single publisher cannot monopolise its worker.
                tokio::task::yield_now().await;
            }
        }));
    }
    Ok(handles)
}

/// Subscribers that drain events promptly. `slow` inverts that: they subscribe
/// and then stop reading, which is what drives queue saturation and the drop
/// policy on the broker side.
pub(crate) async fn spawn_subscribers(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    stats: Arc<LoadStats>,
    stop: Arc<AtomicBool>,
    count: usize,
    slow: bool,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let config = client_config(&harness.cert, auth)?;
        let addr = harness.addr;
        let stats = Arc::clone(&stats);
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let client = match Client::connect(addr, "localhost", config).await {
                Ok(client) => client,
                Err(_) => {
                    stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let mut subscription = match client.subscribe(TENANT, NAMESPACE, STREAM).await {
                Ok(subscription) => subscription,
                Err(_) => return,
            };
            while !stop.load(Ordering::Relaxed) {
                if slow {
                    // Hold the subscription open without draining it.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                match tokio::time::timeout(Duration::from_millis(250), subscription.next_event())
                    .await
                {
                    Ok(Ok(Some(_))) => {
                        stats.received.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Ok(None)) | Ok(Err(_)) => break,
                    Err(_) => {}
                }
            }
        }));
    }
    Ok(handles)
}

/// Repeatedly connect, publish, and disconnect. This is the phase most likely to
/// surface connection, task, or file-descriptor leaks, because each cycle
/// allocates and must fully release a QUIC connection and its per-connection
/// broker state.
pub(crate) async fn run_connection_churn(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    stats: Arc<LoadStats>,
    cycles: usize,
    payload_bytes: usize,
) -> Result<()> {
    let payload = vec![0x5Au8; payload_bytes];
    for _ in 0..cycles {
        let config = client_config(&harness.cert, auth)?;
        let client = match Client::connect(harness.addr, "localhost", config).await {
            Ok(client) => client,
            Err(_) => {
                stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        if let Ok(publisher) = client.publisher().await
            && publisher
                .publish(
                    TENANT,
                    NAMESPACE,
                    STREAM,
                    payload.clone(),
                    AckMode::PerMessage,
                )
                .await
                .is_ok()
        {
            stats.published.fetch_add(1, Ordering::Relaxed);
        }
        // Dropping the client closes the connection; the broker side must
        // release its per-connection state without being told twice.
        drop(client);
    }
    Ok(())
}
