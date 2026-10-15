use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::post;

/// Answers each connection with the next of `answers` and counts requests.
async fn scripted(answers: Vec<(&'static str, &'static str)>) -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);
    tokio::spawn(async move {
        for (status, body) in answers {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 8192];
            let _ = socket.read(&mut request).await;
            counter.fetch_add(1, Ordering::SeqCst);
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
        }
    });
    (format!("http://{addr}/v1/tenants"), seen)
}

const BUSY: (&str, &str) = (
    "503 Service Unavailable",
    r#"{"code":"busy","message":"x"}"#,
);
const UNAVAILABLE: (&str, &str) = (
    "503 Service Unavailable",
    r#"{"code":"unavailable","message":"x"}"#,
);

#[tokio::test]
async fn seeding_retries_busy_but_not_unavailable() {
    let http = reqwest::Client::new();

    let (url, seen) = scripted(vec![BUSY, ("201 Created", "{}")]).await;
    post(&http, &url, "t", json!({})).await.expect("retried");
    assert_eq!(seen.load(Ordering::SeqCst), 2);

    let (url, seen) = scripted(vec![UNAVAILABLE, ("201 Created", "{}")]).await;
    post(&http, &url, "t", json!({}))
        .await
        .expect_err("a Raft 503 may have committed");
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}
