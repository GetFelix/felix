//! The deadlines are the point of this client, so they are what is tested.
use std::time::Instant;

use super::*;

/// A control plane that accepts the connection and never answers must fail the
/// request, not hang it. Without the deadline this test never finishes.
#[tokio::test]
async fn a_stalled_control_plane_times_out() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    // Accept and hold every connection open without reading or writing.
    let holder = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });

    let client = build().expect("client");
    let started = Instant::now();
    let err = client
        .get(format!("http://{addr}/v1/streams/snapshot"))
        .send()
        .await
        .expect_err("a stalled request must fail");
    assert!(err.is_timeout(), "{err}");
    assert!(
        started.elapsed() < REQUEST_TIMEOUT + Duration::from_secs(5),
        "took {:?}",
        started.elapsed(),
    );
    holder.abort();
}
