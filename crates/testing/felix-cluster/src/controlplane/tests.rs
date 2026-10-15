use std::io::{Read, Write};
use std::time::Duration;

use super::ControlPlane;

/// One HTTP request from a plain thread, so the caller can block its own
/// runtime while it runs.
fn answers(base_url: &str) -> std::thread::JoinHandle<std::io::Result<String>> {
    let addr = base_url.trim_start_matches("http://").to_string();
    std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(&addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        write!(
            stream,
            "GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
        )?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    })
}

// The test's runtime is single-threaded and blocked for the whole request: the
// control plane answers only because it does not run there.
#[tokio::test]
async fn answers_while_the_test_runtime_is_blocked() {
    let control_plane = ControlPlane::start("tenant-a").await.expect("start");
    let response = answers(&control_plane.base_url)
        .join()
        .expect("request thread")
        .expect("control plane answered");
    assert!(response.starts_with("HTTP/1.1 "), "got {response:?}");
    control_plane.shutdown().await;
}

#[tokio::test]
async fn restarts_on_the_same_address_and_drops_inside_async_code() {
    let control_plane = ControlPlane::start("tenant-a").await.expect("start");
    let base_url = control_plane.base_url.clone();
    let control_plane = control_plane
        .restart(Duration::from_millis(10))
        .await
        .expect("restart");
    assert_eq!(control_plane.base_url, base_url);
    control_plane.run_placement(Duration::from_millis(50));
    let response = answers(&base_url).join().expect("request thread");
    assert!(response.expect("answered").starts_with("HTTP/1.1 "));
    // Dropped, not shut down, as a cluster dropped by a failing test is.
    drop(control_plane);
}
