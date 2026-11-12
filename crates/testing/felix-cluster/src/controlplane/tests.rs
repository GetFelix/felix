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

fn report(generation: u64, caught_up: &[&str]) -> felix_controlplane_service::model::ReplicaReport {
    serde_json::from_value(serde_json::json!({
        "key": {"tenant_id": "t", "namespace": "n", "stream": "s", "shard": 0},
        "generation": generation,
        "caught_up": caught_up,
        "offsets": {},
        "reported_at_millis": 0,
    }))
    .expect("report")
}

#[test]
fn a_shard_is_reported_only_when_every_replica_is_caught_up_at_its_generation() {
    use super::replica_report_gap as gap;
    let replicas = ["b".to_string(), "c".to_string()];
    assert!(gap(3, &replicas, None).is_some(), "no report yet");
    assert!(gap(3, &replicas, Some(&report(2, &["b", "c"]))).is_some());
    assert!(gap(3, &replicas, Some(&report(3, &["b"]))).is_some());
    assert!(gap(3, &replicas, Some(&report(3, &["b", "c"]))).is_none());
}

/// A metadata group keeps answering at its one address while a member is
/// down, and its members agree again once everything is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_metadata_group_survives_losing_its_leader_and_converges() {
    let control_plane = ControlPlane::start_group("tenant-a", super::LIVENESS)
        .await
        .expect("start the group");
    let group = control_plane.group().expect("a group").clone();
    let base_url = control_plane.base_url.clone();

    let killed = group.kill_leader().await.expect("kill the leader");
    // A survivor leads within the election timeout, and the front reaches it.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if group.leader().await.is_some_and(|leader| leader != killed) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no new leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = answers(&base_url).join().expect("request thread");
    assert!(response.expect("answered").starts_with("HTTP/1.1 "));

    // Restart the killed member so the group has three again, then cut the
    // leader off.
    group.restart(killed).await.expect("restart");
    let isolated = group.isolate_leader().await.expect("isolate the leader");
    // The other two elect a new leader without it, and the group takes a
    // write while the old leader is still cut off.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if group
            .leader()
            .await
            .is_some_and(|leader| leader != isolated)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no leader without {isolated}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    group
        .acknowledge_write()
        .await
        .expect("a write while the leader is cut off");
    group.heal_partition();
    group
        .converge(Duration::from_secs(60))
        .await
        .unwrap_or_else(|err| panic!("members disagree after isolating {isolated}: {err:#}"));
    control_plane.shutdown().await;
}

/// The harness goes through a running member, not the one it first held:
/// member 0 restarted must not leave it writing to a handle that is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_writes_survive_the_first_member_restarting() {
    let control_plane = ControlPlane::start_group("tenant-a", super::LIVENESS)
        .await
        .expect("start the group");
    let group = control_plane.group().expect("a group").clone();
    group.kill(0).await.expect("kill member 0");
    group.restart(0).await.expect("restart member 0");
    group
        .converge(Duration::from_secs(30))
        .await
        .expect("converge");
    control_plane
        .seed_tenant("tenant-b")
        .await
        .expect("a write after member 0 restarted");
    control_plane
        .unreported_shards()
        .await
        .expect("a read after member 0 restarted");
    control_plane.shutdown().await;
}

/// A write the group acknowledged and then lost fails the convergence check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn converge_fails_when_an_acknowledged_write_is_missing() {
    let control_plane = ControlPlane::start_group("tenant-a", super::LIVENESS)
        .await
        .expect("start the group");
    let group = control_plane.group().expect("a group").clone();
    group.acknowledge_write().await.expect("a real write");
    group
        .converge(Duration::from_secs(30))
        .await
        .expect("a held write converges");
    group.claim_acknowledged("never-written");
    let err = group
        .converge(Duration::from_secs(3))
        .await
        .expect_err("a lost write must not converge");
    assert!(
        format!("{err:#}").contains("lost the acknowledged write"),
        "{err:#}"
    );
    control_plane.shutdown().await;
}
