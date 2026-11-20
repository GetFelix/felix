//! A leader hint that names a dead broker costs one dial, not one per caller.

use std::time::{Duration, Instant};

use anyhow::Result;
use felix_wire::Message;

use super::connections::policy;
use super::stub_broker::StubBroker;
use crate::cluster::Client;
use crate::test_support::{build_client_config_with_overrides, build_server_config};

/// A bound UDP socket nobody reads: a QUIC handshake to it never answers.
fn silent() -> Result<(std::net::UdpSocket, std::net::SocketAddr)> {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let addr = socket.local_addr()?;
    Ok((socket, addr))
}

async fn cluster_with(connect_timeout: Duration) -> Result<(Client, StubBroker)> {
    let (server_config, cert) = build_server_config()?;
    let entry =
        StubBroker::start_with(server_config, |request_id| Message::CacheOk { request_id })?;
    let mut config = build_client_config_with_overrides(cert, 0)?;
    config.connect_timeout = connect_timeout;
    let cluster = Client::connect_with_policy(&[entry.addr], "localhost", config, policy()).await?;
    Ok((cluster, entry))
}

/// **Callers queued behind a dial of a dead broker share its failure.** Each
/// one used to dial in turn and wait out its own timeout, so four producers
/// told about the same dead leader were released one timeout apart.
#[tokio::test]
#[serial_test::serial]
async fn callers_queued_behind_a_failed_dial_do_not_dial_again() -> Result<()> {
    let timeout = Duration::from_millis(300);
    let (cluster, _entry) = cluster_with(timeout).await?;
    let (_socket, dead) = silent()?;

    let started = Instant::now();
    let results = tokio::join!(
        cluster.connect_to(dead),
        cluster.connect_to(dead),
        cluster.connect_to(dead),
        cluster.connect_to(dead),
    );
    let took = started.elapsed();

    assert!(results.0.is_err() && results.1.is_err() && results.2.is_err() && results.3.is_err());
    assert!(
        took < timeout * 3,
        "four callers took {took:?}; they dialled one after another"
    );
    Ok(())
}

/// **A leader hint is dialled for a bounded time.** The configured connect
/// timeout is tens of seconds; a hint that is stale during an election must
/// not hold the publish that long.
#[tokio::test]
#[serial_test::serial]
async fn a_leader_hint_that_does_not_answer_is_cut_off_early() -> Result<()> {
    let (cluster, _entry) = cluster_with(Duration::from_secs(30)).await?;
    let (_socket, dead) = silent()?;

    let started = Instant::now();
    let result = cluster.connect_to_leader(dead).await;

    assert!(result.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the dial took {:?}",
        started.elapsed()
    );
    Ok(())
}

/// **A hint dial never outlasts the configured connect timeout.**
#[tokio::test]
#[serial_test::serial]
async fn a_hint_dial_is_capped_by_the_configured_timeout() -> Result<()> {
    let (cluster, _entry) = cluster_with(Duration::from_millis(400)).await?;
    let (_socket, dead) = silent()?;

    let started = Instant::now();
    assert!(cluster.connect_to_leader(dead).await.is_err());

    assert!(started.elapsed() < Duration::from_millis(1500));
    Ok(())
}

/// **A failure of the short hint dial is not handed to a caller that would
/// have waited longer.** A slow but healthy broker must still be reachable by
/// the shard-owner and routing dials, which use the full timeout.
#[tokio::test]
#[serial_test::serial]
async fn a_cut_short_hint_failure_is_not_shared_with_a_full_dial() -> Result<()> {
    let (cluster, _entry) = cluster_with(Duration::from_secs(3)).await?;
    let (_socket, dead) = silent()?;

    let started = Instant::now();
    let (hint, full) = tokio::join!(cluster.connect_to_leader(dead), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let result = cluster.connect_to(dead).await;
        (result, started.elapsed())
    });

    assert!(hint.is_err());
    assert!(full.0.is_err());
    assert!(
        full.1 >= Duration::from_millis(2900),
        "the full dial gave up after {:?}, on the hint's failure",
        full.1
    );
    Ok(())
}

/// **After a hint dial times out, the next one gets the full timeout**, so a
/// leader whose handshake takes longer than the hint cap can still connect.
#[tokio::test]
#[serial_test::serial]
async fn a_hint_that_timed_out_is_dialled_with_the_full_timeout_next() -> Result<()> {
    let (cluster, _entry) = cluster_with(Duration::from_secs(3)).await?;
    let (_socket, dead) = silent()?;

    assert!(cluster.connect_to_leader(dead).await.is_err());
    let started = Instant::now();
    assert!(cluster.connect_to_leader(dead).await.is_err());

    assert!(
        started.elapsed() >= Duration::from_millis(2500),
        "the second hint dial gave up after {:?}",
        started.elapsed()
    );
    Ok(())
}

/// **A hint waiter that runs out of time in the queue does not mark the
/// broker slow.** Only a dial of its own that timed out does; otherwise every
/// later hint to a dead leader would get the full connect timeout again.
#[tokio::test]
#[serial_test::serial]
async fn a_hint_that_timed_out_in_the_queue_does_not_lengthen_the_next() -> Result<()> {
    let (cluster, _entry) = cluster_with(Duration::from_secs(4)).await?;
    let (_socket, dead) = silent()?;

    let (full, hint) = tokio::join!(cluster.connect_to(dead), async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cluster.connect_to_leader(dead).await
    });
    assert!(full.is_err() && hint.is_err());

    let started = Instant::now();
    assert!(cluster.connect_to_leader(dead).await.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the next hint dial took {:?}",
        started.elapsed()
    );
    Ok(())
}

/// **A failure the broker gave, rather than a timeout, is shared with every
/// caller**, and a timeout only with callers that would not have waited longer.
#[test]
fn only_timeouts_are_limited_to_the_callers_that_waited_as_long() {
    use super::super::nodes::failure_reach;
    let limit = Duration::from_secs(2);
    let fast = Duration::from_millis(5);
    assert_eq!(
        failure_reach("tls handshake failed", fast, limit),
        Duration::MAX
    );
    assert_eq!(
        failure_reach("connection refused", fast, limit),
        Duration::MAX
    );
    assert_eq!(
        failure_reach("establish QUIC connection: timed out", fast, limit),
        limit
    );
    assert_eq!(failure_reach("no answer", limit, limit), limit);
}

/// **A shorter dial finishing late does not narrow a longer failure.**
#[tokio::test]
#[serial_test::serial]
async fn a_late_short_failure_does_not_replace_a_longer_one() -> Result<()> {
    let (cluster, _entry) = cluster_with(Duration::from_secs(4)).await?;
    let (_socket, dead) = silent()?;
    let slot = cluster.nodes.slot(dead);
    slot.record_failure("long".into(), Duration::from_secs(4));
    slot.record_failure("short".into(), Duration::from_secs(2));
    assert_eq!(slot.tried(), Some(Duration::from_secs(4)));
    Ok(())
}
