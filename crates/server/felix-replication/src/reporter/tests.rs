//! What batching must not change, and the one thing it does.
use std::sync::Arc;

use parking_lot::Mutex;

use super::*;

/// A control plane that records how many shards arrived in each request.
fn counting_control_plane(
    answer: axum::http::StatusCode,
    delay: std::time::Duration,
) -> (
    ReportTo,
    Arc<Mutex<Vec<usize>>>,
    tokio::task::JoinHandle<()>,
) {
    let sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&sizes);
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(move |body: axum::Json<serde_json::Value>| {
            let seen = Arc::clone(&seen);
            async move {
                let count = body
                    .0
                    .get("shards")
                    .and_then(|shards| shards.as_array())
                    .map_or(0, |shards| shards.len());
                tokio::time::sleep(delay).await;
                seen.lock().push(count);
                answer
            }
        }),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let listener = tokio::net::TcpListener::from_std(listener).expect("adopt");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    (
        ReportTo {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            node_id: "broker-a".to_string(),
            token: None,
            incarnation: 0,
        },
        sizes,
        server,
    )
}

fn report(stream: &str) -> ShardReport {
    ShardReport {
        key: felix_router::ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: stream.to_string(),
            shard: 0,
            kind: felix_router::ShardKind::Stream,
        },
        generation: 4,
        caught_up: vec!["broker-b".to_string()],
        offsets: vec![("broker-b".to_string(), 10)],
        drained: false,
        tail: 0,
    }
}

/// **A pass's reports share a request.**
///
/// One POST per shard put a control-plane round trip on the quorum path for
/// every shard a broker leads, and the endpoint has always taken a list (#479).
#[tokio::test(flavor = "multi_thread")]
async fn reports_submitted_together_travel_together() {
    // Slow enough that everything submitted behind the first request is still
    // waiting when it returns, which is the case batching exists for.
    let (to, sizes, server) = counting_control_plane(
        axum::http::StatusCode::NO_CONTENT,
        std::time::Duration::from_millis(150),
    );
    let shutdown = CancellationToken::new();
    let (reporter, task) = Reporter::spawn(to, shutdown.clone());

    let sent: Vec<_> = (0..8)
        .map(|i| {
            let reporter = reporter.clone();
            tokio::spawn(async move { reporter.send(report(&format!("s{i}"))).await })
        })
        .collect();
    for handle in sent {
        assert!(handle.await.expect("join"), "a report did not land");
    }

    let sizes = sizes.lock().clone();
    assert_eq!(
        sizes.iter().sum::<usize>(),
        8,
        "every report has to reach the control plane: {sizes:?}",
    );
    assert!(
        sizes.len() < 8,
        "eight reports still cost eight requests, so nothing was batched: {sizes:?}",
    );

    shutdown.cancel();
    let _ = task.await;
    server.abort();
}

/// A lone report is not held for company.
///
/// The flush takes what is queued and sends it — there is no window to wait
/// out, because the broker least able to spare added latency on a `Quorum`
/// publish is the one with a single shard to report.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_report_is_not_delayed_waiting_for_others() {
    let (to, sizes, server) = counting_control_plane(
        axum::http::StatusCode::NO_CONTENT,
        std::time::Duration::ZERO,
    );
    let shutdown = CancellationToken::new();
    let (reporter, task) = Reporter::spawn(to, shutdown.clone());

    let started = std::time::Instant::now();
    assert!(reporter.send(report("only")).await);
    assert!(
        started.elapsed() < std::time::Duration::from_millis(100),
        "a single report waited {:?}, so something is batching on a timer",
        started.elapsed(),
    );
    assert_eq!(sizes.lock().clone(), vec![1]);

    shutdown.cancel();
    let _ = task.await;
    server.abort();
}

/// A refused request is refused for everyone in it, and the callers are told.
///
/// Each of them is a shard about to publish its quorum mark; a report that did
/// not land must leave the mark where it was, or a `Quorum` publish is released
/// on something the control plane never saw.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_request_tells_every_caller_in_it() {
    let (to, _sizes, server) = counting_control_plane(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        std::time::Duration::from_millis(50),
    );
    let shutdown = CancellationToken::new();
    let (reporter, task) = Reporter::spawn(to, shutdown.clone());

    let sent: Vec<_> = (0..4)
        .map(|i| {
            let reporter = reporter.clone();
            tokio::spawn(async move { reporter.send(report(&format!("s{i}"))).await })
        })
        .collect();
    for handle in sent {
        assert!(
            !handle.await.expect("join"),
            "a caller was told its report landed when the control plane refused it",
        );
    }

    shutdown.cancel();
    let _ = task.await;
    server.abort();
}

/// Shutdown answers rather than leaving callers to time out.
///
/// A shard waiting on a report it will never get would otherwise sit there
/// until its publish timeout, during a drain that is trying to finish.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_answers_everyone_still_waiting() {
    let (to, _sizes, server) = counting_control_plane(
        axum::http::StatusCode::NO_CONTENT,
        std::time::Duration::ZERO,
    );
    let shutdown = CancellationToken::new();
    let (reporter, task) = Reporter::spawn(to, shutdown.clone());
    shutdown.cancel();
    let _ = task.await;

    assert!(
        !reporter.send(report("after-shutdown")).await,
        "a report submitted after shutdown was reported as landed",
    );

    server.abort();
}

/// A follower the leader cannot reach is left out of the offsets, so its
/// last position does not look like a copy about to finish; one that is
/// merely behind is reported with the tail it is behind.
#[test]
fn a_follower_that_is_not_reached_is_not_reported_as_close() {
    let addr: std::net::SocketAddr = "10.0.0.1:7000".parse().expect("addr");
    let mut unreachable = FollowerCursor::new("broker-b", addr, 9);
    unreachable.stalled = true;
    let mut behind = FollowerCursor::new("broker-c", addr, 7);
    behind.stalled = false;
    let key = report("orders").key;

    let report = shard_report(&key, 4, 10, 10, &[unreachable, behind], false);
    assert_eq!(report.offsets, vec![("broker-c".to_string(), 7)]);
    assert_eq!(report.tail, 10);
}

/// A move's destination that has not answered a batch yet is left out too.
/// Its cursor's offset is only where shipping will start; reported, it reads
/// as a copy within the fence's lag bound, and placement would fence the
/// leader toward a destination that may not be reachable at all.
#[test]
fn a_follower_that_has_not_answered_yet_is_not_reported_as_close() {
    let addr: std::net::SocketAddr = "10.0.0.1:7000".parse().expect("addr");
    let unheard = FollowerCursor::new("broker-b", addr, 0);
    let key = report("orders").key;

    let report = shard_report(&key, 4, 5, 5, &[unheard], false);
    assert!(
        report.offsets.is_empty(),
        "an unheard follower was reported"
    );
}

/// A control plane that stores reports for streams named `led-*` and refuses
/// the rest as not led by this broker, answering per shard as the real one
/// does. `answers` rewrites the body before it goes out.
fn judging_control_plane(
    answers: fn(ReplicaStatusResponse) -> ReplicaStatusResponse,
) -> (ReportTo, tokio::task::JoinHandle<()>) {
    use felix_common::membership::{ReportOutcome, ShardReportOutcome};

    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/replica-status",
        axum::routing::post(move |body: axum::Json<ReplicaStatusRequest>| async move {
            let shards: Vec<ShardReportOutcome> = body
                .0
                .shards
                .into_iter()
                .map(|shard| ShardReportOutcome {
                    outcome: if shard.stream.starts_with("led-") {
                        ReportOutcome::Accepted
                    } else {
                        ReportOutcome::NotLeader
                    },
                    tenant_id: shard.tenant_id,
                    namespace: shard.namespace,
                    stream: shard.stream,
                    shard: shard.shard,
                    kind: shard.kind,
                    generation: shard.generation,
                })
                .collect();
            let status = if shards.iter().all(|shard| shard.outcome.accepted()) {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::CONFLICT
            };
            (
                status,
                axum::Json(answers(ReplicaStatusResponse { shards })),
            )
        }),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let listener = tokio::net::TcpListener::from_std(listener).expect("adopt");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    (
        ReportTo {
            client: reqwest::Client::new(),
            base_url: format!("http://{addr}"),
            node_id: "broker-a".to_string(),
            token: None,
            incarnation: 0,
        },
        server,
    )
}

/// **Only a stored report moves its shard's mark.** A batch answered 409
/// because one shard was refused still landed the others, and the refused one
/// must not read as landed however the rest went: its leader would release
/// `Quorum` acks on a report failover never sees.
#[tokio::test(flavor = "multi_thread")]
async fn each_caller_hears_whether_its_own_report_was_stored() {
    let (to, server) = judging_control_plane(|answer| answer);
    let reports = [report("led-a"), report("orphan"), report("led-b")];

    assert_eq!(send_reports(&to, &reports).await, vec![true, false, true]);

    let shutdown = CancellationToken::new();
    let (reporter, task) = Reporter::spawn(to, shutdown.clone());
    assert!(reporter.send(report("led-a")).await);
    assert!(
        !reporter.send(report("orphan")).await,
        "a report the control plane refused read as landed",
    );

    shutdown.cancel();
    let _ = task.await;
    server.abort();
}

/// An answer that does not line up with the request is no evidence anything
/// landed, even when it says `accepted`.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_that_does_not_match_the_request_lands_nothing() {
    let (short, server) = judging_control_plane(|mut answer| {
        answer.shards.pop();
        answer
    });
    assert_eq!(
        send_reports(&short, &[report("led-a"), report("led-b")]).await,
        vec![false, false]
    );
    server.abort();

    let (renamed, server) = judging_control_plane(|mut answer| {
        answer.shards[0].generation += 1;
        answer
    });
    assert_eq!(
        send_reports(&renamed, &[report("led-a"), report("led-b")]).await,
        vec![false, true]
    );
    server.abort();
}
