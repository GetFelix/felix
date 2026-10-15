use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
use std::time::Duration;

use openraft::error::RPCError;
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::AppendEntriesRequest;

use super::*;
use crate::raft::network::HttpNetwork;

/// An application whose join floor the test sets.
pub(in crate::raft) struct FloorApp(pub(in crate::raft) Arc<AtomicU16>);

#[async_trait::async_trait]
impl AppStateMachine for FloorApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
    fn version(&self) -> u16 {
        10
    }
    async fn join_floor(&self) -> u16 {
        self.0.load(Ordering::SeqCst)
    }
}

pub(in crate::raft) fn hold_at(floor: Arc<AtomicU16>) -> Arc<Hold> {
    Arc::new(Hold::new(1, Arc::new(FloorApp(floor)), Arc::default()))
}

/// A member reporting `version` (`u16::MAX`: not answering) that counts the
/// appends it is sent and answers them with a conflict while `conflict`.
struct Member {
    version: Arc<AtomicU16>,
    conflict: Arc<AtomicBool>,
    appends: Arc<AtomicUsize>,
    addr: String,
}

async fn member(version: u16) -> Member {
    let member = Member {
        version: Arc::new(AtomicU16::new(version)),
        conflict: Arc::default(),
        appends: Arc::default(),
        addr: String::new(),
    };
    let (version, conflict, appends) = (
        Arc::clone(&member.version),
        Arc::clone(&member.conflict),
        Arc::clone(&member.appends),
    );
    let app = axum::Router::new()
        .route(
            "/internal/raft/append-entries",
            axum::routing::post(move || {
                appends.fetch_add(1, Ordering::SeqCst);
                let reply = if conflict.load(Ordering::SeqCst) {
                    "Conflict"
                } else {
                    "Success"
                };
                async move { axum::Json(serde_json::json!({ "Ok": reply })) }
            }),
        )
        .route(
            "/internal/raft/standing",
            axum::routing::get(move || {
                let version = version.load(Ordering::SeqCst);
                async move {
                    if version == u16::MAX {
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    Ok(axum::Json(
                        serde_json::json!({ "last_log_index": 3, "version": version }),
                    ))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });
    Member { addr, ..member }
}

fn network(addr: &str, hold: Arc<Hold>) -> HttpNetwork {
    HttpNetwork::for_tests(addr, hold)
}

async fn append(network: &mut HttpNetwork) -> Result<(), String> {
    let request = AppendEntriesRequest {
        vote: openraft::Vote::new(1, 1),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    match network
        .append_entries(request, RPCOption::new(Duration::from_secs(5)))
        .await
    {
        Ok(_) => Ok(()),
        Err(RPCError::Unreachable(err)) => Err(err.to_string()),
        Err(err) => panic!("not an unreachable error: {err:?}"),
    }
}

/// A member below the floor gets nothing until it reports the floor.
#[tokio::test]
async fn a_member_below_the_floor_is_held_until_it_reports_it() {
    let member = member(8).await;
    let mut network = network(&member.addr, hold_at(Arc::new(AtomicU16::new(10))));

    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("supports metadata version 8"), "{held}");
    assert_eq!(
        member.appends.load(Ordering::SeqCst),
        0,
        "a held member was fed"
    );

    member.version.store(10, Ordering::SeqCst);
    append(&mut network).await.expect("released");
    assert_eq!(member.appends.load(Ordering::SeqCst), 1);
}

/// With a floor, a member that never said its level is not fed; without
/// one, nothing is asked.
#[tokio::test]
async fn a_silent_member_is_held_only_under_a_floor() {
    let member = member(u16::MAX).await;
    let floor = Arc::new(AtomicU16::new(10));
    let mut network = network(&member.addr, hold_at(Arc::clone(&floor)));

    let held = append(&mut network).await.expect_err("held");
    assert!(held.contains("has not reported"), "{held}");
    assert_eq!(member.appends.load(Ordering::SeqCst), 0);

    floor.store(0, Ordering::SeqCst);
    append(&mut network).await.expect("no floor");
    assert_eq!(member.appends.load(Ordering::SeqCst), 1);
}

/// A member that lost its log answers a conflict; its old level is not
/// trusted after that, so an older build restarted under the same id is
/// held before it is sent anything.
#[tokio::test]
async fn a_conflict_makes_the_member_report_again() {
    let member = member(10).await;
    let mut network = network(&member.addr, hold_at(Arc::new(AtomicU16::new(10))));
    append(&mut network).await.expect("at the floor");

    member.conflict.store(true, Ordering::SeqCst);
    member.version.store(8, Ordering::SeqCst);
    append(&mut network)
        .await
        .expect("the conflict is an answer");
    let held = append(&mut network)
        .await
        .expect_err("held after the conflict");
    assert!(held.contains("supports metadata version 8"), "{held}");
    assert_eq!(member.appends.load(Ordering::SeqCst), 2);
}
