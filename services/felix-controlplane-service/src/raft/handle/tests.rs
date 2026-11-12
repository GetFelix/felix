use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use openraft::storage::{IOFlushed, RaftLogStorage};

use super::*;
use crate::raft::types::{Entry, EntryPayload, LeaderId, LogId, Membership, Vote};

struct NullApp;

#[async_trait::async_trait]
impl AppStateMachine for NullApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
}

fn settings(dir: &std::path::Path) -> RaftSettings {
    let mut settings = RaftSettings::new(
        1,
        dir.into(),
        PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    settings
}

/// A peer that counts the vote requests it is sent and grants none.
async fn counting_peer(asked: Arc<AtomicUsize>) -> String {
    let app = axum::Router::new().route(
        "/internal/raft/vote",
        axum::routing::post(move || {
            asked.fetch_add(1, Ordering::SeqCst);
            async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

/// A store whose vote says member 1 led term 5 of a three-member group.
async fn store_of_a_former_leader(dir: &std::path::Path, peer: &str) {
    let db = store::open(&dir.join("raft.redb")).expect("open store");
    let mut log = store::LogStore::new(Arc::clone(&db));
    log.save_vote(&Vote::new_committed(5, 1))
        .await
        .expect("save vote");
    let nodes: BTreeMap<NodeId, openraft::impls::BasicNode> = (1..=3)
        .map(|id| {
            let addr = peer.to_string();
            (id, openraft::impls::BasicNode { addr })
        })
        .collect();
    let membership = Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes).expect("membership");
    let leader = LeaderId {
        term: 5,
        node_id: 1,
    };
    let entry = Entry {
        log_id: LogId::new(leader, 0),
        payload: EntryPayload::Membership(membership),
    };
    log.append([entry], IOFlushed::Noop).await.expect("append");
}

/// openraft has a restarted leader campaign at once, whatever the election
/// switch says. A member withholding its vote may hold an empty log, so it
/// must not ask for votes, even with a stored vote that says it led.
#[tokio::test]
async fn a_member_withholding_its_vote_does_not_campaign_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let asked = Arc::new(AtomicUsize::new(0));
    let peer = counting_peer(Arc::clone(&asked)).await;
    store_of_a_former_leader(dir.path(), &peer).await;
    {
        let db = store::open(&dir.path().join("raft.redb")).expect("open store");
        store::set_vote_withheld(&db, true).expect("withhold");
    }

    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("start");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(asked.load(Ordering::SeqCst), 0, "asked a peer for its vote");
    assert_eq!(handle.status().term, 5);
    handle.shutdown().await.expect("shutdown");
}

/// The same store without the flag: the member does campaign, which is what
/// the test above would see if the flag were not honoured.
#[tokio::test]
async fn a_restarted_leader_that_may_vote_campaigns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let asked = Arc::new(AtomicUsize::new(0));
    let peer = counting_peer(Arc::clone(&asked)).await;
    store_of_a_former_leader(dir.path(), &peer).await;

    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("start");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while asked.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline, "never campaigned");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(handle.status().term > 5);
    handle.shutdown().await.expect("shutdown");
}

/// A leader that cannot confirm a recent quorum refuses a proposal and names
/// no leader. The write path then tries again on this member instead of
/// forwarding.
#[tokio::test]
async fn a_lease_expired_refusal_is_not_forwarded() {
    use openraft::errors::{ClientWriteError, ForwardReason, ForwardToLeader, RaftError};

    let dir = tempfile::tempdir().expect("tempdir");
    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("start");
    let refusal = ForwardToLeader::empty().with_reason(ForwardReason::LeaseExpired);
    let err = RaftError::APIError(ClientWriteError::ForwardToLeader(refusal));
    assert_eq!(handle.forward_target(&err), None);

    let mut named = ForwardToLeader::empty();
    named.leader_id = Some(2);
    named.leader_node = Some(openraft::impls::BasicNode {
        addr: "10.0.0.2:7001".to_string(),
    });
    let err = RaftError::APIError(ClientWriteError::ForwardToLeader(named));
    assert_eq!(
        handle.forward_target(&err).as_deref(),
        Some("http://10.0.0.2:7001")
    );
    handle.shutdown().await.expect("shutdown");
}
