//! The metadata state machine on a real Raft group: the #338 acceptance
//! criteria that only a running cluster can prove.
//!
//! Exactly-once bootstrap here rests on **nothing but log ordering** — no
//! row lock, no serial mutex doing the deciding — and the three replicas'
//! exported states must come out byte-identical after genuinely concurrent
//! proposals.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_controlplane_service::auth::felix_token::{SigningKey, TenantSigningKeys};
use felix_controlplane_service::model::Tenant;
use felix_controlplane_service::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::command::{
    MetaCommand, MetaError, MetaResponse, decode_result, encode_command,
};
use felix_controlplane_service::store::raft::state_machine::MetadataStateMachine;
use felix_controlplane_service::store::{StoreConfig, TenantAuthSeed};

struct TestNode {
    handle: RaftHandle,
    machine: Arc<MetadataStateMachine>,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

async fn start_node(id: NodeId, dir: &std::path::Path) -> TestNode {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(store));
    let mut settings = RaftSettings::new(id, dir.into(), peer_security());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(200), Duration::from_millis(400));
    let handle = RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
        .await
        .expect("start node");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    TestNode {
        handle,
        machine,
        addr,
        server,
    }
}

/// Deterministic per-proposal key material, standing where the API layer's
/// propose-time generation stands.
fn keys(seed: u8) -> TenantSigningKeys {
    let private = [seed; 32];
    let signing = ed25519_dalek::SigningKey::from_bytes(&private);
    TenantSigningKeys {
        current: SigningKey {
            kid: format!("kid-{seed}"),
            alg: jsonwebtoken::Algorithm::EdDSA,
            private_key: private,
            public_key: signing.verifying_key().to_bytes(),
        },
        previous: Vec::new(),
    }
}

fn bootstrap_command(seed_id: u8) -> Vec<u8> {
    encode_command(&MetaCommand::BootstrapTenantAuth {
        tenant_id: "t1".to_string(),
        seed: TenantAuthSeed {
            issuers: Vec::new(),
            policies: Vec::new(),
            groupings: Vec::new(),
            signing_keys: keys(seed_id),
        },
    })
}

#[tokio::test]
async fn concurrent_bootstraps_are_settled_by_log_order_alone() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(start_node((i + 1) as NodeId, dir.path()).await);
    }
    let members: BTreeMap<NodeId, String> = nodes
        .iter()
        .map(|node| (node.handle.status().id, node.addr.to_string()))
        .collect();
    nodes[0]
        .handle
        .initialize(members)
        .await
        .expect("initialize");

    // Wait out the election, then find the leader's handle.
    let leader = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(leader_id) = nodes.iter().find_map(|n| n.handle.status().leader)
                && let Some(node) = nodes.iter().find(|n| n.handle.status().id == leader_id)
            {
                break node.handle.clone();
            }
            assert!(Instant::now() < deadline, "no leader");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    leader
        .write(encode_command(&MetaCommand::CreateTenant {
            tenant: Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One".to_string(),
            },
        }))
        .await
        .expect("create tenant");

    // Eight bootstraps proposed concurrently, each carrying its own candidate
    // keys — the race the Postgres backend settles with a row lock, now
    // settled by nothing but the order the log assigns.
    let mut proposals = Vec::new();
    for seed_id in 0..8u8 {
        let leader = leader.clone();
        proposals.push(tokio::spawn(async move {
            let response = leader
                .write(bootstrap_command(seed_id))
                .await
                .expect("write");
            decode_result(&response).expect("decodes")
        }));
    }

    let mut winner_kid = None;
    let mut conflicts = 0;
    for proposal in proposals {
        match proposal.await.expect("join") {
            Ok(MetaResponse::SigningKeys { keys }) => {
                assert!(winner_kid.is_none(), "two bootstraps both claimed the win");
                winner_kid = Some(keys.current.kid);
            }
            Ok(other) => panic!("unexpected response {other:?}"),
            Err(MetaError::Conflict(_)) => conflicts += 1,
            Err(other) => panic!("unexpected error {other}"),
        }
    }
    let winner_kid = winner_kid.expect("exactly one winner");
    assert_eq!(conflicts, 7);

    // Every replica converges on the same applied index...
    let target = leader.status().last_applied_index;
    let deadline = Instant::now() + Duration::from_secs(10);
    for node in &nodes {
        while node.handle.status().last_applied_index < target {
            assert!(Instant::now() < deadline, "replica never caught up");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ...and once there, all three states are byte-identical, and every one
    // of them holds the winner's keys.
    let exports: Vec<Vec<u8>> = {
        let mut exports = Vec::new();
        for node in &nodes {
            exports.push(AppStateMachine::snapshot(node.machine.as_ref()).await);
        }
        exports
    };
    assert_eq!(exports[0], exports[1]);
    assert_eq!(exports[1], exports[2]);
    for node in &nodes {
        let held = felix_controlplane_service::store::AuthStore::get_tenant_signing_keys(
            node.machine.store().as_ref(),
            "t1",
        )
        .await
        .expect("keys");
        assert_eq!(held.current.kid, winner_kid);
    }

    for node in &nodes {
        let _ = node.handle.shutdown().await;
        node.server.abort();
    }
}

/// Peer credentials every member of a test group shares.
fn peer_security() -> felix_controlplane_service::raft::PeerSecurity {
    felix_controlplane_service::raft::PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        tls: None,
    }
}
