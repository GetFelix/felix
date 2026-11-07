use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use felix_replication::peer::PeerError;
use felix_wire::internal::InternalMessage;
use reqwest::StatusCode;

use super::*;
use crate::test_support::fake_link::FakeLink;

/// Every request fails; it only claims the capabilities a simulated network
/// would offer.
struct NoPeers;

impl PeerRequester for NoPeers {
    async fn request(
        &self,
        node_id: &str,
        _: SocketAddr,
        _: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        Err(PeerError::Unavailable {
            node_id: node_id.to_string(),
            detail: "no network".to_string(),
        })
    }

    fn offers_election(&self) -> bool {
        true
    }

    fn offers_fence(&self) -> bool {
        true
    }
}

fn link() -> FakeLink {
    // A node never registered; the rest answers 503 until set.
    FakeLink::default().status("node", StatusCode::NOT_FOUND)
}

fn catalog(up: Arc<AtomicBool>, paths: Arc<Mutex<Vec<String>>>) -> CatalogFetcher {
    Arc::new(move |path: String| {
        let up = Arc::clone(&up);
        let paths = Arc::clone(&paths);
        Box::pin(async move {
            paths.lock().unwrap().push(path.clone());
            anyhow::ensure!(up.load(Ordering::SeqCst), "control plane is down");
            let body = if path.contains("/snapshot") {
                r#"{"items":[],"next_seq":0,"tombstones":[]}"#
            } else {
                r#"{"items":[],"next_seq":0}"#
            };
            Ok(Bytes::from(body))
        })
    })
}

async fn settle() {
    tokio::time::sleep(Duration::from_secs(5)).await;
}

#[tokio::test(start_paused = true)]
async fn registers_only_after_the_catalog_is_applied_and_reopens_after_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let config = SimBrokerConfig::new("sim-a", dir.path());
    let up = Arc::new(AtomicBool::new(false));
    let paths = Arc::new(Mutex::new(Vec::new()));
    let link = link();

    let opened = SimBroker::open(
        config.clone(),
        link.clone(),
        catalog(Arc::clone(&up), Arc::clone(&paths)),
    )
    .await
    .unwrap();
    assert!(opened.capabilities().contains(PeerCapabilities::FENCE));
    assert!(opened.capabilities().contains(PeerCapabilities::ELECTION));
    let broker = opened.run(Arc::new(NoPeers));

    // The catalog is unreachable: the sync retries and nothing registers.
    settle().await;
    assert!(!broker.seeded().is_cancelled());
    assert!(!link.calls().contains(&"register"));
    assert!(!paths.lock().unwrap().is_empty());

    up.store(true, Ordering::SeqCst);
    settle().await;
    assert!(broker.seeded().is_cancelled());
    assert!(link.calls().contains(&"register"));
    assert!(
        paths
            .lock()
            .unwrap()
            .iter()
            .any(|path| path == "/v1/streams/snapshot")
    );

    broker.crash();
    settle().await;

    // The same directory opens again, and the new run starts unseeded.
    let reopened = SimBroker::open(config, link, catalog(up, paths))
        .await
        .unwrap()
        .run(Arc::new(NoPeers));
    assert!(!reopened.seeded().is_cancelled());
    settle().await;
    assert!(reopened.seeded().is_cancelled());
    reopened.crash();
}

#[tokio::test(start_paused = true)]
async fn attesting_a_copy_does_not_open_a_log() {
    let dir = tempfile::tempdir().unwrap();
    let config = SimBrokerConfig::new("sim-a", dir.path());
    let up = Arc::new(AtomicBool::new(true));
    let paths = Arc::new(Mutex::new(Vec::new()));
    let broker = SimBroker::open(config, link(), catalog(up, paths))
        .await
        .unwrap()
        .run(Arc::new(NoPeers));
    settle().await;

    let key = ShardKey {
        tenant_id: "t".into(),
        namespace: "n".into(),
        stream: "s".into(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    assert!(broker.copy_attestation(&key).await.is_none());
    let open = broker
        .broker()
        .opened_log(LogKind::Stream, "t", "n", "s", 0, None)
        .await;
    assert!(open.is_none(), "the attestation opened the log");
    broker.crash();
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_does_not_open_a_log() {
    let dir = tempfile::tempdir().unwrap();
    let config = SimBrokerConfig::new("sim-a", dir.path());
    let up = Arc::new(AtomicBool::new(true));
    let paths = Arc::new(Mutex::new(Vec::new()));
    let broker = SimBroker::open(config, link(), catalog(up, paths))
        .await
        .unwrap()
        .run(Arc::new(NoPeers));
    settle().await;

    let key = ShardKey {
        tenant_id: "t".into(),
        namespace: "n".into(),
        stream: "s".into(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    assert_eq!(broker.shard_snapshot(&key).await.log_tail, None);
    let open = broker
        .broker()
        .opened_log(LogKind::Stream, "t", "n", "s", 0, None)
        .await;
    assert!(open.is_none(), "the snapshot opened the log");
    broker.crash();
}
