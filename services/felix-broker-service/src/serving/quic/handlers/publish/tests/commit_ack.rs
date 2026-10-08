//! A publish acknowledged on commit is answered by the task that sees it
//! commit, and not before.

use felix_broker::{DurableStorage, StreamMetadata};
use felix_storage::log::{FsyncMode, LogConfig};
use serial_test::serial;

use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;

use super::*;

/// How long every flush is held. The answer must not come sooner.
const FLUSH_HELD: Duration = Duration::from_millis(250);

/// Holds every flush in the process for as long as it lives.
struct HeldFlushes;

impl HeldFlushes {
    fn hold(delay: Duration) -> Self {
        felix_storage::fault::set_fsync_delay(delay);
        Self
    }
}

impl Drop for HeldFlushes {
    fn drop(&mut self) {
        felix_storage::fault::set_fsync_delay(Duration::ZERO);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn a_commit_ack_is_not_sent_before_the_flush() {
    let dir = tempfile::tempdir().expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir.path(), config).expect("storage"));
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let broker = Arc::new(broker);
    let ctx = build_publish_context(
        Arc::clone(&broker),
        &BrokerConfig::default(),
        ClusterContext::default(),
    );
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let (throttle_tx, _throttle_rx) = watch::channel(false);
    let (cancel_tx, _cancel_rx) = watch::channel(false);
    let commit_acks = CommitAcks::for_test(&out_tx, Arc::new(Semaphore::new(8)));

    let _held = HeldFlushes::hold(FLUSH_HELD);
    let sent = Instant::now();
    handle_publish_message(
        &broker,
        &ctx,
        &mut HashMap::new(),
        &mut String::new(),
        false,
        // Acknowledged on commit.
        true,
        &out_tx,
        &Arc::new(AtomicUsize::new(0)),
        &throttle_tx,
        &Arc::new(parking_lot::Mutex::new(
            AckTimeoutState::new(Instant::now()),
        )),
        &cancel_tx,
        &commit_acks,
        "t1".to_string(),
        "ns".to_string(),
        "orders".to_string(),
        b"held".to_vec(),
        None,
        Some(1),
        Some(felix_wire::AckMode::PerMessage),
        false,
        String::new(),
    )
    .await
    .expect("publish handled");

    let answer = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("answered")
        .expect("open queue");
    let waited = sent.elapsed();
    assert!(
        matches!(
            answer,
            Outgoing::Message(Message::PublishOk {
                request_id: 1,
                offset: Some(0),
            })
        ),
        "{answer:?}"
    );
    assert!(
        waited >= FLUSH_HELD,
        "answered after {waited:?}, before the flush held for {FLUSH_HELD:?} was done"
    );
}
