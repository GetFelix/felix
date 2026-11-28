use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use felix_broker::{ConsistencyLevel, ReadBound};
use felix_replication::engine::{CommitError, Unreadable};
use felix_wire::internal::{ErrorCode, InternalMessage};

use super::{Local, Serves, refuse_replication};
use crate::shards::{ShardKey, ShardKind};

/// How this broker serves the one shard the tests ask about.
#[derive(Default)]
struct Serving {
    served: AtomicBool,
    replicated: AtomicBool,
    lease: AtomicBool,
    refusals: AtomicUsize,
}

impl Serving {
    /// Served, with no followers and a valid lease.
    fn owner() -> Arc<Self> {
        let serving = Self::default();
        serving.served.store(true, Ordering::SeqCst);
        serving.lease.store(true, Ordering::SeqCst);
        Arc::new(serving)
    }
}

impl Serves for Serving {
    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        self.served.load(Ordering::SeqCst).then_some(3)
    }

    fn replicated(&self, _key: &ShardKey) -> bool {
        self.replicated.load(Ordering::SeqCst)
    }

    fn lease_valid(&self) -> bool {
        self.lease.load(Ordering::SeqCst)
    }

    fn record_ack_refusal(&self) {
        self.refusals.fetch_add(1, Ordering::SeqCst);
    }
}

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn standalone() -> Local<Serving> {
    Local::new(None)
}

const QUORUM: Option<ConsistencyLevel> = Some(ConsistencyLevel::Quorum);

/// **A standalone broker waits for nothing and refuses nothing.**
#[tokio::test]
async fn a_standalone_broker_has_nothing_to_wait_for() {
    let local = standalone();
    let key = key();
    assert_eq!(
        local.await_commit(ConsistencyLevel::Quorum, Some(&key), Some((0, 0))),
        Ok(())
    );
    assert_eq!(local.read_bound(QUORUM, &key), Ok(None));
    assert_eq!(local.committed_bound(&key), ReadBound::Unbounded);
    assert_eq!(local.confirm_read(), Ok(()));
    assert_eq!(
        local
            .await_readable(QUORUM, &key, Duration::from_millis(10))
            .await,
        Ok(())
    );
}

/// **An owner serving its shard with no followers acknowledges at once**:
/// it is the whole majority.
#[tokio::test]
async fn an_owner_without_followers_is_unbounded() {
    let local = Local::new(Some(Serving::owner()));
    let key = key();
    assert_eq!(
        local.await_commit(ConsistencyLevel::Quorum, Some(&key), Some((0, 4))),
        Ok(())
    );
    assert_eq!(local.read_bound(QUORUM, &key), Ok(None));
    assert_eq!(local.committed_bound(&key), ReadBound::Unbounded);
    assert_eq!(
        local
            .await_readable(QUORUM, &key, Duration::from_millis(10))
            .await,
        Ok(())
    );
}

/// **A broker that no longer serves the shard says so** to every wait,
/// instead of acknowledging or reading from a log another owner moved past.
#[tokio::test]
async fn a_shard_not_served_here_is_refused() {
    let serving = Serving::owner();
    serving.served.store(false, Ordering::SeqCst);
    let local = Local::new(Some(serving));
    let key = key();
    assert_eq!(
        local.await_commit(ConsistencyLevel::Quorum, Some(&key), Some((0, 0))),
        Err(CommitError::LeadershipLost {
            what: "batch",
            detail: "shard ownership changed",
        })
    );
    assert_eq!(local.read_bound(QUORUM, &key), Err(Unreadable::NotServed));
    assert_eq!(local.committed_bound(&key), ReadBound::Refused);
    assert_eq!(
        local
            .await_readable(QUORUM, &key, Duration::from_millis(10))
            .await,
        Err(CommitError::LeadershipLost {
            what: "read",
            detail: "this broker is not serving the shard",
        })
    );
}

/// **Only `Quorum` waits ask about the shard**, and a publish with no
/// offsets has nothing to wait for.
#[test]
fn leader_streams_and_publishes_without_offsets_wait_for_nothing() {
    let serving = Serving::owner();
    serving.served.store(false, Ordering::SeqCst);
    let local = Local::new(Some(serving));
    let key = key();
    assert_eq!(
        local.await_commit(ConsistencyLevel::Leader, Some(&key), Some((0, 0))),
        Ok(())
    );
    assert_eq!(
        local.await_commit(ConsistencyLevel::Quorum, Some(&key), None),
        Ok(())
    );
    assert_eq!(
        local.read_bound(Some(ConsistencyLevel::Leader), &key),
        Ok(None)
    );
}

/// **A state read is confirmed by the lease**: an in-memory shard's owner
/// that lost its lease may have been replaced, so the read is refused and
/// counted.
#[test]
fn a_lapsed_lease_refuses_a_state_read() {
    let serving = Serving::owner();
    let local = Local::new(Some(Arc::clone(&serving)));
    assert_eq!(local.confirm_read(), Ok(()));
    assert_eq!(serving.refusals.load(Ordering::SeqCst), 0);

    serving.lease.store(false, Ordering::SeqCst);
    assert_eq!(
        local.confirm_read(),
        Err(CommitError::LeadershipLost {
            what: "read",
            detail: "the lease lapsed",
        })
    );
    assert_eq!(serving.refusals.load(Ordering::SeqCst), 1);
}

/// **A shard placed with followers is never acknowledged here**: no engine
/// on this broker can say when a majority holds it.
#[tokio::test]
async fn a_replicated_shard_is_never_acknowledged() {
    let serving = Serving::owner();
    serving.replicated.store(true, Ordering::SeqCst);
    let local = Local::new(Some(Arc::clone(&serving)));
    let key = key();
    assert!(matches!(
        local.await_commit(ConsistencyLevel::Quorum, Some(&key), Some((0, 0))),
        Err(CommitError::LeadershipLost { what: "batch", .. })
    ));
    assert_eq!(local.read_bound(QUORUM, &key), Err(Unreadable::Settling));
    assert_eq!(local.committed_bound(&key), ReadBound::Settling);
    assert!(matches!(
        local
            .await_readable(QUORUM, &key, Duration::from_millis(10))
            .await,
        Err(CommitError::TimedOut { what: "read", .. })
    ));
    serving.lease.store(false, Ordering::SeqCst);
    assert_eq!(local.committed_bound(&key), ReadBound::Refused);
}

/// **A peer's replication request is refused as unavailable**: shards
/// replicate over Raft, on a path of their own.
#[test]
fn a_replication_request_is_refused() {
    let request = InternalMessage::ReplicateBootstrap(felix_wire::internal::ReplicateBootstrap {
        correlation_id: 12,
        shard: felix_wire::internal::ShardRef {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            generation: 0,
            incarnation: None,
        },
        base_offset: 0,
    });
    let InternalMessage::ForwardPublishError(error) = refuse_replication(&request) else {
        panic!("not an error");
    };
    assert_eq!(error.code, ErrorCode::Unavailable);
    assert_eq!(error.correlation_id, request.correlation_id());
}
