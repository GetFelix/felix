//! A follower's storage identity: a wiped copy starts over, a restarted one
//! keeps its place.

use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::StorageId;

use super::*;
use crate::FollowerCursor;

fn id(byte: u8) -> StorageId {
    StorageId::from_bytes([byte; 16])
}

/// A follower that stores everything shipped to it, under a storage identity
/// the test can change. Once `busy` it answers every batch with a retryable
/// error, as one still opening its logs after a restart does.
#[derive(Default)]
struct IdentifiedFollower {
    inner: AcceptingFollower,
    identity: Mutex<Option<StorageId>>,
    busy: AtomicBool,
}

impl IdentifiedFollower {
    fn with(identity: StorageId) -> Self {
        Self {
            identity: Mutex::new(Some(identity)),
            ..Self::default()
        }
    }

    fn restart_as(&self, identity: StorageId) {
        *self.identity.lock().expect("lock") = Some(identity);
        self.busy.store(true, Ordering::SeqCst);
    }
}

impl PeerRequester for IdentifiedFollower {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if self.busy.load(Ordering::SeqCst) {
            return Ok(InternalMessage::ReplicateError(
                felix_wire::internal::ReplicateError {
                    correlation_id: message.correlation_id(),
                    code: felix_wire::internal::ErrorCode::Unavailable,
                    expected_offset: 0,
                    detail: "still opening".to_string(),
                },
            ));
        }
        self.inner.request(node_id, addr, message).await
    }

    async fn storage_identity(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<Option<StorageId>, PeerError> {
        Ok(*self.identity.lock().expect("lock"))
    }
}

async fn pass<R: PeerRequester + Sync>(
    follower: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    groups: &mut HashMap<ShardKey, ShardCursors>,
    rebuilds: &Rebuilds,
) -> ShardReport {
    let pass = replicate_once_with(
        follower,
        broker,
        router,
        &Unfenced,
        &crate::promotion::NoGate,
        &QuorumMarks::new(),
        None,
        cursors,
        groups,
        &mut HashMap::new(),
        &mut HashMap::new(),
        rebuilds,
        &MoveThrottle::unlimited(),
    )
    .await;
    pass.reports.last().expect("a report").clone()
}

/// **A wiped follower on an idle shard is not reported caught up (#1266).**
/// Nothing ships on an idle shard, so before the leader asked for the
/// identity it went on reporting the empty copy as holding everything, and
/// placement could promote it.
#[tokio::test]
async fn a_wiped_follower_on_an_idle_shard_is_not_reported_caught_up() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = IdentifiedFollower::with(id(1));
    let mut cursors = HashMap::new();
    let rebuilds = Rebuilds::disabled();

    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(1))]);

    follower.restart_as(id(2));
    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert!(
        report.caught_up.is_empty(),
        "a wiped copy was reported as holding the log: {report:?}",
    );
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(2))]);
}

/// **A follower that only restarted still counts**, before it has answered
/// anything: its identity is the one its position was learned under.
#[tokio::test]
async fn a_restarted_follower_still_counts() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = IdentifiedFollower::with(id(1));
    let mut cursors = HashMap::new();
    let rebuilds = Rebuilds::disabled();

    pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    follower.restart_as(id(1));
    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &rebuilds,
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert_eq!(report.storage_ids, vec![("broker-b".to_string(), id(1))]);
}

/// **A new identity resets the logs that ride the shard too, and gives back
/// a rebuild slot the old copy held.** The slot is otherwise only recounted
/// at the next pass, and the cursors would keep vouching for the old copy.
#[tokio::test]
async fn an_identity_change_resets_ride_along_cursors_and_frees_a_rebuild_slot() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = IdentifiedFollower::with(id(2));
    follower.busy.store(true, Ordering::SeqCst);
    let addr = nodes()["broker-b"].advertise_addr;

    let mut entry = ShardCursors::at(4);
    entry.followers.push(FollowerCursor {
        rebuilding: true,
        storage_id: Some(id(1)),
        ..FollowerCursor::new("broker-b", addr, 0)
    });
    let mut cursors = HashMap::from([(key(), entry)]);
    let mut group = ShardCursors::at(4);
    group.followers.push(FollowerCursor {
        next_offset: 5,
        confirmed: 5,
        stalled: false,
        storage_id: Some(id(1)),
        ..FollowerCursor::new("broker-b", addr, 0)
    });
    let mut groups = HashMap::from([(key(), group)]);
    let rebuilds = Rebuilds::new(crate::RebuildPolicy {
        max_concurrent: 1,
        bytes_per_sec: 0,
    });

    pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut groups,
        &rebuilds,
    )
    .await;

    let cursor = &cursors[&key()].followers[0];
    assert!(!cursor.rebuilding);
    assert_eq!(cursor.storage_id, Some(id(2)));
    assert!(
        rebuilds.try_begin(),
        "the old copy's rebuild slot was not given back"
    );
    let group = &groups[&key()].followers[0];
    assert_eq!(
        (group.confirmed, group.next_offset, group.storage_id),
        (0, 0, Some(id(2))),
        "the group cursor still describes the old copy",
    );
}

/// **A follower that gives no identity is reported without one**, and
/// otherwise exactly as before.
#[tokio::test]
async fn a_follower_without_identity_is_reported_without_one() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let mut cursors = HashMap::new();

    let report = pass(
        &follower,
        &broker,
        &router,
        &mut cursors,
        &mut HashMap::new(),
        &Rebuilds::disabled(),
    )
    .await;
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
    assert!(report.storage_ids.is_empty());
}
