//! The Raft group reader against a fake consensus: entries commit and apply
//! only when a proposer waits for them, so a test decides what the table
//! holds at each step.

use std::collections::VecDeque;

use bytes::Bytes;
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;
use crate::durable::{DurableStorage, StreamLog};
use crate::queue::Folded;

const G: &str = "workers";
const OTHER: &str = "audit";
const VIS: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Fake {
    groups: Arc<RwLock<ShardGroups>>,
    state: parking_lot::Mutex<FakeState>,
}

#[derive(Default)]
struct FakeState {
    next_index: u64,
    /// Proposed, not yet committed. Beyond any read index.
    pending: VecDeque<(u64, GroupEntry)>,
    /// Committed by another proposer just before the next proposal.
    inject: Vec<GroupEntry>,
    /// Waits for an entry time out.
    stalled: bool,
    applied: Vec<(GroupEntry, Folded)>,
}

const TERM: u64 = 1;

impl Fake {
    fn apply_through(&self, state: &mut FakeState, index: u64) {
        while state.pending.front().is_some_and(|(at, _)| *at <= index) {
            let (at, entry) = state.pending.pop_front().expect("front");
            let folded = self.groups.write().apply(TERM, at, &entry);
            state.applied.push((entry, folded));
        }
    }

    fn flush(&self) {
        let mut state = self.state.lock();
        self.apply_through(&mut state, u64::MAX);
    }

    fn written(&self) -> Vec<GroupOp> {
        self.state
            .lock()
            .applied
            .iter()
            .filter(|(_, folded)| *folded == Folded::Applied)
            .map(|(entry, _)| entry.op)
            .collect()
    }
}

impl GroupConsensus for Fake {
    async fn fresh(&self, _key: &GroupKey, _deadline: Instant) -> Result<FreshGroups> {
        Ok(FreshGroups {
            term: TERM,
            groups: Arc::clone(&self.groups),
        })
    }

    async fn propose(&self, _key: &GroupKey, entry: &GroupEntry) -> Result<Proposed> {
        let mut state = self.state.lock();
        for injected in std::mem::take(&mut state.inject) {
            state.next_index += 1;
            let at = state.next_index;
            state.pending.push_back((at, injected));
            self.apply_through(&mut state, at);
        }
        state.next_index += 1;
        let index = state.next_index;
        state.pending.push_back((index, entry.clone()));
        Ok(Proposed { term: TERM, index })
    }

    async fn applied(&self, _key: &GroupKey, proposed: Proposed, _deadline: Instant) -> Result<()> {
        let mut state = self.state.lock();
        if state.stalled && self.groups.read().table().applied() < proposed.index {
            return Err(BrokerError::ProposalsStalled {
                waited: Duration::ZERO,
            });
        }
        self.apply_through(&mut state, proposed.index);
        Ok(())
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    log: StreamLog,
    groups: RaftGroups<Fake>,
}

fn fixture(max_attempts: u32) -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir.path().join("streams"), config).expect("storage");
    let log = storage.open_stream("t", "ns", "s", 0).expect("log");
    Fixture {
        _dir: dir,
        log,
        groups: RaftGroups::new(Fake::default(), VIS, max_attempts),
    }
}

fn key(group: &str) -> GroupKey {
    GroupKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "s".into(),
        shard: 0,
        group: group.into(),
    }
}

fn later() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

async fn publish(log: &StreamLog, n: usize) {
    let records: Vec<Bytes> = (0..n).map(|i| Bytes::from(format!("r{i}"))).collect();
    log.append(&records).await.expect("append");
}

impl Fixture {
    async fn poll(&self, group: &str, max: usize) -> Result<Vec<u64>> {
        let claimed = self
            .groups
            .poll_below(
                &key(group),
                &self.log,
                u64::MAX,
                max,
                Instant::now(),
                None,
                None,
                later(),
            )
            .await?;
        Ok(claimed.iter().map(|c| c.offset).collect())
    }

    fn fake(&self) -> &Fake {
        self.groups.consensus()
    }

    fn cursor(&self, group: &str) -> Option<u64> {
        self.fake()
            .groups
            .read()
            .table()
            .group(group)
            .map(|s| s.cursor)
    }
}

/// M1: a run of records that are not data is settled in the tracker alone,
/// in one poll, and no advance is proposed for it.
#[tokio::test]
async fn skipped_records_propose_nothing() {
    let fx = fixture(5);
    for generation in 2..52 {
        fx.log
            .append_generation_start(generation)
            .await
            .expect("start");
    }
    publish(&fx.log, 1).await;
    fx.groups
        .seek(&key(G), 0, false, later())
        .await
        .expect("seek");
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![50]);
    assert_eq!(
        fx.fake().written(),
        vec![GroupOp::Seek {
            to: 0,
            if_new: false
        }],
        "skipping fifty generation starts proposed an entry"
    );
    fx.groups.ack(&key(G), 50, later()).await.expect("ack");
    assert_eq!(
        fx.cursor(G),
        Some(51),
        "the advance over a data record covers the run"
    );
}

/// Req (a): an ack is answered only once its advance is in the table.
#[tokio::test]
async fn an_ack_answers_after_its_advance_applies() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);
    fx.groups.ack(&key(G), 0, later()).await.expect("ack");
    assert_eq!(fx.cursor(G), Some(1));
}

/// Req (b) and the refresh on drop: a seek another proposer committed
/// before this ack's advance drops it. The fold forgets the tracker, the ack
/// is answered lapsed, and the next poll hydrates at the seek and redelivers.
#[tokio::test]
async fn a_dropped_advance_retires_the_tracker() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    fx.groups
        .seek(&key(G), 0, false, later())
        .await
        .expect("seek");
    assert_eq!(fx.poll(G, 2).await.expect("poll"), vec![0, 1]);
    fx.fake().state.lock().inject.push(GroupEntry {
        group: G.into(),
        op: GroupOp::Seek {
            to: 0,
            if_new: false,
        },
    });
    let acked = fx.groups.ack(&key(G), 0, later()).await;
    assert!(
        matches!(acked, Err(BrokerError::GroupClaimLapsed { offset: 0 })),
        "a dropped advance was answered {acked:?}"
    );
    assert_eq!(fx.cursor(G), Some(0));
    assert_eq!(
        fx.poll(G, 1).await.expect("poll"),
        vec![0],
        "not hydrated past the seek"
    );
}

/// Req (c), Round 5: a delete of another group does not void a new group's
/// claims, because the new group's creation committed at its first hand-out.
#[tokio::test]
async fn a_delete_elsewhere_keeps_a_new_groups_claims() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    fx.groups
        .seek(&key(OTHER), 0, false, later())
        .await
        .expect("other");
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);
    assert!(
        fx.groups
            .delete(&key(OTHER), later())
            .await
            .expect("delete")
    );
    for offset in 0..3 {
        fx.groups.ack(&key(G), offset, later()).await.expect("ack");
    }
    assert_eq!(
        fx.cursor(G),
        Some(3),
        "the delete of another group voided this one's acks"
    );
}

/// Req (d): a record out of attempts is listed before the advance over it.
#[tokio::test]
async fn a_dead_letter_is_written_before_the_advance_over_it() {
    let fx = fixture(1);
    publish(&fx.log, 2).await;
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    fx.groups.nack(&key(G), 0, later()).await.expect("nack");
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![1]);
    let ops = fx.fake().written();
    let dead = ops
        .iter()
        .position(|op| matches!(op, GroupOp::DeadLetter { offset: 0, .. }))
        .expect("dead letter written");
    let advance = ops
        .iter()
        .position(|op| matches!(op, GroupOp::Advance { to, .. } if *to > 0))
        .expect("advance written");
    assert!(
        dead < advance,
        "advance over 0 before its dead letter: {ops:?}"
    );
    assert_eq!(
        fx.groups
            .dead_letters(&key(G), later())
            .await
            .expect("dead"),
        vec![0]
    );
}

/// Req (e): a redrive whose wait timed out is applied before the next poll
/// hydrates, so the redriven record is owed rather than lost to the tracker.
#[tokio::test]
async fn an_unanswered_write_is_awaited_before_hydrate() {
    let fx = fixture(1);
    publish(&fx.log, 2).await;
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    fx.groups.nack(&key(G), 0, later()).await.expect("nack");
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![1]);
    fx.groups.ack(&key(G), 1, later()).await.expect("ack");
    fx.fake().state.lock().stalled = true;
    let redriven = fx.groups.redrive(&key(G), 0, later()).await;
    assert!(
        redriven.is_err(),
        "the stalled redrive answered {redriven:?}"
    );
    fx.fake().state.lock().stalled = false;
    assert_eq!(
        fx.poll(G, 1).await.expect("poll"),
        vec![0],
        "the redriven record went unowed"
    );
    fx.fake().flush();
}

/// R3-2: `if_new` on a group the table holds proposes nothing.
#[tokio::test]
async fn if_new_on_an_existing_group_proposes_nothing() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 2).await.expect("poll"), vec![0, 1]);
    fx.groups.ack(&key(G), 0, later()).await.expect("ack");
    let before = fx.fake().written().len();
    let seek = fx
        .groups
        .seek(&key(G), 2, true, later())
        .await
        .expect("seek");
    assert_eq!(
        seek,
        Seek {
            offset: 1,
            moved: false
        }
    );
    assert_eq!(fx.fake().written().len(), before);
    // A leader with no tracker for the group answers from the table alone.
    let applied = fx.fake().groups.read().table().applied();
    let successor = RaftGroups::new(
        Fake {
            groups: Arc::clone(&fx.fake().groups),
            state: parking_lot::Mutex::new(FakeState {
                next_index: applied,
                ..FakeState::default()
            }),
        },
        VIS,
        5,
    );
    let seek = successor
        .seek(&key(G), 2, true, later())
        .await
        .expect("seek");
    assert_eq!(
        seek,
        Seek {
            offset: 1,
            moved: false
        }
    );
    assert!(successor.consensus().written().is_empty());
    let seek = fx
        .groups
        .seek(&key(OTHER), 2, true, later())
        .await
        .expect("new");
    assert_eq!(
        seek,
        Seek {
            offset: 2,
            moved: true
        }
    );
}
