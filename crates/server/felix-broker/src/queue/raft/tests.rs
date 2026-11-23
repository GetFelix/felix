//! The Raft group reader against a fake consensus. Entries apply when a
//! proposer waits for them or a read index covers them, so a test decides
//! what the table holds at each step; knobs stall commits, lag the read
//! index, and move the term between calls.

use std::collections::VecDeque;

use bytes::Bytes;
use felix_storage::log::{FsyncMode, LogConfig};

use super::*;
use crate::durable::{DurableStorage, StreamLog};
use crate::error::NotReadable;
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
    /// Terms after [`TERM`] this shard has been through.
    terms_passed: u64,
    next_index: u64,
    /// Proposed, not yet applied.
    pending: VecDeque<(Proposed, GroupEntry)>,
    /// Truncated by a new term's leader: never applied.
    lost: Vec<Proposed>,
    /// Committed by another proposer just before the next proposal.
    inject: Vec<GroupEntry>,
    /// Nothing commits: waits for an entry time out, and a read index
    /// covers nothing pending.
    stalled: bool,
    /// `fresh` applies nothing pending, as a read index taken before those
    /// entries committed.
    lag_reads: bool,
    /// The term moves on just before the next proposal.
    new_term_before_propose: bool,
    /// The term moves on just after the next proposal.
    new_term_after_propose: bool,
    /// Entries fold under this term instead of the one they were proposed in.
    fold_term: Option<u64>,
    applied: Vec<(GroupEntry, Folded)>,
}

const TERM: u64 = 1;

impl FakeState {
    fn term(&self) -> u64 {
        TERM + self.terms_passed
    }

    /// A new leader: what this one proposed and did not apply is truncated.
    fn new_term(&mut self) {
        self.terms_passed += 1;
        let lost: Vec<Proposed> = self.pending.drain(..).map(|(at, _)| at).collect();
        self.lost.extend(lost);
    }
}

impl Fake {
    fn apply_through(&self, state: &mut FakeState, index: u64) {
        while state
            .pending
            .front()
            .is_some_and(|(at, _)| at.index <= index)
        {
            let (at, entry) = state.pending.pop_front().expect("front");
            let term = state.fold_term.unwrap_or(at.term);
            let folded = self.groups.write().apply(term, at.index, &entry);
            state.applied.push((entry, folded));
        }
    }

    fn flush(&self) {
        let mut state = self.state.lock();
        self.apply_through(&mut state, u64::MAX);
    }

    fn new_term(&self) {
        self.state.lock().new_term();
    }

    fn written(&self) -> Vec<GroupOp> {
        self.state
            .lock()
            .applied
            .iter()
            .filter(|(_, folded)| *folded == Folded::Applied)
            .map(|(entry, _)| entry.op.clone())
            .collect()
    }
}

impl GroupConsensus for Fake {
    async fn fresh(&self, _key: &GroupKey, _deadline: Instant) -> Result<FreshGroups> {
        let mut state = self.state.lock();
        if !state.stalled && !state.lag_reads {
            self.apply_through(&mut state, u64::MAX);
        }
        Ok(FreshGroups {
            term: state.term(),
            groups: Arc::clone(&self.groups),
        })
    }

    async fn propose(&self, _key: &GroupKey, term: u64, entry: &GroupEntry) -> Result<Proposed> {
        let mut state = self.state.lock();
        if std::mem::take(&mut state.new_term_before_propose) {
            state.new_term();
        }
        if term != state.term() {
            return Err(BrokerError::StateNotReadable(NotReadable::Refused));
        }
        for injected in std::mem::take(&mut state.inject) {
            state.next_index += 1;
            let at = Proposed {
                term: state.term(),
                index: state.next_index,
            };
            state.pending.push_back((at, injected));
            self.apply_through(&mut state, at.index);
        }
        state.next_index += 1;
        let proposed = Proposed {
            term: state.term(),
            index: state.next_index,
        };
        state.pending.push_back((proposed, entry.clone()));
        if std::mem::take(&mut state.new_term_after_propose) {
            state.new_term();
        }
        Ok(proposed)
    }

    async fn applied(&self, _key: &GroupKey, proposed: Proposed, _deadline: Instant) -> Result<()> {
        let mut state = self.state.lock();
        if state.lost.contains(&proposed) {
            return Err(BrokerError::StateNotReadable(NotReadable::Refused));
        }
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

    /// Poll at `now` as `consumer`, returning `(offset, attempts)`.
    async fn poll_as(
        &self,
        max: usize,
        now: Instant,
        consumer: &GroupConsumer,
    ) -> Result<Vec<(u64, u32)>> {
        let claimed = self
            .groups
            .poll_below(
                &key(G),
                &self.log,
                u64::MAX,
                max,
                now,
                Some(consumer),
                None,
                later(),
            )
            .await?;
        Ok(claimed.iter().map(|c| (c.offset, c.attempts)).collect())
    }

    fn fake(&self) -> &Fake {
        self.groups.consensus()
    }

    fn set(&self, change: impl FnOnce(&mut FakeState)) {
        change(&mut self.fake().state.lock());
    }

    fn dead(&self, group: &str) -> Vec<u64> {
        self.fake()
            .groups
            .read()
            .table()
            .group(group)
            .map(|s| s.dead.iter().copied().collect())
            .unwrap_or_default()
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
    let written = fx.fake().written();
    assert_eq!(
        written[0],
        GroupOp::Seek {
            to: 0,
            if_new: false
        }
    );
    assert!(
        matches!(&written[1..], [GroupOp::Claim { claims, .. }] if claims.records == [(50, 1)]),
        "skipping fifty generation starts proposed an entry: {written:?}"
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

/// Req (e): a redrive whose wait timed out is applied before a tracker
/// hydrated afresh in the same term reads the table, so the redriven record
/// is owed rather than lost.
#[tokio::test]
async fn an_unanswered_write_is_awaited_before_hydrate() {
    let fx = fixture(1);
    publish(&fx.log, 2).await;
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    fx.groups.nack(&key(G), 0, later()).await.expect("nack");
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![1]);
    fx.groups.ack(&key(G), 1, later()).await.expect("ack");
    fx.set(|s| {
        s.stalled = true;
        s.lag_reads = true;
    });
    let redriven = fx.groups.redrive(&key(G), 0, later()).await;
    assert!(
        redriven.is_err(),
        "the stalled redrive answered {redriven:?}"
    );
    // The tracker that owes the record is gone; the next one comes from the
    // table, and only the wait for the last proposal applies the redrive.
    fx.groups.step_down(&key(G));
    assert_eq!(fx.groups.tracked(), 0);
    fx.set(|s| s.stalled = false);
    assert_eq!(
        fx.poll(G, 1).await.expect("poll"),
        vec![0],
        "the redriven record went unowed"
    );
    fx.fake().flush();
}

/// A discard whose wait gave up keeps the tracker: its standing claims are
/// not handed out again.
#[tokio::test]
async fn a_timed_out_discard_keeps_the_tracker() {
    let fx = fixture(1);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    fx.groups.nack(&key(G), 0, later()).await.expect("nack");
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![1]);
    assert_eq!(fx.dead(G), vec![0]);
    fx.set(|s| s.stalled = true);
    assert!(fx.groups.discard(&key(G), 0, later()).await.is_err());
    fx.set(|s| s.stalled = false);
    assert_eq!(
        fx.poll(G, 1).await.expect("poll"),
        vec![2],
        "a standing claim was handed out again"
    );
}

/// A dead letter above the cursor stays settled across a leader change that
/// came before the advance over it.
#[tokio::test]
async fn a_dead_letter_above_the_cursor_is_not_redelivered() {
    let fx = fixture(5);
    publish(&fx.log, 2).await;
    assert_eq!(fx.poll(G, 2).await.expect("poll"), vec![0, 1]);
    assert!(
        fx.groups
            .dead_letter(&key(G), 1, later())
            .await
            .expect("dead letter")
    );
    assert_eq!(fx.dead(G), vec![1]);
    assert_eq!(fx.cursor(G), Some(0));
    fx.fake().new_term();
    // 0 is still claimed by the consumer it went to.
    assert_eq!(
        fx.poll(G, 2).await.expect("poll"),
        Vec::<u64>::new(),
        "the dead letter was delivered again"
    );
    fx.groups.ack(&key(G), 0, later()).await.expect("ack");
    assert_eq!(fx.cursor(G), Some(2));
}

/// A seek back over a dead letter delivers it again after a leader change:
/// the seek's fold clears it, so the rebuilt tracker does not settle it.
#[tokio::test]
async fn a_seek_back_over_a_dead_letter_delivers_it_again_after_a_leader_change() {
    let fx = fixture(5);
    publish(&fx.log, 10).await;
    assert_eq!(fx.poll(G, 10).await.expect("poll").len(), 10);
    assert!(
        fx.groups
            .dead_letter(&key(G), 5, later())
            .await
            .expect("dead letter")
    );
    for offset in (0..10).filter(|&offset| offset != 5) {
        fx.groups.ack(&key(G), offset, later()).await.expect("ack");
    }
    assert_eq!(fx.cursor(G), Some(10));
    fx.groups
        .seek(&key(G), 0, false, later())
        .await
        .expect("seek");
    fx.fake().new_term();
    assert_eq!(
        fx.poll(G, 10).await.expect("poll"),
        (0..10).collect::<Vec<_>>()
    );
    assert!(fx.dead(G).is_empty());
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

/// A late ack from a holder of a deleted incarnation is refused by the
/// re-created group, so the record it names is still delivered.
#[tokio::test]
async fn a_stale_ack_after_delete_skips_nothing() {
    let fx = fixture(5);
    publish(&fx.log, 4).await;
    assert_eq!(fx.poll(G, 4).await.expect("poll"), vec![0, 1, 2, 3]);
    assert!(fx.groups.delete(&key(G), later()).await.expect("delete"));
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    let acked = fx.groups.ack(&key(G), 2, later()).await;
    assert!(
        matches!(
            acked,
            Err(BrokerError::GroupOffsetNotHandedOut { offset: 2, .. })
        ),
        "a stale ack was taken: {acked:?}"
    );
    assert_eq!(
        fx.poll(G, 4).await.expect("poll"),
        vec![1, 2, 3],
        "the stale ack skipped a record"
    );
}

/// A decision read in one term is not proposed in the next: the proposal
/// is refused and nothing applies.
#[tokio::test]
async fn a_term_change_before_propose_refuses_the_write() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);
    let before = fx.fake().written().len();
    fx.set(|s| s.new_term_before_propose = true);
    let acked = fx.groups.ack(&key(G), 0, later()).await;
    assert!(acked.is_err(), "an old term's advance answered {acked:?}");
    assert_eq!(
        fx.fake().written().len(),
        before,
        "an old term's write applied"
    );
    assert_eq!(fx.cursor(G), Some(0));
    // The claims were written, so the next term's tracker holds them.
    assert_eq!(fx.poll(G, 3).await.expect("poll"), Vec::<u64>::new());
}

/// An entry lost to a term change after it was proposed answers an error,
/// and the next term's operation serves a fresh tracker.
#[tokio::test]
async fn a_term_change_after_propose_retires_the_tracker() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);
    fx.set(|s| s.new_term_after_propose = true);
    let acked = fx.groups.ack(&key(G), 0, later()).await;
    assert!(acked.is_err(), "a truncated advance answered {acked:?}");
    assert_eq!(fx.cursor(G), Some(0));
    // A fresh tracker: the truncated ack did not settle 0, and the claims
    // written before it still stand.
    assert_eq!(
        fx.poll(G, 3).await.expect("poll"),
        Vec::<u64>::new(),
        "the old term's tracker served the new term"
    );
    fx.groups.ack(&key(G), 0, later()).await.expect("ack");
    assert_eq!(fx.cursor(G), Some(1));
}

/// `ready` does not wait for an older term's entry: it is under the read
/// index or never applies.
#[tokio::test]
async fn an_old_terms_entry_is_not_awaited() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);
    fx.set(|s| s.stalled = true);
    assert!(fx.groups.ack(&key(G), 0, later()).await.is_err());
    fx.fake().new_term();
    fx.set(|s| s.stalled = false);
    // Served at all, so the old term's entry was not awaited; the claims
    // from before it stand.
    assert_eq!(fx.poll(G, 3).await.expect("poll"), Vec::<u64>::new());
}

/// A write whose wait gave up keeps its tracker. When the entry commits
/// after all, standing claims are not handed out again.
#[tokio::test]
async fn a_timed_out_write_keeps_the_tracker() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);
    fx.set(|s| {
        s.stalled = true;
        s.lag_reads = true;
    });
    assert!(fx.groups.ack(&key(G), 0, later()).await.is_err());
    fx.set(|s| s.stalled = false);
    assert_eq!(
        fx.poll(G, 3).await.expect("poll"),
        Vec::<u64>::new(),
        "standing claims were handed out again"
    );
    assert_eq!(fx.cursor(G), Some(1));
    fx.groups.ack(&key(G), 1, later()).await.expect("ack");
    assert_eq!(fx.cursor(G), Some(2));
}

/// A seek's tracker carries the token the fold gave the group, whatever
/// term the fold names it by.
#[tokio::test]
async fn a_seek_takes_the_tables_token() {
    let fx = fixture(5);
    publish(&fx.log, 2).await;
    fx.set(|s| s.fold_term = Some(7));
    fx.groups
        .seek(&key(G), 0, false, later())
        .await
        .expect("seek");
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    fx.groups.ack(&key(G), 0, later()).await.expect("ack");
    assert_eq!(fx.cursor(G), Some(1));
}

/// A poll whose new group's creation lost to another group's delete
/// hydrates again rather than answering lapsed.
#[tokio::test]
async fn a_poll_racing_another_delete_retries() {
    let fx = fixture(5);
    publish(&fx.log, 2).await;
    fx.groups
        .seek(&key(OTHER), 0, false, later())
        .await
        .expect("other");
    fx.set(|s| {
        s.inject.push(GroupEntry {
            group: OTHER.into(),
            op: GroupOp::Delete,
        })
    });
    assert_eq!(fx.poll(G, 2).await.expect("poll"), vec![0, 1]);
    fx.groups.ack(&key(G), 0, later()).await.expect("ack");
    assert_eq!(fx.cursor(G), Some(1));
}

/// Slots go with a delete, with a newer term, on step-down, and when idle
/// with no standing claim.
#[tokio::test]
async fn slots_are_evicted() {
    let fx = fixture(5);
    publish(&fx.log, 2).await;
    assert_eq!(fx.poll(G, 1).await.expect("poll"), vec![0]);
    assert_eq!(fx.poll(OTHER, 1).await.expect("poll"), vec![0]);
    assert!(
        fx.groups
            .delete(&key(OTHER), later())
            .await
            .expect("delete")
    );
    assert_eq!(fx.groups.tracked(), 1, "a deleted group kept its slot");

    assert_eq!(fx.poll(OTHER, 1).await.expect("poll"), vec![0]);
    fx.fake().new_term();
    assert_eq!(fx.poll(G, 2).await.expect("poll"), vec![1]);
    assert_eq!(fx.groups.tracked(), 1, "an old term's tracker was kept");

    fx.groups.step_down(&key(G));
    assert_eq!(fx.groups.tracked(), 0, "a former leader kept its trackers");

    // Its claim on 0 from the last term still stands.
    assert_eq!(fx.poll(OTHER, 1).await.expect("poll"), vec![1]);
    let now = Instant::now();
    fx.groups
        .extend(&key(OTHER), 1, 1, Duration::from_secs(3600), now, later())
        .await
        .expect("extend");
    let idle = now + Duration::from_secs(11 * 60);
    assert_eq!(
        fx.groups.evict_idle(idle),
        0,
        "a standing claim was evicted"
    );
    fx.groups.ack(&key(OTHER), 1, later()).await.expect("ack");
    assert_eq!(fx.groups.evict_idle(idle + Duration::from_secs(1)), 1);
    assert_eq!(fx.groups.tracked(), 0);
}

/// Dead letter, extend and discard through the public API.
#[tokio::test]
async fn dead_letter_extend_and_discard() {
    let fx = fixture(5);
    publish(&fx.log, 2).await;
    assert_eq!(fx.poll(G, 2).await.expect("poll"), vec![0, 1]);
    let now = Instant::now();
    fx.groups
        .extend(&key(G), 1, 1, VIS, now, later())
        .await
        .expect("extend");
    assert!(matches!(
        fx.groups.extend(&key(G), 1, 2, VIS, now, later()).await,
        Err(BrokerError::GroupClaimLapsed { offset: 1 })
    ));
    assert!(matches!(
        fx.groups.extend(&key(G), 5, 1, VIS, now, later()).await,
        Err(BrokerError::GroupOffsetNotHandedOut { offset: 5, .. })
    ));
    assert!(
        fx.groups
            .dead_letter(&key(G), 0, later())
            .await
            .expect("dead")
    );
    assert!(
        !fx.groups
            .dead_letter(&key(G), 0, later())
            .await
            .expect("again")
    );
    assert_eq!(fx.dead(G), vec![0]);
    assert_eq!(fx.cursor(G), Some(1));
    assert!(
        fx.groups
            .discard(&key(G), 0, later())
            .await
            .expect("discard")
    );
    assert!(fx.dead(G).is_empty());
    assert!(!fx.groups.discard(&key(G), 0, later()).await.expect("again"));
}

/// **A claim survives a change of leader** (#2644): the new leader hands
/// the record to nobody else, the holder can still extend and settle it,
/// and the attempt count carries over.
#[tokio::test]
async fn a_claim_survives_a_change_of_leader() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    let worker = GroupConsumer::new("svc", "w", 1, false);
    let now = Instant::now();
    assert_eq!(
        fx.poll_as(2, now, &worker).await.expect("poll"),
        [(0, 1), (1, 1)]
    );
    fx.fake().new_term();
    assert_eq!(
        fx.poll(G, 3).await.expect("poll"),
        vec![2],
        "a standing claim went to someone else"
    );
    fx.groups
        .extend(&key(G), 0, 1, VIS, Instant::now(), later())
        .await
        .expect("the holder extends its claim on the new leader");
    fx.groups.ack(&key(G), 1, later()).await.expect("ack");
    // Once they lapse they are owed again, at their next attempt.
    let lapsed = Instant::now() + VIS * 3;
    let other = GroupConsumer::new("svc", "other", 1, false);
    assert_eq!(
        fx.poll_as(3, lapsed, &other).await.expect("poll"),
        [(0, 2), (2, 2)]
    );
}

/// An extension is written too, so a new leader holds the claim for as
/// long as the extension asked, not the visibility timeout.
#[tokio::test]
async fn an_extension_survives_a_change_of_leader() {
    let fx = fixture(5);
    publish(&fx.log, 1).await;
    let worker = GroupConsumer::new("svc", "w", 1, false);
    let now = Instant::now();
    assert_eq!(fx.poll_as(1, now, &worker).await.expect("poll"), [(0, 1)]);
    fx.groups
        .extend(&key(G), 0, 1, VIS * 10, now, later())
        .await
        .expect("extend");
    fx.fake().new_term();
    let past_visibility = Instant::now() + VIS * 2;
    let other = GroupConsumer::new("svc", "other", 1, false);
    assert_eq!(
        fx.poll_as(1, past_visibility, &other).await.expect("poll"),
        [],
        "the new leader forgot the extension"
    );
}

/// A member that reconnects after a change of leader and reclaims takes its
/// claims back from the new leader, at no attempt (#2647).
#[tokio::test]
async fn a_reclaim_on_the_new_leader_takes_the_claims_back() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    let now = Instant::now();
    let before = GroupConsumer::new("svc", "w", 7, false);
    assert_eq!(
        fx.poll_as(2, now, &before).await.expect("poll"),
        [(0, 1), (1, 1)]
    );
    fx.fake().new_term();
    let other = GroupConsumer::new("svc", "other", 1, false);
    assert_eq!(
        fx.poll_as(3, Instant::now(), &other).await.expect("poll"),
        [(2, 1)]
    );
    // A connection id on the new broker says nothing about the old one's.
    let after = GroupConsumer::new("svc", "w", 1, true);
    assert_eq!(
        fx.poll_as(3, Instant::now(), &after).await.expect("poll"),
        [(0, 1), (1, 1)]
    );
}

/// A write whose entry was proposed and then lost its term, or whose wait
/// ran out, is unsettled: it may commit, so a retry could write it twice.
/// One refused before it was proposed is not.
#[tokio::test]
async fn only_a_write_refused_before_its_proposal_is_not_unsettled() {
    let fx = fixture(5);
    publish(&fx.log, 3).await;
    assert_eq!(fx.poll(G, 3).await.expect("poll"), vec![0, 1, 2]);

    fx.set(|s| s.new_term_before_propose = true);
    let refused = fx.groups.ack(&key(G), 0, later()).await;
    assert!(
        matches!(refused, Err(BrokerError::StateNotReadable(_))),
        "a write refused before its proposal answered {refused:?}"
    );

    assert_eq!(fx.poll(G, 3).await.expect("poll"), Vec::<u64>::new());
    fx.set(|s| s.new_term_after_propose = true);
    let lost = fx.groups.ack(&key(G), 0, later()).await;
    assert!(
        matches!(
            lost,
            Err(BrokerError::WriteUnsettled {
                timed_out: false,
                ..
            })
        ),
        "a write whose term ended after its proposal answered {lost:?}"
    );

    assert_eq!(fx.poll(G, 3).await.expect("poll"), Vec::<u64>::new());
    fx.set(|s| s.stalled = true);
    let waited = fx.groups.ack(&key(G), 0, later()).await;
    assert!(
        matches!(
            waited,
            Err(BrokerError::WriteUnsettled {
                timed_out: true,
                ..
            })
        ),
        "a write whose commit wait ran out answered {waited:?}"
    );
}
