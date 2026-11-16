//! The membership driver: the steps it takes for a target, what it does
//! with a refusal, and the two rules it checks on every step. The first
//! half runs it against a stand-in for the group, the second against real
//! groups.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::raft::group::Group;
use crate::raft::group::driver::Placed;
use crate::raft::group::driver::{self, Driver, Look, Member, MembershipProgress, Pass, Retry};
use crate::raft::group::membership::{Basis, Replication, StepError};
use crate::raft::group::placed::{Budget, Placement, Placing};
use crate::raft::lead::Lead;
use crate::raft::membership::{Step, Told, View, Wait};
use crate::raft::types::Membership;

fn m(broker: &str) -> MemberId {
    MemberId::new(broker, 1)
}

fn set(brokers: &[&str]) -> BTreeSet<MemberId> {
    brokers.iter().map(|broker| m(broker)).collect()
}

fn v(counter: u64) -> TargetVersion {
    TargetVersion {
        incarnation: 1,
        counter,
    }
}

fn target(brokers: &[&str], counter: u64) -> Target {
    Target {
        voters: set(brokers),
        version: v(counter),
    }
}

fn told(brokers: &[&str], counter: u64, term: u64) -> Told {
    Told {
        target: target(brokers, counter),
        term,
    }
}

fn settled(voters: &[&str], learners: &[&str], counter: u64) -> Membership {
    Membership::new(vec![set(voters)], set(learners), v(counter))
}

/// A leader's log as the driver sees it, with nothing behind it.
struct Log {
    leads: bool,
    term: u64,
    membership: Membership,
    /// Members whose log is short of the leader's.
    behind: BTreeSet<MemberId>,
    /// Every membership written, in order.
    written: Vec<Membership>,
    transfers: Vec<(u64, MemberId)>,
    /// What the next steps answer instead of being written.
    refusals: VecDeque<StepError>,
    /// Keep a voter that leaves as a learner, as openraft does when asked
    /// to retain it.
    keeps_leavers: bool,
    /// Never answer a step.
    hangs: bool,
    /// No entry of this term is committed yet.
    fresh: bool,
    /// Members that have not answered the leader lately.
    silent: BTreeSet<MemberId>,
    /// Placement transfers asked, after the target caught up.
    placed: Vec<(u64, MemberId)>,
    /// A transfer never lands, and this member leads the next term, as
    /// when no successor answers.
    transfers_fail: bool,
}

struct Fake {
    me: MemberId,
    log: parking_lot::Mutex<Log>,
}

const LAST: u64 = 10;

impl Fake {
    fn leading(membership: Membership) -> Arc<Self> {
        Arc::new(Self {
            me: m("a"),
            log: parking_lot::Mutex::new(Log {
                leads: true,
                term: 1,
                membership,
                behind: BTreeSet::new(),
                written: Vec::new(),
                transfers: Vec::new(),
                refusals: VecDeque::new(),
                keeps_leavers: false,
                hangs: false,
                fresh: false,
                silent: BTreeSet::new(),
                placed: Vec::new(),
                transfers_fail: false,
            }),
        })
    }

    fn written(&self) -> Vec<Membership> {
        self.log.lock().written.clone()
    }

    /// The writers' own refusals are left out on purpose: these tests are
    /// about what the driver checks itself.
    async fn write(&self, next: impl FnOnce(&Membership) -> Membership) -> Result<(), StepError> {
        if self.log.lock().hangs {
            std::future::pending::<()>().await;
        }
        let mut log = self.log.lock();
        if let Some(refusal) = log.refusals.pop_front() {
            return Err(refusal);
        }
        let next = next(&log.membership);
        log.membership = next.clone();
        log.written.push(next);
        Ok(())
    }
}

impl Member for Arc<Fake> {
    fn me(&self) -> &MemberId {
        &self.me
    }

    async fn look(&self) -> Option<Look> {
        let log = self.log.lock();
        let members = log
            .membership
            .voter_ids()
            .chain(log.membership.learner_ids());
        let members: Vec<MemberId> = members.filter(|member| *member != self.me).collect();
        let live = members
            .iter()
            .filter(|member| !log.silent.contains(*member))
            .cloned()
            .collect();
        let matched: BTreeMap<MemberId, u64> = members
            .into_iter()
            .map(|member| {
                let at = if log.behind.contains(&member) {
                    0
                } else {
                    LAST
                };
                (member, at)
            })
            .collect();
        log.leads.then(|| Look {
            basis: Basis {
                term: log.term,
                // Moves with every write, as the entry's position does.
                at: Some(openraft::LogId::new(
                    log.term.into(),
                    log.written.len() as u64,
                )),
                membership: log.membership.clone(),
                committed: true,
                own_term_committed: !log.fresh,
                commit_index: LAST,
            },
            replication: Replication {
                last_index: LAST,
                matched,
                live,
            },
        })
    }

    async fn add_learner(&self, _basis: &Basis, member: MemberId) -> Result<(), StepError> {
        self.write(|held| {
            let learners = held.learner_ids().chain([member]).collect();
            Membership::new(held.get_joint_config().clone(), learners, held.version())
        })
        .await
    }

    async fn change_voters(
        &self,
        _basis: &Basis,
        voters: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Result<(), StepError> {
        let keeps = self.log.lock().keeps_leavers;
        self.write(|held| {
            let left = held.voter_ids().filter(|voter| !voters.contains(voter));
            let learners = held.learner_ids().filter(|member| !voters.contains(member));
            let learners = learners.chain(left.filter(|_| keeps)).collect();
            Membership::new(vec![voters.clone()], learners, version)
        })
        .await
    }

    async fn leave_joint(&self, _basis: &Basis) -> Result<(), StepError> {
        self.write(|held| {
            let voters = held.get_joint_config().last().cloned().unwrap_or_default();
            Membership::new(vec![voters], BTreeSet::new(), held.version())
        })
        .await
    }

    async fn stamp(
        &self,
        _basis: &Basis,
        learners: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Result<(), StepError> {
        self.write(|held| Membership::new(held.get_joint_config().clone(), learners, version))
            .await
    }

    async fn transfer(&self, term: u64, to: MemberId) -> Option<u64> {
        let mut log = self.log.lock();
        let asked = (log.leads && log.term == term).then(|| {
            log.transfers.push((term, to));
            term
        });
        if asked.is_some() && log.transfers_fail {
            log.term += 1;
        }
        asked
    }

    async fn transfer_caught_up(&self, term: u64, to: MemberId, _within: Duration) -> Option<u64> {
        let mut log = self.log.lock();
        (log.leads && log.term == term).then(|| {
            log.placed.push((term, to));
            term
        })
    }
}

type Progress = watch::Receiver<MembershipProgress>;

fn driving(fake: &Arc<Fake>) -> (Driver<Arc<Fake>>, Progress) {
    let progress = Arc::new(watch::Sender::default());
    let seen = progress.subscribe();
    let driver = Driver::new(Arc::clone(fake), 2, Retry::default(), progress, placing());
    (driver, seen)
}

fn placing() -> Placing {
    let placement = Placement::default();
    let budget = Arc::new(Budget::new(&placement));
    Placing::new(
        placement,
        Duration::from_secs(1),
        Duration::from_secs(3),
        budget,
    )
}

#[tokio::test]
async fn the_driver_steps_until_the_membership_is_the_target() {
    let fake = Fake::leading(settled(&["a", "b"], &[], 1));
    let (mut driver, progress) = driving(&fake);
    let goal = target(&["a", "b", "c"], 2);

    let mut passes = Vec::new();
    loop {
        let pass = driver.pass_any(Some(&goal)).await;
        let again = driver.after(&pass);
        passes.push(pass);
        if again.is_none() {
            break;
        }
        assert_eq!(*progress.borrow(), MembershipProgress::Converging);
    }
    assert_eq!(
        passes,
        [Pass::Stepped, Pass::Stepped, Pass::Parked(Wait::Settled)]
    );
    assert_eq!(
        fake.written(),
        [
            settled(&["a", "b"], &["c"], 1),
            settled(&["a", "b", "c"], &[], 2)
        ]
    );
    assert_eq!(*progress.borrow(), MembershipProgress::Settled);
}

#[tokio::test]
async fn a_busy_step_is_tried_again_and_a_refused_one_is_not() {
    let fake = Fake::leading(settled(&["a", "b"], &["c"], 1));
    let (mut driver, progress) = driving(&fake);
    let goal = target(&["a", "b", "c"], 2);

    // openraft has a change in progress: the same step goes through later.
    let busy = StepError::Busy("a change is in progress".into());
    fake.log.lock().refusals.push_back(busy.clone());
    let pass = driver.pass_any(Some(&goal)).await;
    assert_eq!(pass, Pass::Again(busy));
    assert_eq!(driver.after(&pass), Some(Retry::default().first));
    assert_eq!(*progress.borrow(), MembershipProgress::Converging);

    // openraft refused the membership itself: asking again changes nothing.
    let refused = StepError::Refused("no such learner".into());
    fake.log.lock().refusals.push_back(refused.clone());
    let pass = driver.pass_any(Some(&goal)).await;
    assert_eq!(pass, Pass::Refused(refused));
    assert_eq!(driver.after(&pass), None);
    assert!(matches!(*progress.borrow(), MembershipProgress::Stuck(_)));
    assert_eq!(fake.written(), []);
}

/// A decision that names a set without the leader, which `next_step` never
/// does.
fn without_the_leader(_view: &View, _target: Option<&Target>) -> Step {
    Step::Change {
        voters: set(&["b", "c"]),
        version: v(2),
    }
}

fn leave(_view: &View, _target: Option<&Target>) -> Step {
    Step::LeaveJoint
}

#[tokio::test]
async fn no_membership_without_its_leader_is_written() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    let (mut driver, progress) = driving(&fake);
    driver.decide = without_the_leader;
    let goal = target(&["b", "c"], 2);

    let pass = driver.pass_any(Some(&goal)).await;
    assert!(matches!(pass, Pass::Broken(_)), "{pass:?}");
    assert_eq!(driver.after(&pass), None);
    assert!(matches!(*progress.borrow(), MembershipProgress::Stuck(_)));
    // Nothing more in this term, whatever the decision says now.
    driver.decide = crate::raft::membership::next_step;
    let pass = driver.pass_any(Some(&target(&["a", "b"], 3))).await;
    assert_eq!(pass, Pass::Halted);
    assert_eq!(fake.written(), []);

    // Leaving a joint is a write too.
    let joint = Membership::new(
        vec![set(&["a", "b", "c"]), set(&["b", "c"])],
        BTreeSet::new(),
        v(2),
    );
    let fake = Fake::leading(joint);
    let (mut driver, _progress) = driving(&fake);
    driver.decide = leave;
    let pass = driver.pass_any(Some(&goal)).await;
    assert!(matches!(pass, Pass::Broken(_)), "{pass:?}");
    assert_eq!(fake.written(), []);
}

/// Decisions that break the other two rules `allowed` checks.
fn stamp_without_the_leader(_view: &View, _target: Option<&Target>) -> Step {
    Step::Stamp {
        learners: BTreeSet::new(),
        version: v(2),
    }
}

fn transfer_to_itself(_view: &View, _target: Option<&Target>) -> Step {
    Step::Transfer(m("a"))
}

#[tokio::test]
async fn no_stamp_without_the_leader_and_no_transfer_to_itself() {
    let fake = Fake::leading(settled(&["b", "c"], &[], 1));
    let (mut driver, _progress) = driving(&fake);
    driver.decide = stamp_without_the_leader;
    let pass = driver.pass_any(Some(&target(&["b", "c"], 2))).await;
    assert!(matches!(pass, Pass::Broken(_)), "{pass:?}");
    assert_eq!(fake.written(), []);

    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    let (mut driver, _progress) = driving(&fake);
    driver.decide = transfer_to_itself;
    let pass = driver.pass_any(Some(&target(&["b", "c"], 2))).await;
    assert!(matches!(pass, Pass::Broken(_)), "{pass:?}");
    assert_eq!(fake.log.lock().transfers, []);
}

/// A fresh leader with no target is idle while its first entry commits,
/// not converging toward nothing.
#[tokio::test]
async fn a_fresh_leader_with_no_target_is_idle() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    fake.log.lock().fresh = true;
    let (mut driver, progress) = driving(&fake);
    let pass = driver.pass_any(None).await;
    assert_eq!(pass, Pass::Parked(Wait::NoTarget));
    assert_eq!(driver.after(&pass), None);
    assert_eq!(*progress.borrow(), MembershipProgress::Idle);
}

#[tokio::test]
async fn a_voter_kept_as_a_learner_stops_the_driver() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    fake.log.lock().keeps_leavers = true;
    let (mut driver, progress) = driving(&fake);

    let pass = driver.pass_any(Some(&target(&["a", "b"], 2))).await;
    assert!(matches!(pass, Pass::Broken(_)), "{pass:?}");
    assert_eq!(driver.after(&pass), None);
    assert!(matches!(*progress.borrow(), MembershipProgress::Stuck(_)));
    let pass = driver.pass_any(Some(&target(&["a", "b", "c"], 3))).await;
    assert_eq!(pass, Pass::Halted);
    assert_eq!(fake.written().len(), 1);

    // A new term starts clean.
    fake.log.lock().term = 2;
    fake.log.lock().keeps_leavers = false;
    let pass = driver.pass_any(Some(&target(&["a", "b", "c"], 3))).await;
    assert_eq!(pass, Pass::Stepped);
}

#[tokio::test]
async fn a_transfer_that_expires_is_asked_again_and_never_becomes_a_change() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    let (mut driver, _progress) = driving(&fake);
    let goal = target(&["b", "c"], 2);

    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Stepped);
    // Nothing is asked twice in one term.
    let pass = driver.pass_any(Some(&goal)).await;
    assert_eq!(pass, Pass::Waiting(Wait::Transferring));
    // The transfer expired and this member won the next term.
    fake.log.lock().term = 2;
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Stepped);

    assert_eq!(fake.log.lock().transfers, [(1, m("b")), (2, m("b"))]);
    assert_eq!(fake.written(), []);
}

#[tokio::test]
async fn the_wait_doubles_and_then_the_group_is_reported_stuck() {
    let fake = Fake::leading(settled(&["a", "b"], &["c"], 1));
    fake.log.lock().behind.insert(m("c"));
    let (mut driver, progress) = driving(&fake);
    let goal = target(&["a", "b", "c"], 2);
    let retry = Retry::default();

    let mut waits = Vec::new();
    for _ in 0..retry.attempts {
        let pass = driver.pass_any(Some(&goal)).await;
        assert_eq!(pass, Pass::Waiting(Wait::CatchingUp));
        waits.push(driver.after(&pass).expect("a wait"));
        assert_eq!(*progress.borrow(), MembershipProgress::Converging);
    }
    assert_eq!(waits[0], retry.first);
    assert_eq!(waits[1], retry.first * 2);
    assert!(waits.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(waits.iter().all(|wait| *wait <= retry.cap));

    let pass = driver.pass_any(Some(&goal)).await;
    assert_eq!(driver.after(&pass), Some(retry.stuck));
    assert!(matches!(*progress.borrow(), MembershipProgress::Stuck(_)));

    // The learner catches up: the count starts again.
    fake.log.lock().behind.clear();
    let pass = driver.pass_any(Some(&goal)).await;
    assert_eq!(pass, Pass::Stepped);
    assert_eq!(driver.after(&pass), Some(Duration::ZERO));
    assert_eq!(*progress.borrow(), MembershipProgress::Converging);
}

#[tokio::test(start_paused = true)]
async fn a_step_that_does_not_commit_is_looked_at_again() {
    let fake = Fake::leading(settled(&["a", "b"], &[], 1));
    fake.log.lock().hangs = true;
    let (mut driver, _progress) = driving(&fake);
    let pass = driver.pass_any(Some(&target(&["a", "b", "c"], 2))).await;
    assert_eq!(pass, Pass::Slow);
    assert!(driver.after(&pass).is_some());
}

async fn reported(progress: &mut Progress, want: MembershipProgress) {
    let seen = progress.wait_for(|now| *now == want);
    let waited = tokio::time::timeout(Duration::from_secs(3600), seen).await;
    waited
        .expect("the progress was never reported")
        .expect("the driver");
}

/// A step that timed out is not checked when it is dropped, so it is
/// checked when its entry shows up.
#[tokio::test(start_paused = true)]
async fn a_step_that_commits_late_is_still_checked() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    fake.log.lock().hangs = true;
    let (mut driver, _progress) = driving(&fake);
    let goal = target(&["a", "b"], 2);
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Slow);

    // Still not written: nothing to check yet.
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Slow);
    // Written late, with the voter that left kept as a learner.
    {
        let mut log = fake.log.lock();
        log.hangs = false;
        log.membership = settled(&["a", "b"], &["c"], 2);
        let late = log.membership.clone();
        log.written.push(late);
    }
    let pass = driver.pass_any(Some(&goal)).await;
    assert!(matches!(pass, Pass::Broken(_)), "{pass:?}");
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Halted);
}

/// Every transfer closes the shard for up to an election timeout, so a
/// successor that never takes over is not asked without end.
#[tokio::test(start_paused = true)]
async fn transfers_that_never_land_are_paced_and_reported() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    let (mut driver, progress) = driving(&fake);
    let goal = target(&["b", "c"], 2);
    let retry = Retry::default();

    for term in 1..=u64::from(retry.attempts) {
        fake.log.lock().term = term;
        let pass = driver.pass_any(Some(&goal)).await;
        assert_eq!(pass, Pass::Stepped, "term {term}");
        driver.after(&pass);
    }
    assert!(matches!(*progress.borrow(), MembershipProgress::Stuck(_)));
    // Elected again at once: no transfer is asked yet.
    fake.log.lock().term += 1;
    let pass = driver.pass_any(Some(&goal)).await;
    assert_eq!(pass, Pass::Waiting(Wait::NoSuccessor));
    assert_eq!(driver.after(&pass), Some(retry.stuck));
    assert!(matches!(*progress.borrow(), MembershipProgress::Stuck(_)));
    assert_eq!(fake.log.lock().transfers.len(), retry.attempts as usize);

    tokio::time::sleep(retry.stuck).await;
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Stepped);
    assert_eq!(fake.written(), []);
}

/// Each failed transfer ends a term, and the target is told again for the
/// next. Told again, the same target keeps the pacing of the transfers, and
/// the group stays reported stuck while the new term waits for it; a new
/// target starts the count again.
#[tokio::test(start_paused = true)]
async fn a_target_told_again_keeps_the_transfer_pacing() {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    fake.log.lock().transfers_fail = true;
    let (driver, progress) = driving(&fake);
    let (goal, goals) = watch::channel(None);
    let (_lead, leads) = watch::channel(Lead::default());
    let (_preferred, preference) = watch::channel(None);
    let stop = CancellationToken::new();
    let running = tokio::spawn(driver::run(driver, goals, leads, preference, stop.clone()));
    let retry = Retry::default();
    let term = || fake.log.lock().term;
    let asked = || fake.log.lock().transfers.len();
    let stuck = || matches!(*progress.borrow(), MembershipProgress::Stuck(_));

    for _ in 0..retry.attempts {
        goal.send_replace(Some(told(&["b", "c"], 2, term())));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(asked(), retry.attempts as usize);
    // The new term has been told nothing yet.
    assert!(stuck(), "{:?}", *progress.borrow());

    goal.send_replace(Some(told(&["b", "c"], 2, term())));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(asked(), retry.attempts as usize);
    assert!(stuck(), "{:?}", *progress.borrow());

    goal.send_replace(Some(told(&["b", "c"], 3, term())));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(asked(), retry.attempts as usize + 1);

    stop.cancel();
    running.await.expect("the driver");
}

#[allow(clippy::field_reassign_with_default)]
fn leading(term: u64) -> Lead {
    let mut lead = Lead::default();
    lead.term = Some(term);
    lead.open = true;
    lead
}

#[tokio::test(start_paused = true)]
async fn a_deposed_leader_writes_nothing_more() {
    let fake = Fake::leading(settled(&["a", "b"], &[], 1));
    fake.log.lock().leads = false;
    let (driver, mut progress) = driving(&fake);
    let (goal, goals) = watch::channel(None);
    let (lead, leads) = watch::channel(Lead::default());
    let stop = CancellationToken::new();
    let (_preferred, preference) = watch::channel(None);
    let running = tokio::spawn(driver::run(driver, goals, leads, preference, stop.clone()));

    // A member that does not lead takes no step for a target.
    goal.send_replace(Some(told(&["a", "b", "c"], 2, 1)));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(fake.written(), []);
    assert_eq!(*progress.borrow(), MembershipProgress::Idle);

    // Elected, it is deposed before its first step is written.
    fake.log.lock().refusals.push_back(StepError::NotLeader);
    fake.log.lock().leads = true;
    lead.send_replace(leading(1));
    reported(&mut progress, MembershipProgress::Converging).await;
    fake.log.lock().leads = false;
    lead.send_replace(Lead::default());
    reported(&mut progress, MembershipProgress::Idle).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(fake.written(), []);

    // Elected again, it takes no step on the target told for term 1: a
    // newer one may have been acted on since.
    fake.log.lock().term = 2;
    fake.log.lock().leads = true;
    lead.send_replace(leading(2));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(fake.written(), []);
    assert_eq!(*progress.borrow(), MembershipProgress::Idle);

    // Told the same target for term 2, it finishes from what the log holds.
    goal.send_replace(Some(told(&["a", "b", "c"], 2, 2)));
    reported(&mut progress, MembershipProgress::Settled).await;
    assert_eq!(
        fake.written().last(),
        Some(&settled(&["a", "b", "c"], &[], 2))
    );

    stop.cancel();
    running.await.expect("the driver");
    assert_eq!(*progress.borrow(), MembershipProgress::Idle);
}

/// A leader of `term` over a, b and c that placement would move to `to`.
fn placed_on(term: u64, to: &str) -> (Arc<Fake>, Driver<Arc<Fake>>) {
    let fake = Fake::leading(settled(&["a", "b", "c"], &[], 1));
    fake.log.lock().term = term;
    let (mut driver, _) = driving(&fake);
    driver.placing.prefer(Some(to.into()));
    (fake, driver)
}

/// Look until a placement transfer is asked, as `run` does: taking a permit
/// when queued and sleeping each hold. How long that took, or `None` if
/// none was asked within `limit`.
async fn hands_over(
    driver: &mut Driver<Arc<Fake>>,
    goal: Option<&Target>,
    limit: Duration,
) -> Option<Duration> {
    let start = tokio::time::Instant::now();
    while start.elapsed() < limit {
        let pass = driver.pass_any(goal).await;
        let again = driver.after(&pass);
        match pass {
            Pass::Placed(_, Placed::Asked) => return Some(start.elapsed()),
            Pass::Placed(_, Placed::Queued) => {
                let permit = driver.placing.budget.take().await;
                driver.placing.ready = Some(permit);
            }
            _ => tokio::time::sleep(again.unwrap_or(Duration::from_secs(1))).await,
        }
    }
    None
}

/// **A leader hands its leadership to placement's leader** once that
/// member has stayed caught up for the settle window and this leader has
/// led its term for the least time. Nothing is asked before.
#[tokio::test(start_paused = true)]
async fn a_leader_hands_over_to_placement_once_settled() {
    let (fake, mut driver) = placed_on(2, "b");
    let took = hands_over(&mut driver, None, Duration::from_secs(60)).await;
    let placement = Placement::default();
    let took = took.expect("a transfer to placement's leader");
    assert!(took >= placement.led_min.max(placement.settle), "{took:?}");
    assert!(
        took < placement.led_min + Duration::from_secs(2),
        "{took:?}"
    );
    assert_eq!(fake.log.lock().placed, [(2, m("b"))]);
    assert_eq!(fake.log.lock().transfers, []);
}

/// The broker that formed a group wins its first term, so that term is
/// handed over at once: there is no earlier leader to churn with.
#[tokio::test(start_paused = true)]
async fn a_first_term_is_handed_over_at_once() {
    let (fake, mut driver) = placed_on(1, "c");
    let took = hands_over(&mut driver, None, Duration::from_secs(60)).await;
    assert_eq!(took, Some(Duration::ZERO));
    assert_eq!(fake.log.lock().placed, [(1, m("c"))]);
}

/// **A target that lags never gets a transfer**: no shard is closed for
/// it and nothing is asked, however long it lags. Once it catches up it
/// still waits out the settle window.
#[tokio::test(start_paused = true)]
async fn a_lagging_target_gets_no_transfer() {
    let (fake, mut driver) = placed_on(2, "b");
    fake.log.lock().behind.insert(m("b"));
    let took = hands_over(&mut driver, None, Duration::from_secs(600)).await;
    assert_eq!(took, None);
    assert_eq!(fake.log.lock().placed, []);

    fake.log.lock().behind.clear();
    let took = hands_over(&mut driver, None, Duration::from_secs(60)).await;
    let took = took.expect("a transfer once caught up");
    assert!(took >= Placement::default().settle, "{took:?}");
}

/// **The settle window is continuous.** A target that was caught up, then
/// fell behind, waits out the whole window again once it is back.
#[tokio::test(start_paused = true)]
async fn a_target_that_falls_behind_settles_again() {
    let (fake, mut driver) = placed_on(2, "b");
    let settle = Placement::default().settle;
    // Caught up long enough before the leader has led its least time.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        driver.pass(None).await,
        Pass::Placed(_, Placed::Held(_))
    ));
    tokio::time::sleep(Duration::from_secs(8)).await;
    fake.log.lock().behind.insert(m("b"));
    assert!(matches!(
        driver.pass(None).await,
        Pass::Placed(_, Placed::Held(_))
    ));
    tokio::time::sleep(Duration::from_secs(30)).await;
    fake.log.lock().behind.clear();
    let took = hands_over(&mut driver, None, Duration::from_secs(60)).await;
    let took = took.expect("a transfer once settled again");
    assert!(took >= settle, "{took:?}");
}

/// A first term is handed over at once only while it is young: a
/// preference that comes later waits out the settle window.
#[tokio::test(start_paused = true)]
async fn a_late_preference_in_a_first_term_settles() {
    let (_fake, mut driver) = placed_on(1, "c");
    driver.placing.prefer(None);
    assert_eq!(driver.pass(None).await, Pass::Parked(Wait::NoTarget));
    tokio::time::sleep(Duration::from_secs(60)).await;
    driver.placing.prefer(Some("c".into()));
    let took = hands_over(&mut driver, None, Duration::from_secs(60)).await;
    assert!(took.expect("asked") >= Placement::default().settle);
}

/// A group with a voter down or behind keeps its leader: a transfer then
/// could leave a bare majority with no leader.
#[tokio::test(start_paused = true)]
async fn a_degraded_group_keeps_its_leader() {
    let (fake, mut driver) = placed_on(2, "b");
    fake.log.lock().silent.insert(m("c"));
    assert_eq!(
        hands_over(&mut driver, None, Duration::from_secs(120)).await,
        None
    );
    // A silent target is a lagging one.
    let (fake, mut driver) = placed_on(2, "b");
    fake.log.lock().silent.insert(m("b"));
    assert_eq!(
        hands_over(&mut driver, None, Duration::from_secs(120)).await,
        None
    );
    assert_eq!(fake.log.lock().placed, []);
}

/// Placement that names a broker with no voter in the group, or this
/// leader, asks nothing and parks the driver.
#[tokio::test(start_paused = true)]
async fn no_transfer_to_a_broker_that_is_not_a_voter() {
    for to in ["d", "a"] {
        let (fake, mut driver) = placed_on(2, to);
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(driver.pass(None).await, Pass::Parked(Wait::NoTarget));
        assert_eq!(fake.log.lock().placed, []);
    }
}

/// Membership work always comes first: a target's step is written before
/// any placement transfer, and the transfer follows once settled.
#[tokio::test(start_paused = true)]
async fn a_membership_step_comes_before_placement() {
    let fake = Fake::leading(settled(&["a", "b"], &[], 1));
    let (mut driver, _) = driving(&fake);
    driver.placing.prefer(Some("b".into()));
    let goal = target(&["a", "b", "c"], 2);
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Stepped);
    assert_eq!(driver.pass_any(Some(&goal)).await, Pass::Stepped);
    assert_eq!(fake.log.lock().placed, []);
    assert!(
        hands_over(&mut driver, Some(&goal), Duration::from_secs(5))
            .await
            .is_some()
    );
    assert_eq!(
        fake.written(),
        [
            settled(&["a", "b"], &["c"], 1),
            settled(&["a", "b", "c"], &[], 2)
        ]
    );
}

/// **A transfer that leaves this member leading backs off**: the next one
/// is asked no sooner than the backoff, less its jitter, and the backoff
/// doubles. A new preference starts again.
#[tokio::test(start_paused = true)]
async fn a_transfer_that_does_not_land_backs_off() {
    let (fake, mut driver) = placed_on(2, "b");
    let limit = Duration::from_secs(3600);
    assert!(hands_over(&mut driver, None, limit).await.is_some());
    let placement = Placement::default();
    let mut least = placement.backoff;
    for term in [3, 4] {
        // It won the election the transfer ended its term with.
        fake.log.lock().term = term;
        let took = hands_over(&mut driver, None, limit)
            .await
            .expect("asked again");
        assert!(took >= least * 8 / 10, "term {term}: {took:?}");
        assert!(
            took <= least * 12 / 10 + Duration::from_secs(2),
            "term {term}: {took:?}"
        );
        least *= 2;
    }
    assert_eq!(fake.log.lock().placed.len(), 3);

    fake.log.lock().term = 5;
    driver.placing.prefer(Some("c".into()));
    let took = hands_over(&mut driver, None, limit).await.expect("asked");
    assert!(took < placement.backoff, "{took:?}");
}

/// **A group waiting for the broker's budget still reacts**: a new target
/// wakes it and its step is written, and the transfer follows once a
/// permit is free.
#[tokio::test(start_paused = true)]
async fn a_group_waiting_for_a_permit_takes_a_target() {
    let fake = Fake::leading(settled(&["a", "b"], &[], 1));
    fake.log.lock().term = 2;
    let placement = Placement {
        permits: 1,
        ..Placement::default()
    };
    let budget = Arc::new(Budget::new(&placement));
    let taken = budget.take().await;
    let progress = Arc::new(watch::Sender::default());
    let placing = Placing::new(
        placement,
        Duration::from_secs(1),
        Duration::from_secs(3),
        Arc::clone(&budget),
    );
    let driver = Driver::new(Arc::clone(&fake), 2, Retry::default(), progress, placing);
    let (goal, goals) = watch::channel(None);
    let (_lead, leads) = watch::channel(leading(2));
    let (_preferred, preference) = watch::channel(Some("b".to_owned()));
    let stop = CancellationToken::new();
    let running = tokio::spawn(driver::run(driver, goals, leads, preference, stop.clone()));

    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(fake.log.lock().placed, []);
    goal.send_replace(Some(told(&["a", "b", "c"], 2, 2)));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        fake.written().last(),
        Some(&settled(&["a", "b", "c"], &[], 2))
    );
    assert_eq!(fake.log.lock().placed, []);

    drop(taken);
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(fake.log.lock().placed, [(2, m("b"))]);
    stop.cancel();
    running.await.expect("the driver");
}

/// The member of group 0 that leads, once an entry of its term is
/// committed.
async fn leader_of(cluster: &Cluster) -> Arc<Group> {
    eventually(|| {
        BROKERS.into_iter().find_map(|broker| {
            let status = cluster.runtime(broker).status(&shard(0))?;
            let ready = status.role == Role::Leader && status.committed == status.last_index;
            ready.then(|| cluster.runtime(broker).group(&shard(0)).expect("group"))
        })
    })
    .await
}

/// Voters `a` and `b`, and `c` open with nothing in its log.
async fn pair() -> (Cluster, Arc<Group>) {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    for broker in ["a", "b"] {
        cluster
            .runtime(broker)
            .initialize(&shard(0), set(&["a", "b"]))
            .await
            .expect("initialize");
    }
    let leader = leader_of(&cluster).await;
    (cluster, leader)
}

async fn latest(cluster: &Cluster, broker: &str) -> Membership {
    let member = cluster.runtime(broker).group(&shard(0)).expect("group");
    member
        .raft()
        .with_raft_state(|state| state.membership_state.effective().membership().clone())
        .await
        .expect("state")
}

/// Wait until every one of `brokers` holds `want` as its latest
/// membership.
async fn held_by(cluster: &Cluster, brokers: &[&str], want: &Membership) {
    for broker in brokers {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let held = latest(cluster, broker).await;
            if held == *want {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{broker} holds {held}, not {want}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn tell_all(cluster: &Cluster, target: &Target) -> Telling {
    Telling::start(cluster.runtimes(), shard(0), target.clone())
}

/// Watches every member until it is dropped. A member that leads with a
/// membership that leaves it out, or that kept a voter that left as a
/// learner, fails the test at `check`.
struct Watch {
    done: Arc<AtomicBool>,
    seen: tokio::task::JoinHandle<Vec<String>>,
}

impl Watch {
    fn start(cluster: &Cluster, ever_voters: BTreeSet<MemberId>) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let groups: Vec<Arc<Group>> = BROKERS
            .iter()
            .map(|broker| cluster.runtime(broker).group(&shard(0)).expect("group"))
            .collect();
        let stop = Arc::clone(&done);
        let seen = tokio::spawn(async move {
            let mut broken = Vec::new();
            let mut voters = ever_voters;
            while !stop.load(Ordering::Relaxed) {
                for group in &groups {
                    let Some(basis) = group.membership_basis().await else {
                        continue;
                    };
                    let held = &basis.membership;
                    if !held
                        .get_joint_config()
                        .iter()
                        .all(|set| set.contains(&group.member))
                    {
                        broken.push(format!("{} leads {held}", group.member));
                    }
                    let now: BTreeSet<MemberId> = held.voter_ids().collect();
                    let kept = held
                        .learner_ids()
                        .find(|learner| voters.contains(learner) && !now.contains(learner));
                    if let Some(kept) = kept {
                        broken.push(format!("{kept} was a voter and is a learner in {held}"));
                    }
                    voters.extend(now);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            broken
        });
        Self { done, seen }
    }

    async fn check(self) {
        self.done.store(true, Ordering::Relaxed);
        let broken = self.seen.await.expect("the watch");
        assert!(broken.is_empty(), "{broken:#?}");
    }
}

async fn progress_of(cluster: &Cluster, want: MembershipProgress) {
    eventually(|| {
        BROKERS.into_iter().find_map(|broker| {
            let status = cluster.runtime(broker).status(&shard(0))?;
            (status.role == Role::Leader && status.membership == want).then_some(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_adds_a_voter() {
    let (mut cluster, leader) = pair().await;
    drop(leader);
    let watch = Watch::start(&cluster, set(&["a", "b"]));

    let telling = tell_all(&cluster, &target(&["a", "b", "c"], 2));
    held_by(&cluster, &BROKERS, &settled(&["a", "b", "c"], &[], 2)).await;
    progress_of(&cluster, MembershipProgress::Settled).await;
    drop(telling);

    // An older target changes nothing, and is not kept, even told in a
    // newer term. The same version is kept again only for a newer term.
    let a = cluster.runtime("a");
    // Seed `a` at term 0, so what follows holds whether or not it led.
    a.retarget(&shard(0), target(&["a", "b", "c"], 2), 0);
    assert!(!a.retarget(&shard(0), target(&["a", "b"], 1), u64::MAX));
    assert!(a.retarget(&shard(0), target(&["a", "b", "c"], 2), u64::MAX - 1));
    assert!(!a.retarget(&shard(0), target(&["a", "b", "c"], 2), u64::MAX - 1));
    assert!(a.retarget(&shard(0), target(&["a", "b", "c"], 2), u64::MAX));
    watch.check().await;
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_removes_a_voter_and_keeps_no_learner() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = leader_of(&cluster).await;
    let lead = leader.member.broker.to_string();
    drop(leader);
    let stays = BROKERS
        .into_iter()
        .find(|broker| *broker != lead)
        .expect("a follower");
    let watch = Watch::start(&cluster, set(&BROKERS));

    let kept = [&*lead, stays];
    let _telling = tell_all(&cluster, &target(&kept, 2));
    held_by(&cluster, &kept, &settled(&kept, &[], 2)).await;
    progress_of(&cluster, MembershipProgress::Settled).await;
    watch.check().await;
    cluster.stop().await;
}

/// A transfer asked for a term the member no longer leads gives up
/// nothing: the successor was picked from that term's log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_for_a_term_not_led_is_not_asked() {
    let (mut cluster, leader) = pair().await;
    let term = leader.status().term;
    let other = if &*leader.member.broker == "a" {
        "b"
    } else {
        "a"
    };
    let to = cluster
        .runtime(other)
        .group(&shard(0))
        .expect("group")
        .member
        .clone();
    assert_eq!(leader.transfer_in(term - 1, to).await, None);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let status = leader.status();
    assert_eq!((status.role, status.term), (Role::Leader, term));
    let record = Command {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(b"still leading"),
        written: None,
        producer: None,
        expected: None,
        headers: None,
        publisher: None,
        trim: None,
        commit: false,
        group: false,
    };
    leader.raft().client_write(record).await.expect("write");
    drop(leader);
    cluster.stop().await;
}

/// The target leaves the leader out and names a broker with an empty log:
/// learner, grow, transfer, and the next leader shrinks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_off_the_leader_grows_transfers_and_shrinks() {
    let (mut cluster, leader) = pair().await;
    let lead = leader.member.broker.to_string();
    let record = Command {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(b"before the move"),
        written: None,
        producer: None,
        expected: None,
        headers: None,
        publisher: None,
        trim: None,
        commit: false,
        group: false,
    };
    let written = leader.raft().client_write(record).await.expect("write");
    let before = written.log_id.index;
    drop(leader);
    let other = if lead == "a" { "b" } else { "a" };
    let watch = Watch::start(&cluster, set(&["a", "b"]));

    let moved = [other, "c"];
    let _telling = tell_all(&cluster, &target(&moved, 2));
    held_by(&cluster, &moved, &settled(&moved, &[], 2)).await;
    let leads = leader_of(&cluster).await.member.broker.to_string();
    assert_ne!(leads, lead, "the member moved out still leads");

    // The record written before the move is on both members of the new set.
    for broker in moved {
        let group = cluster.runtime(broker).group(&shard(0)).expect("group");
        eventually(|| (group.status().committed >= Some(before)).then_some(())).await;
    }
    watch.check().await;
    cluster.stop().await;
}

/// The leader is cut off as the target arrives. Whoever is elected next
/// finishes the removal, handing over first if the target leaves it out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removal_is_finished_under_a_leader_change() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = leader_of(&cluster).await;
    let lead = leader.member.broker.to_string();
    drop(leader);
    let mut followers = BROKERS.into_iter().filter(|broker| *broker != lead);
    let stays = followers.next().expect("a follower");
    let leaves = followers.next().expect("a follower");
    let watch = Watch::start(&cluster, set(&BROKERS));

    cluster.net.muted.lock().insert(lead.clone());
    cluster.net.cut.lock().insert(lead.clone());
    // Only the member the target drops can ask for a vote, so it is the
    // one elected, and it has to hand over before the removal. Its handover
    // is lost until it has been seen leading: a muted successor cannot win,
    // and the new leader hands over within milliseconds of its election.
    cluster.net.muted.lock().insert(stays.into());
    cluster.net.lose_transfers.store(true, Ordering::Relaxed);
    let kept = [&*lead, stays];
    let _telling = tell_all(&cluster, &target(&kept, 2));
    let next = cluster.elected(0).await;
    assert_eq!(&*next.broker, leaves);

    // The new set needs the old leader's answer to commit.
    cluster.net.muted.lock().clear();
    cluster.net.cut.lock().clear();
    cluster.net.lose_transfers.store(false, Ordering::Relaxed);
    held_by(&cluster, &kept, &settled(&kept, &[], 2)).await;
    progress_of(&cluster, MembershipProgress::Settled).await;
    watch.check().await;
    cluster.stop().await;
}

/// **A leader does not hand over while a target told for an earlier term
/// is held.** Membership work may be pending behind it.
#[tokio::test(start_paused = true)]
async fn a_leader_does_not_place_while_a_target_is_untold() {
    let (fake, mut driver) = placed_on(2, "b");
    let held = told(&["a", "b", "c"], 1, 1);
    for _ in 0..40 {
        let pass = driver.pass(Some(&held)).await;
        assert_eq!(pass, Pass::Untold);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert_eq!(fake.log.lock().placed, []);
    assert!(!driver.placing.holds_permit());
}

/// **An untold pass in a later term gives back a permit held for placement
/// and counts a transfer asked earlier as failed**, so backoff applies.
#[tokio::test(start_paused = true)]
async fn an_untold_pass_settles_an_earlier_transfer() {
    let (fake, mut driver) = placed_on(2, "b");
    let permit = driver.placing.budget.take().await;
    driver.placing.ready = Some(permit);
    driver.placing.asked(2);
    assert!(driver.placing.holds_permit());
    fake.log.lock().term = 3;
    let held = told(&["a", "b", "c"], 1, 1);
    assert_eq!(driver.pass(Some(&held)).await, Pass::Untold);
    assert!(!driver.placing.holds_permit());
    assert_eq!(driver.placing.failures(), 1);

    // A permit taken and not used goes back too, without a second failure.
    let permit = driver.placing.budget.take().await;
    driver.placing.ready = Some(permit);
    assert_eq!(driver.pass(Some(&held)).await, Pass::Untold);
    assert!(!driver.placing.holds_permit());
    assert_eq!(driver.placing.failures(), 1);
}
