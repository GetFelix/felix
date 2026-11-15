//! Makes a group's membership match the control plane's target, on the
//! member that leads it. One task per group looks at the leader's log and
//! the newest target, takes the one step `next_step` names, and looks
//! again. What it keeps between looks only paces it (the term a transfer
//! was asked in, retry counts), so a new leader or a restart resumes from
//! the log. See
//! `docs/per-shard-raft.md`, "Membership".

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::Group;
use super::membership::{Basis, Replication, StepError};
use crate::raft::lead::Lead;
use crate::raft::membership::{Config, Step, Target, TargetVersion, View, Wait};
use crate::raft::types::MemberId;

pub(crate) const CONVERGING: &str = "felix_broker_raft_membership_converging";
pub(crate) const STUCK: &str = "felix_broker_raft_membership_stuck";
pub(crate) const STEPS_TOTAL: &str = "felix_broker_raft_membership_steps_total";
pub(crate) const REFUSALS_TOTAL: &str = "felix_broker_raft_membership_refusals_total";

/// Where a group's membership stands against its target, as the member
/// that leads it last saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum MembershipProgress {
    /// This member does not lead the group, or has no target newer than
    /// the membership.
    #[default]
    Idle,
    /// The membership is the target.
    Settled,
    /// The leader is taking steps toward the target, or waiting for one to
    /// commit or a learner to catch up.
    Converging,
    /// No step has gone through, and why. The leader goes on trying a step
    /// that can still succeed, slowly. It stops on one that cannot until
    /// the target or the term changes.
    Stuck(String),
}

/// How the driver paces its retries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Retry {
    /// The wait after the first look that changed nothing. It doubles up
    /// to `cap`.
    pub(crate) first: Duration,
    pub(crate) cap: Duration,
    /// Looks in a row that changed nothing before the group is reported
    /// stuck.
    pub(crate) attempts: u32,
    /// The wait between looks once the group is stuck.
    pub(crate) stuck: Duration,
    /// How long one step may take to commit before the driver looks again.
    /// The entry stays in the log either way.
    pub(crate) step: Duration,
}

impl Default for Retry {
    fn default() -> Self {
        Self {
            first: Duration::from_millis(50),
            cap: Duration::from_secs(5),
            attempts: 8,
            stuck: Duration::from_secs(30),
            step: Duration::from_secs(10),
        }
    }
}

/// What a leader's log held when the driver looked.
#[derive(Debug, Clone)]
pub(crate) struct Look {
    pub(crate) basis: Basis,
    pub(crate) replication: Replication,
}

/// The group as the driver uses it. A test stands in for the group here.
pub(crate) trait Member: Send + Sync {
    fn me(&self) -> &MemberId;

    /// `None` on a member that does not lead.
    fn look(&self) -> impl Future<Output = Option<Look>> + Send;

    fn add_learner(
        &self,
        basis: &Basis,
        member: MemberId,
    ) -> impl Future<Output = Result<(), StepError>> + Send;

    fn change_voters(
        &self,
        basis: &Basis,
        voters: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> impl Future<Output = Result<(), StepError>> + Send;

    fn leave_joint(&self, basis: &Basis) -> impl Future<Output = Result<(), StepError>> + Send;

    fn stamp(
        &self,
        basis: &Basis,
        learners: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> impl Future<Output = Result<(), StepError>> + Send;

    /// Ask for a transfer of the leadership of `term`. `None` if this
    /// member no longer leads it, and then nothing was asked.
    fn transfer(&self, term: u64, to: MemberId) -> impl Future<Output = Option<u64>> + Send;
}

impl Member for Arc<Group> {
    fn me(&self) -> &MemberId {
        &self.member
    }

    async fn look(&self) -> Option<Look> {
        let basis = self.membership_basis().await?;
        Some(Look {
            basis,
            replication: self.replication(),
        })
    }

    async fn add_learner(&self, basis: &Basis, member: MemberId) -> Result<(), StepError> {
        Group::add_learner(self, basis, member).await
    }

    async fn change_voters(
        &self,
        basis: &Basis,
        voters: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Result<(), StepError> {
        Group::change_voters(self, basis, voters, version).await
    }

    async fn leave_joint(&self, basis: &Basis) -> Result<(), StepError> {
        Group::leave_joint(self, basis).await
    }

    async fn stamp(
        &self,
        basis: &Basis,
        learners: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Result<(), StepError> {
        Group::stamp(self, basis, learners, version).await
    }

    async fn transfer(&self, term: u64, to: MemberId) -> Option<u64> {
        self.transfer_in(term, to).await
    }
}

/// What one look came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Pass {
    /// This member does not lead.
    NotLeading,
    /// A step was written, or a transfer asked for.
    Stepped,
    /// Nothing to do until the target or the term changes.
    Parked(Wait),
    /// Something this leader cannot hurry: a commit, a learner catching
    /// up, a transfer.
    Waiting(Wait),
    /// The step was not written, and may be on a later look.
    Again(StepError),
    /// The step did not commit in time. It may still.
    Slow,
    /// The step can never be written as decided.
    Refused(StepError),
    /// A rule every membership keeps would be, or was, broken.
    Broken(String),
    /// A rule was broken earlier in this term. Nothing more is written
    /// in it.
    Halted,
    /// The group's core has stopped.
    Closed,
}

pub(crate) struct Driver<M> {
    member: M,
    /// `next_step`. A test puts a wrong decision here.
    pub(crate) decide: fn(&View, Option<&Target>) -> Step,
    lag_threshold: u64,
    retry: Retry,
    /// The term this member last asked a transfer in. openraft ends that
    /// term whether or not the transfer goes through.
    transfer: Option<u64>,
    /// The term a broken rule was found in. Nothing more is written in it.
    halted: Option<u64>,
    /// Looks in a row that changed nothing.
    idle_looks: u32,
    /// Where the log and the other members stood at the last look. A
    /// learner that is behind but moving is not stuck.
    mark: Option<(u64, u64, u64)>,
    /// Transfers asked since the last write or the last new target. Each
    /// closes the shard for up to an election timeout, so a transfer that
    /// never lands is paced like any other retry.
    transfers: u32,
    last_transfer: Option<tokio::time::Instant>,
    /// A step that timed out: the term, the entry it was decided on and
    /// the voters then. Checked when its entry shows up.
    unchecked: Option<(u64, Basis)>,
    progress: Reported,
}

impl<M: Member> Driver<M> {
    pub(crate) fn new(
        member: M,
        lag_threshold: u64,
        retry: Retry,
        progress: Arc<watch::Sender<MembershipProgress>>,
    ) -> Self {
        Self {
            member,
            decide: crate::raft::membership::next_step,
            lag_threshold,
            retry,
            transfer: None,
            halted: None,
            idle_looks: 0,
            mark: None,
            transfers: 0,
            last_transfer: None,
            unchecked: None,
            progress: Reported(progress),
        }
    }

    /// Look once and take at most one step toward `target`.
    pub(crate) async fn pass(&mut self, target: Option<&Target>) -> Pass {
        let Some(look) = self.member.look().await else {
            return Pass::NotLeading;
        };
        let basis = &look.basis;
        if self.halted == Some(basis.term) {
            return Pass::Halted;
        }
        let me = self.member.me().clone();
        // A step that timed out may have been written since.
        if let Some((term, before)) = self.unchecked.take()
            && term == basis.term
        {
            if before.at == basis.at {
                self.unchecked = Some((term, before));
            } else if let Err(broken) = kept(&me, &before, basis) {
                self.halted = Some(basis.term);
                return Pass::Broken(broken);
            }
        }
        let moved: u64 = look.replication.matched.values().sum();
        let mark = Some((basis.term, basis.at.map_or(0, |at| at.index), moved));
        if self.mark != mark {
            self.mark = mark;
            self.idle_looks = 0;
        }
        let transferring = self.transfer == Some(basis.term);
        let view = view(&me, &look, transferring, self.lag_threshold);
        let step = (self.decide)(&view, target);
        if let Err(broken) = allowed(&view, &step) {
            self.halted = Some(basis.term);
            return Pass::Broken(broken);
        }
        let kind = kind(&step);
        let written = match step {
            Step::Wait(wait) => {
                return match wait {
                    Wait::NoTarget | Wait::StaleTarget | Wait::Settled => Pass::Parked(wait),
                    _ => Pass::Waiting(wait),
                };
            }
            Step::Transfer(to) => {
                let recent = |at: tokio::time::Instant| at.elapsed() < self.retry.stuck;
                if self.transfers >= self.retry.attempts && self.last_transfer.is_some_and(recent) {
                    return Pass::Waiting(Wait::NoSuccessor);
                }
                return match self.member.transfer(basis.term, to).await {
                    Some(term) => {
                        self.transfer = Some(term);
                        self.transfers = self.transfers.saturating_add(1);
                        self.last_transfer = Some(tokio::time::Instant::now());
                        metrics::counter!(STEPS_TOTAL, "step" => kind).increment(1);
                        Pass::Stepped
                    }
                    None => Pass::NotLeading,
                };
            }
            Step::AddLearner(member) => self.timed(self.member.add_learner(basis, member)).await,
            Step::Change { voters, version } => {
                self.timed(self.member.change_voters(basis, voters, version))
                    .await
            }
            Step::Stamp { learners, version } => {
                self.timed(self.member.stamp(basis, learners, version))
                    .await
            }
            Step::LeaveJoint => self.timed(self.member.leave_joint(basis)).await,
        };
        match written {
            None => {
                self.unchecked = Some((basis.term, basis.clone()));
                Pass::Slow
            }
            Some(Ok(())) => {
                metrics::counter!(STEPS_TOTAL, "step" => kind).increment(1);
                self.transfers = 0;
                let after = self.member.look().await;
                let after = after.filter(|after| after.basis.term == basis.term);
                match after.map_or(Ok(()), |after| kept(&me, basis, &after.basis)) {
                    Ok(()) => Pass::Stepped,
                    Err(broken) => {
                        self.halted = Some(basis.term);
                        Pass::Broken(broken)
                    }
                }
            }
            Some(Err(StepError::Closed)) => Pass::Closed,
            // Not joint any more: the log moved since the look.
            Some(Err(
                err @ (StepError::Moved
                | StepError::NotLeader
                | StepError::NotJoint
                | StepError::Busy(_)),
            )) => Pass::Again(err),
            Some(Err(
                err @ (StepError::WithoutLeader
                | StepError::Joint
                | StepError::OlderVersion
                | StepError::Refused(_)),
            )) => Pass::Refused(err),
        }
    }

    async fn timed<T>(&self, step: impl Future<Output = T>) -> Option<T> {
        tokio::time::timeout(self.retry.step, step).await.ok()
    }

    /// Record what `pass` came to. Returns how long to wait before the
    /// next look, or `None` to wait for the target or the term to change.
    pub(crate) fn after(&mut self, pass: &Pass) -> Option<Duration> {
        let stalled = match pass {
            Pass::NotLeading | Pass::Closed => {
                self.idle_looks = 0;
                self.progress.set(MembershipProgress::Idle);
                return None;
            }
            Pass::Parked(wait) => {
                self.idle_looks = 0;
                self.progress.set(match wait {
                    Wait::Settled => MembershipProgress::Settled,
                    _ => MembershipProgress::Idle,
                });
                return None;
            }
            Pass::Halted => return None,
            Pass::Stepped if self.transfers >= self.retry.attempts => {
                self.progress.set(MembershipProgress::Stuck(
                    "transfers asked did not go through".into(),
                ));
                return Some(Duration::ZERO);
            }
            Pass::Stepped => {
                self.idle_looks = 0;
                self.progress.set(MembershipProgress::Converging);
                return Some(Duration::ZERO);
            }
            Pass::Refused(err) => {
                metrics::counter!(REFUSALS_TOTAL, "kind" => "permanent").increment(1);
                self.progress
                    .set(MembershipProgress::Stuck(format!("refused: {err:?}")));
                return None;
            }
            Pass::Broken(broken) => {
                metrics::counter!(REFUSALS_TOTAL, "kind" => "broken").increment(1);
                self.progress
                    .set(MembershipProgress::Stuck(format!("stopped: {broken}")));
                return None;
            }
            Pass::Again(err) => {
                metrics::counter!(REFUSALS_TOTAL, "kind" => "transient").increment(1);
                format!("{err:?}")
            }
            Pass::Slow => "a step did not commit in time".into(),
            Pass::Waiting(wait) => format!("waiting: {wait:?}"),
        };
        self.idle_looks = self.idle_looks.saturating_add(1);
        if self.idle_looks > self.retry.attempts || self.transfers >= self.retry.attempts {
            self.progress.set(MembershipProgress::Stuck(stalled));
            return Some(self.retry.stuck);
        }
        self.progress.set(MembershipProgress::Converging);
        let doubled = self
            .retry
            .first
            .saturating_mul(1 << (self.idle_looks - 1).min(16));
        Some(doubled.min(self.retry.cap))
    }

    /// A new target starts every count again.
    fn retargeted(&mut self) {
        self.idle_looks = 0;
        self.transfers = 0;
    }

    /// A new term starts the count of idle looks again. Transfers asked
    /// are still counted: each one that fails ends in a new term.
    fn led(&mut self) {
        self.idle_looks = 0;
    }
}

/// Drive `driver`'s group until `stop`, or until the group's core is gone.
pub(crate) async fn run<M: Member>(
    mut driver: Driver<M>,
    mut target: watch::Receiver<Option<Target>>,
    mut lead: watch::Receiver<Lead>,
    stop: CancellationToken,
) {
    loop {
        let goal = target.borrow_and_update().clone();
        if lead.borrow_and_update().gone() {
            break;
        }
        let pass = tokio::select! {
            biased;
            () = stop.cancelled() => break,
            pass = driver.pass(goal.as_ref()) => pass,
        };
        if let Pass::Broken(broken) = &pass {
            tracing::error!(member = %driver.member.me(), %broken, "a group's membership is no longer driven in this term");
        }
        if let Pass::Refused(err) = &pass {
            tracing::warn!(member = %driver.member.me(), error = ?err, "a membership step was refused");
        }
        let wait = driver.after(&pass);
        if pass == Pass::Closed {
            break;
        }
        let waited = async {
            match wait {
                Some(wait) => tokio::time::sleep(wait).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            changed = target.changed() => {
                if changed.is_err() {
                    break;
                }
                driver.retargeted();
            }
            changed = lead.changed() => {
                if changed.is_err() {
                    break;
                }
                driver.led();
            }
            () = waited => {}
        }
    }
}

fn view(me: &MemberId, look: &Look, transferring: bool, lag_threshold: u64) -> View {
    let basis = &look.basis;
    let sets = basis.membership.get_joint_config();
    let old = sets.first().cloned().unwrap_or_default();
    View {
        me: me.clone(),
        config: Config {
            new: sets.last().cloned().unwrap_or_default(),
            old,
            learners: basis.membership.learner_ids().collect(),
            version: basis.membership.version(),
        },
        config_committed: basis.committed,
        own_term_committed: basis.own_term_committed,
        transferring,
        // No broker acknowledges a restore incarnation yet.
        acked_incarnation: 0,
        last_index: look.replication.last_index,
        commit_index: basis.commit_index,
        matched: look.replication.matched.clone(),
        lag_threshold,
    }
}

/// What a step left in the log, checked against the two rules the writers
/// keep: the leader is a voter of every set, and no voter that left stayed
/// as a learner (#2498).
fn kept(me: &MemberId, before: &Basis, after: &Basis) -> Result<(), String> {
    let sets = after.membership.get_joint_config();
    if !sets.iter().all(|voters| voters.contains(me)) {
        return Err(format!(
            "this leader wrote {}, which leaves it out",
            after.membership
        ));
    }
    let voters: BTreeSet<MemberId> = after.membership.voter_ids().collect();
    let left = before.membership.voter_ids();
    let mut left = left.filter(|voter| !voters.contains(voter));
    let learners: BTreeSet<MemberId> = after.membership.learner_ids().collect();
    match left.find(|voter| learners.contains(voter)) {
        Some(voter) => Err(format!("{voter} left the voters and was kept as a learner")),
        None => Ok(()),
    }
}

/// Whether `step` may be written by the leader `view` describes. The
/// decision never names a step that fails this, and the writers refuse one
/// too. A leader that wrote itself out would go on leading, with a lease
/// on a set it is not in.
pub(crate) fn allowed(view: &View, step: &Step) -> Result<(), String> {
    let me = &view.me;
    match step {
        Step::Change { voters, .. } if !voters.contains(me) => Err(format!(
            "a change to {voters:?} would leave this leader out"
        )),
        Step::LeaveJoint if !view.config.new.contains(me) => Err(format!(
            "leaving the joint for {:?} would leave this leader out",
            view.config.new
        )),
        Step::Stamp { .. } if !view.config.new.contains(me) => {
            Err("a stamp on voters that leave this leader out".into())
        }
        Step::Transfer(to) if to == me => Err("a transfer to this leader itself".into()),
        _ => Ok(()),
    }
}

fn kind(step: &Step) -> &'static str {
    match step {
        Step::AddLearner(_) => "add_learner",
        Step::Change { .. } => "change",
        Step::Stamp { .. } => "stamp",
        Step::LeaveJoint => "leave_joint",
        Step::Transfer(_) => "transfer",
        Step::Wait(_) => "wait",
    }
}

/// The group's progress, published and counted in the broker's gauges.
struct Reported(Arc<watch::Sender<MembershipProgress>>);

impl Drop for Reported {
    /// A driver that is gone drives nothing, however it ended.
    fn drop(&mut self) {
        self.set(MembershipProgress::Idle);
    }
}

impl Reported {
    fn set(&self, now: MembershipProgress) {
        self.0.send_if_modified(|held| {
            if *held == now {
                return false;
            }
            gauge(held, -1.0);
            gauge(&now, 1.0);
            *held = now;
            true
        });
    }
}

fn gauge(progress: &MembershipProgress, by: f64) {
    match progress {
        MembershipProgress::Converging => metrics::gauge!(CONVERGING).increment(by),
        MembershipProgress::Stuck(_) => metrics::gauge!(STUCK).increment(by),
        MembershipProgress::Idle | MembershipProgress::Settled => {}
    }
}
