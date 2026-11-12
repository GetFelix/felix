//! Entering the group at startup: forming it on first boot, or rejoining
//! after this member lost its state.
//!
//! Raft's safety assumes a voter never forgets its vote or its log. A member
//! that starts with no state cannot tell a first boot from a wiped volume by
//! looking at itself, so it neither votes nor stands for election until it
//! has asked its peers:
//!
//! - some peer's log is past the initial membership entry: the group exists.
//!   This member follows it without a vote until it has applied everything
//!   the confirmed leader held when asked, then votes again;
//! - every configured member answers and is empty: first boot, but only if
//!   the operator said so (`InitialClusterState::New`). Every such member
//!   initializes with the same configured group, which openraft documents
//!   as safe. A member that does not answer blocks this: it may be the one
//!   that holds the group's data. Only the operator can say its machine is
//!   gone for good ([`super::waiver`]);
//! - otherwise it asks again.
//!
//! Peers answer only requests carrying this cluster's id and peer token, so
//! a member of some other group is never counted either way.
//!
//! The withheld vote is persisted, so a crash part-way through catching up
//! does not turn a half-filled log into a voter. The argument is in
//! `docs/metadata-raft-design.md` ("Rejoining after a lost volume").
use openraft::async_runtime::WatchReceiver;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{InitialClusterState, NodeId, RaftHandle, store, waiver};

/// What a member reports to one deciding how to enter the group.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Standing {
    pub(super) last_log_index: Option<u64>,
    /// [`super::AppStateMachine::version`]; absent from a build that
    /// predates it, which reads as 0.
    #[serde(default)]
    pub(super) version: u16,
}

/// The leader's answer to a rejoining member: apply up to here, then vote.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct CatchUpTarget {
    pub(super) index: u64,
}

/// What one peer's answer to `standing` says about forming the group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Answer {
    /// It answered, and its log holds nothing past the initial membership.
    Empty,
    /// Its log holds an entry at index 1 or above. Only a leader writes
    /// one, and a leader takes a formed majority. A vote or the initial
    /// membership at index 0 does not count: every first-boot member has
    /// those before anyone leads.
    Exists,
    /// Nothing there: the connection failed or timed out.
    Unreachable(String),
    /// Something is there and its answer cannot be used: a refusal, such as
    /// the 403 another cluster's member gives or a failed TLS handshake, or
    /// a body that is not a [`Standing`]. Neither empty nor a group.
    Unreadable(String),
}

/// What an empty member does with its peers' answers.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Decision {
    /// Catch up with the group that exists.
    Exists,
    /// Every member is empty, so this is a first boot.
    Form,
    /// Ask again; these are the answers in the way.
    Wait(Vec<String>),
}

/// Decide from the other members' `answers`, for a group of `members`.
///
/// Silence is never read as empty, except from a slot the operator `waived`,
/// and then only while the members that do answer empty, this one included,
/// are a majority. A waiver never excuses an answer that was given.
pub(super) fn decide(
    answers: &BTreeMap<NodeId, Answer>,
    waived: &BTreeSet<NodeId>,
    members: usize,
) -> Decision {
    if answers.values().any(|answer| *answer == Answer::Exists) {
        return Decision::Exists;
    }
    let mut empty = 1; // this member
    let mut waiting = Vec::new();
    for (id, answer) in answers {
        match answer {
            Answer::Empty => empty += 1,
            Answer::Unreachable(_) if waived.contains(id) => {}
            Answer::Unreachable(why) => waiting.push(format!("member {id} is unreachable: {why}")),
            Answer::Unreadable(why) => waiting.push(format!("member {id}: {why}")),
            Answer::Exists => unreachable!("returned above"),
        }
    }
    if waiting.is_empty() && empty <= members / 2 {
        waiting.push(format!(
            "only {empty} of {members} members answer empty, which is not a majority; \
             the form-without waiver names too many slots"
        ));
    }
    if waiting.is_empty() {
        Decision::Form
    } else {
        Decision::Wait(waiting)
    }
}

async fn probe(client: &reqwest::Client, base: Option<&str>) -> Answer {
    let Some(base) = base else {
        return Answer::Unreachable("no address configured".to_string());
    };
    let response = match client
        .get(format!("{base}/internal/raft/standing"))
        .send()
        .await
    {
        Ok(response) => response,
        // Only a peer that is not there is silent. One that refuses the
        // handshake or drops the request is up, and a waiver must not
        // excuse it.
        Err(err) if !tls_failure(&err) && (err.is_connect() || err.is_timeout()) => {
            return Answer::Unreachable(format!("{err:#}"));
        }
        Err(err) => return Answer::Unreadable(format!("the request failed: {err:#}")),
    };
    let status = response.status();
    if !status.is_success() {
        return Answer::Unreadable(format!("answered {status}"));
    }
    match response.json::<Standing>().await {
        Ok(Standing {
            last_log_index: Some(index),
            ..
        }) if index >= 1 => Answer::Exists,
        Ok(_) => Answer::Empty,
        Err(err) => Answer::Unreadable(format!("its answer did not decode: {err}")),
    }
}

/// Whether a request failed in TLS: the peer's certificate was not
/// accepted, or the peer refused this member's.
fn tls_failure(err: &(dyn std::error::Error + 'static)) -> bool {
    if err.is::<rustls::Error>() {
        return true;
    }
    // rustls errors arrive inside `io::Error`s, whose `source` skips the
    // error they wrap.
    let wrapped = err
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
        .map(|inner| inner as &(dyn std::error::Error + 'static));
    wrapped.or_else(|| err.source()).is_some_and(tls_failure)
}

const RETRY_INTERVAL: Duration = Duration::from_millis(200);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// A target can be a tail a deposed leader never commits; past this, ask
/// again rather than wait for it forever.
const CATCH_UP_WAIT: Duration = Duration::from_secs(30);
/// How often a member that may not form the group says why it is waiting.
const WAITING_LOG_INTERVAL: Duration = Duration::from_secs(10);

impl RaftHandle {
    /// Take this member's place in the group, in the background.
    ///
    /// A member that kept its state has nothing to do. One that starts empty,
    /// or was still catching up when it stopped, withholds its vote and works
    /// out whether to form the group or catch up with it, per the module
    /// docs. It forms a new group only when `initial` is
    /// [`InitialClusterState::New`] and every member in `peers` answers and
    /// is empty. Call before the RPC routes serve, so no vote leaks out
    /// first.
    pub fn enter_group(
        &self,
        peers: BTreeMap<NodeId, String>,
        initial: InitialClusterState,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let db = self.store()?;
        let holds_state = store::holds_state(&db)?;
        let waived = if holds_state {
            self.drop_waiver("this member holds raft state");
            BTreeSet::new()
        } else {
            self.waived_slots()?
        };
        if self.may_vote.load(Ordering::SeqCst) {
            if holds_state {
                return Ok(());
            }
            store::set_vote_withheld(&db, true)?;
            self.may_vote.store(false, Ordering::SeqCst);
            self.raft.runtime_config().elect(false);
        }
        let handle = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                () = handle.decide_and_join(peers, initial, waived) => {}
            }
        });
        Ok(())
    }

    /// The slots a `form-without` waiver in the data directory names.
    fn waived_slots(&self) -> anyhow::Result<BTreeSet<NodeId>> {
        let Some(slots) = waiver::load(&self.data_dir, &self.security.cluster_id)? else {
            return Ok(BTreeSet::new());
        };
        tracing::warn!(
            ?slots,
            "form-without waiver found: on a first start this member may form the raft group \
             without an answer from these slots"
        );
        Ok(slots)
    }

    /// A waiver is single-use, and a member with nothing to form has no use
    /// for it. Left behind, it would apply to some later empty start.
    fn drop_waiver(&self, why: &str) {
        match waiver::remove(&self.data_dir) {
            Ok(true) => tracing::warn!(why, "form-without waiver removed unused"),
            Ok(false) => {}
            Err(err) => tracing::error!(error = %err, "could not remove the form-without waiver"),
        }
    }

    async fn decide_and_join(
        &self,
        peers: BTreeMap<NodeId, String>,
        initial: InitialClusterState,
        waived: BTreeSet<NodeId>,
    ) {
        let client = match self.security.client(PROBE_TIMEOUT) {
            Ok(client) => client,
            Err(err) => {
                tracing::error!(error = %err, "cannot build the raft join client");
                return;
            }
        };
        // A member without an address stays in the list: it has not
        // answered, and that blocks forming like any other silence.
        let others: Vec<(NodeId, Option<String>)> = peers
            .iter()
            .filter(|(id, _)| **id != self.id)
            .map(|(id, addr)| (*id, self.peer_url(Some(*id), Some(addr))))
            .collect();
        let mut last_waiting_log: Option<tokio::time::Instant> = None;

        // Holding state while withholding the vote means an earlier catch-up
        // was interrupted; the group exists, so there is nothing to decide.
        if !self
            .store()
            .and_then(|db| store::holds_state(&db))
            .unwrap_or(true)
        {
            loop {
                let mut answers = BTreeMap::new();
                for (id, base) in &others {
                    answers.insert(*id, probe(&client, base.as_deref()).await);
                }
                let due = last_waiting_log.is_none_or(|at| at.elapsed() >= WAITING_LOG_INTERVAL);
                match decide(&answers, &waived, peers.len()) {
                    Decision::Exists => break,
                    Decision::Form if initial == InitialClusterState::New => {
                        self.form_group(peers).await;
                        return;
                    }
                    Decision::Form if due => {
                        tracing::warn!(
                            members = peers.len(),
                            "every raft member is empty and none holds the group; not forming \
                             a new one. If this is the cluster's first start, set \
                             FELIX_RAFT_INITIAL_CLUSTER_STATE=new; if volumes were lost, restore \
                             them or recover from an export"
                        );
                        last_waiting_log = Some(tokio::time::Instant::now());
                    }
                    Decision::Wait(waiting) if due => {
                        tracing::warn!(
                            ?waiting,
                            "not forming the raft group: a first start needs every configured \
                             member to answer and be empty, and no member reports a group to \
                             catch up with"
                        );
                        last_waiting_log = Some(tokio::time::Instant::now());
                    }
                    Decision::Form | Decision::Wait(_) => {}
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
        self.drop_waiver("the raft group exists");
        let others: Vec<String> = others.into_iter().filter_map(|(_, base)| base).collect();
        tracing::info!("raft group exists; catching up before this member votes");
        self.catch_up(&client, &others).await;
    }

    async fn form_group(&self, peers: BTreeMap<NodeId, String>) {
        // The waiver goes first, so it can never outlive the start it was
        // written for. A crash after this leaves an empty member with no
        // waiver, and the operator writes it again.
        match waiver::remove(&self.data_dir) {
            Ok(true) => tracing::warn!("forming the raft group under the form-without waiver"),
            Ok(false) => {}
            Err(err) => {
                tracing::error!(error = %err, "could not remove the form-without waiver; not forming");
                return;
            }
        }
        // Cleared before initialize writes anything: a crash in between
        // leaves an empty member, which simply decides again.
        if let Err(err) = self
            .store()
            .and_then(|db| store::set_vote_withheld(&db, false))
        {
            tracing::error!(error = %err, "could not record that this member may vote");
            return;
        }
        self.allow_votes();
        // A peer may have initialized first with the same configuration;
        // openraft reports that as an error, and this member resumes.
        if let Err(err) = self.initialize(peers).await {
            tracing::info!(error = %err, "raft group not initialized here (a peer was first)");
        }
    }

    async fn catch_up(&self, client: &reqwest::Client, others: &[String]) {
        loop {
            let mut target = None;
            for base in others {
                let url = format!("{base}/internal/raft/catch-up-target");
                if let Ok(response) = client.get(url).send().await
                    && response.status().is_success()
                    && let Ok(answer) = response.json::<CatchUpTarget>().await
                {
                    target = Some(answer.index);
                    break;
                }
            }
            let Some(target) = target else {
                tokio::time::sleep(RETRY_INTERVAL).await;
                continue;
            };
            let deadline = tokio::time::Instant::now() + CATCH_UP_WAIT;
            while tokio::time::Instant::now() < deadline {
                let applied = self
                    .raft
                    .metrics()
                    .borrow_watched()
                    .last_applied
                    .map(|log_id| log_id.index)
                    .unwrap_or(0);
                if applied >= target {
                    if let Err(err) = self
                        .store()
                        .and_then(|db| store::set_vote_withheld(&db, false))
                    {
                        tracing::error!(error = %err, "could not record that this member may vote");
                        return;
                    }
                    self.allow_votes();
                    tracing::info!(applied, "caught up with the raft group; voting again");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    fn store(&self) -> anyhow::Result<std::sync::Arc<redb::Database>> {
        self.db
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("raft node has stopped"))
    }

    fn allow_votes(&self) {
        self.may_vote.store(true, Ordering::SeqCst);
        self.raft.runtime_config().elect(true);
    }

    /// The leader's side of a rejoin: its last log index, but only once a
    /// quorum has confirmed it still leads. A deposed leader's answer could
    /// be short of what the group committed without it.
    pub(super) async fn catch_up_target(&self) -> Option<CatchUpTarget> {
        if !self.confirm_leadership().await {
            return None;
        }
        let index = self.raft.metrics().borrow_watched().last_log_index?;
        Some(CatchUpTarget { index })
    }

    /// Refuse to add member `id` at `addr` when it cannot apply what the
    /// group already depends on ([`super::AppStateMachine::join_floor`]).
    /// Added anyway, it would refuse those entries while the others apply
    /// them, and the two would diverge. Once there is a floor, a member that
    /// does not answer is refused too: nothing says it meets it.
    pub(super) async fn admit(&self, id: NodeId, addr: &str) -> anyhow::Result<()> {
        let floor = self.app.join_floor();
        if floor == 0 {
            return Ok(());
        }
        let base = self
            .peer_url(Some(id), Some(addr))
            .ok_or_else(|| anyhow::anyhow!("no address for raft member {id}"))?;
        match self.probe_version(id, &base).await {
            Some(version) if version >= floor => Ok(()),
            Some(version) => anyhow::bail!(
                "raft member {id} supports metadata version {version}, but the group is at \
                 {floor}; upgrade it to a release at metadata version {floor} or later before \
                 adding it"
            ),
            None => anyhow::bail!(
                "raft member {id} did not report its metadata version, and the group is at \
                 {floor}; start it on a release at metadata version {floor} or later before \
                 adding it"
            ),
        }
    }

    pub(super) fn standing(&self) -> Standing {
        Standing {
            last_log_index: self.raft.metrics().borrow_watched().last_log_index,
            version: self.app.version(),
        }
    }
}

#[cfg(test)]
mod tests;
