//! Reading what the brokers report about themselves.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::Cluster;
use crate::wait;

impl Cluster {
    /// The replicas `node_id` has stopped shipping to as a leader, from its
    /// `/replication/halted` listing. A halted replica is out of every quorum
    /// until it is rebuilt.
    pub(crate) async fn halted_replicas(&self, node_id: &str) -> Result<Vec<HaltedReplica>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        self.http
            .get(format!("http://{}/replication/halted", node.metrics_addr))
            .send()
            .await
            .context("read /replication/halted")?
            .json()
            .await
            .context("decode /replication/halted")
    }

    /// Read one counter or gauge from a broker's `/metrics`.
    ///
    /// `None` when the metric has never been recorded, which for a counter is
    /// the difference between "zero so far" and "this code path never ran" —
    /// the distinction a cross-broker test is usually making.
    pub async fn metric(&self, node_id: &str, name: &str) -> Result<Option<f64>> {
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow!("unknown node {node_id}"))?;
        let body = self
            .http
            .get(format!("http://{}/metrics", node.metrics_addr))
            .send()
            .await
            .context("scrape metrics")?
            .text()
            .await
            .context("read metrics body")?;

        let mut total = None;
        for line in body.lines() {
            if line.starts_with('#') {
                continue;
            }
            // `name`, `name{labels}`, then the value. Summed across label sets,
            // because a caller asking "did this happen" does not care which
            // label it happened under.
            let Some(rest) = line.strip_prefix(name) else {
                continue;
            };
            if !(rest.starts_with(' ') || rest.starts_with('{')) {
                continue;
            }
            if let Some(value) = rest.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
                *total.get_or_insert(0.0) += value;
            }
        }
        Ok(total)
    }

    /// Wait until a follower of `stream`'s shard 0 holds everything its leader
    /// has, as the control plane has heard it, and return the leader.
    ///
    /// Tests kill or pause a leader to watch a failover, and failover keeps
    /// only what the leader last reported a follower holds. So this waits for
    /// a report at the leader's current generation, made at or past the tail
    /// the leader has now, that names a caught-up follower whose position is
    /// that tail. Records acknowledged before the call are then on a follower
    /// placement would promote.
    ///
    /// The owner and its tail are read again on every poll: placement may
    /// move between a publish and this wait, and a report about the wrong
    /// broker would never satisfy it. The timeout says which broker led, its
    /// tail, and what the last report claimed about each replica.
    ///
    /// On a Raft shard nothing is reported for placement to promote from: a
    /// record acknowledged by the leader is already on a majority. The wait
    /// is then for an elected leader, and returns the broker that leads.
    pub async fn wait_for_replication(&self, stream: &str, timeout: Duration) -> Result<String> {
        if self.config.raft_brokers() {
            return self.wait_for_raft_leader(stream, timeout).await;
        }
        let budget = wait::budget(timeout);
        let deadline = Instant::now() + budget;
        loop {
            let why = match self.replication_level(stream).await {
                Ok((owner, Ok(_))) => return Ok(owner),
                Ok((owner, Err(short))) => format!("leader {owner}: {short}"),
                Err(err) => format!("{err:#}"),
            };
            if Instant::now() >= deadline {
                bail!(
                    "{stream} shard 0 had no follower holding its leader's tail within {budget:?}; {why}"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The broker that serves `stream`'s shard 0: placement's owner, or on
    /// Raft the member the group elected. Placement names a Raft shard's
    /// leader but cannot make the group elect it. After a failover the group
    /// keeps its elected leader while a voter is down, so the two can differ
    /// until the dead broker is back, and only the elected one serves.
    ///
    /// The broker that forms a Raft group leads it only until the group-start
    /// transfer hands leadership to placement's leader, so an elected leader
    /// that is not placement's is taken only once it has kept leading for a
    /// moment. Past the timeout the elected leader is returned as it stands.
    pub async fn serving_leader(&self, stream: &str, timeout: Duration) -> Result<String> {
        if !self.config.raft_brokers() {
            return self.owner(stream).await;
        }
        let deadline = Instant::now() + wait::budget(timeout);
        let settles = RAFT_LEADER_SETTLES.min(timeout / 2);
        let mut settle = LeaderSettle::default();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let leader = self
                .wait_for_raft_leader(stream, left.max(Duration::from_millis(100)))
                .await?;
            // A test may take the control plane down; then no placement
            // names a leader, and the elected one must hold for `settles`.
            let owner = match self.control_plane {
                Some(_) => self.owner(stream).await.ok(),
                None => None,
            };
            let now = Instant::now();
            if settle.settled(&leader, owner.as_deref(), now, settles) || now >= deadline {
                return Ok(leader);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// [`Self::serving_leader`] for shard 0 of `cache`: placement's owner,
    /// or on Raft the member leading at the highest term with a majority
    /// holding its commit index.
    pub async fn serving_cache_leader(&self, cache: &str, timeout: Duration) -> Result<String> {
        if !self.config.raft_brokers() {
            return self.shard_owner_of("cache", cache, 0).await;
        }
        let what = format!("a leader of cache {cache}");
        wait::until_some(timeout, &what, || async {
            self.raft_serving("cache", cache)
                .await
                .ok()
                .map(|(leader, _)| leader)
        })
        .await
    }

    /// [`Self::serving_leader`] once it is a broker other than `dead`,
    /// stepping placement meanwhile so a hybrid shard is promoted.
    pub async fn promoted_leader(
        &self,
        stream: &str,
        dead: &str,
        timeout: Duration,
    ) -> Result<String> {
        let what = format!("a leader of {stream} other than {dead}");
        wait::until_some(timeout, &what, || async {
            self.place_shards().await;
            let leader = if self.config.raft_brokers() {
                self.wait_for_raft_leader(stream, Duration::from_secs(1))
                    .await
                    .ok()
            } else {
                self.owner(stream).await.ok()
            };
            leader.filter(|leader| leader != dead)
        })
        .await
    }

    /// [`Self::wait_for_replication`], and on Raft every running broker's
    /// member holds the log through the leader's last entry, not only a
    /// majority. For a test that then wipes or stops a follower and needs the
    /// other one to be a full copy.
    pub async fn wait_for_every_member_level(
        &self,
        stream: &str,
        timeout: Duration,
    ) -> Result<String> {
        if !self.config.raft_brokers() {
            return self.wait_for_replication(stream, timeout).await;
        }
        let budget = wait::budget(timeout);
        let deadline = Instant::now() + budget;
        loop {
            let first = self
                .raft_members(felix_client::ShardKind::Stream, stream)
                .await;
            let then = self
                .raft_members(felix_client::ShardKind::Stream, stream)
                .await;
            let why = match raft_leader_caught_up(&first, &then) {
                Ok(leader) => {
                    let tail = first
                        .iter()
                        .find(|(id, _)| *id == leader)
                        .and_then(|(_, raft)| raft.last_index);
                    let short: Vec<&str> = then
                        .iter()
                        .filter(|(_, raft)| raft.last_index < tail)
                        .map(|(id, _)| id.as_str())
                        .collect();
                    if short.is_empty() {
                        return Ok(leader);
                    }
                    format!("{leader} leads at {tail:?}; behind: {short:?}")
                }
                Err(short) => short,
            };
            if Instant::now() >= deadline {
                bail!("{stream} shard 0 had members short of its leader within {budget:?}; {why}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The Raft counterpart of [`wait_for_replication`](Self::wait_for_replication).
    /// Publishes nothing, so a test counting records is unaffected.
    ///
    /// Waits for the broker that leads `stream`'s shard 0 itself, as its own
    /// member reports it, and for a majority of the group to hold the log up
    /// to the leader's last entry. A test that kills the leader next then
    /// loses nothing a `Leader` write or a single replica had acknowledged.
    async fn wait_for_raft_leader(&self, stream: &str, timeout: Duration) -> Result<String> {
        let budget = wait::budget(timeout);
        let deadline = Instant::now() + budget;
        loop {
            let first = self
                .raft_members(felix_client::ShardKind::Stream, stream)
                .await;
            let then = self
                .raft_members(felix_client::ShardKind::Stream, stream)
                .await;
            let why = match raft_leader_caught_up(&first, &then) {
                Ok(leader) => return Ok(leader),
                Err(short) => short,
            };
            if Instant::now() >= deadline {
                bail!(
                    "{stream} shard 0 had no leader with a caught-up majority within {budget:?}; {why}"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Whether the brokers run Raft groups rather than hybrid replication.
    pub(crate) fn raft(&self) -> bool {
        self.config.raft_brokers()
    }

    /// The member leading shard 0 of the `kind` (`stream` or `cache`) named
    /// `name`, by its own report, and its term, once a majority of the
    /// group holds the log through the leader's commit index. Raft's
    /// counterpart of a leader with a replica reported caught up.
    pub(crate) async fn raft_serving(
        &self,
        kind: &str,
        name: &str,
    ) -> Result<(String, u64), String> {
        let kind = if kind == "cache" {
            felix_client::ShardKind::Cache
        } else {
            felix_client::ShardKind::Stream
        };
        let first = self.raft_members(kind, name).await;
        let then = self.raft_members(kind, name).await;
        raft_leader_committed(&first, &then)
    }

    /// `node`'s member of shard 0 of stream `name`, by its own report: its
    /// role and term. `None` when the broker cannot be asked or runs no
    /// Raft member for it.
    pub async fn raft_member(&self, node: &str, name: &str) -> Option<felix_wire::InspectedRaft> {
        self.raft_member_of(node, name, 0).await
    }

    /// [`Self::raft_member`] for shard `shard`.
    pub async fn raft_member_of(
        &self,
        node: &str,
        name: &str,
        shard: u32,
    ) -> Option<felix_wire::InspectedRaft> {
        let addr = self.node(node)?.client_addr;
        let client = crate::client::connect(addr, &self.tenant_id, &self.inspector_token())
            .await
            .ok()?;
        let view = client
            .inspect_shard(
                felix_client::ShardKind::Stream,
                &self.tenant_id,
                &self.namespace,
                name,
                shard,
            )
            .await;
        drop(client);
        view.ok().and_then(|view| view.raft)
    }

    /// Every running broker's member of shard 0 of `name`, by its own
    /// report. A broker that cannot be asked is left out.
    async fn raft_members(
        &self,
        kind: felix_client::ShardKind,
        name: &str,
    ) -> Vec<(String, felix_wire::InspectedRaft)> {
        let mut members = Vec::new();
        for id in self.node_ids() {
            let Some(node) = self.node(&id) else { continue };
            let Ok(client) =
                crate::client::connect(node.client_addr, &self.tenant_id, &self.inspector_token())
                    .await
            else {
                continue;
            };
            let view = client
                .inspect_shard(kind, &self.tenant_id, &self.namespace, name, 0)
                .await;
            drop(client);
            if let Some(raft) = view.ok().and_then(|view| view.raft) {
                members.push((id, raft));
            }
        }
        members
    }

    /// The owner of `stream`'s shard 0, and whether its last report names a
    /// follower that holds the owner's tail as it stands now.
    async fn replication_level(&self, stream: &str) -> Result<(String, Result<String, String>)> {
        let owner = self.owner(stream).await.context("resolve the owner")?;
        let node = self
            .node(&owner)
            .ok_or_else(|| anyhow!("unknown node {owner}"))?;
        let client =
            crate::client::connect(node.client_addr, &self.tenant_id, &self.inspector_token())
                .await?;
        let view = client
            .inspect_shard(
                felix_client::ShardKind::Stream,
                &self.tenant_id,
                &self.namespace,
                stream,
                0,
            )
            .await
            .with_context(|| format!("inspect {stream} shard 0 on {owner}"))?;
        drop(client);
        if view.role != "leader" {
            return Ok((owner, Err(format!("its own role is {}", view.role))));
        }
        let Some(tail) = view.tail else {
            return Ok((owner, Err("its log is not open".to_string())));
        };
        let report = self.replica_report(stream, 0).await?;
        Ok((owner, holds_tail(report.as_ref(), view.generation, tail)))
    }
}

/// The follower `report` names as caught up and holding `tail`, or why there
/// is none.
///
/// `caught_up` alone is measured against what the leader had acknowledged when
/// it reported, which may be short of `tail`, so the follower's own position
/// has to reach it too.
fn holds_tail(
    report: Option<&felix_controlplane_service::model::ReplicaReport>,
    generation: Option<u64>,
    tail: u64,
) -> Result<String, String> {
    let Some(report) = report else {
        return Err(format!("tail {tail}, and no replica report yet"));
    };
    let summary = || {
        format!(
            "tail {tail} at generation {generation:?}; last report: generation {}, \
             leader at {:?}, caught up {:?}, replica offsets {:?}",
            report.generation, report.leader_offset, report.caught_up, report.offsets
        )
    };
    if generation.is_some_and(|generation| generation != report.generation) {
        return Err(format!("report is from another generation: {}", summary()));
    }
    if report.leader_offset.is_none_or(|reported| reported < tail) {
        return Err(format!("report predates the tail: {}", summary()));
    }
    report
        .caught_up
        .iter()
        .find(|follower| report.offsets.get(*follower).is_some_and(|at| *at >= tail))
        .cloned()
        .ok_or_else(|| format!("no caught-up replica at the tail: {}", summary()))
}

/// The broker leading the group, once a majority of its voting members hold
/// the log through the leader's last entry, or what is still short.
///
/// A leader is a member that is one and names itself as the leader it
/// follows: after a cut a deposed leader keeps its role but hears no one.
/// With two such members the later term leads. The leader is taken from
/// `first` and the members' logs from `then`, a later look: brokers are
/// asked one at a time, and under a workload a follower asked before the
/// leader is always short of it.
pub(crate) fn raft_leader_caught_up(
    first: &[(String, felix_wire::InspectedRaft)],
    then: &[(String, felix_wire::InspectedRaft)],
) -> Result<String, String> {
    raft_leader_holding(first, then, |led| led.last_index).map(|(leader, _)| leader)
}

/// [`raft_leader_caught_up`] measured against the leader's commit index
/// rather than its last entry, returning the leader and its term. Under a
/// steady workload the leader's last entry keeps moving ahead of its
/// followers; what it has committed a majority already holds.
pub(crate) fn raft_leader_committed(
    first: &[(String, felix_wire::InspectedRaft)],
    then: &[(String, felix_wire::InspectedRaft)],
) -> Result<(String, u64), String> {
    raft_leader_holding(first, then, |led| led.committed)
}

/// The voters are the leader's: a member removed from the group never hears
/// of it and goes on calling itself a follower, and a voter that could not be
/// asked still counts against the majority. Only a leader from a broker that
/// sends no voter list falls back to the members that call themselves voters.
fn raft_leader_holding(
    first: &[(String, felix_wire::InspectedRaft)],
    then: &[(String, felix_wire::InspectedRaft)],
    through: impl Fn(&felix_wire::InspectedRaft) -> Option<u64>,
) -> Result<(String, u64), String> {
    let Some((leader, led)) = first
        .iter()
        .filter(|(id, raft)| raft.role == "leader" && raft.leader.as_deref() == Some(id.as_str()))
        .max_by_key(|(_, raft)| raft.term)
    else {
        return Err(format!("no member leads yet; members: {first:?}"));
    };
    let Some(tail) = through(led) else {
        return Err(format!("{leader} leads with an empty log"));
    };
    let voters: Vec<&str> = if led.voters.is_empty() {
        then.iter()
            .filter(|(_, raft)| matches!(raft.role.as_str(), "leader" | "follower" | "candidate"))
            .map(|(id, _)| id.as_str())
            .collect()
    } else {
        led.voters.iter().map(String::as_str).collect()
    };
    let holding = voters
        .iter()
        .filter(|voter| {
            then.iter()
                .any(|(id, raft)| id == *voter && raft.last_index.is_some_and(|at| at >= tail))
        })
        .count();
    if holding * 2 > voters.len() {
        Ok((leader.clone(), led.term))
    } else {
        Err(format!(
            "{leader} leads at {tail}, held by {holding} of {} voting members {voters:?}: {then:?}",
            voters.len()
        ))
    }
}

/// How long a Raft leader placement did not name must keep leading before
/// [`Cluster::serving_leader`] takes it.
const RAFT_LEADER_SETTLES: Duration = Duration::from_secs(2);

/// Whether an elected Raft leader is the one that will go on serving: it is
/// placement's leader, or it has led without a break for `settles`.
#[derive(Debug, Default)]
struct LeaderSettle {
    since: Option<(String, Instant)>,
}

impl LeaderSettle {
    fn settled(
        &mut self,
        leader: &str,
        owner: Option<&str>,
        now: Instant,
        settles: Duration,
    ) -> bool {
        if owner == Some(leader) {
            return true;
        }
        let since = match &self.since {
            Some((held, at)) if held == leader => *at,
            _ => {
                self.since = Some((leader.to_string(), now));
                now
            }
        };
        now.duration_since(since) >= settles
    }
}

/// One entry of a broker's `/replication/halted`.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct HaltedReplica {
    /// `stream` or `cache`.
    pub(crate) kind: String,
    pub(crate) stream: String,
    pub(crate) shard: u32,
    /// The follower that stopped.
    pub(crate) node_id: String,
    pub(crate) reason: String,
}

#[cfg(test)]
mod tests;
