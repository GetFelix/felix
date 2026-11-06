//! A promoted leader fences a majority before it serves.
//!
//! `AnswerFence` and `OpenForWrites` in `docs/formal/FelixShard.tla`, under
//! `FenceOnPromote`. The new leader has already persisted its own generation
//! when it opened the shard; here it asks every replica to take it too. Once a
//! majority, itself included, has, no older leader can find a majority for
//! anything, because every majority it could reach includes a replica that
//! now refuses it. Before opening for writes the leader takes the log of the
//! answer furthest ahead by (last generation, length), if that is ahead of its
//! own: whatever a majority held when it answered is in that log.
//!
//! A cache shard is fenced the same way, its cache log first and then its
//! counter log, each taking the log furthest ahead among the answers.
//!
//! A stream shard's dead-letter and group cursor logs are fenced beside its
//! records, both in one round, and taken together from the one replica
//! whose pair is furthest ahead: a cursor is only good with the dead letters
//! its leader had written before it. See [`fence_groups`].
//!
//! Only when every replica offered the capabilities. Otherwise the shard
//! opens as it always has, on the lease, and a mixed fleet behaves exactly as
//! before. See "Fencing a promotion" in `docs/replication-design.md`.
//!
//! A failover's assignment names the storage identity each replica had in
//! the report it was decided on ([`Route::fence_storage_ids`]). An answer
//! counts only from that copy, by the identity its connection's handshake
//! gave: a replica wiped or restarted after a crash under a new identity may
//! have lost what it answered for before (#1335). `FenceCounts` in the model.

use std::collections::BTreeMap;
use std::sync::Arc;

use felix_broker::replication::{self, Divergence};
use felix_broker::{Broker, LogKind};
use felix_router::{Route, ShardKey, ShardKind};
use felix_storage::StorageId;
use felix_wire::internal::{
    ErrorCode, Fence, FenceElection, FenceOk, InternalMessage, PeerCapabilities, ReplicaLog,
    ReplicaSetView, ReplicateFetch, ShardRef,
};
use futures::StreamExt;

use crate::metrics;
use crate::peer::{PeerError, PeerRequester};
use crate::replica::last_generation;

pub(crate) mod joint;

pub use joint::Joint;
use joint::JointSets;
pub(crate) use joint::set_before;

/// How much one catch-up read asks a replica for.
const FETCH_BYTES: u32 = 1024 * 1024;

/// What a replica must have offered for a stream shard to be fenced.
pub const REQUIRED: PeerCapabilities = PeerCapabilities::FENCE.union(PeerCapabilities::TAIL_FETCH);

/// What a replica must have offered for a cache shard to be fenced: its
/// counter log is fenced and read as well.
pub const REQUIRED_FOR_CACHES: PeerCapabilities = REQUIRED.union(PeerCapabilities::CACHE_FENCE);

/// Where a promoted shard waits before it serves, as the broker keeps it.
#[async_trait::async_trait]
pub trait PromotionGate: Send + Sync {
    /// The generation `key` is waiting at to be opened for writes, if it is.
    fn awaiting(&self, key: &crate::ShardKey) -> Option<u64>;
    /// Whether `key`'s fence at `generation` continues a generation this
    /// process served itself: the next one, not draining, with no restart
    /// between. Its log then holds every record acknowledged before it.
    fn continuing(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        false
    }
    /// Open `key` for writes at `generation`.
    ///
    /// Once the fleet has finalized `generation_start`, the broker writes its
    /// generation-start record first. False when the shard stays closed and
    /// waits for another fence.
    async fn open(&self, key: &crate::ShardKey, generation: u64) -> bool;
}

/// A gate nothing waits at, for a broker that does not fence.
pub struct NoGate;

#[async_trait::async_trait]
impl PromotionGate for NoGate {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        None
    }
    async fn open(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        true
    }
}

/// How a promoted shard came to open, or why it has not yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A majority took the fence. `caught_up_from` names the replica whose
    /// log was ahead and was taken.
    Fenced { caught_up_from: Option<String> },
    /// Some replica did not offer the fence, so the shard opens on the lease.
    Lease { lacking: String },
    /// Not yet, and not in this attempt: a replica promised this generation
    /// to `promised_to`, and either so many did that no majority is left to
    /// take it or every other replica that did not take it was unreachable
    /// or timed out. Or, with `promised_to` empty, a replica's ballot, or this
    /// broker's own, holds a newer generation, which may be one no assignment
    /// named. The shard stays closed, and this broker reports the refusal so
    /// the control plane names it again above the generation (#1730).
    Refused {
        promised_to: String,
        /// A newer generation this broker's own copy accepted for another
        /// node, which placement names it above; 0 when there is none.
        ballot: u64,
        why: String,
        took: Vec<String>,
    },
    /// Not at this generation: under `joint_fence` the set it leaves is not
    /// known here, though this log keeps the generation's own set, as after
    /// a restart at a generation opened before the feature was on. The shard
    /// stays closed, and this broker reports the generation refused so the
    /// control plane names it again one up, where the set before is the one
    /// it is named on now (#1872). With no kept set the fence stays
    /// [`Self::Pending`] instead.
    SetUnknown {
        why: String,
        /// The newest generation past this one that a replica of the set
        /// accepted, which placement names this broker above; 0 when none
        /// did. One up could be a ballot a withdrawn standing left, which
        /// this broker's fence would then find promised away (#2046).
        ballot: u64,
    },
    /// Not yet: no majority answered, or the catch-up did not finish. The
    /// shard stays closed and the next pass tries again.
    Pending {
        why: String,
        /// The replicas that took the fence in this attempt, on the log it
        /// stopped at. What an operator reads to see who is missing.
        took: Vec<String>,
    },
}

impl Outcome {
    fn pending(why: impl Into<String>, took: Vec<String>) -> Self {
        Outcome::Pending {
            why: why.into(),
            took,
        }
    }
}

/// Fence `route`'s replicas for the shard this broker was just promoted to
/// lead, and take the tail of any ahead of it.
///
/// Does not open the shard; the caller does, on anything but `Pending`.
///
/// Without `lease_fallback` the shard never opens on the lease: a replica
/// whose capabilities are unknown or lacking is one that has not answered,
/// and the shard waits for a majority that has. That is the rule once the
/// followers decide acknowledgements, since an older leader then no longer
/// stops writing when its lease lapses.
///
/// With `joint`, given once the fleet has finalized `joint_fence`, a leader
/// whose set changed fences the set it leaves as well and needs a majority of
/// each (see [`Joint`]). Without it, or when no set changed, the fence is the
/// one it always was.
pub async fn fence_shard<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    local_node_id: &str,
    key: &ShardKey,
    route: &Route,
    lease_fallback: bool,
    joint: Option<&Joint<'_>>,
) -> Outcome {
    fence_shard_as(
        requester,
        broker,
        local_node_id,
        key,
        route,
        lease_fallback,
        joint,
        None,
    )
    .await
}

/// [`fence_shard`], for a broker standing for election when `candidacy` is
/// given: the fence says so, and carries the set the candidate keeps, with
/// its own generation, rather than the route's. Standing changes no set, so
/// the kept set is not rewritten either.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fence_shard_as<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    local_node_id: &str,
    key: &ShardKey,
    route: &Route,
    lease_fallback: bool,
    joint: Option<&Joint<'_>>,
    candidacy: Option<&crate::election::Candidacy>,
) -> Outcome {
    let joint_fence = joint.is_some();
    // A continuing leader holds every record acknowledged before this
    // generation, so its fence needs only promises (#2047).
    let continuing = candidacy.is_none() && joint.is_some_and(Joint::continuing);
    let replicas: Vec<_> = route
        .replicas
        .iter()
        .filter(|replica| replica.node_id != local_node_id)
        .collect();
    // The shard's own log first: its fence is what refuses the old leader on
    // every log the shard has. A cache's counters are fenced after it, for
    // where each replica's counter log ends.
    let (required, logs): (_, &[LogKind]) = match key.kind {
        ShardKind::Stream => (REQUIRED, &[LogKind::Stream]),
        ShardKind::Cache => (REQUIRED_FOR_CACHES, &[LogKind::Cache, LogKind::Counters]),
    };

    // Every replica, not just a majority. A fenced majority keeps an older
    // leader out whoever the rest are, but what the fence is for later --
    // acknowledging on the followers alone -- counts every follower's promise.
    for replica in replicas.iter().filter(|_| lease_fallback) {
        let offered = match requester.recorded_capabilities(&replica.node_id) {
            Some(offered) => Some(offered),
            None => requester
                .capabilities(&replica.node_id, replica.advertise_addr)
                .await
                .ok(),
        };
        if !offered.is_some_and(|offered| offered.contains(required)) {
            return Outcome::Lease {
                lacking: replica.node_id.clone(),
            };
        }
    }

    let shard = crate::incarnation::shard_ref(broker, key, route.generation);
    let reported_ids = route.fence_storage_ids.as_deref();
    // The set this leader counts, for replicas that check sets.
    let set = requester.offers_election().then(|| match candidacy {
        Some(candidacy) => candidacy.set.clone(),
        None => crate::replica_set::view(route, None),
    });
    let own_logs = async {
        let mut caught_up_from = None;
        for &kind in logs {
            let Some(log) = broker
                .shard_log_for(
                    kind,
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    None,
                    crate::incarnation::expected(broker, key.kind, &shard),
                )
                .await
            else {
                // A log that should be here and could not be opened is not a
                // shard with nothing to fence. Opening it anyway would serve
                // unfenced where the lease no longer stops the old leader.
                let lacking = match kind {
                    LogKind::Stream => {
                        return Err(Outcome::pending(
                            "this broker has no log for the shard",
                            Vec::new(),
                        ));
                    }
                    // A cache kept in memory replicates nothing, so there is no
                    // log to fence and none a pass could acknowledge on.
                    LogKind::Cache if !broker.cache().has_log() => {
                        return Err(Outcome::Lease {
                            lacking: "a cache log on this broker".to_string(),
                        });
                    }
                    LogKind::Cache => "this broker could not open the shard's cache log",
                    // No counter store here, so no counters to take.
                    LogKind::Counters if broker.counters().is_none() => continue,
                    _ => "this broker could not open the shard's counter log",
                };
                if lease_fallback {
                    return Err(Outcome::Lease {
                        lacking: lacking.to_string(),
                    });
                }
                return Err(Outcome::pending(lacking, Vec::new()));
            };
            // The counter log carries no set, and is fenced on the new set
            // alone, as the model's `Counters` are.
            let set = set.as_ref().filter(|_| kind != LogKind::Counters);
            // A copy marked a learner on evidence of a wipe lost what it held; a
            // staged destination is a learner too, and may lead.
            if set.is_some()
                && joint.is_some_and(|joint| joint.is_wiped_learner(&log, local_node_id, route))
            {
                return Err(Outcome::pending(
                    format!(
                        "this broker is a learner of its kept replica set and cannot lead generation {}",
                        route.generation
                    ),
                    Vec::new(),
                ));
            }
            // This copy already promised a newer generation, as a standing
            // here that was given up leaves it, so `keep_own_set` can never
            // take this one's ballot. Refused before any replica is asked, so
            // the control plane names this broker again above it (#1940). This
            // generation promised to another node is not this check's case:
            // `keep_own_set` refuses it, naming that node.
            // A ballot naming another node is carried: a rename only one up
            // would be refused again, one generation per report (#1951).
            let accepted = log.accepted_generation();
            if set.is_some() && candidacy.is_none() && accepted > route.generation {
                let own = log
                    .accepted_leader()
                    .is_some_and(|leader| *leader == *local_node_id);
                return Err(Outcome::Refused {
                    promised_to: String::new(),
                    ballot: if own { 0 } else { accepted },
                    why: format!(
                        "this broker's own copy has accepted generation {accepted}, past {}",
                        route.generation
                    ),
                    took: Vec::new(),
                });
            }
            // Under `joint_fence` a fence whose set being left is unknown
            // stays pending rather than fence the new set alone. A candidate
            // fences the set it keeps, and jointly the one kept beside it.
            let leaving = candidacy.and_then(|candidacy| candidacy.leaving.as_ref());
            let under_joint = match (joint, set, candidacy) {
                (Some(joint), Some(set), None) => {
                    match joint::sets(requester, &log, local_node_id, route, set, joint).await {
                        Ok(fenced) => Some(fenced),
                        Err(why @ joint::NoSetBefore::Unknown(_)) => {
                            return Err(Outcome::SetUnknown {
                                why: why.to_string(),
                                ballot: newest_ballot(
                                    requester,
                                    &log,
                                    local_node_id,
                                    route,
                                    &shard,
                                    kind,
                                    joint,
                                )
                                .await,
                            });
                        }
                        // A newer set here never goes back; the others need
                        // a later generation to tell, and a rename there could
                        // leave a set unfenced, so they stay pending.
                        Err(why @ joint::NoSetBefore::Newer(_)) => {
                            return Err(Outcome::Refused {
                                ballot: 0,
                                promised_to: String::new(),
                                why: why.to_string(),
                                took: Vec::new(),
                            });
                        }
                        Err(why) => return Err(Outcome::pending(why.to_string(), Vec::new())),
                    }
                }
                (Some(joint), Some(set), Some(_)) => match leaving {
                    Some(leaving) => Some(
                        joint::across(
                            requester,
                            local_node_id,
                            route,
                            set,
                            leaving.clone(),
                            Vec::new(),
                            joint,
                        )
                        .await,
                    ),
                    None => None,
                },
                // A set kept beside the candidate's was written by a joint
                // fence; fencing without it could miss a majority of it.
                (None, Some(_), Some(_)) if leaving.is_some() => {
                    return Err(Outcome::pending(
                        "the kept set has a set being left, and joint fences are not on",
                        Vec::new(),
                    ));
                }
                _ => None,
            };
            let joint = under_joint
                .as_ref()
                .and_then(|fenced| fenced.joint.as_ref());
            let asked: Vec<&felix_router::NodeRef>;
            let fenced_on = match joint {
                Some(joint) => {
                    asked = joint.asked.iter().collect();
                    &asked
                }
                None => &replicas,
            };
            let fenced = fence_log(
                requester,
                broker,
                key,
                &shard,
                fenced_on,
                kind,
                &log,
                lease_fallback,
                reported_ids,
                set,
                joint,
                candidacy.is_some(),
                continuing,
            )
            .await;
            caught_up_from = caught_up_from.or(fenced?);
            if set.is_some() && candidacy.is_none() {
                keep_own_set(
                    &log,
                    local_node_id,
                    route,
                    under_joint.as_ref().map(|fenced| &fenced.before),
                    joint_fence,
                )
                .await?;
            }
        }
        Ok(caught_up_from)
    };
    // The group logs' round goes out beside the records' rather than after
    // it, so a promotion waits on one round of fences. Each group log keeps
    // the promise on its own log, so it refuses the old leader there whichever
    // fence lands first.
    let (own, groups) = match key.kind {
        ShardKind::Stream => {
            // A copy marked wiped may lack group records a majority took, so
            // its own group logs count only once the mark is settled.
            let own_counts = !broker
                .shard_log(
                    LogKind::Stream,
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                )
                .await
                .is_some_and(|log| log.member_is_wiped(local_node_id));
            futures::join!(
                own_logs,
                fence_groups(
                    requester,
                    broker,
                    key,
                    &shard,
                    &replicas,
                    reported_ids,
                    own_counts,
                    joint_fence,
                )
            )
        }
        ShardKind::Cache => (own_logs.await, Ok(None)),
    };
    let caught_up_from = match own {
        Ok(from) => from,
        Err(outcome) => return outcome,
    };
    // Taken only once the records are: a shard that stays closed takes
    // nothing.
    let groups = match groups {
        Ok(Some(round)) => take_groups(requester, broker, key, &shard, round).await,
        Ok(None) => Ok(None),
        Err(outcome) => Err(outcome),
    };
    match groups {
        Ok(from) => Outcome::Fenced {
            caught_up_from: caught_up_from.or(from),
        },
        Err(outcome) => outcome,
    }
}

/// The newest generation past `shard`'s that a replica of `route`, or a
/// member of a set `log` keeps, accepted; 0 when none answers with one.
/// Asked with a fetch at the generation, which a replica refuses with its
/// ballot and which takes no ballot itself, so asking promises nothing.
#[allow(clippy::too_many_arguments)]
async fn newest_ballot<R: PeerRequester>(
    requester: &R,
    log: &felix_broker::StreamLog,
    local_node_id: &str,
    route: &Route,
    shard: &ShardRef,
    kind: LogKind,
    joint: &Joint<'_>,
) -> u64 {
    let mut members: Vec<(String, std::net::SocketAddr)> = route
        .replicas
        .iter()
        .map(|replica| (replica.node_id.clone(), replica.advertise_addr))
        .collect();
    if let Some(kept) = log.replica_set() {
        let sets = std::iter::once(&kept.current)
            .chain(&kept.leaving)
            .chain(&kept.earlier);
        for member in sets.flat_map(|set| &set.members) {
            if !members.iter().any(|(node, _)| *node == member.node)
                && let Some(addr) = joint.addr_of(&member.node)
            {
                members.push((member.node.clone(), addr));
            }
        }
    }
    members.retain(|(node, _)| node != local_node_id);
    let asks = members.into_iter().map(|(node_id, addr)| {
        let request = InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 0,
            shard: shard.clone(),
            log: crate::ship::replica_log(kind),
            from_offset: 0,
            max_bytes: 1,
            labelled: false,
            timed: false,
            headers: false,
        });
        async move { requester.request(&node_id, addr, request).await }
    });
    futures::future::join_all(asks)
        .await
        .into_iter()
        .filter_map(|answer| match answer {
            Ok(InternalMessage::ReplicateError(err)) if err.code == ErrorCode::FencedEpoch => {
                crate::replica::superseded_by(&err.detail, shard.generation)
            }
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// Send an elected leader's fence again to one follower that missed it.
/// Nothing names the elected generation on that broker but this fence, so
/// until it lands the follower refuses every batch as behind, and a broker
/// that led before keeps refusing pre-votes as the leader. The leader's own
/// ballot is on disk, since the shard opened. Returns whether it was taken.
pub(crate) async fn refence<R: PeerRequester>(
    requester: &R,
    node_id: &str,
    addr: std::net::SocketAddr,
    shard: &ShardRef,
    kind: LogKind,
    candidacy: &crate::election::Candidacy,
) -> bool {
    let election = FenceElection {
        candidate: true,
        set: candidacy.set.clone(),
        leaving: candidacy.leaving.as_ref().map(joint::view),
        earlier: Vec::new(),
    };
    fence_again(requester, node_id, addr, shard, kind, election).await
}

/// Send a named leader's joint fence again to one follower that missed it.
/// A batch teaches no set being left that the follower cannot read from its
/// own routing view, so without this a follower that was down for the fence
/// refuses every batch until the next generation. The set this leader keeps is
/// sent as it is, with or without a set being left, so the follower keeps what
/// the leader keeps. It may be older than the shard's generation only when
/// the leader stamps its batches with it (`stamp`): an elected leader that
/// restarted keeps the set it was fenced on and no standing. Any other older
/// set is what a promotion that opened on the lease left behind, and that
/// leader fences nobody.
pub(crate) async fn refence_kept<R: PeerRequester>(
    requester: &R,
    node_id: &str,
    addr: std::net::SocketAddr,
    shard: &ShardRef,
    kind: LogKind,
    kept: Option<&felix_storage::disk_log::ReplicaSet>,
    stamp: Option<u64>,
) -> bool {
    let Some(kept) = kept.filter(|kept| {
        let at = kept.current.generation;
        at == shard.generation || (at < shard.generation && stamp == Some(at))
    }) else {
        return false;
    };
    let election = FenceElection {
        candidate: false,
        set: joint::view(&kept.current),
        leaving: kept.leaving.as_ref().map(joint::view),
        earlier: kept.earlier.iter().map(joint::view).collect(),
    };
    fence_again(requester, node_id, addr, shard, kind, election).await
}

async fn fence_again<R: PeerRequester>(
    requester: &R,
    node_id: &str,
    addr: std::net::SocketAddr,
    shard: &ShardRef,
    kind: LogKind,
    election: FenceElection,
) -> bool {
    // A follower without `ELECTION` records no elected leader and checks no
    // set, so the fence would move nothing.
    let offered = match requester.recorded_capabilities(node_id) {
        Some(offered) => Some(offered),
        None => requester.capabilities(node_id, addr).await.ok(),
    };
    if !offered.is_some_and(|offered| offered.contains(PeerCapabilities::ELECTION)) {
        return false;
    }
    let candidate = election.candidate;
    let (_, _, answer, _) = ask_fence(
        requester,
        node_id.to_string(),
        addr,
        shard.clone(),
        crate::ship::replica_log(kind),
        false,
        Some(election),
    )
    .await;
    let took = matches!(answer, Ok(InternalMessage::FenceOk(_)));
    tracing::debug!(
        node_id,
        stream = %shard.stream,
        shard = shard.shard,
        generation = shard.generation,
        took,
        candidate,
        "sent the leader's fence again to a follower whose route is behind",
    );
    took
}

/// Fence one of the shard's logs on a majority, and take the log of the
/// answer furthest ahead if it is ahead of this broker's. `Ok` names the
/// replica it was taken from, if one was; `Err` is the outcome to stop at.
///
/// A joint fence asks `replicas`, every member of both sets, carries the set
/// being left, and needs a majority of each set's voters.
#[allow(clippy::too_many_arguments)]
async fn fence_log<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    shard: &ShardRef,
    replicas: &[&felix_router::NodeRef],
    kind: LogKind,
    log: &felix_broker::StreamLog,
    lease_fallback: bool,
    reported_ids: Option<&BTreeMap<String, String>>,
    set: Option<&ReplicaSetView>,
    joint: Option<&JointSets>,
    candidate: bool,
    continuing: bool,
) -> Result<Option<String>, Outcome> {
    // Answers until a majority, this broker included, has taken the fence.
    // Later answers are not waited for: the model opens on the first majority.
    // A majority of the replicas and this broker, which counts itself.
    let voters = replicas.len() + 1;
    let needed = voters / 2 + 1;
    let mut answered: Vec<(String, std::net::SocketAddr, FenceOk)> = Vec::new();
    let took = |answered: &[(String, std::net::SocketAddr, FenceOk)]| {
        answered.iter().map(|(node, _, _)| node.clone()).collect()
    };
    // Each answer's node with the copy its handshake named, which a joint
    // fence counts per set being left.
    let mut seen: Vec<(String, Option<StorageId>)> = Vec::new();
    let settled = |seen: &[(String, Option<StorageId>)]| match joint {
        None => seen.len() + 1 >= needed,
        Some(joint) => joint.has_majorities(seen.iter().map(|(node, id)| (node.as_str(), *id))),
    };
    // Answers from a copy the report did not name: they took the promise,
    // which still refuses the old leader, but count for nothing here.
    let mut other_copies = 0;
    // The replicas among them that the route's copy pins rule out. Placement
    // keeps the pins across renames until this shard's first fenced open, so
    // they count no more one generation up than here, and a promise short of
    // a majority without them is refused like one short on its own (#2046).
    // Without pins on the route a rename may count them, so they wait.
    let mut pinned_out: Vec<String> = Vec::new();
    // Replicas that answered no in some way other than a promise away or
    // silence, which a refusal below may not blame on the promise.
    let mut declined = 0;
    // Why the replicas that refused did, for the shard's status: a refusal
    // that no retry gets past names its way out there.
    let mut refusals: Vec<String> = Vec::new();
    // Replicas that promised this generation to another node. Each is a vote
    // that node holds, so once a majority can no longer be made without
    // them, no answer still to come opens the shard.
    let mut promised_away: Vec<(String, String)> = Vec::new();
    let reachable = |promised_away: &[(String, String)], pinned_out: &[String]| {
        let left = replicas
            .iter()
            .map(|replica| replica.node_id.as_str())
            .filter(|node| !promised_away.iter().any(|(away, _)| away == node))
            .filter(|node| !pinned_out.iter().any(|out| out == node));
        match joint {
            None => left.count() + 1 >= needed,
            Some(joint) => joint.can_reach(left),
        }
    };
    let mut asks = futures::stream::FuturesUnordered::new();
    for replica in replicas {
        let (node_id, addr) = (replica.node_id.clone(), replica.advertise_addr);
        let shard = shard.clone();
        asks.push(async move {
            // Only to a replica that offered `ELECTION`; any other is sent the
            // fence it always was. A joint fence asks only replicas that did.
            // One whose capabilities are not yet recorded is asked, as
            // `joint::sets` does, inside its own ask so a peer that is down
            // holds up only its own answer.
            let election = match (joint, set) {
                (Some(joint), Some(set)) => Some(FenceElection {
                    candidate,
                    set: set.clone(),
                    leaving: Some(joint.leaving_for(&node_id).clone()),
                    earlier: joint.earlier_for(&node_id),
                }),
                (None, Some(set)) => {
                    let offered = match requester.recorded_capabilities(&node_id) {
                        Some(offered) => Some(offered),
                        None => requester.capabilities(&node_id, addr).await.ok(),
                    };
                    offered
                        .is_some_and(|offered| offered.contains(PeerCapabilities::ELECTION))
                        .then(|| FenceElection {
                            candidate,
                            set: set.clone(),
                            leaving: None,
                            earlier: Vec::new(),
                        })
                }
                (_, None) => None,
            };
            ask_fence(
                requester,
                node_id,
                addr,
                shard,
                crate::ship::replica_log(kind),
                reported_ids.is_some() || joint.is_some_and(JointSets::names_copies),
                election,
            )
            .await
        });
    }
    while !settled(&seen) {
        let Some((node_id, addr, answer, identity)) = asks.next().await else {
            break;
        };
        match refusal(&node_id, &answer, shard.generation) {
            Some(Refusal::PromisedTo { leader, why }) => {
                refusals.push(why);
                promised_away.push((node_id, leader));
                if !reachable(&promised_away, &pinned_out) {
                    return Err(promised_away_refused(
                        &promised_away,
                        &refusals,
                        shard.generation,
                        took(&answered),
                    ));
                }
                continue;
            }
            Some(Refusal::Final { why, ballot }) => {
                return Err(Outcome::Refused {
                    ballot,
                    promised_to: String::new(),
                    why,
                    took: took(&answered),
                });
            }
            None => {}
        }
        match answer {
            // A continuing leader holds what any copy could, so a copy that
            // promised counts whatever its identity.
            Ok(InternalMessage::FenceOk(_))
                if !continuing
                    && (!reported_copy(reported_ids, &node_id, identity)
                        || joint.is_some_and(|joint| !joint.same_copy(&node_id, identity))) =>
            {
                other_copies += 1;
                uncounted(key, kind, &node_id, reported_ids, identity);
                if reported_ids.is_some() && !reported_copy(reported_ids, &node_id, identity) {
                    pinned_out.push(node_id);
                    if !promised_away.is_empty() && !reachable(&promised_away, &pinned_out) {
                        return Err(promised_away_refused(
                            &promised_away,
                            &refusals,
                            shard.generation,
                            took(&answered),
                        ));
                    }
                }
            }
            Ok(InternalMessage::FenceOk(ok)) => {
                seen.push((node_id.clone(), identity));
                answered.push((node_id, addr, ok));
            }
            Err(PeerError::Unsupported { .. }) if lease_fallback => {
                return Err(Outcome::Lease { lacking: node_id });
            }
            Ok(InternalMessage::ForwardPublishError(err))
                if lease_fallback && err.code == ErrorCode::UnsupportedKind =>
            {
                return Err(Outcome::Lease { lacking: node_id });
            }
            other => {
                tracing::debug!(
                    node_id = %node_id,
                    stream = %key.stream,
                    shard = key.shard,
                    log = ?kind,
                    answer = ?other.as_ref().map(InternalMessage::kind),
                    "a replica did not take the fence",
                );
                let silent = matches!(
                    other,
                    Err(PeerError::Unavailable { .. }
                        | PeerError::Handshake { .. }
                        | PeerError::Disconnected { .. }
                        | PeerError::Timeout { .. }
                        | PeerError::ShuttingDown)
                );
                if !silent {
                    declined += 1;
                }
                if let Ok(InternalMessage::ReplicateError(err)) = other {
                    refusals.push(format!("{node_id} refused: {}", err.detail));
                }
            }
        }
    }
    drop(asks);
    if !settled(&seen) {
        let mut why = match joint {
            None => format!(
                "{} of {} replicas took the fence on the {kind:?} log, {} needed with this broker",
                answered.len(),
                replicas.len(),
                needed - 1
            ),
            Some(joint) => format!(
                "on the {kind:?} log, {}",
                joint.shortfall(seen.iter().map(|(node, id)| (node.as_str(), *id)))
            ),
        };
        if other_copies > 0 {
            why.push_str(&format!(
                "; {other_copies} answered from another copy than the failover's report named"
            ));
        }
        for refusal in &refusals {
            why.push_str("; ");
            why.push_str(refusal);
        }
        // A promise away never comes back, so with the rest only silent a
        // retry here can wait forever on a replica that is gone; the refusal
        // moves the shard up. Not when a joint set's count kept the majority
        // out, nor an answer from another copy that no route pin rules out:
        // a rename could count it, so that case waits instead.
        let explained = joint.is_none() && other_copies == pinned_out.len() && declined == 0;
        if let Some((_, promised_to)) = promised_away.first().filter(|_| explained) {
            return Err(Outcome::Refused {
                promised_to: promised_to.clone(),
                ballot: 0,
                why,
                took: took(&answered),
            });
        }
        return Err(Outcome::pending(why, took(&answered)));
    }

    // Nothing to take: this leader wrote every record acknowledged before
    // this generation, and a doubted copy's tail past it is one to drop.
    if continuing {
        return Ok(None);
    }
    // The tail that wins, by the order promotion by log order uses (`Ahead`).
    let own_end = log.tail_offset().await.map_err(|err| {
        Outcome::pending(
            format!("could not read the shard's tail: {err}"),
            took(&answered),
        )
    })?;
    let own = (last_generation(&log.generations(), own_end), own_end);
    let ahead = answered
        .iter()
        .filter(|(_, _, ok)| (ok.last_generation, ok.log_end) > own)
        .max_by_key(|(_, _, ok)| (ok.last_generation, ok.log_end));
    let Some((node_id, addr, best)) = ahead else {
        return Ok(None);
    };
    let from = Source {
        node_id,
        addr: *addr,
        target: best,
    };
    catch_up(requester, broker, log, kind, key, shard, from)
        .await
        .map_err(|why| Outcome::pending(why, took(&answered)))?;
    Ok(Some(node_id.clone()))
}

/// A majority's answers to a stream shard's group-log fence, and this
/// broker's own two logs.
struct GroupRound {
    dead_letters: felix_broker::StreamLog,
    cursors: felix_broker::StreamLog,
    /// Each replica that took both fences, with its dead-letter answer and
    /// then its cursor answer.
    answered: Vec<(String, std::net::SocketAddr, FenceOk, FenceOk)>,
}

/// Fence a stream shard's dead-letter and cursor logs on a majority, a
/// replica counting only once it has taken both.
///
/// `Ok(None)` when there is nothing to take: no consumer groups here, or too
/// few replicas took both fences, because they predate `GROUP_FENCE` or did
/// not answer. This broker then serves the pair it holds. That pair is
/// consistent, since a follower gets dead letters before the cursors that
/// pass them, so serving it may redeliver but never skips a record; and the
/// records' fence already refuses the old leader on both logs. So no group
/// log can keep a shard closed, except a refusal the records' fence would
/// also stop at ([`Refusal`]). A replica that promised the generation away
/// is one of those once the majority is short: the shard is refused, or
/// stays pending when another replica also declined or `joint` is set, as
/// on the records' fence, rather than serve this broker's pair.
///
/// Without `own_counts` this broker's copy is marked wiped, and its group
/// logs may lack what a majority acknowledged, so they do not count toward
/// the majority; short of one, the furthest answer still beats them, and
/// with no answer at all the shard stays pending rather than serve them.
#[allow(clippy::too_many_arguments)]
async fn fence_groups<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    shard: &ShardRef,
    replicas: &[&felix_router::NodeRef],
    reported_ids: Option<&BTreeMap<String, String>>,
    own_counts: bool,
    joint: bool,
) -> Result<Option<GroupRound>, Outcome> {
    if broker.group_reader().is_none() {
        return Ok(None);
    }
    let expected = crate::incarnation::expected(broker, key.kind, shard);
    let open = |kind| {
        broker.shard_log_for(
            kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            None,
            expected,
        )
    };
    let (Some(dead_letters), Some(cursors)) = (
        open(LogKind::GroupDeadLetters).await,
        open(LogKind::GroupCursors).await,
    ) else {
        return Err(Outcome::pending(
            "this broker could not open the shard's group logs",
            Vec::new(),
        ));
    };
    let needed = group_answers_needed(replicas.len(), own_counts);
    let mut asks = futures::stream::FuturesUnordered::new();
    for replica in replicas {
        let (node_id, addr) = (replica.node_id.clone(), replica.advertise_addr);
        asks.push(async move {
            let (dead_letters, cursors) = futures::join!(
                ask_fence(
                    requester,
                    node_id.clone(),
                    addr,
                    shard.clone(),
                    ReplicaLog::GroupDeadLetters,
                    reported_ids.is_some(),
                    None,
                ),
                ask_fence(
                    requester,
                    node_id.clone(),
                    addr,
                    shard.clone(),
                    ReplicaLog::GroupCursors,
                    reported_ids.is_some(),
                    None,
                ),
            );
            // Both answers from the copy the report named, or neither counts.
            let copy = reported_copy(reported_ids, &node_id, dead_letters.3)
                && reported_copy(reported_ids, &node_id, cursors.3);
            // Either log took the fence, but from another copy.
            let other_copy = [&dead_letters, &cursors].into_iter().any(|answer| {
                matches!(answer.2, Ok(InternalMessage::FenceOk(_)))
                    && !reported_copy(reported_ids, &node_id, answer.3)
            });
            (node_id, addr, dead_letters.2, cursors.2, copy, other_copy)
        });
    }
    let mut answered = Vec::new();
    let took = |answered: &[(String, std::net::SocketAddr, FenceOk, FenceOk)]| {
        answered.iter().map(|(node, ..)| node.clone()).collect()
    };
    // As on the records' fence: a replica that promised the generation away
    // ends the fence once no majority is left without it, and a newer ballot,
    // view or set ends it at once. Each is reported, or a shard whose group
    // logs meet one stays pending with nothing moving it on.
    let mut promised_away: Vec<(String, String)> = Vec::new();
    let mut refusals: Vec<String> = Vec::new();
    let mut declined = 0;
    // Replicas that answered from another copy than the route pins, which
    // count no more after a rename, as on the records' fence.
    let mut pinned_out: Vec<String> = Vec::new();
    let short = |promised_away: &[(String, String)], pinned_out: &[String]| {
        let left = replicas
            .iter()
            .filter(|replica| {
                !promised_away
                    .iter()
                    .any(|(away, _)| *away == replica.node_id)
                    && !pinned_out.contains(&replica.node_id)
            })
            .count();
        left < needed
    };
    while answered.len() < needed {
        let Some((node_id, addr, dead, cursor, copy, other_copy)) = asks.next().await else {
            break;
        };
        let refused = [&dead, &cursor]
            .into_iter()
            .filter_map(|answer| refusal(&node_id, answer, shard.generation))
            .min_by_key(|refused| match refused {
                Refusal::Final { .. } => 0,
                Refusal::PromisedTo { .. } => 1,
            });
        match refused {
            Some(Refusal::Final { why, ballot }) => {
                return Err(Outcome::Refused {
                    ballot,
                    promised_to: String::new(),
                    why,
                    took: took(&answered),
                });
            }
            Some(Refusal::PromisedTo { leader, why }) => {
                refusals.push(why);
                promised_away.push((node_id, leader));
                if short(&promised_away, &pinned_out) {
                    return Err(promised_away_refused(
                        &promised_away,
                        &refusals,
                        shard.generation,
                        took(&answered),
                    ));
                }
                continue;
            }
            None => {}
        }
        match (dead, cursor) {
            (Ok(InternalMessage::FenceOk(_)), Ok(InternalMessage::FenceOk(_))) if !copy => {
                pinned_out.push(node_id.clone());
                tracing::info!(
                    node_id = %node_id,
                    stream = %key.stream,
                    shard = key.shard,
                    "not counting a group logs' fence answer from another copy than the \
                     failover's report named",
                );
            }
            (Ok(InternalMessage::FenceOk(dead)), Ok(InternalMessage::FenceOk(cursor))) => {
                answered.push((node_id, addr, dead, cursor));
            }
            (dead, cursor) => {
                let silent = |answer: &Result<InternalMessage, PeerError>| {
                    matches!(
                        answer,
                        Ok(InternalMessage::FenceOk(_))
                            | Err(PeerError::Unavailable { .. }
                                | PeerError::Handshake { .. }
                                | PeerError::Disconnected { .. }
                                | PeerError::Timeout { .. }
                                | PeerError::ShuttingDown)
                    )
                };
                if other_copy {
                    pinned_out.push(node_id.clone());
                } else if !(silent(&dead) && silent(&cursor)) {
                    declined += 1;
                }
                tracing::debug!(
                    node_id = %node_id,
                    stream = %key.stream,
                    shard = key.shard,
                    dead_letters = ?dead.as_ref().map(InternalMessage::kind),
                    cursors = ?cursor.as_ref().map(InternalMessage::kind),
                    "a replica did not take the group logs' fence",
                );
            }
        }
    }
    // A promise away never comes back: with the rest only silent, or
    // answering from a copy the route pins rule out, the refusal moves the
    // shard up. A replica that answered otherwise keeps it pending, as on the
    // records' fence, and so does a joint fence, where a rename could leave a
    // set unfenced. Serving this broker's own pair instead would leave a
    // replica that refuses it.
    if answered.len() < needed && !promised_away.is_empty() {
        if short(&promised_away, &pinned_out) {
            return Err(promised_away_refused(
                &promised_away,
                &refusals,
                shard.generation,
                took(&answered),
            ));
        }
        if declined == 0 && !joint {
            return Err(promised_away_refused(
                &promised_away,
                &refusals,
                shard.generation,
                took(&answered),
            ));
        }
        return Err(Outcome::pending(
            format!(
                "{} of {} replicas took the group logs' fence; {}",
                answered.len(),
                replicas.len(),
                refusals.join("; ")
            ),
            took(&answered),
        ));
    }
    if !own_counts && answered.is_empty() && !replicas.is_empty() {
        return Err(Outcome::pending(
            "this broker's copy is marked wiped and no replica took the group logs' fence",
            Vec::new(),
        ));
    }
    if answered.len() < needed && (own_counts || answered.is_empty()) {
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            took = answered.len(),
            needed,
            "too few replicas took the group logs' fence; serving this broker's own",
        );
        return Ok(None);
    }
    Ok(Some(GroupRound {
        dead_letters,
        cursors,
        answered,
    }))
}

/// How many of `replicas`, this broker aside, must take the group fences for
/// a majority: one fewer when this broker's own group logs count.
fn group_answers_needed(replicas: usize, own_counts: bool) -> usize {
    crate::quorum::majority_of(replicas) - usize::from(own_counts)
}

/// What a replica's refusal of a fence at a generation means for the fence,
/// read the same way on every log a promotion fences.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// The replica promised the generation to `leader`, a vote that node
    /// holds; `why` is the refusal for the shard's status.
    PromisedTo { leader: String, why: String },
    /// No retry opens the fence, so the refusal is reported, with `ballot`,
    /// the newer generation the replica named, or 0 when it named none. A
    /// ballot above the generation that no assignment this broker has seen
    /// names, as an election that never opened leaves: if the control plane
    /// had named it, it has also named someone above this broker and ignores
    /// the report; if not, nothing else ever moves the shard past it (#1914).
    /// The same for a newer assignment the replica saw. Or a newer replica
    /// set, which never goes back. Placement names this broker above the
    /// ballot, so never into a generation a withdrawn standing holds (#2046).
    Final { why: String, ballot: u64 },
}

/// How `node_id`'s fence answer refuses `generation`, or `None` for an
/// answer that is not one of the refusals above.
fn refusal(
    node_id: &str,
    answer: &Result<InternalMessage, PeerError>,
    generation: u64,
) -> Option<Refusal> {
    let Ok(InternalMessage::ReplicateError(err)) = answer else {
        return None;
    };
    let detail = &err.detail;
    match err.code {
        ErrorCode::FencedEpoch => Some(
            if let Some(leader) = crate::replica::promised_to(detail, generation) {
                Refusal::PromisedTo {
                    leader: leader.to_string(),
                    why: format!("{node_id} refused: {detail}"),
                }
            } else {
                Refusal::Final {
                    why: format!("{node_id} has accepted a newer generation: {detail}"),
                    ballot: crate::replica::superseded_by(detail, generation)
                        .or_else(|| crate::replica::assigned_by(detail, generation))
                        .unwrap_or(0),
                }
            },
        ),
        ErrorCode::StaleSet => Some(Refusal::Final {
            why: format!("{node_id} counts a newer replica set: {detail}"),
            ballot: 0,
        }),
        _ => None,
    }
}

/// The refusal once the replicas that promised `generation` away leave no
/// majority to take it, with each replica's refusal in `refusals`.
fn promised_away_refused(
    promised_away: &[(String, String)],
    refusals: &[String],
    generation: u64,
    took: Vec<String>,
) -> Outcome {
    let mut why = format!(
        "{} of the replicas promised generation {generation} to another node",
        promised_away.len(),
    );
    for refusal in refusals {
        why.push_str("; ");
        why.push_str(refusal);
    }
    Outcome::Refused {
        ballot: 0,
        promised_to: promised_away
            .first()
            .map(|(_, leader)| leader.clone())
            .unwrap_or_default(),
        why,
        took,
    }
}

/// Take the dead-letter log from the answer whose pair is furthest ahead, if
/// it is ahead of this broker's, ranked by dead letters first since a lost
/// dead letter is a skipped record and a lost cursor only a redelivery; and
/// that replica's cursors, each group going to the further of its two
/// positions.
///
/// The cursors need not match the dead-letter log beside them: each cursor
/// record says which dead letters it needs, and the shard's cursors are
/// checked against the dead-letter log it ends up with before it is served
/// (`GroupReader`). So this broker's own further cursors are kept as long as
/// they still stand, and taking the replica's appends rather than replaces,
/// so no group is erased.
async fn take_groups<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    shard: &ShardRef,
    round: GroupRound,
) -> Result<Option<String>, Outcome> {
    let GroupRound {
        dead_letters,
        cursors,
        answered,
    } = round;
    let took: Vec<String> = answered.iter().map(|(node, ..)| node.clone()).collect();
    let pending = |why: String| Outcome::pending(why, took.clone());
    let position = |log: &felix_broker::StreamLog| {
        let log = log.clone();
        async move {
            let end = log
                .tail_offset()
                .await
                .map_err(|err| format!("could not read the tail of a group log: {err}"))?;
            Ok::<_, String>((last_generation(&log.generations(), end), end))
        }
    };
    let own = (
        position(&dead_letters).await.map_err(pending)?,
        position(&cursors).await.map_err(pending)?,
    );
    let of = |ok: &FenceOk| (ok.last_generation, ok.log_end);
    let best = answered
        .iter()
        .max_by_key(|(_, _, dead, cursor)| (of(dead), of(cursor)))
        .filter(|(_, _, dead, cursor)| (of(dead), of(cursor)) > own);
    let Some((node_id, addr, dead, cursor)) = best else {
        return Ok(None);
    };
    let from = |target| Source {
        node_id,
        addr: *addr,
        target,
    };
    if of(dead) > own.0 {
        catch_up(
            requester,
            broker,
            &dead_letters,
            LogKind::GroupDeadLetters,
            key,
            shard,
            from(dead),
        )
        .await
        .map_err(pending)?;
    }
    if of(cursor) != own.1 {
        let groups = broker
            .consumer_groups()
            .ok_or_else(|| pending("this broker keeps no consumer groups".to_string()))?;
        let records = read_all(
            requester,
            node_id,
            *addr,
            shard,
            LogKind::GroupCursors,
            cursor.log_end,
        )
        .await
        .map_err(pending)?;
        groups
            .adopt(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &records,
            )
            .await
            .map_err(|err| pending(format!("could not take {node_id}'s cursors: {err}")))?;
    }
    if let Some(reader) = broker.group_reader() {
        reader.invalidate(&key.tenant_id, &key.namespace, &key.stream, key.shard);
    }
    Ok(Some(node_id.clone()))
}

/// Every record of `node_id`'s `kind` log below `end`, from where its log
/// begins.
async fn read_all<R: PeerRequester>(
    requester: &R,
    node_id: &str,
    addr: std::net::SocketAddr,
    shard: &ShardRef,
    kind: LogKind,
    end: u64,
) -> Result<Vec<bytes::Bytes>, String> {
    let mut records = Vec::new();
    let mut next = 0;
    while next < end {
        let request = InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 0,
            shard: shard.clone(),
            log: crate::ship::replica_log(kind),
            from_offset: next,
            max_bytes: FETCH_BYTES,
            labelled: false,
            timed: false,
            headers: false,
        });
        let batch = match requester.request(node_id, addr, request).await {
            Ok(
                InternalMessage::ReplicateGroupRecords(batch)
                | InternalMessage::ReplicateRecords(batch)
                | InternalMessage::ReplicateMarkedRecords(batch),
            ) => batch,
            other => {
                return Err(format!(
                    "reading {node_id}'s {kind:?} log from {next} failed: {:?}",
                    other.map(|answer| answer.kind())
                ));
            }
        };
        if batch.payloads.is_empty() {
            break;
        }
        next = batch.first_offset + batch.payloads.len() as u64;
        records.extend(batch.payloads);
    }
    Ok(records)
}

/// The replica whose log a promoted leader takes, and how far it reaches.
struct Source<'a> {
    node_id: &'a str,
    addr: std::net::SocketAddr,
    target: &'a FenceOk,
}

/// Drop this broker's records from `offset`, superseded by `from`'s log.
/// Storage refuses to cut below the commit offset.
async fn drop_superseded(
    broker: &Arc<Broker>,
    log: &felix_broker::StreamLog,
    kind: LogKind,
    key: &ShardKey,
    offset: u64,
    from: &str,
) -> Result<(), String> {
    log.truncate(offset)
        .await
        .map_err(|err| format!("could not drop this broker's records from {offset}: {err}"))?;
    if let Err(err) = broker
        .reset_log(
            kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
            offset,
        )
        .await
    {
        tracing::warn!(stream = %key.stream, shard = key.shard, log = ?kind, error = %err,
            "dropped superseded records but could not reset what was read from them");
    }
    metrics::record_promotion_truncated();
    tracing::warn!(
        stream = %key.stream,
        shard = key.shard,
        log = ?kind,
        offset,
        from,
        "dropped this broker's records a replica's newer log superseded",
    );
    Ok(())
}

/// Take this leader's ballot and keep the set it fenced with, before the
/// shard opens: the ballot first, as the open of a log expects, so a set is
/// never on disk ahead of the generation it belongs to. The open takes the
/// same ballot again, which is then a no-op.
///
/// Under `joint_fence` the set before is kept beside it, even when it has
/// the same voters, so a fence tried again at this generation knows it.
async fn keep_own_set(
    log: &felix_broker::StreamLog,
    local_node_id: &str,
    route: &Route,
    leaving: Option<&felix_storage::disk_log::Membership>,
    joint_fence: bool,
) -> Result<(), Outcome> {
    match log
        .accept_generation(route.generation, Some(local_node_id))
        .await
    {
        Ok(
            felix_storage::disk_log::GenerationCheck::Current
            | felix_storage::disk_log::GenerationCheck::Raised,
        ) => {}
        // This broker's own ballot is past the generation, as a standing it
        // withdrew leaves, or promised it to another node. Neither changes, so
        // a retry here never opens; the report moves the shard up (#1914).
        Ok(felix_storage::disk_log::GenerationCheck::Superseded { accepted }) => {
            return Err(Outcome::Refused {
                ballot: 0,
                promised_to: String::new(),
                why: format!(
                    "this broker cannot lead generation {}: its own ballot holds {accepted}",
                    route.generation
                ),
                took: Vec::new(),
            });
        }
        Ok(felix_storage::disk_log::GenerationCheck::Promised { leader }) => {
            return Err(Outcome::Refused {
                ballot: 0,
                why: format!(
                    "this broker cannot lead generation {}: it promised it to {leader}",
                    route.generation
                ),
                promised_to: leader,
                took: Vec::new(),
            });
        }
        Err(err) => {
            return Err(Outcome::pending(
                format!("could not persist this leader's ballot: {err}"),
                Vec::new(),
            ));
        }
    }
    let mut set = crate::replica_set::kept(route, None);
    set.leaving = leaving.cloned();
    // From this broker's own ballot: a standing that raised it since keeps
    // the set it stood on.
    match log
        .accept_set_from(set, joint_fence, route.generation, Some(local_node_id))
        .await
    {
        // As a replica's newer set: it never goes back, so report it.
        Ok(Ok(felix_storage::disk_log::SetCheck::Stale { known })) => Err(Outcome::Refused {
            ballot: 0,
            promised_to: String::new(),
            why: format!("this broker counts the set of generation {known}, newer than this one"),
            took: Vec::new(),
        }),
        Ok(Ok(_)) => Ok(()),
        Ok(Err(felix_storage::disk_log::GenerationCheck::Promised { leader })) => {
            Err(Outcome::Refused {
                ballot: 0,
                why: format!(
                    "this broker cannot lead generation {}: it promised it to {leader}",
                    route.generation
                ),
                promised_to: leader,
                took: Vec::new(),
            })
        }
        Ok(Err(ballot)) => Err(Outcome::Refused {
            ballot: 0,
            promised_to: String::new(),
            why: format!(
                "this broker cannot lead generation {}: its own ballot holds {}",
                route.generation,
                match ballot {
                    felix_storage::disk_log::GenerationCheck::Superseded { accepted } => accepted,
                    _ => route.generation,
                }
            ),
            took: Vec::new(),
        }),
        Err(err) => Err(Outcome::pending(
            format!("could not keep this leader's replica set: {err}"),
            Vec::new(),
        )),
    }
}

/// A replica's answer to a fence, and with `identified` the storage identity
/// the connection it came over gave in its handshake.
async fn ask_fence<R: PeerRequester>(
    requester: &R,
    node_id: String,
    addr: std::net::SocketAddr,
    shard: ShardRef,
    log: ReplicaLog,
    identified: bool,
    election: Option<FenceElection>,
) -> (
    String,
    std::net::SocketAddr,
    Result<InternalMessage, PeerError>,
    Option<StorageId>,
) {
    let request = InternalMessage::Fence(Fence {
        correlation_id: 0,
        shard,
        log,
        election,
    });
    if !identified {
        let answer = requester.request(&node_id, addr, request).await;
        return (node_id, addr, answer, None);
    }
    match requester.request_identified(&node_id, addr, request).await {
        Ok((answer, identity)) => (node_id, addr, Ok(answer), identity.storage),
        Err(err) => (node_id, addr, Err(err), None),
    }
}

/// Whether an answer from `node_id`, over a connection whose handshake gave
/// `identity`, may count toward the fence's majority. Every answer may when
/// the assignment names no identities.
///
/// A replica the assignment names counts only from that identity: under
/// another, or with none, it cannot show it still holds what it answered
/// for. Placement names every member of the old set it can: by the report,
/// or with an empty identity, which nothing matches, for one the report could
/// not vouch for (a member its leader had not asked yet). A replica it does
/// not name gave the old leader nothing to count, or predates identities,
/// and counts as it always has. The fence's majority both refuses the old
/// leader and holds what it acknowledged, so a promise alone cannot stand in
/// for an answer.
fn reported_copy(
    expected: Option<&BTreeMap<String, String>>,
    node_id: &str,
    identity: Option<StorageId>,
) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    match (expected.get(node_id), identity) {
        (None, _) => true,
        (Some(named), Some(identity)) => {
            !named.is_empty() && named.eq_ignore_ascii_case(&identity.to_hex())
        }
        (Some(_), None) => false,
    }
}

fn uncounted(
    key: &ShardKey,
    kind: LogKind,
    node_id: &str,
    expected: Option<&BTreeMap<String, String>>,
    identity: Option<StorageId>,
) {
    tracing::warn!(
        node_id,
        stream = %key.stream,
        shard = key.shard,
        log = ?kind,
        named = expected.and_then(|ids| ids.get(node_id)).map(String::as_str),
        answered_from = identity.map(|id| id.to_hex()),
        "not counting a fence answer from another copy than the failover's report named: \
         it may have lost records it held before",
    );
}

/// Take `from`'s copy of the `kind` log where it is ahead of this broker's:
/// compare from where the two may disagree, drop this broker's own records
/// past the first disagreement, and append the rest.
///
/// Compared from the later of the commit offset and where this broker's last
/// generation began, as a follower compares a new leader's batches
/// (`unverified_from` in `replica.rs`): below that the records are committed,
/// or written by the generation both logs share.
async fn catch_up<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    log: &felix_broker::StreamLog,
    kind: LogKind,
    key: &ShardKey,
    shard: &ShardRef,
    from: Source<'_>,
) -> Result<(), String> {
    let Source {
        node_id: from,
        addr,
        target,
    } = from;
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    let last_start = log
        .generations()
        .iter()
        .rev()
        .find(|epoch| epoch.start_offset < tail)
        .map_or(0, |epoch| epoch.start_offset);
    // Never past the replica's end: a log taken from a replica behind this
    // one, as a pair's cursor log can be, loses what this broker held past it.
    let mut next = log
        .commit_offset()
        .max(last_start)
        .min(tail)
        .min(target.log_end);
    let compared_from = next;
    // Taken with the generations that wrote them, so this broker's fence
    // answers and the followers it ships to do not see them as newer.
    let offered = requester.recorded_capabilities(from);
    let labelled =
        offered.is_some_and(|offered| offered.contains(PeerCapabilities::GENERATION_LABELS));
    // And with the times its leader stored, so this broker reports them once
    // it serves.
    let timed =
        labelled && offered.is_some_and(|offered| offered.contains(PeerCapabilities::RECORD_TIMES));
    // And with the key-and-headers blocks, or this broker would hold records
    // that differ from the replica's once it serves.
    let headers =
        timed && offered.is_some_and(|offered| offered.contains(PeerCapabilities::RECORD_HEADERS));
    let mut taken_labels = labelled.then(Vec::new);
    while next < target.log_end {
        let request = InternalMessage::ReplicateFetch(ReplicateFetch {
            correlation_id: 0,
            shard: shard.clone(),
            log: crate::ship::replica_log(kind),
            from_offset: next,
            max_bytes: FETCH_BYTES,
            labelled,
            timed,
            headers,
        });
        let batch = match requester.request(from, addr, request).await {
            Ok(
                InternalMessage::ReplicateRecords(batch)
                | InternalMessage::ReplicateMarkedRecords(batch)
                | InternalMessage::ReplicateCacheRecords(batch)
                | InternalMessage::ReplicateCounterRecords(batch)
                | InternalMessage::ReplicateGroupRecords(batch)
                | InternalMessage::ReplicateDeadLetterRecords(batch),
            ) => batch,
            other => {
                return Err(format!(
                    "reading {from}'s {kind:?} log from {next} failed: {:?}",
                    other.map(|answer| answer.kind())
                ));
            }
        };
        if batch.payloads.is_empty() {
            // It holds less than it said: a batch from its old leader it was
            // storing as it answered was refused and never landed. What it
            // does hold has been taken.
            break;
        }
        broker
            .mark_head_trimmed_for(&batch.marks)
            .await
            .map_err(|err| err.to_string())?;
        broker
            .mark_headers_stored_for(batch.headers.as_deref())
            .await
            .map_err(|err| err.to_string())?;
        let applied = replication::apply(
            log,
            batch.first_offset,
            batch.checksum,
            &batch.payloads,
            &batch.marks,
            &batch.publishers,
            batch.times.as_deref().unwrap_or_default(),
            batch.headers.as_deref(),
        )
        .await
        .map_err(|err| err.to_string())?;
        match (&mut taken_labels, batch.generations.as_deref()) {
            (Some(taken), Some(generations)) => taken.extend_from_slice(generations),
            _ => taken_labels = None,
        }
        match applied {
            Ok(applied) => {
                let level = log
                    .tail_offset()
                    .await
                    .is_ok_and(|tail| applied.durable_offset >= tail);
                if let Some(generations) = batch.generations.as_deref()
                    && level
                {
                    let start = applied.durable_offset - applied.appended as u64;
                    crate::replica::label_appended(
                        log,
                        start,
                        applied.durable_offset,
                        shard.generation,
                        Some(generations),
                    )
                    .await
                    .map_err(|err| format!("could not label the records from {start}: {err}"))?;
                }
                next = applied.durable_offset;
            }
            // This broker's own records from `offset` are a generation the
            // replica's log superseded. The model replaces the whole log; here
            // only the part that differs goes.
            Err(Divergence::Conflict { offset, .. }) => {
                drop_superseded(broker, log, kind, key, offset, from).await?;
            }
            Err(Divergence::Gap { expected, .. }) => next = expected,
            Err(other) => return Err(format!("reading {from}'s {kind:?} log: {other}")),
        }
    }
    // What this broker holds past the replica's end was written under an
    // older generation than the replica's last record, and goes with the
    // rest of what the model replaces.
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    if tail > next {
        drop_superseded(broker, log, kind, key, next, from).await?;
    }
    let tail = log.tail_offset().await.map_err(|err| err.to_string())?;
    // A batch that only matched records this broker held past it was not
    // labelled then: those records kept this broker's own labels. With the
    // log now ending where the replica's does, its labels go over all of it,
    // as the model's log' = log[f] carries them.
    if let Some(taken) = taken_labels.filter(|taken| !taken.is_empty()) {
        crate::replica::label_appended(log, compared_from, tail, shard.generation, Some(&taken))
            .await
            .map_err(|err| format!("could not label the records from {compared_from}: {err}"))?;
    }
    // A cache's index, a counter shard's sums and the group logs' entries
    // catch up with the tail on their next read; a stream's commit order has
    // to be told.
    if kind == LogKind::Stream
        && let Err(err) = broker
            .adopt_replicated(&key.tenant_id, &key.namespace, &key.stream, key.shard, tail)
            .await
    {
        tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
            "took a replica's tail but could not advance the stream's own view of it");
    }
    tracing::info!(
        stream = %key.stream,
        shard = key.shard,
        log = ?kind,
        from,
        tail,
        "took a replica's log that was ahead of this broker's before serving",
    );
    Ok(())
}

#[cfg(test)]
mod tests;
