//! What only the Raft leader keeps: when each broker last heartbeated, and
//! until when the placement lease holder holds it.
//!
//! Neither goes through the log. A heartbeat per broker per interval would be
//! an fsync on a majority for a fact that is stale seconds later, and so would
//! a lease renewal per placement tick. The leader holds both in memory, ages
//! them on its monotonic clock, and puts in the log only what follows from
//! them: a node marked down, the lease changing hands.
//!
//! A new leader starts knowing nothing, and treats that as every broker
//! having heartbeated, and the lease having been renewed, the moment it began
//! judging. Nothing is expired and no lease taken over until a full window
//! has passed under the new leader. That is what makes a leader change safe:
//! a heartbeat the old leader answered was answered only after a quorum
//! confirmed its leadership, so the new term began after it, and the broker's
//! lease ends less than one window after that. It also bounds the wait: a
//! stamp in the log, however far ahead of this leader's clock, does not
//! extend it.
//!
//! The design is in `docs/metadata-raft-design.md` ("Liveness is leader
//! soft state").
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::model::{Node, NodeLifecycle};
use crate::raft::{LeaderService, NotLeader, RaftHandle};
use crate::store::ControlPlaneStore;
use crate::store::raft::command::{
    HeartbeatSeen, MetaCommand, MetaError, MetaResponse, MetaResult, NodeIncarnation,
    decode_result, encode_command, encode_result,
};
use crate::store::raft::state_machine::MetadataStateMachine;

/// How long a heartbeat waits for a quorum to confirm this member still
/// leads. Short: a broker retries, and one stuck here holds nothing.
const CONFIRM_WITHIN: Duration = Duration::from_secs(2);

/// How often the leader writes its heartbeat view into the log, for the
/// benefit of node listings on followers. One entry per interval for the
/// whole fleet, rather than one per heartbeat.
const CHECKPOINT_EVERY: Duration = Duration::from_secs(5);

/// A request only the leader answers. Forwarded as JSON by followers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum LeaderRequest {
    Heartbeat {
        node_id: String,
        incarnation: u64,
    },
    /// The leaders a broker cannot reach. A leader that predates it fails
    /// to read the request, and the suspicion is dropped.
    Suspicion {
        node_id: String,
        incarnation: u64,
        suspects: std::collections::BTreeSet<String>,
    },
    ExpireStaleNodes {
        expiry_before_millis: u64,
    },
    AcquirePlacementLease {
        holder: String,
        ttl_millis: u64,
    },
    ReleasePlacementLease {
        holder: String,
    },
}

/// The leader's soft state and the rules for judging by it.
pub(crate) struct SoftState {
    handle: RaftHandle,
    machine: Arc<MetadataStateMachine>,
    /// One judgement at a time: a sweep deciding who is stale and a
    /// heartbeat for one of them must not interleave, or a node could be
    /// expired right after it was told it is alive.
    serial: tokio::sync::Mutex<()>,
    view: Mutex<Option<View>>,
}

/// One term's view. Replaced whenever the term moves, so nothing a previous
/// leadership saw is trusted by the next.
struct View {
    term: u64,
    /// When this member began judging in `term`: the heartbeat every node is
    /// assumed to have sent, and the renewal the lease is assumed to have had.
    since: Instant,
    /// `since` on the wall clock, which is what listings carry.
    since_millis: u64,
    beats: HashMap<String, Beat>,
    /// What each broker last said about the leaders it cannot reach.
    suspicions: HashMap<String, crate::model::NodeSuspicion>,
    /// Nodes (re)registered during this term, and when: a later start for
    /// their silence than `since`.
    registered: HashMap<String, Instant>,
    lease: Option<LeaseView>,
    last_checkpoint: Instant,
}

struct Beat {
    incarnation: u64,
    /// The leader's wall clock, which is what listings and the broker see.
    at_millis: u64,
    /// The same moment on the monotonic clock, which is what expiry uses: a
    /// wall-clock step on the leader must not age every broker at once.
    at: Instant,
    checkpointed: bool,
}

struct LeaseView {
    holder: String,
    expires: Instant,
}

impl SoftState {
    /// Register with the handle as its leader service and with the machine
    /// for node resets.
    pub(crate) fn install(handle: RaftHandle, machine: Arc<MetadataStateMachine>) -> Arc<Self> {
        let state = Arc::new(Self {
            handle: handle.clone(),
            machine: Arc::clone(&machine),
            serial: tokio::sync::Mutex::new(()),
            view: Mutex::new(None),
        });
        // Weak: the machine outlives nothing it points at.
        let weak: Weak<Self> = Arc::downgrade(&state);
        machine.on_node_reset(Box::new(move |node_id| {
            if let Some(state) = weak.upgrade() {
                state.forget(node_id);
            }
        }));
        handle.set_leader_service(Arc::clone(&state) as Arc<dyn LeaderService>);
        state
    }

    /// `node` with the last heartbeat this member saw as leader, if it has
    /// seen a newer one than the log holds. Unchanged on a follower.
    ///
    /// A node that has left and not been heard from this term is taken to
    /// have heartbeated when this member began judging, as expiry takes
    /// every node. Placement waits out a departed node's lease from its
    /// stamp, and the log's copy can be a checkpoint behind a beat the
    /// previous leader granted.
    pub(crate) fn overlay(&self, mut node: Node) -> Node {
        if !self.handle.is_leader() {
            return node;
        }
        let term = self.handle.current_term();
        let view = self.view.lock().expect("soft state lock");
        let view = view.as_ref().filter(|view| view.term == term);
        let beat = view
            .and_then(|view| view.beats.get(&node.node_id))
            .filter(|beat| beat.incarnation >= node.status.incarnation);
        // A stamp ahead of this leader's clock was taken on another clock, or
        // before a step back. Shown as now, never as later.
        let now_millis = crate::clock::now_millis();
        node.status.last_heartbeat_at_millis = node.status.last_heartbeat_at_millis.min(now_millis);
        let heard = match (beat, view) {
            (Some(beat), _) => Some(beat.at_millis),
            (None, _) if node.status.lifecycle != NodeLifecycle::Left => None,
            // Not judging yet in this term: as good as starting now.
            (None, None) => Some(now_millis),
            (None, Some(view)) => Some(view.since_millis),
        };
        if let Some(heard) = heard {
            node.status.last_heartbeat_at_millis = node.status.last_heartbeat_at_millis.max(heard);
        }
        node
    }

    /// What brokers said this term about the leaders they cannot reach.
    /// Empty on a follower: placement runs on the leader.
    pub(crate) fn suspicions(&self) -> Vec<crate::model::NodeSuspicion> {
        let Ok(term) = self.leading_term() else {
            return Vec::new();
        };
        let view = self.view.lock().expect("soft state lock");
        view.as_ref()
            .filter(|view| view.term == term)
            .map(|view| view.suspicions.values().cloned().collect())
            .unwrap_or_default()
    }

    async fn dispatch(&self, request: LeaderRequest) -> Result<MetaResult, NotLeader> {
        self.leading_term()?;
        // Judging here ends in `ExpireNodes` and `CheckpointHeartbeats`,
        // which a member at an older level cannot apply. Until every member
        // can, decline all of it, so the caller keeps heartbeats, expiry and
        // the lease on the log as the older build does; mixing the two would
        // expire brokers whose heartbeats only this leader has seen.
        let needs = MetaCommand::ExpireNodes { nodes: Vec::new() }.version();
        if self.handle.cluster_version().await < needs {
            // Whatever was seen before is not carried into the next time
            // this is enabled: that starts a fresh window.
            *self.view.lock().expect("soft state lock") = None;
            return Ok(Err(MetaError::Unsupported(format!(
                "leader soft state needs every member at metadata version {needs}"
            ))));
        }
        match request {
            LeaderRequest::Heartbeat {
                node_id,
                incarnation,
            } => self.heartbeat(&node_id, incarnation).await,
            LeaderRequest::Suspicion {
                node_id,
                incarnation,
                suspects,
            } => {
                let term = self.leading_term()?;
                let mut guard = self.view.lock().expect("soft state lock");
                view_for(&mut guard, term).suspicions.insert(
                    node_id.clone(),
                    crate::model::NodeSuspicion {
                        node_id,
                        incarnation,
                        suspects,
                        reported_at_millis: crate::clock::now_millis(),
                    },
                );
                Ok(Ok(MetaResponse::Unit))
            }
            LeaderRequest::ExpireStaleNodes {
                expiry_before_millis,
            } => self.expire(expiry_before_millis).await,
            LeaderRequest::AcquirePlacementLease { holder, ttl_millis } => {
                self.acquire_lease(&holder, ttl_millis).await
            }
            LeaderRequest::ReleasePlacementLease { holder } => self.release_lease(&holder).await,
        }
    }

    /// Record a heartbeat, then confirm this member still leads before
    /// answering: an answer is a promise the broker builds its lease on.
    async fn heartbeat(&self, node_id: &str, incarnation: u64) -> Result<MetaResult, NotLeader> {
        let term = self.leading_term()?;
        let at_millis = {
            let _serial = self.serial.lock().await;
            let node = match self.machine.store().get_node(node_id).await {
                Ok(node) => node,
                Err(err) => return Ok(Err(err.into())),
            };
            if incarnation < node.status.incarnation {
                return Ok(Err(MetaError::Conflict(format!(
                    "heartbeat for incarnation {incarnation} of {node_id}, which is now at {}",
                    node.status.incarnation
                ))));
            }
            // The answer grants a node that has left no lease, so there is
            // nothing to record; see `overlay`.
            if node.status.lifecycle == NodeLifecycle::Left {
                return Ok(Ok(MetaResponse::Node { node }));
            }
            let now_millis = crate::clock::now_millis();
            let mut guard = self.view.lock().expect("soft state lock");
            let view = view_for(&mut guard, term);
            let beat = view.beats.entry(node_id.to_string()).or_insert(Beat {
                incarnation,
                at_millis: 0,
                at: Instant::now(),
                checkpointed: false,
            });
            if incarnation > beat.incarnation {
                beat.at_millis = 0;
            }
            beat.incarnation = incarnation;
            // Never backwards: two connections' heartbeats can arrive out of
            // order, and a wall-clock step must not rewind what was shown.
            beat.at_millis = beat.at_millis.max(now_millis);
            beat.at = Instant::now();
            beat.checkpointed = false;
            beat.at_millis
        };
        if !self.handle.confirm_leadership_within(CONFIRM_WITHIN).await
            || self.handle.current_term() != term
        {
            return Err(NotLeader);
        }
        Ok(self
            .machine
            .store()
            .get_node(node_id)
            .await
            .map(|mut node| {
                node.status.last_heartbeat_at_millis =
                    node.status.last_heartbeat_at_millis.max(at_millis);
                MetaResponse::Node { node }
            })
            .map_err(Into::into))
    }

    /// Mark down every serving node this leader has not heard from since
    /// `expiry_before_millis`, counting the start of its leadership as a
    /// heartbeat from everyone.
    async fn expire(&self, expiry_before_millis: u64) -> Result<MetaResult, NotLeader> {
        let term = self.leading_term()?;
        let _serial = self.serial.lock().await;
        let nodes = match self.machine.store().list_nodes().await {
            Ok(nodes) => nodes,
            Err(err) => return Ok(Err(err.into())),
        };
        let now_millis = crate::clock::now_millis();
        let now = Instant::now();
        let (stale, checkpoint) = {
            let mut guard = self.view.lock().expect("soft state lock");
            let view = view_for(&mut guard, term);
            // Aged on the monotonic clock, then expressed as the wall-clock
            // time the caller's cutoff is in.
            let seen_at = |age: Duration| now_millis.saturating_sub(age.as_millis() as u64);
            let stale: Vec<NodeIncarnation> = nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.status.lifecycle,
                        NodeLifecycle::Live | NodeLifecycle::Draining
                    )
                })
                .filter(|node| {
                    let beat = view
                        .beats
                        .get(&node.node_id)
                        .filter(|beat| beat.incarnation >= node.status.incarnation);
                    // Only monotonic ages, never the log's stamp. That stamp
                    // is a checkpoint from before this term, so it adds
                    // nothing unless it is ahead of this leader's clock, and
                    // then waiting it out would keep a dead broker placeable
                    // for as long as the clocks disagree.
                    let heard = match beat {
                        Some(beat) => beat.at,
                        None => view
                            .registered
                            .get(&node.node_id)
                            .copied()
                            .unwrap_or(view.since),
                    };
                    let last = seen_at(now.saturating_duration_since(heard));
                    last < expiry_before_millis
                })
                .map(|node| NodeIncarnation {
                    node_id: node.node_id.clone(),
                    incarnation: node.status.incarnation,
                })
                .collect();
            let checkpoint =
                if now.saturating_duration_since(view.last_checkpoint) >= CHECKPOINT_EVERY {
                    view.last_checkpoint = now;
                    let mut beats: Vec<HeartbeatSeen> = view
                        .beats
                        .iter_mut()
                        .filter(|(_, beat)| !beat.checkpointed)
                        .map(|(node_id, beat)| {
                            beat.checkpointed = true;
                            HeartbeatSeen {
                                node_id: node_id.clone(),
                                incarnation: beat.incarnation,
                                at_millis: beat.at_millis,
                            }
                        })
                        .collect();
                    beats.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                    beats
                } else {
                    Vec::new()
                };
            (stale, checkpoint)
        };

        let expired = if stale.is_empty() {
            Vec::new()
        } else {
            match self
                .propose(MetaCommand::ExpireNodes { nodes: stale })
                .await
            {
                Ok(MetaResponse::Nodes { nodes }) => nodes,
                Ok(_) => return Ok(Err(MetaError::Internal("unexpected expiry answer".into()))),
                Err(err) => return Ok(Err(err)),
            }
        };
        if !checkpoint.is_empty()
            && let Err(err) = self
                .propose(MetaCommand::CheckpointHeartbeats { beats: checkpoint })
                .await
        {
            // Only listings on followers are behind for it; expiry never
            // reads the checkpoint on the leader that wrote it.
            tracing::debug!(error = %err, "could not checkpoint heartbeats");
        }
        Ok(Ok(MetaResponse::Nodes { nodes: expired }))
    }

    /// Take or renew the placement lease. A renewal is soft state; only a
    /// change of holder is written, because only that advances the token.
    async fn acquire_lease(&self, holder: &str, ttl_millis: u64) -> Result<MetaResult, NotLeader> {
        let term = self.leading_term()?;
        let _serial = self.serial.lock().await;
        let store = self.machine.store();
        let recorded = store.placement_holder().await;
        let ttl = Duration::from_millis(ttl_millis);
        let now = Instant::now();
        let current = {
            let mut guard = self.view.lock().expect("soft state lock");
            let view = view_for(&mut guard, term);
            if view.lease.is_none()
                && let Some(recorded) = &recorded
            {
                // A holder this leadership has not seen renew gets a full
                // window from when it started judging, as a broker does.
                view.lease = Some(LeaseView {
                    holder: recorded.clone(),
                    expires: view.since + ttl,
                });
            }
            view.lease
                .as_ref()
                .map(|lease| (lease.holder.clone(), lease.expires))
        };
        match current {
            Some((current, _)) if current == holder && recorded.as_deref() == Some(holder) => {
                self.set_lease(term, holder, now + ttl);
                Ok(store
                    .placement_token()
                    .await
                    .map(|token| MetaResponse::PlacementLease {
                        token,
                        taken: false,
                    })
                    .map_err(Into::into))
            }
            Some((_, expires)) if expires > now => Ok(Ok(MetaResponse::NoPlacementLease)),
            _ => {
                let taken = self
                    .propose(MetaCommand::TakePlacementLease {
                        holder: holder.to_string(),
                    })
                    .await;
                if matches!(taken, Ok(MetaResponse::PlacementLease { .. })) {
                    self.set_lease(term, holder, Instant::now() + ttl);
                }
                Ok(taken)
            }
        }
    }

    /// Let the lease lapse now if `holder` has it. The holder stays named, so
    /// whoever takes it next still advances the token.
    async fn release_lease(&self, holder: &str) -> Result<MetaResult, NotLeader> {
        let term = self.leading_term()?;
        let _serial = self.serial.lock().await;
        let recorded = self.machine.store().placement_holder().await;
        let mut guard = self.view.lock().expect("soft state lock");
        let view = view_for(&mut guard, term);
        let holds = match &view.lease {
            Some(lease) => lease.holder == holder,
            None => recorded.as_deref() == Some(holder),
        };
        if holds {
            view.lease = Some(LeaseView {
                holder: holder.to_string(),
                expires: Instant::now(),
            });
        }
        Ok(Ok(MetaResponse::Unit))
    }

    fn set_lease(&self, term: u64, holder: &str, expires: Instant) {
        let mut guard = self.view.lock().expect("soft state lock");
        view_for(&mut guard, term).lease = Some(LeaseView {
            holder: holder.to_string(),
            expires,
        });
    }

    fn leading_term(&self) -> Result<u64, NotLeader> {
        let term = self.handle.current_term();
        if self.handle.is_leader() {
            Ok(term)
        } else {
            Err(NotLeader)
        }
    }

    async fn propose(&self, command: MetaCommand) -> MetaResult {
        let bytes = self
            .handle
            .write(encode_command(&command))
            .await
            .map_err(|err| MetaError::Internal(format!("{err:#}")))?;
        decode_result(&bytes)?
    }

    /// A node record was replaced or removed; what was seen of the old one
    /// no longer describes anything.
    fn forget(&self, node_id: Option<&str>) {
        let mut guard = self.view.lock().expect("soft state lock");
        if let Some(view) = guard.as_mut() {
            match node_id {
                Some(node_id) => {
                    view.beats.remove(node_id);
                    view.registered.insert(node_id.to_string(), Instant::now());
                }
                None => {
                    // Every record may have been replaced: judge them all as
                    // if this leadership had just begun.
                    view.beats.clear();
                    view.registered.clear();
                    view.lease = None;
                    view.since = Instant::now();
                    view.since_millis = crate::clock::now_millis();
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl LeaderService for SoftState {
    async fn handle(&self, request: &[u8]) -> Result<Vec<u8>, NotLeader> {
        let result = match serde_json::from_slice::<LeaderRequest>(request) {
            Ok(request) => self.dispatch(request).await?,
            // A request from a newer build: answered, never guessed at.
            Err(err) => Err(MetaError::Unsupported(format!(
                "undecodable leader request: {err}"
            ))),
        };
        Ok(encode_result(&result))
    }
}

/// The view for `term`, started fresh if the one held is from another term.
fn view_for(slot: &mut Option<View>, term: u64) -> &mut View {
    if slot.as_ref().is_none_or(|view| view.term != term) {
        let now = Instant::now();
        *slot = Some(View {
            term,
            since: now,
            since_millis: crate::clock::now_millis(),
            beats: HashMap::new(),
            suspicions: HashMap::new(),
            registered: HashMap::new(),
            lease: None,
            last_checkpoint: now,
        });
    }
    slot.as_mut().expect("just set")
}

#[cfg(test)]
mod tests;
