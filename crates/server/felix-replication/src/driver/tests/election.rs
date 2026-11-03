//! The leader's side of electing without the control plane: stamping
//! batches with its set, keeping that set, and rule 5, counting nothing more
//! once it has answered a newer fence.

use felix_wire::internal::PeerCapabilities;

use super::quorum::{follower_ack_marks, follower_ack_marks_adopted, quorum_leader};
use super::*;

/// Stores every batch, records what it was stamped with, and says which
/// followers offered `ELECTION`.
struct Followers {
    /// Whether this broker offers `ELECTION`.
    electing: bool,
    /// The followers that offered it.
    offered: &'static [&'static str],
    sent: Mutex<Vec<(String, Option<u64>)>>,
}

impl Followers {
    fn new(electing: bool, offered: &'static [&'static str]) -> Self {
        Self {
            electing,
            offered,
            sent: Mutex::new(Vec::new()),
        }
    }

    fn stamps(&self) -> Vec<(String, Option<u64>)> {
        let mut sent = self.sent.lock().expect("lock").clone();
        sent.sort();
        sent.dedup();
        sent
    }
}

impl PeerRequester for Followers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (InternalMessage::ReplicateRecords(batch)
        | InternalMessage::ReplicateMarkedRecords(batch)) = message
        else {
            panic!("the driver sent something other than a replication batch");
        };
        self.sent
            .lock()
            .expect("lock")
            .push((node_id.to_string(), batch.set_generation));
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        Some(if self.offered.contains(&node_id) {
            PeerCapabilities::ELECTION
        } else {
            PeerCapabilities::NONE
        })
    }

    fn offers_election(&self) -> bool {
        self.electing
    }
}

async fn pass<R: PeerRequester + Sync>(followers: &R, broker: &Arc<Broker>, marks: &QuorumMarks) {
    pass_with(followers, broker, marks, &Unfenced).await;
}

async fn pass_with<R: PeerRequester + Sync>(
    followers: &R,
    broker: &Arc<Broker>,
    marks: &QuorumMarks,
    fence: &dyn WriteFence,
) {
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once_with(
        followers,
        broker,
        &router,
        fence,
        &NoGate,
        marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await;
}

/// A broker that has not opened any shard since it started.
struct Restarted;

impl WriteFence for Restarted {
    fn quiesced(&self, _key: &crate::ShardKey) -> bool {
        true
    }

    fn opened_since(&self, _key: &crate::ShardKey, _generation: u64) -> bool {
        false
    }
}

async fn stream_log(broker: &Broker) -> felix_broker::StreamLog {
    broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
}

/// **A leader that answered a newer fence stops counting** once every
/// follower checks sets: a candidate may have been elected on its answer, and
/// what its followers confirmed may not be in the new leader's log. Without
/// the switch, or while a follower does not offer `ELECTION`, the followers'
/// answers still count, as they did before elections.
#[tokio::test]
async fn a_leader_that_answers_a_newer_fence_stops_counting() {
    let watched = watch_key(&key());
    for (electing, offered, counted) in [
        (true, &["broker-b", "broker-c"][..], 0),
        (false, &["broker-b", "broker-c"][..], 3),
        (true, &["broker-b"][..], 3),
    ] {
        let (broker, _dir) = quorum_leader(3, 4).await;
        stream_log(&broker)
            .await
            .accept_generation(5, Some("broker-c"))
            .await
            .expect("answer a newer fence");
        let marks = follower_ack_marks();
        let followers = Followers::new(electing, offered);

        pass(&followers, &broker, &marks).await;

        assert!(
            followers.stamps().len() >= 2,
            "both followers confirmed the records"
        );
        assert_eq!(
            marks.offset(&watched, 4).unwrap_or(0),
            counted,
            "electing: {electing}, offered by {offered:?}",
        );
    }
}

/// **A batch carries the set's generation only to a follower that offered
/// `ELECTION`, and only with the switch on.** The leader keeps that set
/// before it ships at it.
#[tokio::test]
async fn a_stamped_batch_reaches_only_a_follower_that_offered_election() {
    let (broker, _dir) = quorum_leader(3, 4).await;
    // The leader's open records its own ballot; the fixture skips the open.
    stream_log(&broker)
        .await
        .accept_generation(4, Some(LOCAL))
        .await
        .expect("own ballot");
    let followers = Followers::new(true, &["broker-b"]);
    pass(&followers, &broker, &follower_ack_marks()).await;
    assert_eq!(
        followers.stamps(),
        vec![
            ("broker-b".to_string(), Some(4)),
            ("broker-c".to_string(), None),
        ]
    );
    let set = stream_log(&broker).await.replica_set().expect("kept");
    assert_eq!(set.current.generation, 4);
    let nodes: Vec<&str> = set
        .current
        .members
        .iter()
        .map(|member| member.node.as_str())
        .collect();
    assert_eq!(nodes, vec![LOCAL, "broker-b", "broker-c"]);

    let (broker, _dir) = quorum_leader(3, 4).await;
    let followers = Followers::new(false, &["broker-b", "broker-c"]);
    pass(&followers, &broker, &follower_ack_marks()).await;
    assert_eq!(
        followers.stamps(),
        vec![
            ("broker-b".to_string(), None),
            ("broker-c".to_string(), None),
        ]
    );
    assert_eq!(stream_log(&broker).await.set_generation(), 0);
}

/// **Under `joint_fence`, a leader that keeps a changed set while shipping
/// keeps the set it leaves beside it**, so a fence that runs afterwards still
/// fences that set. Without the feature it keeps the new set alone, as before.
#[tokio::test]
async fn under_joint_fence_a_shipping_leader_keeps_the_set_it_leaves() {
    use felix_common::fleet::{FleetGate, GENERATION_START, JOINT_FENCE, MAJORITY_ACK};
    use felix_storage::disk_log::{Member, Membership, ReplicaSet};

    for joint in [true, false] {
        let (broker, _dir) = quorum_leader(3, 4).await;
        let log = stream_log(&broker).await;
        log.accept_generation(3, Some(LOCAL)).await.expect("ballot");
        let before = Membership {
            generation: 3,
            members: [LOCAL, "broker-b", "broker-d"]
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .to_vec(),
        };
        log.accept_set(ReplicaSet {
            current: before,
            leaving: None,
        })
        .await
        .expect("the set before");
        log.accept_generation(4, Some(LOCAL)).await.expect("ballot");
        let names: Vec<&str> = [GENERATION_START, MAJORITY_ACK]
            .iter()
            .chain(joint.then_some(&JOINT_FENCE))
            .map(|feature| feature.name())
            .collect();
        let fleet = FleetGate::new(names.clone());
        fleet.observe(names);
        let marks = QuorumMarks::with_fleet(Arc::new(fleet));

        pass(&Followers::new(true, &["broker-b"]), &broker, &marks).await;

        let kept = stream_log(&broker).await.replica_set().expect("kept");
        assert_eq!(kept.current.generation, 4);
        assert_eq!(
            kept.leaving.map(|old| old.generation),
            joint.then_some(3),
            "joint: {joint}"
        );
    }
}

/// **Under `joint_fence`, a shipping leader that does not know the set it
/// leaves keeps nothing.** Its log keeps the set of generation 2, not the one
/// right before 4, and its watch never saw generation 3: keeping the new set
/// would hide the old one from the fence, which then stays pending instead.
#[tokio::test]
async fn under_joint_fence_a_shipping_leader_keeps_no_set_when_the_one_before_is_unknown() {
    use felix_common::fleet::{FleetGate, GENERATION_START, JOINT_FENCE, MAJORITY_ACK};
    use felix_storage::disk_log::{Member, Membership, ReplicaSet};

    let (broker, _dir) = quorum_leader(3, 4).await;
    let log = stream_log(&broker).await;
    log.accept_generation(2, Some(LOCAL)).await.expect("ballot");
    log.accept_set(ReplicaSet {
        current: Membership {
            generation: 2,
            members: [LOCAL, "broker-b", "broker-d"]
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .to_vec(),
        },
        leaving: None,
    })
    .await
    .expect("an older set");
    log.accept_generation(4, Some(LOCAL)).await.expect("ballot");
    let names = [GENERATION_START, MAJORITY_ACK, JOINT_FENCE].map(|feature| feature.name());
    let fleet = FleetGate::new(names);
    fleet.observe(names);
    let marks = QuorumMarks::with_fleet(Arc::new(fleet));

    pass(&Followers::new(true, &["broker-b"]), &broker, &marks).await;

    assert_eq!(stream_log(&broker).await.set_generation(), 2);
}

/// **A leader elected on the route's members keeps that set at the
/// generation it was elected on, even after a restart.** It was elected at 4
/// on the set of generation 3, and the control plane named it at 4 on the
/// same members. A restart lost the election, which lives in memory, and the
/// shard has not opened since. Keeping the set at 4 would refuse the next
/// leader elected on the set of 3, so the set stays at 3 and batches carry 3.
#[tokio::test]
async fn a_restarted_elected_leader_keeps_the_set_it_was_elected_on() {
    use felix_storage::disk_log::{Member, Membership, ReplicaSet};

    let (broker, _dir) = quorum_leader(3, 4).await;
    let log = stream_log(&broker).await;
    log.accept_generation(3, Some(LOCAL)).await.expect("ballot");
    log.accept_set(ReplicaSet {
        current: Membership {
            generation: 3,
            members: [LOCAL, "broker-b", "broker-c"]
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .to_vec(),
        },
        leaving: None,
    })
    .await
    .expect("the set it was elected on");
    log.accept_generation(4, Some(LOCAL))
        .await
        .expect("its own ballot");
    let followers = Followers::new(true, &["broker-b", "broker-c"]);

    pass_with(&followers, &broker, &follower_ack_marks(), &Restarted).await;

    assert_eq!(stream_log(&broker).await.set_generation(), 3);
    assert_eq!(
        followers.stamps(),
        vec![
            ("broker-b".to_string(), Some(3)),
            ("broker-c".to_string(), Some(3)),
        ]
    );
}

/// **An elected leader the control plane adopted keeps the set it was
/// elected on.** It stood at 4 on the set of 3 and opened once its fence
/// finished; the control plane then named it at 4, which drops the election's
/// entry. Keeping the set at 4 would make it the only copy counting 4,
/// so it would refuse the next leader elected on the set of 3 for good.
#[tokio::test]
async fn an_adopted_elected_leader_keeps_the_set_it_was_elected_on() {
    let (broker, _dir) = quorum_leader(3, 4).await;
    let log = stream_log(&broker).await;
    log.accept_generation(3, Some(LOCAL)).await.expect("ballot");
    log.accept_set(set_of_three(3))
        .await
        .expect("the set it was elected on");
    log.accept_generation(4, Some(LOCAL))
        .await
        .expect("its own ballot");
    let followers = Followers::new(true, &["broker-b", "broker-c"]);

    let marks = follower_ack_marks();
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    elections.record(
        key(),
        crate::election::Elected {
            leader: LOCAL.to_string(),
            generation: 4,
            set: set_of_three(3).current,
            leaving: None,
            incarnation: 0,
        },
    );
    elections.opened(&key(), 4);
    // The control plane adopted it, and the overlay dropped the entry.
    elections.withdraw(&key(), 4);
    marks.set_elections(elections);

    pass(&followers, &broker, &marks).await;

    assert_eq!(stream_log(&broker).await.set_generation(), 3);
    assert_eq!(
        followers.stamps(),
        vec![
            ("broker-b".to_string(), Some(3)),
            ("broker-c".to_string(), Some(3)),
        ]
    );
}

fn set_of_three(generation: u64) -> felix_storage::disk_log::ReplicaSet {
    use felix_storage::disk_log::{Member, Membership, ReplicaSet};

    ReplicaSet {
        current: Membership {
            generation,
            members: [LOCAL, "broker-b", "broker-c"]
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .to_vec(),
        },
        leaving: None,
    }
}

/// **A leader that opened the next generation on the same members keeps
/// the set at it.** The control plane bumped the shard from 3 to 4 without
/// changing its members (a pending set taken, a reissue) and this broker
/// opened it at 4 unfenced. Keeping the set at 3 would leave every copy at 3,
/// and a later promotee with no record of 3 could never fence the shard.
#[tokio::test]
async fn a_leader_opened_at_a_bump_on_the_same_members_keeps_the_new_set() {
    use felix_storage::disk_log::{Member, Membership, ReplicaSet};

    let (broker, _dir) = quorum_leader(3, 4).await;
    let log = stream_log(&broker).await;
    log.accept_generation(3, Some(LOCAL)).await.expect("ballot");
    log.accept_set(ReplicaSet {
        current: Membership {
            generation: 3,
            members: [LOCAL, "broker-b", "broker-c"]
                .map(|node| Member {
                    node: node.to_string(),
                    storage: None,
                    learner: false,
                    wiped: false,
                })
                .to_vec(),
        },
        leaving: None,
    })
    .await
    .expect("the set of 3");
    log.accept_generation(4, Some(LOCAL))
        .await
        .expect("the open's ballot");
    let followers = Followers::new(true, &["broker-b", "broker-c"]);

    pass(&followers, &broker, &follower_ack_marks()).await;

    assert_eq!(stream_log(&broker).await.set_generation(), 4);
    assert_eq!(
        followers.stamps(),
        vec![
            ("broker-b".to_string(), Some(4)),
            ("broker-c".to_string(), Some(4)),
        ]
    );
}

/// Refuses every batch to `behind` as a route that is behind, takes every
/// fence, and records the batches' stamps and the fences.
struct BehindFollower {
    behind: &'static str,
    stamps: Mutex<Vec<(String, Option<u64>)>>,
    fences: Mutex<Vec<(String, felix_wire::internal::Fence)>>,
}

impl PeerRequester for BehindFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let batch = match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => batch,
            InternalMessage::Fence(fence) => {
                self.fences
                    .lock()
                    .expect("lock")
                    .push((node_id.to_string(), fence));
                return Ok(InternalMessage::FenceOk(felix_wire::internal::FenceOk {
                    correlation_id: 0,
                    log_end: 0,
                    commit_offset: 0,
                    last_generation: 0,
                }));
            }
            other => panic!("unexpected {other:?}"),
        };
        self.stamps
            .lock()
            .expect("lock")
            .push((node_id.to_string(), batch.set_generation));
        if node_id == self.behind {
            return Ok(InternalMessage::ReplicateError(
                felix_wire::internal::ReplicateError {
                    correlation_id: 0,
                    code: felix_wire::internal::ErrorCode::StaleRoute,
                    expected_offset: 0,
                    detail: "behind".to_string(),
                },
            ));
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(PeerCapabilities::ELECTION)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// `LOCAL` leading by election at 4, on the set of generation 3.
fn elected_marks(leader: &str) -> QuorumMarks {
    use felix_storage::disk_log::{Member, Membership};

    let marks = follower_ack_marks_adopted(true);
    let elections = Arc::new(crate::election::Elections::new(
        LOCAL,
        Arc::new(tokio::sync::Notify::new()),
    ));
    elections.record(
        key(),
        crate::election::Elected {
            leader: leader.to_string(),
            generation: 4,
            set: Membership {
                generation: 3,
                members: [LOCAL, "broker-b", "broker-c"]
                    .map(|node| Member {
                        node: node.to_string(),
                        storage: None,
                        learner: false,
                        wiped: false,
                    })
                    .to_vec(),
            },
            leaving: None,
            incarnation: 0,
        },
    );
    marks.set_elections(elections);
    marks
}

/// **An elected leader stamps the set it stood on, and sends its fence again
/// to a follower whose route is behind.** No control-plane route carries the
/// elected generation, so a follower that missed the fence (a leader cut off
/// during the election) refuses every batch until the fence comes again. A
/// follower that takes the batches gets no fence. A leader the control plane
/// named sends the set it keeps instead, since a batch teaches no set being
/// left that the follower's view lacks.
#[tokio::test]
async fn an_elected_leader_fences_a_follower_whose_route_is_behind_again() {
    for (leader, candidate, set_generation) in [(LOCAL, true, 3), ("broker-z", false, 4)] {
        let (broker, _dir) = quorum_leader(3, 4).await;
        stream_log(&broker)
            .await
            .accept_generation(4, Some(LOCAL))
            .await
            .expect("own ballot");
        let followers = BehindFollower {
            behind: "broker-b",
            stamps: Mutex::new(Vec::new()),
            fences: Mutex::new(Vec::new()),
        };
        let marks = elected_marks(leader);

        pass(&followers, &broker, &marks).await;

        let stamp = (leader == LOCAL).then_some(3).or(Some(4));
        let stamps = followers.stamps.lock().expect("lock").clone();
        assert!(
            stamps.iter().all(|(_, stamped)| *stamped == stamp),
            "stamped {stamps:?}, expected {stamp:?}"
        );
        let fences = followers.fences.lock().expect("lock").clone();
        assert_eq!(fences.len(), 1, "{fences:?}");
        let (node, fence) = &fences[0];
        assert_eq!(node, "broker-b");
        assert_eq!(fence.shard.generation, 4);
        assert_eq!(fence.log, felix_wire::internal::ReplicaLog::Stream);
        let election = fence.election.as_ref().expect("a fence naming its set");
        assert_eq!(election.candidate, candidate);
        assert_eq!(election.set.generation, set_generation);
    }
}

/// Takes every fence, and says it offers `ELECTION`.
struct FenceTaker;

impl PeerRequester for FenceTaker {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let fence = match message {
            InternalMessage::Fence(fence) => fence,
            // Shipping begins once the shard has opened.
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                return Ok(InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id: 0,
                    durable_offset: batch.first_offset + batch.payloads.len() as u64,
                }));
            }
            other => panic!("unexpected {other:?}"),
        };
        Ok(InternalMessage::FenceOk(felix_wire::internal::FenceOk {
            correlation_id: fence.correlation_id,
            log_end: 0,
            commit_offset: 0,
            last_generation: 0,
        }))
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<PeerCapabilities, PeerError> {
        Ok(crate::promotion::REQUIRED.union(PeerCapabilities::ELECTION))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(crate::promotion::REQUIRED.union(PeerCapabilities::ELECTION))
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// Opens the shard and records whether its standing was still marked as
/// being fenced when the open ran.
struct WatchedOpen {
    elections: Arc<crate::election::Elections>,
    awaiting: Mutex<Option<u64>>,
    held: Mutex<Vec<bool>>,
}

impl WatchedOpen {
    fn new(elections: &Arc<crate::election::Elections>) -> Self {
        Self {
            elections: Arc::clone(elections),
            awaiting: Mutex::new(Some(4)),
            held: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl crate::promotion::PromotionGate for WatchedOpen {
    fn awaiting(&self, _key: &crate::ShardKey) -> Option<u64> {
        *self.awaiting.lock().expect("lock")
    }

    async fn open(&self, _key: &crate::ShardKey, generation: u64) -> bool {
        self.held
            .lock()
            .expect("lock")
            .push(self.elections.is_fencing(&key(), generation));
        *self.awaiting.lock().expect("lock") = None;
        true
    }
}

/// **A standing is kept until its shard has opened.** The shard opens after
/// every shard in the pass has fenced, and the candidate withdraws a standing
/// nothing is fencing, which would burn the generation or open the shard at
/// one the candidate no longer stands for.
#[tokio::test]
async fn a_standing_is_kept_until_its_shard_opens() {
    let (broker, _dir) = quorum_leader(3, 4).await;
    let marks = elected_marks(LOCAL);
    let elections = Arc::clone(marks.elections().expect("elections"));
    let gate = WatchedOpen::new(&elections);
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);

    replicate_once_with(
        &FenceTaker,
        &broker,
        &router,
        &Unfenced,
        &gate,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await;

    assert_eq!(
        *gate.held.lock().expect("lock"),
        [true],
        "the standing was marked fenced while the shard opened"
    );
    assert!(
        !elections.is_fencing(&key(), 4),
        "the standing is released once the shard has opened"
    );
    assert!(
        elections.opened_elected(&key(), 4, 0),
        "an adoption must not make the open read as a plain bump"
    );
}

/// **The running driver keeps a standing until its shard has opened too.**
/// Its shards fence and open in their own futures, apart from the pass that
/// [`a_standing_is_kept_until_its_shard_opens`] drives.
#[tokio::test]
async fn the_running_driver_keeps_a_standing_until_its_shard_opens() {
    let (broker, _dir) = quorum_leader(3, 4).await;
    let marks = Arc::new(elected_marks(LOCAL));
    let elections = Arc::clone(marks.elections().expect("elections"));
    let gate = Arc::new(WatchedOpen::new(&elections));
    let driver = spawn(
        Arc::new(FenceTaker),
        Arc::clone(&broker),
        router(LOCAL, &["broker-b", "broker-c"], 4),
        Arc::new(Unfenced),
        Arc::clone(&gate) as Arc<dyn crate::promotion::PromotionGate>,
        Published {
            marks,
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        CancellationToken::new(),
    );

    let opened = tokio::time::timeout(Duration::from_secs(10), async {
        while gate.held.lock().expect("lock").is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(opened.is_ok(), "the shard never opened");
    assert!(
        gate.held.lock().expect("lock")[0],
        "the standing was marked fenced while the shard opened"
    );
    driver.stop().await;
}
