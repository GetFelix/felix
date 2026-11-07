//! Acknowledging while a set change is pending: a record is held only when a
//! majority of the set being left holds it as well as a majority of the new
//! set (rule 8 of "Electing without the control plane"), and the leader
//! reports its generation-start record once that is so.

use felix_common::fleet::{FENCED_CACHES, FleetGate, GENERATION_START, JOINT_FENCE, MAJORITY_ACK};
use felix_router::PendingChange;
use felix_wire::internal::PeerCapabilities;

use super::quorum::quorum_leader;
use super::*;

/// Answers every batch for the nodes it was given and refuses the rest.
struct Answers {
    ok: &'static [&'static str],
}

impl PeerRequester for Answers {
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
        if !self.ok.contains(&node_id) {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
            learner: false,
        }))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(PeerCapabilities::ELECTION)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

fn marks(joint_fence: bool) -> QuorumMarks {
    marks_for_caches(joint_fence, false)
}

/// [`marks`], and with `fenced_caches` when `by_followers` says so: a cache's
/// marks are then decided by its followers' answers, as in a fleet where a
/// cache can have a pending set at all.
fn marks_for_caches(joint_fence: bool, by_followers: bool) -> QuorumMarks {
    let names: Vec<&str> = [GENERATION_START, MAJORITY_ACK]
        .iter()
        .chain(joint_fence.then_some(&JOINT_FENCE))
        .chain(by_followers.then_some(&FENCED_CACHES))
        .map(|feature| feature.name())
        .collect();
    let fleet = FleetGate::new(names.clone());
    fleet.observe(names);
    QuorumMarks::with_fleet(Arc::new(fleet))
}

/// The route of generation 4 led from here: followers `replicas`, and the
/// control plane's set `leaving` when a change is pending at generation 5.
fn route(replicas: &[&str], leaving: Option<&[&str]>) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: LOCAL.to_string(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation: 4,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            pending: leaving.map(|leaving| PendingChange {
                generation: 5,
                leaving: leaving.iter().map(|node| node.to_string()).collect(),
            }),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// One pass over three records at generation 4: the mark it published, and
/// the pending set it reported as started.
async fn pass(
    ok: &'static [&'static str],
    router: &ShardRouter,
    marks: &QuorumMarks,
) -> (u64, Option<u64>) {
    let (broker, _dir) = quorum_leader(3, 4).await;
    let pass = replicate_once(
        &Answers { ok },
        &broker,
        router,
        marks,
        None,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
    )
    .await;
    let started = pass.reports.first().and_then(|report| {
        assert_eq!(
            report.generation, 4,
            "the report names the route's generation"
        );
        report.joint_start_acked
    });
    (marks.offset(&watch_key(&key()), 4).unwrap_or(0), started)
}

const OLD: &[&str] = &[LOCAL, "broker-b", "broker-c"];

/// **Acknowledgements wait for both majorities.** The set moves from
/// {a, b, c} to {a, c, d}. `c` is in both sets, so its copy completes a
/// majority of each, and the start record is then reported.
#[tokio::test]
async fn a_pending_change_acknowledges_on_both_majorities() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));

    let (mark, started) = pass(&["broker-c"], &router, &marks(true)).await;

    assert_eq!(mark, 3);
    assert_eq!(started, Some(5));
}

/// **A majority of the new set alone does not acknowledge.** `d` and the
/// leader are two of {a, c, d}, and only one of {a, b, c}. Nothing is
/// reported either: the start record is not held by the set being left.
#[tokio::test]
async fn a_new_set_majority_alone_does_not_acknowledge() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));

    let (mark, started) = pass(&["broker-d"], &router, &marks(true)).await;

    assert_eq!(mark, 0);
    assert_eq!(started, None);
}

/// **A pending change a leader cannot count acknowledges nothing.** The route
/// names the new set, so without the joint-fence feature the new set's
/// majority must not acknowledge: failover could be placed from the old set.
#[tokio::test]
async fn without_joint_fence_a_pending_change_acknowledges_nothing() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));

    let (mark, started) = pass(&["broker-d"], &router, &marks(false)).await;

    assert_eq!(mark, 0);
    assert_eq!(started, None);
}

/// **After the flip only the new set counts.** The route no longer carries a
/// pending change, so `d` with the leader is enough.
#[tokio::test]
async fn after_the_flip_the_new_set_alone_acknowledges() {
    let router = route(&["broker-c", "broker-d"], None);

    let (mark, started) = pass(&["broker-d"], &router, &marks(true)).await;

    assert_eq!(mark, 3);
    assert_eq!(started, None);
}

/// **A leader that does not know the set being left acknowledges nothing.**
#[tokio::test]
async fn an_unknown_set_being_left_acknowledges_nothing() {
    let router = route(&["broker-c", "broker-d"], Some(&[]));

    let (mark, started) = pass(&["broker-c", "broker-d"], &router, &marks(true)).await;

    assert_eq!(mark, 0);
    assert_eq!(started, None);
}

/// **A stream that does not acknowledge at quorum reports no joint start**,
/// whatever its tail: nothing there says a majority holds the record.
#[tokio::test]
async fn a_stream_not_acknowledging_at_quorum_reports_no_joint_start() {
    let (broker, _dir) = super::leader_led_from(3, 4, 0).await;
    let router = route(&["broker-c", "broker-d"], Some(OLD));
    let pass = replicate_once(
        &Answers { ok: &["broker-c"] },
        &broker,
        &router,
        &marks(true),
        None,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
    )
    .await;

    assert!(!pass.reports.is_empty());
    assert!(pass.reports.iter().all(|r| r.joint_start_acked.is_none()));
}

/// [`Answers`], for peers that give a storage identity in their handshake.
struct Identified(Answers);

impl PeerRequester for Identified {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        self.0.request(node_id, addr, message).await
    }

    async fn identity(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> std::result::Result<crate::peer::PeerIdentity, PeerError> {
        Ok(crate::peer::PeerIdentity::storage(
            felix_storage::StorageId::from_bytes([9; 16]),
        ))
    }

    fn recorded_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        self.0.recorded_capabilities(node_id)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// **A leader reports the followers that hold its generation's start
/// record.** `c` answered the three records, past the start; `d` is down and
/// holds none. A follower that gave no storage identity is not listed
/// whatever its position, since nobody can say which copy it describes.
#[tokio::test]
async fn a_report_lists_the_followers_holding_the_generations_start() {
    let router = route(&["broker-c", "broker-d"], None);
    let marks = marks(true);

    for (identified, expected) in [(true, vec!["broker-c".to_string()]), (false, Vec::new())] {
        let (broker, _dir) = quorum_leader(3, 4).await;
        let pass = if identified {
            replicate_once(
                &Identified(Answers { ok: &["broker-c"] }),
                &broker,
                &router,
                &marks,
                None,
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
            )
            .await
        } else {
            replicate_once(
                &Answers { ok: &["broker-c"] },
                &broker,
                &router,
                &marks,
                None,
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
            )
            .await
        };
        let report = pass.reports.first().expect("a report");
        assert_eq!(report.holds_start, expected, "identified: {identified}");
    }
}

/// Stores a cache shard's batches: cache records for the nodes in `cache`,
/// counter records for those in `counters`, and refuses the rest.
struct CacheAnswers {
    cache: &'static [&'static str],
    counters: &'static [&'static str],
}

impl PeerRequester for CacheAnswers {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (batch, ok) = match message {
            InternalMessage::ReplicateCacheRecords(batch) => (batch, self.cache),
            InternalMessage::ReplicateCounterRecords(batch) => (batch, self.counters),
            other => panic!("the driver sent {:?} for a cache shard", other.kind()),
        };
        if !ok.contains(&node_id) {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "down".to_string(),
            });
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
            learner: false,
        }))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(PeerCapabilities::ELECTION)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

fn cache_key() -> ShardKey {
    ShardKey {
        kind: felix_router::ShardKind::Cache,
        ..key()
    }
}

/// The cache's route at `generation`, led from here, as [`route`] is.
fn cache_route(replicas: &[&str], generation: u64, leaving: Option<&[&str]>) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: cache_key(),
            leader: LOCAL.to_string(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation,
            draining: false,
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            pending: leaving.map(|leaving| PendingChange {
                generation,
                leaving: leaving.iter().map(|node| node.to_string()).collect(),
            }),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

/// One pass over the cache shard: its counter mark and counter tail, and the
/// pending set it reported as started.
async fn cache_pass(
    answers: CacheAnswers,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    marks: &QuorumMarks,
    generation: u64,
) -> (u64, u64, Option<u64>) {
    let pass = replicate_once(
        &answers,
        broker,
        router,
        marks,
        None,
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
        &mut HashMap::default(),
    )
    .await;
    let report = pass
        .reports
        .iter()
        .find(|report| report.key == cache_key())
        .expect("the cache was reported");
    let tail = broker
        .shard_log(
            felix_broker::LogKind::Counters,
            TENANT,
            NAMESPACE,
            STREAM,
            0,
        )
        .await
        .expect("log")
        .tail_offset()
        .await
        .expect("tail");
    let mark = marks
        .counters()
        .offset(&watch_key(&cache_key()), generation)
        .unwrap_or(0);
    (mark, tail, report.joint_start_acked)
}

/// A cache whose counter updates were acknowledged at generation 4 on the
/// leader and `b`, a majority of {a, b, c}, and whose leader has begun
/// generation 5 in both logs, each with one record past its start.
async fn cache_cut_over(marks: &QuorumMarks) -> (Arc<Broker>, tempfile::TempDir) {
    let (broker, dir) = super::quorum::quorum_cache_leader(4).await;
    let (mark, tail, _) = cache_pass(
        CacheAnswers {
            cache: &["broker-b"],
            counters: &["broker-b"],
        },
        &broker,
        &cache_route(&["broker-b", "broker-c"], 4, None),
        marks,
        4,
    )
    .await;
    assert_eq!(mark, tail, "the counter updates were acknowledged");
    for kind in [
        felix_broker::LogKind::Cache,
        felix_broker::LogKind::Counters,
    ] {
        let log = broker
            .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        let tail = log.tail_offset().await.expect("tail");
        log.record_generation(5, tail).expect("generation");
        log.append(&[Bytes::from_static(b"first")])
            .await
            .expect("append");
    }
    (broker, dir)
}

/// **The flip waits for the counter log on both majorities.** The set moves
/// from {a, b, c} to {a, c, d} with the counter updates on `a` and `b`. The
/// cache log's start record is on both majorities, but the counters reached
/// neither `c` nor `d`. Reporting the start then would let the control plane
/// take {a, c, d}, where only `a` holds the updates.
#[tokio::test]
async fn a_flip_waits_for_the_counter_log_on_both_majorities() {
    for by_followers in [false, true] {
        let marks = marks_for_caches(true, by_followers);
        let (broker, _dir) = cache_cut_over(&marks).await;
        let router = cache_route(&["broker-c", "broker-d"], 5, Some(OLD));

        let (mark, _, started) = cache_pass(
            CacheAnswers {
                cache: &["broker-c", "broker-d"],
                counters: &[],
            },
            &broker,
            &router,
            &marks,
            5,
        )
        .await;

        assert_eq!(
            started, None,
            "the start was reported with the counter log on no follower of the new set \
             (by_followers: {by_followers})"
        );
        assert_eq!(mark, 0, "by_followers: {by_followers}");
    }
}

/// **Counter updates wait for both majorities while a change is pending**, as
/// the cache log's records do. `d` and the leader are a majority of {a, c, d}
/// only.
#[tokio::test]
async fn a_pending_change_counts_counters_on_both_majorities() {
    for by_followers in [false, true] {
        let marks = marks_for_caches(true, by_followers);
        let (broker, _dir) = cache_cut_over(&marks).await;
        let router = cache_route(&["broker-c", "broker-d"], 5, Some(OLD));

        let (mark, tail, started) = cache_pass(
            CacheAnswers {
                cache: &["broker-c", "broker-d"],
                counters: &["broker-d"],
            },
            &broker,
            &router,
            &marks,
            5,
        )
        .await;

        assert_eq!(
            mark, 0,
            "a counter update was acknowledged on the new set alone ({tail} in the log, by_followers: {by_followers})"
        );
        assert_eq!(started, None, "by_followers: {by_followers}");
    }
}

/// **With the counters on both majorities the start is reported.** `c` is in
/// both sets and holds both logs.
#[tokio::test]
async fn a_flip_follows_the_counter_log_on_both_majorities() {
    for by_followers in [false, true] {
        let marks = marks_for_caches(true, by_followers);
        let (broker, _dir) = cache_cut_over(&marks).await;
        let router = cache_route(&["broker-c", "broker-d"], 5, Some(OLD));

        let (mark, tail, started) = cache_pass(
            CacheAnswers {
                cache: &["broker-c"],
                counters: &["broker-c"],
            },
            &broker,
            &router,
            &marks,
            5,
        )
        .await;

        assert_eq!(mark, tail, "by_followers: {by_followers}");
        assert_eq!(started, Some(5), "by_followers: {by_followers}");
    }
}

/// **A pending change a leader cannot count acknowledges no counter update.**
/// Without the joint-fence feature the new set's majority is not enough for
/// the counters either.
#[tokio::test]
async fn without_joint_fence_a_pending_change_acknowledges_no_counter_update() {
    for by_followers in [false, true] {
        let marks = marks_for_caches(false, by_followers);
        let (broker, _dir) = cache_cut_over(&marks).await;
        let router = cache_route(&["broker-c", "broker-d"], 5, Some(OLD));

        let (mark, _, _) = cache_pass(
            CacheAnswers {
                cache: &["broker-c", "broker-d"],
                counters: &["broker-c", "broker-d"],
            },
            &broker,
            &router,
            &marks,
            5,
        )
        .await;

        assert_eq!(mark, 0, "by_followers: {by_followers}");
    }
}

/// Answers every batch, but `lagging` nodes hold the log only up to `upto`.
struct Lagging {
    lagging: &'static [&'static str],
    upto: u64,
}

impl PeerRequester for Lagging {
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
        let end = batch.first_offset + batch.payloads.len() as u64;
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: if self.lagging.contains(&node_id) {
                end.min(self.upto)
            } else {
                end
            },
            learner: false,
        }))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(PeerCapabilities::ELECTION)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// **The joint start counts from the current stint's start record (#1787).**
/// The leader was elected at generation 4 and wrote its start record at 0,
/// then a write at 3 that its earlier passes acknowledged on {a, b} of the
/// old set. A cut-over named it at 4 again with the set {a, c, d}, and its
/// new stint wrote a start record at 4. `c` and `d` joined holding the log
/// up to, not including, the write at 3. `b` is outside the new set, so this leader no
/// longer ships to it and counts nothing for it here. The first start is on
/// both majorities and the write is not on the new one, so no joint start
/// may be reported: the flip would leave the write on one copy of the new
/// set.
#[tokio::test]
async fn the_joint_start_waits_for_the_current_stints_start() {
    let router = route(&["broker-c", "broker-d"], Some(OLD));
    for (lagging, expected) in [(&["broker-c", "broker-d"][..], None), (&[][..], Some(5))] {
        let (broker, _dir) = quorum_leader(0, 4).await;
        let log = broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        assert_eq!(log.append_generation_start(4).await.expect("start"), 0);
        for record in ["r1", "r2", "w"] {
            log.append(&[Bytes::from(record)]).await.expect("append");
        }
        assert_eq!(log.append_generation_start(4).await.expect("start"), 4);

        let pass = replicate_once(
            &Lagging { lagging, upto: 3 },
            &broker,
            &router,
            &marks(true),
            None,
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
        )
        .await;

        let report = pass.reports.first().expect("a report");
        assert_eq!(report.joint_start_acked, expected, "lagging: {lagging:?}");
    }
}

/// Stores every cache batch whole, and a counter batch only up to `upto`.
struct CountersLagging {
    upto: u64,
}

impl PeerRequester for CountersLagging {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (batch, upto) = match message {
            InternalMessage::ReplicateCacheRecords(batch) => (batch, u64::MAX),
            InternalMessage::ReplicateCounterRecords(batch) => (batch, self.upto),
            other => panic!("the driver sent {:?} for a cache shard", other.kind()),
        };
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: (batch.first_offset + batch.payloads.len() as u64).min(upto),
            learner: false,
        }))
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(PeerCapabilities::ELECTION)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// **The counter log's joint start counts from the current stint's start
/// record**, as the shard's own log does. The counter log's history begins
/// generation 5 at 0; past its three records the leader wrote a start record
/// at 3, an update at 4, and a new stint's start record at 5. `c` and `d`
/// hold the counter log up to the stint's start, not including it. Counted
/// from the history's start, the joint start would be reported on a new set
/// that may lack updates an earlier stint acknowledged on the old set alone.
#[tokio::test]
async fn the_counter_logs_joint_start_waits_for_the_current_stints_start() {
    for by_followers in [false, true] {
        for (upto, expected) in [(5, None), (u64::MAX, Some(5))] {
            let marks = marks_for_caches(true, by_followers);
            let (broker, _dir) = super::quorum::quorum_cache_leader(5).await;
            for kind in [
                felix_broker::LogKind::Cache,
                felix_broker::LogKind::Counters,
            ] {
                let log = broker
                    .shard_log(kind, TENANT, NAMESPACE, STREAM, 0)
                    .await
                    .expect("log");
                let tail = log.tail_offset().await.expect("tail");
                assert_eq!(log.append_generation_start(5).await.expect("start"), tail);
                if kind == felix_broker::LogKind::Counters {
                    log.append(&[Bytes::from_static(b"update")])
                        .await
                        .expect("append");
                    assert_eq!(
                        log.append_generation_start(5).await.expect("start"),
                        tail + 2
                    );
                }
            }
            let router = cache_route(&["broker-c", "broker-d"], 5, Some(OLD));

            let pass = replicate_once(
                &CountersLagging { upto },
                &broker,
                &router,
                &marks,
                None,
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
            )
            .await;

            let report = pass
                .reports
                .iter()
                .find(|report| report.key == cache_key())
                .expect("the cache was reported");
            assert_eq!(
                report.joint_start_acked, expected,
                "upto: {upto}, by_followers: {by_followers}"
            );
        }
    }
}

/// **A counter log that does not open holds the flip back.** The cache's
/// counter store refuses the shard's counter log this pass, so its start
/// cannot be counted; reading that as a cache without counters would report
/// the joint start on the cache log alone.
#[tokio::test]
async fn a_counter_log_that_does_not_open_reports_no_joint_start() {
    let marks = marks_for_caches(true, false);
    let (broker, _dir) = super::quorum::quorum_cache_leader(5).await;
    let log = broker
        .shard_log(felix_broker::LogKind::Cache, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.append_generation_start(5).await.expect("start");
    // Both logs then open only at incarnation 0, and the counter store has
    // moved on to another.
    broker.require_registered_caches();
    broker.set_cache_incarnation(TENANT, NAMESPACE, STREAM, Some(0));
    broker
        .counters()
        .expect("counters")
        .set_incarnation(TENANT, NAMESPACE, STREAM, Some(7));
    let router = cache_route(&["broker-c", "broker-d"], 5, Some(OLD));

    let (_, _, started) = cache_pass(
        CacheAnswers {
            cache: &["broker-c", "broker-d"],
            counters: &["broker-c", "broker-d"],
        },
        &broker,
        &router,
        &marks,
        5,
    )
    .await;

    assert_eq!(started, None);
}
