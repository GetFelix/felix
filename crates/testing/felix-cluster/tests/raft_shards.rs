//! Durable stream shards on Raft, across real broker processes.
//!
//! Every broker here runs with `FELIX_SHARD_ENGINE=raft`, which needs a
//! broker built with the `raft` feature. The harness runs whatever
//! `felix-broker` sits beside the test binary, so these are opt-in: without
//! `FELIX_TEST_RAFT_SHARDS` set they pass without starting anything.
//!
//! ```text
//! cargo build -p felix-broker-service --bin felix-broker --features raft
//! FELIX_TEST_RAFT_SHARDS=1 cargo test -p felix-cluster --test raft_shards
//! ```
use std::collections::BTreeMap;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use felix_wire::{AckMode, ErrorCode, RetryClass};
use serial_test::serial;

#[path = "raft_shards/moves.rs"]
mod moves;
#[path = "raft_shards/reform.rs"]
mod reform;

const STREAM: &str = "orders";
const OPT_IN: &str = "FELIX_TEST_RAFT_SHARDS";
const ELECTED_WITHIN: Duration = Duration::from_secs(60);

/// Whether this run asked for the Raft tests. Says so when it did not, so a
/// pass in the default suite does not read as coverage.
fn opted_in() -> bool {
    let asked = std::env::var_os(OPT_IN).is_some();
    if !asked {
        eprintln!("skipped: set {OPT_IN}=1 and build felix-broker with --features raft");
    }
    asked
}

fn engine(name: &str) -> (String, String) {
    ("FELIX_SHARD_ENGINE".to_string(), name.to_string())
}

/// Three brokers, one shard, each broker a member of its group.
fn raft_cluster() -> ClusterConfig {
    raft_cluster_with(StreamSpec::quorum(STREAM, 1, 3))
}

fn raft_cluster_with(stream: StreamSpec) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![stream],
        broker_env: vec![engine("raft")],
        ..Default::default()
    }
}

/// How many times `held` has `payload`.
fn copies(held: &[Vec<u8>], payload: &[u8]) -> usize {
    held.iter().filter(|have| *have == payload).count()
}

/// What a failed publish says about its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failed {
    /// Refused before it was proposed, so the same bytes may go again.
    Refused,
    /// It may have been written. The same bytes sent again could be stored
    /// twice, and the test would count its own re-send as a duplicate.
    InDoubt,
}

/// Only `retry`, `retry_after` and `redirect` promise nothing was applied,
/// plus `feature_not_enabled` and record headers the client never sent.
/// `fatal` in general does not, and an error with no code can be a
/// connection lost after the publish went out.
fn failed(err: &anyhow::Error) -> Failed {
    if err.is::<felix_client::RecordHeadersError>() {
        return Failed::Refused;
    }
    let Some(broker) = err.downcast_ref::<felix_client::BrokerError>() else {
        return Failed::InDoubt;
    };
    match broker.retry {
        RetryClass::Retry | RetryClass::RetryAfter | RetryClass::Redirect => Failed::Refused,
        RetryClass::Fatal if broker.code == ErrorCode::FeatureNotEnabled => Failed::Refused,
        RetryClass::OutcomeUnknown | RetryClass::Fatal => Failed::InDoubt,
    }
}

/// Publish through `node` until the record is acknowledged, sending it
/// again only after a refusal that wrote nothing. Panics on an answer that
/// leaves the record in doubt. `publish` answers `None` when it could not
/// connect or open a publisher, so sent nothing.
async fn publish_once_acked<F, Fut>(node: &str, what: &str, publish: F) -> u64
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Option<anyhow::Result<Option<u64>>>>,
{
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a publish to be acknowledged", || async {
        match publish().await? {
            Ok(offset) => Some(offset.expect("a durable stream acknowledges with an offset")),
            Err(err) if failed(&err) == Failed::Refused => None,
            Err(err) => panic!(
                "publish {what} through {node} may have been written ({err:#}); \
                 sending it again could store it twice"
            ),
        }
    })
    .await
    .unwrap_or_else(|err| panic!("publish {what} through {node}: {err:#}"))
}

/// The broker that leads the shard: the one that reports serving its Raft
/// term. A follower accepts subscribes too, so a subscribe cannot tell them
/// apart. Waits until exactly one broker in `among` says so, since a deposed
/// leader's gauge trails by one feed pass, and until it still says so a
/// little later: the broker that forms a group leads it only until it hands
/// the first leadership to placement's leader. Only brokers in `among` are
/// asked.
async fn leader_among(cluster: &Cluster, among: &[String]) -> String {
    loop {
        let first = one_leader(cluster, among).await;
        tokio::time::sleep(LEADER_SETTLES).await;
        if one_leader(cluster, among).await == first {
            return first;
        }
    }
}

/// How long a leader must keep leading before [`leader_among`] names it.
const LEADER_SETTLES: Duration = Duration::from_secs(2);

async fn one_leader(cluster: &Cluster, among: &[String]) -> String {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a broker to lead the shard", || async {
        let mut leading = leading_now(cluster, among).await;
        (leading.len() == 1).then(|| leading.remove(0))
    })
    .await
    .expect("a leader")
}

/// The brokers in `among` that say they lead a group now.
async fn leading_now(cluster: &Cluster, among: &[String]) -> Vec<String> {
    let mut leading = Vec::new();
    for node in among {
        let led = cluster
            .metric(node, "felix_broker_raft_shards_led")
            .await
            .ok()
            .flatten();
        if led.is_some_and(|led| led >= 1.0) {
            leading.push(node.clone());
        }
    }
    leading
}

/// [`leader_among`] that gives up after `within`, for a group that may not
/// be able to elect: `None` when no one broker led it for that long.
async fn leader_within(cluster: &Cluster, among: &[String], within: Duration) -> Option<String> {
    let deadline = tokio::time::Instant::now() + felix_cluster::wait::budget(within);
    while tokio::time::Instant::now() < deadline {
        let first = leading_now(cluster, among).await;
        if first.len() == 1 {
            tokio::time::sleep(LEADER_SETTLES).await;
            if leading_now(cluster, among).await == first {
                return first.into_iter().next();
            }
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    None
}

/// Publish through `node` until the broker acknowledges, and return the
/// offset. A shard between leaders answers "not ready", which a client
/// retries.
///
/// Only for a publish no fault can meet: before the test kills, cuts off,
/// moves or restarts anything. `Cluster::start` waits out every first-term
/// hand-over and placement steps only when a test asks, so an unknown
/// outcome there is a defect and panics. Use [`publish_tried`] otherwise.
async fn publish_acked(cluster: &Cluster, node: &str, payload: &[u8]) -> u64 {
    let what = format!("{:?}", String::from_utf8_lossy(payload));
    publish_once_acked(node, &what, || async {
        let client = cluster.client_on(node).await.ok()?;
        let publisher = client.publisher().await.ok()?;
        let sent = publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                payload.to_vec(),
                AckMode::PerMessage,
            )
            .await;
        Some(sent)
    })
    .await
}

/// [`publish_acked`] for a publish made while a fault can be in progress (a
/// kill, a partition, a move), where an unknown outcome is expected. Each
/// try sends `{base}/{try}`, so a try that may have been written is never
/// sent again: it goes into `sent`, to be checked as held at most once, as
/// a refused try is checked as never held. Returns the acknowledged try's
/// offset and payload, which `sent` also records.
async fn publish_tried(
    cluster: &Cluster,
    node: &str,
    base: &str,
    sent: &mut Sent,
) -> (u64, Vec<u8>) {
    let tries = std::cell::Cell::new(0u32);
    let failures = std::cell::RefCell::new(Vec::new());
    let acked =
        felix_cluster::wait::until_some(ELECTED_WITHIN, "a publish to be acknowledged", || {
            let payload = format!("{base}/{}", tries.get()).into_bytes();
            let (tries, failures) = (&tries, &failures);
            async move {
                // No connection or publisher: nothing was sent, and the try's
                // payload goes unused.
                let client = cluster.client_on(node).await.ok()?;
                let publisher = client.publisher().await.ok()?;
                tries.set(tries.get() + 1);
                let published = publisher
                    .publish(
                        &cluster.tenant_id,
                        &cluster.namespace,
                        STREAM,
                        payload.clone(),
                        AckMode::PerMessage,
                    )
                    .await;
                match published {
                    Ok(offset) => Some((
                        offset.expect("a durable stream acknowledges with an offset"),
                        payload,
                    )),
                    Err(err) => {
                        failures.borrow_mut().push((payload, err));
                        None
                    }
                }
            }
        })
        .await;
    let failures = failures.into_inner();
    let acked = acked.unwrap_or_else(|err| {
        let last = failures.last().map(|(_, err)| format!("{err:#}"));
        panic!("publish {base} through {node}: {err:#}; last error: {last:?}")
    });
    for (payload, err) in failures {
        sent.failed(payload, &err);
    }
    sent.acked.push(acked.clone());
    acked
}

/// Finalizes `raft_follower_read`, without which a follower sends a read
/// that could start below a trim floor to the leader.
async fn allow_follower_reads(cluster: &Cluster) {
    cluster
        .finalize_fleet_features(&["raft_follower_read"], Duration::from_secs(30))
        .await
        .expect("finalize raft_follower_read");
}

/// Everything `node` replays from the start of the stream, read until every
/// payload in `owed` has arrived.
///
/// A leadership transfer closes the shard on the member, which ends the
/// subscription without ending the stream, so a replay that ends early
/// resubscribes on the same node from the next offset.
async fn replay_holding(cluster: &Cluster, node: &str, owed: &[Vec<u8>]) -> Vec<Vec<u8>> {
    replay_at(cluster, node, owed)
        .await
        .into_iter()
        .map(|(_, payload)| payload)
        .collect()
}

/// [`replay_holding`] with each record's offset, `None` from a broker that
/// sends none.
async fn replay_at(cluster: &Cluster, node: &str, owed: &[Vec<u8>]) -> Vec<(Option<u64>, Vec<u8>)> {
    let (client, mut subscription) = cluster.replay_on(node, STREAM).await.expect("replay");
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    let mut held: Vec<(Option<u64>, Vec<u8>)> = Vec::new();
    let mut next_offset = 0u64;
    let mut ended = Vec::new();
    // A set, as the moving tests owe thousands of records.
    let mut missing: std::collections::HashSet<&[u8]> = owed.iter().map(Vec::as_slice).collect();
    while !missing.is_empty() {
        let step = tokio::time::timeout(Duration::from_secs(10), subscription.next_event()).await;
        match step {
            Ok(Ok(Some(event))) => {
                if let Some(offset) = event.offset {
                    next_offset = offset + 1;
                }
                missing.remove(event.payload.as_ref());
                held.push((event.offset, event.payload.to_vec()));
                continue;
            }
            Ok(Ok(None)) => ended.push("ended".to_string()),
            Ok(Err(err)) => ended.push(format!("error: {err:#}")),
            Err(_) => ended.push("no event for 10 s".to_string()),
        }
        if tokio::time::Instant::now() < deadline {
            if let Ok(resumed) = client
                .subscribe_from(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    Some(felix_client::StartPosition::Offset(next_offset)),
                )
                .await
            {
                subscription = resumed;
            } else {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            continue;
        }
        let text = |payload: &[u8]| String::from_utf8_lossy(payload).into_owned();
        let missing: Vec<String> = missing.iter().map(|payload| text(payload)).collect();
        panic!(
            "{node} gave up after {ended:?}\n delivered: {:?}\n owed: {:?}\n missing: {missing:?}",
            held.iter()
                .map(|(_, payload)| text(payload))
                .collect::<Vec<_>>(),
            owed.iter().map(|payload| text(payload)).collect::<Vec<_>>(),
        );
    }
    held
}

/// **A record published through any broker reaches a subscriber, whether
/// or not its publisher asked for an answer.** The subscriber connects to a
/// follower, which serves it from its own log.
#[tokio::test]
#[serial]
async fn a_publish_through_any_broker_reaches_a_subscriber() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    allow_follower_reads(&cluster).await;
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("follower");

    let reader = std::sync::Arc::new(
        felix_cluster::client::connect_cluster(
            &[cluster.node(follower).expect("node").client_addr],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe through a follower");

    let mut owed = Vec::new();
    let mut acked = Vec::new();
    for node in &nodes {
        let payload = format!("through-{node}").into_bytes();
        acked.push(publish_acked(&cluster, node, &payload).await);
        owed.push(payload);
    }
    assert!(
        acked.windows(2).all(|pair| pair[0] < pair[1]),
        "acknowledged offsets are not in publish order: {acked:?}"
    );

    // A batch nobody waits on is committed and delivered all the same.
    let writer = felix_cluster::client::connect(
        cluster.node(&leader).expect("node").client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("writer");
    let unacked: Vec<Vec<u8>> = (0..16)
        .map(|i| format!("unacked-{i}").into_bytes())
        .collect();
    writer
        .publisher()
        .await
        .expect("publisher")
        .publish_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            unacked.clone(),
            felix_wire::AckMode::None,
        )
        .await
        .expect("an unacknowledged batch");
    owed.extend(unacked);

    let mut seen = Vec::new();
    let mut offsets = Vec::new();
    while !owed.iter().all(|payload| seen.contains(payload)) {
        let event = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s")
            .expect("delivery")
            .expect("the subscription stays open");
        offsets.push(event.offset.expect("a durable stream carries offsets"));
        seen.push(event.payload.to_vec());
    }
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "delivered out of order: {offsets:?}"
    );
}

/// A key and one header naming `trace`.
/// **A shard that started on Raft gets its target from its brokers' copy
/// ids, and its group forms only on that target.** Each broker reports the
/// copy its member votes under once the copy is synced, and the control
/// plane writes the target only when every broker in the set has. No broker
/// leads the group before the target is stored, and the group serves under
/// it.
#[tokio::test]
#[serial]
async fn a_shard_started_on_raft_forms_its_group_on_the_target_from_its_copies() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let mut nodes = cluster.node_ids();
    nodes.sort();
    let target =
        felix_cluster::wait::until_some(ELECTED_WITHIN, "the shard's Raft target", || async {
            // Leaders read before the target: a leader seen while the target
            // still reads as absent led a group formed without one.
            let mut leading = Vec::new();
            for node in &nodes {
                if metric(&cluster, node, "felix_broker_raft_shards_led").await >= 1.0 {
                    leading.push(node.clone());
                }
            }
            let target = cluster.raft_target(STREAM, 0).await.ok().flatten();
            assert!(
                target.is_some() || leading.is_empty(),
                "{leading:?} led the shard's group before the control plane stored a target"
            );
            target
        })
        .await
        .expect("a target");
    assert_eq!(target.keys().cloned().collect::<Vec<_>>(), nodes);
    assert!(target.values().all(|copy| *copy != 0), "{target:?}");
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"under the target").await;
    cluster.shutdown().await;
}

/// **A plain publish made while a fresh group hands over its first term
/// lands, once.** The broker that forms each group leads term 1 and then
/// hands it to the placed leader, closing the shard for the election. A
/// publish refused `not_ready` in that window was never proposed, so the
/// client sends it again, and the shard holds each record once. The client's
/// re-send count shows a refusal was met and retried: without it this test
/// passes on a client that never retries. A publish already proposed when
/// the hand-over began can come back outcome unknown instead; that one is
/// returned, never re-sent, and held at most once.
///
/// The window is made, not waited for. The stream is created after the
/// cluster is up, so its hand-overs are all still ahead, and every shard is
/// published to without a pause from the moment its leader serves until the
/// last hand-over is done. A stream whose hand-overs pass without a refusal
/// is replaced by a fresh one, until the time budget runs out.
#[tokio::test]
#[serial]
async fn a_plain_publish_into_a_first_term_hand_over_lands_once() {
    if !opted_in() {
        return;
    }
    const SHARDS: u32 = 12;
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        broker_env: vec![engine("raft")],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let addrs = cluster.broker_addrs();
    // A hand-over that fails once is asked again about 30 s later, so the
    // budget is time, not a handful of attempts.
    let policy = felix_client::ReconnectPolicy {
        attempts: 600,
        backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(500),
        deadline: Some(ELECTED_WITHIN),
    };
    let client = std::sync::Arc::new(
        felix_cluster::client::connect_cluster_with_policy(
            &addrs,
            &cluster.tenant_id,
            &cluster.client_token(),
            policy,
        )
        .await
        .expect("connect a cluster client"),
    );

    let budget = tokio::time::Instant::now() + HAND_OVER_BUDGET;
    let mut attempt = 0;
    // The stream whose hand-overs a publish met, and what was published.
    let (name, sent) = loop {
        attempt += 1;
        assert!(
            tokio::time::Instant::now() < budget,
            "no publish met a hand-over's refusal in {attempt} streams over {HAND_OVER_BUDGET:?}, \
             so nothing here proves the client re-sends one",
        );
        let spec = StreamSpec::quorum(format!("{STREAM}-{attempt}"), SHARDS, 3);
        cluster
            .create_stream(&spec)
            .await
            .expect("create the stream");
        if let Some(sent) = publish_across_hand_overs(&cluster, &client, &spec).await {
            break (spec.name, sent);
        }
    };

    // An offset here is a log index, and a new leader's first entry takes
    // one, so gaps between acknowledged offsets say nothing. A re-send that
    // wrote twice shows as a payload the shard holds twice. The attempt that
    // was acknowledged is the last one written, so a read that has reached
    // every acknowledged offset has passed any earlier copy.
    let reader = cluster.node_ids().remove(0);
    for (shard, sent) in sent.iter().enumerate() {
        let held = felix_cluster::wait::until_some(
            ELECTED_WITHIN,
            "a replay to reach every acknowledged offset",
            || async {
                // The placed leader once the hand-overs are done, which
                // holds everything acknowledged.
                let from = cluster
                    .shard_owner_of("stream", &name, shard as u32)
                    .await
                    .unwrap_or_else(|_| reader.clone());
                let (_client, mut replay) = cluster
                    .replay_shard(&from, &name, shard as u32)
                    .await
                    .ok()?;
                let mut held: Vec<(u64, Vec<u8>)> = Vec::new();
                while let Ok(Ok(Some(event))) =
                    tokio::time::timeout(Duration::from_secs(3), replay.next_event()).await
                {
                    held.push((event.offset?, event.payload.to_vec()));
                }
                sent.acked
                    .iter()
                    .all(|at| held.contains(at))
                    .then_some(held)
            },
        )
        .await
        .unwrap_or_else(|err| panic!("replay shard {shard}: {err:#}"));
        let payloads: Vec<Vec<u8>> = held.iter().map(|(_, payload)| payload.clone()).collect();
        if let Err(wrong) = sent.check(&format!("shard {shard}"), &payloads) {
            let offsets: Vec<u64> = held.iter().map(|(at, _)| *at).collect();
            panic!("{wrong}, held: {offsets:?}");
        }
    }
}

/// What was published to one shard, by what became of it.
#[derive(Debug, Clone, Default)]
struct Sent {
    /// Acknowledged, with the offset: held exactly once.
    acked: Vec<(u64, Vec<u8>)>,
    /// Answered with no offset and no promise that nothing was written:
    /// held at most once, as it is never sent again.
    in_doubt: Vec<Vec<u8>>,
    /// Refused as not proposed, with the error: never held. One held
    /// would be a refusal that wrote, and a client that trusted it and
    /// re-sent would store the record twice.
    refused: Vec<(Vec<u8>, String)>,
}

impl Sent {
    /// The publish ended in `err`, which `failed` sorts.
    fn failed(&mut self, payload: Vec<u8>, err: &anyhow::Error) {
        match failed(err) {
            Failed::Refused => self.refused.push((payload, format!("{err:#}"))),
            Failed::InDoubt => self.in_doubt.push(payload),
        }
    }

    /// Whether `held`, the payloads `place` holds, has what was sent as it
    /// must.
    fn check(&self, place: &str, held: &[Vec<u8>]) -> Result<(), String> {
        let copies = |payload: &[u8]| held.iter().filter(|have| *have == payload).count();
        let text = |payload: &[u8]| String::from_utf8_lossy(payload).into_owned();
        for (offset, payload) in &self.acked {
            let copies = copies(payload);
            if copies != 1 {
                return Err(format!(
                    "{place} holds {:?} {copies} times; acknowledged at {offset}",
                    text(payload),
                ));
            }
        }
        for payload in &self.in_doubt {
            let copies = copies(payload);
            if copies > 1 {
                return Err(format!(
                    "{place} holds {:?}, answered outcome unknown and never re-sent, \
                     {copies} times",
                    text(payload),
                ));
            }
        }
        for (payload, err) in &self.refused {
            let copies = copies(payload);
            if copies > 0 {
                return Err(format!(
                    "{place} holds {:?} {copies} times, though it was refused as not \
                     proposed: {err}",
                    text(payload),
                ));
            }
        }
        Ok(())
    }
}

/// The hand-over test's check has teeth without a cluster: a refusal that
/// promises nothing was written fails it if the payload is held, and only
/// those refusals let a payload go again.
#[test]
fn a_refused_publish_that_is_held_fails_the_hand_over_check() {
    let refusal = |code, retry| {
        anyhow::Error::new(felix_client::BrokerError::new(code, retry, "refused"))
            .context("gave up")
    };
    let not_ready = refusal(ErrorCode::ShardUnavailable, RetryClass::Retry);
    assert_eq!(failed(&not_ready), Failed::Refused);
    assert_eq!(
        failed(&refusal(ErrorCode::NotLeader, RetryClass::Redirect)),
        Failed::Refused
    );
    assert_eq!(
        failed(&refusal(ErrorCode::Overloaded, RetryClass::RetryAfter)),
        Failed::Refused
    );
    for in_doubt in [
        refusal(ErrorCode::LeadershipLost, RetryClass::OutcomeUnknown),
        refusal(ErrorCode::Unacknowledged, RetryClass::OutcomeUnknown),
        refusal(ErrorCode::Internal, RetryClass::Fatal),
        anyhow::anyhow!("connection lost"),
    ] {
        assert_eq!(failed(&in_doubt), Failed::InDoubt, "{in_doubt:#}");
    }

    let mut sent = Sent {
        acked: vec![(5, b"11/0/2".to_vec())],
        ..Sent::default()
    };
    sent.failed(
        b"11/0/0".to_vec(),
        &refusal(ErrorCode::LeadershipLost, RetryClass::OutcomeUnknown),
    );
    sent.failed(b"11/0/1".to_vec(), &not_ready);
    let at = |_offset: u64, payload: &[u8]| payload.to_vec();

    // The record as the failed run held it: the try in doubt committed under
    // the next leader, and the acknowledged one is a payload of its own.
    let held = [at(3, b"11/0/0"), at(5, b"11/0/2")];
    assert_eq!(sent.check("shard 11", &held), Ok(()));

    let refused_held = [at(3, b"11/0/1"), at(5, b"11/0/2")];
    let wrong = sent
        .check("shard 11", &refused_held)
        .expect_err("a refusal that wrote");
    assert!(wrong.contains("refused as not proposed"), "{wrong}");
    assert!(wrong.contains("gave up"), "names the error: {wrong}");

    let doubt_twice = [at(3, b"11/0/0"), at(5, b"11/0/2"), at(6, b"11/0/0")];
    assert!(sent.check("shard 11", &doubt_twice).is_err());
    let acked_twice = [at(3, b"11/0/2"), at(5, b"11/0/2")];
    assert!(sent.check("shard 11", &acked_twice).is_err());
    assert!(
        sent.check("shard 11", &[at(3, b"11/0/0")]).is_err(),
        "acked missing"
    );
}

/// How long the hand-over test looks for a stream whose hand-overs a publish
/// meets. One stream's hand-overs take seconds, so this is many attempts.
const HAND_OVER_BUDGET: Duration = Duration::from_secs(240);

/// Publishes to every shard of a just-created stream, without a pause, from
/// when each shard's first leader serves until every shard is led by the
/// broker placement names. Returns what each shard was sent, when a publish
/// was refused and sent again in that time; `None` when none was.
async fn publish_across_hand_overs(
    cluster: &Cluster,
    client: &std::sync::Arc<felix_client::Client>,
    spec: &StreamSpec,
) -> Option<Vec<Sent>> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let shards = spec.shards as usize;
    let mut sent = vec![Sent::default(); shards];
    // Every shard serves before the count starts, so a refusal counted below
    // is a hand-over's and not a shard still opening. Placement is stepped
    // here too, and a shard's first hand-over can begin while a later one
    // is still opening, so this publish can meet one. Each try sends its own
    // payload, so one that may have been written is never sent again.
    for shard in 0..spec.shards {
        let tries = std::cell::Cell::new(0u32);
        let failures = std::cell::RefCell::new(Vec::new());
        let acked = felix_cluster::wait::until_some(ELECTED_WITHIN, "a shard to serve", || {
            let payload = format!("{shard}/0/{}", tries.get()).into_bytes();
            tries.set(tries.get() + 1);
            let failures = &failures;
            async move {
                cluster.place_shards().await;
                let published = client
                    .publish_on_shard(
                        &cluster.tenant_id,
                        &cluster.namespace,
                        &spec.name,
                        shard,
                        payload.clone(),
                        AckMode::PerMessage,
                    )
                    .await;
                match published {
                    Ok(offset) => Some((
                        offset.expect("a durable stream acknowledges with an offset"),
                        payload,
                    )),
                    Err(err) => {
                        failures.borrow_mut().push((payload, err));
                        None
                    }
                }
            }
        })
        .await;
        let failures = failures.into_inner();
        let acked = acked.unwrap_or_else(|err| {
            let last = failures.last().map(|(_, err)| format!("{err:#}"));
            panic!("shard {shard} never served: {err:#}; last error: {last:?}")
        });
        let shard = &mut sent[shard as usize];
        shard.acked.push(acked);
        for (payload, err) in failures {
            shard.failed(payload, &err);
        }
    }

    let resends_before = client.publish_resends();
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let publishers: Vec<_> = (0..spec.shards)
        .map(|shard| {
            let client = client.clone();
            let stop = stop.clone();
            let (tenant, namespace, stream) = (
                cluster.tenant_id.clone(),
                cluster.namespace.clone(),
                spec.name.clone(),
            );
            tokio::spawn(async move {
                let mut acked = Vec::new();
                let mut in_doubt = Vec::new();
                let mut seq = 0;
                while !stop.load(Ordering::Relaxed) {
                    seq += 1;
                    let payload = format!("{shard}/{seq}").into_bytes();
                    let sent = client
                        .publish_on_shard(
                            &tenant,
                            &namespace,
                            &stream,
                            shard,
                            payload.clone(),
                            AckMode::PerMessage,
                        )
                        .await;
                    match sent {
                        Ok(offset) => {
                            let offset =
                                offset.expect("a durable stream acknowledges with an offset");
                            acked.push((offset, payload));
                        }
                        // A publish proposed just before the hand-over closed
                        // the shard may commit under the next leader, which
                        // the old one cannot confirm: `leadership_lost`,
                        // outcome unknown. A plain publish must not send that
                        // again, so it is returned, and checked to be held at
                        // most once. Anything else fails the test: the client
                        // re-sends a refusal itself, within its deadline.
                        Err(err)
                            if err.downcast_ref::<felix_client::BrokerError>().is_some_and(
                                |broker| broker.retry == RetryClass::OutcomeUnknown,
                            ) =>
                        {
                            in_doubt.push(payload);
                        }
                        Err(err) => panic!("publish {seq} to shard {shard}: {err:#}"),
                    }
                    // Paced so the replay below stays small; a hand-over
                    // lasts far longer than this.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                (acked, in_doubt)
            })
        })
        .collect();

    // Placement is stepped here, as the harness never runs it on a timer.
    let until = tokio::time::Instant::now() + ELECTED_WITHIN * 2;
    let pending = loop {
        cluster.place_shards().await;
        let pending = cluster.raft_hand_over_pending_for(spec).await;
        if pending.is_none() || tokio::time::Instant::now() >= until {
            break pending;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    stop.store(true, Ordering::Relaxed);
    for (shard, publisher) in publishers.into_iter().enumerate() {
        let (acked, unknown) = publisher.await.expect("a publisher task");
        sent[shard].acked.extend(acked);
        sent[shard].in_doubt.extend(unknown);
    }
    assert!(
        pending.is_none(),
        "the hand-overs did not finish: {pending:?}"
    );
    (client.publish_resends() > resends_before).then_some(sent)
}

fn headers(trace: &str) -> felix_client::RecordHeaders {
    felix_client::RecordHeaders {
        key: Some(bytes::Bytes::from(format!("key-{trace}"))),
        headers: vec![("trace".to_string(), bytes::Bytes::from(trace.to_string()))],
    }
}

/// Publish one headered record through `node` until it is acknowledged. A
/// shard between leaders, or a broker the finalize has not reached yet,
/// refuses, and a client retries.
async fn publish_headered(cluster: &Cluster, node: &str, trace: &str) -> u64 {
    publish_once_acked(node, trace, || async {
        let client = cluster.client_on(node).await.ok()?;
        let publisher = client.publisher().await.ok()?;
        let sent = publisher
            .publish_with_headers(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                headers(trace),
                trace.as_bytes().to_vec(),
                AckMode::PerMessage,
            )
            .await;
        Some(sent)
    })
    .await
}

/// **A record's key and headers on a Raft shard reach every member.**
/// Published through the leader and through followers, which forward to it,
/// each record replays with its headers from every broker's own log.
#[tokio::test]
#[serial]
async fn record_headers_on_a_raft_shard_replay_from_every_member() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(
            &[
                "publisher_principal",
                "record_headers",
                "raft_record_headers",
                "raft_follower_read",
            ],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize the headers features");
    let nodes = cluster.node_ids();
    let mut published = Vec::new();
    for node in &nodes {
        let trace = format!("through-{node}");
        let offset = publish_headered(&cluster, node, &trace).await;
        published.push((offset, trace));
    }
    for node in &nodes {
        let addr = cluster.node(node).expect("node").client_addr;
        let client = felix_cluster::client::connect_with(
            addr,
            &cluster.tenant_id,
            &cluster.client_token(),
            |config| config.publishers = true,
        )
        .await
        .expect("connect");
        let mut subscription = client
            .subscribe_from(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .expect("replay");
        let mut held = Vec::new();
        while held.len() < published.len() {
            let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
                .await
                .unwrap_or_else(|_| panic!("{node} stopped at {} records", held.len()))
                .expect("delivery")
                .expect("the subscription stays open");
            let Some(offset) = event.offset else { continue };
            let Some((_, trace)) = published.iter().find(|(at, _)| *at == offset) else {
                continue;
            };
            assert_eq!(
                event.payload.as_ref(),
                trace.as_bytes(),
                "{node} at {offset}"
            );
            assert_eq!(event.headers, Some(headers(trace)), "{node} at {offset}");
            let publisher = event.publisher.as_deref().unwrap_or_default();
            assert!(
                publisher.contains("harness-client"),
                "{node} at {offset}: {publisher:?}"
            );
            held.push(offset);
        }
    }
}

/// Whether every acknowledged offset has reached the subscriber. A publish
/// retried after an unknown outcome can leave a record the test never saw
/// acknowledged, so the counts are not compared.
fn arrived(acked: &[(u64, Vec<u8>)], seen: &[(u64, Vec<u8>)]) -> bool {
    acked
        .iter()
        .all(|(offset, _)| seen.iter().any(|(got, _)| got == offset))
}

/// **A follower serves a subscriber, and the subscriber keeps every
/// acknowledged record across the leader's death.** The reader connects only
/// to a follower, reads from offset 0, and sees each record at the offset the
/// leader acknowledged. When the leader is killed and a survivor takes over,
/// the subscription resumes (or is resumed by offset) with no record missing
/// and none twice.
#[tokio::test]
#[serial]
async fn a_followers_subscriber_keeps_every_acknowledged_record_when_the_leader_dies() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    allow_follower_reads(&cluster).await;
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("follower")
        .clone();

    // One connection to the follower alone: it does not follow a redirect, so
    // a follower that sent the reader to the leader fails the subscribe.
    let reader = felix_cluster::client::connect(
        cluster.node(&follower).expect("node").client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("reader");
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe on a follower");

    let mut acked: Vec<(u64, Vec<u8>)> = Vec::new();
    for i in 0..10 {
        let payload = format!("before-{i}").into_bytes();
        acked.push((publish_acked(&cluster, &leader, &payload).await, payload));
    }
    let mut seen: Vec<(u64, Vec<u8>)> = Vec::new();
    while !arrived(&acked, &seen) {
        let event = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s")
            .expect("delivery")
            .expect("the subscription stays open while the leader lives");
        seen.push((event.offset.expect("offset"), event.payload.to_vec()));
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    leader_among(&cluster, &survivors).await;
    let mut sent = Sent::default();
    for i in 0..10 {
        let base = format!("after-{i}");
        acked.push(publish_tried(&cluster, &follower, &base, &mut sent).await);
    }

    while !arrived(&acked, &seen) {
        let next = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s");
        match next {
            Ok(Some(event)) => seen.push((event.offset.expect("offset"), event.payload.to_vec())),
            // The subscription ended with its member's term: resume by offset.
            Ok(None) | Err(_) => {
                let from = seen.last().map_or(0, |(offset, _)| offset + 1);
                subscription = felix_cluster::wait::until_some(
                    ELECTED_WITHIN,
                    "a resubscribe to be accepted",
                    || async {
                        reader
                            .subscribe_from(
                                &cluster.tenant_id,
                                &cluster.namespace,
                                STREAM,
                                Some(felix_client::StartPosition::Offset(from)),
                            )
                            .await
                            .ok()
                    },
                )
                .await
                .expect("resubscribe");
            }
        }
    }

    assert!(
        seen.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "a record repeated or arrived out of order: {:?}",
        seen.iter().map(|(offset, _)| offset).collect::<Vec<_>>()
    );
    for (offset, payload) in &acked {
        assert!(
            seen.iter()
                .any(|(got, body)| got == offset && body == payload),
            "acknowledged record {:?} at offset {offset} never arrived",
            String::from_utf8_lossy(payload)
        );
    }
    let seen: Vec<Vec<u8>> = seen.into_iter().map(|(_, payload)| payload).collect();
    if let Err(wrong) = sent.check("the follower's subscriber", &seen) {
        panic!("{wrong}");
    }
}

/// **The leader dies while the control plane is down, and nothing
/// acknowledged is lost.** The group elects among the survivors on its own:
/// no placement, no lease and no promotion from the control plane. Publishes
/// continue, and the new leader holds every acknowledged record.
#[tokio::test]
#[serial]
async fn a_leader_lost_with_the_control_plane_down_loses_nothing_acknowledged() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let mut owed = Vec::new();
    let mut last = None;
    for i in 0..20 {
        let payload = format!("before-{i}").into_bytes();
        last = Some(publish_acked(&cluster, &leader, &payload).await);
        owed.push(payload);
    }

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;

    // Through the survivor that does not lead, so the forward is covered too.
    let other = survivors
        .iter()
        .find(|node| **node != elected)
        .expect("the other survivor");
    let mut sent = Sent::default();
    for i in 0..20 {
        let (offset, payload) =
            publish_tried(&cluster, other, &format!("after-{i}"), &mut sent).await;
        assert!(
            Some(offset) > last,
            "{elected} acknowledged offset {offset} at or below {last:?}"
        );
        last = Some(offset);
        owed.push(payload);
    }

    let held = replay_holding(&cluster, &elected, &owed).await;
    for payload in &owed {
        assert_eq!(
            held.iter().filter(|have| *have == payload).count(),
            1,
            "{elected} holds {:?} other than once",
            String::from_utf8_lossy(payload)
        );
    }
    if let Err(wrong) = sent.check(&elected, &held) {
        panic!("{wrong}");
    }
}

/// **A `Leader` stream keeps acknowledging across a lost leader, and loses
/// at most the tail its old leader acknowledged alone.** A publisher keeps
/// publishing while the leader is cut off from its followers, so what the
/// leader answers on its lease from then on is on its log alone and goes
/// with it. What the next leader keeps of the acknowledged sequence is a
/// prefix of it, once and in order, and the next leader's acknowledgements
/// are all in its log.
#[tokio::test]
#[serial]
async fn a_leader_stream_lost_with_its_leader_keeps_a_prefix_of_what_was_acknowledged() {
    if !opted_in() {
        return;
    }
    let stream = StreamSpec::replicated(STREAM, 1, 3);
    let config = raft_cluster_with(stream);
    let mut cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"first").await;

    // One connection, publishing in order until a publish is not answered:
    // the first one left waiting for a commit that cannot come.
    let client = cluster.client_on(&leader).await.expect("client");
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let publishing = tokio::spawn(async move {
        let publisher = client.publisher().await.expect("publisher");
        let mut acked = vec![b"first".to_vec()];
        for i in 0..100_000 {
            let payload = format!("run-{i}").into_bytes();
            let publish = publisher.publish(
                &tenant,
                &namespace,
                STREAM,
                payload.clone(),
                AckMode::PerMessage,
            );
            match tokio::time::timeout(Duration::from_secs(1), publish).await {
                Ok(Ok(_)) => acked.push(payload),
                _ => break,
            }
        }
        acked
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.partition_node(&leader).expect("partition");
    let acked = tokio::time::timeout(Duration::from_secs(30), publishing)
        .await
        .expect("the publisher stopped being answered")
        .expect("publisher task");

    cluster.kill_node(&leader).expect("kill the leader");
    cluster.heal_partitions().expect("heal");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    let mut after = Vec::new();
    let mut sent = Sent::default();
    for i in 0..20 {
        let (_, payload) =
            publish_tried(&cluster, &elected, &format!("after-{i}"), &mut sent).await;
        after.push(payload);
    }

    let held = replay_holding(&cluster, &elected, &after).await;
    for payload in &after {
        assert_eq!(copies(&held, payload), 1, "{elected} acknowledged it");
    }
    if let Err(wrong) = sent.check(&elected, &held) {
        panic!("{wrong}");
    }
    let kept: Vec<&Vec<u8>> = held.iter().filter(|have| acked.contains(have)).collect();
    assert!(
        kept.len() < acked.len(),
        "{elected} kept all {} acknowledged records: nothing was answered on the lease alone",
        acked.len()
    );
    assert!(
        kept.first().is_some_and(|have| have.as_slice() == b"first"),
        "{elected} lost `first`, acknowledged before the partition with its followers reachable"
    );
    assert_eq!(
        kept,
        acked[..kept.len()].iter().collect::<Vec<_>>(),
        "what {elected} kept is not a prefix of what was acknowledged"
    );
}

/// **A leader of a `Leader` stream that its followers cannot reach
/// acknowledges nothing the group loses once its lease is over.** The
/// broker keeps running and still says it leads, while the others elect.
/// Whatever is acknowledged from then on, by either side, is in the log the
/// group agrees on after the partition heals. This late the group takes no
/// write from the old leader, so this is the whole path and not the lease
/// check alone, which felix-replication's `raft/tests/deliver.rs` covers.
#[tokio::test]
#[serial]
async fn a_cut_off_leader_acknowledges_nothing_the_group_loses_after_its_lease() {
    if !opted_in() {
        return;
    }
    let stream = StreamSpec::replicated(STREAM, 1, 3);
    let config = raft_cluster_with(stream);
    let cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let mut owed = vec![b"before".to_vec()];
    let mut unanswered = Vec::new();
    publish_acked(&cluster, &leader, &owed[0]).await;

    cluster.partition_node(&leader).expect("partition");
    // Longer than the lease (1.5 s) and the longest election timeout (2 s).
    tokio::time::sleep(Duration::from_secs(3)).await;
    for i in 0..3 {
        let payload = format!("cut-off-{i}").into_bytes();
        let publish = cluster.publish_via_at(&leader, STREAM, payload.clone());
        let answered = tokio::time::timeout(Duration::from_secs(3), publish).await;
        if matches!(answered, Ok(Ok(_))) {
            owed.push(payload);
        } else {
            unanswered.push(payload);
        }
    }
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    let mut sent = Sent::default();
    for i in 0..5 {
        let (_, payload) =
            publish_tried(&cluster, &elected, &format!("theirs-{i}"), &mut sent).await;
        owed.push(payload);
    }

    cluster.heal_partitions().expect("heal");
    let (_, healed) = publish_tried(&cluster, &leader, "healed", &mut sent).await;
    owed.push(healed);
    let held = replay_holding(&cluster, &elected, &owed).await;
    for payload in &owed {
        let text = String::from_utf8_lossy(payload);
        assert_eq!(copies(&held, payload), 1, "{elected} holds {text:?}");
    }
    if let Err(wrong) = sent.check(&elected, &held) {
        panic!("{wrong}");
    }
    // A cut-off publish the leader never answered may or may not survive,
    // and so may a try answered outcome unknown, but nothing else is in the
    // log.
    for payload in &held {
        let text = String::from_utf8_lossy(payload);
        assert!(
            owed.contains(payload)
                || unanswered.contains(payload)
                || sent.in_doubt.contains(payload)
                || payload == b"harness-probe",
            "{elected} holds {text:?}, which nobody acknowledged"
        );
    }
}

/// **A shard placed on fewer brokers than its stream asks for forms no
/// group and acknowledges nothing.** The stream asks for three brokers and
/// the cluster has one, so placement names that one alone. A group of one
/// would acknowledge on its own, and brokers placed later would form the
/// full set again on empty logs and elect each other.
#[tokio::test]
#[serial]
async fn a_short_placement_acknowledges_nothing() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: Vec::new(),
        broker_env: vec![engine("raft")],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let only = cluster.node_ids().remove(0);
    cluster
        .create_stream(&StreamSpec::quorum(STREAM, 1, 3))
        .await
        .expect("create the stream");
    felix_cluster::wait::until(ELECTED_WITHIN, "the shard to be placed", || async {
        cluster.place_shards().await;
        cluster
            .shard_owners_for(STREAM)
            .await
            .is_ok_and(|owners| owners.get(&0) == Some(&only))
    })
    .await
    .expect("placed on the one broker");

    let mut acked = 0;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        cluster.place_shards().await;
        let publish = cluster.publish_via_at(&only, STREAM, b"alone".to_vec());
        if let Ok(Ok(Some(_))) = tokio::time::timeout(Duration::from_secs(2), publish).await {
            acked += 1;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        acked, 0,
        "one broker of three acknowledged records on its own"
    );
    // The control plane stores no target for a short set, so the broker
    // forms no group to lead.
    assert_eq!(cluster.raft_target(STREAM, 0).await.expect("record"), None);
    assert_eq!(
        metric(&cluster, &only, "felix_broker_raft_shards_led").await,
        0.0,
        "a group formed with no stored target"
    );
}

/// **Until the fleet finalizes `raft_consumer_groups`, a group on a Raft
/// shard is refused by name** by the leader, which alone serves groups. The
/// refusal names the fleet feature so an operator knows what to finalize; a
/// follower redirects to the leader.
#[tokio::test]
#[serial]
async fn a_consumer_group_on_a_raft_shard_is_refused_until_the_fleet_finalizes_it() {
    if !opted_in() {
        return;
    }
    // A new cluster finalizes the feature itself.
    let cluster = Cluster::start(ClusterConfig {
        upgraded_fleet: Some(true),
        ..raft_cluster()
    })
    .await
    .expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"work").await;

    let err = cluster
        .group_poll_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect_err("a group was served before the fleet finalized the feature");
    assert!(
        format!("{err:#}").contains("raft_consumer_groups"),
        "the leader's refusal does not name the fleet feature: {err:#}"
    );
    for node in nodes.iter().filter(|node| **node != leader) {
        let err = cluster
            .group_poll_via(node, STREAM, 0, "workers", 10)
            .await
            .expect_err("a follower served a group");
        assert!(
            format!("{err:#}").contains(&format!("owned by {leader}")),
            "{node} does not redirect to the leader {leader}: {err:#}"
        );
    }
}

/// **A consumer group on a Raft shard works once the fleet finalizes
/// `raft_consumer_groups`.** Records are claimed and acknowledged through
/// the leader, an acknowledged record is not handed out again, and a
/// follower redirects to the leader.
#[tokio::test]
#[serial]
async fn a_consumer_group_on_a_raft_shard_serves_once_the_fleet_finalizes_it() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_consumer_groups"], Duration::from_secs(30))
        .await
        .expect("finalize the group feature");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    for payload in [b"one".as_slice(), b"two", b"three"] {
        publish_acked(&cluster, &leader, payload).await;
    }

    let first = cluster
        .group_poll_records_via(&leader, STREAM, 0, "workers", 1)
        .await
        .expect("the leader serves the group");
    assert_eq!(first.len(), 1, "one record asked for: {first:?}");
    cluster
        .group_ack_via(&leader, STREAM, 0, "workers", first[0].offset)
        .await
        .expect("ack through the leader");

    let rest = cluster
        .group_poll_records_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect("the leader serves the rest");
    assert!(
        rest.iter().all(|record| record.offset != first[0].offset),
        "an acknowledged record was handed out again: {rest:?}"
    );
    // The stream may hold records written while the cluster started, so check
    // that ours each came out once rather than counting the whole stream. A
    // record is its offset: the startup probe retries after an unanswered
    // publish, so the stream can hold its payload twice.
    let mut offsets: Vec<u64> = first
        .iter()
        .chain(rest.iter())
        .map(|record| record.offset)
        .collect();
    let handed_out = offsets.len();
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(
        offsets.len(),
        handed_out,
        "a record came out twice: {first:?} {rest:?}"
    );
    for payload in [b"one".as_slice(), b"two", b"three"] {
        let times = first
            .iter()
            .chain(rest.iter())
            .filter(|record| record.payload.as_ref() == payload)
            .count();
        assert_eq!(
            times,
            1,
            "{} was handed out {times} times: {first:?} {rest:?}",
            String::from_utf8_lossy(payload)
        );
    }
    for record in &rest {
        cluster
            .group_ack_via(&leader, STREAM, 0, "workers", record.offset)
            .await
            .expect("ack through the leader");
    }

    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("a follower");
    let err = cluster
        .group_poll_records_via(follower, STREAM, 0, "workers", 10)
        .await
        .expect_err("a follower served the group");
    assert!(
        format!("{err:#}").contains(&format!("owned by {leader}")),
        "{follower} does not redirect to the leader {leader}: {err:#}"
    );
    let drained = cluster
        .group_poll_records_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect("poll after everything is acknowledged");
    assert!(drained.is_empty(), "records left after acks: {drained:?}");
}

/// **A consumer's claims survive its Raft leader dying.** Claims are group
/// entries in the shard's log, so the member elected next hands a claimed
/// record to nobody else, the holder can still extend it, and the holder
/// reconnecting with `reclaim` takes it back at the same attempt.
#[tokio::test]
#[serial]
async fn a_group_claim_survives_its_raft_leader_dying() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_consumer_groups"], Duration::from_secs(30))
        .await
        .expect("finalize the group feature");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    for payload in [b"one".as_slice(), b"two", b"three"] {
        publish_acked(&cluster, &leader, payload).await;
    }
    // Drain whatever the stream held before ours, so the claims below are on
    // our records.
    let mut held = Vec::new();
    while held.len() < 2 {
        let records = cluster
            .group_poll_as_via(&leader, STREAM, 0, "workers", "w", false, 1)
            .await
            .expect("poll as a member");
        assert!(!records.is_empty(), "ran out of records: {held:?}");
        for record in records {
            if [b"one".as_slice(), b"two"].contains(&record.payload.as_ref()) {
                held.push(record);
            } else {
                cluster
                    .group_ack_via(&leader, STREAM, 0, "workers", record.offset)
                    .await
                    .expect("ack");
            }
        }
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;

    cluster
        .group_extend_via(
            &elected,
            STREAM,
            0,
            "workers",
            &held[0],
            Duration::from_secs(60),
        )
        .await
        .expect("the holder extends its claim on the new leader");
    let others = cluster
        .group_poll_as_via(&elected, STREAM, 0, "workers", "other", false, 10)
        .await
        .expect("another member polls the new leader");
    for record in &others {
        assert!(
            held.iter().all(|h| h.offset != record.offset),
            "{elected} handed a claimed record to another member: {record:?}"
        );
    }
    let back = cluster
        .group_poll_as_via(&elected, STREAM, 0, "workers", "w", true, 10)
        .await
        .expect("the holder reclaims on the new leader");
    for h in &held {
        let again = back
            .iter()
            .find(|record| record.offset == h.offset)
            .unwrap_or_else(|| panic!("{} was not reclaimed: {back:?}", h.offset));
        assert_eq!(again.attempts, h.attempts, "reclaiming spent an attempt");
    }
}

/// **An idempotent producer keeps its sequence when its Raft leader dies.**
/// The producer's id and sequences are in the entries, so the member
/// elected next answers a re-send of the last batch from its own log
/// without writing it, and takes the producer's next batch as the next in
/// sequence. Kept in the dead leader's memory, the re-send would be written
/// again and the producer told it is unknown.
#[tokio::test]
#[serial]
async fn a_producer_keeps_its_sequence_when_its_raft_leader_dies() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let addrs: Vec<std::net::SocketAddr> = survivors
        .iter()
        .map(|node| cluster.node(node).expect("node").client_addr)
        .collect();
    let client =
        felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("connect");
    let producer = std::sync::Arc::new(client)
        .idempotent_producer()
        .await
        .expect("a producer id");
    let record = |i: u32| format!("record-{i}").into_bytes();
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let publish = |i: u32| {
        let (producer, payload) = (&producer, record(i));
        let (tenant, namespace) = (&tenant, &namespace);
        async move { producer.publish(tenant, namespace, STREAM, payload).await }
    };
    // Acknowledged at the commit, so a majority holds each batch.
    for i in 0..5 {
        publish(i).await.expect("published before the failover");
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let elected = leader_among(&cluster, &survivors).await;

    // What a producer does when the answer to its last batch never came:
    // the same batch under the same sequence, to whoever leads now.
    let node = cluster.node(&elected).expect("node");
    let direct = felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect to the new leader");
    let publisher = direct.publisher().await.expect("publisher");
    felix_cluster::wait::until(
        ELECTED_WITHIN,
        "the new leader to take the re-send",
        || async {
            publisher
                .publish_idempotent_batch(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    vec![record(4)],
                    producer.producer_id(),
                    4,
                )
                .await
                .is_ok()
        },
    )
    .await
    .expect("the re-send is answered");
    for i in 5..8 {
        publish(i).await.expect("published after the failover");
    }

    // A batch of several records is written once, and its re-send is
    // answered without writing it again.
    for _ in 0..2 {
        publisher
            .publish_idempotent_batch(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                vec![record(8), record(9)],
                producer.producer_id(),
                8,
            )
            .await
            .expect("a batch of several is taken");
    }

    let owed: Vec<Vec<u8>> = (0..10).map(record).collect();
    let mut held = replay_holding(&cluster, &elected, &owed).await;
    // The harness's own readiness probe may be on the stream too.
    held.retain(|payload| payload != b"harness-probe");
    assert_eq!(held, owed, "a batch landed twice, or out of order");
    cluster.shutdown().await;
}

/// The sum of `node`'s samples of the metric whose line starts with
/// `series`, read from its own metrics.
async fn metric(cluster: &Cluster, node: &str, series: &str) -> f64 {
    let addr = cluster.node(node).expect("node").metrics_addr;
    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("scrape metrics")
        .text()
        .await
        .expect("metrics body");
    body.lines()
        .filter(|line| line.starts_with(series))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// **A broker on another engine is refused at start.** One of three Raft
/// brokers comes back on the hybrid engine with an empty disk, as a
/// replaced broker would. The control plane's identity names Raft, so the
/// broker exits before it opens anything and says why, and the Raft pair
/// keeps serving the shard. (Brokers that got past that check, under a
/// control plane with no identity route, refuse each other instead:
/// `shards/engine_match.rs`.)
#[tokio::test]
#[serial]
async fn a_broker_on_another_engine_is_refused_at_start() {
    if !opted_in() {
        return;
    }
    let hybrid = "broker-2".to_string();
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    publish_acked(&cluster, &leader_among(&cluster, &nodes).await, b"agreed").await;

    cluster
        .set_node_env(&hybrid, "FELIX_SHARD_ENGINE", "hybrid")
        .expect("set engine");
    cluster.terminate_node(&hybrid).expect("stop");
    cluster
        .wait_for_exit(&hybrid, ELECTED_WITHIN)
        .await
        .expect("exit");
    let node = cluster.node(&hybrid).expect("node");
    std::fs::remove_dir_all(node.storage_dir()).expect("empty the disk");
    let log = node.data_dir.join("broker.log");
    cluster
        .restart_node(&hybrid)
        .await
        .expect_err("a hybrid broker started under a Raft control plane");
    let said = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        said.contains("runs shard engine hybrid but the cluster was formed with raft"),
        "the refused broker does not say why: {said}"
    );

    let pair: Vec<String> = nodes.into_iter().filter(|node| *node != hybrid).collect();
    // Nothing reads the log back, so what was in doubt is only recorded.
    let leader = leader_among(&cluster, &pair).await;
    publish_tried(&cluster, &leader, "still served", &mut Sent::default()).await;
}

/// Publish latency on three brokers, one publisher, one record at a time:
/// a report for the Raft reports workflow, which sets the level and the
/// lease. `RAFT_LATENCY_LEASE_MS=0` turns the lease off, so a `Leader`
/// publish waits for its commit as a `Quorum` one does.
#[tokio::test]
#[serial]
#[ignore = "a measurement; run by .github/workflows/raft-idle.yml"]
async fn report_publish_latency() {
    if !opted_in() {
        return;
    }
    let var = |name: &str| std::env::var(name).ok();
    let level = var("RAFT_LATENCY_LEVEL").unwrap_or_else(|| "Leader".into());
    let lease = var("RAFT_LATENCY_LEASE_MS");
    let publishes = var("RAFT_LATENCY_PUBLISHES").map_or(2000, |n| n.parse().expect("a count"));
    let stream = match level.as_str() {
        "Leader" => StreamSpec::replicated(STREAM, 1, 3),
        "Quorum" => StreamSpec::quorum(STREAM, 1, 3),
        other => panic!("RAFT_LATENCY_LEVEL is Leader or Quorum, not {other}"),
    };
    let mut config = raft_cluster_with(stream);
    if let Some(lease) = &lease {
        let lease = ("FELIX_SHARD_RAFT_LEASE_MS".to_string(), lease.clone());
        config.broker_env.push(lease);
    }
    let (tenant, namespace) = (config.tenant_id.clone(), config.namespace.clone());
    let cluster = Cluster::start(config).await.expect("start cluster");
    let leader = leader_among(&cluster, &cluster.node_ids()).await;
    publish_acked(&cluster, &leader, b"first").await;

    let client = cluster.client_on(&leader).await.expect("client");
    let publisher = client.publisher().await.expect("publisher");
    let mut micros = Vec::with_capacity(publishes);
    // The first tenth warms the connection and the group and is not kept.
    for i in 0..publishes + publishes / 10 {
        let payload = vec![0u8; 256];
        let sent = std::time::Instant::now();
        publisher
            .publish(&tenant, &namespace, STREAM, payload, AckMode::PerMessage)
            .await
            .expect("acknowledged");
        if i >= publishes / 10 {
            micros.push(sent.elapsed().as_micros());
        }
    }
    micros.sort_unstable();
    let at = |percent: usize| micros[(micros.len() * percent / 100).min(micros.len() - 1)];
    let lease = lease.as_deref().unwrap_or("default");
    println!(
        "raft publish latency: level={level} lease_ms={lease} publishes={publishes} p50_us={} p99_us={}",
        at(50),
        at(99)
    );
}

/// **An atomic commit on a Raft shard is applied whole on every member.**
/// Committed through the leader, it is read back by `state_get` at its own
/// offset, a `commit_if` off its offset writes nothing of it, and every
/// broker replays the event, not the commit record, from its own log.
#[tokio::test]
#[serial]
async fn an_atomic_commit_on_a_raft_shard_reaches_every_member() {
    use felix_client::{CommitOp, ConditionalWrite};

    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["atomic_commit", "raft_atomic_commit", "raft_follower_read"],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize the commit features");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let addr = cluster.node(&leader).expect("node").client_addr;
    let client = felix_cluster::client::connect_with(
        addr,
        &cluster.tenant_id,
        &cluster.client_token(),
        |_| {},
    )
    .await
    .expect("connect");
    let ops = |value: &'static str| {
        vec![
            CommitOp::publish(STREAM, "placed"),
            CommitOp::put(STREAM, "status", value),
        ]
    };
    let (tenant, namespace) = (&cluster.tenant_id, &cluster.namespace);
    let last_error = std::sync::Mutex::new(None);
    let receipt = felix_cluster::wait::until_some(ELECTED_WITHIN, "the commit", || async {
        match client
            .commit(tenant, namespace, b"order-42", ops("placed"))
            .await
        {
            Ok(receipt) => Some(receipt),
            Err(err) => {
                *last_error.lock().unwrap() = Some(err.to_string());
                None
            }
        }
    })
    .await
    .unwrap_or_else(|err| panic!("{err}; last refusal: {:?}", last_error.lock().unwrap()));
    let state = client
        .state_get(tenant, namespace, STREAM, b"order-42", "status")
        .await
        .expect("state read");
    assert_eq!(state.value.as_deref(), Some(&b"placed"[..]));
    assert_eq!(state.version, Some(receipt.offset));
    let stale = client
        .commit_if(tenant, namespace, b"order-42", ops("stale"), receipt.offset)
        .await
        .expect("answered");
    assert!(
        matches!(stale, ConditionalWrite::Refused { .. }),
        "{stale:?}"
    );
    let state = client
        .state_get(tenant, namespace, STREAM, b"order-42", "status")
        .await
        .expect("state read");
    assert_eq!(
        state.version,
        Some(receipt.offset),
        "a refused commit moved state"
    );

    for node in &nodes {
        let addr = cluster.node(node).expect("node").client_addr;
        let reader = felix_cluster::client::connect_with(
            addr,
            &cluster.tenant_id,
            &cluster.client_token(),
            |_| {},
        )
        .await
        .expect("connect");
        let mut subscription = reader
            .subscribe_from(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .expect("replay");
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
                .await
                .unwrap_or_else(|_| panic!("{node} never replayed the commit"))
                .expect("delivery")
                .expect("the subscription stays open");
            if event.offset == Some(receipt.offset) {
                assert_eq!(event.payload.as_ref(), b"placed", "{node}");
                break;
            }
        }
    }
}

/// Every broker's sample of `name`, or `None` while one has none.
async fn samples(cluster: &Cluster, nodes: &[String], name: &str) -> Option<Vec<f64>> {
    let mut samples = Vec::new();
    for node in nodes {
        samples.push(cluster.metric(node, name).await.ok().flatten()?);
    }
    Some(samples)
}

/// Whether every shard is led by the broker placement names for it.
async fn led_where_placed(cluster: &Cluster, nodes: &[String], shards: u32) -> bool {
    let led = samples(cluster, nodes, "felix_broker_raft_shards_led").await;
    let off = samples(cluster, nodes, "felix_broker_raft_shards_led_off_placement").await;
    let (Some(led), Some(off)) = (led, off) else {
        return false;
    };
    led.iter().sum::<f64>() == f64::from(shards) && off.iter().all(|off| *off == 0.0)
}

/// **A started cluster has no first hand-over ahead of it (#3157).** The
/// broker that forms a group leads its first term, then hands over to the
/// broker placement names. `Cluster::start` used to return before that, so
/// the owner a test read next could move under it. One look, no wait: each
/// shard's placed leader leads already, and it is the owner the test reads.
#[tokio::test]
#[serial]
async fn a_started_cluster_is_led_where_placed() {
    if !opted_in() {
        return;
    }
    const SHARDS: u32 = 6;
    let cluster = Cluster::start(raft_cluster_with(StreamSpec::quorum(STREAM, SHARDS, 3)))
        .await
        .expect("start cluster");
    assert_eq!(cluster.raft_hand_over_pending().await, None);

    // Placement's owner of shard 0 leads it by its own report, and stays so.
    let owner = cluster.owner(STREAM).await.expect("owner");
    cluster
        .publish_via(&owner, STREAM, b"placed".to_vec())
        .await
        .expect("the placed leader serves the moment start returns");
    assert_eq!(cluster.owner(STREAM).await.expect("owner"), owner);
    assert_eq!(cluster.raft_hand_over_pending().await, None);
}

/// **Raft shard leaders follow placement.** Each shard's leader is the
/// broker placement names, so leaders spread as placement spreads them.
/// When a broker dies the survivors lead its shards, and once it is back
/// they hand them to it again.
#[tokio::test]
#[serial]
async fn raft_shard_leaders_return_to_placement_after_a_restart() {
    if !opted_in() {
        return;
    }
    const SHARDS: u32 = 6;
    let returned_within = Duration::from_secs(120);
    let mut cluster = Cluster::start(raft_cluster_with(StreamSpec::quorum(STREAM, SHARDS, 3)))
        .await
        .expect("start cluster");
    let nodes = cluster.node_ids();
    felix_cluster::wait::until(returned_within, "leaders where placed", || {
        led_where_placed(&cluster, &nodes, SHARDS)
    })
    .await
    .expect("leaders follow placement once formed");
    let led = samples(&cluster, &nodes, "felix_broker_raft_shards_led")
        .await
        .expect("led");
    assert!(led.iter().all(|led| *led < f64::from(SHARDS)), "{led:?}");

    let restarted = "broker-2".to_string();
    let survivors: Vec<String> = nodes.iter().filter(|n| **n != restarted).cloned().collect();
    cluster.kill_node(&restarted).expect("kill");
    felix_cluster::wait::until(ELECTED_WITHIN * 2, "survivors lead every shard", || async {
        samples(&cluster, &survivors, "felix_broker_raft_shards_led")
            .await
            .is_some_and(|led| led.iter().sum::<f64>() == f64::from(SHARDS))
    })
    .await
    .expect("survivors take over");

    cluster.restart_node(&restarted).await.expect("restart");
    felix_cluster::wait::until(returned_within, "leaders back where placed", || {
        led_where_placed(&cluster, &nodes, SHARDS)
    })
    .await
    .expect("leadership returns to the restarted broker");
    cluster.shutdown().await;
}

const CACHE: &str = "sessions";

/// Put `key` through `node` until a broker acknowledges it.
async fn cache_put_acked(cluster: &Cluster, node: &str, key: &str, value: &[u8]) {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache put to be acknowledged", || async {
        cluster.cache_put_via(node, CACHE, key, value).await.ok()
    })
    .await
    .unwrap_or_else(|err| panic!("cache put through {node}: {err:#}"));
}

/// Add one to `key` through `node` until acknowledged, returning the sum and
/// how many adds failed first: each of those may have counted anyway.
async fn counter_add_acked(cluster: &Cluster, node: &str, key: &str) -> (i64, i64) {
    let mut failed = 0;
    let deadline = tokio::time::Instant::now() + ELECTED_WITHIN;
    loop {
        match cluster.counter_add_via(node, CACHE, key, 1).await {
            Ok(sum) => return (sum, failed),
            Err(err) if tokio::time::Instant::now() > deadline => {
                panic!("counter add through {node}: {err:#}")
            }
            Err(_) => {
                failed += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// **A `Quorum` cache on Raft keeps every acknowledged value and counter add
/// when its leader dies.** Writes go through a broker that is not the
/// leader, so the forward is covered, and the survivors elect a leader that
/// serves them and takes new writes.
#[tokio::test]
#[serial]
async fn a_raft_cache_keeps_what_it_acknowledged_when_its_leader_dies() {
    if !opted_in() {
        return;
    }
    // No stream: the default one is a group of one on some broker, which
    // would count as a second leader in `leader_among`.
    let config = ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        caches: vec![felix_cluster::CacheSpec::quorum(CACHE, 1, 3)],
        broker_env: vec![engine("raft")],
        ..Default::default()
    };
    let mut cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let other = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("a follower")
        .clone();
    let mut sum = 0;
    let mut unknown = 0;
    for i in 0..10 {
        let key = format!("k{i}");
        cache_put_acked(&cluster, &other, &key, key.as_bytes()).await;
        let (acked, failed) = counter_add_acked(&cluster, &other, "hits").await;
        (sum, unknown) = (acked, unknown + failed);
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    let reader = survivors
        .iter()
        .find(|node| **node != elected)
        .expect("the other survivor")
        .clone();

    for i in 0..10 {
        let key = format!("k{i}");
        let found = felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache read", || async {
            cluster.cache_get_via(&reader, CACHE, &key).await.ok()
        })
        .await
        .expect("read");
        assert_eq!(found.as_deref(), Some(key.as_bytes()), "{key} was lost");
    }
    let counted = felix_cluster::wait::until_some(ELECTED_WITHIN, "a counter read", || async {
        cluster.counter_get_via(&reader, CACHE, "hits").await.ok()
    })
    .await
    .expect("read")
    .expect("the counter exists");
    assert!(
        (sum..=sum + unknown).contains(&counted),
        "the counter reads {counted}, acknowledged {sum} with {unknown} unknown adds"
    );

    cache_put_acked(&cluster, &reader, "after", b"after").await;
    let (after, _) = counter_add_acked(&cluster, &reader, "hits").await;
    assert!(
        after > counted,
        "an add after the failover answered {after}"
    );
}

/// **A cut-off cache leader acknowledges nothing the group loses.** The
/// cache is `Leader`, so a put is answered on the leader's lease. Once the
/// lease and the longest election timeout have run out, a put the old leader
/// still answers must be in the cache the group agrees on after the heal.
/// The caches' half of `a_cut_off_leader_acknowledges_nothing_the_group_loses_after_its_lease`.
#[tokio::test]
#[serial]
async fn a_cut_off_cache_leader_acknowledges_nothing_the_group_loses() {
    if !opted_in() {
        return;
    }
    let config = ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        caches: vec![felix_cluster::CacheSpec::replicated(CACHE, 1, 3)],
        broker_env: vec![engine("raft")],
        ..Default::default()
    };
    let cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    cache_put_acked(&cluster, &leader, "before", b"before").await;

    cluster.partition_node(&leader).expect("partition");
    // Longer than the lease (1.5 s) and the longest election timeout (2 s).
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut acked = vec!["before".to_string()];
    for i in 0..3 {
        let key = format!("cut-off-{i}");
        let put = cluster.cache_put_via(&leader, CACHE, &key, key.as_bytes());
        if matches!(
            tokio::time::timeout(Duration::from_secs(3), put).await,
            Ok(Ok(_))
        ) {
            acked.push(key);
        }
    }
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    for i in 0..5 {
        let key = format!("theirs-{i}");
        cache_put_acked(&cluster, &elected, &key, key.as_bytes()).await;
        acked.push(key);
    }

    cluster.heal_partitions().expect("heal");
    cache_put_acked(&cluster, &leader, "healed", b"healed").await;
    acked.push("healed".to_string());
    for key in &acked {
        let found = felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache read", || async {
            cluster.cache_get_via(&elected, CACHE, key).await.ok()
        })
        .await
        .expect("read");
        assert_eq!(
            found.as_deref(),
            Some(key.as_bytes()),
            "{key} was acknowledged and is lost"
        );
    }
}

/// **An entry put with a TTL on a Raft cache shard expires on the leader
/// that takes over, and is never read after its expiry.** The expiry is the
/// leader's stamp in the log, not a clock the new leader starts afresh, so a
/// failover neither extends it nor drops an entry that has time left.
#[tokio::test]
#[serial]
async fn a_raft_cache_entry_expires_on_time_across_a_failover() {
    if !opted_in() {
        return;
    }
    let config = ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        caches: vec![felix_cluster::CacheSpec::quorum(CACHE, 1, 3)],
        broker_env: vec![engine("raft")],
        ..Default::default()
    };
    let mut cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let other = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("a follower")
        .clone();

    const SHORT_MS: u64 = 15_000;
    let started = tokio::time::Instant::now();
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a ttl put to be acknowledged", || async {
        cluster
            .cache_put_ttl_via(&other, CACHE, "short", b"short", Some(SHORT_MS))
            .await
            .ok()
    })
    .await
    .expect("put with a short ttl");
    // The deadline is stamped before the ack, so a retried put that lands late
    // still expires by `acked + SHORT_MS`.
    let acked = tokio::time::Instant::now();
    cache_put_acked(&cluster, &other, "plain", b"plain").await;
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a long ttl put", || async {
        cluster
            .cache_put_ttl_via(&other, CACHE, "long", b"long", Some(3_600_000))
            .await
            .ok()
    })
    .await
    .expect("put with a long ttl");

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    leader_among(&cluster, &survivors).await;

    // Elections may outlast the short ttl on a slow runner; when they do the
    // test only checks the entry is gone, which is still the property.
    let get = |node: &str, key: &str| {
        let node = node.to_owned();
        let key = key.to_owned();
        let cluster = &cluster;
        async move {
            felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache read", || async {
                cluster.cache_get_via(&node, CACHE, &key).await.ok()
            })
            .await
            .expect("read")
        }
    };
    if started.elapsed() < Duration::from_millis(SHORT_MS - 3_000) {
        assert_eq!(
            get(&survivors[0], "short").await.as_deref(),
            Some(b"short".as_slice()),
            "an entry with time left was lost in the failover"
        );
    }

    // Past the expiry, on both survivors, the entry never reads again.
    tokio::time::sleep_until(acked + Duration::from_millis(SHORT_MS + 1_000)).await;
    for _ in 0..5 {
        for node in &survivors {
            assert_eq!(get(node, "short").await, None, "read {node} after expiry");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for node in &survivors {
        assert_eq!(
            get(node, "plain").await.as_deref(),
            Some(b"plain".as_slice())
        );
        assert_eq!(get(node, "long").await.as_deref(), Some(b"long".as_slice()));
    }
}

/// Bytes of segment files under `node`'s storage: what its logs hold on disk.
fn segment_bytes(cluster: &Cluster, node: &str) -> u64 {
    fn walk(dir: &std::path::Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                let path = entry.path();
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => walk(&path),
                    Ok(_) if path.extension().is_some_and(|ext| ext == "log") => {
                        entry.metadata().map_or(0, |meta| meta.len())
                    }
                    _ => 0,
                }
            })
            .sum()
    }
    walk(&cluster.node(node).expect("node").storage_dir())
}

/// **Retention frees disk on every member of a Raft shard.** The brokers
/// keep 16 KiB of 4 KiB segments. Once the fleet finalizes
/// `raft_stream_purge`, the leader's head trims hide what that drops and
/// every member's purge deletes the segments below the floor, so each
/// broker's segment files shrink to about the bound while the stream keeps
/// taking publishes.
#[tokio::test]
#[serial]
async fn retention_frees_the_disk_of_every_raft_member() {
    if !opted_in() {
        return;
    }
    const BOUND: u64 = 16 * 1024;
    let mut config = raft_cluster();
    config.broker_env.extend(
        [
            ("FELIX_DURABLE_SEGMENT_BYTES", "4096"),
            ("FELIX_DURABLE_RETENTION_BYTES", "16384"),
            ("FELIX_DURABLE_RETENTION_INTERVAL_SECONDS", "1"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let cluster = Cluster::start(config).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_stream_purge"], Duration::from_secs(30))
        .await
        .expect("finalize raft_stream_purge");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let payload = vec![7u8; 512];
    for _ in 0..512 {
        publish_acked(&cluster, &leader, &payload).await;
    }
    // 256 KiB was written; the bound, the segment the purge point falls in
    // and the open one are kept, plus a margin for the other logs.
    let kept = BOUND + 4 * 4096 + 16 * 1024;
    for node in &nodes {
        felix_cluster::wait::until(Duration::from_secs(60), "the member's disk freed", || {
            let held = segment_bytes(&cluster, node);
            async move { held <= kept }
        })
        .await
        .unwrap_or_else(|err| {
            panic!(
                "{node} holds {} bytes of segments: {err:#}",
                segment_bytes(&cluster, node)
            )
        });
    }
    // Still taking publishes after the purge.
    publish_acked(&cluster, &leader, b"after").await;
    cluster.shutdown().await;
}

/// **A conditional publish of several records on a Raft shard is written
/// whole at its expected offset and refused whole off it, and a leader that
/// dies as a batch lands leaves all of it or none.** Each record is a piece
/// kept only at the expected offset plus its place, and readers on every
/// member skip a batch the next leader cut short.
#[tokio::test]
#[serial]
async fn a_conditional_batch_on_a_raft_shard_is_read_whole_or_not_at_all() {
    use felix_client::ConditionalWrite;

    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["raft_conditional_batches", "raft_follower_read"],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize raft_conditional_batches");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let direct = std::sync::Arc::new(
        felix_cluster::client::connect(
            cluster.node(&leader).expect("node").client_addr,
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect to the leader"),
    );
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let batch = |prefix: &str, n: usize| -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("{prefix}{i}").into_bytes())
            .collect()
    };
    let publish_if = |payloads: Vec<Vec<u8>>, expected: u64| {
        let (direct, tenant, namespace) = (direct.clone(), tenant.clone(), namespace.clone());
        async move {
            direct
                .publish_if(&tenant, &namespace, STREAM, None, payloads, expected)
                .await
        }
    };

    // Retried at whatever tail a refusal names, so an earlier attempt that
    // timed out and landed later only moves the tail.
    let mut tail = 1 << 40;
    let deadline = tokio::time::Instant::now() + ELECTED_WITHIN;
    let kept = loop {
        assert!(tokio::time::Instant::now() < deadline, "no batch written");
        match publish_if(batch("kept", 3), tail).await {
            Ok(ConditionalWrite::Written { offset }) => break offset,
            Ok(ConditionalWrite::Refused { tail: at }) => tail = at,
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };
    let refused = publish_if(batch("stale", 2), kept).await.expect("answered");
    assert_eq!(refused, ConditionalWrite::Refused { tail: kept + 5 });

    // Batches go to the leader back to back, each at the tail the last left,
    // and the leader is killed once a few have landed, so one is very likely
    // in its log but not committed: it may commit whole, be cut short by the
    // next leader, or never be appended.
    let (landed_tx, mut landed_rx) = tokio::sync::mpsc::unbounded_channel();
    let flights = {
        let (direct, tenant, namespace) = (direct.clone(), tenant.clone(), namespace.clone());
        tokio::spawn(async move {
            let mut answers = Vec::new();
            let mut at = kept + 5;
            for k in 0u32.. {
                let pieces = (0..4)
                    .map(|i| format!("flight{k}-{i}").into_bytes())
                    .collect();
                let answer = direct
                    .publish_if(&tenant, &namespace, STREAM, None, pieces, at)
                    .await;
                match answer {
                    Ok(ConditionalWrite::Written { offset }) => {
                        answers.push(k);
                        at = offset + 4;
                        let _ = landed_tx.send(());
                    }
                    Ok(ConditionalWrite::Refused { tail }) => at = tail,
                    Err(_) => break,
                }
            }
            answers
        })
    };
    for _ in 0..3 {
        tokio::time::timeout(ELECTED_WITHIN, landed_rx.recv())
            .await
            .expect("a batch landed in time")
            .expect("a batch landed");
    }
    cluster.kill_node(&leader).expect("kill the leader");
    let acknowledged = flights.await.expect("joined");
    let elected = leader_among(&cluster, &survivors).await;
    let mut sent = Sent::default();
    let (_, after) = publish_tried(&cluster, &elected, "after", &mut sent).await;

    let mut owed = batch("kept", 3);
    owed.push(after);
    for node in &survivors {
        let held = replay_holding(&cluster, node, &owed).await;
        if let Err(wrong) = sent.check(node, &held) {
            panic!("{wrong}");
        }
        let at = held.iter().position(|p| p == b"kept0").expect("kept0");
        assert_eq!(&held[at..at + 3], &batch("kept", 3)[..], "{node}");
        assert!(
            !held.iter().any(|p| p.starts_with(b"stale")),
            "{node} read part of a refused batch: {held:?}"
        );
        // Every batch is all there, in order, or not there at all.
        for k in 0..=acknowledged.last().map_or(0, |k| k + 1) {
            let pieces = batch(&format!("flight{k}-"), 4);
            let found: Vec<usize> = held
                .iter()
                .enumerate()
                .filter(|(_, p)| pieces.contains(p))
                .map(|(at, _)| at)
                .collect();
            match found.first() {
                None => assert!(
                    !acknowledged.contains(&k),
                    "{node} lost acknowledged batch {k}"
                ),
                Some(&first) => {
                    assert_eq!(found.len(), 4, "{node} read part of batch {k}: {held:?}");
                    assert_eq!(held.get(first..first + 4), Some(&pieces[..]), "{node}");
                }
            }
        }
    }
    cluster.shutdown().await;
}

/// Four brokers and one three-copy shard, so every set change has a broker
/// outside the set to swap in.
fn four_broker_raft_cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        ..raft_cluster()
    }
}

/// The shard's stored assignment: placement's leader and followers, and the
/// voters its group's leader last reported, by broker.
async fn stored_set(cluster: &Cluster) -> (String, Vec<String>, Vec<String>) {
    let assignments = cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments");
    let shard = assignments
        .into_iter()
        .find(|a| a.key.stream == STREAM && a.key.shard == 0)
        .expect("shard 0 is placed");
    let voters = shard
        .raft_voters
        .map(|voters| voters.members.into_keys().collect())
        .unwrap_or_default();
    (shard.leader, shard.replicas, voters)
}

/// Waits until the group's reported voters are exactly `want`.
async fn voters_become(cluster: &Cluster, want: &[String], within: Duration) {
    let mut want = want.to_vec();
    want.sort();
    let waited =
        felix_cluster::wait::until(within, "the group to report the new voters", || async {
            stored_set(cluster).await.2 == want
        })
        .await;
    if let Err(err) = waited {
        let shard = cluster
            .control_plane
            .as_ref()
            .expect("control plane running")
            .store
            .list_shard_assignments()
            .await
            .expect("assignments")
            .into_iter()
            .find(|a| a.key.stream == STREAM && a.key.shard == 0)
            .expect("shard 0 is placed");
        panic!(
            "voters never became {want:?}: {err:#}; stored raft_target {:?}, raft_voters {:?}",
            shard.raft_target, shard.raft_voters
        );
    }
}

/// **An operator move outside a Raft shard's set swaps the leader's seat.**
/// The destination joins as a learner, the voters change through joint
/// consensus, the old leader hands its leadership on before it leaves, and
/// the new broker ends up leading with every acknowledged record, including
/// those published during the change.
#[tokio::test]
#[serial]
async fn an_operator_move_outside_the_set_hands_the_leaders_seat_over() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    let (old_leader, followers, _) = stored_set(&cluster).await;
    let mut members: Vec<String> = followers.clone();
    members.push(old_leader.clone());
    voters_become(&cluster, &members, converged_within).await;
    let outsider = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut acked = Vec::new();
    for n in 0..5 {
        let payload = format!("before-{n}").into_bytes();
        publish_acked(&cluster, &followers[0], &payload).await;
        acked.push(payload);
    }
    let step = cluster
        .start_move(STREAM, 0, &outsider)
        .await
        .expect("a move outside the set is taken");
    assert_eq!(step, "raft_replace");
    let mut sent = Sent::default();
    for n in 0..5 {
        let base = format!("during-{n}");
        let (_, payload) = publish_tried(&cluster, &followers[0], &base, &mut sent).await;
        acked.push(payload);
    }

    let mut want = followers.clone();
    want.push(outsider.clone());
    voters_become(&cluster, &want, converged_within).await;
    felix_cluster::wait::until(converged_within, "the moved-to broker to lead", || async {
        cluster
            .metric(&outsider, "felix_broker_raft_shards_led")
            .await
            .ok()
            .flatten()
            .is_some_and(|led| led >= 1.0)
    })
    .await
    .expect("leadership follows placement onto the new member");
    // The old leader left the set, so it must no longer lead, and with that
    // no longer acknowledge writes for the shard.
    felix_cluster::wait::until(
        converged_within,
        "the old leader to stop leading",
        || async {
            cluster
                .metric(&old_leader, "felix_broker_raft_shards_led")
                .await
                .ok()
                .flatten()
                .is_some_and(|led| led < 1.0)
        },
    )
    .await
    .expect("the old leader stops leading once it is out of the set");

    let held = replay_holding(&cluster, &outsider, &acked).await;
    for payload in &acked {
        assert_eq!(copies(&held, payload), 1, "{payload:?} once on {outsider}");
    }
    if let Err(wrong) = sent.check(&outsider, &held) {
        panic!("{wrong}");
    }
    cluster.shutdown().await;
}

/// **A drained broker's follower copy is replaced, and the broker can then
/// go.** Placement swaps it for the broker outside the set; once the group
/// reports the new voters, killing the drained broker costs no write.
#[tokio::test]
#[serial]
async fn a_drained_brokers_follower_copy_is_replaced_so_the_broker_can_go() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    // The new copy is read as a follower at the end.
    allow_follower_reads(&cluster).await;
    let (leader, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(leader.clone());
    voters_become(&cluster, &members, converged_within).await;
    let drained = followers[0].clone();
    let outsider = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut acked = Vec::new();
    for n in 0..5 {
        let payload = format!("before-{n}").into_bytes();
        publish_acked(&cluster, &leader, &payload).await;
        acked.push(payload);
    }
    cluster.drain_node(&drained).await.expect("drain");
    felix_cluster::wait::until(converged_within, "placement to swap the copy", || async {
        cluster.place_shards().await;
        let (leader, replicas, _) = stored_set(&cluster).await;
        leader != drained && !replicas.contains(&drained)
    })
    .await
    .expect("the drained follower leaves the set");
    let want: Vec<String> = members
        .iter()
        .filter(|node| **node != drained)
        .cloned()
        .chain(std::iter::once(outsider.clone()))
        .collect();
    voters_become(&cluster, &want, converged_within).await;

    cluster
        .kill_node(&drained)
        .expect("kill the drained broker");
    let (now_leader, _, _) = stored_set(&cluster).await;
    let mut sent = Sent::default();
    let (_, payload) = publish_tried(&cluster, &now_leader, "after-the-drain", &mut sent).await;
    acked.push(payload);
    let held = replay_holding(&cluster, &outsider, &acked).await;
    for payload in &acked {
        assert_eq!(copies(&held, payload), 1, "{payload:?} once on {outsider}");
    }
    if let Err(wrong) = sent.check(&outsider, &held) {
        panic!("{wrong}");
    }
    cluster.shutdown().await;
}

/// Placement's policy once a test kills a broker: one that has been down a
/// second is lost, so its seat is refilled.
fn refilling() -> felix_controlplane_service::cluster::placement::MovePolicy {
    felix_controlplane_service::cluster::placement::MovePolicy {
        restore_after_millis: Some(1_000),
        ..Default::default()
    }
}

/// One placement pass under [`refilling`].
async fn refill_pass(cluster: &Cluster) {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .place_shards_with(refilling())
        .await;
}

/// Steps placement until `done(leader, followers)` holds of the stored set.
async fn place_until(
    cluster: &Cluster,
    what: &str,
    within: Duration,
    done: impl Fn(&str, &[String]) -> bool,
) {
    let waited = felix_cluster::wait::until(within, what, || async {
        refill_pass(cluster).await;
        let (leader, replicas, _) = stored_set(cluster).await;
        done(&leader, &replicas)
    })
    .await;
    if let Err(err) = waited {
        panic!(
            "{what}: {err:#}; the stored set is {:?}",
            stored_set(cluster).await
        );
    }
}

/// Asserts `node` holds every acknowledged record of `sent` exactly once,
/// and nothing a refusal promised was not written.
async fn assert_holds_acknowledged(cluster: &Cluster, node: &str, sent: &Sent) {
    let owed: Vec<Vec<u8>> = sent
        .acked
        .iter()
        .map(|(_, payload)| payload.clone())
        .collect();
    let held = replay_holding(cluster, node, &owed).await;
    for (offset, payload) in &sent.acked {
        assert_eq!(
            copies(&held, payload),
            1,
            "{node} lost {:?}, acknowledged at offset {offset}",
            String::from_utf8_lossy(payload)
        );
    }
    if let Err(wrong) = sent.check(node, &held) {
        panic!("{wrong}");
    }
}

/// **A failover that brings in a spare broker loses nothing a majority of
/// the old set acknowledged.** Four brokers, one three-copy shard. The
/// leader acknowledges records held by itself and one follower while the
/// other follower is cut off from it, then dies. The group elects among
/// the followers, placement swaps the dead leader's seat for the spare, and
/// once the group reports the new voters, the new leader and the spare hold
/// every acknowledged record.
#[tokio::test]
#[serial]
async fn a_raft_failover_onto_a_spare_broker_keeps_what_the_old_set_acknowledged() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let (old, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(old.clone());
    voters_become(&cluster, &members, converged_within).await;
    let spare = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");
    let (lagging, other) = (followers[0].clone(), followers[1].clone());

    let mut sent = Sent::default();
    publish_tried(&cluster, &old, "before", &mut sent).await;
    cluster
        .wait_for_every_member_level(STREAM, Duration::from_secs(60))
        .await
        .expect("every member holds it");
    // What follows is acknowledged on {old, other}.
    cluster
        .inject(&Fault::Refuse {
            node: old.clone(),
            peers: vec![lagging.clone()],
        })
        .await
        .expect("cut the lagging follower off");
    for i in 0..5 {
        publish_tried(&cluster, &old, &format!("on-a-majority-{i}"), &mut sent).await;
    }
    cluster.kill_node(&old).expect("kill the old leader");

    place_until(
        &cluster,
        "placement to seat the spare for the dead leader",
        converged_within,
        |leader, replicas| {
            *leader != old
                && !replicas.contains(&old)
                && (*leader == spare || replicas.contains(&spare))
        },
    )
    .await;
    voters_become(
        &cluster,
        &[lagging.clone(), other.clone(), spare.clone()],
        converged_within,
    )
    .await;
    let (new, _, _) = stored_set(&cluster).await;
    publish_tried(&cluster, &new, "after", &mut sent).await;
    for node in [&new, &spare] {
        assert_holds_acknowledged(&cluster, node, &sent).await;
    }
    cluster.shutdown().await;
}

/// **A lost follower is replaced, and the shard gets back to three copies,
/// even when the first broker it is copied to dies mid-copy.** Five brokers,
/// one three-copy shard. A follower is killed; once it has been gone past the
/// restore delay, placement swaps in a spare. That spare is killed before the
/// swap is reported, so placement swaps in the other spare. The group ends
/// with three live voters, and the new member holds every record
/// acknowledged before the loss, as the leader after a hand-over.
#[tokio::test]
#[serial]
async fn a_raft_lost_follower_is_restored_even_when_its_first_replacement_dies() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 5,
        ..raft_cluster()
    })
    .await
    .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let (leader, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(leader.clone());
    voters_become(&cluster, &members, converged_within).await;
    let (lost, kept) = (followers[0].clone(), followers[1].clone());
    let spares: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| !members.contains(node))
        .collect();
    assert_eq!(spares.len(), 2, "two brokers outside the set");

    let mut sent = Sent::default();
    publish_tried(&cluster, &leader, "before", &mut sent).await;
    for i in 0..20 {
        publish_tried(&cluster, &leader, &format!("acknowledged-{i}"), &mut sent).await;
    }

    cluster.kill_node(&lost).expect("kill a follower");
    felix_cluster::wait::until(Duration::from_secs(20), "the follower down", || async {
        cluster
            .placeable_nodes()
            .await
            .is_ok_and(|live| !live.contains(&lost))
    })
    .await
    .expect("the follower is marked down");

    place_until(
        &cluster,
        "placement to copy the lost follower's seat to a spare",
        converged_within,
        |leader, replicas| {
            spares
                .iter()
                .any(|spare| leader == spare.as_str() || replicas.contains(spare))
        },
    )
    .await;
    let (now_leader, now_followers, _) = stored_set(&cluster).await;
    let first = spares
        .iter()
        .find(|spare| **spare == now_leader || now_followers.contains(spare))
        .expect("a spare was seated")
        .clone();
    cluster
        .kill_node(&first)
        .expect("kill the first replacement");
    let second = spares
        .iter()
        .find(|spare| **spare != first)
        .expect("the other spare")
        .clone();

    place_until(
        &cluster,
        "placement to replace the dead replacement",
        converged_within,
        |leader, replicas| {
            let nodes: Vec<&str> = std::iter::once(leader)
                .chain(replicas.iter().map(String::as_str))
                .collect();
            nodes.len() == 3
                && nodes.contains(&second.as_str())
                && nodes.contains(&kept.as_str())
                && !nodes.contains(&lost.as_str())
                && !nodes.contains(&first.as_str())
        },
    )
    .await;
    let (now_leader, now_followers, _) = stored_set(&cluster).await;
    let mut want = now_followers.clone();
    want.push(now_leader.clone());
    voters_become(&cluster, &want, converged_within).await;

    // The new copy is complete: hand the leadership to it and read it back.
    cluster
        .start_move(STREAM, 0, &second)
        .await
        .expect("move to the restored copy");
    felix_cluster::wait::until(converged_within, "the restored copy to lead", || async {
        refill_pass(&cluster).await;
        cluster
            .metric(&second, "felix_broker_raft_shards_led")
            .await
            .ok()
            .flatten()
            .is_some_and(|led| led >= 1.0)
    })
    .await
    .expect("the restored copy takes over");
    assert_holds_acknowledged(&cluster, &second, &sent).await;
    cluster.shutdown().await;
}

/// **Seating a replacement keeps every record the old set acknowledged.**
/// Four brokers, one three-copy shard. The leader acknowledges records held
/// by itself and `departing` while `lagging` and the joiner are cut off,
/// then `departing` is drained and the joiner is named in its place. The
/// group cannot commit that change without the joiner, so it must not seat
/// a copy that holds nothing; once the leader dies, whoever the survivors
/// elect holds every acknowledged record.
#[tokio::test]
#[serial]
async fn a_raft_replacement_seat_keeps_what_the_old_set_acknowledged() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let (leader, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(leader.clone());
    voters_become(&cluster, &members, converged_within).await;
    let (departing, lagging) = (followers[0].clone(), followers[1].clone());
    let joiner = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut sent = Sent::default();
    publish_tried(&cluster, &leader, "before", &mut sent).await;
    cluster
        .wait_for_every_member_level(STREAM, Duration::from_secs(60))
        .await
        .expect("every member holds it");
    // What follows is acknowledged on {leader, departing}.
    cluster
        .inject(&Fault::Refuse {
            node: leader.clone(),
            peers: vec![lagging.clone(), joiner.clone()],
        })
        .await
        .expect("cut the lagging follower and the joiner off");
    for i in 0..5 {
        publish_tried(&cluster, &leader, &format!("on-the-old-set-{i}"), &mut sent).await;
    }

    cluster.drain_node(&departing).await.expect("drain");
    place_until(
        &cluster,
        "placement to name the joiner for the drained follower",
        Duration::from_secs(60),
        |leader, replicas| *leader == joiner || replicas.contains(&joiner),
    )
    .await;
    // Placement keeps stepping while the joiner cannot be reached.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        refill_pass(&cluster).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| *node != leader)
        .collect();
    let new = leader_among(&cluster, &survivors).await;
    publish_tried(&cluster, &new, "after", &mut sent).await;
    assert_holds_acknowledged(&cluster, &new, &sent).await;
    cluster.shutdown().await;
}

/// Two brokers and one single-copy shard: an RF 1 move has one broker to go
/// to.
fn single_copy_raft_cluster() -> ClusterConfig {
    ClusterConfig {
        nodes: 2,
        ..raft_cluster_with(StreamSpec::quorum(STREAM, 1, 1))
    }
}

/// Steps placement until the stored set is `leader` alone.
async fn set_becomes_only(cluster: &Cluster, leader: &str, within: Duration) {
    let waited = felix_cluster::wait::until(within, "the set to be one broker", || async {
        cluster.place_shards().await;
        let (now, replicas, _) = stored_set(cluster).await;
        now == leader && replicas.is_empty()
    })
    .await;
    if let Err(err) = waited {
        panic!(
            "the set never became {leader} alone: {err:#}; stored {:?}",
            stored_set(cluster).await
        );
    }
}

/// **An RF 1 Raft shard moves to another broker under writes (#3012).** The
/// set grows to both brokers, the destination joins as a learner and then
/// votes, and placement shrinks the set to it. Every acknowledged record,
/// before, during and after the move, is on the destination once.
#[tokio::test]
#[serial]
async fn a_single_copy_raft_shard_moves_with_every_acknowledged_record() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let cluster = Cluster::start(single_copy_raft_cluster())
        .await
        .expect("start cluster");
    let (old, _, _) = stored_set(&cluster).await;
    voters_become(&cluster, std::slice::from_ref(&old), converged_within).await;
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != old)
        .expect("a second broker");

    let mut acked = Vec::new();
    for n in 0..5 {
        let payload = format!("before-{n}").into_bytes();
        publish_acked(&cluster, &old, &payload).await;
        acked.push(payload);
    }
    let step = cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("an RF 1 move is taken");
    assert_eq!(step, "raft_replace");
    // Writes keep landing while the destination catches up and while
    // placement shrinks the set.
    let deadline = tokio::time::Instant::now() + felix_cluster::wait::budget(converged_within);
    let mut sent = Sent::default();
    for n in 0.. {
        let (_, payload) = publish_tried(&cluster, &old, &format!("during-{n}"), &mut sent).await;
        acked.push(payload);
        cluster.place_shards().await;
        let (leader, replicas, _) = stored_set(&cluster).await;
        if leader == destination && replicas.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "placement never shrank the set to {destination}: stored {:?}",
            stored_set(&cluster).await
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    voters_become(
        &cluster,
        std::slice::from_ref(&destination),
        converged_within,
    )
    .await;
    felix_cluster::wait::until(converged_within, "the destination to lead", || async {
        cluster
            .metric(&destination, "felix_broker_raft_shards_led")
            .await
            .ok()
            .flatten()
            .is_some_and(|led| led >= 1.0)
    })
    .await
    .expect("the destination leads");
    let (_, payload) = publish_tried(&cluster, &destination, "after-the-move", &mut sent).await;
    acked.push(payload);

    let held = replay_holding(&cluster, &destination, &acked).await;
    for payload in &acked {
        assert_eq!(
            copies(&held, payload),
            1,
            "{payload:?} once on {destination}"
        );
    }
    if let Err(wrong) = sent.check(&destination, &held) {
        panic!("{wrong}");
    }
    cluster.shutdown().await;
}

/// **An RF 1 move whose destination dies before it votes is undone.** The
/// destination is killed as the move starts, so it never joins; once the
/// control plane sees it down, placement writes the old broker alone again
/// and the shard keeps serving every acknowledged record from it.
#[tokio::test]
#[serial]
async fn a_single_copy_raft_move_to_a_dead_broker_returns_to_the_old_one() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(single_copy_raft_cluster())
        .await
        .expect("start cluster");
    let (old, _, _) = stored_set(&cluster).await;
    voters_become(&cluster, std::slice::from_ref(&old), converged_within).await;
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != old)
        .expect("a second broker");

    let mut acked = Vec::new();
    for n in 0..5 {
        let payload = format!("before-{n}").into_bytes();
        publish_acked(&cluster, &old, &payload).await;
        acked.push(payload);
    }
    // Killed first so it cannot vote before it dies; the control plane
    // still counts it live until its heartbeats lapse, so the move is taken.
    cluster
        .kill_node(&destination)
        .expect("kill the destination");
    let step = cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("the move is taken before the destination is seen down");
    assert_eq!(step, "raft_replace");

    set_becomes_only(&cluster, &old, converged_within).await;
    voters_become(&cluster, std::slice::from_ref(&old), converged_within).await;
    let mut sent = Sent::default();
    for n in 0..5 {
        let (_, payload) = publish_tried(&cluster, &old, &format!("after-{n}"), &mut sent).await;
        acked.push(payload);
    }
    let held = replay_holding(&cluster, &old, &acked).await;
    for payload in &acked {
        assert_eq!(copies(&held, payload), 1, "{payload:?} once on {old}");
    }
    if let Err(wrong) = sent.check(&old, &held) {
        panic!("{wrong}");
    }
    cluster.shutdown().await;
}

/// One shard's stored assignment, by kind and name.
async fn assignment_of(
    cluster: &Cluster,
    kind: &felix_controlplane_service::model::ShardKind,
    name: &str,
) -> felix_controlplane_service::model::ShardAssignment {
    cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|a| a.key.kind == *kind && a.key.stream == name && a.key.shard == 0)
        .unwrap_or_else(|| panic!("shard 0 of {name} is placed"))
}

/// Moves the leaders of the stream's and the cache's shard to `to`, a member
/// of both sets, with operator moves, and waits until each group reports
/// `to` leading it.
async fn lead_both(cluster: &Cluster, to: &str, within: Duration) {
    use felix_controlplane_service::model::ShardKind;
    let shards = [
        ("stream", ShardKind::Stream, STREAM),
        ("cache", ShardKind::Cache, CACHE),
    ];
    for (kind, key, name) in &shards {
        if assignment_of(cluster, key, name).await.leader == to {
            continue;
        }
        let step = cluster
            .start_move_of(kind, name, 0, to)
            .await
            .unwrap_or_else(|err| panic!("move {kind} {name} to {to}: {err:#}"));
        assert_eq!(step, "raft_leader", "a move within the set names a leader");
    }
    let led = |a: &felix_controlplane_service::model::ShardAssignment| {
        a.leader == to && a.raft_leader.as_ref().is_some_and(|led| led.node == to)
    };
    let waited =
        felix_cluster::wait::until(within, "both groups to report the new leader", || async {
            for (_, key, name) in &shards {
                if !led(&assignment_of(cluster, key, name).await) {
                    return false;
                }
            }
            true
        })
        .await;
    if let Err(err) = waited {
        let mut now = Vec::new();
        for (_, key, name) in &shards {
            let a = assignment_of(cluster, key, name).await;
            now.push((*name, a.leader, a.raft_leader.map(|led| led.node)));
        }
        panic!("the leaders never moved to {to}: {err:#}; placed and reported: {now:?}");
    }
}

/// Runs `op` until it is answered, sending it again only after a refusal
/// that promises nothing was applied, as the client contract allows. `Err`
/// is the error that left it in doubt. Counts the refusals in `refused`.
async fn answered<T, F, Fut>(what: &str, refused: &mut usize, op: F) -> Result<T, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let deadline = tokio::time::Instant::now() + felix_cluster::wait::budget(ELECTED_WITHIN);
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if failed(&err) == Failed::Refused => {
                *refused += 1;
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{what} was refused for {ELECTED_WITHIN:?}: {err:#}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => return Err(format!("{err:#}")),
        }
    }
}

/// What one cache writer's puts and deletes came to.
#[derive(Debug, Default)]
struct CacheWrites {
    /// Acknowledged puts not deleted since: each reads back.
    kept: Vec<(String, Vec<u8>)>,
    /// Acknowledged deletes: each reads back absent.
    deleted: Vec<String>,
    /// A put or delete that ended in doubt, with the value put: the key
    /// reads back as that value or absent.
    in_doubt: Vec<(String, Vec<u8>)>,
    refused: usize,
}

/// Puts fresh keys through `via` until `stop`, deleting every fourth again.
async fn cache_writes_until(
    cluster: &Cluster,
    via: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> CacheWrites {
    use std::sync::atomic::Ordering;
    let client = cluster.client_on(via).await.expect("connect");
    let (tenant, namespace) = (&cluster.tenant_id, &cluster.namespace);
    let mut writes = CacheWrites::default();
    let mut i = 0usize;
    while !stop.load(Ordering::Acquire) {
        i += 1;
        let key = format!("{via}-{i}");
        let value = format!("value-{i}").into_bytes();
        let put = answered(&format!("put {key}"), &mut writes.refused, || {
            client.cache_put(
                tenant,
                namespace,
                CACHE,
                &key,
                bytes::Bytes::from(value.clone()),
                None,
            )
        })
        .await;
        if put.is_err() {
            writes.in_doubt.push((key, value));
        } else if i.is_multiple_of(4) {
            let deleted = answered(&format!("delete {key}"), &mut writes.refused, || {
                client.cache_delete(tenant, namespace, CACHE, &key)
            })
            .await;
            match deleted {
                Ok(_) => writes.deleted.push(key),
                Err(_) => writes.in_doubt.push((key, value)),
            }
        } else {
            writes.kept.push((key, value));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    writes
}

/// How many adds one counter key takes. Add `n` to a key is `4^n`, so the
/// key's sum, read in base 4, says how many times each add was applied.
const ADDS_PER_KEY: u32 = 20;

/// What one counter writer's adds came to.
#[derive(Debug, Default)]
struct CounterAdds {
    /// Each add as its key, its digit and whether it was acknowledged.
    /// One not acknowledged ended in doubt and was never sent again.
    adds: Vec<(String, u32, bool)>,
    refused: usize,
}

/// Adds to counters through `via` until `stop`, each add an amount of its
/// own.
async fn counter_adds_until(
    cluster: &Cluster,
    via: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> CounterAdds {
    use std::sync::atomic::Ordering;
    let client = cluster.client_on(via).await.expect("connect");
    let (tenant, namespace) = (&cluster.tenant_id, &cluster.namespace);
    let mut counted = CounterAdds::default();
    let mut n = 0u32;
    while !stop.load(Ordering::Acquire) {
        let key = format!("{via}-adds-{}", n / ADDS_PER_KEY);
        let digit = n % ADDS_PER_KEY;
        n += 1;
        let added = answered(&format!("add to {key}"), &mut counted.refused, || {
            client.counter_add(tenant, namespace, CACHE, &key, 4i64.pow(digit))
        })
        .await;
        counted.adds.push((key, digit, added.is_ok()));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    counted
}

/// Publishes fresh payloads through `via` until `stop`. A failed publish is
/// never sent again, so `Sent` can tell a refusal that wrote.
async fn publish_jobs_until(
    cluster: &Cluster,
    via: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> Sent {
    use std::sync::atomic::Ordering;
    let client = cluster.client_on(via).await.expect("connect");
    let publisher = client.publisher().await.expect("publisher");
    let mut sent = Sent::default();
    let mut i = 0usize;
    while !stop.load(Ordering::Acquire) {
        let payload = format!("job-{i}").into_bytes();
        i += 1;
        let published = publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                payload.clone(),
                AckMode::PerMessage,
            )
            .await;
        match published {
            Ok(offset) => sent.acked.push((
                offset.expect("a durable stream acknowledges with an offset"),
                payload,
            )),
            Err(err) => sent.failed(payload, &err),
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    sent
}

/// What a group consumer's claims and finishes came to.
#[derive(Debug, Default)]
struct Consumed {
    /// Offsets finished by an acknowledged ack, with their payloads.
    finished: BTreeMap<u64, Vec<u8>>,
    /// Offsets whose ack ended in doubt: each may be handed out again.
    in_doubt: std::collections::HashSet<u64>,
    /// Polls that ended in doubt: what they claimed comes back once the
    /// claim lapses.
    polls_in_doubt: usize,
    refused: usize,
}

/// One poll and an ack for each record it hands out. Returns how many it
/// handed out. A record handed out again after an acknowledged ack fails
/// the test: that would be a record finished twice.
async fn consume_once(group: &felix_client::ShardedGroup, consumed: &mut Consumed) -> usize {
    let polled = answered("group poll", &mut consumed.refused, || group.poll(5)).await;
    let Ok(batch) = polled else {
        consumed.polls_in_doubt += 1;
        return 0;
    };
    for claimed in &batch {
        let offset = claimed.record.offset;
        assert!(
            !consumed.finished.contains_key(&offset),
            "offset {offset} was handed out again after an acknowledged ack"
        );
        let acked = answered(&format!("ack {offset}"), &mut consumed.refused, || {
            group.ack(claimed)
        })
        .await;
        match acked {
            Ok(()) => {
                consumed
                    .finished
                    .insert(offset, claimed.record.payload.to_vec());
            }
            Err(_) => {
                consumed.in_doubt.insert(offset);
            }
        }
    }
    batch.len()
}

/// How many times each of a counter key's adds was applied, from its sum.
fn applied_adds(key: &str, sum: i64) -> Vec<i64> {
    assert!(sum >= 0, "{key} reads {sum}, below every sum of its adds");
    let mut rest = sum;
    let mut times = Vec::new();
    for _ in 0..ADDS_PER_KEY {
        times.push(rest % 4);
        rest /= 4;
    }
    assert_eq!(rest, 0, "{key} reads {sum}, above every sum of its adds");
    times
}

/// The counter check has teeth without a cluster: an add applied twice, an
/// acknowledged add missing and a refused add applied are each told apart
/// from an in-doubt add that landed once.
#[test]
fn a_counter_add_applied_twice_is_seen_in_its_sum() {
    let sum = |adds: &[u32]| adds.iter().map(|digit| 4i64.pow(*digit)).sum::<i64>();
    assert_eq!(applied_adds("k", sum(&[0, 2, 3])), {
        let mut want = vec![0; ADDS_PER_KEY as usize];
        for digit in [0, 2, 3] {
            want[digit] = 1;
        }
        want
    });
    assert_eq!(applied_adds("k", sum(&[1, 1]))[1], 2, "applied twice");
    assert_eq!(applied_adds("k", sum(&[19, 19, 19]))[19], 3);
}

/// **A Raft hand-over keeps every kind of acknowledged write.** Cache puts
/// and deletes, counter adds and a consumer group's claims and finishes go
/// on through two brokers while operator moves hand the leadership of the
/// stream's and the cache's shard back and forth among the three members.
/// A write refused as not applied is sent again; one that ended in doubt is
/// not. Afterwards every acknowledged cache write reads back, each counter
/// add was applied exactly once when acknowledged and at most once when in
/// doubt (each add has its own amount, so its sum says which landed and
/// how often: the counter API has no idempotency key), and no record the
/// group finished with an acknowledged ack is handed out again. The Raft
/// half of `queues/handoff.rs`'s `writes_of_every_kind_through_a_move_are_never_refused`,
/// without its no-refusal claim.
#[tokio::test]
#[serial]
async fn a_raft_hand_over_keeps_every_kind_of_acknowledged_write() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use felix_controlplane_service::model::ShardKind;

    const GROUP: &str = "workers";
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let config = ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        caches: vec![felix_cluster::CacheSpec::quorum(CACHE, 1, 3)],
        // A claim from a poll that ended in doubt comes back after this, so
        // the drain below does not wait out the default.
        broker_env: vec![
            engine("raft"),
            (
                "FELIX_GROUP_VISIBILITY_TIMEOUT_MS".to_string(),
                "10000".to_string(),
            ),
        ],
        ..Default::default()
    };
    let cluster = Cluster::start(config).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_consumer_groups"], Duration::from_secs(30))
        .await
        .expect("finalize the group feature");
    let nodes = cluster.node_ids();
    let first = assignment_of(&cluster, &ShardKind::Stream, STREAM)
        .await
        .leader;
    let others: Vec<String> = nodes.iter().filter(|n| **n != first).cloned().collect();
    // Both shards on one leader, so each hand-over below moves both.
    lead_both(&cluster, &first, converged_within).await;

    let client = std::sync::Arc::new(
        felix_cluster::client::connect_cluster(
            &cluster.broker_addrs(),
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect a cluster client"),
    );
    let group = client
        .group_sharded(&cluster.tenant_id, &cluster.namespace, STREAM, GROUP)
        .await
        .expect("sharded group");

    let stop = AtomicBool::new(false);
    let mover = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        for to in [&others[0], &others[1], &others[0], &first] {
            lead_both(&cluster, to, converged_within).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.store(true, Ordering::Release);
    };
    let consumer = async {
        let mut consumed = Consumed::default();
        while !stop.load(Ordering::Acquire) {
            if consume_once(&group, &mut consumed).await == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        consumed
    };
    let ((), cache_a, cache_b, adds_a, adds_b, jobs, mut consumed) = tokio::join!(
        mover,
        cache_writes_until(&cluster, &nodes[0], &stop),
        cache_writes_until(&cluster, &nodes[1], &stop),
        counter_adds_until(&cluster, &nodes[0], &stop),
        counter_adds_until(&cluster, &nodes[1], &stop),
        publish_jobs_until(&cluster, &nodes[0], &stop),
        consumer,
    );
    let caches = [&cache_a, &cache_b];
    let adds = [&adds_a, &adds_b];
    println!(
        "acknowledged: {} cache puts kept, {} deletes, {} counter adds, {} publishes, {} finishes; \
         in doubt: {} cache, {} counter, {} publishes, {} acks, {} polls; refused and re-sent: \
         {} cache, {} counter, {} group",
        caches.iter().map(|c| c.kept.len()).sum::<usize>(),
        caches.iter().map(|c| c.deleted.len()).sum::<usize>(),
        adds.iter()
            .map(|a| a.adds.iter().filter(|add| add.2).count())
            .sum::<usize>(),
        jobs.acked.len(),
        consumed.finished.len(),
        caches.iter().map(|c| c.in_doubt.len()).sum::<usize>(),
        adds.iter()
            .map(|a| a.adds.iter().filter(|add| !add.2).count())
            .sum::<usize>(),
        jobs.in_doubt.len(),
        consumed.in_doubt.len(),
        consumed.polls_in_doubt,
        caches.iter().map(|c| c.refused).sum::<usize>(),
        adds.iter().map(|a| a.refused).sum::<usize>(),
        consumed.refused,
    );
    for writes in caches {
        assert!(!writes.kept.is_empty(), "no cache put was acknowledged");
    }
    assert!(!jobs.acked.is_empty(), "no publish was acknowledged");

    let reader = &nodes[2];
    let read = |key: String| {
        let cluster = &cluster;
        async move {
            felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache read", || async {
                cluster.cache_get_via(reader, CACHE, &key).await.ok()
            })
            .await
            .unwrap_or_else(|err| panic!("read {key}: {err:#}"))
        }
    };
    for writes in caches {
        for (key, value) in &writes.kept {
            assert_eq!(
                read(key.clone()).await.as_ref(),
                Some(value),
                "{key} was acknowledged and does not read back"
            );
        }
        for key in &writes.deleted {
            assert_eq!(
                read(key.clone()).await,
                None,
                "{key} was deleted and reads back"
            );
        }
        for (key, value) in &writes.in_doubt {
            let found = read(key.clone()).await;
            assert!(
                found.is_none() || found.as_ref() == Some(value),
                "{key}, in doubt, reads {found:?}"
            );
        }
    }

    let mut by_key: BTreeMap<&str, Vec<(u32, bool)>> = BTreeMap::new();
    for counted in adds {
        for (key, digit, acked) in &counted.adds {
            by_key
                .entry(key.as_str())
                .or_default()
                .push((*digit, *acked));
        }
    }
    for (key, sent) in &by_key {
        let sum = felix_cluster::wait::until_some(ELECTED_WITHIN, "a counter read", || async {
            cluster.counter_get_via(reader, CACHE, key).await.ok()
        })
        .await
        .unwrap_or_else(|err| panic!("read {key}: {err:#}"))
        .unwrap_or(0);
        let times = applied_adds(key, sum);
        for (digit, applied) in times.iter().enumerate() {
            let digit = digit as u32;
            match sent.iter().find(|(sent, _)| *sent == digit) {
                Some((_, true)) => assert_eq!(
                    *applied, 1,
                    "{key}'s acknowledged add {digit} was applied {applied} times (sum {sum})"
                ),
                Some((_, false)) => assert!(
                    *applied <= 1,
                    "{key}'s add {digit}, in doubt and never re-sent, was applied {applied} \
                     times (sum {sum})"
                ),
                None => assert_eq!(*applied, 0, "{key}'s add {digit} was never sent"),
            }
        }
    }

    // Finish what the consumer had not got to, the claims of polls in doubt
    // once they lapse. Every acknowledged publish is finished once.
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(90));
    while jobs
        .acked
        .iter()
        .any(|(offset, _)| !consumed.finished.contains_key(offset))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "acknowledged records never finished: {:?}",
            jobs.acked
                .iter()
                .filter(|(offset, _)| !consumed.finished.contains_key(offset))
                .map(|(offset, _)| offset)
                .take(10)
                .collect::<Vec<_>>()
        );
        if consume_once(&group, &mut consumed).await == 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    for (offset, payload) in &jobs.acked {
        assert_eq!(
            consumed.finished.get(offset),
            Some(payload),
            "offset {offset} was finished with another record"
        );
    }
    let leader = assignment_of(&cluster, &ShardKind::Stream, STREAM)
        .await
        .leader;
    assert_holds_acknowledged(&cluster, &leader, &jobs).await;
    cluster.shutdown().await;
}

/// What a subscriber read: each event, and where it subscribed again.
#[derive(Debug, Clone)]
enum Read {
    /// Subscribed again from this offset after its subscription ended.
    From(u64),
    Event {
        offset: u64,
        skipped_before: u64,
        payload: Vec<u8>,
    },
}

/// Checks that `read` names every offset once and in order, and that every
/// jump between events is one the broker said holds no event. Returns each
/// event's offset and payload.
fn read_in_order(read: &[Read]) -> Vec<(u64, Vec<u8>)> {
    let mut next = 0u64;
    let mut events = Vec::new();
    for step in read {
        match step {
            Read::From(at) => assert_eq!(*at, next, "subscribed again from {at}, not {next}"),
            Read::Event {
                offset,
                skipped_before,
                payload,
            } => {
                assert_eq!(
                    *offset,
                    next + skipped_before,
                    "after {next} the subscriber saw {offset} with {skipped_before} \
                     skipped before it: a gap no one was told of, or an offset again"
                );
                next = offset + 1;
                events.push((*offset, payload.clone()));
            }
        }
    }
    events
}

#[test]
fn a_gap_no_one_was_told_of_fails_the_read_check() {
    let event = |offset, skipped_before| Read::Event {
        offset,
        skipped_before,
        payload: Vec::new(),
    };
    let told = [event(1, 1), event(2, 0), Read::From(3), event(5, 2)];
    assert_eq!(read_in_order(&told).len(), 3);
    for wrong in [
        vec![event(0, 0), event(2, 0)],
        vec![event(0, 0), event(0, 0)],
        vec![event(0, 0), Read::From(0), event(0, 0)],
    ] {
        let caught = std::panic::catch_unwind(|| read_in_order(&wrong));
        assert!(caught.is_err(), "{wrong:?} passed");
    }
}

/// **A Raft move undone after the voters changed loses no acknowledged
/// write, and its subscriber sees every offset once, in order.** Four
/// brokers, one three-copy shard. A publisher and a subscriber from offset 0
/// run throughout an operator move to the broker outside the set, which
/// runs until the group reports the new voters, and a move back that
/// undoes it. Each blank and membership entry of those changes takes a
/// stream offset, as the offset is the Raft index, so the subscriber must
/// be told of each (`skipped_before`) and must see no other gap. The final
/// set holds every acknowledged record at its offset, and every record the
/// subscriber saw is the one the log holds there. The Raft half of
/// `routing/operator_moves.rs`'s `cancelling_a_fenced_move_loses_no_acknowledged_write`.
#[tokio::test]
#[serial]
async fn a_raft_move_cancelled_after_the_voters_changed_loses_no_acknowledged_write() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let (old, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(old.clone());
    voters_become(&cluster, &members, converged_within).await;
    let outsider = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");
    let mut before = Sent::default();
    for n in 0..10 {
        publish_tried(&cluster, &followers[0], &format!("before-{n}"), &mut before).await;
    }

    let (addrs, token) = (cluster.broker_addrs(), cluster.client_token());
    let connect = || felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &token);
    let stop = Arc::new(AtomicBool::new(false));
    let done_reading = Arc::new(AtomicBool::new(false));
    let read: Arc<Mutex<Vec<Read>>> = Arc::default();
    let reader = Arc::new(connect().await.expect("reader"));
    let subscriber = {
        let (read, done) = (Arc::clone(&read), Arc::clone(&done_reading));
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let subscribe = |from| {
                let (reader, tenant, namespace) = (&reader, &tenant, &namespace);
                async move {
                    reader
                        .subscribe_from(
                            tenant,
                            namespace,
                            STREAM,
                            Some(felix_client::StartPosition::Offset(from)),
                        )
                        .await
                }
            };
            let mut subscription = subscribe(0).await.expect("subscribe");
            let mut next = 0u64;
            while !done.load(Ordering::Relaxed) {
                let step =
                    tokio::time::timeout(Duration::from_secs(1), subscription.next_event()).await;
                match step {
                    Ok(Ok(Some(event))) => {
                        let offset = event.offset.expect("a durable stream carries offsets");
                        next = offset + 1;
                        read.lock().expect("read").push(Read::Event {
                            offset,
                            skipped_before: event.skipped_before,
                            payload: event.payload.to_vec(),
                        });
                    }
                    // Quiet, not ended.
                    Err(_) => {}
                    // The subscription follows the shard itself; one that
                    // still ends resumes where it stopped.
                    Ok(Ok(None) | Err(_)) => loop {
                        if done.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Ok(resumed) = subscribe(next).await {
                            read.lock().expect("read").push(Read::From(next));
                            subscription = resumed;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    },
                }
            }
        })
    };

    let during: Arc<Mutex<Sent>> = Arc::default();
    let writer = connect().await.expect("writer");
    let publisher = {
        let (during, stop) = (Arc::clone(&during), Arc::clone(&stop));
        let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
        tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let payload = format!("during-{i}").into_bytes();
                i += 1;
                let published = writer
                    .publish(
                        &tenant,
                        &namespace,
                        STREAM,
                        payload.clone(),
                        AckMode::PerMessage,
                    )
                    .await;
                {
                    let mut sent = during.lock().expect("sent");
                    match published {
                        Ok(offset) => sent.acked.push((
                            offset.expect("a durable stream acknowledges with an offset"),
                            payload,
                        )),
                        Err(err) => sent.failed(payload, &err),
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let acked_so_far = || during.lock().expect("sent").acked.len();

    tokio::time::sleep(Duration::from_millis(300)).await;
    let step = cluster
        .start_move(STREAM, 0, &outsider)
        .await
        .expect("a move outside the set is taken");
    assert_eq!(step, "raft_replace");
    let mut moved = followers.clone();
    moved.push(outsider.clone());
    voters_become(&cluster, &moved, converged_within).await;

    // Undone by a move back. The old leader is outside the set now, so this
    // is a seat move of its own.
    let step = felix_cluster::wait::until_some(
        Duration::from_secs(60),
        "the move back to be taken",
        || async { cluster.start_move(STREAM, 0, &old).await.ok() },
    )
    .await
    .unwrap_or_else(|err| {
        panic!("the move back to {old} was never taken: {err:#}");
    });
    assert_eq!(step, "raft_replace");
    voters_become(&cluster, &members, converged_within).await;
    felix_cluster::wait::until(converged_within, "the old leader to lead again", || async {
        cluster
            .metric(&old, "felix_broker_raft_shards_led")
            .await
            .ok()
            .flatten()
            .is_some_and(|led| led >= 1.0)
    })
    .await
    .expect("leadership follows placement back");
    let back_at = acked_so_far();
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "publishes acknowledged after the move back",
        || async { acked_so_far() >= back_at + 10 },
    )
    .await
    .expect("the shard takes writes once it is back");
    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("publisher");

    let mut sent = before;
    {
        let during = during.lock().expect("sent");
        sent.acked.extend(during.acked.iter().cloned());
        sent.in_doubt.extend(during.in_doubt.iter().cloned());
        sent.refused.extend(during.refused.iter().cloned());
    }
    let owed: Vec<Vec<u8>> = sent.acked.iter().map(|(_, p)| p.clone()).collect();
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the subscriber to see every acknowledged record",
        || async {
            let read = read.lock().expect("read");
            let seen: std::collections::HashSet<&[u8]> = read
                .iter()
                .filter_map(|step| match step {
                    Read::Event { payload, .. } => Some(payload.as_slice()),
                    Read::From(_) => None,
                })
                .collect();
            owed.iter().all(|payload| seen.contains(payload.as_slice()))
        },
    )
    .await
    .expect("the subscriber gets every acknowledged record");
    done_reading.store(true, Ordering::Relaxed);
    subscriber.await.expect("subscriber");

    let read = read.lock().expect("read").clone();
    let seen: BTreeMap<u64, Vec<u8>> = read_in_order(&read).into_iter().collect();
    let skipped: u64 = read
        .iter()
        .map(|step| match step {
            Read::Event { skipped_before, .. } => *skipped_before,
            Read::From(_) => 0,
        })
        .sum();
    println!(
        "{} records seen, {skipped} offsets skipped as not events, {} acknowledged",
        seen.len(),
        sent.acked.len()
    );
    for (offset, payload) in &sent.acked {
        assert_eq!(
            seen.get(offset),
            Some(payload),
            "the subscriber did not see {:?} at its acknowledged offset {offset}",
            String::from_utf8_lossy(payload)
        );
    }
    for node in &members {
        let held: BTreeMap<u64, Vec<u8>> = replay_at(&cluster, node, &owed)
            .await
            .into_iter()
            .map(|(offset, payload)| (offset.expect("a durable stream carries offsets"), payload))
            .collect();
        for (offset, payload) in &sent.acked {
            assert_eq!(
                held.get(offset),
                Some(payload),
                "{node} does not hold {:?} at its acknowledged offset {offset}",
                String::from_utf8_lossy(payload)
            );
        }
        // The replay reads to the last acknowledged record, so compare up to
        // there: what the subscriber saw past it may not be replayed yet.
        let last = held.keys().next_back().copied().unwrap_or(0);
        for (offset, payload) in seen.range(..=last) {
            assert_eq!(
                held.get(offset),
                Some(payload),
                "the subscriber saw {:?} at {offset}, which {node} does not hold there",
                String::from_utf8_lossy(payload)
            );
        }
        let payloads: Vec<Vec<u8>> = held.into_values().collect();
        if let Err(wrong) = sent.check(node, &payloads) {
            panic!("{wrong}");
        }
    }
    cluster.shutdown().await;
}

/// How many voter changes the brokers in `nodes` have committed, summed.
async fn membership_changes(cluster: &Cluster, nodes: &[String]) -> f64 {
    let mut total = 0.0;
    for node in nodes {
        total += metric(
            cluster,
            node,
            r#"felix_broker_raft_membership_steps_total{step="change"}"#,
        )
        .await;
    }
    total
}

/// **A Raft move cut short by kills keeps what was acknowledged.** Five
/// brokers, one three-copy shard. One follower, `short`, gets nothing past
/// the first record, so it lacks records a majority acknowledged. An
/// operator move to a broker outside the set starts; once the group's first
/// change has made the destination a voter beside the old leader, before a
/// later one removes the old leader, the old leader and the destination are
/// killed.
///
/// Through that change the voters are the old set and the destination, so
/// the two survivors are no majority and may elect no one until a killed
/// broker is back; if the change had already finished they are, and only
/// the one holding the log can win. Either way any leader holds every
/// acknowledged record. The two brokers then come back, the move finishes
/// with three voters, the destination holds the log, and no group on any
/// broker stopped. The Raft half of `routing/operator_moves.rs`'s two
/// `a_move_cut_short_by_kills_*` tests.
#[tokio::test]
#[serial]
async fn a_raft_move_cut_short_by_kills_keeps_what_was_acknowledged() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 5,
        ..raft_cluster()
    })
    .await
    .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let (old, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(old.clone());
    voters_become(&cluster, &members, converged_within).await;
    let (kept, short) = (followers[0].clone(), followers[1].clone());
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut sent = Sent::default();
    publish_tried(&cluster, &old, "before", &mut sent).await;
    cluster
        .wait_for_every_member_level(STREAM, Duration::from_secs(60))
        .await
        .expect("every member holds it");
    // No broker reaches `short` from here on. Its own requests still go, so
    // it can stand for election with its short log.
    for node in cluster.node_ids().into_iter().filter(|node| *node != short) {
        cluster
            .inject(&Fault::Refuse {
                node,
                peers: vec![short.clone()],
            })
            .await
            .expect("cut the short follower off");
    }
    for i in 0..10 {
        publish_tried(&cluster, &old, &format!("acknowledged-{i}"), &mut sent).await;
    }

    // Any member that leads during the move can take a change step.
    let watched = [old.clone(), kept.clone(), destination.clone()];
    let changed_before = membership_changes(&cluster, &watched).await;
    let step = cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("a move outside the set is taken");
    assert_eq!(step, "raft_replace");
    let waited = felix_cluster::wait::until(
        Duration::from_secs(120),
        "the group to make the destination a voter",
        || async { membership_changes(&cluster, &watched).await > changed_before },
    )
    .await;
    if let Err(err) = waited {
        panic!(
            "{err:#}; the stored set is {:?}",
            stored_set(&cluster).await
        );
    }
    cluster.kill_node(&old).expect("kill the old leader");
    cluster
        .kill_node(&destination)
        .expect("kill the destination");
    let at_kill = stored_set(&cluster).await;
    cluster.heal_all().await.expect("heal");

    let survivors = [kept.clone(), short.clone()];
    match leader_within(&cluster, &survivors, Duration::from_secs(30)).await {
        Some(leader) => {
            println!("{leader} leads the survivors; stored set at the kills {at_kill:?}");
            assert_holds_acknowledged(&cluster, &leader, &sent).await;
        }
        None => println!(
            "no survivor leads with the old leader and the destination down; stored set at \
             the kills {at_kill:?}"
        ),
    }

    cluster
        .restart_node(&destination)
        .await
        .expect("restart the destination");
    cluster
        .restart_node(&old)
        .await
        .expect("restart the old leader");
    let leader = leader_among(
        &cluster,
        &[
            kept.clone(),
            short.clone(),
            destination.clone(),
            old.clone(),
        ],
    )
    .await;
    assert_holds_acknowledged(&cluster, &leader, &sent).await;
    voters_become(
        &cluster,
        &[kept.clone(), short.clone(), destination.clone()],
        converged_within,
    )
    .await;
    let (now_leader, now_followers, _) = stored_set(&cluster).await;
    assert_eq!(
        now_followers.len() + 1,
        3,
        "the set is back to three: {now_leader} {now_followers:?}"
    );
    publish_tried(&cluster, &old, "after", &mut sent).await;
    for node in [&destination, &short, &kept] {
        assert_holds_acknowledged(&cluster, node, &sent).await;
    }
    for node in cluster.node_ids() {
        let stopped = metric(
            &cluster,
            &node,
            "felix_broker_raft_shards_stopped_on_storage_error",
        )
        .await;
        assert_eq!(stopped, 0.0, "{node} stopped a group");
    }
    cluster.shutdown().await;
}
