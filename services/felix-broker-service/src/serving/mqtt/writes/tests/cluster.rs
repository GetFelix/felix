//! The write path in a cluster: the quorum wait on the owner, the refusals
//! when nobody here may write, and forwarding to the owner, against an owner
//! that answers from a script.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Mutex;

use felix_broker::ConsistencyLevel;
use felix_replication::peer::pool::PeerError;
use felix_router::{NodeRef, RegionRouter, ShardRouter};
use felix_wire::internal::{
    ForwardPublish, ForwardPublishError, ForwardPublishOk, InternalMessage, Kind,
};

use super::*;
use crate::shards::routing::routing_table_from;
use crate::shards::watch::ShardAssignment;

const LOCAL: &str = "broker-a";
const OWNER: &str = "broker-b";
const GENERATION: u64 = 3;

fn shard() -> ShardKey {
    ShardKey {
        tenant_id: "acme".to_string(),
        namespace: "ns".to_string(),
        stream: "plant".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

/// This broker's view: `leader` leads the shard with one follower, and the
/// shard is open here when this broker leads it.
fn ingress(leader: &str) -> Arc<IngressRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let ingress = Arc::new(IngressRouter::new(router, Arc::default()));
    show(&ingress, leader, GENERATION);
    ingress
}

/// Publish a view in which `leader` leads the shard at `generation`.
fn show(ingress: &IngressRouter, leader: &str, generation: u64) {
    let nodes: HashMap<String, NodeRef> = [(LOCAL, 7001), (OWNER, 7002), ("broker-c", 7003)]
        .into_iter()
        .map(|(id, port)| {
            (
                id.to_string(),
                NodeRef {
                    node_id: id.to_string(),
                    advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
                    region: "us-west-2".to_string(),
                    live: true,
                },
            )
        })
        .collect();
    let follower = if leader == LOCAL { OWNER } else { LOCAL };
    let assignments: HashMap<ShardKey, ShardAssignment> = [(
        shard(),
        ShardAssignment {
            key: shard(),
            leader: leader.to_string(),
            replicas: vec![follower.to_string()],
            generation,
            state: "active".to_string(),
            successor: None,
            routing: Default::default(),
            incarnation: 0,
            fence_storage_ids: Default::default(),
            member_storage_ids: Default::default(),
            pending_set: None,
            previous_generation: None,
        },
    )]
    .into_iter()
    .collect();
    let servable: HashMap<ShardKey, u64> = if leader == LOCAL {
        ingress.fence().open(&shard(), generation);
        [(shard(), generation)].into_iter().collect()
    } else {
        HashMap::new()
    };
    ingress.publish(routing_table_from(&assignments, &nodes), &nodes, servable);
}

/// An owner that answers forwards from a script and keeps what it was sent.
struct ScriptedOwner {
    answers: Mutex<VecDeque<Result<InternalMessage, PeerError>>>,
    sent: Mutex<Vec<(Kind, ForwardPublish)>>,
}

impl ScriptedOwner {
    fn new(answers: impl IntoIterator<Item = Result<InternalMessage, PeerError>>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into_iter().collect()),
            sent: Mutex::new(Vec::new()),
        })
    }

    fn sent(&self) -> Vec<(Kind, ForwardPublish)> {
        self.sent.lock().expect("lock").clone()
    }
}

impl PeerRequester for ScriptedOwner {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        let kind = message.kind();
        let InternalMessage::ForwardPublish(publish) = message else {
            panic!("an mqtt write sent a {kind:?}");
        };
        self.sent.lock().expect("lock").push((kind, publish));
        self.answers
            .lock()
            .expect("lock")
            .pop_front()
            .expect("the script ran out")
    }
}

fn ok() -> Result<InternalMessage, PeerError> {
    Ok(InternalMessage::ForwardPublishOk(ForwardPublishOk::new(
        0,
        Some((4, 4)),
    )))
}

fn refused(code: ErrorCode) -> Result<InternalMessage, PeerError> {
    Ok(InternalMessage::ForwardPublishError(ForwardPublishError {
        correlation_id: 0,
        code,
        detail: "no".to_string(),
    }))
}

fn lost() -> Result<InternalMessage, PeerError> {
    Err(PeerError::Disconnected {
        node_id: OWNER.to_string(),
    })
}

async fn forwarding(
    owner: &Arc<ScriptedOwner>,
    budget: Duration,
) -> (BrokerWrites<ScriptedOwner>, Arc<Broker>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    broker.record_headers_when(|| true);
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Some(ingress(OWNER)),
        Some(Arc::new(QuorumMarks::new())),
        Duration::from_secs(1),
    )
    .with_forwarding(Arc::clone(owner), budget);
    (writes, broker, dir)
}

/// A batch for a shard another broker owns goes to it as a sequenced forward
/// carrying the connection's producer id, sequence, token and the records'
/// header blocks, and nothing is written here.
#[tokio::test]
async fn a_batch_for_another_owner_is_forwarded_under_its_sequence() {
    let owner = ScriptedOwner::new([ok()]);
    let (writes, broker, _dir) = forwarding(&owner, Duration::from_secs(2)).await;

    writes
        .write(batch(6, 41, &["plant/a", "plant/b"]))
        .await
        .expect("forwarded");

    let sent = owner.sent();
    assert_eq!(sent.len(), 1);
    let (kind, publish) = &sent[0];
    assert_eq!(*kind, Kind::SequencedForwardPublish);
    assert_eq!(
        publish.producer,
        Some(ForwardProducer {
            producer_id: 41,
            sequence: 6
        })
    );
    assert_eq!(publish.credential, "token");
    assert_eq!(publish.shard.generation, GENERATION);
    assert_eq!(publish.payloads.len(), 2);
    assert!(
        publish
            .headers
            .as_ref()
            .is_some_and(|blocks| blocks.iter().all(Option::is_some))
    );
    assert!(stored(&broker).await.is_empty());
}

/// **A forward whose answer was lost is sent again under the same sequence**,
/// and the client is answered from the second send. The owner answers a
/// batch it already holds with its offsets, so this cannot write it twice.
#[tokio::test]
async fn a_forward_whose_answer_was_lost_is_sent_again_under_the_same_sequence() {
    let owner = ScriptedOwner::new([lost(), ok()]);
    let (writes, _broker, _dir) = forwarding(&owner, Duration::from_secs(2)).await;

    writes
        .write(batch(2, 41, &["plant/a"]))
        .await
        .expect("answered by the re-send");

    let sent = owner.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].1, sent[1].1, "the re-send is the same batch");
}

/// Once an answer was lost, a later refusal says nothing about the first
/// send, so the client is not told the message was refused. An owner that is
/// moving is tried again; any other refusal ends the write there.
#[tokio::test]
async fn a_refusal_after_a_lost_answer_is_an_unknown_outcome() {
    let owner = ScriptedOwner::new([lost(), refused(ErrorCode::Unauthorized)]);
    let (writes, _broker, _dir) = forwarding(&owner, Duration::from_millis(400)).await;
    let result = writes.write(batch(0, 41, &["plant/a"])).await;
    assert_eq!(
        result,
        Err(WriteError::OutcomeUnknown("forward_unanswered"))
    );
    assert_eq!(owner.sent().len(), 2);

    let moving = Err(PeerError::Unsupported {
        node_id: OWNER.to_string(),
        kind: Kind::SequencedForwardPublish,
    });
    let owner = ScriptedOwner::new([lost(), moving, ok()]);
    let (writes, _broker, _dir) = forwarding(&owner, Duration::from_secs(2)).await;
    writes
        .write(batch(0, 41, &["plant/a"]))
        .await
        .expect("answered once an owner took it");
    assert_eq!(owner.sent().len(), 3);
}

/// Sends again stop at the budget, and the outcome stays unknown.
#[tokio::test]
async fn re_sends_stop_at_the_budget() {
    let owner = ScriptedOwner::new(std::iter::repeat_with(lost).take(64));
    let (writes, _broker, _dir) = forwarding(&owner, Duration::from_millis(300)).await;

    let started = Instant::now();
    let result = writes.write(batch(0, 41, &["plant/a"])).await;
    assert_eq!(
        result,
        Err(WriteError::OutcomeUnknown("forward_unanswered"))
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(owner.sent().len() > 1);
}

/// The owner's own answers: a failed write is unknown and not sent again, a
/// refused token is `not_authorized`, and an owner that cannot take the
/// sequenced kind is `not_owner`.
#[tokio::test]
async fn the_owners_refusals_reach_the_client() {
    for (answer, expected, sends) in [
        (
            refused(ErrorCode::StorageFailed),
            WriteError::OutcomeUnknown("write_failed"),
            1,
        ),
        (
            refused(ErrorCode::Unauthorized),
            WriteError::Refused(Refusal::NotAuthorized),
            1,
        ),
        (
            Err(PeerError::Unsupported {
                node_id: OWNER.to_string(),
                kind: Kind::SequencedForwardPublish,
            }),
            WriteError::Refused(Refusal::NotOwner),
            1,
        ),
    ] {
        let owner = ScriptedOwner::new([answer]);
        let (writes, _broker, _dir) = forwarding(&owner, Duration::from_secs(2)).await;
        let result = writes.write(batch(0, 41, &["plant/a"])).await;
        assert_eq!(result, Err(expected));
        assert_eq!(owner.sent().len(), sends);
    }
}

/// Nothing to forward with: an anonymous connection has no token for the
/// owner to check, and a broker without peers has nobody to send to.
#[tokio::test]
async fn a_batch_that_cannot_be_forwarded_is_refused_as_not_owner() {
    let owner = ScriptedOwner::new([]);
    let (writes, _broker, _dir) = forwarding(&owner, Duration::from_secs(2)).await;
    let mut anonymous = batch(0, 41, &["plant/a"]);
    anonymous.credential = None;
    assert_eq!(
        writes.write(anonymous).await,
        Err(WriteError::Refused(Refusal::NotOwner))
    );
    assert!(owner.sent().is_empty());

    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    broker.record_headers_when(|| true);
    let alone = BrokerWrites::new(broker, Some(ingress(OWNER)), None, Duration::from_secs(1));
    assert_eq!(
        alone.write(batch(0, 41, &["plant/a"])).await,
        Err(WriteError::Refused(Refusal::NotOwner))
    );
}

/// **On the owner, a `Quorum` stream's batch is not acknowledged before a
/// majority holds it.** The record is durable here, but until the mark passes
/// it the client is not told; once it does, the same batch is answered.
#[tokio::test]
async fn a_quorum_batch_waits_for_the_majority_on_the_owner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with(true, ConsistencyLevel::Quorum, dir.path()).await;
    broker.record_headers_when(|| true);
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&shard(), GENERATION, 0);
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Some(ingress(LOCAL)),
        Some(Arc::clone(&marks)),
        Duration::from_millis(200),
    );

    let result = writes.write(batch(0, 41, &["plant/a"])).await;
    assert_eq!(result, Err(WriteError::OutcomeUnknown("quorum_timeout")));
    assert_eq!(stored(&broker).await.len(), 1, "durable here all the same");

    marks.publish(&shard(), GENERATION, 1_000);
    writes
        .write(batch(0, 41, &["plant/a"]))
        .await
        .expect("acknowledged once a majority holds it");
    assert_eq!(stored(&broker).await.len(), 1);
}

/// A shard this broker is named for but has not opened is not written here.
#[tokio::test]
async fn a_shard_not_open_here_is_refused_as_not_owner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    broker.record_headers_when(|| true);
    let ingress = ingress(LOCAL);
    ingress.publish_servable(HashMap::new());
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Some(ingress),
        None,
        Duration::from_secs(1),
    );

    assert_eq!(
        writes.write(batch(0, 41, &["plant/a"])).await,
        Err(WriteError::Refused(Refusal::NotOwner))
    );
    assert!(stored(&broker).await.is_empty());
}

/// An owner deposed while it waited for the majority says so, and the batch
/// is sent again like one whose answer was lost.
#[tokio::test]
async fn a_forward_whose_owner_was_deposed_is_sent_again() {
    let owner = ScriptedOwner::new([refused(ErrorCode::LeadershipLost), ok()]);
    let (writes, _broker, _dir) = forwarding(&owner, Duration::from_secs(2)).await;

    writes
        .write(batch(0, 41, &["plant/a"]))
        .await
        .expect("answered by the re-send");
    let sent = owner.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].1, sent[1].1);
}

/// **This broker deposed while it waited for the majority sends the batch
/// again to the new owner**, under the same sequence, instead of closing the
/// device's connection.
#[tokio::test]
async fn a_local_write_whose_leadership_moved_is_sent_to_the_new_owner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with(true, ConsistencyLevel::Quorum, dir.path()).await;
    broker.record_headers_when(|| true);
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&shard(), GENERATION, 0);
    let ingress = ingress(LOCAL);
    let owner = ScriptedOwner::new([ok()]);
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Some(Arc::clone(&ingress)),
        Some(Arc::clone(&marks)),
        Duration::from_secs(1),
    )
    .with_forwarding(Arc::clone(&owner), Duration::from_secs(3));

    // Moved once the batch is in the log here, so its quorum wait is what
    // learns of it.
    let moved = tokio::spawn({
        let broker = Arc::clone(&broker);
        async move {
            while stored(&broker).await.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            show(&ingress, OWNER, GENERATION + 1);
            // What the lifecycle does once this broker stops leading it.
            marks.forget(&shard());
        }
    });
    let written = writes.write(batch(0, 41, &["plant/a"])).await;
    moved.abort();
    written.expect("answered by the new owner");

    assert_eq!(stored(&broker).await.len(), 1, "written here first");
    let sent = owner.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].1.producer,
        Some(ForwardProducer {
            producer_id: 41,
            sequence: 0
        })
    );
}
