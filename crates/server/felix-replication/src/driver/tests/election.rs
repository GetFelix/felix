//! The leader's side of electing without the control plane: stamping
//! batches with its set, keeping that set, and rule 5, counting nothing more
//! once it has answered a newer fence.

use felix_wire::internal::PeerCapabilities;

use super::quorum::{follower_ack_marks, quorum_leader};
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

async fn pass(followers: &Followers, broker: &Arc<Broker>, marks: &QuorumMarks) {
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(
        followers,
        broker,
        &router,
        marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
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
