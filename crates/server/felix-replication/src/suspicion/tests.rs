use std::collections::HashMap as Map;

use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardKey, ShardKind};
use felix_wire::internal::Pong;

use super::*;
use crate::peer::PeerError;

const WINDOW: Duration = Duration::from_secs(5);

fn set(nodes: &[&str]) -> HashSet<String> {
    nodes.iter().map(|node| node.to_string()).collect()
}

fn names(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|node| node.to_string()).collect()
}

/// **A leader silent for the whole window is named, and dropped from the
/// names the moment it answers again.**
#[test]
fn a_leader_silent_for_the_window_is_suspected_until_it_answers() {
    let start = Instant::now();
    let mut silence = Silence::new(WINDOW);
    let watched = set(&["broker-b", "broker-c"]);

    assert!(silence.round(&watched, &watched, start).is_empty());
    let quiet = set(&["broker-c"]);
    assert!(
        silence
            .round(&watched, &quiet, start + WINDOW - Duration::from_millis(1))
            .is_empty(),
        "named before its window ran out"
    );
    assert_eq!(
        silence.round(&watched, &quiet, start + WINDOW),
        names(&["broker-b"])
    );
    assert!(
        silence
            .round(&watched, &watched, start + WINDOW + Duration::from_secs(1))
            .is_empty()
    );
}

/// **A leader first watched gets a whole window.** A broker that has just
/// started following a shard has not been ignored by its leader for any time
/// yet, however long that leader has been gone.
#[test]
fn a_newly_watched_leader_gets_a_whole_window() {
    let start = Instant::now();
    let mut silence = Silence::new(WINDOW);
    let none = HashSet::new();
    assert!(silence.round(&set(&["broker-b"]), &none, start).is_empty());
    let later = start + WINDOW * 3;
    assert_eq!(
        silence.round(&set(&["broker-b", "broker-c"]), &none, later),
        names(&["broker-b"]),
        "broker-c was first watched at {later:?} and is not suspect yet"
    );
}

/// **A leader no longer followed is forgotten.** If this broker follows it
/// again later, it starts a fresh window rather than being named at once.
#[test]
fn a_leader_no_longer_followed_is_forgotten() {
    let start = Instant::now();
    let mut silence = Silence::new(WINDOW);
    let none = HashSet::new();
    silence.round(&set(&["broker-b"]), &none, start);
    assert!(
        silence
            .round(&HashSet::new(), &none, start + WINDOW)
            .is_empty()
    );
    assert!(
        silence
            .round(&set(&["broker-b"]), &none, start + WINDOW * 2)
            .is_empty()
    );
}

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

/// **Only the leaders of shards this broker follows are watched, each once.**
#[test]
fn only_the_leaders_of_followed_shards_are_watched() {
    let nodes: Map<String, NodeRef> = ["broker-a", "broker-b", "broker-c", "broker-d"]
        .into_iter()
        .enumerate()
        .map(|(i, id)| (id.to_string(), node(id, 7001 + i as u16)))
        .collect();
    let router = ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    );
    let placed = |shard, leader: &str, replicas: &[&str]| {
        (
            key(shard),
            leader.to_string(),
            replicas.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            1,
        )
    };
    let table = RoutingTable::build(
        [
            placed(0, "broker-b", &["broker-a", "broker-c"]),
            placed(1, "broker-b", &["broker-a", "broker-c"]),
            // Led here: nobody to watch.
            placed(2, "broker-a", &["broker-b", "broker-c"]),
            // Not followed here.
            placed(3, "broker-d", &["broker-b", "broker-c"]),
            placed(4, "broker-c", &["broker-a", "broker-d"]),
        ],
        &nodes,
    );
    router.publish(table, &nodes);

    let mut watched: Vec<String> = leaders_followed(&router)
        .into_iter()
        .map(|(node, _)| node)
        .collect();
    watched.sort();
    assert_eq!(
        watched,
        vec!["broker-b".to_string(), "broker-c".to_string()]
    );
}

/// A peer that offers `offered` and answers a ping with `answer`.
struct Peer {
    offered: Result<PeerCapabilities, ()>,
    answer: Option<InternalMessage>,
}

impl PeerRequester for Peer {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        assert!(matches!(message, InternalMessage::Ping(_)));
        match &self.answer {
            Some(answer) => Ok(answer.clone()),
            None => std::future::pending().await,
        }
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> Result<PeerCapabilities, PeerError> {
        self.offered.map_err(|()| PeerError::ShuttingDown)
    }
}

fn addr() -> SocketAddr {
    "10.0.0.1:7001".parse().expect("addr")
}

/// **What counts as an answer.** A pong is; a peer that never said it answers
/// pings is not watched at all; one that cannot be reached, or does not answer
/// in time, is silent.
#[tokio::test]
async fn a_ping_is_answered_only_by_a_pong_in_time() {
    let within = Duration::from_millis(50);
    let pong = Some(InternalMessage::Pong(Pong { correlation_id: 0 }));
    let cases = [
        (Ok(PeerCapabilities::PING), pong.clone(), Answer::Pong),
        (Ok(PeerCapabilities::FENCE), pong, Answer::Unsupported),
        (Err(()), None, Answer::Silent),
        (Ok(PeerCapabilities::PING), None, Answer::Silent),
    ];
    for (offered, answer, expected) in cases {
        let peer = Peer { offered, answer };
        assert_eq!(ping(&peer, "broker-b", addr(), within).await, expected);
    }
}
