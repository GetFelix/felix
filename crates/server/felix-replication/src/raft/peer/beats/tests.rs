use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_wire::internal::{InternalMessage, RaftAnswer};
use futures::future::join_all;

use super::*;
use crate::peer::PeerError;
use crate::raft::network::Reply;

/// A heartbeat body this peer answers late.
const LATE: u8 = 255;

/// Answers each heartbeat with a read index equal to its body's first byte,
/// so a test can tell whose answer reached whom. Never answers broker
/// `slow`.
#[derive(Default)]
struct Peer {
    frames: parking_lot::Mutex<Vec<usize>>,
    /// Answer one heartbeat fewer than the frame carried.
    short: bool,
}

impl PeerRequester for Peer {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        if node_id == "slow" {
            std::future::pending::<()>().await;
        }
        let InternalMessage::RaftBeats(call) = message else {
            panic!("only heartbeats go to this peer");
        };
        let (_, beats) = wire::beats::decode(call.body).expect("a heartbeat frame");
        self.frames.lock().push(beats.len());
        let mut answers: Vec<Option<Bytes>> = beats
            .iter()
            .map(|beat| {
                (beat[0] != LATE).then(|| {
                    let index = Some(u64::from(beat[0]));
                    wire::encode_reply(&Ok(RaftReply(Reply::ReadIndex { index })))
                })
            })
            .collect();
        if self.short {
            answers.pop();
        }
        Ok(InternalMessage::RaftBeatsAnswer(RaftAnswer {
            correlation_id: 0,
            body: wire::beats::encode_answers(&answers),
        }))
    }
}

fn addr() -> SocketAddr {
    "127.0.0.1:1".parse().expect("addr")
}

async fn beat(peer: &Arc<Peer>, beats: &Arc<Beats>, broker: &str, body: u8) -> Answer {
    let deadline = Duration::from_secs(5);
    send(
        peer,
        beats,
        broker.into(),
        addr(),
        deadline,
        Bytes::from(vec![body]),
    )
    .await
}

fn index(answer: Answer) -> Option<u64> {
    match answer {
        Some(Ok(RaftReply(Reply::ReadIndex { index }))) => index,
        Some(Ok(_)) => panic!("another kind of answer"),
        Some(Err(refused)) => panic!("refused: {refused}"),
        None => panic!("late"),
    }
}

#[tokio::test]
async fn each_caller_gets_its_own_answer_from_one_frame() {
    let peer = Arc::new(Peer::default());
    let beats = Arc::new(Beats::default());
    let answers = join_all((0..50).map(|n| beat(&peer, &beats, "b", n))).await;
    for (n, answer) in answers.into_iter().enumerate() {
        assert_eq!(index(answer), Some(n as u64));
    }
    assert_eq!(*peer.frames.lock(), [50]);
    assert_eq!(beats.sent().frames.load(Ordering::Relaxed), 1);
    assert_eq!(beats.sent().beats.load(Ordering::Relaxed), 50);
}

#[tokio::test]
async fn a_frame_carries_at_most_max_beats() {
    let peer = Arc::new(Peer::default());
    let beats = Arc::new(Beats::default());
    let body = |n: usize| (n % 200) as u8;
    let answers = join_all((0..MAX_BEATS + 10).map(|n| beat(&peer, &beats, "b", body(n)))).await;
    for (n, answer) in answers.into_iter().enumerate() {
        assert_eq!(index(answer), Some(u64::from(body(n))));
    }
    assert_eq!(*peer.frames.lock(), [MAX_BEATS, 10]);
}

#[tokio::test]
async fn a_late_heartbeat_is_sent_again_on_its_own_and_the_rest_are_answered() {
    let peer = Arc::new(Peer::default());
    let beats = Arc::new(Beats::default());
    let answers = join_all([1, LATE, 3].map(|n| beat(&peer, &beats, "b", n))).await;
    let mut answers = answers.into_iter();
    assert_eq!(index(answers.next().expect("first")), Some(1));
    assert!(answers.next().expect("second").is_none(), "late");
    assert_eq!(index(answers.next().expect("third")), Some(3));
}

#[tokio::test]
async fn answers_that_do_not_match_fail_every_heartbeat_in_the_frame() {
    let peer = Arc::new(Peer {
        short: true,
        ..Peer::default()
    });
    let beats = Arc::new(Beats::default());
    for answer in join_all((0..3).map(|n| beat(&peer, &beats, "b", n))).await {
        assert!(
            matches!(answer, Some(Err(Refused::Unreachable(_)))),
            "no answer may reach another heartbeat's caller"
        );
    }
}

#[tokio::test]
async fn a_broker_that_does_not_answer_holds_up_only_its_own_heartbeats() {
    let peer = Arc::new(Peer::default());
    let beats = Arc::new(Beats::default());
    let stuck = tokio::spawn({
        let (peer, beats) = (Arc::clone(&peer), Arc::clone(&beats));
        async move { beat(&peer, &beats, "slow", 1).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let answered = tokio::time::timeout(Duration::from_secs(1), beat(&peer, &beats, "b", 2)).await;
    assert_eq!(
        index(answered.expect("the other broker is answered")),
        Some(2)
    );
    // The next heartbeat to the stuck broker waits behind its frame.
    let behind =
        tokio::time::timeout(Duration::from_millis(50), beat(&peer, &beats, "slow", 3)).await;
    assert!(behind.is_err());
    assert!(!stuck.is_finished());
    stuck.abort();
}
