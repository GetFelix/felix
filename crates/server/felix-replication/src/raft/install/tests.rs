use openraft::type_config::alias::LeaderIdOf;
use openraft::vote::{RaftLeaderId, RaftVote};

use super::*;
use crate::raft::MemberId;

fn vote() -> VoteOf<ShardTypes> {
    let leads = LeaderIdOf::<ShardTypes>::new(1, MemberId::new("a", 1));
    VoteOf::<ShardTypes>::from_leader_id(leads, true)
}

/// A piece's end is the sender's offset plus its length, and the sender
/// can say any offset. One past `u64::MAX` is refused, not wrapped.
#[test]
fn a_piece_whose_end_overflows_is_refused() {
    let incoming = Incoming::default();
    let meta = SnapshotMetaOf::<ShardTypes>::default();
    let taken = incoming.take(&vote(), &meta, 16, u64::MAX, b"x");
    assert!(taken.is_err());
    assert_eq!(incoming.held(), None);
}

/// One task drops an idle snapshot, however many first pieces arrive while
/// it runs. Once it has ended, the next first piece starts another.
#[tokio::test]
async fn one_task_watches_for_an_idle_sender() {
    let incoming = Incoming::default();
    let meta = SnapshotMetaOf::<ShardTypes>::default();
    assert!(matches!(
        incoming.take(&vote(), &meta, 4, 0, b"ab"),
        Ok(Piece::More)
    ));
    assert!(incoming.watch());
    // The sender starts again.
    assert!(matches!(
        incoming.take(&vote(), &meta, 4, 0, b"ab"),
        Ok(Piece::More)
    ));
    assert!(!incoming.watch());
    assert!(!incoming.settled());
    assert!(!incoming.watch());
    assert!(matches!(
        incoming.take(&vote(), &meta, 4, 2, b"cd"),
        Ok(Piece::Whole(_))
    ));
    assert!(incoming.settled());
    assert!(incoming.watch());
}

/// A cache group filled to its bound builds a snapshot the cap takes,
/// sent in pieces, and the member behind the purge installs the same state.
#[test]
fn a_state_at_its_bound_snapshots_and_installs() {
    use bytes::Bytes;

    use crate::raft::cache::{Answer, Bound, CacheCommand, CacheState, Condition, Request};
    const CAP: u64 = 4096;
    let mut state = CacheState::default().bounded(Bound::under(CAP));
    let mut index = 0;
    while index < 200 {
        index += 1;
        let put = CacheCommand::Put {
            key: format!("key-{index}"),
            value: Bytes::from(vec![7; 100]),
            expires_at: 0,
            cond: Condition::None,
            request: Request {
                producer: 1,
                sequence: index,
                floor: index,
            },
        };
        if state.apply(index, 0, &put) == Some(Answer::Full) {
            break;
        }
    }
    assert!((10..200).contains(&index), "filled after {index} puts");
    let data = state.snapshot();
    assert!(data.len() as u64 <= CAP, "{} bytes", data.len());

    let incoming = Incoming::capped(CAP);
    let meta = SnapshotMetaOf::<ShardTypes>::default();
    let len = data.len() as u64;
    let mut whole = None;
    for (n, piece) in data.chunks(1000).enumerate() {
        match incoming.take(&vote(), &meta, len, n as u64 * 1000, piece) {
            Ok(Piece::More) => {}
            Ok(Piece::Whole(bytes)) => whole = Some(bytes),
            Err(why) => panic!("refused: {why}"),
        }
    }
    let installed = CacheState::restore(&Bytes::from(whole.expect("whole")), true).unwrap();
    assert!(installed == state);
}
