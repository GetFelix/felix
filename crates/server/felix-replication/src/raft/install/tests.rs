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
