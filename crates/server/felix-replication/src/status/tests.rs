use super::*;
use crate::{Halt, ShardKind};

fn cursor(next_offset: u64) -> FollowerCursor {
    let mut cursor =
        FollowerCursor::new("broker-b", "127.0.0.1:7000".parse().unwrap(), next_offset);
    cursor.stalled = false;
    cursor
}

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

#[test]
fn a_follower_reads_as_the_worst_thing_true_of_it() {
    assert_eq!(
        follower_status(&cursor(90), Some(100), false).state,
        "shipping"
    );
    assert_eq!(follower_status(&cursor(90), Some(100), false).lag, Some(10));

    let mut stalled = cursor(90);
    stalled.stalled = true;
    assert_eq!(follower_status(&stalled, Some(100), false).state, "stalled");
    assert_eq!(follower_status(&stalled, Some(100), true).state, "copying");

    let mut rebuilding = stalled.clone();
    rebuilding.rebuilding = true;
    assert_eq!(follower_status(&rebuilding, None, true).state, "rebuilding");
    assert_eq!(follower_status(&rebuilding, None, true).lag, None);

    let mut halted = rebuilding.clone();
    halted.halted = Some(Halt::Diverged);
    let status = follower_status(&halted, Some(100), false);
    assert_eq!(status.state, "halted");
    assert_eq!(status.halted, Some("diverged"));
}

#[test]
fn the_board_keeps_only_what_is_still_led() {
    let board = ShardStatusBoard::new();
    let status = ShardStatus {
        generation: 3,
        tail: Some(10),
        followers: vec![],
        fence: None,
        drain_pending: false,
        behind: false,
    };
    board.put(key(0), status.clone());
    board.put(key(1), status.clone());
    assert_eq!(board.get(&key(0)), Some(status.clone()));

    board.retain(|key| key.shard == 1);
    assert_eq!(board.get(&key(0)), None);
    assert_eq!(board.get(&key(1)), Some(status));
}
