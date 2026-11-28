//! An open held on the open-file limit keeps retrying.
use std::time::Duration;

use super::*;

fn open_action(generation: u64) -> Action {
    Action::Open {
        key: key(0),
        generation,
        new_term: true,
        fence: false,
        incarnation: 0,
    }
}

/// **A hold past the report bound keeps the shard opening**, not failed.
#[tokio::test(start_paused = true)]
async fn a_hold_past_the_bound_keeps_retrying() {
    let mut own = lifecycle();
    own.report_held_after(Duration::from_secs(10));
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));

    assert!(
        own.open_held(open_action(1), "emfile".into()),
        "first pass logs"
    );
    tokio::time::advance(Duration::from_secs(11)).await;
    own.open_held(open_action(1), "emfile".into());
    assert_eq!(own.phase(&key(0)), Phase::Opening, "still retrying");
    assert_eq!(own.ready_to_open(|_| true).len(), 1, "open goes again");
}

/// The warning is not logged on every pass.
#[tokio::test(start_paused = true)]
async fn the_held_warning_is_rate_limited() {
    let mut own = lifecycle();
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    assert!(own.open_held(open_action(1), "emfile".into()));
    assert!(!own.open_held(open_action(1), "emfile".into()));
    tokio::time::advance(HELD_WARN_EVERY).await;
    assert!(own.open_held(open_action(1), "emfile".into()));
}
