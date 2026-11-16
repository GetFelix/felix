//! An open held on the open-file limit keeps retrying, and tells the control
//! plane once it has waited too long.
use std::time::Duration;

use felix_replication::driver::WriteFence;

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

/// **A hold past the bound is reported once, and the shard keeps opening.**
/// Without the report a leader whose limit never frees sits in `Opening` with
/// the control plane none the wiser.
#[tokio::test(start_paused = true)]
async fn a_hold_past_the_bound_is_reported_once() {
    let mut own = lifecycle();
    own.report_held_after(Duration::from_secs(10));
    own.observe(&key(0), Some(&assigned_to("broker-a", 1)));
    let fence = Arc::clone(own.fence());

    assert!(
        own.open_held(open_action(1), "emfile".into()),
        "first pass logs"
    );
    assert_eq!(fence.refused_at(&key(0)), None, "not yet");

    tokio::time::advance(Duration::from_secs(5)).await;
    own.open_held(open_action(1), "emfile".into());
    assert_eq!(fence.refused_at(&key(0)), None, "still inside the bound");

    tokio::time::advance(Duration::from_secs(6)).await;
    own.open_held(open_action(1), "emfile".into());
    let refused = fence.refused_at(&key(0)).expect("reported");
    assert_eq!((refused.generation, refused.promised_to.as_str()), (1, ""));
    assert_eq!(own.phase(&key(0)), Phase::Opening, "still retrying");

    // Once, not per pass: a marker left in its place survives the next one.
    fence.refused(&key(0), 1, "marker");
    tokio::time::advance(Duration::from_secs(1)).await;
    own.open_held(open_action(1), "emfile".into());
    assert_eq!(
        fence.refused_at(&key(0)).expect("held").promised_to,
        "marker"
    );
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
