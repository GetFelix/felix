//! The limit of the bound on elections.

use super::*;

fn elections(most: usize) -> Elections {
    let timers = Timers {
        started: Instant::now(),
        tick: 50,
        heartbeat: 200,
        idle_heartbeat: 2000,
        idle_after: 1000,
        election_min: 1000,
        election_max: 2000,
        lease: 1500,
        elections: most,
    };
    Elections::new(&timers, Arc::default())
}

/// The limit starts at the bound and falls only when a vote save here was
/// slow: to what the disk would have saved in time.
#[test]
fn a_slow_vote_save_lowers_the_limit() {
    let mut elections = elections(64);
    elections.settle(1000);
    assert_eq!(elections.limit, 64);
    elections.saves.saved(2000, 250);
    elections.settle(2000);
    assert_eq!(elections.limit, 64, "250 ms is not slow");
    elections.saves.saved(3000, 1000);
    elections.settle(3000);
    assert_eq!(elections.limit, 16);
    // Queued before the limit fell: it says nothing about the new one,
    elections.saves.saved(3500, 900);
    elections.settle(3500);
    assert_eq!(elections.limit, 16);
    // unless it was slower than the save the limit fell for.
    elections.saves.saved(3600, 2000);
    elections.settle(3600);
    assert_eq!(elections.limit, 8);
    elections.saves.saved(60_000, 50_000);
    elections.settle(60_000);
    assert_eq!(elections.limit, 1);
}
