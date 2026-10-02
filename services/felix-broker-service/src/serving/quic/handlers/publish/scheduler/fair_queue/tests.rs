use super::*;

const QUANTUM: usize = 100;

fn queue(capacity: usize, share: usize) -> FairQueue<&'static str, u32> {
    FairQueue::new(capacity, share, QUANTUM)
}

/// Pop everything, completing each lane straight away, and say whose job
/// each was.
fn drain(queue: &mut FairQueue<&'static str, u32>) -> Vec<(&'static str, u32)> {
    let mut order = Vec::new();
    while let Some((lane, item)) = queue.pop() {
        queue.complete(&lane);
        order.push((lane, item));
    }
    order
}

#[test]
fn two_backlogged_tenants_take_turns() {
    let mut queue = queue(64, 32);
    for n in 0..4 {
        queue.push("a", "a/0", n, QUANTUM).expect("room");
        queue.push("b", "b/0", n, QUANTUM).expect("room");
    }
    let lanes: Vec<_> = drain(&mut queue)
        .into_iter()
        .map(|(lane, _)| lane)
        .collect();
    assert_eq!(
        lanes,
        ["a/0", "b/0", "a/0", "b/0", "a/0", "b/0", "a/0", "b/0"]
    );
}

#[test]
fn turns_are_weighted_by_cost_not_by_job() {
    let mut queue = queue(64, 32);
    // Tenant a sends batches four times the size of b's.
    for n in 0..4 {
        queue.push("a", "a/0", n, 4 * QUANTUM).expect("room");
    }
    for n in 0..8 {
        queue.push("b", "b/0", n, QUANTUM).expect("room");
    }
    let first_six: Vec<_> = drain(&mut queue)
        .into_iter()
        .take(6)
        .map(|(lane, _)| lane)
        .collect();
    // b runs a job per quantum; a has to save up four quanta for each of its.
    assert_eq!(
        first_six.iter().filter(|lane| **lane == "b/0").count(),
        5,
        "one large batch should buy about as many turns as four small ones"
    );
}

#[test]
fn a_flooding_tenant_does_not_starve_a_quiet_one() {
    let mut queue = queue(8, 2);
    let mut accepted = 0;
    for n in 0..100 {
        if queue.push("flood", "flood/0", n, QUANTUM).is_ok() {
            accepted += 1;
        }
    }
    // The flooder borrows idle room but never the last `share` slots.
    assert_eq!(accepted, 6);
    assert!(
        queue.push("flood", "flood/0", 100, QUANTUM).is_err(),
        "the flooder is refused once only the reserve is left"
    );
    queue
        .push("quiet", "quiet/0", 0, QUANTUM)
        .expect("a quiet tenant still gets in");

    // And it runs within a round rather than behind the whole backlog.
    let order: Vec<_> = drain(&mut queue)
        .into_iter()
        .map(|(lane, _)| lane)
        .collect();
    let position = order
        .iter()
        .position(|lane| *lane == "quiet/0")
        .expect("the quiet tenant ran");
    assert!(position <= 1, "the quiet tenant waited behind the flood");
}

#[test]
fn a_full_queue_hands_the_job_back() {
    let mut queue = queue(2, 2);
    queue.push("a", "a/0", 1, 1).expect("room");
    queue.push("b", "b/0", 2, 1).expect("room");
    assert_eq!(queue.push("c", "c/0", 3, 1), Err(3));
    assert_eq!(queue.len(), 2);

    // Room comes back once a job leaves the queue, not once it finishes.
    let (lane, _) = queue.pop().expect("a job");
    queue.push("c", "c/0", 3, 1).expect("room after a pop");
    queue.complete(&lane);
}

#[test]
fn a_lane_runs_one_job_at_a_time_in_order() {
    let mut queue = queue(16, 16);
    for n in 0..3 {
        queue.push("a", "a/0", n, 1).expect("room");
    }
    let (lane, first) = queue.pop().expect("the head");
    assert_eq!(first, 0);
    assert!(
        queue.pop().is_none(),
        "a lane's second job ran before its first finished"
    );
    queue.complete(&lane);
    assert_eq!(queue.pop().map(|(_, item)| item), Some(1));
}

#[test]
fn a_busy_lane_does_not_hold_up_its_tenants_other_lanes() {
    let mut queue = queue(16, 16);
    queue.push("a", "a/0", 0, 1).expect("room");
    queue.push("a", "a/0", 1, 1).expect("room");
    queue.push("a", "a/1", 2, 1).expect("room");
    let (busy, _) = queue.pop().expect("a/0's head");
    assert_eq!(busy, "a/0");
    // a/0 is still running; a/1 goes ahead of a/0's second job.
    assert_eq!(queue.pop(), Some(("a/1", 2)));
    assert_eq!(queue.queued_for("a"), 1);
    queue.complete(&"a/1");
    queue.complete(&busy);
    assert_eq!(queue.pop(), Some(("a/0", 1)));
}

#[test]
fn an_idle_tenant_does_not_bank_turns() {
    let mut queue = queue(64, 64);
    // a runs once and goes idle with most of a quantum unspent.
    queue.push("a", "a/0", 0, 1).expect("room");
    let (lane, _) = queue.pop().expect("a's job");
    queue.complete(&lane);
    assert!(queue.is_empty());

    for n in 0..3 {
        queue.push("b", "b/0", n, QUANTUM).expect("room");
    }
    for n in 0..3 {
        queue.push("a", "a/0", n, QUANTUM).expect("room");
    }
    let order: Vec<_> = drain(&mut queue)
        .into_iter()
        .map(|(lane, _)| lane)
        .collect();
    assert_eq!(order, ["b/0", "a/0", "b/0", "a/0", "b/0", "a/0"]);
}

#[test]
fn a_running_lane_hands_out_its_queued_jobs_in_order() {
    let mut queue = queue(64, 32);
    for n in 0..5 {
        queue.push("a", "a/0", n, 10).expect("room");
    }
    let (lane, first) = queue.pop().expect("a job");
    assert_eq!(first, 0);
    let more = queue.take_more(&lane, 3, |_, _| true);
    assert_eq!(more, [1, 2, 3], "at most `max`, from the front");
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.queued_for("a"), 1);
    // The lane is still running, so its last job waits for it.
    assert!(queue.pop().is_none());
    queue.complete(&lane);
    assert_eq!(drain(&mut queue), [("a/0", 4)]);
    assert!(queue.is_empty());
}

#[test]
fn taking_more_stops_at_the_first_job_refused() {
    let mut queue = queue(64, 32);
    for n in 0..5 {
        queue.push("a", "a/0", n, 10).expect("room");
    }
    let (lane, _) = queue.pop().expect("a job");
    // Job 2 cannot join, so job 3 must not jump it.
    let more = queue.take_more(&lane, 8, |job, _| *job != 2);
    assert_eq!(more, [1]);
    queue.complete(&lane);
    assert_eq!(drain(&mut queue), [("a/0", 2), ("a/0", 3), ("a/0", 4)]);
}

#[test]
fn taking_a_lanes_last_job_leaves_nothing_behind() {
    let mut queue = queue(64, 32);
    queue.push("a", "a/0", 0, 10).expect("room");
    queue.push("a", "a/0", 1, 10).expect("room");
    let (lane, _) = queue.pop().expect("a job");
    assert_eq!(queue.take_more(&lane, 8, |_, _| true), [1]);
    assert_eq!(queue.queued_for("a"), 0);
    queue.complete(&lane);
    assert!(queue.is_empty());
    assert!(queue.pop().is_none());
}

/// A tenant pays for what it takes, so taking a lane's backlog in one go
/// buys it no more than popping it one job at a time would have.
#[test]
fn jobs_taken_together_are_paid_for_in_later_turns() {
    let mut queue = queue(64, 32);
    for n in 0..8 {
        queue.push("a", "a/0", n, QUANTUM).expect("room");
    }
    for n in 0..8 {
        queue.push("b", "b/0", n, QUANTUM).expect("room");
    }
    let (lane, _) = queue.pop().expect("a's turn");
    assert_eq!(lane, "a/0");
    // Three more quanta than a's turn earned.
    assert_eq!(queue.take_more(&lane, 3, |_, _| true).len(), 3);
    queue.complete(&lane);
    let lanes: Vec<_> = drain(&mut queue)
        .into_iter()
        .take(4)
        .map(|(lane, _)| lane)
        .collect();
    assert_eq!(
        lanes,
        ["b/0", "b/0", "b/0", "b/0"],
        "a took four turns' worth at once and should sit out until b caught up"
    );
}
