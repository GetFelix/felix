use super::*;

fn entry(term: u64, value: u64) -> Held {
    Held {
        generation: Some(term),
        mark: RecordMark::None,
        payload: Bytes::from(value.to_le_bytes().to_vec()),
    }
}

fn flushed(term: u64, index: u64) -> Option<LogPoint> {
    Some(LogPoint { term, index })
}

/// b1 led term 1 and, cut off, flushed its own tail to 120. b2 leads term 2
/// and acknowledges offset 100 with only itself flushed. b1's index is past
/// 100, but its record there is not b2's, so only one member holds it.
#[test]
fn a_deposed_leaders_flushed_tail_does_not_hold_the_new_record() {
    let acknowledged = entry(2, 7);
    let members = [
        (flushed(1, 120), Some(entry(1, 3))),
        (flushed(2, 100), Some(acknowledged.clone())),
        (None, None),
    ];
    assert_eq!(holders(&acknowledged, 100, &members), 1);
}

#[test]
fn a_member_holds_a_record_once_it_has_the_same_one_flushed() {
    let record = entry(2, 7);
    let members = [
        (flushed(2, 100), Some(record.clone())),
        (flushed(2, 104), Some(record.clone())),
        // Written but not yet flushed.
        (flushed(2, 99), Some(record.clone())),
    ];
    assert_eq!(holders(&record, 100, &members), 2);
}

/// A wiped copy held offset 50 as term 1's value 3. If two members later
/// disagree at 50, the credit backs only the record the copy held.
#[test]
fn a_replaced_copy_is_credited_only_with_the_record_it_held() {
    let held = entry(1, 3);
    let credit = Credit {
        bound: 100,
        records: BTreeMap::from([(50, held.clone())]),
    };
    assert!(credited(Some(&credit), 50, &held));
    assert!(!credited(Some(&credit), 50, &entry(2, 9)));
    assert!(!credited(Some(&credit), 60, &held));
    assert!(!credited(None, 50, &held));
}

/// What a stopping copy of five is credited with, and whether that makes
/// `acked` at offset 100 look held by a majority. b4 and b5 flushed it; b1
/// and b2 are behind; b3 stopped with 100 in its log.
fn held_with_stopped_b3(reported: Option<LogPoint>, durable: u64, acked: &Held) -> usize {
    let log: BTreeMap<u64, Held> = (95..=100)
        .map(|offset| {
            (
                offset,
                if offset == 100 {
                    acked.clone()
                } else {
                    entry(4, offset)
                },
            )
        })
        .collect();
    let bound = flushed_bound(reported, durable);
    let credit = Credit {
        bound,
        records: log.range(..bound).map(|(o, h)| (*o, h.clone())).collect(),
    };
    let b3 = if credited(Some(&credit), 100, acked) {
        (flushed(0, 100), Some(acked.clone()))
    } else {
        (None, None)
    };
    let members = [
        (flushed(4, 99), None),
        (flushed(4, 99), None),
        b3,
        (flushed(4, 100), Some(acked.clone())),
        (flushed(4, 100), Some(acked.clone())),
    ];
    holders(acked, 100, &members)
}

/// A follower flushes and acks before it learns the commit, so a copy
/// stopped right after counts for 100 even with its read bound at 100.
#[test]
fn a_stopped_copy_is_credited_with_what_it_flushed() {
    let acked = entry(4, 7);
    assert_eq!(held_with_stopped_b3(flushed(4, 100), 101, &acked), 3);
}

/// Only two members ever flushed 100. b3 had it in its log but not flushed
/// (reported 99), or had it rewritten after reporting (durable bound 100):
/// either way the credit must leave the ack under-replicated.
#[test]
fn a_stopped_copy_is_not_credited_past_what_it_flushed() {
    let acked = entry(4, 7);
    assert_eq!(held_with_stopped_b3(flushed(4, 99), 101, &acked), 2);
    assert_eq!(held_with_stopped_b3(flushed(4, 100), 100, &acked), 2);
    assert_eq!(held_with_stopped_b3(None, 101, &acked), 2);
}

/// A follower flushes a record before it learns the record is committed.
/// Stopped in that window, its copy must be credited with the record, or
/// the ack and read-bound checks count one holder too few.
#[test]
fn a_follower_stopped_before_it_learns_a_commit_is_credited_with_it() {
    let seed = 1;
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all().start_paused(true);
    let _draws = felix_replication::raft::seed_timers(seed);
    felix_common::jitter::sim::seed(seed);
    let runtime = builder.build().expect("build the simulation runtime");
    runtime.block_on(async {
        crate::clock::install(crate::clock::Clocks::default());
        let config = RaftSimConfig {
            disks: Disks::Wipe,
            ..RaftSimConfig::default()
        };
        let mut rng = Rng::new(seed);
        let sim = Sim::start(&config, &mut rng)
            .await
            .expect("start the group");
        let stop = CancellationToken::new();
        let control_plane = tokio::spawn(sim.clone().control_plane(stop.clone()));
        let writer = tokio::spawn(sim.clone().writer(0, rng.fork(), stop.clone()));
        let shard = key(0);
        let deadline = Instant::now() + Duration::from_secs(30);
        let (follower, last, record) = loop {
            assert!(
                Instant::now() < deadline,
                "no follower was ever behind on a commit it had flushed"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
            let Some(leader) = sim.leader(0) else {
                continue;
            };
            let Some(last) = sim
                .at(leader)
                .engine
                .read_bound(&shard)
                .ok()
                .and_then(|bound| bound.checked_sub(1))
                // Past the group's first entries, among the client writes.
                .filter(|last| *last >= 50)
            else {
                continue;
            };
            let Some(record) = record_in(&sim.at(leader), 0, last).await else {
                continue;
            };
            // No await between this and the stop, so the follower is still
            // behind when `replace` samples it.
            let behind = (0..sim.members.len()).find(|&member| {
                let at = sim.at(member);
                let flushed = at.runtime.status(&shard).and_then(|s| s.flushed);
                member != leader
                    && flushed.is_some_and(|point| point.index >= last)
                    && at
                        .engine
                        .read_bound(&shard)
                        .is_ok_and(|bound| bound <= last)
            });
            if let Some(follower) = behind {
                break (follower, last, record);
            }
        };
        sim.replace(&[follower], Scope::Shard(0), Replace::Wipe, &mut rng)
            .await;
        stop.cancel();
        let _ = writer.await;
        let _ = control_plane.await;
        assert!(
            credited(sim.credit.lock().get(&(follower, 0)), last, &record),
            "{} flushed committed offset {last} but was not credited with it",
            name(follower)
        );
    });
}
