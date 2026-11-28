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
        crate::clock::install();
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

/// A relaxed cut of a majority closes the shards, so it is the last cut of
/// the run, and before it come single cuts. A strict run cuts together once
/// or twice and goes on cutting.
#[test]
fn the_cuts_of_a_majority_run_come_as_drawn() {
    let length = Duration::from_secs(60);
    for count in [3, 5] {
        for seed in 1..20 {
            let mut rng = Rng::new(seed);
            let end = Instant::now();
            let mut cuts = Cuts::new(Disks::MajorityCutRelaxed, &mut rng, end, length);
            let together = cuts.next(count, false, &mut rng).expect("a cut is due");
            assert!(together.len() >= 2 && together.len() <= count / 2 + 1);
            let distinct: BTreeSet<_> = together.iter().collect();
            assert_eq!(distinct.len(), together.len(), "{together:?}");
            let closes = together.len() > count / 2;
            assert_eq!(cuts.next(count, true, &mut rng).is_none(), closes);

            let mut cuts = Cuts::new(Disks::MajorityCut, &mut rng, end, length);
            let mut together = 0;
            for _ in 0..4 {
                let cut = cuts.next(count, true, &mut rng).expect("due");
                together += usize::from(cut.len() > 1);
            }
            assert!((1..=2).contains(&together), "{together} cuts together");
            assert_eq!(cuts.next(count, true, &mut rng).map(|c| c.len()), Some(1));
        }
    }
    let mut rng = Rng::new(1);
    let mut off = Cuts::new(Disks::Wipe, &mut rng, Instant::now(), length);
    assert!(off.next(3, true, &mut rng).is_none());
}

/// Each power mode cuts and checks what came back: a strict cut keeps every
/// copy and recovery repairs each torn tail it reopens, a relaxed one
/// rotates every copy of the member, and a majority mode cuts several
/// members at once.
#[test]
fn every_power_mode_cuts_and_passes() {
    for disks in [
        Disks::PowerLoss,
        Disks::RelaxedPowerLoss,
        Disks::MajorityCut,
        Disks::MajorityCutRelaxed,
    ] {
        let config = RaftSimConfig {
            disks,
            ..RaftSimConfig::default()
        };
        let shards = config.shards as usize;
        let outcome = run_seed(1, config);
        println!("{outcome}");
        assert!(outcome.passed(), "{disks:?}: {}", outcome.first_failure());
        let power = outcome.power;
        assert!(power.cuts > 0, "{disks:?} cut nothing");
        assert_eq!(power.together > 0, disks.together(), "{disks:?}: {power:?}");
        if disks.relaxed() {
            assert_eq!(power.rotated, power.cuts * shards, "{disks:?}: {power:?}");
        } else {
            assert_eq!(power.rotated, 0, "{disks:?}: {power:?}");
            assert_eq!(power.repaired, power.torn, "{disks:?}: {power:?}");
        }
    }
}

/// A run on real threads is not the seed's run: the blocking pool finishes
/// in real time, so a slow build (coverage) interleaves differently from a
/// fast one (#3126). A relaxed power run restarts members, opens groups,
/// reserves log indexes and vouches for unsynced writes, all of which once
/// went to the pool.
#[test]
fn a_run_hands_no_work_to_real_threads() {
    let config = RaftSimConfig {
        disks: Disks::RelaxedPowerLoss,
        nemesis: Duration::from_secs(40),
        ..RaftSimConfig::default()
    };
    let outcome = run_seed(1, config);
    assert!(outcome.power.cuts > 0, "no member started again");
    let off: Vec<_> = outcome
        .findings
        .iter()
        .filter(|finding| finding.contains("real thread"))
        .collect();
    assert!(off.is_empty(), "{off:?}");
}

#[test]
fn work_on_real_threads_is_a_finding() {
    assert_eq!(off_the_runtime(0), None);
    let finding = off_the_runtime(2).expect("a finding");
    assert!(finding.contains("2 real thread(s)"), "{finding}");
}

/// Logs whose flushes keep nothing answer writes that a power cut takes.
/// `raft-power-loss` must catch that: the cut member comes back without
/// records it had flushed, or with a hole recovery refuses to read past.
#[test]
fn ignored_flushes_are_caught_by_power_loss() {
    for seed in [1, 2, 3] {
        let config = RaftSimConfig {
            disks: Disks::PowerLoss,
            ignored_flushes: true,
            ..RaftSimConfig::default()
        };
        let outcome = run_seed(seed, config);
        println!("{outcome}");
        let caught = outcome.findings.iter().any(|f| {
            f.contains("in a power cut, which it had flushed")
                || (f.contains("did not start again") && f.contains("Corruption"))
        });
        assert!(caught, "seed {seed}: {}", outcome.first_failure());
    }
}

fn page(offsets: &[u64], next: u64) -> felix_storage::log::ReadPage {
    felix_storage::log::ReadPage {
        records: offsets
            .iter()
            .map(|offset| felix_storage::log::LogRecord {
                offset: *offset,
                timestamp_micros: 0,
                checksum: 0,
                payload: Bytes::from_static(b"x"),
                mark: RecordMark::None,
                publisher: None,
                headers: None,
                generation: Some(1),
            })
            .collect(),
        next,
    }
}

/// A read that fails mid-log is an error naming the offset, not a short log
/// that the term judges would call a missing entry (#3126).
#[tokio::test]
async fn a_read_error_while_collecting_a_log_is_reported() {
    let mut calls = 0;
    let got = collect_below(10, async |range: ReadRange| {
        calls += 1;
        if calls == 1 {
            Ok(page(&[0, 1, 2], 3))
        } else {
            assert_eq!(range.start, 3);
            Err(felix_storage::StorageError::InvalidRange)
        }
    })
    .await;
    let (offset, error) = got.unwrap_err();
    assert_eq!(offset, 3);
    assert!(!error.is_empty());
}

/// The end of the log, and a start below a trimmed base, end the read
/// quietly.
#[tokio::test]
async fn a_clean_end_of_log_is_not_an_error() {
    let mut calls = 0;
    let got = collect_below(10, async |_| {
        calls += 1;
        Ok(if calls == 1 {
            page(&[0, 1], 2)
        } else {
            page(&[], 2)
        })
    })
    .await;
    assert_eq!(got.unwrap().len(), 2);
    let trimmed = collect_below(10, async |_| {
        Err(felix_storage::StorageError::Trimmed {
            requested: 0,
            oldest: 5,
            trim_headers: None,
        })
    })
    .await;
    assert!(trimmed.unwrap().is_empty());
}

/// After the heal nothing is injected, so a leadership that moved did so
/// by a placement hand-over to the broker placement names (#2990).
#[test]
fn only_a_hand_over_to_placement_moves_leadership_after_the_heal() {
    let handed = [(5, "b2".to_string())];
    assert!(moved_by_placement(5, 5, "b5", "b2", &[]), "nothing moved");
    assert!(moved_by_placement(5, 6, "b2", "b2", &handed));
    // A hand-over can fail and the target still win the next election.
    assert!(moved_by_placement(5, 7, "b2", "b2", &handed));
    assert!(
        !moved_by_placement(5, 6, "b2", "b2", &[]),
        "a new term with no hand-over is an election"
    );
    assert!(
        !moved_by_placement(5, 6, "b3", "b2", &handed),
        "a member placement does not name took over"
    );
    assert!(
        !moved_by_placement(5, 6, "b2", "b2", &[(4, "b2".to_string())]),
        "a hand-over before the heal does not explain a term after it"
    );
    assert!(!moved_by_placement(
        5,
        6,
        "b2",
        "b2",
        &[(5, "b3".to_string())]
    ));
}
