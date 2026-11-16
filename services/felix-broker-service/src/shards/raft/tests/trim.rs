//! Head trims on a Raft shard.

use bytes::Bytes;
use felix_broker::{BrokerError, StartPosition, TrimFailed, TrimOutcome};
use felix_storage::log::AppendOnlyLog;

use super::*;

/// Trim `orders` on `member` at `term`, as the control stream does.
async fn trim(
    broker: &Arc<Broker>,
    engine: &crate::shards::routing::StreamEngine,
    term: u64,
    before: u64,
    expected: u64,
) -> Result<TrimOutcome, String> {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let proposed = engine
        .trim_at_on_raft(
            broker,
            &handle,
            &key("orders", 0),
            term,
            before,
            expected,
            Bytes::from_static(b"holder"),
            Duration::from_secs(10),
        )
        .await
        .expect("a Raft shard")
        .map_err(|refused| refused.to_string())?;
    proposed.complete().await.map_err(|failed| match failed {
        TrimFailed::Engine(err) => err.to_string(),
        TrimFailed::Broker(err) => err.to_string(),
    })
}

/// Publish `payload` to `orders` at `term` and return its offset.
async fn publish(
    broker: &Arc<Broker>,
    engine: &crate::shards::routing::StreamEngine,
    term: u64,
    payload: &'static [u8],
) -> u64 {
    use crate::serving::direct_publish::publish_direct;
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    publish_direct(
        broker,
        None,
        engine,
        target(handle, &key("orders", 0), term),
        &[Bytes::from_static(payload)],
        None,
        Duration::from_secs(10),
    )
    .await
    .expect("published")
    .expect("an offset")
}

/// Whether a resume from `offset` on `broker` is refused as too old.
async fn too_old(broker: &Broker, offset: u64) -> bool {
    matches!(
        broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(offset))
            .await,
        Err(BrokerError::CursorTooOld { .. })
    )
}

/// **A trim is written as an entry and hides the shard below its floor on
/// every member**, each of which marks its data root as holding a trim. A stale one is refused with the tail and nothing
/// written, and one at or below the floor writes nothing and answers with
/// the floor, as on the hybrid.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trim_hides_the_shard_below_its_floor_on_every_member() {
    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    lead.broker.allow_raft_head_trims_when(|| true);
    let first = lead.publish(term, b"r0").await;
    let floor = lead.publish(term, b"r1").await;
    let tail = lead.publish(term, b"r2").await + 1;

    let trimmed = trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    assert_eq!(
        trimmed,
        TrimOutcome {
            offset: Some(tail),
            oldest: floor,
        }
    );
    assert!(too_old(&lead.broker, first).await, "read below the floor");
    for member in &members {
        let log = member.shards.log_of(&orders).await.expect("log");
        until("the floor reaches the member", || {
            log.readable_base() == floor
        })
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !too_old(&member.broker, first).await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} served a resume below the floor",
                member.name
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let resumed = member
            .broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Earliest)
            .await
            .expect("a resume from the start");
        assert!(
            resumed.backlog.iter().all(|record| record.offset >= floor),
            "{} served the ring below the floor",
            member.name
        );
        // Every member stores the entry, so every one must refuse a fleet
        // that has not finalized `head_trim`.
        assert!(
            felix_storage::run_state::head_trimmed(member.root.path()).expect("marker"),
            "{} holds a trim without the head-trim marker",
            member.name
        );
    }

    let stale = trim(&lead.broker, &lead.engine, term, tail, tail).await;
    let next = tail + 1;
    let refused = BrokerError::OffsetMismatch {
        expected: tail,
        tail: next,
    };
    assert_eq!(stale, Err(refused.to_string()), "the hybrid's refusal");
    assert_eq!(
        lead.shards
            .log_of(&orders)
            .await
            .expect("log")
            .tail_offset()
            .await
            .expect("tail"),
        next,
        "a stale trim wrote nothing"
    );
    let noop = trim(&lead.broker, &lead.engine, term, first, next)
        .await
        .expect("a no-op");
    assert_eq!(
        noop,
        TrimOutcome {
            offset: None,
            oldest: floor,
        }
    );
    stop(&members).await;
}

/// **The floor holds after a restart.** The log rebuilds it from the trim
/// record, which no snapshot carries.
#[tokio::test]
async fn a_trim_holds_after_a_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let floor = {
        let broker = broker(root.path()).await;
        broker.allow_raft_head_trims_when(|| true);
        let shards = shards(&broker);
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
        until("the term is served", || shards.leading(&orders).is_some()).await;
        let term = shards.leading(&orders).expect("term");
        let engine = engine(&shards);
        let mut tail = 0;
        for payload in [b"r0", b"r1", b"r2"] {
            tail = publish(&broker, &engine, term, payload).await + 1;
        }
        let floor = tail - 1;
        trim(&broker, &engine, term, floor, tail)
            .await
            .expect("trimmed");
        EngineShards::shutdown(&*shards).await;
        floor
    };
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let log = shards.log_of(&orders).await.expect("log");
    until("the floor is back", || log.readable_base() == floor).await;
    assert!(too_old(&broker, floor - 1).await, "read below the floor");
    EngineShards::shutdown(&*shards).await;
}

/// **A trim waits for `raft_head_trim`.** A member that predates trim
/// entries refuses one and replicates nothing past it, so until the node
/// allows Raft trims one is refused by name with nothing written.
#[tokio::test]
async fn a_trim_is_refused_until_raft_head_trim_is_finalized() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let broker = broker(root.path()).await;
    let finalized = Arc::new(AtomicBool::new(false));
    let gate = Arc::clone(&finalized);
    broker.allow_raft_head_trims_when(move || gate.load(Ordering::Acquire));
    let shards = shards(&broker);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let engine = engine(&shards);
    publish(&broker, &engine, term, b"r0").await;
    let tail = publish(&broker, &engine, term, b"r1").await + 1;

    let log = shards.log_of(&orders).await.expect("log");
    let base = log.readable_base();
    let refused = trim(&broker, &engine, term, tail - 1, tail).await;
    let not_enabled = BrokerError::HeadTrimNotEnabled {
        feature: felix_common::fleet::RAFT_HEAD_TRIM.name(),
    };
    assert_eq!(refused, Err(not_enabled.to_string()));
    assert_eq!(
        log.tail_offset().await.expect("tail"),
        tail,
        "nothing written"
    );
    assert_eq!(log.readable_base(), base, "no floor");

    finalized.store(true, Ordering::Release);
    let trimmed = trim(&broker, &engine, term, tail - 1, tail)
        .await
        .expect("trimmed once finalized");
    assert_eq!(trimmed.oldest, tail - 1);
    EngineShards::shutdown(&*shards).await;
}

/// **A follower holding the trim is not read below its floor after
/// `trim_ok`**, though it never hears the commit. On the hybrid only the
/// leader serves readers, so no reader is served below a floor once it is
/// answered; a Raft follower waits for its own commit to pass the trim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_holding_the_trim_is_not_read_below_it_after_the_answer() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let holder = &members[(leader + 1) % 3];
    lead.broker.allow_raft_head_trims_when(|| true);
    let first = lead.publish(term, b"r0").await;
    let floor = lead.publish(term, b"r1").await;
    let tail = lead.publish(term, b"r2").await + 1;
    let log = holder.shards.log_of(&orders).await.expect("log");
    until("the holder has the publishes", || {
        log.commit_offset() == tail
    })
    .await;

    // The holder gets the trim's entry and nothing after it, so it never
    // hears the trim committed. The third member keeps the leader's quorum.
    net.cut_after_entries.lock().insert(holder.name.to_string());
    trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    // The append to the holder may still be in flight.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while log.tail_offset().await.expect("tail") <= tail {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the holder has the trim"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        log.commit_offset() <= tail,
        "the holder never heard the commit"
    );
    // Every read waits for the leader's read index, a tail read too: the
    // holder's committed tail is below the floor. Cut off, it cannot get one.
    let latest = holder
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Latest)
        .await;
    assert!(
        matches!(
            latest,
            Err(felix_broker::BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "the holder served a tail read below the floor: {:?}",
        latest.map(|latest| latest.join)
    );
    let resumed = holder
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await;
    assert!(
        matches!(
            resumed,
            Err(felix_broker::BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "the holder served a resume below the floor after trim_ok"
    );

    net.cut.lock().clear();
    until("the holder hears the commit", || {
        log.readable_base() == floor
    })
    .await;
    assert!(too_old(&holder.broker, first).await, "read below the floor");
    stop(&members).await;
}

/// **A follower that reaches its leader and holds a trim not yet committed
/// holds a read below its floor, and serves one at or above it.** The read
/// index is below the trim, so only the hold keeps the follower from
/// serving records the trim may hide once it is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reachable_follower_holding_an_uncommitted_trim_holds_reads_below_it() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = Arc::clone(&members[leader].broker);
    let engine = Arc::clone(&members[leader].engine);
    let holder = &members[(leader + 1) % 3];
    let other = &members[(leader + 2) % 3];
    members[leader].broker.allow_raft_head_trims_when(|| true);
    let first = members[leader].publish(term, b"r0").await;
    let floor = members[leader].publish(term, b"r1").await;
    let tail = members[leader].publish(term, b"r2").await + 1;
    let log = holder.shards.log_of(&orders).await.expect("log");
    until("the holder has the publishes", || {
        log.commit_offset() == tail
    })
    .await;

    // The holder writes the trim and the leader never hears it did; the
    // other member gets no entries. Heartbeats still reach both, so the
    // leader keeps answering read indexes, and the trim cannot commit.
    net.unheard.lock().insert(holder.name.to_string());
    net.starved.lock().insert(other.name.to_string());
    let trimming = tokio::spawn(async move { trim(&lead, &engine, term, floor, tail).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while log.tail_offset().await.expect("tail") <= tail {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the holder never got the trim"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let started = tokio::time::Instant::now();
    holder
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(floor))
        .await
        .expect("a read at the pending floor is served");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a read at the pending floor waited for the trim"
    );
    let resumed = holder
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await;
    assert!(
        matches!(
            resumed,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "the holder served a resume below an uncommitted trim: {:?}",
        resumed.map(|resumed| resumed.history)
    );
    assert!(!trimming.is_finished(), "the trim committed");

    net.unheard.lock().clear();
    net.starved.lock().clear();
    trimming.await.expect("joined").expect("trimmed");
    until("the holder hears the commit", || {
        log.readable_base() == floor
    })
    .await;
    assert!(too_old(&holder.broker, first).await, "read below the floor");
    stop(&members).await;
}

/// **A follower that never got the trim's entry is not read below its floor
/// after `trim_ok`.** The leader commits the trim with the other member, so
/// the lagging follower's log has no trim to wait for. It still hears the
/// leader, so it asks for a read index it cannot apply and refuses the read
/// as retryable; once entries reach it again the resume is too old.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_without_the_trim_is_not_read_below_it_after_the_answer() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let lagger = &members[(leader + 1) % 3];
    lead.broker.allow_raft_head_trims_when(|| true);
    let first = lead.publish(term, b"r0").await;
    let floor = lead.publish(term, b"r1").await;
    let tail = lead.publish(term, b"r2").await + 1;
    let log = lagger.shards.log_of(&orders).await.expect("log");
    until("the lagger has the publishes", || {
        log.commit_offset() == tail
    })
    .await;

    net.starved.lock().insert(lagger.name.to_string());
    trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    assert_eq!(
        log.tail_offset().await.expect("tail"),
        tail,
        "the lagger got the trim"
    );

    let resumed = lagger
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await;
    assert!(
        matches!(
            resumed,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "the lagger served a resume below the floor after trim_ok: {:?}",
        resumed.map(|resumed| resumed.history)
    );
    let page = lagger
        .broker
        .read_committed("t1", "ns", "orders", 0, first, 1 << 16)
        .await;
    assert!(
        matches!(
            page,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "the lagger served a page below the floor after trim_ok: {page:?}"
    );

    net.starved.lock().clear();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let last = match lagger
            .broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
            .await
        {
            Err(BrokerError::CursorTooOld { oldest, .. }) => {
                assert_eq!(oldest, floor);
                break;
            }
            Err(
                err @ BrokerError::NotReadable {
                    reason: felix_broker::NotReadable::Settling,
                    ..
                },
            ) => err,
            other => panic!(
                "the lagger answered a resume below the floor with {:?}",
                other.map(|resumed| resumed.history)
            ),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lagger never caught up; last answer: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop(&members).await;
}

/// **Until the fleet finalizes `raft_follower_read`, a follower leaves a
/// read that could start below a floor to the leader.** The leader that
/// would answer its read index may predate the message. A tail read and the
/// leader's own reads are served as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_leaves_offset_reads_to_the_leader_until_raft_follower_read() {
    let (net, members, leader) = trio().await;
    net.follower_reads_off
        .store(true, std::sync::atomic::Ordering::Release);
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let follower = &members[(leader + 1) % 3];
    let first = lead.publish(term, b"r0").await;
    let tail = lead.publish(term, b"r1").await + 1;
    let log = follower.shards.log_of(&orders).await.expect("log");
    until("the follower has the publishes", || {
        log.commit_offset() == tail
    })
    .await;

    for start in [StartPosition::Offset(first), StartPosition::Earliest] {
        let resumed = follower
            .broker
            .subscribe_from("t1", "ns", "orders", 0, start)
            .await;
        assert!(
            matches!(
                resumed,
                Err(BrokerError::NotReadable {
                    reason: felix_broker::NotReadable::OnLeader,
                    ..
                })
            ),
            "the follower served {start:?} before the finalize: {:?}",
            resumed.map(|resumed| resumed.history)
        );
    }
    let page = follower
        .broker
        .read_committed("t1", "ns", "orders", 0, first, 1 << 16)
        .await;
    assert!(
        matches!(
            page,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::OnLeader,
                ..
            })
        ),
        "the follower served a page before the finalize: {page:?}"
    );
    let ranged = follower
        .broker
        .history_to("t1", "ns", "orders", 0, StartPosition::Offset(first), tail)
        .await;
    assert!(
        matches!(
            ranged,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::OnLeader,
                ..
            })
        ),
        "the follower served a bounded range before the finalize: {ranged:?}"
    );
    let latest = follower
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Latest)
        .await;
    assert!(
        matches!(
            latest,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::OnLeader,
                ..
            })
        ),
        "the follower served a tail read before the finalize: {:?}",
        latest.map(|latest| latest.join)
    );
    lead.broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await
        .expect("the leader serves the resume");
    stop(&members).await;
}

/// **A follower that cannot reach its leader refuses a read that could
/// start below a floor as retryable.** It cannot learn whether a trim was
/// answered, so it serves nothing from its own log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_cut_off_from_its_leader_refuses_offset_reads_as_retryable() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let follower = &members[(leader + 1) % 3];
    let first = lead.publish(term, b"r0").await;
    let tail = lead.publish(term, b"r1").await + 1;
    let log = follower.shards.log_of(&orders).await.expect("log");
    until("the follower has the publishes", || {
        log.commit_offset() == tail
    })
    .await;
    follower
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await
        .expect("a follower that reaches its leader serves the resume");

    super::cut_all(&net);
    let resumed = follower
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await;
    assert!(
        matches!(
            resumed,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "a follower cut off from its leader served a resume: {:?}",
        resumed.map(|resumed| resumed.history)
    );
    net.cut.lock().clear();
    stop(&members).await;
}

/// **A `Latest` subscribe on a follower behind a trim starts at or above
/// its floor.** The follower's committed tail is below the floor, and
/// records from there would reach the subscriber live once entries flow
/// again, so it applies the leader's read index first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tail_read_on_a_follower_behind_a_trim_starts_at_or_above_its_floor() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let lagger = &members[(leader + 1) % 3];
    lead.broker.allow_raft_head_trims_when(|| true);
    let first = lead.publish(term, b"r0").await;
    let log = lagger.shards.log_of(&orders).await.expect("log");
    until("the lagger has the first publish", || {
        log.commit_offset() == first + 1
    })
    .await;

    net.starved.lock().insert(lagger.name.to_string());
    lead.publish(term, b"r1").await;
    let floor = lead.publish(term, b"r2").await;
    let tail = lead.publish(term, b"r3").await + 1;
    trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    assert_eq!(
        log.commit_offset(),
        first + 1,
        "the lagger got the publishes"
    );

    let latest = lagger
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Latest)
        .await;
    assert!(
        matches!(
            latest,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            })
        ),
        "the lagger served a tail read from below the floor: {:?}",
        latest.map(|latest| latest.join)
    );

    net.starved.lock().clear();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let joined = loop {
        match lagger
            .broker
            .subscribe_from("t1", "ns", "orders", 0, StartPosition::Latest)
            .await
        {
            Ok(resumed) => break resumed.join.expect("a durable join"),
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            }) => {}
            Err(err) => panic!("a tail read was refused: {err}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the lagger never caught up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        joined.start_offset >= floor,
        "a tail read started at {} below the floor {floor}",
        joined.start_offset
    );
    stop(&members).await;
}

/// A leader cut off both ways while its successor trims below `floor`.
/// Returns the members, the deposed leader's index, the offset of its first
/// record and the floor.
async fn deposed_after_a_trim(
    follower_reads_off: bool,
) -> (Arc<super::Loopback>, Vec<Member>, usize, u64, u64) {
    let (net, members, leader) = trio().await;
    net.follower_reads_off
        .store(follower_reads_off, std::sync::atomic::Ordering::Release);
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let old = &members[leader];
    let first = old.publish(term, b"r0").await;
    let floor = old.publish(term, b"r1").await;
    let tail = old.publish(term, b"r2").await + 1;
    for member in &members {
        let log = member.shards.log_of(&orders).await.expect("log");
        until("every member has the publishes", || {
            log.commit_offset() == tail
        })
        .await;
    }

    super::isolate(&net, old.name);
    let mut next = None;
    until("another member serves a later term", || {
        next = members.iter().enumerate().find_map(|(at, member)| {
            let led = member.shards.leading(&orders)?;
            (at != leader && led > term).then_some((at, led))
        });
        next.is_some()
    })
    .await;
    let (next, next_term) = next.expect("a successor");
    let new = &members[next];
    new.broker.allow_raft_head_trims_when(|| true);
    let log = new.shards.log_of(&orders).await.expect("log");
    let expected = log.commit_offset();
    trim(&new.broker, &new.engine, next_term, floor, expected)
        .await
        .expect("trimmed");
    assert_eq!(
        old.shards.leading(&orders),
        Some(term),
        "the cut-off leader heard it was deposed"
    );
    (net, members, leader, first, floor)
}

/// Every read that could start below a floor on `broker` is refused as
/// retryable.
async fn refuses_reads_below(broker: &Broker, first: u64, tail: u64) {
    let settling = |err: &BrokerError| {
        matches!(
            err,
            BrokerError::NotReadable {
                reason: felix_broker::NotReadable::Settling,
                ..
            }
        )
    };
    for start in [StartPosition::Offset(first), StartPosition::Earliest] {
        match broker.subscribe_from("t1", "ns", "orders", 0, start).await {
            Err(err) if settling(&err) => {}
            other => panic!(
                "a deposed leader answered {start:?} with {:?}",
                other.map(|resumed| resumed.history)
            ),
        }
    }
    let page = broker
        .read_committed("t1", "ns", "orders", 0, first, 1 << 16)
        .await;
    assert!(page.as_ref().is_err_and(settling), "a page: {page:?}");
    let ranged = broker
        .history_to("t1", "ns", "orders", 0, StartPosition::Offset(first), tail)
        .await;
    assert!(ranged.as_ref().is_err_and(settling), "a range: {ranged:?}");
    let read = broker
        .read_range("t1", "ns", "orders", 0, first, None, 16, 1 << 16)
        .await;
    assert!(
        read.as_ref().is_err_and(settling),
        "a stream read: {read:?}"
    );
}

/// **A leader cut off from a successor that trimmed is not read below the
/// trim's floor.** It never heard the later term, so its own read-index
/// round is what tells it a majority moved on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_leader_that_missed_the_trim_is_not_read_below_it() {
    let (net, members, old, first, floor) = deposed_after_a_trim(false).await;
    refuses_reads_below(&members[old].broker, first, floor + 1).await;
    net.cut.lock().clear();
    net.muted.lock().clear();
    stop(&members).await;
}

/// **Before `raft_follower_read` is finalized, a follower redirects a
/// resume to the leader it knows, and a deposed leader at the other end
/// refuses it too.** The redirect can name a leader that has not heard its
/// successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_redirected_before_the_finalize_is_not_served_by_a_deposed_leader() {
    let (net, members, old, first, floor) = deposed_after_a_trim(true).await;
    let follower = members
        .iter()
        .enumerate()
        .find(|(at, member)| *at != old && member.shards.leading(&key("orders", 0)).is_none())
        .map(|(_, member)| member)
        .expect("a follower");
    let resumed = follower
        .broker
        .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
        .await;
    assert!(
        matches!(
            resumed,
            Err(BrokerError::NotReadable {
                reason: felix_broker::NotReadable::OnLeader,
                ..
            })
        ),
        "the follower served the resume before the finalize: {:?}",
        resumed.map(|resumed| resumed.history)
    );
    refuses_reads_below(&members[old].broker, first, floor + 1).await;
    net.cut.lock().clear();
    net.muted.lock().clear();
    stop(&members).await;
}

/// What a follower's read index adds to a resume and a history page, next
/// to the leader serving the same reads. Prints p50 and p99; a rough look,
/// not a benchmark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "prints timings; run by hand"]
async fn follower_read_latency() {
    const ROUNDS: usize = 500;
    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let lead = &members[leader];
    let follower = &members[(leader + 1) % 3];
    let first = lead.publish(term, b"r0").await;
    let mut tail = first;
    for _ in 0..16 {
        tail = lead.publish(term, b"rn").await + 1;
    }
    let log = follower.shards.log_of(&orders).await.expect("log");
    until("the follower has the publishes", || {
        log.commit_offset() == tail
    })
    .await;

    let pct = |mut times: Vec<Duration>| {
        times.sort();
        (times[times.len() / 2], times[times.len() * 99 / 100])
    };
    for (role, member) in [("leader", lead), ("follower", follower)] {
        let mut resumes = Vec::with_capacity(ROUNDS);
        let mut pages = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let started = std::time::Instant::now();
            let resumed = member
                .broker
                .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
                .await
                .expect("resumed");
            resumes.push(started.elapsed());
            drop(resumed);
            let started = std::time::Instant::now();
            member
                .broker
                .read_committed("t1", "ns", "orders", 0, first, 1 << 16)
                .await
                .expect("a page");
            pages.push(started.elapsed());
        }
        let (resume_p50, resume_p99) = pct(resumes);
        let (page_p50, page_p99) = pct(pages);
        println!(
            "{role}: resume p50 {resume_p50:?} p99 {resume_p99:?}; page p50 {page_p50:?} p99 {page_p99:?}"
        );
    }
    stop(&members).await;
}
