//! Purging a Raft stream shard's log below its floor (#2602). The fleet
//! gate is off in these brokers, so only a test's own calls purge.

use felix_broker::StartPosition;
use felix_storage::log::LogConfig;

use super::trim::{publish, too_old, trim};
use super::*;

/// The trio after `broker-b` was cut off holding `r0` and `r1`, and the
/// leader wrote `r2..=r5` and trimmed to `r4`'s offset without it.
struct CutAndTrimmed {
    net: Arc<Loopback>,
    members: Vec<Member>,
    /// `r4`'s offset.
    floor: u64,
    /// One past the trim's offset.
    tail: u64,
    /// One past the last entry `broker-b` holds.
    held: u64,
}

async fn cut_and_trimmed() -> CutAndTrimmed {
    let (net, members, leader) = trio().await;
    assert_eq!(members[leader].name, "broker-a");
    let orders = key("orders", 0);
    let lead = &members[0];
    let term = lead.shards.leading(&orders).expect("term");
    lead.broker.allow_raft_head_trims_when(|| true);
    lead.publish(term, b"r0").await;
    let held = lead.publish(term, b"r1").await + 1;
    let cut = members[1].shards.log_of(&orders).await.expect("log");
    until("broker-b holds r1", || cut.commit_offset() == held).await;
    net.cut.lock().insert(members[1].name.to_string());

    let mut floor = 0;
    let mut tail = 0;
    for payload in [b"r2", b"r3", b"r4", b"r5"] {
        tail = lead.publish(term, payload).await + 1;
        if payload == b"r4" {
            floor = tail - 1;
        }
    }
    trim(&lead.broker, &lead.engine, term, floor, tail)
        .await
        .expect("trimmed");
    for member in [&members[0], &members[2]] {
        let log = member.shards.log_of(&orders).await.expect("log");
        until("the trim applies", || {
            log.readable_base() == floor && log.commit_offset() == tail + 1
        })
        .await;
    }
    CutAndTrimmed {
        net,
        members,
        floor,
        tail: tail + 1,
        held,
    }
}

/// Purge `member`'s log below its floor, as the retention loop does.
async fn purge(member: &Member) -> Option<u64> {
    member
        .shards
        .runtime
        .purge_below_floor(&key("orders", 0))
        .await
}

/// `member` refuses a resume below `floor` and serves `r4` and `r5` from it.
async fn serves_from(member: &Member, floor: u64) {
    assert!(
        too_old(&member.broker, floor - 1).await,
        "{} read below the floor",
        member.name
    );
    assert!(
        too_old(&member.broker, 0).await,
        "{} read the purged head",
        member.name
    );
    let records = member
        .broker
        .read_committed("t1", "ns", "orders", 0, floor, 1 << 16)
        .await
        .expect("a page from the floor");
    let payloads: Vec<_> = records
        .iter()
        .map(|record| (record.offset, record.payload.clone()))
        .collect();
    assert_eq!(
        payloads[..2],
        [
            (floor, bytes::Bytes::from_static(b"r4")),
            (floor + 1, bytes::Bytes::from_static(b"r5")),
        ],
        "{} served the wrong records from the floor",
        member.name
    );
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
}

/// **A member behind a purge catches up by installing a snapshot below the
/// floor.** `broker-c` purges uncapped to `floor - 1` as a follower, then
/// leads; `broker-b`, cut off before the trim, can only catch up from its
/// snapshot. It keeps the floor the snapshot carries, though it never held
/// the trim record, and serves from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_behind_a_purge_installs_the_snapshot_and_keeps_the_floor() {
    let CutAndTrimmed {
        net,
        members,
        floor,
        tail,
        ..
    } = cut_and_trimmed().await;
    let orders = key("orders", 0);
    assert_eq!(
        purge(&members[2]).await,
        Some(floor - 1),
        "a follower purges uncapped"
    );

    let mut moved = replicated();
    moved.leader = "broker-c".to_string();
    for member in &members {
        member.shards.take(&mut assignments([moved.clone()])).await;
    }
    until("broker-c leads", || {
        members[2].shards.leading(&orders).is_some()
    })
    .await;
    net.cut.lock().clear();

    let behind = &members[1];
    let log = behind.shards.log_of(&orders).await.expect("log");
    until("broker-b catches up", || log.commit_offset() >= tail).await;
    let carried = log
        .carried_floor()
        .expect("broker-b installed a purge snapshot");
    assert_eq!(carried.floor, floor);
    assert_eq!(carried.headers, bytes::Bytes::from_static(b"holder"));
    until("broker-b serves its term", || behind.bound() >= tail).await;
    serves_from(behind, floor).await;
    stop(&members).await;
}

/// **No member is read below the floor after a purge, or after a restart
/// that follows it**, leader or follower. `broker-b` was cut off before the
/// trim, so after it installs `broker-c`'s purge snapshot its floor lives
/// only in the carried-floor file; every member then restarts, so what
/// serves is read back from disk and not remembered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_member_is_read_below_the_floor_after_a_purge() {
    let CutAndTrimmed {
        net,
        mut members,
        floor,
        tail,
        ..
    } = cut_and_trimmed().await;
    let orders = key("orders", 0);
    assert_eq!(purge(&members[2]).await, Some(floor - 1));
    let mut moved = replicated();
    moved.leader = "broker-c".to_string();
    for member in &members {
        member.shards.take(&mut assignments([moved.clone()])).await;
    }
    until("broker-c leads", || {
        members[2].shards.leading(&orders).is_some()
    })
    .await;
    net.cut.lock().clear();
    let log = members[1].shards.log_of(&orders).await.expect("log");
    until("broker-b installs the snapshot", || {
        log.commit_offset() >= tail && log.carried_floor().is_some()
    })
    .await;
    for member in &members {
        let log = member.shards.log_of(&orders).await.expect("log");
        until("the floor reaches every member", || {
            log.readable_base() == floor
        })
        .await;
        if member.name != "broker-c" {
            let purged = purge(member).await;
            assert!(
                purged.is_none_or(|at| at < floor),
                "{} purged to {purged:?}",
                member.name
            );
        }
    }
    stop(&members).await;

    let mut restarted = Vec::new();
    for Member { name, root, .. } in members.drain(..).rev() {
        net.forget(name);
        let member = Member::start_on(
            &net,
            name,
            root,
            INCARNATION,
            ConsistencyLevel::Quorum,
            FsyncMode::None,
            false,
        )
        .await;
        member.shards.take(&mut assignments([moved.clone()])).await;
        restarted.push(member);
    }
    restarted.reverse();
    served(&restarted).await;
    for member in &restarted {
        until("the member serves its term", || member.bound() >= tail).await;
        serves_from(member, floor).await;
    }
    stop(&restarted).await;
}

/// **A leader never purges past the match of a member that answers.**
/// `broker-b` is behind the trim but hears heartbeats, so the leader keeps
/// the entries it still needs, and `broker-b` catches up from the log
/// without an install.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_keeps_the_log_a_lagging_member_needs() {
    let CutAndTrimmed {
        net,
        members,
        floor,
        tail,
        held,
    } = cut_and_trimmed().await;
    // Answering heartbeats again but sent no entries: behind, not down.
    net.starved.lock().insert(members[1].name.to_string());
    net.cut.lock().clear();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let purged = purge(&members[0]).await;
    assert!(
        purged.is_none_or(|at| at < held),
        "the leader purged to {purged:?}, past broker-b's {held}"
    );
    net.starved.lock().clear();
    let orders = key("orders", 0);
    let log = members[1].shards.log_of(&orders).await.expect("log");
    until("broker-b catches up", || {
        log.commit_offset() >= tail && log.readable_base() == floor
    })
    .await;
    assert!(
        log.carried_floor().is_none(),
        "broker-b installed a snapshot"
    );
    stop(&members).await;
}

/// **A member that stopped answering does not keep the leader's disk.**
/// `broker-b` stays cut off past the time after which a peer counts as
/// silent, so the leader purges to `floor - 1` as a follower would. When
/// `broker-b` is back it catches up by installing the leader's snapshot and
/// serves from the floor it carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_purges_past_a_member_that_stopped_answering() {
    let CutAndTrimmed {
        net,
        members,
        floor,
        tail,
        held,
    } = cut_and_trimmed().await;
    let orders = key("orders", 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let purged = purge(&members[0]).await;
        if purged == Some(floor - 1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader kept its log for broker-b, which is down: purged to {purged:?}, held {held}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    net.cut.lock().clear();
    let behind = &members[1];
    let log = behind.shards.log_of(&orders).await.expect("log");
    until("broker-b catches up", || log.commit_offset() >= tail).await;
    let carried = log
        .carried_floor()
        .expect("broker-b installed the leader's purge snapshot");
    assert_eq!(carried.floor, floor);
    assert_eq!(carried.headers, bytes::Bytes::from_static(b"holder"));
    until("broker-b serves its term", || behind.bound() >= tail).await;
    serves_from(behind, floor).await;
    stop(&members).await;
}

/// **A member restarted after a purge serves as before.** It opens from the
/// purge snapshot and the log above it: the membership, the consumer-group
/// table and the cut batches the log forgot come from the snapshot, the floor
/// from the trim record, and offsets go on past the tail.
#[tokio::test]
async fn a_member_restarted_after_a_purge_serves_from_its_floor() {
    use felix_broker::{CreatedBy, CreationToken, GroupEntry, GroupOp, SequenceReuse};

    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    // Small segments, so the purge drops the ones holding the group and the
    // cut batch and only the snapshot still has them.
    let small = || LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        segment_size_bytes: 512,
        ..LogConfig::default()
    };
    let (floor, tail) = {
        let broker = broker_with_config(
            root.path(),
            1,
            INCARNATION,
            ConsistencyLevel::Leader,
            small(),
        )
        .await;
        broker.allow_raft_head_trims_when(|| true);
        let shards = shards(&broker);
        lead_orders(&shards).await;
        let term = shards.leading(&orders).expect("term");
        let engine = engine(&shards);
        let raft = shards.engine_for(&orders).expect("raft engine");
        // A group and a cut batch, both written below the purge point.
        let cre = CreationToken {
            term,
            index: 0,
            by: CreatedBy::Hydrate,
        };
        let write = GroupEntry {
            group: "billing".to_owned(),
            op: GroupOp::Advance { cre, to: 3 },
        };
        let written = raft
            .propose_group(&orders, term, 1_700_000_000, &write)
            .await
            .expect("group written");
        raft.await_commit(&orders, written, std::time::Duration::from_secs(10))
            .await
            .expect("group committed");
        write_batch(&shards, &orders, 9, 0, 0, 2, &[b"cut"]).await;
        const FILL: &[u8] = &[7; 200];
        for _ in 0..8 {
            publish(&broker, &engine, term, FILL).await;
        }
        let mut tail = 0;
        let mut floor = 0;
        for payload in [b"r0", b"r1", b"r2", b"r3", b"r4", b"r5"] {
            tail = publish(&broker, &engine, term, payload).await + 1;
            if payload == b"r4" {
                floor = tail - 1;
            }
        }
        trim(&broker, &engine, term, floor, tail)
            .await
            .expect("trimmed");
        let log = shards.log_of(&orders).await.expect("log");
        until("the trim applies", || log.readable_base() == floor).await;
        let purged = shards.runtime.purge_below_floor(&orders).await;
        assert!(purged.is_some_and(|at| at < floor), "purged to {purged:?}");
        until("the purge drops the early segments", || {
            log.segments()
                .first()
                .is_some_and(|first| first.base_offset > 0)
        })
        .await;
        EngineShards::shutdown(&*shards).await;
        (floor, tail + 1)
    };
    let broker = broker_with_config(
        root.path(),
        1,
        INCARNATION,
        ConsistencyLevel::Leader,
        small(),
    )
    .await;
    let shards = shards(&broker);
    lead_orders(&shards).await;
    let log = shards.log_of(&orders).await.expect("log");
    until("the floor is back", || log.readable_base() == floor).await;
    let engine = engine(&shards);
    let term = shards.leading(&orders).expect("term");
    assert!(too_old(&broker, floor - 1).await, "read below the floor");
    let records = broker
        .read_committed("t1", "ns", "orders", 0, floor, 1 << 16)
        .await
        .expect("a page from the floor");
    assert_eq!(records[0].offset, floor);
    assert_eq!(records[0].payload, bytes::Bytes::from_static(b"r4"));

    let groups = shards
        .engine_for(&orders)
        .and_then(|raft| raft.groups(&orders))
        .expect("the shard's groups");
    let billing = groups.read().table().group("billing").map(|g| g.cursor);
    assert_eq!(billing, Some(3), "the group table came back");

    // The cut batch's records are purged, so only the snapshot still names it.
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let turn = broker.producer_turn(&handle, 9).await;
    let resent = engine
        .publish_idempotent_on_raft(
            &broker,
            &handle,
            &orders,
            term,
            turn,
            0,
            SequenceReuse::Refuse,
            vec![
                bytes::Bytes::from_static(b"cut"),
                bytes::Bytes::from_static(b"rest"),
            ],
            Default::default(),
            std::time::Duration::from_secs(10),
        )
        .expect("a Raft shard");
    assert!(
        matches!(
            resent,
            Err(crate::shards::engine::RaftRefusal::Broker(
                felix_broker::BrokerError::SequenceExpired { sequence: 0 }
            ))
        ),
        "a cut batch the purge dropped was accepted again"
    );
    let next = publish(&broker, &engine, term, b"r6").await;
    assert!(next >= tail, "offset {next} given out again below {tail}");
    EngineShards::shutdown(&*shards).await;
}
