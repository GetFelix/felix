//! Consumer groups across a Raft leader change, on three real members.

use felix_broker::{BrokerError, GroupConsumer, GroupKey};

use super::*;

fn group_key() -> GroupKey {
    GroupKey {
        tenant_id: "t1".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        group: "workers".into(),
    }
}

fn soon() -> std::time::Instant {
    std::time::Instant::now() + Duration::from_secs(10)
}

/// Offsets `member` hands `consumer`, once its commit index covers `through`.
async fn poll(
    member: &Member,
    consumer: &GroupConsumer,
    through: u64,
) -> felix_broker::Result<Vec<u64>> {
    let claimed = claim(member, consumer, through).await?;
    Ok(claimed.iter().map(|&(offset, _)| offset).collect())
}

/// `(offset, attempts)` for what `member` hands `consumer`.
async fn claim(
    member: &Member,
    consumer: &GroupConsumer,
    through: u64,
) -> felix_broker::Result<Vec<(u64, u32)>> {
    until("the commit covers the records", || {
        member.bound() >= through
    })
    .await;
    let handle = member
        .broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let log = handle.log().expect("a log");
    let claimed = member
        .shards
        .groups()
        .poll_below(
            &group_key(),
            log,
            member.bound(),
            16,
            std::time::Instant::now(),
            Some(consumer),
            None,
            soon(),
        )
        .await?;
    Ok(claimed.iter().map(|c| (c.offset, c.attempts)).collect())
}

/// The holder `h` reconnecting with `reclaim`.
fn reclaiming_holder() -> GroupConsumer {
    GroupConsumer::new("p", "h", 3, true)
}

async fn move_leader(members: &[Member], to: usize) {
    let orders = key("orders", 0);
    let mut moved = replicated();
    moved.leader = members[to].name.to_string();
    for member in members {
        member.shards.take(&mut assignments([moved.clone()])).await;
    }
    until("placement's leader serves the shard", || {
        members[to].shards.leading(&orders).is_some()
    })
    .await;
}

/// **A leader change mid-claim loses no record and hands none out twice.**
/// Claims are group entries in the shard's log, so the new leader holds them:
/// another consumer gets none of them, the holder reclaiming gets all of them
/// back at the same attempt, and its acks are taken. The old leader serves no
/// group operation and holds no tracker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_change_mid_claim_skips_nothing_and_clears_the_old_trackers() {
    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    assert_eq!(members[leader].name, "broker-a");
    let term = members[leader].shards.leading(&orders).expect("term");
    let mut offsets = Vec::new();
    for payload in [b"r0", b"r1", b"r2", b"r3", b"r4"] {
        offsets.push(members[leader].publish(term, payload).await);
    }
    let last = offsets[4];
    let holder = GroupConsumer::new("p", "h", 1, false);
    let other = GroupConsumer::new("p", "o", 2, false);

    let held = poll(&members[leader], &holder, last + 1)
        .await
        .expect("poll");
    assert_eq!(held, offsets.clone());
    assert_eq!(
        poll(&members[leader], &other, last + 1)
            .await
            .expect("poll"),
        Vec::<u64>::new(),
        "a live claim was handed to another consumer"
    );
    assert_eq!(members[leader].shards.groups().tracked(), 1);

    move_leader(&members, 2).await;
    until("the old leader lets go of its term", || {
        members[leader].shards.leading(&orders).is_none()
    })
    .await;

    assert_eq!(
        members[leader].shards.groups().tracked(),
        0,
        "the deposed leader kept a tracker"
    );
    assert!(
        poll(&members[leader], &holder, last + 1).await.is_err(),
        "the deposed leader served a group poll"
    );

    assert_eq!(
        poll(&members[2], &other, last + 1).await.expect("poll"),
        Vec::<u64>::new(),
        "the new leader handed a claimed record to another consumer"
    );
    let back = claim(&members[2], &reclaiming_holder(), last + 1)
        .await
        .expect("reclaim");
    assert_eq!(
        back,
        offsets.iter().map(|&o| (o, 1)).collect::<Vec<_>>(),
        "the holder did not get its claims back at the same attempt"
    );
    let groups = members[2].shards.groups();
    for &offset in &offsets {
        groups.ack(&group_key(), offset, soon()).await.expect("ack");
    }
    // Past the tail: claim entries take log indices too, so a low offset
    // may be one the predecessor could have handed out.
    assert!(matches!(
        groups.ack(&group_key(), last + 50, soon()).await,
        Err(BrokerError::GroupOffsetNotHandedOut { .. })
    ));
    assert_eq!(
        poll(&members[2], &other, last + 1).await.expect("poll"),
        Vec::<u64>::new()
    );

    stop(&members).await;
}

/// **A member that leads again after losing the shard serves the replicated
/// claims, not a stale tracker.** It rebuilds from the group's entries, so the
/// claims still stand against others and the holder can reclaim them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_leads_again_owes_what_it_claimed_before() {
    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let mut offsets = Vec::new();
    for payload in [b"r0", b"r1", b"r2"] {
        offsets.push(members[leader].publish(term, payload).await);
    }
    let last = offsets[2];
    let holder = GroupConsumer::new("p", "h", 1, false);
    assert_eq!(
        poll(&members[leader], &holder, last + 1)
            .await
            .expect("poll"),
        offsets
    );

    move_leader(&members, 2).await;
    until("the first member lets go", || {
        members[leader].shards.leading(&orders).is_none()
    })
    .await;
    move_leader(&members, leader).await;

    let other = GroupConsumer::new("p", "o", 2, false);
    assert_eq!(
        poll(&members[leader], &other, last + 1)
            .await
            .expect("poll"),
        Vec::<u64>::new(),
        "a claim did not survive two leader changes"
    );
    assert_eq!(
        poll(&members[leader], &reclaiming_holder(), last + 1)
            .await
            .expect("reclaim"),
        offsets,
        "the holder could not reclaim after the shard came back"
    );
    stop(&members).await;
}

/// **A leader that dies mid-claim loses no record and hands none out twice.**
/// No placement move, no hand-over: the member stops and the others elect.
/// The new leader holds the dead one's claims from the log: another consumer
/// gets none, the holder reclaims them, and its acks are taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_leader_mid_claim_skips_nothing_and_hands_out_once() {
    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let mut offsets = Vec::new();
    for payload in [b"r0", b"r1", b"r2"] {
        offsets.push(members[leader].publish(term, payload).await);
    }
    let last = offsets[2];
    for member in &members {
        until("the commit covers the records", || member.bound() > last).await;
    }
    let holder = GroupConsumer::new("p", "h", 1, false);
    let other = GroupConsumer::new("p", "o", 2, false);
    assert_eq!(
        poll(&members[leader], &holder, last + 1)
            .await
            .expect("poll"),
        offsets
    );

    let old = members.remove(leader);
    net.runtimes.lock().remove(old.name);
    EngineShards::shutdown(&*old.shards).await;
    let next = served(&members).await;
    assert!(members[next].shards.leading(&orders).expect("term") > term);

    assert_eq!(
        poll(&members[next], &other, last + 1).await.expect("poll"),
        Vec::<u64>::new(),
        "a claim died with the leader"
    );
    assert_eq!(
        poll(&members[next], &reclaiming_holder(), last + 1)
            .await
            .expect("reclaim"),
        offsets,
        "a record was skipped after the leader died"
    );
    let groups = members[next].shards.groups();
    for &offset in &offsets {
        groups
            .ack(&group_key(), offset, soon())
            .await
            .expect("the old holder's ack");
    }
    assert!(matches!(
        groups.ack(&group_key(), last + 50, soon()).await,
        Err(BrokerError::GroupOffsetNotHandedOut { .. })
    ));
    stop(&members).await;
}
