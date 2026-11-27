//! A member its group removed ends the readers it serves.

use felix_broker::CacheWatchFilter;

use super::*;
use crate::serving::cache_routing::{apply_cache_request, put_request};
use crate::serving::forward::ForwardCredential;
use crate::shards::watch::{RaftTarget, RaftTargetVersion};

fn sessions() -> ShardKey {
    ShardKey {
        kind: ShardKind::Cache,
        ..key("sessions", 0)
    }
}

fn placed_on(brokers: &[&str]) -> ShardAssignment {
    let mut placed = assigned(sessions(), INCARNATION);
    placed.leader = brokers[0].to_string();
    placed.replicas = brokers.iter().map(|name| name.to_string()).collect();
    placed
}

async fn put(member: &Member, key: &'static str, value: &'static [u8]) {
    apply_cache_request(
        &member.broker,
        (&member.engine, Duration::from_secs(10)),
        Some(&ingress(&member.shards)),
        None,
        &ForwardCredential::from(""),
        "t1",
        "ns",
        "sessions",
        key,
        put_request(bytes::Bytes::from_static(value), None),
        None,
    )
    .await
    .expect("served");
}

/// **A follower the group drops ends its cache watches as moved.** broker-b
/// serves a watch, then the leader is told a target without it and removes
/// it. Its log stops, so a watch left open would read as a quiet key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_member_ends_its_cache_watches() {
    let net = Arc::new(Loopback::default());
    let sessions = sessions();
    let mut members = Vec::new();
    for name in BROKERS {
        members.push(Member::start_cache(&net, name).await);
    }
    let all: Vec<_> = members.iter().map(|member| &member.shards).collect();
    form_on(&all, placed_on(&BROKERS)).await;
    let (a, b) = (&members[0], &members[1]);
    until("broker-a serves its term", || {
        a.shards.leading(&sessions).is_some()
    })
    .await;
    put(a, "k", b"one").await;
    until("broker-b follows", || {
        b.shards.runtime.hears_leader(&sessions) == Some(true)
    })
    .await;

    let hub = Arc::clone(b.broker.cache_watches().expect("a hub"));
    let mut watch = hub.register(
        "t1",
        "ns",
        "sessions",
        0,
        CacheWatchFilter::Key("k".to_string()),
        64,
    );

    let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(10)));
    a.shards.follow_lease(Arc::clone(&lease));
    let kept = a
        .shards
        .runtime
        .target_members(&sessions, ["broker-a", "broker-c"], Duration::from_secs(5))
        .await
        .expect("the kept members");
    let mut without_b = placed_on(&["broker-a", "broker-c"]);
    without_b.target = Some(RaftTarget {
        version: RaftTargetVersion {
            incarnation: 1,
            counter: 1,
        },
        members: kept
            .iter()
            .map(|voter| (voter.broker.to_string(), voter.copy))
            .collect(),
    });
    a.shards.take(&mut assignments([without_b.clone()])).await;
    lease.record_head(tokio::time::Instant::now(), 10);
    a.shards
        .answered(Some(tokio::time::Instant::now()), Some(10));
    until("the group drops broker-b", || {
        a.shards.runtime.voters(&sessions).as_ref() == Some(&kept)
    })
    .await;
    put(a, "k", b"two").await;

    // broker-b's feed may say nothing yet: the group's word is enough.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let ended = loop {
        b.shards.take(&mut assignments([placed_on(&BROKERS)])).await;
        match tokio::time::timeout(Duration::from_millis(200), watch.recv()).await {
            // Applied before the removal, and delivered late.
            Ok(Some(change)) if change.value.as_deref() == Some(b"one") => {}
            Ok(Some(change)) => panic!("broker-b was sent {change:?} after its removal"),
            Ok(None) => break true,
            Err(_) if tokio::time::Instant::now() < deadline => {}
            Err(_) => break false,
        }
    };
    assert!(ended, "the removed member's watch was never ended");
    assert!(watch.moved().is_some(), "ended, but not as moved");
    stop(&members).await;
}

/// Hands `sessions` from broker-a to broker-b alone, the way a move that
/// replaces broker-a does: the target names only broker-b, so broker-a
/// hands its leadership over and broker-b removes it. broker-b's messages
/// stop reaching broker-a the moment it appends the entry that removes it,
/// as a leader stops replicating to a member it removed.
async fn replace_a_by_b(net: &Loopback, a: &Member, b: &Member) {
    let sessions = sessions();
    let kept = a
        .shards
        .runtime
        .target_members(&sessions, ["broker-b"], Duration::from_secs(5))
        .await
        .expect("the kept member");
    let mut only_b = placed_on(&["broker-b"]);
    only_b.target = Some(RaftTarget {
        version: RaftTargetVersion {
            incarnation: 1,
            counter: 1,
        },
        members: kept
            .iter()
            .map(|voter| (voter.broker.to_string(), voter.copy))
            .collect(),
    });
    let leases = [a, b].map(|member| {
        let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(10)));
        member.shards.follow_lease(Arc::clone(&lease));
        lease
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    for checkpoint in 10.. {
        for (member, lease) in [a, b].into_iter().zip(&leases) {
            member.shards.take(&mut assignments([only_b.clone()])).await;
            lease.record_head(tokio::time::Instant::now(), checkpoint);
            member
                .shards
                .answered(Some(tokio::time::Instant::now()), Some(checkpoint));
        }
        if b.shards.runtime.voters(&sessions).as_ref() == Some(&kept) {
            net.cut.lock().insert("broker-a".to_string());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker-b never became the group's only voter"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **A former leader removed from a group of two ends its cache watches as
/// moved.** broker-a leads and serves a watch, hands its leadership to
/// broker-b, and is removed without hearing it. It still counts itself a
/// voter and names broker-b as its last leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_former_leader_ends_its_cache_watches() {
    let net = Arc::new(Loopback::default());
    let sessions = sessions();
    let members = vec![
        Member::start_cache_of(&net, "broker-a", 1).await,
        Member::start_cache_of(&net, "broker-b", 1).await,
    ];
    let all: Vec<_> = members.iter().map(|member| &member.shards).collect();
    form_on(&all, placed_on(&["broker-a", "broker-b"])).await;
    let (a, b) = (&members[0], &members[1]);
    until("broker-a serves its term", || {
        a.shards.leading(&sessions).is_some()
    })
    .await;
    put(a, "k", b"one").await;

    let hub = Arc::clone(a.broker.cache_watches().expect("a hub"));
    let mut watch = hub.register(
        "t1",
        "ns",
        "sessions",
        0,
        CacheWatchFilter::Key("k".to_string()),
        64,
    );
    replace_a_by_b(&net, a, b).await;
    put(b, "k", b"two").await;

    let only_b = placed_on(&["broker-b"]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let ended = loop {
        a.shards.take(&mut assignments([only_b.clone()])).await;
        match tokio::time::timeout(Duration::from_millis(200), watch.recv()).await {
            Ok(Some(_)) => {}
            Ok(None) => break true,
            Err(_) if tokio::time::Instant::now() < deadline => {}
            Err(_) => break false,
        }
    };
    assert!(ended, "the removed former leader's watch was never ended");
    assert!(watch.moved().is_some(), "ended, but not as moved");
    stop(&members).await;
}
