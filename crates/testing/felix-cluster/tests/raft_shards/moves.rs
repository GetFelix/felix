//! Raft moves whose destination is dead or cut off, at three copies.
//!
//! The Raft half of the hybrid planned-move tests that were never run on
//! Raft: a destination that dies mid-move, one that is live but
//! unreachable while it is still a learner, and a move whose old leader and
//! destination die while the membership change is still uncommitted.

use std::time::Duration;

use felix_cluster::{Cluster, Fault, FsyncFault};

use super::{
    STREAM, Sent, assert_holds_acknowledged, copies, four_broker_raft_cluster, leader_among,
    leader_within, metric, opted_in, publish_tried, replay_holding, stored_set, voters_become,
};

/// Every member in `members` holds each acknowledged record once, and no
/// record answered as unproven or refused broke its promise.
async fn assert_each_holds(cluster: &Cluster, members: &[String], acked: &[Vec<u8>], sent: &Sent) {
    for node in members {
        let held = replay_holding(cluster, node, acked).await;
        for payload in acked {
            assert_eq!(
                copies(&held, payload),
                1,
                "{node} must hold {:?} once",
                String::from_utf8_lossy(payload)
            );
        }
        if let Err(wrong) = sent.check(node, &held) {
            panic!("{wrong}");
        }
    }
}

/// **An RF 3 leader move whose destination dies is undone, and nothing
/// acknowledged is lost.** The destination is killed before the move is
/// taken, so it never votes. Writes keep being acknowledged while placement
/// steps, and once the control plane sees the destination down the set is
/// the three original brokers again, each holding every acknowledged record.
/// The RF 1 version is `a_single_copy_raft_move_to_a_dead_broker_returns_to_the_old_one`.
#[tokio::test]
#[serial_test::serial]
async fn a_raft_leader_move_to_a_dead_broker_leaves_the_set_as_it_was() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    let (old, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(old.clone());
    voters_become(&cluster, &members, converged_within).await;
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut sent = Sent::default();
    let mut acked = Vec::new();
    for n in 0..5 {
        let (_, payload) =
            publish_tried(&cluster, &followers[0], &format!("before-{n}"), &mut sent).await;
        acked.push(payload);
    }
    // Killed first so it cannot vote before it dies. The control plane still
    // counts it live until its heartbeats lapse, so the move is taken.
    cluster
        .kill_node(&destination)
        .expect("kill the destination");
    let step = cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("the move is taken before the destination is seen down");
    assert_eq!(step, "raft_replace");

    // The move writes the destination into the stored set (as leader, with
    // the swapped-out member's seat). Seeing it there proves a revert below
    // is placement undoing the move, not a move that never took hold.
    let named = |(leader, replicas, _): &(String, Vec<String>, Vec<String>)| {
        *leader == destination || replicas.contains(&destination)
    };
    let mut placed = named(&stored_set(&cluster).await);

    // Placement steps and writes land throughout, so a set that stalls on a
    // dead learner shows up as a refused or unacknowledged write.
    for n in 0..10 {
        let (_, payload) =
            publish_tried(&cluster, &followers[0], &format!("during-{n}"), &mut sent).await;
        acked.push(payload);
        cluster.place_shards().await;
        placed |= named(&stored_set(&cluster).await);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        placed,
        "the move never reached placement: the stored set never named {destination}, \
         so the revert below would prove nothing; stored {:?}",
        stored_set(&cluster).await
    );
    let settled = felix_cluster::wait::until(
        converged_within,
        "the set to be the original three again",
        || async {
            cluster.place_shards().await;
            let (leader, mut replicas, _) = stored_set(&cluster).await;
            replicas.push(leader);
            replicas.sort();
            let mut want = members.clone();
            want.sort();
            replicas == want
        },
    )
    .await;
    if let Err(err) = settled {
        panic!(
            "the set never returned to {members:?}: {err:#}; stored {:?}",
            stored_set(&cluster).await
        );
    }
    voters_become(&cluster, &members, converged_within).await;
    for n in 0..5 {
        let (_, payload) =
            publish_tried(&cluster, &followers[0], &format!("after-{n}"), &mut sent).await;
        acked.push(payload);
    }
    assert_each_holds(&cluster, &members, &acked, &sent).await;
    cluster.shutdown().await;
}

/// **A destination that is live but cut off while still a learner does not
/// hold up writes, and the move finishes once it is reachable.** Every other
/// broker refuses the destination before the move is taken, so it can never
/// catch up. Writes through a follower keep being acknowledged. After the cut
/// is healed the destination becomes a voter, and it holds every record
/// acknowledged meanwhile. Raft has no move timeout, so this does not assert
/// that an unreachable learner is given up on.
#[tokio::test]
#[serial_test::serial]
async fn a_raft_move_to_a_cut_off_learner_does_not_hold_up_writes() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    let (old, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(old.clone());
    voters_become(&cluster, &members, converged_within).await;
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut sent = Sent::default();
    let mut acked = Vec::new();
    for n in 0..5 {
        let (_, payload) =
            publish_tried(&cluster, &followers[0], &format!("before-{n}"), &mut sent).await;
        acked.push(payload);
    }
    // Neither side reaches the other, but the destination stays registered
    // and alive, so the control plane takes the move.
    for node in &members {
        cluster
            .inject(&Fault::Refuse {
                node: node.clone(),
                peers: vec![destination.clone()],
            })
            .await
            .expect("cut the destination off");
    }
    cluster
        .inject(&Fault::Refuse {
            node: destination.clone(),
            peers: members.clone(),
        })
        .await
        .expect("cut the destination off from its side");
    let step = cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("a move to a live broker is taken");
    assert_eq!(step, "raft_replace");

    for n in 0..10 {
        let (_, payload) =
            publish_tried(&cluster, &followers[0], &format!("cut-off-{n}"), &mut sent).await;
        acked.push(payload);
        cluster.place_shards().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    cluster.heal_all().await.expect("heal");
    let mut want = followers.clone();
    want.push(destination.clone());
    let finished = felix_cluster::wait::until(
        converged_within,
        "the move to finish once the destination is reachable",
        || async {
            cluster.place_shards().await;
            stored_set(&cluster).await.2 == {
                let mut sorted = want.clone();
                sorted.sort();
                sorted
            }
        },
    )
    .await;
    if let Err(err) = finished {
        panic!(
            "the move never finished after the heal: {err:#}; stored {:?}",
            stored_set(&cluster).await
        );
    }
    for n in 0..5 {
        let (_, payload) =
            publish_tried(&cluster, &followers[0], &format!("after-{n}"), &mut sent).await;
        acked.push(payload);
    }
    assert_each_holds(&cluster, &want, &acked, &sent).await;
    cluster.shutdown().await;
}

/// How long `kept`'s flushes take in the test below: the window in which the
/// grown membership is written but not committed.
const SLOW_FLUSH: Duration = Duration::from_secs(5);

/// **A move whose old leader and destination die before its membership
/// change commits elects no member missing acknowledged records.** Four
/// brokers, one three-copy shard. One follower, `short`, gets nothing past
/// the first record, so it lacks records a majority acknowledged. The other,
/// `kept`, flushes slowly. A leader move to the fourth broker grows the set
/// to the old three plus the destination first, and with `short` cut off
/// that change commits only once `kept` has flushed it. While the old leader
/// holds the change written and uncommitted, it and the destination are
/// killed.
///
/// A seat move adds or removes one voter per change, and openraft writes no
/// joint entry for that, so this window (the new membership in force on some
/// members, not committed) is the nearest a move comes to a joint change.
/// The survivors cannot agree on the membership: `kept` may count four
/// voters and need three votes, `short` counts three and needs `kept`, which
/// refuses it for its shorter log. So any leader they elect holds every
/// acknowledged record. Once both killed brokers are back, the move
/// finishes with three voters that each hold the log, and both brokers
/// serve again.
#[tokio::test]
#[serial_test::serial]
async fn a_raft_move_killed_before_its_grown_set_commits_elects_no_short_member() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let mut cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    let (old, followers, _) = stored_set(&cluster).await;
    let mut members = followers.clone();
    members.push(old.clone());
    voters_become(&cluster, &members, converged_within).await;
    let (kept, short) = (followers[0].clone(), followers[1].clone());
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|node| !members.contains(node))
        .expect("a broker outside the set");

    let mut sent = Sent::default();
    publish_tried(&cluster, &old, "before", &mut sent).await;
    cluster
        .wait_for_every_member_level(STREAM, Duration::from_secs(60))
        .await
        .expect("every member holds it");
    // No broker reaches `short` from here on. Its own requests still go, so
    // it can stand for election with its short log.
    for node in cluster.node_ids().into_iter().filter(|node| *node != short) {
        cluster
            .inject(&Fault::Refuse {
                node,
                peers: vec![short.clone()],
            })
            .await
            .expect("cut the short follower off");
    }
    for i in 0..10 {
        publish_tried(&cluster, &old, &format!("acknowledged-{i}"), &mut sent).await;
    }
    cluster
        .inject(&Fault::Fsync {
            node: kept.clone(),
            fault: FsyncFault::Delay(SLOW_FLUSH),
        })
        .await
        .expect("slow kept's flushes");

    let step = cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("a move outside the set is taken");
    assert_eq!(step, "raft_replace");
    // The learner's entry names no new voter, so four voters on the old
    // leader is the grown set. Nothing else is written meanwhile, so a commit
    // index short of the log's end is that change not yet committed.
    let seen = std::cell::RefCell::new(None);
    let caught = felix_cluster::wait::until(
        Duration::from_secs(120),
        "the old leader to hold the grown set uncommitted",
        || async {
            let Some(member) = cluster.raft_member(&old, STREAM).await else {
                return false;
            };
            let pending = member.role == "leader"
                && member.voters.len() == 4
                && member.voters.contains(&destination)
                && matches!(
                    (member.committed, member.last_index),
                    (Some(committed), Some(last)) if committed < last
                );
            *seen.borrow_mut() = Some(member);
            pending
        },
    )
    .await;
    let seen = seen.into_inner();
    if let Err(err) = caught {
        panic!(
            "{err:#}; the old leader last reported {seen:?}, the stored set is {:?}",
            stored_set(&cluster).await
        );
    }
    cluster.kill_node(&old).expect("kill the old leader");
    cluster
        .kill_node(&destination)
        .expect("kill the destination");
    let kept_view = cluster.raft_member(&kept, STREAM).await;
    println!("killed with the old leader at {seen:?}; kept reports {kept_view:?}");
    cluster.heal_all().await.expect("heal");

    let survivors = [kept.clone(), short.clone()];
    match leader_within(&cluster, &survivors, Duration::from_secs(30)).await {
        Some(leader) => {
            println!("{leader} leads the survivors");
            assert_holds_acknowledged(&cluster, &leader, &sent).await;
        }
        None => println!("no survivor leads with the old leader and the destination down"),
    }

    cluster
        .restart_node(&destination)
        .await
        .expect("restart the destination");
    cluster
        .restart_node(&old)
        .await
        .expect("restart the old leader");
    let everyone = [
        kept.clone(),
        short.clone(),
        destination.clone(),
        old.clone(),
    ];
    let leader = leader_among(&cluster, &everyone).await;
    assert_holds_acknowledged(&cluster, &leader, &sent).await;
    let want = [kept.clone(), short.clone(), destination.clone()];
    voters_become(&cluster, &want, converged_within).await;
    // Through each restarted broker: both answer again.
    publish_tried(&cluster, &old, "after-old", &mut sent).await;
    publish_tried(&cluster, &destination, "after-destination", &mut sent).await;
    for node in &want {
        assert_holds_acknowledged(&cluster, node, &sent).await;
    }
    for node in cluster.node_ids() {
        let stopped = metric(
            &cluster,
            &node,
            "felix_broker_raft_shards_stopped_on_storage_error",
        )
        .await;
        assert_eq!(stopped, 0.0, "{node} stopped a group");
    }
    cluster.shutdown().await;
}
