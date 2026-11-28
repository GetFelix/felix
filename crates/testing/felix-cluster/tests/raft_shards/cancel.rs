//! Raft moves undone after the voters changed, under a publisher and a
//! subscriber.
//!
//! Raft stages nothing to cancel: a move is undone by a move back, which is
//! a seat move of its own once the old leader is outside the set. One test
//! undoes the move once the new voters are final, the other as soon as the
//! group's voters name the destination, which can be mid joint change.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use felix_cluster::Cluster;
use felix_wire::AckMode;
use serial_test::serial;

use super::{
    STREAM, Sent, allow_follower_reads, four_broker_raft_cluster, opted_in, publish_tried,
    replay_at, stored_set, voters_become,
};

/// What a subscriber read: each event, and where it subscribed again.
#[derive(Debug, Clone)]
enum Read {
    /// Subscribed again from this offset after its subscription ended.
    From(u64),
    Event {
        offset: u64,
        skipped_before: u64,
        payload: Vec<u8>,
    },
}

/// Checks that `read` names every offset once and in order, and that every
/// jump between events is one the broker said holds no event. Returns each
/// event's offset and payload.
fn read_in_order(read: &[Read]) -> Vec<(u64, Vec<u8>)> {
    let mut next = 0u64;
    let mut events = Vec::new();
    for step in read {
        match step {
            Read::From(at) => assert_eq!(*at, next, "subscribed again from {at}, not {next}"),
            Read::Event {
                offset,
                skipped_before,
                payload,
            } => {
                assert_eq!(
                    *offset,
                    next + skipped_before,
                    "after {next} the subscriber saw {offset} with {skipped_before} \
                     skipped before it: a gap no one was told of, or an offset again"
                );
                next = offset + 1;
                events.push((*offset, payload.clone()));
            }
        }
    }
    events
}

#[test]
fn a_gap_no_one_was_told_of_fails_the_read_check() {
    let event = |offset, skipped_before| Read::Event {
        offset,
        skipped_before,
        payload: Vec::new(),
    };
    let told = [event(1, 1), event(2, 0), Read::From(3), event(5, 2)];
    assert_eq!(read_in_order(&told).len(), 3);
    for wrong in [
        vec![event(0, 0), event(2, 0)],
        vec![event(0, 0), event(0, 0)],
        vec![event(0, 0), Read::From(0), event(0, 0)],
    ] {
        let caught = std::panic::catch_unwind(|| read_in_order(&wrong));
        assert!(caught.is_err(), "{wrong:?} passed");
    }
}

/// The shard's set and its brokers before a move: the leader, the
/// followers, and the broker outside the set.
struct Set {
    old: String,
    followers: Vec<String>,
    members: Vec<String>,
    outsider: String,
}

impl Set {
    async fn of(cluster: &Cluster, within: Duration) -> Self {
        let (old, followers, _) = stored_set(cluster).await;
        let mut members = followers.clone();
        members.push(old.clone());
        voters_become(cluster, &members, within).await;
        let outsider = cluster
            .node_ids()
            .into_iter()
            .find(|node| !members.contains(node))
            .expect("a broker outside the set");
        Self {
            old,
            followers,
            members,
            outsider,
        }
    }

    /// Moves the leader's seat back to `old`, retrying while the control
    /// plane refuses it (the set must settle first), and waits until the
    /// set is the original one again with `old` leading it.
    async fn move_back(&self, cluster: &Cluster, within: Duration) {
        let old = &self.old;
        let step = felix_cluster::wait::until_some(
            Duration::from_secs(60),
            "the move back to be taken",
            || async { cluster.start_move(STREAM, 0, old).await.ok() },
        )
        .await
        .unwrap_or_else(|err| panic!("the move back to {old} was never taken: {err:#}"));
        assert_eq!(step, "raft_replace", "the old leader is outside the set");
        voters_become(cluster, &self.members, within).await;
        let mut followers = self.followers.clone();
        followers.sort();
        let placed = felix_cluster::wait::until(within, "the set to be placed back", || async {
            let (leader, mut replicas, _) = stored_set(cluster).await;
            replicas.sort();
            leader == *old && replicas == followers
        })
        .await;
        if let Err(err) = placed {
            panic!(
                "the set is not back as it was: {err:#}; stored {:?}, wanted {old} leading {followers:?}",
                stored_set(cluster).await
            );
        }
        felix_cluster::wait::until(within, "the old leader to lead again", || async {
            cluster
                .metric(old, "felix_broker_raft_shards_led")
                .await
                .ok()
                .flatten()
                .is_some_and(|led| led >= 1.0)
        })
        .await
        .expect("leadership follows placement back");
    }
}

/// A publisher and a subscriber from offset 0, each on a cluster client of
/// its own, running until [`Traffic::finish`].
struct Traffic {
    stop: Arc<AtomicBool>,
    done_reading: Arc<AtomicBool>,
    read: Arc<Mutex<Vec<Read>>>,
    during: Arc<Mutex<Sent>>,
    subscriber: tokio::task::JoinHandle<()>,
    publisher: tokio::task::JoinHandle<()>,
}

impl Traffic {
    async fn start(cluster: &Cluster) -> Self {
        let (addrs, token) = (cluster.broker_addrs(), cluster.client_token());
        let connect = || felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &token);
        let stop = Arc::new(AtomicBool::new(false));
        let done_reading = Arc::new(AtomicBool::new(false));
        let read: Arc<Mutex<Vec<Read>>> = Arc::default();
        let reader = Arc::new(connect().await.expect("reader"));
        let subscriber = {
            let (read, done) = (Arc::clone(&read), Arc::clone(&done_reading));
            let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
            tokio::spawn(async move {
                let subscribe = |from| {
                    let (reader, tenant, namespace) = (&reader, &tenant, &namespace);
                    async move {
                        reader
                            .subscribe_from(
                                tenant,
                                namespace,
                                STREAM,
                                Some(felix_client::StartPosition::Offset(from)),
                            )
                            .await
                    }
                };
                let mut subscription = subscribe(0).await.expect("subscribe");
                let mut next = 0u64;
                while !done.load(Ordering::Relaxed) {
                    let step =
                        tokio::time::timeout(Duration::from_secs(1), subscription.next_event())
                            .await;
                    match step {
                        Ok(Ok(Some(event))) => {
                            let offset = event.offset.expect("a durable stream carries offsets");
                            next = offset + 1;
                            read.lock().expect("read").push(Read::Event {
                                offset,
                                skipped_before: event.skipped_before,
                                payload: event.payload.to_vec(),
                            });
                        }
                        // Quiet, not ended.
                        Err(_) => {}
                        // The subscription follows the shard and a lag
                        // itself; one that still ends resumes where it
                        // stopped.
                        Ok(Ok(None) | Err(_)) => loop {
                            if done.load(Ordering::Relaxed) {
                                break;
                            }
                            if let Ok(resumed) = subscribe(next).await {
                                read.lock().expect("read").push(Read::From(next));
                                subscription = resumed;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        },
                    }
                }
            })
        };

        let during: Arc<Mutex<Sent>> = Arc::default();
        let writer = connect().await.expect("writer");
        let publisher = {
            let (during, stop) = (Arc::clone(&during), Arc::clone(&stop));
            let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
            tokio::spawn(async move {
                let mut i = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let payload = format!("during-{i}").into_bytes();
                    i += 1;
                    let published = writer
                        .publish(
                            &tenant,
                            &namespace,
                            STREAM,
                            payload.clone(),
                            AckMode::PerMessage,
                        )
                        .await;
                    {
                        let mut sent = during.lock().expect("sent");
                        match published {
                            Ok(offset) => sent.acked.push((
                                offset.expect("a durable stream acknowledges with an offset"),
                                payload,
                            )),
                            Err(err) => sent.failed(payload, &err),
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };
        Self {
            stop,
            done_reading,
            read,
            during,
            subscriber,
            publisher,
        }
    }

    fn acked(&self) -> usize {
        self.during.lock().expect("sent").acked.len()
    }

    /// Waits for ten more acknowledged publishes, stops the publisher, and
    /// checks what was sent (`before` and since [`Traffic::start`]) against
    /// what the subscriber read and what each of `members` replays: every
    /// acknowledged record once, at its acknowledged offset, every offset
    /// read once and in order, and nothing refused held.
    async fn finish(self, cluster: &Cluster, before: Sent, members: &[String]) {
        let back_at = self.acked();
        felix_cluster::wait::until(
            Duration::from_secs(60),
            "publishes acknowledged after the move back",
            || async { self.acked() >= back_at + 10 },
        )
        .await
        .expect("the shard takes writes once it is back");
        self.stop.store(true, Ordering::Relaxed);
        self.publisher.await.expect("publisher");

        let mut sent = before;
        {
            let during = self.during.lock().expect("sent");
            sent.acked.extend(during.acked.iter().cloned());
            sent.in_doubt.extend(during.in_doubt.iter().cloned());
            sent.refused.extend(during.refused.iter().cloned());
        }
        let owed: Vec<Vec<u8>> = sent.acked.iter().map(|(_, p)| p.clone()).collect();
        felix_cluster::wait::until(
            Duration::from_secs(60),
            "the subscriber to see every acknowledged record",
            || async {
                let read = self.read.lock().expect("read");
                let seen: std::collections::HashSet<&[u8]> = read
                    .iter()
                    .filter_map(|step| match step {
                        Read::Event { payload, .. } => Some(payload.as_slice()),
                        Read::From(_) => None,
                    })
                    .collect();
                owed.iter().all(|payload| seen.contains(payload.as_slice()))
            },
        )
        .await
        .expect("the subscriber gets every acknowledged record");
        self.done_reading.store(true, Ordering::Relaxed);
        self.subscriber.await.expect("subscriber");

        let read = self.read.lock().expect("read").clone();
        let seen: BTreeMap<u64, Vec<u8>> = read_in_order(&read).into_iter().collect();
        let skipped: u64 = read
            .iter()
            .map(|step| match step {
                Read::Event { skipped_before, .. } => *skipped_before,
                Read::From(_) => 0,
            })
            .sum();
        println!(
            "{} records seen, {skipped} offsets skipped as not events, {} resubscribes, \
             {} acknowledged, {} in doubt, {} refused",
            seen.len(),
            read.iter()
                .filter(|step| matches!(step, Read::From(_)))
                .count(),
            sent.acked.len(),
            sent.in_doubt.len(),
            sent.refused.len(),
        );
        for (offset, payload) in &sent.acked {
            assert_eq!(
                seen.get(offset),
                Some(payload),
                "the subscriber did not see {:?} at its acknowledged offset {offset}",
                String::from_utf8_lossy(payload)
            );
        }
        for node in members {
            let held: BTreeMap<u64, Vec<u8>> = replay_at(cluster, node, &owed)
                .await
                .into_iter()
                .map(|(offset, payload)| {
                    (offset.expect("a durable stream carries offsets"), payload)
                })
                .collect();
            for (offset, payload) in &sent.acked {
                assert_eq!(
                    held.get(offset),
                    Some(payload),
                    "{node} does not hold {:?} at its acknowledged offset {offset}",
                    String::from_utf8_lossy(payload)
                );
            }
            // The replay reads to the last acknowledged record, so compare up
            // to there: what the subscriber saw past it may not be replayed
            // yet.
            let last = held.keys().next_back().copied().unwrap_or(0);
            for (offset, payload) in seen.range(..=last) {
                assert_eq!(
                    held.get(offset),
                    Some(payload),
                    "the subscriber saw {:?} at {offset}, which {node} does not hold there",
                    String::from_utf8_lossy(payload)
                );
            }
            let payloads: Vec<Vec<u8>> = held.into_values().collect();
            if let Err(wrong) = sent.check(node, &payloads) {
                panic!("{wrong}");
            }
        }
    }
}

/// Ten records through a follower before anything moves.
async fn publish_before(cluster: &Cluster, set: &Set) -> Sent {
    let mut before = Sent::default();
    for n in 0..10 {
        publish_tried(
            cluster,
            &set.followers[0],
            &format!("before-{n}"),
            &mut before,
        )
        .await;
    }
    before
}

/// **A Raft move undone after the voters changed loses no acknowledged
/// write, and its subscriber sees every offset once, in order.** Four
/// brokers, one three-copy shard. A publisher and a subscriber from offset 0
/// run throughout an operator move to the broker outside the set, which
/// runs until the group reports the new voters, and a move back that
/// undoes it. Each blank and membership entry of those changes takes a
/// stream offset, as the offset is the Raft index, so the subscriber must
/// be told of each (`skipped_before`) and must see no other gap. The final
/// set holds every acknowledged record at its offset, and every record the
/// subscriber saw is the one the log holds there. The Raft half of
/// `routing/operator_moves.rs`'s `cancelling_a_fenced_move_loses_no_acknowledged_write`.
#[tokio::test]
#[serial]
async fn a_raft_move_cancelled_after_the_voters_changed_loses_no_acknowledged_write() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let set = Set::of(&cluster, converged_within).await;
    let before = publish_before(&cluster, &set).await;

    let traffic = Traffic::start(&cluster).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let step = cluster
        .start_move(STREAM, 0, &set.outsider)
        .await
        .expect("a move outside the set is taken");
    assert_eq!(step, "raft_replace");
    let mut moved = set.followers.clone();
    moved.push(set.outsider.clone());
    voters_become(&cluster, &moved, converged_within).await;

    set.move_back(&cluster, converged_within).await;
    traffic.finish(&cluster, before, &set.members).await;
    cluster.shutdown().await;
}

/// **A Raft move undone as soon as its destination votes loses no
/// acknowledged write, and its subscriber sees every offset once, in
/// order.** As above, but the move back is asked for the moment the group
/// reports the destination among its voters, which during the joint change
/// is the union of both sets, and is retried until the control plane takes
/// it. So the old set's target can come back while the group is still
/// leaving it. The set ends as it began, with the old leader leading, the
/// destination out of the voters, and every member holding every
/// acknowledged record at its offset.
#[tokio::test]
#[serial]
async fn a_raft_move_cancelled_once_its_destination_votes_loses_no_acknowledged_write() {
    if !opted_in() {
        return;
    }
    let converged_within = Duration::from_secs(180);
    let cluster = Cluster::start(four_broker_raft_cluster())
        .await
        .expect("start cluster");
    allow_follower_reads(&cluster).await;
    let set = Set::of(&cluster, converged_within).await;
    let before = publish_before(&cluster, &set).await;

    let traffic = Traffic::start(&cluster).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let step = cluster
        .start_move(STREAM, 0, &set.outsider)
        .await
        .expect("a move outside the set is taken");
    assert_eq!(step, "raft_replace");
    let voting =
        felix_cluster::wait::until_some(converged_within, "the destination to vote", || async {
            let (_, _, voters) = stored_set(&cluster).await;
            voters.contains(&set.outsider).then_some(voters)
        })
        .await;
    let voting = match voting {
        Ok(voters) => voters,
        Err(err) => panic!(
            "{} never voted: {err:#}; stored {:?}",
            set.outsider,
            stored_set(&cluster).await
        ),
    };
    println!(
        "moving back once the voters were {voting:?} ({})",
        if voting.len() > set.members.len() {
            "joint"
        } else {
            "final"
        }
    );

    set.move_back(&cluster, converged_within).await;
    traffic.finish(&cluster, before, &set.members).await;
    cluster.shutdown().await;
}
