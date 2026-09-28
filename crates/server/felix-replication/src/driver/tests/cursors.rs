//! Where a follower's cursor starts, and when it starts again.

use super::*;

/// **A failover does not re-ship the whole log.**
///
/// Cursors used to start at zero, so every follower of every shard the failed
/// broker led byte-compared the entire log — records it already held, read off
/// the leader's disk and pushed across the network, before anything new could
/// move. On a log of any size that is the difference between a failover and an
/// outage.
///
/// The generation history is the bound: below where this leadership began, both
/// logs came from the same predecessor. One record earlier than that, so the
/// boundary is compared rather than assumed.
#[tokio::test]
async fn a_fresh_cursor_starts_at_the_generation_boundary() {
    let (broker, _dir) = leader_led_from(20, 4, 12).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(
        follower.batches().first().map(|(_, first, _)| *first),
        Some(11),
        "the follower was re-sent records from before this leadership began",
    );
}

/// Without a history there is nothing to bound the comparison with, so it
/// starts at zero — slow, and what every shard did before generations were
/// recorded. Worth pinning: the fallback is what keeps a shard written by an
/// older build replicating at all.
#[tokio::test]
async fn a_shard_with_no_generation_history_still_starts_at_zero() {
    let (broker, _dir) = leader_with(20).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(
        follower.batches().first().map(|(_, first, _)| *first),
        Some(0),
    );
}

/// The boundary bounds a replica added mid-generation too. It is not assumed
/// caught up — it starts below where this leadership began, and a `LogGap`
/// still rewinds the leader if it turns out to hold less than that.
#[tokio::test]
async fn a_replica_added_later_starts_at_the_boundary_too() {
    let (broker, _dir) = leader_led_from(20, 4, 12).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    publish(&router, LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let from_c: Vec<(String, u64, usize)> = follower
        .batches()
        .into_iter()
        .filter(|(node, _, _)| node == "broker-c")
        .collect();
    assert_eq!(
        from_c.first().map(|(_, first, _)| *first),
        Some(11),
        "a newly added replica was sent the whole log",
    );
}

#[test]
fn the_comparison_point_is_one_below_where_the_generation_began() {
    use felix_storage::log::Epoch;
    let history = [
        Epoch {
            generation: 3,
            start_offset: 40,
        },
        Epoch {
            generation: 7,
            start_offset: 90,
        },
    ];

    assert_eq!(compare_from(&history, 7), 89);
    assert_eq!(compare_from(&history, 3), 39);
    // A generation this log never recorded cannot bound anything, and neither
    // can one that began at zero.
    assert_eq!(compare_from(&history, 8), 0);
    assert_eq!(compare_from(&[], 7), 0);
    assert_eq!(
        compare_from(
            &[Epoch {
                generation: 1,
                start_offset: 0
            }],
            1
        ),
        0,
    );
}

/// **A new generation discards the cursors.** A cursor is a belief about where
/// a follower stood under a particular leadership; a new one invalidates the
/// belief, not the follower.
#[tokio::test]
async fn a_new_generation_starts_the_cursors_again() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    let after_first = follower.batches().len();

    publish(&router, LOCAL, &["broker-b"], 5);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert!(
        follower.batches().len() > after_first,
        "a new generation reused the old generation's cursor",
    );
    let (_, batch) = &follower.sent.lock().expect("lock")[after_first];
    assert_eq!(
        batch.shard.generation, 5,
        "the old epoch was still being sent"
    );
}

/// A replica added to the set starts from zero rather than from the tail. The
/// leader does not know what it holds, and starting at the tail would declare
/// it caught up while it held nothing.
#[tokio::test]
async fn a_replica_added_later_starts_from_the_beginning() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    publish(&router, LOCAL, &["broker-b", "broker-c"], 4);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let from_c: Vec<(String, u64, usize)> = follower
        .batches()
        .into_iter()
        .filter(|(node, _, _)| node == "broker-c")
        .collect();
    assert_eq!(
        from_c.first().map(|(_, first, _)| *first),
        Some(0),
        "a newly added replica was assumed to be caught up",
    );
}

/// A replica dropped from the set stops being shipped to.
#[tokio::test]
async fn a_replica_removed_from_the_set_is_dropped() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = AcceptingFollower::default();
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    publish(&router, LOCAL, &["broker-b"], 4);
    // Something new to ship, so a still-registered follower would show up.
    let storage = broker.durable_storage().expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    log.append(&[Bytes::from_static(b"more")])
        .await
        .expect("append");

    let before = follower.batches().len();
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    let after: Vec<String> = follower
        .batches()
        .into_iter()
        .skip(before)
        .map(|(node, _, _)| node)
        .collect();
    assert!(
        !after.contains(&"broker-c".to_string()),
        "a replica removed from the set was still shipped to",
    );
}

/// A follower only reachable at one address; anything sent elsewhere is a
/// dial to a port nobody listens on any more.
struct FollowerAt {
    addr: SocketAddr,
    dialled: Mutex<Vec<SocketAddr>>,
    stored: AcceptingFollower,
}

impl PeerRequester for FollowerAt {
    async fn request(
        &self,
        node_id: &str,
        addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        self.dialled.lock().expect("lock").push(addr);
        if addr != self.addr {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "connection refused".to_string(),
            });
        }
        self.stored.request(node_id, addr, message).await
    }
}

/// **A follower that comes back at a new address is shipped to there.**
///
/// A restarted broker re-registers on new ports, but its leader's generation
/// does not change, so the cursor outlives the restart. The address has to
/// come from the current route rather than from the cursor, or the leader
/// dials the dead port for the rest of the generation and a move onto the
/// restarted broker never catches up.
#[tokio::test]
async fn a_follower_that_moved_address_is_shipped_to_at_the_new_one() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let moved: SocketAddr = "10.0.0.2:9002".parse().expect("addr");
    let follower = FollowerAt {
        addr: moved,
        dialled: Mutex::new(Vec::new()),
        stored: AcceptingFollower::default(),
    };
    let marks = QuorumMarks::new();
    let mut cursors = HashMap::new();

    // The first pass reaches only the old address, which is gone.
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;
    assert!(follower.stored.batches().is_empty());

    // Same generation, same replica set; only the catalog's address changed.
    let mut nodes = nodes();
    nodes.get_mut("broker-b").expect("broker-b").advertise_addr = moved;
    let table = RoutingTable::build(
        [(key(), LOCAL.to_string(), vec!["broker-b".to_string()], 4)],
        &nodes,
    );
    router.publish(table, &nodes);
    replicate_once(
        &follower,
        &broker,
        &router,
        &marks,
        None,
        &mut cursors,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
    )
    .await;

    assert_eq!(
        follower.dialled.lock().expect("lock").last(),
        Some(&moved),
        "the leader kept dialling the address the follower had before",
    );
    assert_eq!(
        follower
            .stored
            .batches()
            .first()
            .map(|(_, first, n)| (*first, *n)),
        Some((0, 3)),
    );
}
