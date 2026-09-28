//! The quorum mark: how far a majority of the replica set holds the shard.

use super::*;

/// Answers at once for everyone but one node, which it keeps waiting.
struct OneSlowFollower {
    slow: &'static str,
    delay: Duration,
}

impl PeerRequester for OneSlowFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        if node_id == self.slow {
            tokio::time::sleep(self.delay).await;
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// **The mark advances at the majority, not at the last follower.**
///
/// With three replicas a record is on a majority the moment one follower has
/// it; the second is a durability margin, not a precondition. Waiting for every
/// follower put one dead or slow replica's whole timeout in front of every
/// acknowledgement on the shard, every pass — the failure `Quorum` exists to
/// tolerate, turned into the thing that stalls it (#411).
///
/// The clock is the assertion: the mark has to reach the tail long before the
/// slow follower answers at all.
#[tokio::test(start_paused = true)]
async fn the_mark_advances_at_the_majority_not_at_the_slowest_follower() {
    const SLOW: Duration = Duration::from_secs(60);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = OneSlowFollower {
        slow: "broker-c",
        delay: SLOW,
    };
    let marks = QuorumMarks::new();
    let watched = watch_key(&key());
    let started = tokio::time::Instant::now();

    let observe = async {
        loop {
            if marks.offset(&watched, 4) == Some(3) {
                return tokio::time::Instant::now().duration_since(started);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    let (mut cursors, mut group, mut dead, mut counters) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    let (_, reached) = tokio::join!(
        replicate_once(
            &follower,
            &broker,
            &router,
            &marks,
            None,
            &mut cursors,
            &mut group,
            &mut dead,
            &mut counters,
        ),
        observe,
    );

    assert!(
        reached < SLOW,
        "the majority held every record after {reached:?}, but the mark waited \
         {SLOW:?} for the slowest follower",
    );
}

/// The slow follower is still shipped to and still ends the pass at the tail.
/// Publishing at the majority is about when the mark moves, not about giving up
/// on the rest of the replica set.
#[tokio::test(start_paused = true)]
async fn the_slower_follower_is_still_caught_up_by_the_end_of_the_pass() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], 4);
    let follower = OneSlowFollower {
        slow: "broker-c",
        delay: Duration::from_secs(60),
    };
    let marks = QuorumMarks::new();

    let pass = replicate_once(
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

    let mut caught_up = pass.reports[0].caught_up.clone();
    caught_up.sort();
    assert_eq!(
        caught_up,
        vec!["broker-b".to_string(), "broker-c".to_string()],
        "the slow follower was abandoned rather than waited for",
    );
    let offsets: Map<String, u64> = pass.reports[0].offsets.iter().cloned().collect();
    assert_eq!(offsets.get("broker-c"), Some(&3));
}

/// **A record the only follower disagrees with is never acknowledged.**
///
/// This is the end of the chain #406 described. A follower keeps an orphan from
/// a dead leader; the new leader reuses that offset for its own record; and if
/// the mark moved past it anyway, `Quorum` would acknowledge a record no
/// majority holds — and the follower, looking level, would then be promoted
/// over it.
///
/// The two links before this one are checked next door, on a real follower log:
/// `a_batch_wholly_overlapping_does_not_confirm_past_itself` and
/// `an_orphan_at_a_reused_offset_is_reported_as_a_conflict` in
/// `felix-broker`. This is the part that decides whether a client is told yes.
#[tokio::test]
async fn a_quorum_mark_does_not_pass_a_record_the_follower_disagrees_with() {
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 4);
    let marks = QuorumMarks::new();

    replicate_once(
        &DivergingFollower,
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

    // What holds here is that the mark is derived from where followers actually
    // are: this one never got past its first batch, so no majority reaches the
    // leader's tail and there is nothing to mark. A mark taken from the
    // leader's own tail instead fails this with "reached Some(3)", which is
    // what acknowledging on the leader's word alone would look like.
    let mark = marks.offset(&watch_key(&key()), 4);
    assert!(
        mark.is_none_or(|offset| offset < 3),
        "the quorum mark reached {mark:?} with the only follower in \
         disagreement, so a record no majority holds was acknowledged",
    );
}
