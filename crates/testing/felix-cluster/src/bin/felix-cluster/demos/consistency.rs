//! `consistency`: what `Quorum` buys and what `Leader` costs, under one fault.

use std::time::Duration;

use anyhow::{Result, bail};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, wait};

use super::{beat, step};
use crate::args::pace_from;
use crate::init_tracing;

mod on_raft;

/// What `Quorum` buys and what `Leader` costs, under the same fault.
///
/// The failover demo shows a quorum-acknowledged record surviving. This is its
/// counterpart, and the harder half to look at: the same fault applied to a
/// `Leader` stream loses an acknowledged record.
///
/// The fault is a leader cut off from its replicas — frozen followers, so the
/// leader is healthy and alone. Each stream's own followers are frozen in turn,
/// so neither run depends on the two streams happening to share a leader, and
/// the demo cannot flake on where placement put them.
pub(crate) async fn consistency(args: &[String]) -> Result<()> {
    init_tracing(false);
    let pace = pace_from(args)?;
    // What follows is the hybrid engine's story, where a `Leader` shard goes
    // unavailable. On Raft it stays up and loses records instead.
    if felix_cluster::suite_runs_on_raft() {
        return on_raft::consistency(pace).await;
    }
    const QUORUM_STREAM: &str = "orders-quorum";
    const LEADER_STREAM: &str = "orders-leader";

    step("Two streams, identical but for one field");
    println!("  Three brokers. Both streams are replicated three ways, so every");
    println!("  broker holds every record. The only difference is `consistency`:");
    println!("    {QUORUM_STREAM:<16} Quorum — a majority must hold it before it is acknowledged");
    println!("    {LEADER_STREAM:<16} Leader — the leader alone decides, which is the default");
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![
            StreamSpec::quorum(QUORUM_STREAM, 1, 3),
            StreamSpec::replicated(LEADER_STREAM, 1, 3),
        ],
        ..Default::default()
    })
    .await?;
    beat(pace).await;

    // ---- the same fault, applied to each stream in turn -------------------
    // Each phase returns whether the record published during the isolation was
    // acknowledged, and whether the shard ended up serving without it.
    let (quorum_acked, _) =
        run_consistency_phase(&mut cluster, QUORUM_STREAM, "Quorum", pace, false).await?;
    let (_, leader_served_without_it) =
        run_consistency_phase(&mut cluster, LEADER_STREAM, "Leader", pace, true).await?;
    let quorum_refused = !quorum_acked;

    step("The trade, stated plainly");
    println!("  The same fault — a leader cut off from its replicas — asked of both:");
    println!();
    println!("    Quorum   refused the write, while the shard stayed available");
    println!("             throughout. The publisher was told it could not be");
    println!("             vouched for, and could decide what to do about that.");
    println!("    Leader   took the write, and the shard went unavailable when the");
    println!("             leader died. Nothing was served that contradicted the");
    println!("             acknowledgement, but nothing was served at all.");
    println!();
    println!("  Neither of those is data loss, and that is the point worth taking away.");
    println!(
        "  `Leader` does not trade safety for latency — it trades \x1b[1mavailability\x1b[0m for"
    );
    println!("  latency, and it moves the moment you find out. Quorum tells you at");
    println!("  publish time, when you still hold the record and can retry. Leader");
    println!("  tells you at failover time, when the only copy is on a dead broker's");
    println!("  disk and you are waiting for it to come back.");
    println!();
    println!("  `Leader` is the default because one round trip beats two and most");
    println!("  streams would rather have the latency than the guarantee. Set");
    println!("  `consistency: Quorum` on the ones that would rather be refused.");

    if !quorum_refused {
        bail!("a Quorum publish was acknowledged with no reachable majority");
    }
    if leader_served_without_it {
        bail!(
            "a shard was served without a record its leader had acknowledged — that is \
             silent data loss, not the unavailability this demo expects"
        );
    }
    Ok(())
}

/// Publish under one consistency level with the leader cut off from its
/// replicas, then kill the leader and read the stream back from the promotion.
///
/// Returns `(acknowledged, served_without_it)`: whether the record published
/// during the isolation was acknowledged, and whether the shard was afterwards
/// served by a broker that does not have it. The second is the one that would
/// be silent data loss.
async fn run_consistency_phase(
    cluster: &mut Cluster,
    stream: &str,
    level: &str,
    pace: Duration,
    kill_the_leader: bool,
) -> Result<(bool, bool)> {
    step(&format!("{level}: two records the normal way"));
    let leader = cluster.owner(stream).await?;
    println!("  {stream} is led by {leader}.");
    for index in 1..=2u32 {
        cluster
            .publish_via_any(stream, format!("{level}-before-{index}").into_bytes())
            .await?;
        println!("  acknowledged: {level}-before-{index}");
    }
    wait::until(Duration::from_secs(20), "replication to settle", || async {
        matches!(
            cluster
                .metric(&leader, "felix_broker_replication_lag_records")
                .await,
            Ok(Some(lag)) if lag == 0.0
        )
    })
    .await?;
    println!("  Replication lag on {leader}: 0 — both replicas hold both records.");
    beat(pace).await;

    step(&format!("{level}: cutting {leader} off from its replicas"));
    let followers: Vec<String> = cluster
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .filter(|id| id != &leader)
        .collect();
    for follower in &followers {
        cluster.pause_node(follower)?;
    }
    println!(
        "  {} frozen. Not killed — {leader} still believes they are there,",
        followers.join(" and ")
    );
    println!("  and keeps trying to ship to them. It is healthy and alone.");
    beat(pace).await;

    step(&format!("{level}: publishing while alone"));
    let attempted = format!("{level}-while-alone");
    let outcome = cluster
        .publish_via_any(stream, attempted.clone().into_bytes())
        .await;
    let acknowledged = outcome.is_ok();
    match &outcome {
        Ok(()) => {
            println!("  acknowledged: {attempted}");
            println!("  The leader had it durably, and that was enough to answer the client.");
        }
        Err(err) => {
            println!("  refused: {attempted}");
            // `{:#}` so the reason the broker gave is shown, and not just the
            // harness's context line wrapped around it.
            println!("  \"{err:#}\"");
            println!("  No majority could be reached, so the write was not acknowledged.");
            println!("  A publisher that sees this knows to retry. That is the point.");
        }
    }
    beat(pace).await;

    if !kill_the_leader {
        for follower in &followers {
            cluster.resume_node(follower)?;
        }
        println!("\n  Replicas resumed.");
        step(&format!("{level}: reading the stream back"));
        let survived = replay_contains(cluster, &leader, stream, &attempted).await?;
        report_replay(acknowledged, survived, &attempted);
        beat(pace).await;
        // The leader is still up, so nothing was served in its absence.
        return Ok((acknowledged, false));
    }

    // The leader dies while its replicas are still frozen, so they never
    // receive what it acknowledged alone. Resuming first would let replication
    // catch up and hand the record over, which is the opposite of the point.
    step(&format!(
        "{level}: killing {leader} before its replicas can catch up"
    ));
    cluster.kill_node(&leader)?;
    println!("  Gone, with the replicas still frozen. Whatever it acknowledged");
    println!("  alone went with it.");
    for follower in &followers {
        cluster.resume_node(follower)?;
    }
    println!("  Replicas resumed. They are what is left of the shard.");
    beat(pace).await;

    step(&format!("{level}: nobody is promoted"));
    // Long enough to be a conclusion rather than a race: the failover demo sees
    // a promotion inside a second when one is possible at all.
    let settle = Duration::from_secs(15);
    let deadline = std::time::Instant::now() + settle;
    let mut promoted = None;
    while std::time::Instant::now() < deadline {
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(stream).await
            && owner != leader
        {
            promoted = Some(owner);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    if let Some(promoted) = promoted {
        println!("  {promoted} was promoted after all.");
        let survived = replay_contains(cluster, &promoted, stream, &attempted).await?;
        report_replay(acknowledged, survived, &attempted);
        return Ok((acknowledged, acknowledged && !survived));
    }

    println!("  Still no owner after {settle:?}, and the control plane says why:");
    println!();
    println!("      the leader is gone and no replica holding this shard's log can take over");
    println!();
    println!("  Both replicas are alive and reachable. Neither is allowed to serve,");
    println!("  because {leader} acknowledged a record neither of them has. Promoting");
    println!("  one would open the shard \x1b[1mwithout\x1b[0m `{attempted}` — and a client that");
    println!("  read it would be told, in effect, that the record never existed.");
    println!();
    println!(
        "  \x1b[1mSo the shard is unavailable instead.\x1b[0m That is the cost of `Leader`, and"
    );
    println!("  it is the deliberate choice: a silently empty shard is worse than an");
    println!("  absent one, because nothing downstream can detect it. The shard comes");
    println!("  back when {leader} does, with its disk — that is where the record is.");
    beat(pace).await;
    // Unavailable, not lost: nothing was served that contradicts the ack.
    Ok((acknowledged, false))
}

/// Replay a stream from one broker and say whether `needle` is in it, printing
/// what was actually read so the answer is visible rather than asserted.
async fn replay_contains(
    cluster: &Cluster,
    node_id: &str,
    stream: &str,
    needle: &str,
) -> Result<bool> {
    let (_client, mut subscription) = cluster.replay_on(node_id, stream).await?;
    let mut seen = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).to_string();
        // The harness publishes a probe per broker at startup to prove the
        // cluster can serve before anything is measured; it is not this demo's.
        if payload == "harness-probe" {
            continue;
        }
        let offset = event
            .offset
            .map(|o| o.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!("      ← offset {offset:>4}  {payload}");
        seen.push(payload);
    }
    Ok(seen.iter().any(|payload| payload == needle))
}

/// The four combinations of "was it acknowledged" and "is it still there", so
/// the one that matters is named rather than left to be noticed.
fn report_replay(acknowledged: bool, survived: bool, record: &str) {
    println!();
    match (acknowledged, survived) {
        (true, true) => println!("  `{record}` was acknowledged and is still here."),
        (true, false) => {
            println!("  \x1b[1m`{record}` was acknowledged and is gone.\x1b[0m");
            println!("  Nothing lied about it afterwards — the cluster simply does not");
            println!("  have a record it told a client it had.");
        }
        (false, false) => {
            println!("  \x1b[1m`{record}` was refused and is not here.\x1b[0m");
            println!("  The two answers agree, which is the only thing being asked of it.");
        }
        (false, true) => {
            println!("  `{record}` was refused and is here anyway — the write landed after");
            println!("  the answer. Not loss, but a publisher cannot tell the difference,");
            println!("  which is why a refusal means \"unknown\" and not \"did not happen\".")
        }
    }
}
