//! Cross-broker conformance: the same assertions on one node and on three.
//!
//! Every scenario runs against both deployments. A cluster is not allowed to
//! have different semantics from a single broker — that equivalence is the
//! claim, and running one set of assertions against both is the only way to
//! keep making it as the cluster path grows.
//!
//! Everything here goes through the client-facing API. A test that reached into
//! broker internals could not tell the difference between a publish that was
//! routed correctly and one that was handled by the wrong broker, which is the
//! failure this suite exists to catch.
use anyhow::{Context, Result};
use felix_cluster::scenarios::{self, Ingress, Outcome};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::{ErrorCode, RetryClass};
use serial_test::serial;

/// A scenario's future, boxed so `on_both` can name it for any borrow of the
/// cluster it is handed.
type ScenarioFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Outcome>> + Send + 'a>>;

const STREAM: &str = "orders";
/// A second stream, so placement has more than one thing to distribute and the
/// suite is not describing a cluster where every shard happens to land together.
const OTHER: &str = "events";

fn config(nodes: usize) -> ClusterConfig {
    ClusterConfig {
        nodes,
        streams: vec![StreamSpec::new(STREAM, 1), StreamSpec::new(OTHER, 1)],
        ..Default::default()
    }
}

/// Run one scenario against both deployments.
///
/// The single-node run is what makes this a conformance suite rather than a
/// cluster test: a scenario that only passes with three brokers, or only with
/// one, is a semantic difference and fails here.
async fn on_both<F>(name: &str, scenario: F) -> Result<()>
where
    F: for<'a> Fn(&'a Cluster) -> ScenarioFuture<'a>,
{
    for nodes in [1usize, 3] {
        let cluster = Cluster::start(config(nodes)).await?;
        let outcome = scenario(&cluster).await;
        // Torn down before the result is unwrapped, so a failing scenario does
        // not also leave broker processes behind.
        cluster.shutdown().await;
        let outcome = outcome.map_err(|err| {
            anyhow::anyhow!("{name} failed on a {nodes}-node deployment: {err:#}")
        })?;

        if let Outcome::Skipped(why) = outcome {
            // Printed rather than silent: a scenario that skips everywhere is
            // covering nothing, and that should be visible in the log.
            println!("{name}: skipped on {nodes} node(s) — {why}");
        }
    }
    Ok(())
}

#[serial]
#[tokio::test]
async fn delivery_through_the_owner() -> Result<()> {
    on_both("delivery via owner", |cluster| {
        Box::pin(scenarios::delivery(cluster, STREAM, Ingress::Owner))
    })
    .await
}

#[serial]
#[tokio::test]
async fn delivery_through_a_non_owner() -> Result<()> {
    on_both("delivery via non-owner", |cluster| {
        Box::pin(scenarios::delivery(cluster, STREAM, Ingress::NonOwner))
    })
    .await
}

#[serial]
#[tokio::test]
async fn ordering_and_no_duplicates_through_the_owner() -> Result<()> {
    on_both("ordering via owner", |cluster| {
        Box::pin(scenarios::ordering_and_integrity(
            cluster,
            STREAM,
            Ingress::Owner,
            25,
        ))
    })
    .await
}

#[serial]
#[tokio::test]
async fn ordering_and_no_duplicates_through_a_non_owner() -> Result<()> {
    on_both("ordering via non-owner", |cluster| {
        Box::pin(scenarios::ordering_and_integrity(
            cluster,
            STREAM,
            Ingress::NonOwner,
            25,
        ))
    })
    .await
}

#[serial]
#[tokio::test]
async fn payloads_survive_the_crossing() -> Result<()> {
    on_both("payload integrity via non-owner", |cluster| {
        Box::pin(scenarios::payload_integrity(
            cluster,
            STREAM,
            Ingress::NonOwner,
        ))
    })
    .await
}

#[serial]
#[tokio::test]
async fn an_unknown_stream_is_refused_everywhere() -> Result<()> {
    on_both("unknown stream via non-owner", |cluster| {
        Box::pin(scenarios::unknown_stream_is_refused(
            cluster,
            Ingress::NonOwner,
        ))
    })
    .await?;
    on_both("unknown stream via owner", |cluster| {
        Box::pin(scenarios::unknown_stream_is_refused(
            cluster,
            Ingress::Owner,
        ))
    })
    .await
}

/// Routing must not launder authorization. A forwarded publish crosses a trust
/// boundary the client never sees.
#[serial]
#[tokio::test]
async fn a_forwarded_publish_is_still_authorized() -> Result<()> {
    on_both("unauthorized publish via non-owner", |cluster| {
        Box::pin(scenarios::unauthorized_publish_is_refused(
            cluster,
            STREAM,
            Ingress::NonOwner,
        ))
    })
    .await
}

/// A shard that moves converges: the old owner stops serving it locally and
/// starts forwarding to the new one, within a bounded time.
///
/// The move is a planned handoff -- staged, fenced, cut over -- so the old
/// owner has stopped serving before the new one is named, and publishes in
/// between are refused rather than written somewhere the new owner cannot
/// see. What this asserts is the routing half: the old owner ends up
/// forwarding. `tests/routing/rebalance.rs` asserts the data half.
#[serial]
#[tokio::test]
async fn a_moved_shard_converges_on_the_new_owner() -> Result<()> {
    let cluster = Cluster::start(config(3)).await?;

    let key = format!("{}/{}/{}/0", cluster.tenant_id, cluster.namespace, STREAM);
    let before = cluster.shard_assignments().await?;
    let before = before.get(&key).expect("assigned").clone();

    let new_owner = cluster.move_shard(STREAM).await?;
    assert_ne!(new_owner, before.leader, "the shard did not move");
    // Placement names a Raft shard's new leader before the group hands
    // leadership over.
    cluster
        .await_raft_leaders_placed(std::time::Duration::from_secs(60))
        .await?;

    let after = cluster.shard_assignments().await?;
    let after = after.get(&key).expect("assigned").clone();
    assert!(
        after.generation > before.generation,
        "a move must advance the generation: {} -> {}",
        before.generation,
        after.generation,
    );

    // Publish through the broker that used to own it until it forwards rather
    // than serving locally. That transition is convergence, and what follows
    // bounds how long it may take.
    //
    // A **deadline**, not an attempt count. Two hundred attempts is a duration
    // only if every attempt costs about the same, and they do not: an attempt
    // that fails immediately — the old owner has not seen the new assignment,
    // so the publish is refused rather than forwarded — is far cheaper than one
    // that succeeds. So the count ran out in seconds on a machine where the
    // assignment feed itself needed longer than that, which is how this failed
    // under coverage instrumentation while passing in eight times less wall
    // clock without it.
    let old_owner = before.leader.clone();
    let mut converged = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        let forwards_before = cluster
            .metric(&old_owner, "felix_broker_forwards_total")
            .await?
            .unwrap_or(0.0);
        let published = cluster
            .publish_via(&old_owner, STREAM, b"probe".to_vec())
            .await
            .is_ok();
        let forwards_after = cluster
            .metric(&old_owner, "felix_broker_forwards_total")
            .await?
            .unwrap_or(0.0);
        if published && forwards_after > forwards_before {
            converged = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        converged,
        "{old_owner} never started forwarding {key} after it moved to {new_owner} \
         (generation {} -> {})",
        before.generation, after.generation,
    );

    // Converged, so a record published through the old owner must now reach a
    // subscriber on the new one.
    let (_client, mut subscription) = cluster.subscribe_on(&new_owner, STREAM).await?;
    let payload = b"after-the-move".to_vec();
    cluster
        .publish_via(&old_owner, STREAM, payload.clone())
        .await?;

    let event = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        subscription.next_event(),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("the record never reached the new owner {new_owner} (shard={key})")
    })?
    .expect("subscription failed")
    .expect("subscription closed");
    assert_eq!(event.payload, payload);

    cluster.shutdown().await;
    Ok(())
}

/// A broker that cannot renew its lease stops serving.
///
/// This is the fence from `docs/replication-design.md`, end to end, and it is
/// what closes the write-loss path in #239: a broker whose authority has lapsed
/// refuses rather than writing a shard someone else may already lead.
///
/// The control plane is stopped rather than the network cut, because that is the
/// partition this harness can produce — and it is the same loss of authority
/// from the broker's point of view.
#[serial]
#[tokio::test]
async fn a_broker_that_cannot_renew_its_lease_stops_serving() -> Result<()> {
    if felix_cluster::skip_on_raft_retire_with_hybrid(
        "a broker that cannot renew its lease stops writing; Raft leadership rests on a quorum heard, not a lease",
    ) {
        return Ok(());
    }
    let mut cluster = Cluster::start(config(3)).await?;
    let owner = cluster.owner(STREAM).await?;

    // Serving normally, on a lease it is renewing.
    cluster
        .publish_via(&owner, STREAM, b"while-leased".to_vec())
        .await
        .expect("a leased broker must serve");

    cluster.stop_control_plane().await;

    // The lease outlives a brief outage — that is the point of a lease — and
    // then lapses. The harness runs a 1s expiry window, so this is a wide
    // margin around a short wait.
    //
    // A deadline rather than an attempt count, for the reason above: the
    // attempts that cost real time here are the ones that *succeed*, and a
    // count is only a duration if they all cost about the same.
    let mut refused = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if cluster
            .publish_via(&owner, STREAM, b"after-expiry".to_vec())
            .await
            .is_err()
        {
            refused = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        refused,
        "{owner} kept accepting writes after it could no longer renew its lease",
    );

    cluster.shutdown().await;
    Ok(())
}

/// A shard fenced halfway through a move refuses, and says the refusal is
/// safe to retry.
///
/// Held fenced rather than caught in passing: placement is not stepped past
/// the fence, so the refusal is there to be seen for as long as the test
/// looks, instead of in whatever window a live move happens to leave.
#[serial]
#[tokio::test]
async fn a_publish_refused_during_a_move_is_retryable() -> Result<()> {
    if felix_cluster::skip_on_raft_retire_with_hybrid(
        "hybrid-only: holds the draining fence between stage and cut-over, which Raft has no counterpart of; a Raft hand-over refusal is covered by raft_shards",
    ) {
        return Ok(());
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::replicated(STREAM, 1, 3)],
        ..Default::default()
    })
    .await?;
    let owner = cluster.fence_shard(STREAM).await?;

    // The owner hears of the fence from its assignment feed.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let refusal = loop {
        match cluster
            .publish_via(&owner, STREAM, b"mid-move".to_vec())
            .await
        {
            Err(err) => break err,
            Ok(()) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Ok(()) => panic!("{owner} kept serving {STREAM} after it was fenced"),
        }
    };
    // Asserted without formatting the refusal: it arrived over TLS, and a
    // failure message is not where that belongs.
    let typed = scenarios::broker_error(&refusal).expect("a refusal during a move had no code");
    assert!(
        matches!(
            typed.code,
            ErrorCode::ShardUnavailable | ErrorCode::NotLeader
        ),
        "a refusal during a move was not typed as an unavailable shard",
    );
    assert!(
        matches!(typed.retry, RetryClass::Retry | RetryClass::Redirect),
        "a refusal during a move was not sent as safe to retry",
    );

    cluster.shutdown().await;
    Ok(())
}

/// A leader cut off from its followers cannot say whether a `Quorum` write
/// will survive, and says so rather than refusing it.
#[serial]
#[tokio::test]
async fn a_write_no_majority_confirmed_is_outcome_unknown() -> Result<()> {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        ..Default::default()
    })
    .await?;
    let leader = cluster.owner(STREAM).await?;
    cluster
        .publish_via(&leader, STREAM, b"whole".to_vec())
        .await
        .context("publish while the cluster is whole")?;

    cluster.partition_node(&leader)?;
    let outcome = cluster
        .publish_via(&leader, STREAM, b"no majority".to_vec())
        .await;
    cluster.heal_partitions()?;

    let err = outcome.expect_err("a partitioned leader acknowledged a Quorum publish");
    let typed = scenarios::broker_error(&err).expect("a lost quorum had no code");
    assert_eq!(typed.code, ErrorCode::QuorumTimeout);
    assert_eq!(typed.retry, RetryClass::OutcomeUnknown);

    cluster.shutdown().await;
    Ok(())
}
