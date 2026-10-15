//! Correctness assertions that must hold on any deployment.
//!
//! Every scenario here runs unchanged against a single broker and against a
//! three-node cluster. That is the point: the semantics a client sees must not
//! depend on how many brokers there are, or on which one it happened to connect
//! to. A scenario that only makes sense in a cluster says so and skips.
//!
//! # Diagnosis
//!
//! Failures name the ingress broker, the owner, the shard, and the assignment
//! generation. Cross-broker failures are otherwise indistinguishable from one
//! another: "the record never arrived" is the same sentence whether the publish
//! was written by the wrong broker, refused by the right one, or lost between
//! them.
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_client::BrokerError;
use felix_wire::ErrorCode;

use crate::Cluster;

/// How long a delivery may take before the scenario calls it lost.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// The broker's count of lapsed leases.
const LEASE_EXPIRIES: &str = "felix_broker_lease_expiries_total";

/// Which broker a scenario publishes through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ingress {
    /// The broker that owns the shard. Always available.
    Owner,
    /// A broker that does not. Only exists in a cluster; scenarios asking for it
    /// on a single node are skipped rather than quietly run against the owner,
    /// which would assert nothing.
    NonOwner,
}

/// What a scenario did.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    /// The deployment cannot express this scenario — a non-owner on a
    /// single-node cluster. Reported, never silently treated as a pass.
    Skipped(&'static str),
}

/// A published record reaches a subscriber, whichever broker it was published
/// through.
///
/// The assertion the whole milestone rests on. It also checks that a publish
/// through a non-owner *was* forwarded: a broker that handled it locally
/// delivers the record to its own subscribers and looks correct from any single
/// vantage point.
pub async fn delivery(cluster: &Cluster, stream: &str, ingress: Ingress) -> Result<Outcome> {
    let (via, owner) = match ingress_node(cluster, stream, ingress).await? {
        Ok(pair) => pair,
        Err(skipped) => return Ok(skipped),
    };

    let (_client, mut subscription) = cluster.subscribe_on(&owner, stream).await?;
    let forwarded_before = forward_count(cluster, &via).await?;

    let payload = format!("delivery-{ingress:?}").into_bytes();
    cluster.publish_via(&via, stream, payload.clone()).await?;

    let forwarded_after = forward_count(cluster, &via).await?;
    match ingress {
        Ingress::NonOwner if forwarded_after <= forwarded_before => {
            bail!(
                "a non-owner handled the publish locally instead of forwarding it ({})",
                diagnose(cluster, stream, &via).await
            );
        }
        Ingress::Owner if forwarded_after > forwarded_before => {
            bail!(
                "the owner forwarded a shard it owns ({})",
                diagnose(cluster, stream, &via).await
            );
        }
        _ => {}
    }

    let event = tokio::time::timeout(DELIVERY_TIMEOUT, subscription.next_event())
        .await
        .with_context(|| {
            format!(
                "no delivery within {DELIVERY_TIMEOUT:?} ({})",
                "see the diagnosis below"
            )
        })
        .map_err(|err| {
            anyhow::anyhow!("{err}") // context is attached by the caller below
        })?
        .context("subscription failed")?;
    let Some(event) = event else {
        return Err(ended(
            cluster,
            stream,
            &via,
            &subscription,
            "subscription closed before the record arrived",
        )
        .await);
    };

    if event.payload != payload {
        bail!(
            "payload corrupted in transit: sent {:?}, received {:?} ({})",
            String::from_utf8_lossy(&payload),
            String::from_utf8_lossy(&event.payload),
            diagnose(cluster, stream, &via).await
        );
    }
    Ok(Outcome::Passed)
}

/// A batch published through one broker arrives in order, complete, and once.
///
/// Ordering, integrity, and duplicate-freedom are one scenario because they are
/// one property of the same sequence: checking them separately would need three
/// publishes and could pass on each while the stream was wrong.
pub async fn ordering_and_integrity(
    cluster: &Cluster,
    stream: &str,
    ingress: Ingress,
    count: usize,
) -> Result<Outcome> {
    let (via, owner) = match ingress_node(cluster, stream, ingress).await? {
        Ok(pair) => pair,
        Err(skipped) => return Ok(skipped),
    };

    let (_client, mut subscription) = cluster.subscribe_on(&owner, stream).await?;
    // One connection for the whole batch, as an application would publish it.
    // Acked one at a time, so each record is durable before the next is sent.
    let publisher_client = cluster.client_on(&via).await?;
    let publisher = publisher_client
        .publisher()
        .await
        .context("open publisher")?;
    for index in 0..count {
        publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                stream,
                record(index),
                felix_wire::AckMode::PerMessage,
            )
            .await
            .with_context(|| format!("publish {index} to {stream} via {via}"))?;
    }

    let mut received = Vec::with_capacity(count);
    for index in 0..count {
        let event = tokio::time::timeout(DELIVERY_TIMEOUT, subscription.next_event())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "only {index} of {count} records arrived within {DELIVERY_TIMEOUT:?}"
                )
            })?
            .context("subscription failed")?;
        let Some(event) = event else {
            return Err(ended(
                cluster,
                stream,
                &via,
                &subscription,
                &format!("subscription closed early, after {index} of {count} records"),
            )
            .await);
        };
        received.push(event.payload.to_vec());
    }

    let expected: Vec<Vec<u8>> = (0..count).map(record).collect();
    if received != expected {
        let context = diagnose(cluster, stream, &via).await;
        // Say which of the three properties broke, because the fixes differ.
        let mut sorted = received.clone();
        sorted.sort();
        sorted.dedup();
        if sorted.len() != received.len() {
            bail!("duplicate records delivered ({context})");
        }
        if sorted.len() != expected.len() {
            bail!(
                "expected {count} records, got {} ({context})",
                received.len()
            );
        }
        bail!("records delivered out of order ({context})");
    }

    // Nothing beyond the batch. A forward that was retried after an ambiguous
    // failure would show up here and nowhere else.
    match tokio::time::timeout(Duration::from_millis(500), subscription.next_event()).await {
        Err(_) => Ok(Outcome::Passed),
        Ok(Ok(Some(extra))) => bail!(
            "an extra record was delivered: {:?} ({})",
            String::from_utf8_lossy(&extra.payload),
            diagnose(cluster, stream, &via).await
        ),
        Ok(_) => Ok(Outcome::Passed),
    }
}

/// Bytes survive the crossing exactly, including ones a text-shaped path would
/// mangle.
pub async fn payload_integrity(
    cluster: &Cluster,
    stream: &str,
    ingress: Ingress,
) -> Result<Outcome> {
    let (via, owner) = match ingress_node(cluster, stream, ingress).await? {
        Ok(pair) => pair,
        Err(skipped) => return Ok(skipped),
    };

    // Every byte value, a NUL, and something long enough not to fit a small
    // buffer by accident.
    let mut payload: Vec<u8> = (0..=255u8).collect();
    payload.extend(std::iter::repeat_n(0u8, 64));
    payload.extend(b"tail");

    let (_client, mut subscription) = cluster.subscribe_on(&owner, stream).await?;
    cluster.publish_via(&via, stream, payload.clone()).await?;

    let event = tokio::time::timeout(DELIVERY_TIMEOUT, subscription.next_event())
        .await
        .map_err(|_| anyhow::anyhow!("no delivery within {DELIVERY_TIMEOUT:?}"))?
        .context("subscription failed")?;
    let Some(event) = event else {
        return Err(ended(cluster, stream, &via, &subscription, "subscription closed").await);
    };

    if event.payload.as_ref() != payload.as_slice() {
        bail!(
            "payload changed in transit: {} bytes sent, {} received ({})",
            payload.len(),
            event.payload.len(),
            diagnose(cluster, stream, &via).await
        );
    }
    Ok(Outcome::Passed)
}

/// A publish to a stream nobody registered is refused, with the same answer
/// whichever broker it arrives at.
///
/// The interesting half is the non-owner: an unknown stream must not become a
/// forward to an owner that does not exist, and must not be answered
/// differently just because routing was involved.
pub async fn unknown_stream_is_refused(cluster: &Cluster, ingress: Ingress) -> Result<Outcome> {
    // Resolved against a stream that does exist, because the point is to vary
    // the ingress broker, not to find one for a stream with no owner.
    let (via, _) = match ingress_node(cluster, "orders", ingress).await? {
        Ok(pair) => pair,
        Err(skipped) => return Ok(skipped),
    };

    match cluster
        .publish_via(&via, "no-such-stream", b"nowhere".to_vec())
        .await
    {
        Ok(()) => bail!("publishing to an unregistered stream succeeded via {via}"),
        Err(err) => match broker_error(&err) {
            Some(typed) if typed.code == ErrorCode::NotFound => Ok(Outcome::Passed),
            // A broker with no assignment for the shard cannot tell an
            // unregistered stream from one not placed yet, and says the
            // latter. The reason is not checked: a binary ack carries no detail.
            Some(typed) if typed.code == ErrorCode::ShardUnavailable => Ok(Outcome::Passed),
            _ => expect_code(&err, ErrorCode::NotFound, &via),
        },
    }
}

/// A credential without publish permission is refused, and routing does not
/// launder it.
///
/// A forwarded publish crosses a trust boundary the client never sees. If
/// authorization were checked only at the owner, or only at the ingress broker
/// and then trusted, this is where that shows.
pub async fn unauthorized_publish_is_refused(
    cluster: &Cluster,
    stream: &str,
    ingress: Ingress,
) -> Result<Outcome> {
    let (via, owner) = match ingress_node(cluster, stream, ingress).await? {
        Ok(pair) => pair,
        Err(skipped) => return Ok(skipped),
    };

    // The credential has to be good for something first. A token rejected
    // outright — a malformed one, or one carrying an action the broker does not
    // recognise — would fail the publish below for the wrong reason and make
    // this scenario pass while asserting nothing.
    //
    // Checked against the owner, not `via`. A subscribe to a broker that does
    // not own the shard is answered with a redirect (#118), which says nothing
    // about the credential — and this is a question about the credential.
    // Routing is what the publish below exercises.
    cluster
        .subscribe_on_with_token(&owner, stream, &cluster.subscribe_only_token())
        .await
        .context("the subscribe-only credential could not subscribe, so the publish check below would prove nothing")?;

    match cluster
        .publish_via_token(
            &via,
            stream,
            b"denied".to_vec(),
            &cluster.subscribe_only_token(),
        )
        .await
    {
        Ok(()) => bail!(
            "a token without stream.publish was accepted ({})",
            diagnose(cluster, stream, &via).await
        ),
        Err(err) => expect_code(&err, ErrorCode::Forbidden, &via),
    }
}

/// The broker's typed error under whatever context the client added.
pub fn broker_error(err: &anyhow::Error) -> Option<&BrokerError> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<BrokerError>())
}

/// A refusal is typed, and typed as `code`: a client branches on the code, so
/// an untyped or mistyped refusal is a failure even when the request was
/// refused.
fn expect_code(err: &anyhow::Error, code: ErrorCode, via: &str) -> Result<Outcome> {
    match broker_error(err) {
        Some(typed) if typed.code == code => Ok(Outcome::Passed),
        Some(_) => bail!("refused via {via} with a code other than {code}"),
        None => bail!("refused via {via} without a code, expected {code}"),
    }
}

/// Context for a failure message.
async fn diagnose(cluster: &Cluster, stream: &str, ingress: &str) -> String {
    let key = format!("{}/{}/{}/0", cluster.tenant_id, cluster.namespace, stream);
    match cluster.shard_assignments().await {
        Ok(map) => match map.get(&key) {
            Some(assignment) => format!(
                "ingress={ingress} owner={} shard={key} generation={}",
                assignment.leader, assignment.generation
            ),
            None => format!("ingress={ingress} shard={key} (unassigned)"),
        },
        Err(err) => format!("ingress={ingress} shard={key} (assignments unreadable: {err})"),
    }
}

/// Why a subscription ended with `None`, for a failure to say.
///
/// A plain subscription ends cleanly for two reasons with different fixes:
/// the broker said the shard moved, or it closed the stream without a word. A
/// lapsed lease also reads as a move, one that names no owner, so each
/// broker's lapse count goes with it.
async fn ended(
    cluster: &Cluster,
    stream: &str,
    ingress: &str,
    subscription: &felix_client::Subscription,
    what: &str,
) -> anyhow::Error {
    let why = match subscription.shard_moved() {
        Some(moved) => format!(
            "the broker said the shard moved (to {}, generation {}, resume from {})",
            moved.node_id.as_deref().unwrap_or("no named owner"),
            moved.generation,
            moved
                .resume_from
                .map_or_else(|| "nowhere".to_string(), |offset| offset.to_string()),
        ),
        None => "the broker finished the event stream without saying why".to_string(),
    };
    let mut lapses = Vec::new();
    for node_id in cluster.node_ids() {
        let count = match cluster.metric(&node_id, LEASE_EXPIRIES).await {
            Ok(count) => count.unwrap_or(0.0).to_string(),
            Err(_) => "?".to_string(),
        };
        lapses.push(format!("{node_id}={count}"));
    }
    anyhow::anyhow!(
        "{what}: {why}; lease lapses {} ({})",
        lapses.join(" "),
        diagnose(cluster, stream, ingress).await
    )
}

/// Resolve which broker to publish through, or say why the scenario cannot run.
async fn ingress_node(
    cluster: &Cluster,
    stream: &str,
    ingress: Ingress,
) -> Result<Result<(String, String), Outcome>> {
    let owner = cluster.owner(stream).await?;
    match ingress {
        Ingress::Owner => Ok(Ok((owner.clone(), owner))),
        Ingress::NonOwner => match cluster.owner_and_non_owner(stream).await {
            Ok((owner, non_owner)) => Ok(Ok((non_owner, owner))),
            // A single-node deployment has an owner and nobody else. Skipped
            // rather than run against the owner, which would assert nothing
            // while looking like coverage.
            Err(_) => Ok(Err(Outcome::Skipped(
                "no non-owner exists on a single-node deployment",
            ))),
        },
    }
}

fn record(index: usize) -> Vec<u8> {
    format!("record-{index:06}").into_bytes()
}

async fn forward_count(cluster: &Cluster, node_id: &str) -> Result<f64> {
    Ok(cluster
        .metric(node_id, "felix_broker_forwards_total")
        .await?
        .unwrap_or(0.0))
}
