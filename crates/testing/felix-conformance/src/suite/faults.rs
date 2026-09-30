//! The catalogue's connection-fault scenarios, run with `felix-client`
//! through a [`link::Interposer`] in front of the in-process broker.
//!
//! Each scenario runs twice for a subscribe step: once through
//! `ClusterClient`, which should resume, and once through a plain `Client`,
//! which has nowhere to resume to and must report the loss. A publish step runs
//! through `ClusterClient` only, since a plain `Client` cannot reconnect and
//! "publishes again afterwards" is part of the bar; it runs a second time with
//! an idempotent producer pipelining its batches, which must land every record
//! exactly once. They all run at once, each on a stream and an interposer of
//! its own.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use felix_broker::{Broker, StreamMetadata};
use felix_client::{Client, ClientConfig, ClusterClient, ClusterSubscription, Event, Subscription};
use felix_conformance::kit::{self, FaultScenario, Phase};
use felix_conformance::link::{Interposer, LinkFault};
use felix_wire::{AckMode, StartPosition};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};

const TENANT: &str = "t1";
const NAMESPACE: &str = "default";

/// How long a subscriber waits for one record before polling again.
///
/// Deliberately short: bindings read with a timeout and call again, which
/// drops the read in flight, and a client has to survive that mid-resume.
const POLL: Duration = Duration::from_millis(250);

/// Room for the idle timeout to fire and a reconnect to land, on top of the
/// fault's own hold.
const SETTLE: Duration = Duration::from_secs(45);

#[derive(Clone, Copy, Debug)]
enum Via {
    Cluster,
    Plain,
    /// An idempotent producer on `ClusterClient`, with a window of batches in
    /// flight when the fault lands.
    Pipelined,
}

/// One-record batches per call in a pipelined publish case: enough that a
/// window's worth is unanswered when the fault lands.
const PIPELINED_BATCHES: usize = 32;

pub(crate) async fn run_link_faults(
    broker: &Arc<Broker>,
    broker_addr: SocketAddr,
    config: ClientConfig,
) -> Result<()> {
    println!("Running connection-fault checks...");
    let cases = kit::catalogue()?.fault_scenarios();
    if cases.is_empty() {
        bail!("the catalogue has no fault scenarios");
    }

    let mut runs = JoinSet::new();
    for (index, case) in cases.into_iter().enumerate() {
        let vias: &[Via] = match case.step.during {
            Phase::Publish => &[Via::Cluster, Via::Pipelined],
            Phase::Subscribe => &[Via::Cluster, Via::Plain],
        };
        for (n, via) in vias.iter().enumerate() {
            let stream = format!("link-fault-{index}-{n}");
            broker
                .register_stream(
                    TENANT,
                    NAMESPACE,
                    &stream,
                    StreamMetadata {
                        durable: true,
                        ..Default::default()
                    },
                )
                .await?;
            let case = case.clone();
            let config = config.clone();
            let via = *via;
            runs.spawn(async move {
                let label = format!("{} via {via:?}", case.id);
                let outcome = run_case(&case, via, &stream, broker_addr, config).await;
                (label, outcome)
            });
        }
    }

    let mut failures = Vec::new();
    while let Some(joined) = runs.join_next().await {
        let (label, outcome) = joined.context("a fault case panicked")?;
        match outcome {
            Ok(note) => println!("  {label}: {note}"),
            Err(err) => failures.push(format!("{label}: {err:#}")),
        }
    }
    if !failures.is_empty() {
        bail!(
            "connection-fault checks failed:\n  {}",
            failures.join("\n  ")
        );
    }
    Ok(())
}

async fn run_case(
    case: &FaultScenario,
    via: Via,
    stream: &str,
    broker_addr: SocketAddr,
    config: ClientConfig,
) -> Result<&'static str> {
    let link = Interposer::start(broker_addr).await?;
    let direct = Client::connect(broker_addr, "localhost", config.clone()).await?;
    match case.step.during {
        Phase::Publish if matches!(via, Via::Pipelined) => {
            pipelined_case(case, &link, &direct, stream, config).await
        }
        Phase::Publish => publish_case(case, &link, &direct, stream, config).await,
        Phase::Subscribe => subscribe_case(case, via, &link, &direct, stream, config).await,
    }
}

async fn publish_case(
    case: &FaultScenario,
    link: &Interposer,
    direct: &Client,
    stream: &str,
    config: ClientConfig,
) -> Result<&'static str> {
    let step = &case.step;
    let hold = Duration::from_millis(step.hold_ms);
    let cluster = ClusterClient::connect(&[link.addr()], "localhost", config).await?;
    let mut acked = Vec::new();
    let mut errors = 0;
    let mut healed_at = None;
    for index in 0..step.records {
        if index == step.after_records {
            link.inject(step.fault, hold);
            healed_at = Some(Instant::now() + hold);
        }
        let payload = format!("{}-{index}", case.id).into_bytes();
        let publish = cluster.publish(
            TENANT,
            NAMESPACE,
            stream,
            payload.clone(),
            AckMode::PerMessage,
        );
        match timeout(hold + SETTLE, publish).await {
            Err(_) => bail!("publish {index} neither returned nor failed"),
            Ok(Ok(_)) => acked.push(payload),
            Ok(Err(err)) if step.fault == LinkFault::Stall => {
                bail!("publish {index} failed during a stall: {err:#}")
            }
            Ok(Err(_)) => errors += 1,
        }
    }

    // The client may have given up on the link while the fault held; once
    // it is over, it has to be able to publish again.
    if let Some(at) = healed_at {
        tokio::time::sleep_until(at).await;
    }
    let deadline = Instant::now() + SETTLE;
    loop {
        let payload = format!("{}-after", case.id).into_bytes();
        match cluster
            .publish(
                TENANT,
                NAMESPACE,
                stream,
                payload.clone(),
                AckMode::PerMessage,
            )
            .await
        {
            Ok(_) => {
                acked.push(payload);
                break;
            }
            Err(err) if Instant::now() >= deadline => {
                bail!("the client never published again after the fault: {err:#}")
            }
            Err(_) => tokio::time::sleep(POLL).await,
        }
    }

    // Read back on an untouched connection: an acknowledged record that is
    // not there was lost.
    let mut check = direct
        .subscribe_from(TENANT, NAMESPACE, stream, Some(StartPosition::Earliest))
        .await?;
    let mut missing: Vec<Vec<u8>> = acked.clone();
    let deadline = Instant::now() + SETTLE;
    while !missing.is_empty() {
        let event = match timeout(deadline - Instant::now(), check.next_event()).await {
            Ok(event) => event?.ok_or_else(|| anyhow!("read-back ended early"))?,
            Err(_) => break,
        };
        missing.retain(|payload| payload.as_slice() != event.payload.as_ref());
    }
    if !missing.is_empty() {
        bail!(
            "{} acknowledged records were not in the stream",
            missing.len()
        );
    }
    Ok(if errors == 0 {
        "every publish acknowledged"
    } else {
        "reported the loss, then recovered"
    })
}

/// The publish step with pipelining on: an idempotent producer sends each
/// step's records as a call of one-record batches, several unanswered at
/// once, and re-makes a failed call until it lands. The bar is higher than
/// for a plain publish: every record lands exactly once, in order.
async fn pipelined_case(
    case: &FaultScenario,
    link: &Interposer,
    direct: &Client,
    stream: &str,
    config: ClientConfig,
) -> Result<&'static str> {
    let step = &case.step;
    let hold = Duration::from_millis(step.hold_ms);
    let cluster = ClusterClient::connect(&[link.addr()], "localhost", config).await?;
    let producer = cluster.idempotent_producer().await?;
    let mut published = Vec::new();
    let mut errors = 0;
    for index in 0..step.records {
        if index == step.after_records {
            link.inject(step.fault, hold);
        }
        let batches: Vec<Vec<Vec<u8>>> = (0..PIPELINED_BATCHES)
            .map(|n| vec![format!("{}-{index}-{n}", case.id).into_bytes()])
            .collect();
        let deadline = Instant::now() + hold + SETTLE;
        loop {
            let publish = producer.publish_batches(TENANT, NAMESPACE, stream, batches.clone());
            match timeout(hold + SETTLE, publish).await {
                Err(_) => bail!("pipelined call {index} neither returned nor failed"),
                Ok(Ok(())) => break,
                Ok(Err(err)) if step.fault == LinkFault::Stall => {
                    bail!("pipelined call {index} failed during a stall: {err:#}")
                }
                Ok(Err(err)) if Instant::now() >= deadline => {
                    bail!("pipelined call {index} never landed after the fault: {err:#}")
                }
                // The contract: the same call again re-sends what is in doubt.
                Ok(Err(_)) => {
                    errors += 1;
                    tokio::time::sleep(POLL).await;
                }
            }
        }
        published.extend(batches.into_iter().flatten());
    }

    let mut check = direct
        .subscribe_from(TENANT, NAMESPACE, stream, Some(StartPosition::Earliest))
        .await?;
    let mut stored = Vec::with_capacity(published.len());
    while stored.len() < published.len() {
        let event = timeout(SETTLE, check.next_event())
            .await
            .context("read-back stalled")??
            .ok_or_else(|| anyhow!("read-back ended early"))?;
        stored.push(event.payload.to_vec());
    }
    if let Ok(Ok(Some(extra))) = timeout(POLL, check.next_event()).await {
        bail!(
            "a record landed twice: {:?}",
            String::from_utf8_lossy(&extra.payload)
        );
    }
    if stored != published {
        bail!("records were lost, repeated or reordered");
    }
    Ok(if errors == 0 {
        "every pipelined batch landed once"
    } else {
        "re-sent what was in doubt; every batch landed once"
    })
}

/// Either kind of subscription, read the same way.
enum Reader {
    Cluster(ClusterSubscription),
    Plain(Subscription),
}

impl Reader {
    async fn next_event(&mut self) -> Result<Option<Event>> {
        match self {
            Self::Cluster(subscription) => subscription.next_event().await,
            Self::Plain(subscription) => subscription.next_event().await,
        }
    }
}

async fn subscribe_case(
    case: &FaultScenario,
    via: Via,
    link: &Interposer,
    direct: &Client,
    stream: &str,
    config: ClientConfig,
) -> Result<&'static str> {
    let step = &case.step;
    let hold = Duration::from_millis(step.hold_ms);
    // Held for the whole case: dropping a client closes its connections.
    let (_cluster, _plain, mut reader) = match via {
        Via::Cluster => {
            let cluster =
                Arc::new(ClusterClient::connect(&[link.addr()], "localhost", config).await?);
            let subscription = cluster.subscribe(TENANT, NAMESPACE, stream).await?;
            (Some(cluster), None, Reader::Cluster(subscription))
        }
        Via::Plain | Via::Pipelined => {
            let client = Client::connect(link.addr(), "localhost", config).await?;
            let subscription = client.subscribe(TENANT, NAMESPACE, stream).await?;
            (None, Some(client), Reader::Plain(subscription))
        }
    };

    let publisher = direct.publisher().await?;
    let payloads: Vec<Vec<u8>> = (0..step.records)
        .map(|index| format!("{}-{index}", case.id).into_bytes())
        .collect();
    let (before, rest) = payloads.split_at(step.after_records as usize);
    for payload in before {
        publisher
            .publish(
                TENANT,
                NAMESPACE,
                stream,
                payload.clone(),
                AckMode::PerMessage,
            )
            .await?;
    }

    let mut received: Vec<Event> = Vec::new();
    let mut injected = false;
    let mut deadline = Instant::now() + SETTLE;
    while received.len() < payloads.len() {
        if !injected && received.len() == before.len() {
            link.inject(step.fault, hold);
            injected = true;
            deadline = Instant::now() + hold + SETTLE;
            for payload in rest {
                publisher
                    .publish(
                        TENANT,
                        NAMESPACE,
                        stream,
                        payload.clone(),
                        AckMode::PerMessage,
                    )
                    .await?;
            }
        }
        if Instant::now() >= deadline {
            bail!(
                "neither resumed nor reported the loss: {} of {} records, then nothing",
                received.len(),
                payloads.len()
            );
        }
        match timeout(POLL, reader.next_event()).await {
            // Dropping the read in flight is the point; see `POLL`.
            Err(_) => continue,
            Ok(Ok(Some(event))) => received.push(event),
            Ok(Ok(None)) => bail!(
                "the subscription ended cleanly after {} of {} records; a dead \
                 connection must be resumed or reported, not read as end of stream",
                received.len(),
                payloads.len()
            ),
            Ok(Err(err)) if !injected || step.fault == LinkFault::Stall => {
                bail!("the subscription failed with no connection loss to report: {err:#}")
            }
            Ok(Err(_)) => {
                check_delivered(&received, &payloads)?;
                return Ok("reported the loss");
            }
        }
    }
    check_delivered(&received, &payloads)?;
    Ok("resumed with no gap")
}

/// What arrived is a prefix of what was published, in order, at contiguous
/// offsets.
fn check_delivered(received: &[Event], payloads: &[Vec<u8>]) -> Result<()> {
    for (index, event) in received.iter().enumerate() {
        if event.payload.as_ref() != payloads[index].as_slice() {
            bail!(
                "record {index} was {:?}, expected {:?}: a gap or a duplicate",
                String::from_utf8_lossy(&event.payload),
                String::from_utf8_lossy(&payloads[index])
            );
        }
    }
    for pair in received.windows(2) {
        let (Some(earlier), Some(later)) = (pair[0].offset, pair[1].offset) else {
            bail!("a durable stream delivered a record without an offset");
        };
        if later != earlier + 1 {
            bail!("offsets jumped from {earlier} to {later}");
        }
    }
    Ok(())
}
