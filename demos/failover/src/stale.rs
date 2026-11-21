//! The stale writer: one connection to one broker, no routing, one numbered
//! write every 250 ms. Pointed at a leader that gets frozen and replaced, it
//! shows what that broker does with writes once it is back.
//!
//! Each write is sent once and never re-sent, so every outcome is the broker's
//! answer to that one attempt. The client can tell these apart and no more:
//!
//! - `ok`: acknowledged, with the offset when the broker reports one.
//! - `forwarded`: acknowledged, and the broker said another broker wrote it.
//!   The client only counts this process-wide, which is exact here because the
//!   writer is the only publisher in its process and sends one write at a time.
//! - `refused`: a typed refusal that wrote nothing.
//! - `indeterminate`: the broker may have written it (`AckLost`, or a code
//!   whose retry class is outcome-unknown).
//! - `no answer`: no typed answer at all. Treated as indeterminate, since the
//!   client cannot say whether the write left the machine.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_client::{
    AckLost, AckMode, BrokerError, Connection, PublishRefused, Publisher, outcome_unknown,
    publishes_forwarded,
};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::Args;
use crate::ledger::Ledger;
use crate::payload;

const WRITE_EVERY: Duration = Duration::from_millis(250);

/// How long one write may wait for an answer. A frozen broker never answers;
/// the connection's idle timeout (6 s) ends the wait first, this is a backstop.
const NO_ANSWER_AFTER: Duration = Duration::from_secs(15);

/// How long one attempt to open a connection may take.
const CONNECT_ATTEMPT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ok { offset: Option<u64> },
    Forwarded { offset: Option<u64> },
    Refused(String),
    Indeterminate(String),
    NoAnswer(String),
}

impl Outcome {
    /// The line the writer prints for one write.
    pub(crate) fn describe(&self) -> String {
        let offset = |offset: &Option<u64>| {
            offset.map_or_else(|| "ok".to_string(), |offset| format!("ok offset {offset}"))
        };
        match self {
            Outcome::Ok { offset: o } => offset(o),
            Outcome::Forwarded { offset: o } => format!("forwarded ({})", offset(o)),
            Outcome::Refused(why) => format!("refused {why}"),
            Outcome::Indeterminate(why) => format!("indeterminate {why}"),
            Outcome::NoAnswer(why) => format!("no answer ({why})"),
        }
    }

    /// Whether the connection can be trusted for the next write.
    fn connection_usable(&self) -> bool {
        matches!(
            self,
            Outcome::Ok { .. } | Outcome::Forwarded { .. } | Outcome::Refused(_)
        )
    }
}

/// Name a broker refusal by its wire code, with the broker's reason when it
/// gave one.
fn code_name(err: &BrokerError) -> String {
    match err.reason() {
        Some(reason) => format!("{} ({reason})", err.code),
        None => err.code.to_string(),
    }
}

/// Sort one publish result into an outcome. `forwarded` is whether the
/// client's forwarded-publish counter moved during the call.
pub(crate) fn classify(result: Result<Option<u64>>, forwarded: bool) -> Outcome {
    let err = match result {
        Ok(offset) if forwarded => return Outcome::Forwarded { offset },
        Ok(offset) => return Outcome::Ok { offset },
        Err(err) => err,
    };
    if let Some(lost) = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<AckLost>())
    {
        return Outcome::Indeterminate(format!("ack_lost ({})", lost.reason));
    }
    if let Some(broker) = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<BrokerError>())
    {
        let name = code_name(broker);
        return if outcome_unknown(&err) {
            Outcome::Indeterminate(name)
        } else {
            Outcome::Refused(name)
        };
    }
    if let Some(refused) = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<PublishRefused>())
    {
        return Outcome::Refused(format!("{:?}", refused.reason));
    }
    Outcome::NoAnswer(format!("{err:#}"))
}

/// A connection to the pinned broker and the publisher on it.
struct Link {
    // Held so the connection outlives the publisher.
    _connection: Connection,
    publisher: Publisher,
}

async fn dial(addr: SocketAddr, args: &Args) -> Result<Link> {
    let config = crate::client_config(args)?;
    let connection = tokio::time::timeout(
        CONNECT_ATTEMPT,
        Connection::connect(addr, &args.server_name, config),
    )
    .await??;
    let publisher = connection.publisher().await?;
    Ok(Link {
        _connection: connection,
        publisher,
    })
}

/// Keep dialling the same address until it answers or the run stops.
async fn connect(
    addr: SocketAddr,
    args: &Args,
    started: Instant,
    stop: &mut watch::Receiver<bool>,
) -> Option<Link> {
    loop {
        if *stop.borrow() {
            return None;
        }
        match dial(addr, args).await {
            Ok(link) => return Some(link),
            Err(err) => println!(
                "[{:>4}s] cannot reach {addr}: {err:#}; trying again",
                started.elapsed().as_secs()
            ),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            _ = stop.changed() => {}
        }
    }
}

pub(crate) async fn run(
    addr: SocketAddr,
    args: Arc<Args>,
    run_id: String,
    ledger: Arc<Mutex<Ledger>>,
    mut stop: watch::Receiver<bool>,
) {
    let started = Instant::now();
    let mut tick = tokio::time::interval(WRITE_EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut link: Option<Link> = None;
    let mut connected_before = false;
    let mut seq = 0u64;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => return,
        }
        if *stop.borrow() {
            return;
        }
        let Some(open) = (match link.take() {
            Some(open) => Some(open),
            None => {
                let open = connect(addr, &args, started, &mut stop).await;
                if open.is_some() && connected_before {
                    ledger.lock().expect("ledger").reconnected();
                    println!(
                        "[{:>4}s] reconnected to {addr}",
                        started.elapsed().as_secs()
                    );
                }
                connected_before |= open.is_some();
                open
            }
        }) else {
            return;
        };
        let key = (0, seq);
        let body = payload::encode(&run_id, key, args.payload_bytes);
        ledger.lock().expect("ledger").begin(key, Instant::now());
        let before = publishes_forwarded();
        let result = tokio::time::timeout(
            NO_ANSWER_AFTER,
            open.publisher.publish(
                &args.tenant,
                &args.namespace,
                &args.stream,
                body,
                AckMode::PerMessage,
            ),
        )
        .await;
        let outcome = match result {
            Ok(result) => classify(result, publishes_forwarded() > before),
            // Dropping the publish after it was queued leaves its fate unknown.
            Err(_) => Outcome::NoAnswer(format!("nothing in {} s", NO_ANSWER_AFTER.as_secs())),
        };
        {
            let mut book = ledger.lock().expect("ledger");
            match &outcome {
                Outcome::Ok { offset } | Outcome::Forwarded { offset } => {
                    if let Some(offset) = offset {
                        book.landed_at(*offset);
                    }
                    if matches!(outcome, Outcome::Forwarded { .. }) {
                        book.was_forwarded();
                    }
                    book.ack(key, Instant::now());
                }
                Outcome::Refused(_) => book.refuse(key),
                Outcome::Indeterminate(_) | Outcome::NoAnswer(_) => book.abandon(key),
            }
        }
        println!(
            "[{:>4}s] write {seq}: {}",
            started.elapsed().as_secs(),
            outcome.describe()
        );
        if outcome.connection_usable() {
            link = Some(open);
        }
        seq += 1;
    }
}

#[cfg(test)]
mod tests;
