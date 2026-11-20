//! One producer: numbers its writes itself, sends each through the SDK's
//! idempotent producer, and re-sends the same write until it is acknowledged.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_client::{Client, PublishRefused};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::Args;
use crate::ledger::{Ledger, Stall};
use crate::payload;

/// Wait between a failed attempt and its re-send.
const RESEND_AFTER: Duration = Duration::from_millis(200);

enum Outcome {
    Acked(Option<Stall>),
    Refused(String),
    Failed,
}

pub(crate) async fn run(
    index: u16,
    cluster: Arc<Client>,
    args: Arc<Args>,
    run_id: String,
    ledger: Arc<Mutex<Ledger>>,
    mut stop: watch::Receiver<bool>,
) {
    let producer = match cluster.idempotent_producer().await {
        Ok(producer) => producer,
        Err(err) => {
            eprintln!("producer {index}: cannot start: {err:#}");
            return;
        }
    };
    let mut tick =
        tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(args.rate.max(1))));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut seq = 0u64;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => return,
        }
        if *stop.borrow() {
            return;
        }
        let key = (index, seq);
        let body = payload::encode(&run_id, key, args.payload_bytes);
        ledger.lock().expect("ledger").begin(key, Instant::now());
        // Re-send the same bytes under the same sequence until the broker
        // answers. The SDK refuses a different payload while one is in doubt.
        loop {
            let sent = producer
                .publish(&args.tenant, &args.namespace, &args.stream, body.clone())
                .await;
            let outcome = {
                let mut book = ledger.lock().expect("ledger");
                match sent {
                    Ok(_) => Outcome::Acked(book.ack(key, Instant::now())),
                    Err(err) if err.chain().any(|cause| cause.is::<PublishRefused>()) => {
                        book.refused(key);
                        Outcome::Refused(format!("{err:#}"))
                    }
                    Err(_) => {
                        book.failed(key);
                        Outcome::Failed
                    }
                }
            };
            match outcome {
                Outcome::Acked(stall) => {
                    if let Some(stall) = stall {
                        println!(
                            "[{:>4}s] writes blocked {} ms (oldest waiting publish to first ack after)",
                            stall.at_secs + stall.blocked.as_secs(),
                            stall.blocked.as_millis()
                        );
                    }
                    break;
                }
                Outcome::Refused(why) => {
                    eprintln!("producer {index}: the broker refused this producer: {why}");
                    return;
                }
                Outcome::Failed => {}
            }
            if *stop.borrow() {
                // Shutting down with this write still in doubt.
                return;
            }
            tokio::time::sleep(RESEND_AFTER).await;
            ledger.lock().expect("ledger").retrying(key);
        }
        seq += 1;
    }
}
