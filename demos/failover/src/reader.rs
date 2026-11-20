//! The reader: follows the stream from its first record and reports each
//! record to the ledger. It also reads the whole stream back for a receipt.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_client::{Client, StartPosition};

use crate::Args;
use crate::ledger::{Key, Ledger};
use crate::payload;

pub(crate) async fn follow(
    cluster: Arc<Client>,
    args: Arc<Args>,
    run_id: String,
    ledger: Arc<Mutex<Ledger>>,
) {
    let mut next_offset: Option<u64> = None;
    loop {
        let start = match next_offset {
            Some(offset) => StartPosition::Offset(offset),
            None => StartPosition::Earliest,
        };
        let mut subscription = match cluster
            .subscribe_from(&args.tenant, &args.namespace, &args.stream, Some(start))
            .await
        {
            Ok(subscription) => subscription,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        loop {
            match subscription.next_event().await {
                Ok(Some(event)) => {
                    if let Some(offset) = event.offset {
                        next_offset = Some(offset + 1);
                    }
                    let key = payload::decode(&run_id, &event.payload);
                    ledger.lock().expect("ledger").delivered(key);
                }
                // The broker ended the stream: subscribe again from here.
                Ok(None) => break,
                // The subscription follows the shard itself; calling again
                // retries the resubscribe.
                Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
        ledger.lock().expect("ledger").reader_resubscribed();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Every record of this run on the stream, as how many times each key appears,
/// and how many records carried a key no producer ever started.
pub(crate) async fn read_back(
    cluster: &Client,
    args: &Args,
    run_id: &str,
    ledger: &Mutex<Ledger>,
) -> Result<(HashMap<Key, u32>, u64, u64)> {
    let mut counts: HashMap<Key, u32> = HashMap::new();
    let mut from = 0u64;
    let mut records = 0u64;
    let give_up = Instant::now() + Duration::from_secs(60);
    loop {
        match cluster
            .read(
                &args.tenant,
                &args.namespace,
                &args.stream,
                0,
                from,
                None,
                1000,
            )
            .await
        {
            Ok(page) => {
                if page.records.is_empty() {
                    break;
                }
                for record in &page.records {
                    records += 1;
                    if let Some(key) = payload::decode(run_id, &record.payload) {
                        *counts.entry(key).or_insert(0) += 1;
                    }
                }
                from = page.next_offset;
            }
            Err(err) => {
                if Instant::now() > give_up {
                    return Err(err).context("read the stream back");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    let unexpected = {
        let ledger = ledger.lock().expect("ledger");
        counts.keys().filter(|key| !ledger.was_sent(**key)).count() as u64
    };
    Ok((counts, unexpected, records))
}
