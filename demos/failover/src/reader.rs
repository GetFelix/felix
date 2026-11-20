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

/// One page of the stream: the payloads in it, and where the next page starts.
pub(crate) struct Page {
    pub(crate) payloads: Vec<Vec<u8>>,
    pub(crate) next_offset: u64,
}

/// How long a read may keep failing, or sit short of an acknowledged offset,
/// before the receipt gives up.
const READ_PATIENCE: Duration = Duration::from_secs(60);

/// Page through the stream with `fetch(from)` until it ends, and return how
/// many records there were and how many times each key of this run appears.
///
/// An empty page is not the end: the broker skips offsets that hold no record
/// of ours and can answer empty just after a failover. The end is a page that
/// does not move the offset on, once `read_to` (the first offset past every
/// acknowledged record) is reached. Short of it, the same answer is retried.
pub(crate) async fn collect<F, Fut>(
    run_id: &str,
    read_to: Option<u64>,
    patience: Duration,
    mut fetch: F,
) -> Result<(HashMap<Key, u32>, u64)>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<Page>>,
{
    let mut counts: HashMap<Key, u32> = HashMap::new();
    let mut from = 0u64;
    let mut records = 0u64;
    let give_up = Instant::now() + patience;
    loop {
        let page = match fetch(from).await {
            Ok(page) => page,
            Err(err) if Instant::now() > give_up => {
                return Err(err).context("read the stream back");
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        for payload in &page.payloads {
            records += 1;
            if let Some(key) = payload::decode(run_id, payload) {
                *counts.entry(key).or_insert(0) += 1;
            }
        }
        if page.next_offset > from {
            from = page.next_offset;
            continue;
        }
        anyhow::ensure!(
            page.payloads.is_empty(),
            "the broker returned records at offset {from} without moving the offset on"
        );
        {
            if read_to.is_none_or(|to| from >= to) {
                return Ok((counts, records));
            }
            if Instant::now() > give_up {
                anyhow::bail!(
                    "the stream ends at offset {from}, short of an acknowledged record at {}",
                    read_to.unwrap_or(0)
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// Every record of this run on the stream, as how many times each key appears,
/// and how many records carried a key no producer ever started.
pub(crate) async fn read_back(
    cluster: &Client,
    args: &Args,
    run_id: &str,
    ledger: &Mutex<Ledger>,
    read_to: Option<u64>,
) -> Result<(HashMap<Key, u32>, u64, u64)> {
    let (counts, records) = collect(run_id, read_to, READ_PATIENCE, |from| async move {
        let page = cluster
            .read(
                &args.tenant,
                &args.namespace,
                &args.stream,
                0,
                from,
                None,
                1000,
            )
            .await?;
        Ok(Page {
            payloads: page.records.iter().map(|r| r.payload.to_vec()).collect(),
            next_offset: page.next_offset,
        })
    })
    .await?;
    let unexpected = {
        let ledger = ledger.lock().expect("ledger");
        counts.keys().filter(|key| !ledger.was_sent(**key)).count() as u64
    };
    Ok((counts, unexpected, records))
}

#[cfg(test)]
mod tests;
