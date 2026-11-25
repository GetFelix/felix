//! Reading a stream back after a failover without trusting one replay.
//!
//! A replay on a broker that has just taken a shard over can stop below
//! records the cluster acknowledged (#3112). Read once, that looks exactly
//! like data loss. So a demo names the last record it knows is in the log
//! and reads until a replay reaches it, saying so each time one stops short.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use felix_cluster::{Cluster, wait};

/// One record: what was published, and the offset its ack or the replay
/// reported, when it reported one.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Record {
    pub(super) offset: Option<u64>,
    pub(super) payload: String,
}

impl Record {
    pub(super) fn new(offset: Option<u64>, payload: impl Into<String>) -> Self {
        Self {
            offset,
            payload: payload.into(),
        }
    }

    /// The same record: the same payload, at the same offset when both sides
    /// know it. A resent record sits at a second offset, and only the one the
    /// ack named counts.
    fn is(&self, other: &Record) -> bool {
        self.payload == other.payload
            && match (self.offset, other.offset) {
                (Some(mine), Some(theirs)) => mine == theirs,
                _ => true,
            }
    }

    pub(super) fn place(&self) -> String {
        match self.offset {
            Some(offset) => format!("{:?} at offset {offset}", self.payload),
            None => format!("{:?}", self.payload),
        }
    }
}

/// How one replay compares with the last record known to be in the log.
#[derive(Debug, PartialEq)]
pub(super) enum Reach {
    /// It read through that record.
    Whole,
    /// It ended below it. `stopped_at` is the last offset it did deliver.
    Short { stopped_at: Option<u64> },
}

/// Judge one replay against `last`. Fails when the offsets do not climb,
/// which no retry explains.
pub(super) fn reach(replayed: &[Record], last: &Record) -> Result<Reach> {
    let mut before: Option<u64> = None;
    for record in replayed {
        let Some(offset) = record.offset else {
            continue;
        };
        if before.is_some_and(|before| offset <= before) {
            bail!(
                "the replay delivered offset {offset} after offset {}",
                before.unwrap_or_default()
            );
        }
        before = Some(offset);
    }
    if replayed.iter().any(|record| record.is(last)) {
        Ok(Reach::Whole)
    } else {
        Ok(Reach::Short { stopped_at: before })
    }
}

/// The acknowledged records a replay does not hold. Only meaningful for a
/// replay that reached its last record: each one is then a hole the replay
/// read past. Offsets alone cannot show a hole, because a Raft shard's log
/// has offsets that hold no record and readers skip them.
pub(super) fn missing(acknowledged: &[Record], replayed: &[Record]) -> Vec<Record> {
    acknowledged
        .iter()
        .filter(|record| !replayed.iter().any(|held| held.is(record)))
        .cloned()
        .collect()
}

/// Replay `stream` on `node_id` until a replay reaches `last`, the newest
/// record the caller saw acknowledged. Says so whenever one stops short.
pub(super) async fn through(
    cluster: &Cluster,
    node_id: &str,
    stream: &str,
    last: &Record,
    within: Duration,
) -> Result<Vec<Record>> {
    let deadline = Instant::now() + wait::budget(within);
    loop {
        let why = match once(cluster, node_id, stream).await {
            Ok(replayed) => match reach(&replayed, last)? {
                Reach::Whole => return Ok(replayed),
                Reach::Short { stopped_at } => {
                    let stopped = match stopped_at {
                        Some(offset) => format!("at offset {offset}"),
                        None => "before its first record".to_string(),
                    };
                    format!(
                        "replay stopped short (#3112) {stopped}, below {}",
                        last.place()
                    )
                }
            },
            // A broker still settling its new term refuses the subscribe.
            Err(err) => format!("{node_id} would not replay yet ({err:#})"),
        };
        if Instant::now() >= deadline {
            bail!(
                "no replay on {node_id} reached {} within {within:?}; the last try: {why}",
                last.place()
            );
        }
        println!("  {why}, retrying");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// One replay from the start of the stream, read until it goes quiet.
async fn once(cluster: &Cluster, node_id: &str, stream: &str) -> Result<Vec<Record>> {
    let (_client, mut subscription) = cluster.replay_on(node_id, stream).await?;
    let mut replayed = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        replayed.push(Record::new(
            event.offset,
            String::from_utf8_lossy(&event.payload),
        ));
    }
    Ok(replayed)
}

#[cfg(test)]
mod tests;
