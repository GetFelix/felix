//! `felixctl sub`.
//!
//! Without `--shard`, a single-shard stream is read through the cluster
//! client's followed subscription and a wider one through its sharded
//! subscription, so moves and lost brokers are followed either way. With
//! `--shard`, the shard is read through a plain client: the cluster client
//! cannot subscribe to one chosen shard, so a `NotLeader` redirect is
//! followed here, once.

use felix_client::{NotLeaderError, ShardEvent, StartPosition, Subscription};

use crate::cli::{FormatArg, StartArg, SubArgs};
use crate::connect::Broker;
use crate::context::Settings;
use crate::error::{Exit, fail};
use crate::output::{Output, payload_field};

pub(crate) async fn run(args: &SubArgs, settings: &Settings, out: &Output) -> anyhow::Result<()> {
    let broker = Broker::connect(settings).await?;
    let format = if out.json {
        FormatArg::Json
    } else {
        args.format
    };
    let printer = Printer {
        out: *out,
        format,
        stream: args.stream.clone(),
    };
    let start = start_position(args.from);
    let limit = args.count.unwrap_or(u64::MAX);
    if limit == 0 {
        return Ok(());
    }
    let (tenant, namespace, stream) = (
        broker.tenant.as_str(),
        broker.namespace.as_str(),
        args.stream.as_str(),
    );

    if let Some(shard) = args.shard {
        let mut subscription = subscribe_shard(&broker, stream, shard, start).await?;
        let mut seen = 0;
        while let Some(event) = subscription.next_event().await? {
            printer.event(Some(shard), event.offset, &event.payload)?;
            seen += 1;
            if seen >= limit {
                break;
            }
        }
        return Ok(());
    }

    let shards = broker
        .cluster
        .client()
        .await
        .stream_shards(tenant, namespace, stream)
        .await?;
    if shards == 0 {
        return Err(fail(
            Exit::NotFound,
            format!("the broker knows no stream {stream:?} in {tenant}/{namespace}"),
        ));
    }
    let mut seen = 0;
    if shards == 1 {
        let mut subscription = broker
            .cluster
            .subscribe_from(tenant, namespace, stream, start)
            .await?;
        while let Some(event) = subscription.next_event().await? {
            printer.event(Some(0), event.offset, &event.payload)?;
            seen += 1;
            if seen >= limit {
                break;
            }
        }
        return Ok(());
    }
    let mut subscription = broker
        .cluster
        .subscribe_sharded(tenant, namespace, stream, start)
        .await?;
    while let Some(item) = subscription.next().await {
        match item {
            ShardEvent::Record { shard, event } => {
                printer.event(Some(shard), event.offset, &event.payload)?;
                seen += 1;
                if seen >= limit {
                    break;
                }
            }
            ShardEvent::ShardLost { shard, error } => {
                eprintln!("shard {shard} lost, reconnecting: {error}");
            }
            ShardEvent::ShardRecovered { shard } => eprintln!("shard {shard} recovered"),
            ShardEvent::ShardMoved { shard, moved } => {
                eprintln!(
                    "shard {shard} moved to {}",
                    moved.node_id.as_deref().unwrap_or("another broker")
                );
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn start_position(from: StartArg) -> Option<StartPosition> {
    match from {
        StartArg::Latest => None,
        StartArg::Earliest => Some(StartPosition::Earliest),
        StartArg::Offset(offset) => Some(StartPosition::Offset(offset)),
    }
}

/// Subscribe to one shard, following one `NotLeader` redirect to its owner.
async fn subscribe_shard(
    broker: &Broker,
    stream: &str,
    shard: u32,
    start: Option<StartPosition>,
) -> anyhow::Result<Subscription> {
    let entry = broker.cluster.client().await;
    let (tenant, namespace) = (broker.tenant.as_str(), broker.namespace.as_str());
    match entry
        .subscribe_shard(tenant, namespace, stream, shard, start)
        .await
    {
        Ok(subscription) => Ok(subscription),
        Err(err) => {
            let Some(addr) = err
                .downcast_ref::<NotLeaderError>()
                .and_then(|redirect| redirect.addr.as_deref())
                .and_then(|addr| addr.parse().ok())
            else {
                return Err(err);
            };
            let owner = broker.client_at(addr).await?;
            owner
                .subscribe_shard(tenant, namespace, stream, shard, start)
                .await
        }
    }
}

/// Prints one delivered message in the chosen format.
pub(crate) struct Printer {
    pub(crate) out: Output,
    pub(crate) format: FormatArg,
    pub(crate) stream: String,
}

impl Printer {
    pub(crate) fn event(
        &self,
        shard: Option<u32>,
        offset: Option<u64>,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        self.out.raw_line(&self.line(shard, offset, payload))
    }

    /// The line printed for one message, without its newline.
    pub(crate) fn line(&self, shard: Option<u32>, offset: Option<u64>, payload: &[u8]) -> Vec<u8> {
        let dash = |n: Option<String>| n.unwrap_or_else(|| "-".to_string());
        match self.format {
            FormatArg::Raw => payload.to_vec(),
            FormatArg::Offsets => {
                let mut line = format!(
                    "{}\t{}\t",
                    dash(shard.map(|s| s.to_string())),
                    dash(offset.map(|o| o.to_string()))
                )
                .into_bytes();
                line.extend_from_slice(payload);
                line
            }
            FormatArg::Json => {
                let (field, value) = payload_field(payload);
                let mut object = serde_json::json!({
                    "stream": self.stream,
                    "shard": shard,
                    "offset": offset,
                });
                object[field] = value;
                object.to_string().into_bytes()
            }
        }
    }
}

#[cfg(test)]
mod tests;
