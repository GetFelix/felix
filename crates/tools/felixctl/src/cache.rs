//! `felixctl cache get|put|del|watch`. `ls` and `info` are control-plane
//! requests and live in [`crate::controlplane`].
//!
//! Gets, puts and deletes go through whichever broker the cluster client is
//! using; a broker that does not own the key's shard forwards the request.

use felix_client::{CacheChange, CacheWatchFilter, CacheWatchItem, ShardedCacheWatchItem};

use crate::cli::CacheCommand;
use crate::connect::Broker;
use crate::context::Settings;
use crate::controlplane;
use crate::error::{Exit, MarkExit, fail};
use crate::output::{Output, payload_field};

pub(crate) async fn run(
    command: &CacheCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    match command {
        CacheCommand::Ls => return controlplane::list_caches(settings, out).await,
        CacheCommand::Info { cache } => {
            return controlplane::cache_info(settings, cache, out).await;
        }
        _ => {}
    }
    let broker = Broker::connect(settings).await?;
    let client = broker.cluster.client().await;
    let (tenant, namespace) = (broker.tenant.as_str(), broker.namespace.as_str());
    match command {
        CacheCommand::Get { cache, key } => {
            let Some(value) = client.cache_get(tenant, namespace, cache, key).await? else {
                return Err(fail(Exit::NotFound, format!("{cache}/{key} is not set")));
            };
            if out.json {
                let (field, payload) = payload_field(&value);
                let mut object = serde_json::json!({ "cache": cache, "key": key });
                object[field.replace("payload", "value")] = payload;
                out.json_value(&object)
            } else {
                out.raw_line(&value)
            }
        }
        CacheCommand::Put {
            cache,
            key,
            value,
            file,
            ttl_ms,
        } => {
            let value = match (value, file) {
                (Some(value), _) => value.as_bytes().to_vec(),
                (None, Some(path)) => tokio::fs::read(path)
                    .await
                    .mark(Exit::Usage, format!("read {}", path.display()))?,
                (None, None) => {
                    let mut bytes = Vec::new();
                    std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut bytes)
                        .mark(Exit::Usage, "read stdin")?;
                    bytes
                }
            };
            let len = value.len();
            client
                .cache_put(tenant, namespace, cache, key, value.into(), *ttl_ms)
                .await?;
            out.done(
                &format!("set {cache}/{key} ({len} bytes)"),
                serde_json::json!({ "cache": cache, "key": key, "bytes": len, "ttl_ms": ttl_ms }),
            )
        }
        CacheCommand::Del { cache, key } => {
            let previous = client.cache_delete(tenant, namespace, cache, key).await?;
            let text = if previous.is_some() {
                format!("deleted {cache}/{key}")
            } else {
                format!("{cache}/{key} was not set")
            };
            out.done(
                &text,
                serde_json::json!({ "cache": cache, "key": key, "deleted": previous.is_some() }),
            )
        }
        CacheCommand::Watch {
            cache,
            key,
            prefix,
            retained,
            from,
            count,
        } => {
            let limit = count.unwrap_or(u64::MAX);
            watch(&broker, out, cache, key, prefix, *retained, *from, limit).await
        }
        CacheCommand::Ls | CacheCommand::Info { .. } => unreachable!("handled above"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn watch(
    broker: &Broker,
    out: &Output,
    cache: &str,
    key: &Option<String>,
    prefix: &Option<String>,
    retained: bool,
    from: Option<u64>,
    limit: u64,
) -> anyhow::Result<()> {
    if limit == 0 {
        return Ok(());
    }
    let (tenant, namespace) = (broker.tenant.as_str(), broker.namespace.as_str());
    let mut seen = 0;
    if let Some(key) = key {
        let filter = CacheWatchFilter::Key(key.clone());
        let mut watch = if retained {
            broker
                .cluster
                .watch_cache_retained(tenant, namespace, cache, filter)
                .await?
        } else {
            broker
                .cluster
                .watch_cache(tenant, namespace, cache, filter, from)
                .await?
        };
        while let Some(item) = watch.recv().await {
            match item {
                CacheWatchItem::Change(change) => {
                    print_change(out, cache, None, &change)?;
                    seen += 1;
                    if seen >= limit {
                        break;
                    }
                }
                CacheWatchItem::Lagged { resume_from } => {
                    return Err(fail(
                        Exit::Server,
                        format!(
                            "the watch fell behind and the broker ended it; resume with --from {resume_from}"
                        ),
                    ));
                }
                CacheWatchItem::ShardMoved(moved) => eprintln!(
                    "shard moved to {}",
                    moved.node_id.as_deref().unwrap_or("another broker")
                ),
            }
        }
        return Ok(());
    }

    let prefix = prefix.clone().unwrap_or_default();
    let mut watch = if retained {
        broker
            .cluster
            .watch_cache_sharded_retained(tenant, namespace, cache, &prefix)
            .await?
    } else {
        broker
            .cluster
            .watch_cache_sharded(tenant, namespace, cache, &prefix, None)
            .await?
    };
    while let Some(item) = watch.recv().await {
        match item {
            ShardedCacheWatchItem::Change { shard, change } => {
                print_change(out, cache, Some(shard), &change)?;
                seen += 1;
                if seen >= limit {
                    break;
                }
            }
            ShardedCacheWatchItem::StateComplete => {
                if !out.json {
                    eprintln!("current values done; watching for changes");
                }
            }
            ShardedCacheWatchItem::Lagged { shard, .. } => {
                eprintln!("shard {shard} fell behind and its watch ended");
            }
            ShardedCacheWatchItem::ShardMoved { shard, moved } => eprintln!(
                "shard {shard} moved to {}",
                moved.node_id.as_deref().unwrap_or("another broker")
            ),
            ShardedCacheWatchItem::ShardClosed { shard } => {
                eprintln!("shard {shard}'s watch ended");
            }
        }
    }
    Ok(())
}

fn print_change(
    out: &Output,
    cache: &str,
    shard: Option<u32>,
    change: &CacheChange,
) -> anyhow::Result<()> {
    out.raw_line(&change_line(out.json, cache, shard, change))
}

/// One change as printed: `key<TAB>value`, `key<TAB>(deleted)`, or a JSON
/// object.
pub(crate) fn change_line(
    json: bool,
    cache: &str,
    shard: Option<u32>,
    change: &CacheChange,
) -> Vec<u8> {
    if json {
        let mut object = serde_json::json!({
            "cache": cache,
            "shard": shard,
            "key": change.key,
            "offset": change.offset,
            "deleted": change.value.is_none(),
        });
        if let Some(value) = &change.value {
            let (field, payload) = payload_field(value);
            object[field.replace("payload", "value")] = payload;
        }
        return object.to_string().into_bytes();
    }
    let mut line = format!("{}\t", change.key).into_bytes();
    match &change.value {
        Some(value) => line.extend_from_slice(value),
        None => line.extend_from_slice(b"(deleted)"),
    }
    line
}

#[cfg(test)]
mod tests;
