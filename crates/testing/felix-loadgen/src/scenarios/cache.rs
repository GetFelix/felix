//! Cache and counter round trips.

use std::sync::Arc;

use anyhow::Result;
use felix_client::Client;

use super::round_trips::round_trips;
use super::{Common, scope};

/// Cache put then get, reported separately: a put pays the log append and a
/// get pays the index-plus-read, and folding them together hides both.
pub(crate) async fn cache(common: &Common, cache: &str) -> Result<serde_json::Value> {
    let value = bytes::Bytes::from(vec![0u8; common.payload_bytes]);
    let (tenant, namespace, name) = scope(common, cache);
    let put_value = value.clone();
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (put, put_tp, put_n) = round_trips(
        common,
        "cache put",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c, v) = (t.clone(), ns.clone(), c.clone(), put_value.clone());
            async move {
                client
                    .cache_put(&t, &ns, &c, &format!("k{key}"), v, None)
                    .await
            }
        }),
    )
    .await?;
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (get, get_tp, get_n) = round_trips(
        common,
        "cache get",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c) = (t.clone(), ns.clone(), c.clone());
            async move {
                client.cache_get(&t, &ns, &c, &format!("k{key}")).await?;
                Ok(())
            }
        }),
    )
    .await?;

    Ok(serde_json::json!({
        "scenario": "cache",
        "environment": common.environment,
        "cache": name,
        "payload_bytes": common.payload_bytes,
        "concurrency": common.concurrency.max(1),
        "put": { "n": put_n, "throughput_op_s": put_tp,
                 "latency_us": { "p50": put.p50_us, "p99": put.p99_us, "p999": put.p999_us, "max": put.max_us } },
        "get": { "n": get_n, "throughput_op_s": get_tp,
                 "latency_us": { "p50": get.p50_us, "p99": get.p99_us, "p999": get.p999_us, "max": get.max_us } },
    }))
}

/// Counter add then get. The add is the composed-semantics headline: one
/// round trip that both applies the delta and answers with the sum.
pub(crate) async fn counter(common: &Common, cache: &str) -> Result<serde_json::Value> {
    let (tenant, namespace, name) = scope(common, cache);
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (add, add_tp, add_n) = round_trips(
        common,
        "counter add",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c) = (t.clone(), ns.clone(), c.clone());
            async move {
                client
                    .counter_add(&t, &ns, &c, &format!("c{}", key % 128), 1)
                    .await?;
                Ok(())
            }
        }),
    )
    .await?;
    let (t, ns, c) = (tenant.clone(), namespace.clone(), name.clone());
    let (get, get_tp, get_n) = round_trips(
        common,
        "counter get",
        Arc::new(move |client: Arc<Client>, key: u64| {
            let (t, ns, c) = (t.clone(), ns.clone(), c.clone());
            async move {
                client
                    .counter_get(&t, &ns, &c, &format!("c{}", key % 128))
                    .await?;
                Ok(())
            }
        }),
    )
    .await?;

    Ok(serde_json::json!({
        "scenario": "counter",
        "environment": common.environment,
        "cache": name,
        "concurrency": common.concurrency.max(1),
        "add": { "n": add_n, "throughput_op_s": add_tp,
                 "latency_us": { "p50": add.p50_us, "p99": add.p99_us, "p999": add.p999_us, "max": add.max_us } },
        "get": { "n": get_n, "throughput_op_s": get_tp,
                 "latency_us": { "p50": get.p50_us, "p99": get.p99_us, "p999": get.p999_us, "max": get.max_us } },
    }))
}
