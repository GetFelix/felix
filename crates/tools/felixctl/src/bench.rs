//! `felixctl bench`: felix-loadgen's scenarios, run in process against the
//! current context.
//!
//! The scenarios are felix-loadgen's own code, linked as a library, so a
//! number from here and one from the perf suite measure the same thing. Only
//! the defaults (smaller, so a run takes seconds) and the report differ: the
//! loadgen's prose is turned off and [`summarize`] picks the rate and the p50
//! and p99 out of its JSON result.

use felix_loadgen::{Common, IngestOptions, Scenario};
use serde_json::Value;

use crate::cli::{BenchCommand, BenchSize};
use crate::connect::{addresses, client_config, server_name};
use crate::context::Settings;
use crate::output::{Output, table};

pub(crate) async fn run(
    command: &BenchCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let mut common = Common::new(
        addresses(settings.brokers()?).await?,
        settings.tenant()?.to_string(),
        settings.namespace.clone(),
        settings.token()?,
    );
    common.client_config = Some(client_config(settings)?);
    common.server_name = server_name(settings);
    common.environment = "felix-cli".to_string();
    common.prose = false;

    let (name, scenario) = match command {
        BenchCommand::Ingest(args) => {
            common.total = args.total;
            common.payload_bytes = args.payload_bytes;
            common.concurrency = args.concurrency;
            common.batch = args.batch;
            let options = IngestOptions {
                keys: args.keys,
                in_flight: args.in_flight,
                duration: args.duration_secs.map(std::time::Duration::from_secs_f64),
                ..IngestOptions::default()
            };
            (
                "ingest",
                Scenario::Ingest {
                    stream: args.stream.clone(),
                    options,
                },
            )
        }
        BenchCommand::Latency(args) => {
            size(&mut common, &args.size);
            ("latency", pubsub(&args.stream))
        }
        BenchCommand::Fanout(args) => {
            size(&mut common, &args.size);
            common.fanout = args.fanout;
            ("fanout", pubsub(&args.stream))
        }
        BenchCommand::Cache(args) => {
            size(&mut common, &args.size);
            common.concurrency = args.concurrency;
            (
                "cache",
                Scenario::Cache {
                    cache: args.cache.clone(),
                },
            )
        }
    };

    if !out.json {
        eprintln!("running {name}...");
    }
    let result = felix_loadgen::run(&common, &scenario).await?;
    let lines = summarize(&result);
    if out.json {
        return out.json_value(&serde_json::json!({
            "bench": name,
            "summary": lines,
            "result": result,
        }));
    }
    out.text(&render(name, &lines))
}

/// One row of a summary.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct Line {
    pub(crate) label: &'static str,
    /// Operations per second.
    pub(crate) rate: Option<f64>,
    pub(crate) unit: &'static str,
    pub(crate) p50_us: Option<u64>,
    pub(crate) p99_us: Option<u64>,
}

/// The rows worth printing for a felix-loadgen result.
pub(crate) fn summarize(result: &Value) -> Vec<Line> {
    let latency = |value: &Value| (value["p50"].as_u64(), value["p99"].as_u64());
    match result["scenario"].as_str() {
        Some("ingest") => {
            let (p50_us, p99_us) = latency(&result["batch_ack_latency_us"]);
            let mut lines = vec![Line {
                label: "published",
                rate: result["throughput_msg_s"].as_f64(),
                unit: "msg/s",
                p50_us: None,
                p99_us: None,
            }];
            if result["acked_throughput_msg_s"].is_number() {
                lines.push(Line {
                    label: "acked (batch ack latency)",
                    rate: result["acked_throughput_msg_s"].as_f64(),
                    unit: "msg/s",
                    p50_us,
                    p99_us,
                });
            }
            lines
        }
        Some("pubsub") => {
            let mut lines = Vec::new();
            let (ack_p50, ack_p99) = latency(&result["ack_latency_us"]);
            lines.push(Line {
                label: "publish (ack latency)",
                rate: result["publish_throughput_msg_s"].as_f64(),
                unit: "msg/s",
                p50_us: ack_p50,
                p99_us: ack_p99,
            });
            let (p50_us, p99_us) = latency(&result["delivery_latency_us"]);
            lines.push(Line {
                label: "delivered (publish to delivery)",
                rate: result["delivered_throughput_msg_s"].as_f64(),
                unit: "msg/s",
                p50_us,
                p99_us,
            });
            lines
        }
        Some("cache") => ["put", "get"]
            .into_iter()
            .map(|half| {
                let (p50_us, p99_us) = latency(&result[half]["latency_us"]);
                Line {
                    label: if half == "put" { "put" } else { "get" },
                    rate: result[half]["throughput_op_s"].as_f64(),
                    unit: "op/s",
                    p50_us,
                    p99_us,
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The summary as a table, headed by the scenario name.
pub(crate) fn render(name: &str, lines: &[Line]) -> String {
    let dash = || "-".to_string();
    let rows = lines
        .iter()
        .map(|line| {
            vec![
                line.label.to_string(),
                line.rate
                    .map_or_else(dash, |rate| format!("{rate:.0} {}", line.unit)),
                line.p50_us.map_or_else(dash, micros),
                line.p99_us.map_or_else(dash, micros),
            ]
        })
        .collect();
    format!("{name}\n{}", table(&["", "RATE", "P50", "P99"], rows))
}

/// `850 us` or `12.3 ms`.
pub(crate) fn micros(us: u64) -> String {
    if us >= 10_000 {
        format!("{:.1} ms", us as f64 / 1000.0)
    } else {
        format!("{us} us")
    }
}

fn size(common: &mut Common, size: &BenchSize) {
    common.total = size.total;
    common.warmup = size.warmup;
    common.payload_bytes = size.payload_bytes;
}

fn pubsub(stream: &str) -> Scenario {
    Scenario::Pubsub {
        stream: stream.to_string(),
        binary: false,
        via_entry: false,
    }
}

#[cfg(test)]
mod tests;
