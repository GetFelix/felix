//! `felixctl counter get|add`: counters, which live in a cache's shards
//! beside its keys and are routed the same way.

use clap::Subcommand;
use serde_json::json;

use crate::connect::Broker;
use crate::context::Settings;
use crate::error::{Exit, fail};
use crate::output::Output;

#[derive(Debug, Subcommand)]
pub(crate) enum CounterCommand {
    /// Print a counter's sum
    #[command(
        long_about = "Print a counter's sum. Exits with status 5 when it has never been \
                      written.",
        after_long_help = "Examples:
  felixctl counter get stats page-views
  felixctl counter get stats page-views --json"
    )]
    Get {
        /// Cache the counter lives in
        cache: String,
        /// Counter key
        key: String,
    },
    /// Add to a counter and print the new sum
    #[command(
        long_about = "Add DELTA, which may be negative, to a counter and print its sum \
                      including the add. A counter that has never been written starts \
                      at zero. The add is sent once: a failure is not retried, since the \
                      broker may already have applied it.",
        after_long_help = "Examples:
  felixctl counter add stats page-views 1
  felixctl counter add stats stock -3"
    )]
    Add {
        /// Cache the counter lives in
        cache: String,
        /// Counter key
        key: String,
        /// Signed amount to add
        #[arg(allow_negative_numbers = true)]
        delta: i64,
    },
}

pub(crate) async fn run(
    command: &CounterCommand,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let broker = Broker::connect(settings).await?;
    let (tenant, namespace) = (broker.tenant.as_str(), broker.namespace.as_str());
    match command {
        CounterCommand::Get { cache, key } => {
            let Some(sum) = broker
                .cluster
                .counter_get(tenant, namespace, cache, key)
                .await?
            else {
                return Err(fail(
                    Exit::NotFound,
                    format!("counter {cache}/{key} is not set"),
                ));
            };
            out.done(
                &sum.to_string(),
                json!({ "cache": cache, "key": key, "value": sum }),
            )
        }
        CounterCommand::Add { cache, key, delta } => {
            let sum = broker
                .cluster
                .counter_add(tenant, namespace, cache, key, *delta)
                .await?;
            out.done(
                &sum.to_string(),
                json!({ "cache": cache, "key": key, "delta": delta, "value": sum }),
            )
        }
    }
}
