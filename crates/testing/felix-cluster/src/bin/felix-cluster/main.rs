//! Start a local Felix cluster, and talk to it from another terminal.
//!
//! ```text
//! # window 1
//! task cluster:up
//!
//! # window 2
//! task cluster:subscribe
//!
//! # window 3
//! task cluster:publish -- hello
//! ```
//!
//! `up` writes the addresses and a credential to a session file so the other
//! commands need nothing copied into them.
//!
//! Every line names the broker it went through and whether the record crossed a
//! node boundary, because that is the only part of this a cluster does
//! differently from a single broker.

mod args;
mod attach;
mod demos;
mod fixture;
mod lifecycle;
mod signals;

use anyhow::{Result, bail};

const STREAM: &str = "orders";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("up");

    match command {
        "up" => lifecycle::up(&args).await,
        "status" => lifecycle::status(&args).await,
        "smoke" => lifecycle::smoke_command(&args).await,
        "demo" => demos::demo(&args).await,
        "failover" => demos::failover(&args).await,
        "consistency" => demos::consistency(&args).await,
        "subscribe" => attach::subscribe(&args).await,
        "publish" => attach::publish(&args).await,
        "owners" => attach::owners().await,
        "client-fixture" => fixture::client_fixture(&args[1..]).await,
        "nodes" => attach::nodes(),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        other => {
            print_help();
            bail!("unknown command {other}")
        }
    }
}

fn print_help() {
    eprintln!(
        "\
felix-cluster — a local multi-node Felix cluster

  up [--nodes N] [--stream NAME] [--shards N] [--replication-factor N]
                                  start a cluster and hold it until Ctrl-C
  status [--nodes N]              start, print membership and ownership, exit
  smoke [--nodes N]               publish through a non-owner, receive from the owner
  demo [--pace SECONDS]           the whole cross-broker story, start to finish
  failover [--pace SECONDS]       replicate, kill the leader, keep publishing
  consistency [--pace SECONDS]    what Quorum buys, and what Leader costs, under one fault
  client-fixture [--out PATH]     a cluster for a client conformance suite to run against
  nodes                           every broker in a running cluster, one per line
  owners                          who leads each shard of a running cluster
  subscribe STREAM [--on NODE]    stream events from a running cluster
  publish STREAM MSG [--via NODE] publish to a running cluster

`up` first; the rest attach to it."
    );
}

pub(crate) fn init_tracing(verbose: bool) {
    let default = if verbose { "info" } else { "warn" };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default)),
        )
        .try_init();
}
