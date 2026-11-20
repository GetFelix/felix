//! An independent checker for a Felix failover.
//!
//! It runs N idempotent producers at a steady rate, a reader that follows the
//! stream, and prints one line a second. It judges the run against a full
//! re-read of the stream, not against what the reader happened to deliver.
//! See `demos/failover/README.md` for exactly what it checks.

mod ledger;
mod payload;
mod producer;
mod reader;
mod receipt;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use felix_client::{Client, ClientConfig, ReconnectPolicy};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

use ledger::Ledger;

/// Where the checker connects and what it writes.
#[derive(Debug, Parser)]
#[command(name = "failover-check", about = "Check a Felix failover from outside")]
pub(crate) struct Args {
    /// Broker addresses, comma separated: host:port
    #[arg(long, env = "FELIX_BROKERS", value_delimiter = ',')]
    brokers: Vec<String>,
    /// TLS server name the brokers' certificates carry
    #[arg(long, env = "FELIX_SERVER_NAME", default_value = "localhost")]
    server_name: String,
    #[arg(long, env = "FELIX_AUTH_TENANT")]
    tenant: String,
    #[arg(long, env = "FELIX_AUTH_TOKEN_FILE")]
    token_file: String,
    /// PEM file holding every broker's certificate
    #[arg(long, env = "FELIX_CA_FILE")]
    ca_file: String,
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "orders")]
    stream: String,
    /// Producers, each with its own sequence numbers
    #[arg(long, default_value_t = 4)]
    producers: u16,
    /// Writes per second, per producer
    #[arg(long, default_value_t = 25)]
    rate: u32,
    /// Payload size in bytes
    #[arg(long, default_value_t = 128)]
    payload_bytes: usize,
    /// Stop after this many seconds and print the final receipt. 0 runs
    /// until SIGTERM
    #[arg(long, default_value_t = 0)]
    duration: u64,
    /// After a stop, how long a write in doubt may keep being re-sent
    #[arg(long, default_value_t = 30)]
    drain_secs: u64,
    /// Where the receipt is written
    #[arg(long, default_value = "/out/receipt.txt")]
    receipt_file: String,
    /// Stream settings to print in the receipt, as given by the compose file
    #[arg(long, env = "FAILOVER_CONFIG", default_value = "")]
    config_note: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(Args::parse()).await {
        eprintln!("failover-check: {err:#}");
        std::process::exit(2);
    }
}

async fn run(args: Args) -> Result<()> {
    anyhow::ensure!(!args.brokers.is_empty(), "no brokers given");
    let cluster = Arc::new(connect(&args).await?);
    let started = Instant::now();
    let run_id = payload::run_id();
    let ledger = Arc::new(Mutex::new(Ledger::new(started)));
    ledger
        .lock()
        .expect("ledger")
        .expect_producers(args.producers);
    let args = Arc::new(args);
    println!(
        "run {run_id}: {} producers x {}/s, stream {}/{}",
        args.producers, args.rate, args.namespace, args.stream
    );

    let (stop_tx, stop_rx) = watch::channel(false);
    let reader = tokio::spawn(reader::follow(
        Arc::clone(&cluster),
        Arc::clone(&args),
        run_id.clone(),
        Arc::clone(&ledger),
    ));
    let mut producers = Vec::new();
    for index in 0..args.producers {
        producers.push(tokio::spawn(producer::run(
            index,
            Arc::clone(&cluster),
            Arc::clone(&args),
            run_id.clone(),
            Arc::clone(&ledger),
            stop_rx.clone(),
        )));
    }

    let mut term = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut usr1 = signal(SignalKind::user_defined1())?;
    let deadline = (args.duration > 0)
        .then(|| tokio::time::Instant::now() + Duration::from_secs(args.duration));
    let mut second = tokio::time::interval(Duration::from_secs(1));
    let mut watchdog = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = watchdog.tick() => {
                let mut ledger = ledger.lock().expect("ledger");
                if let Some(since) = ledger.watch(Instant::now()) {
                    let waited = Instant::now().saturating_duration_since(since);
                    println!("[{:>4}s] writes waiting: oldest publish unanswered for {} ms",
                        started.elapsed().as_secs(), waited.as_millis());
                }
            }
            _ = second.tick() => receipt::print_line(&ledger, started),
            _ = usr1.recv() => {
                receipt::interim(&cluster, &args, &run_id, &ledger, started).await;
            }
            _ = term.recv() => break,
            _ = interrupt.recv() => break,
            _ = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => break,
        }
    }

    println!(
        "stopping producers; writes in doubt are re-sent for up to {} s",
        args.drain_secs
    );
    let _ = stop_tx.send(true);
    let drain = tokio::time::Instant::now() + Duration::from_secs(args.drain_secs);
    for handle in producers {
        let _ = tokio::time::timeout_at(drain, handle).await;
    }
    // Let the reader catch up before the line it prints last.
    tokio::time::sleep(Duration::from_secs(2)).await;
    receipt::print_line(&ledger, started);
    reader.abort();
    let holds = receipt::finish(&cluster, &args, &run_id, &ledger, started).await?;
    std::process::exit(if holds { 0 } else { 1 });
}

async fn connect(args: &Args) -> Result<Client> {
    let roots = felix_client::root_store_from_pem_file(&args.ca_file)?;
    let quinn = felix_client::quic_client_config(Some(Arc::new(roots)), false)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(args.tenant.clone());
    config.auth_token = Some(
        std::fs::read_to_string(&args.token_file)
            .with_context(|| format!("read {}", args.token_file))?
            .trim()
            .to_string(),
    );
    let seeds = args
        .brokers
        .iter()
        .map(|broker| broker.parse::<felix_client::Seed>())
        .collect::<Result<Vec<_>>>()?;
    // The brokers may still be settling when this starts, so keep trying.
    let mut last = None;
    for _ in 0..60 {
        match Client::connect_seeds(
            &seeds,
            &args.server_name,
            config.clone(),
            ReconnectPolicy::default(),
        )
        .await
        {
            Ok(client) => return Ok(client),
            Err(err) => {
                last = Some(err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Err(last.expect("at least one attempt")).context("connect to the brokers")
}
