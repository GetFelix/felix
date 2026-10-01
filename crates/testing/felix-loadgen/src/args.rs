//! Command-line flags. Parsed by hand: the instrument has one flat set of
//! them, and `--help` is the documentation.

use anyhow::{Context, Result, bail};

use crate::scenarios::{Common, IngestOptions};

/// What the command line asked for.
pub(crate) struct Args {
    pub(crate) common: Common,
    pub(crate) ingest: IngestOptions,
    pub(crate) scenario: String,
    pub(crate) stream: String,
    pub(crate) cache: String,
    pub(crate) binary: bool,
    /// pubsub: publish through the first broker instead of the shard's owner.
    pub(crate) via_entry: bool,
}

/// Print the flags and exit with status 2.
pub(crate) fn usage() -> ! {
    eprintln!(
        "felix-loadgen — drive a remote Felix cluster and measure it

  --brokers <addr,addr,...>   client-facing broker addresses (required)
  --tenant <id>               tenant to authenticate as (required)
  --token <jwt>               Felix token, or --token-file <path>
  --namespace <ns>            default: default
  --scenario <name>           pubsub | cache | counter | watch | queue | retained (required)
  --stream <name>             stream for pubsub (default: perf)
  --keys <n>                  ingest: spread batches over n routing keys (default 0,
                              unkeyed -- every record lands on shard 0)
  --in-flight <n>             ingest: acked batches each publisher keeps outstanding
                              (default 0, fire-and-forget)
  --duration-secs <n>         ingest: publish for n seconds instead of --total records
  --start-at <unix-secs>      ingest: connect, then start publishing at this wall-clock
                              time (fractional seconds), so generators overlap
  --cache <name>              cache scope for cache/counter/watch (default: perf)
  --warmup <n>                discarded operations (default: 2000)
  --total <n>                 measured operations (default: 20000)
  --payload-bytes <n>         payload size (default: 256)
  --fanout <n>                subscribers or watchers (default: 1)
  --slow-subscribers <n>      make the last n pubsub subscribers dawdle (default: 0)
  --slow-delay-ms <n>         per-delivery delay for a slow subscriber (default: 0)
  --batch <n>                 publish batch size; 1 = per-message ack (default: 1)
  --binary                    binary publish framing (no per-message ack)
  --via-entry                 pubsub: publish through the first broker, which relays
                              to the shard's owner, instead of to the owner directly
  --concurrency <n>           workers for cache/counter (default: 8)
  --environment <label>       stamped into LOADGEN_JSON (default: unknown)

Certificates are accepted without verification — brokers self-sign and
publish nothing to verify against. Run this only against a cluster you own,
on a network you own."
    );
    std::process::exit(2);
}

/// Read the process arguments. `--help` and an unknown flag exit through
/// [`usage`].
pub(crate) fn parse_args() -> Result<Args> {
    let mut brokers = Vec::new();
    let mut tenant = None;
    let mut token = None;
    let mut namespace = "default".to_string();
    let mut scenario = None;
    let mut stream = "perf".to_string();
    let mut cache = "perf".to_string();
    let mut warmup = 2000usize;
    let mut total = 20000usize;
    let mut payload_bytes = 256usize;
    let mut fanout = 1usize;
    let mut batch = 1usize;
    let mut binary = false;
    let mut via_entry = false;
    let mut concurrency = 8usize;
    let mut ingest = IngestOptions::default();
    let mut environment = "unknown".to_string();
    let mut slow_subscribers = 0usize;
    let mut slow_delay_ms = 0u64;

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = |name: &str| -> Result<String> {
            args.next().with_context(|| format!("{name} needs a value"))
        };
        match flag.as_str() {
            "--brokers" => {
                for part in value("--brokers")?.split(',') {
                    brokers.push(
                        part.trim().parse().with_context(|| {
                            format!("--brokers entry {part:?} is not host:port")
                        })?,
                    );
                }
            }
            "--tenant" => tenant = Some(value("--tenant")?),
            "--token" => token = Some(value("--token")?),
            "--token-file" => {
                let path = value("--token-file")?;
                token = Some(
                    std::fs::read_to_string(&path)
                        .with_context(|| format!("read {path}"))?
                        .trim()
                        .to_string(),
                );
            }
            "--namespace" => namespace = value("--namespace")?,
            "--scenario" => scenario = Some(value("--scenario")?),
            "--stream" => stream = value("--stream")?,
            "--cache" => cache = value("--cache")?,
            "--warmup" => warmup = value("--warmup")?.parse().context("--warmup")?,
            "--total" => total = value("--total")?.parse().context("--total")?,
            "--payload-bytes" => {
                payload_bytes = value("--payload-bytes")?
                    .parse()
                    .context("--payload-bytes")?
            }
            "--fanout" => fanout = value("--fanout")?.parse().context("--fanout")?,
            "--slow-subscribers" => {
                slow_subscribers = value("--slow-subscribers")?
                    .parse()
                    .context("--slow-subscribers")?
            }
            "--slow-delay-ms" => {
                slow_delay_ms = value("--slow-delay-ms")?
                    .parse()
                    .context("--slow-delay-ms")?
            }
            "--batch" => batch = value("--batch")?.parse().context("--batch")?,
            "--binary" => binary = true,
            "--via-entry" => via_entry = true,
            // How many distinct routing keys to spread the batches over.
            // 0 keeps the unkeyed behaviour, where every record resolves to
            // shard 0 regardless of the stream's shard count -- which is what
            // made every multi-shard measurement so far a single-shard one.
            "--keys" => ingest.keys = value("--keys")?.parse().context("--keys")?,
            "--in-flight" => {
                ingest.in_flight = value("--in-flight")?.parse().context("--in-flight")?
            }
            "--duration-secs" => {
                let secs: f64 = value("--duration-secs")?
                    .parse()
                    .context("--duration-secs")?;
                ingest.duration = Some(std::time::Duration::from_secs_f64(secs));
            }
            "--start-at" => {
                let secs: f64 = value("--start-at")?.parse().context("--start-at")?;
                ingest.start_at =
                    Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(secs));
            }
            "--concurrency" => {
                concurrency = value("--concurrency")?.parse().context("--concurrency")?
            }
            "--environment" => environment = value("--environment")?,
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unknown flag {other}");
                usage();
            }
        }
    }

    if brokers.is_empty() {
        bail!("--brokers is required");
    }
    let tenant = tenant.context("--tenant is required")?;
    let token = token.context("--token or --token-file is required")?;
    let scenario = scenario.context("--scenario is required")?;

    Ok(Args {
        ingest,
        common: Common {
            brokers,
            tenant,
            namespace,
            token,
            warmup,
            total,
            payload_bytes,
            fanout,
            batch,
            concurrency,
            environment,
            slow_subscribers,
            slow_delay: std::time::Duration::from_millis(slow_delay_ms),
        },
        scenario,
        stream,
        cache,
        binary,
        via_entry,
    })
}
