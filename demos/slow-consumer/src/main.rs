//! # Felix demo: slow-consumer isolation
//!
//! ## What this shows
//! One telemetry stream, three dashboard consumers. Partway through, one consumer
//! stops draining — a blocked render loop, a degraded link, a paused container. The
//! demo runs that identical scenario twice, under the two subscriber-queue policies,
//! and puts the results side by side.
//!
//! Under `drop_new` (the production default) the fault stays local: the healthy
//! consumers keep their rate and their tail latency, the publisher is never
//! throttled, and the stalled consumer sheds events that are counted and gone.
//!
//! Under `block` nothing is lost, and the cost lands everywhere: the stalled
//! consumer backpressures the publisher, which slows delivery to the healthy
//! consumers too.
//!
//! ## Why this demo exists
//! `docs-site/docs/getting-started/what-felix-is-for.md` describes Felix today as
//! "optimized for high-fanout delivery with predictable tail latency and strict
//! slow-consumer isolation". This is that claim, made runnable.
//!
//! Neither policy is the right answer. Quoting
//! `docs-site/docs/development/how-felix-works.md`: "dropping isolates healthy
//! publishers and subscribers from a slow consumer; blocking preserves delivery but
//! can let one slow subscriber throttle every producer of that stream." Which one you
//! want is a product decision. The demo exists to make the decision concrete.
//!
//! ## Honest limits
//! Single-node, loopback, fanout 3. These numbers say nothing about behaviour at
//! thousands of subscribers or across a real network. Dropped events are gone
//! permanently, because this demo runs an ephemeral stream that drops on
//! overflow — that is what makes the drops observable. Felix offers stronger
//! options: a durable stream replays by offset, and a queue redelivers with
//! bounded attempts and a dead-letter destination.

mod render;
mod scenario;

use anyhow::{Result, bail};
use scenario::{Policy, RunConfig, RunOutcome};
use std::io::IsTerminal;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DemoArgs {
    pub target_rate: u64,
    pub subscribers: usize,
    pub payload_bytes: usize,
    pub queue_capacity: usize,
    /// Seconds per phase. The run is four phases long: baseline, stall, stall, recover.
    pub phase_secs: u64,
    pub policies: Vec<Policy>,
    pub tui: bool,
}

impl Default for DemoArgs {
    fn default() -> Self {
        Self {
            target_rate: 20_000,
            subscribers: 3,
            payload_bytes: 256,
            // The broker default is 512; the demo default matches it so what the
            // viewer sees is the shipped behaviour, not a tuned-for-demo variant.
            queue_capacity: 512,
            phase_secs: 5,
            policies: vec![Policy::DropNew, Policy::Block],
            tui: true,
        }
    }
}

fn parse_args<I: Iterator<Item = String>>(args: I) -> Result<DemoArgs> {
    let mut out = DemoArgs::default();
    let args: Vec<String> = args.collect();
    let mut idx = 0;
    while idx < args.len() {
        let take = |idx: &mut usize| -> Result<String> {
            *idx += 1;
            match args.get(*idx) {
                Some(value) => Ok(value.clone()),
                None => bail!("missing value for {}", args[*idx - 1]),
            }
        };
        match args[idx].as_str() {
            "--rate" => out.target_rate = take(&mut idx)?.parse()?,
            "--subscribers" => out.subscribers = take(&mut idx)?.parse()?,
            "--payload" => out.payload_bytes = take(&mut idx)?.parse()?,
            "--queue-capacity" => out.queue_capacity = take(&mut idx)?.parse()?,
            "--duration" => out.phase_secs = take(&mut idx)?.parse()?,
            "--policy" => {
                out.policies = match take(&mut idx)?.as_str() {
                    "drop_new" => vec![Policy::DropNew],
                    "block" => vec![Policy::Block],
                    "both" => vec![Policy::DropNew, Policy::Block],
                    other => bail!("unknown policy {other}; expected drop_new, block, or both"),
                }
            }
            "--no-tui" => out.tui = false,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
        idx += 1;
    }
    if out.subscribers < 2 {
        bail!(
            "--subscribers must be at least 2: the point is that one stalls and the others do not"
        );
    }
    Ok(out)
}

fn print_usage() {
    println!(
        "\
Felix demo: slow-consumer isolation

One stream, N dashboard consumers, one of which stalls. Runs under both subscriber
queue policies and compares them.

  --rate N             target publish rate, messages/sec (default 20000)
  --subscribers N      consumer count, >= 2 (default 3)
  --payload N          payload bytes (default 256)
  --queue-capacity N   subscriber queue depth (default 512, the broker default)
  --duration N         seconds per phase; a run is 4 phases (default 5)
  --policy P           drop_new | block | both (default both)
  --no-tui             plain text output instead of the terminal UI
  -h, --help           this message
"
    );
}

pub async fn run_demo(args: DemoArgs) -> Result<Vec<RunOutcome>> {
    // A recorder must be installed before the broker starts for its counters to be
    // readable. Installed once for the whole process; counters are cumulative across
    // both policy runs, which is fine for naming the checkpoint that shed.
    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .ok();
    // Fall back automatically when stdout is not a terminal. Entering raw mode
    // against a pipe or a CI log either errors or writes cursor escapes into the
    // captured output, so `cargo run | tee` and CI both get readable output without
    // the caller having to remember `--no-tui`.
    let interactive = args.tui && std::io::stdout().is_terminal();
    if args.tui && !interactive {
        eprintln!("stdout is not a terminal; using plain output");
    }
    let mut outcomes = Vec::new();
    for policy in args.policies.clone() {
        let config = RunConfig {
            policy,
            subscribers: args.subscribers,
            target_rate: args.target_rate,
            payload_bytes: args.payload_bytes,
            queue_capacity: args.queue_capacity,
            phase_secs: args.phase_secs,
        };
        let outcome = if interactive {
            render::run_with_tui(config).await?
        } else {
            render::run_with_plain(config).await?
        };
        if let Some(handle) = &metrics {
            let drops = scenario::scrape_drop_counters(&handle.render());
            if !drops.is_empty() {
                println!("\n  broker drop counters after {}:", policy.label());
                for (name, value) in drops {
                    println!("    {name:<48} {value}");
                }
            }
        }
        outcomes.push(outcome);
        // A beat between runs so a viewer can register that the policy changed.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    Ok(outcomes)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let outcomes = run_demo(args).await?;
    // Printed after the alternate screen is torn down, so the comparison is what
    // remains in the user's scrollback when the demo exits.
    render::print_comparison(&outcomes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_rejects_a_single_subscriber() {
        let err = parse_args(["--subscribers", "1"].iter().map(|s| s.to_string()))
            .expect_err("one subscriber cannot demonstrate isolation");
        assert!(err.to_string().contains("at least 2"));
    }

    #[test]
    fn parse_args_reads_policy_selection() {
        let args = parse_args(["--policy", "block"].iter().map(|s| s.to_string())).expect("parse");
        assert_eq!(args.policies, vec![Policy::Block]);
    }

    /// The property the project's headline claim rests on: under `drop_new`, a
    /// consumer that stops reading loses events while the healthy consumers lose
    /// none, and the publisher keeps running.
    ///
    /// This is the demo doubling as a behavioural regression test. If isolation ever
    /// breaks, this fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drop_new_confines_loss_to_the_stalled_consumer() {
        let args = DemoArgs {
            target_rate: 5_000,
            subscribers: 3,
            payload_bytes: 64,
            // Every consumer drops at its own client queue once it is full, so a
            // healthy one loses events whenever its task goes unscheduled for
            // longer than the queue lasts: 13 ms at 64, which a busy host
            // exceeds. 1024 lasts 200 ms, and the 4 s stall overflows it many
            // times over.
            queue_capacity: 1024,
            phase_secs: 2,
            policies: vec![Policy::DropNew],
            tui: false,
        };
        let outcomes = tokio::time::timeout(Duration::from_secs(90), run_demo(args))
            .await
            .expect("demo should finish well inside the timeout")
            .expect("demo run");

        let run = outcomes.first().expect("one outcome");
        assert_eq!(run.policy, Policy::DropNew);

        assert!(
            run.stalled_gaps() > 0,
            "the stalled consumer should have missed events under drop_new; \
             saw {} gaps. Subscribers: {:?}",
            run.stalled_gaps(),
            run.subscribers
        );

        // Isolation is a claim about orders of magnitude, not about perfection.
        //
        // A healthy consumer's own queue can still overflow briefly on a loaded
        // machine, for reasons unrelated to the stalled consumer, so an exact-zero
        // bound would test how busy the host is.
        let healthy_lost = run.healthy_gaps();
        let healthy_received = run.healthy_received();
        let stalled_lost = run.stalled_gaps();

        let healthy_loss_rate = healthy_lost as f64 / healthy_received.max(1) as f64;
        assert!(
            healthy_loss_rate < 0.001,
            "healthy consumers should lose a negligible fraction of their traffic; \
             lost {healthy_lost} of {healthy_received} ({:.4}%). Subscribers: {:?}",
            healthy_loss_rate * 100.0,
            run.subscribers
        );
        assert!(
            stalled_lost > healthy_lost.saturating_mul(100),
            "the stalled consumer's loss should dwarf anything the healthy ones shed, \
             otherwise the fault is not being contained: stalled={stalled_lost}, \
             healthy={healthy_lost}. Subscribers: {:?}",
            run.subscribers
        );

        assert!(
            run.achieved_rate > 0,
            "the publisher must keep publishing while a consumer is stalled"
        );
    }
}
