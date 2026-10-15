//! Flags, named as felix-loadgen names them where they mean the same thing.

use anyhow::{Context, Result, bail};

pub(crate) enum Mode {
    Js,
    Core,
}

impl Mode {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Mode::Js => "js",
            Mode::Core => "core",
        }
    }
}

pub(crate) struct Args {
    pub server: String,
    /// CA for the server certificate; `None` only with `--no-tls`.
    pub tlsca: Option<String>,
    pub mode: Mode,
    pub stream: String,
    pub subject: String,
    pub warmup: usize,
    pub total: usize,
    pub payload_bytes: usize,
    pub fanout: usize,
    pub environment: String,
    /// Create the stream if missing (`file` or `memory`); for local runs,
    /// where nats-agent has not made it.
    pub ensure_stream: Option<String>,
}

fn usage() -> ! {
    eprintln!(
        "nats-latency — felix-loadgen's pubsub latency scenario against NATS

  --server <url>         e.g. tls://10.0.0.4:4222 (required)
  --tlsca <path>         CA for the server certificate (default /etc/nats/tls/ca.pem)
  --no-tls               plain TCP, for a local smoke test only
  --mode <js|core>       JetStream publish + PubAck, or core publish + flush (default js)
  --stream <name>        JetStream stream (default bench0)
  --subject <subject>    subject to publish to (default bench.0)
  --warmup <n>           discarded publishes (default 2000)
  --total <n>            measured publishes (default 20000)
  --payload-bytes <n>    payload size; under 16 grows to the header (default 256)
  --fanout <n>           subscribers; delivery is sampled on the first (default 1)
  --batch 1              accepted for felix-loadgen's flag set; only 1 is supported
  --environment <label>  stamped into LOADGEN_JSON (default unknown)
  --ensure-stream <file|memory>  create the stream if it is missing (local runs)"
    );
    std::process::exit(2);
}

pub(crate) fn parse_args() -> Result<Args> {
    let mut server = None;
    let mut tlsca = Some("/etc/nats/tls/ca.pem".to_string());
    let mut mode = Mode::Js;
    let mut stream = "bench0".to_string();
    let mut subject = "bench.0".to_string();
    let mut warmup = 2000usize;
    let mut total = 20000usize;
    let mut payload_bytes = 256usize;
    let mut fanout = 1usize;
    let mut environment = "unknown".to_string();
    let mut ensure_stream = None;

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = |name: &str| -> Result<String> {
            args.next().with_context(|| format!("{name} needs a value"))
        };
        match flag.as_str() {
            "--server" => server = Some(value("--server")?),
            "--tlsca" => tlsca = Some(value("--tlsca")?),
            "--no-tls" => tlsca = None,
            "--mode" => {
                mode = match value("--mode")?.as_str() {
                    "js" => Mode::Js,
                    "core" => Mode::Core,
                    other => bail!("--mode is js or core, not {other}"),
                }
            }
            "--stream" => stream = value("--stream")?,
            "--subject" => subject = value("--subject")?,
            "--warmup" => warmup = value("--warmup")?.parse().context("--warmup")?,
            "--total" => total = value("--total")?.parse().context("--total")?,
            "--payload-bytes" => {
                payload_bytes = value("--payload-bytes")?
                    .parse()
                    .context("--payload-bytes")?
            }
            "--fanout" => fanout = value("--fanout")?.parse().context("--fanout")?,
            "--batch" => {
                if value("--batch")? != "1" {
                    bail!("only --batch 1 is supported");
                }
            }
            "--environment" => environment = value("--environment")?,
            "--ensure-stream" => ensure_stream = Some(value("--ensure-stream")?),
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unknown flag {other}");
                usage();
            }
        }
    }
    Ok(Args {
        server: server.context("--server is required")?,
        tlsca,
        mode,
        stream,
        subject,
        warmup,
        total,
        payload_bytes,
        fanout,
        environment,
        ensure_stream,
    })
}
