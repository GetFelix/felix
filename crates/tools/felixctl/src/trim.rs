//! `felixctl stream trim`.

use felix_client::ConditionalTrim;

use crate::cli::StreamTrimArgs;
use crate::connect::Broker;
use crate::context::Settings;
use crate::error::{Exit, fail};
use crate::output::Output;

pub(crate) async fn run(
    args: &StreamTrimArgs,
    settings: &Settings,
    out: &Output,
) -> anyhow::Result<()> {
    let broker = Broker::connect(settings).await?;
    let outcome = broker
        .cluster
        .trim_before(
            &broker.tenant,
            &broker.namespace,
            &args.stream,
            args.shard,
            args.before,
            args.expected_offset,
            args.headers.as_deref().map(str::as_bytes),
        )
        .await?;
    match outcome {
        ConditionalTrim::Trimmed { offset, oldest } => out.done(
            &trimmed_line(&args.stream, args.shard, offset, oldest),
            serde_json::json!({
                "stream": args.stream,
                "shard": args.shard,
                "offset": offset,
                "oldest": oldest,
            }),
        ),
        ConditionalTrim::Refused { tail } => Err(fail(Exit::Server, refused_line(args, tail))),
    }
}

fn trimmed_line(stream: &str, shard: u32, offset: Option<u64>, oldest: u64) -> String {
    match offset {
        Some(offset) => format!(
            "trimmed {stream} shard {shard}: trim record at offset {offset}, reads now start at {oldest}"
        ),
        None => {
            format!("nothing to trim on {stream} shard {shard}: reads already start at {oldest}")
        }
    }
}

fn refused_line(args: &StreamTrimArgs, tail: u64) -> String {
    format!(
        "{} shard {} is at offset {tail}, not {}; nothing was written",
        args.stream, args.shard, args.expected_offset
    )
}

#[cfg(test)]
mod tests;
