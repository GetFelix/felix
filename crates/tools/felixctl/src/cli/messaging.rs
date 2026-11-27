//! `pub` and `sub`: their arguments and the value types they share.

use std::path::PathBuf;

use clap::{Args, ValueEnum};

#[derive(Debug, Args)]
pub(crate) struct PubArgs {
    /// Stream to publish to
    pub(crate) stream: String,
    /// The message. Without it, --file or stdin is read
    pub(crate) data: Option<String>,
    /// Publish this file's contents as one message
    #[arg(long, value_name = "PATH", conflicts_with = "data")]
    pub(crate) file: Option<PathBuf>,
    /// Read all of stdin as one message instead of one per line
    #[arg(long, conflicts_with_all = ["data", "file"])]
    pub(crate) whole: bool,
    /// Routing key; records with the same key go to the same shard
    #[arg(long, value_name = "KEY")]
    pub(crate) key: Option<String>,
    /// Publish each message this many times
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub(crate) count: u64,
    /// What to wait for: `message` waits for each acknowledgement, `none`
    /// sends without one
    #[arg(long, value_enum, value_name = "MODE", default_value_t = AckArg::Message)]
    pub(crate) ack: AckArg,
    /// Publish through an idempotent producer, so a re-send after a
    /// reconnect cannot duplicate a record. Always acknowledged
    #[arg(long)]
    pub(crate) idempotent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum AckArg {
    /// Wait for each message's acknowledgement
    Message,
    /// Do not ask for acknowledgements
    None,
}

#[derive(Debug, Args)]
pub(crate) struct SubArgs {
    /// Stream to read
    pub(crate) stream: String,
    /// Where to start: `latest`, `earliest`, or a log offset
    #[arg(long, value_name = "POSITION", default_value = "latest")]
    pub(crate) from: StartArg,
    /// Read only this shard
    #[arg(long, value_name = "N")]
    pub(crate) shard: Option<u32>,
    /// Stop after this many messages
    #[arg(long, value_name = "N")]
    pub(crate) count: Option<u64>,
    /// How to print each message. --json is the same as --format json
    #[arg(long, value_enum, value_name = "FORMAT", default_value_t = FormatArg::Raw)]
    pub(crate) format: FormatArg,
}

/// `--from`: the live tail, the oldest retained record, or an offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartArg {
    Latest,
    Earliest,
    Offset(u64),
}

impl std::str::FromStr for StartArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "latest" => Ok(Self::Latest),
            "earliest" => Ok(Self::Earliest),
            other => other
                .parse()
                .map(Self::Offset)
                .map_err(|_| format!("expected `latest`, `earliest` or an offset, not {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum FormatArg {
    /// The payload only, one per line
    Raw,
    /// The shard and offset, then the payload
    Offsets,
    /// One JSON object per line
    Json,
}
