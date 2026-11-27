//! Cluster re-formation and `backup-point`.

use std::path::PathBuf;

use clap::{Args, Subcommand};

#[derive(Debug, Subcommand)]
pub(crate) enum ReformCommand {
    /// Survey the brokers and print what a re-form would do, changing nothing
    #[command(
        long_about = "Ask the control plane for a re-form plan and wait while it surveys \
                      the brokers placed on each Raft shard in scope. Prints one line per \
                      shard: `reform` with the copy it would start from and the new \
                      voters, `blocked` with the reason, or `left_alone` when the group \
                      still has a leader. A plan that could lose acknowledged records says \
                      so and names them. The plan changes nothing on any broker or in the \
                      control plane. It is kept by the control-plane leader only, so after \
                      a leader change, plan again. A survey still running after 5 minutes \
                      prints what it has and exits with an error. With --restore-point, \
                      each shard re-forms from the backup point after `felix-broker \
                      restore-point`: a shard whose base does not end at the point is \
                      blocked, and the next epoch gives out no offset below the point plus \
                      --offset-gap.",
        after_long_help = "Examples:
  felixctl raft reform plan
  felixctl raft reform plan --scope cluster
  felixctl raft reform plan --scope node broker-2 --json
  felixctl raft reform plan --restore-point nightly.backup-point.json"
    )]
    Plan(ReformPlanArgs),
    /// Carry out a plan: stop the copies, check the plan still holds, start the next epoch
    #[command(
        long_about = "Apply a plan made with `raft reform plan`. For each shard the plan \
                      marked `reform`, the brokers stop their copies, the control plane \
                      plans again from the stopped copies and skips any shard whose plan \
                      changed (`changed`), then writes the shard's next epoch and the \
                      brokers start it. A shard whose plan may lose acknowledged records \
                      is skipped unless named with --accept-loss. Waits up to 5 minutes \
                      and prints each shard's state: `started`, `skipped`, `changed`, \
                      `left_alone` or `failed`, or the round it is still in. Applying the \
                      same plan again retries the shards that ended without a write.",
        after_long_help = "Examples:
  felixctl raft reform apply 3f2a9c
  felixctl raft reform apply 3f2a9c --accept-loss t1/payments/orders/2
  felixctl raft reform apply 3f2a9c --accept-loss cache:t1/payments/sessions/0 --json"
    )]
    Apply(ReformApplyArgs),
}

/// `raft reform apply`.
#[derive(Debug, Args)]
pub(crate) struct ReformApplyArgs {
    /// The plan id `raft reform plan` printed
    pub(crate) plan_id: String,
    /// A shard, as TENANT/NAMESPACE/STREAM/SHARD (`cache:` in front for a cache
    /// shard), to re-form even though its plan may lose acknowledged records.
    /// Repeat for more shards
    #[arg(long, value_name = "SHARD")]
    pub(crate) accept_loss: Vec<String>,
}

/// `raft reform plan`.
#[derive(Debug, Args)]
pub(crate) struct ReformPlanArgs {
    /// `cluster` for every Raft shard, or `node ID` for the shards placed on one broker
    #[arg(
        long,
        num_args = 1..=2,
        value_names = ["SCOPE", "NODE"],
        default_values_t = ["cluster".to_string()]
    )]
    pub(crate) scope: Vec<String>,
    /// Re-form from the backup point in FILE, written by `felixctl backup-point`
    #[arg(long, value_name = "FILE")]
    pub(crate) restore_point: Option<PathBuf>,
    /// Offsets past the time line (2^14 per millisecond of the control plane's
    /// clock at plan time) the next epoch starts at [default: 2^40]
    #[arg(long, value_name = "N", requires = "restore_point")]
    pub(crate) offset_gap: Option<u64>,
    /// Take an --offset-gap below 2^32, or a control-plane clock behind the point's end
    #[arg(long, requires = "restore_point")]
    pub(crate) accept_short_gap: bool,
}

/// `backup-point NAME`.
#[derive(Debug, Args)]
pub(crate) struct BackupPointArgs {
    /// Name recorded in the manifest
    pub(crate) name: String,
    /// Where to write the manifest [default: NAME.backup-point.json]
    #[arg(long, value_name = "FILE")]
    pub(crate) out: Option<PathBuf>,
    /// Metrics URL of a broker, where the address the control plane has for it
    /// is not the one to use. Repeat for more brokers
    #[arg(long, value_name = "NODE=URL", value_parser = broker_override)]
    pub(crate) broker: Vec<(String, String)>,
    /// Port of the brokers' metrics listeners
    #[arg(long, value_name = "PORT", default_value_t = 8080)]
    pub(crate) metrics_port: u16,
}

/// Parse `NODE=URL`.
fn broker_override(value: &str) -> Result<(String, String), String> {
    match value.split_once('=') {
        Some((node, url)) if !node.is_empty() && !url.is_empty() => {
            Ok((node.to_string(), url.trim_end_matches('/').to_string()))
        }
        _ => Err(format!("expected NODE=URL, not {value:?}")),
    }
}
