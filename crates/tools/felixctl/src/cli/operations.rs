//! Audit, feature and Raft commands.

use clap::Subcommand;

use super::reform::ReformCommand;
use super::root::Confirm;

#[derive(Debug, Subcommand)]
pub(crate) enum AuditCommand {
    /// List audit records, oldest first
    #[command(
        visible_alias = "list",
        long_about = "List audit records after a sequence number, oldest first. With --follow, \
                      keep polling for new records; with --json, print one record per line, \
                      ready to ship to a log store. A warning on stderr says when records were \
                      trimmed before they were read.",
        after_long_help = "Examples:
  felixctl audit ls
  felixctl audit ls --after 1200 --limit 50
  felixctl audit ls --follow --json"
    )]
    Ls {
        /// Start after this sequence number (0 lists from the oldest kept)
        #[arg(long, default_value_t = 0)]
        after: u64,
        /// Records per request, up to 1000
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Keep polling for new records until interrupted
        #[arg(long)]
        follow: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum FeaturesCommand {
    /// List the features the brokers support and the ones that are on
    #[command(
        long_about = "Show how many brokers are serving, the features every one of them \
                      supports, and the features finalized fleet-wide.",
        after_long_help = "Examples:
  felixctl features ls
  felixctl features ls --json"
    )]
    Ls,
    /// Switch a feature on for the whole fleet. One-way
    #[command(
        long_about = "Switch a feature on for the whole fleet. ONE-WAY: it cannot be turned \
                      off, and brokers without the feature are refused from then on. The \
                      control plane refuses it unless every serving broker supports the \
                      feature and the features it needs are on. --dry-run says whether it \
                      would be accepted and changes nothing.",
        after_long_help = "Examples:
  felixctl features finalize jump_hash_routing --dry-run
  felixctl features finalize jump_hash_routing --yes"
    )]
    Finalize {
        /// Feature name, as `features ls` lists it
        feature: String,
        /// Say whether it could be finalized, without finalizing it
        #[arg(long)]
        dry_run: bool,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum RaftCommand {
    /// Plan re-forming Raft groups that lost their majority
    #[command(
        subcommand,
        long_about = "Re-form Raft groups that lost their majority. `plan` shows what a \
                      re-form would do and changes nothing; `apply` carries out a plan \
                      you have read.",
        after_long_help = "Examples:
  felixctl raft reform plan
  felixctl raft reform plan --scope node broker-2
  felixctl raft reform apply 3f2a9c"
    )]
    Reform(ReformCommand),
}
