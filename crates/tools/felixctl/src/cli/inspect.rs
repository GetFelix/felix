//! Inspection commands (`inspect`, `topology`).

use clap::{Args, Subcommand};

#[derive(Debug, Subcommand)]
pub(crate) enum InspectCommand {
    /// One shard's live state: its assignment and every replica's own view
    #[command(
        long_about = "Show one shard as its brokers see it: the generation and leader, \
                      whether the leader serves and why not, the fence a promoted leader \
                      waits on, its lease, the tail and commit mark, and each replica's \
                      position and state.\n\n\
                      The assignment comes from the control plane when a control-plane URL \
                      is configured, else from the broker connected to. Every broker in it \
                      is then asked directly; one that cannot be reached is listed under \
                      unreachable rather than described. Without --shard, every shard of \
                      the stream is shown.",
        after_long_help = "Examples:
  felixctl inspect shard orders --shard 3
  felixctl inspect shard acme/default/orders
  felixctl inspect shard sessions --cache --shard 0 --json"
    )]
    Shard(InspectShardArgs),
    /// Subscriptions the brokers serve: queues, drops and how far behind
    #[command(
        long_about = "List the subscriptions each broker serves: the stream and shard, \
                      the connection and principal it delivers to, the queue's overflow \
                      policy, depth and capacity, the records it has dropped, and its \
                      position against the shard's tail.\n\n\
                      Every broker the cluster advertises is asked, or only --node. A page \
                      holds --limit subscriptions per broker (at most 1000); when a broker \
                      has more, felixctl prints the --cursor that continues from there. \
                      Depth and capacity count batches; dropped counts records.",
        after_long_help = "Examples:
  felixctl inspect subs
  felixctl inspect subs orders --shard 0
  felixctl inspect subs --dropping --json
  felixctl inspect subs --principal p:billing
  felixctl inspect subs --node broker-a --cursor eyJ0ZW5hbnRfaWQiOi4uLn0"
    )]
    Subs(InspectSubsArgs),
}

#[derive(Debug, Args)]
pub(crate) struct InspectSubsArgs {
    /// Only this stream: NAME, or TENANT/NAMESPACE/NAME for another tenant's
    pub(crate) stream: Option<String>,
    /// Only this shard of the stream
    #[arg(long, value_name = "N", requires = "stream")]
    pub(crate) shard: Option<u32>,
    /// Only this broker, by node id
    #[arg(long, value_name = "NODE")]
    pub(crate) node: Option<String>,
    /// Only subscriptions made under this principal
    #[arg(long, value_name = "P")]
    pub(crate) principal: Option<String>,
    /// Only subscriptions that have dropped records
    #[arg(long)]
    pub(crate) dropping: bool,
    /// At most this many per broker (1-1000)
    #[arg(long, value_name = "N", default_value_t = 100,
          value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub(crate) limit: u32,
    /// Continue after the page a previous run printed
    #[arg(long, value_name = "C", requires = "node")]
    pub(crate) cursor: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct InspectShardArgs {
    /// Stream (or, with --cache, cache) name, or TENANT/NAMESPACE/NAME for
    /// another tenant's
    pub(crate) name: String,
    /// Only this shard
    #[arg(long, value_name = "N")]
    pub(crate) shard: Option<u32>,
    /// NAME is a cache, not a stream
    #[arg(long)]
    pub(crate) cache: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TopologyArgs {
    /// Stream (or, with --cache, cache) name
    pub(crate) name: String,
    /// NAME is a cache, not a stream
    #[arg(long)]
    pub(crate) cache: bool,
}
