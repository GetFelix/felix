//! Node, shard, placement and data commands.

use std::ffi::OsString;

use clap::{Args, Subcommand};

use super::root::Confirm;

#[derive(Debug, Subcommand)]
pub(crate) enum NodeCommand {
    /// List brokers
    #[command(
        long_about = "List the brokers registered with the control plane.",
        after_long_help = "Examples:
  felixctl node ls
  felixctl node ls --json"
    )]
    Ls,
    /// Show one broker
    #[command(
        long_about = "Show one broker: its addresses, lifecycle, heartbeat and placement \
                      eligibility.",
        after_long_help = "Examples:
  felixctl node info broker-1"
    )]
    Info {
        /// Node id
        node: String,
    },
    /// Stop placing shards on a broker and move its shards away
    #[command(
        long_about = "Mark a broker draining: placement puts nothing new on it and moves \
                      the shards it holds to other brokers, while it keeps serving each one \
                      until the shard is handed off. Draining a node already draining \
                      succeeds. Asks first on a terminal; elsewhere it needs --yes.",
        after_long_help = "Examples:
  felixctl node drain broker-2
  felixctl node drain broker-2 --yes"
    )]
    Drain {
        /// Node id
        node: String,
        #[command(flatten)]
        confirm: Confirm,
    },
    /// Mark a broker as having left the cluster
    #[command(
        long_about = "Mark a broker as having left the cluster on purpose. Its shards stay \
                      put until its lease has run out, then fail over. Drain it first to \
                      move them without a failover. Asks first on a terminal; elsewhere it \
                      needs --yes.",
        after_long_help = "Examples:
  felixctl node deregister broker-2
  felixctl node deregister broker-2 --yes"
    )]
    Deregister {
        /// Node id
        node: String,
        #[command(flatten)]
        confirm: Confirm,
    },
    /// Declare a stopped broker's data directory gone for good
    #[command(
        name = "declare-dead",
        long_about = "Declare a broker's data directory gone for good, under the storage id \
                      it is registered with. A Raft re-form then plans without its copies, \
                      and the broker registers again only with a new data directory. It \
                      cannot be undone. Only a broker that is down or left can be declared \
                      dead, but the control plane cannot tell that it has stopped: a Down \
                      broker may still be voting. First make sure the broker process is \
                      stopped or its host fenced, and that its disk is gone. Asks first on \
                      a terminal; elsewhere it needs --yes.",
        after_long_help = "Examples:
  felixctl node declare-dead broker-2
  felixctl node declare-dead broker-2 --yes"
    )]
    DeclareDead {
        /// Node id
        node: String,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ShardCommand {
    /// List shard assignments
    #[command(
        long_about = "List shard assignments: for each shard of each stream and cache, \
                      its leader, replicas, generation and state.",
        after_long_help = "Examples:
  felixctl shard ls
  felixctl shard ls --leader broker-2
  felixctl shard ls --name orders --json"
    )]
    Ls {
        /// Only shards this node leads
        #[arg(long, value_name = "NODE")]
        leader: Option<String>,
        /// Only shards of this stream or cache
        #[arg(long, value_name = "NAME")]
        name: Option<String>,
    },
    /// Move a shard's leadership to another broker, or cancel a move
    #[command(
        long_about = "Move a shard's leadership to the broker --to names. The destination \
                      copies the log first and the leader keeps serving until it has caught \
                      up. Prints the step the move is at and the assignment it wrote. \
                      --dry-run prints what it would write without starting it.\n\n\
                      `shard move cancel` stops a move that has not cut over yet.",
        after_long_help = "Examples:
  felixctl shard move orders 2 --to broker-3
  felixctl shard move orders 2 --to broker-3 --dry-run
  felixctl shard move sessions 0 --to broker-1 --cache
  felixctl shard move cancel orders 2"
    )]
    Move(ShardMoveArgs),
    /// List the moves in progress
    #[command(
        long_about = "List the shard moves in progress: the step each is at, why it \
                      started, the old and new leader and how far the destination's copy \
                      lags. Says so when placement is paused.",
        after_long_help = "Examples:
  felixctl shard moves
  felixctl shard moves --json"
    )]
    Moves,
}

/// `shard move NAME SHARD --to NODE`, or `shard move cancel NAME SHARD`.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
pub(crate) struct ShardMoveArgs {
    #[command(subcommand)]
    pub(crate) cancel: Option<ShardMoveCommand>,
    /// Stream (or, with --cache, cache) name
    #[arg(required = true)]
    pub(crate) name: Option<String>,
    /// Shard number
    #[arg(required = true)]
    pub(crate) shard: Option<u32>,
    /// Broker to move the leadership to
    #[arg(long, value_name = "NODE", required = true)]
    pub(crate) to: Option<String>,
    /// NAME is a cache, not a stream
    #[arg(long)]
    pub(crate) cache: bool,
    /// Print what the move would write, without starting it
    #[arg(long)]
    pub(crate) dry_run: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ShardMoveCommand {
    /// Cancel a shard's move
    #[command(
        long_about = "Cancel a shard's move or follower replacement, whoever started it. \
                      Before the fence the destination is dropped; after it the old leader \
                      serves again. A move that has cut over cannot be cancelled; move the \
                      shard back instead. Placement may choose the same move again, so \
                      pause it first to keep the shard where it is.",
        after_long_help = "Examples:
  felixctl shard move cancel orders 2
  felixctl shard move cancel sessions 0 --cache"
    )]
    Cancel(ShardRef),
}

/// One shard of a stream or cache in the current tenant and namespace.
#[derive(Debug, Args)]
pub(crate) struct ShardRef {
    /// Stream (or, with --cache, cache) name
    pub(crate) name: String,
    /// Shard number
    pub(crate) shard: u32,
    /// NAME is a cache, not a stream
    #[arg(long)]
    pub(crate) cache: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum PlacementCommand {
    /// Show what placement would do next
    #[command(
        long_about = "Show what placement would do next for each shard it has work for: \
                      stage a move, wait for a slot, or leave it. Starts nothing.",
        after_long_help = "Examples:
  felixctl placement plan"
    )]
    Plan,
    /// Show shards short of their replication factor
    #[command(
        long_about = "Show the shards with fewer copies than their replication factor: how \
                      many they have, which copies are on brokers that are unavailable, \
                      which are halted and why, and the broker a copy is being added on.",
        after_long_help = "Examples:
  felixctl placement replication"
    )]
    Replication,
    /// Stop placement starting moves of its own
    #[command(
        long_about = "Stop placement starting moves of its own, on every control-plane \
                      instance. Moves already under way finish; cancel one to stop it. New \
                      shards are still placed and a failed leader is still replaced, and an \
                      operator can still move shards.",
        after_long_help = "Examples:
  felixctl placement pause"
    )]
    Pause,
    /// Let placement start moves again
    #[command(
        long_about = "Let placement start moves of its own again.",
        after_long_help = "Examples:
  felixctl placement resume"
    )]
    Resume,
    /// Give up a stranded shard's log and place it afresh. Loses data
    #[command(
        long_about = "Give up the log of a durable shard that placement is holding unplaced \
                      because the only copies of it are out of reach, and place the shard \
                      afresh. Records only the old leader held are lost, acknowledged ones \
                      included. The control plane refuses it while the leader serves or a \
                      replica can take over without loss. Never asks: it always needs \
                      --yes.",
        after_long_help = "Examples:
  felixctl placement abandon orders 2 --yes
  felixctl placement abandon sessions 0 --cache --yes"
    )]
    Abandon {
        #[command(flatten)]
        shard: ShardRef,
        #[command(flatten)]
        confirm: Confirm,
    },
}

/// `data inspect ...` and `data restore-point ...`. Everything after the
/// subcommand name is the broker's, so it is taken as an external subcommand:
/// clap does not look at it, and a `--json` or `--help` there reaches the
/// broker unchanged.
#[derive(Debug, Subcommand)]
pub(crate) enum DataCommand {
    #[command(external_subcommand)]
    Run(Vec<OsString>),
}
