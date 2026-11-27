//! Stream commands and the settings they take on create and update.

use clap::{Args, Subcommand, ValueEnum};

use super::root::Confirm;

#[derive(Debug, Args)]
pub(crate) struct StreamCreateArgs {
    /// Stream name
    pub(crate) stream: String,
    /// Shards; fixed at creation
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub(crate) shards: u32,
    /// Brokers holding a copy of each shard, the leader included. `quorum`
    /// needs at least 3 to mean anything
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub(crate) replication: u32,
    /// The kind the control plane records for the stream
    #[arg(long, value_enum, value_name = "KIND", default_value_t = KindArg::Stream)]
    pub(crate) kind: KindArg,
    /// `quorum` acknowledges a publish once a majority of the shard's copies
    /// hold it
    #[arg(long, value_enum, value_name = "LEVEL", default_value_t = ConsistencyArg::Leader)]
    pub(crate) consistency: ConsistencyArg,
    /// The delivery guarantee readers get
    #[arg(long, value_enum, value_name = "GUARANTEE", default_value_t = DeliveryArg::AtLeastOnce)]
    pub(crate) delivery: DeliveryArg,
    /// Keep records in an on-disk log. `false` keeps them in memory only
    #[arg(long, value_name = "BOOL", default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) durable: bool,
    /// Keep records this many seconds [default: the broker's own bound]
    #[arg(long, value_name = "SECS")]
    pub(crate) retention_secs: Option<u64>,
    /// Keep at most this many bytes per shard [default: the broker's own bound]
    #[arg(long, value_name = "BYTES")]
    pub(crate) retention_bytes: Option<u64>,
    /// Region the stream's data must stay in; fixed at creation [default:
    /// anywhere]
    #[arg(long, value_name = "REGION")]
    pub(crate) region: Option<String>,
    /// How routing keys map to shards; fixed at creation. `jump-hash` needs
    /// the jump_hash_routing fleet feature finalized
    #[arg(long, value_enum, value_name = "ROUTING", default_value_t = RoutingArg::Modulo)]
    pub(crate) routing: RoutingArg,
}

#[derive(Debug, Args)]
#[command(group(
    clap::ArgGroup::new("change")
        .required(true)
        .multiple(true)
        .args(["consistency", "delivery", "durable", "retention_secs", "retention_bytes"])
))]
pub(crate) struct StreamSetArgs {
    /// Stream name
    pub(crate) stream: String,
    /// New consistency level
    #[arg(long, value_enum, value_name = "LEVEL")]
    pub(crate) consistency: Option<ConsistencyArg>,
    /// New delivery guarantee
    #[arg(long, value_enum, value_name = "GUARANTEE")]
    pub(crate) delivery: Option<DeliveryArg>,
    /// Keep records in an on-disk log, or not
    #[arg(long, value_name = "BOOL", action = clap::ArgAction::Set)]
    pub(crate) durable: Option<bool>,
    /// Keep records this many seconds; `default` for the broker's own bound
    #[arg(long, value_name = "SECS")]
    pub(crate) retention_secs: Option<Bound>,
    /// Keep at most this many bytes per shard; `default` for the broker's own
    /// bound
    #[arg(long, value_name = "BYTES")]
    pub(crate) retention_bytes: Option<Bound>,
}

/// A retention bound to set: a value, or `default` to clear it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bound {
    BrokerDefault,
    Value(u64),
}

impl std::str::FromStr for Bound {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "default" {
            return Ok(Self::BrokerDefault);
        }
        value
            .parse()
            .map(Self::Value)
            .map_err(|_| format!("expected a number or `default`, not {value:?}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum KindArg {
    /// Recorded as `Stream`
    Stream,
    /// Recorded as `Queue`
    Queue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ConsistencyArg {
    /// The leader alone acknowledges
    Leader,
    /// A majority of the copies acknowledge
    Quorum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum DeliveryArg {
    /// A plain subscription
    AtMostOnce,
    /// Consumer groups with acknowledgements
    AtLeastOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum RoutingArg {
    /// hash(key) % shards
    Modulo,
    /// Jump consistent hashing
    JumpHash,
}

#[derive(Debug, Subcommand)]
pub(crate) enum StreamCommand {
    /// List streams in the namespace
    #[command(
        long_about = "List the streams of the current tenant and namespace.",
        after_long_help = "Examples:
  felixctl stream ls
  felixctl stream ls -n payments --json"
    )]
    Ls,
    /// Show a stream's settings
    #[command(
        long_about = "Show a stream's shards, replication, retention, consistency and \
                      delivery settings.",
        after_long_help = "Examples:
  felixctl stream info orders
  felixctl stream info orders --json"
    )]
    Info {
        /// Stream name
        stream: String,
    },
    /// Create a stream
    #[command(
        long_about = "Create a stream in the current tenant and namespace. Creating one \
                      that already exists with the same settings succeeds; with different \
                      settings it is refused. Shards, replication, region and routing are \
                      fixed once it exists.",
        after_long_help = "Examples:
  felixctl stream create orders
  felixctl stream create orders --shards 4 --replication 3 --consistency quorum
  felixctl stream create audit --retention-secs 604800 --region eu-west"
    )]
    Create(StreamCreateArgs),
    /// Change a stream's settings
    #[command(
        long_about = "Change a stream's consistency, delivery, durability or retention, and \
                      print what changed. A retention bound not given keeps its current \
                      value.",
        after_long_help = "Examples:
  felixctl stream set orders --retention-secs 86400
  felixctl stream set orders --retention-bytes default
  felixctl stream set orders --consistency quorum"
    )]
    Set(StreamSetArgs),
    /// Hide a shard's oldest records (broker)
    #[command(
        long_about = "Hide the records of one shard of a durable stream below an offset, \
                      only if the shard's next offset is exactly --expected-offset. The \
                      trim is a record of its own and lands at that offset, so --before \
                      can be at most that. Reads below the new floor are refused as too \
                      old.\n\n\
                      A stale --expected-offset is refused with the shard's next offset and \
                      exit status 4, and nothing is written. A trim that times out may \
                      still have been written and exits with status 6; the same command \
                      again is then refused with the tail, so read the shard's earliest offset to tell. Needs a \
                      token allowed stream.trim, and a broker fleet that has finalized \
                      head trims.",
        after_long_help = "Examples:
  felixctl stream trim orders --shard 0 --before 1000 --expected-offset 5000
  felixctl stream trim orders --shard 2 --before 400 --expected-offset 410 --headers holder-7"
    )]
    Trim(StreamTrimArgs),
    /// Delete a stream and its records
    #[command(
        long_about = "Delete a stream and its records. Asks first on a terminal; elsewhere \
                      it needs --yes.",
        after_long_help = "Examples:
  felixctl stream rm orders
  felixctl stream rm orders --yes"
    )]
    Rm {
        /// Stream name
        stream: String,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Args)]
pub(crate) struct StreamTrimArgs {
    /// Stream name
    pub(crate) stream: String,
    /// The shard to trim
    #[arg(long, value_name = "N")]
    pub(crate) shard: u32,
    /// The new floor: records below this offset are hidden
    #[arg(long, value_name = "OFFSET")]
    pub(crate) before: u64,
    /// The shard's next offset, as the caller last saw it
    #[arg(long, value_name = "OFFSET")]
    pub(crate) expected_offset: u64,
    /// Kept with the trim and shown to a reader refused below its floor
    #[arg(long, value_name = "TEXT")]
    pub(crate) headers: Option<String>,
}
