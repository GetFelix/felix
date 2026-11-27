//! Cache commands.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::root::Confirm;
use super::stream::ConsistencyArg;

#[derive(Debug, Subcommand)]
pub(crate) enum CacheCommand {
    /// Print a key's value
    #[command(
        long_about = "Print a key's value as stored. Exits with status 5 when the key is \
                      not set.",
        after_long_help = "Examples:
  felixctl cache get sessions user-1
  felixctl cache get sessions user-1 --json"
    )]
    Get {
        /// Cache name
        cache: String,
        /// Key to read
        key: String,
    },
    /// Set a key
    #[command(
        long_about = "Set a key to VALUE, to the contents of --file, or to all of stdin.",
        after_long_help = "Examples:
  felixctl cache put sessions user-1 'logged-in'
  felixctl cache put sessions user-1 'logged-in' --ttl-ms 60000
  felixctl cache put config app --file app.json"
    )]
    Put {
        /// Cache name
        cache: String,
        /// Key to set
        key: String,
        /// The value. Without it, --file or stdin is read
        value: Option<String>,
        /// Use this file's contents as the value
        #[arg(long, value_name = "PATH", conflicts_with = "value")]
        file: Option<PathBuf>,
        /// Expire the key after this many milliseconds [default: never]
        #[arg(long, value_name = "MS")]
        ttl_ms: Option<u64>,
    },
    /// Delete a key
    #[command(
        long_about = "Delete a key. Deleting a key that is not set succeeds.",
        after_long_help = "Examples:
  felixctl cache del sessions user-1"
    )]
    Del {
        /// Cache name
        cache: String,
        /// Key to delete
        key: String,
    },
    /// Print changes to a key or a key prefix as they happen
    #[command(
        long_about = "Print changes to one key (--key) or to every key under a prefix \
                      (--prefix, default every key) as they happen. A prefix watch covers \
                      every shard of the cache. With --retained, the current values are \
                      printed first.",
        after_long_help = "Examples:
  felixctl cache watch sessions
  felixctl cache watch sessions --key user-1
  felixctl cache watch sessions --prefix user- --retained --count 100"
    )]
    Watch {
        /// Cache name
        cache: String,
        /// Watch only this key
        #[arg(long, value_name = "KEY", conflicts_with = "prefix")]
        key: Option<String>,
        /// Watch keys starting with this prefix [default: every key]
        #[arg(long, value_name = "PREFIX")]
        prefix: Option<String>,
        /// Print each matching key's current value before live changes
        #[arg(long)]
        retained: bool,
        /// Resume a key watch at this cache-log offset
        #[arg(
            long,
            value_name = "OFFSET",
            requires = "key",
            conflicts_with = "retained"
        )]
        from: Option<u64>,
        /// Stop after this many changes
        #[arg(long, value_name = "N")]
        count: Option<u64>,
    },
    /// List caches in the namespace (control plane)
    #[command(
        long_about = "List the caches of the current tenant and namespace.",
        after_long_help = "Examples:
  felixctl cache ls
  felixctl cache ls -n payments"
    )]
    Ls,
    /// Show a cache's settings (control plane)
    #[command(
        long_about = "Show a cache's shard count, replication factor and consistency.",
        after_long_help = "Examples:
  felixctl cache info sessions"
    )]
    Info {
        /// Cache name
        cache: String,
    },
    /// Create a cache (control plane)
    #[command(
        long_about = "Create a cache in the current tenant and namespace. Creating one that \
                      already exists with the same settings succeeds; with different \
                      settings it is refused.",
        after_long_help = "Examples:
  felixctl cache create sessions
  felixctl cache create sessions --shards 4 --replication 3 --consistency quorum"
    )]
    Create(CacheCreateArgs),
    /// Change a cache's display name (control plane)
    #[command(
        long_about = "Change a cache's display name, the only setting that can change after \
                      creation, and print what changed.",
        after_long_help = "Examples:
  felixctl cache set sessions --display-name 'Login sessions'"
    )]
    Set {
        /// Cache name
        cache: String,
        /// New display name
        #[arg(long, value_name = "TEXT")]
        display_name: String,
    },
    /// Delete a cache and every key in it (control plane)
    #[command(
        long_about = "Delete a cache and every key in it. Asks first on a terminal; \
                      elsewhere it needs --yes.",
        after_long_help = "Examples:
  felixctl cache rm sessions
  felixctl cache rm sessions --yes"
    )]
    Rm {
        /// Cache name
        cache: String,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Args)]
pub(crate) struct CacheCreateArgs {
    /// Cache name
    pub(crate) cache: String,
    /// Display name [default: the cache name]
    #[arg(long, value_name = "TEXT")]
    pub(crate) display_name: Option<String>,
    /// Shards to split the keyspace across
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub(crate) shards: u32,
    /// Brokers holding a copy of each shard, the leader included
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub(crate) replication: u32,
    /// `quorum` acknowledges a write once a majority of the shard's copies
    /// hold it
    #[arg(long, value_enum, value_name = "LEVEL", default_value_t = ConsistencyArg::Leader)]
    pub(crate) consistency: ConsistencyArg,
}
