//! The command line: every command, flag and help text, as clap types.
//!
//! Nothing here acts. `main` hands the parsed [`Cli`] to the module that owns
//! each command.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// Printed by `felixctl` with no arguments.
pub(crate) const OVERVIEW: &str = "\
felixctl: publish, subscribe, read caches and inspect a Felix cluster.

Get started:
  felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 \\
      --token-file token.jwt --ca-file broker-cert.pem
  felixctl pub orders 'hello'
  felixctl sub orders --from earliest

Commands:
  Data plane     pub, sub, cache get|put|del|watch, topology
  Control plane  tenant, namespace, stream, cache ls|info, node, shard
  Tools          context, bench, completions

Run `felixctl help <command>` or `felixctl <command> --help` for details.
";

#[derive(Debug, Parser)]
#[command(
    name = "felixctl",
    version,
    about = "Work with a Felix cluster from the terminal",
    long_about = "Publish and subscribe to streams, read and watch caches, see where shards \
                  live, list what the control plane knows, and run benchmarks.\n\n\
                  Connection settings come from a named context (see `felixctl context`), \
                  overridden by FELIX_* environment variables, overridden by flags.",
    after_long_help = "Examples:
  felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 --token-file token.jwt
  felixctl pub orders 'hello'
  felixctl sub orders --from earliest --count 10
  felixctl stream ls --json"
)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) connection: ConnectionFlags,
    /// Print JSON instead of human-readable output. Errors go to stderr as
    /// JSON too.
    #[arg(long, global = true, help_heading = "Output")]
    pub(crate) json: bool,
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

/// Where and how to connect. Each one overrides the environment variable named
/// in its help, which overrides the context.
#[derive(Debug, Default, Clone, Args)]
#[command(next_help_heading = "Connection")]
pub(crate) struct ConnectionFlags {
    /// Context to use instead of the current one [env: FELIX_CONTEXT]
    #[arg(long, global = true, value_name = "NAME")]
    pub(crate) context: Option<String>,
    /// Config file holding the contexts [env: FELIX_CLI_CONFIG] [default: the
    /// platform config directory, e.g. ~/.config/felixctl/config.toml]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,
    /// Broker addresses, host:port, comma-separated [env: FELIX_BROKERS]
    #[arg(long, global = true, value_name = "ADDRS", value_delimiter = ',')]
    pub(crate) brokers: Option<Vec<String>>,
    /// Control-plane base URL [env: FELIX_CONTROLPLANE_URL]
    #[arg(long, global = true, value_name = "URL")]
    pub(crate) controlplane_url: Option<String>,
    /// Tenant to act as [env: FELIX_AUTH_TENANT]
    #[arg(long, global = true, value_name = "ID")]
    pub(crate) tenant: Option<String>,
    /// Namespace [env: FELIX_NAMESPACE] [default: default]
    #[arg(long, short = 'n', global = true, value_name = "NS")]
    pub(crate) namespace: Option<String>,
    /// Broker token [env: FELIX_AUTH_TOKEN]
    #[arg(long, global = true, value_name = "JWT")]
    pub(crate) token: Option<String>,
    /// File holding the broker token [env: FELIX_AUTH_TOKEN_FILE]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) token_file: Option<PathBuf>,
    /// Control-plane token [env: FELIX_CONTROLPLANE_TOKEN]
    #[arg(long, global = true, value_name = "JWT")]
    pub(crate) controlplane_token: Option<String>,
    /// File holding the control-plane token [env: FELIX_CONTROLPLANE_TOKEN_FILE]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) controlplane_token_file: Option<PathBuf>,
    /// PEM bundle the broker certificates are checked against [env:
    /// FELIX_CA_FILE] [default: the platform trust store]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) ca_file: Option<PathBuf>,
    /// PEM client certificate chain to present to brokers [env:
    /// FELIX_CLIENT_CERT_FILE]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) client_cert_file: Option<PathBuf>,
    /// PEM private key for --client-cert-file [env: FELIX_CLIENT_KEY_FILE]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) client_key_file: Option<PathBuf>,
    /// PEM bundle an https control plane is checked against [env:
    /// FELIX_CONTROLPLANE_CA] [default: the platform trust store]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) controlplane_ca_file: Option<PathBuf>,
    /// TLS server name sent to brokers [env: FELIX_SERVER_NAME] [default: the
    /// first broker's host name, or localhost for an IP address]
    #[arg(long, global = true, value_name = "NAME")]
    pub(crate) server_name: Option<String>,
    /// Offer the felix/1 ALPN, which a broker with FELIX_TLS_REQUIRE_ALPN
    /// needs. Brokers from before ALPN support refuse it
    #[arg(long, global = true)]
    pub(crate) alpn: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Manage named connection profiles
    #[command(
        subcommand,
        long_about = "Manage named connection profiles.\n\n\
                      A context holds the broker addresses, control-plane URL, tenant, \
                      namespace, tokens and TLS files for one cluster, so commands do not \
                      need them as flags. Contexts live in a TOML file in the platform \
                      config directory; --config or FELIX_CLI_CONFIG points elsewhere.",
        after_long_help = "Examples:
  felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 --token-file token.jwt
  felixctl context ls
  felixctl context use prod
  felixctl context rm local"
    )]
    Context(ContextCommand),

    /// Publish messages to a stream
    #[command(
        long_about = "Publish messages to a stream.\n\n\
                      The message comes from the DATA argument, from --file, or from \
                      stdin. Stdin is read one message per line unless --whole is given. \
                      By default each publish waits for the broker's acknowledgement and \
                      reports the record's offset when the ack carries one. A broker that \
                      owns the shard and acks on enqueue (FELIX_ACK_ON_COMMIT off) answers \
                      before the record has an offset. An --idempotent publish is always answered \
                      after the write.",
        after_long_help = "Examples:
  felixctl pub orders 'hello'
  felixctl pub orders --key customer-42 '{\"total\": 10}'
  tail -f app.log | felixctl pub logs
  felixctl pub images --file cat.png --idempotent"
    )]
    Pub(PubArgs),

    /// Read messages from a stream
    #[command(
        long_about = "Read messages from a stream and print them.\n\n\
                      Starts at the live tail unless --from says otherwise. Without \
                      --shard, every shard of the stream is read and merged. Runs until \
                      --count messages have arrived or it is interrupted.",
        after_long_help = "Examples:
  felixctl sub orders
  felixctl sub orders --from earliest --count 10
  felixctl sub orders --shard 2 --from 1500 --format offsets
  felixctl sub orders --json | jq .payload"
    )]
    Sub(SubArgs),

    /// Read, write and watch cache keys, or list caches
    #[command(
        subcommand,
        long_about = "Read, write and watch cache keys, or list and inspect caches.\n\n\
                      get, put, del and watch go to the brokers. ls and info ask the \
                      control plane.",
        after_long_help = "Examples:
  felixctl cache put sessions user-1 'logged-in' --ttl-ms 60000
  felixctl cache get sessions user-1
  felixctl cache watch sessions --prefix user-
  felixctl cache ls"
    )]
    Cache(CacheCommand),

    /// Show a stream's or cache's shards, owners and brokers
    #[command(
        long_about = "Show where a stream or cache lives: its shard count, the broker that \
                      leads each shard, and the brokers clients can reach.\n\n\
                      The shard count and brokers come from a broker. Owners come from \
                      the control plane, so they are shown only when a control-plane URL \
                      is configured.",
        after_long_help = "Examples:
  felixctl topology orders
  felixctl topology sessions --cache
  felixctl topology orders --json"
    )]
    Topology(TopologyArgs),

    /// List and inspect tenants (control plane)
    #[command(
        subcommand,
        long_about = "List and inspect tenants. Needs a control-plane token allowed \
                      tenant.manage on the cluster.",
        after_long_help = "Examples:
  felixctl tenant ls
  felixctl tenant info t1"
    )]
    Tenant(TenantCommand),

    /// List and inspect namespaces (control plane)
    #[command(
        subcommand,
        long_about = "List and inspect the namespaces of the current tenant.",
        after_long_help = "Examples:
  felixctl namespace ls
  felixctl namespace info default --tenant t1"
    )]
    Namespace(NamespaceCommand),

    /// List and inspect streams (control plane)
    #[command(
        subcommand,
        long_about = "List and inspect the streams of the current tenant and namespace.",
        after_long_help = "Examples:
  felixctl stream ls
  felixctl stream info orders
  felixctl stream ls -n payments --json"
    )]
    Stream(StreamCommand),

    /// List and inspect brokers (control plane)
    #[command(
        subcommand,
        long_about = "List and inspect the brokers registered with the control plane, \
                      with their lifecycle and whether placement may use them.",
        after_long_help = "Examples:
  felixctl node ls
  felixctl node info broker-1 --json"
    )]
    Node(NodeCommand),

    /// List shard assignments (control plane)
    #[command(
        subcommand,
        long_about = "List which broker leads each shard of every stream and cache, as the \
                      control plane assigned them.",
        after_long_help = "Examples:
  felixctl shard ls
  felixctl shard ls --leader broker-2
  felixctl shard ls --name orders"
    )]
    Shard(ShardCommand),

    /// Run a load test against the cluster
    #[command(
        subcommand,
        long_about = "Run one of felix-loadgen's scenarios against the current context and \
                      print a short summary: the rate, and p50 and p99 latency.\n\n\
                      The defaults are small enough to finish in seconds. The stream or \
                      cache must already exist. --json prints the summary together with \
                      the full felix-loadgen result.",
        after_long_help = "Examples:
  felixctl bench latency orders
  felixctl bench fanout orders --fanout 20
  felixctl bench ingest orders --concurrency 8 --duration-secs 30
  felixctl bench cache sessions --json"
    )]
    Bench(BenchCommand),

    /// Print a shell completion script
    #[command(
        long_about = "Print a completion script for SHELL to stdout. Source it from your \
                      shell's startup file.",
        after_long_help = "Examples:
  felixctl completions bash > ~/.local/share/bash-completion/completions/felixctl
  felixctl completions zsh > \"${fpath[1]}/_felixctl\"
  felixctl completions fish > ~/.config/fish/completions/felixctl.fish"
    )]
    Completions(CompletionsArgs),

    /// Write man pages for every command
    #[command(
        hide = true,
        long_about = "Write a man page for felixctl and each of its commands into DIR."
    )]
    Man(ManArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ContextCommand {
    /// Save the connection flags given on this command line as a context
    #[command(
        long_about = "Save the connection flags given on this command line as a new \
                      context. Only flags are saved; environment variables are not. The \
                      first context added becomes the current one.",
        after_long_help = "Examples:
  felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 \\
      --token-file token.jwt --ca-file broker-cert.pem
  felixctl context add prod --brokers a.example:5000,b.example:5000 \\
      --controlplane-url https://cp.example --tenant acme --use"
    )]
    Add {
        /// Name of the context
        name: String,
        /// Make it the current context
        #[arg(long = "use")]
        make_current: bool,
        /// Replace a context that already has this name
        #[arg(long)]
        replace: bool,
    },
    /// Make a context the current one
    #[command(
        long_about = "Make NAME the context every command uses unless --context or \
                      FELIX_CONTEXT says otherwise.",
        after_long_help = "Examples:
  felixctl context use prod"
    )]
    Use {
        /// Name of the context
        name: String,
    },
    /// List contexts
    #[command(
        long_about = "List the saved contexts. The current one is marked with *.",
        after_long_help = "Examples:
  felixctl context ls
  felixctl context ls --json"
    )]
    Ls,
    /// Delete a context
    #[command(
        long_about = "Delete a saved context. Deleting the current one leaves no context \
                      current.",
        after_long_help = "Examples:
  felixctl context rm local"
    )]
    Rm {
        /// Name of the context
        name: String,
    },
}

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
}

#[derive(Debug, Args)]
pub(crate) struct TopologyArgs {
    /// Stream (or, with --cache, cache) name
    pub(crate) name: String,
    /// NAME is a cache, not a stream
    #[arg(long)]
    pub(crate) cache: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TenantCommand {
    /// List tenants
    #[command(
        long_about = "List every tenant.",
        after_long_help = "Examples:
  felixctl tenant ls
  felixctl tenant ls --json"
    )]
    Ls,
    /// Show one tenant
    #[command(
        long_about = "Show one tenant. Exits with status 5 when it does not exist.",
        after_long_help = "Examples:
  felixctl tenant info t1"
    )]
    Info {
        // Its own id: one named `tenant` would replace the global --tenant here.
        /// Tenant id
        #[arg(id = "tenant_id", value_name = "TENANT")]
        tenant: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespaceCommand {
    /// List namespaces in the tenant
    #[command(
        long_about = "List the namespaces of the current tenant.",
        after_long_help = "Examples:
  felixctl namespace ls
  felixctl namespace ls --tenant acme"
    )]
    Ls,
    /// Show one namespace
    #[command(
        long_about = "Show one namespace of the current tenant.",
        after_long_help = "Examples:
  felixctl namespace info default"
    )]
    Info {
        // Its own id: one named `namespace` would replace the global --namespace.
        /// Namespace name
        #[arg(id = "namespace_name", value_name = "NAMESPACE")]
        namespace: String,
    },
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
}

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
}

#[derive(Debug, Subcommand)]
pub(crate) enum BenchCommand {
    /// Aggregate publish throughput from several publishers
    #[command(
        long_about = "Measure aggregate publish throughput: --concurrency publishers, each \
                      on its own connection, send batches with --in-flight acknowledged \
                      batches outstanding. No subscribers. Reports msg/s, MB/s and batch \
                      acknowledgement latency.",
        after_long_help = "Examples:
  felixctl bench ingest orders
  felixctl bench ingest orders --concurrency 8 --batch 256 --duration-secs 30
  felixctl bench ingest orders --keys 64 --in-flight 0"
    )]
    Ingest(IngestBenchArgs),
    /// Publish acknowledgement and delivery latency
    #[command(
        long_about = "Measure latency with one publisher and one subscriber: the publish \
                      acknowledgement round trip and the publish-to-delivery time.",
        after_long_help = "Examples:
  felixctl bench latency orders
  felixctl bench latency orders --total 20000 --payload-bytes 1024"
    )]
    Latency(PubsubBenchArgs),
    /// Delivery to many subscribers at once
    #[command(
        long_about = "Measure fanout: one publisher and --fanout subscribers. Reports the \
                      publish rate, the delivered rate across all subscribers, and \
                      delivery latency.",
        after_long_help = "Examples:
  felixctl bench fanout orders
  felixctl bench fanout orders --fanout 50"
    )]
    Fanout(FanoutBenchArgs),
    /// Cache put and get round trips
    #[command(
        long_about = "Measure cache round trips: puts, then gets of the same keys, from \
                      --concurrency workers spread across the brokers.",
        after_long_help = "Examples:
  felixctl bench cache sessions
  felixctl bench cache sessions --concurrency 32 --payload-bytes 64"
    )]
    Cache(CacheBenchArgs),
}

/// Flags every benchmark takes.
#[derive(Debug, Clone, Args)]
pub(crate) struct BenchSize {
    /// Operations measured
    #[arg(long, value_name = "N", default_value_t = 5000)]
    pub(crate) total: usize,
    /// Operations run first and not measured
    #[arg(long, value_name = "N", default_value_t = 500)]
    pub(crate) warmup: usize,
    /// Payload size in bytes
    #[arg(long, value_name = "BYTES", default_value_t = 256)]
    pub(crate) payload_bytes: usize,
}

#[derive(Debug, Args)]
pub(crate) struct IngestBenchArgs {
    /// Stream to publish to
    pub(crate) stream: String,
    /// Records to publish in total
    #[arg(long, value_name = "N", default_value_t = 100_000)]
    pub(crate) total: usize,
    /// Payload size in bytes
    #[arg(long, value_name = "BYTES", default_value_t = 256)]
    pub(crate) payload_bytes: usize,
    /// Publishers, each on its own connection
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub(crate) concurrency: usize,
    /// Records per batch
    #[arg(long, value_name = "N", default_value_t = 64)]
    pub(crate) batch: usize,
    /// Acknowledged batches each publisher keeps outstanding; 0 sends without
    /// acknowledgements
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub(crate) in_flight: usize,
    /// Spread batches over this many routing keys; 0 publishes unkeyed, which
    /// puts everything on shard 0
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub(crate) keys: usize,
    /// Publish for this many seconds instead of --total records
    #[arg(long, value_name = "SECS")]
    pub(crate) duration_secs: Option<f64>,
}

#[derive(Debug, Args)]
pub(crate) struct PubsubBenchArgs {
    /// Stream to publish to and subscribe to
    pub(crate) stream: String,
    #[command(flatten)]
    pub(crate) size: BenchSize,
}

#[derive(Debug, Args)]
pub(crate) struct FanoutBenchArgs {
    /// Stream to publish to and subscribe to
    pub(crate) stream: String,
    /// Subscribers
    #[arg(long, value_name = "N", default_value_t = 10)]
    pub(crate) fanout: usize,
    #[command(flatten)]
    pub(crate) size: BenchSize,
}

#[derive(Debug, Args)]
pub(crate) struct CacheBenchArgs {
    /// Cache to write and read
    pub(crate) cache: String,
    /// Concurrent workers
    #[arg(long, value_name = "N", default_value_t = 8)]
    pub(crate) concurrency: usize,
    #[command(flatten)]
    pub(crate) size: BenchSize,
}

#[derive(Debug, Args)]
pub(crate) struct CompletionsArgs {
    /// Shell to generate the script for
    #[arg(value_enum)]
    pub(crate) shell: clap_complete::Shell,
}

#[derive(Debug, Args)]
pub(crate) struct ManArgs {
    /// Directory to write the pages into
    #[arg(long, value_name = "DIR", default_value = ".")]
    pub(crate) out_dir: PathBuf,
}

#[cfg(test)]
mod tests;
