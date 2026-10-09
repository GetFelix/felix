//! The command line: every command, flag and help text, as clap types.
//!
//! Nothing here acts. `main` hands the parsed [`Cli`] to the module that owns
//! each command.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::counter::CounterCommand;
use crate::group::GroupCommand;

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
  Groups         group, counter
  Control plane  tenant, namespace, stream, cache ls|info|create|set|rm,
                 node, shard, placement, rbac
  Operators      inspect shard|subs
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

    /// Read, write and watch cache keys, or manage caches
    #[command(
        subcommand,
        long_about = "Read, write and watch cache keys, or create, change, list, inspect \
                      and delete caches.\n\n\
                      get, put, del and watch go to the brokers. ls, info, create, set and \
                      rm go to the control plane.",
        after_long_help = "Examples:
  felixctl cache put sessions user-1 'logged-in' --ttl-ms 60000
  felixctl cache get sessions user-1
  felixctl cache watch sessions --prefix user-
  felixctl cache create sessions --shards 4
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

    /// Create, inspect and move consumer groups; claim and settle records
    #[command(
        subcommand,
        long_about = "Work with consumer groups: create, describe, seek and delete them, \
                      claim records with poll, and settle them with ack, nack, extend and \
                      dead-letters.\n\n\
                      A group keeps a cursor on each shard of its stream, with that \
                      shard's leader. A record is named by its claim, SHARD:OFFSET, which \
                      poll prints with the delivery attempt added.",
        after_long_help = "Examples:
  felixctl group create orders billing --from earliest
  felixctl group poll orders billing --max 5
  felixctl group ack orders billing 0:15:1
  felixctl group describe orders billing"
    )]
    Group(GroupCommand),

    /// Read and add to counters
    #[command(
        subcommand,
        long_about = "Read a counter's sum, or add a signed delta to it. Counters live in a \
                      cache, beside its keys.",
        after_long_help = "Examples:
  felixctl counter add stats page-views 1
  felixctl counter get stats page-views"
    )]
    Counter(CounterCommand),

    /// Create, list, inspect and delete tenants (control plane)
    #[command(
        subcommand,
        long_about = "Create, list, inspect and delete tenants. Needs a control-plane token \
                      allowed tenant.manage on the cluster.",
        after_long_help = "Examples:
  felixctl tenant ls
  felixctl tenant info t1
  felixctl tenant create acme --display-name 'Acme Corp'
  felixctl tenant rm acme --yes"
    )]
    Tenant(TenantCommand),

    /// Create, list, inspect and delete namespaces (control plane)
    #[command(
        subcommand,
        long_about = "Create, list, inspect and delete the namespaces of the current tenant.",
        after_long_help = "Examples:
  felixctl namespace ls
  felixctl namespace info default --tenant t1
  felixctl namespace create payments
  felixctl namespace rm payments --yes"
    )]
    Namespace(NamespaceCommand),

    /// Create, change, list, inspect and delete streams (control plane)
    #[command(
        subcommand,
        long_about = "Create, change, list, inspect and delete the streams of the current \
                      tenant and namespace.",
        after_long_help = "Examples:
  felixctl stream ls
  felixctl stream info orders
  felixctl stream create orders --shards 4 --replication 3 --consistency quorum
  felixctl stream set orders --retention-secs 86400
  felixctl stream rm orders --yes"
    )]
    Stream(StreamCommand),

    /// List, inspect, drain and deregister brokers (control plane)
    #[command(
        subcommand,
        long_about = "List and inspect the brokers registered with the control plane, \
                      with their lifecycle and whether placement may use them, and drain or \
                      deregister one.",
        after_long_help = "Examples:
  felixctl node ls
  felixctl node info broker-1 --json
  felixctl node drain broker-2
  felixctl node deregister broker-2 --yes"
    )]
    Node(NodeCommand),

    /// List shard assignments and move shards (control plane)
    #[command(
        subcommand,
        long_about = "List which broker leads each shard of every stream and cache, as the \
                      control plane assigned them, and move a shard's leadership or cancel \
                      a move.",
        after_long_help = "Examples:
  felixctl shard ls
  felixctl shard ls --leader broker-2
  felixctl shard move orders 2 --to broker-3
  felixctl shard move cancel orders 2"
    )]
    Shard(ShardCommand),

    /// Pause or resume placement's own moves (control plane)
    #[command(
        subcommand,
        long_about = "Pause or resume the moves placement starts by itself, and give up a \
                      stranded shard's log. Needs a control-plane token allowed node.manage \
                      on the cluster.",
        after_long_help = "Examples:
  felixctl placement pause
  felixctl placement resume
  felixctl placement abandon orders 2 --yes"
    )]
    Placement(PlacementCommand),

    /// List, grant and revoke RBAC policies and role assignments (control plane)
    #[command(
        subcommand,
        long_about = "List, add and remove the current tenant's RBAC rules.\n\n\
                      A policy lets a subject (usually a role) take an action on an \
                      object. A grouping assigns a user, or an IdP group, to a role. \
                      Changes reach brokers with the next token the principal is issued.",
        after_long_help = "Examples:
  felixctl rbac policy ls
  felixctl rbac policy add role:reader stream:t1/payments/orders stream.subscribe
  felixctl rbac grouping add p:alice role:reader
  felixctl rbac policy rm role:reader stream:t1/payments/orders stream.subscribe --yes"
    )]
    Rbac(RbacCommand),
    /// Look at a cluster's live state, read-only (operators)
    #[command(
        subcommand,
        long_about = "Look at what the brokers themselves hold, read-only. Each answer is \
                      one broker's own view; felixctl asks every broker that has a part in \
                      what is shown.\n\n\
                      Needs a broker token allowed node.view on cluster:*. A broker too old \
                      to answer is reported, not guessed at.\n\n\
                      inspect segments is the exception: it reads a data directory from \
                      disk and needs no broker.",
        after_long_help = "Examples:
  felixctl inspect shard orders --shard 3
  felixctl inspect shard acme/default/orders
  felixctl inspect shard sessions --cache --json
  felixctl inspect subs orders --dropping
  felixctl inspect subs --node broker-a --limit 500
  felixctl inspect segments /var/lib/felix"
    )]
    Inspect(InspectCommand),

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

/// `--yes`, which every destructive command takes.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct Confirm {
    /// Do not ask for confirmation. Needed when stdin is not a terminal
    #[arg(long, short = 'y')]
    pub(crate) yes: bool,
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
    /// A broker's data directory, offline: segments, indexes and the startup verdict
    #[command(
        long_about = "Read a broker's data directory (FELIX_DURABLE_STORAGE_DIR) and report, \
                      for every shard of every store, its segments, whether each one's \
                      records and index verify, and what the broker would do with it at \
                      startup: open it as it is, repair it (cut a torn tail, discard what \
                      an interrupted rollover left), or refuse to start, and where.\n\n\
                      Strictly read-only: nothing is created, repaired, truncated or \
                      re-indexed, so it is safe on a mounted volume, a snapshot or a copy. \
                      It needs no broker and no connection settings. Next to a running \
                      broker the reads are safe, but an active segment may be mid-write.\n\n\
                      The verdict depends on three broker settings; pass the same values \
                      with --repair-checksum-tail, --index-spacing and \
                      --verify-all-on-open.\n\n\
                      Exits 0 when every shard opens as it is, 6 when startup would repair \
                      something, and 7 when startup would refuse a shard or a record fails \
                      its checksum.",
        after_long_help = "Examples:
  felixctl inspect segments /var/lib/felix
  felixctl inspect segments /var/lib/felix acme/default/orders/3
  felixctl inspect segments /data --kind cache --json
  kubectl exec felix-broker-0 -- felixctl inspect segments /var/lib/felix"
    )]
    Segments(InspectSegmentsArgs),
}

#[derive(Debug, Args)]
pub(crate) struct InspectSegmentsArgs {
    /// The broker's data directory
    pub(crate) data_dir: std::path::PathBuf,
    /// Only this shard, as TENANT/NAMESPACE/NAME/SHARD; prints its segments
    pub(crate) shard: Option<String>,
    /// Only this store [default with a shard: stream]
    #[arg(long, value_enum)]
    pub(crate) kind: Option<StoreKind>,
    /// List the segments of every shard, not only of those with findings
    #[arg(long)]
    pub(crate) segments: bool,
    /// As the broker's FELIX_DURABLE_REPAIR_CHECKSUM_TAIL
    #[arg(long)]
    pub(crate) repair_checksum_tail: bool,
    /// As the broker's FELIX_DURABLE_INDEX_SPACING_BYTES
    #[arg(long, value_name = "BYTES")]
    pub(crate) index_spacing: Option<u64>,
    /// As the broker's FELIX_DURABLE_VERIFY_ALL_ON_OPEN
    #[arg(long)]
    pub(crate) verify_all_on_open: bool,
}

/// The broker's stores, each its own directory of shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum StoreKind {
    Stream,
    Cache,
    Groups,
    DeadLetters,
    Counters,
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
    /// Create a tenant
    #[command(
        long_about = "Create a tenant. One that already exists is refused.",
        after_long_help = "Examples:
  felixctl tenant create acme
  felixctl tenant create acme --display-name 'Acme Corp'"
    )]
    Create {
        /// Tenant id
        #[arg(id = "tenant_id", value_name = "TENANT")]
        tenant: String,
        /// Display name [default: the tenant id]
        #[arg(long, value_name = "TEXT")]
        display_name: Option<String>,
    },
    /// Delete a tenant
    #[command(
        long_about = "Delete a tenant with everything in it: namespaces, streams, caches, \
                      signing keys and RBAC rules. Asks first on a terminal; elsewhere it \
                      needs --yes.",
        after_long_help = "Examples:
  felixctl tenant rm acme
  felixctl tenant rm acme --yes"
    )]
    Rm {
        /// Tenant id
        #[arg(id = "tenant_id", value_name = "TENANT")]
        tenant: String,
        #[command(flatten)]
        confirm: Confirm,
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
    /// Create a namespace
    #[command(
        long_about = "Create a namespace in the current tenant. One that already exists is \
                      refused.",
        after_long_help = "Examples:
  felixctl namespace create payments
  felixctl namespace create payments --display-name Payments"
    )]
    Create {
        /// Namespace name
        #[arg(id = "namespace_name", value_name = "NAMESPACE")]
        namespace: String,
        /// Display name [default: the namespace name]
        #[arg(long, value_name = "TEXT")]
        display_name: Option<String>,
    },
    /// Delete a namespace
    #[command(
        long_about = "Delete a namespace of the current tenant, its streams and caches \
                      included. Asks first on a terminal; elsewhere it needs --yes.",
        after_long_help = "Examples:
  felixctl namespace rm payments
  felixctl namespace rm payments --yes"
    )]
    Rm {
        /// Namespace name
        #[arg(id = "namespace_name", value_name = "NAMESPACE")]
        namespace: String,
        #[command(flatten)]
        confirm: Confirm,
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

#[derive(Debug, Subcommand)]
pub(crate) enum RbacCommand {
    /// Policies: what a subject may do to an object
    #[command(
        subcommand,
        long_about = "List, add and remove policies. Each one is a subject, an object and \
                      an action.\n\n\
                      Objects: cluster:*, node:{node}, tenant:{tenant}, \
                      namespace:{tenant}/{ns}, stream:{tenant}/{ns}/{stream}, \
                      cache:{tenant}/{ns}/{cache}, cache:{tenant}/{ns}/{cache}/{key}, \
                      cache:{tenant}/{ns}/{cache}/{prefix}*, and \
                      group:{tenant}/{ns}/{stream}/{group}. A `*` may stand for a stream, \
                      cache or group, and for the namespace above it when it does.",
        after_long_help = "Examples:
  felixctl rbac policy ls --subject role:reader
  felixctl rbac policy add role:reader stream:t1/payments/* stream.subscribe
  felixctl rbac policy add role:session-1 cache:t1/default/sessions/user-1 cache.read
  felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.write
  felixctl rbac policy rm role:reader stream:t1/payments/* stream.subscribe"
    )]
    Policy(PolicyCommand),
    /// Groupings: which users and groups hold which roles
    #[command(
        subcommand,
        long_about = "List, add and remove groupings. Each one assigns a user, or a group \
                      named by an IdP claim, to a role.",
        after_long_help = "Examples:
  felixctl rbac grouping ls --role role:reader
  felixctl rbac grouping add p:alice role:reader
  felixctl rbac grouping rm p:alice role:reader"
    )]
    Grouping(GroupingCommand),
}

#[derive(Debug, Subcommand)]
pub(crate) enum PolicyCommand {
    /// List policies
    #[command(
        long_about = "List the tenant's policies that the control-plane token may see.",
        after_long_help = "Examples:
  felixctl rbac policy ls
  felixctl rbac policy ls --subject role:reader --json"
    )]
    Ls {
        /// Only policies for this subject
        #[arg(long, value_name = "SUBJECT")]
        subject: Option<String>,
    },
    /// Add a policy
    #[command(
        long_about = "Let SUBJECT take ACTION on OBJECT. The object is checked here before \
                      it is sent; the control plane checks it again, along with the action \
                      and whether the token may grant it. A cache key or key prefix object \
                      takes only cache.read or cache.write.",
        after_long_help = "Examples:
  felixctl rbac policy add role:writer stream:t1/payments/orders stream.publish
  felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.read"
    )]
    Add(PolicyArgs),
    /// Remove a policy
    #[command(
        long_about = "Remove one policy, named exactly as `policy ls` prints it. Asks first \
                      on a terminal; elsewhere it needs --yes. Exits with status 5 when \
                      there is no such policy.",
        after_long_help = "Examples:
  felixctl rbac policy rm role:writer stream:t1/payments/orders stream.publish
  felixctl rbac policy rm role:writer stream:t1/payments/orders stream.publish --yes"
    )]
    Rm {
        #[command(flatten)]
        policy: PolicyArgs,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PolicyArgs {
    /// Who the policy is for, usually a role such as role:reader
    pub(crate) subject: String,
    /// What it covers, such as stream:t1/payments/orders
    pub(crate) object: String,
    /// What it allows, such as stream.publish
    pub(crate) action: String,
}

#[derive(Debug, Subcommand)]
pub(crate) enum GroupingCommand {
    /// List groupings
    #[command(
        long_about = "List the tenant's groupings for roles the control-plane token may see.",
        after_long_help = "Examples:
  felixctl rbac grouping ls
  felixctl rbac grouping ls --user p:alice --json"
    )]
    Ls {
        /// Only groupings for this user or group
        #[arg(long, value_name = "USER")]
        user: Option<String>,
        /// Only groupings to this role
        #[arg(long, value_name = "ROLE")]
        role: Option<String>,
    },
    /// Assign a role
    #[command(
        long_about = "Assign ROLE to USER. The token must be able to grant every policy \
                      the role carries.",
        after_long_help = "Examples:
  felixctl rbac grouping add p:alice role:reader"
    )]
    Add(GroupingArgs),
    /// Remove a role assignment
    #[command(
        long_about = "Take ROLE away from USER. Asks first on a terminal; elsewhere it \
                      needs --yes. Exits with status 5 when there is no such assignment.",
        after_long_help = "Examples:
  felixctl rbac grouping rm p:alice role:reader --yes"
    )]
    Rm {
        #[command(flatten)]
        grouping: GroupingArgs,
        #[command(flatten)]
        confirm: Confirm,
    },
}

#[derive(Debug, Clone, Args)]
pub(crate) struct GroupingArgs {
    /// The user or group, such as p:alice
    pub(crate) user: String,
    /// The role, such as role:reader
    pub(crate) role: String,
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
