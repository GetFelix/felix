//! The top-level flags, the command list and the shared `--yes`.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::counter::CounterCommand;
use crate::group::GroupCommand;

use super::bench::BenchCommand;
use super::cache::CacheCommand;
use super::cluster::{DataCommand, NodeCommand, PlacementCommand, ShardCommand};
use super::context::ContextCommand;
use super::inspect::{InspectCommand, TopologyArgs};
use super::messaging::{PubArgs, SubArgs};
use super::operations::{AuditCommand, FeaturesCommand, RaftCommand};
use super::rbac::RbacCommand;
use super::reform::BackupPointArgs;
use super::stream::StreamCommand;
use super::tenancy::{NamespaceCommand, TenantCommand};

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
                 node, shard, placement, features, rbac
  Operators      inspect shard|subs, backup-point, data
  Tools          context, bench, completions

Run `felixctl help <command>` or `felixctl <command> --help` for details.
";

#[derive(Debug, Parser)]
#[command(
    name = "felixctl",
    version,
    about = "Work with a Felix cluster from the terminal",
    long_about = "Publish and subscribe to streams, read and watch caches, see where shards \
                  live, list what the control plane knows, steer placement and run \
                  benchmarks. It is the one operator command line: `data` runs the broker's \
                  own tools on the host that holds its data.\n\n\
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
    /// The felix-broker binary `data` runs [env: FELIX_BROKER_BIN] [default:
    /// felix-broker on PATH]. Goes before the command
    #[arg(long, value_name = "PATH", help_heading = "Tools")]
    pub(crate) broker_bin: Option<PathBuf>,
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
    /// PEM client certificate chain to present to the control plane [env:
    /// FELIX_CONTROLPLANE_CLIENT_CERT]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) controlplane_client_cert_file: Option<PathBuf>,
    /// PEM private key for --controlplane-client-cert-file [env:
    /// FELIX_CONTROLPLANE_CLIENT_KEY]
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) controlplane_client_key_file: Option<PathBuf>,
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
                      owns the shard and acks on enqueue (FELIX_ACK_ON_COMMIT=0, or an in-memory stream) answers \
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
                      control plane assigned them, list the moves in progress, and move a \
                      shard's leadership or cancel a move.",
        after_long_help = "Examples:
  felixctl shard ls
  felixctl shard ls --leader broker-2
  felixctl shard moves
  felixctl shard move orders 2 --to broker-3
  felixctl shard move cancel orders 2"
    )]
    Shard(ShardCommand),

    /// See what placement will do, and pause or resume its moves (control plane)
    #[command(
        subcommand,
        long_about = "See what placement would do next and which shards are short of their \
                      replication factor, pause or resume the moves placement starts by \
                      itself, and give up a stranded shard's log. Reading needs a \
                      control-plane token allowed node.view on the cluster; pausing, \
                      resuming and abandoning need node.manage.",
        after_long_help = "Examples:
  felixctl placement plan
  felixctl placement replication
  felixctl placement pause
  felixctl placement resume
  felixctl placement abandon orders 2 --yes"
    )]
    Placement(PlacementCommand),

    /// Plan re-forming Raft groups that lost their majority (control plane)
    #[command(
        subcommand,
        long_about = "Re-form a Raft group that can no longer elect a leader, because too \
                      many of its copies are lost. `raft reform plan` surveys the brokers \
                      and prints what a re-form would do for each shard: which copy it \
                      starts from, the new voters, and whether records could be lost. It \
                      changes nothing. Needs a control-plane token allowed node.manage on \
                      the cluster.",
        after_long_help = "Examples:
  felixctl raft reform plan
  felixctl raft reform plan --scope node broker-2
  felixctl raft reform plan --json"
    )]
    Raft(RaftCommand),

    /// List fleet features and finalize them (control plane)
    #[command(
        subcommand,
        long_about = "A fleet feature is a behaviour every serving broker must support \
                      before it is switched on. `features ls` shows which each broker \
                      version offers and which are on; `features finalize` switches one on \
                      fleet-wide. Finalizing is one-way: brokers without the feature are \
                      refused afterwards. Reading needs a control-plane token allowed \
                      node.view on the cluster; finalizing needs node.manage.",
        after_long_help = "Examples:
  felixctl features ls
  felixctl features finalize jump_hash_routing --dry-run
  felixctl features finalize jump_hash_routing --yes"
    )]
    Features(FeaturesCommand),

    /// Read the control plane's audit log of who did what (control plane)
    #[command(
        subcommand,
        long_about = "The control plane records every change made through its API and every \
                      refused request: who asked, from where, what, and the answer. Records \
                      are committed through the control plane's Raft log and numbered without \
                      gaps; it keeps the newest 10,000. Reading needs a control-plane token \
                      allowed audit.view on the cluster.",
        after_long_help = "Examples:
  felixctl audit ls
  felixctl audit ls --after 1200 --limit 50
  felixctl audit ls --follow --json"
    )]
    Audit(AuditCommand),

    /// Record every shard's committed offsets as a backup point (control plane)
    #[command(
        name = "backup-point",
        long_about = "Record one committed offset per shard log, across every broker, and \
                      write them to a manifest a restore cuts back to. The offsets are read \
                      from each shard leader's metrics listener, so run it from somewhere \
                      that can reach those listeners. Reading needs a control-plane token \
                      allowed node.view on the cluster.\n\n\
                      The brokers do not agree on an instant, so a record written while the \
                      point is collected can be in it on one shard and not on another.",
        after_long_help = "Examples:
  felixctl backup-point nightly-2026-11-05
  felixctl backup-point nightly --out /backups/nightly.json
  felixctl backup-point nightly --broker broker-1=http://10.0.0.5:9090 --metrics-port 9090"
    )]
    BackupPoint(BackupPointArgs),

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
                      to answer is reported, not guessed at.",
        after_long_help = "Examples:
  felixctl inspect shard orders --shard 3
  felixctl inspect shard acme/default/orders
  felixctl inspect shard sessions --cache --json
  felixctl inspect subs orders --dropping
  felixctl inspect subs --node broker-a --limit 500"
    )]
    Inspect(InspectCommand),

    /// Inspect and restore a broker's data directory (runs felix-broker)
    #[command(
        subcommand,
        name = "data",
        long_about = "Work on a broker's data directory with the tools in the felix-broker \
                      binary. These run on the host that holds the directory, with \
                      felix-broker installed there: felixctl finds it with --broker-bin (before \
                      `data`), then $FELIX_BROKER_BIN, then PATH, passes every argument after \
                      the subcommand through unchanged, --help included, and ends with its \
                      exit status.\n\n\
                      `data inspect segments|records` reads a data directory, read-only: \
                      what startup would decide about every shard's segments, and the \
                      records around an offset. `data restore-point` cuts a stopped \
                      broker's logs back to a backup point written by `felixctl \
                      backup-point`. For their own usage run `felixctl data inspect \
                      --help` or `felixctl data restore-point --help`.",
        after_long_help = "Examples:
  felixctl data inspect segments /var/lib/felix/data
  felixctl data inspect records /var/lib/felix/data acme/default/orders/3 --offset 1500
  felixctl data restore-point --point nightly.backup-point.json --node broker-1"
    )]
    Data(DataCommand),

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

    /// Print the command reference page as Markdown
    #[command(
        hide = true,
        long_about = "Print the Markdown behind the command reference page in the docs. \
                      `task docs:felixctl` writes it to the page."
    )]
    Reference,
}

/// `--yes`, which every destructive command takes.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct Confirm {
    /// Do not ask for confirmation. Needed when stdin is not a terminal
    #[arg(long, short = 'y')]
    pub(crate) yes: bool,
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
