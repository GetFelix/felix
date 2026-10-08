//! `felixctl`: publish, subscribe, read caches, inspect the control plane and
//! run benchmarks from the terminal.
//!
//! Data-plane commands use `felix-client`'s public API and nothing else, so
//! this binary doubles as a check that the API is enough to build tools on.
//! Control-plane commands use the REST API. `bench` runs felix-loadgen's
//! scenarios in process.
//!
//! - `cli`: every command and flag, with its help text.
//! - `context`: the config file of named profiles, and layering flags and
//!   `FELIX_*` variables over the chosen profile.
//! - `connect`: TLS, addresses and the client configuration.
//! - `publish`, `subscribe`, `cache`, `topology`: the data-plane commands.
//! - `group`, `counter`: consumer groups and counters, through the brokers.
//! - `controlplane`: the REST client and the read-only commands.
//! - `manage`: the control-plane writes, and confirming destructive ones.
//! - `bench`: the load-test front end.
//! - `output`, `error`, `help`: printing, exit statuses, completions and man
//!   pages.

mod bench;
mod cache;
mod cli;
mod connect;
mod context;
mod controlplane;
mod counter;
mod error;
mod group;
mod help;
mod manage;
mod output;
mod publish;
mod subscribe;
mod topology;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::error::exit_for;
use crate::output::Output;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let out = Output { json: cli.json };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => return report(&out, &anyhow::Error::new(err).context("start the runtime")),
    };
    match runtime.block_on(run(cli, &out)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => report(&out, &err),
    }
}

async fn run(cli: Cli, out: &Output) -> anyhow::Result<()> {
    let Some(command) = &cli.command else {
        return out.text(cli::OVERVIEW);
    };
    let env = |name: &str| std::env::var(name).ok();
    match command {
        Command::Completions(args) => return help::completions(args.shell),
        Command::Man(args) => {
            let written = help::man_pages(&args.out_dir)?;
            return out.done(
                &format!(
                    "wrote {} pages to {}",
                    written.len(),
                    args.out_dir.display()
                ),
                serde_json::json!({ "written": written }),
            );
        }
        _ => {}
    }
    let path = context::config_path(cli.connection.config.as_deref(), &env)?;
    if let Command::Context(command) = command {
        return context::run(command, &cli.connection, &path, out);
    }
    let config = context::ConfigFile::load(&path)?;
    let settings = context::resolve(&cli.connection, &env, &config)?;
    match command {
        Command::Pub(args) => publish::run(args, &settings, out).await,
        Command::Sub(args) => subscribe::run(args, &settings, out).await,
        Command::Cache(command) => cache::run(command, &settings, out).await,
        Command::Topology(args) => topology::run(args, &settings, out).await,
        Command::Group(command) => group::run(command, &settings, out).await,
        Command::Counter(command) => counter::run(command, &settings, out).await,
        Command::Tenant(command) => controlplane::tenant(command, &settings, out).await,
        Command::Namespace(command) => controlplane::namespace(command, &settings, out).await,
        Command::Stream(command) => controlplane::stream(command, &settings, out).await,
        Command::Node(command) => controlplane::node(command, &settings, out).await,
        Command::Shard(command) => controlplane::shard(command, &settings, out).await,
        Command::Placement(command) => manage::placement(command, &settings, out).await,
        Command::Bench(command) => bench::run(command, &settings, out).await,
        Command::Context(_) | Command::Completions(_) | Command::Man(_) => {
            unreachable!("handled above")
        }
    }
}

/// Print `err` to stderr and return its exit status.
fn report(out: &Output, err: &anyhow::Error) -> ExitCode {
    let exit = exit_for(err);
    if out.json {
        eprintln!(
            "{}",
            serde_json::json!({ "error": format!("{err:#}"), "exit": exit.code() })
        );
    } else {
        eprintln!("felixctl: {err:#}");
    }
    ExitCode::from(exit.code())
}
