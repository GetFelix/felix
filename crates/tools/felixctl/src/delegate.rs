//! `data` and `controlplane migrate`: the server-side tools, run through the
//! binary that owns them.
//!
//! They work on a data directory or a database that only the server crates
//! know how to read, and felixctl stays free of those crates. So it runs
//! `felix-broker` or `felix-controlplane`, passes the arguments through and
//! ends with the same exit status.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::process::Command;

use crate::cli::{ControlplaneCommand, DataCommand};
use crate::error::{Exit, fail};

const BROKER: Tool = Tool {
    binary: "felix-broker",
    flag: "--broker-bin",
    env: "FELIX_BROKER_BIN",
};
const CONTROLPLANE: Tool = Tool {
    binary: "felix-controlplane",
    flag: "--controlplane-bin",
    env: "FELIX_CONTROLPLANE_BIN",
};

/// A server binary felixctl can run, and how an operator points at it.
struct Tool {
    binary: &'static str,
    flag: &'static str,
    env: &'static str,
}

/// `data <inspect|restore-point> ARGS...`.
pub(crate) fn data(command: &DataCommand, broker_bin: Option<PathBuf>) -> anyhow::Result<()> {
    let DataCommand::Run(words) = command;
    run(&BROKER, broker_bin, &["inspect", "restore-point"], words)
}

/// `controlplane migrate ARGS...`.
pub(crate) fn controlplane(
    command: &ControlplaneCommand,
    controlplane_bin: Option<PathBuf>,
) -> anyhow::Result<()> {
    let ControlplaneCommand::Run(words) = command;
    run(&CONTROLPLANE, controlplane_bin, &["migrate"], words)
}

/// Run `tool subcommand args...` with this process's stdio, and exit with its
/// status if that is not success.
fn run(
    tool: &Tool,
    flag: Option<PathBuf>,
    known: &[&str],
    words: &[OsString],
) -> anyhow::Result<()> {
    let name = words[0].to_string_lossy();
    if !known.contains(&name.as_ref()) {
        return Err(fail(
            Exit::Usage,
            format!(
                "unknown subcommand {name:?}; expected one of: {}",
                known.join(", ")
            ),
        ));
    }
    let program = program(tool, flag, std::env::var_os(tool.env));
    let status = Command::new(&program)
        .args(words)
        .status()
        .map_err(|err| spawn_error(tool, &program, &err))?;
    if status.success() {
        return Ok(());
    }
    std::process::exit(exit_code(status))
}

/// The binary to run: the flag, then the environment variable, then the name
/// itself, which the OS looks up on PATH.
fn program(tool: &Tool, flag: Option<PathBuf>, env: Option<OsString>) -> PathBuf {
    flag.or_else(|| env.filter(|value| !value.is_empty()).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(tool.binary))
}

fn spawn_error(tool: &Tool, program: &std::path::Path, err: &std::io::Error) -> anyhow::Error {
    let name = tool.binary;
    if err.kind() == ErrorKind::NotFound {
        fail(
            Exit::Usage,
            format!(
                "cannot run {}: {name} is not installed here or not on PATH. This command \
                 runs {name} on the host that holds the data; install it there or point \
                 felixctl at it with {} PATH or ${}",
                program.display(),
                tool.flag,
                tool.env,
            ),
        )
    } else {
        fail(Exit::Failure, format!("run {}: {err}", program.display()))
    }
}

/// The status to end with: the child's own code, or 128 plus the signal that
/// killed it, as a shell reports it.
fn exit_code(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    status.code().unwrap_or(1)
}

#[cfg(test)]
mod tests;
