//! Help that is not clap's own: shell completions and man pages.

use std::path::Path;

use anyhow::Context as _;
use clap::CommandFactory;

use crate::cli::Cli;

/// Print the completion script for `shell`.
pub(crate) fn completions(shell: clap_complete::Shell) -> anyhow::Result<()> {
    let mut command = Cli::command();
    let mut script = Vec::new();
    clap_complete::generate(shell, &mut command, "felixctl", &mut script);
    match std::io::Write::write_all(&mut std::io::stdout().lock(), &script) {
        // `felixctl completions zsh | head` is not an error.
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => other.context("write the completion script"),
    }
}

/// Write `felixctl.1` and a `felixctl-<command>.1` per command and subcommand into
/// `dir`. Returns the files written.
pub(crate) fn man_pages(dir: &Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let mut command = Cli::command();
    command.build();
    let mut written = Vec::new();
    write_pages(&command, "felixctl", dir, &mut written)?;
    Ok(written)
}

fn write_pages(
    command: &clap::Command,
    name: &str,
    dir: &Path,
    written: &mut Vec<std::path::PathBuf>,
) -> anyhow::Result<()> {
    if command.is_hide_set() {
        return Ok(());
    }
    let page = command.clone().name(name.to_string());
    let path = dir.join(format!("{name}.1"));
    let mut bytes = Vec::new();
    clap_mangen::Man::new(page)
        .render(&mut bytes)
        .with_context(|| format!("render {name}"))?;
    std::fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))?;
    written.push(path);
    // clap's generated `help` subcommands are not worth a page each.
    for sub in command
        .get_subcommands()
        .filter(|sub| sub.get_name() != "help")
    {
        write_pages(sub, &format!("{name}-{}", sub.get_name()), dir, written)?;
    }
    Ok(())
}
