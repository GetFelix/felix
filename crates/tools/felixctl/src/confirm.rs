//! Asking before a command that cannot be undone, and `--yes` to skip it.

use std::io::{BufRead, IsTerminal, Write};

use clap::Args;

use crate::error::{Exit, fail};

/// `--yes`, which every destructive command takes.
#[derive(Debug, Clone, Copy, Args)]
pub(crate) struct Confirm {
    /// Do not ask for confirmation. Needed when stdin is not a terminal
    #[arg(long, short = 'y')]
    pub(crate) yes: bool,
}

/// Go ahead only with `--yes`, or when someone at a terminal says yes.
pub(crate) fn ask(question: &str, confirm: Confirm) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let interactive = stdin.is_terminal();
    confirm_with(question, confirm, interactive, || {
        let mut stderr = std::io::stderr().lock();
        write!(stderr, "{question} [y/N] ")?;
        stderr.flush()?;
        let mut answer = String::new();
        stdin.lock().read_line(&mut answer)?;
        Ok(answer)
    })
}

/// [`ask`] with the terminal check and the prompt passed in.
pub(crate) fn confirm_with(
    question: &str,
    confirm: Confirm,
    interactive: bool,
    prompt: impl FnOnce() -> std::io::Result<String>,
) -> anyhow::Result<()> {
    if confirm.yes {
        return Ok(());
    }
    if !interactive {
        return Err(fail(
            Exit::Usage,
            format!("{question} Pass --yes to confirm; stdin is not a terminal to ask on"),
        ));
    }
    let answer =
        prompt().map_err(|err| fail(Exit::Failure, format!("read the confirmation: {err}")))?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => Ok(()),
        _ => Err(fail(Exit::Failure, "not confirmed; nothing was changed")),
    }
}

#[cfg(test)]
mod tests;
