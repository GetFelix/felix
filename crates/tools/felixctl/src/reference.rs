//! The command reference page, rendered from the clap definitions.
//!
//! `docs-site/.../reference/felixctl.md` is this output. A test compares the
//! two, so a flag added without regenerating the page fails CI; `task
//! docs:felixctl` rewrites it. Help text is written once, in `cli.rs`.

use std::fmt::Write as _;

use clap::{Arg, ArgAction, Command, CommandFactory};

use crate::cli::Cli;
use crate::error::Exit;

/// The whole page, front matter included.
pub(crate) fn markdown() -> String {
    let mut root = Cli::command();
    root.build();
    let mut out = String::new();
    out.push_str(
        "---\ntitle: \"felixctl Reference\"\n\
         description: \"Every felixctl command, subcommand and flag, with defaults and examples.\"\n---\n\n",
    );
    out.push_str(INTRO);
    if let Some(long) = root.get_long_about() {
        paragraphs(&mut out, &long.to_string(), None);
    }
    if let Some(examples) = root.get_after_long_help() {
        examples_block(&mut out, &examples.to_string());
    }
    index(&mut out, &root);
    globals(&mut out, &root);
    exit_status(&mut out);
    for sub in visible_subcommands(&root) {
        command(&mut out, sub, "felixctl", 2);
    }
    out
}

const INTRO: &str = "\
Every felixctl command and flag. This page is generated from the command definitions, and `felixctl help <command>` prints the same text. For installing felixctl, setting up a context and a first session, see [felixctl](/getting-started/felixctl/).

";

fn visible_subcommands(command: &Command) -> impl Iterator<Item = &Command> {
    command
        .get_subcommands()
        .filter(|sub| !sub.is_hide_set() && sub.get_name() != "help")
}

fn index(out: &mut String, root: &Command) {
    out.push_str("## Commands\n\n| Command | Summary |\n| --- | --- |\n");
    for sub in visible_subcommands(root) {
        let summary = sub.get_about().map(ToString::to_string).unwrap_or_default();
        let name = sub.get_name();
        let _ = writeln!(
            out,
            "| [`felixctl {name}`](#felixctl-{name}) | {} |",
            prose(&summary)
        );
    }
    out.push('\n');
}

fn globals(out: &mut String, root: &Command) {
    out.push_str("## Global options\n\nThese flags apply to every command. Each connection flag overrides the environment variable named in its help, which overrides the current context.\n\n");
    let mut headings: Vec<Option<&str>> = Vec::new();
    for arg in options(root).filter(|arg| arg.is_global_set()) {
        if !headings.contains(&arg.get_help_heading()) {
            headings.push(arg.get_help_heading());
        }
    }
    for heading in headings {
        let _ = writeln!(out, "### {}\n", heading.unwrap_or("Options"));
        option_table(
            out,
            options(root).filter(|arg| arg.is_global_set() && arg.get_help_heading() == heading),
        );
    }
}

fn exit_status(out: &mut String) {
    out.push_str("## Exit status\n\nScripts can branch on the exit status. Each value means one thing.\n\n| Status | Meaning |\n| --- | --- |\n| 0 | Success. |\n");
    for (exit, meaning) in [
        (Exit::Failure, "Anything not covered below."),
        (
            Exit::Usage,
            "Bad arguments, or settings missing or unreadable.",
        ),
        (
            Exit::Connection,
            "No broker or control plane could be reached.",
        ),
        (
            Exit::Server,
            "A broker or the control plane refused the request.",
        ),
        (
            Exit::NotFound,
            "The key, resource or context does not exist.",
        ),
        (
            Exit::Unknown,
            "A write may have been applied: the broker stopped waiting for it.",
        ),
    ] {
        let _ = writeln!(out, "| {} | {meaning} |", exit.code());
    }
    out.push_str(
        "\nWith `--json`, errors go to stderr as a JSON object holding `error` and `exit`.\n\n",
    );
}

/// One command, then its subcommands. `depth` is the heading level.
fn command(out: &mut String, command: &Command, parent: &str, depth: usize) {
    let path = format!("{parent} {}", command.get_name());
    let hashes = "#".repeat(depth.min(5));
    let _ = writeln!(out, "{hashes} `{path}`\n");
    let about = command.get_about().map(ToString::to_string);
    match command.get_long_about() {
        Some(long) => paragraphs(out, &long.to_string(), about.as_deref()),
        None => {
            if let Some(about) = &about {
                let _ = writeln!(out, "{}\n", prose(about));
            }
        }
    }
    let mut usage_command = command.clone();
    let usage = usage_command.render_usage().to_string();
    let _ = writeln!(
        out,
        "```text\n{}\n```\n",
        usage.trim().trim_start_matches("Usage:").trim()
    );

    let positionals: Vec<&Arg> = command
        .get_arguments()
        .filter(|arg| arg.is_positional() && !arg.is_hide_set())
        .collect();
    if !positionals.is_empty() {
        out.push_str("| Argument | Required | Description |\n| --- | --- | --- |\n");
        for arg in positionals {
            let name = arg
                .get_value_names()
                .and_then(|names| names.first().map(ToString::to_string))
                .unwrap_or_else(|| arg.get_id().to_string().to_uppercase());
            let required = if arg.is_required_set() { "yes" } else { "no" };
            let _ = writeln!(out, "| `{name}` | {required} | {} |", describe(arg));
        }
        out.push('\n');
    }
    let own: Vec<&Arg> = options(command)
        .filter(|arg| !arg.is_global_set())
        .collect();
    if !own.is_empty() {
        option_table(out, own.into_iter());
    }
    if let Some(examples) = command.get_after_long_help() {
        examples_block(out, &examples.to_string());
    }
    for sub in visible_subcommands(command) {
        self::command(out, sub, &path, depth + 1);
    }
}

/// Non-positional arguments other than clap's own `--help` and `--version`.
fn options(command: &Command) -> impl Iterator<Item = &Arg> {
    command.get_arguments().filter(|arg| {
        !arg.is_positional()
            && !arg.is_hide_set()
            && !matches!(arg.get_id().as_str(), "help" | "version")
    })
}

fn option_table<'a>(out: &mut String, args: impl Iterator<Item = &'a Arg>) {
    out.push_str("| Option | Value | Default | Description |\n| --- | --- | --- | --- |\n");
    for arg in args {
        let mut names = Vec::new();
        if let Some(short) = arg.get_short() {
            names.push(format!("-{short}"));
        }
        if let Some(long) = arg.get_long() {
            names.push(format!("--{long}"));
        }
        let takes_value = !matches!(
            arg.get_action(),
            ArgAction::SetTrue | ArgAction::SetFalse | ArgAction::Count | ArgAction::Help
        );
        let value = if takes_value {
            let name = arg
                .get_value_names()
                .and_then(|names| names.first().map(ToString::to_string))
                .unwrap_or_else(|| arg.get_id().to_string().to_uppercase());
            format!("`<{name}>`")
        } else {
            "-".to_string()
        };
        let defaults: Vec<String> = if takes_value {
            arg.get_default_values()
                .iter()
                .map(|value| format!("`{}`", value.to_string_lossy()))
                .collect()
        } else {
            Vec::new()
        };
        let default = if defaults.is_empty() {
            "-".to_string()
        } else {
            defaults.join(", ")
        };
        let _ = writeln!(
            out,
            "| `{}` | {value} | {default} | {} |",
            names.join(", "),
            describe(arg)
        );
    }
    out.push('\n');
}

/// An argument's help on one table line, with its allowed values.
fn describe(arg: &Arg) -> String {
    let help = arg
        .get_long_help()
        .or_else(|| arg.get_help())
        .map(ToString::to_string)
        .unwrap_or_default();
    let mut text = prose(&help);
    let values: Vec<String> = arg
        .get_possible_values()
        .iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| format!("`{}`", value.get_name()))
        .collect();
    if !values.is_empty() {
        if !text.ends_with('.') && !text.is_empty() {
            text.push('.');
        }
        let _ = write!(text, " One of {}.", values.join(", "));
    }
    text
}

/// Text for a paragraph or table cell. Inside backticks only `|` is escaped;
/// outside, `<` is too, because the page is Markdown and `<STREAM>` would
/// read as a tag.
fn prose(text: &str) -> String {
    let joined = text.split_whitespace().collect::<Vec<_>>().join(" ");
    joined
        .split('`')
        .enumerate()
        .map(|(index, part)| {
            let part = part.replace('|', "\\|");
            if index % 2 == 0 {
                part.replace('<', "\\<").replace('*', "\\*")
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join("`")
}

/// Blocks of `text` separated by blank lines. A block with an indented line is
/// a list or a layout and goes in a code block; the rest are paragraphs. The
/// first block is skipped when it only repeats `about`.
fn paragraphs(out: &mut String, text: &str, about: Option<&str>) {
    for (index, block) in text.trim().split("\n\n").enumerate() {
        if index == 0
            && about.is_some_and(|about| {
                about.trim_end_matches('.') == block.trim().trim_end_matches('.')
            })
        {
            // A one-line long_about that repeats the summary is still worth
            // printing once, as the summary.
            let _ = writeln!(out, "{}\n", prose(block));
            continue;
        }
        if block.lines().any(|line| line.starts_with(' ')) {
            let _ = writeln!(out, "```text\n{}\n```\n", block.trim_end());
        } else {
            let _ = writeln!(out, "{}\n", prose(block));
        }
    }
}

/// The `Examples:` block of `after_long_help`, as a shell block.
fn examples_block(out: &mut String, text: &str) {
    let body = text.trim();
    let Some(rest) = body.strip_prefix("Examples:") else {
        paragraphs(out, body, None);
        return;
    };
    let lines: Vec<&str> = rest
        .lines()
        .skip_while(|line| line.trim().is_empty())
        .map(|line| line.strip_prefix("  ").unwrap_or(line))
        .collect();
    let _ = writeln!(out, "Examples:\n\n```bash\n{}\n```\n", lines.join("\n"));
}

#[cfg(test)]
mod tests;
