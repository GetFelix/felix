//! The command line: every command, flag and help text, as clap types.
//!
//! Nothing here acts. `main` hands the parsed [`Cli`] to the module that owns
//! each command.

/// Benchmark commands.
mod bench;
/// Cache commands.
mod cache;
/// Node, shard, placement and data commands.
mod cluster;
/// Connection profiles (`context`).
mod context;
/// Inspection commands (`inspect`, `topology`).
mod inspect;
/// `pub` and `sub`: their arguments and the value types they share.
mod messaging;
/// Audit, feature and Raft commands.
mod operations;
/// RBAC policy and grouping commands.
mod rbac;
/// Cluster re-formation and `backup-point`.
mod reform;
/// The top-level flags, the command list and the shared `--yes`.
mod root;
/// Stream commands and the settings they take on create and update.
mod stream;
/// Tenant and namespace commands.
mod tenancy;

pub(crate) use bench::*;
pub(crate) use cache::*;
pub(crate) use cluster::*;
pub(crate) use context::*;
pub(crate) use inspect::*;
pub(crate) use messaging::*;
pub(crate) use operations::*;
pub(crate) use rbac::*;
pub(crate) use reform::*;
pub(crate) use root::*;
pub(crate) use stream::*;
pub(crate) use tenancy::*;

#[cfg(test)]
/// clap::{CommandFactory, Parser};
mod tests;
