//! Behaviour every store backend must satisfy, written once and run against
//! each of them.
//!
//! Parity is the point: a rule that holds only in memory is a rule the
//! deployed system does not have. Each backend's tests call the `run_*`
//! entry points with a store of its own.
pub(crate) mod nodes;
pub(crate) mod pagination;
pub(crate) mod placement;
pub(crate) mod rbac;
pub(crate) mod refresh_tokens;
mod replica_reports;
pub(crate) mod resources;
pub(crate) mod shards;
pub(crate) mod signing_keys;
