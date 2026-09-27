//! Per-message work for the client protocol, one module per operation family.

pub(crate) mod cache_watch;
pub(crate) mod publish;
pub(crate) mod redirect;
pub(crate) mod subscribe;
