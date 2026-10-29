//! Caches across brokers: keys routed to the shard owner, and a cache that
//! survives losing that owner.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test caches`, or one module with
//! `--test caches cache_failover::`.

mod cache_failover;
mod cache_routing;
mod dead_broker;
