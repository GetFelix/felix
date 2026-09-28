//! Where records go in a multi-node cluster: forwarding across brokers,
//! redirects, keyed routing over shards, and shards moving between brokers.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test routing`, or one module with
//! `--test routing sharding::`.

mod cross_broker;
mod generation_start;
mod kafka_leaders;
mod moved_readers;
mod operator_moves;
mod publish_routing;
mod rebalance;
mod redirect;
mod regions;
mod sharded_subscribe;
mod sharding;
mod shutdown_handoff;
mod subscriptions_follow;
mod watches_follow;
mod zones;
