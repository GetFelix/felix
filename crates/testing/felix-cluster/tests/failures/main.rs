//! Faults and what survives them: the injected faults themselves, leader
//! failover, fencing a deposed leader, and a leader partitioned from its
//! replicas.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test failures`, or one module with
//! `--test failures fencing::`.

mod clocks;
mod failover;
mod faults;
mod fenced_caches;
mod fencing;
mod followers_word;
mod fsync;
mod halted;
mod kafka_produce;
mod lease_free_reads;
mod lease_free_sessions;
mod links;
mod majority_ack;
mod membership;
mod partition;
mod promotion_fence;
mod retention_floor;
mod writes;
