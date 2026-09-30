//! A list-append history checker, in the style of Jepsen's Elle, and the
//! campaign that feeds it: concurrent clients appending to and reading
//! `Quorum` streams, and putting to and getting `Quorum` cache keys, while a
//! nemesis kills, pauses and partitions brokers, and with
//! [`RandomNemesis::all_faults`] also cuts links, skews clocks, fails
//! fsyncs, moves shards and drains brokers. [`RandomNemesis::adversarial`]
//! overlaps faults, forces failovers, and cuts moves and restarts short.
//!
//! - [`model`]: what a history is.
//! - [`checker`]: the rules, and the report of what broke them.
//! - [`register`]: `Quorum` cache keys as registers, and the stale-read check.
//! - [`commit`]: atomic commits, and the check that no reader sees part of one.
//! - [`nemesis`]: which fault next, and how to inject and heal it.
//! - [`campaign`]: the run itself, configured from the environment, and
//!   the state dump a failing run prints.
//! - `liveness`: after every heal, the check that each shard serves again.
//!
//! What each rule means and how to read a violation is in
//! `docs/history-checker.md`.

pub mod campaign;
pub mod checker;
pub mod commit;
mod dump;
mod liveness;
pub mod model;
pub mod nemesis;
pub mod register;
pub mod rng;
mod workload;

pub use campaign::{Campaign, Mode};
pub use checker::{Report, Rule, Violation, check};
pub use model::History;
pub use nemesis::{Fault, FaultFamily, FaultKind, Nemesis, RandomNemesis};
