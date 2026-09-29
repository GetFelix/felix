//! A list-append history checker, in the style of Jepsen's Elle, and the
//! campaign that feeds it: concurrent clients appending to and reading
//! `Quorum` streams, and putting to and getting `Quorum` cache keys, while a
//! nemesis kills, pauses and partitions brokers, and with
//! [`RandomNemesis::all_faults`] also cuts links, skews clocks and fails
//! fsyncs.
//!
//! - [`model`]: what a history is.
//! - [`checker`]: the rules, and the report of what broke them.
//! - [`register`]: `Quorum` cache keys as registers, and the stale-read check.
//! - [`nemesis`]: which fault next, and how to inject and heal it.
//! - [`campaign`]: the run itself, configured from the environment.
//!
//! What each rule means and how to read a violation is in
//! `docs/history-checker.md`.

pub mod campaign;
pub mod checker;
pub mod model;
pub mod nemesis;
pub mod register;
pub mod rng;
mod workload;

pub use campaign::{Campaign, Mode};
pub use checker::{Report, Rule, Violation, check};
pub use model::History;
pub use nemesis::{Fault, FaultFamily, FaultKind, Nemesis, RandomNemesis};
