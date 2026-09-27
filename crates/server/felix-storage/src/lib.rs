//! Where records are kept: a log-structured segment store, and the caches
//! projected from it.
//!
//! **Start at [`disk_log::DiskLog`].** That is the durable log a stream shard
//! is made of — append, read a range, recover a torn tail. [`segment`] is the
//! byte format underneath it, [`EphemeralCache`] is the in-memory store a
//! broker without durable storage uses instead, and [`LogCache`] is the
//! key-to-latest-value projection that makes a cache out of a log.
//!
//! Three properties everything here rests on, each explained in
//! `docs/durable-storage.md`:
//!
//! - **Records below the high-water mark are never rewritten**, which is what
//!   lets recovery trust "valid bytes end at EOF".
//! - **A torn tail is repaired; interior corruption is fatal.** Refusing to
//!   start beats silently losing acknowledged records.
//! - **Indexes are derived, never trusted** — a missing, short or stale index
//!   is rebuilt from the segment it describes.
//!
//! [`CommitSequencer`] is the odd one out: it orders publishes rather than
//! storing them, and lives here because the order is per durable log and is
//! shared with the cache write path.
//!
//! ## Modules
//!
//! - The log: [`log`] (the trait and its types), [`disk_log`] (the durable
//!   implementation), [`segment`] (one segment file and its index), and `io`
//!   (positioned reads, preallocation, flushes).
//! - Projections of a log: [`cache`] ([`StorageApi`] and its two stores) and
//!   [`counter_log`], which both compact through `log_swap`.
//! - Ordering and reporting: `commit_order` ([`CommitSequencer`]),
//!   [`metrics_names`], and the errors every call returns ([`StorageError`],
//!   [`Corruption`]).
//! - [`tiered`] is an interface with no implementation yet.

pub mod cache;
mod commit_order;
pub mod counter_log;
pub mod disk_log;
mod error;
#[cfg(any(debug_assertions, test, feature = "fault-injection"))]
pub mod fault;
pub(crate) mod io;
pub mod log;
mod log_swap;
pub mod metrics_names;
pub mod segment;
pub mod tiered;

pub use cache::{
    CacheChange, CacheObserver, CacheSnapshotEntry, EphemeralCache, LogCache, StorageApi,
};
pub use commit_order::{CommitSequencer, CommitTurn, Superseded};
pub use counter_log::CounterStore;
pub use disk_log::{DiskLog, DiskLogProvider};
pub use error::{Corruption, CorruptionKind, CorruptionSite, Result, StorageError};
