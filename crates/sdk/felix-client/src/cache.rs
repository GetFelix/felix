//! Cache traffic: the workers that carry requests, and cache watches.
//!
//! Each cache worker owns exactly one bi-directional stream and runs strictly
//! sequential request/response round trips on it, fed by a bounded queue that
//! pushes back on callers when the worker falls behind. Parallelism comes
//! from having several workers across several connections; see
//! `crate::connection`.

mod versioned;
mod watch;
mod worker;

pub use versioned::{CacheConditionResult, VersionedValue};
pub use watch::{CacheChange, CacheWatch, CacheWatchFilter, CacheWatchItem};

pub(crate) use watch::filter_fields;
pub(crate) use worker::{CacheRequest, CacheWorker, run_cache_worker_with_limit};

#[cfg(test)]
mod tests;
