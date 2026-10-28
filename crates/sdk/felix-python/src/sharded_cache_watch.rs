//! The items a prefix watch across every shard of a cache yields.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::types::{CacheChange, ShardMoved};

/// A change from one shard of a sharded cache watch.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardedCacheChange {
    pub shard: u32,
    pub change: Py<CacheChange>,
}

#[pymethods]
impl ShardedCacheChange {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "ShardedCacheChange(shard={}, change={})",
            self.shard,
            self.change
                .bind(py)
                .repr()
                .map_or_else(|_| "?".into(), |r| r.to_string())
        )
    }
}

/// Every shard has delivered its current state; everything further is live.
/// Only on a retained watch, at most once.
#[pyclass(module = "felix", frozen)]
pub struct CacheStateComplete;

#[pymethods]
impl CacheStateComplete {
    fn __repr__(&self) -> &'static str {
        "CacheStateComplete()"
    }
}

/// One shard fell behind and its watch ended. The others are unaffected, and
/// `resume_offsets()` already accounts for it.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CacheShardLagged {
    pub shard: u32,
    pub resume_from: u64,
}

#[pymethods]
impl CacheShardLagged {
    fn __repr__(&self) -> String {
        format!(
            "CacheShardLagged(shard={}, resume_from={})",
            self.shard, self.resume_from
        )
    }
}

/// One shard's watch ended without a lag signal, usually because its owner
/// went away. The other shards are unaffected.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CacheShardClosed {
    pub shard: u32,
}

#[pymethods]
impl CacheShardClosed {
    fn __repr__(&self) -> String {
        format!("CacheShardClosed(shard={})", self.shard)
    }
}

/// A sharded watch item carried off the runtime thread.
pub(crate) struct OwnedShardedWatchItem(felix_client::ShardedCacheWatchItem);

impl From<felix_client::ShardedCacheWatchItem> for OwnedShardedWatchItem {
    fn from(item: felix_client::ShardedCacheWatchItem) -> Self {
        Self(item)
    }
}

impl<'py> IntoPyObject<'py> for OwnedShardedWatchItem {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        use felix_client::ShardedCacheWatchItem as Item;
        Ok(match self.0 {
            Item::Change { shard, change } => {
                let change = Bound::new(
                    py,
                    CacheChange {
                        key: change.key,
                        value: change.value.map(|bytes| PyBytes::new(py, &bytes).unbind()),
                        offset: change.offset,
                        expires_at_millis: change.expires_at_millis,
                    },
                )?;
                Bound::new(
                    py,
                    ShardedCacheChange {
                        shard,
                        change: change.unbind(),
                    },
                )?
                .into_any()
            }
            Item::StateComplete => Bound::new(py, CacheStateComplete)?.into_any(),
            Item::Lagged { shard, resume_from } => {
                Bound::new(py, CacheShardLagged { shard, resume_from })?.into_any()
            }
            Item::ShardMoved { shard, moved } => Bound::new(
                py,
                ShardMoved {
                    shard,
                    resume_from: moved.resume_from,
                    node_id: moved.node_id,
                    addr: moved.addr,
                    generation: moved.generation,
                },
            )?
            .into_any(),
            Item::ShardClosed { shard } => Bound::new(py, CacheShardClosed { shard })?.into_any(),
        })
    }
}
