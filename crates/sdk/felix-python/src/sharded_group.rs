//! One consumer group over every shard of a stream: the record it hands out.

use pyo3::prelude::*;

use crate::types::{GroupRecord, OwnedGroupRecord};

/// A record claimed through a sharded group, and the shard it came from.
/// Offsets are per shard, so settling it needs both.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardedGroupRecord {
    pub shard: u32,
    pub record: Py<GroupRecord>,
}

#[pymethods]
impl ShardedGroupRecord {
    fn __repr__(&self) -> String {
        format!(
            "ShardedGroupRecord(shard={}, offset={})",
            self.shard,
            self.record.get().offset
        )
    }
}

impl ShardedGroupRecord {
    /// The record as the client settles it.
    pub(crate) fn to_client(&self) -> felix_client::ShardedGroupRecord {
        felix_client::ShardedGroupRecord {
            shard: self.shard,
            record: self.record.get().delivery(),
        }
    }
}

/// Records carried off the runtime thread before there is a `Python<'_>`.
pub(crate) struct OwnedShardedGroupRecord(pub felix_client::ShardedGroupRecord);

impl<'py> IntoPyObject<'py> for OwnedShardedGroupRecord {
    type Target = ShardedGroupRecord;
    type Output = Bound<'py, ShardedGroupRecord>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let record = OwnedGroupRecord::from(self.0.record)
            .into_pyobject(py)?
            .unbind();
        Bound::new(
            py,
            ShardedGroupRecord {
                shard: self.0.shard,
                record,
            },
        )
    }
}
