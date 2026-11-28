//! Commits and state reads: the conditional-write surface over a stream's state.

use std::sync::Arc;

use pyo3::prelude::*;

use super::Client;
use crate::args::owned4;
use crate::conditional::ConditionalWrite;
use crate::errors::to_py_err;
use crate::runtime::block_on;

#[pymethods]
impl Client {
    /// Commit `ops` as one record on the shard `entity_key` routes to.
    ///
    /// Every reader sees all of it or none of it. `ops` must carry exactly
    /// one event (`CommitOp.publish` or `CommitOp.enqueue`) and name one
    /// stream; otherwise `EventCountError` or `NotOnOwningShardError` is
    /// raised before anything is sent. Returns a `CommitReceipt` whose
    /// `offset` is the event's offset and the version of every key written.
    fn commit(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'_, crate::commit::CommitOp>>,
    ) -> PyResult<crate::commit::CommitReceipt> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        let receipt = block_on(py, async move {
            inner
                .commit(&tenant_id, &namespace, &entity_key, ops)
                .await
                .map_err(to_py_err)
        })?;
        Ok(crate::commit::CommitReceipt {
            offset: receipt.offset,
        })
    }

    /// `key` in the state of `stream`'s shard that `entity_key` routes to,
    /// as a `StateValue` (value, version, as_of).
    fn state_get(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        entity_key: &[u8],
        key: &str,
    ) -> PyResult<Py<crate::commit::StateValue>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = owned4(tenant_id, namespace, stream, key);
        let entity_key = entity_key.to_vec();
        let state = block_on(py, async move {
            inner
                .state_get(&tenant_id, &namespace, &stream, &entity_key, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(crate::commit::OwnedStateValue(state)
            .into_pyobject(py)?
            .unbind())
    }

    /// Commit `ops` only if the shard's next offset is `expected_offset`, a
    /// compare-and-set on the whole shard. A refusal is a `ConditionalWrite`
    /// with `written` false and the shard's `tail`.
    fn commit_if(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'_, crate::commit::CommitOp>>,
        expected_offset: u64,
    ) -> PyResult<ConditionalWrite> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        block_on(py, async move {
            inner
                .commit_if(&tenant_id, &namespace, &entity_key, ops, expected_offset)
                .await
                .map(Into::into)
                .map_err(to_py_err)
        })
    }

    /// `commit`, to the named shard instead of the one `entity_key` hashes
    /// to. A shard the stream does not have is refused and nothing is written.
    fn commit_on_shard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<PyRef<'_, crate::commit::CommitOp>>,
    ) -> PyResult<crate::commit::CommitReceipt> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        let receipt = block_on(py, async move {
            inner
                .commit_on_shard(&tenant_id, &namespace, shard, &entity_key, ops)
                .await
                .map_err(to_py_err)
        })?;
        Ok(crate::commit::CommitReceipt {
            offset: receipt.offset,
        })
    }

    /// `commit_if`, to the named shard. `expected_offset` is that shard's
    /// next offset.
    fn commit_if_on_shard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<PyRef<'_, crate::commit::CommitOp>>,
        expected_offset: u64,
    ) -> PyResult<ConditionalWrite> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        block_on(py, async move {
            inner
                .commit_if_on_shard(
                    &tenant_id,
                    &namespace,
                    shard,
                    &entity_key,
                    ops,
                    expected_offset,
                )
                .await
                .map(Into::into)
                .map_err(to_py_err)
        })
    }

    /// `key` in the state of the named shard of `stream`, which takes no
    /// entity key.
    fn state_get_on_shard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        key: &str,
    ) -> PyResult<Py<crate::commit::StateValue>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = owned4(tenant_id, namespace, stream, key);
        let state = block_on(py, async move {
            inner
                .state_get_on_shard(&tenant_id, &namespace, &stream, shard, &key)
                .await
                .map_err(to_py_err)
        })?;
        Ok(crate::commit::OwnedStateValue(state)
            .into_pyobject(py)?
            .unbind())
    }
}
