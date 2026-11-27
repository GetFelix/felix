//! Commits and state reads: the conditional-write surface over a stream's state.

use std::sync::Arc;

use pyo3::prelude::*;

use super::AsyncClient;
use crate::args::owned4;
use crate::conditional::ConditionalWrite;
use crate::errors::to_py_err;

#[pymethods]
impl AsyncClient {
    /// Commit `ops` as one record on the shard `entity_key` routes to. See
    /// the synchronous `Client.commit`.
    fn commit<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let receipt = inner
                .commit(&tenant_id, &namespace, &entity_key, ops)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::CommitReceipt {
                offset: receipt.offset,
            })
        })
    }

    /// `key` in the state of `stream`'s shard that `entity_key` routes to.
    fn state_get<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        entity_key: &[u8],
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
            key.to_string(),
        );
        let entity_key = entity_key.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = inner
                .state_get(&tenant_id, &namespace, &stream, &entity_key, &key)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::OwnedStateValue(state))
        })
    }

    /// Commit `ops` only if the shard's next offset is `expected_offset`.
    fn commit_if<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
        expected_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .commit_if(&tenant_id, &namespace, &entity_key, ops, expected_offset)
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// `commit`, to the named shard.
    fn commit_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let receipt = inner
                .commit_on_shard(&tenant_id, &namespace, shard, &entity_key, ops)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::CommitReceipt {
                offset: receipt.offset,
            })
        })
    }

    /// `commit_if`, to the named shard.
    fn commit_if_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<PyRef<'py, crate::commit::CommitOp>>,
        expected_offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let ops = crate::commit::ops(ops);
        let (tenant_id, namespace, entity_key) = (
            tenant_id.to_string(),
            namespace.to_string(),
            entity_key.to_vec(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let answer = inner
                .commit_if_on_shard(
                    &tenant_id,
                    &namespace,
                    shard,
                    &entity_key,
                    ops,
                    expected_offset,
                )
                .await
                .map_err(to_py_err)?;
            Ok(ConditionalWrite::from(answer))
        })
    }

    /// `key` in the state of the named shard of `stream`, with no entity key.
    fn state_get_on_shard<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, key) = owned4(tenant_id, namespace, stream, key);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = inner
                .state_get_on_shard(&tenant_id, &namespace, &stream, shard, &key)
                .await
                .map_err(to_py_err)?;
            Ok(crate::commit::OwnedStateValue(state))
        })
    }
}
