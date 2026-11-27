//! Introspection: capabilities, shard inspection, subscriptions and topology.

use std::sync::Arc;

use pyo3::prelude::*;

use super::AsyncClient;
use crate::errors::to_py_err;
use crate::inspect;

#[pymethods]
impl AsyncClient {
    /// Which questions the broker in use answers. See the synchronous
    /// `Client.supports`.
    fn supports<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inspect::supports(&inner).await)
        })
    }

    /// The connected broker's view of one shard. See the synchronous
    /// `Client.inspect_shard`.
    fn inspect_shard<'py>(
        &self,
        py: Python<'py>,
        kind: &str,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kind = inspect::parse_kind(kind)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, name) = (
            tenant_id.to_string(),
            namespace.to_string(),
            name.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inspect::inspect_shard(&inner, kind, &tenant_id, &namespace, &name, shard).await
        })
    }

    /// One page of the connected broker's subscriptions. See the synchronous
    /// `Client.list_subscriptions`.
    #[pyo3(signature = (*, tenant_id=None, namespace=None, stream=None, shard=None, principal=None, dropping=false, limit=None, cursor=None))]
    fn list_subscriptions<'py>(
        &self,
        py: Python<'py>,
        tenant_id: Option<String>,
        namespace: Option<String>,
        stream: Option<String>,
        shard: Option<u32>,
        principal: Option<String>,
        dropping: bool,
        limit: Option<u32>,
        cursor: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = inspect::SubscriptionQuery {
            filter: felix_client::SubscriptionFilter {
                tenant_id,
                namespace,
                stream,
                shard,
                principal,
                dropping,
            },
            limit,
            cursor: cursor
                .filter(|c| !c.is_none())
                .map(inspect::parse_cursor)
                .transpose()?,
        };
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inspect::list_subscriptions(&inner, query).await
        })
    }

    /// The brokers clients may connect to. See the synchronous
    /// `Client.topology`.
    fn topology<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { inspect::topology(&inner).await },
        )
    }

    /// Who owns each shard of a stream or cache. See the synchronous
    /// `Client.shard_owners`.
    fn shard_owners<'py>(
        &self,
        py: Python<'py>,
        kind: &str,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kind = inspect::parse_kind(kind)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, name) = (
            tenant_id.to_string(),
            namespace.to_string(),
            name.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inspect::shard_owners(&inner, kind, &tenant_id, &namespace, &name).await
        })
    }

    /// `(shards, routing)` of a stream. See the synchronous
    /// `Client.stream_routing`.
    fn stream_routing<'py>(
        &self,
        py: Python<'py>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inspect::stream_routing(&inner, &tenant_id, &namespace, &stream).await
        })
    }

    /// Ask the broker which brokers a client may connect to. Resolves to how
    /// many the cluster named.
    fn refresh_topology<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.refresh_topology().await.map_err(to_py_err)
        })
    }

    /// Live connections to each broker, as `(address, count)` pairs.
    fn connections_per_node<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner
                .connections_per_node()
                .await
                .into_iter()
                .map(|(addr, count)| (addr.to_string(), count))
                .collect::<Vec<_>>())
        })
    }
}
