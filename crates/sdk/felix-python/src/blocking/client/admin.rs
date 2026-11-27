//! Introspection: capabilities, shard inspection, subscriptions and topology.

use std::sync::Arc;

use pyo3::prelude::*;

use super::Client;
use crate::errors::to_py_err;
use crate::inspect;
use crate::runtime::block_on;

#[pymethods]
impl Client {
    /// Which questions the broker in use answers, as a dict of booleans:
    /// `inspect` (`inspect_shard` and `list_subscriptions`), `topology`,
    /// `stream_shards`, `cache_shards` and `shard_owners`. A broker older than
    /// a feature refuses its call; check here to tell that from a real failure.
    fn supports(&self, py: Python<'_>) -> PyResult<std::collections::BTreeMap<&'static str, bool>> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move { Ok(inspect::supports(&inner).await) })
    }

    /// The connected broker's own view of one shard, as a dict: its role and
    /// phase, whether it is serving and why not, its generation, fence, lease,
    /// tail and commit mark, and, on a leader, every replica's position.
    /// `kind` is `"stream"` or `"cache"`. Needs `node.view:cluster:*`, and
    /// raises on a broker that does not advertise it (see `supports`).
    /// Not forwarded: ask each broker for its own.
    fn inspect_shard(
        &self,
        py: Python<'_>,
        kind: &str,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
    ) -> PyResult<Py<PyAny>> {
        let kind = inspect::parse_kind(kind)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, name) = (
            tenant_id.to_string(),
            namespace.to_string(),
            name.to_string(),
        );
        let record = block_on(py, async move {
            inspect::inspect_shard(&inner, kind, &tenant_id, &namespace, &name, shard).await
        })?;
        Ok(record.into_pyobject(py)?.unbind())
    }

    /// One page of the subscriptions the connected broker serves, as
    /// `{"node_id", "subscriptions", "next_cursor"}`. Each subscription dict
    /// carries its queue `depth` and `capacity`, overflow `policy`, `dropped`
    /// count, position against the shard's tail, `peer` and `principal`.
    /// The keyword arguments narrow the list; `dropping=True` keeps only
    /// subscriptions that have dropped records. Pass `next_cursor` back as
    /// `cursor` for the next page; it is `None` on the last. `limit` defaults
    /// to 100 and the broker caps it at 1000. Needs `node.view:cluster:*`.
    #[pyo3(signature = (*, tenant_id=None, namespace=None, stream=None, shard=None, principal=None, dropping=false, limit=None, cursor=None))]
    fn list_subscriptions(
        &self,
        py: Python<'_>,
        tenant_id: Option<String>,
        namespace: Option<String>,
        stream: Option<String>,
        shard: Option<u32>,
        principal: Option<String>,
        dropping: bool,
        limit: Option<u32>,
        cursor: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
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
        let record = block_on(py, async move {
            inspect::list_subscriptions(&inner, query).await
        })?;
        Ok(record.into_pyobject(py)?.unbind())
    }

    /// The brokers clients may connect to, as a list of
    /// `{"node_id", "addr"}` dicts. Empty for a broker with no cluster behind
    /// it; raises on a broker that predates discovery.
    fn topology(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let record = block_on(py, async move { inspect::topology(&inner).await })?;
        Ok(record.into_pyobject(py)?.unbind())
    }

    /// Who owns each shard of a stream or cache, as a list of dicts with
    /// `shard`, `node_id`, `addr`, `generation` and, for a shard nobody can
    /// serve now, `unavailable`. A snapshot that can be stale the way a
    /// redirect can. `kind` is `"stream"` or `"cache"`.
    fn shard_owners(
        &self,
        py: Python<'_>,
        kind: &str,
        tenant_id: &str,
        namespace: &str,
        name: &str,
    ) -> PyResult<Py<PyAny>> {
        let kind = inspect::parse_kind(kind)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, name) = (
            tenant_id.to_string(),
            namespace.to_string(),
            name.to_string(),
        );
        let record = block_on(py, async move {
            inspect::shard_owners(&inner, kind, &tenant_id, &namespace, &name).await
        })?;
        Ok(record.into_pyobject(py)?.unbind())
    }

    /// How many shards a stream has and how a routing key maps onto them:
    /// `(shards, routing)` with `routing` `"modulo"` or `"jump_hash"`.
    fn stream_routing(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> PyResult<(u32, String)> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream) = (
            tenant_id.to_string(),
            namespace.to_string(),
            stream.to_string(),
        );
        block_on(py, async move {
            inspect::stream_routing(&inner, &tenant_id, &namespace, &stream).await
        })
    }

    /// Ask the broker in use which brokers a client may connect to, and add
    /// them to the endpoints this client will try. Returns how many the
    /// cluster named; zero is a legitimate answer. Raises when the broker
    /// predates discovery.
    fn refresh_topology(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            inner.refresh_topology().await.map_err(to_py_err)
        })
    }

    /// Live connections to each broker this client holds one for, as
    /// `(address, count)` pairs.
    fn connections_per_node(&self, py: Python<'_>) -> PyResult<Vec<(String, usize)>> {
        let inner = Arc::clone(&self.inner);
        block_on(py, async move {
            Ok(inner
                .connections_per_node()
                .await
                .into_iter()
                .map(|(addr, count)| (addr.to_string(), count))
                .collect())
        })
    }
}
