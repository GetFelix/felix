//! What a broker reports about itself and the cluster: shard inspection,
//! subscription listings, topology and shard ownership.
//!
//! These answers are wire records with many nested fields, so they reach
//! Python as plain dicts and lists with the wire's field names. `_felix.pyi`
//! types them as `TypedDict`s. Each call asks the broker the client is
//! currently connected to, which is what an operator wants to compare: ask
//! each broker for its own view.

use felix_client::{ShardKind, SubscriptionCursor, SubscriptionFilter};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde::Serialize;
use serde_json::Value;

use crate::errors::to_py_err;

/// A wire record carried off the runtime thread, built into dicts and lists
/// once there is a `Python<'_>`.
pub(crate) struct Record(Value);

impl Record {
    fn of<T: Serialize>(value: &T) -> PyResult<Self> {
        serde_json::to_value(value)
            .map(Self)
            .map_err(|err| PyValueError::new_err(format!("could not encode the answer: {err}")))
    }
}

fn to_py<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Bool(flag) => flag.into_pyobject(py)?.to_owned().into_any(),
        Value::Number(n) => match (n.as_u64(), n.as_i64()) {
            (Some(u), _) => u.into_pyobject(py)?.into_any(),
            (None, Some(i)) => i.into_pyobject(py)?.into_any(),
            _ => n.as_f64().unwrap_or(f64::NAN).into_pyobject(py)?.into_any(),
        },
        Value::String(text) => text.into_pyobject(py)?.into_any(),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(to_py(py, item)?)?;
            }
            list.into_any()
        }
        Value::Object(fields) => {
            let dict = PyDict::new(py);
            for (name, field) in fields {
                dict.set_item(name, to_py(py, field)?)?;
            }
            dict.into_any()
        }
    })
}

impl<'py> IntoPyObject<'py> for Record {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        to_py(py, &self.0)
    }
}

pub(crate) fn parse_kind(kind: &str) -> PyResult<ShardKind> {
    match kind {
        "stream" => Ok(ShardKind::Stream),
        "cache" => Ok(ShardKind::Cache),
        other => Err(PyValueError::new_err(format!(
            "kind must be 'stream' or 'cache', not {other:?}"
        ))),
    }
}

/// The paging cursor `list_subscriptions` returned, given back as the dict it
/// came as.
pub(crate) fn parse_cursor(cursor: &Bound<'_, PyAny>) -> PyResult<SubscriptionCursor> {
    let bad =
        || PyValueError::new_err("cursor must be the next_cursor dict list_subscriptions returned");
    let dict = cursor.cast::<PyDict>().map_err(|_| bad())?;
    let field = |name: &str| dict.get_item(name).ok().flatten().ok_or_else(bad);
    Ok(SubscriptionCursor {
        tenant_id: field("tenant_id")?.extract().map_err(|_| bad())?,
        namespace: field("namespace")?.extract().map_err(|_| bad())?,
        stream: field("stream")?.extract().map_err(|_| bad())?,
        shard: field("shard")?.extract().map_err(|_| bad())?,
        subscriber_id: field("subscriber_id")?.extract().map_err(|_| bad())?,
    })
}

/// The arguments of `list_subscriptions`, owned for the runtime thread.
pub(crate) struct SubscriptionQuery {
    pub filter: SubscriptionFilter,
    pub limit: Option<u32>,
    pub cursor: Option<SubscriptionCursor>,
}

pub(crate) async fn inspect_shard(
    client: &felix_client::Client,
    kind: ShardKind,
    tenant_id: &str,
    namespace: &str,
    name: &str,
    shard: u32,
) -> PyResult<Record> {
    let view = client
        .client()
        .await
        .inspect_shard(kind, tenant_id, namespace, name, shard)
        .await
        .map_err(to_py_err)?;
    Record::of(&view)
}

pub(crate) async fn list_subscriptions(
    client: &felix_client::Client,
    query: SubscriptionQuery,
) -> PyResult<Record> {
    let page = client
        .client()
        .await
        .list_subscriptions(query.filter, query.limit, query.cursor)
        .await
        .map_err(to_py_err)?;
    Record::of(&serde_json::json!({
        "node_id": page.node_id,
        "subscriptions": page.subscriptions,
        "next_cursor": page.next_cursor,
    }))
}

pub(crate) async fn topology(client: &felix_client::Client) -> PyResult<Record> {
    let brokers = client.client().await.topology().await.map_err(to_py_err)?;
    Record::of(&brokers)
}

pub(crate) async fn shard_owners(
    client: &felix_client::Client,
    kind: ShardKind,
    tenant_id: &str,
    namespace: &str,
    name: &str,
) -> PyResult<Record> {
    let owners = client
        .client()
        .await
        .shard_owners(tenant_id, namespace, name, kind)
        .await
        .map_err(to_py_err)?;
    Record::of(&owners)
}

pub(crate) async fn stream_routing(
    client: &felix_client::Client,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
) -> PyResult<(u32, String)> {
    let (shards, routing) = client
        .client()
        .await
        .stream_routing(tenant_id, namespace, stream)
        .await
        .map_err(to_py_err)?;
    let name = match routing {
        felix_client::ShardRouting::Modulo => "modulo",
        felix_client::ShardRouting::JumpHash => "jump_hash",
    };
    Ok((shards, name.to_string()))
}

pub(crate) async fn cache_shards(
    client: &felix_client::Client,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
) -> PyResult<u32> {
    client
        .client()
        .await
        .cache_shards(tenant_id, namespace, cache)
        .await
        .map_err(to_py_err)
}

/// Which questions the broker in use answers, as `{name: bool}`.
pub(crate) async fn capabilities(
    client: &felix_client::Client,
) -> std::collections::BTreeMap<&'static str, bool> {
    let broker = client.client().await;
    std::collections::BTreeMap::from([
        ("inspect", broker.supports_inspect()),
        ("topology", broker.supports_topology()),
        ("stream_shards", broker.supports_stream_shards()),
        ("cache_shards", broker.supports_cache_shards()),
        ("shard_owners", broker.supports_shard_owners()),
    ])
}
