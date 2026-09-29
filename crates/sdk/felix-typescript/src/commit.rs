//! Atomic commits: the op objects JavaScript passes in and the results it
//! gets back. The checks (one event, one stream) are the Rust client's.
//! See `docs/atomic-commit.md`.

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::errors::invalid;

/// One part of a commit, as a plain object:
/// `{ op: "publish", stream, payload }`, `{ op: "enqueue", queue, payload }`,
/// `{ op: "put", stream, key, value }` or `{ op: "delete", stream, key }`.
#[napi(object)]
pub struct CommitOp {
    pub op: String,
    pub stream: Option<String>,
    pub queue: Option<String>,
    pub key: Option<String>,
    pub payload: Option<Buffer>,
    pub value: Option<Buffer>,
}

/// A commit the broker made durable.
#[napi(object)]
pub struct CommitReceipt {
    /// Where the commit's event is read, and the version of every key it
    /// wrote.
    pub offset: BigInt,
}

/// A key in a stream shard's state.
#[napi(object)]
pub struct StateValue {
    /// Absent when the key was never written or was deleted.
    pub value: Option<Buffer>,
    /// Offset of the commit that wrote `value`.
    pub version: Option<BigInt>,
    /// Offset of the last commit the answer reflects.
    pub as_of: Option<BigInt>,
}

impl From<felix_client::StateValue> for StateValue {
    fn from(state: felix_client::StateValue) -> Self {
        Self {
            value: state.value.map(|bytes| bytes.to_vec().into()),
            version: state.version.map(BigInt::from),
            as_of: state.as_of.map(BigInt::from),
        }
    }
}

/// The Rust client's op for one JavaScript object, or which field is missing.
pub(crate) fn to_client(op: CommitOp) -> Result<felix_client::CommitOp> {
    fn need<T>(field: Option<T>, op: &str, name: &str) -> Result<T> {
        field.ok_or_else(|| invalid(format!("a `{op}` commit op needs `{name}`")))
    }
    let kind = op.op.as_str();
    Ok(match kind {
        "publish" => felix_client::CommitOp::publish(
            need(op.stream, kind, "stream")?,
            need(op.payload, kind, "payload")?.to_vec(),
        ),
        "enqueue" => felix_client::CommitOp::enqueue(
            need(op.queue.or(op.stream), kind, "queue")?,
            need(op.payload, kind, "payload")?.to_vec(),
        ),
        "put" => felix_client::CommitOp::put(
            need(op.stream, kind, "stream")?,
            need(op.key, kind, "key")?,
            need(op.value, kind, "value")?.to_vec(),
        ),
        "delete" => felix_client::CommitOp::delete(
            need(op.stream, kind, "stream")?,
            need(op.key, kind, "key")?,
        ),
        other => return Err(invalid(format!("unknown commit op `{other}`"))),
    })
}
