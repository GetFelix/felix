//! Types the two surfaces share.
//!
//! Kept here rather than beside either surface because a `GroupRecord` handed
//! back by `Client` and one handed back by `AsyncClient` must be the same
//! Python type — an application that switches surfaces should not have to
//! switch its `isinstance` checks too.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// One record delivered to a subscriber.
#[pyclass(module = "felix", frozen, get_all)]
pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Py<PyBytes>,
    /// Log offset on a durable stream; `None` on an in-memory stream or from a
    /// broker that did not negotiate offsets.
    ///
    /// Two uses. Checkpoint `offset + 1` to resume after a reconnect. And to
    /// see drops: `offset - previous - 1 - skipped_before` events were dropped
    /// between two consecutive events.
    pub offset: Option<u64>,
    /// How many offsets just before `offset` hold no event. Non-zero only on
    /// the first event after a leader change, whose generation-start record
    /// took an offset; always zero from a broker that predates it.
    pub skipped_before: u64,
}

#[pymethods]
impl Event {
    fn __repr__(&self, py: Python<'_>) -> String {
        let len = self.payload.bind(py).as_bytes().len();
        match self.offset {
            Some(offset) => format!(
                "Event(stream='{}/{}/{}', {len} bytes, offset={offset})",
                self.tenant_id, self.namespace, self.stream
            ),
            None => format!(
                "Event(stream='{}/{}/{}', {len} bytes)",
                self.tenant_id, self.namespace, self.stream
            ),
        }
    }
}

/// One record handed to a consumer group member, with the offset to settle.
#[pyclass(module = "felix", frozen, get_all)]
pub struct GroupRecord {
    /// Acknowledge or hand back *this* offset.
    pub offset: u64,
    pub payload: Py<PyBytes>,
    /// How many times this record has been delivered, this one included.
    ///
    /// `1` is a first attempt; higher is a redelivery, so a consumer can treat
    /// a retry differently. `0` means the broker did not say — absent rather
    /// than first, because claiming a first attempt for an unknown one would
    /// skip exactly the retry handling that was wanted.
    pub attempts: u32,
    /// How many offsets directly below this one were settled without being
    /// delivered (generation starts, records retention removed), so a hole
    /// with this count will not fill. `0` from a broker that predates it.
    pub skipped_before: u64,
}

#[pymethods]
impl GroupRecord {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "GroupRecord(offset={}, {} bytes, attempts={})",
            self.offset,
            self.payload.bind(py).as_bytes().len(),
            self.attempts
        )
    }
}

/// One change delivered on a cache watch.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CacheChange {
    pub key: String,
    /// What the key now holds; `None` means it was deleted.
    pub value: Option<Py<PyBytes>>,
    /// The cache-log offset of this change. Checkpoint `offset + 1`.
    ///
    /// Offsets are sparse on a filtered watch, because other keys' changes
    /// consume them — so a gap here is *not* a drop signal. `CacheWatchLagged`
    /// is.
    pub offset: u64,
    /// Absolute Unix milliseconds this value expires at; `0` means never.
    pub expires_at_millis: u64,
}

#[pymethods]
impl CacheChange {
    fn __repr__(&self) -> String {
        match &self.value {
            Some(_) => format!("CacheChange(key={:?}, offset={})", self.key, self.offset),
            None => format!(
                "CacheChange(key={:?}, deleted, offset={})",
                self.key, self.offset
            ),
        }
    }
}

/// The watch fell behind and the broker ended it.
///
/// A separate type rather than an exception because it is not a failure: the
/// watch did its job and said so. Re-watching from `resume_from` is gapless.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CacheWatchLagged {
    /// The offset of the first change this watch missed.
    pub resume_from: u64,
}

#[pymethods]
impl CacheWatchLagged {
    fn __repr__(&self) -> String {
        format!("CacheWatchLagged(resume_from={})", self.resume_from)
    }
}

/// The watch's shard moved to another broker.
///
/// A notice, not an end: the watch follows the shard to its new owner and
/// carries on where it left off.
#[pyclass(module = "felix", frozen, get_all)]
pub struct CacheWatchShardMoved {
    /// Where the old owner says to resume, when it could say.
    pub resume_from: Option<u64>,
    /// The broker taking the shard, when known.
    pub node_id: Option<String>,
    /// That broker's client address, when the cluster publishes one.
    pub addr: Option<String>,
    /// The assignment generation that moved the shard.
    pub generation: u64,
}

#[pymethods]
impl CacheWatchShardMoved {
    fn __repr__(&self) -> String {
        format!(
            "CacheWatchShardMoved(resume_from={:?}, node_id={:?}, generation={})",
            self.resume_from, self.node_id, self.generation
        )
    }
}

/// A record from one shard of a multi-shard subscription.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardRecord {
    pub shard: u32,
    pub event: Py<Event>,
}

#[pymethods]
impl ShardRecord {
    fn __repr__(&self) -> String {
        format!("ShardRecord(shard={})", self.shard)
    }
}

/// One shard of a multi-shard subscription stopped delivering.
///
/// Surfaced rather than swallowed: the other shards carry on, so a consumer
/// that ignored this would silently be reading part of the stream while
/// believing it read all of it.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardLost {
    pub shard: u32,
    pub error: String,
}

#[pymethods]
impl ShardLost {
    fn __repr__(&self) -> String {
        format!("ShardLost(shard={}, error={:?})", self.shard, self.error)
    }
}

/// A shard that was lost is delivering again.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardRecovered {
    pub shard: u32,
}

#[pymethods]
impl ShardRecovered {
    fn __repr__(&self) -> String {
        format!("ShardRecovered(shard={})", self.shard)
    }
}

/// A shard of a multi-shard subscription moved to another broker.
///
/// The subscription follows it on its own: the shard's records carry on from
/// the new owner, or `ShardLost` comes next if it cannot be reached. Surfaced
/// so a consumer can see a rebalance happen.
#[pyclass(module = "felix", frozen, get_all)]
pub struct ShardMoved {
    pub shard: u32,
    /// Where the old owner says the shard resumes, when it could say.
    pub resume_from: Option<u64>,
    /// The broker taking the shard, when known.
    pub node_id: Option<String>,
    /// That broker's client address, when the cluster publishes one.
    pub addr: Option<String>,
    /// The assignment generation that moved the shard.
    pub generation: u64,
}

#[pymethods]
impl ShardMoved {
    fn __repr__(&self) -> String {
        format!(
            "ShardMoved(shard={}, resume_from={:?}, node_id={:?}, generation={})",
            self.shard, self.resume_from, self.node_id, self.generation
        )
    }
}

/// Which key or prefix a cache watch follows.
///
/// A function pair rather than an enum class, because `felix.key("a")` reads
/// better at a call site than constructing a variant, and Python has no enum
/// shape that would.
#[pyclass(module = "felix", frozen, from_py_object)]
#[derive(Clone)]
pub struct CacheWatchFilter {
    pub(crate) prefix: bool,
    pub(crate) value: String,
}

#[pymethods]
impl CacheWatchFilter {
    /// Watch exactly this key.
    #[staticmethod]
    fn key(value: &str) -> Self {
        Self {
            prefix: false,
            value: value.to_string(),
        }
    }

    /// Watch every key beginning with this prefix; `""` is every key in the
    /// shard.
    #[staticmethod]
    fn prefix(value: &str) -> Self {
        Self {
            prefix: true,
            value: value.to_string(),
        }
    }

    fn __repr__(&self) -> String {
        if self.prefix {
            format!("CacheWatchFilter.prefix({:?})", self.value)
        } else {
            format!("CacheWatchFilter.key({:?})", self.value)
        }
    }
}

impl CacheWatchFilter {
    pub(crate) fn to_client(&self) -> felix_client::CacheWatchFilter {
        if self.prefix {
            felix_client::CacheWatchFilter::Prefix(self.value.clone())
        } else {
            felix_client::CacheWatchFilter::Key(self.value.clone())
        }
    }
}

/// An event carried back to the Python loop.
///
/// A separate owned type because the payload has to cross a thread boundary
/// before there is a `Python<'_>` to build a `PyBytes` with; the conversion to
/// [`Event`] happens once the result reaches the loop.
pub(crate) struct OwnedEvent {
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Vec<u8>,
    offset: Option<u64>,
    skipped_before: u64,
}

impl From<felix_client::Event> for OwnedEvent {
    fn from(event: felix_client::Event) -> Self {
        Self {
            tenant_id: event.tenant_id.to_string(),
            namespace: event.namespace.to_string(),
            stream: event.stream.to_string(),
            payload: event.payload.to_vec(),
            offset: event.offset,
            skipped_before: event.skipped_before,
        }
    }
}

impl<'py> IntoPyObject<'py> for OwnedEvent {
    type Target = Event;
    type Output = Bound<'py, Event>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            Event {
                tenant_id: self.tenant_id,
                namespace: self.namespace,
                stream: self.stream,
                payload: PyBytes::new(py, &self.payload).unbind(),
                offset: self.offset,
                skipped_before: self.skipped_before,
            },
        )
    }
}

/// Owned forms, for carrying results off the runtime thread before there is a
/// `Python<'_>` to build the Python objects with.
pub(crate) struct OwnedGroupRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
    pub attempts: u32,
    pub skipped_before: u64,
}

impl From<felix_wire::GroupRecord> for OwnedGroupRecord {
    fn from(record: felix_wire::GroupRecord) -> Self {
        Self {
            offset: record.offset,
            payload: record.payload.to_vec(),
            attempts: record.attempts,
            skipped_before: record.skipped_before,
        }
    }
}

impl<'py> IntoPyObject<'py> for OwnedGroupRecord {
    type Target = GroupRecord;
    type Output = Bound<'py, GroupRecord>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            GroupRecord {
                offset: self.offset,
                payload: PyBytes::new(py, &self.payload).unbind(),
                attempts: self.attempts,
                skipped_before: self.skipped_before,
            },
        )
    }
}

pub(crate) enum OwnedWatchItem {
    Change {
        key: String,
        value: Option<Vec<u8>>,
        offset: u64,
        expires_at_millis: u64,
    },
    Lagged {
        resume_from: u64,
    },
    ShardMoved(felix_client::ShardMoved),
}

impl From<felix_client::CacheWatchItem> for OwnedWatchItem {
    fn from(item: felix_client::CacheWatchItem) -> Self {
        match item {
            felix_client::CacheWatchItem::Change(change) => Self::Change {
                key: change.key,
                value: change.value.map(|bytes| bytes.to_vec()),
                offset: change.offset,
                expires_at_millis: change.expires_at_millis,
            },
            felix_client::CacheWatchItem::Lagged { resume_from } => Self::Lagged { resume_from },
            felix_client::CacheWatchItem::ShardMoved(moved) => Self::ShardMoved(moved),
        }
    }
}

impl<'py> IntoPyObject<'py> for OwnedWatchItem {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Change {
                key,
                value,
                offset,
                expires_at_millis,
            } => Ok(Bound::new(
                py,
                CacheChange {
                    key,
                    value: value.map(|bytes| PyBytes::new(py, &bytes).unbind()),
                    offset,
                    expires_at_millis,
                },
            )?
            .into_any()),
            Self::Lagged { resume_from } => {
                Ok(Bound::new(py, CacheWatchLagged { resume_from })?.into_any())
            }
            Self::ShardMoved(moved) => Ok(Bound::new(
                py,
                CacheWatchShardMoved {
                    resume_from: moved.resume_from,
                    node_id: moved.node_id,
                    addr: moved.addr,
                    generation: moved.generation,
                },
            )?
            .into_any()),
        }
    }
}

pub(crate) enum OwnedShardEvent {
    Record {
        shard: u32,
        tenant_id: String,
        namespace: String,
        stream: String,
        payload: Vec<u8>,
        offset: Option<u64>,
        skipped_before: u64,
    },
    Lost {
        shard: u32,
        error: String,
    },
    Recovered {
        shard: u32,
    },
    Moved {
        shard: u32,
        moved: felix_client::ShardMoved,
    },
}

/// The next sharded event this binding has a shape for. `ShardEvent` is
/// non-exhaustive, so a kind added later is skipped until it gets one.
pub(crate) async fn next_shard_event(
    subscription: &mut felix_client::ShardedSubscription,
) -> Option<OwnedShardEvent> {
    loop {
        if let Some(event) = OwnedShardEvent::from_event(subscription.next().await?) {
            return Some(event);
        }
    }
}

impl OwnedShardEvent {
    fn from_event(event: felix_client::ShardEvent) -> Option<Self> {
        Some(match event {
            felix_client::ShardEvent::Record { shard, event } => Self::Record {
                shard,
                tenant_id: event.tenant_id.to_string(),
                namespace: event.namespace.to_string(),
                stream: event.stream.to_string(),
                payload: event.payload.to_vec(),
                offset: event.offset,
                skipped_before: event.skipped_before,
            },
            felix_client::ShardEvent::ShardLost { shard, error } => Self::Lost { shard, error },
            felix_client::ShardEvent::ShardRecovered { shard } => Self::Recovered { shard },
            felix_client::ShardEvent::ShardMoved { shard, moved } => Self::Moved { shard, moved },
            _ => return None,
        })
    }
}

impl<'py> IntoPyObject<'py> for OwnedShardEvent {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Record {
                shard,
                tenant_id,
                namespace,
                stream,
                payload,
                offset,
                skipped_before,
            } => {
                let event = Bound::new(
                    py,
                    Event {
                        tenant_id,
                        namespace,
                        stream,
                        payload: PyBytes::new(py, &payload).unbind(),
                        offset,
                        skipped_before,
                    },
                )?;
                Ok(Bound::new(
                    py,
                    ShardRecord {
                        shard,
                        event: event.unbind(),
                    },
                )?
                .into_any())
            }
            Self::Lost { shard, error } => {
                Ok(Bound::new(py, ShardLost { shard, error })?.into_any())
            }
            Self::Recovered { shard } => Ok(Bound::new(py, ShardRecovered { shard })?.into_any()),
            Self::Moved { shard, moved } => Ok(Bound::new(
                py,
                ShardMoved {
                    shard,
                    resume_from: moved.resume_from,
                    node_id: moved.node_id,
                    addr: moved.addr,
                    generation: moved.generation,
                },
            )?
            .into_any()),
        }
    }
}
