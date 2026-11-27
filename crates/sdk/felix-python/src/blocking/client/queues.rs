//! Consumer groups. A queue rather than a stream: records are *pulled*, because only
//! the consumer knows when it has capacity, and each one is claimed by one member
//! until it is settled or its visibility timeout lapses.

use std::sync::Arc;

use pyo3::prelude::*;

use super::Client;
use crate::args::{non_negative_duration, owned4, parse_required_start, positive_duration};
use crate::blocking::ShardedGroupHandle;
use crate::errors::to_py_err;
use crate::group_admin::{GroupInfo, GroupPosition};
use crate::runtime::block_on;
use crate::types::{GroupRecord, OwnedGroupRecord};

#[pymethods]
impl Client {
    /// Take up to `max_records` for this group, waiting up to `wait` seconds
    /// for work to appear.
    ///
    /// An empty list is an answer, not an error: the log has nothing unclaimed
    /// for this group right now. `wait=0` polls without blocking, which spins
    /// if you loop on it — prefer a few seconds so the broker holds the
    /// request open instead.
    #[pyo3(signature = (tenant_id, namespace, stream, shard, group, *, max_records=32, wait=5.0, consumer=None, reclaim=false, visibility=None))]
    fn group_poll(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: f64,
        consumer: Option<String>,
        reclaim: bool,
        visibility: Option<f64>,
    ) -> PyResult<Vec<Py<GroupRecord>>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        let wait = non_negative_duration(wait, "wait")?;
        let options = felix_client::GroupPollOptions {
            member: consumer.map(|consumer| felix_client::GroupMember { consumer, reclaim }),
            wait,
            visibility: visibility
                .map(|seconds| positive_duration(seconds, "visibility"))
                .transpose()?,
        };
        let records = block_on(py, async move {
            inner
                .group_poll_with(
                    &tenant_id,
                    &namespace,
                    &stream,
                    shard,
                    &group,
                    max_records,
                    &options,
                )
                .await
                .map_err(to_py_err)
        })?;
        records
            .into_iter()
            .map(|record| {
                OwnedGroupRecord::from(record)
                    .into_pyobject(py)
                    .map(Bound::unbind)
            })
            .collect()
    }

    /// Finish one record. Everything below the group's cursor stays finished.
    fn group_ack(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_ack(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Hand one record back for immediate redelivery, rather than waiting out
    /// the visibility timeout. Refused once its claim has lapsed or the
    /// record has gone out again.
    fn group_nack(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
    ) -> PyResult<()> {
        let record = record.delivery();
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_nack(&tenant_id, &namespace, &stream, shard, &group, &record)
                .await
                .map_err(to_py_err)
        })
    }

    /// Offsets this group gave up on, lowest first.
    ///
    /// The records are still in the log at these offsets: this is a list of
    /// what to look at, not a copy of it.
    fn group_dead_letters(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<Vec<u64>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_dead_letters(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Drop one dead letter, having decided the record is not worth
    /// reprocessing. Does not touch the record itself.
    fn group_discard(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_discard(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Put one dead letter back in the queue with its attempt count reset.
    ///
    /// For when the reason it failed has been fixed. The group's cursor does
    /// not move backwards — the record is owed again, which is a different
    /// thing: everything the group finished stays finished.
    fn group_redrive(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_redrive(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Keep the claim on `record` standing for `extend` seconds from now.
    ///
    /// Returns how many seconds it now stands, which is less than asked when
    /// the broker capped it. Refused with `stale_claim` once the claim has
    /// lapsed or the record has gone out again, even to this consumer. Needs
    /// a broker that supports claim control.
    fn group_extend(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
        extend: f64,
    ) -> PyResult<f64> {
        let record = record.delivery();
        let extend = positive_duration(extend, "extend")?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        let stands = block_on(py, async move {
            inner
                .group_extend(
                    &tenant_id, &namespace, &stream, shard, &group, &record, extend,
                )
                .await
                .map_err(to_py_err)
        })?;
        Ok(stands.as_secs_f64())
    }

    /// Move a group's cursor on one shard to `start`: `"latest"`,
    /// `"earliest"` or an integer offset.
    ///
    /// Records the group had handed out are void and delivered again from the
    /// new position; dead letters are kept. Needs the `group.manage`
    /// permission.
    fn group_seek(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<GroupPosition> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_seek(&tenant_id, &namespace, &stream, shard, &group, start)
                .await
                .map(GroupPosition::from)
                .map_err(to_py_err)
        })
    }

    /// Create a group on one shard at `start`, so its first poll begins there.
    ///
    /// A group that already exists is left where it is and `moved` is false,
    /// so this is safe to call on every start. Needs the `group.manage`
    /// permission.
    fn group_create(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<GroupPosition> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_create(&tenant_id, &namespace, &stream, shard, &group, start)
                .await
                .map(GroupPosition::from)
                .map_err(to_py_err)
        })
    }

    /// Where a group stands on one shard.
    fn group_describe(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<GroupInfo> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_describe(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map(GroupInfo::from)
                .map_err(to_py_err)
        })
    }

    /// Delete a group on one shard: its cursor, its dead letters and the
    /// records it has handed out. Returns whether there was anything to
    /// delete. Needs the `group.manage` permission.
    fn group_delete(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> PyResult<bool> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_delete(&tenant_id, &namespace, &stream, shard, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// Give up on one record: list it as a dead letter of the group and
    /// finish it, as the group does once a record runs out of attempts.
    /// `group_redrive` puts it back. Refused for a record already finished.
    /// Needs a broker that supports claim control.
    fn group_dead_letter(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_dead_letter(&tenant_id, &namespace, &stream, shard, &group, offset)
                .await
                .map_err(to_py_err)
        })
    }

    /// Hand `record` back to be redelivered once `delay` seconds have passed
    /// rather than at once, for a consumer that backs off before retrying.
    ///
    /// A delay of 0 is `group_nack`. The broker caps the delay. Refused with
    /// `stale_claim` once the claim has lapsed or the record has gone out
    /// again. A nonzero delay needs a broker that supports claim control.
    fn group_nack_after(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        record: PyRef<'_, GroupRecord>,
        delay: f64,
    ) -> PyResult<()> {
        let delay = non_negative_duration(delay, "delay")?;
        let record = record.delivery();
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_nack_after(
                    &tenant_id, &namespace, &stream, shard, &group, &record, delay,
                )
                .await
                .map_err(to_py_err)
        })
    }

    /// `group_seek` on every shard of the stream, in shard order. Returns each
    /// shard's position, indexed by shard.
    ///
    /// One request per shard, so `"latest"` is each shard's tail when its own
    /// seek lands. An integer offset is refused for a stream with more than
    /// one shard, since offsets are per shard. A failure partway leaves the
    /// earlier shards moved; the call is safe to repeat. Needs the
    /// `group.manage` permission.
    fn group_seek_stream(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<Vec<GroupPosition>> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_seek_stream(&tenant_id, &namespace, &stream, &group, start)
                .await
                .map(|positions| positions.into_iter().map(GroupPosition::from).collect())
                .map_err(to_py_err)
        })
    }

    /// `group_create` on every shard of the stream, as `group_seek_stream`
    /// does. A shard where the group already exists is left alone.
    fn group_create_stream(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
        start: Py<PyAny>,
    ) -> PyResult<Vec<GroupPosition>> {
        let start = parse_required_start(py, start)?;
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_create_stream(&tenant_id, &namespace, &stream, &group, start)
                .await
                .map(|positions| positions.into_iter().map(GroupPosition::from).collect())
                .map_err(to_py_err)
        })
    }

    /// `group_describe` for every shard of the stream, indexed by shard.
    fn group_describe_stream(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> PyResult<Vec<GroupInfo>> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_describe_stream(&tenant_id, &namespace, &stream, &group)
                .await
                .map(|infos| infos.into_iter().map(GroupInfo::from).collect())
                .map_err(to_py_err)
        })
    }

    /// `group_delete` on every shard of the stream. Returns whether any shard
    /// had anything to delete. Safe to repeat after a failure partway. Needs
    /// the `group.manage` permission.
    fn group_delete_stream(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> PyResult<bool> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        block_on(py, async move {
            inner
                .group_delete_stream(&tenant_id, &namespace, &stream, &group)
                .await
                .map_err(to_py_err)
        })
    }

    /// One consumer group across every shard of a stream. See
    /// `ShardedGroupHandle`.
    fn group_sharded(
        &self,
        py: Python<'_>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> PyResult<ShardedGroupHandle> {
        let inner = Arc::clone(&self.inner);
        let (tenant_id, namespace, stream, group) = owned4(tenant_id, namespace, stream, group);
        let sharded = block_on(py, async move {
            inner
                .group_sharded(&tenant_id, &namespace, &stream, &group)
                .await
                .map_err(to_py_err)
        })?;
        Ok(ShardedGroupHandle::new(sharded))
    }
}
