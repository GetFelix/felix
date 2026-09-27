//! Writer-lane data model: lane/connection commands, the lane manager, and its routing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::mpsc;

use crate::observability::timings;
use crate::serving::quic::handlers::subscribe::conn_counts::{
    connection_subscriber_unregister, hash64,
};
use crate::serving::quic::handlers::subscribe::writer::{
    ConnectionWriterConfig, run_connection_writer, run_writer_lane,
};
use crate::serving::quic::telemetry::{t_counter, t_histogram};

#[derive(Debug)]
pub(super) struct LaneSubscriber {
    pub(super) event_send: quinn::SendStream,
    // Keep the connection alive as long as this subscriber is registered.
    pub(super) _connection: felix_transport::QuicConnection,
    pub(super) _connection_id: Option<u64>,
    pub(super) _unsubscribe_guard: felix_broker::SubscriptionGuard,
}

#[derive(Debug)]
pub(super) enum ConnectionCommand {
    Register {
        subscriber_id: u64,
        connection: felix_transport::QuicConnection,
        connection_id: Option<u64>,
        event_send: quinn::SendStream,
        guard: felix_broker::SubscriptionGuard,
    },
    Delivery {
        subscriber_id: u64,
        frame: Bytes,
        item_count: usize,
        first_enqueued_at: Instant,
        enqueue_at: Instant,
    },
    Unregister {
        subscriber_id: u64,
        /// A frame to write after everything queued and before letting the
        /// stream go: `shard_moved`, for a subscription whose shard moved.
        last: Option<Bytes>,
    },
}

#[derive(Debug)]
pub(crate) enum LaneCommand {
    Register {
        subscriber_id: u64,
        connection_id: Option<u64>,
        connection: felix_transport::QuicConnection,
        event_send: quinn::SendStream,
        guard: felix_broker::SubscriptionGuard,
    },
    Delivery {
        subscriber_id: u64,
        frame: Bytes,
        item_count: usize,
        first_enqueued_at: Instant,
        enqueue_at: Instant,
    },
    Unregister {
        subscriber_id: u64,
        // Carried explicitly rather than looked up in `subscriber_connections`.
        // Teardown enqueues this command and then immediately calls
        // `unregister_subscriber`, which removes that entry -- so by the time the
        // lane worker dequeues, the lookup would already miss and the cleanup
        // would be skipped entirely.
        connection_id: Option<u64>,
        /// The subscription's last frame, if it has one. Carried here rather
        /// than as a delivery so it is never dropped: control commands are
        /// sent with backpressure, deliveries are not.
        last: Option<Bytes>,
    },
}

/// One instance per QUIC connection (see `handle_connection`'s `PublishContext`
/// construction). Previously this was cached in a process-wide static keyed on
/// `connection.info().id.0` (QUIC's `stable_id()`), which is only unique *within one
/// QUIC endpoint* — across independently created endpoints (e.g. one per test) it can
/// collide, silently sharing lane state (and its background tasks) between unrelated
/// connections, and the cache never evicted entries, leaking a manager + its spawned
/// tasks per historical connection for the life of the process. Constructing a fresh
/// instance per connection and letting it drop with the connection avoids both.
#[derive(Debug)]
pub(crate) struct WriterLaneManager {
    pub(super) lanes: Vec<mpsc::Sender<LaneCommand>>,
    pub(super) lane_queue_capacity: usize,
    pub(super) lane_queue_policy: felix_broker::SubQueuePolicy,
    pub(super) lane_queue_highwater: Vec<AtomicUsize>,
    pub(super) connection_writers: DashMap<u64, mpsc::Sender<ConnectionCommand>>,
    pub(super) subscriber_connections: DashMap<u64, u64>,
    pub(super) connection_queue_capacity: usize,
    pub(super) max_bytes_per_write: usize,
    pub(super) connection_lanes: DashMap<u64, usize>,
    pub(super) subscriber_pins: DashMap<u64, usize>,
    pub(super) shard: crate::config::SubscriberLaneShard,
    pub(super) single_writer_per_conn: bool,
    pub(super) rr_counter: AtomicUsize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LaneRuntimeConfig {
    pub(super) flush_max_items: usize,
}

#[derive(Debug)]
pub(super) struct LaneDelivery {
    pub(super) subscriber_id: u64,
    pub(super) frame: Bytes,
    pub(super) item_count: usize,
    pub(super) first_enqueued_at: Instant,
    pub(super) enqueue_at: Instant,
}

impl WriterLaneManager {
    pub(crate) fn new(config: &crate::config::BrokerConfig) -> Arc<Self> {
        let requested_lanes = config.subscriber_writer_lanes.max(1);
        let max_lanes = config.max_subscriber_writer_lanes.max(1);
        let lane_count = requested_lanes.min(max_lanes);
        if requested_lanes > max_lanes {
            tracing::warn!(
                requested_lanes,
                max_lanes,
                "subscriber_writer_lanes exceeds max_subscriber_writer_lanes; clamping"
            );
        }
        let queue_depth = config.subscriber_lane_queue_depth.max(1);
        let lane_cfg = LaneRuntimeConfig {
            flush_max_items: config.subscriber_flush_max_items.max(1),
        };
        let mut lanes = Vec::with_capacity(lane_count);
        let mut lane_receivers = Vec::with_capacity(lane_count);
        for _lane_id in 0..lane_count {
            let (tx, rx) = mpsc::channel(queue_depth);
            lanes.push(tx);
            lane_receivers.push(rx);
        }
        let mut lane_queue_highwater = Vec::with_capacity(lane_count);
        for _ in 0..lane_count {
            lane_queue_highwater.push(AtomicUsize::new(0));
        }
        let manager = Self {
            lanes,
            lane_queue_capacity: queue_depth,
            lane_queue_policy: config.subscriber_lane_queue_policy,
            lane_queue_highwater,
            connection_writers: DashMap::new(),
            subscriber_connections: DashMap::new(),
            connection_queue_capacity: queue_depth,
            max_bytes_per_write: config.subscriber_max_bytes_per_write.max(1),
            connection_lanes: DashMap::new(),
            subscriber_pins: DashMap::new(),
            shard: config.subscriber_lane_shard,
            single_writer_per_conn: config.subscriber_single_writer_per_conn,
            rr_counter: AtomicUsize::new(0),
        };
        let manager = Arc::new(manager);
        let weak_manager = Arc::downgrade(&manager);
        for (lane_id, rx) in lane_receivers.into_iter().enumerate() {
            tokio::spawn(run_writer_lane(
                lane_id,
                rx,
                lane_cfg,
                Weak::clone(&weak_manager),
            ));
        }
        manager
    }

    pub(super) fn lane_for_subscriber(&self, subscriber_id: u64) -> usize {
        let lanes = self.lanes.len().max(1);
        hash64(subscriber_id) as usize % lanes
    }

    pub(super) fn lane_for_connection(&self, connection_id: u64) -> usize {
        let lanes = self.lanes.len().max(1);
        *self
            .connection_lanes
            .entry(connection_id)
            .or_insert_with(|| hash64(connection_id) as usize % lanes)
    }

    pub(crate) fn select_lane(&self, subscriber_id: u64, connection_id: Option<u64>) -> usize {
        if self.single_writer_per_conn
            && let Some(connection_id) = connection_id
        {
            return self.lane_for_connection(connection_id);
        }
        // Design intent:
        // - `Auto` defaults to connection ownership to avoid cross-lane contention on shared QUIC
        //   send state when multiple subscribers share one connection.
        // - `SubscriberIdHash` maximizes distribution independent of connection topology.
        // - `RoundRobinPin` assigns once at subscribe time (not per message) so ordering is stable.
        match self.shard {
            crate::config::SubscriberLaneShard::Auto => self.lane_for_subscriber(subscriber_id),
            crate::config::SubscriberLaneShard::SubscriberIdHash => {
                self.lane_for_subscriber(subscriber_id)
            }
            crate::config::SubscriberLaneShard::ConnectionIdHash => match connection_id {
                Some(connection_id) => self.lane_for_connection(connection_id),
                None => self.lane_for_subscriber(subscriber_id),
            },
            crate::config::SubscriberLaneShard::RoundRobinPin => *self
                .subscriber_pins
                .entry(subscriber_id)
                .or_insert_with(|| {
                    self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.lanes.len().max(1)
                }),
        }
    }

    pub(crate) fn unregister_subscriber(&self, subscriber_id: u64, connection_id: Option<u64>) {
        self.subscriber_pins.remove(&subscriber_id);
        self.subscriber_connections.remove(&subscriber_id);
        if let Some(connection_id) = connection_id {
            self.connection_lanes.remove(&connection_id);
        }
    }

    pub(super) fn ensure_connection_writer(
        &self,
        connection_id: u64,
        connection: Option<&felix_transport::QuicConnection>,
    ) -> mpsc::Sender<ConnectionCommand> {
        match self.connection_writers.entry(connection_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(self.connection_queue_capacity.max(1));
                entry.insert(tx.clone());
                let writer = run_connection_writer(
                    connection_id,
                    rx,
                    ConnectionWriterConfig {
                        max_bytes_per_write: self.max_bytes_per_write,
                        max_queued_per_subscriber: self.connection_queue_capacity,
                        block: matches!(
                            self.lane_queue_policy,
                            felix_broker::SubQueuePolicy::Block
                        ),
                    },
                );
                // Colocate with the connection's transport drivers (`spawn_pump`)
                // so per-write wakeups stay on-thread. Register always creates
                // the writer; the `None` arm only covers a delivery racing
                // ahead of its registration.
                match connection {
                    Some(connection) => {
                        connection.spawn_pump(writer);
                    }
                    None => {
                        tokio::spawn(writer);
                    }
                }
                tx
            }
        }
    }

    pub(super) async fn enqueue_connection(
        &self,
        connection_id: u64,
        cmd: ConnectionCommand,
    ) -> std::result::Result<(), ()> {
        let connection = match &cmd {
            ConnectionCommand::Register { connection, .. } => Some(connection.clone()),
            _ => None,
        };
        let sender = self.ensure_connection_writer(connection_id, connection.as_ref());
        let wait_start = Instant::now();
        let is_control = matches!(
            &cmd,
            ConnectionCommand::Register { .. } | ConnectionCommand::Unregister { .. }
        );
        let result = match (self.lane_queue_policy, is_control) {
            (_, true) | (felix_broker::SubQueuePolicy::Block, false) => {
                sender.send(cmd).await.map_err(|_| ())
            }
            (felix_broker::SubQueuePolicy::DropNew, false)
            | (felix_broker::SubQueuePolicy::DropOld, false) => {
                sender.try_send(cmd).map_err(|_| ())
            }
        };
        let wait_ns = wait_start.elapsed().as_nanos() as u64;
        timings::record_sub_queue_wait_ns(wait_ns);
        t_histogram!("felix_broker_sub_queue_wait_ns").record(wait_ns as f64);
        t_histogram!("broker_sub_conn_enqueue_wait_ns", "connection_id" => connection_id.to_string())
            .record(wait_ns as f64);
        result
    }

    pub(super) fn update_lane_queue_highwater(&self, lane_idx: usize) {
        let sender = &self.lanes[lane_idx];
        let queue_len = self.lane_queue_capacity.saturating_sub(sender.capacity());
        loop {
            let current = self.lane_queue_highwater[lane_idx].load(Ordering::Relaxed);
            if queue_len <= current {
                break;
            }
            if self.lane_queue_highwater[lane_idx]
                .compare_exchange(current, queue_len, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                t_counter!(
                    "broker_sub_lane_queue_len_highwater",
                    "lane" => lane_idx.to_string()
                )
                .increment((queue_len - current) as u64);
                break;
            }
        }
    }

    pub(crate) async fn enqueue(
        &self,
        lane_idx: usize,
        cmd: LaneCommand,
    ) -> std::result::Result<(), ()> {
        let lane = lane_idx.to_string();
        let sender = &self.lanes[lane_idx];
        let enqueue_wait_start = Instant::now();
        let is_control = matches!(
            &cmd,
            LaneCommand::Register { .. } | LaneCommand::Unregister { .. }
        );
        let result = match (self.lane_queue_policy, is_control) {
            (_, true) | (felix_broker::SubQueuePolicy::Block, false) => {
                sender.send(cmd).await.map_err(|_| ())
            }
            (felix_broker::SubQueuePolicy::DropNew, false) => sender.try_send(cmd).map_err(|err| {
                if matches!(err, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                    t_counter!("broker_sub_lane_queue_full_total", "lane" => lane.clone())
                        .increment(1);
                }
            }),
            (felix_broker::SubQueuePolicy::DropOld, false) => sender.try_send(cmd).map_err(|err| {
                if matches!(err, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                    t_counter!("broker_sub_lane_queue_full_total", "lane" => lane.clone())
                        .increment(1);
                    t_counter!(
                        "broker_sub_lane_drop_old_emulated_total",
                        "lane" => lane.clone()
                    )
                    .increment(1);
                }
            }),
        };
        let enqueue_wait_ns = enqueue_wait_start.elapsed().as_nanos() as u64;
        t_histogram!("broker_sub_lane_enqueue_block_ns", "lane" => lane.clone())
            .record(enqueue_wait_ns as f64);
        match result {
            Ok(()) => {
                t_counter!("broker_sub_lane_enqueued_total", "lane" => lane.clone()).increment(1);
                self.update_lane_queue_highwater(lane_idx);
                let queue_len = self.lane_queue_capacity.saturating_sub(sender.capacity());
                metrics::gauge!("felix_sub_lane_queue_len", "lane" => lane).set(queue_len as f64);
                Ok(())
            }
            Err(()) => {
                t_counter!("broker_sub_lane_dropped_total", "lane" => lane).increment(1);
                Err(())
            }
        }
    }
}

impl Drop for WriterLaneManager {
    fn drop(&mut self) {
        for entry in &self.subscriber_connections {
            connection_subscriber_unregister(Some(*entry.value()));
        }
    }
}
