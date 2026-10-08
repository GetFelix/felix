//! Building a [`Client`]: the connections, the streams on them, and where
//! they go.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use anyhow::{Context, Result};
use felix_transport::{QuicClient, QuicConnection, TransportConfig};
use tokio::sync::mpsc;
use tracing::debug;

use super::Client;
use super::discovery::ask_stream_routing;
use crate::cache::{CacheWorker, run_cache_worker_with_limit};
use crate::config::{
    CACHE_WORKER_QUEUE_DEPTH, ClientConfig, ClientRuntimeConfig, cache_transport_config,
    event_transport_config, shared_transport_config,
};
use crate::connection::{Credentials, NodeConnections, NodeLimits, OpenedStream};
use crate::publish::{
    LearnWidth, OpenWorker, PublishAdmission, PublishWorker, ShardStreams, StreamWidths,
    run_publisher_writer_with_limit,
};

impl Client {
    /// Connect to the broker at `addr` with default transport settings.
    ///
    /// Every pool is built before this returns. `client_config` must name a
    /// tenant and a token or token provider.
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        client_config: ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_transport(addr, server_name, client_config, TransportConfig::default())
            .await
    }

    /// Connect to whichever of `addrs` answers first, in order.
    ///
    /// A cluster has more than one broker and any of them will serve a publish —
    /// one that does not own the shard forwards it. So a client given a single
    /// address has a single point of failure that the cluster itself does not:
    /// the broker it was pointed at can be the one that just died, and every
    /// other broker is sitting there able to serve.
    ///
    /// Tried in the order given, so a caller can express preference. The errors
    /// are collected rather than discarded: "nothing answered" is the only
    /// outcome worth reporting, but *why* each endpoint refused is what an
    /// operator needs, and a bare "connection refused" from the last one in the
    /// list hides the credential error from the first.
    ///
    /// This picks a starting broker and nothing more: the client does not learn
    /// the rest of the cluster from it, and does not move if it later dies.
    /// [`crate::ClusterClient`] does both.
    pub async fn connect_any(
        addrs: &[SocketAddr],
        server_name: &str,
        client_config: ClientConfig,
    ) -> Result<Self> {
        Self::connect_any_with_transport(
            addrs,
            server_name,
            client_config,
            TransportConfig::default(),
        )
        .await
    }

    /// [`Client::connect_any`] with an explicit transport configuration.
    pub async fn connect_any_with_transport(
        addrs: &[SocketAddr],
        server_name: &str,
        client_config: ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        if addrs.is_empty() {
            return Err(anyhow::anyhow!(
                "no broker addresses were given to connect to"
            ));
        }
        let mut refusals = Vec::with_capacity(addrs.len());
        for addr in addrs {
            match Self::connect_with_transport(
                *addr,
                server_name,
                client_config.clone(),
                transport.clone(),
            )
            .await
            {
                Ok(client) => return Ok(client),
                Err(err) => refusals.push(format!("{addr}: {err:#}")),
            }
        }
        Err(anyhow::anyhow!(
            "no broker answered ({})",
            refusals.join("; ")
        ))
    }

    /// [`Client::connect`] with an explicit transport configuration.
    pub async fn connect_with_transport(
        addr: SocketAddr,
        server_name: &str,
        client_config: ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        Self::build(addr, server_name, client_config, transport, Layout::Pooled).await
    }

    /// A client whose publish, cache and event streams all share one
    /// connection, with more opened only when it is saturated, up to
    /// `cluster_conn_pool`. What a [`crate::ClusterClient`] holds per broker.
    pub(crate) async fn connect_shared(
        addr: SocketAddr,
        server_name: &str,
        client_config: ClientConfig,
    ) -> Result<Self> {
        Self::build(
            addr,
            server_name,
            client_config,
            TransportConfig::default(),
            Layout::Shared,
        )
        .await
    }

    async fn build(
        addr: SocketAddr,
        server_name: &str,
        client_config: ClientConfig,
        transport: TransportConfig,
        layout: Layout,
    ) -> Result<Self> {
        let runtime_config = client_config.runtime_config();
        let auth_tenant_id = client_config
            .auth_tenant_id
            .clone()
            .context("FELIX_AUTH_TENANT must be set")?;
        let credentials = Arc::new(
            Credentials::new(auth_tenant_id.clone(), client_config.tokens()?)
                .with_ack_on_commit(client_config.ack_on_commit)
                .with_publishers(client_config.publishers)
                .with_timestamps(client_config.timestamps),
        );
        let publish_pool_size = client_config.publish_conn_pool;
        let publish_streams_per_conn = client_config.publish_streams_per_conn;
        if publish_pool_size == 0 || publish_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("publish pool misconfigured"));
        }
        let cache_pool_size = client_config.cache_conn_pool;
        let cache_streams_per_conn = client_config.cache_streams_per_conn;
        if cache_pool_size == 0 || cache_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("cache pool misconfigured"));
        }
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("bind addr");
        let limits = |role, ceiling, streams_per_conn| NodeLimits {
            role,
            ceiling,
            streams_per_conn,
            max_frame_bytes: runtime_config.max_frame_bytes,
            event_router_max_pending: runtime_config.event_router_max_pending,
        };
        let node = |endpoint, limits| {
            Arc::new(NodeConnections::new(
                endpoint,
                addr,
                server_name,
                Arc::clone(&credentials),
                limits,
            ))
        };
        let max_streams = usize::from(transport.max_streams);
        let (publish_node, cache_node, event_node) = match layout {
            Layout::Pooled => {
                let publish =
                    QuicClient::bind(bind_addr, client_config.quinn.clone(), transport.clone())?;
                let cache = QuicClient::bind(
                    bind_addr,
                    client_config.quinn.clone(),
                    cache_transport_config(transport.clone(), &client_config),
                )?;
                let event = QuicClient::bind(
                    bind_addr,
                    client_config.quinn.clone(),
                    event_transport_config(transport, &client_config),
                )?;
                (
                    node(publish, limits("publish", publish_pool_size, max_streams)),
                    node(cache, limits("cache", cache_pool_size, max_streams)),
                    node(
                        event,
                        limits("event", client_config.event_conn_pool, max_streams),
                    ),
                )
            }
            Layout::Shared => {
                let endpoint = QuicClient::bind(
                    bind_addr,
                    client_config.quinn.clone(),
                    shared_transport_config(transport, &client_config),
                )?;
                let shared = node(
                    endpoint,
                    limits(
                        "shared",
                        client_config.cluster_conn_pool,
                        client_config.cluster_streams_per_conn,
                    ),
                );
                (Arc::clone(&shared), Arc::clone(&shared), shared)
            }
        };
        let nodes = [&publish_node, &cache_node, &event_node];

        let publish_chunk_bytes = client_config.publish_chunk_bytes;
        let publish_queue_depth = client_config.publish_queue_depth.max(1);
        let publish_admission =
            Arc::new(PublishAdmission::new(client_config.publish_inflight_bytes));
        let publish_stream_count = publish_pool_size * publish_streams_per_conn;
        let mut publish_workers = Vec::with_capacity(publish_stream_count);
        let mut worker_connections: Vec<QuicConnection> = Vec::new();

        // The first stream's `AuthOk` names the broker's listener ports, so it
        // is opened before any other connection is placed.
        //
        // A broker may bind several client-facing ports, each its own UDP
        // socket and so its own endpoint driver -- the single task that reads
        // every datagram for that socket. Connections that all dial one port
        // land entirely on one driver, which is the per-broker ceiling this
        // exists to lift.
        let first = publish_node.open().await?;
        debug!("client publish stream authenticated");
        let negotiated = first.negotiated.clone();
        // Every stream negotiates with the same broker, so any stream's answer
        // is the broker's answer.
        let server_features = negotiated.server_features;
        for node in dedup(&nodes) {
            node.learn_listeners(addr, &negotiated.listener_ports);
        }
        note_connection(&mut worker_connections, first.lease.connection());
        publish_workers.push(spawn_publish_worker(
            first,
            &runtime_config,
            publish_queue_depth,
            publish_chunk_bytes,
        ));
        if layout == Layout::Pooled {
            publish_node.fill(publish_pool_size, false).await?;
        }
        for _ in 1..publish_stream_count {
            let opened = publish_node.open().await?;
            debug!("client publish stream authenticated");
            note_connection(&mut worker_connections, opened.lease.connection());
            publish_workers.push(spawn_publish_worker(
                opened,
                &runtime_config,
                publish_queue_depth,
                publish_chunk_bytes,
            ));
        }
        // A client that has lost a pooled stream's connection is finished
        // (`is_usable`), so it does not open shard streams on a fresh one: the
        // caller replaces the client instead.
        let open_shard_stream: OpenWorker = {
            let node = Arc::clone(&publish_node);
            let pooled = worker_connections.clone();
            Arc::new(move || {
                let node = Arc::clone(&node);
                let lost = pooled
                    .iter()
                    .any(|connection| connection.close_reason().is_some());
                Box::pin(async move {
                    anyhow::ensure!(!lost, "the client's connection to the broker was lost");
                    let opened = node.open().await?;
                    debug!("client shard publish stream authenticated");
                    Ok(spawn_publish_worker(
                        opened,
                        &runtime_config,
                        publish_queue_depth,
                        publish_chunk_bytes,
                    ))
                })
            })
        };
        let publish_shard_streams = Arc::new(ShardStreams::new(
            client_config.publish_shard_streams,
            open_shard_stream,
        ));
        let cache_request_counter = Arc::new(AtomicU64::new(1));
        let learn_width: LearnWidth = {
            let node = Arc::clone(&event_node);
            let requests = Arc::clone(&cache_request_counter);
            let tenant = auth_tenant_id.clone();
            let max_frame_bytes = runtime_config.max_frame_bytes;
            Arc::new(move |tenant_id, namespace, stream| {
                let node = Arc::clone(&node);
                let request_id = requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let tenant = tenant.clone();
                Box::pin(async move {
                    // The checks `Client::stream_routing` makes before asking.
                    anyhow::ensure!(
                        felix_wire::supports_feature(
                            server_features,
                            felix_wire::FEATURE_STREAM_SHARDS
                        ),
                        "broker does not report stream shard counts"
                    );
                    anyhow::ensure!(tenant_id == tenant, "tenant mismatch");
                    ask_stream_routing(
                        &node,
                        request_id,
                        max_frame_bytes,
                        &tenant_id,
                        &namespace,
                        &stream,
                    )
                    .await
                })
            })
        };
        let publish_widths = Arc::new(StreamWidths::new(learn_width));

        // Several streams per cache connection: a new connection per cache op
        // would pay a handshake each time, and one stream would head-of-line
        // block independent ops behind each other.
        if layout == Layout::Pooled {
            cache_node.fill(cache_pool_size, false).await?;
        }
        let cache_slots = match layout {
            Layout::Pooled => cache_pool_size,
            Layout::Shared => client_config.cluster_conn_pool,
        };
        let cache_conn_counts: Arc<Vec<AtomicUsize>> = Arc::new(
            (0..cache_slots.max(1))
                .map(|_| AtomicUsize::new(0))
                .collect(),
        );
        let mut cache_workers = Vec::with_capacity(cache_pool_size * cache_streams_per_conn);
        for _ in 0..cache_pool_size * cache_streams_per_conn {
            let OpenedStream {
                send, recv, lease, ..
            } = cache_node.open().await?;
            let conn_index = lease.slot();
            debug!(conn_index, "client cache stream authenticated");
            note_connection(&mut worker_connections, lease.connection());
            let (tx, rx) = mpsc::channel(CACHE_WORKER_QUEUE_DEPTH);
            let counts = Arc::clone(&cache_conn_counts);
            let max_frame_bytes = runtime_config.max_frame_bytes;
            tokio::spawn(async move {
                let _lease = lease;
                run_cache_worker_with_limit(conn_index, send, recv, rx, counts, max_frame_bytes)
                    .await
            });
            cache_workers.push(CacheWorker { tx, conn_index });
        }

        // Event connections may sit idle until the first subscribe, and a
        // broker closes a connection that has authenticated nothing within its
        // auth timeout, so they announce themselves.
        let event_slots = match layout {
            Layout::Pooled => {
                event_node.fill(client_config.event_conn_pool, true).await?;
                client_config.event_conn_pool
            }
            Layout::Shared => client_config.cluster_conn_pool,
        };
        let event_conn_counts = Arc::new(
            (0..event_slots.max(1))
                .map(|_| AtomicUsize::new(0))
                .collect(),
        );
        Ok(Self {
            dialled: addr,
            publish_node,
            cache_node,
            event_node,
            worker_connections,
            server_features,
            publish_workers: Arc::new(publish_workers),
            publish_stream_hasher: ahash::RandomState::new(),
            publish_shard_streams,
            publish_widths,
            publish_sharding: client_config.publish_sharding,
            publish_admission,
            cache_workers,
            cache_request_counter,
            cache_worker_rr: AtomicUsize::new(0),
            cache_conn_counts,
            event_conn_counts,
            auth_tenant_id,
            runtime_config,
        })
    }

    /// The distinct broker addresses this client's connections are on.
    ///
    /// More than one means the broker advertised several listeners and the
    /// connections were spread across them, which is what keeps a client off
    /// a single endpoint driver. One means a single-listener broker, an older
    /// one, or too few connections to spread.
    ///
    /// In the order they were first dialled, not sorted -- a caller wanting a
    /// set rather than an order should sort it.
    pub fn listeners_in_use(&self) -> Vec<SocketAddr> {
        let mut listeners = Vec::new();
        for node in dedup(&[&self.publish_node, &self.cache_node, &self.event_node]) {
            for listener in node.listeners() {
                if !listeners.contains(&listener) {
                    listeners.push(listener);
                }
            }
        }
        listeners
    }

    /// Live QUIC connections this client holds to its broker.
    pub fn connection_count(&self) -> usize {
        dedup(&[&self.publish_node, &self.cache_node, &self.event_node])
            .iter()
            .map(|node| node.connection_count())
            .sum()
    }

    /// The address this client was built for.
    pub(crate) fn dialled(&self) -> SocketAddr {
        self.dialled
    }

    /// Whether this client can still publish and serve cache requests.
    ///
    /// Its worker streams are opened once, so a client that has lost the
    /// connection under them, or every publish writer, is finished; the
    /// event side would open new streams, but a caller wanting a broker needs
    /// both.
    pub(crate) fn is_usable(&self) -> bool {
        self.worker_connections
            .iter()
            .all(|connection| connection.close_reason().is_none())
            && self
                .publish_workers
                .iter()
                .any(|worker| !worker.tx.is_closed())
    }

    /// Open an authenticated stream for a request of its own, or a
    /// subscription, on the event connections.
    pub(crate) async fn open_event_stream(&self) -> Result<OpenedStream> {
        self.event_node.open().await
    }
}

/// How a client lays its streams over connections.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// A pool per kind of traffic, every connection opened up front.
    Pooled,
    /// One set for everything, starting from a single connection.
    Shared,
}

/// Start the writer for one publish stream. The writer holds the stream's
/// lease, so the connection's count drops when the writer exits.
fn spawn_publish_worker(
    opened: OpenedStream,
    runtime_config: &ClientRuntimeConfig,
    publish_queue_depth: usize,
    publish_chunk_bytes: usize,
) -> PublishWorker {
    let OpenedStream {
        send,
        recv,
        negotiated,
        lease,
    } = opened;
    let (tx, rx) = mpsc::channel(publish_queue_depth);
    let max_frame_bytes = runtime_config.max_frame_bytes;
    let window = lease.publish_window(&negotiated);
    // Not colocated with the transport drivers (unlike the subscription read
    // pump): publisher writers block in `write_all` against a full send
    // window, and parking them on the I/O thread starves the drivers they
    // wait on (measured 5x throughput loss).
    let handle = tokio::spawn(async move {
        let _lease = lease;
        run_publisher_writer_with_limit(
            send,
            recv,
            rx,
            publish_chunk_bytes,
            max_frame_bytes,
            window,
        )
        .await
    });
    PublishWorker {
        tx,
        handle: tokio::sync::Mutex::new(Some(handle)),
        request_counter: AtomicU64::new(1),
        server_flags: negotiated.server_flags,
        publish_window: negotiated.publish_window,
    }
}

fn note_connection(connections: &mut Vec<QuicConnection>, connection: &QuicConnection) {
    if !connections
        .iter()
        .any(|known| known.info().id == connection.info().id)
    {
        connections.push(connection.clone());
    }
}

/// The distinct sets among `nodes`: a shared client has one set three times.
fn dedup<'a>(nodes: &[&'a Arc<NodeConnections>]) -> Vec<&'a Arc<NodeConnections>> {
    let mut distinct: Vec<&Arc<NodeConnections>> = Vec::new();
    for node in nodes {
        if !distinct.iter().any(|seen| Arc::ptr_eq(seen, node)) {
            distinct.push(node);
        }
    }
    distinct
}
