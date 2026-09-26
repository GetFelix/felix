//! Building a [`Client`]: the pools, the streams on them, and where they go.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use anyhow::{Context, Result};
use felix_transport::{QuicClient, QuicConnection, TransportConfig};
use tokio::sync::mpsc;
use tracing::debug;

use super::Client;
use crate::cache::{CacheWorker, run_cache_worker_with_limit};
use crate::config::{
    CACHE_WORKER_QUEUE_DEPTH, ClientConfig, ClientRuntimeConfig, cache_transport_config,
    event_transport_config,
};
use crate::connection::{
    Credentials, Negotiated, listener_targets, pool_target, spawn_conn_stats_logger,
    spawn_event_router_with_config,
};
use crate::publish::{PublishAdmission, PublishWorker, run_publisher_writer_with_limit};

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
        let runtime_config = client_config.runtime_config();
        let auth_tenant_id = client_config
            .auth_tenant_id
            .clone()
            .context("FELIX_AUTH_TENANT must be set")?;
        let credentials = Credentials::new(auth_tenant_id.clone(), client_config.tokens()?);
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("bind addr");
        let publish_client =
            QuicClient::bind(bind_addr, client_config.quinn.clone(), transport.clone())?;
        let publish_pool_size = client_config.publish_conn_pool;
        let publish_streams_per_conn = client_config.publish_streams_per_conn;
        if publish_pool_size == 0 || publish_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("publish pool misconfigured"));
        }
        let publish_chunk_bytes = client_config.publish_chunk_bytes;
        let publish_queue_depth = client_config.publish_queue_depth.max(1);
        let publish_admission =
            Arc::new(PublishAdmission::new(client_config.publish_inflight_bytes));
        // Learn the broker's listener set while building the first connection's
        // streams, rather than probing for it.
        //
        // A broker may bind several client-facing ports, each its own UDP
        // socket and so its own endpoint driver -- the single task that reads
        // every datagram for that socket. A pool that dials one port lands
        // entirely on one driver, which is the per-broker ceiling this exists
        // to lift.
        //
        // The answer rides the first stream's `AuthOk`, which has to be sent
        // anyway, so a single-listener deployment pays nothing for this.
        let mut publish_workers = Vec::with_capacity(publish_pool_size * publish_streams_per_conn);
        let first = publish_client.connect(addr, server_name).await?;
        debug!("client established publish connection");
        spawn_conn_stats_logger(&first, "publish");
        let negotiated = open_publish_streams(
            &first,
            publish_streams_per_conn,
            &credentials,
            &runtime_config,
            publish_queue_depth,
            publish_chunk_bytes,
            &mut publish_workers,
        )
        .await?;
        // Every publish stream negotiates with the same broker, so any stream's
        // answer is the broker's answer.
        let server_features = negotiated.server_features;
        let targets = listener_targets(addr, &negotiated.listener_ports);
        if targets.len() > 1 {
            debug!(
                listeners = targets.len(),
                "spreading pools across listeners"
            );
        }

        // Every distinct address the pools actually land on, for
        // `listeners_in_use`.
        let mut listeners: Vec<SocketAddr> = vec![addr];
        let mut publish_connections = vec![first];
        for index in 1..publish_pool_size {
            let target = pool_target(&targets, index, &mut listeners);
            let connection = publish_client.connect(target, server_name).await?;
            debug!("client established publish connection");
            spawn_conn_stats_logger(&connection, "publish");
            open_publish_streams(
                &connection,
                publish_streams_per_conn,
                &credentials,
                &runtime_config,
                publish_queue_depth,
                publish_chunk_bytes,
                &mut publish_workers,
            )
            .await?;
            publish_connections.push(connection);
        }
        // Held so the streams above keep their connections open.
        let _publish_connections = publish_connections;
        let publish_sharding = client_config.publish_sharding;
        // Cache connections are pooled to avoid head-of-line blocking.
        // DESIGN NOTE:
        // We pool *connections* and then open multiple *streams per connection*.
        // This avoids (a) creating a new QUIC connection per cache op and
        // (b) HOL blocking between independent cache ops on a single stream.
        let cache_pool_size = client_config.cache_conn_pool;
        let cache_transport = cache_transport_config(transport.clone(), &client_config);
        let cache_client =
            QuicClient::bind(bind_addr, client_config.quinn.clone(), cache_transport)?;
        let mut cache_connections = Vec::with_capacity(cache_pool_size);
        for index in 0..cache_pool_size {
            let target = pool_target(&targets, index, &mut listeners);
            let connection = cache_client.connect(target, server_name).await?;
            debug!("client established cache connection");
            cache_connections.push(connection);
        }
        // Each cache connection runs multiple independent bi-directional streams.
        let cache_streams_per_conn = client_config.cache_streams_per_conn;
        if cache_pool_size == 0 || cache_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("cache pool misconfigured"));
        }
        let mut cache_workers = Vec::with_capacity(cache_pool_size * cache_streams_per_conn);
        let mut cache_conn_counts = Vec::with_capacity(cache_pool_size);
        for _ in 0..cache_pool_size {
            cache_conn_counts.push(AtomicUsize::new(0));
        }
        let cache_conn_counts = Arc::new(cache_conn_counts);
        for (conn_index, connection) in cache_connections.iter().enumerate() {
            for _ in 0..cache_streams_per_conn {
                let (send, recv, _) = credentials
                    .open(connection, runtime_config.max_frame_bytes)
                    .await?;
                debug!(conn_index, "client cache stream authenticated");
                let (tx, rx) = mpsc::channel(CACHE_WORKER_QUEUE_DEPTH);
                tokio::spawn(run_cache_worker_with_limit(
                    conn_index,
                    send,
                    recv,
                    rx,
                    Arc::clone(&cache_conn_counts),
                    runtime_config.max_frame_bytes,
                ));
                cache_workers.push(CacheWorker { tx, conn_index });
            }
        }
        // Event connections are reserved for subscription streams.
        let event_pool_size = client_config.event_conn_pool;
        let event_transport = event_transport_config(transport, &client_config);
        let event_client = QuicClient::bind(bind_addr, client_config.quinn, event_transport)?;
        let mut event_connections = Vec::with_capacity(event_pool_size);
        for index in 0..event_pool_size {
            let target = pool_target(&targets, index, &mut listeners);
            let connection = event_client.connect(target, server_name).await?;
            debug!("client established event connection");
            // An event connection may sit idle until the first subscribe, and
            // a broker closes a connection that has authenticated nothing
            // within its auth timeout.
            credentials.announce(&connection).await?;
            event_connections.push(connection);
        }
        let mut event_stream_routers = Vec::with_capacity(event_pool_size);
        for connection in &event_connections {
            event_stream_routers.push(spawn_event_router_with_config(
                connection.clone(),
                runtime_config.event_router_max_pending,
                runtime_config.max_frame_bytes,
            ));
        }
        let mut event_conn_counts = Vec::with_capacity(event_pool_size);
        for _ in 0..event_pool_size {
            event_conn_counts.push(AtomicUsize::new(0));
        }
        Ok(Self {
            listeners,
            server_features,
            _publish_client: publish_client,
            _cache_client: cache_client,
            _event_client: event_client,
            publish_workers: Arc::new(publish_workers),
            publish_stream_hasher: ahash::RandomState::new(),
            publish_sharding,
            publish_admission,
            cache_workers,
            event_connections,
            subscription_counter: AtomicU64::new(1),
            cache_request_counter: AtomicU64::new(1),
            event_pool_size,
            cache_worker_rr: AtomicUsize::new(0),
            cache_conn_counts,
            event_stream_routers,
            event_conn_counts: Arc::new(event_conn_counts),
            auth_tenant_id,
            credentials,
            runtime_config,
        })
    }

    /// The distinct broker addresses this client's pooled connections are on.
    ///
    /// More than one means the broker advertised several listeners and the
    /// pools were spread across them, which is what keeps a client off a single
    /// endpoint driver. One means a single-listener broker, an older one, or a
    /// pool too small to spread.
    ///
    /// In discovery order (the dialled address first, then each new target the
    /// first pool spread reaches it), not sorted -- a caller wanting a set
    /// rather than an order should sort it.
    pub fn listeners_in_use(&self) -> &[SocketAddr] {
        &self.listeners
    }
}

/// Open and authenticate one connection's publish streams, pushing a worker for
/// each, and return what the broker said during negotiation.
///
/// Every stream negotiates with the same broker, so the answer is the same for
/// all of them; the caller keeps the first, which is what reports the listener
/// set before the rest of the pool is placed.
#[allow(clippy::too_many_arguments)]
async fn open_publish_streams(
    connection: &QuicConnection,
    streams_per_conn: usize,
    credentials: &Credentials,
    runtime_config: &ClientRuntimeConfig,
    publish_queue_depth: usize,
    publish_chunk_bytes: usize,
    workers: &mut Vec<PublishWorker>,
) -> Result<Negotiated> {
    let mut last = None;
    for _ in 0..streams_per_conn {
        let (send, recv, negotiated) = credentials
            .open(connection, runtime_config.max_frame_bytes)
            .await?;
        debug!("client opened publish stream");
        let server_flags = negotiated.server_flags;
        debug!(server_flags, "client publish stream authenticated");
        let (tx, rx) = mpsc::channel(publish_queue_depth);
        // Not colocated with the transport drivers (unlike the subscription
        // read pump): publisher writers block in `write_all` against a full
        // send window, and parking them on the I/O thread starves the drivers
        // they wait on (measured 5x throughput loss).
        let handle = tokio::spawn(run_publisher_writer_with_limit(
            send,
            recv,
            rx,
            publish_chunk_bytes,
            runtime_config.max_frame_bytes,
        ));
        workers.push(PublishWorker {
            tx,
            handle: tokio::sync::Mutex::new(Some(handle)),
            request_counter: AtomicU64::new(1),
            server_flags,
        });
        last = Some(negotiated);
    }
    last.context("publish pool misconfigured: no streams per connection")
}
