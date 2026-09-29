//! The protocol suite: an in-process broker, checked twice — once with frames
//! built by hand over raw QUIC, once through `felix-client`.

mod checks;
mod commit;
mod faults;
mod fixture;
mod frames;
mod raw;
mod sdk;
mod shard_move;

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_broker::{Broker, CacheMetadata, DurableStorage};
use felix_broker_service::serving::quic;
use felix_storage::EphemeralCache;
use felix_storage::log::LogConfig;
use felix_transport::{QuicClient, QuicServer, TransportConfig};

use commit::{COMMIT_STREAM, run_client_commit, run_commit};
use faults::run_link_faults;
use fixture::{
    build_auth_fixture, build_client_config, build_quinn_client_config, build_server_config,
};
use raw::{run_cache, run_pubsub};
use sdk::{run_client_cache, run_client_pubsub};
use shard_move::{MOVED_STREAM, run_shard_move};

pub(crate) const MAX_TEST_FRAME_BYTES: usize = 64 * 1024;

pub(crate) async fn run_protocol_suite() -> Result<()> {
    println!("== Felix Conformance Runner ==");
    let auth = build_auth_fixture()?;
    // Durable storage for the fault scenarios, which need offsets to resume
    // from and to check for gaps.
    let storage_dir = tempfile::tempdir().context("create a storage directory")?;
    let storage = DurableStorage::open(storage_dir.path(), LogConfig::default())?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", CacheMetadata::default())
        .await?;
    broker
        .register_stream("t1", "default", "conformance", Default::default())
        .await?;
    broker
        .register_stream("t1", "default", MOVED_STREAM, Default::default())
        .await?;
    broker
        .register_stream(
            "t1",
            "default",
            COMMIT_STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                ..Default::default()
            },
        )
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = felix_broker_service::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
        Arc::clone(&auth.broker_auth),
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    run_pubsub(&connection, &auth).await?;
    run_cache(&connection, &auth).await?;
    run_client_pubsub(addr, cert.clone(), &auth).await?;
    run_client_cache(addr, cert.clone(), &auth).await?;
    run_commit(&connection, &auth).await?;
    run_client_commit(addr, cert.clone(), &auth).await?;
    run_shard_move(&connection, &auth, &broker).await?;
    run_link_faults(&broker, addr, build_client_config(cert, &auth)?).await?;

    drop(connection);
    server_task.abort();
    println!("Conformance checks passed.");
    Ok(())
}

#[cfg(test)]
mod tests;
