//! Connecting to the cluster under test.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use felix_client::{Client, ClientConfig, ClusterClient};

use super::Common;

pub(super) async fn cluster(common: &Common) -> Result<ClusterClient> {
    let config = client_config(common)?;
    ClusterClient::connect(&common.brokers, &common.server_name, config)
        .await
        .context("connect a cluster client")
}

pub(super) async fn client(common: &Common, addr: SocketAddr) -> Result<Client> {
    let config = client_config(common)?;
    Client::connect(addr, &common.server_name, config)
        .await
        .with_context(|| format!("connect to {addr}"))
}

fn client_config(common: &Common) -> Result<ClientConfig> {
    match &common.client_config {
        Some(config) => Ok(config.clone()),
        None => crate::tls::client_config(&common.tenant, &common.token),
    }
}
