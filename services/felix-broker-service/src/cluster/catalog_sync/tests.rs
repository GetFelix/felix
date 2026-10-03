//! Tests cover error tolerance (skipping on fetch errors), deletion propagation, and cursor advancement.

mod apply;
mod consistency;
mod fetch;
mod pass;
mod retention;
mod seeding;
mod start_sync;

use std::net::SocketAddr;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use axum::{Json, Router, http::StatusCode, routing::get};
use felix_broker::{CacheMetadata, ConsistencyLevel, StreamMetadata};
use felix_storage::EphemeralCache;
use tokio::net::TcpListener;

use super::apply::*;
use super::fetch::*;
use super::pass::*;
use super::wire::*;
use super::*;
use crate::test_support::{build_test_client, spawn_axum_with_shutdown, wait_for_listen};

async fn serve_router(
    router: Router,
) -> Result<(
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, handle) = spawn_axum_with_shutdown(listener, router);
    wait_for_listen(addr).await?;
    Ok((addr, shutdown_tx, handle))
}

fn error_router() -> Router {
    Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR })
}
