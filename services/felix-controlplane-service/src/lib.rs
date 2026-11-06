//! The control plane: the cluster's metadata, and the decisions taken from it.
//!
//! It carries no payload data. It owns *who and where*: tenants, namespaces,
//! streams, caches, the node catalog, and which broker leads each shard.
//! Brokers read it and act; it never reaches into a broker.
//!
//! There are two entry points, both reached from the `felix-controlplane`
//! binary: [`server::run`] serves the API and runs the background loops, and
//! [`migrate::run`] moves metadata between backends. The operator's commands
//! over the API are in `felixctl`.
//!
//! # Vocabulary
//!
//! [`model`] is what every endpoint reads and writes, and the place to look
//! first: the rules about a shard assignment or a node lifecycle are stated on
//! the type rather than scattered through handlers.
//!
//! # Serving
//!
//! [`api`] is the REST layer: handlers, the router, the OpenAPI document, and
//! `/ready`, which gates traffic on the store being usable rather than on the
//! process being up. [`auth`] verifies Felix tokens, mints them through token
//! exchange, and publishes JWKS.
//!
//! # Keeping
//!
//! [`store::ControlPlaneStore`] has three implementations: in memory, over
//! Postgres, and over an embedded Raft group ([`store::raft`], built on the
//! consensus seam in [`raft`]). All three are held to one shared contract
//! test, because a rule that holds only in memory is a rule the deployed
//! system does not have.
//!
//! # Deciding
//!
//! [`cluster`] makes the timer-driven decisions about the fleet:
//! [`cluster::membership`] expires nodes that stopped heartbeating, and
//! [`cluster::placement`] decides which broker leads each shard, as a pure
//! function of a metadata snapshot so that two instances agree without
//! coordinating.
//!
//! # Running
//!
//! [`config`] reads the environment and YAML, [`server`] wires everything
//! together and drains it in order, [`migrate`] is the backend migration tool,
//! and [`clock`] is the wall clock the stored timestamps use.
//!
//! Each module keeps its unit tests at `<module>/tests.rs`.

pub mod model;

pub mod api;
pub mod auth;

pub mod raft;
pub mod store;

pub mod cluster;

pub mod clock;
pub mod config;
pub mod migrate;
pub mod server;

#[cfg(test)]
mod test_support;
