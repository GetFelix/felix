//! Node.js / TypeScript bindings for the Felix client.
//!
//! This is a **wrapper over `felix-client`, not a second implementation of the
//! protocol.** Reconnection, redirect-following, retry classification, offset
//! bookkeeping and the frame codec all live in the Rust client and are shared
//! by every language that binds to it. A TypeScript-native client would be a
//! second place for those to be subtly wrong, in exactly the areas — failover
//! and delivery accounting — where subtly wrong is most expensive. The Python
//! binding is built on the same reasoning and exposes the same surface.
//!
//! The surface is asynchronous: every call returns a `Promise`, because
//! blocking Node's event loop is not a thing a Node library may do. napi-rs
//! runs the future on its own Tokio runtime and settles the promise from
//! there, so the event loop stays free while a publish is in flight.
//!
//! A cache entry is named by tenant, namespace, cache and key before its value
//! and options ever appear, so several of these methods carry more arguments
//! than clippy's default. Bundling them into an object would move the argument
//! list rather than shorten it, and every caller has the parts separately
//! anyway — the same reasoning the Python binding records.
#![allow(clippy::too_many_arguments)]

mod args;
mod cache_watch;
mod client;
mod commit;
mod errors;
mod sharded_subscription;
mod subscription;
mod tls;
mod types;
