//! A C ABI over the Felix Rust client.
//!
//! This is what the Go and C# SDKs will bind to, and it can be used from C
//! directly. Like the Python and Node bindings it wraps `felix-client`, so
//! reconnection, redirect-following and error classes exist once.
//!
//! The rules every function follows:
//!
//! - Handles are opaque pointers. Each one the library returns has a matching
//!   `*_free` function, and nothing else frees it.
//! - Calls that talk to the broker return at once and report their result to
//!   a callback, exactly once. The returned [`FelixStatus`] only says whether
//!   the call was accepted; when it was not, [`felix_last_error_message`]
//!   describes why, for the calling thread.
//! - No Rust panic crosses the boundary. A panic becomes
//!   [`FELIX_STATUS_PANIC`].
//! - Each client owns its Tokio runtime, so a caller needs no async runtime
//!   of its own. Callbacks run on its threads.
//!
//! The crate README explains the model and why it was chosen.
//!
//! `include/felix.h` is generated from this crate by cbindgen and checked in;
//! `task capi:header` regenerates it.

mod boundary;
mod cancel;
mod client;
mod completion;
mod error;
mod event;
mod publish;
mod status;
mod subscription;

pub use cancel::{
    FelixCancelToken, felix_cancel_token_cancel, felix_cancel_token_free, felix_cancel_token_new,
};
pub use client::{FelixClient, FelixConnectCallback, felix_client_connect, felix_client_free};
pub use error::felix_last_error_message;
pub use event::{
    FelixEvent, felix_event_free, felix_event_offset, felix_event_payload,
    felix_event_skipped_before,
};
pub use publish::{
    FELIX_ACK_NONE, FELIX_ACK_PER_BATCH, FELIX_ACK_PER_MESSAGE, FELIX_DELIVERY_AT_LEAST_ONCE,
    FELIX_DELIVERY_AT_MOST_ONCE, FelixAckMode, FelixBatchCallback, FelixDelivery,
    FelixPublishCallback, FelixRecordResult, felix_client_publish, felix_client_publish_batch,
    felix_client_publish_on_shard, felix_client_publish_records,
};
pub use status::{
    FELIX_STATUS_AUTH, FELIX_STATUS_CANCELLED, FELIX_STATUS_CLOSED, FELIX_STATUS_CONNECTION,
    FELIX_STATUS_CURSOR, FELIX_STATUS_ERROR, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_NOT_FOUND,
    FELIX_STATUS_OK, FELIX_STATUS_OUTCOME_UNKNOWN, FELIX_STATUS_OVERLOADED, FELIX_STATUS_PANIC,
    FELIX_STATUS_SHARD_UNAVAILABLE, FELIX_STATUS_TIMEOUT, FelixStatus,
};
pub use subscription::{
    FELIX_START_EARLIEST, FELIX_START_LATEST, FELIX_START_OFFSET, FelixEventCallback,
    FelixStartKind, FelixSubscribeCallback, FelixSubscription, felix_client_subscribe,
    felix_subscription_free, felix_subscription_next,
};
