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
mod cache;
mod cache_watch;
mod cancel;
mod client;
mod commit;
mod completion;
mod error;
mod event;
mod groups;
mod producer;
mod publish;
mod sharded;
mod status;
mod subscription;

pub use cache::{
    FELIX_IF_ABSENT, FELIX_IF_VERSION, FelixCacheCondition, FelixConditionCallback,
    FelixCounterCallback, FelixValueCallback, FelixVersionedCallback, felix_client_cache_delete,
    felix_client_cache_delete_if, felix_client_cache_get, felix_client_cache_get_versioned,
    felix_client_cache_put, felix_client_cache_put_if, felix_client_counter_add,
    felix_client_counter_get,
};
pub use cache_watch::{
    FELIX_WATCH_CHANGE, FELIX_WATCH_KEY, FELIX_WATCH_LAGGED, FELIX_WATCH_PREFIX,
    FELIX_WATCH_SHARD_CLOSED, FELIX_WATCH_SHARD_MOVED, FELIX_WATCH_STATE_COMPLETE, FelixCacheWatch,
    FelixCacheWatchCallback, FelixWatchFilter, FelixWatchItem, FelixWatchItemCallback,
    FelixWatchItemKind, felix_cache_watch_free, felix_cache_watch_next,
    felix_cache_watch_resume_offsets, felix_cache_watch_retained_count, felix_client_watch_cache,
    felix_client_watch_cache_sharded,
};
pub use cancel::{
    FelixCancelToken, felix_cancel_token_cancel, felix_cancel_token_free, felix_cancel_token_new,
};
pub use client::{
    FelixClient, FelixConnectCallback, FelixConnectOptions, FelixEndpointsCallback,
    felix_client_connect, felix_client_connect_with, felix_client_endpoints, felix_client_free,
};
pub use commit::{
    FELIX_COMMIT_DELETE, FELIX_COMMIT_ENQUEUE, FELIX_COMMIT_PUBLISH, FELIX_COMMIT_PUT,
    FelixCommitCallback, FelixCommitOp, FelixCommitOpKind, FelixConditionalCallback,
    FelixStateCallback, FelixStateValue, felix_client_commit, felix_client_commit_if,
    felix_client_commit_if_on_shard, felix_client_commit_on_shard, felix_client_publish_if,
    felix_client_publish_if_on_shard, felix_client_state_get, felix_client_state_get_on_shard,
};
pub use error::felix_last_error_message;
pub use event::{
    FelixEvent, felix_event_free, felix_event_offset, felix_event_payload, felix_event_publisher,
    felix_event_skipped_before, felix_event_timestamp,
};
pub use groups::{
    FelixDeletedCallback, FelixDoneCallback, FelixExtendCallback, FelixGroupInfo,
    FelixGroupInfoCallback, FelixGroupInfosCallback, FelixGroupPollCallback, FelixGroupPosition,
    FelixGroupPositionCallback, FelixGroupPositionsCallback, FelixGroupRecord,
    FelixOffsetsCallback, FelixShardedGroup, FelixShardedGroupCallback,
    FelixShardedGroupPollCallback, FelixShardedGroupRecord, FelixShardsCallback,
    felix_client_group_ack, felix_client_group_create, felix_client_group_create_stream,
    felix_client_group_dead_letter, felix_client_group_dead_letters, felix_client_group_delete,
    felix_client_group_delete_stream, felix_client_group_describe,
    felix_client_group_describe_stream, felix_client_group_discard, felix_client_group_extend,
    felix_client_group_nack, felix_client_group_poll, felix_client_group_redrive,
    felix_client_group_seek, felix_client_group_seek_stream, felix_client_group_sharded,
    felix_client_stream_shards, felix_sharded_group_free, felix_sharded_group_poll,
    felix_sharded_group_shards,
};
pub use producer::{
    FelixProducer, FelixProducerCallback, felix_client_idempotent_producer,
    felix_producer_abandon_in_doubt, felix_producer_close, felix_producer_free, felix_producer_id,
    felix_producer_id_for, felix_producer_publish, felix_producer_publish_batches,
};
pub use publish::{
    FELIX_ACK_NONE, FELIX_ACK_PER_BATCH, FELIX_ACK_PER_MESSAGE, FELIX_DELIVERY_AT_LEAST_ONCE,
    FELIX_DELIVERY_AT_MOST_ONCE, FelixAckMode, FelixBatchCallback, FelixDelivery,
    FelixPublishCallback, FelixRecordResult, felix_client_publish, felix_client_publish_batch,
    felix_client_publish_on_shard, felix_client_publish_records,
};
pub use sharded::{
    FELIX_SHARD_LOST, FELIX_SHARD_MOVED, FELIX_SHARD_NONE, FELIX_SHARD_RECORD,
    FELIX_SHARD_RECOVERED, FELIX_SHARD_STREAM_DELETED, FelixShardItemCallback, FelixShardItemKind,
    FelixShardedSubscribeCallback, FelixShardedSubscription, felix_client_resubscribe_sharded,
    felix_client_subscribe_sharded, felix_sharded_next, felix_sharded_positions,
    felix_sharded_shards, felix_sharded_subscription_free,
};
pub use status::{
    FELIX_STATUS_AUTH, FELIX_STATUS_CANCELLED, FELIX_STATUS_CLOSED, FELIX_STATUS_CONNECTION,
    FELIX_STATUS_CURSOR, FELIX_STATUS_ERROR, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_NOT_FOUND,
    FELIX_STATUS_OK, FELIX_STATUS_OUTCOME_UNKNOWN, FELIX_STATUS_OVERLOADED, FELIX_STATUS_PANIC,
    FELIX_STATUS_SHARD_UNAVAILABLE, FELIX_STATUS_TIMEOUT, FELIX_STATUS_UNSUPPORTED, FelixStatus,
};
pub use subscription::{
    FELIX_CLOSE_ENDED, FELIX_CLOSE_OPEN, FELIX_CLOSE_STREAM_DELETED, FELIX_START_EARLIEST,
    FELIX_START_LATEST, FELIX_START_OFFSET, FelixCloseReason, FelixEventCallback,
    FelixOffsetCallback, FelixStartKind, FelixSubscribeCallback, FelixSubscription,
    felix_client_offset_for_time, felix_client_subscribe, felix_subscription_close_reason,
    felix_subscription_free, felix_subscription_next, felix_subscription_start_offset,
};
