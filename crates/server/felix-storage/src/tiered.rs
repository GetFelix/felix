//! Moving sealed segments to cold storage and back. Only the interface exists;
//! nothing implements it yet. Copying segments out is `disk_log::offload`.

use std::time::Duration;

use crate::Result;
use crate::log::{BoxFuture, SegmentDescriptor, SegmentId, ShardKey};

#[derive(Debug, Clone)]
pub struct OffloadedSegment {
    pub segment: SegmentDescriptor,
    pub uri: String,
    pub checksum: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ColdCacheConfig {
    pub max_bytes: u64,
    pub root_dir: String,
}

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub local_retention: Duration,
    pub cold_retention: Option<Duration>,
}

pub trait TieredStore: Send + Sync {
    fn offload_segment(
        &self,
        shard: &ShardKey,
        segment: &SegmentDescriptor,
    ) -> BoxFuture<'_, Result<OffloadedSegment>>;
    fn fetch_segment(
        &self,
        shard: &ShardKey,
        segment_id: SegmentId,
    ) -> BoxFuture<'_, Result<OffloadedSegment>>;
    fn list_segments(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Vec<OffloadedSegment>>>;
}
