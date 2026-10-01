//! `FrameSource`: "read the next felix-wire frame", abstracted so the control
//! and uni loops can be tested without real QUIC sockets. Production reads
//! from `quinn::RecvStream`; tests feed fixed sequences, errors, EOF, or
//! delays.
//!
//! `Ok(None)` is a clean EOF, `Err` a transport or frame-format violation.
//! The caller-owned `scratch` holds bytes read past one frame for the next
//! read, so it must stay with its stream; the boxed future keeps the trait object-safe without
//! pulling in `async_trait`.

use std::future::Future;
use std::pin::Pin;

use crate::serving::quic::codec::FrameScratch;
use anyhow::Result;
use felix_wire::Frame;
use quinn::RecvStream;

use crate::serving::quic::codec::read_frame_limited_into;

/// Read one frame, capped at `max_frame_bytes`. `None` is a clean close.
pub(super) trait FrameSource {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut FrameScratch,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>>;
}

impl FrameSource for RecvStream {
    fn next_frame<'a>(
        &'a mut self,
        max_frame_bytes: usize,
        scratch: &'a mut FrameScratch,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        Box::pin(read_frame_limited_into(self, max_frame_bytes, scratch))
    }
}

/// Deterministic source backed by a queue: fixed frames, injected errors, or
/// `Ok(None)` for a clean close.
#[cfg(test)]
pub(super) struct TestFrameSource {
    pub(super) frames: std::collections::VecDeque<Result<Option<Frame>>>,
}

#[cfg(test)]
impl TestFrameSource {
    pub(super) fn new(frames: Vec<Result<Option<Frame>>>) -> Self {
        Self {
            frames: frames.into(),
        }
    }
}

#[cfg(test)]
impl FrameSource for TestFrameSource {
    fn next_frame<'a>(
        &'a mut self,
        _max_frame_bytes: usize,
        _scratch: &'a mut FrameScratch,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        Box::pin(async move { self.frames.pop_front().unwrap_or_else(|| Ok(None)) })
    }
}

/// Returns EOF after a delay, for cancellation and drain-timeout paths.
#[cfg(test)]
pub(super) struct DelayFrameSource {
    pub(super) delay: std::time::Duration,
}

#[cfg(test)]
impl FrameSource for DelayFrameSource {
    fn next_frame<'a>(
        &'a mut self,
        _max_frame_bytes: usize,
        _scratch: &'a mut FrameScratch,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Frame>>> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            Ok(None)
        })
    }
}
