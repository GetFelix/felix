//! Test hooks: thin wrappers over the codec functions so tests can force
//! write/encode failures and throttle resets without touching production code.

use crate::serving::quic::{ACK_HI_WATER, ACK_LO_WATER};
use anyhow::{Context, Result};
use felix_wire::{Frame, Message};

pub(super) fn encode_message_with_hook(message: Message) -> Result<Frame> {
    #[cfg(test)]
    if test_hooks::force_write_message_error() {
        return Err(anyhow::anyhow!("forced write_message error"));
    }
    message.encode().context("encode message")
}

pub(super) fn check_frame_write_hook() -> Result<()> {
    #[cfg(test)]
    if test_hooks::force_write_frame_error() {
        return Err(anyhow::anyhow!("forced write_frame error"));
    }
    Ok(())
}

pub(super) fn encode_cache_message_with_hook(message: Message) -> Result<Frame> {
    #[cfg(test)]
    if test_hooks::force_cache_encode_error() {
        return Err(anyhow::anyhow!("forced cache encode error"));
    }
    message.encode().context("encode message")
}

/// Reset the ACK throttle once the queue depth has fallen from at or above
/// the high watermark to below the low one. The hysteresis gap is what stops
/// the throttle flapping when depth hovers near a single threshold.
pub(super) fn should_reset_throttle(depth_update: Option<(usize, usize)>) -> bool {
    if let Some((prev, cur)) = depth_update
        && prev >= ACK_HI_WATER
        && cur < ACK_LO_WATER
    {
        return true;
    }
    #[cfg(test)]
    if test_hooks::force_throttle_reset() {
        return true;
    }
    false
}

/// Toggles for hard-to-reach error, timeout, and special-case branches.
#[cfg(test)]
pub(super) mod test_hooks {
    use std::sync::atomic::{AtomicBool, Ordering};

    static FORCE_WRITE_MESSAGE_ERROR: AtomicBool = AtomicBool::new(false);
    static FORCE_WRITE_FRAME_ERROR: AtomicBool = AtomicBool::new(false);
    static FORCE_CACHE_ENCODE_ERROR: AtomicBool = AtomicBool::new(false);
    static FORCE_THROTTLE_RESET: AtomicBool = AtomicBool::new(false);
    static FORCE_DRAIN_TIMEOUT: AtomicBool = AtomicBool::new(false);

    pub(crate) fn set_force_write_message_error(enabled: bool) {
        FORCE_WRITE_MESSAGE_ERROR.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn set_force_write_frame_error(enabled: bool) {
        FORCE_WRITE_FRAME_ERROR.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn set_force_cache_encode_error(enabled: bool) {
        FORCE_CACHE_ENCODE_ERROR.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn set_force_throttle_reset(enabled: bool) {
        FORCE_THROTTLE_RESET.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn set_force_drain_timeout(enabled: bool) {
        FORCE_DRAIN_TIMEOUT.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn force_write_message_error() -> bool {
        FORCE_WRITE_MESSAGE_ERROR.load(Ordering::Relaxed)
    }

    pub(crate) fn force_write_frame_error() -> bool {
        FORCE_WRITE_FRAME_ERROR.load(Ordering::Relaxed)
    }

    pub(crate) fn force_cache_encode_error() -> bool {
        FORCE_CACHE_ENCODE_ERROR.load(Ordering::Relaxed)
    }

    pub(crate) fn force_throttle_reset() -> bool {
        FORCE_THROTTLE_RESET.load(Ordering::Relaxed)
    }

    pub(crate) fn force_drain_timeout() -> bool {
        FORCE_DRAIN_TIMEOUT.load(Ordering::Relaxed)
    }

    /// Back to a clean slate between tests.
    pub(crate) fn reset() {
        set_force_write_message_error(false);
        set_force_write_frame_error(false);
        set_force_cache_encode_error(false);
        set_force_throttle_reset(false);
        set_force_drain_timeout(false);
    }
}
