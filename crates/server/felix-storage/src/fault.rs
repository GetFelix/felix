//! Test-only faults at the I/O seam: a slow device, one whose flushes fail,
//! and one that refuses or stalls writes.
//!
//! Compiled into debug builds and into builds with the `fault-injection`
//! feature, never into a plain release build. Nothing here does anything until
//! a caller sets it, so a debug broker that is never told about a fault
//! behaves exactly as a release one.
//!
//! Every flush the crate issues goes through `io` (`sync_data`, `sync_all`,
//! `sync_dir`, and the `io_uring` submission), and each of those consults this
//! module first. That is what makes a fault here reach every durability path,
//! including the macOS `F_FULLFSYNC` branch and the ring. A segment append's
//! write goes through `io::write_all` the same way.
//!
//! A process that cannot be called into, such as a broker under the cluster
//! harness, takes its faults from the file `FELIX_STORAGE_FAULT_FILE` names,
//! re-read at most every [`REREAD_AFTER`] by whichever flush comes next:
//!
//! - `fsync_delay_ms=<u64>` delays every flush.
//! - `fsync=fail` fails every flush with `EIO` until the file changes.
//! - `fsync=fail_once` fails the next flush only, the way Linux reports a
//!   writeback error once and then lets a retry "succeed". A new
//!   `generation=<n>` arms it again.
//!
//! - `write=enospc` or `write=eio` fails every segment write with that error
//!   until the file changes; `write=eio_once` fails the next one only. A new
//!   `write_generation=<n>` arms `eio_once` again.
//! - `write_delay_ms=<u64>` delays every segment write, the way a write blocks
//!   once the kernel throttles a process that dirties pages faster than the
//!   device takes them.
//!
//! - `power_loss=<seed>` with `power_loss_into=<dir>` builds the tree a
//!   reboot after a power loss would find into `<dir>` and kills the process.
//!   It needs the model armed at startup by [`arm_power_loss_from_env`] and is
//!   Linux only; see `io::power_loss`.
//!
//! A failed write lands half its buffer first, the way a disk that fills
//! mid-batch leaves a partial record behind, so the writer's rewind is what
//! gets tested and not a write that conveniently did nothing.
//!
//! A missing file is no fault. An injected failure is reported instead of
//! flushing, but the dirty pages are not dropped: whether data survives is the
//! power-loss layer's question, not this one's.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Mutex, Once, PoisonError};
use std::time::{Duration, Instant};

/// Names the storage root a broker runs under the power-loss model.
pub const POWER_LOSS_ROOT_VAR: &str = "FELIX_STORAGE_POWER_LOSS_ROOT";

/// How long a reading of the fault file is reused.
const REREAD_AFTER: Duration = Duration::from_millis(50);

/// Microseconds each flush waits before it is issued. Zero means no delay.
static FSYNC_DELAY_MICROS: AtomicU64 = AtomicU64::new(0);
/// Microseconds each segment write waits before it is issued. Zero means no
/// delay.
static WRITE_DELAY_MICROS: AtomicU64 = AtomicU64::new(0);
/// A [`FsyncFailure`], as its discriminant.
static FSYNC_FAILURE: AtomicU8 = AtomicU8::new(FsyncFailure::None as u8);
/// A [`WriteFailure`], as its discriminant.
static WRITE_FAILURE: AtomicU8 = AtomicU8::new(WriteFailure::None as u8);

static FROM_ENV: Once = Once::new();
static FOLLOWING: AtomicBool = AtomicBool::new(false);
static FOLLOWER: Mutex<Option<Follower>> = Mutex::new(None);

/// Whether flushes fail, and for how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FsyncFailure {
    None = 0,
    /// Every flush fails with `EIO`: a device that has gone bad.
    Always = 1,
    /// The next flush fails with `EIO` and later ones succeed. The case that
    /// catches code which retries a failed fsync and trusts the retry.
    Once = 2,
}

/// Whether segment writes fail, with what, and for how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum WriteFailure {
    None = 0,
    /// Every write fails with `ENOSPC`: a full disk.
    NoSpace = 1,
    /// Every write fails with `EIO`: a device that has gone bad.
    Io = 2,
    /// The next write fails with `EIO` and later ones succeed.
    IoOnce = 3,
}

/// When [`POWER_LOSS_ROOT_VAR`] is set, put that directory under the
/// power-loss model and follow `FELIX_STORAGE_FAULT_FILE` for the
/// `power_loss` directive. Call before any store under the root is opened:
/// everything already there is taken as durable.
///
/// Every flush under the root then records what it made durable, which costs
/// a read of the flushed bytes, so this is for fault-testing runs only.
pub fn arm_power_loss_from_env() -> std::io::Result<()> {
    let Some(root) = std::env::var_os(POWER_LOSS_ROOT_VAR) else {
        return Ok(());
    };
    let Some(fault_file) = std::env::var_os("FELIX_STORAGE_FAULT_FILE") else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{POWER_LOSS_ROOT_VAR} needs FELIX_STORAGE_FAULT_FILE to be told when"),
        ));
    };
    std::fs::create_dir_all(&root)?;
    #[cfg(target_os = "linux")]
    {
        crate::io::power_loss::trigger::arm(std::path::Path::new(&root), fault_file.into())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = fault_file;
        tracing::warn!("{POWER_LOSS_ROOT_VAR} is ignored: the power-loss model is Linux only");
        Ok(())
    }
}

/// Make every flush in this process wait `delay` before it reaches the device.
///
/// Process-wide, because the thing it stands in for is a slow disk, and a
/// broker has one. `Duration::ZERO` turns it off.
pub fn set_fsync_delay(delay: Duration) {
    let micros = u64::try_from(delay.as_micros()).unwrap_or(u64::MAX);
    if FSYNC_DELAY_MICROS.swap(micros, Ordering::Relaxed) != micros {
        tracing::warn!(
            delay_ms = delay.as_millis() as u64,
            "storage fault injection: every fsync is delayed (test-only facility)",
        );
    }
}

/// The delay currently injected before each flush, if any.
pub fn fsync_delay() -> Option<Duration> {
    match FSYNC_DELAY_MICROS.load(Ordering::Relaxed) {
        0 => None,
        micros => Some(Duration::from_micros(micros)),
    }
}

/// Make every segment write in this process wait `delay` first. Process-wide,
/// like the flush delay. `Duration::ZERO` turns it off.
pub fn set_write_delay(delay: Duration) {
    let micros = u64::try_from(delay.as_micros()).unwrap_or(u64::MAX);
    if WRITE_DELAY_MICROS.swap(micros, Ordering::Relaxed) != micros {
        tracing::warn!(
            delay_ms = delay.as_millis() as u64,
            "storage fault injection: every segment write is delayed (test-only facility)",
        );
    }
}

/// The delay currently injected before each segment write, if any.
pub fn write_delay() -> Option<Duration> {
    match WRITE_DELAY_MICROS.load(Ordering::Relaxed) {
        0 => None,
        micros => Some(Duration::from_micros(micros)),
    }
}

/// Make flushes in this process fail. Process-wide, like the delay.
pub fn set_fsync_failure(failure: FsyncFailure) {
    if FSYNC_FAILURE.swap(failure as u8, Ordering::AcqRel) != failure as u8 {
        tracing::warn!(
            ?failure,
            "storage fault injection: fsync failure set (test-only facility)",
        );
    }
}

/// Make segment writes in this process fail. Process-wide, like the flush
/// faults.
pub(crate) fn set_write_failure(failure: WriteFailure) {
    if WRITE_FAILURE.swap(failure as u8, Ordering::AcqRel) != failure as u8 {
        tracing::warn!(
            ?failure,
            "storage fault injection: write failure set (test-only facility)",
        );
    }
}

/// Pick up any change to the fault file. Cheap when there is none: one
/// atomic load unless `FELIX_STORAGE_FAULT_FILE` is set.
pub(crate) fn refresh() {
    FROM_ENV.call_once(|| {
        if let Some(path) = std::env::var_os("FELIX_STORAGE_FAULT_FILE") {
            let path = PathBuf::from(path);
            tracing::warn!(
                path = %path.display(),
                "storage fault injection is ENABLED; this is a test-only facility",
            );
            *FOLLOWER.lock().unwrap_or_else(PoisonError::into_inner) = Some(Follower {
                path,
                read_at: None,
                applied: None,
            });
            FOLLOWING.store(true, Ordering::Release);
        }
    });
    if !FOLLOWING.load(Ordering::Acquire) {
        return;
    }
    let mut follower = FOLLOWER.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(follower) = follower.as_mut() else {
        return;
    };
    if follower
        .read_at
        .is_some_and(|read_at| read_at.elapsed() < REREAD_AFTER)
    {
        return;
    }
    follower.read_at = Some(Instant::now());
    let setting = std::fs::read_to_string(&follower.path)
        .map(|body| FileSetting::parse(&body))
        .unwrap_or_default();
    if follower.applied == Some(setting) {
        return;
    }
    set_fsync_delay(setting.delay);
    set_write_delay(setting.write_delay);
    if rearms(follower.applied.as_ref(), &setting) {
        set_fsync_failure(setting.failure);
    }
    if rearms_write(follower.applied.as_ref(), &setting) {
        set_write_failure(setting.write);
    }
    follower.applied = Some(setting);
}

/// The error the next flush should report instead of flushing, if any.
/// Consumes a [`FsyncFailure::Once`].
pub(crate) fn injected_failure() -> Option<std::io::Error> {
    let failing = match FSYNC_FAILURE.load(Ordering::Acquire) {
        x if x == FsyncFailure::Always as u8 => true,
        x if x == FsyncFailure::Once as u8 => FSYNC_FAILURE
            .compare_exchange(
                FsyncFailure::Once as u8,
                FsyncFailure::None as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok(),
        _ => false,
    };
    failing.then(eio)
}

/// The error the next segment write should report, if any. Consumes a
/// [`WriteFailure::IoOnce`].
pub(crate) fn injected_write_failure() -> Option<std::io::Error> {
    match WRITE_FAILURE.load(Ordering::Acquire) {
        x if x == WriteFailure::NoSpace as u8 => Some(enospc()),
        x if x == WriteFailure::Io as u8 => Some(eio()),
        x if x == WriteFailure::IoOnce as u8 => WRITE_FAILURE
            .compare_exchange(
                WriteFailure::IoOnce as u8,
                WriteFailure::None as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
            .then(eio),
        _ => None,
    }
}

/// Whether reading `next` after `applied` sets the failure again.
///
/// Only a new failure mode or a new generation does. A `fail_once` the flush
/// path already consumed must stay consumed while the file still says
/// `fail_once`, even when something else in the file (the delay) changed;
/// otherwise every edit would fail one more flush.
fn rearms(applied: Option<&FileSetting>, next: &FileSetting) -> bool {
    applied.is_none_or(|applied| {
        (applied.failure, applied.generation) != (next.failure, next.generation)
    })
}

/// [`rearms`] for the write failure, which has its own generation so that
/// arming a flush fault does not re-fire a consumed `eio_once`.
fn rearms_write(applied: Option<&FileSetting>, next: &FileSetting) -> bool {
    applied.is_none_or(|applied| {
        (applied.write, applied.write_generation) != (next.write, next.write_generation)
    })
}

#[cfg(unix)]
fn enospc() -> std::io::Error {
    std::io::Error::from_raw_os_error(libc::ENOSPC)
}

#[cfg(not(unix))]
fn enospc() -> std::io::Error {
    std::io::Error::other("injected write failure (ENOSPC)")
}

#[cfg(unix)]
fn eio() -> std::io::Error {
    std::io::Error::from_raw_os_error(libc::EIO)
}

#[cfg(not(unix))]
fn eio() -> std::io::Error {
    std::io::Error::other("injected I/O failure (EIO)")
}

struct Follower {
    path: PathBuf,
    read_at: Option<Instant>,
    applied: Option<FileSetting>,
}

/// What the fault file asks for. The default is no fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileSetting {
    pub(crate) delay: Duration,
    pub(crate) failure: FsyncFailure,
    pub(crate) generation: u64,
    pub(crate) write: WriteFailure,
    pub(crate) write_generation: u64,
    pub(crate) write_delay: Duration,
}

impl Default for FileSetting {
    fn default() -> Self {
        Self {
            delay: Duration::ZERO,
            failure: FsyncFailure::None,
            generation: 0,
            write: WriteFailure::None,
            write_generation: 0,
            write_delay: Duration::ZERO,
        }
    }
}

impl FileSetting {
    /// Unknown lines are ignored: a half-written file must read as less of a
    /// fault, never as a broker that refuses to flush for a parse error.
    pub(crate) fn parse(body: &str) -> Self {
        let mut setting = Self::default();
        for line in body.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let value = value.trim();
            match key.trim() {
                "fsync_delay_ms" => {
                    if let Ok(ms) = value.parse() {
                        setting.delay = Duration::from_millis(ms);
                    }
                }
                "fsync" => {
                    setting.failure = match value {
                        "fail" => FsyncFailure::Always,
                        "fail_once" => FsyncFailure::Once,
                        _ => FsyncFailure::None,
                    }
                }
                "generation" => setting.generation = value.parse().unwrap_or(0),
                "write" => {
                    setting.write = match value {
                        "enospc" => WriteFailure::NoSpace,
                        "eio" => WriteFailure::Io,
                        "eio_once" => WriteFailure::IoOnce,
                        _ => WriteFailure::None,
                    }
                }
                "write_generation" => setting.write_generation = value.parse().unwrap_or(0),
                "write_delay_ms" => {
                    if let Ok(ms) = value.parse() {
                        setting.write_delay = Duration::from_millis(ms);
                    }
                }
                _ => {}
            }
        }
        setting
    }
}

#[cfg(test)]
mod tests;
