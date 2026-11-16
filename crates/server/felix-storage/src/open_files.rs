//! The process's open-file limit.
//!
//! Every held shard keeps files open, so a process that opens storage wants
//! its soft `RLIMIT_NOFILE` at the hard one. Most Linux systems start a
//! process at a soft limit of 1,024 and a much higher hard limit. When the
//! limit is still exhausted, opens fail with [`StorageError::OpenFileLimit`]
//! and can be retried once files close: see "The open-file limit" in
//! `docs/durable-storage.md`.

use std::io;
use std::sync::Once;

use crate::StorageError;

/// macOS's `OPEN_MAX` (`sys/syslimits.h`), which the `libc` crate does not
/// export: the soft limit it always accepts.
#[cfg(target_os = "macos")]
const OPEN_MAX: u64 = 10_240;

/// Raise the soft open-file limit to the hard one. Never lowers it.
///
/// Returns the soft limit before and after (as the kernel reports it), or
/// the limit still in force and why it could not be raised. On a platform
/// without `RLIMIT_NOFILE` it reports `u64::MAX` for both.
pub fn raise_soft_limit() -> Result<(u64, u64), (u64, io::Error)> {
    raise()
}

/// [`raise_soft_limit`], once per process, ignoring a failure.
///
/// For test binaries and harnesses that open many logs at once (see
/// [`raise_soft_limit_at_start!`](crate::raise_soft_limit_at_start)): they get
/// the limit a broker runs with, so a suite does not depend on the shell's
/// `ulimit -n`. The hard limit stays where it was, so a real leak still
/// runs out.
pub fn raise_soft_limit_once() {
    static RAISED: Once = Once::new();
    RAISED.call_once(|| {
        let _ = raise();
    });
}

/// Call [`raise_soft_limit_once`] before `main`, in the test binary that
/// expands this. Put it once in a crate root under `#[cfg(test)]`, so every
/// test in the binary runs with the raised limit whichever starts first.
#[macro_export]
macro_rules! raise_soft_limit_at_start {
    () => {
        const _: () = {
            extern "C" fn raise_soft_limit_at_start() {
                $crate::open_files::raise_soft_limit_once();
            }
            // SAFETY: the function only reads and sets the process's
            // RLIMIT_NOFILE, which needs nothing the runtime sets up.
            #[used]
            #[cfg_attr(
                any(target_os = "linux", target_os = "android"),
                unsafe(link_section = ".init_array")
            )]
            #[cfg_attr(
                target_vendor = "apple",
                unsafe(link_section = "__DATA,__mod_init_func")
            )]
            static RAISE_SOFT_LIMIT_AT_START: extern "C" fn() = raise_soft_limit_at_start;
        };
    };
}

/// Whether `err` is the process (`EMFILE`) or the system (`ENFILE`) running
/// out of file descriptors.
pub fn is_exhausted(err: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(err.raw_os_error(), Some(libc::EMFILE | libc::ENFILE))
    }
    #[cfg(not(unix))]
    {
        let _ = err;
        false
    }
}

/// Whether `err`, or any error in its source chain, is an exhausted
/// open-file limit: an [`io::Error`] for which [`is_exhausted`] holds or a
/// [`StorageError`] for which [`StorageError::is_open_file_limit`] does.
pub fn exhausted_in(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(err);
    while let Some(err) = next {
        if let Some(io) = err.downcast_ref::<io::Error>() {
            // A wrapped error is not the custom error's `source`.
            if is_exhausted(io) || io.get_ref().is_some_and(|inner| exhausted_in(inner)) {
                return true;
            }
        }
        if err
            .downcast_ref::<StorageError>()
            .is_some_and(StorageError::is_open_file_limit)
        {
            return true;
        }
        next = err.source();
    }
    false
}

/// The soft limit to ask for, given the current pair and the most the
/// platform grants one process.
fn target(soft: u64, hard: u64, ceiling: u64) -> u64 {
    soft.max(hard.min(ceiling))
}

/// macOS reports an unlimited hard limit and refuses a soft limit above
/// `kern.maxfilesperproc`, which sits well over `OPEN_MAX`.
#[cfg(target_os = "macos")]
fn ceiling() -> u64 {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated, and `value` and `len` describe a
    // buffer the call fills.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            (&raw mut value).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 && value > 0 {
        value as u64
    } else {
        OPEN_MAX
    }
}

#[cfg(target_os = "linux")]
fn ceiling() -> u64 {
    u64::MAX
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn limits() -> io::Result<libc::rlimit> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid `rlimit` for the call to fill.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(limit)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn set_soft(limit: &libc::rlimit, soft: u64) -> io::Result<()> {
    let wanted = libc::rlimit {
        rlim_cur: soft as libc::rlim_t,
        rlim_max: limit.rlim_max,
    };
    // SAFETY: `wanted` is a valid `rlimit`, the one read with its soft value
    // changed to at most the hard one.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &wanted) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn raise() -> Result<(u64, u64), (u64, io::Error)> {
    let limit = limits().map_err(|err| (0, err))?;
    let before = limit.rlim_cur as u64;
    let wanted = target(before, limit.rlim_max as u64, ceiling());
    if wanted > before {
        let result = set_soft(&limit, wanted);
        // The ceiling is a guess on some systems; fall back to the one
        // number macOS always accepts.
        #[cfg(target_os = "macos")]
        let result = if result.is_err() && wanted > OPEN_MAX {
            set_soft(&limit, OPEN_MAX.max(before))
        } else {
            result
        };
        if let Err(err) = result {
            return Err((before, err));
        }
    }
    let after = limits().map_err(|err| (before, err))?;
    Ok((before, after.rlim_cur as u64))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raise() -> Result<(u64, u64), (u64, io::Error)> {
    Ok((u64::MAX, u64::MAX))
}

#[cfg(test)]
mod tests;
