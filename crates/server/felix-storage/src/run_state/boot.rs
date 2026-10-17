//! Which boot of the machine this process runs in.
//!
//! A process crash keeps the page cache, so every write the kernel accepted
//! still reaches the device; only a machine that went down can lose them. The
//! kernel gives each boot a random id, and a different one after any restart,
//! panic or kexec. Containers share the host's, which is right: the page cache
//! is the host's too.

/// A boot of the machine, from the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootId([u8; 16]);

impl BootId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    /// A UUID in its usual text form, hyphens optional, surrounding
    /// whitespace ignored. `None` for anything else.
    pub fn parse(text: &str) -> Option<Self> {
        let hex: Vec<u8> = text.trim().bytes().filter(|b| *b != b'-').collect();
        if hex.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 16];
        for (byte, pair) in bytes.iter_mut().zip(hex.chunks(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
        }
        Some(Self(bytes))
    }
}

/// The current boot's id, or `None` where the platform has none or it could
/// not be read. `None` never vouches for a crash.
pub fn this_boot() -> Option<BootId> {
    read().and_then(|text| BootId::parse(&text))
}

#[cfg(target_os = "linux")]
fn read() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()
}

/// `sysctlbyname`, so no process is spawned.
#[cfg(target_os = "macos")]
fn read() -> Option<String> {
    let name = c"kern.bootsessionuuid";
    let mut buf = [0u8; 64];
    let mut len = buf.len();
    // SAFETY: `name` is NUL-terminated, `buf` is writable for `len` bytes, and
    // `len` is updated to the bytes written.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    let text = buf.get(..len)?;
    let text = text.split(|b| *b == 0).next()?;
    String::from_utf8(text.to_vec()).ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read() -> Option<String> {
    None
}
