//! Scratch diagnostic: env-gated timeline of the quorum ack path.
//! `FELIX_TRACE_ACK_PATH=1`, lines go to `$FELIX_TRACE_ACK_DIR/ackpath-<pid>.log`.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::OnceLock;

use parking_lot::Mutex;

const RING: usize = 120_000;
const WINDOW_BEFORE_US: u64 = 4_000;

struct State {
    ring: VecDeque<(u64, String, String)>,
    file: Option<std::fs::File>,
    last_dump: std::collections::HashMap<String, u64>,
    skipped: std::collections::HashMap<String, u64>,
}

static STATE: Mutex<Option<State>> = Mutex::new(None);
static ON: OnceLock<bool> = OnceLock::new();

pub(crate) fn on() -> bool {
    *ON.get_or_init(|| std::env::var_os("FELIX_TRACE_ACK_PATH").is_some())
}

pub(crate) fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

pub(crate) trait Tagged {
    fn parts(&self) -> (&str, u32);
}
impl Tagged for crate::ShardKey {
    fn parts(&self) -> (&str, u32) {
        (&self.stream, self.shard)
    }
}
impl Tagged for felix_router::ShardKey {
    fn parts(&self) -> (&str, u32) {
        (&self.stream, self.shard)
    }
}

pub(crate) fn tag<K: Tagged>(key: &K) -> String {
    let (stream, shard) = key.parts();
    format!("{stream}/{shard}")
}

fn with<T>(f: impl FnOnce(&mut State) -> T) -> T {
    let mut guard = STATE.lock();
    let state = guard.get_or_insert_with(|| {
        let file = std::env::var("FELIX_TRACE_ACK_DIR").ok().and_then(|dir| {
            let _ = std::fs::create_dir_all(&dir);
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{dir}/ackpath-{}.log", std::process::id()))
                .ok()
        });
        State {
            ring: VecDeque::with_capacity(RING),
            file,
            last_dump: Default::default(),
            skipped: Default::default(),
        }
    });
    f(state)
}

/// Record an event for `tag` ("*" for global ones) in the ring.
pub(crate) fn ev(tag: &str, msg: impl FnOnce() -> String) {
    if !on() {
        return;
    }
    let (t, m) = (now_us(), msg());
    with(|s| {
        if s.ring.len() >= RING {
            s.ring.pop_front();
        }
        s.ring.push_back((t, tag.to_string(), m));
    });
}

/// Write a line straight to the log.
pub(crate) fn line(text: impl FnOnce() -> String) {
    if !on() {
        return;
    }
    let text = text();
    with(|s| {
        if let Some(f) = s.file.as_mut() {
            let _ = writeln!(f, "{text}");
        }
    });
}

/// A slow ack on `tag` that began at `start_us`: one line with the timeline
/// of that shard (and global events) since just before it began. At most one
/// timeline per shard per 50 ms; the rest are counted.
pub(crate) fn slow_ack(tag: &str, start_us: u64, head: String) {
    if !on() {
        return;
    }
    let now = now_us();
    with(|s| {
        let last = s.last_dump.get(tag).copied().unwrap_or(0);
        if now.saturating_sub(last) < 50_000 {
            *s.skipped.entry(tag.to_string()).or_default() += 1;
            return;
        }
        s.last_dump.insert(tag.to_string(), now);
        let skipped = s.skipped.remove(tag).unwrap_or(0);
        let from = start_us.saturating_sub(WINDOW_BEFORE_US);
        let mut out =
            format!("SLOW tag={tag} start={start_us} skipped_since_last={skipped} {head} ::");
        let mut n = 0;
        for (t, tg, m) in s.ring.iter() {
            if *t >= from && (tg == tag || tg == "*") {
                n += 1;
                if n > 400 {
                    out.push_str(" ...truncated");
                    break;
                }
                out.push_str(&format!(
                    " | +{}us {}: {}",
                    *t as i64 - start_us as i64,
                    tg,
                    m
                ));
            }
        }
        if let Some(f) = s.file.as_mut() {
            let _ = writeln!(f, "{out}");
        }
    });
}
