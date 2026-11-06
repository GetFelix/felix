//! The per-connection cache of resolved stream handles, and its key.

use felix_common::random::HashMap;
use std::time::Instant;

use felix_broker::StreamHandle;

pub(crate) type StreamHandleCache = HashMap<String, (Option<StreamHandle>, Instant)>;

/// Build the stream-handle cache key for one `(tenant, namespace, stream, shard)`.
///
/// **Every part is length-prefixed**, because nothing forbids a `\0` inside a
/// tenant id, namespace or stream name. Joining the parts with a separator
/// alone made tenant `"a\0b"` namespace `"c"` produce the same key as tenant
/// `"a"` namespace `"b\0c"`, and a cache hit would then hand a publish the
/// handle of a *different stream* (#295). A length says where a part ends
/// whatever bytes are inside it, so no two distinct tuples can collide.
///
/// Same reasoning, and the same shape, as `layout::shard_dir_name` in the
/// storage layer.
///
/// Written by hand rather than through `write!`: this key is rebuilt on every
/// publish, and `core::fmt` is heavy next to the `push_str` calls the rest of
/// it is deliberately made of.
pub(super) fn push_stream_cache_key(
    buf: &mut String,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) {
    buf.clear();
    // Three lengths, three separators, and the shard: a handful of bytes beyond
    // the parts themselves.
    let needed = tenant_id.len() + namespace.len() + stream.len() + 32;
    if buf.capacity() < needed {
        buf.reserve(needed - buf.capacity());
    }
    for part in [tenant_id, namespace, stream] {
        push_decimal(buf, part.len() as u32);
        buf.push('\0');
        buf.push_str(part);
    }
    // No separator needed: the part before it has a declared length, so the
    // digits that follow can only be the shard.
    push_decimal(buf, shard);
}

/// Append `value` as ASCII decimal, without `core::fmt`.
///
/// On the publish path, where the difference between this and `write!` is the
/// whole formatting machinery for a number that is almost always one digit.
pub(super) fn push_decimal(buf: &mut String, mut value: u32) {
    // Almost every value here is one digit — a shard number, or the length of a
    // short identifier — and going straight to a byte push skips building a
    // slice and validating it as UTF-8 for a single character.
    if value < 10 {
        buf.push((b'0' + value as u8) as char);
        return;
    }
    let mut digits = [0u8; 10];
    let mut at = digits.len();
    loop {
        at -= 1;
        digits[at] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    // Pushed one at a time rather than as a validated `&str`: these are ASCII
    // digits, so each is one byte, and `from_utf8` would rescan them.
    for &digit in &digits[at..] {
        buf.push(digit as char);
    }
}
