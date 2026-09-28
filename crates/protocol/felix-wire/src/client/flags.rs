//! Frame flags: header bits that say how to interpret the frame payload.
//!
//! A flag changes the payload's layout, so a receiver rejects a bit it does not
//! know rather than masking it off, and a sender only sets a bit the peer
//! advertised during `Auth`.

/// The payload is a binary publish batch rather than JSON.
pub const FLAG_BINARY_PUBLISH_BATCH: u16 = 0x0001;
/// The payload is a binary event batch for one subscription.
pub const FLAG_BINARY_EVENT_BATCH: u16 = 0x0002;
/// The payload is a binary event batch encoded once for every subscriber of
/// a stream, so it names no subscription.
pub const FLAG_BINARY_EVENT_BATCH_SHARED: u16 = 0x0004;
/// Modifier on `FLAG_BINARY_PUBLISH_BATCH`: the payload is prefixed with a
/// `request_id` and an ack mode, and the broker owes the client an ack frame.
/// Never meaningful on its own — see [`crate::binary::decode_acked_publish_batch`].
pub const FLAG_BINARY_PUBLISH_ACKED: u16 = 0x0008;
/// Broker → client acknowledgement of an acked publish, replacing the JSON
/// `PublishOk`/`PublishError` messages on the binary path.
pub const FLAG_BINARY_PUBLISH_ACK: u16 = 0x0010;
/// Modifier on either event-batch flag: the payload carries a `base_offset`
/// before its payload count, giving the log offset of the batch's first event.
///
/// A batch's offsets are contiguous, so one `u64` per *batch* is enough — a
/// client derives each event's offset by adding its index. That is what keeps
/// this off the per-event cost model, and it is why offsets can ride the shared
/// encode-once batch at all: the offsets belong to the stream, not to the
/// subscriber, so one encoding still serves every subscriber that negotiated
/// the bit.
pub const FLAG_EVENT_BATCH_OFFSETS: u16 = 0x0020;

/// Modifier on `FLAG_BINARY_PUBLISH_BATCH`: the payload is prefixed with a
/// length-delimited routing key, which decides the batch's shard.
///
/// Without this bit a keyed publish had to fall back to the JSON encoding — the
/// binary layouts had nowhere to put a key — and a perf session measured that
/// fallback at roughly 30% of throughput (#549). The key prefix goes *after* the
/// `FLAG_BINARY_PUBLISH_ACKED` prefix when both are set, so the correlation id
/// stays readable at offset 0 without parsing anything else.
///
/// An empty key is a key: it hashes like any other, and is not the same as an
/// unkeyed frame.
pub const FLAG_BINARY_PUBLISH_KEYED: u16 = 0x0040;

/// Modifier on `FLAG_BINARY_PUBLISH_ACK`: the batch was *forwarded*, and the
/// ack names the broker that owns the shard.
///
/// A publish for a shard this broker does not own is sent on to the owner and
/// acknowledged once the owner has written it. That is correct and invisible,
/// and the invisibility is the problem: the client keeps publishing to the same
/// entry broker forever, and every record is decrypted, re-encrypted and
/// decrypted again on the way. A perf session measured the cost at roughly half
/// the throughput per core — ~250 MB/s per busy vCPU direct against ~140
/// forwarded (#536).
///
/// So the ack says so. The bit's *presence* is the signal that forwarding
/// happened; the payload carries who to send to instead. It is a hint, not a
/// refusal — the publish already succeeded, so a client that ignores it is
/// exactly as correct as before, just as slow.
///
/// Only ever set for a client that advertised this bit in `Auth.client_flags`.
/// A client that did not would reject the whole frame, since an unknown flag
/// bit is refused rather than masked off — and it would be rejecting an
/// acknowledgement for a publish that succeeded.
pub const FLAG_BINARY_PUBLISH_ACK_OWNER: u16 = 0x0080;

/// Modifier on an acked binary publish: the batch belongs to an idempotent
/// producer, and a `u64` producer id and `u64` sequence follow the acked
/// prefix.
///
/// The binary counterpart of `publish_idempotent`, so an idempotent producer
/// no longer has to give up the binary encoding. Only valid with
/// `FLAG_BINARY_PUBLISH_ACKED`, since a producer that never hears back cannot
/// know what to send next. The broker answers the way it answers
/// `publish_idempotent`, including `publish_refused`.
///
/// Sent only to a broker that advertised it in `AuthOk.server_flags`.
pub const FLAG_BINARY_PUBLISH_IDEMPOTENT: u16 = 0x0100;

/// Modifier on `FLAG_BINARY_PUBLISH_ACK`: a failed ack carries a typed error
/// code and retry class after its message.
///
/// ```text
/// u16 code          (ErrorCode::to_u16)
/// u8  retry         (RetryClass::to_u8)
/// ```
///
/// The binary counterpart of `publish_error.code`. A flag rather than the
/// feature bit because it changes the payload layout: a client that does not
/// know it would reject the frame, and with it the answer to its publish. Set
/// only for a client that advertised it in `Auth.client_flags`, and only on a
/// failed ack.
pub const FLAG_BINARY_PUBLISH_ACK_CODE: u16 = 0x0200;

/// Modifier on `FLAG_BINARY_PUBLISH_ACK_CODE`: the code is followed by the
/// error's detail.
///
/// ```text
/// u16 reason_len      (0 when there is no reason)
/// u8[reason_len] reason
/// u64 retry_after_ms  (0 when the broker suggests no wait)
/// ```
///
/// The binary counterpart of `publish_error.detail`: without it a binary
/// publisher is told `shard_unavailable` but not whether the shard is moving,
/// fenced or not yet open, nor how long a move suggests waiting. A separate
/// bit because it changes the layout again. Set only for a client that
/// advertised it, and only alongside the code.
pub const FLAG_BINARY_PUBLISH_ACK_DETAIL: u16 = 0x0400;

/// Modifier on `FLAG_EVENT_BATCH_OFFSETS`: a `u64 skipped_before` follows the
/// `base_offset`, counting the offsets immediately before the batch's first
/// event that hold no event.
///
/// A durable stream's log holds records that are not events: a promoted
/// leader writes a generation-start record before it serves, and it takes an
/// offset like any other. Without this, a subscriber sees `N-1` then `N+1`
/// and reads a drop that never happened. With it, a jump of exactly
/// `skipped_before` is not a drop.
///
/// Set only on a batch that follows such a record, so every other batch is
/// byte-identical to the offsets-only frame. Only sent to a client that
/// advertised it in `Auth.client_flags`; a frame with this bit and no
/// `FLAG_EVENT_BATCH_OFFSETS` is rejected.
pub const FLAG_EVENT_BATCH_SKIPPED: u16 = 0x0800;

/// Every flag bit this version understands.
///
/// Frames carrying bits outside this mask are rejected rather than parsed with
/// the unknown bits ignored. That distinction matters: flag bits here change how
/// the *payload* is laid out, so silently ignoring one means confidently
/// misparsing the body. `FLAG_BINARY_PUBLISH_ACKED` is exactly that case — an
/// older broker that masked it off would read the new `request_id` prefix as a
/// `tenant_len` and produce garbage instead of an error. Rejecting unknown bits
/// cannot help those older brokers, but it means the next extension fails loudly
/// on this version instead of repeating the same trap.
pub const KNOWN_FLAGS: u16 = FLAG_BINARY_PUBLISH_BATCH
    | FLAG_BINARY_EVENT_BATCH
    | FLAG_BINARY_EVENT_BATCH_SHARED
    | FLAG_BINARY_PUBLISH_ACKED
    | FLAG_BINARY_PUBLISH_ACK
    | FLAG_EVENT_BATCH_OFFSETS
    | FLAG_BINARY_PUBLISH_KEYED
    | FLAG_BINARY_PUBLISH_ACK_OWNER
    | FLAG_BINARY_PUBLISH_IDEMPOTENT
    | FLAG_BINARY_PUBLISH_ACK_CODE
    | FLAG_BINARY_PUBLISH_ACK_DETAIL
    | FLAG_EVENT_BATCH_SKIPPED;

/// The flag bits that existed before capability negotiation.
///
/// This is what a peer must be assumed to support when it does not advertise a
/// mask: brokers predating negotiation answer `Auth` with a plain `Ok`, and the
/// only safe reading of that silence is "the original three bits and nothing
/// more". Deliberately frozen — new bits must never be added here, or clients
/// will start assuming support that old brokers do not have.
pub const ORIGINAL_V1_FLAGS: u16 =
    FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_EVENT_BATCH | FLAG_BINARY_EVENT_BATCH_SHARED;

/// True if `flags` contains any bit this version does not define.
pub fn has_unknown_flags(flags: u16) -> bool {
    flags & !KNOWN_FLAGS != 0
}

/// True if `peer_flags` advertises support for every bit in `required`.
pub fn supports(peer_flags: u16, required: u16) -> bool {
    peer_flags & required == required
}

#[cfg(test)]
mod tests;
