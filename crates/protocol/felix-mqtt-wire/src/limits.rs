//! The bounds a decoder enforces on input from a device.

/// Largest value a variable byte integer can hold, which caps the remaining
/// length of any packet.
pub(crate) const MAX_VARINT: u32 = 268_435_455;

/// Longest string or binary field the format can express (a two byte prefix).
pub(crate) const MAX_FIELD_BYTES: usize = u16::MAX as usize;

/// What the decoder accepts. Every check runs before the thing it guards is
/// allocated or read: an oversized packet is refused from its fixed header, and
/// the counts bound growth while a list is walked, never a reservation made from
/// a number the peer sent.
///
/// The defaults match the listener's defaults. Fields are public so a caller can
/// set them from configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Limits {
    /// Largest whole packet in bytes, fixed header included. This is what a
    /// server advertises as `Maximum Packet Size`.
    pub max_packet_size: u32,
    /// Longest topic name or topic filter in bytes.
    pub max_topic_bytes: usize,
    /// Most levels (separated by `/`) in a topic name or topic filter.
    pub max_topic_levels: usize,
    /// Most filters in one `SUBSCRIBE` or `UNSUBSCRIBE`, and most return codes
    /// in one `SUBACK` or `UNSUBACK`.
    pub max_subscriptions: usize,
    /// Most entries of a repeatable property in one packet: user properties,
    /// and the subscription identifiers of a `PUBLISH`.
    pub max_repeated_properties: usize,
    /// Longest client identifier in bytes.
    pub max_client_id_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_packet_size: 1024 * 1024,
            max_topic_bytes: 1024,
            max_topic_levels: 16,
            max_subscriptions: 64,
            max_repeated_properties: 64,
            max_client_id_bytes: 256,
        }
    }
}
