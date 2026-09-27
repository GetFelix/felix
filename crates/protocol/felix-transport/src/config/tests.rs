use super::*;

/// quinn's `MAX_TRANSMIT_SEGMENTS`, which is private to quinn and so cannot
/// be read through its API. Restated here because the whole bound depends
/// on it, and a change to it upstream is exactly what would break us.
const QUINN_MAX_TRANSMIT_SEGMENTS: u32 = 10;

/// One `sendmsg` batch is one IP datagram, whatever GSO splits it into.
const IP_DATAGRAM_MAX: u32 = 65535;

#[test]
fn default_transport_config() {
    // Basic sanity checks on defaults.
    let config = TransportConfig::default();
    assert!(config.max_frame_bytes > 0);
    assert!(config.max_streams > 0);
}

/// The default keep-alive must fit inside the idle window with room to lose
/// a packet or two.
///
/// These two numbers are a pair. Raising the idle timeout without raising
/// the keep-alive is harmless; lowering the idle timeout below the
/// keep-alive silently reintroduces the bug this exists to prevent.
#[test]
fn the_keep_alive_fits_inside_the_idle_window() {
    let config = TransportConfig::default();
    let keep_alive = config.keep_alive_interval.expect("a keep-alive by default");
    let idle = config.max_idle_timeout.expect("an idle timeout by default");
    assert!(
        keep_alive * 3 <= idle,
        "keep-alive {keep_alive:?} leaves no margin inside idle timeout {idle:?}",
    );
    assert_eq!(config.effective_keep_alive(), Some(keep_alive));
}

/// The idle timeout is how long a publish waits on a leader that died without
/// closing, so the default must leave room to fail over well inside ten
/// seconds, while the keep-alive stays under half of it.
#[test]
fn the_defaults_notice_a_dead_peer_quickly_and_keep_a_quiet_one() {
    let config = TransportConfig {
        max_idle_timeout: Some(DEFAULT_MAX_IDLE_TIMEOUT),
        keep_alive_interval: Some(DEFAULT_KEEP_ALIVE_INTERVAL),
        ..TransportConfig::default()
    };
    assert!(DEFAULT_MAX_IDLE_TIMEOUT <= Duration::from_secs(6));
    assert_eq!(
        config.effective_keep_alive(),
        Some(DEFAULT_KEEP_ALIVE_INTERVAL)
    );
}

#[test]
fn a_keep_alive_too_close_to_the_idle_timeout_is_replaced() {
    let idle = Duration::from_secs(9);
    let with = |keep_alive| TransportConfig {
        max_idle_timeout: Some(idle),
        keep_alive_interval: keep_alive,
        ..TransportConfig::default()
    };
    assert_eq!(
        with(Some(Duration::from_secs(4))).effective_keep_alive(),
        Some(Duration::from_secs(4))
    );
    for bad in [
        Some(Duration::from_millis(4_500)),
        Some(Duration::from_secs(20)),
        Some(Duration::ZERO),
    ] {
        assert_eq!(
            with(bad).effective_keep_alive(),
            Some(Duration::from_secs(3)),
            "{bad:?}"
        );
    }
    assert_eq!(with(None).effective_keep_alive(), None, "off stays off");
    let no_idle = TransportConfig {
        max_idle_timeout: None,
        keep_alive_interval: Some(Duration::from_secs(4)),
        ..TransportConfig::default()
    };
    assert_eq!(no_idle.effective_keep_alive(), Some(Duration::from_secs(4)));
}

#[test]
fn transport_config_custom_values() {
    let config = TransportConfig {
        max_frame_bytes: 8 * 1024 * 1024,
        max_streams: 2048,
        receive_window: 128 * 1024 * 1024,
        stream_receive_window: 32 * 1024 * 1024,
        send_window: 128 * 1024 * 1024,
        ..TransportConfig::default()
    };
    assert_eq!(config.max_frame_bytes, 8 * 1024 * 1024);
    assert_eq!(config.max_streams, 2048);
    assert_eq!(config.receive_window, 128 * 1024 * 1024);
    assert_eq!(config.stream_receive_window, 32 * 1024 * 1024);
    assert_eq!(config.send_window, 128 * 1024 * 1024);
}

#[test]
fn transport_config_mtu_defaults_are_probe_high_start_safe() {
    let config = TransportConfig::default();
    // Start at the RFC-safe minimum unless explicitly overridden.
    assert!(config.initial_mtu >= 1200);
    // Probe up to the QUIC maximum so high-MTU paths (loopback, jumbo LAN)
    // converge to their real MTU.
    assert!(config.mtu_discovery_upper_bound >= config.initial_mtu);
    assert!(config.max_udp_payload_size >= config.mtu_discovery_upper_bound);
    // Building quinn configs must not panic for the defaults.
    let _ = config.quinn_transport_config();
    let _ = config.quinn_endpoint_config();
}

#[test]
fn transport_config_initial_cwnd_applies_without_panic() {
    let config = TransportConfig {
        initial_congestion_window_bytes: Some(4 * 1024 * 1024),
        ..TransportConfig::default()
    };
    let _ = config.quinn_transport_config();
}

/// **The default MTU bound must keep a GSO batch inside one IP datagram.**
///
/// Above it Linux rejects every batch with `EMSGSIZE`, and quinn falls back
/// off segmentation only on `EIO`/`EINVAL` -- so the transmit is dropped
/// after quinn has counted it as sent, and delivery stalls permanently
/// rather than degrading. The bound was 16384 for a while, which is over
/// the line; loopback was capped separately but a routed jumbo-frame path
/// was not.
///
/// The investigation that found this said no test could catch a regression
/// in the invariant. One can catch the part that matters: that the default
/// we ship still fits.
#[test]
fn the_default_mtu_bound_fits_a_gso_batch() {
    // The non-macOS value specifically, whatever host is running this:
    // macOS has no GSO and no aggregate limit, so its 16336 is fine and
    // would make this vacuous on a developer's machine.
    let bound = mtu_discovery_upper_bound_for(false);
    let aggregate = u32::from(bound) * QUINN_MAX_TRANSMIT_SEGMENTS;
    assert!(
        aggregate <= IP_DATAGRAM_MAX,
        "an MTU of {bound} batches to {aggregate} bytes, \
         over the {IP_DATAGRAM_MAX} an IP datagram holds. Linux answers EMSGSIZE, \
         quinn does not recognise it as a GSO failure, and delivery stalls for good.",
    );
}

/// And with margin: the ceiling moves if quinn's batch size does.
///
/// 6553 is the exact limit at 10 segments and breaks the moment that rises.
/// The margin is the reason the default is 4096 rather than the largest
/// value that happens to work today.
#[test]
fn the_default_mtu_bound_survives_a_larger_batch() {
    let grown = QUINN_MAX_TRANSMIT_SEGMENTS + 5;
    let aggregate = u32::from(mtu_discovery_upper_bound_for(false)) * grown;
    assert!(
        aggregate <= IP_DATAGRAM_MAX,
        "the default has no margin: at {grown} segments it batches to {aggregate} bytes. \
         Pick a bound that survives quinn changing MAX_TRANSMIT_SEGMENTS, because \
         nothing here will notice when it does.",
    );
}

/// An MTU override cannot reopen the GSO stall the default avoids: on Linux it
/// is clamped to what one batch of quinn's segments can carry.
#[test]
fn an_mtu_override_is_clamped_below_the_gso_ceiling() {
    let ceiling = mtu_override_ceiling_for(false);
    // The batch is one UDP datagram, so the IPv4 and UDP headers come out of
    // the 65535: 6553 * 10 fits the IP limit but not the payload.
    let udp_payload_max = IP_DATAGRAM_MAX - 20 - 8;
    assert!(u32::from(ceiling) * QUINN_MAX_TRANSMIT_SEGMENTS <= udp_payload_max);
    assert_eq!(
        clamp_mtu_override("FELIX_MTU_UPPER_BOUND", 16384, ceiling),
        ceiling
    );
    assert_eq!(
        clamp_mtu_override("FELIX_MTU_UPPER_BOUND", 4096, ceiling),
        4096
    );
    assert_eq!(clamp_mtu_override("FELIX_INITIAL_MTU", 100, ceiling), 1200);
    // macOS keeps the QUIC maximum.
    assert_eq!(
        clamp_mtu_override(
            "FELIX_MTU_UPPER_BOUND",
            16384,
            mtu_override_ceiling_for(true)
        ),
        16384
    );
}
