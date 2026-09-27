//! Turning a [`TransportConfig`] into quinn's transport and endpoint
//! settings.

use std::sync::Arc;

use super::{TransportConfig, env_u64};

impl TransportConfig {
    pub(crate) fn quinn_transport_config(&self) -> quinn::TransportConfig {
        self.quinn_transport_config_inner(self.initial_mtu, None)
    }

    /// Loopback variant: start at `initial_mtu` and also *guarantee* it via
    /// quinn's `min_mtu`. The guarantee is what defuses the black-hole
    /// detector — its reset target is `min_mtu`, not `initial_mtu`, so
    /// raising only the start size still collapses to 1200 on a false
    /// verdict. Loopback is the one path where the larger payload size is
    /// guaranteed by construction; never set `min_mtu` for a real network.
    pub(crate) fn quinn_transport_config_for_loopback(&self, mtu: u16) -> quinn::TransportConfig {
        self.quinn_transport_config_inner(mtu, Some(mtu))
    }

    fn quinn_transport_config_inner(
        &self,
        initial_mtu: u16,
        guaranteed_mtu: Option<u16>,
    ) -> quinn::TransportConfig {
        // Translate Felix defaults into Quinn transport settings.
        let mut config = quinn::TransportConfig::default();
        let streams = quinn::VarInt::from_u32(self.max_streams as u32);
        config.max_concurrent_bidi_streams(streams);
        config.max_concurrent_uni_streams(streams);
        let stream_window =
            quinn::VarInt::from_u64(self.stream_receive_window).expect("stream receive window");
        let receive_window = quinn::VarInt::from_u64(self.receive_window).expect("receive window");
        config.stream_receive_window(stream_window);
        config.receive_window(receive_window);
        config.send_window(self.send_window);
        if let Some(interval) = self.effective_keep_alive() {
            config.keep_alive_interval(Some(interval));
        }
        if let Some(timeout) = self.max_idle_timeout {
            // Falls back to quinn's own default if the value does not fit a
            // VarInt, rather than failing to build a transport over a knob.
            if let Ok(timeout) = timeout.try_into() {
                config.max_idle_timeout(Some(timeout));
            }
        }
        // Path MTU: start safe, probe high. Fewer, larger datagrams directly
        // reduce per-byte syscall and crypto costs on high-MTU paths.
        let initial_mtu = initial_mtu.clamp(1200, self.max_udp_payload_size);
        config.initial_mtu(initial_mtu);
        if let Some(guaranteed) = guaranteed_mtu {
            config.min_mtu(guaranteed.clamp(1200, initial_mtu));
        }
        // With a guaranteed MTU there is nothing above it to discover, so pin
        // the probe bound to it. This is load-bearing, not an optimization: a
        // probe is full-MTU, bypasses the congestion check, and counts against
        // the window once in flight — on a quiet connection at the two-segment
        // initial window, a doomed probe toward a higher bound starves every
        // ordinary small send behind it ("blocked by congestion control")
        // until its retransmits exhaust, and the search then starts over.
        let mtu_bound = match guaranteed_mtu {
            Some(_) => initial_mtu,
            None => self
                .mtu_discovery_upper_bound
                .clamp(initial_mtu, self.max_udp_payload_size),
        };
        let mut mtud = quinn::MtuDiscoveryConfig::default();
        mtud.upper_bound(mtu_bound);
        // Quinn's MTU black-hole detector cannot tell "large packets are being
        // silently eaten by the path" from "a congestive loss burst dropped a
        // window of large packets". When a sender overruns the receiver's UDP
        // socket buffer (easy at high rate: the standing queue sits within a
        // couple MB of the buffer size, so one scheduling stall overflows it),
        // every lost packet is full-MTU, the detector calls it a black hole,
        // and the path MTU collapses to `initial_mtu`. With quinn's default
        // 60 s cooldown the connection then pays ~13x the datagrams (and
        // syscalls) per byte for a minute — measured as a 5-6x throughput
        // collapse that is indistinguishable from a scheduling defect. A short
        // cooldown re-probes within seconds and restores the discovered MTU;
        // on a genuine black-hole path the extra cost is one loss-tolerant
        // probe packet per cooldown.
        let cooldown_ms = env_u64("FELIX_MTU_BLACK_HOLE_COOLDOWN_MS")
            .map(|value| value.max(100))
            .unwrap_or(2_000);
        mtud.black_hole_cooldown(std::time::Duration::from_millis(cooldown_ms));
        config.mtu_discovery_config(Some(mtud));
        // ACK frequency extension (quinn peers only): cap ACK delay well below
        // the RFC's 25 ms — a window-limited sender resumes only on an ACK, so
        // delayed ACKs stall the whole pipeline — and ACK less often than every
        // other packet, since each reverse-path ACK costs a datagram plus its
        // wakeup chain (+15% throughput measured at threshold 20).
        if env_u64("FELIX_ACK_FREQ_DISABLE").is_none() {
            let mut ack_frequency = quinn::AckFrequencyConfig::default();
            ack_frequency.max_ack_delay(Some(std::time::Duration::from_millis(2)));
            let threshold = env_u64("FELIX_ACK_ELICITING_THRESHOLD").unwrap_or(20);
            ack_frequency.ack_eliciting_threshold(
                quinn::VarInt::from_u64(threshold.min(u32::MAX as u64)).expect("threshold fits"),
            );
            config.ack_frequency_config(Some(ack_frequency));
        }
        // Quinn follows RFC 9002 by default and raises the minimum window to
        // two datagrams when path-MTU discovery finds a larger MTU. Keep that
        // safe default unless a trusted low-loss deployment explicitly opts
        // into a larger initial burst.
        if let Some(window) = self.initial_congestion_window_bytes {
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(window);
            config.congestion_controller_factory(Arc::new(cubic));
        } else if u64::from(initial_mtu) * 2 > 14_720 {
            // Quinn's default initial congestion window is a flat 14,720 bytes
            // (RFC 9002's constant, sized for ~1200-byte datagrams) and its
            // send path reserves a full segment per datagram — so an initial
            // MTU larger than the window deadlocks the connection before the
            // first packet ("blocked by congestion control", forever). Scale
            // the window with the datagram size using RFC 9002's own formula.
            let mtu = u64::from(initial_mtu);
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(14_720u64.clamp(2 * mtu, 10 * mtu));
            config.congestion_controller_factory(Arc::new(cubic));
        }
        config
    }

    pub(crate) fn quinn_endpoint_config(&self) -> quinn::EndpointConfig {
        let mut config = quinn::EndpointConfig::default();
        // Accept datagrams up to the configured bound (quinn's default of 1472
        // would silently cap peers that discovered a larger path MTU).
        if let Err(err) = config.max_udp_payload_size(self.max_udp_payload_size) {
            tracing::warn!(error = %err, "invalid max_udp_payload_size; using quinn default");
        }
        config
    }
}
