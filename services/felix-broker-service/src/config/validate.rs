//! Checks on settings that are each fine alone and wrong together.

use anyhow::Result;

use super::BrokerConfig;

impl BrokerConfig {
    /// Settings that are each fine alone and wrong together.
    ///
    /// Every knob validates its own value where it is parsed. Nothing looked at
    /// *pairs*, which is where the confusing failures live — a setting that
    /// never takes effect, or one that produces output the other end will not
    /// accept. Neither shows up as an error at the time; both show up later as
    /// behaviour nobody configured (#416).
    ///
    /// Refusing at startup is the same choice the peer transport already makes
    /// for a shared port: a broker that will not do what its configuration says
    /// should say so while someone is watching.
    pub fn validate(&self) -> Result<()> {
        if self.event_batch_max_bytes > self.max_frame_bytes {
            anyhow::bail!(
                "event_batch_max_bytes ({}) exceeds max_frame_bytes ({}): this \
                 broker would send subscribers frames larger than it will itself \
                 accept, and a client applying the same limit drops them",
                self.event_batch_max_bytes,
                self.max_frame_bytes,
            );
        }
        if self.pub_conn_inflight_bytes > self.pub_inflight_bytes {
            anyhow::bail!(
                "pub_conn_inflight_bytes ({}) exceeds pub_inflight_bytes ({}): \
                 the per-connection limit can never be the one that applies, so \
                 one connection may take the whole broker-wide allowance",
                self.pub_conn_inflight_bytes,
                self.pub_inflight_bytes,
            );
        }
        if self.cache_stream_recv_window > self.cache_conn_recv_window {
            anyhow::bail!(
                "cache_stream_recv_window ({}) exceeds cache_conn_recv_window \
                 ({}): a single stream can never reach its own window, because \
                 the connection's runs out first",
                self.cache_stream_recv_window,
                self.cache_conn_recv_window,
            );
        }
        self.validate_io_runtime_covers_every_listener()?;
        self.validate_credential_can_outlive_itself()?;
        self.validate_peers_are_authenticated()?;
        self.client_tls.validate()?;
        super::tls::validate_controlplane_ca(
            self.controlplane_ca.as_deref(),
            self.controlplane_url.as_deref(),
        )?;
        Ok(())
    }

    /// Refuse to join a cluster without peer mTLS unless told to.
    ///
    /// The internal listener binds whenever this broker has a node id, and
    /// without mTLS anything that reaches it is a peer: it can forward writes
    /// and send `ReplicateRebuild`, which discards a follower's log. Keyed on
    /// membership rather than replication factor because the broker learns
    /// replication factors from the control plane after it is already
    /// listening, and an RF=1 cluster still forwards over this port.
    fn validate_peers_are_authenticated(&self) -> Result<()> {
        match &self.peer_transport {
            Some(peer) if peer.tls.is_none() && !peer.allow_unauthenticated => anyhow::bail!(
                "this broker joins a cluster (FELIX_NODE_ID is set) without peer mTLS:                  anything that can reach FELIX_INTERNAL_BIND could act as a broker and                  rewrite replicas. Set FELIX_INTERNAL_TLS_CERT, FELIX_INTERNAL_TLS_KEY                  and FELIX_INTERNAL_TLS_CA, or set FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true                  if the internal port is reachable from brokers only"
            ),
            _ => Ok(()),
        }
    }

    /// Refuse an I/O runtime pool too small for the listeners that will use it.
    ///
    /// An endpoint's driver is a single task, so two endpoints on one runtime
    /// share its one thread. `io_runtime_index` gives a server endpoint
    /// `sequence % (pool_len - 1)`, which for a pool of 2 is `% 1` -- every
    /// listener on the same thread, which is the single feeder that binding
    /// several listeners exists to escape.
    ///
    /// Each setting is fine alone. `FELIX_IO_RUNTIME_THREADS=1` isolates a
    /// driver; `FELIX_QUIC_LISTENERS=4` asks for four. Together they quietly
    /// collapse the four onto one, and the only symptom is throughput that does
    /// not improve -- which reads as the listeners being pointless rather than
    /// as a misconfiguration.
    ///
    /// Unset is not a conflict: the pool is derived from the listener count, so
    /// there is nothing to disagree with.
    fn validate_io_runtime_covers_every_listener(&self) -> Result<()> {
        let Some(configured) = self.io_runtime_threads else {
            return Ok(());
        };
        // Zero is the documented way to turn the pool off entirely and put
        // drivers back on the app runtime, where tokio spreads them. That is
        // the Linux default and not a conflict.
        if configured == 0 {
            return Ok(());
        }
        let needed = felix_transport::required_io_runtime_threads(self.server_endpoints());
        if configured < needed {
            anyhow::bail!(
                "FELIX_IO_RUNTIME_THREADS ({configured}) is too small for \
                 FELIX_QUIC_LISTENERS ({}): this broker binds {} server \
                 endpoints and every driver past the first would share a thread \
                 with another, which is the single feeder several listeners \
                 exist to escape. Use {needed}, or 0 to put drivers on the app \
                 runtime",
                self.quic_listeners,
                self.server_endpoints(),
            );
        }
        Ok(())
    }

    /// Refuse a credential that will expire with nothing able to renew it.
    ///
    /// The broker's control-plane calls all read one token, and the heartbeat is
    /// among them -- and the heartbeat *is* the lease renewal. So an expired
    /// credential is not a degraded broker, it is one that stops serving the
    /// shards it led once the lease lapses. That is the correct, safe outcome;
    /// what is not correct is finding out about it an hour into a deployment
    /// nobody touched.
    ///
    /// Two things can keep a token alive: the refresh loop
    /// (`FELIX_NODE_REFRESH_TOKEN_FILE`), or something outside the broker
    /// rewriting `FELIX_NODE_TOKEN_FILE`, which is re-read. With neither, and a
    /// token that says when it expires, the outage is already scheduled.
    ///
    /// A token passed by value is not a seam anything can write, which is why
    /// the file is what counts rather than merely having a token.
    fn validate_credential_can_outlive_itself(&self) -> Result<()> {
        let Some(membership) = self.membership.as_ref() else {
            // Not joining a cluster: no heartbeat, no lease, nothing to lose.
            return Ok(());
        };
        if membership.refresh_token_file.is_some() || membership.node_token_file.is_some() {
            return Ok(());
        }
        // Only a token that says when it expires. One this broker cannot read
        // the claims of is someone else's format, and guessing is worse than
        // letting it run.
        let Some(expires_at) = crate::cluster::credential::read_claims(&self.controlplane_token)
            .map(|claims| claims.exp)
        else {
            return Ok(());
        };
        anyhow::bail!(
            "the node credential expires (exp {expires_at}) and nothing can renew it: \
             FELIX_NODE_TOKEN was passed by value, and neither \
             FELIX_NODE_REFRESH_TOKEN_FILE nor FELIX_NODE_TOKEN_FILE is set. The \
             heartbeat carries this token and the heartbeat is the lease renewal, so \
             when it expires this broker stops serving the shards it leads. Set \
             FELIX_NODE_REFRESH_TOKEN_FILE to refresh it, or write the token to \
             FELIX_NODE_TOKEN_FILE and have whatever mints it rewrite that path -- \
             the broker re-reads it.",
        );
    }
}
