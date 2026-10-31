//! Conditional head trims, sent to whichever broker leads the shard.

use anyhow::Result;

use super::Client;
use crate::ConditionalTrim;

impl Client {
    /// [`crate::Connection::trim_before`], on whichever broker leads `shard`.
    /// Never forwarded between brokers; this follows the leader's redirect
    /// instead.
    ///
    /// Asked again only after a coded `not_leader`, which the broker answers
    /// before it writes anything. A stale tail is returned as
    /// [`ConditionalTrim::Refused`] and never retried, and `unacknowledged`
    /// is returned as it came because the trim may have landed.
    #[allow(clippy::too_many_arguments)]
    pub async fn trim_before(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        before: u64,
        expected_offset: u64,
        headers: Option<&[u8]>,
    ) -> Result<ConditionalTrim> {
        self.on_write_shard(
            (
                tenant_id.to_owned(),
                namespace.to_owned(),
                stream.to_owned(),
                shard,
            ),
            |client| async move {
                client
                    .trim_before(
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        before,
                        expected_offset,
                        headers,
                    )
                    .await
            },
        )
        .await
    }
}
