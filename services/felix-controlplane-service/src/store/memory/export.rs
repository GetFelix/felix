//! Exporting and importing the whole store, for Raft snapshots and `migrate`.
use std::collections::HashMap;

use super::{InMemoryStore, NodeState, PlacementState, ShardState};
use crate::model::{
    Cache, CacheKey, Namespace, NamespaceKey, ShardAssignment, ShardKey, Stream, StreamKey,
};
use crate::store::export::{EXPORTED_STATE_VERSION, ExportedLog, ExportedState};
use crate::store::{StoreError, StoreResult};

impl InMemoryStore {
    /// Serialize the entire store, deterministically.
    pub async fn export_state(&self) -> ExportedState {
        let mut namespaces: Vec<(NamespaceKey, Namespace)> = self
            .namespaces
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        namespaces.sort_by(|a, b| {
            (&a.0.tenant_id, &a.0.namespace).cmp(&(&b.0.tenant_id, &b.0.namespace))
        });

        let mut streams: Vec<(StreamKey, Stream)> = self
            .streams
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        streams.sort_by(|a, b| {
            (&a.0.tenant_id, &a.0.namespace, &a.0.stream).cmp(&(
                &b.0.tenant_id,
                &b.0.namespace,
                &b.0.stream,
            ))
        });

        let mut caches: Vec<(CacheKey, Cache)> = self
            .caches
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        caches.sort_by(|a, b| {
            (&a.0.tenant_id, &a.0.namespace, &a.0.cache).cmp(&(
                &b.0.tenant_id,
                &b.0.namespace,
                &b.0.cache,
            ))
        });

        let (nodes, node_changes, fleet_enabled) = {
            let state = self.nodes.read().await;
            (
                sorted_by_string_key(&state.records),
                ExportedLog::from_log(&state.changes),
                state.fleet_enabled.clone(),
            )
        };

        let (shards, shard_changes, placement_token, placement_holder) = {
            let state = self.shards.read().await;
            let mut records: Vec<(ShardKey, ShardAssignment)> = state
                .records
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            records.sort_by(|a, b| {
                (
                    &a.0.tenant_id,
                    &a.0.namespace,
                    a.0.kind,
                    &a.0.stream,
                    a.0.shard,
                )
                    .cmp(&(
                        &b.0.tenant_id,
                        &b.0.namespace,
                        b.0.kind,
                        &b.0.stream,
                        b.0.shard,
                    ))
            });
            (
                records,
                ExportedLog::from_log(&state.changes),
                state.placement.token,
                state.placement.holder.clone(),
            )
        };

        let mut refresh_tokens: Vec<_> =
            self.refresh_tokens.read().await.values().cloned().collect();
        refresh_tokens
            .sort_by(|a, b| (&a.tenant_id, &a.token_id).cmp(&(&b.tenant_id, &b.token_id)));

        ExportedState {
            v: EXPORTED_STATE_VERSION,
            tenants: sorted_by_string_key(&*self.tenants.read().await),
            namespaces,
            streams,
            caches,
            nodes,
            node_changes,
            shards,
            shard_changes,
            tenant_changes: ExportedLog::from_log(&*self.tenant_changes.read().await),
            namespace_changes: ExportedLog::from_log(&*self.namespace_changes.read().await),
            stream_changes: ExportedLog::from_log(&*self.stream_changes.read().await),
            cache_changes: ExportedLog::from_log(&*self.cache_changes.read().await),
            idp_issuers: sorted_by_string_key(&*self.idp_issuers.read().await),
            tenant_signing_keys: sorted_by_string_key(&*self.tenant_signing_keys.read().await),
            rbac_policies: sorted_by_string_key(&*self.rbac_policies.read().await),
            rbac_groupings: sorted_by_string_key(&*self.rbac_groupings.read().await),
            auth_bootstrapped: sorted_by_string_key(&*self.auth_bootstrapped.read().await),
            moves_paused: *self.moves_paused.read().await,
            fleet_enabled,
            placement_token,
            placement_holder,
            refresh_tokens,
        }
    }

    /// Replace the entire store with a previously exported state.
    ///
    /// Sequence numbers and retained change windows come back exactly, so a
    /// consumer polling `changes(since)` across a restore sees the same
    /// answers — including the same "your checkpoint is too old, resnapshot"
    /// signals — as it would have from the original.
    pub async fn import_state(&self, state: ExportedState) -> StoreResult<()> {
        if state.v > EXPORTED_STATE_VERSION {
            return Err(StoreError::Unexpected(anyhow::anyhow!(
                "exported state version {} is newer than this build's {}",
                state.v,
                EXPORTED_STATE_VERSION
            )));
        }
        let capacity = self.config.change_window();

        *self.tenants.write().await = state.tenants.into_iter().collect();
        *self.namespaces.write().await = state.namespaces.into_iter().collect();
        *self.streams.write().await = state.streams.into_iter().collect();
        *self.caches.write().await = state.caches.into_iter().collect();
        *self.nodes.write().await = NodeState {
            records: state.nodes.into_iter().collect(),
            changes: state.node_changes.into_log(capacity),
            fleet_enabled: state.fleet_enabled,
        };
        *self.shards.write().await = ShardState {
            records: state.shards.into_iter().collect(),
            changes: state.shard_changes.into_log(capacity),
            placement: PlacementState {
                token: state.placement_token,
                holder: state.placement_holder,
                expires_at_millis: 0,
            },
        };
        *self.tenant_changes.write().await = state.tenant_changes.into_log(capacity);
        *self.namespace_changes.write().await = state.namespace_changes.into_log(capacity);
        *self.stream_changes.write().await = state.stream_changes.into_log(capacity);
        *self.cache_changes.write().await = state.cache_changes.into_log(capacity);
        *self.idp_issuers.write().await = state.idp_issuers.into_iter().collect();
        *self.tenant_signing_keys.write().await = state.tenant_signing_keys.into_iter().collect();
        *self.rbac_policies.write().await = state.rbac_policies.into_iter().collect();
        *self.rbac_groupings.write().await = state.rbac_groupings.into_iter().collect();
        *self.auth_bootstrapped.write().await = state.auth_bootstrapped.into_iter().collect();
        *self.moves_paused.write().await = state.moves_paused;
        *self.refresh_tokens.write().await = state
            .refresh_tokens
            .into_iter()
            .map(|token| ((token.tenant_id.clone(), token.token_id.clone()), token))
            .collect();
        // The derived-key cache may hold keys the imported state replaced —
        // an --overwrite restore in a live process would otherwise keep
        // verifying tokens against a world that no longer exists.
        for tenant_id in self.tenant_signing_keys.read().await.keys() {
            crate::auth::felix_token::invalidate_tenant_cache(tenant_id);
        }
        Ok(())
    }

    /// Whether this store has never held anything — no records and no
    /// change-feed history. The guard an import checks before replacing
    /// everything: a store with any past has consumers whose checkpoints an
    /// accidental import would silently invalidate.
    pub async fn is_unused(&self) -> bool {
        self.tenants.read().await.is_empty()
            && self.namespaces.read().await.is_empty()
            && self.streams.read().await.is_empty()
            && self.caches.read().await.is_empty()
            && self.nodes.read().await.records.is_empty()
            && self.shards.read().await.records.is_empty()
            && self.tenant_changes.read().await.next_seq == 0
            && self.namespace_changes.read().await.next_seq == 0
            && self.stream_changes.read().await.next_seq == 0
            && self.cache_changes.read().await.next_seq == 0
            && self.nodes.read().await.changes.next_seq == 0
            && self.shards.read().await.changes.next_seq == 0
    }
}

fn sorted_by_string_key<V: Clone>(map: &HashMap<String, V>) -> Vec<(String, V)> {
    let mut entries: Vec<(String, V)> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}
