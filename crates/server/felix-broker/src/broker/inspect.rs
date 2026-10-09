//! Listing the subscriptions this broker serves, for an operator.
//!
//! Read from each shard's fanout snapshot, the list a publish already reads
//! without a lock. Nothing here walks a queue or waits on a subscriber.

use std::sync::Arc;

use felix_wire::{InspectedSubscription, SubscriptionCursor, SubscriptionFilter};

use super::Broker;
use super::keys::TopicKey;
use crate::stream::{StreamState, SubQueuePolicy, SubscriberEntry};

/// One page of subscriptions, and where the next one starts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionPage {
    pub subscriptions: Vec<InspectedSubscription>,
    /// `None` on the last page.
    pub next_cursor: Option<SubscriptionCursor>,
}

impl Broker {
    /// Up to `limit` subscriptions matching `filter`, ordered by shard and
    /// then subscriber id, starting after `after`.
    pub async fn list_subscriptions(
        &self,
        filter: &SubscriptionFilter,
        after: Option<&SubscriptionCursor>,
        limit: usize,
    ) -> SubscriptionPage {
        let start = after.map(cursor_key);
        let mut shards: Vec<(TopicKey, Arc<StreamState>)> = self
            .topics
            .read()
            .await
            .iter()
            .filter(|(key, state)| {
                state.active.load(std::sync::atomic::Ordering::Acquire)
                    && shard_matches(filter, key)
                    && start
                        .as_ref()
                        .is_none_or(|start| order(key) >= order(start))
            })
            .map(|(key, state)| (key.clone(), Arc::clone(state)))
            .collect();
        shards.sort_unstable_by(|(a, _), (b, _)| order(a).cmp(&order(b)));

        let mut page = SubscriptionPage::default();
        for (key, state) in shards {
            let after_id = after
                .filter(|_| start.as_ref() == Some(&key))
                .map(|cursor| cursor.subscriber_id);
            let snapshot = state.subscribers_snapshot.load();
            // Read once per shard, and only for a shard with someone to show.
            let mut tail = None;
            for entry in snapshot.iter() {
                if after_id.is_some_and(|after| entry.id <= after)
                    || entry.sender.is_closed()
                    || !subscriber_matches(filter, entry)
                {
                    continue;
                }
                if page.subscriptions.len() == limit {
                    page.next_cursor = page.subscriptions.last().map(cursor_of);
                    return page;
                }
                let tail = *tail.get_or_insert_with(|| state.tail_seq());
                page.subscriptions.push(describe(&key, &state, entry, tail));
            }
        }
        page
    }
}

fn shard_matches(filter: &SubscriptionFilter, key: &TopicKey) -> bool {
    filter
        .tenant_id
        .as_ref()
        .is_none_or(|t| *t == key.tenant_id)
        && filter
            .namespace
            .as_ref()
            .is_none_or(|n| *n == key.namespace)
        && filter.stream.as_ref().is_none_or(|s| *s == key.stream)
        && filter.shard.is_none_or(|shard| shard == key.shard)
}

fn subscriber_matches(filter: &SubscriptionFilter, entry: &SubscriberEntry) -> bool {
    (!filter.dropping || entry.stats.dropped_records() > 0)
        && filter.principal.as_ref().is_none_or(|principal| {
            entry
                .stats
                .owner()
                .and_then(|owner| owner.principal.as_ref())
                == Some(principal)
        })
}

fn describe(
    key: &TopicKey,
    state: &StreamState,
    entry: &SubscriberEntry,
    tail: u64,
) -> InspectedSubscription {
    let owner = entry.stats.owner();
    let capacity = entry.sender.max_capacity();
    InspectedSubscription {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        subscriber_id: entry.id,
        subscription_id: owner.map(|owner| owner.subscription_id),
        connection: owner.map(|owner| owner.connection_id),
        peer: owner.map(|owner| owner.peer.clone()),
        principal: owner.and_then(|owner| owner.principal.clone()),
        policy: match state.subscriber_queue_policy {
            SubQueuePolicy::Block => "block",
            SubQueuePolicy::DropNew => "drop_new",
            SubQueuePolicy::DropOld => "drop_old",
        }
        .to_string(),
        depth: capacity.saturating_sub(entry.sender.capacity()) as u64,
        capacity: capacity as u64,
        dropped: entry.stats.dropped_records(),
        position: entry.stats.position(),
        tail,
        age_ms: u64::try_from(entry.stats.age().as_millis()).unwrap_or(u64::MAX),
    }
}

fn cursor_key(cursor: &SubscriptionCursor) -> TopicKey {
    TopicKey::new(
        cursor.tenant_id.clone(),
        cursor.namespace.clone(),
        cursor.stream.clone(),
        cursor.shard,
    )
}

fn cursor_of(subscription: &InspectedSubscription) -> SubscriptionCursor {
    SubscriptionCursor {
        tenant_id: subscription.tenant_id.clone(),
        namespace: subscription.namespace.clone(),
        stream: subscription.stream.clone(),
        shard: subscription.shard,
        subscriber_id: subscription.subscriber_id,
    }
}

fn order(key: &TopicKey) -> (&str, &str, &str, u32) {
    (&key.tenant_id, &key.namespace, &key.stream, key.shard)
}

#[cfg(test)]
mod tests;
