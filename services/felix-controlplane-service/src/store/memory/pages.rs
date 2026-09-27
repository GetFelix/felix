//! Paged listings. The maps hold no order, so each page sorts what is past
//! the cursor; fine at the sizes an in-memory store holds.
use super::InMemoryStore;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{Cache, Namespace, Node, ShardAssignment, ShardKey, Stream, Tenant};
use crate::store::{Page, PageRequest, StoreResult};

pub(super) async fn tenants(
    store: &InMemoryStore,
    page: PageRequest<String>,
) -> StoreResult<Page<Tenant>> {
    let tenants = store.tenants.read().await;
    Ok(Page::from_unordered(
        tenants.values().cloned(),
        |tenant| tenant.tenant_id.clone(),
        page.after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn namespaces(
    store: &InMemoryStore,
    tenant_id: &str,
    page: PageRequest<String>,
) -> StoreResult<Page<Namespace>> {
    let namespaces = store.namespaces.read().await;
    Ok(Page::from_unordered(
        namespaces
            .values()
            .filter(|ns| ns.tenant_id == tenant_id)
            .cloned(),
        |ns| ns.namespace.clone(),
        page.after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn streams(
    store: &InMemoryStore,
    tenant_id: &str,
    namespace: &str,
    page: PageRequest<String>,
) -> StoreResult<Page<Stream>> {
    let streams = store.streams.read().await;
    Ok(Page::from_unordered(
        streams
            .values()
            .filter(|stream| stream.tenant_id == tenant_id && stream.namespace == namespace)
            .cloned(),
        |stream| stream.stream.clone(),
        page.after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn caches(
    store: &InMemoryStore,
    tenant_id: &str,
    namespace: &str,
    page: PageRequest<String>,
) -> StoreResult<Page<Cache>> {
    let caches = store.caches.read().await;
    Ok(Page::from_unordered(
        caches
            .values()
            .filter(|cache| cache.tenant_id == tenant_id && cache.namespace == namespace)
            .cloned(),
        |cache| cache.cache.clone(),
        page.after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn nodes(
    store: &InMemoryStore,
    page: PageRequest<String>,
) -> StoreResult<Page<Node>> {
    let nodes = store.nodes.read().await;
    Ok(Page::from_unordered(
        nodes.records.values().cloned(),
        |node| node.node_id.clone(),
        page.after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn shard_assignments(
    store: &InMemoryStore,
    leader: Option<&str>,
    page: PageRequest<ShardKey>,
) -> StoreResult<Page<ShardAssignment>> {
    let shards = store.shards.read().await;
    let after = page.after.as_ref().map(ShardKey::page_order);
    Ok(Page::from_unordered(
        shards
            .records
            .values()
            .filter(|assignment| leader.is_none_or(|leader| assignment.leader == leader))
            .cloned(),
        |assignment| assignment.key.page_order(),
        after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn rbac_policies(
    store: &InMemoryStore,
    tenant_id: &str,
    page: PageRequest<PolicyRule>,
) -> StoreResult<Page<PolicyRule>> {
    let policies = store.rbac_policies.read().await;
    let mut rules: Vec<PolicyRule> = policies.get(tenant_id).cloned().unwrap_or_default();
    // Postgres keys the table on the rule, so a duplicate cannot exist there;
    // it would also be indistinguishable from its twin across a page edge.
    rules.sort_by_key(policy_order);
    rules.dedup();
    let after = page.after.as_ref().map(policy_order);
    Ok(Page::from_unordered(
        rules,
        policy_order,
        after.as_ref(),
        page.limit,
    ))
}

pub(super) async fn rbac_groupings(
    store: &InMemoryStore,
    tenant_id: &str,
    page: PageRequest<GroupingRule>,
) -> StoreResult<Page<GroupingRule>> {
    let groupings = store.rbac_groupings.read().await;
    let mut rules: Vec<GroupingRule> = groupings.get(tenant_id).cloned().unwrap_or_default();
    rules.sort_by_key(grouping_order);
    rules.dedup();
    let after = page.after.as_ref().map(grouping_order);
    Ok(Page::from_unordered(
        rules,
        grouping_order,
        after.as_ref(),
        page.limit,
    ))
}

fn policy_order(rule: &PolicyRule) -> (String, String, String) {
    (
        rule.subject.clone(),
        rule.object.clone(),
        rule.action.clone(),
    )
}

fn grouping_order(rule: &GroupingRule) -> (String, String) {
    (rule.user.clone(), rule.role.clone())
}
