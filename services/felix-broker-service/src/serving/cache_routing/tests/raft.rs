//! A cache shard a Raft group runs answers every cache and counter operation
//! as the hybrid's store does.

use std::time::Duration;

use felix_storage::CacheCondition;

use super::*;
use crate::serving::forward::CacheRequest;
use crate::shards::raft::tests as fixture;
use crate::shards::routing::EngineShards;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Versions differ between the engines; the order they were first seen in
/// does not.
#[derive(Default)]
struct Versions(Vec<u64>);

impl Versions {
    fn name(&mut self, version: Option<u64>) -> String {
        let Some(version) = version else {
            return "none".to_string();
        };
        let at = match self.0.iter().position(|seen| *seen == version) {
            Some(at) => at,
            None => {
                self.0.push(version);
                self.0.len() - 1
            }
        };
        format!("v{at}")
    }

    fn get(&self, name: usize) -> u64 {
        self.0[name]
    }
}

/// Run one script of operations and write down what each answered.
async fn script(
    broker: &felix_broker::Broker,
    engine: &StreamEngine,
    ingress: Option<&IngressRouter>,
) -> Vec<String> {
    let mut versions = Versions::default();
    let mut said = Vec::new();
    let cache = |request: CacheRequest| async move {
        apply_cache_request(
            broker,
            (engine, TIMEOUT),
            ingress,
            None,
            "",
            TENANT,
            NAMESPACE,
            CACHE,
            "k",
            request,
            None,
        )
        .await
        .expect("served")
    };
    let counter = |key: &'static str, request: CacheRequest| async move {
        apply_counter_op(
            broker,
            (engine, TIMEOUT),
            ingress,
            None,
            "",
            TENANT,
            NAMESPACE,
            CACHE,
            key,
            request,
        )
        .await
        .expect("served")
    };
    let put_if = |value: &'static [u8], condition| CacheRequest::PutIf {
        value: Bytes::from_static(value),
        ttl_ms: 0,
        condition,
    };
    let mut note = |what: &str, answer: CacheAnswer, versions: &mut Versions| {
        let outcome = answer
            .outcome
            .map(|outcome| format!("{} {}", outcome.applied, versions.name(outcome.version)));
        said.push(format!("{what}: {:?} {outcome:?}", answer.value));
    };

    note("get", cache(CacheRequest::Get).await, &mut versions);
    note(
        "put",
        cache(put_request(Bytes::from_static(b"one"), None)).await,
        &mut versions,
    );
    note("get", cache(CacheRequest::Get).await, &mut versions);
    note(
        "versioned",
        cache(CacheRequest::GetVersioned).await,
        &mut versions,
    );
    let first = versions.get(0);
    let second = put_if(b"two", CacheCondition::Version(first));
    note("put if v0", cache(second).await, &mut versions);
    let stale = put_if(b"three", CacheCondition::Version(first));
    note("put if v0 again", cache(stale).await, &mut versions);
    let present = put_if(b"four", CacheCondition::Absent);
    note("put if absent", cache(present).await, &mut versions);
    let stale = CacheRequest::DeleteIf { version: first };
    note("delete if v0", cache(stale).await, &mut versions);
    let current = CacheRequest::DeleteIf {
        version: versions.get(1),
    };
    note("delete if v1", cache(current).await, &mut versions);
    let absent = put_if(b"five", CacheCondition::Absent);
    note("put if absent", cache(absent).await, &mut versions);
    note("delete", cache(CacheRequest::Delete).await, &mut versions);
    note("delete", cache(CacheRequest::Delete).await, &mut versions);
    note(
        "versioned",
        cache(CacheRequest::GetVersioned).await,
        &mut versions,
    );

    let add = |delta| CacheRequest::CounterAdd { delta };
    said.push(format!("add: {:?}", counter("hits", add(5)).await));
    said.push(format!("add: {:?}", counter("hits", add(-2)).await));
    said.push(format!(
        "sum: {:?}",
        counter("hits", CacheRequest::CounterGet).await
    ));
    said.push(format!(
        "sum: {:?}",
        counter("misses", CacheRequest::CounterGet).await
    ));
    said
}

/// **Every cache and counter operation answers on a Raft cache shard as it
/// does on the hybrid's store**: values, applied flags, which version a
/// refusal names, the value a plain delete removed, and counter sums.
#[tokio::test]
async fn a_raft_cache_shard_answers_as_the_hybrid_does() {
    let hybrid_root = tempfile::tempdir().expect("tempdir");
    let hybrid = fixture::cache_broker(hybrid_root.path(), CACHE).await;
    let on_hybrid = script(&hybrid, &StreamEngine::default(), None).await;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::cache_broker(root.path(), CACHE).await;
    let shards = fixture::shards(&broker);
    let key = cache_key(0);
    let assigned = fixture::assigned(key.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&key).is_some()).await;
    let engine = fixture::engine(&shards);
    let ingress = fixture::ingress(&shards);
    let on_raft = script(&broker, &engine, Some(&ingress)).await;

    assert_eq!(on_raft, on_hybrid);
    assert!(
        engine.on_raft(&key),
        "the cache shard did not run on its Raft group"
    );
    // The hybrid's store never opened the shard: its directory is the
    // member's, and the counters live in the member's log.
    assert!(
        broker
            .cache()
            .get(TENANT, NAMESPACE, CACHE, 0, "k")
            .await
            .is_err(),
        "the hybrid store opened a Raft member's directory"
    );
    let counters = broker.counters().expect("counters");
    assert_eq!(
        counters
            .get(TENANT, NAMESPACE, CACHE, 0, "hits")
            .await
            .ok()
            .flatten(),
        None,
        "a counter add reached the hybrid's counter store"
    );
}

/// **A cache operation routed at a term this broker no longer serves is
/// refused as not ready, with nothing written.**
#[tokio::test]
async fn a_raft_cache_write_at_another_term_is_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::cache_broker(root.path(), CACHE).await;
    let shards = fixture::shards(&broker);
    let key = cache_key(0);
    let assigned = fixture::assigned(key.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&key).is_some()).await;
    let engine = fixture::engine(&shards);
    let term = shards.leading(&key).expect("led");

    let refused = apply_cache_local(
        &broker,
        (&engine, TIMEOUT),
        None,
        TENANT,
        NAMESPACE,
        CACHE,
        "k",
        put_request(Bytes::from_static(b"stale"), None),
        None,
        0,
        term + 1,
        None,
    )
    .await
    .expect_err("written at a term it was not routed at");
    let client = ClientError::from_anyhow(&refused);
    assert_eq!(client.retry(), felix_wire::RetryClass::Retry, "{client:?}");

    let ingress = fixture::ingress(&shards);
    let found = apply_cache_request(
        &broker,
        (&engine, TIMEOUT),
        Some(&ingress),
        None,
        "",
        TENANT,
        NAMESPACE,
        CACHE,
        "k",
        CacheRequest::Get,
        None,
    )
    .await
    .expect("read");
    assert_eq!(found.value, None, "the refused put was written");
}
