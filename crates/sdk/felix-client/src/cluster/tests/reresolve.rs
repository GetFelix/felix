//! A cluster client given names finds its brokers again after every one of
//! them has moved.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_wire::{AckMode, Message};
use tokio::sync::Notify;

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::nodes::Nodes;
use crate::cluster::seeds::{Lookup, RESOLVE_AGAIN_AFTER, Seeds};
use crate::cluster::{ClusterClient, Seed};
use crate::test_support::build_server_config;

const NAME: &str = "brokers.test:5000";

/// A resolver whose answers the test moves.
#[derive(Default)]
struct FakeDns {
    answers: Mutex<HashMap<String, Vec<SocketAddr>>>,
    /// Names whose lookup never completes, as with a resolver that has
    /// stopped answering.
    hanging: Mutex<HashSet<String>>,
    /// Notified when a hanging lookup starts.
    hung: Notify,
    /// How long every answer takes.
    delay: Mutex<Duration>,
    asked: Mutex<Vec<String>>,
    lookups: AtomicUsize,
}

impl FakeDns {
    fn point(&self, name: &str, addrs: Vec<SocketAddr>) {
        self.answers.lock().unwrap().insert(name.to_string(), addrs);
    }

    fn hang(&self, name: &str) {
        self.hanging.lock().unwrap().insert(name.to_string());
    }

    fn asked(&self) -> Vec<String> {
        let mut asked = self.asked.lock().unwrap().clone();
        asked.sort();
        asked
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }
}

impl Lookup for FakeDns {
    fn lookup<'a>(
        &'a self,
        host_port: &'a str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'a>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        self.asked.lock().unwrap().push(host_port.to_string());
        let answer = self.answers.lock().unwrap().get(host_port).cloned();
        let hangs = self.hanging.lock().unwrap().contains(host_port);
        let delay = *self.delay.lock().unwrap();
        Box::pin(async move {
            if hangs {
                self.hung.notify_one();
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(delay).await;
            answer.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such host"))
        })
    }
}

fn publish_ok(id: u64) -> Message {
    Message::PublishOk {
        request_id: id,
        offset: None,
    }
}

async fn publish(cluster: &ClusterClient, payload: &[u8]) -> Result<Option<u64>> {
    cluster
        .publish_at_least_once(
            "t1",
            "default",
            "orders",
            payload.to_vec(),
            AckMode::PerMessage,
        )
        .await
}

/// **Every broker moving at once does not strand the client.** The one
/// broker behind the name goes away and another comes up at a new address,
/// as in a rollout that restarts every pod. No address the client holds will
/// ever answer again; the name, resolved again, does.
#[tokio::test]
async fn a_name_is_resolved_again_when_every_broker_has_moved() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let old = StubBroker::start_with(server_config.clone(), publish_ok)?;
    let dns = Arc::new(FakeDns::default());
    dns.point(NAME, vec![old.addr]);

    let cluster = ClusterClient::connect_resolving(
        &[NAME.parse()?],
        "localhost",
        default_config(cert)?,
        policy(),
        Arc::clone(&dns) as Arc<dyn Lookup>,
    )
    .await?;
    publish(&cluster, b"before").await?;
    assert_eq!(old.publishes(), 1);

    let new = StubBroker::start_with(server_config, publish_ok)?;
    old.shut_down();
    dns.point(NAME, vec![new.addr]);
    // Past the connect-time lookup's reuse window, as any real rollout is.
    tokio::time::sleep(RESOLVE_AGAIN_AFTER).await;

    publish(&cluster, b"after").await?;
    assert_eq!(new.publishes(), 1, "the publish reached the moved broker");
    assert_eq!(
        cluster.endpoints().await,
        vec![new.addr],
        "the dead address is dropped and the new one kept"
    );
    Ok(())
}

/// **A dead cluster does not turn every failed operation into a lookup.**
/// Every reconnect inside one reuse window shares one resolution.
#[tokio::test]
async fn reconnects_against_a_dead_cluster_share_a_lookup() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let old = StubBroker::start_with(server_config, publish_ok)?;
    let dns = Arc::new(FakeDns::default());
    dns.point(NAME, vec![old.addr]);
    let cluster = ClusterClient::connect_resolving(
        &[NAME.parse()?],
        "localhost",
        default_config(cert)?,
        policy(),
        Arc::clone(&dns) as Arc<dyn Lookup>,
    )
    .await?;
    old.shut_down();
    tokio::time::sleep(RESOLVE_AGAIN_AFTER).await;

    let before = dns.lookups();
    let started = Instant::now();
    for _ in 0..10 {
        assert!(publish(&cluster, b"lost").await.is_err());
    }
    let windows = started.elapsed().as_millis() / RESOLVE_AGAIN_AFTER.as_millis() + 1;
    let lookups = dns.lookups() - before;
    assert!(lookups >= 1, "the name was looked up again");
    assert!(
        lookups as u128 <= windows,
        "{lookups} lookups in {windows} reuse windows"
    );
    Ok(())
}

/// **Advertised names are looked up beside the seed names, and literal
/// addresses are not looked up at all.**
#[tokio::test]
async fn advertised_names_are_resolved_beside_the_seeds() -> Result<()> {
    let (_, cert) = build_server_config()?;
    let dns = Arc::new(FakeDns::default());
    let seed_addr: SocketAddr = "10.0.0.1:5000".parse()?;
    let pod_addr: SocketAddr = "10.0.0.2:5000".parse()?;
    dns.point(NAME, vec![seed_addr]);
    dns.point("felix-1.felix:5000", vec![pod_addr]);
    let nodes = Nodes::new(
        "localhost",
        default_config(cert)?,
        Arc::clone(&dns) as Arc<dyn Lookup>,
    );
    let seeds = Seeds::new(vec![NAME.parse()?, "10.0.0.9:5000".parse()?]);
    seeds.set_advertised([
        "felix-1.felix:5000".to_string(),
        "10.0.0.3:5000".to_string(),
    ]);

    assert_eq!(seeds.resolve(&nodes).await, vec![seed_addr, pod_addr]);
    assert_eq!(
        dns.asked(),
        vec![NAME.to_string(), "felix-1.felix:5000".to_string()]
    );
    assert_eq!(
        seeds.addrs(),
        vec![seed_addr, "10.0.0.9:5000".parse()?],
        "the seeds are the literal ones and the seed names' addresses, not a broker's"
    );
    Ok(())
}

fn nodes(dns: &Arc<FakeDns>) -> Result<Nodes> {
    let (_, cert) = build_server_config()?;
    Ok(Nodes::new(
        "localhost",
        default_config(cert)?,
        Arc::clone(dns) as Arc<dyn Lookup>,
    ))
}

/// **A seed name that stops resolving keeps its last addresses** while
/// another seed name moves on. The configured seeds are never trimmed, and a
/// name the resolver has lost is still a seed.
#[tokio::test]
async fn a_seed_name_that_stops_resolving_keeps_its_last_addresses() -> Result<()> {
    let dns = Arc::new(FakeDns::default());
    let a1: SocketAddr = "10.0.0.1:5000".parse()?;
    let a2: SocketAddr = "10.0.0.2:5000".parse()?;
    let b1: SocketAddr = "10.0.1.1:5000".parse()?;
    dns.point("a.svc:5000", vec![a1]);
    dns.point("b.svc:5000", vec![b1]);
    let nodes = nodes(&dns)?;
    let seeds = Seeds::new(vec!["a.svc:5000".parse()?, "b.svc:5000".parse()?]);
    seeds.resolve(&nodes).await;
    assert_eq!(seeds.addrs(), vec![a1, b1]);

    dns.point("a.svc:5000", vec![a2]);
    dns.answers.lock().unwrap().remove("b.svc:5000");
    tokio::time::sleep(RESOLVE_AGAIN_AFTER).await;
    assert_eq!(seeds.resolve(&nodes).await, vec![a2]);
    assert_eq!(seeds.addrs(), vec![a2, b1], "b keeps its last answer");
    Ok(())
}

/// **Callers that need a resolution at the same time share one.** A client
/// and the identities derived from it share their seeds, and each has its own
/// client lock, so nothing else stops them looking up together.
#[tokio::test]
async fn concurrent_resolutions_share_one_lookup() -> Result<()> {
    let dns = Arc::new(FakeDns::default());
    dns.point(NAME, vec!["10.0.0.1:5000".parse()?]);
    *dns.delay.lock().unwrap() = Duration::from_millis(100);
    let nodes = nodes(&dns)?;
    let seeds = Seeds::new(vec![NAME.parse()?]);
    let (one, two) = tokio::join!(seeds.resolve(&nodes), seeds.resolve(&nodes));
    assert_eq!(one, two);
    assert_eq!(dns.lookups(), 1);
    Ok(())
}

/// **A resolver that stops answering neither stalls the client nor strands
/// it.** The lookup runs outside the client lock, so other operations can
/// still read the client while it waits, and each name is bounded and looked
/// up beside the others, so the name that does answer gets the client back.
#[tokio::test]
async fn a_hanging_lookup_neither_holds_the_client_nor_hides_other_names() -> Result<()> {
    const HANGS: &str = "hangs.test:5000";
    let (server_config, cert) = build_server_config()?;
    let old = StubBroker::start_with(server_config.clone(), publish_ok)?;
    let dns = Arc::new(FakeDns::default());
    dns.point(NAME, vec![old.addr]);
    dns.point(HANGS, vec![old.addr]);
    let cluster = Arc::new(
        ClusterClient::connect_resolving(
            &[HANGS.parse()?, NAME.parse()?],
            "localhost",
            default_config(cert)?,
            policy(),
            Arc::clone(&dns) as Arc<dyn Lookup>,
        )
        .await?,
    );

    let new = StubBroker::start_with(server_config, publish_ok)?;
    old.shut_down();
    dns.point(NAME, vec![new.addr]);
    dns.hang(HANGS);
    tokio::time::sleep(RESOLVE_AGAIN_AFTER).await;

    let publishing = tokio::spawn({
        let cluster = Arc::clone(&cluster);
        async move { publish(&cluster, b"after").await }
    });
    tokio::time::timeout(Duration::from_secs(5), dns.hung.notified()).await?;
    tokio::time::timeout(Duration::from_millis(500), cluster.client())
        .await
        .map_err(|_| anyhow::anyhow!("the client lock is held across the lookup"))?;

    // A fixed bound, not one derived from the timeout under test.
    tokio::time::timeout(Duration::from_secs(10), publishing)
        .await
        .map_err(|_| anyhow::anyhow!("the hanging lookup was not bounded"))???;
    assert_eq!(new.publishes(), 1);
    Ok(())
}

#[test]
fn a_seed_parses_as_an_address_or_a_name() {
    assert_eq!(
        "10.0.0.4:5000".parse::<Seed>().unwrap(),
        Seed::Addr("10.0.0.4:5000".parse().unwrap())
    );
    assert_eq!(
        "[::1]:5000".parse::<Seed>().unwrap(),
        Seed::Addr("[::1]:5000".parse().unwrap())
    );
    assert_eq!(
        "felix.svc:5000".parse::<Seed>().unwrap(),
        Seed::Name("felix.svc:5000".to_string())
    );
    assert!("felix.svc".parse::<Seed>().is_err());
    assert!("felix.svc:http".parse::<Seed>().is_err());
    assert!(":5000".parse::<Seed>().is_err());
}
