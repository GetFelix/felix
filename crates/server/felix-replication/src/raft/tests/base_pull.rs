//! Who a voter serves a re-formed stream's base to.

use super::*;
use crate::raft::network::{Ask, Beat, RaftReply, RaftRequest, Refused};

fn pull(from: &str) -> RaftRequest {
    RaftRequest {
        group: group(0),
        reform_epoch: 0,
        to: MemberId::new("a", 1),
        beat: Beat::default(),
        report: Ask::Nothing,
        deadline: Duration::from_secs(1),
        body: Request::BasePull {
            from: MemberId::new(from, 1),
            start: 0,
            max_bytes: u32::MAX,
            max_count: u32::MAX,
        },
    }
}

fn refusal(answer: Result<RaftReply, Refused>) -> String {
    match answer {
        Err(Refused::Unreachable(why)) => why,
        Err(other) => panic!("refused otherwise: {other:?}"),
        Ok(_) => panic!("served"),
    }
}

/// A base pull names its learner, so only a broker of the group, asking in
/// its own name, gets as far as the base. The base is every committed
/// record below the gap: an outsider must not read it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_base_is_served_only_to_a_broker_of_the_group_in_its_own_name() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let a = cluster.runtime("a");

    let outsider = refusal(a.handle_from("x", pull("x")).await);
    assert!(outsider.contains("not a member"), "{outsider}");
    let impostor = refusal(a.handle_from("x", pull("b")).await);
    assert!(impostor.contains("as b"), "{impostor}");
    // A member gets as far as the base, which this group, never re-formed,
    // does not have.
    let member = refusal(a.handle_from("b", pull("b")).await);
    assert!(member.contains("no re-form base"), "{member}");
}

/// A copy a re-form left behind holds the old epoch's records. It must not
/// pull a base from the new group, nor a new copy pull one from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_base_pull_from_another_reform_epoch_is_refused() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let a = cluster.runtime("a");

    let mut other = pull("b");
    other.reform_epoch = 1;
    let refused = refusal(a.handle_from("b", other).await);
    assert!(refused.contains("re-form epoch 0"), "{refused}");
}
