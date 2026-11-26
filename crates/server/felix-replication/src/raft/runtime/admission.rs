//! Which peer messages reach a member. Every Raft message from the internal
//! port comes in through [`GroupRuntime::handle_from`], and
//! [`GroupRuntime::admit_message`] decides it before any member, stopped
//! copy or refusal that names a copy sees it. The threat it answers is in
//! `docs/threat-model-internal.md`.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::GroupRuntime;
use crate::ShardKey;
use crate::raft::group::Group;
use crate::raft::network::{Addressee, RaftReply, RaftRequest, Refused, Request};
use crate::raft::types::{GroupId, MemberId};

/// See [`GroupRuntime::admit`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Admission {
    /// The group epoch (stream incarnation) it was set for.
    incarnation: u64,
    pub(crate) blank: BTreeSet<String>,
    pub(crate) current: BTreeSet<String>,
}

/// Where an admitted message goes.
enum Admitted {
    /// This broker's member of the message's group, which takes it from the
    /// sender. It may be another copy than the one the message names.
    Member(Arc<Group>),
    /// No member of the message's group runs here. `placed` says whether
    /// the sender's broker is in the group's epoch by its admission.
    Absent { placed: bool },
}

impl GroupRuntime {
    /// Who `group`'s member takes a message from beyond its membership,
    /// on a connection bound to a broker ([`Self::handle_from`]). A copy
    /// with no membership cannot tell its leader from an outsider, so it
    /// hears only from `blank`: the brokers in the group now, by the
    /// control plane's placement and target and the voters its leader
    /// last reported. A member also hears from `current`, the placement
    /// and target alone, so one that missed a membership change still
    /// hears from a new member that leads. A broker removed from both is
    /// heard by no copy that does not hold it in its membership. Without an
    /// admission for the group's epoch, or past [`Self::admit_until`], a
    /// copy hears only from its membership.
    pub fn admit(&self, group: &GroupId, blank: BTreeSet<String>, current: BTreeSet<String>) {
        let admission = Admission {
            incarnation: group.epoch,
            blank,
            current,
        };
        let mut admitted = self.inner.admitted.lock();
        if admitted
            .get(&*group.shard)
            .is_none_or(|held| **held != admission)
        {
            admitted.insert((*group.shard).clone(), Arc::new(admission));
        }
    }

    /// Drop the admissions of shards `keep` does not name.
    pub fn retain_admitted(&self, keep: impl Fn(&ShardKey) -> bool) {
        self.inner.admitted.lock().retain(|shard, _| keep(shard));
    }

    /// The admissions stand until `until`: they come from a view of the
    /// control plane that ages, and a broker it has since removed must not
    /// stay admitted while that view cannot be refreshed. `None` lapses them.
    pub fn admit_until(&self, until: Option<tokio::time::Instant>) {
        *self.inner.admitted_until.lock() = until;
    }

    /// Hand a message that came on a connection bound to broker `from` to
    /// the member it names, once [`Self::admit_message`] lets it in.
    pub(crate) async fn handle_from(
        &self,
        from: &str,
        request: RaftRequest,
    ) -> Result<RaftReply, Refused> {
        let admitted = self.admit_message(from, &request).await?;
        self.deliver(admitted, request).await
    }

    /// As [`Self::handle_from`] with no check of the sender: what a test's
    /// in-process transport, which knows no sender, hands messages to. Not
    /// an API.
    #[cfg(any(test, feature = "testing"))]
    pub async fn handle_unchecked(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let admitted = match self.addressed(&request) {
            Some(member) => Admitted::Member(member),
            None => Admitted::Absent { placed: true },
        };
        self.deliver(admitted, request).await
    }

    /// The one admission check for a Raft message from broker `from`. A
    /// message that names its sender must name a member of `from`
    /// ([`impersonation`]). One for this broker's member of the group must
    /// come from a broker of the group ([`Group::outsider`]), even when it
    /// names another copy, whose id the refusal would tell.
    async fn admit_message(&self, from: &str, request: &RaftRequest) -> Result<Admitted, Refused> {
        if let Some(refused) = impersonation(from, request) {
            return Err(refused);
        }
        let admission = self.admission(&request.group);
        match self.addressed(request) {
            Some(member) => match member
                .outsider(from, sender(&request.body), admission.as_deref())
                .await
            {
                Some(refused) => Err(refused),
                None => Ok(Admitted::Member(member)),
            },
            None => Ok(Admitted::Absent {
                placed: admission.is_some_and(|admitted| admitted.blank.contains(from)),
            }),
        }
    }

    /// This broker's member of `request`'s group, whichever copy it names.
    /// Another epoch of the shard is another member.
    fn addressed(&self, request: &RaftRequest) -> Option<Arc<Group>> {
        self.inner
            .registry
            .get(&request.group.shard)
            .filter(|member| {
                member.id == request.group && member.member.broker == request.to.broker
            })
    }

    async fn deliver(
        &self,
        admitted: Admitted,
        request: RaftRequest,
    ) -> Result<RaftReply, Refused> {
        let RaftRequest {
            group,
            to,
            beat,
            report,
            body,
            deadline: _,
        } = request;
        match admitted {
            Admitted::Member(member) if member.member == to => {
                member.handle(body, beat, report).await.map(RaftReply)
            }
            Admitted::Member(member) => {
                let copy = member.member.copy;
                Err(Refused::OtherCopy(Box::new(Addressee { group, to }), copy))
            }
            Admitted::Absent { placed } if self.stopped_member(&group, &to, &body, placed) => {
                Err(Refused::Stopped(
                    Box::new(Addressee { group, to }),
                    "a re-form stopped this copy".to_owned(),
                ))
            }
            Admitted::Absent { .. } => {
                Err(Refused::NoSuchMember(Box::new(Addressee { group, to })))
            }
        }
    }

    /// `group`'s admission, while it stands.
    fn admission(&self, group: &GroupId) -> Option<Arc<Admission>> {
        let until = (*self.inner.admitted_until.lock())?;
        if tokio::time::Instant::now() >= until {
            return None;
        }
        self.inner
            .admitted
            .lock()
            .get(&*group.shard)
            .filter(|admitted| admitted.incarnation == group.epoch)
            .cloned()
    }
}

/// Why `request` is refused when the connection it came on is bound to
/// `from`, or `None` when it may go on. A vote, append, snapshot, transfer
/// or written position names the member that sent it, and openraft acts on
/// that name; a peer may only use its own. A read index names no sender:
/// the group answers it to any member that asks.
fn impersonation(from: &str, request: &RaftRequest) -> Option<Refused> {
    match sender(&request.body) {
        Some(named) if *named.broker != *from => {
            tracing::warn!(
                from,
                named = %named,
                group = %request.group(),
                "refusing a Raft message sent in another broker's name",
            );
            Some(Refused::Unreachable(format!(
                "broker {from} sent a message as {named}"
            )))
        }
        _ => None,
    }
}

/// The member `body` says sent it. A read index names none.
fn sender(body: &Request) -> Option<&MemberId> {
    use openraft::vote::RaftLeaderId;
    match body {
        Request::Append(append) => Some(append.vote.leader_id().node_id()),
        Request::Vote(vote) | Request::PreVote(vote) => Some(vote.vote.leader_id().node_id()),
        Request::Snapshot { vote, .. } => Some(vote.leader_id().node_id()),
        Request::TransferLeader(transfer) => Some(transfer.from_leader().leader_id().node_id()),
        Request::Written { from, .. } | Request::BasePull { from, .. } => Some(from),
        Request::ReadIndex | Request::Watched => None,
    }
}
