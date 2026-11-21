---------------------------- MODULE FelixRaftSessions ----------------------------
(***************************************************************************)
(* FelixRaftReads with the long-lived sessions on a shard: a subscriber    *)
(* reading the stream, and a consumer group committing its state.          *)
(* `FelixShardSessions.tla` asks the same of the hybrid: what keeps a      *)
(* leader that was deposed without knowing it from handing out something   *)
(* wrong?                                                                  *)
(*                                                                         *)
(* The subscriber is FelixRaft's: it takes records in offset order from    *)
(* any broker that believes it leads, up to what that broker applied, and  *)
(* resumes after its last offset when it moves. The broker hands out       *)
(* records below its read bound (`read_bound`, crates/server/              *)
(* felix-replication/src/raft/group.rs:1794, which moves on apply and so   *)
(* never passes the commit index; `follow_commits`, raft/deliver.rs:669).  *)
(* No round and no lease on that path. NoLostDelivery says everything      *)
(* delivered is at its offset in the log of an open leader no broker has   *)
(* outvoted: what was handed out is never taken back.                     *)
(* `DeliverUncommitted` hands out the uncommitted tail, and TLC finds a    *)
(* successor holding something else at that offset.                       *)
(*                                                                         *)
(* A group commit (a poll's claim, an ack, a dead-letter change) is a      *)
(* proposal to the shard's Raft group in the term the coordinator serves   *)
(* (`OnRaft::propose`, services/felix-broker-service/src/shards/raft/      *)
(* groups.rs:61), answered once it is applied as the entry that term       *)
(* wrote there (`OnRaft::applied`, groups.rs:80, waiting on                *)
(* `commit::committed`, raft/commit.rs:372). The read the coordinator      *)
(* decides from (`OnRaft::fresh`) is FelixRaftReads' read, checked there,  *)
(* so the group configs run no read. NoStaleGroupCommit says no            *)
(* commit is answered at a term older than one that had opened before the  *)
(* commit began: two coordinators of one group are never both taking       *)
(* commits. The knobs:                                                     *)
(*                                                                         *)
(*   GroupAckOnWrite   -- answered once in the coordinator's own log, as   *)
(*                        the hybrid answered group state.                 *)
(*   CommitIgnoresTerm -- answered once applied past its index, whatever   *)
(*                        entry holds it: the next leader's entry took the *)
(*                        offset.                                          *)
(*                                                                         *)
(* A group entry is a record here, so it holds an offset; what it does to  *)
(* group state is not modelled, only when it is answered.                  *)
(***************************************************************************)
EXTENDS FelixRaftReads

CONSTANTS
    MaxCommits,
    GroupAckOnWrite,
    CommitIgnoresTerm

VARIABLES
    cm,         \* the group commit in flight: who, term, idx, top; at = Nobody when none
    ncommits,   \* how many group commits have begun
    topOpen,    \* history: the newest term that has opened so far
    staleGroup  \* history: a commit answered after a newer coordinator opened

sessionVars == << cm, ncommits, staleGroup >>
sessAllVars == << allVars, sessionVars, topOpen >>

NoCommit == [at |-> Nobody, term |-> 0, idx |-> 0, top |-> 0]

\* The newest term a broker has open right now.
OpenTerm ==
    LET open == {vote[s].term : s \in {s \in Servers : Open(s)}}
    IN IF open = {} THEN 0 ELSE MaxOf(open)

SessionsInit ==
    /\ ReadsInit
    /\ cm = NoCommit
    /\ ncommits = 0
    /\ topOpen = 0
    /\ staleGroup = FALSE

\* The proposal, in the term the coordinator has open. `top` is what had
\* opened by then.
BeginCommit(s) ==
    /\ ncommits < MaxCommits
    /\ cm.at = Nobody
    /\ Open(s)
    /\ AppendRecord(s)
    /\ cm' = [at |-> s, term |-> vote[s].term, idx |-> LastIdx(s) + 1, top |-> topOpen]
    /\ ncommits' = ncommits + 1
    /\ UNCHANGED << staleGroup, readVars >>

Answerable(s) ==
    CASE GroupAckOnWrite ->
            /\ role[s] = "leader" /\ vote[s].term = cm.term
            /\ Has(s, cm.idx) /\ EntryAt(s, cm.idx).term = cm.term
      [] CommitIgnoresTerm -> Len(applied[s]) >= cm.idx
      [] OTHER ->
            /\ Len(applied[s]) >= cm.idx
            /\ applied[s][cm.idx].term = cm.term

\* The answer. Under the commit wait it needs no role check: an entry
\* applied as its term wrote it is committed, whoever leads now.
EndCommit ==
    LET s == cm.at IN
    /\ s # Nobody
    /\ Answerable(s)
    /\ staleGroup' = (staleGroup \/ cm.top > cm.term)
    /\ cm' = NoCommit
    /\ UNCHANGED << vars, ncommits, readVars >>

SessionsStep ==
    \/ ReadsNext /\ UNCHANGED sessionVars
    \/ EndCommit
    \/ \E s \in Servers : BeginCommit(s)

\* `topOpen` follows every step, so a term that opened and then closed is
\* still remembered.
SessionsNext ==
    /\ SessionsStep
    /\ topOpen' = Max(topOpen, OpenTerm')

SessionsSpec == SessionsInit /\ [][SessionsNext]_sessAllVars

\* Everything delivered is at its offset in the log of an open leader no
\* broker has outvoted, or below its purged id.
NoLostDelivery ==
    \A l \in Servers : (Open(l) /\ \A s \in Servers : vote[s].term <= vote[l].term) =>
        \A k \in 1..Len(got) :
            \/ got[k].off <= base[l].idx
            \/ Has(l, got[k].off) /\ IsRecord(EntryAt(l, got[k].off), got[k].val)

\* No group commit is answered by a coordinator a newer one had already
\* replaced.
NoStaleGroupCommit == ~staleGroup

SessionsTypeOK ==
    /\ ReadsTypeOK
    /\ ncommits \in 0..MaxCommits
    /\ topOpen \in 0..MaxTerm

=============================================================================
