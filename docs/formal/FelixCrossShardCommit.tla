------------------------ MODULE FelixCrossShardCommit ------------------------
(***************************************************************************)
(* OUTLINE. A design model for docs/cross-shard-commit.md. Nothing in the  *)
(* code implements it yet, no configuration checks it, and it is not in   *)
(* any CI job. It parses; it has not been model-checked.                   *)
(*                                                                         *)
(* A commit across shards: a home shard coordinates, every other shard    *)
(* the commit writes is a participant. Each shard is a Raft group, and    *)
(* the model takes Raft as given (FelixRaft.tla): a shard's committed log *)
(* only grows, and a leader change keeps it. What a leader change loses   *)
(* is the leader's memory (prepare answers it collected, confirmations)   *)
(* and the home's uncommitted proposals, of which any prefix may survive. *)
(*                                                                         *)
(* The steps, each one record in one log:                                  *)
(*   Prepare  home asks a participant to lock its key; the participant    *)
(*            appends a lock record, granted or refused at its place in   *)
(*            the log, and answers once it is committed.                   *)
(*   Decide   home appends a commit (every lock granted) or an abort. The *)
(*            first decision record for a txn in home's log is the        *)
(*            decision; a later one is ignored.                            *)
(*   Apply    home sends the decision; a participant holding the lock     *)
(*            appends an apply (commit) or release (abort) record.        *)
(*   Retire   home appends done once every participant confirmed.          *)
(*                                                                         *)
(* A participant whose lock is old asks home. Home appends an abort if it *)
(* has no decision. The participant never releases on a timer.            *)
(*                                                                         *)
(* Each shard has one key, which a lock covers. A single-shard writer may *)
(* bump the key's version when it is not locked. A txn's precondition on  *)
(* a participant is the version the client read, or "any" (a blind put).  *)
(*                                                                         *)
(* Messages live in a set. Receiving does not remove one, so every        *)
(* message can be delivered again (duplication) at any later time         *)
(* (reordering), and Lose removes one (loss).                             *)
(*                                                                         *)
(* Txn ids are 1..NTxns, started in order. The model's low-water mark     *)
(* ("every txn up to t is finished at home") stands for the per-term      *)
(* sequence the design gives each prepare.                                 *)
(*                                                                         *)
(* Knobs, each the violating twin of one rule:                             *)
(*   NoDedup           apply without holding the lock      -> NoDoubleApply *)
(*   NoFirstWins       each decision record acts on its own -> Atomicity    *)
(*   ReleaseOnTimeout  a lock is released on its timer     -> Atomicity     *)
(*   NoStaleFence      a participant forgets a txn with no fence, and      *)
(*                     takes a delayed prepare as new      -> NoDoubleApply *)
(*                     (needs a blind put: a precondition hides it)        *)
(*   TrimUndone        retention trims an undone decision  -> Atomicity     *)
(*   WritesIgnoreLocks a single-shard write ignores a lock -> PreconditionsHeld *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Parts,          \* participant shards (home's own part is one of them)
    NTxns,          \* txns 1..NTxns
    MaxTerm,        \* home leader changes allowed
    MaxWrites,      \* single-shard writes allowed
    NoDedup,
    NoFirstWins,
    ReleaseOnTimeout,
    NoStaleFence,
    TrimUndone,
    WritesIgnoreLocks

ASSUME /\ NTxns \in Nat \ {0}
       /\ MaxTerm \in Nat \ {0}
       /\ MaxWrites \in Nat
       /\ \A k \in {NoDedup, NoFirstWins, ReleaseOnTimeout, NoStaleFence,
                    TrimUndone, WritesIgnoreLocks} : k \in BOOLEAN

Txns == 1..NTxns

VARIABLES
    hlog,       \* home's committed log: [k |-> "commit"|"abort"|"done", t]
    hpend,      \* home leader's proposals not yet committed, in order
    hterm,      \* home leader's term
    hgrant,     \* home leader's memory: parts that answered granted, per txn
    hconf,      \* home leader's memory: parts that confirmed apply, per txn
    htrim,      \* txns whose home records retention removed
    plog,       \* each participant's committed log
    forgot,     \* txns each participant dropped from its dedup table
    pterm,      \* each participant's leader term
    started,    \* txns the client has sent (and may send again)
    pre,        \* each txn's precondition per participant: a version or "any"
    writes,     \* single-shard writes so far
    decided,    \* ghost: the first decision any home record made, per txn
    msgs        \* messages in flight

vars == << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
           started, pre, writes, decided, msgs >>

------------------------------------------------------------------------------
\* Helpers over logs.

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

HIdx(k, t) == IF t \in htrim THEN {}
              ELSE { i \in 1..Len(hlog) : hlog[i].k = k /\ hlog[i].t = t }

HHas(k, t) == HIdx(k, t) /= {}

\* First record wins: the decision is whichever of commit and abort came first.
Winner(t) ==
    LET d == HIdx("commit", t) \cup HIdx("abort", t)
    IN IF d = {} THEN "none"
       ELSE hlog[CHOOSE i \in d : \A j \in d : i <= j].k

\* What home acts on when it drives a participant. NoFirstWins lets each
\* record act on its own, so a commit and a late abort both get sent.
SaysCommit(t) == IF NoFirstWins THEN HHas("commit", t) ELSE Winner(t) = "commit"
SaysAbort(t)  == IF NoFirstWins THEN HHas("abort", t)  ELSE Winner(t) = "abort"

Pending(t) == \E i \in 1..Len(hpend) : hpend[i].t = t /\ hpend[i].k /= "done"

PIdx(p, ks, t) == { i \in 1..Len(plog[p]) : plog[p][i].k \in ks /\ plog[p][i].t = t }

\* p holds t's lock: a granted lock record after the last apply or release.
Holds(p, t) ==
    LET l == { i \in PIdx(p, {"lock"}, t) : plog[p][i].ok }
        r == PIdx(p, {"apply", "release"}, t)
    IN l /= {} /\ (r = {} \/ Max(r) < Max(l))

Locked(p) == \E t \in Txns : Holds(p, t)

Applied(p, t)  == PIdx(p, {"apply"}, t) /= {}
Released(p, t) == PIdx(p, {"release"}, t) /= {}
Resolved(p, t) == Applied(p, t) \/ Released(p, t)

\* The key's version: single-shard writes and applies both bump it.
Ver(p) == Cardinality({ i \in 1..Len(plog[p]) : plog[p][i].k \in {"write", "apply"} })

\* The lock record the dedup table still remembers.
KnownLock(p, t) == t \notin forgot[p] /\ PIdx(p, {"lock"}, t) /= {}
LastLock(p, t) == plog[p][Max(PIdx(p, {"lock"}, t))]

\* The fence: a record that every txn up to f.t is finished at home.
Fenced(p, t) == \E i \in 1..Len(plog[p]) : plog[p][i].k = "fence" /\ plog[p][i].t >= t

\* The newest home term p has recorded.
MaxHt(p) == LET s == { plog[p][i].ht : i \in { j \in 1..Len(plog[p]) : plog[p][j].k = "lock" } }
            IN IF s = {} THEN 0 ELSE Max(s)

Stale(p, m) == ~NoStaleFence /\ (m.ht < MaxHt(p) \/ Fenced(p, m.t))

Send(m) == msgs' = msgs \cup {m}

------------------------------------------------------------------------------
Init ==
    /\ hlog = << >>
    /\ hpend = << >>
    /\ hterm = 1
    /\ hgrant = [t \in Txns |-> {}]
    /\ hconf = [t \in Txns |-> {}]
    /\ htrim = {}
    /\ plog = [p \in Parts |-> << >>]
    /\ forgot = [p \in Parts |-> {}]
    /\ pterm = [p \in Parts |-> 1]
    /\ started = {}
    /\ pre = [t \in Txns |-> [p \in Parts |-> "any"]]
    /\ writes = 0
    /\ decided = [t \in Txns |-> "none"]
    /\ msgs = {}

\* The client sends txn t, with preconditions from what it read.
ClientStart(t) ==
    /\ t \notin started
    /\ \A u \in Txns : u < t => u \in started
    /\ \E c \in [Parts -> {"read", "any"}] :
         pre' = [pre EXCEPT ![t] = [p \in Parts |-> IF c[p] = "read" THEN Ver(p) ELSE "any"]]
    /\ started' = started \cup {t}
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    writes, decided, msgs >>

\* Home (re)sends a prepare. Enabled for any started, undecided txn, which
\* also stands for the client retrying at a new home leader. A txn retention
\* trimmed is past the client's retry window and is not retried.
HomePrepare(t, p) ==
    /\ t \in started
    /\ t \notin htrim
    /\ Winner(t) = "none"
    /\ Send([type |-> "prepare", t |-> t, p |-> p, ht |-> hterm])
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided >>

\* A participant takes a prepare. A lock it remembers is answered again,
\* unchanged. A stale prepare (older home term, or fenced) is dropped.
\* Otherwise it appends a lock record, decided at its place in the log, and
\* answers once the record is committed.
ParticipantLock(p, m) ==
    /\ m \in msgs
    /\ m.type = "prepare"
    /\ m.p = p
    /\ IF KnownLock(p, m.t)
         THEN /\ Send([type |-> "vote", t |-> m.t, p |-> p, ok |-> LastLock(p, m.t).ok])
              /\ UNCHANGED plog
         ELSE /\ ~Stale(p, m)
              /\ LET ok == /\ ~\E u \in Txns : u /= m.t /\ Holds(p, u)
                           /\ pre[m.t][p] \in {"any", Ver(p)}
                 IN /\ plog' = [plog EXCEPT ![p] = Append(@,
                          [k |-> "lock", t |-> m.t, ok |-> ok, ht |-> m.ht])]
                    /\ Send([type |-> "vote", t |-> m.t, p |-> p, ok |-> ok])
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, forgot, pterm,
                    started, pre, writes, decided >>

Propose(r) == hpend' = Append(hpend, r)

\* Home collects a vote. A refusal proposes an abort.
HomeVote(m) ==
    /\ m \in msgs
    /\ m.type = "vote"
    /\ Winner(m.t) = "none"
    /\ IF m.ok
         THEN /\ m.p \notin hgrant[m.t]
              /\ hgrant' = [hgrant EXCEPT ![m.t] = @ \cup {m.p}]
              /\ UNCHANGED hpend
         ELSE /\ ~Pending(m.t)
              /\ Propose([k |-> "abort", t |-> m.t])
              /\ UNCHANGED hgrant
    /\ UNCHANGED << hlog, hterm, hconf, htrim, plog, forgot, pterm, started, pre,
                    writes, decided, msgs >>

\* Every participant granted: home proposes the commit.
HomeDecide(t) ==
    /\ hgrant[t] = Parts
    /\ Winner(t) = "none"
    /\ ~Pending(t)
    /\ Propose([k |-> "commit", t |-> t])
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* Home gives up on a txn it cannot finish preparing.
HomeGiveUp(t) ==
    /\ t \in started
    /\ Winner(t) = "none"
    /\ ~Pending(t)
    /\ Propose([k |-> "abort", t |-> t])
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* A participant holding an old lock asks home.
ParticipantAsk(p, t) ==
    /\ Holds(p, t)
    /\ Send([type |-> "ask", t |-> t, p |-> p])
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided >>

\* Home answers an ask: with no decision it proposes an abort. The check
\* reads only committed records, so it races a commit still in hpend; first
\* record wins settles that race in the log.
HomeAnswer(m) ==
    /\ m \in msgs
    /\ m.type = "ask"
    /\ Winner(m.t) = "none"
    /\ \A i \in 1..Len(hpend) : ~(hpend[i].t = m.t /\ hpend[i].k = "abort")
    /\ Propose([k |-> "abort", t |-> m.t])
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* The head proposal commits in home's log.
HomeCommitHead ==
    /\ hpend /= << >>
    /\ LET r == Head(hpend) IN
       /\ hlog' = Append(hlog, r)
       /\ decided' = IF r.k \in {"commit", "abort"} /\ decided[r.t] = "none"
                       THEN [decided EXCEPT ![r.t] = r.k] ELSE decided
    /\ hpend' = Tail(hpend)
    /\ UNCHANGED << hterm, hgrant, hconf, htrim, plog, forgot, pterm, started, pre,
                    writes, msgs >>

\* Home drives a decided txn: apply on commit, release on abort.
HomeDrive(t, p) ==
    /\ ~HHas("done", t)
    /\ \/ SaysCommit(t) /\ Send([type |-> "apply", t |-> t, p |-> p, ht |-> hterm])
       \/ SaysAbort(t)  /\ Send([type |-> "release", t |-> t, p |-> p, ht |-> hterm])
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided >>

\* A participant applies its part only under t's lock, which the apply
\* releases in the same record: the lock is the dedup entry.
ParticipantApply(p, m) ==
    /\ m \in msgs
    /\ m.type = "apply"
    /\ m.p = p
    /\ IF Holds(p, m.t) \/ (NoDedup /\ PIdx(p, {"lock"}, m.t) /= {})
         THEN /\ plog' = [plog EXCEPT ![p] = Append(@,
                    [k |-> "apply", t |-> m.t, ver |-> Ver(p)])]
              /\ Send([type |-> "applied", t |-> m.t, p |-> p])
         ELSE /\ Resolved(p, m.t) \/ Fenced(p, m.t)
              /\ Send([type |-> "applied", t |-> m.t, p |-> p])
              /\ UNCHANGED plog
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, forgot, pterm,
                    started, pre, writes, decided >>

ParticipantRelease(p, m) ==
    /\ m \in msgs
    /\ m.type = "release"
    /\ m.p = p
    /\ Holds(p, m.t)
    /\ plog' = [plog EXCEPT ![p] = Append(@, [k |-> "release", t |-> m.t])]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* The twin: a lock past its timer is released without asking home.
TimeoutRelease(p, t) ==
    /\ ReleaseOnTimeout
    /\ Holds(p, t)
    /\ plog' = [plog EXCEPT ![p] = Append(@, [k |-> "release", t |-> t])]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, forgot, pterm,
                    started, pre, writes, decided, msgs >>

HomeConfirm(m) ==
    /\ m \in msgs
    /\ m.type = "applied"
    /\ SaysCommit(m.t)
    /\ m.p \notin hconf[m.t]
    /\ hconf' = [hconf EXCEPT ![m.t] = @ \cup {m.p}]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, htrim, plog, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* Every participant confirmed: home proposes done.
HomeRetire(t) ==
    /\ SaysCommit(t)
    /\ hconf[t] = Parts
    /\ ~HHas("done", t)
    /\ \A i \in 1..Len(hpend) : hpend[i] /= [k |-> "done", t |-> t]
    /\ Propose([k |-> "done", t |-> t])
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* Home tells participants that every txn up to t is finished. A
\* participant records it before it may forget any of them.
HomeLowWater(t, p) ==
    /\ ~NoStaleFence
    /\ \A u \in 1..t : u \in started /\ (Winner(u) = "abort" \/ HHas("done", u))
    /\ Send([type |-> "lw", t |-> t, p |-> p])
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided >>

ParticipantFence(p, m) ==
    /\ m \in msgs
    /\ m.type = "lw"
    /\ m.p = p
    /\ ~Fenced(p, m.t)
    /\ plog' = [plog EXCEPT ![p] = Append(@, [k |-> "fence", t |-> m.t])]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* A participant drops a resolved txn from its dedup table: only under a
\* fence, unless the twin says otherwise.
ParticipantForget(p, t) ==
    /\ Resolved(p, t)
    /\ t \notin forgot[p]
    /\ NoStaleFence \/ Fenced(p, t)
    /\ forgot' = [forgot EXCEPT ![p] = @ \cup {t}]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, pterm,
                    started, pre, writes, decided, msgs >>

\* Retention removes a txn's home records: aborted or done, unless the twin.
HomeTrim(t) ==
    /\ t \notin htrim
    /\ IF TrimUndone THEN Winner(t) /= "none"
                     ELSE Winner(t) = "abort" \/ HHas("done", t)
    /\ htrim' = htrim \cup {t}
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, plog, forgot, pterm,
                    started, pre, writes, decided, msgs >>

\* A single-shard commit writes the participant's key.
SingleWrite(p) ==
    /\ writes < MaxWrites
    /\ WritesIgnoreLocks \/ ~Locked(p)
    /\ plog' = [plog EXCEPT ![p] = Append(@, [k |-> "write", t |-> 0])]
    /\ writes' = writes + 1
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, forgot, pterm,
                    started, pre, decided, msgs >>

\* Home's leader changes. Any prefix of its proposals commits (the new
\* leader may carry them); the rest are lost, with the old leader's memory.
HomeLeaderChange ==
    /\ hterm < MaxTerm
    /\ \E n \in 0..Len(hpend) :
         LET kept == SubSeq(hpend, 1, n)
             firsts == { kept[i] : i \in 1..n }
         IN /\ hlog' = hlog \o kept
            /\ decided' = [t \in Txns |->
                 IF decided[t] /= "none" THEN decided[t]
                 ELSE LET d == { i \in 1..n : kept[i].t = t /\ kept[i].k \in {"commit", "abort"} }
                      IN IF d = {} THEN "none"
                         ELSE kept[CHOOSE i \in d : \A j \in d : i <= j].k]
    /\ hpend' = << >>
    /\ hterm' = hterm + 1
    /\ hgrant' = [t \in Txns |-> {}]
    /\ hconf' = [t \in Txns |-> {}]
    /\ UNCHANGED << htrim, plog, forgot, pterm, started, pre, writes, msgs >>

\* A participant's leader changes. It keeps nothing in memory that matters:
\* the lock table and dedup table are read from its committed log, and it
\* answers only after its record commits, so a change mid-step is a lost
\* answer, which Lose covers. Its lock timers restart.
ParticipantLeaderChange(p) ==
    /\ pterm[p] < MaxTerm
    /\ pterm' = [pterm EXCEPT ![p] = @ + 1]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot,
                    started, pre, writes, decided, msgs >>

Lose(m) ==
    /\ m \in msgs
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, plog, forgot, pterm,
                    started, pre, writes, decided >>

Next ==
    \/ \E t \in Txns : ClientStart(t) \/ HomeDecide(t) \/ HomeGiveUp(t)
                       \/ HomeRetire(t) \/ HomeTrim(t)
    \/ \E t \in Txns, p \in Parts :
          HomePrepare(t, p) \/ HomeDrive(t, p) \/ HomeLowWater(t, p)
          \/ ParticipantAsk(p, t) \/ TimeoutRelease(p, t) \/ ParticipantForget(p, t)
    \/ \E m \in msgs : HomeVote(m) \/ HomeAnswer(m) \/ HomeConfirm(m) \/ Lose(m)
    \/ \E p \in Parts, m \in msgs :
          ParticipantLock(p, m) \/ ParticipantApply(p, m)
          \/ ParticipantRelease(p, m) \/ ParticipantFence(p, m)
    \/ HomeCommitHead
    \/ HomeLeaderChange
    \/ \E p \in Parts : SingleWrite(p) \/ ParticipantLeaderChange(p)

\* Home keeps driving and proposals keep committing; a message that keeps
\* being sent is eventually received (strong fairness, since Lose can
\* disable a receive for a while). Leader changes are bounded by MaxTerm.
Fairness ==
    /\ WF_vars(HomeCommitHead)
    /\ \A t \in Txns : WF_vars(HomeRetire(t)) /\ WF_vars(HomeGiveUp(t))
    /\ \A t \in Txns, p \in Parts :
          /\ WF_vars(HomeDrive(t, p)) /\ WF_vars(HomePrepare(t, p))
          /\ WF_vars(ParticipantAsk(p, t))
    /\ \A p \in Parts :
          SF_vars(\E m \in msgs : ParticipantLock(p, m) \/ ParticipantApply(p, m)
                                  \/ ParticipantRelease(p, m))
    /\ SF_vars(\E m \in msgs : HomeVote(m) \/ HomeAnswer(m) \/ HomeConfirm(m))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
\* Invariants.

\* All parts or none: no txn is applied on one participant and released on
\* another; an applied part means the decision was commit; a done txn is
\* applied everywhere.
Atomicity ==
    \A t \in Txns :
        /\ \A p, q \in Parts : ~(Applied(p, t) /\ Released(q, t))
        /\ \A p \in Parts : Applied(p, t) => decided[t] = "commit"
        /\ HHas("done", t) => \A p \in Parts : Applied(p, t)

NoDoubleApply ==
    \A p \in Parts, t \in Txns : Cardinality(PIdx(p, {"apply"}, t)) <= 1

\* Each part applies on the version its precondition named.
PreconditionsHeld ==
    \A p \in Parts : \A i \in 1..Len(plog[p]) :
        plog[p][i].k = "apply" =>
            pre[plog[p][i].t][p] \in {"any", plog[p][i].ver}

\* Liveness: a committed txn is eventually applied on every participant.
EventuallyApplied ==
    \A t \in Txns : (decided[t] = "commit") ~> (\A p \in Parts : Applied(p, t))

\* Liveness: no lock is held forever.
LocksReleased ==
    \A t \in Txns, p \in Parts : Holds(p, t) ~> ~Holds(p, t)

=============================================================================
