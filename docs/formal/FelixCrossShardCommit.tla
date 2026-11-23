------------------------ MODULE FelixCrossShardCommit ------------------------
(***************************************************************************)
(* The design in docs/cross-shard-commit.md. Nothing in the code builds it *)
(* yet; the configurations run as random walks in tla-walk.yml.           *)
(*                                                                         *)
(* A commit across shards: a home shard coordinates, and every other      *)
(* shard the commit writes is a participant. Each shard is a Raft group,  *)
(* taken as given (FelixRaft.tla): a leader proposes records, they commit *)
(* in order, and a leader change keeps any prefix of what was proposed    *)
(* and loses the rest along with the leader's memory. A record's effect   *)
(* is decided when it commits, at its place in the log, never when it is  *)
(* proposed.                                                               *)
(*                                                                         *)
(* Home writes commit, abort, its own part, and done. The first commit or *)
(* abort for a txn in home's log is the decision. A participant writes a  *)
(* lock (granted or refused), an apply that releases the lock, a release, *)
(* and a fence holding home's low-water mark. It answers only once its    *)
(* record commits.                                                         *)
(*                                                                         *)
(* Each participant has one key. A lock holds it exclusively. A           *)
(* single-shard write to it is refused while it is locked. A txn's        *)
(* precondition on a participant is the version the client read, or -1   *)
(* for a blind put.                                                        *)
(*                                                                         *)
(* Messages live in a set. Receiving one does not remove it, so any       *)
(* message can be delivered again at any later time; Lose removes one.    *)
(* Txn ids 1..NTxns are started in order and stand for the sequence home  *)
(* gives each commit; hlw is home's low-water mark: every txn up to it is *)
(* aborted or done.                                                        *)
(*                                                                         *)
(* A forger (a broker that may send on the internal port but holds no     *)
(* client token) can send prepares and applies for any txn.               *)
(*                                                                         *)
(* Knobs, each the violating twin of one rule:                             *)
(*   NoFirstWins      each decision record acts on its own -> Atomicity    *)
(*   ReleaseOnTimeout a lock is released on its timer      -> Atomicity    *)
(*   TrimUndone       retention trims an undone decision   -> Atomicity    *)
(*   NoFence          no term or low-water fence; ids are  -> NoDoubleApply *)
(*                    forgotten at will                                    *)
(*   WriteCheckEarly  a single-shard write checks locks    -> PreconditionsHeld *)
(*                    when proposed, not at its place                      *)
(*   ApplyWithoutLock an apply is accepted with no lock    -> NoForgedApply *)
(*   NoTokenCheck     a prepare is granted without a token -> NoForgedApply *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANTS
    Parts,          \* participant shards
    NTxns,          \* txns 1..NTxns
    MaxTerm,        \* leader terms per group
    MaxWrites,      \* single-shard writes
    Forger,         \* TRUE: forged prepares and applies are sent
    NoFirstWins,
    ReleaseOnTimeout,
    TrimUndone,
    NoFence,
    WriteCheckEarly,
    ApplyWithoutLock,
    NoTokenCheck

ASSUME /\ NTxns \in Nat \ {0}
       /\ MaxTerm \in Nat \ {0}
       /\ MaxWrites \in Nat
       /\ \A k \in {Forger, NoFirstWins, ReleaseOnTimeout, TrimUndone, NoFence,
                    WriteCheckEarly, ApplyWithoutLock, NoTokenCheck} : k \in BOOLEAN

Txns == 1..NTxns
None == "none"
Any == -1

VARIABLES
    hlog,       \* home's committed log: [k, t]
    hpend,      \* home leader's proposals, in order
    hterm,      \* home leader's term
    hgrant,     \* home leader's memory: parts that answered granted
    hconf,      \* home leader's memory: parts that confirmed apply
    htrim,      \* per txn: home records at or below this index are trimmed
    hlw,        \* home's low-water mark, kept in its log
    plog,       \* each participant's committed log
    ppend,      \* each participant leader's proposals (messages to act on)
    pterm,      \* each participant's leader term
    forgot,     \* txns each participant dropped from its dedup table
    started,    \* txns the client has sent
    pre,        \* each txn's precondition per participant
    writes,     \* single-shard writes proposed
    msgs        \* messages in flight

vars == << hlog, hpend, hterm, hgrant, hconf, htrim, hlw, plog, ppend, pterm,
           forgot, started, pre, writes, msgs >>

homeVars == << hlog, hpend, hterm, hgrant, hconf, htrim, hlw >>
partVars == << plog, ppend, pterm, forgot >>

------------------------------------------------------------------------------
\* 0 for an empty set: every index and term here is at least 1.
Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

Msg(ty, t, p, ht, ok, auth, d) ==
    [type |-> ty, t |-> t, p |-> p, ht |-> ht, ok |-> ok, auth |-> auth, d |-> d]

PRec(k, t, ok, auth, ht, ver, d) ==
    [k |-> k, t |-> t, ok |-> ok, auth |-> auth, ht |-> ht, ver |-> ver, d |-> d]

Send(m) == msgs' = msgs \cup {m}

\* Home's log. Trimmed records are gone for answering, but the model keeps
\* them so Decided can say what the first decision was.
AllIdx(k, t) == { i \in 1..Len(hlog) : hlog[i].k = k /\ hlog[i].t = t }
HIdx(k, t) == { i \in AllIdx(k, t) : i > htrim[t] }
HHas(k, t) == HIdx(k, t) /= {}
FirstOf(S) == IF S = {} THEN None ELSE hlog[Min(S)].k
Decided(t) == FirstOf(AllIdx("commit", t) \cup AllIdx("abort", t))
Winner(t) == FirstOf(HIdx("commit", t) \cup HIdx("abort", t))

\* What home acts on. NoFirstWins lets each record act on its own.
SaysCommit(t) == IF NoFirstWins THEN HHas("commit", t) ELSE Winner(t) = "commit"
SaysAbort(t)  == IF NoFirstWins THEN HHas("abort", t)  ELSE Winner(t) = "abort"

Pending(k, t) == \E i \in 1..Len(hpend) : hpend[i].k = k /\ hpend[i].t = t
PendingDecision(t) == Pending("commit", t) \/ Pending("abort", t)
Propose(k, t) == hpend' = Append(hpend, [k |-> k, t |-> t])

\* A participant's log.
PIdx(p, ks, t) == { i \in 1..Len(plog[p]) : plog[p][i].k \in ks /\ plog[p][i].t = t }
Holds(p, t) ==
    Max({ i \in PIdx(p, {"lock"}, t) : plog[p][i].ok }) > Max(PIdx(p, {"apply", "release"}, t))
Locked(p) == \E t \in Txns : Holds(p, t)
Applied(p, t)  == PIdx(p, {"apply"}, t) /= {}
Released(p, t) == PIdx(p, {"release"}, t) /= {}
Ver(p) == Cardinality({ i \in 1..Len(plog[p]) : plog[p][i].k \in {"write", "apply"} })
Known(p, t) == t \notin forgot[p] /\ PIdx(p, {"lock"}, t) /= {}
LastLock(p, t) == plog[p][Max(PIdx(p, {"lock"}, t))]
FenceIdx(p) == { i \in 1..Len(plog[p]) : plog[p][i].k = "fence" }
Lw(p) == Max({ plog[p][i].t : i \in FenceIdx(p) })
SeenTerm(p) == Max({0} \cup { plog[p][i].ht : i \in
                    { j \in 1..Len(plog[p]) : plog[p][j].k \in {"lock", "fence"} } })

\* The fence: a prepare from an older home term, or at or below the mark.
Stale(p, m) == ~NoFence /\ (m.ht < SeenTerm(p) \/ m.t <= Lw(p))

PAppend(p, r) == plog' = [plog EXCEPT ![p] = Append(@, r)]

------------------------------------------------------------------------------
Init ==
    /\ hlog = << >>
    /\ hpend = << >>
    /\ hterm = 1
    /\ hgrant = [t \in Txns |-> {}]
    /\ hconf = [t \in Txns |-> {}]
    /\ htrim = [t \in Txns |-> 0]
    /\ hlw = 0
    /\ plog = [p \in Parts |-> << >>]
    /\ ppend = [p \in Parts |-> << >>]
    /\ pterm = [p \in Parts |-> 1]
    /\ forgot = [p \in Parts |-> {}]
    /\ started = {}
    /\ pre = [t \in Txns |-> [p \in Parts |-> Any]]
    /\ writes = 0
    /\ msgs = {}

\* The client sends txn t, with preconditions from what it read.
ClientStart(t) ==
    /\ t \notin started
    /\ \A u \in Txns : u < t => u \in started
    /\ \E c \in [Parts -> BOOLEAN] :
         pre' = [pre EXCEPT ![t] = [p \in Parts |-> IF c[p] THEN Ver(p) ELSE Any]]
    /\ started' = started \cup {t}
    /\ UNCHANGED << homeVars, partVars, writes, msgs >>

\* Home (re)sends a prepare with the client's token. This also stands for
\* the client retrying the same id at a new home leader.
HomePrepare(t, p) ==
    /\ t \in started
    /\ t > hlw
    /\ htrim[t] = 0
    /\ Winner(t) = None
    /\ Send(Msg("prepare", t, p, hterm, FALSE, TRUE, "real"))
    /\ UNCHANGED << homeVars, partVars, started, pre, writes >>

\* A forger sends a prepare with no valid token, or an apply.
Forge(t, p) ==
    /\ Forger
    /\ \/ Send(Msg("prepare", t, p, hterm, FALSE, FALSE, "forged"))
       \/ Send(Msg("apply", t, p, hterm, FALSE, FALSE, "forged"))
    /\ UNCHANGED << homeVars, partVars, started, pre, writes >>

HomeVote(m) ==
    /\ m.type = "vote"
    /\ m.t > hlw
    /\ Winner(m.t) = None
    /\ IF m.ok
         THEN /\ m.p \notin hgrant[m.t]
              /\ hgrant' = [hgrant EXCEPT ![m.t] = @ \cup {m.p}]
              /\ UNCHANGED hpend
         ELSE /\ ~PendingDecision(m.t)
              /\ Propose("abort", m.t)
              /\ UNCHANGED hgrant
    /\ UNCHANGED << hlog, hterm, hconf, htrim, hlw, partVars, started, pre, writes, msgs >>

\* Every participant granted: home proposes the commit. Never at or below
\* the mark: first record wins needs the first record, and a trimmed abort
\* is answered by the mark alone.
HomeDecide(t) ==
    /\ t > hlw
    /\ hgrant[t] = Parts
    /\ Winner(t) = None
    /\ ~PendingDecision(t)
    /\ Propose("commit", t)
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, hlw, partVars, started, pre,
                    writes, msgs >>

\* The prepare round did not finish in time.
HomeGiveUp(t) ==
    /\ t \in started
    /\ t > hlw
    /\ Winner(t) = None
    /\ ~PendingDecision(t)
    /\ Propose("abort", t)
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, hlw, partVars, started, pre,
                    writes, msgs >>

\* A lock past its timeout asks home. Home answers from its committed log
\* only, so its abort can race a commit still proposed; first record wins
\* settles it. A txn at or below the mark is retired: a lock still held for
\* it can only be a late lock of an aborted commit, so home releases it.
HomeAnswer(m) ==
    /\ m.type = "ask"
    /\ Winner(m.t) = None
    /\ IF m.t <= hlw
         THEN /\ Send(Msg("release", m.t, m.p, hterm, FALSE, TRUE, "real"))
              /\ UNCHANGED hpend
         ELSE /\ ~Pending("abort", m.t)
              /\ Propose("abort", m.t)
              /\ UNCHANGED msgs
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, hlw, partVars, started, pre, writes >>

HomeConfirm(m) ==
    /\ m.type = "applied"
    /\ SaysCommit(m.t)
    /\ m.p \notin hconf[m.t]
    /\ hconf' = [hconf EXCEPT ![m.t] = @ \cup {m.p}]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, htrim, hlw, partVars, started, pre,
                    writes, msgs >>

HomeReceive(m) == m \in msgs /\ (HomeVote(m) \/ HomeAnswer(m) \/ HomeConfirm(m))

HomeCommitHead ==
    /\ hpend /= << >>
    /\ hlog' = Append(hlog, Head(hpend))
    /\ hpend' = Tail(hpend)
    /\ UNCHANGED << hterm, hgrant, hconf, htrim, hlw, partVars, started, pre, writes, msgs >>

\* Home's own part, appended behind the commit decision.
HomePart(t) ==
    /\ SaysCommit(t)
    /\ AllIdx("part", t) = {}
    /\ ~Pending("part", t)
    /\ Propose("part", t)
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, hlw, partVars, started, pre,
                    writes, msgs >>

\* Home sends the decision until the txn is done: apply or release.
HomeDrive(t, p) ==
    /\ ~HHas("done", t)
    /\ \/ SaysCommit(t) /\ Send(Msg("apply", t, p, hterm, FALSE, TRUE, "real"))
       \/ SaysAbort(t)  /\ Send(Msg("release", t, p, hterm, FALSE, TRUE, "real"))
    /\ UNCHANGED << homeVars, partVars, started, pre, writes >>

HomeRetire(t) ==
    /\ SaysCommit(t)
    /\ hconf[t] = Parts
    /\ AllIdx("part", t) /= {}
    /\ ~HHas("done", t)
    /\ ~Pending("done", t)
    /\ Propose("done", t)
    /\ UNCHANGED << hlog, hterm, hgrant, hconf, htrim, hlw, partVars, started, pre,
                    writes, msgs >>

\* The mark moves past a txn once it is aborted or done.
HomeAdvance ==
    /\ hlw + 1 \in started
    /\ Winner(hlw + 1) = "abort" \/ HHas("done", hlw + 1)
    /\ hlw' = hlw + 1
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, htrim, partVars, started, pre,
                    writes, msgs >>

HomeLowWater(p) ==
    /\ hlw > 0
    /\ Send(Msg("lw", hlw, p, hterm, FALSE, TRUE, "real"))
    /\ UNCHANGED << homeVars, partVars, started, pre, writes >>

\* Retention removes a txn's home records, only once the mark covers it.
HomeTrim(t) ==
    /\ htrim[t] = 0
    /\ Decided(t) /= None
    /\ TrimUndone \/ t <= hlw
    /\ htrim' = [htrim EXCEPT ![t] = Len(hlog)]
    /\ UNCHANGED << hlog, hpend, hterm, hgrant, hconf, hlw, partVars, started, pre,
                    writes, msgs >>

HomeLeaderChange ==
    /\ hterm < MaxTerm
    /\ \E n \in 0..Len(hpend) : hlog' = hlog \o SubSeq(hpend, 1, n)
    /\ hpend' = << >>
    /\ hterm' = hterm + 1
    /\ hgrant' = [t \in Txns |-> {}]
    /\ hconf' = [t \in Txns |-> {}]
    /\ UNCHANGED << htrim, hlw, partVars, started, pre, writes, msgs >>

------------------------------------------------------------------------------
\* A participant's leader proposes a record for a message; what the record
\* does is decided when it commits (ParticipantCommit).
ParticipantReceive(p, m) ==
    /\ m \in msgs
    /\ m.p = p
    /\ m.type \in {"prepare", "apply", "release", "lw"}
    /\ \A i \in 1..Len(ppend[p]) : ppend[p][i] /= m
    /\ ppend' = [ppend EXCEPT ![p] = Append(@, m)]
    /\ UNCHANGED << homeVars, plog, pterm, forgot, started, pre, writes, msgs >>

\* A single-shard commit on the participant's key.
SingleWrite(p) ==
    /\ writes < MaxWrites
    /\ WriteCheckEarly => ~Locked(p)
    /\ ppend' = [ppend EXCEPT ![p] = Append(@, Msg("write", 0, p, 0, FALSE, TRUE, "real"))]
    /\ writes' = writes + 1
    /\ UNCHANGED << homeVars, plog, pterm, forgot, started, pre, msgs >>

\* A repeated prepare gets the original answer. A stale one, or one whose
\* token does not check, gets nothing. Otherwise the lock is granted if no
\* other txn holds the key and the precondition holds, here in the log.
CommitPrepare(p, m) ==
    IF Known(p, m.t)
      THEN /\ Send(Msg("vote", m.t, p, 0, LastLock(p, m.t).ok, TRUE, "real"))
           /\ UNCHANGED plog
      ELSE IF Stale(p, m) \/ ~(m.auth \/ NoTokenCheck)
        THEN UNCHANGED << plog, msgs >>
        ELSE LET ok == /\ ~\E u \in Txns : u /= m.t /\ Holds(p, u)
                       /\ pre[m.t][p] \in {Any, Ver(p)}
             IN /\ PAppend(p, PRec("lock", m.t, ok, m.auth, m.ht, 0, m.d))
                /\ Send(Msg("vote", m.t, p, 0, ok, TRUE, "real"))

\* An apply writes only against a lock this participant holds, with the
\* operations its prepare named; the record releases the lock. So a resent
\* apply writes nothing, and a forged one matches no lock.
CommitApply(p, m) ==
    IF \/ ApplyWithoutLock
       \/ Holds(p, m.t) /\ LastLock(p, m.t).d = m.d
      THEN /\ PAppend(p, PRec("apply", m.t, TRUE, m.auth, 0, Ver(p), m.d))
           /\ Send(Msg("applied", m.t, p, 0, TRUE, TRUE, "real"))
      ELSE IF Applied(p, m.t) \/ m.t <= Lw(p)
        THEN /\ Send(Msg("applied", m.t, p, 0, TRUE, TRUE, "real"))
             /\ UNCHANGED plog
        ELSE UNCHANGED << plog, msgs >>

CommitRelease(p, m) ==
    /\ IF Holds(p, m.t)
         THEN PAppend(p, PRec("release", m.t, TRUE, TRUE, 0, 0, "real"))
         ELSE UNCHANGED plog
    /\ UNCHANGED msgs

\* The fence is written before any id under it is forgotten.
CommitFence(p, m) ==
    /\ IF m.t > Lw(p)
         THEN PAppend(p, PRec("fence", m.t, TRUE, TRUE, m.ht, 0, "real"))
         ELSE UNCHANGED plog
    /\ UNCHANGED msgs

\* key_locked is decided at the write's place in the log.
CommitWrite(p) ==
    /\ IF WriteCheckEarly \/ ~Locked(p)
         THEN PAppend(p, PRec("write", 0, TRUE, TRUE, 0, 0, "real"))
         ELSE UNCHANGED plog
    /\ UNCHANGED msgs

ParticipantCommit(p) ==
    /\ ppend[p] /= << >>
    /\ LET m == Head(ppend[p]) IN
         CASE m.type = "prepare" -> CommitPrepare(p, m)
           [] m.type = "apply"   -> CommitApply(p, m)
           [] m.type = "release" -> CommitRelease(p, m)
           [] m.type = "lw"      -> CommitFence(p, m)
           [] m.type = "write"   -> CommitWrite(p)
    /\ ppend' = [ppend EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED << homeVars, pterm, forgot, started, pre, writes >>

\* A lock past its timeout asks home. Timers are left out: asking is safe
\* at any time, and a leader change only restarts them.
ParticipantAsk(p, t) ==
    /\ Holds(p, t)
    /\ Send(Msg("ask", t, p, 0, FALSE, TRUE, "real"))
    /\ UNCHANGED << homeVars, partVars, started, pre, writes >>

TimeoutRelease(p, t) ==
    /\ ReleaseOnTimeout
    /\ Holds(p, t)
    /\ PAppend(p, PRec("release", t, TRUE, TRUE, 0, 0, "real"))
    /\ UNCHANGED << homeVars, ppend, pterm, forgot, started, pre, writes, msgs >>

\* The dedup table drops a finished id, only under the fence.
ParticipantForget(p, t) ==
    /\ PIdx(p, {"lock"}, t) /= {}
    /\ ~Holds(p, t)
    /\ t \notin forgot[p]
    /\ NoFence \/ t <= Lw(p)
    /\ forgot' = [forgot EXCEPT ![p] = @ \cup {t}]
    /\ UNCHANGED << homeVars, plog, ppend, pterm, started, pre, writes, msgs >>

ParticipantLeaderChange(p) ==
    /\ pterm[p] < MaxTerm
    /\ \E n \in 0..Len(ppend[p]) : ppend' = [ppend EXCEPT ![p] = SubSeq(@, 1, n)]
    /\ pterm' = [pterm EXCEPT ![p] = @ + 1]
    /\ UNCHANGED << homeVars, plog, forgot, started, pre, writes, msgs >>

Lose(m) ==
    /\ m \in msgs
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED << homeVars, partVars, started, pre, writes >>

------------------------------------------------------------------------------
Next ==
    \/ \E t \in Txns : \/ ClientStart(t) \/ HomeDecide(t) \/ HomeGiveUp(t)
                       \/ HomePart(t) \/ HomeRetire(t) \/ HomeTrim(t)
    \/ \E t \in Txns, p \in Parts :
          \/ HomePrepare(t, p) \/ HomeDrive(t, p) \/ Forge(t, p)
          \/ ParticipantAsk(p, t) \/ TimeoutRelease(p, t) \/ ParticipantForget(p, t)
    \/ \E m \in msgs : HomeReceive(m) \/ Lose(m)
    \/ \E p \in Parts, m \in msgs : ParticipantReceive(p, m)
    \/ \E p \in Parts : \/ ParticipantCommit(p) \/ ParticipantLeaderChange(p)
                        \/ SingleWrite(p) \/ HomeLowWater(p)
    \/ HomeCommitHead \/ HomeAdvance \/ HomeLeaderChange

Terms == 0..MaxTerm

\* Groups keep committing and home keeps driving. A message that keeps being
\* sent is eventually received: strong fairness per message kind, txn,
\* shard and term, since Lose can disable a receive for a while. Leader
\* changes are bounded by MaxTerm.
Fairness ==
    /\ WF_vars(HomeCommitHead)
    /\ WF_vars(HomeAdvance)
    /\ \A t \in Txns : /\ WF_vars(HomeDecide(t)) /\ WF_vars(HomeGiveUp(t))
                       /\ WF_vars(HomePart(t)) /\ WF_vars(HomeRetire(t))
    /\ \A t \in Txns, p \in Parts :
          /\ WF_vars(HomePrepare(t, p)) /\ WF_vars(HomeDrive(t, p))
          /\ WF_vars(ParticipantAsk(p, t))
    /\ \A p \in Parts : WF_vars(ParticipantCommit(p))
    /\ \A p \in Parts, ty \in {"prepare", "apply", "release"}, t \in Txns, h \in Terms :
          SF_vars(\E m \in msgs : /\ m.type = ty /\ m.t = t /\ m.ht = h
                                  /\ ParticipantReceive(p, m))
    /\ \A p \in Parts, ty \in {"vote", "applied", "ask"}, t \in Txns :
          SF_vars(\E m \in msgs : /\ m.type = ty /\ m.t = t /\ m.p = p
                                  /\ HomeReceive(m))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
\* Invariants.

\* All parts or none: an applied part, home's included, means the decision
\* was commit; a release means it was abort; what home acts on never
\* contradicts its first decision; a done txn is applied everywhere.
Atomicity ==
    \A t \in Txns :
        /\ Winner(t) \in {None, Decided(t)}
        /\ AllIdx("part", t) /= {} => Decided(t) = "commit"
        /\ \A p \in Parts : Applied(p, t) => Decided(t) = "commit"
        /\ \A p \in Parts : Released(p, t) => Decided(t) = "abort"
        /\ AllIdx("done", t) /= {} => \A p \in Parts : Applied(p, t)

NoDoubleApply ==
    /\ \A p \in Parts, t \in Txns : Cardinality(PIdx(p, {"apply"}, t)) <= 1
    /\ \A t \in Txns : Cardinality(AllIdx("part", t)) <= 1

\* Each part applies on the version its precondition named.
PreconditionsHeld ==
    \A p \in Parts : \A i \in 1..Len(plog[p]) :
        plog[p][i].k = "apply" => pre[plog[p][i].t][p] \in {Any, plog[p][i].ver}

\* Every apply carries a client's operations for a txn the client sent,
\* and lands on a lock granted to a prepare whose token checked.
NoForgedApply ==
    \A p \in Parts : \A i \in 1..Len(plog[p]) :
        LET r == plog[p][i] IN
        r.k = "apply" =>
            /\ r.d = "real"
            /\ r.t \in started
            /\ \E j \in 1..(i - 1) : /\ plog[p][j].k = "lock" /\ plog[p][j].t = r.t
                                     /\ plog[p][j].ok /\ plog[p][j].auth

\* Liveness: every txn the client sent is retired: committed, applied
\* everywhere and done, or aborted with no lock left on it.
Retired(t) ==
    \/ Decided(t) = "commit" /\ AllIdx("done", t) /= {}
    \/ Decided(t) = "abort" /\ \A p \in Parts : ~Holds(p, t)

EventuallyRetired == \A t \in Txns : (t \in started) ~> Retired(t)

=============================================================================
