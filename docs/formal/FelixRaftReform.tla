--------------------------- MODULE FelixRaftReform ---------------------------
(***************************************************************************)
(* Re-forming a Raft group through the control plane, as                   *)
(* docs/per-shard-raft.md decides it ("Re-forming a group", "Shards with   *)
(* one replica", Questions 1 and 10).                                      *)
(*                                                                         *)
(* A group id carries a group epoch that starts at 0. A member id is a     *)
(* broker and a copy id. A copy that rotates has a new id that never       *)
(* voted, and keeps what is left of its log as plain records. When too     *)
(* many voters have rotated, or nobody can elect, the control plane        *)
(* re-forms the group in five steps:                                       *)
(*                                                                         *)
(*   1. FirstReport -- a leaderless copy tells the control plane its       *)
(*                     member id and the newest membership in its log.     *)
(*                     It promises nothing.                                *)
(*   2. AskRetire   -- the control plane asks only when every voter's      *)
(*                     broker reported the group leaderless, or a          *)
(*                     majority of some voter set has rotated.             *)
(*   3. Retire      -- an asked copy stops its member behind a durable     *)
(*                     marker, then reports its log and membership.        *)
(*   4. Reform      -- when every voter of every membership any report     *)
(*                     names has retired and reported, the control plane   *)
(*                     writes the next epoch by compare-and-swap: the base *)
(*                     is the highest reported last log id, the voters are *)
(*                     the copies whose reports hold it.                   *)
(*   5. StartEpoch  -- members start at the new epoch from the base.       *)
(*                     Messages carry the epoch.                           *)
(*                                                                         *)
(* Membership inside an epoch changes through a joint configuration, and a *)
(* member uses the newest membership in its log, committed or not.         *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   AckedSurvive       -- an acknowledged record is in the base of every  *)
(*                         later epoch and in the log of every later       *)
(*                         leader, unless it is a documented loss: at the  *)
(*                         re-form no disk held it, a voter's or not.      *)
(*   StateMachineSafety -- no index is committed with two different        *)
(*                         entries, in one epoch or across epochs, except  *)
(*                         over a documented loss.                         *)
(*   OneLeaderPerTerm   -- no term of an epoch elects two brokers.         *)
(*   NothingLost        -- no documented loss at all. Holds when no copy   *)
(*                         rotates, which is fsync on commit.              *)
(*   OffsetNeverReused  -- StateMachineSafety without the exception. It    *)
(*                         holds when nothing is lost. After a relaxed     *)
(*                         fsync loss the lost offsets are given out again *)
(*                         (FelixRaftReformTwoRotatedOffsets.cfg).         *)
(*   SlowGroupLeftAlone -- with no copy rotated and a voter that has not   *)
(*                         reported, nothing is retired.                   *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   ReformBeforeAll    -- the control plane re-forms from the reports it  *)
(*                         has.                                            *)
(*   EpochNotChecked    -- a member takes an append from another epoch.    *)
(*   ReportNotPromise   -- a copy reports without retiring.                *)
(*   OwnMembershipOnly  -- the control plane waits only for the voters in  *)
(*                         its own record of the membership.               *)
(*   RetireNotDurable   -- a restart forgets the retired marker.           *)
(*   BaseNotHighest     -- the base is any report's log.                   *)
(*   NoCas              -- the next epoch is written without comparing the *)
(*                         epoch it was computed from.                     *)
(*   RotatedNotWaited   -- a voter known to have rotated counts as done    *)
(*                         before it has reported its records.             *)
(*   JoinKeepsReport    -- a copy that rotates after it reported joins as  *)
(*                         a learner while the control plane still holds   *)
(*                         the old report. With the knob off the new copy  *)
(*                         announces itself first and the control plane    *)
(*                         drops the report.                               *)
(*                                                                         *)
(* The design page does not have the rules the last two knobs remove.     *)
(* Step 4 as written is RotatedNotWaited, and nothing drops an old report, *)
(* which is JoinKeepsReport (issue 2304). A configuration that passes with *)
(* a rotation or a rejoin passes on those two rules and says so.           *)
(*                                                                         *)
(* Two constants are not knobs:                                            *)
(*                                                                         *)
(*   DirLost     -- what a rotation leaves. FALSE is a relaxed fsync run   *)
(*                  that lost power: the tail may be gone, but membership  *)
(*                  entries are synced in every fsync mode, so the log     *)
(*                  keeps everything up to its newest membership entry.    *)
(*                  TRUE is a directory that was lost or restored from an  *)
(*                  older copy: any prefix is left. It covers FALSE. With  *)
(*                  no membership change the two are the same.             *)
(*   WaitPlaced  -- the rule issue 2311 proposes. The control plane        *)
(*                  places a broker, and records the target of a           *)
(*                  membership change, before the leader sends or proposes *)
(*                  anything, and step 4 waits for a retire report from    *)
(*                  every broker placed in the epoch, voter or not. FALSE  *)
(*                  is the design as written.                              *)
(*                                                                         *)
(* Abstracted: storage is a log and a rotation that cuts it to a prefix    *)
(* (FelixRaft.tla has the fsync rules). A membership entry is synced when  *)
(* it is appended. An election is one step                                 *)
(* over a quorum. An append is one step between two brokers, plus a        *)
(* bounded set of in-flight appends that can arrive late and more than     *)
(* once. A report carries the whole log where the design sends its last    *)
(* log id. A leader commits only entries of its own term. A leader does    *)
(* not propose its own removal. A leader replicates to any broker of the   *)
(* model: one that no membership names is a learner.                       *)
(*                                                                         *)
(* What 0.8 ships. Decided (issue 2167): in 0.8 a re-form starts only from *)
(* an operator's command, one bulk command with a dry run, and nothing     *)
(* starts one automatically. Steps 1 and 2 are the automatic trigger, so   *)
(* they describe a design that is not shipping, and so do                  *)
(* SlowGroupLeftAlone and the configurations about when AskRetire fires    *)
(* (FelixRaftReformSlow). Steps 3 to 5 are what the command still has to   *)
(* do. It stands where AskRetire does, and it must list every placed       *)
(* broker, a move's target included, which is WaitPlaced (issue 2311).     *)
(*                                                                         *)
(* Not modelled: the command itself. Here step 2 fires only under the      *)
(* automatic conditions, so a command given to a group that can still      *)
(* elect is not covered. Neither is its refusal while a placed broker is   *)
(* unreachable and not declared dead, nor a re-form that goes on without a *)
(* dead broker.                                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,
    InitBrokers,        \* brokers of the first voter set
    None,
    MaxEpoch,
    MaxTerm,
    MaxWrites,
    MaxChangeSteps,     \* 0, or 2 for one joint change
    ChangeOut,          \* broker the change removes
    ChangeIn,           \* broker the change adds
    MaxRotations,
    DirLost,
    MaxRejoins,
    MaxRestarts,
    MaxMsgs,
    ReformBeforeAll,
    EpochNotChecked,
    ReportNotPromise,
    OwnMembershipOnly,
    RetireNotDurable,
    BaseNotHighest,
    NoCas,
    RotatedNotWaited,
    JoinKeepsReport,
    WaitPlaced

VARIABLES
    ep,         \* group epoch of the broker's member
    cid,        \* copy id
    term,       \* highest term voted in or heard of, per epoch
    role,
    log,
    commit,     \* a leader's commit index
    match,      \* a leader's record of what each voter acknowledged
    retired,    \* the retired marker for the member's epoch
    boot,       \* membership the member started its epoch with
    msgs,       \* appends in flight
    cpEpoch,
    cpMem,      \* the control plane's own record of the membership
    firstRep,
    asked,      \* epochs whose copies were asked to retire
    rep,        \* retire reports
    forms,      \* epochs written: epoch, base, voters
    acked,      \* history: entries committed, by epoch, term and index
    lost,       \* history: acknowledged entries no disk held at a re-form
    elected,    \* history: epoch, term, broker
    writes, steps, rotations, rejoins, restarts

vars == <<ep, cid, term, role, log, commit, match, retired, boot, msgs,
          cpEpoch, cpMem, firstRep, asked, rep, forms, acked, lost, elected,
          writes, steps, rotations, rejoins, restarts>>

Mid(b) == <<b, cid[b]>>
Simple(V) == [o |-> V, n |-> V]
Sets(c) == {c.o, c.n}
Voters(c) == c.o \cup c.n
InitConfig == Simple({<<b, 0>> : b \in InitBrokers})

\* First voters the membership change does not single out are interchangeable.
Symm == Permutations(InitBrokers \ {ChangeOut, ChangeIn})

Min(a, b) == IF a < b THEN a ELSE b

IsPrefix(s, t) == Len(s) <= Len(t) /\ \A j \in 1..Len(s) : s[j] = t[j]

CommonPrefix(s, t) ==
    LET m == Min(Len(s), Len(t))
        n == CHOOSE k \in 0..m :
                /\ \A j \in 1..k : s[j] = t[j]
                /\ k = m \/ s[k + 1] # t[k + 1]
    IN SubSeq(s, 1, n)

Has(l, x) == x.i <= Len(l) /\ l[x.i] = x.ent

LastKey(l) == IF Len(l) = 0 THEN <<0, 0>> ELSE <<l[Len(l)].e, l[Len(l)].t>>
KeyLess(k1, k2) == k1[1] < k2[1] \/ (k1[1] = k2[1] /\ k1[2] < k2[2])

\* Raft's election restriction, on the last log id. The control plane uses
\* the same order to pick the base.
UpToDate(a, b) ==
    \/ KeyLess(LastKey(b), LastKey(a))
    \/ LastKey(a) = LastKey(b) /\ Len(a) >= Len(b)

\* The newest membership entry of this epoch in the log, committed or not.
\* Entries of an earlier epoch are plain records.
ConfigIn(l, e, start) ==
    LET idx == {j \in 1..Len(l) : l[j].k = "cfg" /\ l[j].e = e}
    IN IF idx = {} THEN start
       ELSE l[CHOOSE j \in idx : \A h \in idx : h <= j].v

Config(b) == ConfigIn(log[b], ep[b], boot[b])

LastCfgIndex(b) ==
    LET idx == {j \in 1..Len(log[b]) : log[b][j].k = "cfg" /\ log[b][j].e = ep[b]}
    IN IF idx = {} THEN 0 ELSE CHOOSE j \in idx : \A h \in idx : h <= j

\* How much of a log a relaxed fsync run cannot lose: a membership entry is
\* synced in every fsync mode, and a sync covers everything before it.
SyncedLen(l) ==
    LET idx == {j \in 1..Len(l) : l[j].k = "cfg"}
    IN IF idx = {} THEN 0 ELSE CHOOSE j \in idx : \A h \in idx : h <= j

\* Q is a set of brokers whose current copies answered.
Quorum(c, Q) ==
    \A S \in Sets(c) :
        2 * Cardinality({m \in S : m[1] \in Q /\ m = Mid(m[1])}) > Cardinality(S)

\* A leader counts the acknowledgements it recorded, which may be from a
\* copy that has rotated since.
AckQuorum(c, l, Q) ==
    \A S \in Sets(c) :
        2 * Cardinality({m \in S : m[1] \in Q /\ (m[1] = l => m = Mid(l))})
            > Cardinality(S)

\* Under the announce rule a copy that rotated after reporting takes no
\* append until the control plane has dropped that report.
StaleReport(b) ==
    rep[b] # None /\ rep[b].e = ep[b] /\ rep[b].cid < cid[b]
MayJoin(b) == JoinKeepsReport \/ ~StaleReport(b)

Init ==
    /\ ep = [b \in Brokers |-> 0]
    /\ cid = [b \in Brokers |-> 0]
    /\ term = [b \in Brokers |-> 0]
    /\ role = [b \in Brokers |-> "follower"]
    /\ log = [b \in Brokers |-> <<>>]
    /\ commit = [b \in Brokers |-> 0]
    /\ match = [b \in Brokers |-> [f \in Brokers |-> 0]]
    /\ retired = [b \in Brokers |-> FALSE]
    /\ boot = [b \in Brokers |-> InitConfig]
    /\ msgs = {}
    /\ cpEpoch = 0
    /\ cpMem = InitConfig
    /\ firstRep = [b \in Brokers |-> None]
    /\ asked = {}
    /\ rep = [b \in Brokers |-> None]
    /\ forms = {}
    /\ acked = {}
    /\ lost = {}
    /\ elected = {}
    /\ writes = 0 /\ steps = 0 /\ rotations = 0 /\ rejoins = 0 /\ restarts = 0

cpVars == <<cpEpoch, cpMem, firstRep, asked, rep, forms, lost>>
counters == <<writes, steps, rotations, rejoins, restarts>>

--------------------------------------------------------------------------------
\* Raft inside one epoch.

Elect(b, Q) ==
    /\ b \in Q
    /\ term[b] < MaxTerm
    /\ Mid(b) \in Voters(Config(b))
    /\ Quorum(Config(b), Q)
    /\ \A q \in Q :
        /\ ep[q] = ep[b]
        /\ ~retired[q]
        /\ term[q] <= term[b]
        /\ UpToDate(log[b], log[q])
    /\ term' = [q \in Brokers |-> IF q \in Q THEN term[b] + 1 ELSE term[q]]
    /\ role' = [q \in Brokers |->
                    IF q = b THEN "leader" ELSE IF q \in Q THEN "follower" ELSE role[q]]
    /\ match' = [match EXCEPT ![b] = [f \in Brokers |-> 0]]
    /\ elected' = elected \cup {[e |-> ep[b], t |-> term[b] + 1, b |-> b]}
    /\ UNCHANGED <<ep, cid, log, commit, retired, boot, msgs, acked>>
    /\ UNCHANGED <<cpVars, counters>>

Leads(l) == role[l] = "leader" /\ ~retired[l]

ClientWrite(l) ==
    /\ Leads(l)
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ log' = [log EXCEPT ![l] =
                Append(@, [e |-> ep[l], t |-> term[l], k |-> "rec", v |-> writes + 1])]
    /\ UNCHANGED <<ep, cid, term, role, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, steps, rotations, rejoins, restarts>>

\* One joint change: old set, then old and new, then new. The next step
\* waits for the previous membership entry to commit.
ProposeChange(l) ==
    /\ Leads(l)
    /\ steps < MaxChangeSteps
    /\ l # ChangeOut
    /\ LastCfgIndex(l) <= commit[l]
    /\ LET c == Config(l)
           next == IF c.o = c.n
                   THEN [o |-> c.o,
                         n |-> {m \in c.o : m[1] # ChangeOut} \cup {Mid(ChangeIn)}]
                   ELSE Simple(c.n)
       IN /\ c.o = c.n => (ep[ChangeIn] = ep[l] /\ Mid(ChangeIn) \notin c.o)
          /\ log' = [log EXCEPT ![l] =
                        Append(@, [e |-> ep[l], t |-> term[l], k |-> "cfg", v |-> next])]
    /\ steps' = steps + 1
    /\ UNCHANGED <<ep, cid, term, role, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, writes, rotations, rejoins, restarts>>

\* What a follower's log is after an append carrying the sender's log. It
\* never cuts entries that agree with the sender.
Adopt(mine, theirs) == IF IsPrefix(theirs, mine) THEN mine ELSE theirs

Replicate(l, f) ==
    /\ l # f
    /\ Leads(l)
    /\ ~retired[f]
    /\ ep[f] = ep[l]
    /\ term[f] <= term[l]
    /\ MayJoin(f)
    /\ LET new == Adopt(log[f], log[l])
       IN /\ new # log[f] \/ term[f] # term[l] \/ role[f] # "follower"
             \/ match[l][f] # Len(log[l])
          /\ log' = [log EXCEPT ![f] = new]
          /\ match' = [match EXCEPT ![l][f] =
                        IF Mid(f) \in Voters(Config(l)) THEN Len(log[l]) ELSE @]
    /\ term' = [term EXCEPT ![f] = term[l]]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ UNCHANGED <<ep, cid, commit, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>

Send(l) ==
    /\ Leads(l)
    /\ Cardinality(msgs) < MaxMsgs
    /\ log[l] # <<>>
    /\ msgs' = msgs \cup {[e |-> ep[l], t |-> term[l], log |-> log[l]]}
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>

\* A late append. The sender may be gone, so nothing is acknowledged. The
\* message stays and can arrive again.
Receive(f, m) ==
    /\ ~retired[f]
    /\ EpochNotChecked \/ m.e = ep[f]
    /\ m.t > term[f] \/ (m.t = term[f] /\ role[f] # "leader")
    /\ MayJoin(f)
    /\ Adopt(log[f], m.log) # log[f]
    /\ log' = [log EXCEPT ![f] = Adopt(@, m.log)]
    /\ term' = [term EXCEPT ![f] = m.t]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ UNCHANGED <<ep, cid, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>

Commit(l, i) ==
    /\ Leads(l)
    /\ i > commit[l]
    /\ i <= Len(log[l])
    /\ log[l][i].e = ep[l]
    /\ log[l][i].t = term[l]
    /\ AckQuorum(Config(l), l, {l} \cup {f \in Brokers \ {l} : match[l][f] >= i})
    /\ commit' = [commit EXCEPT ![l] = i]
    /\ acked' = acked \cup
            {[e |-> ep[l], t |-> term[l], i |-> j, ent |-> log[l][j]] :
                j \in (commit[l] + 1)..i}
    /\ UNCHANGED <<ep, cid, term, role, log, match, retired, boot, msgs, elected>>
    /\ UNCHANGED <<cpVars, counters>>

--------------------------------------------------------------------------------
\* Faults.

\* A restart keeps the log, the term and the retired marker.
Restart(b) ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ retired' = [retired EXCEPT ![b] = IF RetireNotDurable THEN FALSE ELSE @]
    /\ UNCHANGED <<ep, cid, term, log, commit, match, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, writes, steps, rotations, rejoins>>

NewCopy(b, kept) ==
    /\ cid' = [cid EXCEPT ![b] = @ + 1]
    /\ log' = [log EXCEPT ![b] = kept]
    /\ term' = [term EXCEPT ![b] = 0]
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ commit' = [commit EXCEPT ![b] = 0]
    /\ retired' = [retired EXCEPT ![b] = FALSE]
    /\ UNCHANGED <<ep, match, boot, msgs, acked, elected>>

\* The copy id rotates. A relaxed fsync run that lost power keeps its synced
\* membership entries. A directory that was lost, or that failed the restore
\* check, keeps any prefix.
Rotate(b, n) ==
    /\ rotations < MaxRotations
    /\ rotations' = rotations + 1
    /\ n \in 0..Len(log[b])
    /\ DirLost \/ n >= SyncedLen(log[b])
    /\ NewCopy(b, SubSeq(log[b], 1, n))
    /\ UNCHANGED <<cpVars, writes, steps, rejoins, restarts>>

\* A retired copy whose group went on without a re-form cannot take its
\* promise back. It rotates and comes back as a learner.
RetiredRejoins(b) ==
    /\ retired[b]
    /\ rejoins < MaxRejoins
    /\ \E l \in Brokers : Leads(l) /\ ep[l] = ep[b]
    /\ rejoins' = rejoins + 1
    /\ NewCopy(b, log[b])
    /\ UNCHANGED <<cpVars, writes, steps, rotations, restarts>>

--------------------------------------------------------------------------------
\* The control plane and the five steps.

At(r, e) == r # None /\ r.e = e

FirstReport(b) ==
    /\ role[b] # "leader"
    /\ ep[b] = cpEpoch
    /\ ep[b] \notin asked
    /\ LET r == [e |-> ep[b], cid |-> cid[b], mem |-> Config(b)]
       IN /\ firstRep[b] # r
          /\ firstRep' = [firstRep EXCEPT ![b] = r]
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
    /\ UNCHANGED <<cpEpoch, cpMem, asked, rep, forms, acked, lost, elected, counters>>

\* The control plane's record follows broker reports and can be stale.
CpObserve(b) ==
    /\ ep[b] = cpEpoch
    /\ cpMem # Config(b)
    /\ cpMem' = Config(b)
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
    /\ UNCHANGED <<cpEpoch, firstRep, asked, rep, forms, acked, lost, elected, counters>>

KnownConfigs(e) == {cpMem} \cup {firstRep[b].mem : b \in {x \in Brokers : At(firstRep[x], e)}}

AskRetire ==
    LET e == cpEpoch
        known == KnownConfigs(e)
        all == UNION {Voters(c) : c \in known}
        rotated(m) == At(firstRep[m[1]], e) /\ firstRep[m[1]].cid > m[2]
    IN /\ e < MaxEpoch
       /\ e \notin asked
       /\ \/ \A m \in all : At(firstRep[m[1]], e)
          \/ \E c \in known : \E S \in Sets(c) :
                2 * Cardinality({m \in S : rotated(m)}) > Cardinality(S)
       /\ asked' = asked \cup {e}
       /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
       /\ UNCHANGED <<cpEpoch, cpMem, firstRep, rep, forms, acked, lost, elected, counters>>

\* Retire first, then report. The report is the log as it will stay.
Retire(b) ==
    /\ ep[b] \in asked
    /\ LET r == [e |-> ep[b], cid |-> cid[b], log |-> log[b], mem |-> Config(b)]
       IN /\ rep[b] # r \/ (~ReportNotPromise /\ ~retired[b])
          /\ rep' = [rep EXCEPT ![b] = r]
    /\ IF ReportNotPromise
       THEN UNCHANGED <<retired, role>>
       ELSE /\ retired' = [retired EXCEPT ![b] = TRUE]
            /\ role' = [role EXCEPT ![b] = "follower"]
    /\ UNCHANGED <<ep, cid, term, log, commit, match, boot, msgs>>
    /\ UNCHANGED <<cpEpoch, cpMem, firstRep, asked, forms, acked, lost, elected, counters>>

\* A new copy tells the control plane its id before it joins, and the
\* control plane drops the report of the copy it replaces.
Announce(b) ==
    /\ ~JoinKeepsReport
    /\ StaleReport(b)
    /\ ep[b] = cpEpoch
    /\ rep' = [rep EXCEPT ![b] = None]
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
    /\ UNCHANGED <<cpEpoch, cpMem, firstRep, asked, forms, acked, lost, elected, counters>>

\* Brokers placed in the epoch: every broker of the model. A broker that no
\* membership names holds its copy as a learner, and the control plane
\* placed it before the leader sent it anything.
Placed == Brokers

Reform(e, pick) ==
    LET R == {b \in Brokers : At(rep[b], e)}
        named == Voters(cpMem) \cup
                 (IF OwnMembershipOnly THEN {}
                  ELSE UNION {Voters(rep[b].mem) : b \in R} \cup
                       UNION {Voters(firstRep[b].mem) :
                                b \in {x \in Brokers : At(firstRep[x], e)}})
        done(m) ==
            \/ m[1] \in R /\ rep[m[1]].cid >= m[2]
            \/ RotatedNotWaited /\ At(firstRep[m[1]], e) /\ firstRep[m[1]].cid > m[2]
        base == rep[pick].log
        voters == {<<b, rep[b].cid>> : b \in {x \in R : IsPrefix(base, rep[x].log)}}
    IN /\ e < MaxEpoch
       /\ e \in asked
       /\ IF NoCas THEN e <= cpEpoch ELSE e = cpEpoch
       /\ pick \in R
       /\ ReformBeforeAll \/ \A m \in named : done(m)
       /\ ReformBeforeAll \/ ~WaitPlaced \/ Placed \subseteq R
       /\ BaseNotHighest \/ \A b \in R : UpToDate(base, rep[b].log)
       /\ [e |-> e + 1, base |-> base, voters |-> voters] \notin forms
       /\ forms' = forms \cup {[e |-> e + 1, base |-> base, voters |-> voters]}
       /\ cpEpoch' = e + 1
       /\ cpMem' = Simple(voters)
       \* Everything at or below the base is served as committed from here.
       /\ acked' = acked \cup
            {[e |-> e + 1, t |-> 0, i |-> j, ent |-> base[j]] : j \in 1..Len(base)}
       /\ lost' = lost \cup
            {x \in acked : x.e <= e /\ \A b \in Brokers : ~Has(log[b], x)}
       /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
       /\ UNCHANGED <<firstRep, asked, rep, elected, counters>>

\* A voter's report holds the base, so its log is the base. Anyone else
\* keeps what agrees with the base and is a learner.
StartEpoch(b, f) ==
    /\ f \in forms
    /\ f.e = cpEpoch
    /\ f.e > ep[b]
    /\ ep' = [ep EXCEPT ![b] = f.e]
    /\ term' = [term EXCEPT ![b] = 0]
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ retired' = [retired EXCEPT ![b] = FALSE]
    /\ boot' = [boot EXCEPT ![b] = Simple(f.voters)]
    /\ log' = [log EXCEPT ![b] = CommonPrefix(@, f.base)]
    /\ commit' = [commit EXCEPT ![b] = Len(CommonPrefix(log[b], f.base))]
    /\ UNCHANGED <<cid, match, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>

Next ==
    \/ \E b \in Brokers : \E Q \in SUBSET Brokers : Elect(b, Q)
    \/ \E b \in Brokers : ClientWrite(b) \/ ProposeChange(b) \/ Send(b)
    \/ \E l, f \in Brokers : Replicate(l, f)
    \/ \E f \in Brokers : \E m \in msgs : Receive(f, m)
    \/ \E l \in Brokers : \E i \in 1..Len(log[l]) : Commit(l, i)
    \/ \E b \in Brokers : Restart(b) \/ RetiredRejoins(b)
    \/ \E b \in Brokers : \E n \in 0..Len(log[b]) : Rotate(b, n)
    \/ \E b \in Brokers : FirstReport(b) \/ CpObserve(b) \/ Retire(b) \/ Announce(b)
    \/ AskRetire
    \/ \E e \in 0..cpEpoch : \E pick \in Brokers : Reform(e, pick)
    \/ \E b \in Brokers : \E f \in forms : StartEpoch(b, f)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------------
\* Checks.

TypeOK ==
    /\ ep \in [Brokers -> 0..MaxEpoch]
    /\ cid \in [Brokers -> 0..(MaxRotations + MaxRejoins)]
    /\ term \in [Brokers -> 0..MaxTerm]
    /\ role \in [Brokers -> {"follower", "leader"}]
    /\ retired \in [Brokers -> BOOLEAN]
    /\ cpEpoch \in 0..MaxEpoch
    /\ asked \subseteq 0..MaxEpoch
    /\ \A b \in Brokers : commit[b] <= Len(log[b])
    /\ lost \subseteq acked

OneLeaderPerTerm ==
    \A x, y \in elected : x.e = y.e /\ x.t = y.t => x.b = y.b

\* An acknowledged record outlives its epoch. The only loss allowed is the
\* one the design documents: no surviving disk held the record any more.
AckedSurvive ==
    \A x \in acked \ lost :
        /\ \A f \in forms : f.e > x.e => Has(f.base, x)
        /\ \A b \in Brokers :
            (role[b] = "leader" /\ KeyLess(<<x.e, x.t>>, <<ep[b], term[b]>>))
                => Has(log[b], x)

StateMachineSafety ==
    \A x, y \in acked :
        (x.i = y.i /\ x.ent # y.ent) => (x \in lost \/ y \in lost)

OffsetNeverReused ==
    \A x, y \in acked : x.i = y.i => x.ent = y.ent

NothingLost == lost = {}

\* Step 2: a group that is only slow is not retired.
SlowGroupLeftAlone ==
    (/\ \A b \in Brokers : cid[b] = 0
     /\ \E m \in Voters(cpMem) : ~At(firstRep[m[1]], cpEpoch))
        => \A b \in Brokers : ~(retired[b] /\ ep[b] = cpEpoch)

\* Witnesses: each is an invariant that must fail.

\* A re-form completes and the new epoch commits a record of its own.
NeverCommitsAfterReform == \A x \in acked : x.e = 0 \/ x.t = 0

\* Two voters reported the group leaderless, the third never did, nothing
\* was asked or retired, and the group elected and committed by itself.
NeverSlowGroupElects ==
    ~(/\ asked = {}
      /\ \A b \in Brokers : ~retired[b] /\ cid[b] = 0
      /\ Cardinality({b \in InitBrokers : firstRep[b] # None}) = Cardinality(InitBrokers) - 1
      /\ \E x \in acked : x.ent.k = "rec")

\* A retired copy rotates and takes an append as a learner.
NeverRetiredRejoins ==
    ~(/\ rejoins > 0
      /\ \E b \in Brokers : \E l \in Brokers \ {b} :
            /\ Leads(l) /\ cid[b] > 0 /\ term[b] = term[l]
            /\ log[b] = log[l] /\ Len(log[b]) > 0)

\* The stale-membership case: the change commits and the group re-forms.
NeverReformsAfterChange ==
    ~(/\ forms # {}
      /\ \E x \in acked : x.e = 0 /\ x.ent.k = "cfg" /\ x.ent.v.o = x.ent.v.n)

\* A group re-forms with one voter, which commits a record by itself.
NeverOneVoterCommits ==
    ~(/\ \E f \in forms : Cardinality(f.voters) = 1
      /\ \E x \in acked : x.e > 0 /\ x.t > 0)
================================================================================
