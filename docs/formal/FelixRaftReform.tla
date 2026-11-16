--------------------------- MODULE FelixRaftReform ---------------------------
(***************************************************************************)
(* Re-forming a Raft group by the operator's command, as                   *)
(* docs/per-shard-raft.md decides it ("Re-forming a group", "Shards with   *)
(* one replica", Questions 10 and 13).                                     *)
(*                                                                         *)
(* A group id carries a group epoch that starts at 0. A member id is a     *)
(* broker and a copy id. A copy whose disk was lost or restored rotates:   *)
(* it has a new id that never voted, and keeps what is left of its log as  *)
(* plain records. A group that lost a majority of its voters that way      *)
(* cannot elect. Nothing repairs it automatically. An operator runs one    *)
(* command, and the model has it as these steps:                           *)
(*                                                                         *)
(*   Plan       -- the dry run. It lists every broker the control plane    *)
(*                 has placed for the group in its epoch, removed ones     *)
(*                 included, and reads each copy that is not declared      *)
(*                 dead: copy id, last log id, newest membership. It       *)
(*                 refuses while a listed copy leads, or while a copy      *)
(*                 names a voter that is neither listed nor declared dead. *)
(*                 It changes nothing.                                     *)
(*   Stop       -- apply. A listed copy stops its member behind a durable  *)
(*                 marker, unless a listed copy leads by now.              *)
(*   Reform     -- apply. Every listed copy that is not dead is stopped    *)
(*                 and is read again, and what it holds now must be what   *)
(*                 the plan showed. Then the control plane writes the next *)
(*                 epoch by compare-and-swap: the base is the highest last *)
(*                 log id read, padded with a gap up to the offset floor,  *)
(*                 and the voters are the copies that hold the base. When  *)
(*                 no copy read holds the floor's log id, the base is the  *)
(*                 longest log a copy can vouch for, and padded: a copy's  *)
(*                 whole log if its last log id has the floor's epoch and  *)
(*                 term, else its log up to the commit index it saved.     *)
(*   StartEpoch -- members start at the new epoch from the base. Messages  *)
(*                 carry the epoch.                                        *)
(*                                                                         *)
(* No answer of a copy is stored between runs. A plan holds what the       *)
(* operator saw, and Reform uses only what it reads after the stop.        *)
(*                                                                         *)
(* The control plane places a broker before the leader sends it anything,  *)
(* and a move's target before the leader proposes the change (Place). A    *)
(* broker stays placed until the leader reports the change that removes it *)
(* as committed (Unplace). The control plane also keeps every broker it    *)
(* placed in the epoch, and Unplace does not remove one from that record:  *)
(* a removed voter's disk can hold the newest acknowledged records. The    *)
(* leader reports its commit offset, and the control plane keeps the       *)
(* highest as the floor (ReportFloor). The floor is a log id: the index    *)
(* and the epoch and term of the entry there. Every copy keeps its commit  *)
(* index on disk. It can lag the leader's, never pass it, and a restart    *)
(* keeps it. A copy refuses an append that would cut its log below it.     *)
(*                                                                         *)
(* Why the floor needs the term (issue 2402): the copy left after two      *)
(* disks are lost can hold an entry of an older term that never committed, *)
(* at an offset a later leader acknowledged with another record. Its log   *)
(* reaches the floor's index without holding the floor's entry. Such a log *)
(* is cut to the commit index a copy saved, since everything up to it is   *)
(* committed. If any copy read holds the floor's entry, the highest copy   *)
(* does too, and it agrees with every committed entry up to the floor.     *)
(*                                                                         *)
(* A saved commit index can lag, so cutting every log there would lose     *)
(* records a copy still holds. A log that ends below the floor's index in  *)
(* an entry of the floor's epoch and term is kept whole: one leader wrote  *)
(* both entries into one log, so by log matching the copy's log is a       *)
(* prefix of the log the floor's entry committed in.                       *)
(*                                                                         *)
(* A restored copy of a shard with one replica starts by itself            *)
(* (SelfStart): it is the only placed broker, so no other disk can hold a  *)
(* newer record.                                                           *)
(*                                                                         *)
(* Membership inside an epoch changes through a joint configuration, and a *)
(* member uses the newest membership in its log, committed or not.         *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   AckedSurvive       -- an acknowledged record is in the base of every  *)
(*                         later epoch and in the log of every later       *)
(*                         leader, unless it is a documented loss: at the  *)
(*                         re-form no disk held it any more, not counting  *)
(*                         the disks of brokers declared dead, or no copy  *)
(*                         held the floor's entry and no disk that held    *)
(*                         the record could vouch for it. Lost disks and   *)
(*                         dead brokers are the two ways to get there.     *)
(*                         With Kept a loss is any record no copy read     *)
(*                         vouched for; KeptLossBound bounds that.         *)
(*   KeptLossBound      -- with Kept, in an unmarked group where no vote   *)
(*                         read is above every entry read, no record a     *)
(*                         read copy holds below its epoch's cap is lost.  *)
(*   StateMachineSafety -- no index is committed with two different        *)
(*                         entries, in one epoch or across epochs, except  *)
(*                         over a documented loss.                         *)
(*   OneLeaderPerTerm   -- no term of an epoch elects two brokers.         *)
(*   NothingLost        -- no documented loss at all. Holds when no disk   *)
(*                         is lost and no broker is declared dead.         *)
(*   OffsetNeverReused  -- StateMachineSafety without the exception: no    *)
(*                         offset is given out with two records. A gap is  *)
(*                         not a record.                                   *)
(*   ElectedGroupLeftAlone -- no copy is stopped while a broker leads its  *)
(*                         epoch: apply skips a group that elected after   *)
(*                         the plan.                                       *)
(*   LossNeedsLostDisk  -- a re-form loses a record only if no disk held   *)
(*                         it. Holds with one term: there a copy can vouch *)
(*                         for everything it holds.                        *)
(*   BootIsRecorded     -- the first voters a member has for its epoch are *)
(*                         the ones the control plane recorded for it.     *)
(*   PlacedIsRecorded   -- every broker a membership of the current epoch  *)
(*                         names is in the control plane's record of the   *)
(*                         brokers placed in the epoch.                    *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   ReformBeforeAll    -- the command goes on while a placed broker has   *)
(*                         not answered and is not declared dead.          *)
(*   EpochNotChecked    -- a member takes an append from another epoch.    *)
(*   ReportNotPromise   -- copies are read without being stopped.          *)
(*   PlanNotRechecked   -- apply uses the list and the answers of the      *)
(*                         plan, and does not read the copies again.       *)
(*   RotatedNotWaited   -- a rotated copy is not read.                     *)
(*   RetireNotDurable   -- a restart forgets the stop marker.              *)
(*   BaseNotHighest     -- the base is any copy's log.                     *)
(*   NoCas              -- the command does not compare the epoch.         *)
(*   SoloPlacedTwo      -- a restored copy starts by itself although a     *)
(*                         second broker is placed.                        *)
(*   StopNotRechecked   -- apply stops a copy although a listed copy       *)
(*                         leads.                                          *)
(*   PlacedForgotten    -- the list is the brokers placed now: a broker    *)
(*                         drops off once the change that removes it       *)
(*                         commits.                                        *)
(*   FloorIndexOnly     -- the floor is an offset with no term, and a base *)
(*                         is never cut (issue 2402).                      *)
(*   CutToCommit        -- a cut base ends at the saved commit index even  *)
(*                         on a copy whose last log id has the floor's     *)
(*                         epoch and term.                                 *)
(*                                                                         *)
(* Three constants are not knobs:                                          *)
(*                                                                         *)
(*   WaitPlaced -- TRUE: the command lists the placed brokers. FALSE: it   *)
(*                 lists the voters in the control plane's own record of   *)
(*                 the membership, which leaves out a move's target and a  *)
(*                 learner (issue 2311).                                   *)
(*   Floor      -- "trailing" is the design: the floor is the last commit  *)
(*                 offset a leader reported. "exact" has every commit      *)
(*                 reported at once, which the design does not do.         *)
(*                 "none" has no floor (issue 2305).                       *)
(*   DeadRuns   -- FALSE: a broker declared dead is gone. TRUE: the        *)
(*                 declaration is wrong and the broker keeps running in    *)
(*                 the old epoch.                                          *)
(*                                                                         *)
(* Abstracted: storage is a log and a rotation that cuts it to any prefix  *)
(* (FelixRaft.tla has the fsync rules; a power cut does not rotate). An    *)
(* election is one step over a quorum. An append is one step between two   *)
(* brokers, plus a bounded set of in-flight appends that can arrive late   *)
(* and more than once. A leader commits only entries of its own term, can  *)
(* step down at any step, and does not propose its own removal. It sends   *)
(* to the brokers its newest membership names and to placed brokers no     *)
(* membership of the epoch has named, which are learners. The command      *)
(* reads whole logs where the design reads last log ids. The read after    *)
(* the stop and the write of the base are one step, so a disk lost between *)
(* the two is not modelled. A removed copy keeps its directory: its        *)
(* deletion is in FelixRaftPlacement.tla. How a copy stopped by an apply   *)
(* that did not finish comes back is not decided (issue 2403); here it     *)
(* rotates. An unreachable broker is one that takes no step, and the scope *)
(* of the command is one group.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,
    InitBrokers,        \* brokers of the first voter set, all placed
    None,
    MaxEpoch,
    MaxTerm,
    MaxWrites,
    MaxChangeSteps,     \* 0, 2 for one move, or 4 for a move and the move back
    ChangeOut,          \* broker the change removes
    ChangeIn,           \* broker the change adds
    MaxPlace,           \* brokers the control plane places after the start
    MaxRotations,
    MaxRejoins,
    MaxRestarts,
    MaxMsgs,
    MaxPlans,
    MaxDead,
    DeadRuns,
    WaitPlaced,
    Floor,
    ReformBeforeAll,
    EpochNotChecked,
    ReportNotPromise,
    PlanNotRechecked,
    RotatedNotWaited,
    RetireNotDurable,
    BaseNotHighest,
    NoCas,
    SoloPlacedTwo,
    StopNotRechecked,
    PlacedForgotten,
    FloorIndexOnly,
    CutToCommit,
    Kept,               \* rotations keep every copy whole, and the re-form reads them all
    Reserve,            \* "every", "leader" or "none": who reserves before its write counts
    MaxCuts,
    MaxReturns

VARIABLES
    ep,         \* group epoch of the broker's member
    cid,        \* copy id
    term,       \* highest term voted in or heard of, per epoch
    role,
    log,
    commit,     \* commit index, saved on each copy's disk
    match,      \* a leader's record of what each voter acknowledged
    retired,    \* the stop marker for the member's epoch
    boot,       \* first voters of the member's epoch, as the control plane recorded them
    msgs,       \* appends in flight
    cpEpoch,
    cpMem,      \* the control plane's own record of the membership
    ordered,    \* targets the control plane ordered, by epoch, recorded before the leader is asked
    placed,     \* brokers the control plane has placed for the group
    everPlaced, \* brokers the control plane has placed in the epoch, removed ones too
    dead,       \* brokers declared dead
    cpFloor,    \* log id of the highest commit the control plane has stored
    plans,      \* dry runs: epoch, and what each copy read held
    forms,      \* epochs written: epoch, base, voters, whether the base was cut
    acked,      \* history: entries committed, by epoch, term and index
    lost,       \* history: documented losses at a re-form (see AckedSurvive)
    elected,    \* history: epoch, term, broker
    writes, steps, places, rotations, rejoins, restarts,
    keptLogs,   \* the copies a broker's rotations kept (Kept)
    synced,     \* how far the current copy's log is on disk (Kept)
    reserve,    \* the index the current copy reserved, synced (Kept)
    cuts, returns

diskVars == <<keptLogs, synced, reserve, cuts, returns>>

vars == <<ep, cid, term, role, log, commit, match, retired, boot, msgs,
          cpEpoch, cpMem, ordered, placed, everPlaced, dead, cpFloor, plans, forms, acked, lost,
          elected, writes, steps, places, rotations, rejoins, restarts, diskVars>>

Mid(b) == <<b, cid[b]>>
Simple(V) == [o |-> V, n |-> V]
Sets(c) == {c.o, c.n}
Voters(c) == c.o \cup c.n
Named(c) == {m[1] : m \in Voters(c)}
InitConfig == Simple({<<b, 0>> : b \in InitBrokers})

\* First voters the membership change does not single out are interchangeable.
Symm == Permutations(InitBrokers \ {ChangeOut, ChangeIn})

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

IsPrefix(s, t) == Len(s) <= Len(t) /\ \A j \in 1..Len(s) : s[j] = t[j]

CommonPrefix(s, t) ==
    LET m == Min(Len(s), Len(t))
        n == CHOOSE k \in 0..m :
                /\ \A j \in 1..k : s[j] = t[j]
                /\ k = m \/ s[k + 1] # t[k + 1]
    IN SubSeq(s, 1, n)

Has(l, x) == x.i <= Len(l) /\ l[x.i] = x.ent

\* A log id: an index, and the epoch and term of the entry there.
LogId(l, i) == [i |-> i, e |-> l[i].e, t |-> l[i].t]
NoFloor == [i |-> 0, e |-> 0, t |-> 0]
\* A gap a later re-form padded up to the floor stands for the floor's entry:
\* everything up to it is that epoch's base, which is committed.
HoldsFloor(l) ==
    \/ cpFloor.i = 0
    \/ /\ cpFloor.i <= Len(l)
       /\ \/ LogId(l, cpFloor.i) = cpFloor
          \/ l[cpFloor.i].k = "gap" /\ l[cpFloor.i].e > cpFloor.e

LastKey(l) == IF Len(l) = 0 THEN <<0, 0>> ELSE <<l[Len(l)].e, l[Len(l)].t>>
KeyLess(k1, k2) == k1[1] < k2[1] \/ (k1[1] = k2[1] /\ k1[2] < k2[2])

\* Raft's election restriction, on the last log id.
UpToDate(a, b) ==
    \/ KeyLess(LastKey(b), LastKey(a))
    \/ LastKey(a) = LastKey(b) /\ Len(a) >= Len(b)

\* The newest membership entry of this epoch in the log, committed or not.
\* Entries of an earlier epoch are plain records.
CfgIdx(l, e) == {j \in 1..Len(l) : l[j].k = "cfg" /\ l[j].e = e}

ConfigIn(l, e, start) ==
    LET idx == CfgIdx(l, e)
    IN IF idx = {} THEN start
       ELSE l[CHOOSE j \in idx : \A h \in idx : h <= j].v

Config(b) == ConfigIn(log[b], ep[b], boot[b])

LastCfgIndex(b) ==
    LET idx == CfgIdx(log[b], ep[b])
    IN IF idx = {} THEN 0 ELSE CHOOSE j \in idx : \A h \in idx : h <= j

\* Brokers some membership of the member's epoch has named.
EverNamed(b) ==
    Named(boot[b]) \cup UNION {Named(log[b][j].v) : j \in CfgIdx(log[b], ep[b])}

\* Who a leader sends appends to: the brokers its newest membership names,
\* and learners. A removed voter gets nothing more.
SendsTo(l, f) ==
    /\ f \in placed
    /\ f \in Named(Config(l)) \/ f \notin EverNamed(l)

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

\* A broker declared dead takes no step, unless the declaration is wrong.
Alive(b) == DeadRuns \/ b \notin dead

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
    /\ ordered = {}
    /\ placed = InitBrokers
    /\ everPlaced = InitBrokers
    /\ dead = {}
    /\ cpFloor = NoFloor
    /\ plans = {}
    /\ forms = {}
    /\ acked = {}
    /\ lost = {}
    /\ elected = {}
    /\ writes = 0 /\ steps = 0 /\ places = 0
    /\ rotations = 0 /\ rejoins = 0 /\ restarts = 0
    /\ keptLogs = [b \in Brokers |-> {}]
    /\ synced = [b \in Brokers |-> 0]
    /\ reserve = [b \in Brokers |-> 0]
    /\ cuts = 0 /\ returns = 0

cpVars == <<cpEpoch, cpMem, ordered, placed, everPlaced, dead, cpFloor, plans, forms, lost>>
counters == <<writes, steps, places, rotations, rejoins, restarts>>

--------------------------------------------------------------------------------
\* Raft inside one epoch.

Elect(b, Q) ==
    /\ b \in Q
    /\ term[b] < MaxTerm
    /\ Mid(b) \in Voters(Config(b))
    /\ Quorum(Config(b), Q)
    /\ \A q \in Q :
        /\ Alive(q)
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
    /\ UNCHANGED diskVars

Leads(l) == role[l] = "leader" /\ ~retired[l] /\ Alive(l)

ClientWrite(l) ==
    /\ Leads(l)
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ log' = [log EXCEPT ![l] =
                Append(@, [e |-> ep[l], t |-> term[l], k |-> "rec", v |-> writes + 1])]
    /\ UNCHANGED <<ep, cid, term, role, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, steps, places, rotations, rejoins, restarts>>
    /\ UNCHANGED diskVars

\* A move is one joint change: old set, then old and new, then new. The
\* first move takes ChangeOut out for ChangeIn, the second moves back. The
\* next step waits for the previous membership entry to commit. The control
\* plane has recorded the target before the leader proposes. The target has
\* started the leader's epoch: a bound of the model, since changes are only
\* configured in epoch 0.
MoveOut(c) == IF ChangeOut \in Named(Simple(c.o)) THEN ChangeOut ELSE ChangeIn
MoveIn(c) == IF ChangeOut \in Named(Simple(c.o)) THEN ChangeIn ELSE ChangeOut

ProposeChange(l) ==
    /\ Leads(l)
    /\ steps < MaxChangeSteps
    /\ LastCfgIndex(l) <= commit[l]
    /\ LET c == Config(l)
           out == MoveOut(c)
           in == MoveIn(c)
           next == IF c.o = c.n
                   THEN [o |-> c.o, n |-> {m \in c.o : m[1] # out} \cup {Mid(in)}]
                   ELSE Simple(c.n)
       IN /\ l # out
          /\ c.o = c.n =>
                (in \in placed /\ ep[in] = ep[l] /\ Mid(in) \notin c.o)
          /\ ordered' = ordered \cup {[e |-> ep[l], v |-> next]}
          /\ log' = [log EXCEPT ![l] =
                        Append(@, [e |-> ep[l], t |-> term[l], k |-> "cfg", v |-> next])]
    /\ steps' = steps + 1
    /\ UNCHANGED <<ep, cid, term, role, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpEpoch, cpMem, placed, everPlaced, dead, cpFloor, plans, forms, lost, writes, places, rotations, rejoins, restarts>>
    /\ UNCHANGED diskVars

\* A write to the current copy. Entries it replaces are off the disk too, and
\* with Reserve = "every" the copy reserves what it wrote before its answer
\* counts. Unsynced: the match a leader records counts what a copy wrote.
Written(f, new) ==
    /\ synced' = IF Kept THEN [synced EXCEPT ![f] = Min(@, Len(CommonPrefix(log[f], new)))]
                 ELSE synced
    /\ reserve' = IF Kept /\ Reserve = "every" THEN [reserve EXCEPT ![f] = Max(@, Len(new))]
                  ELSE reserve

\* What a follower's log is after an append carrying the sender's log. It
\* never cuts entries that agree with the sender.
Adopt(mine, theirs) == IF IsPrefix(theirs, mine) THEN mine ELSE theirs

\* The log refuses a cut below its saved commit index (BelowCommit), so a
\* copy takes no append that would make that index name another entry.
KeepsCommit(b, new) ==
    new = log[b] \/ Len(CommonPrefix(log[b], new)) >= commit[b]

Replicate(l, f) ==
    /\ l # f
    /\ Leads(l)
    /\ SendsTo(l, f)
    /\ Alive(f)
    /\ ~retired[f]
    /\ ep[f] = ep[l]
    /\ term[f] <= term[l]
    /\ LET new == Adopt(log[f], log[l])
       IN /\ new # log[f] \/ term[f] # term[l] \/ role[f] # "follower"
             \/ match[l][f] # Len(log[l]) \/ commit[f] < commit[l]
          /\ KeepsCommit(f, new)
          /\ log' = [log EXCEPT ![f] = new]
          \* The new log agrees with the leader's up to the leader's commit.
          /\ commit' = [commit EXCEPT ![f] = Max(commit[f], commit[l])]
          /\ match' = [match EXCEPT ![l][f] =
                        IF Mid(f) \in Voters(Config(l)) THEN Len(log[l]) ELSE @]
          /\ Written(f, new)
    /\ term' = [term EXCEPT ![f] = term[l]]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ UNCHANGED <<ep, cid, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters, keptLogs, cuts, returns>>

Send(l) ==
    /\ Leads(l)
    /\ Cardinality(msgs) < MaxMsgs
    /\ log[l] # <<>>
    /\ msgs' = msgs \cup {[e |-> ep[l], t |-> term[l], log |-> log[l],
                           to |-> {f \in Brokers \ {l} : SendsTo(l, f)}]}
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>
    /\ UNCHANGED diskVars

\* A late append. The sender may be gone, so nothing is acknowledged. The
\* message stays and can arrive again.
Receive(f, m) ==
    /\ f \in m.to
    /\ Alive(f)
    /\ ~retired[f]
    /\ EpochNotChecked \/ m.e = ep[f]
    /\ m.t > term[f] \/ (m.t = term[f] /\ role[f] # "leader")
    /\ Adopt(log[f], m.log) # log[f]
    /\ KeepsCommit(f, Adopt(log[f], m.log))
    /\ log' = [log EXCEPT ![f] = Adopt(@, m.log)]
    /\ Written(f, Adopt(log[f], m.log))
    /\ term' = [term EXCEPT ![f] = m.t]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ UNCHANGED <<ep, cid, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters, keptLogs, cuts, returns>>

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
    /\ cpFloor' = IF Floor = "exact" /\ i > cpFloor.i THEN LogId(log[l], i) ELSE cpFloor
    /\ reserve' = IF Kept /\ Reserve # "none" THEN [reserve EXCEPT ![l] = Max(@, i)] ELSE reserve
    /\ UNCHANGED <<ep, cid, term, role, log, match, retired, boot, msgs, elected>>
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, placed, everPlaced, dead, plans, forms, lost, counters>>
    /\ UNCHANGED <<keptLogs, synced, cuts, returns>>

\* A leader that no longer hears a quorum steps down.
StepDown(l) ==
    /\ Leads(l)
    /\ role' = [role EXCEPT ![l] = "follower"]
    /\ UNCHANGED <<ep, cid, term, log, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>
    /\ UNCHANGED diskVars

--------------------------------------------------------------------------------
\* Faults.

\* A restart keeps the log, the term and the stop marker.
Restart(b) ==
    /\ Alive(b)
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ retired' = [retired EXCEPT ![b] = IF RetireNotDurable THEN FALSE ELSE @]
    /\ UNCHANGED <<ep, cid, term, log, commit, match, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, writes, steps, places, rotations, rejoins>>
    /\ UNCHANGED diskVars

\* boot stays: it is not on the disk. The command reads it from the control
\* plane's record of the epoch (BootIsRecorded). The saved commit index is
\* never past the log kept with it. This is the highest it can be; a lower
\* one only cuts a base shorter.
NewCopy(b, kept) ==
    /\ cid' = [cid EXCEPT ![b] = @ + 1]
    /\ log' = [log EXCEPT ![b] = kept]
    /\ term' = [term EXCEPT ![b] = 0]
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ commit' = [commit EXCEPT ![b] = Min(@, Len(kept))]
    /\ retired' = [retired EXCEPT ![b] = FALSE]
    /\ UNCHANGED <<ep, match, boot, msgs, acked, elected>>

\* With Kept a rotation renames the whole copy into a kept copy, with the
\* vote it held, why it rotated and its reservation, and the broker starts
\* an empty copy under a new id. A restore can bring back any prefix and an
\* older vote; only a power cut keeps the newest vote.
KeptCopy(b, l, t, by) ==
    [e |-> ep[b], cid |-> cid[b], log |-> l, commit |-> Min(commit[b], Len(l)),
     vote |-> <<ep[b], t>>, by |-> by, res |-> reserve[b]]

KeepCopy(b, l, t, by) ==
    /\ keptLogs' = [keptLogs EXCEPT ![b] = @ \cup {KeptCopy(b, l, t, by)}]
    /\ NewCopy(b, <<>>)
    /\ synced' = [synced EXCEPT ![b] = 0]
    /\ reserve' = [reserve EXCEPT ![b] = 0]

\* A replaced disk keeps nothing, not even the old copy's vote (#2671). A
\* restore whose kept copy is later reclaimed ends in the same state: a
\* new empty copy and no kept copy of the old id.
ReplaceDisk(b) ==
    /\ NewCopy(b, <<>>)
    /\ synced' = [synced EXCEPT ![b] = 0]
    /\ reserve' = [reserve EXCEPT ![b] = 0]
    /\ UNCHANGED keptLogs

\* The shard directory is lost, or restored from an older copy: the copy id
\* rotates and any prefix of the log is left. The stop marker goes with it.
Rotate(b, n) ==
    /\ Alive(b)
    /\ rotations < MaxRotations
    /\ rotations' = rotations + 1
    /\ n \in 0..Len(log[b])
    /\ IF Kept
       THEN \/ \E t \in 0..term[b] : KeepCopy(b, SubSeq(log[b], 1, n), t, "restore")
            \/ n = 0 /\ ReplaceDisk(b)
       ELSE NewCopy(b, SubSeq(log[b], 1, n)) /\ UNCHANGED <<keptLogs, synced, reserve>>
    /\ UNCHANGED <<cpVars, writes, steps, places, rejoins, restarts, cuts, returns>>

\* The current copy's log reaches the disk.
Sync(b) ==
    /\ Kept
    /\ Alive(b)
    /\ synced[b] < Len(log[b])
    /\ synced' = [synced EXCEPT ![b] = Len(log[b])]
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters, keptLogs, reserve, cuts, returns>>

\* A power cut on a relaxed broker: the copy loses some of its unsynced
\* tail and rotates (the log is below its mark). It keeps its term and vote.
PowerCutRotate(b, n) ==
    /\ Kept
    /\ Alive(b)
    /\ cuts < MaxCuts
    /\ cuts' = cuts + 1
    /\ n \in synced[b]..Len(log[b])
    /\ KeepCopy(b, SubSeq(log[b], 1, n), term[b], "power-cut")
    /\ UNCHANGED <<cpVars, writes, steps, places, rotations, rejoins, restarts, returns>>

\* A broker declared dead comes back under a new storage id: its current copy
\* rotates as a restore, its kept copies are intact, and it is live again.
\* Its node is no longer in the dead set.
Return(b) ==
    /\ Kept
    /\ b \in dead
    /\ returns < MaxReturns
    /\ returns' = returns + 1
    /\ dead' = dead \ {b}
    /\ KeepCopy(b, log[b], term[b], "restore")
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, placed, everPlaced, cpFloor, plans, forms, lost>>
    /\ UNCHANGED <<writes, steps, places, rotations, rejoins, restarts, cuts>>

\* A copy stopped by an apply that did not finish, in a group that then
\* elected without it, stays stopped in that epoch. It comes back only
\* under a new copy id, as a learner.
RetiredRejoins(b) ==
    /\ Alive(b)
    /\ retired[b]
    /\ rejoins < MaxRejoins
    /\ \E l \in Brokers : Leads(l) /\ ep[l] = ep[b]
    /\ rejoins' = rejoins + 1
    /\ NewCopy(b, log[b])
    /\ UNCHANGED <<cpVars, writes, steps, places, rotations, restarts>>
    /\ UNCHANGED diskVars

--------------------------------------------------------------------------------
\* The control plane's records.

brokerVars == <<ep, cid, term, role, log, commit, match, retired, boot, msgs, acked, elected>>

\* A learner, or the target of a move, is recorded before the leader sends
\* it anything.
Place(b) ==
    /\ b \notin placed
    /\ places < MaxPlace
    /\ places' = places + 1
    /\ placed' = placed \cup {b}
    /\ everPlaced' = everPlaced \cup {b}
    /\ UNCHANGED brokerVars
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, dead, cpFloor, plans, forms, lost>>
    /\ UNCHANGED <<writes, steps, rotations, rejoins, restarts>>
    /\ UNCHANGED diskVars

\* A broker stays placed until the leader reports the membership that
\* removes it as committed. The record of the epoch keeps it.
Unplace(l) ==
    /\ Leads(l)
    /\ ep[l] = cpEpoch
    /\ LastCfgIndex(l) > 0
    /\ LastCfgIndex(l) <= commit[l]
    /\ Config(l).o = Config(l).n
    /\ LET gone == (placed \cap EverNamed(l)) \ Named(Config(l))
       IN /\ gone # {}
          /\ placed' = placed \ gone
    /\ UNCHANGED brokerVars
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, everPlaced, dead, cpFloor, plans, forms, lost, counters>>
    /\ UNCHANGED diskVars

\* The control plane's own record of the voters follows broker reports and
\* can be stale. Only the list that leaves placed brokers out reads it.
CpObserve(b) ==
    /\ ~WaitPlaced
    /\ ep[b] = cpEpoch
    /\ cpMem # Config(b)
    /\ cpMem' = Config(b)
    /\ UNCHANGED brokerVars
    /\ UNCHANGED <<cpEpoch, placed, everPlaced, dead, cpFloor, plans, forms, lost, counters>>
    /\ UNCHANGED diskVars

\* The leader's report carries the log id of its commit.
ReportFloor(l) ==
    /\ Floor = "trailing"
    /\ Leads(l)
    /\ ep[l] = cpEpoch
    /\ commit[l] > cpFloor.i
    /\ cpFloor' = LogId(log[l], commit[l])
    /\ UNCHANGED brokerVars
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, placed, everPlaced, dead, plans, forms, lost, counters>>
    /\ UNCHANGED diskVars

\* An operator's separate, recorded act. It can be wrong.
DeclareDead(b) ==
    /\ b \notin dead
    /\ Cardinality(dead) < MaxDead
    /\ dead' = dead \cup {b}
    /\ role' = [role EXCEPT ![b] = IF DeadRuns THEN @ ELSE "follower"]
    /\ UNCHANGED <<ep, cid, term, log, commit, match, retired, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, placed, everPlaced, cpFloor, plans, forms, lost, counters>>
    /\ UNCHANGED diskVars

--------------------------------------------------------------------------------
\* The command.

\* How much of its log a copy can vouch for as committed when no copy read
\* holds the floor's entry. A log that ends at or below the floor's index in
\* an entry of the floor's epoch and term is a prefix of the log that entry
\* committed in (log matching). Any other log is known only up to the commit
\* index saved with it.
Vouches(b) ==
    IF LastKey(log[b]) = <<cpFloor.e, cpFloor.t>> /\ Len(log[b]) <= cpFloor.i
    THEN Len(log[b]) ELSE commit[b]

\* The brokers placed in the epoch. Leaving out the removed ones loses what a
\* removed voter's disk holds when the copies left are empty (PlacedForgotten).
Placed == IF PlacedForgotten THEN placed ELSE everPlaced

List == IF WaitPlaced THEN Placed ELSE Named(cpMem)

\* What the command reads from one copy.
Fact(b) ==
    [cid |-> cid[b], key |-> LastKey(log[b]), len |-> Len(log[b]), mem |-> Config(b),
     kept |-> keptLogs[b]]

Higher(f, g) == KeyLess(g.key, f.key) \/ (f.key = g.key /\ f.len >= g.len)

\* The member id some membership has for the broker is an earlier copy's.
Rotated(b) ==
    \E m \in Voters(Config(b)) \cup Voters(cpMem) : m[1] = b /\ m[2] < cid[b]

\* The voters a copy names. One with no membership of the epoch names the
\* epoch's first voters. With every placed broker listed this never
\* refuses (PlacedIsRecorded); it guards against a wrong record.
Names(b) == Named(Config(b))

\* The copies the command has to read.
Asked ==
    LET live == List \ dead
    IN IF RotatedNotWaited THEN {b \in live : ~Rotated(b)} ELSE live

Plan(e, S) ==
    /\ e < MaxEpoch
    /\ IF NoCas THEN e <= cpEpoch ELSE e = cpEpoch
    /\ Cardinality(plans) < MaxPlans
    /\ S # {}
    /\ IF ReformBeforeAll THEN S \subseteq Asked
       ELSE IF Kept THEN Asked \subseteq S /\ S \cap dead = {}
       ELSE S = Asked
    /\ \A b \in S : (ep[b] = e \/ (Kept /\ b \notin Asked /\ ep[b] < e)) /\ ~Leads(b)
    /\ \A b \in S : ep[b] = e => Names(b) \subseteq List \cup dead
    /\ [e |-> e, facts |-> [b \in S |-> Fact(b)]] \notin plans
    /\ plans' = plans \cup {[e |-> e, facts |-> [b \in S |-> Fact(b)]]}
    /\ UNCHANGED brokerVars
    /\ UNCHANGED <<cpEpoch, cpMem, ordered, placed, everPlaced, dead, cpFloor, forms, lost, counters>>
    /\ UNCHANGED diskVars

\* The stop is durable before the copy is read again. Apply asks the listed
\* copies for a leader again first, and leaves a group that has one alone.
Stop(b) ==
    /\ ~ReportNotPromise
    /\ Alive(b)
    /\ ~retired[b]
    /\ \E p \in plans : p.e = ep[b] /\ b \in DOMAIN p.facts
    /\ StopNotRechecked \/ \A x \in List \ dead : ~(Leads(x) /\ ep[x] = ep[b])
    /\ retired' = [retired EXCEPT ![b] = TRUE]
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ UNCHANGED <<ep, cid, term, log, commit, match, boot, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters>>
    /\ UNCHANGED diskVars

Gap(e) == [e |-> e, t |-> 0, k |-> "gap", v |-> 0]

PadTo(base, n, e) ==
    IF n <= Len(base) THEN base ELSE base \o [j \in 1..(n - Len(base)) |-> Gap(e)]

\* Offsets up to the floor are not given out again.
Pad(base, e) == IF Floor = "none" THEN base ELSE PadTo(base, cpFloor.i, e)

\* why and held are for the checks only: which rules of the vouch decided
\* something, and the acknowledged records KeptLossBound says must survive.
WriteForm(e, base, padded, voters, cut, marked, why, held) ==
    LET f == [e |-> e + 1, base |-> padded, end |-> Len(base), voters |-> voters,
              cut |-> cut, marked |-> marked, why |-> why, held |-> held]
    IN /\ f \notin forms
       /\ forms' = forms \cup {f}
       /\ cpEpoch' = e + 1
       /\ cpMem' = Simple(voters)
       \* Everything at or below the base is served as committed from here.
       /\ acked' = acked \cup
            {[e |-> e + 1, t |-> 0, i |-> j, ent |-> base[j]] : j \in 1..Len(base)}

\* The compare-and-swap is the caller's guard on e. A record is a documented
\* loss if no disk holds it, or the base was cut and no disk that holds it
\* can vouch for it.
WriteEpoch(e, base, voters, cut) ==
    /\ WriteForm(e, base, Pad(base, e + 1), voters, cut, FALSE, {}, {})
    /\ lost' = lost \cup
            {x \in acked : /\ x.e <= e
                           /\ \A b \in Brokers \ dead :
                                \/ ~Has(log[b], x)
                                \/ cut /\ x.i > Vouches(b)}

\* No copy read holds the floor's entry. Then even the highest log can hold
\* an uncommitted entry at an offset acknowledged with another record, and
\* only what a copy can vouch for is kept.
Cut(S) ==
    /\ ~FloorIndexOnly
    /\ Floor # "none"
    /\ \A b \in S : ~HoldsFloor(log[b])

Reform(p, pick) ==
    LET e == p.e
        S == DOMAIN p.facts
        F == IF PlanNotRechecked THEN p.facts ELSE [b \in S |-> Fact(b)]
        cut == Cut(S)
        Keep(b) == IF ~cut THEN F[b].len
                   ELSE Min(IF CutToCommit THEN commit[b] ELSE Vouches(b), F[b].len)
        base == SubSeq(log[pick], 1, Keep(pick))
        voters == {<<b, F[b].cid>> : b \in {x \in S : IsPrefix(base, log[x])}}
    IN /\ ~Kept
       /\ e < MaxEpoch
       /\ IF NoCas THEN e <= cpEpoch ELSE e = cpEpoch
       /\ pick \in S
       /\ PlanNotRechecked \/
            /\ ReformBeforeAll \/ S = Asked
            /\ \A b \in S : ep[b] = e /\ Fact(b) = p.facts[b]
       /\ ReportNotPromise \/ \A b \in S : retired[b] /\ ep[b] = e
       /\ Len(log[pick]) >= F[pick].len
       /\ BaseNotHighest \/
            IF cut THEN \A b \in S : Keep(b) <= Keep(pick)
            ELSE \A b \in S : Higher(F[pick], F[b])
       /\ WriteEpoch(e, base, voters, cut)
       \* Every listed copy is a voter or a learner of the new epoch.
       /\ placed' = placed \cup S
       /\ everPlaced' = placed \cup S
       /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
       /\ UNCHANGED <<ordered, dead, cpFloor, plans, elected, counters>>
       /\ UNCHANGED diskVars

\* The re-form with Kept: it reads every copy of every broker read, current
\* and kept, and takes the longest prefix one of them vouches for.
Copies(b) ==
    {[e |-> ep[b], cid |-> cid[b], log |-> log[b], commit |-> commit[b],
      vote |-> <<ep[b], term[b]>>, by |-> "current", res |-> reserve[b]]} \cup keptLogs[b]

MaxKey(K) == CHOOSE k \in K : \A j \in K : ~KeyLess(k, j)
MinOf(X) == CHOOSE x \in X : \A y \in X : x <= y
MaxOf(X) == CHOOSE x \in X : \A y \in X : y <= x

\* T: the highest key of any vote read, of any entry read, and of the floor.
\* Only a voter of epoch E raises a term of E, so T is sound when every
\* broker placed in E has its epoch-E vote read.
Top(R) == MaxKey({c.vote : c \in R} \cup {LastKey(c.log) : c \in R} \cup {<<cpFloor.e, cpFloor.t>>})

\* An epoch-e copy agrees with today's log only up to the lowest base end of
\* the epochs after e: above it each later epoch put its gap and entries.
Cap(e, E) ==
    LET ends == {f.end : f \in {g \in forms : e < g.e /\ g.e <= E}}
    IN IF ends = {} THEN MaxWrites + MaxEpoch * 8 ELSE MinOf(ends)

\* The voter sets of epoch E a re-form can know: the seed voters the control
\* plane recorded for E, each target it ordered in E, and both halves of
\* every membership entry of E in a copy read, committed or not. Never the
\* placed brokers: a placed broker need not be a voter.
VoterSets(E, R) ==
    (IF E = 0 THEN {InitConfig.o} ELSE {f.voters : f \in {g \in forms : g.e = E}})
        \cup UNION {Sets(o.v) : o \in {x \in ordered : x.e = E}}
        \cup UNION {UNION {Sets(c.log[j].v) : j \in CfgIdx(c.log, E)} : c \in R}

\* A member's epoch-E vote is read when the re-form read the copy it gave it
\* from, current or kept by a power cut, and its broker is not dead and has
\* no kept copy of E rotated by a restore, which can hold an older vote than
\* one it gave. Keyed by copy id (#2671): a later copy of the same broker
\* says nothing of an earlier one's vote.
VoteRead(m, E, R) ==
    /\ m[1] \notin dead
    /\ ~\E k \in keptLogs[m[1]] : k.e = E /\ k.by = "restore"
    /\ \E c \in R \cap Copies(m[1]) : c.e = E /\ c.cid = m[2] /\ c.by \in {"current", "power-cut"}

\* The epoch-E mark: the votes not read are a majority of some voter set of
\* E, so an election of E may have taken none the re-form read.
Marked(E, R) ==
    \E V \in VoterSets(E, R) : 2 * Cardinality({m \in V : ~VoteRead(m, E, R)}) > Cardinality(V)

\* A copy vouches for its saved commit, for the floor's index if it holds the
\* floor's entry, and for its whole log if the group is unmarked and its last
\* entry has key T; never past its epoch's cap.
VouchSafe(c) == Max(c.commit, IF cpFloor.i > 0 /\ HoldsFloor(c.log) THEN cpFloor.i ELSE 0)
VouchUncapped(c, T, mk) == Max(VouchSafe(c), IF ~mk /\ LastKey(c.log) = T THEN Len(c.log) ELSE 0)
VouchK(c, E, T, mk) == Min(Cap(c.e, E), VouchUncapped(c, T, mk))

\* What the vouch rule decided at a re-form: "cap" when an epoch's cap
\* lowered a copy, "T" when T kept one copy whole and left another, longer
\* than its safe part, cut to it.
Decided(R, E, T, mk) ==
    {w \in {"cap", "T"} :
        IF w = "cap" THEN \E c \in R : VouchUncapped(c, T, mk) > Cap(c.e, E)
        ELSE /\ ~mk
             /\ \E c \in R : LastKey(c.log) = T /\ Len(c.log) > VouchSafe(c)
             /\ \E c \in R : LastKey(c.log) # T /\ Len(c.log) > VouchSafe(c)}

ReformKept(p, src) ==
    LET e == p.e
        S == DOMAIN p.facts
        R == UNION {Copies(b) : b \in S}
        T == Top(R)
        mk == Marked(e, R)
        V(c) == VouchK(c, e, T, mk)
        base == SubSeq(src.log, 1, V(src))
        voters == {<<b, cid[b]>> : b \in {x \in S : \E c \in Copies(x) : IsPrefix(base, c.log)}}
        top == Max(Len(base), Max(IF Floor = "none" THEN 0 ELSE cpFloor.i,
                                   MaxOf({c.res : c \in R})))
        \* Unmarked, and no vote read is above every entry read.
        tEntry == ~mk /\ T = MaxKey({LastKey(c.log) : c \in R})
        held == IF tEntry
                THEN {x \in acked \ lost : x.e <= e /\ \E c \in R : Has(c.log, x) /\ x.i <= Cap(c.e, e)}
                ELSE {}
    IN /\ Kept
       /\ e < MaxEpoch
       /\ IF NoCas THEN e <= cpEpoch ELSE e = cpEpoch
       /\ src \in R
       /\ ReformBeforeAll \/ Asked \subseteq S
       /\ \A b \in S : ep[b] <= e /\ Fact(b) = p.facts[b]
       /\ ReportNotPromise \/ \A b \in S : ep[b] = e => retired[b]
       /\ \A c \in R : V(c) <= V(src)
       /\ voters # {}
       /\ WriteForm(e, base, PadTo(base, top, e + 1), voters, TRUE, mk, Decided(R, e, T, mk), held)
       /\ lost' = lost \cup
            {x \in acked : x.e <= e /\ \A c \in R : ~Has(c.log, x) \/ x.i > V(c)}
       \* The next epoch's record leaves out the dead.
       /\ placed' = (placed \ dead) \cup S
       /\ everPlaced' = (placed \ dead) \cup S
       /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
       /\ UNCHANGED <<ordered, dead, cpFloor, plans, elected, counters, diskVars>>

\* A shard with one replica whose copy rotated and kept records: it is a
\* re-form of one, without the command. No other broker was placed in the
\* epoch, so no other disk can hold a newer record, and the copy's log is
\* a prefix of the one log the shard had. It is never cut.
SelfStart(b) ==
    /\ ~Kept
    /\ SoloPlacedTwo \/ Placed = {b}
    /\ b \notin dead
    /\ ep[b] = cpEpoch
    /\ cpEpoch < MaxEpoch
    /\ \E m \in Voters(Config(b)) :
        Voters(Config(b)) = {m} /\ m[1] = b /\ m[2] < cid[b]
    /\ Len(log[b]) > 0
    /\ WriteEpoch(cpEpoch, log[b], {Mid(b)}, FALSE)
    /\ everPlaced' = placed
    /\ UNCHANGED <<ep, cid, term, role, log, commit, match, retired, boot, msgs>>
    /\ UNCHANGED <<placed, dead, cpFloor, plans, elected, counters>>
    /\ UNCHANGED diskVars

\* A voter held the base, so its log is the base and the gap. Anyone else
\* keeps what agrees with the base and is a learner. A broker declared dead
\* is refused.
StartEpoch(b, f) ==
    /\ f \in forms
    /\ f.e = cpEpoch
    /\ f.e > ep[b]
    /\ b \notin dead
    /\ LET new == IF Mid(b) \in f.voters THEN f.base ELSE CommonPrefix(log[b], f.base)
       IN /\ log' = [log EXCEPT ![b] = new]
          /\ commit' = [commit EXCEPT ![b] = Len(new)]
    /\ ep' = [ep EXCEPT ![b] = f.e]
    /\ term' = [term EXCEPT ![b] = 0]
    /\ role' = [role EXCEPT ![b] = "follower"]
    /\ retired' = [retired EXCEPT ![b] = FALSE]
    /\ boot' = [boot EXCEPT ![b] = Simple(f.voters)]
    \* The start syncs the copy and reserves up to the epoch's first index.
    /\ synced' = IF Kept THEN [synced EXCEPT ![b] = Len(log'[b])] ELSE synced
    /\ reserve' = IF Kept THEN [reserve EXCEPT ![b] = Max(@, Len(f.base))] ELSE reserve
    /\ UNCHANGED <<cid, match, msgs, acked, elected>>
    /\ UNCHANGED <<cpVars, counters, keptLogs, cuts, returns>>

Next ==
    \/ \E b \in Brokers : \E Q \in SUBSET Brokers : Elect(b, Q)
    \/ \E b \in Brokers : ClientWrite(b) \/ ProposeChange(b) \/ Send(b)
    \/ \E l, f \in Brokers : Replicate(l, f)
    \/ \E f \in Brokers : \E m \in msgs : Receive(f, m)
    \/ \E l \in Brokers : \E i \in 1..Len(log[l]) : Commit(l, i)
    \/ \E b \in Brokers : StepDown(b) \/ Restart(b) \/ RetiredRejoins(b)
    \/ \E b \in Brokers : \E n \in 0..Len(log[b]) : Rotate(b, n) \/ PowerCutRotate(b, n)
    \/ \E b \in Brokers : Sync(b) \/ Return(b)
    \/ \E b \in Brokers : Place(b) \/ Unplace(b) \/ CpObserve(b) \/ ReportFloor(b)
    \/ \E b \in Brokers : DeclareDead(b) \/ Stop(b) \/ SelfStart(b)
    \/ \E e \in 0..cpEpoch : \E S \in SUBSET Brokers : Plan(e, S)
    \/ \E p \in plans : \E pick \in Brokers : Reform(p, pick)
    \/ \E p \in plans : \E b \in DOMAIN p.facts : \E c \in Copies(b) : ReformKept(p, c)
    \/ \E b \in Brokers : \E f \in forms : StartEpoch(b, f)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------------
\* Checks.

TypeOK ==
    /\ ep \in [Brokers -> 0..MaxEpoch]
    /\ cid \in [Brokers -> 0..(MaxRotations + MaxRejoins + MaxCuts + MaxReturns)]
    /\ \A b \in Brokers : synced[b] <= Len(log[b])
    /\ term \in [Brokers -> 0..MaxTerm]
    /\ role \in [Brokers -> {"follower", "leader"}]
    /\ retired \in [Brokers -> BOOLEAN]
    /\ cpEpoch \in 0..MaxEpoch
    /\ placed \subseteq Brokers
    /\ everPlaced \subseteq Brokers
    /\ dead \subseteq Brokers
    /\ cpFloor \in [i : Nat, e : 0..MaxEpoch, t : 0..MaxTerm]
    /\ \A p \in plans : p.e \in 0..MaxEpoch /\ DOMAIN p.facts \subseteq Brokers
    /\ \A b \in Brokers : commit[b] <= Len(log[b])
    /\ lost \subseteq acked

OneLeaderPerTerm ==
    \A x, y \in elected : x.e = y.e /\ x.t = y.t => x.b = y.b

\* An acknowledged record outlives its epoch. The only losses allowed are
\* the ones the design documents: no live disk held it any more, or the
\* base was cut and every live disk that held it had it above what that
\* disk vouches for.
AckedSurvive ==
    \A x \in acked \ lost :
        /\ \A f \in forms : f.e > x.e => Has(f.base, x)
        /\ \A b \in Brokers :
            (role[b] = "leader" /\ KeyLess(<<x.e, x.t>>, <<ep[b], term[b]>>))
                => Has(log[b], x)

IsRecord(x) == x.ent.k # "gap"

StateMachineSafety ==
    \A x, y \in acked :
        (x.i = y.i /\ x.ent # y.ent /\ IsRecord(x) /\ IsRecord(y))
            => (x \in lost \/ y \in lost)

OffsetNeverReused ==
    \A x, y \in acked : (x.i = y.i /\ IsRecord(x) /\ IsRecord(y)) => x.ent = y.ent

NothingLost == lost = {}

ElectedGroupLeftAlone ==
    [][\A b \in Brokers : (~retired[b] /\ retired'[b]) =>
            \A l \in Brokers : ~(Leads(l) /\ ep[l] = ep[b])]_vars

\* With Kept, AckedSurvive allows any record the vouch rule left out. This
\* bounds it: in an unmarked group where no vote read is above every entry
\* read, no acknowledged record a read copy holds below its epoch's cap is
\* lost.
KeptLossBound ==
    [][\A f \in forms' \ forms : f.held \cap lost' = {}]_vars

\* No record is lost while a disk that is not declared dead holds it.
LossNeedsLostDisk ==
    [][\A x \in lost' \ lost : \A b \in Brokers \ dead : ~Has(log[b], x)]_vars

BootIsRecorded ==
    \A b \in Brokers :
        \/ ep[b] = 0 /\ boot[b] = InitConfig
        \/ \E f \in forms : f.e = ep[b] /\ boot[b] = Simple(f.voters)

PlacedIsRecorded ==
    /\ placed \subseteq everPlaced
    /\ \A b \in Brokers : ep[b] = cpEpoch => EverNamed(b) \subseteq everPlaced

\* Witnesses: each is an invariant that must fail.

\* A re-form completes and the new epoch commits a record of its own.
NeverCommitsAfterReform == \A x \in acked : x.e = 0 \/ x.t = 0

\* A copy stopped by an apply rotates and takes an append as a learner.
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

\* A restored single copy starts by itself and commits.
NeverSelfStarts ==
    ~(/\ plans = {}
      /\ \E x \in acked : x.e > 0 /\ x.t > 0)

\* A group re-forms without a broker declared dead and commits.
NeverReformsWithoutDead ==
    ~(/\ dead # {}
      /\ \E x \in acked : x.e > 0 /\ x.t > 0)

\* No copy read holds the floor's entry, and the base is cut.
NeverCuts == \A f \in forms : ~f.cut

\* A base ends below the floor and is padded.
NeverPads == \A f \in forms : \A j \in 1..Len(f.base) : f.base[j].k # "gap"

\* The second re-form pads past its base and the floor by a reservation
\* alone: a copy that counted a record and lost it to a power cut still
\* keeps its offset from being given out again.
NeverPadsByReserve ==
    ~/\ cuts = 2
     /\ \E f \in forms : f.e = 2 /\ Len(f.base) > Max(f.end, cpFloor.i)
     /\ \E b \in Brokers : \E k \in keptLogs[b] :
            /\ k.by = "power-cut" /\ k.res > Len(k.log)
            /\ \E x \in acked : x.e = k.e /\ x.t > 0 /\ x.i = k.res

\* The cap of an epoch lowers what a copy of it vouches for.
NeverCaps == \A f \in forms : "cap" \notin f.why

\* T keeps one copy whole and cuts another.
NeverVouchesByT == \A f \in forms : "T" \notin f.why

\* At the second re-form a restored epoch-0 copy and an epoch-0 voter
\* declared dead leave the group unmarked, and an epoch-1 record is in the
\* base: the mark looks only at brokers placed in the epoch being closed.
NeverKeepsAcrossOldRestore ==
    ~\E f \in forms :
        /\ f.e = 2 /\ ~f.marked
        /\ \E b \in Brokers : \E k \in keptLogs[b] : k.e = 0 /\ k.by = "restore"
        /\ dead # {}
        /\ \E j \in 1..Len(f.base) : f.base[j].k = "rec" /\ f.base[j].e = 1

\* A learner that no membership names holds a record.
NeverLearnerHolds ==
    \A b \in Brokers \ InitBrokers : \A j \in 1..Len(log[b]) : log[b][j].k # "rec"

--------------------------------------------------------------------------------
\* The shape of the two re-form configs (ACTION_CONSTRAINT). It cuts only
\* interleavings their scenarios do not need: epoch 0 sees only the command
\* and the faults that leave its empty logs alone, each later epoch takes at
\* most one write, and the plan to close epoch 1 waits until epoch 1
\* committed a record. Records of an older epoch read at a re-form are
\* checked by the one re-form configs.
EpochOneCommitted == \E x \in acked : x.e = 1 /\ x.t > 0

TwoReformsShape ==
    /\ cpEpoch = 0 => UNCHANGED <<term, log, commit, msgs, cuts>>
    /\ writes' # writes => writes < cpEpoch
    /\ (cpEpoch = 1 /\ plans' # plans) => EpochOneCommitted

\* The shape of the OldKept configs: epoch 0 writes and a broker is declared
\* dead in it; epoch 1 elects nobody, and the dead broker returns in it with
\* its epoch-0 copy, which the second re-form reads.
OldKeptShape ==
    /\ writes' # writes => cpEpoch = 0
    /\ dead' \ dead # {} => cpEpoch = 0
    /\ returns' # returns => cpEpoch = 1
    /\ elected' # elected => cpEpoch = 0
================================================================================
