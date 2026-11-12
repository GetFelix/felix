------------------------- MODULE FelixShardUnified -------------------------
(***************************************************************************)
(* One fence rule for every change of leader of a shard: failover,         *)
(* election, rename, skip, readmission, set change and a continuing        *)
(* leader. The rule splits what a fence has to prove into two questions.   *)
(*                                                                         *)
(*   Exclusion. No leader below the fence's generation acknowledges        *)
(*   anything once it opens, and no second leader opens at it. Only a      *)
(*   promise from a copy that kept every ballot counts. A copy keeps its   *)
(*   ballots across any crash (they are fsynced) and loses them only to a  *)
(*   wipe or a restore. A copy that lost them counts again only for a set  *)
(*   introduced after the loss that names it and has shipped to it         *)
(*   (readmission).                                                        *)
(*                                                                         *)
(*   Coverage. The new leader holds every acknowledged record. Either it   *)
(*   is continuing (it opened g - 1 in the same process), or a majority of *)
(*   each set it fences answered from a trusted log: one that started      *)
(*   clean, or that a leader shipped level through max(its start record,   *)
(*   the quorum mark it carries) since its last unclean start.             *)
(*                                                                         *)
(* A fence opens when, for every set it must fence, it holds a majority of *)
(* counted promises and (unless continuing) a majority of trusted answers. *)
(* The sets are its own and every set started from the anchor up to its    *)
(* generation. The anchor is a generation it has evidence opened before:   *)
(* one that served (its start record reached a majority), or one a         *)
(* trusted answer holds a record of. Sets in between are never dropped     *)
(* on silence.                                                             *)
(*                                                                         *)
(* `UnifiedFence = FALSE` is today's rules in outline, enough to show the  *)
(* stalls the 200-seed sweep found: identity pins (the copy the previous   *)
(* generation reached), the doubted copy's refusal, the narrow continuing  *)
(* window. The shortcut switches below each break one part of the rule,    *)
(* and TLC finds the defect each one lets through.                         *)
(*                                                                         *)
(* Abstractions: a node is a seat in the set; its copy changes under it    *)
(* (unclean crash, wipe, restore). A batch ships a prefix of the leader's  *)
(* log. Each generation acknowledges on its own set; a set change is a new *)
(* generation whose fence also fences the sets it leaves.                  *)
(* docs/replication-design.md, "One fence rule for every change of         *)
(* leader", states the rule.                                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,          \* the seats
    First,          \* the leader serving generation 1 at the start
    NoNode,         \* a ballot naming nobody
    MaxGen,         \* highest generation anything may start
    MaxWrites,      \* client writes
    MaxStarts,      \* budgeted starts: elections, failovers, renames, skips, set changes
    MaxUnclean,     \* unclean crashes: a suffix of the log is lost, ballots kept
    MaxWipes,       \* lost directories: log and ballots gone, detected
    MaxRestores,    \* restored directories: an older ballot and log prefix
    MaxRetries,     \* fence rounds that start over
    SetChoices,     \* sets a set change may write
    OldPeers        \* nodes that predate the kept-ballot attestation

\* Knobs, overridden in a configuration with `<-`.
Yes == TRUE
No == FALSE
Three == 3
UnifiedFence == TRUE
\* Liveness runs: the control plane renames above every ballot while no
\* leader serves, and faults and budgeted starts stop at `FaultUntil`.
LiveCp == FALSE
FaultUntil == MaxGen
\* `ReadmitFirst`: the one budgeted start is a readmission, a set change by the
\* serving leader after another member crashed uncleanly. The leader
\* crashes only while that fence is pending, and the control plane fails
\* over only once it has, so a stall TLC finds goes through it.
ReadmitFirst == FALSE
\* `MoveFirst`: the two budgeted starts are a move by the serving `First` (a
\* set change that stages a learner) and then the learner's cut-over, a set
\* change that leaves the move's set. In between, once the move opened, a
\* voter other than `First` crashes uncleanly. The control plane fails over
\* only after both starts.
MoveFirst == FALSE
\* `RotatedRefuses`: a copy whose id rotated on an unclean crash refuses
\* every fence as a non-voter. The code did this for a fence carrying a set
\* that names its old id; this is the coarse form.
RotatedRefuses == FALSE
\* `RotatedForgets`: an unclean crash also loses the copy's ballot, yet the
\* copy still answers as the voter its old id named. The code counts a rotated
\* copy only while its directory kept every ballot.
RotatedForgets == FALSE
\* `DropEvidenced`: a set started between the anchor and the fence is not
\* fenced when its leader answers with a kept ballot above it and a trusted
\* log holding no record of it: it never opened and never will.
DropEvidenced == FALSE
\* `IdleVouch`: a leader sends a follower level with it a batch anyway, its
\* last record again with the mark, so an idle shard's untrusted follower is
\* vouched without a write. FALSE is a leader that ships only what a
\* follower lacks: nothing on an idle shard.
IdleVouch == TRUE
\* `VouchEmpty`: a batch to a follower holding no client record vouches when
\* the leader holds none either. The code's log has no start records, so
\* there its mark stays 0 on such a shard and the mark rule never fires.
\* FALSE is a leader that vouches only through a mark covering a write.
VouchEmpty == TRUE

\* Learners: a set change may stage members that are not voters yet (a move's
\* successor). `LearnerChoices` is the learner sets a set change may name.
\* An acknowledgement counts a majority of the voters, or of the voters and
\* learners together (a leader that counts the learners as voters). So a
\* fence needs a majority of the voters and, by default, a majority of the
\* whole set too. `LearnerBlocking` asks only half of the whole set:
\* that still meets every majority of it, and two fences meet in the voters.
NoLearners == {{}}
OneLearner == {{}} \cup {{n} : n \in Nodes}
TwoLearners == {{}} \cup {{m, n} : m, n \in Nodes}
LearnerChoices == NoLearners
LearnerBlocking == FALSE
\* Shortcuts. Each must make TLC fail.
LearnerDropped == FALSE    \* the whole set is not fenced, only its voters
CountNonKept == FALSE      \* a promise counts whatever the copy kept
CoverFromDoubted == FALSE  \* any answer counts for coverage
DropUnevidenced == FALSE   \* drop a set no answer shows a record of
SkipCoverage == FALSE      \* every fence opens on its own log, as a continuing one
CountsWiped == FALSE       \* #1331: a readmission's own fence counts the wiped seat
AnyCopy == FALSE           \* a continuing fence counts every answer
BatchBallot == FALSE       \* a ballot a lost copy took from a batch counts as kept
CountsForgotten == FALSE   \* a restore goes unnoticed: the copy still claims kept
NamesByRegistration == FALSE \* a wiped copy is counted as the copy it replaced
TrustsRegistered == FALSE  \* only its log is: coverage counts the empty copy
VouchAtStart == FALSE      \* a batch reaching the leader's start record vouches
VouchBelowStart == FALSE   \* a batch level through the mark vouches, short of the start record

ASSUME First \in Nodes /\ NoNode \notin Nodes
ASSUME OldPeers \subseteq Nodes
ASSUME \A S \in SetChoices : S \subseteq Nodes /\ S /= {}

Inf == MaxGen + 1
NoBallot == [g |-> 0, l |-> NoNode]
StartRec(g) == [g |-> g, w |-> 0]
Range(s) == {s[i] : i \in DOMAIN s}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Majority(X, S) == 2 * Cardinality(X \cap S) > Cardinality(S)
\* Meets every majority of S: enough for coverage, since an acknowledgement
\* needs a majority. Exclusion still needs a majority, so that two fences at
\* one generation meet.
MeetsQuorums(X, S) == 2 * Cardinality(X \cap S) >= Cardinality(S)
LastGen(s) == IF s = <<>> THEN 0 ELSE s[Len(s)].g
\* (last generation, length): the order the catch-up takes the furthest by.
Ahead(s, t) == LastGen(s) > LastGen(t) \/ (LastGen(s) = LastGen(t) /\ Len(s) >= Len(t))
Prefix(s, j) == SubSeq(s, 1, j)
IsPrefix(s, t) == Len(s) <= Len(t) /\ s = Prefix(t, Len(s))
Min2(a, b) == IF a < b THEN a ELSE b
Common(s, t) == Max({j \in 0..Min2(Len(s), Len(t)) : Prefix(s, j) = Prefix(t, j)})

VARIABLES
    ballot,     \* durable promise: [g, l]
    log,        \* each seat's copy
    trusted,    \* the copy holds what it acknowledged: clean start, or vouched to the mark
    keptFrom,   \* promises count for sets introduced at this generation or later
    lostAt,     \* how many starts existed when the copy last lost its ballots
    batchSince, \* a lost copy took a ballot from a batch since
    setGen,     \* the newest set generation the copy has heard
    openAt,     \* generation this process leads, open; 0 if none
    openIdx,    \* the start it leads
    fence,      \* [g, i, cont]: the fence this process runs; g = 0 if none
    ans,        \* answers to that fence
    starts,     \* every start: [g, l, S, L, sg, si]; L the learners
    opened,     \* [g, l] that opened
    served,     \* generations whose start record reached a majority
    acked,      \* acknowledged records
    ackedAt,    \* [r, at]: a record and the generation whose leader acknowledged it
    rotAt,      \* the ballot generation at the copy's last unclean crash (its id rotated)
    note,       \* how the newest open happened, for the shape probes
    writes, budget

vars == <<ballot, log, trusted, keptFrom, lostAt, batchSince, setGen, openAt,
          openIdx, fence, ans, starts, opened, served, acked, ackedAt, rotAt, note, writes, budget>>

NoFence == [g |-> 0, i |-> 0, cont |-> FALSE]
NoAns == [n \in Nodes |-> [none |-> TRUE]]
Answered(f, m) == ~("none" \in DOMAIN ans[f][m])
Snapshot(m) == [log |-> log[m], tr |-> trusted[m], kf |-> keptFrom[m],
                lost |-> lostAt[m], bb |-> batchSince[m], prior |-> ballot[m],
                rot |-> rotAt[m]]

TopGen == Max({ballot[n].g : n \in Nodes})
Serving == \E n \in Nodes : openAt[n] = TopGen /\ ballot[n] = [g |-> TopGen, l |-> n]
FaultsAllowed == ~LiveCp \/ TopGen < FaultUntil

Init ==
    /\ ballot = [n \in Nodes |-> [g |-> 1, l |-> First]]
    /\ log = [n \in Nodes |-> <<StartRec(1)>>]
    /\ trusted = [n \in Nodes |-> TRUE]
    /\ keptFrom = [n \in Nodes |-> 0]
    /\ lostAt = [n \in Nodes |-> 0]
    /\ batchSince = [n \in Nodes |-> FALSE]
    /\ setGen = [n \in Nodes |-> 1]
    /\ openAt = [n \in Nodes |-> IF n = First THEN 1 ELSE 0]
    /\ openIdx = [n \in Nodes |-> IF n = First THEN 1 ELSE 0]
    /\ fence = [n \in Nodes |-> NoFence]
    /\ ans = [n \in Nodes |-> NoAns]
    /\ starts = <<[g |-> 1, l |-> First, S |-> Nodes, L |-> {}, sg |-> 1, si |-> 1]>>
    /\ opened = {[g |-> 1, l |-> First]}
    /\ served = {1}
    /\ acked = {StartRec(1)}
    /\ ackedAt = {[r |-> StartRec(1), at |-> 1]}
    /\ rotAt = [n \in Nodes |-> 0]
    /\ note = [g |-> 1, cont |-> FALSE, foreign |-> FALSE]
    /\ writes = 0
    /\ budget = [starts |-> MaxStarts, unclean |-> MaxUnclean, wipes |-> MaxWipes,
                 restores |-> MaxRestores, retries |-> MaxRetries]

(***************************************************************************)
(* Starting a fence. `BeginFence` is shared by the budgeted starts and the *)
(* control plane's liveness renames.                                       *)
(***************************************************************************)
BeginFence(f, g, st) ==
    /\ ballot[f].g < g
    /\ ballot' = [ballot EXCEPT ![f] = [g |-> g, l |-> f]]
    /\ starts' = Append(starts, st)
    /\ fence' = [fence EXCEPT ![f] = [g |-> g, i |-> Len(starts) + 1,
                                      cont |-> openAt[f] > 0 /\ openAt[f] = g - 1]]
    /\ ans' = [ans EXCEPT ![f] = [NoAns EXCEPT ![f] = Snapshot(f)]]
    /\ setGen' = [setGen EXCEPT ![f] = IF st.sg > @ THEN st.sg ELSE @]
    /\ openAt' = [openAt EXCEPT ![f] = 0]
    /\ openIdx' = [openIdx EXCEPT ![f] = 0]

\* The set a start names: one an earlier start named (a candidate on the set
\* it knows, a failover, a rename), or a new one (a set change).
SetOptions(f, g) ==
    {[S |-> starts[j].S, L |-> starts[j].L, sg |-> starts[j].sg, si |-> starts[j].si] : j \in DOMAIN starts}
    \cup UNION {{[S |-> S, L |-> L, sg |-> g, si |-> Len(starts) + 1] :
                   L \in {Y \in LearnerChoices : Y \cap S = {}}} : S \in {X \in SetChoices : f \in X}}

Start(f) ==
    /\ FaultsAllowed /\ budget.starts > 0
    /\ \E g \in (ballot[f].g + 1)..MaxGen, o \in SetOptions(f, 0) :
        LET opt == IF o.si = Len(starts) + 1 THEN [o EXCEPT !.sg = g] ELSE o
        IN /\ ReadmitFirst => /\ openAt[f] > 0 /\ g = openAt[f] + 1 /\ o.si = Len(starts) + 1
                        /\ \E m \in o.S \ {f} : rotAt[m] > 0
           /\ MoveFirst =>
                /\ o.si = Len(starts) + 1
                /\ IF budget.starts = MaxStarts
                   THEN f = First /\ openAt[f] > 0 /\ g = openAt[f] + 1 /\ o.L /= {}
                   ELSE /\ f \in starts[Len(starts)].L
                        /\ \E m \in starts[Len(starts)].S \ {First} : rotAt[m] > 0
           /\ BeginFence(f, g, [g |-> g, l |-> f, S |-> opt.S, L |-> opt.L, sg |-> opt.sg, si |-> opt.si])
    /\ budget' = [budget EXCEPT !.starts = @ - 1]
    /\ UNCHANGED <<log, trusted, keptFrom, lostAt, batchSince, opened, served, acked, ackedAt, rotAt, note, writes>>

\* Liveness: while nobody serves, the control plane names a leader above
\* every ballot on the newest start's set.
CpStart(f) ==
    /\ LiveCp /\ ~Serving /\ TopGen < MaxGen
    /\ ReadmitFirst => budget.starts = 0 /\ rotAt[First] > 0
    /\ MoveFirst => budget.starts = 0
    /\ LET last == starts[Len(starts)]
       IN BeginFence(f, TopGen + 1, [g |-> TopGen + 1, l |-> f, S |-> last.S, L |-> last.L,
                                     sg |-> last.sg, si |-> last.si])
    /\ UNCHANGED <<log, trusted, keptFrom, lostAt, batchSince, opened, served, acked, ackedAt, rotAt, note,
                   writes, budget>>

(***************************************************************************)
(* A member answers a fence: a promise for any generation above its ballot *)
(* (or a retry of the same one), unless it heard a newer set. Under the    *)
(* old rules a copy that is not trusted refuses, except a continuing fence *)
(* its ballot names at g - 1 or g.                                         *)
(***************************************************************************)
StartOf(f) == starts[fence[f].i]

LegacyRefuses(f, m) ==
    /\ (~UnifiedFence \/ m \in OldPeers \/ f \in OldPeers)
    /\ ~trusted[m]
    /\ ~(fence[f].cont /\ ballot[m].l = f /\ ballot[m].g >= fence[f].g - 1 /\ keptFrom[m] = 0)

Answer(f, m) ==
    /\ m /= f /\ fence[f].g > 0 /\ ~Answered(f, m)
    /\ ballot[m].g < fence[f].g \/ ballot[m] = [g |-> fence[f].g, l |-> f]
    /\ setGen[m] <= StartOf(f).sg
    /\ ~LegacyRefuses(f, m)
    /\ ~(RotatedRefuses /\ rotAt[m] > 0)
    /\ ans' = [ans EXCEPT ![f][m] = Snapshot(m)]
    /\ ballot' = [ballot EXCEPT ![m] = [g |-> fence[f].g, l |-> f]]
    /\ setGen' = [setGen EXCEPT ![m] = StartOf(f).sg]
    /\ openAt' = [openAt EXCEPT ![m] = 0]
    /\ openIdx' = [openIdx EXCEPT ![m] = 0]
    /\ UNCHANGED <<log, trusted, keptFrom, lostAt, batchSince, fence, starts, opened,
                   served, acked, ackedAt, rotAt, note, writes, budget>>

Retry(f) ==
    /\ FaultsAllowed /\ fence[f].g > 0 /\ budget.retries > 0
    /\ ans' = [ans EXCEPT ![f] = [NoAns EXCEPT ![f] = Snapshot(f)]]
    /\ budget' = [budget EXCEPT !.retries = @ - 1]
    /\ UNCHANGED <<ballot, log, trusted, keptFrom, lostAt, batchSince, setGen, openAt,
                   openIdx, fence, starts, opened, served, acked, ackedAt, rotAt, note, writes>>

(***************************************************************************)
(* The open condition.                                                     *)
(***************************************************************************)
Legacy(f, m) == ~UnifiedFence \/ f \in OldPeers \/ m \in OldPeers

\* Today's counting, in outline: never a lost copy; a copy that is not
\* trusted only on the continuing window; on a failover only the copy the
\* previous generation's leader reached under the identity it has now (the
\* pin): it took that generation, and its id has not rotated since.
PrevGen(f) == Max({0} \cup {o.g : o \in {x \in opened : x.g < fence[f].g}})
LegacyCounts(f, m, a) ==
    /\ a.kf = 0
    /\ a.tr \/ (fence[f].cont /\ a.prior.l = f /\ a.prior.g >= fence[f].g - 1)
    /\ fence[f].cont \/ m = f \/ (a.prior.g >= PrevGen(f) /\ a.rot < PrevGen(f))

Promises(f, R, m) ==
    LET a == ans[f][m] IN
    IF Legacy(f, m) THEN LegacyCounts(f, m, a)
    ELSE \/ a.kf <= R.sg
         \/ CountNonKept
         \/ CountsWiped /\ StartOf(f).si > a.lost
         \/ AnyCopy /\ fence[f].cont
         \/ BatchBallot /\ a.bb

Covers(f, m) ==
    LET a == ans[f][m] IN
    IF Legacy(f, m) THEN LegacyCounts(f, m, a) ELSE a.tr \/ CoverFromDoubted

Continues(f) == fence[f].cont \/ SkipCoverage

\* Generations the fence has evidence opened: served ones, and those a
\* trusted answer holds a record of. The continuing leader's own g - 1.
Anchors(f) ==
    {a \in served \cup {r.g : r \in UNION {Range(ans[f][m].log) :
                                         m \in {x \in Nodes : Answered(f, x) /\ ans[f][x].tr}}}
       : a < fence[f].g}
    \cup (IF fence[f].cont THEN {fence[f].g - 1} ELSE {})

NoRecordOf(s, g) == \A r \in Range(s) : r.g /= g
Evidenced(f, st) ==
    /\ DropEvidenced
    /\ Answered(f, st.l)
    /\ ans[f][st.l].tr /\ ans[f][st.l].kf <= st.sg
    /\ NoRecordOf(ans[f][st.l].log, st.g)
Silent(f, st) ==
    /\ DropUnevidenced
    /\ \A m \in Nodes : Answered(f, m) => NoRecordOf(ans[f][m].log, st.g)

Required(f, anchor) ==
    {StartOf(f)} \cup
    {st \in Range(starts) :
        /\ anchor <= st.g /\ st.g < fence[f].g
        /\ ~(st.g > anchor /\ (Evidenced(f, st) \/ Silent(f, st)))}

\* A set fenced as `V`: its voters, or its voters and learners together.
\* Exclusion needs a majority of `V`, or with `blocking` half of it.
FencesAs(f, R, V, blocking) ==
    LET promised == {m \in V : Answered(f, m) /\ Promises(f, R, m)}
    IN /\ IF blocking THEN MeetsQuorums(promised, V) ELSE Majority(promised, V)
       /\ Continues(f) \/ MeetsQuorums({m \in V : Answered(f, m) /\ Covers(f, m)}, V)

OpenOK(f, anchor) ==
    \A R \in Required(f, anchor) :
        /\ FencesAs(f, R, R.S, FALSE)
        /\ R.L = {} \/ LearnerDropped \/ FencesAs(f, R, R.S \cup R.L, LearnerBlocking)

Furthest(f) ==
    LET logs == {ans[f][m].log : m \in {x \in Nodes : Answered(f, x)}} \cup {log[f]}
    IN CHOOSE s \in logs : \A t \in logs : Ahead(s, t)

Open(f) ==
    /\ fence[f].g > 0
    /\ ballot[f] = [g |-> fence[f].g, l |-> f]
    /\ \E anchor \in Anchors(f) : OpenOK(f, anchor)
    /\ LET base == IF Continues(f) THEN log[f] ELSE Furthest(f)
       IN log' = [log EXCEPT ![f] = Append(base, StartRec(fence[f].g))]
    /\ trusted' = [trusted EXCEPT ![f] = TRUE]
    /\ openAt' = [openAt EXCEPT ![f] = fence[f].g]
    /\ openIdx' = [openIdx EXCEPT ![f] = fence[f].i]
    /\ opened' = opened \cup {[g |-> fence[f].g, l |-> f]}
    /\ note' = [g |-> fence[f].g, cont |-> fence[f].cont,
                foreign |-> \E m \in StartOf(f).S \ {f} :
                    /\ Answered(f, m) /\ ~ans[f][m].tr
                    /\ Promises(f, StartOf(f), m)
                    /\ ~(ans[f][m].prior.l = f /\ ans[f][m].prior.g >= fence[f].g - 1)]
    /\ fence' = [fence EXCEPT ![f] = NoFence]
    /\ ans' = [ans EXCEPT ![f] = NoAns]
    /\ UNCHANGED <<ballot, keptFrom, lostAt, batchSince, setGen, starts, served, acked, ackedAt, rotAt,
                   writes, budget>>

(***************************************************************************)
(* Serving.                                                                *)
(***************************************************************************)
Leads(l) == openAt[l] > 0 /\ ballot[l] = [g |-> openAt[l], l |-> l]
LeadStart(l) == starts[openIdx[l]]
StartPos(l) == CHOOSE i \in DOMAIN log[l] : log[l][i] = StartRec(openAt[l])
\* The mark a batch carries, as the code computes it (commit_offset, the
\* quorum offset at the leader's generation): the furthest prefix a majority
\* of its set holds at its ballot, and never below what the leader
\* acknowledged at its generation. It is not clamped to the start record. The
\* code's mark only grows, so it is never below this one, and a lower mark
\* only vouches more.
\* A leader that counts its learners as voters takes the mark over them too,
\* which a lagging learner holds lower, so the lower of the two is taken.
MarkOver(l, V) == Max({0} \cup {j \in 0..Len(log[l]) :
    Majority({l} \cup {m \in V : ballot[m] = ballot[l] /\ keptFrom[m] <= LeadStart(l).sg
                        /\ Len(log[m]) >= j /\ Prefix(log[m], j) = Prefix(log[l], j)}, V)})
QuorumMark(l) == Min2(MarkOver(l, LeadStart(l).S), MarkOver(l, LeadStart(l).S \cup LeadStart(l).L))
AckedMark(l) == Max({0} \cup {j \in 1..Len(log[l]) : [r |-> log[l][j], at |-> openAt[l]] \in ackedAt})
CarriedMark(l) == LET q == QuorumMark(l) IN IF q > AckedMark(l) THEN q ELSE AckedMark(l)

Write(l) ==
    /\ Leads(l) /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ log' = [log EXCEPT ![l] = Append(@, [g |-> openAt[l], w |-> writes + 1])]
    /\ UNCHANGED <<ballot, trusted, keptFrom, lostAt, batchSince, setGen, openAt, openIdx,
                   fence, ans, starts, opened, served, acked, ackedAt, rotAt, note, budget>>

\* A leader whose set was introduced after `m`'s copy lost its ballots, and
\* names it, opened after the loss and holds whatever that seat acknowledged
\* before it. Only such a leader readmits the copy or vouches for it: a
\* deposed leader would take a lost copy back below what its seat acknowledged.
Readmits(l, m) == m \in LeadStart(l).S /\ LeadStart(l).si > lostAt[m]

\* A batch vouches when it leaves the copy level through both the leader's
\* start record and the mark it carries. The mark alone can sit below the
\* start record right after an open (VouchBelowStart), and a batch carrying
\* no mark (commit_offset 0) vouches for nothing. The code's offsets count
\* client records only, so on a log of start records alone its mark is 0
\* for good; there the mark never vouches and `VouchEmpty` does: nothing
\* was ever acknowledged, so a follower holding no write holds all of it.
Writes(s) == {i \in DOMAIN s : s[i].w > 0}
Vouches(l, m, k) ==
    \/ /\ CarriedMark(l) > 0 /\ Writes(log[l]) /= {}
       /\ k >= CarriedMark(l) /\ (k >= StartPos(l) \/ VouchBelowStart)
    \/ VouchEmpty /\ Writes(log[l]) = {} /\ Writes(log[m]) = {} /\ k = Len(log[l])
    \/ VouchAtStart /\ k >= StartPos(l)

\* A batch: the leader's log from where the two agree to some later point,
\* or to its end, even when the follower already holds it all (`IdleVouch`).
\* It readmits a copy that lost its ballots when the leader's
\* set was introduced after the loss and names it.
Ship(l, m) ==
    /\ Leads(l) /\ m /= l
    /\ ballot[m].g < openAt[l] \/ ballot[m] = ballot[l]
    /\ setGen[m] <= LeadStart(l).sg
    /\ \E k \in Common(log[m], log[l])..Len(log[l]) :
        /\ k > Common(log[m], log[l]) \/ (IdleVouch /\ k = Len(log[l]))
        /\ log' = [log EXCEPT ![m] = Prefix(log[l], k)]
        /\ trusted' = [trusted EXCEPT ![m] =
                         @ \/ (Vouches(l, m, k) /\ (Readmits(l, m) \/ keptFrom[m] < Inf))]
    /\ ballot' = [ballot EXCEPT ![m] = ballot[l]]
    /\ batchSince' = [batchSince EXCEPT ![m] = @ \/ (keptFrom[m] = Inf /\ ballot[m] /= ballot[l])]
    /\ keptFrom' = [keptFrom EXCEPT ![m] =
                       IF Readmits(l, m) /\ LeadStart(l).sg < @ THEN LeadStart(l).sg ELSE @]
    /\ setGen' = [setGen EXCEPT ![m] = LeadStart(l).sg]
    /\ openAt' = [openAt EXCEPT ![m] = 0]
    /\ openIdx' = [openIdx EXCEPT ![m] = 0]
    /\ UNCHANGED <<lostAt, fence, ans, starts, opened, served, acked, ackedAt, rotAt, note, writes, budget>>

\* Acknowledge a prefix held by a majority of the generation's set, each a
\* copy whose promises count for that set and that took this leader's ballot.
\* The majority is of the voters, or of voters and learners together.
Ack(l) ==
    /\ Leads(l)
    /\ \E j \in StartPos(l)..Len(log[l]), M \in SUBSET (LeadStart(l).S \cup LeadStart(l).L) :
        /\ Majority(M, LeadStart(l).S) \/ Majority(M, LeadStart(l).S \cup LeadStart(l).L)
        /\ \A m \in M : /\ m = l \/ ballot[m] = ballot[l]
                        /\ Len(log[m]) >= j /\ Prefix(log[m], j) = Prefix(log[l], j)
                        /\ keptFrom[m] <= LeadStart(l).sg
        /\ acked' = acked \cup Range(Prefix(log[l], j))
        /\ ackedAt' = ackedAt \cup {[r |-> x, at |-> openAt[l]] : x \in Range(Prefix(log[l], j))}
    /\ served' = served \cup {openAt[l]}
    /\ UNCHANGED <<ballot, log, trusted, keptFrom, lostAt, batchSince, setGen, openAt,
                   openIdx, fence, ans, starts, opened, rotAt, note, writes, budget>>

(***************************************************************************)
(* Faults. A crash restarts the process at once: it leads nothing and its  *)
(* fence is gone.                                                          *)
(***************************************************************************)
Stop(n) ==
    /\ openAt' = [openAt EXCEPT ![n] = 0]
    /\ openIdx' = [openIdx EXCEPT ![n] = 0]
    /\ fence' = [fence EXCEPT ![n] = NoFence]
    /\ ans' = [ans EXCEPT ![n] = NoAns]

UncleanCrash(n) ==
    /\ FaultsAllowed /\ budget.unclean > 0
    /\ ReadmitFirst => n /= First \/ (budget.starts = 0 /\ fence[n].g > 0)
    /\ MoveFirst => n /= First /\ budget.starts = MaxStarts - 1 /\ openAt[First] > 1
    /\ \E j \in 0..Len(log[n]) : log' = [log EXCEPT ![n] = Prefix(@, j)]
    /\ trusted' = [trusted EXCEPT ![n] = FALSE]
    /\ rotAt' = [rotAt EXCEPT ![n] = ballot[n].g]
    /\ ballot' = IF RotatedForgets THEN [ballot EXCEPT ![n] = NoBallot] ELSE ballot
    /\ Stop(n)
    /\ budget' = [budget EXCEPT !.unclean = @ - 1]
    /\ UNCHANGED <<keptFrom, lostAt, batchSince, setGen, starts, opened, served,
                   acked, ackedAt, note, writes>>

Lose(n) ==
    /\ lostAt' = [lostAt EXCEPT ![n] = Len(starts)]
    /\ batchSince' = [batchSince EXCEPT ![n] = FALSE]

Wipe(n) ==
    /\ FaultsAllowed /\ budget.wipes > 0
    /\ log' = [log EXCEPT ![n] = <<>>]
    /\ ballot' = [ballot EXCEPT ![n] = NoBallot]
    /\ setGen' = [setGen EXCEPT ![n] = 0]
    /\ IF NamesByRegistration
       THEN UNCHANGED <<trusted, keptFrom, lostAt, batchSince>>
       ELSE /\ trusted' = [trusted EXCEPT ![n] = TrustsRegistered]
            /\ keptFrom' = [keptFrom EXCEPT ![n] = Inf]
            /\ Lose(n)
    /\ Stop(n)
    /\ budget' = [budget EXCEPT !.wipes = @ - 1]
    /\ UNCHANGED <<starts, opened, served, acked, ackedAt, rotAt, note, writes>>

\* A restore takes the copy back to an older state: a prefix of its log and
\* a ballot no newer than the one it had.
Restore(n) ==
    /\ FaultsAllowed /\ budget.restores > 0
    /\ \E j \in 0..Len(log[n]) : log' = [log EXCEPT ![n] = Prefix(@, j)]
    /\ \E b \in {NoBallot} \cup {[g |-> h, l |-> x] : h \in 1..ballot[n].g, x \in Nodes} :
        ballot' = [ballot EXCEPT ![n] = b]
    /\ IF CountsForgotten
       THEN UNCHANGED <<trusted, keptFrom, lostAt, batchSince>>
       ELSE /\ trusted' = [trusted EXCEPT ![n] = FALSE]
            /\ keptFrom' = [keptFrom EXCEPT ![n] = Inf]
            /\ Lose(n)
    /\ Stop(n)
    /\ budget' = [budget EXCEPT !.restores = @ - 1]
    /\ UNCHANGED <<setGen, starts, opened, served, acked, ackedAt, rotAt, note, writes>>

Next ==
    \/ \E f \in Nodes : Start(f) \/ CpStart(f) \/ Retry(f) \/ Open(f)
    \/ \E f, m \in Nodes : Answer(f, m) \/ Ship(f, m)
    \/ \E l \in Nodes : Write(l) \/ Ack(l)
    \/ \E n \in Nodes : UncleanCrash(n) \/ Wipe(n) \/ Restore(n)

Spec == Init /\ [][Next]_vars

\* A leader keeps shipping and acknowledging. No fairness on starts, faults
\* or writes, so a run ends idle once the budgets are spent.
ShipSpec ==
    /\ Spec
    /\ \A l \in Nodes : WF_vars(Ack(l))
    /\ \A l, m \in Nodes : WF_vars(Ship(l, m))

LiveSpec ==
    /\ Spec
    /\ WF_vars(\E f \in Nodes : CpStart(f))
    /\ \A f \in Nodes : WF_vars(Open(f))
    /\ \A f, m \in Nodes : WF_vars(Answer(f, m))

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)
OneLeaderPerGeneration ==
    \A x, y \in opened : x.g = y.g => x.l = y.l

\* A leader holds every record a leader at or below its generation acknowledged.
AckedHeldByLeader ==
    \A n \in Nodes : Leads(n) =>
        \A x \in ackedAt : x.at <= openAt[n] => x.r \in Range(log[n])

\* Liveness: once faults stop, some leader serves for good. A shard whose
\* every set lost its trusted majority waits for the operator by design.
CoverLost == 2 * Cardinality({n \in Nodes : trusted[n]}) <= Cardinality(Nodes)
Settled == Serving \/ CoverLost
EventuallyServes == <>[]Settled

\* Liveness: a follower of a serving leader that has not lost its ballots
\* is trusted again, writes or none, while that leader serves it.
FollowerTrusted ==
    \A l, m \in Nodes :
        (/\ Leads(l) /\ m /= l /\ m \in LeadStart(l).S /\ ~trusted[m]
         /\ ballot[m] = ballot[l] /\ keptFrom[m] < Inf)
            ~> (trusted[m] \/ ~Leads(l) \/ ballot[m] /= ballot[l])

\* Reachability probe: TLC reports a "violation" when a continuing fence
\* opens counting a doubted copy whose ballot names another leader, or one
\* older than g - 1, which today's window refuses.
NoContinuingOnForeignBallot == ~(note.cont /\ note.foreign)
=============================================================================
