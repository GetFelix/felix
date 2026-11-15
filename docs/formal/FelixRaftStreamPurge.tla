------------------------ MODULE FelixRaftStreamPurge ------------------------
(***************************************************************************)
(* Freeing disk on a Raft stream shard: purging the log below a head trim  *)
(* and installing a stream snapshot (issue 2602).                          *)
(*                                                                         *)
(* An offset is a Raft index. A head trim is an entry trim(f): every       *)
(* record below offset f is unreadable once a member applies it. A trim    *)
(* names a floor at or below the tail when it is proposed, so the trim     *)
(* entry is always above its own floor.                                    *)
(*                                                                         *)
(* A member purges its own log up to P and writes a stream snapshot there: *)
(* its index, the floor it has applied, and its tables (producers, groups, *)
(* cut batches) as of the snapshot's index. The tables are one counter     *)
(* here, the number of records applied: a fold that is wrong when an entry *)
(* is applied twice or skipped, as the real tables are.                    *)
(*                                                                         *)
(* A member whose next entry the leader has purged can only be sent the    *)
(* leader's snapshot. It installs it only when every entry the snapshot    *)
(* skips is below the floor the snapshot carries; otherwise it refuses, as *)
(* every stream member refuses a snapshot today.                           *)
(*                                                                         *)
(* A re-form starts a new epoch from the copies an operator's command      *)
(* reads (FelixRaftReform.tla has the command). Copies not read are        *)
(* declared dead. The new epoch's base is the highest log id a read copy   *)
(* vouches for, padded to the offset floor the control plane holds (the    *)
(* last commit a leader reported), and the new epoch gives out offsets     *)
(* above it.                                                               *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   NoReadBelowFloor  -- a leader serving reads (it has applied every     *)
(*                        committed entry) serves no record below the      *)
(*                        highest floor any live member ever had.          *)
(*   NoAckedLoss       -- every member holds every committed record at or  *)
(*                        above the committed floor, up to the end of its  *)
(*                        log. No purge and no install removes one.        *)
(*   PurgeBelowFloor   -- a member that purged anything purged below its   *)
(*                        floor.                                           *)
(*   TablesAgree       -- every member's tables are the fold of the log    *)
(*                        up to the entry it has applied, across purges,   *)
(*                        installs and restarts.                           *)
(*   OffsetNeverReused -- a re-form never gives out again a committed      *)
(*                        offset that a copy it read held or installed a   *)
(*                        snapshot or floor for (a history variable).      *)
(*   CatchUpPossible   -- a member behind the leader's purge can install   *)
(*                        the leader's snapshot. Not safety: a member that *)
(*                        fails it is stuck until a later purge.           *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   PurgeBelowFloorRule = FALSE -- a purge is bounded by the applied      *)
(*                        entry only, not by floor - 1.                    *)
(*   InstallRule = FALSE  -- a member installs any snapshot ahead of it.   *)
(*   LeaderCaps = FALSE   -- the leader purges past a member's match.      *)
(*   SnapAtApplied = TRUE -- the snapshot is taken at the applied entry    *)
(*                        and the purge goes below it, as a plain Raft     *)
(*                        snapshot would be.                               *)
(*   TablesAtApplied = TRUE -- the snapshot is at the purge point but its  *)
(*                        tables are the member's current ones.            *)
(*   ReformCarriesFloor = FALSE -- a re-form keeps each copy's own floor,  *)
(*                        not the highest floor it read.                   *)
(*   ReformStoresFloor = FALSE -- the re-form raises the floor in memory   *)
(*                        only, so a restart loses it.                     *)
(*   ReformReads          -- what a re-form reads of a copy: "full" is its *)
(*                        last entry, purged id and floor; "purged"        *)
(*                        leaves the floor out; "entries" reads the last   *)
(*                        entry only, so an empty log reads as nothing.    *)
(*                                                                         *)
(* Abstracted: terms and uncommitted divergence (FelixRaft.tla has them;   *)
(* here one global log, and a member holds a prefix of it), votes (a new   *)
(* leader is any member holding the committed log), record payloads, and   *)
(* lost disks (a copy not read at a re-form is the same thing). The        *)
(* producer table's own rules at a purge and an install are                *)
(* FelixRaftBatch.tla's; here only that the table travels at the right     *)
(* index. A member's log and snapshot are durable; a restart rolls its     *)
(* state machine back to its snapshot and applies the log again.           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Members,
    Voters,              \* the rest of Members are learners
    MaxIndex,
    MaxTrims,
    MaxChanges,          \* changes of leader
    MaxRestarts,
    MaxReforms,
    PurgeBelowFloorRule,
    InstallRule,
    LeaderCaps,
    SnapAtApplied,
    TablesAtApplied,
    ReformReads,
    ReformCarriesFloor,
    ReformStoresFloor

ASSUME
    /\ Voters \subseteq Members /\ Voters # {}
    /\ MaxIndex \in Nat /\ MaxTrims \in Nat /\ MaxChanges \in Nat
    /\ MaxRestarts \in Nat /\ MaxReforms \in Nat
    /\ PurgeBelowFloorRule \in BOOLEAN /\ InstallRule \in BOOLEAN
    /\ LeaderCaps \in BOOLEAN /\ SnapAtApplied \in BOOLEAN
    /\ TablesAtApplied \in BOOLEAN /\ ReformCarriesFloor \in BOOLEAN
    /\ ReformStoresFloor \in BOOLEAN
    /\ ReformReads \in {"full", "purged", "entries"}

VARIABLES
    log,      \* every entry of the current epoch's history, in order
    commit,
    leader,
    live,     \* members not declared dead at a re-form
    len,      \* per member: it holds entries purged+1 .. len
    purged,
    applied,
    floor,    \* per member: the highest floor it has applied or installed
    tbl,      \* per member: its tables, as of applied
    snap,     \* per member: its stream snapshot [idx, floor, tbl]
    cpFloor,  \* the control plane's offset floor: a commit a leader reported
    reused,   \* history: a re-form gave out an offset a read copy vouched for
    installed, \* history: the members that installed a snapshot
    given,    \* history, per member: the highest committed offset it held or
              \* installed a snapshot or floor for
    hiFloor,  \* history, per member: the highest floor it ever had
    trims, changes, restarts, reforms

vars == <<log, commit, leader, live, len, purged, applied, floor, tbl, snap,
          cpFloor, reused, installed, given, hiFloor, trims, changes, restarts,
          reforms>>

Symm == Permutations(Voters) \cup Permutations(Members \ Voters)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

Rec == [kind |-> "rec", f |-> 0]
Trim(f) == [kind |-> "trim", f |-> f]
Gap == [kind |-> "gap", f |-> 0]

\* Records in log[lo..hi].
Recs(lo, hi) == Cardinality({i \in lo..hi : log[i].kind = "rec"})

\* The floor of the trims in log[1..hi].
FloorUpTo(hi) == Max({1} \cup {log[i].f : i \in {j \in 1..hi : log[j].kind = "trim"}})

\* Every floor a live member ever had came from a committed trim. A restart
\* that loses a floor does not lower this.
CommittedFloor == Max({FloorUpTo(commit)} \cup {hiFloor[m] : m \in live})

RaiseHiFloor == hiFloor' = [k \in Members |-> Max({hiFloor[k], floor'[k]})]

NoSnap == [idx |-> 0, floor |-> 1, tbl |-> 0]

----------------------------------------------------------------------------

Init ==
    /\ log = <<>>
    /\ commit = 0
    /\ leader \in Voters
    /\ live = Members
    /\ len = [m \in Members |-> 0]
    /\ purged = [m \in Members |-> 0]
    /\ applied = [m \in Members |-> 0]
    /\ floor = [m \in Members |-> 1]
    /\ tbl = [m \in Members |-> 0]
    /\ snap = [m \in Members |-> NoSnap]
    /\ cpFloor = 0
    /\ reused = FALSE
    /\ installed = {}
    /\ given = [m \in Members |-> 0]
    /\ hiFloor = [m \in Members |-> 1]
    /\ trims = 0 /\ changes = 0 /\ restarts = 0 /\ reforms = 0

AppendEntry(e) ==
    /\ Len(log) < MaxIndex
    /\ log' = Append(log, e)
    /\ len' = [len EXCEPT ![leader] = Len(log) + 1]

\* A client write.
Write ==
    /\ AppendEntry(Rec)
    /\ UNCHANGED <<commit, leader, live, purged, applied, floor, tbl, snap,
                   cpFloor, reused, installed, trims, changes, restarts, reforms, given, hiFloor>>

\* The leader's retention driver proposes a floor at or below its tail,
\* above the floor it has applied.
ProposeTrim(f) ==
    /\ trims < MaxTrims
    /\ f \in (floor[leader] + 1)..Len(log)
    /\ AppendEntry(Trim(f))
    /\ trims' = trims + 1
    /\ UNCHANGED <<commit, leader, live, purged, applied, floor, tbl, snap,
                   cpFloor, reused, installed, changes, restarts, reforms, given, hiFloor>>

\* The leader sends a follower its next entry, which it still holds.
Replicate(n) ==
    /\ n \in live \ {leader}
    /\ len[n] < Len(log)
    /\ len[n] + 1 > purged[leader]
    /\ len' = [len EXCEPT ![n] = @ + 1]
    /\ given' = [given EXCEPT ![n] = Max({@, Min({len[n] + 1, commit})})]
    /\ UNCHANGED <<log, commit, leader, live, purged, applied, floor, tbl, snap,
                   cpFloor, reused, installed, trims, changes, restarts, reforms, hiFloor>>

\* The leader has purged the follower's next entry: it sends its snapshot.
Installable(s, n) ==
    /\ s.idx > len[n]
    /\ InstallRule => s.idx <= s.floor - 1

Install(n) ==
    LET s == snap[leader] IN
    /\ n \in live \ {leader}
    /\ len[n] + 1 <= purged[leader]
    /\ Installable(s, n)
    /\ len' = [len EXCEPT ![n] = s.idx]
    /\ purged' = [purged EXCEPT ![n] = s.idx]
    /\ applied' = [applied EXCEPT ![n] = s.idx]
    /\ floor' = [floor EXCEPT ![n] = IF s.floor > @ THEN s.floor ELSE @]
    /\ RaiseHiFloor
    /\ given' = [given EXCEPT ![n] = Max({@, s.idx, s.floor})]
    /\ tbl' = [tbl EXCEPT ![n] = s.tbl]
    /\ snap' = [snap EXCEPT ![n] = s]
    /\ installed' = installed \cup {n}
    /\ UNCHANGED <<log, commit, leader, live, cpFloor, reused, trims, changes,
                   restarts, reforms>>

Quorum(i) ==
    LET V == Voters \cap live IN
    2 * Cardinality({v \in V : len[v] >= i}) > Cardinality(V)

Commit(i) ==
    /\ i \in (commit + 1)..Len(log)
    /\ Quorum(i)
    /\ commit' = i
    /\ given' = [m \in Members |-> Max({given[m], Min({len[m], i})})]
    /\ UNCHANGED <<log, leader, live, len, purged, applied, floor, tbl, snap,
                   cpFloor, reused, installed, trims, changes, restarts, reforms, hiFloor>>

\* A leader reports its commit to the control plane.
ReportFloor ==
    /\ cpFloor < commit
    /\ cpFloor' = commit
    /\ UNCHANGED <<log, commit, leader, live, len, purged, applied, floor, tbl,
                   snap, reused, installed, trims, changes, restarts, reforms, given, hiFloor>>

Apply(m) ==
    LET i == applied[m] + 1 IN
    /\ m \in live
    /\ i <= commit /\ i <= len[m]
    /\ applied' = [applied EXCEPT ![m] = i]
    /\ floor' = [floor EXCEPT ![m] =
          IF log[i].kind = "trim" /\ log[i].f > @ THEN log[i].f ELSE @]
    /\ RaiseHiFloor
    /\ tbl' = [tbl EXCEPT ![m] = IF log[i].kind = "rec" THEN @ + 1 ELSE @]
    /\ UNCHANGED <<log, commit, leader, live, len, purged, snap, cpFloor,
                   reused, installed, trims, changes, restarts, reforms, given>>

\* A member purges its log up to p and writes its snapshot. It builds the
\* tables as of p from its previous snapshot and the log it still holds.
Purge(m, p) ==
    /\ m \in live
    /\ p \in (purged[m] + 1)..applied[m]
    /\ PurgeBelowFloorRule => p <= floor[m] - 1
    /\ (LeaderCaps /\ m = leader) => \A n \in live \ {leader} : p <= len[n]
    /\ purged' = [purged EXCEPT ![m] = p]
    /\ snap' = [snap EXCEPT ![m] =
          [idx |-> IF SnapAtApplied THEN applied[m] ELSE p,
           floor |-> floor[m],
           tbl |-> IF SnapAtApplied \/ TablesAtApplied THEN tbl[m]
                  ELSE snap[m].tbl + Recs(snap[m].idx + 1, p)]]
    /\ UNCHANGED <<log, commit, leader, live, len, applied, floor, tbl, cpFloor,
                   reused, installed, trims, changes, restarts, reforms, given, hiFloor>>

\* A member restarts: its state machine is its snapshot again, and it
\* applies its log from there.
Restart(m) ==
    /\ restarts < MaxRestarts
    /\ m \in live
    /\ applied' = [applied EXCEPT ![m] = snap[m].idx]
    /\ floor' = [floor EXCEPT ![m] = snap[m].floor]
    /\ tbl' = [tbl EXCEPT ![m] = snap[m].tbl]
    /\ restarts' = restarts + 1
    /\ UNCHANGED <<log, commit, leader, live, len, purged, snap, cpFloor, reused,
                   installed, trims, changes, reforms, given, hiFloor>>

\* A voter holding the committed log is elected. Entries past its log are
\* dropped; they were never committed.
ChangeLeader(n) ==
    /\ changes < MaxChanges
    /\ n \in Voters \cap live /\ n # leader
    /\ len[n] >= commit
    /\ log' = SubSeq(log, 1, len[n])
    /\ len' = [m \in Members |-> IF len[m] > len[n] THEN len[n] ELSE len[m]]
    /\ leader' = n
    /\ changes' = changes + 1
    /\ UNCHANGED <<commit, live, purged, applied, floor, tbl, snap, cpFloor,
                   reused, installed, trims, restarts, reforms, given, hiFloor>>

\* What a re-form reads as the last offset a copy vouches for. A floor f
\* vouches for f itself: a trim names a floor at or below the tail, so the
\* record at f was committed before the trim was.
ReadLast(m) ==
    CASE ReformReads = "full" ->
           Max({len[m], purged[m], IF floor[m] > 1 THEN floor[m] ELSE 0})
      [] ReformReads = "purged" ->
           Max({len[m], purged[m]})
      [] ReformReads = "entries" ->
           IF len[m] > purged[m] THEN len[m] ELSE 0

\* The operator re-forms the group from the copies in R. Each read copy is
\* a voter of the new epoch; the leader is one that holds the base, and the
\* re-form writes it the gap up to the base. The trim that set a read copy's
\* floor may be in no copy's log, so the new epoch starts at the highest
\* floor read.
Reform(R) ==
    LET b == Max({cpFloor} \cup {ReadLast(m) : m \in R})
        l == CHOOSE m \in R : \A k \in R : len[k] <= len[m]
        keep == Min({len[l], b})
    IN
    /\ reforms < MaxReforms
    /\ R # {} /\ R \subseteq live /\ R \cap Voters # {}
    /\ l \in Voters
    /\ log' = SubSeq(log, 1, keep) \o [i \in 1..(b - keep) |-> Gap]
    /\ reused' = (reused \/ \E m \in R : given[m] > b)
    /\ given' = [m \in Members |-> Min({given[m], b})]
    /\ live' = R
    /\ leader' = l
    /\ commit' = b
    /\ len' = [m \in Members |-> IF m = l THEN b ELSE Min({len[m], b})]
    /\ purged' = [m \in Members |-> Min({purged[m], b})]
    /\ applied' = [m \in Members |-> Min({applied[m], b})]
    /\ floor' = IF ReformCarriesFloor
              THEN [m \in Members |-> IF m \in R THEN Max({floor[k] : k \in R}) ELSE floor[m]]
              ELSE floor
    /\ RaiseHiFloor
    \* The re-form writes each read copy a snapshot at its purge point with the
    \* floor it starts at, so a restart keeps it.
    /\ snap' = IF ReformCarriesFloor /\ ReformStoresFloor
              THEN [m \in Members |-> IF m \in R
                                      THEN [snap[m] EXCEPT !.floor = floor'[m]]
                                      ELSE snap[m]]
              ELSE snap
    /\ reforms' = reforms + 1
    /\ UNCHANGED <<tbl, cpFloor, installed, trims, changes, restarts>>

Next ==
    \/ Write
    \/ \E f \in 1..MaxIndex : ProposeTrim(f)
    \/ \E n \in Members : Replicate(n) \/ Install(n)
    \/ \E i \in 1..MaxIndex : Commit(i)
    \/ ReportFloor
    \/ \E m \in Members : Apply(m) \/ Restart(m)
    \/ \E m \in Members, p \in 1..MaxIndex : Purge(m, p)
    \/ \E n \in Members : ChangeLeader(n)
    \/ \E R \in SUBSET Members : Reform(R)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Entries == {Rec, Gap} \cup {Trim(f) : f \in 1..MaxIndex}

TypeOK ==
    /\ Len(log) <= MaxIndex
    /\ \A i \in 1..Len(log) : log[i] \in Entries
    /\ commit \in 0..Len(log)
    /\ leader \in live
    /\ live \subseteq Members
    /\ \A m \in live : purged[m] <= applied[m] /\ applied[m] <= len[m]
    /\ \A m \in live : len[m] <= Len(log) /\ applied[m] <= commit
    /\ len[leader] = Len(log)

\* A leader serves reads once it has applied every committed entry.
Serving(m) == m = leader /\ applied[m] = commit

Readable(m) ==
    {i \in (purged[m] + 1)..applied[m] : log[i].kind = "rec" /\ i >= floor[m]}

NoReadBelowFloor ==
    \A m \in live : Serving(m) => \A i \in Readable(m) : i >= CommittedFloor

NoAckedLoss ==
    \A m \in live, i \in 1..commit :
        (log[i].kind = "rec" /\ i >= CommittedFloor /\ i <= len[m]) => i > purged[m]

PurgeBelowFloor == \A m \in live : purged[m] > 0 => purged[m] < floor[m]

TablesAgree == \A m \in live : tbl[m] = Recs(1, applied[m])

OffsetNeverReused == ~reused

CatchUpPossible ==
    \A n \in live \ {leader} :
        len[n] + 1 <= purged[leader] => Installable(snap[leader], n)

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

\* A member that installed a snapshot serves as leader.
NeverServesAfterInstall == \A m \in installed : ~Serving(m)

\* A member frees disk: it purges past a record.
NeverPurgesARecord == \A m \in live : Recs(1, purged[m]) = 0

=============================================================================
