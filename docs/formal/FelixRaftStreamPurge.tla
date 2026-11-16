------------------------ MODULE FelixRaftStreamPurge ------------------------
(***************************************************************************)
(* Freeing disk on a Raft stream shard: purging the log below a head trim  *)
(* and installing a stream snapshot (issue 2602).                          *)
(*                                                                         *)
(* An offset is a Raft index. A head trim is an entry trim(f): every       *)
(* record below offset f is unreadable once a member applies it. A trim    *)
(* names a floor no higher than its own offset (the trim proposal allows   *)
(* the offset it lands at), so the trim entry is never below its floor.    *)
(* A member answers a read below its floor with the trim's headers, so the *)
(* headers travel with the floor.                                          *)
(*                                                                         *)
(* A member purges its own log up to P in two steps: it writes and fsyncs  *)
(* a stream snapshot at P, then drops its log up to P. The snapshot holds  *)
(* P, the tables as of P, and the floor and headers the member has applied *)
(* (the carried floor, above P). The tables (producers, groups, cut        *)
(* batches) are a fold: the number of records, the number of trims that    *)
(* raised the floor, and the floor as of the index. Like the real tables,  *)
(* the fold is wrong when an entry is applied twice or skipped, and it     *)
(* reads the floor, so a snapshot that mixed P with the carried floor      *)
(* would be caught.                                                        *)
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
(* Checked:                                                               *)
(*                                                                         *)
(*   NoReadBelowFloor  -- a leader serving reads (it has applied every     *)
(*                        committed entry) serves no record below the      *)
(*                        highest floor any live member ever had.          *)
(*   NoAckedLoss       -- every member holds every committed record at or  *)
(*                        above the committed floor, up to the end of its  *)
(*                        log. No purge and no install removes one.        *)
(*   PurgeBelowFloor   -- a member that purged anything purged below its   *)
(*                        floor.                                           *)
(*   AppliedCoversPurge -- a member's state covers every entry it dropped, *)
(*                        across restarts.                                 *)
(*   TablesAgree       -- every member's tables are the fold of the log    *)
(*                        up to the entry it has applied, across purges,   *)
(*                        installs and restarts.                           *)
(*   HeadersAgree      -- a member with a floor answers with the headers   *)
(*                        of a trim that named that floor.                 *)
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
(*   InstallFloor         -- what an install does to the member's durable  *)
(*                        floor. "replace" stores the leader's snapshot as *)
(*                        it is, so a snapshot carrying a lower floor than *)
(*                        the member's lowers it at the next restart (a    *)
(*                        plain Raft install replaces the whole state      *)
(*                        machine, so it lowers it at once). "max" stores  *)
(*                        the higher of the two floors with that floor's   *)
(*                        headers. "refuse" refuses a snapshot carrying a  *)
(*                        floor below the member's.                        *)
(*   LeaderCaps = FALSE   -- the leader purges past a member's match.      *)
(*   SnapAtApplied = TRUE -- the snapshot is taken at the applied entry    *)
(*                        and the purge goes below it, as a plain Raft     *)
(*                        snapshot would be.                               *)
(*   TablesAtApplied = TRUE -- the snapshot is at the purge point but its  *)
(*                        tables are the member's current ones.            *)
(*   SnapFloorAtP = FALSE -- the snapshot holds one floor, the carried     *)
(*                        one, and the tables fold from it: they lose the  *)
(*                        floor as of P.                                   *)
(*   SnapCarriesHeaders = FALSE -- the snapshot carries the floor but not  *)
(*                        the headers of the trim that set it.             *)
(*   DropFirst = TRUE     -- a purge drops the log before the snapshot is  *)
(*                        durable.                                         *)
(*   ReformCarriesFloor = FALSE -- a re-form keeps each copy's own floor,  *)
(*                        not the highest floor it read.                   *)
(*   ReformStoresFloor = FALSE -- the re-form raises the floor in memory   *)
(*                        only, so a restart loses it.                     *)
(*   ReformReads          -- what a re-form reads of a copy: "full" is its *)
(*                        last entry, purged id and floor; "purged"        *)
(*                        leaves the floor out; "logged" reads a floor     *)
(*                        only from a copy that installed or whose log     *)
(*                        reaches it, not one a re-form carried in;        *)
(*                        "entries" reads the last                         *)
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
    InstallFloor,
    LeaderCaps,
    SnapAtApplied,
    TablesAtApplied,
    SnapFloorAtP,
    SnapCarriesHeaders,
    DropFirst,
    ReformReads,
    ReformCarriesFloor,
    ReformStoresFloor

ASSUME
    /\ Voters \subseteq Members /\ Voters # {}
    /\ MaxIndex \in Nat /\ MaxTrims \in Nat /\ MaxChanges \in Nat
    /\ MaxRestarts \in Nat /\ MaxReforms \in Nat
    /\ PurgeBelowFloorRule \in BOOLEAN /\ InstallRule \in BOOLEAN
    /\ LeaderCaps \in BOOLEAN /\ SnapAtApplied \in BOOLEAN
    /\ TablesAtApplied \in BOOLEAN /\ SnapFloorAtP \in BOOLEAN
    /\ SnapCarriesHeaders \in BOOLEAN /\ DropFirst \in BOOLEAN
    /\ ReformCarriesFloor \in BOOLEAN /\ ReformStoresFloor \in BOOLEAN
    /\ ReformReads \in {"full", "purged", "logged", "entries"}
    /\ InstallFloor \in {"replace", "max", "refuse"}

VARIABLES
    log,      \* every entry of the current epoch's history, in order
    commit,
    leader,
    live,     \* members not declared dead at a re-form
    len,      \* per member: it holds entries purged+1 .. len
    purged,
    pend,     \* per member: a purge point between its two steps, else 0
    applied,
    floor,    \* per member: the highest floor it has applied, installed or
              \* been given by a re-form; reads below it are refused
    hdr,      \* per member: the trim whose headers it answers with, else 0
    tbl,      \* per member: its tables, as of applied
    snap,     \* per member: its durable stream snapshot
    cpFloor,  \* the control plane's offset floor: a commit a leader reported
    reused,   \* history: a re-form gave out an offset a read copy vouched for
    installed, \* history: the members that installed a snapshot
    given,    \* history, per member: the highest committed offset it held or
              \* installed a snapshot or floor for
    hiFloor,  \* history, per member: the highest floor it ever had
    trimF,    \* history: trimF[h] is the floor trim h named
    post,     \* history: "write" and "purge" once one happened after a re-form;
              \* with two re-forms allowed, also "writeMid", "trimMid" and
              \* "raiseMid" for a write, a proposed trim and an applied floor
              \* raise between the first re-form and the second
    trims, changes, restarts, reforms

vars == <<log, commit, leader, live, len, purged, pend, applied, floor, hdr,
          tbl, snap, cpFloor, reused, installed, given, hiFloor, trimF, post,
          trims, changes, restarts, reforms>>

Symm == Permutations(Voters) \cup Permutations(Members \ Voters)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

Rec == [kind |-> "rec", f |-> 0, h |-> 0]
Trim(f, h) == [kind |-> "trim", f |-> f, h |-> h]
Gap == [kind |-> "gap", f |-> 0, h |-> 0]

\* Records in log[lo..hi].
Recs(lo, hi) == Cardinality({i \in lo..hi : log[i].kind = "rec"})

\* The floor of the trims in log[1..hi].
FloorUpTo(hi) == Max({1} \cup {log[i].f : i \in {j \in 1..hi : log[j].kind = "trim"}})

\* The tables: records applied, trims that raised the floor, and the floor.
\* cur is the floor a step compares a trim against.
Tbl0 == [n |-> 0, r |-> 0, f |-> 1]
Step(t, e, cur) ==
    CASE e.kind = "rec" -> [t EXCEPT !.n = @ + 1]
      [] e.kind = "trim" /\ e.f > cur -> [t EXCEPT !.r = @ + 1, !.f = e.f]
      [] OTHER -> t

RECURSIVE FoldFrom(_, _, _)
FoldFrom(t, lo, hi) ==
    IF lo > hi THEN t ELSE FoldFrom(Step(t, log[lo], t.f), lo + 1, hi)

Fold(hi) == FoldFrom(Tbl0, 1, hi)

\* Every floor a live member ever had came from a committed trim. A restart
\* that loses a floor does not lower this.
CommittedFloor == Max({FloorUpTo(commit)} \cup {hiFloor[m] : m \in live})

RaiseHiFloor == hiFloor' = [k \in Members |-> Max({hiFloor[k], floor'[k]})]

\* A snapshot: its index, the tables as of it, and the carried floor and
\* headers, which are the member's at the purge.
NoSnap == [idx |-> 0, tbl |-> Tbl0, floor |-> 1, hdr |-> 0]

----------------------------------------------------------------------------

Init ==
    /\ log = <<>>
    /\ commit = 0
    /\ leader \in Voters
    /\ live = Members
    /\ len = [m \in Members |-> 0]
    /\ purged = [m \in Members |-> 0]
    /\ pend = [m \in Members |-> 0]
    /\ applied = [m \in Members |-> 0]
    /\ floor = [m \in Members |-> 1]
    /\ hdr = [m \in Members |-> 0]
    /\ tbl = [m \in Members |-> Tbl0]
    /\ snap = [m \in Members |-> NoSnap]
    /\ cpFloor = 0
    /\ reused = FALSE
    /\ installed = {}
    /\ given = [m \in Members |-> 0]
    /\ hiFloor = [m \in Members |-> 1]
    /\ trimF = <<>>
    /\ post = {}
    /\ trims = 0 /\ changes = 0 /\ restarts = 0 /\ reforms = 0

AppendEntry(e) ==
    /\ Len(log) < MaxIndex
    /\ log' = Append(log, e)
    /\ len' = [len EXCEPT ![leader] = Len(log) + 1]

\* A tag for the history between the first and the second re-form. It exists
\* only when a second re-form is allowed, so a one-re-form model's states do
\* not change.
Mid(t) == IF reforms = 1 /\ MaxReforms > 1 THEN {t} ELSE {}

\* A client write.
Write ==
    /\ AppendEntry(Rec)
    /\ post' = IF reforms > 0 THEN post \cup {"write"} \cup Mid("writeMid") ELSE post
    /\ UNCHANGED <<commit, leader, live, purged, pend, applied, floor, hdr, tbl,
                   snap, cpFloor, reused, installed, given, hiFloor, trimF,
                   trims, changes, restarts, reforms>>

\* The leader's retention driver proposes a floor above the floor it has
\* applied and at most the trim's own offset (proposed.rs allows a floor
\* equal to the tail the trim lands at).
ProposeTrim(f) ==
    /\ trims < MaxTrims
    /\ f \in (floor[leader] + 1)..(Len(log) + 1)
    /\ AppendEntry(Trim(f, trims + 1))
    /\ trimF' = Append(trimF, f)
    /\ trims' = trims + 1
    /\ post' = post \cup Mid("trimMid")
    /\ UNCHANGED <<commit, leader, live, purged, pend, applied, floor, hdr, tbl,
                   snap, cpFloor, reused, installed, given, hiFloor,
                   changes, restarts, reforms>>

\* The leader sends a follower its next entry, which it still holds.
Replicate(n) ==
    /\ n \in live \ {leader}
    /\ len[n] < Len(log)
    /\ len[n] + 1 > purged[leader]
    /\ len' = [len EXCEPT ![n] = @ + 1]
    /\ given' = [given EXCEPT ![n] = Max({@, Min({len[n] + 1, commit})})]
    /\ UNCHANGED <<log, commit, leader, live, purged, pend, applied, floor, hdr,
                   tbl, snap, cpFloor, reused, installed, hiFloor, trimF, post,
                   trims, changes, restarts, reforms>>

\* The leader has purged the follower's next entry: it sends its snapshot.
\* The member compares against its own log end; with terms, the code must
\* compare against its matching index and drop a divergent tail.
Installable(s, n) ==
    /\ s.idx > len[n]
    /\ InstallRule => s.idx <= s.floor - 1
    /\ InstallFloor = "refuse" => s.floor >= floor[n]

Install(n) ==
    LET s == snap[leader] IN
    /\ n \in live \ {leader}
    /\ len[n] + 1 <= purged[leader]
    /\ Installable(s, n)
    /\ len' = [len EXCEPT ![n] = s.idx]
    /\ purged' = [purged EXCEPT ![n] = s.idx]
    /\ pend' = [pend EXCEPT ![n] = 0]
    /\ applied' = [applied EXCEPT ![n] = s.idx]
    /\ floor' = [floor EXCEPT ![n] = IF s.floor > @ THEN s.floor ELSE @]
    /\ hdr' = [hdr EXCEPT ![n] = IF s.floor > floor[n] THEN s.hdr ELSE @]
    /\ RaiseHiFloor
    /\ given' = [given EXCEPT ![n] = Max({@, s.idx, s.floor})]
    /\ tbl' = [tbl EXCEPT ![n] = s.tbl]
    \* The floor the member vouched for must survive a restart.
    /\ snap' = [snap EXCEPT ![n] =
          IF InstallFloor = "max" THEN [s EXCEPT !.floor = floor'[n], !.hdr = hdr'[n]]
          ELSE s]
    /\ installed' = installed \cup {n}
    /\ UNCHANGED <<log, commit, leader, live, cpFloor, reused, trimF, post,
                   trims, changes, restarts, reforms>>

Quorum(i) ==
    LET V == Voters \cap live IN
    2 * Cardinality({v \in V : len[v] >= i}) > Cardinality(V)

Commit(i) ==
    /\ i \in (commit + 1)..Len(log)
    /\ Quorum(i)
    /\ commit' = i
    /\ given' = [m \in Members |-> Max({given[m], Min({len[m], i})})]
    /\ UNCHANGED <<log, leader, live, len, purged, pend, applied, floor, hdr,
                   tbl, snap, cpFloor, reused, installed, hiFloor, trimF, post,
                   trims, changes, restarts, reforms>>

\* A leader reports its commit to the control plane.
ReportFloor ==
    /\ cpFloor < commit
    /\ cpFloor' = commit
    /\ UNCHANGED <<log, commit, leader, live, len, purged, pend, applied, floor,
                   hdr, tbl, snap, reused, installed, given, hiFloor, trimF,
                   post, trims, changes, restarts, reforms>>

\* The tables fold against their own floor. With SnapFloorAtP off they fold
\* against the member's floor, which an install or restart sets to the
\* carried floor, above the snapshot's index.
Apply(m) ==
    LET i == applied[m] + 1
        e == log[i]
        raise == e.kind = "trim" /\ e.f > floor[m]
    IN
    /\ m \in live
    /\ i <= commit /\ i <= len[m] /\ i > purged[m]
    /\ applied' = [applied EXCEPT ![m] = i]
    /\ floor' = [floor EXCEPT ![m] = IF raise THEN e.f ELSE @]
    /\ hdr' = [hdr EXCEPT ![m] = IF raise THEN e.h ELSE @]
    /\ RaiseHiFloor
    /\ tbl' = [tbl EXCEPT ![m] =
          Step(@, e, IF SnapFloorAtP THEN @.f ELSE floor[m])]
    /\ post' = IF raise THEN post \cup Mid("raiseMid") ELSE post
    /\ UNCHANGED <<log, commit, leader, live, len, purged, pend, snap, cpFloor,
                   reused, installed, given, trimF, trims, changes,
                   restarts, reforms>>

\* Where a member may purge to.
PurgeOK(m, p) ==
    /\ m \in live /\ pend[m] = 0
    /\ p \in (purged[m] + 1)..applied[m]
    /\ PurgeBelowFloorRule => p <= floor[m] - 1
    /\ (LeaderCaps /\ m = leader) => \A n \in live \ {leader} : p <= len[n]

\* The snapshot for a purge to p. Its tables are built from the previous
\* snapshot and the log the member still holds up to p.
SnapAt(m, p) ==
    [idx |-> IF SnapAtApplied THEN applied[m] ELSE p,
     tbl |-> IF SnapAtApplied \/ TablesAtApplied THEN tbl[m]
             ELSE FoldFrom(snap[m].tbl, snap[m].idx + 1, p),
     floor |-> floor[m],
     hdr |-> IF SnapCarriesHeaders THEN hdr[m] ELSE 0]

\* A purge, step one: write and fsync the snapshot. With DropFirst this is
\* step two, after the log is gone.
WriteSnap(m, p) ==
    /\ IF DropFirst THEN m \in live /\ pend[m] # 0 /\ p = pend[m]
                    ELSE PurgeOK(m, p)
    /\ snap' = [snap EXCEPT ![m] = SnapAt(m, p)]
    /\ pend' = [pend EXCEPT ![m] = IF DropFirst THEN 0 ELSE p]
    /\ post' = IF ~DropFirst /\ reforms > 0 THEN post \cup {"purge"} ELSE post
    /\ UNCHANGED <<log, commit, leader, live, len, purged, applied, floor, hdr,
                   tbl, cpFloor, reused, installed, given, hiFloor, trimF,
                   trims, changes, restarts, reforms>>

\* A purge, step two: drop the log up to p.
DropLog(m, p) ==
    /\ IF DropFirst THEN PurgeOK(m, p)
                    ELSE m \in live /\ pend[m] # 0 /\ p = pend[m]
    /\ purged' = [purged EXCEPT ![m] = p]
    /\ pend' = [pend EXCEPT ![m] = IF DropFirst THEN p ELSE 0]
    /\ post' = IF DropFirst /\ reforms > 0 THEN post \cup {"purge"} ELSE post
    /\ UNCHANGED <<log, commit, leader, live, len, applied, floor, hdr, tbl,
                   snap, cpFloor, reused, installed, given, hiFloor, trimF,
                   trims, changes, restarts, reforms>>

\* A member restarts: its state machine is its snapshot again, and it
\* applies its log from there. A durable snapshot above the log's start
\* tells it to finish the drop; a snapshot not yet written is lost.
Restart(m) ==
    /\ restarts < MaxRestarts
    /\ m \in live
    /\ applied' = [applied EXCEPT ![m] = snap[m].idx]
    /\ floor' = [floor EXCEPT ![m] = snap[m].floor]
    /\ hdr' = [hdr EXCEPT ![m] = snap[m].hdr]
    /\ tbl' = [tbl EXCEPT ![m] = snap[m].tbl]
    /\ pend' = [pend EXCEPT ![m] = IF DropFirst THEN 0 ELSE @]
    /\ restarts' = restarts + 1
    /\ UNCHANGED <<log, commit, leader, live, len, purged, snap, cpFloor, reused,
                   installed, given, hiFloor, trimF, post, trims, changes,
                   reforms>>

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
    /\ UNCHANGED <<commit, live, purged, pend, applied, floor, hdr, tbl, snap,
                   cpFloor, reused, installed, given, hiFloor, trimF, post,
                   trims, restarts, reforms>>

\* What a re-form reads as the last offset a copy vouches for. A floor f
\* vouches for f itself: the trim that named it is at offset f or above,
\* and it was committed.
ReadLast(m) ==
    CASE ReformReads = "full" ->
           Max({len[m], purged[m], IF floor[m] > 1 THEN floor[m] ELSE 0})
      [] ReformReads = "purged" ->
           Max({len[m], purged[m]})
      [] ReformReads = "logged" ->
           Max({len[m], purged[m],
                IF floor[m] > 1 /\ (m \in installed \/ len[m] >= floor[m])
                THEN floor[m] ELSE 0})
      [] ReformReads = "entries" ->
           IF len[m] > purged[m] THEN len[m] ELSE 0

\* The operator re-forms the group from the copies in R, led by l, a read
\* voter with the longest log; Voters stay as they were, so a read learner
\* whose log is longer than every read voter's blocks the re-form. The
\* re-form writes l the gap up to the base. The trim that set a
\* read copy's floor may be in no copy's log, so the new epoch starts at
\* the highest floor read, with its headers.
Reform(R, l) ==
    LET b == Max({cpFloor} \cup {ReadLast(m) : m \in R})
        F == Max({floor[k] : k \in R})
        H == Max({hdr[k] : k \in {j \in R : floor[j] = F}})
        keep == Min({len[l], b})
        nf(m) == IF ReformCarriesFloor /\ m \in R THEN F ELSE floor[m]
        nh(m) == IF ReformCarriesFloor /\ m \in R THEN H ELSE hdr[m]
    IN
    /\ reforms < MaxReforms
    /\ R # {} /\ R \subseteq live
    /\ l \in R \cap Voters
    /\ \A k \in R : len[k] <= len[l]
    /\ log' = SubSeq(log, 1, keep) \o [i \in 1..(b - keep) |-> Gap]
    /\ reused' = (reused \/ \E m \in R : given[m] > b)
    /\ given' = [m \in Members |->
          IF m \in R /\ nf(m) > 1 THEN Max({Min({given[m], b}), nf(m)})
          ELSE Min({given[m], b})]
    /\ live' = R
    /\ leader' = l
    /\ commit' = b
    /\ len' = [m \in Members |-> IF m = l THEN b ELSE Min({len[m], b})]
    /\ purged' = [m \in Members |-> Min({purged[m], b})]
    /\ applied' = [m \in Members |-> Min({applied[m], b})]
    /\ floor' = [m \in Members |-> nf(m)]
    /\ hdr' = [m \in Members |-> nh(m)]
    /\ RaiseHiFloor
    \* The re-form writes each read copy's snapshot with the floor it starts
    \* at, so a restart keeps it.
    /\ snap' = IF ReformCarriesFloor /\ ReformStoresFloor
              THEN [m \in Members |-> IF m \in R
                      THEN [snap[m] EXCEPT !.floor = F,
                                           !.hdr = IF SnapCarriesHeaders THEN H ELSE 0]
                      ELSE snap[m]]
              ELSE snap
    /\ reforms' = reforms + 1
    /\ UNCHANGED <<pend, tbl, cpFloor, installed, trimF, post, trims, changes,
                   restarts>>

Next ==
    \/ Write
    \/ \E f \in 1..MaxIndex : ProposeTrim(f)
    \/ \E n \in Members : Replicate(n) \/ Install(n)
    \/ \E i \in 1..MaxIndex : Commit(i)
    \/ ReportFloor
    \/ \E m \in Members : Apply(m) \/ Restart(m)
    \/ \E m \in Members, p \in 1..MaxIndex : WriteSnap(m, p) \/ DropLog(m, p)
    \/ \E n \in Members : ChangeLeader(n)
    \/ \E R \in SUBSET Members, l \in Members : Reform(R, l)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Entries == {Rec, Gap} \cup {Trim(f, h) : f \in 1..MaxIndex, h \in 1..MaxTrims}

TypeOK ==
    /\ Len(log) <= MaxIndex
    /\ \A i \in 1..Len(log) : log[i] \in Entries
    /\ commit \in 0..Len(log)
    /\ leader \in live
    /\ live \subseteq Members
    /\ \A m \in live : applied[m] <= len[m]
    /\ \A m \in live : len[m] <= Len(log) /\ applied[m] <= commit
    /\ len[leader] = Len(log)
    /\ Len(trimF) = trims

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

AppliedCoversPurge == \A m \in live : purged[m] <= applied[m]

TablesAgree == \A m \in live : tbl[m] = Fold(applied[m])

HeadersAgree ==
    \A m \in live :
        floor[m] > 1 => hdr[m] \in 1..trims /\ trimF[hdr[m]] = floor[m]

OffsetNeverReused == ~reused

\* Steers FelixRaftStreamPurgeInstallLowersFloor.cfg to the floor drop: trims
\* naming 3 then 4, and no floor reported to the control plane.
FloorDropPath == trimF \in {<<>>, <<3>>, <<3, 4>>} /\ cpFloor = 0

CatchUpPossible ==
    \A n \in live \ {leader} :
        len[n] + 1 <= purged[leader] => Installable(snap[leader], n)

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

\* A member that installed a snapshot serves as leader.
NeverServesAfterInstall == \A m \in installed : ~Serving(m)

\* A member frees disk: it purges past a record.
NeverPurgesARecord == \A m \in live : Recs(1, purged[m]) = 0

\* A member purges past a trim entry.
NeverPurgesATrim ==
    \A m \in live : \A i \in 1..purged[m] : log[i].kind # "trim"

\* A write, and a purge, after a re-form.
NeverWritesAfterReform == "write" \notin post
NeverPurgesAfterReform == "purge" \notin post

\* Between two re-forms: a write and a trim, a floor raised above the one the
\* first re-form carried, and a trim entry purged.
NeverWriteAndTrimBetweenReforms ==
    ~(reforms = 2 /\ {"writeMid", "trimMid"} \subseteq post)
NeverRaiseBetweenReforms ==
    ~(reforms = 2 /\ "raiseMid" \in post)
NeverPurgesATrimAfterTwoReforms ==
    ~(reforms = 2 /\ \E m \in live : \E i \in 1..purged[m] : log[i].kind = "trim")

\* A learner installs a snapshot and a re-form follows.
NeverInstallsThenReforms == ~(reforms = 1 /\ installed # {})

=============================================================================
