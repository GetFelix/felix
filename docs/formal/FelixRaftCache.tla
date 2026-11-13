---------------------------- MODULE FelixRaftCache ----------------------------
(***************************************************************************)
(* The state machine of a cache shard's Raft group, as the second design   *)
(* on issue 2238 has it: one log holds cache puts and counter adds, every  *)
(* member applies it to an in-memory projection, and each member takes its *)
(* own snapshot of that projection and purges its log behind it. Nothing   *)
(* here is built.                                                          *)
(*                                                                         *)
(* Raft itself is abstracted to what FelixRaft.tla already checks: one     *)
(* committed sequence (`clog`) that only grows, a leader that proposes     *)
(* entries (`pend`), and a leader change that drops what it had not        *)
(* committed. FelixRaft.tla's snapshot configs check the log side of a     *)
(* snapshot, install and purge. This module checks the projection side:    *)
(*                                                                         *)
(*   - Apply is deterministic. A conditional put checks expiry against     *)
(*     the entry's time, stamped by the leader, never a member's clock.    *)
(*   - The leader stamps an entry no earlier than the entry before it, so  *)
(*     the log's time only moves forward across leaders.                  *)
(*   - A read takes the read-index path: the leader waits until it has    *)
(*     applied its commit index, and reads expiry at the log's time there. *)
(*   - A counter add carries a request id, and the projection keeps the    *)
(*     ids it has applied, so a retry after an unknown outcome is applied  *)
(*     once. The snapshot holds those ids.                                 *)
(*   - A snapshot is the projection and the id of the last entry it        *)
(*     applied, taken together. Restart and install both resume from it.  *)
(*                                                                         *)
(* One key and one counter. Three requests: an add (id 1), a put that      *)
(* expires one time unit after its stamp (id 2), and a put-if-absent (id   *)
(* 3), which is refused while the key holds an unexpired value. A request  *)
(* whose answer was lost may be proposed again: that is a retry.           *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   MembersAgree     -- two members that applied the same prefix hold the *)
(*                       same projection.                                  *)
(*   ProjectionIsFold -- every member holds the fold of the committed log  *)
(*                       up to what it applied.                            *)
(*   SumExact         -- a counter counts each request once.               *)
(*   ReadsFresh       -- a read answers what the committed log said when   *)
(*                       the read began.                                   *)
(*   OneClock         -- a read that found the key absent is never         *)
(*                       followed, with no write between, by a             *)
(*                       put-if-absent that finds it present.              *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   NoRequestId        -- an add carries no request id.                   *)
(*   SnapshotDropsDedup -- the snapshot leaves out the applied request ids.*)
(*   SnapshotIdAhead    -- the snapshot's id is read after its state, so   *)
(*                         it can name an entry the state does not hold.   *)
(*   ApplyLocalClock    -- apply checks expiry on the member's own clock.  *)
(*   ReadsLocalClock    -- a read checks expiry on the leader's own clock. *)
(*   StampNotMonotone   -- the leader stamps its own clock, even if it is  *)
(*                         behind the entry before.                        *)
(*   ReadNoApplyWait    -- a read answers from what the leader has applied *)
(*                         without waiting for its commit index.           *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    MaxProposals,       \* entries proposed, retries included
    MaxClock,           \* every clock runs 0..MaxClock
    MaxLeaderChanges,
    MaxCrashes,
    MaxReads,
    MaxSnapshots,       \* snapshots built, across all members
    NoRequestId,
    SnapshotDropsDedup,
    SnapshotIdAhead,
    ApplyLocalClock,
    ReadsLocalClock,
    StampNotMonotone,
    ReadNoApplyWait

VARIABLES
    clog,       \* the committed entries
    pend,       \* the leader's proposed, uncommitted entries
    ldr,
    clock,      \* each broker's wall clock
    applied,    \* per member: the last entry applied
    st,         \* per member: the projection
    snap,       \* per member: [idx, st], its stored snapshot
    purged,     \* per member: its log is purged up to here
    answered,   \* requests whose answer reached the client
    reads,      \* history: every read served
    used,
    hist

vars == <<clog, pend, ldr, clock, applied, st, snap, purged, answered, reads, used, hist>>

Symm == Permutations(Servers)

Max2(a, b) == IF a >= b THEN a ELSE b

Requests == {1, 2, 3}
Op(r) == CASE r = 1 -> "add" [] r = 2 -> "put" [] r = 3 -> "putAbsent"

Empty == [val |-> 0, ver |-> 0, exp |-> 0, sum |-> 0, seen |-> {}]

\* Present and not expired at time t. exp 0 is "never".
Live(s, t) == s.val # 0 /\ (s.exp = 0 \/ t < s.exp)

\* Apply entry e at index i, checking expiry at time t.
Step(s, e, i, t) ==
    CASE e.op = "add" ->
            IF ~NoRequestId /\ e.rid \in s.seen THEN s
            ELSE [s EXCEPT !.sum = @ + 1, !.seen = @ \cup {e.rid}]
      [] e.op = "put" ->
            [s EXCEPT !.val = e.rid, !.ver = i, !.exp = e.exp]
      [] e.op = "putAbsent" ->
            IF Live(s, t) THEN s
            ELSE [s EXCEPT !.val = e.rid, !.ver = i, !.exp = 0]

\* The fold of the committed log, always at the entries' own times.
RECURSIVE FoldTo(_)
FoldTo(n) == IF n = 0 THEN Empty ELSE Step(FoldTo(n - 1), clog[n], n, clog[n].t)

\* The log's time at index n: the stamp of entry n.
LogT(n) == IF n = 0 THEN 0 ELSE clog[n].t

All == clog \o pend
LastT == IF All = <<>> THEN 0 ELSE All[Len(All)].t

Init ==
    /\ clog = <<>>
    /\ pend = <<>>
    /\ ldr \in Servers
    /\ clock = [s \in Servers |-> 0]
    /\ applied = [s \in Servers |-> 0]
    /\ st = [s \in Servers |-> Empty]
    /\ snap = [s \in Servers |-> [idx |-> 0, st |-> Empty]]
    /\ purged = [s \in Servers |-> 0]
    /\ answered = {}
    /\ reads = {}
    /\ used = [props |-> 0, changes |-> 0, crashes |-> 0, reads |-> 0, snaps |-> 0]
    /\ hist = [installed |-> {}, readAfterInstall |-> FALSE]

----------------------------------------------------------------------------

\* A client request, or a retry of one whose answer never came.
Propose(r) ==
    LET t == IF StampNotMonotone THEN clock[ldr] ELSE Max2(clock[ldr], LastT)
    IN /\ r \notin answered
       /\ used.props < MaxProposals
       /\ pend' = Append(pend, [rid |-> r, op |-> Op(r), t |-> t, exp |-> t + 1])
       /\ used' = [used EXCEPT !.props = @ + 1]
       /\ UNCHANGED <<clog, ldr, clock, applied, st, snap, purged, answered, reads, hist>>

\* The first pending entry commits. Its answer can be lost.
Commit(lost) ==
    /\ pend # <<>>
    /\ clog' = Append(clog, Head(pend))
    /\ pend' = Tail(pend)
    /\ answered' = IF lost THEN answered ELSE answered \cup {Head(pend).rid}
    /\ UNCHANGED <<ldr, clock, applied, st, snap, purged, reads, used, hist>>

\* A new leader. It holds every committed entry and none of the old
\* leader's uncommitted ones.
ChangeLeader(n) ==
    /\ n # ldr
    /\ used.changes < MaxLeaderChanges
    /\ ldr' = n
    /\ pend' = <<>>
    /\ used' = [used EXCEPT !.changes = @ + 1]
    /\ UNCHANGED <<clog, clock, applied, st, snap, purged, answered, reads, hist>>

Tick(s) ==
    /\ clock[s] < MaxClock
    /\ clock' = [clock EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<clog, pend, ldr, applied, st, snap, purged, answered, reads, used, hist>>

Apply(s) ==
    LET i == applied[s] + 1
    IN /\ i <= Len(clog)
       /\ st' = [st EXCEPT ![s] =
                   Step(@, clog[i], i, IF ApplyLocalClock THEN clock[s] ELSE clog[i].t)]
       /\ applied' = [applied EXCEPT ![s] = i]
       /\ UNCHANGED <<clog, pend, ldr, clock, snap, purged, answered, reads, used, hist>>

\* The projection and the id of the last entry it applied, together.
BuildSnapshot(s) ==
    /\ applied[s] > snap[s].idx
    /\ used.snaps < MaxSnapshots
    /\ used' = [used EXCEPT !.snaps = @ + 1]
    /\ snap' = [snap EXCEPT ![s] =
                  [idx |-> IF SnapshotIdAhead THEN Len(clog) ELSE applied[s],
                   st |-> IF SnapshotDropsDedup THEN [st[s] EXCEPT !.seen = {}]
                          ELSE st[s]]]
    /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, purged, answered, reads, hist>>

\* Only behind the stored snapshot (FelixRaft.tla's PurgeBeforeSnapshot
\* checks the other order).
Purge(s) ==
    /\ purged[s] < snap[s].idx
    /\ purged' = [purged EXCEPT ![s] = snap[s].idx]
    /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, snap, answered, reads, used, hist>>

\* A restart resumes from the snapshot and applies the log after it again.
Crash(s) ==
    /\ used.crashes < MaxCrashes
    /\ st' = [st EXCEPT ![s] = snap[s].st]
    /\ applied' = [applied EXCEPT ![s] = snap[s].idx]
    /\ pend' = IF s = ldr THEN <<>> ELSE pend
    /\ used' = [used EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<clog, ldr, clock, snap, purged, answered, reads, hist>>

\* A member behind the leader's purge takes the leader's snapshot.
Install(s) ==
    /\ s # ldr
    /\ purged[ldr] > applied[s]
    /\ st' = [st EXCEPT ![s] = snap[ldr].st]
    /\ applied' = [applied EXCEPT ![s] = snap[ldr].idx]
    /\ snap' = [snap EXCEPT ![s] = snap[ldr]]
    /\ purged' = [purged EXCEPT ![s] = snap[ldr].idx]
    /\ hist' = [hist EXCEPT !.installed = @ \cup {s}]
    /\ UNCHANGED <<clog, pend, ldr, clock, answered, reads, used>>

\* A read-index read on the leader.
Read ==
    LET c == Len(clog)
        a == applied[ldr]
    IN /\ used.reads < MaxReads
       /\ ReadNoApplyWait \/ a = c
       /\ reads' = reads \cup {[c |-> c,
                                present |-> Live(st[ldr],
                                   IF ReadsLocalClock THEN clock[ldr] ELSE LogT(a))]}
       /\ used' = [used EXCEPT !.reads = @ + 1]
       /\ hist' = [hist EXCEPT !.readAfterInstall = @ \/ ldr \in hist.installed]
       /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, snap, purged, answered>>

Next ==
    \/ \E r \in Requests : Propose(r)
    \/ \E lost \in BOOLEAN : Commit(lost)
    \/ Read
    \/ \E s \in Servers :
        \/ ChangeLeader(s) \/ Tick(s) \/ Apply(s) \/ BuildSnapshot(s)
        \/ Purge(s) \/ Crash(s) \/ Install(s)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ ldr \in Servers
    /\ \A s \in Servers :
        /\ applied[s] \in 0..Len(clog)
        /\ purged[s] <= snap[s].idx
    /\ answered \subseteq Requests

MembersAgree ==
    \A s, t \in Servers : applied[s] = applied[t] => st[s] = st[t]

ProjectionIsFold == \A s \in Servers : st[s] = FoldTo(applied[s])

SumExact ==
    \A s \in Servers :
        st[s].sum = Cardinality({clog[i].rid : i \in {j \in 1..applied[s] : clog[j].op = "add"}})

ReadsFresh == \A r \in reads : r.present = Live(FoldTo(r.c), LogT(r.c))

\* Nothing wrote the key between index a and index b.
Untouched(a, b) == FoldTo(a).ver = FoldTo(b).ver

OneClock ==
    \A r \in reads : \A j \in 1..Len(clog) :
        (/\ ~r.present
         /\ j > r.c
         /\ clog[j].op = "putAbsent"
         /\ Untouched(r.c, j - 1))
        => ~Live(FoldTo(j - 1), clog[j].t)

\* Witnesses. Each says something never happens and must be violated.

\* A member that installed a snapshot leads and serves a read.
NeverReadAfterInstall == ~hist.readAfterInstall

\* A retried add is committed twice and counted once.
NeverRetryApplied ==
    ~ \E s \in Servers :
        Cardinality({i \in 1..applied[s] : clog[i].op = "add"}) > 1

\* A read finds the expired key absent and a later put-if-absent applies.
NeverExpiredThenTaken ==
    ~ \E r \in reads : \E j \in 1..Len(clog) :
        /\ ~r.present /\ j > r.c /\ clog[j].op = "putAbsent"
        /\ FoldTo(r.c).val = 2
        /\ FoldTo(j).val = 3

=============================================================================
