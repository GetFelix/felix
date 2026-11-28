----------------------- MODULE FelixRaftRestoreRepeated -----------------------
(***************************************************************************)
(* Repeated restores of one shard from backup points, as                  *)
(* docs/per-shard-raft.md decides the first index ("A restore from a       *)
(* backup point is a re-form").                                           *)
(*                                                                         *)
(* The shard's running epoch gives out offsets next, next + 1, ... at most *)
(* Rate per tick of true time (Issue). A backup (Backup) holds every       *)
(* offset below the running epoch's next one. A restore (Restore) replaces *)
(* the running epoch with one started from a backup: the control-plane     *)
(* node that plans it reads its own clock, which is off true time by at    *)
(* most Skew, and the new epoch starts at                                  *)
(*                                                                         *)
(*   max(end, TimeRate * clock + Gap)          (Line)                      *)
(*   max(end + Gap)                            (EndGap, no Line)           *)
(*   max(end + Gap, TimeRate * clock + Gap)    (both)                      *)
(*                                                                         *)
(* where end is the backup's next offset. The restored floor, reservation  *)
(* and earlier bases are all offsets given out before the backup, so end   *)
(* stands for them. Epoch 0 starts at time 0 at offset Gap + 1, on the     *)
(* line. Restores are at least MinApart ticks apart in true time.          *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   OffsetNeverReused -- no offset is given out twice, across every       *)
(*                        epoch and every restore, from any backup.        *)
(*                                                                         *)
(* The code's TimeRate is 2^14 offsets per millisecond, and a shard is     *)
(* taken to give out at most half that. With Rate = TimeRate / 2 and skew  *)
(* S, two restores need about 4 S of true time between them.               *)
(*                                                                         *)
(* With Batch, the shard gives out batch entries, as                       *)
(* docs/raft-batch-entries-design.md decides them, and Rate is enforced    *)
(* instead of assumed:                                                     *)
(*                                                                         *)
(*   IssueBatch -- one entry of 1 to MaxBatch offsets, proposed when the   *)
(*                 leader's token bucket is positive. It takes its count   *)
(*                 and may leave the bucket in debt by up to one entry.    *)
(*                 The bucket fills at Rate per tick up to Capacity. A new *)
(*                 leader's starts in debt by one maximum entry, MaxBatch  *)
(*                 (the first leader's starts full, which only lets the    *)
(*                 shard give out more).                                   *)
(*   Raise      -- the entry is counted before it is synced (relaxed       *)
(*                 fsync), so an entry past the reservation waits for a    *)
(*                 raise that is synced first: it covers the entry and     *)
(*                 Step offsets past its end.                              *)
(*   PowerCut   -- a power cut takes what was not synced, and the shard    *)
(*                 re-forms at the higher of its synced end and its        *)
(*                 reservation, under a new leader.                        *)
(*   Elect      -- a leader change, with nothing lost.                     *)
(*                                                                         *)
(* With Apart above 0 a restore is also refused unless its first index is  *)
(* at least Apart above every earlier epoch's (docs/per-shard-raft.md: the  *)
(* 2^32 + (2^40 - gap) check, where every restore here has the same gap).  *)
(* The floor is left out of that check: it trails the running epoch.       *)
(*                                                                         *)
(* Checked as well:                                                        *)
(*                                                                         *)
(*   ReservedCovers -- every offset given out is below the synced          *)
(*                     reservation, so a power cut never gives it out      *)
(*                     again.                                              *)
(*                                                                         *)
(* Each knob removes one rule: NoBucket caps entries per tick (Rate of     *)
(* them) and not offsets; BucketStartsFull gives each new leader a full    *)
(* bucket and BucketStartsEmpty an empty one, not one in debt;             *)
(* RaiseFromNext raises the reservation Step past the entry's start        *)
(* instead of its end; IssueBeforeRaise counts an entry before the raise   *)
(* that covers it.                                                         *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Rate,         \* offsets given out per tick at most
    TimeRate,     \* the time line's offsets per tick
    Gap,          \* the restore offset gap
    Skew,         \* how far a planning node's clock is off true time
    MinApart,     \* ticks between two restores at least
    MaxTime,      \* ticks at most
    MaxBackups,   \* backups at most
    MaxRestores,  \* restores at most
    Line,         \* the first index takes the time line
    EndGap,       \* the first index takes the backup's end plus the gap
    Batch,        \* batch entries, a token bucket and relaxed fsync
    MaxBatch,     \* offsets in one entry at most
    Capacity,     \* the token bucket's capacity
    Step,         \* how far past an entry's end a reservation raise goes
    MaxCuts,      \* power cuts at most
    MaxLeaders,   \* leader changes at most
    NoBucket,
    BucketStartsFull,
    RaiseFromNext,
    BucketStartsEmpty,
    IssueBeforeRaise,
    Apart         \* how far above every earlier first index a restore starts (0: unchecked)

ASSUME Rate >= 1 /\ TimeRate >= 0 /\ Gap \in Nat /\ Skew \in Nat
ASSUME MaxBatch >= 1 /\ Capacity >= 1 /\ Step \in Nat /\ Apart \in Nat

VARIABLES
    now,         \* true time
    burst,       \* offsets given out in this tick
    next,        \* the running epoch's next offset
    issued,      \* every offset given out so far
    backups,     \* each backup's end
    restores,    \* restores so far
    lastRestore, \* true time of the last restore (epoch 0 counts, at 0)
    reused,      \* some offset was given out twice
    bucket,      \* the leader's token bucket (Batch)
    reserved,    \* the synced reservation: offsets below it are covered (Batch)
    synced,      \* the running epoch's next offset on disk (Batch)
    cuts,        \* power cuts so far
    leaders,     \* leader changes so far
    raisedFor,   \* the size of the entry a synced raise was made for, or 0 (Batch)
    topFirst     \* the highest first index of any epoch so far (Apart)

batchVars == <<bucket, reserved, synced, cuts, leaders, raisedFor>>
vars == <<now, burst, next, issued, backups, restores, lastRestore, reused, batchVars, topFirst>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min2(a, b) == IF a <= b THEN a ELSE b
Max2(a, b) == IF a >= b THEN a ELSE b

\* A new leader's bucket: in debt by one maximum entry, so that a leader
\* change never adds to the burst the old leader's debt took.
FreshBucket ==
    CASE BucketStartsFull -> Capacity
      [] BucketStartsEmpty -> 0
      [] OTHER -> -MaxBatch

Init ==
    /\ now = 0
    /\ burst = 0
    /\ next = Gap + 1
    /\ issued = {}
    /\ backups = {}
    /\ restores = 0
    /\ lastRestore = 0
    /\ reused = FALSE
    /\ bucket = Capacity
    /\ reserved = Gap + 1
    /\ synced = Gap + 1
    /\ cuts = 0
    /\ leaders = 0
    /\ raisedFor = 0
    /\ topFirst = Gap + 1

Issue ==
    /\ ~Batch
    /\ burst < Rate
    /\ reused' = (reused \/ next \in issued)
    /\ issued' = issued \cup {next}
    /\ next' = next + 1
    /\ burst' = burst + 1
    /\ UNCHANGED <<now, backups, restores, lastRestore, batchVars, topFirst>>

CanPropose == IF NoBucket THEN burst < Rate ELSE bucket > 0

\* The synced raise an entry of n offsets past the reservation waits for.
Raise(n) ==
    /\ Batch
    /\ CanPropose
    /\ raisedFor = 0
    /\ next + n > reserved
    /\ reserved' = (IF RaiseFromNext THEN next ELSE next + n) + Step
    /\ raisedFor' = n
    /\ UNCHANGED <<now, burst, next, issued, backups, restores, lastRestore, reused,
                   bucket, synced, cuts, leaders, topFirst>>

\* One entry of n offsets, counted once its reservation is synced.
IssueBatch(n) ==
    /\ Batch
    /\ CanPropose
    /\ next + n <= reserved \/ raisedFor = n \/ IssueBeforeRaise
    /\ burst' = IF NoBucket THEN burst + 1 ELSE burst
    /\ bucket' = IF NoBucket THEN bucket ELSE bucket - n
    /\ LET range == next..(next + n - 1)
       IN /\ reused' = (reused \/ range \cap issued # {})
          /\ issued' = issued \cup range
    /\ next' = next + n
    /\ raisedFor' = 0
    /\ UNCHANGED <<now, backups, restores, lastRestore, reserved, synced, cuts, leaders, topFirst>>

Sync ==
    /\ Batch
    /\ synced < next
    /\ synced' = next
    /\ UNCHANGED <<now, burst, next, issued, backups, restores, lastRestore, reused,
                   bucket, reserved, cuts, leaders, raisedFor, topFirst>>

\* The re-form after a power cut starts at its kept end or its reservation.
PowerCut ==
    /\ Batch
    /\ cuts < MaxCuts
    /\ cuts' = cuts + 1
    /\ next' = Max2(synced, reserved)
    /\ synced' = Max2(synced, reserved)
    /\ reserved' = Max2(synced, reserved)
    /\ bucket' = FreshBucket
    /\ raisedFor' = 0
    /\ UNCHANGED <<now, burst, issued, backups, restores, lastRestore, reused, leaders, topFirst>>

Elect ==
    /\ Batch
    /\ leaders < MaxLeaders
    /\ leaders' = leaders + 1
    /\ bucket' = FreshBucket
    /\ raisedFor' = 0
    /\ UNCHANGED <<now, burst, next, issued, backups, restores, lastRestore, reused,
                   reserved, synced, cuts, topFirst>>

\* A backup point is a committed offset at a frame end.
Backup ==
    /\ Cardinality(backups) < MaxBackups
    /\ next \notin backups
    /\ backups' = backups \cup {next}
    /\ UNCHANGED <<now, burst, next, issued, restores, lastRestore, reused, batchVars, topFirst>>

FirstIndex(end, clock) ==
    Max({end}
        \cup (IF Line THEN {TimeRate * clock + Gap} ELSE {})
        \cup (IF EndGap THEN {end + Gap} ELSE {}))

\* The clock of whichever control-plane node plans the restore: a leader
\* change can hand planning to a node whose clock runs slow.
Restore ==
    /\ restores < MaxRestores
    /\ now >= lastRestore + MinApart
    /\ \E end \in backups, clock \in {c \in (now - Skew)..(now + Skew) : c >= 0} :
        /\ Apart = 0 \/ FirstIndex(end, clock) >= topFirst + Apart
        /\ next' = FirstIndex(end, clock)
    /\ topFirst' = IF Apart > 0 THEN Max2(topFirst, next') ELSE topFirst
    /\ restores' = restores + 1
    /\ lastRestore' = now
    \* The restored epoch reserves its start and has a new leader.
    /\ reserved' = IF Batch THEN next' ELSE reserved
    /\ synced' = IF Batch THEN next' ELSE synced
    /\ bucket' = IF Batch THEN FreshBucket ELSE bucket
    /\ raisedFor' = 0
    /\ UNCHANGED <<now, burst, issued, backups, reused, cuts, leaders>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ burst' = 0
    /\ bucket' = IF Batch THEN Min2(Capacity, bucket + Rate) ELSE bucket
    /\ UNCHANGED <<next, issued, backups, restores, lastRestore, reused,
                   reserved, synced, cuts, leaders, raisedFor, topFirst>>

Next ==
    \/ Issue \/ Backup \/ Restore \/ Tick
    \/ \E n \in 1..MaxBatch : Raise(n) \/ IssueBatch(n)
    \/ Sync \/ PowerCut \/ Elect

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

OffsetNeverReused == ~reused

\* Not by construction: IssueBatch never raises the reservation itself.
ReservedCovers == next <= reserved

=============================================================================
