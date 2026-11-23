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
    EndGap        \* the first index takes the backup's end plus the gap

ASSUME Rate >= 1 /\ TimeRate >= 0 /\ Gap \in Nat /\ Skew \in Nat

VARIABLES
    now,         \* true time
    burst,       \* offsets given out in this tick
    next,        \* the running epoch's next offset
    issued,      \* every offset given out so far
    backups,     \* each backup's end
    restores,    \* restores so far
    lastRestore, \* true time of the last restore (epoch 0 counts, at 0)
    reused       \* some offset was given out twice

vars == <<now, burst, next, issued, backups, restores, lastRestore, reused>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init ==
    /\ now = 0
    /\ burst = 0
    /\ next = Gap + 1
    /\ issued = {}
    /\ backups = {}
    /\ restores = 0
    /\ lastRestore = 0
    /\ reused = FALSE

Issue ==
    /\ burst < Rate
    /\ reused' = (reused \/ next \in issued)
    /\ issued' = issued \cup {next}
    /\ next' = next + 1
    /\ burst' = burst + 1
    /\ UNCHANGED <<now, backups, restores, lastRestore>>

Backup ==
    /\ Cardinality(backups) < MaxBackups
    /\ next \notin backups
    /\ backups' = backups \cup {next}
    /\ UNCHANGED <<now, burst, next, issued, restores, lastRestore, reused>>

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
        next' = FirstIndex(end, clock)
    /\ restores' = restores + 1
    /\ lastRestore' = now
    /\ UNCHANGED <<now, burst, issued, backups, reused>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ burst' = 0
    /\ UNCHANGED <<next, issued, backups, restores, lastRestore, reused>>

Next == Issue \/ Backup \/ Restore \/ Tick

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

OffsetNeverReused == ~reused

=============================================================================
