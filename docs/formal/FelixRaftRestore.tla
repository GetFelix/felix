--------------------------- MODULE FelixRaftRestore --------------------------
(***************************************************************************)
(* Offsets across a restore from a backup point, as docs/per-shard-raft.md *)
(* decides it ("A restore from a backup point is a re-form").              *)
(*                                                                         *)
(* One shard, one group. Epoch 0 runs and acknowledges records at offsets  *)
(* 1, 2, ... (an offset is a Raft index here). Every copy reserves before  *)
(* it writes: when a write passes its reservation it first raises it to    *)
(* the write's index plus Step (raft-reserved). The control plane keeps    *)
(* the last commit offset a leader reported as its floor.                  *)
(*                                                                         *)
(* The backup, in the guide's order:                                       *)
(*                                                                         *)
(*   TakePoint -- felixctl backup-point reads the leader's commit offset.  *)
(*   CopyDir   -- the operator copies that leader's shard directory while  *)
(*                it runs. The copy holds the point and the reservation    *)
(*                the copy had then.                                       *)
(*   Export    -- the control plane is exported, with the floor it had     *)
(*                then. It can come before or after the other two.         *)
(*                                                                         *)
(* Restore replaces the whole cluster with the backup: epoch 0 stops for   *)
(* good, the copied directory is cut back to the point, and the re-form    *)
(* starts epoch 1 at                                                       *)
(*                                                                         *)
(*   max(point + 1 + Gap, floor + 1, reservation + 1)                      *)
(*                                                                         *)
(* where the floor and the reservation are the restored ones. Gap = 0 is   *)
(* the design as built. A positive Gap is the restore offset gap decided   *)
(* for #2250 and not built yet (default 2^40). It holds only while no more *)
(* than Gap offsets were acknowledged after the point: MaxAfter bounds it. *)
(* Epoch 1 then gives out offsets from its first index (NewWrite).         *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   BackedUpNeverReused -- no offset acknowledged by the time the         *)
(*                         directory was copied is given out again by      *)
(*                         epoch 1. What the design as built claims.       *)
(*   OffsetNeverReused   -- no offset epoch 0 acknowledged, before or      *)
(*                         after the backup, is given out again. Needs the *)
(*                         gap.                                            *)
(*   BaseKept            -- epoch 1 starts above the point.                *)
(*                                                                         *)
(* Knobs:                                                                  *)
(*                                                                         *)
(*   NoReserve  -- the first index leaves out the restored reservation.    *)
(*   LeaderMoves -- the copied copy can stop leading after the point and   *)
(*                 fall behind, so epoch 0 commits without it.             *)
(*                                                                         *)
(* Abstracted: the other copies of epoch 0 are only the commits they make  *)
(* without the copied one. A restored follower copy is removed, so it      *)
(* carries nothing. The re-form's plan, stop and vouch rules are           *)
(* FelixRaftReform.tla's.                                                  *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    MaxWrites,      \* offsets epoch 0 acknowledges at most
    MaxAfter,       \* offsets acknowledged after the point at most
    MaxNew,         \* offsets epoch 1 gives out at most
    Step,           \* how far past the write a reservation raise goes
    Gap,            \* the restore offset gap, 0 as built
    NoReserve,
    LeaderMoves

\* Not yet set. An integer, so TLC can compare it with offsets.
None == -1

ASSUME Step >= 1 /\ Gap \in Nat /\ MaxAfter >= 1

VARIABLES
    acked,      \* epoch 0's highest acknowledged offset
    written,    \* the copied copy's last written index
    reserve,    \* the copied copy's reservation
    leads,      \* the copied copy still leads
    floor,      \* the control plane's floor
    point,      \* the backup point
    bkReserve,  \* the reservation in the copied directory
    copiedAt,   \* acked when the directory was copied
    bkFloor,    \* the floor in the export
    first,      \* epoch 1's first index, None before the restore
    newN        \* offsets epoch 1 gave out

vars == <<acked, written, reserve, leads, floor, point, bkReserve, copiedAt,
          bkFloor, first, newN>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Running == first = None

Init ==
    /\ acked = 0
    /\ written = 0
    /\ reserve = 0
    /\ leads = TRUE
    /\ floor = 0
    /\ point = None
    /\ bkReserve = None
    /\ copiedAt = None
    /\ bkFloor = None
    /\ first = None
    /\ newN = 0

\* The copy writes its next index: a leader past the commit, a follower that
\* fell behind only up to it. The reservation is raised and synced first.
Write ==
    /\ Running
    /\ IF leads THEN written = acked ELSE written < acked
    /\ written < MaxWrites
    /\ written' = written + 1
    /\ reserve' = IF written + 1 > reserve THEN written + Step ELSE reserve
    /\ UNCHANGED <<acked, leads, floor, point, bkReserve, copiedAt, bkFloor, first, newN>>

\* Epoch 0 acknowledges its next offset. While the copy leads it wrote the
\* entry first; once it stopped leading the others commit without it.
Commit ==
    /\ Running
    /\ acked < MaxWrites
    /\ point = None \/ acked < point + MaxAfter
    /\ ~leads \/ written > acked
    /\ acked' = acked + 1
    /\ UNCHANGED <<written, reserve, leads, floor, point, bkReserve, copiedAt, bkFloor, first, newN>>

\* The floor trails: a leader reports some commit offset it reached.
Report ==
    /\ Running
    /\ \E f \in (floor + 1)..acked : floor' = f
    /\ UNCHANGED <<acked, written, reserve, leads, point, bkReserve, copiedAt, bkFloor, first, newN>>

StepDown ==
    /\ Running
    /\ LeaderMoves
    /\ leads
    /\ point # None
    /\ leads' = FALSE
    /\ UNCHANGED <<acked, written, reserve, floor, point, bkReserve, copiedAt, bkFloor, first, newN>>

TakePoint ==
    /\ Running
    /\ point = None
    /\ leads
    /\ point' = acked
    /\ UNCHANGED <<acked, written, reserve, leads, floor, bkReserve, copiedAt, bkFloor, first, newN>>

CopyDir ==
    /\ Running
    /\ point # None
    /\ bkReserve = None
    /\ bkReserve' = reserve
    /\ copiedAt' = acked
    /\ UNCHANGED <<acked, written, reserve, leads, floor, point, bkFloor, first, newN>>

Export ==
    /\ Running
    /\ bkFloor = None
    /\ bkFloor' = floor
    /\ UNCHANGED <<acked, written, reserve, leads, floor, point, bkReserve, copiedAt, first, newN>>

FirstIndex ==
    Max({point + 1 + Gap, bkFloor + 1}
        \cup (IF NoReserve THEN {} ELSE {bkReserve + 1}))

Restore ==
    /\ Running
    /\ point # None /\ bkReserve # None /\ bkFloor # None
    /\ first' = FirstIndex
    /\ UNCHANGED <<acked, written, reserve, leads, floor, point, bkReserve, copiedAt, bkFloor, newN>>

NewWrite ==
    /\ first # None
    /\ newN < MaxNew
    /\ newN' = newN + 1
    /\ UNCHANGED <<acked, written, reserve, leads, floor, point, bkReserve, copiedAt, bkFloor, first>>

Next ==
    \/ Write \/ Commit \/ Report \/ StepDown
    \/ TakePoint \/ CopyDir \/ Export
    \/ Restore \/ NewWrite

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

NewOffsets == IF newN = 0 THEN {} ELSE first..(first + newN - 1)

\* Every count is at or below a synced reservation, on the copy that wrote it.
ReserveCovers == written <= reserve

BackedUpNeverReused ==
    copiedAt # None => NewOffsets \cap 1..copiedAt = {}

OffsetNeverReused == NewOffsets \cap 1..acked = {}

BaseKept == first # None => first > point

=============================================================================
