--------------------------- MODULE FelixRaftRestore --------------------------
(***************************************************************************)
(* Offsets across a restore from a backup point, as docs/per-shard-raft.md *)
(* decides it ("A restore from a backup point is a re-form").              *)
(*                                                                         *)
(* One shard, one group. Epoch 0 runs and acknowledges records at offsets  *)
(* 1, 2, ... (an offset is a Raft index here). An offset is acknowledged  *)
(* once a majority wrote it or on the lease, which can be before it        *)
(* commits, so the commit trails the acknowledged mark, and the copy's     *)
(* saved commit trails both. Every copy reserves before                    *)
(* it writes: when a write passes its reservation it first raises it to    *)
(* the write's index plus Step (raft-reserved). The control plane keeps    *)
(* the last commit offset a leader reported as its floor.                  *)
(*                                                                         *)
(* The backup, in the guide's order:                                       *)
(*                                                                         *)
(*   TakePoint -- felixctl backup-point reads the leader's acknowledged    *)
(*                mark, at or past its commit.                             *)
(*   CopyDir   -- the operator copies that leader's shard directory while  *)
(*                it runs. The copy holds the point, its saved commit and  *)
(*                the reservation the copy had then.                       *)
(*   Export    -- the control plane is exported, with the floor it had     *)
(*                then. It can come before or after the other two.         *)
(*                                                                         *)
(* Restore replaces the whole cluster with the backup: epoch 0 stops for   *)
(* good, the copied directory is cut back to the point with its saved      *)
(* commit set to the point (raised when below), and the re-form, which     *)
(* vouches for the copy only through its saved commit and refuses a base   *)
(* that does not end at the point, starts epoch 1 at                       *)
(*                                                                         *)
(*   max(point + 1 + Gap, floor + 1, reservation + 1)                      *)
(*                                                                         *)
(* where the floor and the reservation are the restored ones. Gap is the  *)
(* restore offset gap, 2^40 by default (`raft reform plan --offset-gap`);  *)
(* Gap = 0 is a restore with none. The code's point is the point's offset, *)
(* one past its last record, so point + 1 here is that offset. The gap     *)
(* holds only while no more than Gap offsets were acknowledged after the   *)
(* point: MaxAfter bounds it.                                              *)
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
(*   RestoresAtPoint     -- a restore is refused only when the copy does   *)
(*                         not reach the point.                            *)
(*                                                                         *)
(* Knobs:                                                                  *)
(*                                                                         *)
(*   NoReserve  -- the first index leaves out the restored reservation.    *)
(*   LeaderMoves -- the copied copy can stop leading after the point and   *)
(*                 fall behind, so epoch 0 commits without it.             *)
(*   RaiseOnCut -- the cut raises a saved commit below the point. FALSE    *)
(*                 only lowers one above it.                               *)
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
    Gap,            \* the restore offset gap
    NoReserve,
    LeaderMoves,
    RaiseOnCut

\* Not yet set. An integer, so TLC can compare it with offsets.
None == -1

ASSUME Step >= 1 /\ Gap \in Nat /\ MaxAfter >= 1

VARIABLES
    acked,      \* epoch 0's highest acknowledged offset
    commit,     \* epoch 0's commit, at or below acked
    saved,      \* the copied copy's saved commit
    written,    \* the copied copy's last written index
    reserve,    \* the copied copy's reservation
    leads,      \* the copied copy still leads
    floor,      \* the control plane's floor
    point,      \* the backup point
    bkReserve,  \* the reservation in the copied directory
    copiedAt,   \* acked when the directory was copied
    bkWritten,  \* the last index in the copied directory
    bkSaved,    \* the saved commit in the copied directory
    refused,    \* the re-form refused the restored copy
    bkFloor,    \* the floor in the export
    first,      \* epoch 1's first index, None before the restore
    newN        \* offsets epoch 1 gave out

vars == <<acked, commit, saved, written, reserve, leads, floor, point,
          bkReserve, copiedAt, bkWritten, bkSaved, refused, bkFloor, first,
          newN>>

\* The variables a step of epoch 0 or of the backup leaves alone.
restored == <<refused, first, newN>>
copied == <<bkReserve, copiedAt, bkWritten, bkSaved>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(a, b) == IF a < b THEN a ELSE b

Running == first = None /\ ~refused

Init ==
    /\ acked = 0
    /\ commit = 0
    /\ saved = 0
    /\ written = 0
    /\ reserve = 0
    /\ leads = TRUE
    /\ floor = 0
    /\ point = None
    /\ bkReserve = None
    /\ copiedAt = None
    /\ bkWritten = None
    /\ bkSaved = None
    /\ refused = FALSE
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
    /\ UNCHANGED <<acked, commit, saved, leads, floor, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

\* Epoch 0 acknowledges its next offset, on a written majority or the
\* lease, before or at its commit. While the copy leads it wrote the entry
\* first; once it stopped leading the others take it without it.
Ack ==
    /\ Running
    /\ acked < MaxWrites
    /\ point = None \/ acked < point + MaxAfter
    /\ ~leads \/ written > acked
    /\ acked' = acked + 1
    /\ UNCHANGED <<commit, saved, written, reserve, leads, floor, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

\* The commit catches up with acknowledged offsets.
Commit ==
    /\ Running
    /\ \E c \in (commit + 1)..acked : commit' = c
    /\ UNCHANGED <<acked, saved, written, reserve, leads, floor, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

\* The copy saves a commit it knows of and holds, behind the commit.
SaveCommit ==
    /\ Running
    /\ \E c \in (saved + 1)..Min(commit, written) : saved' = c
    /\ UNCHANGED <<acked, commit, written, reserve, leads, floor, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

\* The floor trails: a leader reports some commit offset it reached.
Report ==
    /\ Running
    /\ \E f \in (floor + 1)..commit : floor' = f
    /\ UNCHANGED <<acked, commit, saved, written, reserve, leads, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

StepDown ==
    /\ Running
    /\ LeaderMoves
    /\ leads
    /\ point # None
    /\ leads' = FALSE
    /\ UNCHANGED <<acked, commit, saved, written, reserve, floor, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

TakePoint ==
    /\ Running
    /\ point = None
    /\ leads
    /\ point' = acked
    /\ UNCHANGED <<acked, commit, saved, written, reserve, leads, floor, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED restored

CopyDir ==
    /\ Running
    /\ point # None
    /\ bkReserve = None
    /\ bkReserve' = reserve
    /\ copiedAt' = acked
    /\ bkWritten' = written
    /\ bkSaved' = saved
    /\ UNCHANGED <<acked, commit, saved, written, reserve, leads, floor, point, bkFloor>>
    /\ UNCHANGED restored

Export ==
    /\ Running
    /\ bkFloor = None
    /\ bkFloor' = floor
    /\ UNCHANGED <<acked, commit, saved, written, reserve, leads, floor, point>>
    /\ UNCHANGED copied /\ UNCHANGED restored

FirstIndex ==
    Max({point + 1 + Gap, bkFloor + 1}
        \cup (IF NoReserve THEN {} ELSE {bkReserve + 1}))

\* The copy's saved commit after the cut to the point.
CutSaved == IF RaiseOnCut THEN point ELSE Min(bkSaved, point)

\* The cut needs the point in the copy, and the plan vouches for the copy
\* only through its saved commit, so it refuses a base below the point.
Restore ==
    /\ Running
    /\ point # None /\ bkReserve # None /\ bkFloor # None
    /\ IF bkWritten >= point /\ CutSaved = point
       THEN first' = FirstIndex /\ UNCHANGED refused
       ELSE refused' = TRUE /\ UNCHANGED first
    /\ UNCHANGED <<acked, commit, saved, written, reserve, leads, floor, point, bkFloor, newN>>
    /\ UNCHANGED copied

NewWrite ==
    /\ first # None
    /\ newN < MaxNew
    /\ newN' = newN + 1
    /\ UNCHANGED <<acked, commit, saved, written, reserve, leads, floor, point, bkFloor>>
    /\ UNCHANGED copied /\ UNCHANGED <<refused, first>>

Next ==
    \/ Write \/ Ack \/ Commit \/ SaveCommit \/ Report \/ StepDown
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

RestoresAtPoint == refused => bkWritten < point

=============================================================================
