------------------------- MODULE FelixPlacementPacing -------------------------
(***************************************************************************)
(* How placement paces copies across shards: moves that stage a destination *)
(* and follower replacements that copy one in. The shard model next door    *)
(* checks one shard's handoff is safe; this one checks the limits hold      *)
(* across many.                                                             *)
(*                                                                         *)
(*   CopiesWithinLimit -- never more copies in flight than `MaxConcurrent`, *)
(*                        and never more than `MaxPerNode` into or out of   *)
(*                        one node.                                         *)
(*   FencedNeverTimesOut -- a move past its fence is finished, never       *)
(*                        abandoned.                                        *)
(*                                                                         *)
(* Placement counts copies from what the store says is in progress. A move *)
(* names its destination as `successor`; a replacement names the follower  *)
(* it copies in as `joining`. `CountReplacements` is whether a replacement *)
(* is visible to that count at all -- without it, TLC finds a replacement  *)
(* beside a move under a limit of one.                                      *)
(*                                                                         *)
(* Starting a copy is decided from a planner's own read of the store, not  *)
(* from the store as it stands, because several instances may plan: the    *)
(* lease holder, an instance that paused past its lease, and an operator's *)
(* request on any instance. A planner reads only while it holds the lease, *)
(* unless it is an operator. The lease may change hands at any moment,     *)
(* which is expiry under a pause. Each start is also conditional on its    *)
(* shard being as read, the generation check. `Fenced` is whether a start  *)
(* also needs the placement token unchanged since the read, counting the   *)
(* planner's own writes as part of its read; without it, TLC finds two     *)
(* planners each starting a copy on a different shard from a read with one *)
(* free slot. The steps after a start take no slot, so they are written    *)
(* from the store as it stands, and advance the token like any write.      *)
(*                                                                         *)
(* Out of scope: failing a lost leader's shard over to a node that holds no *)
(* copy. That writes an assignment rather than starting a copy, takes no   *)
(* slot here, and is paced separately (`FAILOVERS_PER_PASS` in the          *)
(* reconciler); its safety is the generation check the shard model covers. *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Shards,             \* the shards placement moves
    Nodes,              \* the brokers
    MaxConcurrent,      \* FELIX_SHARD_MOVES_MAX_CONCURRENT
    MaxPerNode,         \* FELIX_SHARD_MOVES_MAX_PER_NODE
    CountReplacements,  \* whether a follower replacement counts as a copy in flight
    Planners,           \* instances that may start copies
    Operators,          \* planners that read without the lease: operator requests
    Fenced              \* whether a start needs the placement token unchanged

ASSUME CountReplacements \in BOOLEAN
ASSUME Fenced \in BOOLEAN
ASSUME Operators \subseteq Planners

None == "none"

VARIABLES
    leader,     \* who leads each shard
    copy,       \* the node a copy is going to, or None
    kind,       \* "move" or "replacement" while a copy is in flight
    fenced,     \* the move's leader has been fenced
    holder,     \* who holds the placement lease
    view,       \* each planner's read of leader, copy and kind, plus its own writes
    fresh       \* no other placement write since the planner's read: its token still holds

vars == << leader, copy, kind, fenced, holder, view, fresh >>

Snapshot == [leader |-> leader, copy |-> copy, kind |-> kind]

Init ==
    /\ leader \in [Shards -> Nodes]
    /\ copy = [s \in Shards |-> None]
    /\ kind = [s \in Shards |-> None]
    /\ fenced = [s \in Shards |-> FALSE]
    /\ holder \in Planners
    /\ view = [p \in Planners |-> Snapshot]
    /\ fresh = [p \in Planners |-> FALSE]

\* Every write advances the token, so every other planner's read is stale.
Written(p) == fresh' = [q \in Planners |-> q = p /\ fresh[q]]
WrittenByNobody == fresh' = [q \in Planners |-> FALSE]

Copying(s) == copy[s] /= None

\* What the planner counts: a copy the store names.
Counted(s) == Copying(s) /\ (kind[s] = "move" \/ CountReplacements)

Touches(s, n) == n = leader[s] \/ n = copy[s]

\* The same, as planner `p` read them.
ViewCounted(p, s) ==
    /\ view[p].copy[s] /= None
    /\ (view[p].kind[s] = "move" \/ CountReplacements)
ViewTouches(p, s, n) == n = view[p].leader[s] \/ n = view[p].copy[s]

Room(p, from, to) ==
    /\ Cardinality({s \in Shards : ViewCounted(p, s)}) < MaxConcurrent
    /\ \A n \in {from, to} :
        Cardinality({s \in Shards : ViewCounted(p, s) /\ ViewTouches(p, s, n)}) < MaxPerNode

TakeLease(p) ==
    /\ holder /= p
    /\ holder' = p
    /\ WrittenByNobody
    /\ UNCHANGED << leader, copy, kind, fenced, view >>

Read(p) ==
    /\ holder = p \/ p \in Operators
    /\ view' = [view EXCEPT ![p] = Snapshot]
    /\ fresh' = [fresh EXCEPT ![p] = TRUE]
    /\ UNCHANGED << leader, copy, kind, fenced, holder >>

Start(p, s, n, k) ==
    /\ Fenced => fresh[p]
    \* The generation check: the shard is as the planner read it.
    /\ leader[s] = view[p].leader[s] /\ copy[s] = view[p].copy[s]
    /\ ~Copying(s)
    /\ n /= leader[s]
    /\ Room(p, leader[s], n)
    /\ copy' = [copy EXCEPT ![s] = n]
    /\ kind' = [kind EXCEPT ![s] = k]
    /\ view' = [view EXCEPT ![p].copy[s] = n, ![p].kind[s] = k]
    /\ Written(p)
    /\ UNCHANGED << leader, fenced, holder >>

Fence(s) ==
    /\ Copying(s) /\ kind[s] = "move" /\ ~fenced[s]
    /\ fenced' = [fenced EXCEPT ![s] = TRUE]
    /\ WrittenByNobody
    /\ UNCHANGED << leader, copy, kind, holder, view >>

CutOver(s) ==
    /\ fenced[s]
    /\ leader' = [leader EXCEPT ![s] = copy[s]]
    /\ copy' = [copy EXCEPT ![s] = None]
    /\ kind' = [kind EXCEPT ![s] = None]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ WrittenByNobody
    /\ UNCHANGED << holder, view >>

Seat(s) ==
    /\ Copying(s) /\ kind[s] = "replacement"
    /\ copy' = [copy EXCEPT ![s] = None]
    /\ kind' = [kind EXCEPT ![s] = None]
    /\ WrittenByNobody
    /\ UNCHANGED << leader, fenced, holder, view >>

\* Past `FELIX_SHARD_MOVE_TIMEOUT_MS`, which in this model may be any time
\* before the fence.
TimeOut(s) ==
    /\ Copying(s) /\ ~fenced[s]
    /\ copy' = [copy EXCEPT ![s] = None]
    /\ kind' = [kind EXCEPT ![s] = None]
    /\ WrittenByNobody
    /\ UNCHANGED << leader, fenced, holder, view >>

Next ==
    \/ \E p \in Planners : TakeLease(p) \/ Read(p)
    \/ \E p \in Planners, s \in Shards, n \in Nodes :
        Start(p, s, n, "move") \/ Start(p, s, n, "replacement")
    \/ \E s \in Shards :
        \/ Fence(s)
        \/ CutOver(s)
        \/ Seat(s)
        \/ TimeOut(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

CopiesWithinLimit ==
    /\ Cardinality({s \in Shards : Copying(s)}) <= MaxConcurrent
    /\ \A n \in Nodes :
        Cardinality({s \in Shards : Copying(s) /\ Touches(s, n)}) <= MaxPerNode

FencedNeverTimesOut == \A s \in Shards : fenced[s] => Copying(s) /\ kind[s] = "move"

TypeOK ==
    /\ leader \in [Shards -> Nodes]
    /\ copy \in [Shards -> Nodes \cup {None}]
    /\ kind \in [Shards -> {"move", "replacement", None}]
    /\ fenced \in [Shards -> BOOLEAN]
    /\ holder \in Planners
    /\ fresh \in [Planners -> BOOLEAN]

Symm == Permutations(Shards) \cup Permutations(Nodes)

=============================================================================
