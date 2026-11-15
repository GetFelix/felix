--------------------------- MODULE FelixRaftGroups ---------------------------
(***************************************************************************)
(* A consumer group's state on a Raft stream shard.                        *)
(*                                                                         *)
(* Group writes are entries in the stream's own Raft log, and every member *)
(* folds the committed ones, in order, into the same group table: the      *)
(* cursor, the dead letters and the redriven records. The leader serves    *)
(* the group from an in-memory tracker (claims, acknowledgements above a   *)
(* gap) hydrated from that table. One group, records 1..N; the other      *)
(* groups of the shard appear only through their deletes, since the        *)
(* delete watermark is shard-wide.                                         *)
(*                                                                         *)
(* `log` is every committed entry; `tail` is the leader's uncommitted      *)
(* entries. A change of leader commits a prefix of the tail and drops the  *)
(* rest. `pend` is what the leader's trackers have decided to propose and  *)
(* not yet appended: an acknowledgement updates the tracker first and      *)
(* proposes after, so a proposal can land after a seek or a delete that    *)
(* was placed later. A change of leader drops `pend`, since a proposal is  *)
(* bound to the term it was routed in.                                     *)
(*                                                                         *)
(* Entries:                                                                *)
(*                                                                         *)
(*   advance(to)  -- the tracker closed a run; the cursor moves to `to`    *)
(*   dead(o)      -- `o` ran out of attempts and is a dead letter          *)
(*   redrive(o)   -- a dead letter is put back; it is owed again           *)
(*   finish(o)    -- a redriven record was acknowledged                    *)
(*   seek(to)     -- the cursor is set, backwards or forwards              *)
(*   delete       -- the group and its dead letters are gone               *)
(*   fdelete      -- another group of the shard is deleted                 *)
(*                                                                         *)
(* A live group has a creation index `cre`: the index of the seek that    *)
(* created or last reset it, or, for a group created by its first          *)
(* acknowledgement or dead letter, the index its tracker hydrated at (a    *)
(* tracker cannot know the index its first entry will land at). Every      *)
(* tracker-driven entry carries the `cre` of the group its tracker serves. *)
(* The shard keeps `lastDel`, the index of its latest delete of any group, *)
(* and keeps nothing per deleted name. Apply drops an entry whose `cre`    *)
(* differs from the live group's, or that names an absent group with a     *)
(* `cre` below `lastDel`. Within that, the cursor only moves forward. A    *)
(* delete of another group therefore leaves a live group's tracker alone. *)
(*                                                                         *)
(* The leader decides from its applied table, never from its own entries   *)
(* in flight. So a hydrate, a redrive and a seek wait until the leader has *)
(* applied its own last proposed entry; after a change of leader that also *)
(* covers everything committed before the term.                            *)
(*                                                                         *)
(* A seek with if_new moves only a group that does not exist. The leader   *)
(* decides that under the shard lock from the table and its tracker: a     *)
(* group with nothing committed but records handed out exists.             *)
(*                                                                         *)
(* What is checked:                                                        *)
(*                                                                         *)
(*   NoSkip           -- every record between where the group started (or *)
(*                       was last seeked to) and its cursor is a dead      *)
(*                       letter, redriven, or was acknowledged to a        *)
(*                       tracker of this incarnation of the group.         *)
(*   RedrivenOwed     -- the leader's tracker owes, holds or is finishing  *)
(*                       every redriven record in the table, so a redrive  *)
(*                       is delivered again.                               *)
(*   IfNewKeepsServed -- an if_new seek is never placed while the group    *)
(*                       has handed out records in this term.              *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   CreationCheck = FALSE      -- apply ignores `cre`: a late advance     *)
(*                                 from before a seek or a delete still    *)
(*                                 moves or recreates the group.           *)
(*   DeleteWatermark = FALSE    -- an entry naming an absent group is      *)
(*                                 never dropped, so a late advance        *)
(*                                 recreates a deleted group.              *)
(*   StaleByHyd = TRUE          -- the earlier rule instead: apply drops   *)
(*                                 any entry whose tracker hydrated before *)
(*                                 the group's last seek or the shard's    *)
(*                                 last delete, so deleting another group  *)
(*                                 silences a live group's tracker.        *)
(*   IfNewChecksTracker = FALSE -- if_new looks at the table only.         *)
(*   TermBoundTrackers = FALSE  -- a member keeps its tracker across terms *)
(*                                 and serves from it when it leads again. *)
(*   HydrateFromApplied = FALSE -- the leader's table is whatever prefix   *)
(*                                 of the committed log it has applied,    *)
(*                                 not all of it.                          *)
(*   WaitOwnProposals = FALSE   -- hydrate and user operations do not wait *)
(*                                 for the leader's own entries in flight. *)
(*   DeadBeforeAdvance = FALSE  -- a dead letter is proposed like an       *)
(*                                 advance, in any order with it.          *)
(*   KeepRedriven = FALSE       -- a redrive deletes the dead letter and   *)
(*                                 the record is owed only in memory.      *)
(*                                                                         *)
(* Abstracted: elections and replication (a change of leader is one step), *)
(* the group marks' place in the stream's offsets (skipping them proposes  *)
(* nothing, so they do not appear here; that skipping never proposes, and  *)
(* the bound on re-skipping, are tested in code), visibility timeouts (a claim is     *)
(* acknowledged or dead-lettered, never lapses; a lapse is a redelivery,   *)
(* which only adds behaviour already covered by a fresh tracker), attempt  *)
(* counts, discard, snapshots and purge. Every member holds the committed  *)
(* log, so any member can lead.                                            *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Members,
    N,               \* records 1..N
    SeekTo,          \* where a seek may move the cursor
    MaxChanges,      \* changes of leader
    MaxLog,          \* entries written, committed or not
    CreationCheck,
    DeleteWatermark,
    IfNewChecksTracker,
    TermBoundTrackers,
    HydrateFromApplied,
    WaitOwnProposals,
    StaleByHyd,
    DeadBeforeAdvance,
    KeepRedriven

ASSUME
    /\ N \in Nat /\ N > 0 /\ MaxChanges \in Nat /\ MaxLog \in Nat
    /\ SeekTo \subseteq 1..(N + 1)
    /\ CreationCheck \in BOOLEAN /\ DeleteWatermark \in BOOLEAN
    /\ IfNewChecksTracker \in BOOLEAN /\ TermBoundTrackers \in BOOLEAN
    /\ HydrateFromApplied \in BOOLEAN /\ KeepRedriven \in BOOLEAN
    /\ WaitOwnProposals \in BOOLEAN /\ StaleByHyd \in BOOLEAN
    /\ DeadBeforeAdvance \in BOOLEAN

VARIABLES
    log,       \* every committed entry, in order
    tail,      \* the leader's uncommitted entries
    pend,      \* entries the leader's trackers decided on, not yet appended
    leader,
    trk,       \* per member: its tracker, or None
    acks,      \* history: per record, the hyds of the trackers that took its
               \* acknowledgement; a change of leader forgets those past the
               \* committed log, since their index is reused
    served,    \* history: the highest hyd of a tracker that handed out a record
               \* in this term, -1 if none
    badIfNew,  \* history: an if_new seek was placed over a group that had served
    changes

vars == <<log, tail, pend, leader, trk, acks, served, badIfNew, changes>>

Symm == Permutations(Members)

Records == 1..N
Max2(a, b) == IF a >= b THEN a ELSE b
None == [none |-> TRUE, any |-> FALSE]
NoCre == [at |-> 0, by |-> "none"]

Full == log \o tail

----------------------------------------------------------------------------
\* The group table.

\* `from` is where this incarnation started reading; `gfloor` is the index
\* of its last seek or delete whatever the knobs say, for the checks only.
\* `reset` is the index of the last seek, for the StaleByHyd rule only.
T0 == [exists |-> FALSE, cre |-> NoCre, cursor |-> 1, from |-> 1, reset |-> 0,
       lastDel |-> 0, gfloor |-> 0, dead |-> {}, red |-> {}]

Stale(T, e) ==
    IF StaleByHyd THEN e.hyd < T.lastDel \/ e.hyd < T.reset
    ELSE /\ CreationCheck
         /\ IF T.exists THEN e.cre # T.cre
            ELSE DeleteWatermark /\ e.cre.at < T.lastDel

\* An entry that names an absent group creates it with the entry's `cre`.
Live(T, e) == [T EXCEPT !.exists = TRUE, !.cre = IF T.exists THEN @ ELSE e.cre]

Step(T, e, i) ==
    CASE e.kind = "seek" ->
            [T EXCEPT !.exists = TRUE, !.cre = e.cre, !.cursor = e.to,
                      !.from = e.to, !.reset = i, !.gfloor = i]
      [] e.kind = "delete" ->
            [T0 EXCEPT !.lastDel = i, !.gfloor = i]
      [] e.kind = "fdelete" -> [T EXCEPT !.lastDel = i]
      [] Stale(T, e) -> T
      [] e.kind = "advance" ->
            [Live(T, e) EXCEPT !.cursor = Max2(T.cursor, e.to)]
      [] e.kind = "dead" ->
            [Live(T, e) EXCEPT !.dead = @ \cup {e.o}, !.red = @ \ {e.o}]
      [] e.kind = "redrive" ->
            IF e.o \in T.dead
            THEN [T EXCEPT !.dead = @ \ {e.o},
                           !.red = IF KeepRedriven THEN @ \cup {e.o} ELSE @]
            ELSE T
      [] e.kind = "finish" -> [T EXCEPT !.red = @ \ {e.o}]

RECURSIVE FoldFrom(_, _, _)
FoldFrom(T, L, i) == IF i > Len(L) THEN T ELSE FoldFrom(Step(T, L[i], i), L, i + 1)

Fold(L) == FoldFrom(T0, L, 1)

----------------------------------------------------------------------------
\* Trackers.

Tracker(h, cre, pos, owed) ==
    [none |-> FALSE, hyd |-> h, cre |-> cre, pos |-> pos, out |-> {},
     done |-> {}, owed |-> owed, any |-> FALSE]

\* The first position at or above `p` that is not in `S`.
RECURSIVE RunEnd(_, _)
RunEnd(p, S) == IF p \in S THEN RunEnd(p + 1, S) ELSE p

\* Close the run over `o` (acknowledged or dead-lettered): the new tracker
\* and the advance it proposes, if any.
Settle(t, o) ==
    IF o < t.pos THEN [t |-> t, adv |-> {}]
    ELSE LET d == t.done \cup {o}
             p == RunEnd(t.pos, d)
         IN [t |-> [t EXCEPT !.done = {x \in d : x >= p}, !.pos = p],
             adv |-> IF p > t.pos
                     THEN {[kind |-> "advance", to |-> p, hyd |-> t.hyd, cre |-> t.cre]}
                     ELSE {}]

Room == Len(log) + Len(tail) < MaxLog

Lead == trk[leader]

\* What the leader decides from: its applied table. With the rules on, it
\* has applied the whole committed log and has nothing of its own in flight.
Ready == WaitOwnProposals => tail = <<>>
Views == IF HydrateFromApplied THEN {Len(log)} ELSE 0..Len(log)
View(k) == Fold(SubSeq(log, 1, k))

----------------------------------------------------------------------------

Init ==
    /\ log = <<>>
    /\ tail = <<>>
    /\ pend = {}
    /\ leader \in Members
    /\ trk = [m \in Members |-> None]
    /\ acks = [o \in Records |-> {}]
    /\ served = -1
    /\ badIfNew = FALSE
    /\ changes = 0

\* A poll on a group with no tracker.
Hydrate ==
    /\ Lead.none
    /\ Ready
    /\ \E k \in Views :
        LET T == View(k) IN
        trk' = [trk EXCEPT ![leader] =
                    IF T.exists THEN Tracker(k, T.cre, T.cursor, T.red)
                    ELSE Tracker(k, [at |-> k, by |-> "hydrate"], 1, {})]
    /\ UNCHANGED <<log, tail, pend, leader, acks, served, badIfNew, changes>>

HandOut(o) ==
    LET t == Lead IN
    /\ ~t.none
    /\ o \notin t.out
    /\ \/ o \in t.owed
       \/ o >= t.pos /\ o \notin t.done
    /\ trk' = [trk EXCEPT ![leader] = [t EXCEPT !.out = @ \cup {o}, !.any = TRUE]]
    /\ served' = Max2(served, t.hyd)
    /\ UNCHANGED <<log, tail, pend, leader, acks, badIfNew, changes>>

Ack(o) ==
    LET t == Lead
        t1 == [t EXCEPT !.out = @ \ {o}, !.owed = @ \ {o}]
        s == Settle(t1, o)
    IN
    /\ ~t.none
    /\ o \in t.out
    /\ trk' = [trk EXCEPT ![leader] = s.t]
    /\ pend' = pend \cup s.adv
               \cup (IF o \in t.owed
                     THEN {[kind |-> "finish", o |-> o, hyd |-> t.hyd, cre |-> t.cre]}
                     ELSE {})
    /\ acks' = [acks EXCEPT ![o] = @ \cup {t.hyd}]
    /\ UNCHANGED <<log, tail, leader, served, badIfNew, changes>>

\* The dead letter is appended before the run closes, as the code writes it
\* before settling: an advance past it never lands ahead of it.
DeadLetter(o) ==
    LET t == Lead
        t1 == [t EXCEPT !.out = @ \ {o}, !.owed = @ \ {o}]
        s == Settle(t1, o)
        d == [kind |-> "dead", o |-> o, hyd |-> t.hyd, cre |-> t.cre]
    IN
    /\ ~t.none
    /\ o \in t.out
    /\ trk' = [trk EXCEPT ![leader] = s.t]
    /\ IF DeadBeforeAdvance
       THEN /\ Room
            /\ tail' = Append(tail, d)
            /\ pend' = pend \cup s.adv
       ELSE /\ pend' = pend \cup s.adv \cup {d}
            /\ UNCHANGED tail
    /\ UNCHANGED <<log, leader, acks, served, badIfNew, changes>>

\* A tracker's decision reaches the log, in any order.
Propose(e) ==
    /\ e \in pend
    /\ Room
    /\ tail' = Append(tail, e)
    /\ pend' = pend \ {e}
    /\ UNCHANGED <<log, leader, trk, acks, served, badIfNew, changes>>

\* User operations, decided and appended under the shard lock.
Redrive(o) ==
    /\ Room
    /\ Ready
    /\ \E k \in Views :
        /\ o \in View(k).dead
        /\ tail' = Append(tail, [kind |-> "redrive", o |-> o,
                                  hyd |-> Len(Full) + 1, cre |-> View(k).cre])
    /\ trk' = IF Lead.none THEN trk
              ELSE [trk EXCEPT ![leader].owed = @ \cup {o}]
    /\ UNCHANGED <<log, pend, leader, acks, served, badIfNew, changes>>

Seek(to, ifNew) ==
    LET t == Lead
        i == Len(Full) + 1
    IN
    /\ Room
    /\ Ready
    /\ \E k \in Views :
        LET T == View(k)
            exists == T.exists \/ (IfNewChecksTracker /\ t.any)
        IN
        /\ ifNew => ~exists
        /\ trk' = [trk EXCEPT ![leader] =
                       Tracker(i, [at |-> i, by |-> "seek"], to, T.red)]
    /\ tail' = Append(tail, [kind |-> "seek", to |-> to, hyd |-> i,
                              cre |-> [at |-> i, by |-> "seek"]])
    /\ badIfNew' = (badIfNew \/
                    (ifNew /\ served >= Fold(Full).gfloor))
    /\ UNCHANGED <<log, pend, leader, acks, served, changes>>

Delete ==
    /\ Room
    /\ tail' = Append(tail, [kind |-> "delete", hyd |-> Len(Full) + 1])
    /\ trk' = [trk EXCEPT ![leader] = None]
    /\ UNCHANGED <<log, pend, leader, acks, served, badIfNew, changes>>

\* Another group of the shard is deleted.
ForeignDelete ==
    /\ Room
    /\ tail' = Append(tail, [kind |-> "fdelete", hyd |-> Len(Full) + 1])
    /\ UNCHANGED <<log, pend, leader, trk, acks, served, badIfNew, changes>>

Commit ==
    /\ tail # <<>>
    /\ log' = Append(log, Head(tail))
    /\ tail' = Tail(tail)
    /\ UNCHANGED <<pend, leader, trk, acks, served, badIfNew, changes>>

ChangeLeader(m) ==
    /\ changes < MaxChanges
    /\ \E k \in 0..Len(tail) :
        /\ log' = log \o SubSeq(tail, 1, k)
        /\ tail' = <<>>
        /\ acks' = [o \in Records |-> {h \in acks[o] : h <= Len(log) + k}]
    /\ pend' = {}
    /\ leader' = m
    /\ trk' = IF TermBoundTrackers THEN [x \in Members |-> None] ELSE trk
    /\ changes' = changes + 1
    /\ served' = -1
    /\ UNCHANGED badIfNew

Next ==
    \/ Hydrate
    \/ \E o \in Records : HandOut(o) \/ Ack(o) \/ DeadLetter(o) \/ Redrive(o)
    \/ \E e \in pend : Propose(e)
    \/ \E to \in SeekTo, ifNew \in BOOLEAN : Seek(to, ifNew)
    \/ Delete
    \/ ForeignDelete
    \/ Commit
    \/ \E m \in Members : ChangeLeader(m)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

NoSkip ==
    LET T == Fold(log) IN
    T.exists =>
        \A o \in T.from..(T.cursor - 1) :
            \/ o \in T.dead \cup T.red
            \/ \E h \in acks[o] : h >= T.gfloor

RedrivenOwed ==
    LET t == Lead IN
    ~t.none =>
        Fold(Full).red \subseteq
            t.owed \cup t.out \cup {e.o : e \in {x \in pend : x.kind = "finish"}}

IfNewKeepsServed == ~badIfNew

=============================================================================
