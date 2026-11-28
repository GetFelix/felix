--------------------------- MODULE FelixRaftGroups ---------------------------
(***************************************************************************)
(* A consumer group's state on a Raft stream shard.                        *)
(*                                                                         *)
(* Group writes are entries in the stream's own Raft log, and every member *)
(* folds the committed ones, in order, into the same group table: the      *)
(* cursor, the dead letters, the redriven records and the acks above a    *)
(* gap. The leader serves the group from an in-memory tracker of claims    *)
(* hydrated from that table. One group, records 1..N; the other groups of  *)
(* the shard appear only through their deletes, since the delete           *)
(* watermark is shard-wide.                                                *)
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
(*   ack(o)       -- `o` was acknowledged above a gap                      *)
(*   dead(o)      -- `o` ran out of attempts and is a dead letter          *)
(*   redrive(o)   -- a dead letter is put back; it is owed again           *)
(*   finish(o)    -- a redriven record was acknowledged                    *)
(*   seek(to)     -- the cursor is set, backwards or forwards              *)
(*   delete       -- the group and its dead letters are gone               *)
(*   fdelete      -- another group of the shard is deleted                 *)
(*                                                                         *)
(* A live group has a creation token `cre`: the term and index of the     *)
(* seek that created or last reset it, or, for a group created by its     *)
(* first acknowledgement or dead letter, the term its tracker was made in  *)
(* and the index it hydrated at (a tracker cannot know the index its first *)
(* entry will land at). The term keeps a seek dropped at a change of       *)
(* leader apart from a later seek placed at the same index. Every          *)
(* tracker-driven entry carries the `cre` of the group its tracker serves. *)
(* The shard keeps `lastDel`, the index of its latest delete of any group, *)
(* and keeps nothing per deleted name. Apply drops an entry whose `cre`    *)
(* differs from the live group's, or that names an absent group with a     *)
(* `cre` below `lastDel`. Within that, the cursor only moves forward. A    *)
(* delete of another group therefore leaves a live group's tracker alone.  *)
(*                                                                         *)
(* A group with nothing committed whose tracker hydrated before another    *)
(* group's delete has its entries dropped by that watermark. When the      *)
(* leader applies a dropped entry carrying its own tracker's `cre`, it     *)
(* drops the tracker (keeping only whether it handed anything out), and    *)
(* the next poll hydrates a fresh one from the table, past the delete.     *)
(* What the dropped entries settled is delivered again.                    *)
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
(* An acknowledgement is answered once every entry it proposed, and every *)
(* entry the leader decided before it, has committed and applied: the      *)
(* advance it closed, the finish of a redriven record, or the ack of a     *)
(* record above a gap. A new leader's tracker takes the table's acks       *)
(* above the cursor as settled, as it does its dead letters there.         *)
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
(*   TrackerHeard     -- no committed entry carrying the leader's current  *)
(*                       tracker's `cre` was dropped, so a tracker never   *)
(*                       serves on after the table threw its writes away   *)
(*                       (with NoSkip, the model shows only that a dropped *)
(*                       dead letter's record is never passed over).       *)
(*   AckedStaysSettled -- no record whose acknowledgement was answered is  *)
(*                       handed out again by a tracker of the same         *)
(*                       incarnation of the group, in any term.            *)
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
(*   RefreshOnDrop = FALSE      -- the leader keeps a tracker whose        *)
(*                                 entries apply drops, so a group with    *)
(*                                 nothing committed is silenced for the   *)
(*                                 rest of the term.                       *)
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
(*   DurableAckAhead = FALSE    -- an ack above a gap is kept in the       *)
(*                                 leader's tracker only and answered at   *)
(*                                 once, so the next leader hands it out   *)
(*                                 again.                                  *)
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
    RefreshOnDrop,
    DeadBeforeAdvance,
    KeepRedriven,
    DurableAckAhead

ASSUME
    /\ N \in Nat /\ N > 0 /\ MaxChanges \in Nat /\ MaxLog \in Nat
    /\ SeekTo \subseteq 1..(N + 1)
    /\ CreationCheck \in BOOLEAN /\ DeleteWatermark \in BOOLEAN
    /\ IfNewChecksTracker \in BOOLEAN /\ TermBoundTrackers \in BOOLEAN
    /\ HydrateFromApplied \in BOOLEAN /\ KeepRedriven \in BOOLEAN
    /\ WaitOwnProposals \in BOOLEAN /\ StaleByHyd \in BOOLEAN
    /\ DeadBeforeAdvance \in BOOLEAN /\ RefreshOnDrop \in BOOLEAN
    /\ DurableAckAhead \in BOOLEAN

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
    changes,
    waiting,   \* acknowledgements not yet answered, with the entries they wait on
    answered,  \* history: [o, cre] of every acknowledgement answered
    reissued   \* history: a record was handed out again after its answered ack

hist == <<waiting, answered, reissued>>
vars == <<log, tail, pend, leader, trk, acks, served, badIfNew, changes, hist>>

Symm == Permutations(Members)

Records == 1..N
Max2(a, b) == IF a >= b THEN a ELSE b
\* No tracker. A tracker dropped for its dropped entries keeps `any`, for if_new.
None == [none |-> TRUE, any |-> FALSE]
NoCre == [term |-> 0, at |-> 0, by |-> "none"]
\* The creation token of a seek or hydrate in this term at index i.
Cre(i, by) == [term |-> changes, at |-> i, by |-> by]

Full == log \o tail
Range(L) == {L[i] : i \in 1..Len(L)}

----------------------------------------------------------------------------
\* The group table.

\* `from` is where this incarnation started reading; `gfloor` is the index
\* of its last seek or delete whatever the knobs say, for the checks only.
\* `reset` is the index of the last seek, for the StaleByHyd rule only.
T0 == [exists |-> FALSE, cre |-> NoCre, cursor |-> 1, from |-> 1, reset |-> 0,
       lastDel |-> 0, gfloor |-> 0, dead |-> {}, red |-> {}, acked |-> {}]

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
                      !.dead = {o \in @ : o < e.to}, !.acked = {},
                      !.from = e.to, !.reset = i, !.gfloor = i]
      [] e.kind = "delete" ->
            [T0 EXCEPT !.lastDel = i, !.gfloor = i]
      [] e.kind = "fdelete" -> [T EXCEPT !.lastDel = i]
      [] Stale(T, e) -> T
      [] e.kind = "advance" ->
            [Live(T, e) EXCEPT !.cursor = Max2(T.cursor, e.to)]
      [] e.kind = "ack" ->
            [Live(T, e) EXCEPT !.acked = @ \cup {e.o}]
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

\* Apply drops `e` (seeks and deletes always apply).
Tracked == {"advance", "ack", "dead", "redrive", "finish"}

Dropped(T, e) == e.kind \in Tracked /\ Stale(T, e)

\* The `cre` of every entry of `L` that apply dropped.
RECURSIVE DroppedFrom(_, _, _, _)
DroppedFrom(T, L, i, S) ==
    IF i > Len(L) THEN S
    ELSE DroppedFrom(Step(T, L[i], i), L, i + 1,
                     IF Dropped(T, L[i]) THEN S \cup {L[i].cre} ELSE S)

\* Every entry of `L` that apply took.
RECURSIVE KeptFrom(_, _, _, _)
KeptFrom(T, L, i, S) ==
    IF i > Len(L) THEN S
    ELSE KeptFrom(Step(T, L[i], i), L, i + 1,
                  IF Dropped(T, L[i]) THEN S ELSE S \cup {L[i]})

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
    /\ waiting = {}
    /\ answered = {}
    /\ reissued = FALSE

\* A dead letter or an ack at or above the cursor starts settled: a leader
\* change before the advance over it leaves it there.
Rebuilt(k, T) ==
    LET D == {o \in T.dead \cup T.acked : o >= T.cursor}
        p == RunEnd(T.cursor, D)
        t == Tracker(k, T.cre, p, T.red)
    IN [t EXCEPT !.done = {o \in D : o >= p}]

\* A poll on a group with no tracker.
Hydrate ==
    /\ Lead.none
    /\ Ready
    /\ \E k \in Views :
        LET T == View(k) IN
        LET t == IF T.exists THEN Rebuilt(k, T)
                 ELSE Tracker(k, Cre(k, "hydrate"), 1, {})
        IN trk' = [trk EXCEPT ![leader] = [t EXCEPT !.any = Lead.any]]
    /\ UNCHANGED <<log, tail, pend, leader, acks, served, badIfNew, changes, hist>>

HandOut(o) ==
    LET t == Lead IN
    /\ ~t.none
    /\ o \notin t.out
    /\ \/ o \in t.owed
       \/ o >= t.pos /\ o \notin t.done
    /\ trk' = [trk EXCEPT ![leader] = [t EXCEPT !.out = @ \cup {o}, !.any = TRUE]]
    /\ served' = Max2(served, t.hyd)
    /\ reissued' = (reissued \/ \E a \in answered : a.o = o /\ a.cre = t.cre)
    /\ UNCHANGED <<log, tail, pend, leader, acks, badIfNew, changes, waiting, answered>>

\* The acknowledgement is answered once what it proposed has applied (see
\* Answer), and so has everything the leader decided before it: the code
\* waits for its own last proposal before any operation of the group. With
\* nothing to wait on it is answered at once. An ack that closes no run
\* and that the table's cursor has not passed proposes `ack(o)` of its own.
\* The tracker's position is not enough: a rebuilt tracker settles a dead
\* letter above the cursor with no advance, and its redrive is then below
\* the tracker only. The cursor is read with the leader's own entries
\* applied, as the code waits for them before any operation.
Ack(o) ==
    LET t == Lead
        t1 == [t EXCEPT !.out = @ \ {o}, !.owed = @ \ {o}]
        s == Settle(t1, o)
        fin == IF o \in t.owed
               THEN {[kind |-> "finish", o |-> o, hyd |-> t.hyd, cre |-> t.cre]}
               ELSE {}
        ahead == IF DurableAckAhead /\ s.adv = {} /\ o >= Fold(Full).cursor
                 THEN {[kind |-> "ack", o |-> o, hyd |-> t.hyd, cre |-> t.cre]}
                 ELSE {}
        es == s.adv \cup fin \cup ahead
        wait == es \cup pend \cup Range(tail)
    IN
    /\ ~t.none
    /\ o \in t.out
    /\ trk' = [trk EXCEPT ![leader] = s.t]
    /\ pend' = pend \cup es
    /\ acks' = [acks EXCEPT ![o] = @ \cup {t.hyd}]
    /\ IF wait = {}
       THEN /\ answered' = answered \cup {[o |-> o, cre |-> t.cre]}
            /\ UNCHANGED waiting
       ELSE /\ waiting' = waiting \cup {[o |-> o, cre |-> t.cre, es |-> wait]}
            /\ UNCHANGED answered
    /\ UNCHANGED <<log, tail, leader, served, badIfNew, changes, reissued>>

\* Every entry the acknowledgement waits on has committed and applied.
Answer(w) ==
    /\ w \in waiting
    /\ w.es \subseteq KeptFrom(T0, log, 1, {})
    /\ answered' = answered \cup {[o |-> w.o, cre |-> w.cre]}
    /\ waiting' = waiting \ {w}
    /\ UNCHANGED <<log, tail, pend, leader, trk, acks, served, badIfNew, changes,
                   reissued>>

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
    /\ UNCHANGED <<log, leader, acks, served, badIfNew, changes, hist>>

\* A tracker's decision reaches the log, in any order.
Propose(e) ==
    /\ e \in pend
    /\ Room
    /\ tail' = Append(tail, e)
    /\ pend' = pend \ {e}
    /\ UNCHANGED <<log, leader, trk, acks, served, badIfNew, changes, hist>>

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
    /\ UNCHANGED <<log, pend, leader, acks, served, badIfNew, changes, hist>>

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
                       Tracker(i, Cre(i, "seek"), to, T.red)]
    /\ tail' = Append(tail, [kind |-> "seek", to |-> to, hyd |-> i,
                              cre |-> Cre(i, "seek")])
    /\ badIfNew' = (badIfNew \/
                    (ifNew /\ served >= Fold(Full).gfloor))
    /\ UNCHANGED <<log, pend, leader, acks, served, changes, hist>>

Delete ==
    /\ Room
    /\ tail' = Append(tail, [kind |-> "delete", hyd |-> Len(Full) + 1])
    /\ trk' = [trk EXCEPT ![leader] = None]
    /\ UNCHANGED <<log, pend, leader, acks, served, badIfNew, changes, hist>>

\* Another group of the shard is deleted.
ForeignDelete ==
    /\ Room
    /\ tail' = Append(tail, [kind |-> "fdelete", hyd |-> Len(Full) + 1])
    /\ UNCHANGED <<log, pend, leader, trk, acks, served, badIfNew, changes, hist>>

\* The leader applies what commits. A dropped entry of its own tracker means
\* the table will never take that tracker's writes, so it starts over.
Commit ==
    LET e == Head(tail) IN
    /\ tail # <<>>
    /\ log' = Append(log, e)
    /\ tail' = Tail(tail)
    /\ trk' = IF /\ RefreshOnDrop
                 /\ ~Lead.none
                 /\ e.kind \in Tracked
                 /\ e.cre = Lead.cre
                 /\ Dropped(Fold(log), e)
              THEN [trk EXCEPT ![leader] = [None EXCEPT !.any = Lead.any]]
              ELSE trk
    /\ UNCHANGED <<pend, leader, acks, served, badIfNew, changes, hist>>

ChangeLeader(m) ==
    /\ changes < MaxChanges
    /\ \E k \in 0..Len(tail) :
        /\ log' = log \o SubSeq(tail, 1, k)
        /\ tail' = <<>>
        /\ acks' = [o \in Records |-> {h \in acks[o] : h <= Len(log) + k}]
        \* What the old leader dropped never commits.
        /\ waiting' = {w \in waiting : w.es \subseteq Range(log')}
    /\ pend' = {}
    /\ leader' = m
    /\ trk' = IF TermBoundTrackers THEN [x \in Members |-> None] ELSE trk
    /\ changes' = changes + 1
    /\ served' = -1
    /\ UNCHANGED <<badIfNew, answered, reissued>>

Next ==
    \/ Hydrate
    \/ \E o \in Records : HandOut(o) \/ Ack(o) \/ DeadLetter(o) \/ Redrive(o)
    \/ \E e \in pend : Propose(e)
    \/ \E w \in waiting : Answer(w)
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

TrackerHeard ==
    ~Lead.none => Lead.cre \notin DroppedFrom(T0, log, 1, {})

AckedStaysSettled == ~reissued

=============================================================================
