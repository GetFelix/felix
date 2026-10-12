------------------------------ MODULE FelixGroups ------------------------------
(***************************************************************************)
(* A stream shard's consumer-group state across crashes and promotions:   *)
(* the dead-letter log and the cursor log, two logs on every replica,     *)
(* each flushed on its own.                                                *)
(*                                                                         *)
(* A group passes a record by finishing it (Ack) or by giving up on it,    *)
(* which writes a dead letter first. Each cursor record says what it needs *)
(* of the dead-letter log beneath it: the log's length when it was written *)
(* (`e`, the broker's `dlq_end`) and the generation that wrote the entry   *)
(* at that length (`dg`). The leader syncs its dead letters before a       *)
(* cursor that needs them. Before a broker serves the shard, after a crash *)
(* or a promotion, each group whose latest cursor no longer stands on the  *)
(* dead-letter log it has is put back to its latest record that does, or  *)
(* to its origin: where it was last placed needing no dead letter.        *)
(*                                                                         *)
(* A promotion fences both logs in one round on a majority, takes the      *)
(* dead-letter log of the answer furthest ahead (dead letters first, then *)
(* cursors) and merges that replica's cursors into its own, each group    *)
(* going to the further position. When the round does not reach a        *)
(* majority it serves its own pair.                                        *)
(*                                                                         *)
(* NoSkip: on the serving leader, every record a group's cursor has passed *)
(* was finished by a consumer or is listed as that group's dead letter.   *)
(*                                                                         *)
(* Knobs, each off in a twin that TLC finds a skip in:                    *)
(*   CheckCursors   -- cursors are checked before the shard is served.    *)
(*   DependencyGen  -- the check compares the generation at `e`, not only *)
(*                     the length.                                         *)
(*   KeepGroups     -- a group with no standing cursor goes to its origin *)
(*                     rather than being dropped, which a seek `if_new`   *)
(*                     would recreate at the tail.                        *)
(* SplitSources takes the dead letters from the replica furthest ahead on *)
(* them and replaces the cursors with the one furthest ahead on cursors.  *)
(*                                                                         *)
(* FelixShard.tla models the fence, the lease and the records' log in     *)
(* full. This module takes from it only that a promotion fences a majority *)
(* and that a replica refuses a leader older than one it accepted.        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Replicas,
    G,              \* groups, 1..G
    N,              \* records in the stream, offsets 0..N-1
    MaxGen,
    MaxCrashes,
    MaxCursors,     \* bound on a cursor log's commits
    CheckCursors,
    DependencyGen,
    KeepGroups,
    SplitSources

ASSUME /\ CheckCursors \in BOOLEAN /\ DependencyGen \in BOOLEAN
       /\ KeepGroups \in BOOLEAN /\ SplitSources \in BOOLEAN

Groups == 1..G

VARIABLES
    dl,         \* each replica's dead-letter log: [grp, o, g]
    cur,        \* each replica's cursor log: [grp, p, e, dg, org, g]
    dlDur,      \* how much of each dead-letter log is on disk
    curDur,     \* how much of each cursor log is on disk
    gen,        \* the generation each replica last accepted
    leader,
    serving,    \* the leader has checked its cursors and serves
    processed,  \* << group, offset >> pairs a consumer finished
    crashes

vars == << dl, cur, dlDur, curDur, gen, leader, serving, processed, crashes >>

Max(S) == CHOOSE m \in S : \A x \in S : x <= m
Min2(a, b) == IF a < b THEN a ELSE b
Majority == { Q \in SUBSET Replicas : 2 * Cardinality(Q) > Cardinality(Replicas) }

LabelAt(l, e) == IF e = 0 THEN 0 ELSE l[e].g
LastLabel(l) == LabelAt(l, Len(l))
CommonPrefix(a, b) ==
    Max({ k \in 0..Min2(Len(a), Len(b)) : SubSeq(a, 1, k) = SubSeq(b, 1, k) })

\* Whether a cursor record stands on dead-letter log `d`.
StandsOn(d, rec) ==
    \/ rec.e = 0
    \/ /\ Len(d) >= rec.e
       /\ DependencyGen => LabelAt(d, rec.e) = rec.dg

RecsIn(c, grp) == { i \in 1..Len(c) : c[i].grp = grp }
LatestIn(c, grp) == c[Max(RecsIn(c, grp))]
PosIn(c, grp) == IF RecsIn(c, grp) = {} THEN 0 ELSE LatestIn(c, grp).p
OriginIn(c, grp) == IF RecsIn(c, grp) = {} THEN 0 ELSE LatestIn(c, grp).org
DeadIn(d, grp) == { d[i].o : i \in { j \in 1..Len(d) : d[j].grp = grp } }

\* What checking appends for `grp`: nothing when its latest record stands,
\* else its latest record that does, else its origin (or the tail, when a
\* group with nothing standing is dropped and recreated there).
FixFor(d, c, grp, g) ==
    IF RecsIn(c, grp) = {} \/ StandsOn(d, LatestIn(c, grp))
    THEN << >>
    ELSE LET ok == { i \in RecsIn(c, grp) : StandsOn(d, c[i]) }
             at == IF KeepGroups THEN OriginIn(c, grp) ELSE N
         IN IF ok /= {}
            THEN << [c[Max(ok)] EXCEPT !.g = g] >>
            ELSE << [grp |-> grp, p |-> at, e |-> 0, dg |-> 0, org |-> at, g |-> g] >>

RECURSIVE FixesFrom(_, _, _, _)
FixesFrom(d, c, k, g) ==
    IF k > G THEN << >> ELSE FixFor(d, c, k, g) \o FixesFrom(d, c, k + 1, g)

Normalize(d, c, g) == IF CheckCursors THEN c \o FixesFrom(d, c, 1, g) ELSE c

\* Taking a replica's cursors: each group to the further of the two.
TakeFor(own, theirs, grp, g) ==
    IF RecsIn(theirs, grp) /= {}
       /\ (RecsIn(own, grp) = {} \/ PosIn(own, grp) < PosIn(theirs, grp))
    THEN << [LatestIn(theirs, grp) EXCEPT !.g = g] >>
    ELSE << >>

RECURSIVE TakesFrom(_, _, _, _)
TakesFrom(own, theirs, k, g) ==
    IF k > G THEN << >> ELSE TakeFor(own, theirs, k, g) \o TakesFrom(own, theirs, k + 1, g)

Merge(own, theirs, g) == own \o TakesFrom(own, theirs, 1, g)

\* How far ahead a replica's logs are, as a fence answer orders them.
DLKey(r) == << LastLabel(dl[r]), Len(dl[r]) >>
CurKey(r) == << IF Len(cur[r]) = 0 THEN 0 ELSE cur[r][Len(cur[r])].g, Len(cur[r]) >>
Gt(x, y) == x[1] > y[1] \/ (x[1] = y[1] /\ x[2] > y[2])
JointGt(x, y) == Gt(DLKey(x), DLKey(y)) \/ (DLKey(x) = DLKey(y) /\ Gt(CurKey(x), CurKey(y)))

Pos(r, grp) == PosIn(cur[r], grp)

Init ==
    /\ dl = [r \in Replicas |-> << >>]
    /\ cur = [r \in Replicas |-> << >>]
    /\ dlDur = [r \in Replicas |-> 0]
    /\ curDur = [r \in Replicas |-> 0]
    /\ gen = [r \in Replicas |-> 1]
    /\ leader \in Replicas
    /\ serving = TRUE
    /\ processed = {}
    /\ crashes = 0

\* A consumer finishes the group's next record.
Ack(grp) ==
    LET o == Pos(leader, grp) IN
    /\ serving
    /\ o < N
    /\ << grp, o >> \notin processed
    /\ processed' = processed \cup {<< grp, o >>}
    /\ UNCHANGED << dl, cur, dlDur, curDur, gen, leader, serving, crashes >>

\* The group gives up on its next record: the dead letter is written first.
DeadLetter(grp) ==
    LET o == Pos(leader, grp) IN
    /\ serving
    /\ o < N
    /\ o \notin DeadIn(dl[leader], grp)
    /\ dl' = [dl EXCEPT ![leader] = Append(@, [grp |-> grp, o |-> o, g |-> gen[leader]])]
    /\ UNCHANGED << cur, dlDur, curDur, gen, leader, serving, processed, crashes >>

\* The cursor passes a record that was finished or dead-lettered, needing
\* the dead-letter log as it is now, which is synced first.
Advance(grp) ==
    LET o == Pos(leader, grp)
        e == Len(dl[leader])
    IN /\ serving
       /\ o < N
       /\ << grp, o >> \in processed \/ o \in DeadIn(dl[leader], grp)
       /\ Len(cur[leader]) < MaxCursors
       /\ cur' = [cur EXCEPT ![leader] = Append(@,
              [grp |-> grp, p |-> o + 1, e |-> e, dg |-> LastLabel(dl[leader]),
               org |-> IF e = 0 THEN o + 1 ELSE OriginIn(cur[leader], grp),
               g |-> gen[leader]])]
       /\ dlDur' = [dlDur EXCEPT ![leader] = e]
       /\ UNCHANGED << dl, curDur, gen, leader, serving, processed, crashes >>

\* Each log reaches disk on its own.
FlushDeadLetters(r) ==
    /\ dlDur' = [dlDur EXCEPT ![r] = Len(dl[r])]
    /\ UNCHANGED << dl, cur, curDur, gen, leader, serving, processed, crashes >>

FlushCursors(r) ==
    /\ curDur' = [curDur EXCEPT ![r] = Len(cur[r])]
    /\ UNCHANGED << dl, cur, dlDur, gen, leader, serving, processed, crashes >>

\* A crash keeps what each log had on disk.
Crash(r) ==
    /\ crashes < MaxCrashes
    /\ dl' = [dl EXCEPT ![r] = SubSeq(dl[r], 1, dlDur[r])]
    /\ cur' = [cur EXCEPT ![r] = SubSeq(cur[r], 1, curDur[r])]
    /\ serving' = IF r = leader THEN FALSE ELSE serving
    /\ crashes' = crashes + 1
    /\ UNCHANGED << dlDur, curDur, gen, leader, processed >>

\* The leader comes back and checks its cursors before serving.
Recover ==
    LET c == Normalize(dl[leader], cur[leader], gen[leader]) IN
    /\ ~serving
    /\ cur' = [cur EXCEPT ![leader] = c]
    /\ curDur' = [curDur EXCEPT ![leader] = IF c = cur[leader] THEN @ ELSE Len(c)]
    /\ serving' = TRUE
    /\ UNCHANGED << dl, dlDur, gen, leader, processed, crashes >>

\* The leader ships its dead letters to a follower, which takes the
\* leader's log over its own.
ShipDeadLetters(f) ==
    /\ serving
    /\ f /= leader
    /\ gen[f] <= gen[leader]
    /\ dl[f] /= dl[leader]
    /\ dl' = [dl EXCEPT ![f] = dl[leader]]
    /\ dlDur' = [dlDur EXCEPT ![f] = Min2(@, CommonPrefix(dl[f], dl[leader]))]
    /\ gen' = [gen EXCEPT ![f] = gen[leader]]
    /\ UNCHANGED << cur, curDur, leader, serving, processed, crashes >>

\* And its cursors, only to a follower that holds every dead letter.
ShipCursors(f) ==
    /\ serving
    /\ f /= leader
    /\ gen[f] <= gen[leader]
    /\ dl[f] = dl[leader]
    /\ cur[f] /= cur[leader]
    /\ cur' = [cur EXCEPT ![f] = cur[leader]]
    /\ curDur' = [curDur EXCEPT ![f] = Min2(@, CommonPrefix(cur[f], cur[leader]))]
    /\ gen' = [gen EXCEPT ![f] = gen[leader]]
    /\ UNCHANGED << dl, dlDur, leader, serving, processed, crashes >>

\* `b` takes `newDl` and `newCur` as its pair, checks its cursors and serves.
Serve(b, g, newDl, newCur) ==
    LET c == Normalize(newDl, newCur, g) IN
    /\ dl' = [dl EXCEPT ![b] = newDl]
    /\ dlDur' = [dlDur EXCEPT ![b] = Min2(@, CommonPrefix(dl[b], newDl))]
    /\ cur' = [cur EXCEPT ![b] = c]
    /\ curDur' = [curDur EXCEPT ![b] = IF c = cur[b] THEN @ ELSE Len(c)]
    /\ leader' = b
    /\ serving' = TRUE

\* The leader is lost or moved; `b` is promoted at the next generation and
\* fences a majority with it.
Promote(b) ==
    LET g == Max({ gen[r] : r \in Replicas }) + 1 IN
    /\ g <= MaxGen
    /\ \E Q \in Majority :
        /\ b \in Q
        /\ gen' = [r \in Replicas |-> IF r \in Q THEN g ELSE gen[r]]
        /\ \/ \* The group round reached the majority.
              /\ ~SplitSources
              /\ \E s \in Q :
                   /\ \A q \in Q : ~JointGt(q, s)
                   /\ LET take == JointGt(s, b) IN
                      Serve(b, g,
                            IF take /\ Gt(DLKey(s), DLKey(b)) THEN dl[s] ELSE dl[b],
                            IF take /\ CurKey(s) /= CurKey(b)
                               THEN Merge(cur[b], cur[s], g) ELSE cur[b])
           \/ /\ SplitSources
              /\ \E s1, s2 \in Q :
                   /\ \A q \in Q : ~Gt(DLKey(q), DLKey(s1))
                   /\ \A q \in Q : ~Gt(CurKey(q), CurKey(s2))
                   /\ Serve(b, g,
                            IF Gt(DLKey(s1), DLKey(b)) THEN dl[s1] ELSE dl[b],
                            IF Gt(CurKey(s2), CurKey(b)) THEN cur[s2] ELSE cur[b])
           \/ \* It did not: the leader serves its own pair.
              Serve(b, g, dl[b], cur[b])
    /\ UNCHANGED << processed, crashes >>

Next ==
    \/ \E grp \in Groups : Ack(grp) \/ DeadLetter(grp) \/ Advance(grp)
    \/ \E r \in Replicas : FlushDeadLetters(r) \/ FlushCursors(r) \/ Crash(r)
    \/ Recover
    \/ \E f \in Replicas : ShipDeadLetters(f) \/ ShipCursors(f)
    \/ \E b \in Replicas : Promote(b)

Spec == Init /\ [][Next]_vars

NoSkip ==
    serving =>
        \A grp \in Groups : \A o \in 0..(N - 1) :
            o < Pos(leader, grp) =>
                << grp, o >> \in processed \/ o \in DeadIn(dl[leader], grp)

=============================================================================
