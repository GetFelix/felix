--------------------------- MODULE FelixAtomicCommit ---------------------------
(***************************************************************************)
(* An atomic commit on one shard: an event, a state update and an enqueue  *)
(* written together, and the question every reader has to be able to      *)
(* answer the same way: did this commit happen?                            *)
(*                                                                         *)
(* The broker writes a commit as one log record holding every part, so     *)
(* replication, truncation and the commit mark move over the whole thing   *)
(* or none of it. Each broker keeps three views of its log (stream readers,*)
(* the cache index, the queue view), and each applies committed records    *)
(* only. A reader may read any view on any broker at any time.             *)
(*                                                                         *)
(* NoPartialCommit: no view set on any broker shows some parts of a commit *)
(* and not others. CommitSurvives: a commit any reader can see is in the   *)
(* leader's log, across failover and promotion.                            *)
(*                                                                         *)
(* Two knobs make it fail, to show each rule is load-bearing:              *)
(*                                                                         *)
(*   SplitRecords  -- the commit is written as one record per part, so the *)
(*                    commit mark can stop between them.                   *)
(*   PartialApply  -- the views apply a record's parts one at a time, so a *)
(*                    reader can land between them.                        *)
(*                                                                         *)
(* Leadership is Raft-shaped and deliberately plain: a candidate whose log *)
(* is at least as up to date as a majority's takes the next generation,    *)
(* and the mark counts only records of the leader's own generation.        *)
(* FelixShard.tla models the lease, fence and promotion in full; this      *)
(* model asks only what a mixed batch adds on top of them.                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Brokers,
    MaxCommits,
    MaxGen,
    SplitRecords,
    PartialApply

ASSUME SplitRecords \in BOOLEAN /\ PartialApply \in BOOLEAN

VARIABLES
    log,        \* each broker's log: records [c |-> commit, g |-> generation, parts |-> set]
    gen,        \* the generation each broker last accepted
    leader,     \* who leads now
    mark,       \* each broker's commit mark (records below it are committed)
    applied,    \* each broker's views: records fully or partly applied, as << commit, part >> pairs
    next,       \* each broker's next record to apply
    done,       \* the parts of record `next` already applied (PartialApply only)
    commits     \* commits begun

vars == << log, gen, leader, mark, applied, next, done, commits >>

\* The parts a commit carries, in the order SplitRecords writes them.
PartList == << "event", "state", "queue" >>
Parts == { PartList[i] : i \in 1..Len(PartList) }

Majority == { Q \in SUBSET Brokers : 2 * Cardinality(Q) > Cardinality(Brokers) }

LastGen(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].g

\* Raft's "at least as up to date".
UpToDate(x, y) ==
    \/ LastGen(log[x]) > LastGen(log[y])
    \/ LastGen(log[x]) = LastGen(log[y]) /\ Len(log[x]) >= Len(log[y])

Init ==
    /\ log = [b \in Brokers |-> << >>]
    /\ gen = [b \in Brokers |-> 1]
    /\ leader \in Brokers
    /\ mark = [b \in Brokers |-> 0]
    /\ applied = [b \in Brokers |-> {}]
    /\ next = [b \in Brokers |-> 1]
    /\ done = [b \in Brokers |-> {}]
    /\ commits = 0

\* The leader appends a commit: one record, or one record per part.
Commit ==
    /\ commits < MaxCommits
    /\ LET c == commits + 1
           g == gen[leader]
           recs == IF SplitRecords
                      THEN [i \in 1..Len(PartList) |-> [c |-> c, g |-> g, parts |-> {PartList[i]}]]
                      ELSE << [c |-> c, g |-> g, parts |-> Parts] >>
       IN log' = [log EXCEPT ![leader] = @ \o recs]
    /\ commits' = commits + 1
    /\ UNCHANGED << gen, leader, mark, applied, next, done >>

\* A follower takes the next record it is missing, dropping any suffix that
\* disagrees with the leader. Committed records never disagree, so nothing a
\* view applied is dropped (CommitSurvives checks that).
Replicate(f) ==
    /\ f /= leader
    /\ gen[f] <= gen[leader]
    /\ LET l == log[leader]
           k == CHOOSE n \in 0..Len(l) :
                   /\ n <= Len(log[f])
                   /\ \A i \in 1..n : log[f][i] = l[i]
                   /\ (n < Len(log[f]) /\ n < Len(l)) => log[f][n + 1] /= l[n + 1]
       IN /\ k < Len(l)
          /\ log' = [log EXCEPT ![f] = SubSeq(l, 1, k + 1)]
          /\ gen' = [gen EXCEPT ![f] = gen[leader]]
          /\ mark' = [mark EXCEPT ![f] = IF mark[leader] < k + 1 THEN mark[leader] ELSE k + 1]
    /\ UNCHANGED << leader, applied, next, done, commits >>

\* The mark moves to the longest prefix a majority holds, counting only up to
\* a record of the leader's own generation.
AdvanceMark ==
    /\ \E n \in (mark[leader] + 1)..Len(log[leader]) :
        /\ log[leader][n].g = gen[leader]
        \* Split records are given the best a leader can do: its mark stops
        \* only at the end of a commit it wrote.
        /\ n < Len(log[leader]) => log[leader][n + 1].c /= log[leader][n].c
        /\ \E Q \in Majority : \A q \in Q :
              Len(log[q]) >= n /\ SubSeq(log[q], 1, n) = SubSeq(log[leader], 1, n)
        /\ mark' = [mark EXCEPT ![leader] = n]
    /\ UNCHANGED << log, gen, leader, applied, next, done, commits >>

\* A view applies the next committed record: whole, or one part at a time.
Apply(b) ==
    /\ next[b] <= mark[b]
    /\ LET r == log[b][next[b]] IN
       IF PartialApply
          THEN \E p \in r.parts \ done[b] :
                 /\ applied' = [applied EXCEPT ![b] = @ \cup {<< r.c, p >>}]
                 /\ IF done[b] \cup {p} = r.parts
                       THEN /\ next' = [next EXCEPT ![b] = @ + 1]
                            /\ done' = [done EXCEPT ![b] = {}]
                       ELSE /\ done' = [done EXCEPT ![b] = @ \cup {p}]
                            /\ UNCHANGED next
          ELSE /\ applied' = [applied EXCEPT ![b] = @ \cup {<< r.c, p >> : p \in r.parts}]
               /\ next' = [next EXCEPT ![b] = @ + 1]
               /\ UNCHANGED done
    /\ UNCHANGED << log, gen, leader, mark, commits >>

\* The leader is lost and a broker a majority votes for takes the next
\* generation. An uncommitted tail may or may not survive it.
Promote(b) ==
    LET g == 1 + CHOOSE m \in { gen[q] : q \in Brokers } : \A q \in Brokers : gen[q] <= m
    IN /\ b /= leader
       /\ g <= MaxGen
       /\ \E Q \in Majority :
           /\ b \in Q
           /\ \A q \in Q : UpToDate(b, q)
           /\ gen' = [q \in Brokers |-> IF q \in Q THEN g ELSE gen[q]]
       /\ leader' = b
       /\ UNCHANGED << log, mark, applied, next, done, commits >>

Next ==
    \/ Commit
    \/ AdvanceMark
    \/ \E b \in Brokers : Replicate(b) \/ Apply(b) \/ Promote(b)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------------

\* The parts of commit c a broker's views show.
Seen(b, c) == { p \in Parts : << c, p >> \in applied[b] }

NoPartialCommit ==
    \A b \in Brokers : \A c \in 1..commits : Seen(b, c) \in { {}, Parts }

\* Every part of a commit any view shows is in the leader's log.
CommitSurvives ==
    \A b \in Brokers : \A c \in 1..commits : \A p \in Seen(b, c) :
        \E i \in 1..Len(log[leader]) : log[leader][i].c = c /\ p \in log[leader][i].parts

=============================================================================
