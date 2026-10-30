----------------------------- MODULE FelixHeadTrim -----------------------------
(***************************************************************************)
(* A client-driven head trim on one shard: `trim_before(stream, before)`,  *)
(* conditional on the tail the caller expects. Designed in issue 1662, not *)
(* built yet.                                                              *)
(*                                                                         *)
(* The trim is a log record carrying the floor it asks for. It is appended *)
(* only at the tail the caller names, replicated like any record, and      *)
(* acknowledged when the commit mark passes it. Each broker keeps a        *)
(* logical floor, the largest floor among trim records at or below its     *)
(* commit offset, and refuses reads below it. Separately it deletes whole  *)
(* segments below min(floor, commit offset); `base` is where its oldest    *)
(* segment now starts, anywhere up to that bound, since segment            *)
(* boundaries differ between replicas.                                     *)
(*                                                                         *)
(* Two lease holders write to the stream, as felixd does: epoch 1, then    *)
(* epoch 2 after a takeover. Every write and trim names the tail its       *)
(* holder expects and carries its epoch. A new holder's first action is a  *)
(* fenced write, and it trims only once that write is acknowledged. A      *)
(* holder that learns the tail again reads the committed records from      *)
(* where it last was, or from the floor; a record from a newer epoch tells *)
(* it it has lost the lease. Readers see trim records and the epoch on     *)
(* them.                                                                   *)
(*                                                                         *)
(* NoReadBelowAckedTrim  -- the serving leader never serves a record below *)
(*                          an acknowledged trim's floor.                  *)
(* NothingLostAboveFloor -- the serving leader holds every acknowledged    *)
(*                          write at or above that floor.                  *)
(* NeverMoreThanAsked    -- no broker's floor is above what acknowledged   *)
(*                          trims asked for.                               *)
(* StaleTrimRefused      -- no log holds a trim from one holder after a    *)
(*                          write from a newer one.                        *)
(*                                                                         *)
(* Each knob removes one rule, and TLC finds the trace that rule prevents: *)
(*                                                                         *)
(*   Local             -- the floor is the leader's own state, not a       *)
(*                        record; a promoted follower never learns it.    *)
(*   FloorFromBase     -- a broker's readable start is where its segments  *)
(*                        happen to start, with no logical floor.          *)
(*   ApplyOnStore      -- a broker applies a trim's floor when it stores   *)
(*                        the record, before the record commits.          *)
(*   DeleteAboveCommit -- deletion follows the floor of every stored trim, *)
(*                        committed or not.                               *)
(*   Unconditional     -- a trim lands at whatever the tail is.            *)
(*   HiddenTrims       -- readers skip trim records, as the design first   *)
(*                        had it. A new holder that trimmed past its own   *)
(*                        fenced write leaves nothing a stale holder can   *)
(*                        see, and the stale holder writes and trims after *)
(*                        it.                                              *)
(*                                                                         *)
(* Leadership is Raft-shaped, as in FelixAtomicCommit.tla: a candidate at  *)
(* least as up to date as a majority takes the next generation and writes  *)
(* a generation-start record, and the mark counts only records of the      *)
(* leader's own generation. A leader serves once its mark covers what it   *)
(* inherited. FelixShard.tla models the lease, fence and promotion in      *)
(* full; this model asks what a trim adds on top of them.                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,
    MaxWrites,
    MaxTrims,
    MaxGen,
    Local,
    FloorFromBase,
    ApplyOnStore,
    DeleteAboveCommit,
    Unconditional,
    HiddenTrims

ASSUME /\ Local \in BOOLEAN /\ FloorFromBase \in BOOLEAN /\ ApplyOnStore \in BOOLEAN
       /\ DeleteAboveCommit \in BOOLEAN /\ Unconditional \in BOOLEAN
       /\ HiddenTrims \in BOOLEAN

VARIABLES
    log,        \* each broker's log; positions below `base` are deleted and kept only as history
    gen,        \* the generation each broker last accepted
    leader,     \* who leads now
    commit,     \* each broker's commit offset: positions 1..commit are committed
    base,       \* each broker's physical base: the first position it still holds
    localFloor, \* each broker's floor when it is the leader's own state (Local only)
    lease,      \* the newest holder epoch
    expected,   \* each holder's idea of the tail (the number of records)
    fenced,     \* whether a holder's write has been acknowledged to it
    retired,    \* whether a holder has seen it lost the lease
    acked,      \* committed writes, as [i |-> position, r |-> record]
    ackedFloor, \* the largest floor any committed trim asked for
    writes,     \* writes appended so far
    trims       \* trims taken so far

vars == << log, gen, leader, commit, base, localFloor, lease, expected, fenced,
           retired, acked, ackedFloor, writes, trims >>

Holders == {1, 2}

\* A record: generation, kind ("w" write, "t" trim, "s" generation start),
\* holder epoch (0 for a start record) and, for a trim, the floor it asks for.
Write(g, h) == [g |-> g, k |-> "w", h |-> h, f |-> 0]
TrimRec(g, h, f) == [g |-> g, k |-> "t", h |-> h, f |-> f]
StartRec(g) == [g |-> g, k |-> "s", h |-> 0, f |-> 0]

Min(x, y) == IF x <= y THEN x ELSE y
Max(x, y) == IF x >= y THEN x ELSE y
MaxOf(S) == CHOOSE m \in S : \A s \in S : s <= m

Majority == { Q \in SUBSET Brokers : 2 * Cardinality(Q) > Cardinality(Brokers) }

Symm == Permutations(Brokers)

LastGen(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].g

UpToDate(x, y) ==
    \/ LastGen(log[x]) > LastGen(log[y])
    \/ LastGen(log[x]) = LastGen(log[y]) /\ Len(log[x]) >= Len(log[y])

\* The floors asked for by trim records b still holds, from lo to hi.
FloorsIn(b, lo, hi) == { log[b][i].f : i \in { j \in lo..hi : log[b][j].k = "t" } }

StoredFloor(b) == MaxOf({1} \cup FloorsIn(b, base[b], Len(log[b])))
CommittedFloor(b) == MaxOf({1} \cup FloorsIn(b, base[b], commit[b]))

\* Reads below the floor are refused. Records below it may still be on disk.
Floor(b) ==
    IF Local THEN localFloor[b]
    ELSE IF ApplyOnStore THEN StoredFloor(b)
    ELSE CommittedFloor(b)

ReadableStart(b) == IF FloorFromBase THEN base[b] ELSE Max(base[b], Floor(b))

\* Segments wholly below this may be deleted. Floor(b) is at most commit + 1
\* when only committed trims count; the cap holds either way.
DeleteBound(b) ==
    IF DeleteAboveCommit THEN StoredFloor(b) ELSE Min(Floor(b), commit[b] + 1)

\* The leader serves reads once its mark covers every record it inherited,
\* which its generation-start record committing does.
Serving(b) ==
    /\ b = leader
    /\ \A i \in 1..Len(log[b]) : log[b][i].g < gen[b] => i <= commit[b]

Init ==
    /\ log = [b \in Brokers |-> << >>]
    /\ gen = [b \in Brokers |-> 1]
    /\ leader \in Brokers
    /\ commit = [b \in Brokers |-> 0]
    /\ base = [b \in Brokers |-> 1]
    /\ localFloor = [b \in Brokers |-> 1]
    /\ lease = 1
    /\ expected = [h \in Holders |-> 0]
    /\ fenced = [h \in Holders |-> FALSE]
    /\ retired = [h \in Holders |-> FALSE]
    /\ acked = {}
    /\ ackedFloor = 1
    /\ writes = 0
    /\ trims = 0

Active(h) == h <= lease /\ ~retired[h]

\* The lease passes to the second holder. The first is not told.
TakeOver ==
    /\ lease = 1
    /\ lease' = 2
    /\ UNCHANGED << log, gen, leader, commit, base, localFloor, expected, fenced,
                    retired, acked, ackedFloor, writes, trims >>

\* A holder writes at the tail it expects, as publish_if does.
HolderWrite(h) ==
    /\ Active(h)
    /\ writes < MaxWrites
    /\ Len(log[leader]) = expected[h]
    /\ log' = [log EXCEPT ![leader] = Append(@, Write(gen[leader], h))]
    /\ writes' = writes + 1
    /\ UNCHANGED << gen, leader, commit, base, localFloor, lease, expected, fenced,
                    retired, acked, ackedFloor, trims >>

\* A holder trims before f. The record lands at offset expected + 1, and f
\* may be at most that. A floor at or below the current one changes nothing.
Trim(h, f) ==
    /\ Active(h)
    /\ fenced[h]
    /\ trims < MaxTrims
    /\ Unconditional \/ Len(log[leader]) = expected[h]
    /\ f > Floor(leader)
    /\ f <= Min(expected[h], Len(log[leader])) + 1
    /\ trims' = trims + 1
    /\ IF Local
          THEN /\ localFloor' = [localFloor EXCEPT ![leader] = f]
               /\ ackedFloor' = Max(ackedFloor, f)
               /\ UNCHANGED log
          ELSE /\ log' = [log EXCEPT ![leader] = Append(@, TrimRec(gen[leader], h, f))]
               /\ UNCHANGED << localFloor, ackedFloor >>
    /\ UNCHANGED << gen, leader, commit, base, lease, expected, fenced, retired,
                    acked, writes >>

\* A holder reads the serving leader's committed records from where it last
\* was, or from the readable start if that is later. A record from a newer
\* epoch tells it it lost the lease; otherwise it takes the committed tail
\* as the one it expects. Start records carry no epoch.
Refresh(h) ==
    /\ Active(h)
    /\ Serving(leader)
    /\ commit[leader] > expected[h]
    /\ LET l == log[leader]
           newer == { i \in Max(expected[h] + 1, ReadableStart(leader))..commit[leader] :
                        l[i].h > h /\ (l[i].k = "w" \/ ~HiddenTrims) }
       IN IF newer /= {}
             THEN /\ retired' = [retired EXCEPT ![h] = TRUE]
                  /\ UNCHANGED expected
             ELSE /\ expected' = [expected EXCEPT ![h] = commit[leader]]
                  /\ UNCHANGED retired
    /\ UNCHANGED << log, gen, leader, commit, base, localFloor, lease, fenced,
                    acked, ackedFloor, writes, trims >>

\* A follower takes the next record it is missing, dropping any suffix that
\* disagrees with the leader. One that needs a record the leader has
\* deleted starts again at the leader's base: records from there on, and
\* the floor once its commit offset passes the trim that set it.
Replicate(f) ==
    /\ f /= leader
    /\ gen[f] <= gen[leader]
    /\ LET l == log[leader]
           k == CHOOSE n \in 0..Len(l) :
                   /\ n <= Len(log[f])
                   /\ \A i \in 1..n : log[f][i] = l[i]
                   /\ (n < Len(log[f]) /\ n < Len(l)) => log[f][n + 1] /= l[n + 1]
       IN /\ k < Len(l)
          /\ IF k + 1 < base[leader]
                THEN /\ log' = [log EXCEPT ![f] = SubSeq(l, 1, base[leader])]
                     /\ base' = [base EXCEPT ![f] = base[leader]]
                     /\ commit' = [commit EXCEPT ![f] = Max(@, Min(commit[leader], base[leader]))]
                ELSE /\ log' = [log EXCEPT ![f] = SubSeq(l, 1, k + 1)]
                     /\ commit' = [commit EXCEPT ![f] = Max(@, Min(commit[leader], k + 1))]
                     /\ UNCHANGED base
          /\ gen' = [gen EXCEPT ![f] = gen[leader]]
    /\ UNCHANGED << leader, localFloor, lease, expected, fenced, retired, acked,
                    ackedFloor, writes, trims >>

\* The mark moves to the longest prefix a majority holds, counting only up to
\* a record of the leader's own generation. The leader applies a trim's floor
\* in the same step, before it answers. The holders whose requests this
\* leader took are answered with where their record landed.
AdvanceMark ==
    /\ \E n \in (commit[leader] + 1)..Len(log[leader]) :
        LET l == log[leader]
            new == (commit[leader] + 1)..n
            mine(h) == { i \in new : l[i].h = h /\ l[i].g = gen[leader] }
        IN /\ l[n].g = gen[leader]
           /\ \E Q \in Majority : \A q \in Q :
                 Len(log[q]) >= n /\ SubSeq(log[q], 1, n) = SubSeq(l, 1, n)
           /\ commit' = [commit EXCEPT ![leader] = n]
           /\ acked' = acked \cup { [i |-> i, r |-> l[i]] : i \in { j \in new : l[j].k = "w" } }
           /\ ackedFloor' = MaxOf({ackedFloor} \cup { l[i].f : i \in { j \in new : l[j].k = "t" } })
           /\ expected' = [h \in Holders |-> IF mine(h) = {} THEN expected[h] ELSE MaxOf(mine(h))]
           /\ fenced' = [h \in Holders |-> fenced[h] \/ \E i \in mine(h) : l[i].k = "w"]
    /\ UNCHANGED << log, gen, leader, base, localFloor, lease, retired, writes, trims >>

\* A broker deletes whole segments below its bound, as far as its segment
\* boundaries allow: any position up to the bound.
Delete(b) ==
    /\ \E p \in (base[b] + 1)..DeleteBound(b) :
        base' = [base EXCEPT ![b] = p]
    /\ UNCHANGED << log, gen, leader, commit, localFloor, lease, expected, fenced,
                    retired, acked, ackedFloor, writes, trims >>

\* The leader is lost and a broker a majority votes for takes the next
\* generation and writes its start record. An uncommitted tail may or may
\* not survive it; a follower drops what disagrees when it next replicates.
Promote(b) ==
    LET g == 1 + MaxOf({ gen[q] : q \in Brokers })
    IN /\ b /= leader
       /\ g <= MaxGen
       /\ \E Q \in Majority :
           /\ b \in Q
           /\ \A q \in Q : UpToDate(b, q)
           /\ gen' = [q \in Brokers |-> IF q \in Q THEN g ELSE gen[q]]
       /\ leader' = b
       /\ log' = [log EXCEPT ![b] = Append(@, StartRec(g))]
       /\ UNCHANGED << commit, base, localFloor, lease, expected, fenced, retired,
                       acked, ackedFloor, writes, trims >>

Next ==
    \/ TakeOver
    \/ AdvanceMark
    \/ \E h \in Holders : HolderWrite(h) \/ Refresh(h)
    \/ \E h \in Holders, f \in 2..(MaxWrites + MaxTrims + MaxGen) : Trim(h, f)
    \/ \E b \in Brokers : Replicate(b) \/ Delete(b) \/ Promote(b)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------------

TypeOK ==
    /\ leader \in Brokers
    /\ \A b \in Brokers :
        /\ commit[b] <= Len(log[b])
        /\ base[b] \in 1..(commit[b] + 1)
    /\ lease \in Holders
    /\ ackedFloor \in Nat

NoReadBelowAckedTrim ==
    \A b \in Brokers : Serving(b) => ReadableStart(b) >= ackedFloor

NothingLostAboveFloor ==
    \A b \in Brokers : Serving(b) =>
        \A a \in acked : a.i >= ackedFloor =>
            /\ base[b] <= a.i
            /\ a.i <= Len(log[b])
            /\ log[b][a.i] = a.r

NeverMoreThanAsked ==
    \A b \in Brokers : Floor(b) <= ackedFloor

StaleTrimRefused ==
    \A b \in Brokers : \A i, j \in 1..Len(log[b]) :
        (j < i /\ log[b][i].k = "t" /\ log[b][j].k = "w") => log[b][j].h <= log[b][i].h

=============================================================================
