----------------------------- MODULE FelixHeadTrim -----------------------------
(***************************************************************************)
(* A client-driven head trim on one shard: `trim_before(stream, before)`,  *)
(* conditional on the tail the caller expects. Designed in issue 1662, not *)
(* built yet.                                                              *)
(*                                                                         *)
(* The trim is a log record carrying the floor it asks for and the         *)
(* caller's tag. It is appended only at the tail the caller names,         *)
(* replicated like any record, and acknowledged once the commit mark       *)
(* passes it. Each broker writes a head entry (record offset, floor, tag)  *)
(* for every trim it stores; the entry outlives the record's deletion.     *)
(* Its floor is the largest among entries at or below its commit offset,  *)
(* and reads below it are refused with CursorTooOld, which carries the tag *)
(* of the entry that set the floor. Readers skip trim records. A broker    *)
(* deletes whole segments below min(floor, commit offset), never the       *)
(* active one; `base` is where its oldest segment now starts, anywhere up  *)
(* to that bound, since segment boundaries differ between replicas.        *)
(*                                                                         *)
(* Two lease holders write to the stream, as felixd does: epoch 1, then    *)
(* epoch 2 after a takeover nobody tells epoch 1 about. Every write and    *)
(* trim names the tail its holder expects and carries its epoch. A new     *)
(* holder's first action is a fenced write, and it trims only once that    *)
(* write is acknowledged. A holder learns the tail again by reading the    *)
(* committed records from its own cursor: a write from a newer epoch, or a *)
(* CursorTooOld whose tag is newer, tells it it has lost the lease. It     *)
(* never takes the tail from a refusal. A trim at or below the floor is a  *)
(* no-op that still checks the tail, and its answer names the tail.        *)
(*                                                                         *)
(* A broker can crash and come back with its commit offset read back       *)
(* behind, never ahead. A deposed leader keeps taking writes and trims     *)
(* until a newer generation reaches it. A commit, the answer to the client *)
(* and the client acting on it are separate steps.                         *)
(*                                                                         *)
(* NoReadBelowAckedTrim  -- the serving leader never serves a record below *)
(*                          an acknowledged trim's floor.                  *)
(* NothingLostAboveFloor -- the serving leader holds every acknowledged    *)
(*                          write at or above that floor.                  *)
(* NeverMoreThanAsked    -- no broker's floor is above what acknowledged   *)
(*                          trims asked for.                               *)
(* StaleTrimRefused      -- no committed write or trim from one holder     *)
(*                          follows a committed write from a newer one.    *)
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
(*   Untagged          -- CursorTooOld carries no tag, as the design first *)
(*                        had it; a holder resumes at the floor.          *)
(*   NoOpUnchecked     -- a no-op trim answers with the tail without       *)
(*                        checking it.                                     *)
(*   Retention         -- retention deletes past the floor, up to the      *)
(*                        commit offset.                                   *)
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
    MaxCrashes,
    Local,
    FloorFromBase,
    ApplyOnStore,
    DeleteAboveCommit,
    Unconditional,
    Untagged,
    NoOpUnchecked,
    Retention

ASSUME /\ Local \in BOOLEAN /\ FloorFromBase \in BOOLEAN /\ ApplyOnStore \in BOOLEAN
       /\ DeleteAboveCommit \in BOOLEAN /\ Unconditional \in BOOLEAN
       /\ Untagged \in BOOLEAN /\ NoOpUnchecked \in BOOLEAN /\ Retention \in BOOLEAN

VARIABLES
    log,        \* each broker's log; positions below `base` are deleted and kept only as history
    gen,        \* the generation each broker last accepted
    leads,      \* whether a broker believes it leads at its generation
    commit,     \* each broker's commit offset: positions 1..commit are committed
    base,       \* each broker's physical base: the first position it still holds
    head,       \* each broker's head entries, [o |-> offset, f |-> floor, h |-> tag]
    localFloor, \* each broker's floor when it is the leader's own state (Local only)
    lease,      \* the newest holder epoch
    expected,   \* each holder's idea of the tail (the number of records)
    fenced,     \* whether a holder's write has been acknowledged to it
    retired,    \* whether a holder has seen it lost the lease
    acked,      \* committed writes, as [i |-> position, r |-> record]
    ackedFloor, \* the largest floor any committed trim asked for
    writes,     \* writes appended so far
    trims,      \* trims taken so far, no-ops included
    crashes     \* broker crashes so far

vars == << log, gen, leads, commit, base, head, localFloor, lease, expected, fenced,
           retired, acked, ackedFloor, writes, trims, crashes >>

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
TopGen == MaxOf({ gen[q] : q \in Brokers })

UpToDate(x, y) ==
    \/ LastGen(log[x]) > LastGen(log[y])
    \/ LastGen(log[x]) = LastGen(log[y]) /\ Len(log[x]) >= Len(log[y])

\* The head entries for the trim records of l from lo to hi.
EntriesOf(l, lo, hi) ==
    { [o |-> i, f |-> l[i].f, h |-> l[i].h] : i \in { j \in lo..hi : l[j].k = "t" } }

Committed(b) == { e \in head[b] : e.o <= commit[b] }
Applied(b) == IF ApplyOnStore THEN head[b] ELSE Committed(b)
FloorOf(S) == MaxOf({1} \cup { e.f : e \in S })

\* Reads below the floor are refused. Records below it may still be on disk.
Floor(b) == IF Local THEN localFloor[b] ELSE FloorOf(Applied(b))

\* The tag CursorTooOld carries: that of the first entry to set the floor.
FloorTag(b) ==
    LET top == { e \in Applied(b) : e.f = Floor(b) }
    IN IF Local \/ Untagged \/ top = {} THEN 0
       ELSE (CHOOSE e \in top : \A x \in top : e.o <= x.o).h

ReadableStart(b) == IF FloorFromBase THEN base[b] ELSE Max(base[b], Floor(b))

\* Segments wholly below this may be deleted. The active segment never is,
\* so the last record stays.
DeleteBound(b) ==
    Min(Len(log[b]),
        IF Retention THEN commit[b] + 1
        ELSE IF DeleteAboveCommit THEN FloorOf(head[b])
        ELSE Min(Floor(b), commit[b] + 1))

\* The newest leader serves reads once its mark covers every record it
\* inherited, which its generation-start record committing does.
Serving(b) ==
    /\ leads[b]
    /\ gen[b] = TopGen
    /\ \A i \in 1..Len(log[b]) : log[b][i].g < gen[b] => i <= commit[b]

Init ==
    /\ log = [b \in Brokers |-> << >>]
    /\ gen = [b \in Brokers |-> 1]
    /\ \E l \in Brokers : leads = [b \in Brokers |-> b = l]
    /\ commit = [b \in Brokers |-> 0]
    /\ base = [b \in Brokers |-> 1]
    /\ head = [b \in Brokers |-> {}]
    /\ localFloor = [b \in Brokers |-> 1]
    /\ lease = 1
    /\ expected = [h \in Holders |-> 0]
    /\ fenced = [h \in Holders |-> FALSE]
    /\ retired = [h \in Holders |-> FALSE]
    /\ acked = {}
    /\ ackedFloor = 1
    /\ writes = 0
    /\ trims = 0
    /\ crashes = 0

Active(h) == h <= lease /\ ~retired[h]

\* The lease passes to the second holder. The first is not told.
TakeOver ==
    /\ lease = 1
    /\ lease' = 2
    /\ UNCHANGED << log, gen, leads, commit, base, head, localFloor, expected, fenced,
                    retired, acked, ackedFloor, writes, trims, crashes >>

\* A holder writes at the tail it expects, as publish_if does, on any broker
\* that believes it leads.
HolderWrite(h, b) ==
    /\ Active(h)
    /\ leads[b]
    /\ writes < MaxWrites
    /\ Len(log[b]) = expected[h]
    /\ log' = [log EXCEPT ![b] = Append(@, Write(gen[b], h))]
    /\ writes' = writes + 1
    /\ UNCHANGED << gen, leads, commit, base, head, localFloor, lease, expected, fenced,
                    retired, acked, ackedFloor, trims, crashes >>

\* A holder trims before f, above the floor. The record lands at offset
\* expected + 1, and f may be at most that. The head entry is written with
\* the record; a crash between the two leaves an entry without its record,
\* which the broker drops at open, so the model writes them together.
Trim(h, b, f) ==
    /\ Active(h)
    /\ fenced[h]
    /\ leads[b]
    /\ trims < MaxTrims
    /\ Unconditional \/ Len(log[b]) = expected[h]
    /\ f > Floor(b)
    /\ f <= Min(expected[h], Len(log[b])) + 1
    /\ trims' = trims + 1
    /\ IF Local
          THEN /\ localFloor' = [localFloor EXCEPT ![b] = f]
               /\ ackedFloor' = Max(ackedFloor, f)
               /\ UNCHANGED << log, head >>
          ELSE /\ log' = [log EXCEPT ![b] = Append(@, TrimRec(gen[b], h, f))]
               /\ head' = [head EXCEPT ![b] = @ \cup EntriesOf(log'[b], Len(log'[b]), Len(log'[b]))]
               /\ UNCHANGED << localFloor, ackedFloor >>
    /\ UNCHANGED << gen, leads, commit, base, lease, expected, fenced, retired,
                    acked, writes, crashes >>

\* A trim at or below the floor writes nothing. It checks the tail first,
\* and answers with it; the holder takes its next tail from the answer.
NoOpTrim(h, b) ==
    /\ Active(h)
    /\ fenced[h]
    /\ leads[b]
    /\ trims < MaxTrims
    /\ NoOpUnchecked \/ Len(log[b]) = expected[h]
    /\ expected' = [expected EXCEPT ![h] = Len(log[b])]
    /\ trims' = trims + 1
    /\ UNCHANGED << log, gen, leads, commit, base, head, localFloor, lease, fenced,
                    retired, acked, ackedFloor, writes, crashes >>

\* A broker that leads answers a holder's write or trim once its commit
\* offset passes it. The holder takes the offset as its tail.
Answer(h) ==
    \E b \in Brokers :
        LET mine == { i \in (expected[h] + 1)..commit[b] :
                        log[b][i].h = h /\ log[b][i].g = gen[b] }
        IN /\ leads[b]
           /\ mine /= {}
           /\ expected' = [expected EXCEPT ![h] = MaxOf(mine)]
           /\ fenced' = [fenced EXCEPT ![h] = @ \/ \E i \in mine : log[b][i].k = "w"]
           /\ UNCHANGED << log, gen, leads, commit, base, head, localFloor, lease,
                           retired, acked, ackedFloor, writes, trims, crashes >>

\* A holder reads the serving leader's committed records from its own
\* cursor. Readers skip trim records. Below the readable start the read is
\* refused with CursorTooOld and the floor's tag; a newer tag ends the
\* holder, an older one sends it on from the readable start. A write from a
\* newer epoch ends it too; otherwise it takes the committed tail.
Refresh(h) ==
    /\ Active(h)
    /\ \E s \in Brokers :
        /\ Serving(s)
        /\ commit[s] > expected[h]
        /\ LET from == expected[h] + 1
               rs == ReadableStart(s)
               tooOld == from < rs
               newer == { i \in Max(from, rs)..commit[s] :
                            log[s][i].k = "w" /\ log[s][i].h > h }
           IN IF (tooOld /\ FloorTag(s) > h) \/ newer /= {}
                 THEN /\ retired' = [retired EXCEPT ![h] = TRUE]
                      /\ UNCHANGED expected
                 ELSE /\ expected' = [expected EXCEPT ![h] = commit[s]]
                      /\ UNCHANGED retired
    /\ UNCHANGED << log, gen, leads, commit, base, head, localFloor, lease, fenced,
                    acked, ackedFloor, writes, trims, crashes >>

\* A follower takes the next record it is missing from a broker that leads
\* at a generation no older than its own, dropping any suffix that
\* disagrees, and the head entries with it. One that needs a record the
\* leader has deleted is rebuilt at the leader's base: records from there on,
\* and their head entries.
Replicate(s, f) ==
    /\ s /= f
    /\ leads[s]
    /\ gen[f] <= gen[s]
    /\ LET l == log[s]
           k == CHOOSE n \in 0..Len(l) :
                   /\ n <= Len(log[f])
                   /\ \A i \in 1..n : log[f][i] = l[i]
                   /\ (n < Len(log[f]) /\ n < Len(l)) => log[f][n + 1] /= l[n + 1]
       IN /\ k < Len(l)
          /\ IF k + 1 < base[s]
                THEN /\ log' = [log EXCEPT ![f] = SubSeq(l, 1, base[s])]
                     /\ base' = [base EXCEPT ![f] = base[s]]
                     /\ head' = [head EXCEPT ![f] = EntriesOf(l, base[s], base[s])]
                     /\ commit' = [commit EXCEPT ![f] = Max(@, Min(commit[s], base[s]))]
                ELSE /\ log' = [log EXCEPT ![f] = SubSeq(l, 1, k + 1)]
                     /\ head' = [head EXCEPT ![f] = { e \in @ : e.o <= k } \cup EntriesOf(l, k + 1, k + 1)]
                     /\ commit' = [commit EXCEPT ![f] = Max(@, Min(commit[s], k + 1))]
                     \* Only a twin deletes above the commit offset, which
                     \* truncation can then cut below.
                     /\ base' = [base EXCEPT ![f] = Min(@, k + 1)]
          /\ gen' = [gen EXCEPT ![f] = gen[s]]
          /\ leads' = [leads EXCEPT ![f] = FALSE]
    /\ UNCHANGED << localFloor, lease, expected, fenced, retired, acked,
                    ackedFloor, writes, trims, crashes >>

\* The mark moves to the longest prefix a majority holds, counting only up to
\* a record of the broker's own generation. The floor follows from the head
\* entries the mark now covers; the answers are a later step.
AdvanceMark(b) ==
    /\ leads[b]
    /\ \E n \in (commit[b] + 1)..Len(log[b]) :
        LET l == log[b]
            new == (commit[b] + 1)..n
        IN /\ l[n].g = gen[b]
           /\ \E Q \in Majority : \A q \in Q :
                 Len(log[q]) >= n /\ SubSeq(log[q], 1, n) = SubSeq(l, 1, n)
           /\ commit' = [commit EXCEPT ![b] = n]
           /\ acked' = acked \cup { [i |-> i, r |-> l[i]] : i \in { j \in new : l[j].k = "w" } }
           /\ ackedFloor' = MaxOf({ackedFloor} \cup { l[i].f : i \in { j \in new : l[j].k = "t" } })
    /\ UNCHANGED << log, gen, leads, base, head, localFloor, lease, expected, fenced,
                    retired, writes, trims, crashes >>

\* A broker deletes whole segments below its bound, as far as its segment
\* boundaries allow: any position up to the bound. Head entries stay.
Delete(b) ==
    /\ \E p \in (base[b] + 1)..DeleteBound(b) :
        base' = [base EXCEPT ![b] = p]
    /\ UNCHANGED << log, gen, leads, commit, head, localFloor, lease, expected, fenced,
                    retired, acked, ackedFloor, writes, trims, crashes >>

\* A broker restarts. It no longer leads, and its commit offset reads back
\* behind, never ahead and never below what it deleted.
Crash(b) ==
    /\ crashes < MaxCrashes
    /\ \E c \in (base[b] - 1)..commit[b] : commit' = [commit EXCEPT ![b] = c]
    /\ leads' = [leads EXCEPT ![b] = FALSE]
    /\ crashes' = crashes + 1
    /\ UNCHANGED << log, gen, base, head, localFloor, lease, expected, fenced,
                    retired, acked, ackedFloor, writes, trims >>

\* A broker a majority votes for takes the next generation and writes its
\* start record. The voters stop leading; a deposed leader outside the
\* majority keeps believing it leads until a newer generation reaches it.
Promote(b) ==
    LET g == 1 + TopGen
    IN /\ ~(leads[b] /\ gen[b] = TopGen)
       /\ g <= MaxGen
       /\ \E Q \in Majority :
           /\ b \in Q
           /\ \A q \in Q : UpToDate(b, q)
           /\ gen' = [q \in Brokers |-> IF q \in Q THEN g ELSE gen[q]]
           /\ leads' = [q \in Brokers |-> IF q = b THEN TRUE ELSE IF q \in Q THEN FALSE ELSE leads[q]]
       /\ log' = [log EXCEPT ![b] = Append(@, StartRec(g))]
       /\ UNCHANGED << commit, base, head, localFloor, lease, expected, fenced, retired,
                       acked, ackedFloor, writes, trims, crashes >>

Next ==
    \/ TakeOver
    \/ \E h \in Holders : Answer(h) \/ Refresh(h)
    \/ \E h \in Holders, b \in Brokers :
          \/ HolderWrite(h, b)
          \/ NoOpTrim(h, b)
          \/ \E f \in 2..(MaxWrites + MaxTrims + MaxGen) : Trim(h, b, f)
    \/ \E b \in Brokers : AdvanceMark(b) \/ Delete(b) \/ Promote(b) \/ Crash(b)
    \/ \E s, f \in Brokers : Replicate(s, f)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------------

TypeOK ==
    /\ \A b \in Brokers :
        /\ commit[b] <= Len(log[b])
        /\ base[b] \in 1..(commit[b] + 1)
        /\ \A e \in head[b] : e.o <= Len(log[b]) /\ log[b][e.o].k = "t"
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
    \A b \in Brokers : \A i, j \in 1..commit[b] :
        (j < i /\ log[b][j].k = "w" /\ log[b][i].k \in {"w", "t"})
            => log[b][j].h <= log[b][i].h

=============================================================================
