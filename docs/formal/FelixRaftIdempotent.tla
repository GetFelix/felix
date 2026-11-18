------------------------- MODULE FelixRaftIdempotent -------------------------
(***************************************************************************)
(* Idempotent publishes across a change of Raft leader.                    *)
(*                                                                         *)
(* One producer numbers its batches and starts batch s + 1 once s is      *)
(* written whole to some member's log, so two can be unanswered at once,  *)
(* as the client pipelines. It re-sends any unacknowledged batch to       *)
(* whichever member leads, as often as it likes, in any order. A batch of N records is N log entries,     *)
(* each a piece carrying the sequence, its position and the batch length.  *)
(*                                                                         *)
(* The leader places a send by its producer table: the highest sequence    *)
(* with a piece, how many of its pieces are held and the batch length.     *)
(* Every piece held: the send is answered as held once they are all        *)
(* committed, and nothing is written. Some held (a leader change cut the   *)
(* batch): only the rest is written. None held and the batch before it     *)
(* whole: the batch is new. Anything else is refused. Placing a send and   *)
(* appending its pieces is one step (the producer's turn), and the pieces  *)
(* go in at consecutive indexes under the leader's term or not at all. A   *)
(* proposal is acknowledged once its last piece is committed under the     *)
(* leader that wrote it.                                                   *)
(*                                                                         *)
(* A cut batch here is what Raft leaves: a follower that copied only the   *)
(* first pieces wins the next election, and the rest of the old leader's   *)
(* tail is truncated wherever it was copied.                               *)
(*                                                                         *)
(* Checked, over each member's committed prefix:                           *)
(*                                                                         *)
(*   NoDuplicate  -- no (sequence, position) is committed twice.           *)
(*   AckedWhole   -- an acknowledged batch has every position committed.   *)
(*   InOrder      -- pieces are committed in (sequence, position) order;   *)
(*                   a repeated piece is NoDuplicate's to report.          *)
(*   MembersAgree -- two members that committed an index hold the same     *)
(*                   entry there.                                          *)
(*                                                                         *)
(* The knobs:                                                              *)
(*                                                                         *)
(*   TableFromLog = FALSE -- the leader keeps its table in memory, filled  *)
(*                           by its own appends and started empty when it  *)
(*                           wins a term. What earlier leaders wrote is     *)
(*                           unknown to it, so a re-send after a change of *)
(*                           leader is taken as new.                       *)
(*                                                                         *)
(*   FinishFromHeld = FALSE -- the re-send of a cut batch is written whole, *)
(*                           over the pieces already held.                 *)
(*                                                                         *)
(* With TableFromLog TRUE the table is read from the leader's whole stored *)
(* log, uncommitted entries included, as the broker reads its own log.     *)
(*                                                                         *)
(* Abstracted: disks, crashes, snapshots and purges (FelixRaftBatch.tla    *)
(* has the table across purges and installs), payloads and the digest that *)
(* ties a re-send to its batch, the lease, and producer expiry. A message  *)
(* is one atomic step that reads both members. A leader's own write never  *)
(* fails partway: with batches of at most two records the rest of a cut    *)
(* batch is one entry, so that matters only for longer batches. A new      *)
(* leader places sends at once; the code first applies every earlier       *)
(* entry, so the model allows more than the code. New batches of several  *)
(* records are modelled ahead of the engine, which refuses them today.     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    MaxTerm,
    MaxLog,
    MaxSeq,
    Lens,          \* the batch lengths the producer may pick
    TableFromLog,
    FinishFromHeld

ASSUME
    /\ MaxTerm \in Nat /\ MaxLog \in Nat /\ MaxSeq \in Nat
    /\ Lens # {} /\ \A n \in Lens : n \in Nat /\ n > 0
    /\ TableFromLog \in BOOLEAN /\ FinishFromHeld \in BOOLEAN
    /\ Nobody \notin Servers

VARIABLES
    term,
    role,
    votedFor,
    granted,
    log,       \* per member: its stored entries
    commit,    \* per member: its commit index
    mem,       \* per member: its in-memory table (TableFromLog = FALSE only)
    cur,       \* the producer's highest sequence begun, 0 before the first
    blen,      \* per sequence: that batch's length
    props,     \* proposals written and not yet answered
    acked      \* history: every acknowledgement, and how it was given

vars == <<term, role, votedFor, granted, log, commit, mem, cur, blen, props, acked>>

Symm == Permutations(Servers)

Min(a, b) == IF a <= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

Blank(t) == [term |-> t, kind |-> "blank", s |-> 0, pos |-> 0, len |-> 0]
Piece(t, s, pos, n) == [term |-> t, kind |-> "piece", s |-> s, pos |-> pos, len |-> n]

LastTerm(s) == IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])].term

Common(i, j) ==
    MaxOf({k \in 0..Min(Len(log[i]), Len(log[j])) :
              k = 0 \/ log[i][k].term = log[j][k].term})

----------------------------------------------------------------------------
\* The producer table.

None == [seq |-> 0, held |-> 0, len |-> 0]

\* The table read from L. Pieces land in position order, so the count held
\* is one more than the highest position of the highest sequence.
Over(L) ==
    LET I == {k \in 1..Len(L) : L[k].kind = "piece"} IN
    IF I = {} THEN None
    ELSE LET sq == MaxOf({L[k].s : k \in I})
             J == {k \in I : L[k].s = sq}
         IN [seq |-> sq, held |-> 1 + MaxOf({L[k].pos : k \in J}),
             len |-> L[CHOOSE k \in J : TRUE].len]

Table(i) == IF TableFromLog THEN Over(log[i]) ELSE mem[i]

Place(e, s) ==
    IF s < e.seq THEN [do |-> "held", from |-> 0]
    ELSE IF s = e.seq THEN
        IF e.held = e.len THEN [do |-> "held", from |-> 0]
        ELSE [do |-> "finish", from |-> e.held]
    ELSE IF s = e.seq + 1 /\ e.held = e.len THEN [do |-> "new", from |-> 0]
    ELSE [do |-> "refuse", from |-> 0]

\* Every position of batch s is in the first n entries of member i's log.
WholeIn(i, s, n, len) ==
    \A pos \in 0..(len - 1) :
        \E k \in 1..n : log[i][k].kind = "piece" /\ log[i][k].s = s /\ log[i][k].pos = pos

IsAcked(s) == \E a \in acked : a.s = s

----------------------------------------------------------------------------

Init ==
    /\ term = [s \in Servers |-> 0]
    /\ role = [s \in Servers |-> "follower"]
    /\ votedFor = [s \in Servers |-> Nobody]
    /\ granted = [s \in Servers |-> {}]
    /\ log = [s \in Servers |-> <<>>]
    /\ commit = [s \in Servers |-> 0]
    /\ mem = [s \in Servers |-> None]
    /\ cur = 0
    /\ blen = [s \in 1..MaxSeq |-> 0]
    /\ props = {}
    /\ acked = {}

StepDown(s, t) ==
    /\ term' = [term EXCEPT ![s] = t]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ votedFor' = [votedFor EXCEPT ![s] = Nobody]
    /\ granted' = [granted EXCEPT ![s] = {}]

Campaign(c) ==
    /\ role[c] # "leader"
    /\ term[c] < MaxTerm
    /\ term' = [term EXCEPT ![c] = @ + 1]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<log, commit, mem, cur, blen, props, acked>>

GrantVote(s, c) ==
    /\ s # c
    /\ role[c] = "candidate"
    /\ term[s] <= term[c]
    /\ term[s] = term[c] => votedFor[s] \in {Nobody, c}
    /\ s \notin granted[c]
    /\ \/ LastTerm(c) > LastTerm(s)
       \/ LastTerm(c) = LastTerm(s) /\ Len(log[c]) >= Len(log[s])
    /\ term' = [term EXCEPT ![s] = term[c]]
    /\ role' = [role EXCEPT ![s] = IF term[s] < term[c] THEN "follower" ELSE @]
    /\ votedFor' = [votedFor EXCEPT ![s] = c]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {s},
                                  ![s] = IF term[s] < term[c] THEN {} ELSE @]
    /\ UNCHANGED <<log, commit, mem, cur, blen, props, acked>>

\* The winner appends its blank entry. Its in-memory table starts empty.
BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c])
    /\ Len(log[c]) < MaxLog
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(term[c]))]
    /\ mem' = [mem EXCEPT ![c] = None]
    /\ UNCHANGED <<term, votedFor, granted, commit, cur, blen, props, acked>>

LearnTerm(i, j) ==
    /\ term[i] > term[j]
    /\ StepDown(j, term[i])
    /\ UNCHANGED <<log, commit, mem, cur, blen, props, acked>>

\* The producer starts its next batch once the one before is written whole
\* to a member's log, committed or not.
Begin(n) ==
    /\ cur < MaxSeq
    /\ cur = 0 \/ \E m \in Servers : WholeIn(m, cur, Len(log[m]), blen[cur])
    /\ cur' = cur + 1
    /\ blen' = [blen EXCEPT ![cur + 1] = n]
    /\ UNCHANGED <<term, role, votedFor, granted, log, commit, mem, props, acked>>

\* The producer sends batch s, for the first time or again, to leader i.
Propose(i, s) ==
    LET d == Place(Table(i), s)
        from == IF d.do = "finish" /\ FinishFromHeld THEN d.from ELSE 0
        n == blen[s] - from
    IN
    /\ role[i] = "leader"
    /\ ~IsAcked(s)
    /\ \/ /\ d.do = "held"
          /\ WholeIn(i, s, commit[i], blen[s])
          /\ acked' = acked \cup {[s |-> s, len |-> blen[s], how |-> "held", t |-> term[i]]}
          /\ UNCHANGED <<log, mem, props>>
       \/ /\ d.do \in {"new", "finish"}
          /\ Len(log[i]) + n <= MaxLog
          /\ log' = [log EXCEPT ![i] =
                @ \o [k \in 1..n |-> Piece(term[i], s, from + k - 1, blen[s])]]
          /\ mem' = [mem EXCEPT ![i] = [seq |-> s, held |-> blen[s], len |-> blen[s]]]
          /\ props' = props \cup {[by |-> i, t |-> term[i], s |-> s, len |-> blen[s],
                                    last |-> Len(log[i]) + n, from |-> from]}
          /\ UNCHANGED acked
    /\ UNCHANGED <<term, role, votedFor, granted, commit, cur, blen>>

\* A leader answers a proposal of its own term once its last piece commits.
Answer(q) ==
    /\ role[q.by] = "leader"
    /\ term[q.by] = q.t
    /\ commit[q.by] >= q.last
    /\ props' = props \ {q}
    /\ acked' = acked \cup {[s |-> q.s, len |-> q.len,
                             how |-> IF q.from = 0 THEN "written" ELSE "finished", t |-> q.t]}
    /\ UNCHANGED <<term, role, votedFor, granted, log, commit, mem, cur, blen>>

\* A proposal whose leader moved on is never answered.
Forget(q) ==
    /\ term[q.by] # q.t
    /\ props' = props \ {q}
    /\ UNCHANGED <<term, role, votedFor, granted, log, commit, mem, cur, blen, acked>>

Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ LET p == Common(i, j) IN
       /\ p < Len(log[i])
       /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p), log[i][p + 1])]
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ UNCHANGED <<term, votedFor, granted, commit, mem, cur, blen, props, acked>>

Agree(i, k) == {j \in Servers : Common(i, j) >= k}

AdvanceCommit(i) ==
    /\ role[i] = "leader"
    /\ \E k \in (commit[i] + 1)..Len(log[i]) :
          /\ log[i][k].term = term[i]
          /\ Quorum(Agree(i, k))
          /\ commit' = [commit EXCEPT ![i] = k]
    /\ UNCHANGED <<term, role, votedFor, granted, log, mem, cur, blen, props, acked>>

LearnCommit(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ LET k == Min(Common(i, j), commit[i]) IN
       /\ k > commit[j]
       /\ commit' = [commit EXCEPT ![j] = k]
    /\ UNCHANGED <<term, role, votedFor, granted, log, mem, cur, blen, props, acked>>

Next ==
    \/ \E s \in Servers : Campaign(s)
    \/ \E s, c \in Servers : GrantVote(s, c)
    \/ \E s \in Servers : BecomeLeader(s)
    \/ \E i, j \in Servers : LearnTerm(i, j)
    \/ \E n \in Lens : Begin(n)
    \/ \E i \in Servers, s \in 1..cur : Propose(i, s)
    \/ \E q \in props : Answer(q) \/ Forget(q)
    \/ \E i, j \in Servers : Replicate(i, j)
    \/ \E i \in Servers : AdvanceCommit(i)
    \/ \E i, j \in Servers : LearnCommit(i, j)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

MaxLen == MaxOf(Lens)

Entries ==
    {Blank(t) : t \in 1..MaxTerm}
    \cup [term : 1..MaxTerm, kind : {"piece"}, s : 1..MaxSeq, pos : 0..(MaxLen - 1), len : Lens]

TypeOK ==
    /\ term \in [Servers -> 0..MaxTerm]
    /\ role \in [Servers -> {"follower", "candidate", "leader"}]
    /\ votedFor \in [Servers -> Servers \cup {Nobody}]
    /\ \A s \in Servers :
          /\ Len(log[s]) <= MaxLog
          /\ \A k \in 1..Len(log[s]) : log[s][k] \in Entries
          /\ commit[s] <= Len(log[s])
    /\ cur \in 0..MaxSeq
    /\ blen \in [1..MaxSeq -> {0} \cup Lens]

IsPiece(e) == e.kind = "piece"

NoDuplicate ==
    \A m \in Servers : \A i, j \in 1..commit[m] :
        (i < j /\ IsPiece(log[m][i]) /\ IsPiece(log[m][j])) =>
            <<log[m][i].s, log[m][i].pos>> # <<log[m][j].s, log[m][j].pos>>

AckedWhole ==
    \A a \in acked : \E m \in Servers : WholeIn(m, a.s, commit[m], a.len)

InOrder ==
    \A m \in Servers : \A i, j \in 1..commit[m] :
        (i < j /\ IsPiece(log[m][i]) /\ IsPiece(log[m][j])) =>
            \/ log[m][i].s < log[m][j].s
            \/ log[m][i].s = log[m][j].s /\ log[m][i].pos <= log[m][j].pos

MembersAgree ==
    \A s1, s2 \in Servers : \A k \in 1..Min(commit[s1], commit[s2]) :
        log[s1][k] = log[s2][k]

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

\* A cut batch is finished by a later leader's re-send and acknowledged.
NeverFinishesACut == \A a \in acked : a.how # "finished"

\* A re-send to a later leader is answered as held from that leader's log.
NeverHeldByLaterLeader ==
    \A a \in acked : a.how = "held" =>
        \A m \in Servers : \A k \in 1..commit[m] :
            (IsPiece(log[m][k]) /\ log[m][k].s = a.s) => log[m][k].term = a.t

=============================================================================
