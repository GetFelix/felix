--------------------------- MODULE FelixRaftBatch ---------------------------
(***************************************************************************)
(* Idempotent batches of several records on a Raft stream shard.           *)
(*                                                                         *)
(* A producer numbers its batches. A batch of N records is N log entries,  *)
(* and each entry carries a piece mark with the batch's full identity:     *)
(* producer, sequence, the record's position in the batch and the batch's  *)
(* length. A producer sends sequence s + 1 only after s was acknowledged,  *)
(* and re-sends an unacknowledged batch at any time until it is.           *)
(*                                                                         *)
(* `log` is every committed entry since the shard was made. `tail` is what *)
(* the leader holds past it, uncommitted: its own appends, and after a     *)
(* change of leader the entries of earlier terms it took over, then its    *)
(* blank entry. A member holds the committed log from its own base: what   *)
(* is below the base was purged, or never held because the member          *)
(* installed a snapshot, and is summarised by its table snapshot.          *)
(*                                                                         *)
(* How a batch is written:                                                 *)
(*                                                                         *)
(*   - one message for the whole batch, bound to the leader's term. The    *)
(*     term and the lease are checked once, so the leader appends every    *)
(*     piece or none, at consecutive indexes;                              *)
(*   - the commit is not atomic. A change of leader keeps a prefix of the  *)
(*     old leader's tail and drops the rest, and the new leader's blank    *)
(*     entry follows the prefix. That is a cut batch: its first k pieces   *)
(*     are kept and nothing else of it is;                                 *)
(*   - a batch is acknowledged when every piece its message carried has    *)
(*     committed under the leader that took it. Anything less is           *)
(*     unacknowledged and the producer sends it again.                     *)
(*                                                                         *)
(* How a send is placed. The leader reads its producer table, which is a   *)
(* function of its table snapshot and the log it retains, the uncommitted  *)
(* tail included:                                                          *)
(*                                                                         *)
(*   - the producer's entry is its highest sequence with a piece, how many *)
(*     pieces of it are held, and its length. A piece at position j means  *)
(*     positions 0..j are held, because pieces only ever land in order, so *)
(*     the count is one more than the highest position seen. That makes    *)
(*     the entry derivable from any suffix of the log;                     *)
(*   - every piece held: the batch is answered as held, nothing written,   *)
(*     once the committed log holds every piece too;                       *)
(*   - k of N held: the send finishes the batch, writing pieces k..N-1;    *)
(*   - none held: the batch is new;                                        *)
(*   - the table cannot say how many are held: the send is refused.        *)
(*                                                                         *)
(* A purge writes the table as of the new base into the member's table     *)
(* snapshot. `AtPurge` is what it does with a batch that is cut there:     *)
(*                                                                         *)
(*   "refuse" -- keeps the sequence and drops the count. The member        *)
(*               refuses the re-send until the log shows it more.          *)
(*   "keep"   -- keeps the sequence and the count, so the member can       *)
(*               finish the batch.                                         *)
(*   "forget" -- drops the cut. The member takes the re-send as new.       *)
(*                                                                         *)
(* A snapshot install gives a member another member's base and table       *)
(* snapshot.                                                               *)
(*                                                                         *)
(* What is checked, all over the committed log:                            *)
(*                                                                         *)
(*   BatchExactlyOnce    -- no (producer, sequence, position) is in the    *)
(*                          log twice, and an acknowledged batch has every *)
(*                          position in it.                                *)
(*   BatchInOrder        -- a producer's records are in the log in         *)
(*                          sequence and position order, and a batch       *)
(*                          starts only after the one before it is whole.  *)
(*   ProducerPlaceAgrees -- every member decides a batch the way the whole *)
(*                          log does, or refuses it. So two members never  *)
(*                          decide one re-send differently, whatever each  *)
(*                          has purged or installed.                       *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   TermBoundBatch = FALSE  -- each record is its own message with its    *)
(*                              own term and lease check. A lease that     *)
(*                              lapses and comes back inside the batch     *)
(*                              refuses some records and takes others, so  *)
(*                              any subset lands, under one leader.        *)
(*   ResendFinishes = FALSE  -- the re-send of a cut batch is written      *)
(*                              whole, over the prefix that is held.       *)
(*   AtPurge = "forget"      -- above.                                     *)
(*   TableAtAppend = FALSE   -- the leader's table reads committed entries *)
(*                              only, so it misses a batch still in flight *)
(*                              and the pieces a new leader took over.     *)
(*   HeldAfterCommit = FALSE -- a batch is answered as held while its      *)
(*                              pieces are still uncommitted.              *)
(*                                                                         *)
(* Abstracted: elections, replication and the uncommitted tails of         *)
(* followers (FelixRaft.tla has them; here a change of leader is one step  *)
(* that hands a prefix of the tail to the new leader), the lease itself,   *)
(* record payloads and the digest that ties a re-send to the batch it      *)
(* repeats, and producers that expire. Every member holds the committed    *)
(* log up to its tail, so any member can lead. A refused producer keeps    *)
(* re-sending: it never gives the sequence up. The records of a cut batch  *)
(* are visible to readers before the batch is finished.                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Members,
    Producers,
    MaxSeq,          \* sequences per producer
    Lens,            \* the batch lengths a producer may pick
    MaxChanges,      \* changes of leader
    MaxPurges,
    MaxLog,          \* a bound on pieces written; the designed rules never reach it
    TermBoundBatch,
    ResendFinishes,
    AtPurge,
    TableAtAppend,
    HeldAfterCommit

ASSUME
    /\ MaxSeq \in Nat /\ MaxChanges \in Nat /\ MaxPurges \in Nat /\ MaxLog \in Nat
    /\ Lens # {} /\ \A n \in Lens : n \in Nat /\ n > 0
    /\ TermBoundBatch \in BOOLEAN /\ ResendFinishes \in BOOLEAN
    /\ TableAtAppend \in BOOLEAN /\ HeldAfterCommit \in BOOLEAN
    /\ AtPurge \in {"refuse", "keep", "forget"}

VARIABLES
    log,       \* history: every committed entry, in order
    tail,      \* the leader's uncommitted entries, in order
    inflight,  \* the proposals the leader took and has not answered
    leader,
    base,      \* per member: how many entries of the log it no longer holds
    snap,      \* per member: its table snapshot, the table as of its base
    cur,       \* per producer: the sequence it is sending, 0 before the first
    blen,      \* per producer: the length of that batch
    acked,     \* history: every <<producer, sequence>> acknowledged
    changes,   \* budgets spent
    purges

vars == <<log, tail, inflight, leader, base, snap, cur, blen, acked, changes, purges>>

Symm == Permutations(Members) \cup Permutations(Producers)

MaxLen == CHOOSE n \in Lens : \A k \in Lens : k <= n
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Blank == [kind |-> "blank"]
Piece(p, s, pos, len) == [kind |-> "piece", p |-> p, s |-> s, pos |-> pos, len |-> len]

\* What the leader holds.
Full == log \o tail

Sorted(S) == CHOOSE f \in [1..Cardinality(S) -> S] :
    \A i, j \in 1..Cardinality(S) : i < j => f[i] < f[j]

----------------------------------------------------------------------------
\* The producer table.

\* No batch seen. `known` is whether `held` can be trusted.
None == [seq |-> 0, held |-> 0, len |-> 0, known |-> TRUE]
NoSnap == [p \in Producers |-> None]

Whole(e) == e.known /\ e.held = e.len

\* The table read from a snapshot and entries lo..hi of L. The entries win
\* over the snapshot: they are at or past what it describes.
Over(sn, L, lo, hi) ==
    [p \in Producers |->
        LET I == {i \in lo..hi : L[i].kind = "piece" /\ L[i].p = p} IN
        IF I = {} THEN sn[p]
        ELSE LET sq == Max({L[i].s : i \in I})
                 J == {i \in I : L[i].s = sq}
             IN [seq |-> sq,
                 held |-> 1 + Max({L[i].pos : i \in J}),
                 len |-> L[CHOOSE i \in J : TRUE].len,
                 known |-> TRUE]]

\* A member's table over the committed log it retains.
Table(m) == Over(snap[m], log, base[m] + 1, Len(log))

\* The table the leader places a send by.
LeaderTable ==
    IF TableAtAppend THEN Over(snap[leader], Full, base[leader] + 1, Len(Full))
    ELSE Table(leader)

\* What a member with nothing purged reads.
Truth == Over(NoSnap, log, 1, Len(log))

\* What a purge writes into the table snapshot for one producer.
Kept(e) ==
    IF Whole(e) \/ ~e.known THEN e
    ELSE CASE AtPurge = "keep" -> e
           [] AtPurge = "refuse" -> [e EXCEPT !.held = 0, !.known = FALSE]
           [] AtPurge = "forget" -> [seq |-> e.seq - 1, held |-> 0, len |-> 0, known |-> TRUE]

\* How a table entry places sequence s of its producer. `from` is the first
\* position still to write.
Place(e, s) ==
    IF s < e.seq THEN [do |-> "held", from |-> 0]
    ELSE IF s = e.seq THEN
        IF ~e.known THEN [do |-> "refuse", from |-> 0]
        ELSE IF e.held = e.len THEN [do |-> "held", from |-> 0]
        ELSE [do |-> "finish", from |-> e.held]
    ELSE IF s = e.seq + 1 /\ Whole(e) THEN [do |-> "new", from |-> 0]
    ELSE [do |-> "refuse", from |-> 0]

----------------------------------------------------------------------------

Init ==
    /\ log = <<>>
    /\ tail = <<>>
    /\ inflight = {}
    /\ leader \in Members
    /\ base = [m \in Members |-> 0]
    /\ snap = [m \in Members |-> NoSnap]
    /\ cur = [p \in Producers |-> 0]
    /\ blen = [p \in Producers |-> 0]
    /\ acked = {}
    /\ changes = 0
    /\ purges = 0

\* A producer starts its next batch once the one before is acknowledged.
Begin(p, n) ==
    /\ cur[p] < MaxSeq
    /\ cur[p] = 0 \/ <<p, cur[p]>> \in acked
    /\ cur' = [cur EXCEPT ![p] = @ + 1]
    /\ blen' = [blen EXCEPT ![p] = n]
    /\ UNCHANGED <<log, tail, inflight, leader, base, snap, acked, changes, purges>>

\* The sets of positions one write of positions k..n-1 can append.
Lands(k, n) == IF TermBoundBatch THEN {k..(n - 1)} ELSE (SUBSET (k..(n - 1))) \ {{}}

PiecesHeld == Cardinality({i \in 1..Len(Full) : Full[i].kind = "piece"})

\* A producer sends its batch, for the first time or again, and the leader
\* places it. Nothing stops a send while an earlier one is still in flight.
Propose(p) ==
    LET s == cur[p]
        d == Place(LeaderTable[p], s)
        k == IF d.do = "finish" /\ ResendFinishes THEN d.from ELSE 0
    IN
    /\ s > 0
    /\ <<p, s>> \notin acked
    /\ \/ /\ d.do = "held"
          /\ HeldAfterCommit => Place(Table(leader)[p], s).do = "held"
          /\ acked' = acked \cup {<<p, s>>}
          /\ UNCHANGED <<tail, inflight>>
       \/ /\ d.do \in {"new", "finish"}
          /\ \E S \in Lands(k, blen[p]) :
                /\ PiecesHeld + Cardinality(S) <= MaxLog
                /\ tail' = tail \o [i \in 1..Cardinality(S) |-> Piece(p, s, Sorted(S)[i], blen[p])]
                /\ inflight' = inflight \cup
                      {[p |-> p, s |-> s, from |-> Sorted(S)[1], whole |-> S = k..(blen[p] - 1)]}
          /\ UNCHANGED acked
    /\ UNCHANGED <<log, leader, base, snap, cur, blen, changes, purges>>

\* The leader's tail commits. A proposal is acknowledged when all it asked
\* for was appended.
Commit ==
    /\ tail # <<>>
    /\ log' = Full
    /\ tail' = <<>>
    /\ acked' = acked \cup {<<q.p, q.s>> : q \in {r \in inflight : r.whole}}
    /\ inflight' = {}
    /\ UNCHANGED <<leader, base, snap, cur, blen, changes, purges>>

\* A change of leader. The new leader holds a prefix of the old one's tail,
\* still uncommitted, and appends its blank entry. The rest is dropped and
\* no proposal in flight is answered.
ChangeLeader(n, k) ==
    /\ changes < MaxChanges
    /\ k \in 0..Len(tail)
    /\ tail' = SubSeq(tail, 1, k) \o <<Blank>>
    /\ inflight' = {}
    /\ leader' = n
    /\ changes' = changes + 1
    /\ UNCHANGED <<log, base, snap, cur, blen, acked, purges>>

\* A member purges its log up to b and writes its table snapshot there.
Purge(m, b) ==
    /\ purges < MaxPurges
    /\ b \in (base[m] + 1)..Len(log)
    /\ snap' = [snap EXCEPT ![m] =
          LET t == Over(snap[m], log, base[m] + 1, b) IN [p \in Producers |-> Kept(t[p])]]
    /\ base' = [base EXCEPT ![m] = b]
    /\ purges' = purges + 1
    /\ UNCHANGED <<log, tail, inflight, leader, cur, blen, acked, changes>>

\* A member installs another member's snapshot: its base and its table.
Install(m, n) ==
    /\ base[n] > base[m]
    /\ base' = [base EXCEPT ![m] = base[n]]
    /\ snap' = [snap EXCEPT ![m] = snap[n]]
    /\ UNCHANGED <<log, tail, inflight, leader, cur, blen, acked, changes, purges>>

Next ==
    \/ \E p \in Producers, n \in Lens : Begin(p, n)
    \/ \E p \in Producers : Propose(p)
    \/ Commit
    \/ \E n \in Members : \E k \in 0..Len(tail) : ChangeLeader(n, k)
    \/ \E m \in Members : \E b \in 1..Len(log) : Purge(m, b)
    \/ \E m, n \in Members : Install(m, n)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Entries ==
    {Blank} \cup [kind : {"piece"}, p : Producers, s : 1..MaxSeq, pos : 0..(MaxLen - 1), len : Lens]
TableEntries == [seq : 0..MaxSeq, held : 0..MaxLen, len : {0} \cup Lens, known : BOOLEAN]

TypeOK ==
    /\ \A i \in 1..Len(Full) : Full[i] \in Entries
    /\ Len(Full) <= MaxLog + MaxChanges
    /\ inflight \subseteq [p : Producers, s : 1..MaxSeq, from : 0..(MaxLen - 1), whole : BOOLEAN]
    /\ leader \in Members
    /\ base \in [Members -> 0..Len(log)]
    /\ snap \in [Members -> [Producers -> TableEntries]]
    /\ cur \in [Producers -> 0..MaxSeq]
    /\ blen \in [Producers -> {0} \cup Lens]
    /\ acked \subseteq (Producers \X (1..MaxSeq))
    /\ changes \in 0..MaxChanges
    /\ purges \in 0..MaxPurges

IsPiece(i, p, s) == log[i].kind = "piece" /\ log[i].p = p /\ log[i].s = s
At(p, s) == {i \in 1..Len(log) : IsPiece(i, p, s)}

BatchExactlyOnce ==
    /\ \A i, j \in 1..Len(log) :
          (i < j /\ log[i].kind = "piece" /\ log[j].kind = "piece") => log[i] # log[j]
    /\ \A a \in acked :
          /\ At(a[1], a[2]) # {}
          /\ LET n == log[CHOOSE i \in At(a[1], a[2]) : TRUE].len IN
             \A pos \in 0..(n - 1) : \E i \in At(a[1], a[2]) : log[i].pos = pos

\* Two copies of one piece are BatchExactlyOnce's to report, so equal
\* identities pass here.
BatchInOrder ==
    /\ \A i, j \in 1..Len(log) :
          (i < j /\ log[i].kind = "piece" /\ log[j].kind = "piece" /\ log[i].p = log[j].p) =>
              \/ log[i].s < log[j].s
              \/ log[i].s = log[j].s /\ log[i].pos <= log[j].pos
    /\ \A j \in 1..Len(log) : (log[j].kind = "piece" /\ log[j].s > 1) =>
          \E i \in 1..(j - 1) :
              /\ IsPiece(i, log[j].p, log[j].s - 1)
              /\ \A pos \in 0..(log[i].len - 1) :
                    \E h \in 1..(j - 1) : IsPiece(h, log[j].p, log[j].s - 1) /\ log[h].pos = pos

ProducerPlaceAgrees ==
    \A m \in Members, p \in Producers, s \in 1..MaxSeq :
        LET d == Place(Table(m)[p], s) IN
        d.do = "refuse" \/ d = Place(Truth[p], s)

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

\* A batch cut by a change of leader is finished by its re-send and
\* acknowledged: its pieces are not one run in the log.
NeverFinishesACut ==
    \A a \in acked :
        LET I == At(a[1], a[2]) IN
        I = {} \/ \A i \in (Max(I) - Cardinality(I) + 1)..Max(I) : i \in I

\* A member refuses the batch its producer is sending.
NeverRefuses ==
    \A m \in Members, p \in Producers :
        (cur[p] > 0 /\ <<p, cur[p]>> \notin acked) => Place(Table(m)[p], cur[p]).do # "refuse"

\* A leader finishes a cut batch whose held pieces are committed and none of
\* which it retains: the count came from its table snapshot.
NeverFinishesFromSnapshot ==
    \A q \in inflight :
        (q.from > 0 /\ At(q.p, q.s) # {}) => base[leader] < Max(At(q.p, q.s))

\* A new leader finishes a cut batch while the held pieces it took over are
\* still uncommitted.
NeverFinishesOverTail ==
    \A q \in inflight :
        q.from > 0 => \E i \in At(q.p, q.s) : log[i].pos = q.from - 1

\* A member installs a snapshot: more members have a base than purges ran.
NeverInstalls == Cardinality({m \in Members : base[m] > 0}) <= purges

=============================================================================
