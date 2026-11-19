---------------------------- MODULE FelixRaftCond ----------------------------
(***************************************************************************)
(* Conditional publishes on a Raft stream shard.                           *)
(*                                                                         *)
(* A conditional publish names `expected`, an offset, and must land there  *)
(* or not at all. The offset is the Raft index. The leader appends one     *)
(* entry for it, {term, "cond", expected, id}, wherever its log ends, and  *)
(* every member decides from the entry and its index alone: at index       *)
(* `expected` it holds the record, anywhere else it holds a blank that     *)
(* readers skip. The stored entry carries that decision as `kept`, worked  *)
(* out when the member writes it, the leader's own append and a            *)
(* follower's copy alike. A truncation on a log conflict removes stored    *)
(* entries together with their decision.                                   *)
(*                                                                         *)
(* Other entries take an index too: a plain publish, the blank a leader    *)
(* writes at the start of its term, and a membership entry. Publishes go   *)
(* through the leader's queue in order; the blank and the membership entry *)
(* are written by Raft itself and do not, so a membership entry can land  *)
(* between a publish's arrival and its append. That is why the leader     *)
(* cannot decide `kept` from its guess of the next index. The queue is     *)
(* modelled only for that guess: when members decide at write time a      *)
(* publish is appended as it arrives, which changes no outcome.            *)
(*                                                                         *)
(* The leader answers a client once the client's entry is committed:       *)
(* "ok at i" when it is kept, "mismatch" otherwise (the code reports the   *)
(* tail, i + 1).                                                           *)
(*                                                                         *)
(* A conditional batch of N records is N entries, appended in one step at  *)
(* consecutive indexes, and lands at expected .. expected + N - 1 or not   *)
(* at all. Each entry carries its position in the batch and the batch's    *)
(* length, and a member keeps the piece at position p iff its index is     *)
(* expected + p, so a batch is kept whole or blanked whole. The commit is  *)
(* not atomic: a change of leader can commit a strict prefix of a batch    *)
(* and put the new leader's blank where the next piece was. That prefix is *)
(* a cut batch. It stays in the log, kept, and readers must skip it.       *)
(*                                                                         *)
(* What a reader on a member sees, from that member's log alone:           *)
(*                                                                         *)
(*   - the read bound is the commit index, except that it stops before a   *)
(*     kept piece whose batch's last index is past the commit (the batch   *)
(*     may still commit whole, or be cut);                                 *)
(*   - below the bound, a kept piece at i with position p and length n is  *)
(*     a record only if the entry at i + n - 1 - p is a kept last piece    *)
(*     of the same length. Pieces only take consecutive indexes and a cut  *)
(*     takes only a tail, so that entry is this batch's own last piece     *)
(*     and the batch is whole. The check reads marks only, not the         *)
(*     client: the code has no batch identity to compare.                  *)
(*                                                                         *)
(* Checked, over each member's committed prefix:                           *)
(*                                                                         *)
(*   CondKeptAtExpected -- every kept conditional record is at its         *)
(*                         expected offset.                                *)
(*   OneKeptPerOffset   -- of the conditional publishes expecting one      *)
(*                         offset, at most one is kept.                    *)
(*   MembersAgree       -- two members that both committed an index hold   *)
(*                         the same entry there, `kept` included.          *)
(*   AnswersHold        -- a client answered "ok at i" has its record kept *)
(*                         at i = expected on every member that committed  *)
(*                         i; one answered "mismatch" is kept nowhere      *)
(*                         committed.                                      *)
(*   WholeBatchesOnly   -- every record a reader can see belongs to a      *)
(*                         batch whose every piece the member holds        *)
(*                         committed and kept.                             *)
(*   ReadsStable        -- (property) a member's read bound never falls,   *)
(*                         and what a reader sees below it never changes.  *)
(*                                                                         *)
(* The knobs:                                                              *)
(*                                                                         *)
(*   LeaderDecides = TRUE -- the leader decides `kept` when the publish    *)
(*                           arrives, from its guess of the index (its log *)
(*                           length plus the queue ahead, plus one), and   *)
(*                           followers copy the decision. A membership     *)
(*                           entry appended in between moves the publish   *)
(*                           one index on, and it is kept there.           *)
(*                                                                         *)
(*   Hide = "whole" -- the read rule above.                                *)
(*   Hide = "bound" -- the read bound stops before an unfinished batch,    *)
(*                     but a kept piece below it is a record whether or    *)
(*                     not its batch's last piece is there.                *)
(*   Hide = "none"  -- a reader sees every committed kept piece.           *)
(*                                                                         *)
(* Abstracted: disks, crashes and snapshots (FelixRaft.tla has them). A    *)
(* message is one atomic step that reads both members, and a leader counts *)
(* a follower toward a commit by reading its log, not from a stale answer. *)
(* Each client sends once and is not retried: a retry is a new publish.    *)
(* Any leader that holds a client's entry committed may answer it, which   *)
(* allows more than the code, where only the proposer answers. A batch is  *)
(* answered from its first piece: "mismatch" when that is a blank, "ok"    *)
(* once its last piece is committed, and a cut batch is not answered (the  *)
(* proposer lost its term, and a retry is a new publish). Every member     *)
(* runs the read rule: an older member that would deliver a cut prefix is  *)
(* kept out by a fleet gate, not modelled here.                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    MaxTerm,
    MaxLog,        \* the longest log a leader appends to
    MaxPlain,      \* plain publishes
    MaxMembers,    \* membership entries
    Expected,      \* Expected[c] is the offset client c names
    CondLen,       \* CondLen[c] is how many records client c's batch holds
    LeaderDecides,
    Hide

ASSUME
    /\ MaxTerm \in Nat /\ MaxLog \in Nat /\ MaxPlain \in Nat /\ MaxMembers \in Nat
    /\ \A c \in DOMAIN Expected : Expected[c] \in Nat \ {0}
    /\ DOMAIN CondLen = DOMAIN Expected
    /\ \A c \in DOMAIN CondLen : CondLen[c] \in Nat \ {0}
    /\ LeaderDecides \in BOOLEAN
    /\ Hide \in {"whole", "bound", "none"}
    /\ Nobody \notin Servers

VARIABLES
    term,       \* per member: its current term
    role,
    votedFor,   \* per member: whom it voted for in `term`
    granted,    \* per candidate: who voted for it
    log,        \* per member: its stored entries
    commit,     \* per member: its commit index
    queue,      \* per member: publishes it took as leader and has not appended
    sent,       \* clients whose publish a leader took
    answer,     \* per client: the answer, once given
    used        \* budgets spent

vars == <<term, role, votedFor, granted, log, commit, queue, sent, answer, used>>

Symm == Permutations(Servers)

Clients == DOMAIN Expected

Min(a, b) == IF a <= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

NoAnswer == [r |-> "none", at |-> 0]

Entry(t, k, c) == [term |-> t, kind |-> k, id |-> c, pos |-> 0, len |-> 1, kept |-> FALSE]

\* Piece p of client c's conditional batch.
Piece(t, c, p) == [Entry(t, "cond", c) EXCEPT !.pos = p, !.len = CondLen[c]]

LastTerm(s) == IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])].term

\* The entry a member stores at index i. With the rule on, a conditional
\* entry is decided here, from the entry and i alone.
Store(e, i) ==
    IF e.kind = "cond" /\ ~LeaderDecides THEN [e EXCEPT !.kept = (i = Expected[e.id] + e.pos)]
    ELSE e

\* How far two logs agree. Equal terms at an index mean equal prefixes up to
\* it (Raft's log matching), so the highest such index is the common prefix.
Common(i, j) ==
    MaxOf({k \in 0..Min(Len(log[i]), Len(log[j])) :
              k = 0 \/ log[i][k].term = log[j][k].term})

----------------------------------------------------------------------------

Init ==
    /\ term = [s \in Servers |-> 0]
    /\ role = [s \in Servers |-> "follower"]
    /\ votedFor = [s \in Servers |-> Nobody]
    /\ granted = [s \in Servers |-> {}]
    /\ log = [s \in Servers |-> <<>>]
    /\ commit = [s \in Servers |-> 0]
    /\ queue = [s \in Servers |-> <<>>]
    /\ sent = {}
    /\ answer = [c \in Clients |-> NoAnswer]
    /\ used = [plain |-> 0, members |-> 0]

\* A member that sees a higher term takes it and follows. What it queued as
\* leader is dropped unanswered.
StepDown(s, t) ==
    /\ term' = [term EXCEPT ![s] = t]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ votedFor' = [votedFor EXCEPT ![s] = Nobody]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ queue' = [queue EXCEPT ![s] = <<>>]

Campaign(c) ==
    /\ role[c] # "leader"
    /\ term[c] < MaxTerm
    /\ term' = [term EXCEPT ![c] = @ + 1]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<log, commit, queue, sent, answer, used>>

\* A vote request and its answer in one step. A voter of a lower term takes
\* the candidate's term first.
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
    /\ queue' = [queue EXCEPT ![s] = IF term[s] < term[c] THEN <<>> ELSE @]
    /\ UNCHANGED <<log, commit, sent, answer, used>>

\* The winner appends its blank entry.
BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c])
    /\ Len(log[c]) < MaxLog
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ log' = [log EXCEPT ![c] = Append(@, Entry(term[c], "blank", 0))]
    /\ UNCHANGED <<term, votedFor, granted, commit, queue, sent, answer, used>>

\* A member of a higher term tells another of it.
LearnTerm(i, j) ==
    /\ term[i] > term[j]
    /\ StepDown(j, term[i])
    /\ UNCHANGED <<log, commit, sent, answer, used>>

\* The leader takes a publish, a sequence of entries. With LeaderDecides it
\* goes into the queue, and a conditional one is decided now, from the index
\* the leader expects it to get: past its log and everything queued ahead
\* of it. With the rule on, nothing can see a publish between its arrival
\* and its append, so it is appended at once, every entry of a batch in one
\* step at consecutive indexes (one write bound to the leader's term).
Take(i, es) ==
    IF LeaderDecides
    THEN /\ queue' = [queue EXCEPT ![i] = @ \o es]
         /\ UNCHANGED log
    ELSE /\ Len(log[i]) + Len(es) <= MaxLog
         /\ log' = [log EXCEPT ![i] =
                @ \o [p \in 1..Len(es) |-> Store(es[p], Len(log[i]) + p)]]
         /\ UNCHANGED queue

TakeCond(i, c) ==
    /\ role[i] = "leader"
    /\ c \notin sent
    /\ LET guess == Len(log[i]) + Len(queue[i]) + 1
       IN Take(i, [p \in 1..CondLen[c] |->
                     [Piece(term[i], c, p - 1) EXCEPT !.kept = LeaderDecides /\ guess = Expected[c]]])
    /\ sent' = sent \cup {c}
    /\ UNCHANGED <<term, role, votedFor, granted, commit, answer, used>>

TakePlain(i) ==
    /\ role[i] = "leader"
    /\ used.plain < MaxPlain
    /\ Take(i, <<Entry(term[i], "plain", 0)>>)
    /\ used' = [used EXCEPT !.plain = @ + 1]
    /\ UNCHANGED <<term, role, votedFor, granted, commit, sent, answer>>

\* The head of the queue goes into the leader's log.
AppendQueued(i) ==
    /\ role[i] = "leader"
    /\ queue[i] # <<>>
    /\ Len(log[i]) < MaxLog
    /\ log' = [log EXCEPT ![i] = Append(@, Store(Head(queue[i]), Len(@) + 1))]
    /\ queue' = [queue EXCEPT ![i] = Tail(@)]
    /\ UNCHANGED <<term, role, votedFor, granted, commit, sent, answer, used>>

\* A membership entry takes an index and holds no record. The voter set is
\* left as it is: only the index matters here.
AppendMember(i) ==
    /\ role[i] = "leader"
    /\ used.members < MaxMembers
    /\ Len(log[i]) < MaxLog
    /\ log' = [log EXCEPT ![i] = Append(@, Entry(term[i], "member", 0))]
    /\ used' = [used EXCEPT !.members = @ + 1]
    /\ UNCHANGED <<term, role, votedFor, granted, commit, queue, sent, answer>>

\* AppendEntries with one entry past the common prefix. A follower holding
\* something else there cuts it off first.
Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ LET p == Common(i, j) IN
       /\ p < Len(log[i])
       /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p), Store(log[i][p + 1], p + 1))]
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ UNCHANGED <<term, votedFor, granted, commit, queue, sent, answer, used>>

\* An entry of the leader's own term held by a majority commits it and
\* everything before it.
Agree(i, k) == {j \in Servers : Common(i, j) >= k}

AdvanceCommit(i) ==
    /\ role[i] = "leader"
    /\ \E k \in (commit[i] + 1)..Len(log[i]) :
          /\ log[i][k].term = term[i]
          /\ Quorum(Agree(i, k))
          /\ commit' = [commit EXCEPT ![i] = k]
    /\ UNCHANGED <<term, role, votedFor, granted, log, queue, sent, answer, used>>

\* A follower commits up to the leader's commit index, as far as its log
\* matches the leader's.
LearnCommit(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ LET k == Min(Common(i, j), commit[i]) IN
       /\ k > commit[j]
       /\ commit' = [commit EXCEPT ![j] = k]
    /\ UNCHANGED <<term, role, votedFor, granted, log, queue, sent, answer, used>>

\* A leader answers a client whose first entry it holds committed: "ok" once
\* the last one is committed too, "mismatch" when the first is a blank.
Answer(i, c) ==
    /\ role[i] = "leader"
    /\ answer[c] = NoAnswer
    /\ \E k \in 1..commit[i] :
          /\ log[i][k].kind = "cond"
          /\ log[i][k].id = c
          /\ log[i][k].pos = 0
          /\ log[i][k].kept =>
                LET j == k + CondLen[c] - 1 IN
                    /\ j <= commit[i]
                    /\ log[i][j].kind = "cond"
                    /\ log[i][j].id = c
                    /\ log[i][j].kept
          /\ answer' = [answer EXCEPT ![c] =
                [r |-> IF log[i][k].kept THEN "ok" ELSE "mismatch", at |-> k]]
    /\ UNCHANGED <<term, role, votedFor, granted, log, commit, queue, sent, used>>

Next ==
    \/ \E s \in Servers : Campaign(s)
    \/ \E s, c \in Servers : GrantVote(s, c)
    \/ \E s \in Servers : BecomeLeader(s)
    \/ \E i, j \in Servers : LearnTerm(i, j)
    \/ \E i \in Servers, c \in Clients : TakeCond(i, c)
    \/ \E i \in Servers : TakePlain(i)
    \/ \E i \in Servers : AppendQueued(i)
    \/ \E i \in Servers : AppendMember(i)
    \/ \E i, j \in Servers : Replicate(i, j)
    \/ \E i \in Servers : AdvanceCommit(i)
    \/ \E i, j \in Servers : LearnCommit(i, j)
    \/ \E i \in Servers, c \in Clients : Answer(i, c)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Entries ==
    [term : 1..MaxTerm, kind : {"blank", "plain", "member"}, id : {0},
     pos : {0}, len : {1}, kept : {FALSE}]
    \cup [term : 1..MaxTerm, kind : {"cond"}, id : Clients,
           pos : 0..MaxLog, len : 1..MaxLog, kept : BOOLEAN]

TypeOK ==
    /\ term \in [Servers -> 0..MaxTerm]
    /\ role \in [Servers -> {"follower", "candidate", "leader"}]
    /\ votedFor \in [Servers -> Servers \cup {Nobody}]
    /\ \A s \in Servers :
          /\ \A k \in 1..Len(log[s]) : log[s][k] \in Entries
          /\ commit[s] <= Len(log[s])
          /\ \A k \in 1..Len(queue[s]) : queue[s][k] \in Entries
    /\ sent \subseteq Clients
    /\ answer \in [Clients -> [r : {"none", "ok", "mismatch"}, at : 0..MaxLog]]

\* Index k is committed on member s.
Committed(s, k) == k \in 1..commit[s]

KeptCond(s, k) == log[s][k].kind = "cond" /\ log[s][k].kept

\* The offset a conditional entry names: its batch's, plus its position.
Target(s, k) == Expected[log[s][k].id] + log[s][k].pos

CondKeptAtExpected ==
    \A s \in Servers : \A k \in 1..commit[s] :
        KeptCond(s, k) => k = Target(s, k)

OneKeptPerOffset ==
    \A s1, s2 \in Servers : \A k1 \in 1..commit[s1], k2 \in 1..commit[s2] :
        (/\ KeptCond(s1, k1) /\ KeptCond(s2, k2)
         /\ Target(s1, k1) = Target(s2, k2))
            => log[s1][k1].id = log[s2][k2].id

MembersAgree ==
    \A s1, s2 \in Servers : \A k \in 1..Min(commit[s1], commit[s2]) :
        log[s1][k] = log[s2][k]

AnswersHold ==
    \A c \in Clients :
        /\ answer[c].r = "ok" =>
              /\ answer[c].at = Expected[c]
              /\ \A s \in Servers, p \in 0..(CondLen[c] - 1) :
                    Committed(s, answer[c].at + p) =>
                        /\ log[s][answer[c].at + p].kind = "cond"
                        /\ log[s][answer[c].at + p].id = c
                        /\ log[s][answer[c].at + p].pos = p
                        /\ log[s][answer[c].at + p].kept
        /\ answer[c].r = "mismatch" =>
              \A s \in Servers : \A k \in 1..commit[s] :
                  ~(KeptCond(s, k) /\ log[s][k].id = c)

\* The index of the last piece of the batch whose kept piece is at k, from
\* the piece's own mark.
LastAt(s, k) == k + log[s][k].len - 1 - log[s][k].pos

\* The entry at LastAt is committed and is a kept last piece of the same
\* length. Marks only: no client identity.
Finished(s, k) ==
    LET j == LastAt(s, k) IN
        /\ j <= commit[s]
        /\ log[s][j].kind = "cond"
        /\ log[s][j].kept
        /\ log[s][j].len = log[s][k].len
        /\ log[s][j].pos = log[s][j].len - 1

\* Committed kept pieces whose batch may still commit whole or be cut.
Unfinished(s) == {k \in 1..commit[s] : KeptCond(s, k) /\ LastAt(s, k) > commit[s]}

ReadBound(s) ==
    IF Hide = "none" \/ Unfinished(s) = {} THEN commit[s]
    ELSE MinOf(Unfinished(s)) - 1

\* A reader on s sees the entry at k (a record, when it is plain or a kept
\* piece; anything else it skips).
Visible(s, k) ==
    /\ k \in 1..ReadBound(s)
    /\ (Hide = "whole" /\ KeptCond(s, k)) => Finished(s, k)

WholeBatchesOnly ==
    \A s \in Servers : \A k \in 1..commit[s] :
        (KeptCond(s, k) /\ Visible(s, k)) =>
            LET c == log[s][k].id IN
                \A p \in 0..(CondLen[c] - 1) :
                    /\ Expected[c] + p \in 1..commit[s]
                    /\ log[s][Expected[c] + p].kind = "cond"
                    /\ log[s][Expected[c] + p].id = c
                    /\ log[s][Expected[c] + p].kept

ReadsStable ==
    [][\A s \in Servers :
          /\ ReadBound(s)' >= ReadBound(s)
          /\ \A k \in 1..ReadBound(s) : Visible(s, k)' = Visible(s, k)]_vars

----------------------------------------------------------------------------
\* Witness: says something never happens, and TLC must find it does.

\* One client is answered "ok" and another "mismatch".
NeverOkAndMismatch ==
    ~(\E c, d \in Clients : answer[c].r = "ok" /\ answer[d].r = "mismatch")

\* A cut batch is committed: a kept piece whose last index is committed but
\* holds something else.
NeverCutCommitted ==
    ~\E s \in Servers : \E k \in 1..commit[s] :
        KeptCond(s, k) /\ LastAt(s, k) <= commit[s] /\ ~Finished(s, k)

\* A reader sees a record of a batch of more than one.
NeverReadsBatch ==
    ~\E s \in Servers : \E k \in 1..commit[s] :
        KeptCond(s, k) /\ log[s][k].len > 1 /\ Visible(s, k)

\* Expected offsets for the configurations: two clients name offset 2, the
\* first index after the first leader's blank, and one names 3.
TwoAtTwo == <<2, 2, 3>>
\* One record each.
OneEach == <<1, 1, 1>>
\* Batches: the first client's three records name offsets 2..4, a single
\* record names 2, and two records name 3..4.
BatchLens == <<3, 1, 2>>

=============================================================================
