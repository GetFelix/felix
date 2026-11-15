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
(* cannot decide `kept` from its guess of the next index.                  *)
(*                                                                         *)
(* The leader answers a client once the client's entry is committed:       *)
(* "ok at i" when it is kept, "mismatch" otherwise (the code reports the   *)
(* tail, i + 1).                                                           *)
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
(*                                                                         *)
(* The knob:                                                               *)
(*                                                                         *)
(*   LeaderDecides = TRUE -- the leader decides `kept` when the publish    *)
(*                           arrives, from its guess of the index (its log *)
(*                           length plus the queue ahead, plus one), and   *)
(*                           followers copy the decision. A membership     *)
(*                           entry appended in between moves the publish   *)
(*                           one index on, and it is kept there.           *)
(*                                                                         *)
(* Abstracted: disks, crashes and snapshots (FelixRaft.tla has them). A    *)
(* message is one atomic step that reads both members, and a leader counts *)
(* a follower toward a commit by reading its log, not from a stale answer. *)
(* Each client sends once and is not retried: a retry is a new publish.    *)
(* Any leader that holds a client's entry committed may answer it, which   *)
(* allows more than the code, where only the proposer answers.             *)
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
    LeaderDecides

ASSUME
    /\ MaxTerm \in Nat /\ MaxLog \in Nat /\ MaxPlain \in Nat /\ MaxMembers \in Nat
    /\ \A c \in DOMAIN Expected : Expected[c] \in Nat \ {0}
    /\ LeaderDecides \in BOOLEAN
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
Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

NoAnswer == [r |-> "none", at |-> 0]

Entry(t, k, c) == [term |-> t, kind |-> k, id |-> c, kept |-> FALSE]

LastTerm(s) == IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])].term

\* The entry a member stores at index i. With the rule on, a conditional
\* entry is decided here, from the entry and i alone.
Store(e, i) ==
    IF e.kind = "cond" /\ ~LeaderDecides THEN [e EXCEPT !.kept = (i = Expected[e.id])]
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

\* The leader takes a publish into its queue. With LeaderDecides a
\* conditional one is decided now, from the index the leader expects it to
\* get: past its log and everything queued ahead of it.
TakeCond(i, c) ==
    /\ role[i] = "leader"
    /\ c \notin sent
    /\ LET guess == Len(log[i]) + Len(queue[i]) + 1
           e == [Entry(term[i], "cond", c) EXCEPT !.kept = LeaderDecides /\ guess = Expected[c]]
       IN queue' = [queue EXCEPT ![i] = Append(@, e)]
    /\ sent' = sent \cup {c}
    /\ UNCHANGED <<term, role, votedFor, granted, log, commit, answer, used>>

TakePlain(i) ==
    /\ role[i] = "leader"
    /\ used.plain < MaxPlain
    /\ queue' = [queue EXCEPT ![i] = Append(@, Entry(term[i], "plain", 0))]
    /\ used' = [used EXCEPT !.plain = @ + 1]
    /\ UNCHANGED <<term, role, votedFor, granted, log, commit, sent, answer>>

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

\* A leader answers a client whose entry it holds committed.
Answer(i, c) ==
    /\ role[i] = "leader"
    /\ answer[c] = NoAnswer
    /\ \E k \in 1..commit[i] :
          /\ log[i][k].kind = "cond"
          /\ log[i][k].id = c
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
    [term : 1..MaxTerm, kind : {"blank", "plain", "member"}, id : {0}, kept : {FALSE}]
    \cup [term : 1..MaxTerm, kind : {"cond"}, id : Clients, kept : BOOLEAN]

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

CondKeptAtExpected ==
    \A s \in Servers : \A k \in 1..commit[s] :
        KeptCond(s, k) => k = Expected[log[s][k].id]

OneKeptPerOffset ==
    \A s1, s2 \in Servers : \A k1 \in 1..commit[s1], k2 \in 1..commit[s2] :
        (/\ KeptCond(s1, k1) /\ KeptCond(s2, k2)
         /\ Expected[log[s1][k1].id] = Expected[log[s2][k2].id])
            => log[s1][k1].id = log[s2][k2].id

MembersAgree ==
    \A s1, s2 \in Servers : \A k \in 1..Min(commit[s1], commit[s2]) :
        log[s1][k] = log[s2][k]

AnswersHold ==
    \A c \in Clients :
        /\ answer[c].r = "ok" =>
              /\ answer[c].at = Expected[c]
              /\ \A s \in Servers : Committed(s, answer[c].at) =>
                    /\ log[s][answer[c].at].kind = "cond"
                    /\ log[s][answer[c].at].id = c
                    /\ log[s][answer[c].at].kept
        /\ answer[c].r = "mismatch" =>
              \A s \in Servers : \A k \in 1..commit[s] :
                  ~(KeptCond(s, k) /\ log[s][k].id = c)

----------------------------------------------------------------------------
\* Witness: says something never happens, and TLC must find it does.

\* One client is answered "ok" and another "mismatch".
NeverOkAndMismatch ==
    ~(\E c, d \in Clients : answer[c].r = "ok" /\ answer[d].r = "mismatch")

\* Expected offsets for the configurations: two clients name offset 2, the
\* first index after the first leader's blank, and one names 3.
TwoAtTwo == <<2, 2, 3>>

=============================================================================
