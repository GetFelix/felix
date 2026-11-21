---------------------------- MODULE FelixRaftReads ----------------------------
(***************************************************************************)
(* FelixRaft with `Quorum` reads of a shard's state, and the question a    *)
(* read has to answer: does this broker still lead, and does it hold every *)
(* write answered before the read began? `FelixShardReads.tla` asks the    *)
(* same of the hybrid. Here the fence round and the lease are replaced by  *)
(* openraft's read index.                                                  *)
(*                                                                         *)
(* The read, as the broker runs it on a Raft shard                         *)
(* (`state_get_on_raft`, services/felix-broker-service/src/serving/        *)
(* commit_ops.rs:492, and the group reads in `OnRaft::fresh`,              *)
(* services/felix-broker-service/src/shards/raft/groups.rs:49):            *)
(*                                                                         *)
(*   1. take the open term (`open_term`, crates/server/felix-replication/  *)
(*      src/raft/group.rs:2194). A term opens once an entry of it is       *)
(*      applied, so its blank entry is committed (raft/lead.rs:117).       *)
(*   2. `read_index` (group.rs:2036): openraft's `get_read_log_id` with    *)
(*      `ReadPolicy::ReadIndex` (`read_round`, group.rs:2637). The read    *)
(*      log id is fixed when the call is made, max(the term's blank entry, *)
(*      the commit index), and returned once a majority answered a         *)
(*      heartbeat of that term sent after it.                              *)
(*   3. `confirmed_index` (group.rs:2022) raises the bound to the newest   *)
(*      write of the term answered on a written majority, which can be     *)
(*      above the commit index (`AckWritten`).                             *)
(*   4. `applied_through` (group.rs:2105), then the term is checked still  *)
(*      open (group.rs:1900).                                              *)
(*   5. the value is taken, from what this broker applied.                 *)
(*                                                                         *)
(* The lease read (`leased_index`, group.rs:1980) skips steps 2 and 3. It  *)
(* rests on bounded clock drift and is checked in FelixRaftLease.tla, not  *)
(* here.                                                                   *)
(*                                                                         *)
(* NoStaleRead is linearizability for a read: every write acknowledged     *)
(* before the read began is in its answer. Each knob removes one step and  *)
(* TLC finds the stale answer:                                             *)
(*                                                                         *)
(*   NoRound        -- step 2's majority is skipped: a deposed leader      *)
(*                     answers without its successor's write.              *)
(*   NoBlankBarrier -- steps 1 and 2 ignore the blank entry: a new leader  *)
(*                     reads before it learned what its predecessor        *)
(*                     committed.                                          *)
(*   SkipsAnswered  -- step 3 is skipped: a write answered on a written    *)
(*                     majority, not yet committed, is missed.             *)
(*                                                                         *)
(* Written acknowledgements (`AckWritten`) follow FelixRaftWritten.tla: a  *)
(* follower reports the id of its last entry, and a report counts for a    *)
(* record only when it carries the record's term (`majority_wrote`,        *)
(* raft/written.rs:233). They are checked without crashes: what a crash    *)
(* does to an unsynced majority is FelixRaftWritten's question.            *)
(***************************************************************************)
EXTENDS FelixRaft

CONSTANTS
    MaxReads,       \* how many reads the run begins
    AckWritten,     \* a record may be answered on a written majority
    NoRound,
    NoBlankBarrier,
    SkipsAnswered

ASSUME AckWritten => MaxCrashes = 0

VARIABLES
    rd,         \* the read in flight: who, term, bound, votes, seen; at = Nobody when none
    nreads,     \* how many reads have begun
    wrote,      \* a leader's latest written report from each member: [term, idx]
    ans,        \* the newest entry each broker answered on a written majority: [term, idx]
    wacked,     \* record values answered on a written majority
    staleRead   \* history: a read was answered without a write acknowledged before it began

readVars == << rd, nreads, wrote, ans, wacked, staleRead >>
allVars == << vars, readVars >>

NoRead == [at |-> Nobody, term |-> 0, bound |-> 0, votes |-> {}, seen |-> {}]
NoPoint == [term |-> 0, idx |-> 0]

ReadsInit ==
    /\ Init
    /\ rd = NoRead
    /\ nreads = 0
    /\ wrote = [s \in Servers |-> [t \in Servers |-> NoPoint]]
    /\ ans = [s \in Servers |-> NoPoint]
    /\ wacked = {}
    /\ staleRead = FALSE

\* The term is open: an entry of it is applied (raft/lead.rs:117). The
\* leader applies only entries of its own term's commits, so this is its
\* blank entry committed.
Open(s) ==
    /\ role[s] = "leader"
    /\ Len(applied[s]) > 0
    /\ applied[s][Len(applied[s])].term = vote[s].term

\* The index of the blank entry `s` appended for its term.
BlankIdx(s) ==
    LET own == {i \in (base[s].idx + 1)..LastIdx(s) :
                    EntryAt(s, i).term = vote[s].term /\ EntryAt(s, i).kind = "blank"}
    IN IF own = {} THEN 0 ELSE MaxOf(own)

\* The record values in what `s` applied.
Applied(s) == {applied[s][i].val : i \in {k \in DOMAIN applied[s] : applied[s][k].kind = "record"}}

\* Steps 1 to 3, as the call is made. What is answered by now is `seen`.
\* The answered bound is read here, before the round, where the code reads
\* it after: a smaller bound, so a check at least as strict.
BeginRead(s) ==
    /\ nreads < MaxReads
    /\ rd.at = Nobody
    /\ IF NoBlankBarrier THEN role[s] = "leader" ELSE Open(s)
    /\ LET idx == IF NoBlankBarrier THEN Len(applied[s]) ELSE Max(BlankIdx(s), Len(applied[s]))
           own == IF ~SkipsAnswered /\ ans[s].term = vote[s].term THEN ans[s].idx ELSE 0
       IN rd' = [at |-> s, term |-> vote[s].term, bound |-> Max(idx, own),
                 votes |-> {s}, seen |-> acked \cup wacked]
    /\ nreads' = nreads + 1
    /\ UNCHANGED << vars, wrote, ans, wacked, staleRead >>

\* `j` answers the read's heartbeat: it has taken the reader as leader of
\* the read's term. One with a newer vote refuses.
ConfirmRead(j) ==
    LET s == rd.at IN
    /\ s # Nobody
    /\ j \notin rd.votes
    /\ role[s] = "leader"
    /\ vote[s].term = rd.term
    /\ vote[j] = vote[s]
    /\ rd' = [rd EXCEPT !.votes = @ \cup {j}]
    /\ UNCHANGED << vars, nreads, wrote, ans, wacked, staleRead >>

\* Step 4 and 5: the bound applied, the term still open, then the value.
EndRead ==
    LET s == rd.at IN
    /\ s # Nobody
    /\ IF NoBlankBarrier THEN role[s] = "leader" ELSE Open(s)
    /\ vote[s].term = rd.term
    /\ NoRound \/ Quorum(rd.votes)
    /\ Len(applied[s]) >= rd.bound
    /\ staleRead' = (staleRead \/ ~(rd.seen \subseteq Applied(s)))
    /\ rd' = NoRead
    /\ UNCHANGED << vars, nreads, wrote, ans, wacked >>

\* A follower's written report: the id of its last entry, flushed or not.
Report(i, j) ==
    /\ AckWritten
    /\ Following(i, j)
    /\ wrote' = [wrote EXCEPT ![i][j] = [term |-> TermAt(j, LastIdx(j)), idx |-> LastIdx(j)]]
    /\ UNCHANGED << vars, rd, nreads, ans, wacked, staleRead >>

\* A record of the leader's own term answered once a majority, the leader
\* included, reported its term at or past it (raft/written.rs:163 and 233).
\* `ans` moves before the publisher is told (raft/group.rs:1867).
WrittenAck(i) ==
    /\ AckWritten
    /\ role[i] = "leader"
    /\ \E k \in (base[i].idx + 1)..LastIdx(i) :
        /\ EntryAt(i, k).kind = "record"
        /\ EntryAt(i, k).term = vote[i].term
        /\ EntryAt(i, k).val \notin wacked
        /\ Quorum({i} \cup {j \in Servers \ {i} :
                    wrote[i][j].term = vote[i].term /\ wrote[i][j].idx >= k})
        /\ wacked' = wacked \cup {EntryAt(i, k).val}
        /\ ans' = [ans EXCEPT ![i] =
                IF ans[i].term < vote[i].term \/ ans[i].idx < k
                THEN [term |-> vote[i].term, idx |-> k] ELSE @]
    /\ UNCHANGED << vars, rd, nreads, wrote, staleRead >>

ReadsNext ==
    \/ Next /\ UNCHANGED readVars
    \/ EndRead
    \/ \E s \in Servers : BeginRead(s) \/ ConfirmRead(s) \/ WrittenAck(s)
    \/ \E i, j \in Servers : Report(i, j)

ReadsSpec == ReadsInit /\ [][ReadsNext]_allVars

\* Every write acknowledged before a read began is in the read's answer.
NoStaleRead == ~staleRead

ReadsTypeOK ==
    /\ TypeOK
    /\ nreads \in 0..MaxReads
    /\ rd.votes \subseteq Servers
    /\ wacked \subseteq 1..MaxWrites

=============================================================================
