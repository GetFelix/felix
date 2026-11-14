-------------------------- MODULE FelixRaftWritten --------------------------
(***************************************************************************)
(* The early acknowledgement of a `Quorum` write on a `Periodic` stream,   *)
(* as docs/per-shard-raft.md decides it ("Two signals from one append").   *)
(*                                                                         *)
(* A follower tells the leader how far its log is written: the id (term    *)
(* and index) of its last entry, taken once `append` has handed the        *)
(* records to the kernel and before the fsync. The leader acknowledges a   *)
(* record of its own term once a majority of the voters has written it:    *)
(*                                                                         *)
(*   - a report counts when its log id carries the leader's own term and   *)
(*     is at or past the record. An entry of term T comes from the one     *)
(*     leader of T, so such a log matches the leader's up to that index.   *)
(*     A log id of another term says nothing about what sits below it;     *)
(*   - the leader counts itself, its own copy being written;               *)
(*   - only voters of the latest configuration in the leader's log count,  *)
(*     a majority of each half while it is joint, and never a learner.     *)
(*     A leader that is not a voter does not count itself either.          *)
(*                                                                         *)
(* The count releases the acknowledgement and nothing else. The commit     *)
(* index, and with it apply and what a subscriber is given, counts copies  *)
(* that are fsynced, as FelixRaft.tla has it. A record is acknowledged at  *)
(* the written majority or at commit, whichever comes first.               *)
(*                                                                         *)
(* What a broker keeps:                                                    *)
(*                                                                         *)
(*   log    -- what is written. A process crash keeps all of it: the       *)
(*             kernel has it.                                              *)
(*   synced -- how far the log is on disk. A power cut keeps a prefix no   *)
(*             shorter than this.                                          *)
(*   match  -- a leader's view of each member's fsynced, matching prefix.  *)
(*   wrote  -- a leader's latest written report from each member. It is    *)
(*             kept, so the leader can act on it after the member's log    *)
(*             has changed, and nothing clears it at an election: the term *)
(*             on the log id is the whole defence.                         *)
(*                                                                         *)
(* A report is one atomic step that reads the sender's log. It is not tied *)
(* to the sender's vote, so a member that already voted in a later term    *)
(* can still report to the leader it left.                                 *)
(*                                                                         *)
(* What the acknowledgement promises (the checks below):                   *)
(*                                                                         *)
(*   AckedHeld      -- no broker that could still win an election lacks an *)
(*                     acknowledged record, unless a power cut took that   *)
(*                     record from a copy before its fsync and it was not  *)
(*                     committed. Process crashes, leader changes and a    *)
(*                     membership change lose nothing.                     *)
(*   AckedNeverLost -- the same with no exception. It holds when no broker *)
(*                     loses power. With one power cut it does not, and    *)
(*                     FelixRaftWrittenPowerCut pins that trace: one copy  *)
(*                     of the written majority loses the record to a power *)
(*                     cut, and the voters that never had it elect with    *)
(*                     that broker while the remaining copy is not heard.  *)
(*                     The copy that loses power can be the leader's.      *)
(*   CommittedHeld  -- no broker that could still win an election lacks a  *)
(*                     committed entry, power cuts or not.                 *)
(*                                                                         *)
(* and OneLeaderPerTerm, LogMatching, LeaderCompleteness and               *)
(* StateMachineSafety as in FelixRaft.tla.                                 *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   LearnerCounts   -- a learner's report counts toward the majority.     *)
(*   OtherTermCounts -- a report counts by its index alone, whatever term  *)
(*                      its log id carries.                                *)
(*   JointOneHalf    -- in a joint configuration a majority of either half *)
(*                      is enough.                                         *)
(*   WrittenCommits  -- the commit index counts written reports too.       *)
(*                                                                         *)
(* Abstracted: votes are durable and followers do not learn the commit     *)
(* index (FelixRaft.tla has the adapter's rules and what followers apply), *)
(* snapshots, the subscriber and the lease. One membership change: the     *)
(* first voter set to `NewVoters` through a joint configuration.           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    InitVoters,
    InitLearners,
    NewVoters,
    MaxTerm,
    MaxWrites,
    MaxChanges,
    MaxCrashes,
    MaxPowerCuts,
    LearnerCounts,
    OtherTermCounts,
    JointOneHalf,
    WrittenCommits

VARIABLES
    role,       \* "follower", "candidate" or "leader"
    term,
    votedFor,
    granted,    \* the votes a candidate has
    elected,    \* history: every <<term, broker>> that won an election
    log,        \* what is written
    synced,     \* how far the log is on disk
    commitIdx,  \* what a leader has committed; a power cut can take it back
    match,      \* a leader's view of each member's fsynced, matching prefix
    wrote,      \* a leader's latest written report from each member: [term, idx]
    committed,  \* history: the committed sequence, each entry with the term that committed it
    acked,      \* history: [val, by, joint] for each record acknowledged
    lost,       \* history: the record values a power cut took from each broker
    used        \* budgets spent: writes, changes, crashes, cuts

election == <<role, term, votedFor, granted, elected>>
raft == <<log, synced, commitIdx, match, committed>>
vars == <<election, raft, wrote, acked, lost, used>>

\* Only brokers that every named set treats alike permute.
Symm == Permutations(InitVoters \cap NewVoters)

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

NoCfg == [old |-> {}, new |-> {}, learners |-> {}]
InitCfg == [old |-> InitVoters, new |-> InitVoters, learners |-> InitLearners]
Blank(t) == [term |-> t, kind |-> "blank", val |-> 0, cfg |-> NoCfg]
Record(t, v) == [term |-> t, kind |-> "record", val |-> v, cfg |-> NoCfg]
Member(t, c) == [term |-> t, kind |-> "member", val |-> 0, cfg |-> c]
NoId == [term |-> 0, idx |-> 0]

LastIdx(s) == Len(log[s])
LastTerm(s) == IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])].term
MemberIdxs(s) == {i \in 1..Len(log[s]) : log[s][i].kind = "member"}
LastMember(s) == IF MemberIdxs(s) = {} THEN 0 ELSE MaxOf(MemberIdxs(s))
\* The latest membership in the broker's own log, committed or not.
Cfg(s) == IF LastMember(s) = 0 THEN InitCfg ELSE log[s][LastMember(s)].cfg

Joint(c) == c.old # c.new
Voters(c) == c.old \cup c.new
Members(c) == Voters(c) \cup c.learners
Majority(Q, V) == 2 * Cardinality(Q \cap V) > Cardinality(V)
\* A joint configuration needs both sets; otherwise the two are the same set.
Quorum(Q, c) == Majority(Q, c.old) /\ Majority(Q, c.new)

\* The vote's log comparison: c's log is at least as up to date as s's.
UpToDate(c, s) ==
    \/ LastTerm(c) > LastTerm(s)
    \/ LastTerm(c) = LastTerm(s) /\ LastIdx(c) >= LastIdx(s)

\* How far two logs agree by (index, term).
Common(i, j) ==
    MaxOf({q \in 0..Min(LastIdx(i), LastIdx(j)) : q = 0 \/ log[i][q].term = log[j][q].term})

\* The leader's count for the entry at n. A report is evidence for its
\* sender and its log id only: the id has to carry the leader's own term.
Reported(i, j, n) ==
    /\ wrote[i][j].idx >= n
    /\ OtherTermCounts \/ wrote[i][j].term = term[i]
Wrote(i, n) == {i} \cup {j \in Servers \ {i} : Reported(i, j, n)}

WrittenHalf(Q, V, c) ==
    2 * Cardinality(Q \cap (IF LearnerCounts THEN V \cup c.learners ELSE V)) > Cardinality(V)
WrittenQuorum(Q, c) ==
    IF JointOneHalf
    THEN WrittenHalf(Q, c.old, c) \/ WrittenHalf(Q, c.new, c)
    ELSE WrittenHalf(Q, c.old, c) /\ WrittenHalf(Q, c.new, c)

AckedVals == {a.val : a \in acked}

Init ==
    /\ role = [s \in Servers |-> "follower"]
    /\ term = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nobody]
    /\ granted = [s \in Servers |-> {}]
    /\ elected = {}
    /\ log = [s \in Servers |-> <<>>]
    /\ synced = [s \in Servers |-> 0]
    /\ commitIdx = [s \in Servers |-> 0]
    /\ match = [s \in Servers |-> [t \in Servers |-> 0]]
    /\ wrote = [s \in Servers |-> [t \in Servers |-> NoId]]
    /\ committed = <<>>
    /\ acked = {}
    /\ lost = [s \in Servers |-> {}]
    /\ used = [writes |-> 0, changes |-> 0, crashes |-> 0, cuts |-> 0]

\* Only a voter of the configuration it holds campaigns.
Campaign(c) ==
    /\ role[c] # "leader"
    /\ c \in Voters(Cfg(c))
    /\ term[c] < MaxTerm
    /\ term' = [term EXCEPT ![c] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<elected, raft, wrote, acked, lost, used>>

\* A voter compares the candidate's log with its own, unsynced entries
\* included.
GrantVote(s, c) ==
    /\ s # c
    /\ role[c] = "candidate"
    /\ s \in Voters(Cfg(c))
    /\ term[s] < term[c]
    /\ UpToDate(c, s)
    /\ term' = [term EXCEPT ![s] = term[c]]
    /\ votedFor' = [votedFor EXCEPT ![s] = c]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {s}, ![s] = {}]
    /\ UNCHANGED <<elected, raft, wrote, acked, lost, used>>

BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c], Cfg(c))
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<term[c], c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(term[c]))]
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<term, votedFor, granted, synced, commitIdx, committed, wrote, acked, lost, used>>

AppendRecord(i) ==
    /\ role[i] = "leader"
    /\ used.writes < MaxWrites
    /\ log' = [log EXCEPT ![i] = Append(@, Record(term[i], used.writes + 1))]
    /\ used' = [used EXCEPT !.writes = @ + 1]
    /\ UNCHANGED <<election, synced, commitIdx, match, committed, wrote, acked, lost>>

\* The one change: into the joint configuration, once the previous
\* membership is committed.
EnterJoint(i) ==
    /\ role[i] = "leader"
    /\ used.changes < MaxChanges
    /\ ~Joint(Cfg(i))
    /\ Cfg(i).new # NewVoters
    /\ commitIdx[i] >= LastMember(i)
    /\ log' = [log EXCEPT ![i] = Append(@, Member(term[i],
            [old |-> Cfg(i).new, new |-> NewVoters, learners |-> Cfg(i).learners \ NewVoters]))]
    /\ used' = [used EXCEPT !.changes = @ + 1]
    /\ UNCHANGED <<election, synced, commitIdx, match, committed, wrote, acked, lost>>

\* openraft leaves the joint configuration once its entry is committed. A
\* new leader that finds one does the same.
LeaveJoint(i) ==
    /\ role[i] = "leader"
    /\ Joint(Cfg(i))
    /\ commitIdx[i] >= LastMember(i)
    /\ log' = [log EXCEPT ![i] = Append(@, Member(term[i],
            [old |-> Cfg(i).new, new |-> Cfg(i).new, learners |-> Cfg(i).learners]))]
    /\ UNCHANGED <<election, synced, commitIdx, match, committed, wrote, acked, lost, used>>

\* AppendEntries with one entry after the highest index the two logs agree
\* on, to a voter or a learner of the leader's configuration. A follower
\* holding something else there cuts it off first.
Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ j \in Members(Cfg(i))
    /\ term[j] <= term[i]
    /\ LET p == Common(i, j)
       IN /\ p < LastIdx(i)
          /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p), log[i][p + 1])]
          /\ synced' = [synced EXCEPT ![j] = Min(@, p)]
    /\ term' = [term EXCEPT ![j] = term[i]]
    /\ votedFor' = [votedFor EXCEPT ![j] = IF term[j] < term[i] THEN i ELSE @]
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ UNCHANGED <<elected, commitIdx, match, committed, wrote, acked, lost, used>>

Flush(s) ==
    /\ synced[s] < LastIdx(s)
    /\ synced' = [synced EXCEPT ![s] = LastIdx(s)]
    /\ UNCHANGED <<election, log, commitIdx, match, committed, wrote, acked, lost, used>>

\* The follower's answer to an append: how far its log matches the
\* leader's and is fsynced.
Ack(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ LET p == Min(Common(i, j), synced[j])
       IN /\ p > match[i][j]
          /\ match' = [match EXCEPT ![i][j] = p]
    /\ UNCHANGED <<election, log, synced, commitIdx, committed, wrote, acked, lost, used>>

\* The written report: the id of the sender's last entry, fsynced or not.
\* Nothing ties it to the sender's vote or to the leader's term.
Report(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ LET id == [term |-> LastTerm(j), idx |-> LastIdx(j)]
       IN /\ id # wrote[i][j]
          /\ wrote' = [wrote EXCEPT ![i][j] = id]
    /\ UNCHANGED <<election, raft, acked, lost, used>>

\* The early acknowledgement: a record of the leader's own term that a
\* majority of the voters has written.
AckWritten(i) ==
    /\ role[i] = "leader"
    /\ \E n \in 1..LastIdx(i) :
        LET e == log[i][n]
        IN /\ e.kind = "record"
           /\ e.term = term[i]
           /\ e.val \notin AckedVals
           /\ WrittenQuorum(Wrote(i, n), Cfg(i))
           /\ acked' = acked \cup {[val |-> e.val, by |-> "written", joint |-> Joint(Cfg(i))]}
    /\ UNCHANGED <<election, raft, wrote, lost, used>>

\* The commit index counts fsynced copies only, the leader's own included.
Agree(i, n) ==
    {j \in Servers :
        IF j = i THEN synced[i] >= n \/ WrittenCommits
        ELSE match[i][j] >= n \/ (WrittenCommits /\ Reported(i, j, n))}

\* An entry of the leader's own term on a majority's disks commits it and
\* everything before it. `await_commit` then acknowledges any record there
\* that the written count has not.
AdvanceCommit(i) ==
    /\ role[i] = "leader"
    /\ LET can == {n \in (commitIdx[i] + 1)..LastIdx(i) :
                    /\ log[i][n].term = term[i]
                    /\ Quorum(Agree(i, n), Cfg(i))}
       IN /\ can # {}
          /\ LET n == MaxOf(can)
                 have == Len(committed)
                 new == {log[i][k].val : k \in {x \in (commitIdx[i] + 1)..n : log[i][x].kind = "record"}}
             IN /\ commitIdx' = [commitIdx EXCEPT ![i] = n]
                /\ committed' =
                    IF have >= n THEN committed
                    ELSE committed \o [k \in 1..(n - have) |->
                                        [e |-> log[i][have + k], by |-> term[i]]]
                /\ acked' = acked \cup
                    {[val |-> v, by |-> "commit", joint |-> Joint(Cfg(i))] : v \in new \ AckedVals}
    /\ UNCHANGED <<election, log, synced, match, wrote, lost, used>>

Forget(s) ==
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ match' = [match EXCEPT ![s] = [t \in Servers |-> 0]]
    /\ wrote' = [wrote EXCEPT ![s] = [t \in Servers |-> NoId]]

\* The process dies and starts again. What it wrote is in the kernel.
Crash(s) ==
    /\ used.crashes < MaxCrashes
    /\ Forget(s)
    /\ used' = [used EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<term, votedFor, elected, log, synced, commitIdx, committed, acked, lost>>

\* The machine loses power and takes some of the unsynced tail with it.
PowerCut(s) ==
    /\ used.cuts < MaxPowerCuts
    /\ \E n \in {x \in synced[s]..LastIdx(s) : x < LastIdx(s)} :
        /\ log' = [log EXCEPT ![s] = SubSeq(@, 1, n)]
        /\ commitIdx' = [commitIdx EXCEPT ![s] = Min(@, n)]
        /\ lost' = [lost EXCEPT ![s] = @ \cup
                {log[s][k].val : k \in {x \in (n + 1)..LastIdx(s) : log[s][x].kind = "record"}}]
    /\ Forget(s)
    /\ used' = [used EXCEPT !.cuts = @ + 1]
    /\ UNCHANGED <<term, votedFor, elected, synced, committed, acked>>

Next ==
    \/ \E s \in Servers :
        \/ Campaign(s) \/ BecomeLeader(s)
        \/ AppendRecord(s) \/ EnterJoint(s) \/ LeaveJoint(s)
        \/ Flush(s) \/ AckWritten(s) \/ AdvanceCommit(s)
        \/ Crash(s) \/ PowerCut(s)
    \/ \E i, j \in Servers :
        \/ GrantVote(i, j)
        \/ Replicate(i, j) \/ Ack(i, j) \/ Report(i, j)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Servers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ term[s] \in 0..MaxTerm
        /\ synced[s] <= LastIdx(s)
        /\ commitIdx[s] <= LastIdx(s)
    /\ AckedVals \subseteq 1..MaxWrites
    /\ \A a, b \in acked : a.val = b.val => a = b

OneLeaderPerTerm == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

LogMatching ==
    \A s, t \in Servers : \A i \in 1..Min(LastIdx(s), LastIdx(t)) :
        log[s][i].term = log[t][i].term =>
            \A k \in 1..i : log[s][k] = log[t][k]

LeaderCompleteness ==
    \A l \in Servers : role[l] = "leader" =>
        \A i \in 1..Len(committed) : committed[i].by < term[l] =>
            i <= LastIdx(l) /\ log[l][i] = committed[i].e

StateMachineSafety ==
    \A s \in Servers : \A i \in 1..commitIdx[s] :
        i <= Len(committed) /\ log[s][i] = committed[i].e

Holds(s, v) == \E i \in 1..LastIdx(s) : log[s][i].kind = "record" /\ log[s][i].val = v
IsCommitted(v) ==
    \E i \in 1..Len(committed) : committed[i].e.kind = "record" /\ committed[i].e.val = v
\* No power cut took the record from a copy.
Untouched(v) == \A s \in Servers : v \notin lost[s]

\* With the logs as they stand, a later term could elect this broker: it
\* is a voter of the configuration it holds, and the brokers whose logs are
\* not ahead of its own make a majority of each voter set.
Electable(s) ==
    /\ s \in Voters(Cfg(s))
    /\ Quorum({t \in Servers : UpToDate(s, t)}, Cfg(s))

CommittedHeld ==
    \A s \in Servers : Electable(s) =>
        \A i \in 1..Len(committed) : i <= LastIdx(s) /\ log[s][i] = committed[i].e

AckedHeld ==
    \A s \in Servers : Electable(s) =>
        \A v \in AckedVals : (Untouched(v) \/ IsCommitted(v)) => Holds(s, v)

AckedNeverLost ==
    \A s \in Servers : Electable(s) => \A v \in AckedVals : Holds(s, v)

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

\* A record is acknowledged on the written count, ahead of its commit.
NeverAcksEarly == \A a \in acked : a.by # "written"

\* The same while the leader's configuration is joint.
NeverAcksEarlyInJoint == \A a \in acked : ~(a.by = "written" /\ a.joint)

\* A record acknowledged early is committed later, by a leader of a later
\* term than the one that wrote it.
NeverCommitsAfterFailover ==
    ~\E a \in acked : a.by = "written" /\
        \E i \in 1..Len(committed) :
            /\ committed[i].e.kind = "record" /\ committed[i].e.val = a.val
            /\ committed[i].by > committed[i].e.term

=============================================================================
