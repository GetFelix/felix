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
(*                     loses power, and with a power cut when the cut      *)
(*                     rotates the broker's copy (CutRotates, as a broker  *)
(*                     that vouched does: FelixRaftWrittenPowerCut). Without *)
(*                     that it does not, and FelixRaftWrittenPowerCutKeeps *)
(*                     pins the trace: one copy of the written majority    *)
(*                     loses the record to a power cut, and the voters     *)
(*                     that never had it elect with that broker while the  *)
(*                     remaining copy is not heard. The copy that loses    *)
(*                     power can be the leader's.                          *)
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
(* WrittenCommits is also the proposal for relaxed roots in                *)
(* docs/per-shard-raft.md ("Proposal: commit on a written majority"):      *)
(* openraft hears an append is flushed once it is written, so the commit   *)
(* index, apply and delivery move on a written majority. With it go:       *)
(*                                                                         *)
(*   Reserve    -- a copy counts (or reports) an entry only below an index *)
(*                 it has reserved in a synced file that a rotation keeps. *)
(*   MaxReforms -- 1 lets the operator re-form a group that lost a         *)
(*                 majority to power cuts (Reform). The new epoch starts   *)
(*                 above the longest log any copy kept, padded up to the   *)
(*                 floor: the last commit index a leader reported to the   *)
(*                 control plane (ReportFloor), and with Reserve the       *)
(*                 highest reservation of any copy. FelixRaftReform.tla    *)
(*                 has the command's steps and the floor's terms. Here     *)
(*                 the base is the longest log, which is right only with   *)
(*                 one term, and the configurations with Vouch off have    *)
(*                 one.                                                    *)
(*   Vouch      -- the re-form takes the vouch rule of the design for      *)
(*                 #2250 instead of the longest log: let T be the highest  *)
(*                 term of any vote it read (a live copy's term, a cut     *)
(*                 copy's kept vote) and of the floor; a copy vouches for  *)
(*                 its saved commit, for the floor's entry if it holds     *)
(*                 it, and for its whole log if its last entry is of term  *)
(*                 T. The base is the longest vouched prefix. A declared   *)
(*                 dead broker is not read (DeclareDead, up to MaxDead);   *)
(*                 its vote is unknown. While the dead are a minority of   *)
(*                 each voter set, every election took a vote the re-form  *)
(*                 read, so T stands; once they hold a majority of one,    *)
(*                 the group is marked and no copy vouches for its whole   *)
(*                 log.                                                    *)
(*   FloorHolder -- the shortcut the rule replaces: when some copy holds   *)
(*                 the floor's entry, the base is the copy with the        *)
(*                 highest last log id.                                    *)
(*                                                                         *)
(*   OffsetNeverReused   -- after a re-form, no committed offset holds     *)
(*                          another record: each is in the base or below   *)
(*                          where the new epoch starts. A subscriber is    *)
(*                          given only committed records.                  *)
(*   NeverLosesCommitted -- a re-form keeps every committed record. A      *)
(*                          witness under WrittenCommits.                  *)
(*   LossNeedsLostDisk   -- with no broker declared dead, a re-form keeps  *)
(*                          every committed entry a copy it read holds. A  *)
(*                          witness: a vote in a term that wrote nothing   *)
(*                          cuts every copy to its saved commit.           *)
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
    WrittenCommits,
    CutRotates,
    Reserve,
    MaxReforms,
    MaxDead,
    Vouch,
    FloorHolder

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
    used,       \* budgets spent: writes, changes, crashes, cuts
    gone,       \* brokers whose copy a power cut rotated (CutRotates)
    kept,       \* what a rotated copy kept, read by a re-form
    keptCommit, \* the commit index a rotated copy saved, cut like its log
    keptVote,   \* the term (and vote) a rotated copy held when it was cut
    dead,       \* brokers the operator declared dead: a re-form does not read them
    reserve,    \* the index each copy reserved, synced; a rotation keeps it
    cpFloor,    \* the highest commit index a leader reported to the control plane
    reform      \* the re-form, once: the base and where the new epoch starts

election == <<role, term, votedFor, granted, elected>>
raft == <<log, synced, commitIdx, match, committed>>
floor == <<kept, keptCommit, keptVote, dead, reserve, cpFloor, reform>>
vars == <<election, raft, wrote, acked, lost, used, gone, floor>>

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
NoReform == [on |-> FALSE, base |-> <<>>, start |-> 0]
NoFloor == [idx |-> 0, term |-> 0]

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
\* With Reserve, a leader counts its own unsynced entry only below its
\* reservation.
SelfWrote(i, n) == ~Reserve \/ reserve[i] >= n
Wrote(i, n) == (IF SelfWrote(i, n) THEN {i} ELSE {}) \cup {j \in Servers \ {i} : Reported(i, j, n)}

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
    /\ used = [writes |-> 0, changes |-> 0, crashes |-> 0, cuts |-> 0, reforms |-> 0]
    /\ gone = {}
    /\ kept = [s \in Servers |-> <<>>]
    /\ keptCommit = [s \in Servers |-> 0]
    /\ keptVote = [s \in Servers |-> 0]
    /\ dead = {}
    /\ reserve = [s \in Servers |-> 0]
    /\ cpFloor = NoFloor
    /\ reform = NoReform

\* Only a voter of the configuration it holds campaigns.
Campaign(c) ==
    /\ role[c] # "leader"
    /\ c \notin gone
    /\ c \in Voters(Cfg(c))
    /\ term[c] < MaxTerm
    /\ term' = [term EXCEPT ![c] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<elected, raft, wrote, acked, lost, gone, used, floor>>

\* A voter compares the candidate's log with its own, unsynced entries
\* included.
GrantVote(s, c) ==
    /\ s # c
    /\ s \notin gone
    /\ role[c] = "candidate"
    /\ s \in Voters(Cfg(c))
    /\ term[s] < term[c]
    /\ UpToDate(c, s)
    /\ term' = [term EXCEPT ![s] = term[c]]
    /\ votedFor' = [votedFor EXCEPT ![s] = c]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {s}, ![s] = {}]
    /\ UNCHANGED <<elected, raft, wrote, acked, lost, gone, used, floor>>

BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c], Cfg(c))
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<term[c], c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(term[c]))]
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<term, votedFor, granted, synced, commitIdx, committed, wrote, acked, lost, gone, used, floor>>

AppendRecord(i) ==
    /\ role[i] = "leader"
    /\ used.writes < MaxWrites
    /\ log' = [log EXCEPT ![i] = Append(@, Record(term[i], used.writes + 1))]
    /\ used' = [used EXCEPT !.writes = @ + 1]
    /\ UNCHANGED <<election, synced, commitIdx, match, committed, wrote, acked, lost, gone, floor>>

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
    /\ UNCHANGED <<election, synced, commitIdx, match, committed, wrote, acked, lost, gone, floor>>

\* openraft leaves the joint configuration once its entry is committed. A
\* new leader that finds one does the same.
LeaveJoint(i) ==
    /\ role[i] = "leader"
    /\ Joint(Cfg(i))
    /\ commitIdx[i] >= LastMember(i)
    /\ log' = [log EXCEPT ![i] = Append(@, Member(term[i],
            [old |-> Cfg(i).new, new |-> Cfg(i).new, learners |-> Cfg(i).learners]))]
    /\ UNCHANGED <<election, synced, commitIdx, match, committed, wrote, acked, lost, gone, used, floor>>

\* AppendEntries with one entry after the highest index the two logs agree
\* on, to a voter or a learner of the leader's configuration. A follower
\* holding something else there cuts it off first.
Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ j \in Members(Cfg(i))
    /\ j \notin gone
    /\ term[j] <= term[i]
    /\ LET p == Common(i, j)
       IN /\ p < LastIdx(i)
          /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p), log[i][p + 1])]
          /\ synced' = [synced EXCEPT ![j] = Min(@, p)]
    /\ term' = [term EXCEPT ![j] = term[i]]
    /\ votedFor' = [votedFor EXCEPT ![j] = IF term[j] < term[i] THEN i ELSE @]
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ UNCHANGED <<elected, commitIdx, match, committed, wrote, acked, lost, gone, used, floor>>

Flush(s) ==
    /\ synced[s] < LastIdx(s)
    /\ synced' = [synced EXCEPT ![s] = LastIdx(s)]
    /\ UNCHANGED <<election, log, commitIdx, match, committed, wrote, acked, lost, gone, used, floor>>

\* The follower's answer to an append: how far its log matches the
\* leader's and is fsynced.
Ack(i, j) ==
    /\ i # j
    /\ j \notin gone
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ LET p == Min(Min(Common(i, j), synced[j]), IF Reserve THEN reserve[j] ELSE synced[j])
       IN /\ p > match[i][j]
          /\ match' = [match EXCEPT ![i][j] = p]
    /\ UNCHANGED <<election, log, synced, commitIdx, committed, wrote, acked, lost, gone, used, floor>>

\* The written report: the id of the sender's last entry, fsynced or not.
\* Nothing ties it to the sender's vote or to the leader's term.
Report(i, j) ==
    /\ i # j
    /\ j \notin gone
    /\ role[i] = "leader"
    /\ ~Reserve \/ reserve[j] >= LastIdx(j)
    /\ LET id == [term |-> LastTerm(j), idx |-> LastIdx(j)]
       IN /\ id # wrote[i][j]
          /\ wrote' = [wrote EXCEPT ![i][j] = id]
    /\ UNCHANGED <<election, raft, acked, lost, gone, used, floor>>

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
    /\ UNCHANGED <<election, raft, wrote, lost, gone, used, floor>>

\* The commit index counts fsynced copies only, the leader's own included.
Agree(i, n) ==
    {j \in Servers :
        IF j = i THEN (synced[i] >= n /\ SelfWrote(i, n)) \/ (WrittenCommits /\ SelfWrote(i, n))
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
    /\ UNCHANGED <<election, log, synced, match, wrote, lost, gone, used, floor>>

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
    /\ UNCHANGED <<term, votedFor, elected, log, synced, commitIdx, committed, acked, lost, gone, floor>>

\* The machine loses power and takes some of the unsynced tail with it.
\* With CutRotates the broker comes back under a new storage id, so its copy
\* rotates: the old identity is gone from the voter set and the new copy
\* starts with an empty log and no vote. It never votes, campaigns or
\* answers as that seat again; adding the new copy needs a membership
\* change, which this model does not make. The leader keeps whatever it
\* already heard from the old copy. The broker rotates only a copy that
\* vouched for an unsynced record, and rotates it even when the cut took
\* nothing; this model rotates every cut copy and cuts only unsynced tails.
PowerCut(s) ==
    /\ used.cuts < MaxPowerCuts
    /\ s \notin gone
    /\ \E n \in {x \in synced[s]..LastIdx(s) : x < LastIdx(s)} :
        /\ log' = [log EXCEPT ![s] = IF CutRotates THEN <<>> ELSE SubSeq(@, 1, n)]
        /\ commitIdx' = [commitIdx EXCEPT ![s] = IF CutRotates THEN 0 ELSE Min(@, n)]
        /\ kept' = IF CutRotates /\ MaxReforms > 0
                   THEN [kept EXCEPT ![s] = SubSeq(log[s], 1, n)] ELSE kept
        /\ keptCommit' = IF CutRotates /\ MaxReforms > 0 /\ Vouch
                         THEN [keptCommit EXCEPT ![s] = Min(commitIdx[s], n)] ELSE keptCommit
        /\ lost' = [lost EXCEPT ![s] = @ \cup
                {log[s][k].val : k \in {x \in (n + 1)..LastIdx(s) : log[s][x].kind = "record"}}]
    /\ synced' = [synced EXCEPT ![s] = IF CutRotates THEN 0 ELSE @]
    /\ votedFor' = [votedFor EXCEPT ![s] = IF CutRotates THEN Nobody ELSE @]
    /\ keptVote' = IF CutRotates /\ MaxReforms > 0 /\ Vouch
                   THEN [keptVote EXCEPT ![s] = term[s]] ELSE keptVote
    /\ gone' = IF CutRotates THEN gone \cup {s} ELSE gone
    /\ Forget(s)
    /\ used' = [used EXCEPT !.cuts = @ + 1]
    /\ UNCHANGED <<term, elected, committed, acked, dead, reserve, cpFloor, reform>>

\* With the logs as they stand, a later term could elect this broker: it
\* is a voter of the configuration it holds, and the brokers whose logs are
\* not ahead of its own make a majority of each voter set. A broker a power
\* cut rotated neither stands nor votes.
Electable(s) ==
    /\ s \notin gone
    /\ s \in Voters(Cfg(s))
    /\ Quorum({t \in Servers \ gone : UpToDate(s, t)}, Cfg(s))

\* A copy reserves up to its last entry in a synced file. The broker would
\* reserve far ahead, in steps; a larger reservation only widens the gap a
\* re-form leaves.
ReserveTo(s) ==
    /\ Reserve
    /\ s \notin gone
    /\ reserve[s] < LastIdx(s)
    /\ reserve' = [reserve EXCEPT ![s] = LastIdx(s)]
    /\ UNCHANGED <<election, raft, wrote, acked, lost, used, gone, kept, keptCommit, keptVote, dead, cpFloor, reform>>

\* The leader's report of its commit index, which the control plane keeps
\* as the floor. It trails the commit by as long as the report takes.
ReportFloor(i) ==
    /\ MaxReforms > 0
    /\ role[i] = "leader"
    /\ commitIdx[i] > cpFloor.idx
    /\ cpFloor' = [idx |-> commitIdx[i], term |-> log[i][commitIdx[i]].term]
    /\ UNCHANGED <<election, raft, wrote, acked, lost, used, gone, kept, keptCommit, keptVote, dead, reserve, reform>>

\* The operator declares a broker dead: its disk is lost. It never acts
\* again, and a re-form reads nothing of it, its reservation included.
DeclareDead(s) ==
    /\ MaxReforms > 0
    /\ Cardinality(dead) < MaxDead
    /\ s \notin dead
    /\ dead' = dead \cup {s}
    /\ gone' = gone \cup {s}
    /\ Forget(s)
    /\ UNCHANGED <<term, votedFor, elected, log, synced, commitIdx, committed, acked, lost, used,
                   kept, keptCommit, keptVote, reserve, cpFloor, reform>>

\* What the re-form reads: the logs of copies still in place, and what each
\* rotated copy kept. Nothing of a dead broker.
Copies == {log[s] : s \in Servers \ gone} \cup {kept[s] : s \in gone \ dead}
ReformFloor == Max(cpFloor.idx, IF Reserve THEN MaxOf({0} \cup {reserve[s] : s \in Servers \ dead}) ELSE 0)

\* A read copy as the vouch rule sees it: its log, saved commit and vote.
Read == {[l |-> log[s], c |-> commitIdx[s], v |-> term[s]] : s \in Servers \ gone}
    \cup {[l |-> kept[s], c |-> keptCommit[s], v |-> keptVote[s]] : s \in gone \ dead}
LTerm(l) == IF l = <<>> THEN 0 ELSE l[Len(l)].term
\* T: the highest term of any vote read, and of the floor. Votes cover
\* every entry, since a copy holds no entry above its term.
TopTerm == MaxOf({cpFloor.term} \cup {r.v : r \in Read})
HoldsFloor(l) == cpFloor.idx > 0 /\ Len(l) >= cpFloor.idx /\ l[cpFloor.idx].term = cpFloor.term
\* T is the top elected term unless the dead hold a majority of a voter set:
\* any other election took a vote from a broker the re-form read.
VotesRead == \A V \in {InitVoters, NewVoters} : ~Majority(dead, V)
VouchLen(r) ==
    Max(r.c, Max(IF HoldsFloor(r.l) THEN cpFloor.idx ELSE 0,
                 IF VotesRead /\ LTerm(r.l) = TopTerm THEN Len(r.l) ELSE 0))
Vouched == {SubSeq(r.l, 1, VouchLen(r)) : r \in Read}
HighestLog(S) == CHOOSE l \in S : \A m \in S :
    LTerm(m) < LTerm(l) \/ (LTerm(m) = LTerm(l) /\ Len(m) <= Len(l))
Longest(S) == CHOOSE l \in S : \A m \in S : Len(m) <= Len(l)
ReformBase ==
    IF FloorHolder /\ \E l \in Copies : HoldsFloor(l) THEN HighestLog(Copies)
    ELSE IF Vouch THEN Longest(Vouched)
    ELSE Longest(Copies)

\* The operator's re-form, once no broker leads and none can be elected.
Reform ==
    /\ used.reforms < MaxReforms
    /\ \A s \in Servers : role[s] # "leader" /\ ~Electable(s)
    /\ Copies # {}
    /\ LET base == ReformBase
       IN reform' = [on |-> TRUE, base |-> base, start |-> Max(Len(base), ReformFloor)]
    /\ used' = [used EXCEPT !.reforms = @ + 1]
    /\ UNCHANGED <<election, raft, wrote, acked, lost, gone, kept, keptCommit, keptVote, dead, reserve, cpFloor>>

\* After the re-form the old epoch is over, and the model stops there.
Next ==
    \/ Reform
    \/ /\ ~reform.on
       /\ \/ \E s \in Servers :
                \/ Campaign(s) \/ BecomeLeader(s)
                \/ AppendRecord(s) \/ EnterJoint(s) \/ LeaveJoint(s)
                \/ Flush(s) \/ AckWritten(s) \/ AdvanceCommit(s)
                \/ Crash(s) \/ PowerCut(s)
                \/ ReserveTo(s) \/ ReportFloor(s) \/ DeclareDead(s)
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
    /\ gone \subseteq Servers
    /\ \A a, b \in acked : a.val = b.val => a = b
    /\ DOMAIN kept = Servers /\ DOMAIN reserve = Servers
    /\ \A s \in Servers :
        /\ DOMAIN kept[s] = 1..Len(kept[s])
        /\ reserve[s] \in Nat
    /\ cpFloor.idx \in Nat /\ cpFloor.term \in Nat
    /\ dead \subseteq gone
    /\ DOMAIN reform = {"on", "base", "start"}
    /\ reform.on \in BOOLEAN
    /\ DOMAIN reform.base = 1..Len(reform.base)
    /\ reform.start \in Nat
    /\ ~reform.on => reform = NoReform

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

CommittedHeld ==
    \A s \in Servers : Electable(s) =>
        \A i \in 1..Len(committed) : i <= LastIdx(s) /\ log[s][i] = committed[i].e

AckedHeld ==
    \A s \in Servers : Electable(s) =>
        \A v \in AckedVals : (Untouched(v) \/ IsCommitted(v)) => Holds(s, v)

AckedNeverLost ==
    \A s \in Servers : Electable(s) => \A v \in AckedVals : Holds(s, v)

\* A committed offset after a re-form is the same record in the base, or a
\* gap below where the new epoch starts. Never another record.
OffsetNeverReused ==
    reform.on =>
        \A k \in 1..Len(committed) :
            IF k <= Len(reform.base) THEN reform.base[k] = committed[k].e
            ELSE k <= reform.start

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


\* A re-form keeps every committed record. Holds when the commit counts
\* fsynced copies. With WrittenCommits a power cut of a majority loses a
\* committed, delivered record, and TLC must find it.
NeverLosesCommitted ==
    reform.on => Len(committed) <= Len(reform.base)

\* With no broker declared dead, an entry that a read copy holds at its
\* index and that was committed is in the base.
LossNeedsLostDisk ==
    (reform.on /\ dead = {}) =>
        \A k \in 1..Len(committed) :
            (\E l \in Copies : k <= Len(l) /\ l[k] = committed[k].e) => k <= Len(reform.base)

\* The positive half of LossNeedsLostDisk: with no broker declared dead and
\* no vote read above every entry read, T is the highest entry's term, the
\* copy holding that entry is kept whole, and a committed entry a read copy
\* holds is in the base.
LTop == MaxOf({LTerm(r.l) : r \in Read})
LossNeedsLostDiskAtTop ==
    (reform.on /\ dead = {} /\ TopTerm = LTop /\ LTop > 0) =>
        \A k \in 1..Len(committed) :
            (\E l \in Copies : k <= Len(l) /\ l[k] = committed[k].e) => k <= Len(reform.base)

\* A re-form after a leader of term 2 was elected cuts the longest copy it
\* read: the rule is not vacuous across two terms.
NeverCutsAcrossTerms ==
    reform.on /\ (\E e \in elected : e[1] = 2) => Len(reform.base) = Len(Longest(Copies))

=============================================================================
