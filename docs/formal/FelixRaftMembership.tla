------------------------- MODULE FelixRaftMembership -------------------------
(***************************************************************************)
(* Membership change for one Raft group, as docs/per-shard-raft.md decides *)
(* it ("Membership", "Adopting the leader's appends"), and the write-fence *)
(* quiesce around the entries openraft writes itself.                      *)
(*                                                                         *)
(* Membership. The voter set is carried by membership entries in the log.  *)
(* A broker uses the latest one in its own log, committed or not. A        *)
(* configuration is [old, new, learners]; it is joint while old and new    *)
(* differ, and then an election or a commit needs a majority of both. A    *)
(* learner receives entries and is never counted. A move is: add the       *)
(* newcomer as a learner, enter the joint configuration once it is level,  *)
(* leave it. openraft leaves the joint configuration itself once the joint *)
(* entry is committed; a new leader that finds one repeats the call. A     *)
(* change starts only after the previous membership entry is committed. A  *)
(* leader outside the new set steps down once that set is committed. The   *)
(* design transfers leadership first; only the step-down is modelled.      *)
(*                                                                         *)
(* Fence. A publish claims the next offset on the leader (`begin_append`)  *)
(* and later hands the record to Raft (`appended`), which gives it the     *)
(* next index. A membership entry gets the next index too, so the engine   *)
(* holds the fence, waits until no claim is left, and only then calls. The *)
(* fence reopens when every entry the call proposed is committed, or the   *)
(* call was refused before appending. If Raft assigns an index a claimed   *)
(* record is not at, the group stops.                                      *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   OneLeaderPerTerm   -- no term elects two brokers, whichever           *)
(*                         configurations the voters hold.                 *)
(*   LogMatching        -- two logs that agree on an entry's term agree on *)
(*                         everything up to it.                            *)
(*   LeaderCompleteness -- a leader holds every entry committed in an      *)
(*                         earlier term.                                   *)
(*   StateMachineSafety -- every broker's committed prefix is the one      *)
(*                         committed sequence.                             *)
(*   AckedSurvive       -- no broker that could still win an election      *)
(*                         lacks an acknowledged record. "Held by a        *)
(*                         majority of each voter set" is not an invariant *)
(*                         here: a record committed under the leader's     *)
(*                         latest configuration need not be on a majority  *)
(*                         of an older one, and the vote's log comparison  *)
(*                         is what protects it.                            *)
(*   NeverStops         -- Raft never assigns an index a claimed record is *)
(*                         not at.                                         *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   SkipJoint            -- a move swaps one voter for another in one     *)
(*                           entry, with no joint configuration.           *)
(*   ChangeBeforeCommit   -- a change is proposed while the previous       *)
(*                           membership entry is uncommitted.              *)
(*   LearnerCounts        -- a learner's vote and its copy count toward a  *)
(*                           majority.                                     *)
(*   ClaimRacesMembership -- the membership call is made with the fence    *)
(*                           open, so a publish can hold a claim.          *)
(*   ReopenOnReturn       -- the fence reopens when the call returned or   *)
(*                           timed out, with an entry still queued or      *)
(*                           uncommitted.                                  *)
(*                                                                         *)
(* A removed leader that does not step down is not a safety matter: it     *)
(* commits with the new set's majority and never counts itself. It costs   *)
(* liveness (a leader the routing table should not point at), so there is  *)
(* no twin for it. The step-down is an action that may happen, and the     *)
(* checks pass whether or not it does.                                     *)
(*                                                                         *)
(* Abstracted: storage (an entry in a log is durable; FelixRaft.tla covers *)
(* the adapter's fsync rules), crashes, snapshots, the subscriber, and the *)
(* lease. A message is one atomic step between two brokers. A deposed      *)
(* leader's claims are dropped, which is the adapter's term gate. A broker *)
(* dropped from the set gets no more entries and may still campaign with   *)
(* the configuration it last saw.                                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    InitVoters,
    InitLearners,
    MaxTerm,
    MaxWrites,
    MaxCalls,
    SkipJoint,
    ChangeBeforeCommit,
    LearnerCounts,
    ClaimRacesMembership,
    ReopenOnReturn

VARIABLES
    role,       \* "follower", "candidate" or "leader"
    term,
    votedFor,
    granted,    \* the votes a candidate has
    elected,    \* history: every <<term, broker>> that won an election
    log,
    commitIdx,
    match,      \* a leader's view of each member's matching prefix
    committed,  \* history: the committed sequence, each entry with the term that committed it
    fence,      \* "closed", "open", or "held" around a membership call
    pend,       \* claims not yet handed to Raft: [off, val], in commit order
    mq,         \* a membership entry queued inside openraft, at most one
    mwait,      \* index of the last entry the leader's call appended, 0 if none
    stopped,    \* Raft assigned an index the claimed record is not at
    used        \* budgets spent: writes, calls

election == <<role, term, votedFor, granted, elected>>
raft == <<log, commitIdx, match, committed>>
quiesce == <<fence, pend, mq, mwait>>
vars == <<election, raft, quiesce, stopped, used>>

\* The newcomer is told apart from the first voters, so only those permute.
Symm == Permutations(InitVoters)

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

NoCfg == [old |-> {}, new |-> {}, learners |-> {}]
InitCfg == [old |-> InitVoters, new |-> InitVoters, learners |-> InitLearners]
Blank(t) == [term |-> t, kind |-> "blank", val |-> 0, cfg |-> NoCfg]
Record(t, v) == [term |-> t, kind |-> "record", val |-> v, cfg |-> NoCfg]
Member(t, c) == [term |-> t, kind |-> "member", val |-> 0, cfg |-> c]

LastIdx(s) == Len(log[s])
LastTerm(s) == IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])].term
MemberIdxs(s) == {i \in 1..Len(log[s]) : log[s][i].kind = "member"}
LastMember(s) == IF MemberIdxs(s) = {} THEN 0 ELSE MaxOf(MemberIdxs(s))
\* The latest membership in the broker's own log, committed or not.
Cfg(s) == IF LastMember(s) = 0 THEN InitCfg ELSE log[s][LastMember(s)].cfg

Joint(c) == c.old # c.new
Voters(c) == c.old \cup c.new
Members(c) == Voters(c) \cup c.learners
Counted(c) == IF LearnerCounts THEN Members(c) ELSE Voters(c)
Majority(Q, V, c) ==
    2 * Cardinality(Q \cap (IF LearnerCounts THEN V \cup c.learners ELSE V)) > Cardinality(V)
\* A joint configuration needs both sets; otherwise the two are the same set.
Quorum(Q, c) == Majority(Q, c.old, c) /\ Majority(Q, c.new, c)

\* The vote's log comparison: c's log is at least as up to date as s's.
UpToDate(c, s) ==
    \/ LastTerm(c) > LastTerm(s)
    \/ LastTerm(c) = LastTerm(s) /\ LastIdx(c) >= LastIdx(s)

\* The configurations a leader may propose next. From a joint one the only
\* step is to leave it. A move swaps one voter for a learner that is level.
NextCfgs(s) ==
    LET c == Cfg(s) IN
    IF Joint(c)
    THEN {[old |-> c.new, new |-> c.new, learners |-> c.learners]}
    ELSE {[c EXCEPT !.learners = @ \cup {x}] : x \in Servers \ Members(c)}
         \cup {LET to == (c.old \ {x}) \cup {l}
               IN [old |-> IF SkipJoint THEN to ELSE c.old,
                   new |-> to,
                   learners |-> c.learners \ {l}] :
                 x \in c.old, l \in {m \in c.learners : match[s][m] = LastIdx(s)}}

\* An entry of the leader's call is still queued, or appended and not yet
\* known committed, or openraft has the second entry of a joint change left
\* to write.
CallOpen(s) ==
    \/ mq[s] # <<>>
    \/ /\ mwait[s] # 0
       /\ \/ commitIdx[s] < mwait[s]
          \/ Joint(log[s][mwait[s]].cfg)

\* An entry at the next index would land on an offset a publish claimed.
Collides(s) == \E k \in 1..Len(pend[s]) : pend[s][k].off = LastIdx(s) + 1

\* Brokers in S are no longer leading: the fence closes, the term gate fails
\* their claims, and openraft drops what it had queued.
Deposed(S) ==
    /\ fence' = [s \in Servers |-> IF s \in S THEN "closed" ELSE fence[s]]
    /\ pend' = [s \in Servers |-> IF s \in S THEN <<>> ELSE pend[s]]
    /\ mq' = [s \in Servers |-> IF s \in S THEN <<>> ELSE mq[s]]
    /\ mwait' = [s \in Servers |-> IF s \in S THEN 0 ELSE mwait[s]]

Init ==
    /\ role = [s \in Servers |-> "follower"]
    /\ term = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nobody]
    /\ granted = [s \in Servers |-> {}]
    /\ elected = {}
    /\ log = [s \in Servers |-> <<>>]
    /\ commitIdx = [s \in Servers |-> 0]
    /\ match = [s \in Servers |-> [t \in Servers |-> 0]]
    /\ committed = <<>>
    /\ fence = [s \in Servers |-> "closed"]
    /\ pend = [s \in Servers |-> <<>>]
    /\ mq = [s \in Servers |-> <<>>]
    /\ mwait = [s \in Servers |-> 0]
    /\ stopped = FALSE
    /\ used = [writes |-> 0, calls |-> 0]

\* Only a voter of the configuration it holds campaigns.
Campaign(c) ==
    /\ role[c] # "leader"
    /\ c \in Voters(Cfg(c))
    /\ term[c] < MaxTerm
    /\ term' = [term EXCEPT ![c] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<elected, raft, quiesce, stopped, used>>

\* The candidate asks the brokers its own configuration counts.
GrantVote(s, c) ==
    /\ s # c
    /\ role[c] = "candidate"
    /\ s \in Counted(Cfg(c))
    /\ term[s] < term[c]
    /\ UpToDate(c, s)
    /\ term' = [term EXCEPT ![s] = term[c]]
    /\ votedFor' = [votedFor EXCEPT ![s] = c]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {s}, ![s] = {}]
    /\ Deposed({s})
    /\ UNCHANGED <<elected, raft, stopped, used>>

\* The blank entry needs no quiesce: the fence is still closed.
BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c], Cfg(c))
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<term[c], c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(term[c]))]
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<term, votedFor, granted, commitIdx, committed, quiesce, stopped, used>>

\* AppendEntries with one entry after the highest index the two logs agree
\* on, to a voter or a learner of the leader's configuration.
Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ j \in Members(Cfg(i))
    /\ term[j] <= term[i]
    /\ LET p == MaxOf({q \in 0..Min(LastIdx(i), LastIdx(j)) :
                        q = 0 \/ log[i][q].term = log[j][q].term})
       IN /\ p < LastIdx(i)
          /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p), log[i][p + 1])]
          /\ match' = [match EXCEPT ![i][j] = p + 1]
          /\ commitIdx' = [commitIdx EXCEPT ![j] = Max(@, Min(commitIdx[i], p + 1))]
    /\ term' = [term EXCEPT ![j] = term[i]]
    /\ votedFor' = [votedFor EXCEPT ![j] = IF term[j] < term[i] THEN i ELSE @]
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ Deposed({j})
    /\ UNCHANGED <<elected, committed, stopped, used>>

\* The leader counts copies under the latest configuration in its log, and
\* only for an entry of its own term.
AdvanceCommit(i) ==
    /\ role[i] = "leader"
    /\ LET can == {n \in (commitIdx[i] + 1)..LastIdx(i) :
                    /\ log[i][n].term = term[i]
                    /\ Quorum({i} \cup {j \in Servers : match[i][j] >= n}, Cfg(i))}
       IN /\ can # {}
          /\ LET n == MaxOf(can)
                 have == Len(committed)
             IN /\ commitIdx' = [commitIdx EXCEPT ![i] = n]
                /\ committed' =
                    IF have >= n THEN committed
                    ELSE committed \o [k \in 1..(n - have) |->
                                        [e |-> log[i][have + k], by |-> term[i]]]
    /\ UNCHANGED <<election, log, match, quiesce, stopped, used>>

LearnCommit(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ commitIdx[j] < Min(commitIdx[i], match[i][j])
    /\ commitIdx' = [commitIdx EXCEPT ![j] = Min(commitIdx[i], match[i][j])]
    /\ UNCHANGED <<election, log, match, committed, quiesce, stopped, used>>

\* `opened`: the blank entry of the leader's term is committed.
Open(s) ==
    /\ role[s] = "leader"
    /\ fence[s] = "closed"
    /\ commitIdx[s] > 0
    /\ log[s][commitIdx[s]].term = term[s]
    /\ fence' = [fence EXCEPT ![s] = "open"]
    /\ UNCHANGED <<election, raft, pend, mq, mwait, stopped, used>>

\* `begin_append`: the publish takes the next offset of the leader's log.
Claim(s) ==
    /\ role[s] = "leader"
    /\ fence[s] = "open"
    /\ used.writes < MaxWrites
    /\ LET off == IF pend[s] = <<>> THEN LastIdx(s) + 1 ELSE pend[s][Len(pend[s])].off + 1
       IN pend' = [pend EXCEPT ![s] = Append(@, [off |-> off, val |-> used.writes + 1])]
    /\ used' = [used EXCEPT !.writes = @ + 1]
    /\ UNCHANGED <<election, raft, fence, mq, mwait, stopped>>

\* `appended`: Raft gives the record the next index. The adapter expects to
\* find it there.
Handover(s) ==
    /\ role[s] = "leader"
    /\ pend[s] # <<>>
    /\ LET h == Head(pend[s])
       IN IF h.off = LastIdx(s) + 1
          THEN /\ log' = [log EXCEPT ![s] = Append(@, Record(term[s], h.val))]
               /\ UNCHANGED stopped
          ELSE /\ stopped' = TRUE
               /\ UNCHANGED log
    /\ pend' = [pend EXCEPT ![s] = Tail(@)]
    /\ UNCHANGED <<election, commitIdx, match, committed, fence, mq, mwait, used>>

\* The engine closes the fence for a membership step.
Hold(s) ==
    /\ role[s] = "leader"
    /\ fence[s] = "open"
    /\ ~CallOpen(s)
    /\ used.calls < MaxCalls
    /\ fence' = [fence EXCEPT ![s] = "held"]
    /\ UNCHANGED <<election, raft, pend, mq, mwait, stopped, used>>

\* `add_learner` or `change_membership`, once the host is quiesced and
\* every `appended` is handed over. openraft refuses a change while the
\* previous membership entry is uncommitted.
Call(s, c) ==
    /\ role[s] = "leader"
    /\ mq[s] = <<>>
    /\ used.calls < MaxCalls
    /\ c \in NextCfgs(s)
    /\ IF ClaimRacesMembership
       THEN fence[s] \in {"open", "held"}
       ELSE fence[s] = "held" /\ pend[s] = <<>>
    /\ \/ ChangeBeforeCommit
       \/ ~CallOpen(s) /\ commitIdx[s] >= LastMember(s)
    /\ mq' = [mq EXCEPT ![s] = <<c>>]
    /\ used' = [used EXCEPT !.calls = @ + 1]
    /\ UNCHANGED <<election, raft, fence, pend, mwait, stopped>>

\* openraft appends the queued entry at the next index.
AppendQueued(s) ==
    /\ role[s] = "leader"
    /\ mq[s] # <<>>
    /\ IF Collides(s)
       THEN /\ stopped' = TRUE
            /\ UNCHANGED <<log, mwait>>
       ELSE /\ log' = [log EXCEPT ![s] = Append(@, Member(term[s], mq[s][1]))]
            /\ mwait' = [mwait EXCEPT ![s] = LastIdx(s) + 1]
            /\ UNCHANGED stopped
    /\ mq' = [mq EXCEPT ![s] = <<>>]
    /\ UNCHANGED <<election, commitIdx, match, committed, fence, pend, used>>

\* openraft refuses the proposal before appending it. The entry is dead.
Refuse(s) ==
    /\ role[s] = "leader"
    /\ mq[s] # <<>>
    /\ mq' = [mq EXCEPT ![s] = <<>>]
    /\ UNCHANGED <<election, raft, fence, pend, mwait, stopped, used>>

\* The second entry of the same call: openraft leaves the joint
\* configuration once its entry is committed.
LeaveJoint(s) ==
    /\ role[s] = "leader"
    /\ mq[s] = <<>>
    /\ mwait[s] # 0
    /\ mwait[s] = LastMember(s)
    /\ Joint(Cfg(s))
    /\ commitIdx[s] >= mwait[s]
    /\ IF Collides(s)
       THEN /\ stopped' = TRUE
            /\ UNCHANGED <<log, mwait>>
       ELSE /\ log' = [log EXCEPT ![s] =
                        Append(@, Member(term[s], CHOOSE c \in NextCfgs(s) : TRUE))]
            /\ mwait' = [mwait EXCEPT ![s] = LastIdx(s) + 1]
            /\ UNCHANGED stopped
    /\ UNCHANGED <<election, commitIdx, match, committed, fence, pend, mq, used>>

\* The fence opens again when every entry the call proposed is committed
\* or dead.
Reopen(s) ==
    /\ role[s] = "leader"
    /\ fence[s] = "held"
    /\ ReopenOnReturn \/ ~CallOpen(s)
    /\ fence' = [fence EXCEPT ![s] = "open"]
    /\ UNCHANGED <<election, raft, pend, mq, mwait, stopped, used>>

\* A leader outside the committed new set stops leading.
StepDown(s) ==
    /\ role[s] = "leader"
    /\ ~Joint(Cfg(s))
    /\ s \notin Voters(Cfg(s))
    /\ commitIdx[s] >= LastMember(s)
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ Deposed({s})
    /\ UNCHANGED <<term, votedFor, granted, elected, raft, stopped, used>>

\* A stopped group takes no step.
Next ==
    /\ ~stopped
    /\ \/ \E s \in Servers :
            \/ Campaign(s) \/ BecomeLeader(s) \/ AdvanceCommit(s)
            \/ Open(s) \/ Claim(s) \/ Handover(s)
            \/ Hold(s) \/ AppendQueued(s) \/ Refuse(s) \/ LeaveJoint(s)
            \/ Reopen(s) \/ StepDown(s)
            \/ \E c \in NextCfgs(s) : Call(s, c)
       \/ \E i, j \in Servers :
            \/ GrantVote(i, j) \/ Replicate(i, j) \/ LearnCommit(i, j)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Servers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ term[s] \in 0..MaxTerm
        /\ commitIdx[s] <= LastIdx(s)
        /\ fence[s] \in {"closed", "open", "held"}
        /\ Len(mq[s]) <= 1
        /\ mwait[s] <= LastIdx(s)
        /\ role[s] # "leader" =>
            fence[s] = "closed" /\ pend[s] = <<>> /\ mq[s] = <<>> /\ mwait[s] = 0
    /\ used.writes \in 0..MaxWrites
    /\ used.calls \in 0..MaxCalls

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

\* `await_commit` returns for a record once the commit index passes it.
Acked == {committed[i].e.val : i \in {k \in 1..Len(committed) : committed[k].e.kind = "record"}}
Holds(s, v) == \E i \in 1..LastIdx(s) : log[s][i].kind = "record" /\ log[s][i].val = v

\* With the logs as they stand, a later term could elect this broker: it
\* is a voter of the configuration it holds, and the brokers whose logs are
\* not ahead of its own make a majority of each voter set.
Electable(s) ==
    /\ s \in Voters(Cfg(s))
    /\ Quorum({t \in Servers : UpToDate(s, t)}, Cfg(s))

AckedSurvive == \A s \in Servers : Electable(s) => \A v \in Acked : Holds(s, v)

NeverStops == ~stopped

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

IsMember(e) == e.kind = "member"

\* A whole move is committed: the first voter set gave way to another. With
\* no learner at the start and SkipJoint off, that took a learner entry, a
\* joint entry and the entry that leaves it.
NeverMoves ==
    ~\E i \in 1..Len(committed) :
        LET e == committed[i].e
        IN IsMember(e) /\ ~Joint(e.cfg) /\ e.cfg.new # InitVoters

\* A broker that won its term is a follower in that same term, outside the
\* committed voter set. Only the step-down gets it there.
NeverRemovedLeaderStepsDown ==
    ~\E s \in Servers :
        /\ <<term[s], s>> \in elected
        /\ role[s] = "follower"
        /\ ~Joint(Cfg(s))
        /\ s \notin Voters(Cfg(s))
        /\ commitIdx[s] >= LastMember(s)

\* A joint entry of one term is committed, and the entry that leaves it is
\* written and committed by the leader of a later term.
NeverFailoverInJoint ==
    ~\E i, j \in 1..Len(committed) :
        /\ i < j
        /\ IsMember(committed[i].e) /\ Joint(committed[i].e.cfg)
        /\ IsMember(committed[j].e) /\ ~Joint(committed[j].e.cfg)
        /\ committed[j].e.term > committed[i].e.term

=============================================================================
