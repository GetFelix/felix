---------------------------- MODULE FelixRaftElect ----------------------------
(***************************************************************************)
(* Election liveness for one Raft group, as openraft 0.10 runs it with     *)
(* Felix's timers (`raft/runtime.rs`, `raft/group.rs`): pre-vote on, each  *)
(* member's election timeout randomised, a leader lease, and a restarted   *)
(* leader standing again at once (`enable_leader_restore`).                *)
(*                                                                         *)
(* FelixRaft.tla checks safety and, on states where nothing more can       *)
(* happen, that a standing leader serves (`QuiescentServed`). It never     *)
(* asks whether a leader is elected at all. This module does, as temporal  *)
(* properties under weak fairness:                                         *)
(*                                                                         *)
(*   ElectionSettles  -- once a majority stays up and connected, the group *)
(*                       ends up with a leader among them that none of     *)
(*                       them has outvoted, and keeps it.                  *)
(*   EventuallyServes -- the same leader has its blank entry on a          *)
(*                       majority, so the shard is open (`lead.rs`         *)
(*                       `observe`), and every member of that majority     *)
(*                       follows it with its log.                          *)
(*                                                                         *)
(* Storage is left out: votes and logs survive a crash whole, as           *)
(* FelixRaft.tla shows they must. Logs hold only blank entries, so a       *)
(* log is its sequence of terms. A message is one atomic step between two  *)
(* members, and an append copies the leader's whole log.                   *)
(*                                                                         *)
(* Faults are crashes and cuts (one member cut off from every other), at  *)
(* most MaxFaults of them. A crashed member may come back and a cut may    *)
(* heal, but nothing makes either happen. The faults stopping is the       *)
(* stable suffix the properties ask for: after the last one, who is up and *)
(* connected never changes again.                                          *)
(*                                                                         *)
(* Time is abstract, as in FelixRaftSleep.tla. An election timeout fires   *)
(* only while no leader the member would follow can reach it: that        *)
(* leader's heartbeat comes first. The randomised timeout is what keeps    *)
(* two members' rounds apart. The model cannot say how long that takes, so *)
(* with Staggered at most Splits timeouts in a behaviour fire while some   *)
(* round is open: a pre-vote that passed and has not stood yet, or a      *)
(* candidate that can still win. Without Staggered they fire whenever.     *)
(* Splits = 1 rests on openraft drawing each timeout from [1 s, 2 s)      *)
(* (`raft/runtime.rs:152`, passed at `:280`) on top of a fixed per-member *)
(* offset (`raft/group.rs:339`), against a vote round of one round trip:  *)
(* two draws that close are rare, two in one election are taken as none.  *)
(*                                                                         *)
(* Terms are bounded by MaxTerm. A behaviour that uses them all and has no *)
(* leader violates the properties, so the passing configurations set it   *)
(* above what their faults and splits can spend.                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Servers,
    Nobody,
    MaxTerm,
    MaxFaults,
    Splits,
    Staggered

VARIABLES
    up,       \* the member's process runs
    cut,      \* the member is cut off from every other
    role,     \* "follower", "candidate" or "leader"
    vote,     \* [term, node, committed], durable
    pre,      \* a pre-vote passed and the member has not stood yet
    granted,  \* the votes a candidate has
    log,      \* the terms of the entries, durable
    acked,    \* the members a leader has given its log in its term
    open,     \* the leader's blank entry is committed: the shard is open
    elected,  \* history: every <<term, member>> that won an election
    used      \* budgets spent: faults, splits

vars == <<up, cut, role, vote, pre, granted, log, acked, open, elected, used>>

Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

NoVote == [term |-> 0, node |-> Nobody, committed |-> FALSE]

\* openraft's order on votes with one leader per term, as in FelixRaft.tla.
VoteGE(a, b) ==
    \/ a.term > b.term
    \/ /\ a.term = b.term
       /\ \/ a = b
          \/ a.committed /\ ~b.committed
          \/ ~a.committed /\ ~b.committed /\ b.node \in {a.node, Nobody}
VoteGT(a, b) == VoteGE(a, b) /\ a # b
Uncommitted(v) == [v EXCEPT !.committed = FALSE]
NextVote(c) == [term |-> vote[c].term + 1, node |-> c, committed |-> FALSE]

Link(i, j) == i # j /\ up[i] /\ up[j] /\ ~cut[i] /\ ~cut[j]
Healthy == {s \in Servers : up[s] /\ ~cut[s]}

\* openraft compares last log ids: term, then index.
LastTerm(s) == IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])]
LogOK(c, s) ==
    \/ LastTerm(c) > LastTerm(s)
    \/ LastTerm(c) = LastTerm(s) /\ Len(log[c]) >= Len(log[s])

\* A leader `s` would follow reaches it, so its timeout does not fire.
Heard(s) == \E l \in Servers : Link(l, s) /\ role[l] = "leader" /\ VoteGE(vote[l], vote[s])

\* A follower's lease on the leader it follows, renewed by its heartbeats.
LeaseHeld(s) ==
    vote[s].committed /\ \E l \in Servers : Link(l, s) /\ role[l] = "leader" /\ vote[l] = vote[s]

\* A leader's quorum-ack lease: a majority still takes its heartbeats.
LeaderLease(s) ==
    role[s] = "leader" /\ Quorum({s} \cup {t \in Servers : Link(s, t) /\ vote[t] = vote[s]})

\* openraft's `handle_pre_vote_req`.
PreGrants(s, c) ==
    /\ Link(c, s)
    /\ ~LeaseHeld(s) /\ ~LeaderLease(s)
    /\ LogOK(c, s)
    /\ VoteGE(NextVote(c), vote[s])

\* openraft's `handle_vote_req`. The lease does not hold back a campaign by
\* the leader it was taken on.
WouldGrant(s, c) ==
    /\ ~(LeaseHeld(s) /\ vote[s].node # c)
    /\ ~LeaderLease(s)
    /\ LogOK(c, s)
    /\ VoteGE(vote[c], vote[s])

\* A round that can still end in a leader. A timeout during one is a split.
RoundOpen(d) ==
    /\ up[d]
    /\ \/ pre[d]
       \/ /\ role[d] = "candidate"
          /\ Quorum(granted[d] \cup {s \in Servers : Link(d, s) /\ WouldGrant(s, d)})
Split == \E d \in Servers : RoundOpen(d)

\* Every change of vote ends a pending pre-vote, as openraft clears its
\* pre-candidate when the vote moves.
Votes(new) ==
    /\ vote' = new
    /\ pre' = [s \in Servers |-> pre[s] /\ new[s] = vote[s]]

Init ==
    /\ up = [s \in Servers |-> TRUE]
    /\ cut = [s \in Servers |-> FALSE]
    /\ role = [s \in Servers |-> "follower"]
    /\ vote = [s \in Servers |-> NoVote]
    /\ pre = [s \in Servers |-> FALSE]
    /\ granted = [s \in Servers |-> {}]
    /\ log = [s \in Servers |-> <<>>]
    /\ acked = [s \in Servers |-> {}]
    /\ open = [s \in Servers |-> FALSE]
    /\ elected = {}
    /\ used = [faults |-> 0, splits |-> 0]

\* The election timeout fires and the member runs a pre-vote. It passes on a
\* majority, or a refusal names a higher vote and the member takes that
\* uncommitted (`handle_pre_vote_resp`, and `pre_vote_answer` in
\* raft/network.rs).
Timeout(c) ==
    /\ up[c] /\ role[c] # "leader" /\ ~Heard(c)
    /\ Split /\ Staggered => used.splits < Splits
    /\ used' = IF Split /\ Staggered THEN [used EXCEPT !.splits = @ + 1] ELSE used
    /\ \/ /\ ~pre[c]
          /\ vote[c].term < MaxTerm
          /\ Quorum({c} \cup {s \in Servers : PreGrants(s, c)})
          /\ pre' = [pre EXCEPT ![c] = TRUE]
          /\ UNCHANGED <<vote, role, granted>>
       \/ \E s \in Servers :
          /\ Link(c, s) /\ ~PreGrants(s, c)
          /\ VoteGT(Uncommitted(vote[s]), vote[c])
          /\ Votes([vote EXCEPT ![c] = Uncommitted(vote[s])])
          /\ role' = [role EXCEPT ![c] = "follower"]
          /\ granted' = [granted EXCEPT ![c] = {}]
    /\ UNCHANGED <<up, cut, log, acked, open, elected>>

\* The pre-vote's majority is in: the member stands for the next term.
Elect(c) ==
    /\ up[c] /\ pre[c] /\ role[c] # "leader" /\ vote[c].term < MaxTerm
    /\ Votes([vote EXCEPT ![c] = NextVote(c)])
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<up, cut, log, acked, open, elected, used>>

\* `s` answers `c`'s vote request. A refusal that names a higher vote ends
\* the candidacy, the vote taken uncommitted (`handle_vote_resp`).
AnswerVote(s, c) ==
    /\ s # c /\ up[c] /\ role[c] = "candidate" /\ Link(c, s) /\ s \notin granted[c]
    /\ IF WouldGrant(s, c)
       THEN LET moved == vote[s] # vote[c]
            IN /\ Votes([vote EXCEPT ![s] = vote[c]])
               /\ granted' = [granted EXCEPT ![c] = @ \cup {s}, ![s] = IF moved THEN {} ELSE @]
               /\ role' = [role EXCEPT ![s] = IF moved THEN "follower" ELSE @]
               /\ open' = [open EXCEPT ![s] = @ /\ ~moved]
               /\ acked' = [acked EXCEPT ![s] = IF moved THEN {} ELSE @]
       ELSE /\ VoteGT(Uncommitted(vote[s]), vote[c])
            /\ Votes([vote EXCEPT ![c] = Uncommitted(vote[s])])
            /\ role' = [role EXCEPT ![c] = "follower"]
            /\ granted' = [granted EXCEPT ![c] = {}]
            /\ UNCHANGED <<open, acked>>
    /\ UNCHANGED <<up, cut, log, elected, used>>

\* The winner commits its vote and appends its blank entry.
BecomeLeader(c) ==
    /\ up[c] /\ role[c] = "candidate" /\ Quorum(granted[c])
    /\ Votes([vote EXCEPT ![c].committed = TRUE])
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ granted' = [granted EXCEPT ![c] = {}]
    /\ log' = [log EXCEPT ![c] = Append(@, vote[c].term)]
    /\ acked' = [acked EXCEPT ![c] = {}]
    /\ elected' = elected \cup {<<vote[c].term, c>>}
    /\ UNCHANGED <<up, cut, open, used>>

\* An append or heartbeat. A member whose vote is not below the leader's
\* refuses with it, and the leader takes it and steps down.
Replicate(l, s) ==
    /\ role[l] = "leader" /\ Link(l, s)
    /\ IF VoteGE(vote[l], vote[s])
       THEN /\ Votes([vote EXCEPT ![s] = vote[l]])
            /\ role' = [role EXCEPT ![s] = "follower"]
            /\ granted' = [granted EXCEPT ![s] = {}]
            /\ log' = [log EXCEPT ![s] = log[l]]
            /\ acked' = [acked EXCEPT ![l] = @ \cup {s}, ![s] = {}]
            /\ open' = [open EXCEPT ![s] = FALSE]
       ELSE /\ VoteGE(vote[s], vote[l])
            /\ Votes([vote EXCEPT ![l] = vote[s]])
            /\ role' = [role EXCEPT ![l] = "follower"]
            /\ acked' = [acked EXCEPT ![l] = {}]
            /\ open' = [open EXCEPT ![l] = FALSE]
            /\ UNCHANGED <<granted, log>>
    /\ UNCHANGED <<up, cut, elected, used>>

\* A majority holds the leader's blank entry, so it commits and the shard
\* opens.
Open(l) ==
    /\ up[l] /\ role[l] = "leader" /\ ~open[l]
    /\ Quorum({l} \cup acked[l])
    /\ open' = [open EXCEPT ![l] = TRUE]
    /\ UNCHANGED <<up, cut, role, vote, pre, granted, log, acked, elected, used>>

Crash(s) ==
    /\ up[s] /\ used.faults < MaxFaults
    /\ up' = [up EXCEPT ![s] = FALSE]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ pre' = [pre EXCEPT ![s] = FALSE]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ acked' = [acked EXCEPT ![s] = {}]
    /\ open' = [open EXCEPT ![s] = FALSE]
    /\ used' = [used EXCEPT !.faults = @ + 1]
    /\ UNCHANGED <<cut, vote, log, elected>>

\* A member whose stored vote is its own committed one stands again as it
\* starts, with no pre-vote (`enable_leader_restore`).
Recover(s) ==
    /\ ~up[s]
    /\ up' = [up EXCEPT ![s] = TRUE]
    /\ LET restore == vote[s].committed /\ vote[s].node = s /\ vote[s].term < MaxTerm
       IN /\ vote' = IF restore THEN [vote EXCEPT ![s] = NextVote(s)] ELSE vote
          /\ role' = [role EXCEPT ![s] = IF restore THEN "candidate" ELSE "follower"]
          /\ granted' = [granted EXCEPT ![s] = IF restore THEN {s} ELSE {}]
    /\ UNCHANGED <<cut, pre, log, acked, open, elected, used>>

Cut(s) ==
    /\ ~cut[s] /\ used.faults < MaxFaults
    /\ cut' = [cut EXCEPT ![s] = TRUE]
    /\ used' = [used EXCEPT !.faults = @ + 1]
    /\ UNCHANGED <<up, role, vote, pre, granted, log, acked, open, elected>>

Heal(s) ==
    /\ cut[s]
    /\ cut' = [cut EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, role, vote, pre, granted, log, acked, open, elected, used>>

Next ==
    \/ \E s \in Servers :
        \/ Timeout(s) \/ Elect(s) \/ BecomeLeader(s) \/ Open(s)
        \/ Crash(s) \/ Recover(s) \/ Cut(s) \/ Heal(s)
    \/ \E s, t \in Servers : AnswerVote(s, t) \/ Replicate(s, t)

Spec == Init /\ [][Next]_vars

\* What liveness assumes: a step a member that is up takes once it stays
\* possible. Every one needs its member up, and every message needs both
\* ends up and connected, so a crashed or cut member is owed nothing.
\* Faults, restarts and heals are never owed.
ElectFairness ==
    /\ \A s \in Servers : WF_vars(Timeout(s)) /\ WF_vars(Elect(s)) /\ WF_vars(BecomeLeader(s))
    /\ \A s, t \in Servers : WF_vars(AnswerVote(s, t)) /\ WF_vars(Replicate(s, t))

LiveSpec == Spec /\ ElectFairness /\ \A s \in Servers : WF_vars(Open(s))

\* The same with the commit of the blank entry not owed.
NoOpenSpec == Spec /\ ElectFairness

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Servers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ vote[s].term \in 0..MaxTerm
        /\ vote[s].node \in Servers \cup {Nobody}
        /\ granted[s] \subseteq Servers
        /\ acked[s] \subseteq Servers
    /\ used.faults <= MaxFaults
    /\ used.splits <= Splits

OneLeaderPerTerm == \A x, y \in elected : x[1] = y[1] => x[2] = y[2]

\* A leader up and connected that no member up and connected has outvoted.
Settled(l) ==
    /\ l \in Healthy
    /\ role[l] = "leader"
    /\ \A s \in Healthy : VoteGE(vote[l], vote[s])

\* That leader is open and every member up and connected follows it.
Serves(l) ==
    /\ Settled(l)
    /\ open[l]
    /\ \A s \in Healthy : vote[s] = vote[l] /\ log[s] = log[l]

\* SANY gives Cardinality of a state-level set level 0, and TLC then evaluates
\* it before it has a state. So the majority is found by a subset instead.
MajorityHealthy == \E Q \in SUBSET Servers : Quorum(Q) /\ Q \subseteq Healthy
SettledLeaders == {l \in Servers : Settled(l)}
ServingLeaders == {l \in Servers : Serves(l)}

\* Terms are bounded and a new leader needs a new term, so a leader that is
\* eventually always there is eventually always the same one.
ElectionSettles == (<>[]MajorityHealthy) => <>[](SettledLeaders # {})

EventuallyServes == (<>[]MajorityHealthy) => <>[](ServingLeaders # {})

=============================================================================
