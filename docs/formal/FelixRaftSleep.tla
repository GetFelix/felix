---------------------------- MODULE FelixRaftSleep ----------------------------
(***************************************************************************)
(* Sleep and wake for one Raft group, as docs/per-shard-raft.md describes  *)
(* them ("Store liveness", "Sleep and wake", "One timer for every group"). *)
(*                                                                         *)
(* Sleep and wake are deferred to v0.9.0 (decided in issue 2167), and so   *)
(* is the store liveness they rest on. Nothing here gates 0.8. The         *)
(* configurations stay registered so the design keeps its checks until it  *)
(* is built.                                                               *)
(*                                                                         *)
(* A member is awake or asleep. The state is held by the node ticker,      *)
(* outside Raft: an asleep member gets no ticks, so it starts no election, *)
(* and an asleep leader sends no heartbeat. Votes, appends and commits     *)
(* work on any message, and a Raft message wakes the member it reaches.    *)
(*                                                                         *)
(* The group starts with a leader in term 1 whose blank entry every member *)
(* holds committed. What the model adds to plain Raft:                     *)
(*                                                                         *)
(*   awake  -- whether the ticker ticks the member.                        *)
(*   hold   -- an awake member that gets no ticks yet: "ask" after a       *)
(*             liveness wake, "restart" after a broker restart.            *)
(*   prop   -- a leader's open sleep proposal: who was asked, who answered.*)
(*   reqs   -- sleep requests in flight. Each names the leader's vote,     *)
(*             last log id and commit index. A request outlives the        *)
(*             proposal and the leader that sent it.                       *)
(*   sup    -- store liveness: whether a broker supports a peer.           *)
(*   unseen -- a restart (a new store epoch) a peer has not noticed yet.   *)
(*                                                                         *)
(* Time is abstract. Each timeout is a step, enabled only when the event   *)
(* that would have beaten it cannot happen:                                *)
(*                                                                         *)
(*   - An awake follower's election timeout expires only while no live,    *)
(*     awake leader it would accept exists: such a leader's heartbeat      *)
(*     arrives first.                                                      *)
(*   - A hold ends only while no live leader the member would accept       *)
(*     exists: a healthy leader answers the ask, or notices the new epoch, *)
(*     inside the hold. This is an assumption about timing, not a result.  *)
(*     HoldTooShort drops it, and MaxAsksLost loses the ask so that        *)
(*     nothing answers.                                                    *)
(*   - A sleeping follower's two audit periods pass only while the leader  *)
(*     it follows is not live: a live sleeping leader audits every period. *)
(*                                                                         *)
(* Liveness is checked on states where nothing more can happen, as         *)
(* FelixRaft.tla's QuiescentServed does. Every budget is bounded and no    *)
(* step can repeat without spending one, so every behaviour ends, and the  *)
(* state it ends in is what "eventually" reaches. That keeps the check an  *)
(* invariant, which TLC can run with the symmetry set. FelixRaftSleepEnds  *)
(* checks the premise (every behaviour ends) as a temporal property under  *)
(* weak fairness, without symmetry.                                        *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   OneLeaderPerTerm, LogMatching, LeaderCompleteness, StateMachineSafety,*)
(*   AckedSurvive    -- as in FelixRaft.tla. With MaxDozes above 0 any     *)
(*                      member falls asleep at any moment with no          *)
(*                      handshake, in any state: safety does not rest on   *)
(*                      the handshake.                                     *)
(*   QuiescentLed    -- with a majority alive, the group ends with a live  *)
(*                      leader no live member has outvoted.                *)
(*   QuiescentLevel  -- under that leader every write was taken and        *)
(*                      committed, and every live member holds it.         *)
(*   AsleepIsLevel   -- if every live member is asleep they share the last *)
(*                      log id and the commit index.                       *)
(*   NoNeedlessDeposal -- unless a leader's broker died or restarted, no   *)
(*                      election is won and the first leader never stops   *)
(*                      leading. A leader that steps down for a campaign   *)
(*                      that never wins counts. A violation costs          *)
(*                      availability (a healthy leader deposed, writes     *)
(*                      stalled for an election), never safety.            *)
(*                                                                         *)
(* What NoNeedlessDeposal does not test. With the two timing assumptions   *)
(* above, a held or awake follower cannot campaign while a live leader it  *)
(* accepts can still answer, so the configurations that pass it show that  *)
(* the rules leave no other path to a deposal. They do not show that the   *)
(* hold is long enough. Two faults on the wake path are modelled, a lost   *)
(* ask (MaxAsksLost) and a hold shorter than the ask's round trip          *)
(* (HoldTooShort), and each deposes a healthy leader. A lost wake          *)
(* heartbeat is not modelled: with no clock it cannot be told from a late  *)
(* one, and the woken leader's next heartbeat follows it.                  *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   NoLivenessWake    -- a liveness change wakes nobody.                  *)
(*   NoAudit           -- no audit timer, for leaders or followers.        *)
(*   AcceptUnchecked   -- a member accepts a sleep request without         *)
(*                        comparing vote, last log id and commit index.    *)
(*   CampaignOnWake    -- a follower woken by liveness ticks at once       *)
(*                        instead of asking the leader's broker first.     *)
(*   LeaderSleepsFirst -- the leader sleeps as soon as it has sent the     *)
(*                        requests.                                        *)
(*   RestartNoHold     -- a restarted member ticks at once.                *)
(*   HoldTooShort      -- a hold ends before the leader could answer: the  *)
(*                        hold is shorter than the ask's round trip, or    *)
(*                        than the time a leader takes to notice a         *)
(*                        restarted broker's new epoch.                    *)
(*                                                                         *)
(* Two more knobs drop rules the design page does not state. The model     *)
(* needs both to pass, and TLC finds the trace without each:               *)
(*                                                                         *)
(*   HeartbeatKeepsProposal -- a heartbeat sent while a sleep proposal is  *)
(*                        open leaves it open. The heartbeat wakes a       *)
(*                        follower that already answered, the last answer  *)
(*                        arrives and the leader sleeps.                   *)
(*   SleepUnsupported  -- a follower accepts a sleep request from a leader *)
(*                        whose broker it does not support, or while it is *)
(*                        held.                                            *)
(*                                                                         *)
(* The passing configurations run with both rules on (issue 2310).         *)
(*                                                                         *)
(* Rules of the second design on issue 2239, each with a knob that turns   *)
(* it on (the older configurations keep them off):                        *)
(*                                                                         *)
(*   PreVote           -- a member runs a pre-vote round before it raises *)
(*                        its term. A member grants once its own window   *)
(*                        has lapsed; a sleeping leader's has lapsed too  *)
(*                        (openraft's handle_pre_vote_req).               *)
(*   VoteHold          -- a vote or pre-vote request that wakes a member  *)
(*                        is dropped, and the member is held and asks the *)
(*                        leader's broker first. A held member drops      *)
(*                        every vote and pre-vote request.                *)
(*   LeftOutAsked      -- the sleep request also goes to a member the     *)
(*                        leader left out.                                *)
(*   RefusalWakes      -- a refused sleep request wakes the leader.       *)
(*   AuditAsks         -- a follower woken by its audit timer asks first, *)
(*                        as a liveness wake does.                        *)
(*   LeaseLapses       -- a leader's lease lapses while it sleeps, and    *)
(*                        openraft refuses a write until a majority took  *)
(*                        a message sent after the wake. The write waits  *)
(*                        for that heartbeat round.                       *)
(*   MaxTtlWakes       -- a cache leader's expiry timer fires while it    *)
(*                        sleeps; it wakes and proposes the delete.       *)
(*                                                                         *)
(* And their twins, each dropping one rule:                               *)
(*                                                                         *)
(*   DroppedPreVoteGrants -- a pre-vote the hold drops counts as granted, *)
(*                        which openraft's default `pre_vote` does.       *)
(*   WriteBeforeRound  -- the first write after a wake is proposed at     *)
(*                        once, and openraft refuses it (LeaseExpired).   *)
(*   NoTtlWake         -- the expiry timer does not wake the leader.      *)
(*   MaxAuditsLost     -- not a rule but a fault: a sleeping leader's     *)
(*                        audit heartbeat to a follower is lost, so the   *)
(*                        follower's audit fires under a healthy leader.  *)
(*                                                                         *)
(* One gap is left as the design has it: a leader leaves out a member      *)
(* whose broker it does not support. After a false alarm that member is    *)
(* alive, gets no heartbeat from the sleeping leader and campaigns, or     *)
(* sleeps behind on an older request. The FalseAlarmFollower configs pin   *)
(* both as expected violations, and the passing configs raise false alarms *)
(* only about a leader's broker (AlarmsOnFollowers off).                   *)
(*                                                                         *)
(* Abstracted: storage (an append is durable, FelixRaft.tla covers the     *)
(* adapter), snapshots, membership changes, read rounds, leases beyond     *)
(* their lapse (LeaseLapses), ranked starts and the per-tick election cap  *)
(* (they shape timing only). A message is one atomic step between two      *)
(* brokers, except the sleep request. The leader knows a follower's commit *)
(* index exactly. Only a leader's broker dies, and a dead broker stays     *)
(* dead. A disk stall is a death here: the broker goes silent. Only sleep  *)
(* requests and answers are lost. A leader replicates entries a follower   *)
(* lacks without ticks, but a commit index reaches a follower only on a    *)
(* heartbeat.                                                              *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    MaxTerm,
    MaxWrites,
    MaxSleeps,          \* sleep proposals
    MaxAudits,          \* a sleeping leader's audit wakes
    MaxDeaths,
    MaxRestarts,
    MaxAlarms,          \* support withdrawn for a healthy broker
    MaxLost,            \* sleep requests and answers lost
    MaxAsksLost,        \* wake messages (the ask before campaigning) lost
    MaxDozes,           \* members falling asleep with no handshake
    LivenessMisses,     \* liveness never reports a dead broker
    AlarmsOnFollowers,  \* a false alarm can name any broker, not only a leader's
    NoLivenessWake,
    NoAudit,
    AcceptUnchecked,
    CampaignOnWake,
    LeaderSleepsFirst,
    RestartNoHold,
    HoldTooShort,
    HeartbeatKeepsProposal,
    SleepUnsupported,
    PreVote,
    VoteHold,
    DroppedPreVoteGrants,
    LeftOutAsked,
    RefusalWakes,
    AuditAsks,
    MaxAuditsLost,
    LeaseLapses,
    WriteBeforeRound,
    MaxTtlWakes,
    NoTtlWake

VARIABLES
    role,       \* "follower", "candidate" or "leader"
    vote,
    granted,    \* the votes a candidate has
    elected,    \* history: every <<term, broker>> that won an election
    log,
    match,      \* a leader's view of each follower's matching prefix
    commit,     \* commit index
    committed,  \* history: the committed sequence, each entry with the term that committed it
    acked,      \* record values acknowledged to their publisher
    awake,
    hold,       \* "none", "ask" or "restart"
    prop,       \* a leader's open sleep proposal
    reqs,       \* sleep requests in flight
    up,         \* live brokers
    sup,        \* sup[p][q]: p supports q
    unseen,     \* <<p, q>>: p has not noticed q's restart
    used,       \* budgets spent
    hist,       \* history flags for the checks and witnesses
    heard,      \* heard[l]: who took a message from leader l since it last woke or won
    pre,        \* pre[c]: pre-votes a pre-candidate holds, itself included; {} when none
    sleptWith,  \* sleptWith[l]: the members l counted when it last slept
    ttlDue      \* a cache expiry delete is due and not yet proposed

raft == <<role, vote, granted, elected, log, match, commit, committed, acked>>
sleep == <<awake, hold, prop, reqs>>
liveness == <<up, sup, unseen>>
extra == <<heard, pre, sleptWith, ttlDue>>
vars == <<raft, sleep, liveness, used, hist, extra>>

Symm == Permutations(Servers)

Min(a, b) == IF a <= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

Blank(t) == [term |-> t, val |-> 0]
NoProp == [asked |-> {}, answered |-> {}]

\* openraft's order on votes with one leader per term, as in FelixRaft.tla.
VoteGE(a, b) ==
    \/ a.term > b.term
    \/ /\ a.term = b.term
       /\ \/ a = b
          \/ a.committed /\ ~b.committed
          \/ ~a.committed /\ ~b.committed /\ b.node \in {a.node, Nobody}

Last(s) == [idx |-> Len(log[s]), term |-> IF log[s] = <<>> THEN 0 ELSE log[s][Len(log[s])].term]

\* The longest prefix on which two logs agree.
Prefix(i, j) ==
    MaxOf({p \in 0..Min(Len(log[i]), Len(log[j])) : p = 0 \/ log[i][p].term = log[j][p].term})

Leads(l) == l \in up /\ role[l] = "leader"

\* A live, awake leader this member would accept: its heartbeats arrive
\* before the member's election timeout.
HearsLeader(c) ==
    \E l \in Servers \ {c} : Leads(l) /\ awake[l] /\ VoteGE(vote[l], vote[c])

\* A majority took a message from l since it last woke: openraft's lease.
Fresh(l) == Quorum(heard[l] \cup {l})

Init ==
    \E l \in Servers :
        /\ role = [s \in Servers |-> IF s = l THEN "leader" ELSE "follower"]
        /\ vote = [s \in Servers |-> [term |-> 1, node |-> l, committed |-> TRUE]]
        /\ granted = [s \in Servers |-> {}]
        /\ elected = {<<1, l>>}
        /\ log = [s \in Servers |-> <<Blank(1)>>]
        /\ match = [s \in Servers |-> [t \in Servers |-> IF s = l THEN 1 ELSE 0]]
        /\ commit = [s \in Servers |-> 1]
        /\ committed = <<[e |-> Blank(1), by |-> 1]>>
        /\ acked = {}
        /\ awake = [s \in Servers |-> TRUE]
        /\ hold = [s \in Servers |-> "none"]
        /\ prop = [s \in Servers |-> NoProp]
        /\ reqs = {}
        /\ up = Servers
        /\ sup = [p \in Servers |-> [q \in Servers |-> TRUE]]
        /\ unseen = {}
        /\ used = [writes |-> 0, sleeps |-> 0, audits |-> 0, deaths |-> 0,
                   restarts |-> 0, alarms |-> 0, lost |-> 0, asks |-> 0, dozes |-> 0,
                   auditsLost |-> 0, ttls |-> 0]
        /\ hist = [excuse |-> FALSE, diedAsleep |-> FALSE, wroteAsleep |-> FALSE,
                   refused |-> FALSE, midHandshake |-> FALSE]
        /\ heard = [s \in Servers |-> IF s = l THEN Servers ELSE {}]
        /\ pre = [s \in Servers |-> {}]
        /\ sleptWith = [s \in Servers |-> {}]
        /\ ttlDue = FALSE

----------------------------------------------------------------------------
\* Raft. Only Campaign and the heartbeats need a tick.

Campaign(c) ==
    /\ c \in up
    /\ awake[c]
    /\ hold[c] = "none"
    /\ role[c] # "leader"
    /\ vote[c].term < MaxTerm
    /\ ~HearsLeader(c)
    /\ PreVote => Quorum(pre[c])
    /\ vote' = [vote EXCEPT ![c] = [term |-> vote[c].term + 1, node |-> c, committed |-> FALSE]]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ pre' = [pre EXCEPT ![c] = {}]
    /\ UNCHANGED <<elected, log, match, commit, committed, acked, sleep, liveness, used, hist,
                   heard, sleptWith, ttlDue>>

\* The pre-vote round, before the term is raised.
PreCampaign(c) ==
    /\ PreVote
    /\ c \in up
    /\ awake[c]
    /\ hold[c] = "none"
    /\ role[c] # "leader"
    /\ vote[c].term < MaxTerm
    /\ ~HearsLeader(c)
    /\ pre[c] = {}
    /\ pre' = [pre EXCEPT ![c] = {c}]
    /\ UNCHANGED <<raft, sleep, liveness, used, hist, heard, sleptWith, ttlDue>>

\* A pre-vote request. It wakes a sleeping member. openraft grants once the
\* member's window has lapsed, which it has for any sleeper, a sleeping
\* leader included. The hold drops it first; the network then answers Err,
\* unless DroppedPreVoteGrants, where the drop reads as a grant. A refusal
\* that carries a higher vote is adopted, uncommitted, and ends the round
\* (openraft's handle_pre_vote_resp): without it a member with the longer
\* log and one with the higher term refuse each other forever.
PreVoteReq(s, c) ==
    LET wakes == ~awake[s]
        held == VoteHold /\ (wakes \/ hold[s] # "none")
        engine == /\ vote[c].term + 1 > vote[s].term
                  /\ \/ Last(c).term > Last(s).term
                     \/ Last(c).term = Last(s).term /\ Last(c).idx >= Last(s).idx
                  /\ ~(awake[s] /\ role[s] = "leader")
                  /\ wakes \/ ~HearsLeader(s)
        grant == IF held THEN DroppedPreVoteGrants ELSE engine
        adopt == /\ ~held /\ ~grant
                 /\ VoteGE(vote[s], vote[c]) /\ vote[s] # vote[c]
    IN /\ PreVote
       /\ s # c
       /\ s \in up /\ c \in up
       /\ pre[c] # {}
       /\ s \notin pre[c]
       /\ wakes \/ grant \/ adopt
       /\ pre' = IF grant THEN [pre EXCEPT ![c] = @ \cup {s}]
                 ELSE IF adopt THEN [pre EXCEPT ![c] = {}] ELSE pre
       /\ vote' = IF adopt THEN [vote EXCEPT ![c] = [vote[s] EXCEPT !.committed = FALSE]]
                  ELSE vote
       /\ role' = IF adopt THEN [role EXCEPT ![c] = "follower"] ELSE role
       /\ granted' = IF adopt THEN [granted EXCEPT ![c] = {}] ELSE granted
       /\ awake' = [awake EXCEPT ![s] = TRUE]
       /\ hold' = IF VoteHold /\ wakes /\ role[s] # "leader"
                  THEN [hold EXCEPT ![s] = "ask"] ELSE hold
       /\ UNCHANGED <<elected, log, match, commit, committed, acked,
                      prop, reqs, liveness, used, hist, heard, sleptWith, ttlDue>>

\* A vote request that wakes a sleeper is dropped, and the member is held.
VoteWakesHeld(s, c) ==
    /\ VoteHold
    /\ s # c
    /\ s \in up /\ c \in up
    /\ role[c] = "candidate"
    /\ ~awake[s]
    /\ awake' = [awake EXCEPT ![s] = TRUE]
    /\ hold' = IF role[s] # "leader" THEN [hold EXCEPT ![s] = "ask"] ELSE hold
    /\ UNCHANGED <<raft, prop, reqs, liveness, used, hist, extra>>

\* The request wakes the voter and the answer wakes the candidate.
GrantVote(s, c) ==
    /\ s # c
    /\ s \in up /\ c \in up
    /\ role[c] = "candidate"
    /\ s \notin granted[c]
    /\ VoteHold => awake[s] /\ hold[s] = "none"
    /\ VoteGE(vote[c], vote[s])
    /\ \/ Last(c).term > Last(s).term
       \/ Last(c).term = Last(s).term /\ Last(c).idx >= Last(s).idx
    /\ LET rose == vote[s].term < vote[c].term
       IN /\ vote' = [vote EXCEPT ![s] = vote[c]]
          /\ role' = [role EXCEPT ![s] = IF rose THEN "follower" ELSE @]
          /\ granted' = [granted EXCEPT ![c] = @ \cup {s}, ![s] = IF rose THEN {} ELSE @]
          /\ prop' = [prop EXCEPT ![s] = IF rose THEN NoProp ELSE @]
    /\ awake' = [awake EXCEPT ![s] = TRUE, ![c] = TRUE]
    /\ UNCHANGED <<elected, log, match, commit, committed, acked, hold, reqs, liveness, used, hist,
                   extra>>

BecomeLeader(c) ==
    /\ c \in up
    /\ role[c] = "candidate"
    /\ Quorum(granted[c])
    /\ vote' = [vote EXCEPT ![c].committed = TRUE]
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<vote[c].term, c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(vote[c].term))]
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ heard' = [heard EXCEPT ![c] = {}]
    /\ hist' = [hist EXCEPT !.midHandshake =
                   @ \/ reqs # {} \/ \E s \in Servers : prop[s].asked # {}]
    /\ UNCHANGED <<granted, commit, committed, acked, sleep, liveness, used,
                   pre, sleptWith, ttlDue>>

\* What any message from a leader does to the member it reaches: the member
\* wakes, takes the leader's vote and counts as having heard a leader.
Hears(l, f) ==
    /\ vote' = [vote EXCEPT ![f] = vote[l]]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ granted' = [granted EXCEPT ![f] = {}]
    /\ awake' = [awake EXCEPT ![f] = TRUE]
    /\ hold' = [hold EXCEPT ![f] = "none"]
    /\ heard' = [heard EXCEPT ![l] = @ \cup {f}]
    /\ pre' = [pre EXCEPT ![f] = {}]

\* A heartbeat from an awake leader, where it changes something. One sent
\* while a sleep proposal is open closes the proposal: a follower that
\* already answered is awake again.
Heartbeat(l, f) ==
    /\ l # f
    /\ Leads(l) /\ awake[l]
    /\ f \in up
    /\ VoteGE(vote[l], vote[f])
    /\ \/ ~awake[f] \/ hold[f] # "none" \/ vote[f] # vote[l]
       \/ LeaseLapses /\ f \notin heard[l]
    /\ Hears(l, f)
    /\ prop' = [prop EXCEPT ![f] = NoProp,
                            ![l] = IF HeartbeatKeepsProposal THEN @ ELSE NoProp]
    /\ UNCHANGED <<elected, log, match, commit, committed, acked, reqs, liveness, used, hist,
                   sleptWith, ttlDue>>

\* AppendEntries with one entry, and the follower's answer. The leader's
\* replication stream needs no tick, so a sleeping leader still sends what a
\* follower lacks.
Replicate(l, f) ==
    /\ l # f
    /\ Leads(l)
    /\ f \in up
    /\ VoteGE(vote[l], vote[f])
    /\ LET p == Prefix(l, f)
       IN /\ p < Len(log[l])
          /\ log' = [log EXCEPT ![f] = Append(SubSeq(@, 1, p), log[l][p + 1])]
          /\ match' = [match EXCEPT ![l][f] = p + 1]
    /\ Hears(l, f)
    /\ prop' = [prop EXCEPT ![f] = NoProp]
    /\ UNCHANGED <<elected, commit, committed, acked, reqs, liveness, used, hist,
                   sleptWith, ttlDue>>

Agree(l, k) == {l} \cup {j \in Servers : match[l][j] >= k}

Committable(l) ==
    {k \in (commit[l] + 1)..Len(log[l]) :
        Quorum(Agree(l, k)) /\ log[l][k].term = vote[l].term}

AdvanceCommit(l) ==
    /\ Leads(l)
    /\ Committable(l) # {}
    /\ LET k == MaxOf(Committable(l))
       IN /\ commit' = [commit EXCEPT ![l] = k]
          /\ committed' = IF k <= Len(committed) THEN committed
                ELSE committed \o [x \in 1..(k - Len(committed)) |->
                        [e |-> log[l][Len(committed) + x], by |-> vote[l].term]]
          /\ acked' = acked \cup {log[l][i].val : i \in {j \in (commit[l] + 1)..k : log[l][j].val # 0}}
    /\ UNCHANGED <<role, vote, granted, elected, log, match, sleep, liveness, used, hist, extra>>

\* The commit index rides on a heartbeat, so the leader must be awake.
LearnCommit(l, f) ==
    /\ l # f
    /\ Leads(l) /\ awake[l]
    /\ f \in up
    /\ vote[f] = vote[l]
    /\ LET k == Min(Prefix(l, f), commit[l])
       IN /\ k > commit[f]
          /\ commit' = [commit EXCEPT ![f] = k]
    /\ awake' = [awake EXCEPT ![f] = TRUE]
    /\ hold' = [hold EXCEPT ![f] = "none"]
    /\ prop' = [prop EXCEPT ![l] = IF HeartbeatKeepsProposal THEN @ ELSE NoProp]
    /\ heard' = [heard EXCEPT ![l] = @ \cup {f}]
    /\ pre' = [pre EXCEPT ![f] = {}]
    /\ UNCHANGED <<role, vote, granted, elected, log, match, committed, acked, reqs, liveness, used, hist,
                   sleptWith, ttlDue>>

\* An awake leader or candidate reaches a member of a later term and
\* becomes a follower in that term.
StepDown(i, j) ==
    /\ i # j
    /\ i \in up /\ j \in up
    /\ role[i] # "follower"
    /\ awake[i]
    /\ vote[j].term > vote[i].term
    /\ vote' = [vote EXCEPT ![i] = [term |-> vote[j].term, node |-> Nobody, committed |-> FALSE]]
    /\ role' = [role EXCEPT ![i] = "follower"]
    /\ granted' = [granted EXCEPT ![i] = {}]
    /\ prop' = [prop EXCEPT ![i] = NoProp]
    /\ awake' = [awake EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<elected, log, match, commit, committed, acked, hold, reqs, liveness, used, hist,
                   extra>>

\* A write wakes the leader and closes its sleep proposal. With LeaseLapses
\* openraft refuses it until a majority took a message sent since the wake,
\* so the write waits for that heartbeat round, or with WriteBeforeRound is
\* proposed at once and refused.
AppendRecord(l) ==
    /\ Leads(l)
    /\ used.writes < MaxWrites
    /\ IF ~LeaseLapses \/ Fresh(l)
       THEN /\ log' = [log EXCEPT ![l] = Append(@, [term |-> vote[l].term, val |-> used.writes + 1])]
            /\ awake' = [awake EXCEPT ![l] = TRUE]
            /\ prop' = [prop EXCEPT ![l] = NoProp]
            /\ used' = [used EXCEPT !.writes = @ + 1]
            /\ hist' = [hist EXCEPT !.wroteAsleep = @ \/ ~awake[l]]
            /\ UNCHANGED <<role, vote, granted, elected, match, commit, committed, acked, hold, reqs,
                           liveness, extra>>
       ELSE IF WriteBeforeRound
       THEN /\ awake' = [awake EXCEPT ![l] = TRUE]
            /\ prop' = [prop EXCEPT ![l] = NoProp]
            /\ used' = [used EXCEPT !.writes = @ + 1]
            /\ hist' = [hist EXCEPT !.refused = TRUE, !.wroteAsleep = @ \/ ~awake[l]]
            /\ UNCHANGED <<raft, hold, reqs, liveness, extra>>
       ELSE /\ ~awake[l]
            /\ awake' = [awake EXCEPT ![l] = TRUE]
            /\ prop' = [prop EXCEPT ![l] = NoProp]
            /\ hist' = [hist EXCEPT !.wroteAsleep = TRUE]
            /\ UNCHANGED <<raft, hold, reqs, liveness, used, extra>>

\* A cache leader's expiry timer fires while it sleeps.
TtlFires(l) ==
    /\ Leads(l) /\ ~awake[l]
    /\ ~ttlDue
    /\ used.ttls < MaxTtlWakes
    /\ ttlDue' = TRUE
    /\ awake' = IF NoTtlWake THEN awake ELSE [awake EXCEPT ![l] = TRUE]
    /\ used' = [used EXCEPT !.ttls = @ + 1]
    /\ UNCHANGED <<raft, hold, prop, reqs, liveness, hist, heard, pre, sleptWith>>

\* The expiry delete, a write like any other.
ExpiryDelete(l) ==
    /\ Leads(l) /\ awake[l]
    /\ ttlDue
    /\ ~LeaseLapses \/ Fresh(l)
    /\ log' = [log EXCEPT ![l] = Append(@, [term |-> vote[l].term, val |-> 0])]
    /\ ttlDue' = FALSE
    /\ prop' = [prop EXCEPT ![l] = NoProp]
    /\ UNCHANGED <<role, vote, granted, elected, match, commit, committed, acked, awake, hold, reqs,
                   liveness, used, hist, heard, pre, sleptWith>>

----------------------------------------------------------------------------
\* The handshake.

\* The members the leader asks: those whose broker it supports.
Live(l) == {m \in Servers \ {l} : sup[l][m]}

ProposeSleep(l) ==
    /\ Leads(l) /\ awake[l]
    /\ used.sleeps < MaxSleeps
    /\ prop[l] = NoProp
    /\ ~ttlDue
    /\ commit[l] = Len(log[l])
    /\ \E i \in 1..commit[l] : log[l][i].term = vote[l].term
    /\ Quorum(Live(l) \cup {l})
    /\ \A m \in Live(l) : match[l][m] = Len(log[l]) /\ commit[m] = commit[l]
    /\ prop' = [prop EXCEPT ![l] = [asked |-> Live(l), answered |-> {}]]
    /\ reqs' = reqs \cup {[to |-> m, vote |-> vote[l], last |-> Last(l), commit |-> commit[l]] :
                            m \in IF LeftOutAsked THEN Servers \ {l} ELSE Live(l)}
    /\ used' = [used EXCEPT !.sleeps = @ + 1]
    /\ UNCHANGED <<raft, awake, hold, liveness, hist, extra>>

\* A request arrives. The member sleeps if its own position is the one the
\* request names, and answers. The answer counts only for the proposal the
\* request came from, and can be lost.
SleepRequest(m, lost) ==
    LET f == m.to
        l == m.vote.node
        ok == /\ \/ AcceptUnchecked
                 \/ vote[f] = m.vote /\ Last(f) = m.last /\ commit[f] = m.commit
              /\ \/ SleepUnsupported
                 \/ sup[f][l] /\ hold[f] = "none"
        counted == /\ ok /\ ~lost
                   /\ Leads(l)
                   /\ f \in prop[l].asked
                   /\ vote[l] = m.vote /\ Last(l) = m.last /\ commit[l] = m.commit
        wakeLeader == /\ ~ok /\ RefusalWakes
                      /\ Leads(l) /\ vote[l] = m.vote
    IN /\ m \in reqs
       /\ f \in up
       /\ lost => ok /\ used.lost < MaxLost
       /\ reqs' = reqs \ {m}
       /\ awake' = IF ok THEN [awake EXCEPT ![f] = FALSE]
                   ELSE IF wakeLeader THEN [awake EXCEPT ![l] = TRUE] ELSE awake
       /\ hold' = IF ok THEN [hold EXCEPT ![f] = "none"] ELSE hold
       /\ prop' = IF counted THEN [prop EXCEPT ![l].answered = @ \cup {f}]
                  ELSE IF wakeLeader THEN [prop EXCEPT ![l] = NoProp] ELSE prop
       /\ used' = IF lost THEN [used EXCEPT !.lost = @ + 1] ELSE used
       /\ UNCHANGED <<raft, liveness, hist, extra>>

DropRequest(m) ==
    /\ m \in reqs
    /\ used.lost < MaxLost
    /\ reqs' = reqs \ {m}
    /\ used' = [used EXCEPT !.lost = @ + 1]
    /\ UNCHANGED <<raft, awake, hold, prop, liveness, hist, extra>>

\* Followers first, the leader last.
LeaderSleep(l) ==
    /\ Leads(l) /\ awake[l]
    /\ prop[l].asked # {}
    /\ LeaderSleepsFirst \/ prop[l].answered = prop[l].asked
    /\ awake' = [awake EXCEPT ![l] = FALSE]
    /\ prop' = [prop EXCEPT ![l] = NoProp]
    /\ heard' = [heard EXCEPT ![l] = {}]
    /\ sleptWith' = [sleptWith EXCEPT ![l] = prop[l].asked]
    /\ UNCHANGED <<raft, hold, reqs, liveness, used, hist, pre, ttlDue>>

\* Falling asleep with no handshake, in any state.
Doze(s) ==
    /\ s \in up
    /\ awake[s]
    /\ used.dozes < MaxDozes
    /\ awake' = [awake EXCEPT ![s] = FALSE]
    /\ used' = [used EXCEPT !.dozes = @ + 1]
    /\ heard' = IF role[s] = "leader" THEN [heard EXCEPT ![s] = {}] ELSE heard
    /\ UNCHANGED <<raft, hold, prop, reqs, liveness, hist, pre, sleptWith, ttlDue>>

----------------------------------------------------------------------------
\* Waking.

\* A sleeping leader's audit: it wakes, and its heartbeats follow.
LeaderAudit(l) ==
    /\ ~NoAudit
    /\ Leads(l) /\ ~awake[l]
    /\ used.audits < MaxAudits
    /\ awake' = [awake EXCEPT ![l] = TRUE]
    /\ used' = [used EXCEPT !.audits = @ + 1]
    /\ UNCHANGED <<raft, hold, prop, reqs, liveness, hist, extra>>

\* A sleeping follower that heard nothing for two audit periods ticks, or
\* with AuditAsks asks the leader's broker first. Under a live leader this
\* needs a lost audit heartbeat (MaxAuditsLost).
FollowerAudit(f) ==
    LET led == \E l \in Servers \ {f} : Leads(l) /\ vote[l] = vote[f]
    IN /\ ~NoAudit
       /\ f \in up
       /\ ~awake[f]
       /\ role[f] # "leader"
       /\ ~led \/ used.auditsLost < MaxAuditsLost
       /\ awake' = [awake EXCEPT ![f] = TRUE]
       /\ hold' = IF AuditAsks THEN [hold EXCEPT ![f] = "ask"] ELSE hold
       /\ used' = IF led THEN [used EXCEPT !.auditsLost = @ + 1] ELSE used
       /\ UNCHANGED <<raft, prop, reqs, liveness, hist, extra>>

\* A liveness change wakes a sleeping member. A leader heartbeats at once
\* (it is awake, so Heartbeat is enabled). A follower asks first.
LivenessWake(p) ==
    LET woken == ~NoLivenessWake /\ ~awake[p]
    IN /\ awake' = IF woken THEN [awake EXCEPT ![p] = TRUE] ELSE awake
       /\ hold' = IF woken /\ role[p] # "leader" /\ ~CampaignOnWake
                  THEN [hold EXCEPT ![p] = "ask"] ELSE hold

\* The ask reaches the leader's broker and wakes the leader.
AskWakes(f, l) ==
    /\ f \in up
    /\ hold[f] = "ask"
    /\ l # f
    /\ Leads(l) /\ ~awake[l]
    /\ vote[l] = vote[f]
    /\ awake' = [awake EXCEPT ![l] = TRUE]
    /\ UNCHANGED <<raft, hold, prop, reqs, liveness, used, hist, extra>>

\* One election timeout after the ask, or the suspect time plus one after
\* a restart, with nothing heard.
HoldExpires(f) ==
    /\ f \in up
    /\ hold[f] # "none"
    /\ HoldTooShort \/ ~ \E l \in Servers \ {f} : Leads(l) /\ VoteGE(vote[l], vote[f])
    /\ hold' = [hold EXCEPT ![f] = "none"]
    /\ UNCHANGED <<raft, awake, prop, reqs, liveness, used, hist, extra>>

\* The ask is lost. The sleeping leader never hears it, the hold runs out
\* with nothing heard, and ticks start.
AskLost(f) ==
    /\ f \in up
    /\ hold[f] = "ask"
    /\ used.asks < MaxAsksLost
    /\ \E l \in Servers \ {f} : Leads(l) /\ ~awake[l] /\ vote[l] = vote[f]
    /\ hold' = [hold EXCEPT ![f] = "none"]
    /\ used' = [used EXCEPT !.asks = @ + 1]
    /\ UNCHANGED <<raft, awake, prop, reqs, liveness, hist, extra>>

----------------------------------------------------------------------------
\* Store liveness.

Die(s) ==
    /\ Leads(s)
    /\ used.deaths < MaxDeaths
    /\ up' = up \ {s}
    /\ used' = [used EXCEPT !.deaths = @ + 1]
    /\ hist' = [hist EXCEPT !.excuse = TRUE,
                            !.diedAsleep = @ \/ \A t \in up : ~awake[t]]
    /\ UNCHANGED <<raft, awake, hold, prop, reqs, sup, unseen, extra>>

\* A dead broker is silent, and a peer withdraws support after the suspect
\* time.
Withdraw(p, q) ==
    /\ ~LivenessMisses
    /\ p \in up /\ q \notin up
    /\ sup[p][q]
    /\ sup' = [sup EXCEPT ![p][q] = FALSE]
    /\ LivenessWake(p)
    /\ UNCHANGED <<raft, prop, reqs, up, unseen, used, hist, extra>>

\* A false alarm: support withdrawn for a healthy broker.
Alarm(p, q) ==
    /\ p # q
    /\ p \in up /\ q \in up
    /\ sup[p][q]
    /\ used.alarms < MaxAlarms
    /\ AlarmsOnFollowers \/ role[q] = "leader"
    /\ sup' = [sup EXCEPT ![p][q] = FALSE]
    /\ LivenessWake(p)
    /\ used' = [used EXCEPT !.alarms = @ + 1]
    /\ UNCHANGED <<raft, prop, reqs, up, unseen, hist, extra>>

\* The alarm ends. Support coming back wakes nobody.
Regain(p, q) ==
    /\ p \in up /\ q \in up
    /\ ~sup[p][q]
    /\ sup' = [sup EXCEPT ![p][q] = TRUE]
    /\ UNCHANGED <<raft, sleep, up, unseen, used, hist, extra>>

\* A broker restart. Log, vote and commit index are on disk. The member
\* starts awake and held, with a new store epoch.
Restart(s) ==
    /\ s \in up
    /\ used.restarts < MaxRestarts
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ match' = [match EXCEPT ![s] = [t \in Servers |-> 0]]
    /\ awake' = [awake EXCEPT ![s] = TRUE]
    /\ hold' = [hold EXCEPT ![s] = IF RestartNoHold THEN "none" ELSE "restart"]
    /\ prop' = [prop EXCEPT ![s] = NoProp]
    /\ unseen' = unseen \cup {<<p, s>> : p \in Servers \ {s}}
    /\ used' = [used EXCEPT !.restarts = @ + 1]
    /\ hist' = [hist EXCEPT !.excuse = @ \/ role[s] = "leader"]
    /\ heard' = [heard EXCEPT ![s] = {}]
    /\ pre' = [pre EXCEPT ![s] = {}]
    /\ UNCHANGED <<vote, elected, log, commit, committed, acked, reqs, up, sup, sleptWith, ttlDue>>

\* A peer sees the new epoch.
EpochSeen(p, s) ==
    /\ <<p, s>> \in unseen
    /\ p \in up
    /\ unseen' = unseen \ {<<p, s>>}
    /\ LivenessWake(p)
    /\ UNCHANGED <<raft, prop, reqs, up, sup, used, hist, extra>>

----------------------------------------------------------------------------

Next ==
    \/ \E s \in Servers :
        \/ Campaign(s) \/ BecomeLeader(s) \/ AdvanceCommit(s) \/ AppendRecord(s)
        \/ PreCampaign(s) \/ TtlFires(s) \/ ExpiryDelete(s)
        \/ ProposeSleep(s) \/ LeaderSleep(s) \/ Doze(s)
        \/ LeaderAudit(s) \/ FollowerAudit(s) \/ HoldExpires(s) \/ AskLost(s)
        \/ Die(s) \/ Restart(s)
    \/ \E i, j \in Servers :
        \/ GrantVote(i, j) \/ Heartbeat(i, j) \/ Replicate(i, j)
        \/ LearnCommit(i, j) \/ StepDown(i, j) \/ AskWakes(i, j)
        \/ Withdraw(i, j) \/ Alarm(i, j) \/ Regain(i, j) \/ EpochSeen(i, j)
        \/ PreVoteReq(i, j) \/ VoteWakesHeld(i, j)
    \/ \E m \in reqs :
        \/ DropRequest(m)
        \/ \E lost \in BOOLEAN : SleepRequest(m, lost)

Spec == Init /\ [][Next]_vars

FairSpec == Spec /\ WF_vars(Next)

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Servers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ vote[s].term \in 1..MaxTerm
        /\ hold[s] \in {"none", "ask", "restart"}
        /\ commit[s] <= Len(log[s])
        /\ prop[s].answered \subseteq prop[s].asked
    /\ up \subseteq Servers
    /\ acked \subseteq 1..MaxWrites

OneLeaderPerTerm == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

LogMatching ==
    \A s, t \in Servers : \A i \in 1..Min(Len(log[s]), Len(log[t])) :
        log[s][i].term = log[t][i].term => \A k \in 1..i : log[s][k] = log[t][k]

LeaderCompleteness ==
    \A l \in Servers : role[l] = "leader" =>
        \A i \in 1..Len(committed) : committed[i].by < vote[l].term =>
            Len(log[l]) >= i /\ log[l][i] = committed[i].e

StateMachineSafety ==
    \A s \in Servers :
        /\ commit[s] <= Len(committed)
        /\ \A i \in 1..commit[s] : log[s][i] = committed[i].e

AckedSurvive ==
    \A v \in acked : \E Q \in SUBSET Servers :
        Quorum(Q) /\ \A s \in Q : \E i \in 1..Len(log[s]) : log[s][i].val = v

\* The simulator's checker of the same name.
AsleepIsLevel ==
    (up # {} /\ \A s \in up : ~awake[s]) =>
        \A s, t \in up : Last(s) = Last(t) /\ commit[s] = commit[t]

\* The same, for the members the leader counted when it slept. A member it
\* left out is caught up later, which QuiescentLevel checks.
AsleepIsLevelCounted ==
    (up # {} /\ \A s \in up : ~awake[s]) =>
        \A l \in up : Leads(l) =>
            \A s \in sleptWith[l] \cap up : Last(s) = Last(l) /\ commit[s] = commit[l]

\* A write to a healthy leader is never refused.
NoWriteRefused == ~hist.refused

\* The first leader is deposed by an election another member wins, or by
\* stepping down for a campaign that never wins.
NoNeedlessDeposal ==
    ~hist.excuse => \A e \in elected : e[1] = 1 /\ role[e[2]] = "leader"

TopLeader(l) == Leads(l) /\ \A s \in up : vote[s].term <= vote[l].term

\* A member whose election timeout has expired and that only the model's
\* term bound keeps from campaigning. A state that holds one is cut short by
\* the bound and says nothing about the design.
OutOfTerms ==
    \E s \in up :
        /\ awake[s] /\ hold[s] = "none" /\ role[s] # "leader"
        /\ ~HearsLeader(s)
        /\ vote[s].term = MaxTerm

Settled == ~ENABLED Next /\ Quorum(up) /\ ~OutOfTerms

\* Liveness, on states where nothing more can happen.
QuiescentLed == Settled => \E l \in Servers : TopLeader(l)

QuiescentLevel ==
    Settled => \A l \in Servers : TopLeader(l) =>
        /\ used.writes = MaxWrites
        /\ commit[l] = Len(log[l])
        /\ \A s \in up : log[s] = log[l] /\ commit[s] = commit[l]

\* A due expiry delete is proposed.
QuiescentTtl == Settled => ~ttlDue

\* Every behaviour ends, which is what lets the two checks above stand for
\* "eventually". Checked with FairSpec.
Ends == <>(~ENABLED Next)

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens and must be violated.

\* A write reaches a sleeping leader, is acknowledged, and the group sleeps
\* again.
NeverWakesOnWrite ==
    ~ (hist.wroteAsleep /\ acked # {} /\ \A s \in up : ~awake[s])

\* A leader's broker dies with every member asleep and a later term gets a
\* leader. With LivenessMisses on, only the audit can have woken them.
NeverElectsAfterDyingAsleep ==
    ~ (hist.diedAsleep /\ \E e \in elected : e[1] > 1)

\* A leader is elected while a sleep proposal or request is still out.
NeverElectedMidHandshake == ~hist.midHandshake

=============================================================================
