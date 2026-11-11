--------------------------- MODULE FelixRaftLease ---------------------------
(***************************************************************************)
(* The Raft leader lease for one shard group, as docs/per-shard-raft.md    *)
(* ("Acknowledging a write") and docs/replication-engine.md (Decisions, 5) *)
(* decide it. These configs gate the pull request that turns on            *)
(* `admit_local_ack`.                                                      *)
(*                                                                         *)
(* Part 1 is the first version's lease. Part 2 (`StoreLeases = TRUE`, the  *)
(* `FelixRaftLeaseStore*` configs) is the later design, "Leases that       *)
(* survive sleep". It is NOT in the first version. Its variables do not    *)
(* move when `StoreLeases` is off.                                         *)
(*                                                                         *)
(* Time. Real time moves in ticks. In one tick each broker's clock gains   *)
(* between `RateLo` and `RateHi` units, chosen again every tick. A broker  *)
(* only ever reads its own clock, and only to ask how long ago something   *)
(* happened, so the model keeps ages instead of clock readings: every age  *)
(* a broker holds grows by that broker's gain at a tick, and stops at      *)
(* `Old`, past which no rule tells ages apart. That leaves the state space *)
(* finite with no bound on how long a run lasts. The config check is       *)
(*                                                                         *)
(*     Lease * DriftHi <= ElectionTimeout * DriftLo                        *)
(*                                                                         *)
(* which is `lease * (1 + p) / (1 - p) <= election_timeout_max` with the   *)
(* rates written as integers.                                              *)
(*                                                                         *)
(* Part 1, the lease.                                                      *)
(*                                                                         *)
(*   A leader's append or heartbeat is in flight: it is stamped when it is *)
(*   handed to the network, the follower takes it at some later time and   *)
(*   answers, and the answer arrives later still. `ackAge` keeps, per      *)
(*   follower, how long ago the newest message that follower answered was  *)
(*   handed off. The lease runs from the newest hand-off a majority has    *)
(*   answered (`last_quorum_acked`).                                       *)
(*                                                                         *)
(*   A follower refuses to vote for `ElectionTimeout` on its own clock     *)
(*   from the moment it took a leader's message, and does not campaign in  *)
(*   that time either. A leader refuses while a majority has answered it   *)
(*   within the election timeout, as openraft 0.10 does.                   *)
(*                                                                         *)
(*   The window is kept in memory. A restarted broker is held from         *)
(*   campaigning ("Restarted members"; the model holds it for one election *)
(*   timeout, the shortest the design allows) and, by a rule this model    *)
(*   adds, refuses votes for one election timeout from its start.          *)
(*                                                                         *)
(*   A write is acknowledged on local durability when                      *)
(*   `now < last_quorum_acked + Lease`. Otherwise it waits for commit,     *)
(*   which is also what a `Quorum` write does, so the model has no         *)
(*   separate `Quorum` level. A failed check changes no role. The          *)
(*   subscriber is given records up to the commit index only.              *)
(*                                                                         *)
(* Part 2, store-liveness leases.                                          *)
(*                                                                         *)
(*   The group sends nothing of its own (every member is asleep). Each     *)
(*   pair of brokers exchanges liveness heartbeats instead. A broker gives *)
(*   a peer's store support for (epoch, expiry on its own clock), keeps    *)
(*   that on disk, and withdraws only after the expiry. Withdrawing raises *)
(*   the epoch it will accept, so withdrawn support never comes back at    *)
(*   the same epoch; the peer learns of it and raises its own epoch. A     *)
(*   store's epoch also rises at every start. A voter that elects a leader *)
(*   whose store it supports records that epoch on disk (`fortEpoch`), and *)
(*   refuses to vote for anyone else until it has withdrawn. The leader's  *)
(*   lease holds while a majority has recorded its current epoch and has   *)
(*   answered a liveness heartbeat handed off less than `Lease` ago. A     *)
(*   broker whose disk has stalled sends no liveness heartbeats and grants *)
(*   none.                                                                 *)
(*                                                                         *)
(* Abstracted:                                                             *)
(*                                                                         *)
(*   Storage. Log, term and vote survive a restart whole. FelixRaft.tla    *)
(*   has the adapter's fsync rules.                                        *)
(*   Elections. One atomic step over a majority. Vote messages in flight   *)
(*   could only make the election later.                                   *)
(*   Appends. A message carries the leader's whole log.                    *)
(*   The heartbeat batch window and the tick interval. Both only make a    *)
(*   step late: the window sits between the hand-off (where the stamp is   *)
(*   taken) and the send, so it is part of the message delay, which has    *)
(*   no upper bound here; a timeout noticed at the next tick is a vote or  *)
(*   campaign taken later than allowed. Neither can break the checks. The  *)
(*   lease check reads the clock when it runs; a check that used a clock   *)
(*   value cached at the last tick would add the tick to the lease.        *)
(*   Blank entries, membership, snapshots, pre-vote, leadership transfer.  *)
(*   One in-flight message per pair and direction; a newer answer replaces *)
(*   an older one still in flight. Any message can be lost. A message that *)
(*   could only be discarded is dropped at once.                           *)
(*   Part 2: a liveness heartbeat is taken the moment it is handed off,    *)
(*   the earliest its support can start. Only its answer is in flight.     *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   LeaseExclusive          -- no two brokers pass the lease check at one *)
(*                              real time.                                 *)
(*   NoAckBehindNewLeader    -- no write is acknowledged on local          *)
(*                              durability once a leader of a higher term  *)
(*                              has been elected. This is the edge of the  *)
(*                              `Leader` loss window: a record             *)
(*                              acknowledged this way can be lost only to  *)
(*                              a failover that starts after the           *)
(*                              acknowledgement.                           *)
(*   AckedSurvive            -- a write acknowledged through commit is on  *)
(*                              a majority of logs.                        *)
(*   StateMachineSafety      -- committed prefixes agree.                  *)
(*   DeliveredNeverReplaced  -- what the subscriber was given is what      *)
(*                              every broker has committed at that offset. *)
(*   OneLeaderPerTerm        -- no term elects two brokers.                *)
(*   StalledLeaseLapses      -- (Part 2) a broker whose disk has been      *)
(*                              stalled for a lease does not pass the      *)
(*                              check, so it cannot block a failover.      *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   LeaseTooLong           -- the config accepts a lease that breaks the  *)
(*                             inequality.                                 *)
(*   DriftBeyondBound       -- clocks drift further than the inequality    *)
(*                             was given.                                  *)
(*   LeaseFromAck           -- the lease runs from when the answer came    *)
(*                             back instead of the hand-off time.          *)
(*   VoteInWindow           -- a follower grants a vote inside its refusal *)
(*                             window.                                     *)
(*   RestartForgetsWindow   -- a restarted follower may vote at once. This *)
(*                             is openraft 0.10.0 (`storage/helper.rs`     *)
(*                             builds the vote with a zero lease). With it *)
(*                             off, a restarted broker refuses votes for   *)
(*                             one election timeout from its start. It is  *)
(*                             held from campaigning either way.           *)
(*   DeliverUncommitted     -- the subscriber reads the leader's whole     *)
(*                             log.                                        *)
(*   SupportNotDurable      -- (Part 2) support and the recorded epoch are *)
(*                             lost at a restart.                          *)
(*   VoteWhileSupported     -- (Part 2) a vote is granted while support    *)
(*                             for the leader's store stands.              *)
(*   LeaseKeptAcrossRestart -- (Part 2) a restarted leader goes on leading *)
(*                             and its lease check does not compare        *)
(*                             epochs.                                     *)
(*   ResupportWithdrawn     -- (Part 2) withdrawing does not raise the     *)
(*                             epoch, so the same epoch is supported again.*)
(*   WithdrawEarly          -- (Part 2) support is withdrawn before its    *)
(*                             expiry.                                     *)
(*   StalledKeepsAsking     -- (Part 2) a broker with a stalled disk keeps *)
(*                             sending liveness heartbeats.                *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Servers, Nobody,
    MaxTerm, MaxWrites, MaxRestarts,
    RateLo, RateHi,          \* what clocks really gain per tick
    DriftLo, DriftHi,        \* what the config check was told
    Lease, ElectionTimeout,
    Consumer,
    LeaseTooLong, DriftBeyondBound, LeaseFromAck, VoteInWindow,
    RestartForgetsWindow, DeliverUncommitted,
    StoreLeases, SupportTime, MaxEpoch, MaxBeats, MaxStalls,
    SupportNotDurable, VoteWhileSupported, LeaseKeptAcrossRestart,
    ResupportWithdrawn, WithdrawEarly, StalledKeepsAsking

\* The follower's side of the inequality: how long its promise lasts.
Promise == IF StoreLeases THEN SupportTime ELSE ElectionTimeout

\* "The config refuses a lease that breaks the inequality."
ASSUME LeaseTooLong \/ Lease * DriftHi <= Promise * DriftLo
ASSUME DriftBeyondBound \/ (DriftLo <= RateLo /\ RateHi <= DriftHi)
ASSUME 1 <= RateLo /\ RateLo <= RateHi

VARIABLES
    term, votedFor, role, log, commit,
    heardAge,     \* how long ago a leader's message was last taken
    startAge,     \* how long ago the broker started
    ackAge,       \* leader: per follower, age of the newest hand-off answered
    match,        \* leader: per follower, log length answered
    msgs,
    restarts, nextId,
    delivered,    \* what the subscriber holds
    acked,        \* history: per write, "none", "local" or "commit"
    staleAck,     \* history: a local acknowledgement behind a newer leader
    elected,      \* history: who was elected in each term
    \* Part 2
    epoch,        \* store epoch, on disk
    sup,          \* sup[f][l]: f's support for l's store, on disk
    fortEpoch,    \* the epoch of its leader's store a voter recorded, on disk
    supAge,       \* supAge[l][f]: age of the newest liveness hand-off f answered
    fortAck,      \* fortAck[l][f]: the epoch f recorded when it elected l
    beatMsgs,
    beats,        \* liveness heartbeats sent so far, to bound the run
    stalledAge    \* how long a broker's disk has been stalled, or None

groupVars == <<term, votedFor, role, log, commit, heardAge, startAge, ackAge,
              match>>
storeVars == <<epoch, sup, fortEpoch, supAge, fortAck, beatMsgs, beats, stalledAge>>
histVars  == <<delivered, acked, staleAck, elected, nextId>>
vars == <<groupVars, msgs, restarts, histVars, storeVars>>

None == -1
Symm == Permutations(Servers)

\* Past this age nothing is told apart. It also stands for "never".
Old == IF Lease > Promise THEN Lease ELSE Promise
Grow(a, d) == IF a + d >= Old THEN Old ELSE a + d
\* A liveness answer is only ever compared with the lease.
GrowS(a, d) == IF a + d >= Lease THEN Old ELSE a + d

Majority(S) == Cardinality(S) * 2 > Cardinality(Servers)
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Others(b) == Servers \ {b}

NoAges  == [f \in Servers |-> Old]
NoMatch == [f \in Servers |-> 0]
\* `left` is the time to the expiry on the supporter's clock. 0 is expired
\* but not withdrawn. None is withdrawn, or never given.
NoSup   == [epoch |-> 0, left |-> None]

\* A message that could only be discarded is dropped at once: an append
\* below its target's term, an answer to a broker that no longer leads in
\* that term, a liveness answer for an epoch its target has left.
LiveMsgs(ms, t, r) ==
    {m \in ms : IF m.kind = "app" THEN m.term >= t[m.to]
                ELSE r[m.to] = "leader" /\ m.term = t[m.to]}
LiveBeats(bs, e) ==
    {m \in bs : \/ m.ok /\ m.epoch = e[m.to]
                \/ ~m.ok /\ m.epoch > e[m.to]}

IsPrefix(s, t) == Len(s) <= Len(t) /\ \A i \in 1..Len(s) : s[i] = t[i]
LastTerm(b) == IF log[b] = <<>> THEN 0 ELSE log[b][Len(log[b])][1]
LogOk(c, v) ==
    \/ LastTerm(c) > LastTerm(v)
    \/ LastTerm(c) = LastTerm(v) /\ Len(log[c]) >= Len(log[v])

\* Part 1 lease: the age of the newest hand-off a majority has answered.
QAge(l) ==
    LET good == {a \in {ackAge[l][f] : f \in Others(l)} :
                    Majority({l} \cup {f \in Others(l) : ackAge[l][f] <= a})}
    IN IF good = {} THEN Old ELSE Min(good)

GroupLease(l) == role[l] = "leader" /\ QAge(l) < Lease

\* Part 2 lease: a majority recorded this epoch and still answers.
StoreLease(l) ==
    /\ role[l] = "leader"
    /\ Majority({l} \cup {f \in Others(l) :
          /\ IF LeaseKeptAcrossRestart THEN fortAck[l][f] # 0
                                       ELSE fortAck[l][f] = epoch[l]
          /\ supAge[l][f] < Lease})

LeaseValid(l) == IF StoreLeases THEN StoreLease(l) ELSE GroupLease(l)

InWindow(b) == heardAge[b] < ElectionTimeout

\* The window is kept in memory. A broker that has just started may have
\* answered a leader before it stopped, so it waits out one election timeout.
JustStarted(b) == startAge[b] < ElectionTimeout

\* A leader's own refusal, as openraft keeps it.
LeaderHolds(b) ==
    IF StoreLeases THEN StoreLease(b)
    ELSE role[b] = "leader" /\ QAge(b) < ElectionTimeout

\* A voter's promise stands until it has withdrawn the support it recorded.
SupportStands(v) ==
    LET l == votedFor[v] IN
    /\ l \notin {Nobody, v}
    /\ fortEpoch[v] # 0
    /\ sup[v][l].epoch = fortEpoch[v]
    /\ sup[v][l].left # None

MayGrant(v, c) ==
    /\ ~LeaderHolds(v)
    /\ IF StoreLeases
       THEN VoteWhileSupported \/ ~SupportStands(v) \/ votedFor[v] = c
       ELSE /\ VoteInWindow \/ ~InWindow(v)
            /\ RestartForgetsWindow \/ ~JustStarted(v)

\* The epoch a voter records for the leader it elects: the one it supports
\* now, or none.
Fort(v, c) == IF sup[v][c].left > 0 THEN sup[v][c].epoch ELSE 0

Init ==
    /\ term = [b \in Servers |-> 0]
    /\ votedFor = [b \in Servers |-> Nobody]
    /\ role = [b \in Servers |-> "follower"]
    /\ log = [b \in Servers |-> <<>>]
    /\ commit = [b \in Servers |-> 0]
    /\ heardAge = NoAges
    /\ startAge = NoAges
    /\ ackAge = [b \in Servers |-> NoAges]
    /\ match = [b \in Servers |-> NoMatch]
    /\ msgs = {}
    /\ restarts = 0
    /\ nextId = 1
    /\ delivered = <<>>
    /\ acked = [i \in 1..MaxWrites |-> "none"]
    /\ staleAck = FALSE
    /\ elected = [t \in 1..MaxTerm |-> {}]
    /\ epoch = [b \in Servers |-> 1]
    /\ sup = [b \in Servers |-> [l \in Servers |-> NoSup]]
    /\ fortEpoch = [b \in Servers |-> 0]
    /\ supAge = [b \in Servers |-> NoAges]
    /\ fortAck = [b \in Servers |-> NoMatch]
    /\ beatMsgs = {}
    /\ beats = 0
    /\ stalledAge = [b \in Servers |-> None]

\* One tick of real time. Every age grows by the gain of the clock it is
\* read on: a message's stamp is read on the leader's clock.
Tick ==
    \E d \in [Servers -> RateLo..RateHi] :
        /\ heardAge' = [b \in Servers |-> Grow(heardAge[b], d[b])]
        /\ startAge' = [b \in Servers |-> Grow(startAge[b], d[b])]
        /\ ackAge' = [l \in Servers |-> [f \in Servers |-> Grow(ackAge[l][f], d[l])]]
        /\ supAge' = [l \in Servers |-> [f \in Servers |-> GrowS(supAge[l][f], d[l])]]
        /\ msgs' = {[m EXCEPT !.age = Grow(@, d[IF m.kind = "app" THEN m.from ELSE m.to])]
                      : m \in msgs}
        /\ beatMsgs' = {[m EXCEPT !.age = GrowS(@, d[m.to])] : m \in beatMsgs}
        /\ sup' = [f \in Servers |-> [l \in Servers |->
                     [sup[f][l] EXCEPT !.left =
                         IF @ = None THEN None
                         ELSE IF @ <= d[f] THEN 0 ELSE @ - d[f]]]]
        /\ stalledAge' = [b \in Servers |->
                            IF stalledAge[b] = None THEN None
                            ELSE GrowS(stalledAge[b], d[b])]
        /\ UNCHANGED <<term, votedFor, role, log, commit, match, restarts,
                       histVars, epoch, fortEpoch, fortAck, beats>>

Elect(c, Q, nt) ==
    /\ c \in Q /\ Majority(Q)
    /\ role[c] # "leader"
    /\ \A v \in Q : term[v] < nt
    \* A restarted member is held from campaigning ("Restarted members").
    /\ IF StoreLeases THEN ~SupportStands(c)
       ELSE ~InWindow(c) /\ ~JustStarted(c)
    /\ \A v \in Q \ {c} : LogOk(c, v) /\ MayGrant(v, c)
    /\ term' = [b \in Servers |-> IF b \in Q THEN nt ELSE term[b]]
    /\ votedFor' = [b \in Servers |-> IF b \in Q THEN c ELSE votedFor[b]]
    /\ role' = [b \in Servers |->
                  IF b = c THEN "leader"
                  ELSE IF b \in Q THEN "follower" ELSE role[b]]
    \* A vote for a candidate is not a leader heard: it starts no window.
    /\ heardAge' = [b \in Servers |-> IF b \in Q THEN Old ELSE heardAge[b]]
    /\ ackAge' = [b \in Servers |-> IF b \in Q THEN NoAges ELSE ackAge[b]]
    /\ match' = [b \in Servers |-> IF b \in Q THEN NoMatch ELSE match[b]]
    /\ elected' = [elected EXCEPT ![nt] = @ \cup {c}]
    /\ IF StoreLeases
       THEN /\ fortEpoch' = [b \in Servers |->
                               IF b = c THEN 0
                               ELSE IF b \in Q THEN Fort(b, c) ELSE fortEpoch[b]]
            /\ fortAck' = [b \in Servers |->
                              IF b = c
                              THEN [v \in Servers |->
                                      IF v \in Q \ {c} THEN Fort(v, c) ELSE 0]
                              ELSE IF b \in Q THEN NoMatch ELSE fortAck[b]]
       ELSE UNCHANGED <<fortEpoch, fortAck>>
    /\ msgs' = LiveMsgs(msgs, term', role')
    /\ UNCHANGED <<log, commit, startAge, restarts, delivered, acked, staleAck, nextId,
                   epoch, sup, supAge, beatMsgs, beats, stalledAge>>

\* Hand an append or heartbeat to the network. The stamp is taken here.
Send(l, f) ==
    /\ ~StoreLeases
    /\ role[l] = "leader" /\ f # l
    /\ ~\E m \in msgs : m.kind = "app" /\ m.from = l /\ m.to = f
    /\ msgs' = msgs \cup {[kind |-> "app", from |-> l, to |-> f,
                           term |-> term[l], age |-> 0,
                           log |-> log[l], n |-> commit[l]]}
    /\ UNCHANGED <<groupVars, restarts, histVars, storeVars>>

Drop(m) ==
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<groupVars, restarts, histVars, storeVars>>

RecvApp(m) ==
    LET f == m.to
        stale == {a \in msgs : a.kind = "ack" /\ a.from = f /\ a.to = m.from}
    IN
    /\ m.kind = "app" /\ m.term >= term[f]
    /\ term' = [term EXCEPT ![f] = m.term]
    /\ role' = [role EXCEPT ![f] = "follower"]
    /\ ackAge' = [ackAge EXCEPT ![f] = NoAges]
    /\ match' = [match EXCEPT ![f] = NoMatch]
    /\ heardAge' = [heardAge EXCEPT ![f] = 0]
    /\ log' = [log EXCEPT ![f] = IF IsPrefix(m.log, @) THEN @ ELSE m.log]
    /\ commit' = [commit EXCEPT ![f] = Max({@, m.n})]
    /\ msgs' = LiveMsgs(((msgs \ {m}) \ stale)
                           \cup {[kind |-> "ack", from |-> f, to |-> m.from,
                                  term |-> m.term, age |-> m.age,
                                  log |-> <<>>, n |-> Len(m.log)]},
                        term', role')
    /\ UNCHANGED <<votedFor, startAge, restarts, histVars, storeVars>>

RecvAck(m) ==
    LET l == m.to
        f == m.from
        age == IF LeaseFromAck THEN 0 ELSE m.age
        newMatch == [match[l] EXCEPT ![f] = Max({@, m.n})]
        canCommit == {i \in 1..Len(log[l]) :
                        /\ log[l][i][1] = term[l]
                        /\ Majority({l} \cup {g \in Others(l) : newMatch[g] >= i})}
        newCommit == Max({commit[l]} \cup canCommit)
    IN
    /\ m.kind = "ack" /\ role[l] = "leader" /\ m.term = term[l]
    /\ ackAge' = [ackAge EXCEPT ![l][f] = Min({@, age})]
    /\ match' = [match EXCEPT ![l] = newMatch]
    /\ commit' = [commit EXCEPT ![l] = newCommit]
    \* A write that waited is acknowledged when its own term commits it.
    /\ acked' = [i \in 1..MaxWrites |->
                   IF acked[i] = "none"
                      /\ \E k \in 1..newCommit : log[l][k] = <<term[l], i>>
                   THEN "commit" ELSE acked[i]]
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<term, votedFor, role, log, heardAge, startAge, restarts,
                   delivered, staleAck, elected, nextId, storeVars>>

ClientWrite(l) ==
    /\ role[l] = "leader" /\ nextId <= MaxWrites
    /\ stalledAge[l] = None
    /\ log' = [log EXCEPT ![l] = Append(@, <<term[l], nextId>>)]
    /\ acked' = [acked EXCEPT ![nextId] =
                    IF LeaseValid(l) THEN "local" ELSE "none"]
    /\ staleAck' = (staleAck \/
                     (LeaseValid(l) /\ \E t \in 1..MaxTerm :
                          t > term[l] /\ elected[t] # {}))
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<term, votedFor, role, commit, heardAge, startAge, ackAge,
                   match, msgs, restarts, delivered, elected, storeVars>>

Deliver(b) ==
    LET i == Len(delivered) + 1 IN
    /\ Consumer /\ role[b] = "leader"
    /\ i <= (IF DeliverUncommitted THEN Len(log[b]) ELSE commit[b])
    /\ delivered' = Append(delivered, log[b][i])
    /\ UNCHANGED <<groupVars, msgs, restarts, acked, staleAck, elected,
                   nextId, storeVars>>

Restart(b) ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ ackAge' = [ackAge EXCEPT ![b] = NoAges]
    /\ match' = [match EXCEPT ![b] = NoMatch]
    /\ IF StoreLeases
       THEN /\ epoch[b] < MaxEpoch
            /\ epoch' = [epoch EXCEPT ![b] = @ + 1]
            /\ role' = IF LeaseKeptAcrossRestart THEN role
                       ELSE [role EXCEPT ![b] = "follower"]
            /\ supAge' = [supAge EXCEPT ![b] = NoAges]
            /\ fortAck' = IF LeaseKeptAcrossRestart THEN fortAck
                          ELSE [fortAck EXCEPT ![b] = NoMatch]
            /\ sup' = IF SupportNotDurable
                      THEN [sup EXCEPT ![b] = [l \in Servers |-> NoSup]]
                      ELSE sup
            /\ fortEpoch' = IF SupportNotDurable
                            THEN [fortEpoch EXCEPT ![b] = 0] ELSE fortEpoch
            /\ beatMsgs' = LiveBeats(beatMsgs, epoch')
            /\ UNCHANGED <<heardAge, startAge>>
       ELSE /\ role' = [role EXCEPT ![b] = "follower"]
            /\ heardAge' = [heardAge EXCEPT ![b] = Old]
            /\ startAge' = [startAge EXCEPT ![b] = 0]
            /\ UNCHANGED <<epoch, supAge, fortAck, sup, fortEpoch, beatMsgs>>
    /\ msgs' = LiveMsgs(msgs, term, role')
    /\ UNCHANGED <<term, votedFor, log, commit, histVars, beats, stalledAge>>

(***************************************************************************)
(* Part 2: liveness heartbeats between stores.                             *)
(***************************************************************************)

\* One liveness heartbeat. It is taken the moment it is handed off, which is
\* the earliest its support can start; only the answer is in flight.
\* Granting support is a write to disk, so a stalled broker grants none.
Beat(a, f) ==
    LET s == sup[f][a]
        grant == epoch[a] >= s.epoch
        stale == {m \in beatMsgs : m.from = f /\ m.to = a}
    IN
    /\ StoreLeases /\ a # f /\ beats < MaxBeats
    /\ stalledAge[a] = None \/ StalledKeepsAsking
    /\ stalledAge[f] = None
    /\ sup' = IF grant
              THEN [sup EXCEPT ![f][a] =
                      [epoch |-> epoch[a], left |-> SupportTime]]
              ELSE sup
    /\ beatMsgs' = (beatMsgs \ stale)
                     \cup {[from |-> f, to |-> a,
                            epoch |-> IF grant THEN epoch[a] ELSE s.epoch,
                            age |-> 0, ok |-> grant]}
    /\ beats' = beats + 1
    /\ UNCHANGED <<groupVars, msgs, restarts, histVars, epoch, fortEpoch,
                   supAge, fortAck, stalledAge>>

RecvReply(m) ==
    LET l == m.to IN
    /\ IF m.ok /\ m.epoch = epoch[l]
       THEN /\ supAge' = [supAge EXCEPT ![l][m.from] = Min({@, m.age})]
            /\ UNCHANGED epoch
       ELSE IF ~m.ok /\ m.epoch > epoch[l]
       \* The peer withdrew. Support at the old epoch is gone for good.
       THEN /\ epoch' = [epoch EXCEPT ![l] = m.epoch]
            /\ supAge' = [supAge EXCEPT ![l] = NoAges]
       ELSE UNCHANGED <<epoch, supAge>>
    /\ beatMsgs' = LiveBeats(beatMsgs \ {m}, epoch')
    /\ UNCHANGED <<groupVars, msgs, restarts, histVars, sup, fortEpoch,
                   fortAck, beats, stalledAge>>

DropBeat(m) ==
    /\ beatMsgs' = beatMsgs \ {m}
    /\ UNCHANGED <<groupVars, msgs, restarts, histVars, epoch, sup, fortEpoch,
                   supAge, fortAck, beats, stalledAge>>

Withdraw(f, l) ==
    /\ StoreLeases /\ f # l
    /\ sup[f][l].left # None
    /\ WithdrawEarly \/ sup[f][l].left = 0
    /\ ResupportWithdrawn \/ sup[f][l].epoch < MaxEpoch
    /\ sup' = [sup EXCEPT ![f][l] =
                 [epoch |-> IF ResupportWithdrawn THEN @.epoch ELSE @.epoch + 1,
                  left |-> None]]
    /\ UNCHANGED <<groupVars, msgs, restarts, histVars, epoch, fortEpoch,
                   supAge, fortAck, beatMsgs, beats, stalledAge>>

\* The disk probe stops finishing. It does not recover in this model.
Stall(b) ==
    /\ StoreLeases
    /\ Cardinality({x \in Servers : stalledAge[x] # None}) < MaxStalls
    /\ stalledAge[b] = None
    /\ stalledAge' = [stalledAge EXCEPT ![b] = 0]
    /\ UNCHANGED <<groupVars, msgs, restarts, histVars, epoch, sup, fortEpoch,
                   supAge, fortAck, beatMsgs, beats>>

Next ==
    \/ Tick
    \/ \E c \in Servers, Q \in SUBSET Servers, nt \in 1..MaxTerm : Elect(c, Q, nt)
    \/ \E l, f \in Servers : Send(l, f) \/ Beat(l, f) \/ Withdraw(l, f)
    \/ \E m \in msgs : Drop(m) \/ RecvApp(m) \/ RecvAck(m)
    \/ \E m \in beatMsgs : DropBeat(m) \/ RecvReply(m)
    \/ \E b \in Servers : ClientWrite(b) \/ Deliver(b) \/ Restart(b) \/ Stall(b)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Checks.                                                                 *)
(***************************************************************************)

Ages == 0..Old
Entry == (0..MaxTerm) \X (1..MaxWrites)

TypeOK ==
    /\ term \in [Servers -> 0..MaxTerm]
    /\ votedFor \in [Servers -> Servers \cup {Nobody}]
    /\ role \in [Servers -> {"follower", "leader"}]
    /\ \A b \in Servers : \A i \in 1..Len(log[b]) : log[b][i] \in Entry
    /\ \A b \in Servers : commit[b] \in 0..Len(log[b])
    /\ heardAge \in [Servers -> Ages]
    /\ startAge \in [Servers -> Ages]
    /\ ackAge \in [Servers -> [Servers -> Ages]]
    /\ match \in [Servers -> [Servers -> 0..MaxWrites]]
    /\ \A m \in msgs : m.kind \in {"app", "ack"} /\ m.age \in Ages
    /\ restarts \in 0..MaxRestarts
    /\ nextId \in 1..(MaxWrites + 1)
    /\ acked \in [1..MaxWrites -> {"none", "local", "commit"}]
    /\ staleAck \in BOOLEAN
    /\ epoch \in [Servers -> 1..MaxEpoch]
    /\ \A f, l \in Servers : /\ sup[f][l].epoch \in 0..MaxEpoch
                             /\ sup[f][l].left \in None..SupportTime
    /\ fortEpoch \in [Servers -> 0..MaxEpoch]
    /\ supAge \in [Servers -> [Servers -> Ages]]
    /\ fortAck \in [Servers -> [Servers -> 0..MaxEpoch]]
    /\ \A m \in beatMsgs : m.age \in Ages
    /\ beats \in 0..MaxBeats
    /\ stalledAge \in [Servers -> None..Old]

LeaseExclusive ==
    \A a, b \in Servers : a # b => ~(LeaseValid(a) /\ LeaseValid(b))

NoAckBehindNewLeader == ~staleAck

OneLeaderPerTerm ==
    /\ \A t \in 1..MaxTerm : Cardinality(elected[t]) <= 1
    /\ \A a, b \in Servers :
          role[a] = "leader" /\ role[b] = "leader" /\ term[a] = term[b] => a = b

Holds(b, i) == \E k \in 1..Len(log[b]) : log[b][k][2] = i

AckedSurvive ==
    \A i \in 1..MaxWrites :
        acked[i] = "commit" => Majority({b \in Servers : Holds(b, i)})

StateMachineSafety ==
    \A a, b \in Servers : \A i \in 1..commit[a] :
        i <= commit[b] => log[a][i] = log[b][i]

DeliveredNeverReplaced ==
    \A b \in Servers : \A i \in 1..Len(delivered) :
        i <= commit[b] => log[b][i] = delivered[i]

\* A broker that cannot write to its disk must not hold a lease for longer
\* than the lease itself, or it blocks the failover.
StalledLeaseLapses ==
    \A b \in Servers :
        stalledAge[b] = Old => ~LeaseValid(b)

(***************************************************************************)
(* Witnesses. Each says something never happens; TLC must find that it     *)
(* does.                                                                   *)
(***************************************************************************)

\* A write is acknowledged on local durability under the lease.
NeverLocalAck == \A i \in 1..MaxWrites : acked[i] # "local"

\* A lease is held, lapses so a write waits for commit, and that commit
\* renews it: three writes in one term, local, commit, local.
NeverLapsedThenRenewed ==
    ~\E b \in Servers, i, j, k \in 1..MaxWrites :
        /\ i < j /\ j < k
        /\ acked[i] = "local" /\ acked[j] = "commit" /\ acked[k] = "local"
        /\ \A w \in {i, j, k} :
              \E p \in 1..Len(log[b]) : log[b][p] = <<term[b], w>>

\* A leader that acknowledged locally is followed by a leader of a higher
\* term that passes the lease check.
NeverFailoverAfterLease ==
    ~\E i \in 1..MaxWrites, a, b \in Servers :
        /\ acked[i] = "local"
        /\ \E p \in 1..Len(log[a]) : log[a][p][2] = i /\ log[a][p][1] < term[b]
        /\ LeaseValid(b)

=============================================================================
