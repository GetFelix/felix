---------------------------- MODULE FelixShard ----------------------------
(***************************************************************************)
(* One shard of a Felix stream: the lease that lets a broker serve it, the *)
(* replication that puts its records on a majority, and the promotion that *)
(* picks the next leader when the lease lapses.                            *)
(*                                                                         *)
(* The model follows docs/replication-design.md. What it checks:           *)
(*                                                                         *)
(*   AtMostOneServing   -- no two brokers serve the shard at once, under   *)
(*                         clock drift, lost heartbeats, and a broker that  *)
(*                         pauses between admitting a write and committing *)
(*                         it.                                             *)
(*   AckedSurvive       -- a record acknowledged to a client is held by     *)
(*                         the broker serving the shard, always, including *)
(*                         after a promotion.                              *)
(*   AckedHeldByLeader  -- the same of the current leader once it may       *)
(*                         serve, whatever any lease says.                 *)
(*   AckedAgree         -- two brokers never hold different acknowledged   *)
(*                         records at one offset.                          *)
(*   NoTruncationBelowHwm -- a follower never discards a record below its   *)
(*                         high-water mark.                                *)
(*   NoStaleCommit      -- no broker commits at a generation the control    *)
(*                         plane has already superseded.                   *)
(*                                                                         *)
(* With `Handoff`, the control plane may also move the shard while its     *)
(* leader is alive: it fences the leader, which stops serving when it sees *)
(* the fence but keeps shipping, and names the successor only once the    *)
(* leader has reported that its log stopped growing. `WaitForDrained =    *)
(* FALSE` cuts over as soon as the fence is written, and TLC finds the     *)
(* leader landing a write after its successor has taken over.             *)
(*                                                                         *)
(* A write is admitted, then claimed, then committed. Admission checks the *)
(* broker is serving; the claim is where it takes its place in the log,    *)
(* and anything may happen while it waits in between. `FenceAtClaim`      *)
(* checks the fence again at the claim, and the drained report counts only *)
(* claimed writes, as the broker's fence does. Without the check TLC finds *)
(* a write admitted before the fence, claimed after the drained report,    *)
(* committed and acknowledged by the old leader, and missing from the new. *)
(*                                                                         *)
(* `AckOnAdmit` acknowledges a write when it is admitted, as the broker    *)
(* does by default, so a claim refused at the fence is an acknowledged     *)
(* write that never lands. `FenceFromAdmit` has a write hold the fence     *)
(* from admission instead, as the broker's routing does for every local    *)
(* write: its claim is not refused, and the drained report waits for it.   *)
(* Without that, TLC finds the old leader refusing an acknowledged write   *)
(* and the successor taking over without it.                               *)
(*                                                                         *)
(* `StageMove` starts the run with a move's destination added to the       *)
(* replica set and still copying: `staged`, which the leader leaves out of *)
(* the quorum. `LearnerVotes` counts it anyway, as the broker once did,    *)
(* and TLC finds a `Quorum` write held on a majority of the stream's own   *)
(* replicas but not acknowledged, waiting for the copy                     *)
(* (StagedCopyNeverDelaysAck). Leaving it out is safe because a promotion  *)
(* only picks a replica the last report names caught up, and the cut-over  *)
(* waits for the destination to be level.                                  *)
(*                                                                         *)
(* Time is discrete. `now` is real time; each broker has its own clock,    *)
(* within `Drift` of real time, which is the drift-rate assumption of the  *)
(* design in the only form a finite model needs. A broker anchors a lease  *)
(* at the instant it sent the heartbeat, on its own clock, and stops       *)
(* serving `Eps` before its own expiry; the control plane grants the next  *)
(* generation no earlier than `Margin` after the expiry it recorded.       *)
(*                                                                         *)
(* Two knobs exist to show the model has teeth. `CheckAtCommit = FALSE`    *)
(* drops the design's second lease check, and TLC finds the paused broker  *)
(* that commits after its lease lapsed. `Promotion = "leader-report"` is   *)
(* the design as written: the leader reports which followers are caught   *)
(* up, asynchronously, and the control plane promotes from the last report *)
(* it received. `Promotion = "log-order"` promotes the live replica with   *)
(* the highest (last generation, length), Raft's election restriction.     *)
(*                                                                         *)
(* `ReportBound` is what a follower must hold to be reported caught up.    *)
(* "acknowledged", the design: under `Quorum`, everything up to the offset *)
(* a majority holds, the most the mark sent with the report can release,  *)
(* and the leader's whole log otherwise or once it has stopped. "tail"     *)
(* demands the whole log under `Quorum` too, and TLC finds a report naming *)
(* nobody while a majority holds every acknowledged record -- a leader     *)
(* dying then leaves nothing to promote (QuorumReportNamesASuccessor).     *)
(* "unpaired" measures at the majority's offset but reports the full       *)
(* length, so the mark can run past what the holders were measured at, and *)
(* TLC finds the acknowledged record the promoted follower lacks.          *)
(*                                                                         *)
(* `Resends` lets a client send a write it has no answer for again, as an  *)
(* idempotent producer does after a lost acknowledgement or a leader       *)
(* change. The serving broker appends it unless it already knows the       *)
(* write. `SequencesInLog` is where it looks: the records in its log, which *)
(* is what the broker does, or only what it wrote itself since it took     *)
(* over, which is a leader keeping sequences in memory. TLC finds the      *)
(* latter appending a write a second time after a failover or a move       *)
(* (NoDuplicate).                                                          *)
(*                                                                         *)
(* The control plane decides from a read. A decision may read and write in *)
(* one step, or come from a read one of `Planners` took earlier (`cpView`,  *)
(* by Snapshot) and still holds. Every assignment write bumps `ver`, the   *)
(* store's generation. `CasWrites` makes a write land only if `ver` is     *)
(* still what its read saw; without it, TLC finds a planner writing from a *)
(* read another instance has already acted on.                             *)
(*                                                                         *)
(* `Cancel` lets an operator cancel a fenced move: the leader that stopped *)
(* serves again at a new generation, keeping the writes still inside its  *)
(* fence, which land in its own log. The cancel is a planner decision like *)
(* any other, so it too may come from a held read. With `CancelCas =     *)
(* FALSE` only the cancel writes unconditionally, and TLC finds one read   *)
(* while the move was fenced and written after its cut-over, handing the   *)
(* shard back to a leader that never saw what the new one acknowledged.   *)
(*                                                                         *)
(* `AckByFollowers` and `FenceOnPromote` are the design without the clock  *)
(* in `Quorum` safety. A write is acknowledged once a majority holds it at *)
(* the leader's generation, with no report and no lease in the condition;  *)
(* a promoted leader fences a majority and catches up before it serves.    *)
(* Without the fence, TLC finds a deposed leader whose slow clock still    *)
(* lets it write acknowledging on a follower the new one never fenced      *)
(* (AckedHeldByLeader). The broker acknowledges this way on a `Quorum`   *)
(* stream shard once the fleet finalized `majority_ack`, and before that   *)
(* with `FenceOnPromote` alone, alongside the report and the lease.        *)
(*                                                                         *)
(* `Spares` are brokers outside the replica set, which placement may bring *)
(* in when it fails over. `Promotion = "any"` promotes any replica, as a   *)
(* placement reading a stale report or none may. `ReplaceOnPromote` swaps  *)
(* the old leader for a spare in the same write that names the new one, as *)
(* `choose_replicas` would; the fence then counts a majority of the new    *)
(* set, and                                                                *)
(* TLC finds the new leader and the spare opening without the one replica  *)
(* that held what the old set acknowledged (AckedHeldByLeader). Without it *)
(* a promotion keeps the set, the old leader included.                     *)
(*                                                                         *)
(* With spares, `MaxMoves` also bounds follower replacements. `Reseat`     *)
(* adds a spare beside a follower that is leaving, at a new generation, so *)
(* the leader ships to and counts four; `Seat` then drops the leaving one, *)
(* at another. `SeatHoldsCopy` seats only once the newcomer holds what a   *)
(* majority of the set held when it joined. Without it TLC finds a record  *)
(* acknowledged on the leader and the leaving follower, the newcomer       *)
(* seated without it, and the next leader fencing the newcomer and the     *)
(* lagging follower, a majority of the new set that never saw it           *)
(* (AckedHeldByLeader).                                                    *)
(*                                                                         *)
(* `Grow` tops up a set a failover left short of the replication factor:   *)
(* a spare joins with nobody leaving, and `Seat` makes it a member. A set  *)
(* that starts with spares outside it is such a short set.                 *)
(*                                                                         *)
(* Under `AckByFollowers` a planned move and a cancel name a leader without *)
(* the fence, as the broker opens them: the cut-over waits for the drained *)
(* leader's whole log, and a cancel hands the shard back to the leader that *)
(* had it. A failover that names the move's destination is opened the same *)
(* way, because its broker cannot tell it from the cut-over it expected.   *)
(* With `PromoteDestination` placement may name it, and TLC finds the      *)
(* destination opening without a record the old leader acknowledged after *)
(* its last report (AckedHeldByLeader). Without it a `Quorum` failover     *)
(* ends the move instead.                                                  *)
(*                                                                         *)
(* `StartRecord` has a leader write a generation-start record before any   *)
(* client write, and lets the mark and the report's length stop only at a  *)
(* record of its own generation. Without it, a leader acknowledges a       *)
(* record it inherited once a majority holds it, and a later leader whose  *)
(* last record is newer overwrites it: Raft's Figure 8, which             *)
(* FelixShardFigure8NoStartRecord.cfg finds.                               *)
(*                                                                         *)
(* `ReportFromAnswers` has the report count a follower by what it last     *)
(* answered holding at the leader's generation (`confirmed`), as the       *)
(* broker does, rather than by what its log holds. A new leader that has   *)
(* heard from nobody then counts nothing, and names every follower. With   *)
(* two failovers in a row TLC finds the second promoting a follower that   *)
(* lacks a record the first leader acknowledged (AckedSurvive).            *)
(* `ReportFloor` names a follower only once it holds everything the leader *)
(* inherited, which is a bound on what any earlier leader acknowledged.    *)
(*                                                                         *)
(* `Counters` makes the shard a cache shard: `log` is the cache log, and a *)
(* second log, `clog`, holds the counter updates. It ships, is counted and *)
(* acknowledged like the cache log, under one promise per replica: the     *)
(* fence a replica took on the cache log refuses an older leader on both.  *)
(* A promoted leader also fences the counter log on a majority and takes   *)
(* the counter log furthest ahead. `CounterCatchUp = FALSE` skips taking   *)
(* it, and TLC finds a counter update acknowledged by the old leader and   *)
(* missing from the new one (CountersHeldByLeader).                        *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,        \* the shard's replica set
    L,              \* lease length, in ticks
    Margin,         \* how long past a lapse the control plane waits before granting again
    Eps,            \* how long before its own expiry a broker stops serving
    Drift,          \* how far a broker's clock may sit from real time
    MaxTime,        \* real time runs out here; bounds the state space
    MaxWrites,      \* how many client writes the run admits
    CheckAtCommit,  \* re-check the lease before committing, or only at admission
    Quorum,         \* acknowledge on a majority (TRUE) or on the leader alone (FALSE)
    Promotion,      \* "leader-report", "log-order" or "any"
    ReportBeforeAck, \* whether a Quorum ack waits for the report describing it
    Handoff,        \* whether the control plane may move the shard off a live leader
    WaitForDrained, \* whether a cut-over waits for the leader's drained report
    FenceAtClaim,   \* whether a claim re-checks the fence, or only admission does
    AckOnAdmit,     \* under `Leader`, acknowledge on admission rather than on commit
    FenceFromAdmit, \* whether a write acknowledged on admission holds the fence from there
    MaxMoves,       \* how many planned moves the run starts; bounds the state space
    Planners,       \* control-plane instances deciding placement, each from its own read
    CasWrites,      \* whether an assignment write lands only at the generation it read
    StageMove,      \* whether the run starts with a destination staged and copying
    LearnerVotes,   \* whether that destination counts toward the quorum while it copies
    Resends,        \* whether a client may send an unanswered write again
    SequencesInLog, \* whether a re-send is checked against the log, or the leader's own writes
    Cancel,         \* whether an operator may cancel a fenced move
    CancelCas,      \* whether that cancel, too, lands only at the generation it read
    ReportBound,    \* what a follower must hold to be reported: "acknowledged", "tail", "unpaired"
    AckChecksLease, \* whether a Quorum acknowledgement needs a valid lease, or only the report
    AckOnResponse,  \* whether the leader judges its report by the answer it got, or by the store
    AckByFollowers, \* whether a Quorum ack counts followers at this generation instead of the report
    FenceOnPromote, \* whether a promoted leader fences a majority and catches up before serving
    LabelOnReceipt, \* whether a follower labels a shipped record with the sender's generation
    StartRecord,    \* whether a new leader writes a generation-start record and the mark waits for it
    Spares,         \* brokers outside the replica set that a failover may bring in
    ReplaceOnPromote, \* whether a promotion swaps the old leader for a spare
    SeatHoldsCopy,  \* whether a replacement is seated only once it holds what the old set held
    ReportFromAnswers, \* whether the report counts a follower by its answers, or by its log
    ReportFloor,    \* whether the report never names a follower short of the leader's inherited log
    Grow,           \* whether placement may add a spare to the set with nobody leaving
    PromoteDestination, \* whether a failover may name a move's destination leader
    Counters,       \* whether the shard is a cache shard with a counter log beside its log
    CounterCatchUp  \* whether the fence takes the counter log furthest ahead too

ASSUME Promotion \in {"leader-report", "log-order", "any"}
ASSUME ReportBeforeAck \in BOOLEAN
ASSUME Handoff \in BOOLEAN /\ WaitForDrained \in BOOLEAN /\ FenceAtClaim \in BOOLEAN
ASSUME AckOnAdmit \in BOOLEAN /\ FenceFromAdmit \in BOOLEAN
ASSUME CasWrites \in BOOLEAN
ASSUME StageMove \in BOOLEAN /\ LearnerVotes \in BOOLEAN
ASSUME Resends \in BOOLEAN /\ SequencesInLog \in BOOLEAN
ASSUME Cancel \in BOOLEAN /\ CancelCas \in BOOLEAN
ASSUME ReportBound \in {"acknowledged", "tail", "unpaired"}
ASSUME AckChecksLease \in BOOLEAN /\ AckOnResponse \in BOOLEAN
ASSUME AckByFollowers \in BOOLEAN /\ FenceOnPromote \in BOOLEAN
ASSUME LabelOnReceipt \in BOOLEAN
ASSUME StartRecord \in BOOLEAN
ASSUME ReportFromAnswers \in BOOLEAN /\ ReportFloor \in BOOLEAN
ASSUME PromoteDestination \in BOOLEAN
\* Only a promotion or a replacement changes the set, so spares are checked
\* with follower acks (no handoff, no cancel) and no staged copy.
ASSUME Spares \subseteq Brokers /\ ReplaceOnPromote \in BOOLEAN /\ SeatHoldsCopy \in BOOLEAN
ASSUME Grow \in BOOLEAN
ASSUME Spares /= {} => AckByFollowers /\ ~StageMove /\ ~Handoff /\ ~Cancel
ASSUME ReplaceOnPromote => Spares /= {} /\ MaxMoves = 0
\* No lease anywhere on a `Quorum` write's path: not at admission (see
\* Serving), not at the commit, not at the acknowledgement.
ASSUME AckByFollowers => Quorum /\ ~CheckAtCommit /\ ~AckChecksLease
\* Cache shards are checked with follower acks and failover alone, the only
\* way the broker acknowledges a cache write without the lease.
ASSUME Counters \in BOOLEAN /\ CounterCatchUp \in BOOLEAN
ASSUME Counters => /\ AckByFollowers /\ ~LabelOnReceipt /\ ~Resends
                   /\ ~Handoff /\ ~Cancel /\ ~StageMove /\ MaxMoves = 0 /\ Spares = {}
ASSUME Eps < L /\ Margin >= 0

VARIABLES
    now,        \* real time
    clock,      \* each broker's monotonic clock
    gen,        \* the assignment generation at the control plane
    leader,     \* who the control plane assigned at gen
    cpExpiry,   \* when the lease at gen lapses, on the control plane's clock (real time)
    report,     \* what the control plane was last told: [holders, len, drained, gen]
    inflight,   \* a leader report on its way to the control plane, or <<>>
    bgen,       \* the generation each broker believes it leads; 0 means it does not
    bexpiry,    \* each broker's own belief of its lease expiry, on its own clock
    hbOut,      \* whether each broker has a heartbeat in flight
    hbAt,       \* when that heartbeat was sent, on the broker's clock
    log,        \* each broker's log: a sequence of [g |-> generation, id |-> write]
    hwm,        \* each broker's high-water mark: the prefix known committed
    halted,     \* followers that found a divergence they may not repair
    queued,     \* a write admitted by each broker and not yet claimed; 0 means none
    pending,    \* a write claimed by each broker and not yet committed; 0 means none
    acked,      \* writes acknowledged to a client
    writes,     \* how many writes have been admitted so far
    staleCommit, \* history: a broker committed at a generation already superseded
    draining,   \* the control plane has fenced the leader so the shard can move
    successor,  \* where it is moving to; meaningful only while draining
    stopped,    \* each broker has seen the fence and stopped serving
    moves,      \* how many planned moves have been started
    ver,        \* the store's generation for the assignment: bumped by every write
    cpView,     \* the read each planner holds: {} or {view}
    staged,     \* a move's destination added to the replica set and still copying: {} or {f}
    heard,      \* the last report each broker was told the control plane stored
    promised,   \* the highest generation each broker has durably accepted
    fencing,    \* a promoted leader that has not finished its fence
    answered,   \* who has answered each broker's fence at the generation it leads
    confirmed,  \* per leader, how far each follower answered that it holds, at the leader's generation
    out,        \* brokers outside the replica set: the spares, and a leader swapped out
    mine,       \* the replica set each broker was given when it was named leader
    joining,    \* a spare being copied in beside a leaving follower: {} or {j}
    leaving,    \* the follower it replaces: {} or {o}
    joinedAt,   \* how much of the leader's log a majority of the set held when it joined
    clog,       \* each broker's counter log, under `Counters`
    chwm,       \* each broker's counter mark, as leader
    cconfirmed, \* per leader, how far each follower answered that it holds the counter log
    canswered,  \* who has answered each broker's counter fence
    cacked      \* counter updates acknowledged to a client

\* The counter log's state, which only a cache shard's actions change.
counterVars == << clog, chwm, cconfirmed, canswered, cacked >>

vars == << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
           hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
           draining, successor, stopped, moves, ver, cpView, staged, heard,
           promised, fencing, answered, confirmed, out, mine, joining, leaving, joinedAt,
           counterVars >>

\* Placement's state, which only the control plane's decisions change.
handoffVars == << draining, successor, stopped, moves, ver, cpView, staged, out, mine,
                  joining, leaving, joinedAt >>

\* The promotion fence's state.
fenceVars == << promised, fencing, answered, confirmed >>

\* Whether brokers keep and check `promised`: the fence needs it, and so do
\* follower acks.
Promises == AckByFollowers \/ FenceOnPromote

NoReport == [holders |-> {}, len |-> 0, drained |-> FALSE, gen |-> 0]

\* The replica set the stream asked for: everyone but a destination still
\* copying, a replacement still joining, and the brokers outside it. A quorum
\* is a majority of this set, unless `LearnerVotes`.
ReplicaSet == Brokers \ (staged \cup out \cup joining)
QuorumSet == IF LearnerVotes THEN Brokers ELSE ReplicaSet

\* The set plus a copy joining it, where that copy may be part of every
\* majority that acknowledged. The leader counts the newcomer, and beside an
\* even number of members a majority of the larger set can be the leader and
\* the newcomer alone, so a failover keeps it (`keep_replicas`). Beside an
\* odd number every majority still holds a majority of the old set.
Counted == ReplicaSet \cup (IF Cardinality(ReplicaSet) % 2 = 0 THEN joining ELSE {})

MajorityOf(S, of) == Cardinality(S \cap of) * 2 > Cardinality(of)
Majority(S) == MajorityOf(S, QuorumSet)

\* A leader ships to, fences and counts the set its own assignment named,
\* which a deposed leader still holds after the control plane has moved on.
\* With no spares every broker is in every set, as before.
Members(b) == IF Spares = {} THEN Brokers ELSE mine[b]
LeaderMajority(b, S) == IF Spares = {} THEN Majority(S) ELSE MajorityOf(S, mine[b])

\* Brokers are interchangeable, and so are planners, which lets TLC fold
\* their permutations.
Symm == Permutations(Brokers \ Spares) \cup Permutations(Planners)

\* A broker's lease is good while it believes it leads and its own clock is
\* short of its own expiry by the margin it gives up.
LeaseValid(b) == bgen[b] > 0 /\ clock[b] + Eps < bexpiry[b]

\* It serves the shard on that lease until it has seen a fence, and not before
\* its own promotion fence is done. With `AckByFollowers` a `Quorum` shard
\* serves without the lease, for as long as the broker believes it leads:
\* a deposed leader that goes on writing finds no majority to acknowledge
\* on once its successor has fenced one.
Serving(b) == /\ IF AckByFollowers THEN bgen[b] > 0 ELSE LeaseValid(b)
              /\ ~stopped[b] /\ ~fencing[b]

\* `g` is the generation the record was written at, `lg` the one the broker
\* holding it believes that was: its generation history. They differ only
\* under `LabelOnReceipt`. Records are compared by what was written, as the
\* code compares checksums, and ordered by what the broker believes.
Record(g, id) == [g |-> g, id |-> id, lg |-> g]

Same(r, s) == r.g = s.g /\ r.id = s.id

\* With `StartRecord`, a leader's first record at a new generation is a
\* generation-start record, Raft's no-op at the start of a term. It ships and
\* occupies an offset like any record but is no client's write: it is never
\* acknowledged and costs no write. Two generations' start records differ by
\* `g`, which `Same` compares.
StartId == 0
Start(g) == Record(g, StartId)

\* `b`'s log once it starts leading at `g`.
Opened(b, g) == IF StartRecord THEN Append(log[b], Start(g)) ELSE log[b]

\* With `StartRecord` the mark stops only at a record of the leader's own
\* generation, so an inherited record is acknowledged by the start record (or
\* a later write) reaching a majority, never on its own. Counting it on its
\* own is Raft's Figure 8: FelixShardFigure8NoStartRecord.cfg.
OwnGen(b, k) == StartRecord => log[b][k].g = bgen[b]

LastGen(b) == IF Len(log[b]) = 0 THEN 0 ELSE log[b][Len(log[b])].lg

\* A move's destination: staged and copying, or named successor by the fence.
\* The broker opens it without the promotion fence whichever write names it
\* leader (`begin_open` in services/felix-broker-service/src/shards/lifecycle.rs
\* skips a shard it expects as `incoming`), because a cut-over only names it
\* once it holds the drained leader's whole log.
Incoming(f) == f \in staged \/ (draining /\ successor = f)

\* The counter log's start record, written beside the cache log's.
COpened(b, g) == IF StartRecord THEN Append(clog[b], Start(g)) ELSE clog[b]

CLastGen(b) == IF Len(clog[b]) = 0 THEN 0 ELSE clog[b][Len(clog[b])].lg

CounterInit ==
    /\ clog = [b \in Brokers |-> <<>>]
    /\ chwm = [b \in Brokers |-> 0]
    /\ cconfirmed = [b \in Brokers |-> [f \in Brokers |-> 0]]
    /\ canswered = [b \in Brokers |-> {}]
    /\ cacked = {}

-----------------------------------------------------------------------------

Init ==
    /\ now = 0
    /\ clock = [b \in Brokers |-> 0]
    /\ gen = 1
    /\ leader \in Brokers \ Spares
    /\ cpExpiry = L
    /\ report = NoReport
    /\ inflight = <<>>
    /\ bgen = [b \in Brokers |-> IF b = leader THEN 1 ELSE 0]
    /\ bexpiry = [b \in Brokers |-> IF b = leader THEN L ELSE 0]
    /\ hbOut = [b \in Brokers |-> FALSE]
    /\ hbAt = [b \in Brokers |-> 0]
    /\ log = [b \in Brokers |-> <<>>]
    /\ hwm = [b \in Brokers |-> 0]
    /\ halted = {}
    /\ queued = [b \in Brokers |-> 0]
    /\ pending = [b \in Brokers |-> 0]
    /\ acked = {}
    /\ writes = 0
    /\ staleCommit = FALSE
    /\ draining = FALSE
    /\ successor = leader
    /\ stopped = [b \in Brokers |-> FALSE]
    /\ moves = 0
    /\ ver = 0
    /\ cpView = [p \in Planners |-> {}]
    /\ staged \in IF StageMove THEN {{f} : f \in Brokers \ {leader}} ELSE {{}}
    /\ heard = [b \in Brokers |-> NoReport]
    /\ promised = [b \in Brokers |-> IF b = leader THEN 1 ELSE 0]
    /\ fencing = [b \in Brokers |-> FALSE]
    /\ answered = [b \in Brokers |-> {}]
    /\ confirmed = [b \in Brokers |-> [f \in Brokers |-> 0]]
    /\ out = Spares
    /\ mine = [b \in Brokers |-> Brokers \ Spares]
    /\ joining = {}
    /\ leaving = {}
    /\ joinedAt = 0
    /\ CounterInit

-----------------------------------------------------------------------------
(* Time. Real time ticks, and with it each broker's clock moves by zero,   *)
(* one or two, staying within Drift of real time. A clock that stands      *)
(* still is slow; one that moves by two is fast. Both are the design's     *)
(* assumption, and moving them in the same step as real time keeps the     *)
(* state space to what the drift can actually produce.                     *)

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    \* Drawn from the drift window, not from 0..MaxTime + Drift: the same
    \* clocks, without enumerating every function up to MaxTime on each tick,
    \* which the long walks (FelixShardWalk*.cfg) could not afford.
    /\ clock' \in { c \in [Brokers -> (IF now + 1 > Drift THEN now + 1 - Drift ELSE 0)..(now + 1 + Drift)] :
                     \A b \in Brokers : /\ c[b] >= clock[b]
                                        /\ c[b] <= clock[b] + 2 }
    /\ UNCHANGED << gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

-----------------------------------------------------------------------------
(* The lease is the heartbeat. A broker that believes it leads sends one,  *)
(* remembering when on its own clock; the control plane accepts it only    *)
(* while the lease it granted has not lapsed, and the broker then extends  *)
(* its belief from the instant it sent, never from the instant it heard    *)
(* back. A heartbeat can be lost.                                          *)

SendHeartbeat(b) ==
    /\ bgen[b] > 0
    /\ ~hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = TRUE]
    /\ hbAt' = [hbAt EXCEPT ![b] = clock[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

AcceptHeartbeat(b) ==
    /\ hbOut[b]
    /\ leader = b /\ bgen[b] = gen
    /\ now <= cpExpiry
    /\ cpExpiry' = now + L
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ bexpiry' = [bexpiry EXCEPT ![b] = hbAt[b] + L]
    /\ UNCHANGED << now, clock, gen, leader, report, inflight, bgen, hbAt,
                    log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

LoseHeartbeat(b) ==
    /\ hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* A broker that finds its lease lapsed, or that hears of a newer generation,
\* stops believing it leads. Modelled as the broker noticing; the safety
\* argument does not rely on it noticing in time.
StepDown(b) ==
    /\ bgen[b] > 0
    /\ (bgen[b] < gen \/ clock[b] + Eps >= bexpiry[b])
    /\ bgen' = [bgen EXCEPT ![b] = 0]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ stopped' = [stopped EXCEPT ![b] = FALSE]
    /\ fencing' = [fencing EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit,
                    draining, successor, moves, ver, cpView, staged, promised, answered,
                    confirmed, out, mine, joining, leaving, joinedAt >>

-----------------------------------------------------------------------------
(* Writes. Admission checks the broker is serving; the write then waits,  *)
(* and claims its place in the log; the commit checks the lease again, or  *)
(* does not, which is the knob. Anything may happen between admission and *)
(* the claim, and between the claim and the commit: those gaps are a       *)
(* queue and a paused process.                                             *)

\* A write holds the fence from admission, with `FenceFromAdmit`: the broker's
\* routing enters it for every local write (`IngressRouter::dispatch_write`).
Held == FenceFromAdmit

Admit(b) ==
    /\ Serving(b)
    /\ queued[b] = 0
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ queued' = [queued EXCEPT ![b] = writes + 1]
    /\ acked' = IF AckOnAdmit /\ ~Quorum THEN acked \cup {writes + 1} ELSE acked
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* The claim, and with `FenceAtClaim` the fence checked again: a broker that
\* has seen the fence refuses a write it admitted before it. This is
\* `ShardFence::enter` in `services/felix-broker-service/src/shards/lifecycle/fence.rs`,
\* entered right before a publish claims its offsets. A refused write is
\* simply never claimed. Unless it was acknowledged on admission, it was
\* never acknowledged either; one that was holds the fence with `Held`, and
\* is claimed regardless.
Claim(b) ==
    /\ queued[b] /= 0
    /\ pending[b] = 0
    /\ bgen[b] > 0
    /\ (FenceAtClaim /\ ~Held) => ~stopped[b]
    /\ pending' = [pending EXCEPT ![b] = queued[b]]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

Commit(b) ==
    /\ pending[b] /= 0
    /\ bgen[b] > 0
    /\ CheckAtCommit => LeaseValid(b)
    /\ log' = [log EXCEPT ![b] = Append(@, Record(bgen[b], pending[b]))]
    /\ pending' = [pending EXCEPT ![b] = 0]
    \* Under `Leader`, the leader's own durable write is the acknowledgement.
    /\ acked' = IF Quorum THEN acked ELSE acked \cup {pending[b]}
    \* History: the control plane has moved on, and this write still landed.
    /\ staleCommit' = (staleCommit \/ bgen[b] < gen)
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, writes >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* The writes a broker would answer a re-send of without appending. With
\* `SequencesInLog`, every write its log holds: the broker's producer state is
\* derived from the records (`disk_log/producers.rs`), so a replica promoted or
\* moved to knows what it was shipped. Without it, only what the broker wrote
\* itself under the generation it now leads -- a leader's memory, which a new
\* leader starts without.
Known(b) ==
    IF SequencesInLog
    THEN { log[b][i].id : i \in 1..Len(log[b]) }
    ELSE { log[b][i].id : i \in { j \in 1..Len(log[b]) : log[b][j].g = bgen[b] } }

\* A write sent again. The producer's batches are serialised, so nothing of
\* its own is waiting at the broker; the re-send is then either answered
\* from what the broker knows or admitted like any write. A client re-sends
\* whether or not its first send was acknowledged: the answer may have been
\* lost.
Resend(b) ==
    /\ Resends
    /\ Serving(b)
    /\ queued[b] = 0 /\ pending[b] = 0
    /\ \E w \in 1..writes :
        /\ w \notin Known(b)
        /\ queued' = [queued EXCEPT ![b] = w]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

-----------------------------------------------------------------------------
(* Replication. The leader ships the next record a follower is missing. A  *)
(* follower whose log disagrees with the leader's keeps what a newer       *)
(* generation than its own last accepted one says, provided the            *)
(* disagreement sits above its high-water mark; otherwise it halts. A      *)
(* follower refuses a leader older than one it has already heard from.     *)

\* The first offset at which two logs disagree, or one past the shorter.
Diverge(a, c) ==
    LET n == IF Len(a) < Len(c) THEN Len(a) ELSE Len(c)
        d == { i \in 1..n : ~Same(a[i], c[i]) }
    IN IF d = {} THEN n + 1 ELSE CHOOSE i \in d : \A j \in d : i <= j

\* The follower's answer to a ship: its first `k` records are the leader's.
\* The leader keeps it in the follower's cursor as `confirmed`
\* (crates/server/felix-replication/src/ship.rs) and counts it later, by
\* which time the follower may have taken a newer leader's fence.
Confirm(b, f, k) ==
    confirmed' = IF AckByFollowers \/ ReportFromAnswers
                 THEN [confirmed EXCEPT ![b][f] = k] ELSE confirmed

\* Under `AckByFollowers` or `FenceOnPromote` the follower also refuses a
\* leader older than the generation it persisted, and persists the leader's:
\* `accept_sender` in crates/server/felix-replication/src/replica.rs. A leader still fencing
\* does not ship: it may yet take a tail from a follower it would truncate.
\*
\* With `LabelOnReceipt` the follower labels what it appends with the
\* sender's generation rather than the one the record was written at, as a
\* follower did when it recorded a new generation as starting where the
\* batch that brought it appended. A record the leader inherited then looks
\* newer on the follower than it is, and its fence answer overclaims.
Ship(b, f) ==
    /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ f \in Members(b)
    /\ bgen[f] = 0
    /\ bgen[b] >= LastGen(f)
    /\ ~fencing[b]
    /\ Promises => promised[f] <= bgen[b]
    /\ promised' = IF Promises THEN [promised EXCEPT ![f] = bgen[b]] ELSE promised
    /\ LET i == Diverge(log[b], log[f]) IN
       \/ /\ i > Len(log[f])
          /\ i <= Len(log[b])
          /\ log' = [log EXCEPT ![f] = Append(@, IF LabelOnReceipt
                                                    THEN [log[b][i] EXCEPT !.lg = bgen[b]]
                                                    ELSE log[b][i])]
          /\ Confirm(b, f, i)
          /\ UNCHANGED halted
       \/ /\ i <= Len(log[f])
          /\ i <= Len(log[b])
          /\ IF bgen[b] > LastGen(f) /\ i > hwm[f]
             THEN /\ log' = [log EXCEPT ![f] = SubSeq(@, 1, i - 1)]
                  /\ Confirm(b, f, i - 1)
                  /\ UNCHANGED halted
             ELSE /\ halted' = halted \cup {f}
                  /\ UNCHANGED << log, confirmed >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered >>

\* Under `Quorum`, a record is acknowledged once a majority including the
\* leader holds it, and the leader's mark moves up to it.
\*
\* With `ReportBeforeAck`, the mark may not move past what the control plane
\* has already been told: the leader reports who holds the record, waits for
\* that report to land, and only then releases the acknowledgement. This is
\* `publish_mark` in `crates/server/felix-replication/src/driver/shard.rs`, which moves
\* the mark only `if reported`, and `await_quorum`, which blocks the publish on
\* the mark. Without it a leader can tell a client its record is on a majority
\* while the control plane knows nothing about which replica holds it, and a
\* leader dying in that window is replaced from a report that predates the
\* acknowledgement.
\*
\* The majority is over `of`: the quorum set, or for the check below, the
\* stream's own replica set.
\*
\* The broker learns that its report landed from the control plane's answer,
\* per shard: it may act on a report only when the answer says the report was
\* stored for its own generation. So the stored report counts only while it is
\* this broker's, at the generation it leads. A report stored by a later
\* leader says nothing to a deposed one, whose own reports are answered
\* `not_leader` and whose mark stays where it was.
\*
\* With `AckOnResponse` the broker goes by the answer instead, as the code
\* does: `heard` is what it was told was stored, and it stays true for the
\* broker after the control plane has moved on -- promoted someone else,
\* stored a newer report -- until the broker learns otherwise.
AckReadyOver(b, i, of) ==
    LET r == IF AckOnResponse THEN heard[b] ELSE report IN
    /\ OwnGen(b, i)
    /\ MajorityOf({ m \in Brokers : Len(log[m]) >= i /\ Same(log[m][i], log[b][i]) } \cup {b}, of)
    /\ ReportBeforeAck => /\ r.gen = bgen[b]
                          /\ i <= r.len
                          /\ MajorityOf(r.holders \cup {b}, of)

\* The ack decided by the followers alone: a majority, the leader counted
\* like anyone, has answered that it holds the record at the leader's
\* generation.
\*
\* A follower counts from its own answer (`confirmed`), which the leader
\* goes on counting after the follower has taken a newer leader's fence:
\* the leader cannot know. That is safe because the answer came first, so
\* the follower's fence answer carries the record. The leader counts itself
\* only while its own generation is still the highest it accepted: once
\* fenced, what it writes next is in no fence's answer.
\* `held_at_generation` in crates/server/felix-replication/src/quorum.rs.
\*
\* With `StartRecord` only a record written at the leader's own generation
\* is counted, taking the ones below it along, as in Raft. Without it the
\* leader counts a record it inherited, and a later leader whose last record
\* is newer overwrites it: FelixShardFigure8FollowerAcksNoStartRecord.cfg.
HeldAtGen(b, i) ==
    /\ OwnGen(b, i)
    /\ LeaderMajority(b, { m \in Brokers \ {b} : confirmed[b][m] >= i }
                         \cup (IF promised[b] = bgen[b] THEN {b} ELSE {}))

\* With `AckChecksLease = FALSE` the lease plays no part: a broker that still
\* believes it leads acknowledges on the report alone, however lapsed its own
\* clock says the lease is. That admits more than the code does, which moves
\* the mark without looking at the lease (`publish_mark`) and checks it only
\* when the acknowledgement is released (`quorum::release`).
\*
\* With `AckByFollowers` the report plays no part either: see HeldAtGen.
AckQuorum(b) ==
    /\ Quorum
    /\ bgen[b] > 0
    /\ AckChecksLease => LeaseValid(b)
    /\ \E i \in (hwm[b] + 1)..Len(log[b]) :
        /\ IF AckByFollowers THEN HeldAtGen(b, i) ELSE AckReadyOver(b, i, QuorumSet)
        /\ acked' = acked \cup ({ log[b][j].id : j \in 1..i } \ {StartId})
        /\ hwm' = [hwm EXCEPT ![b] = i]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, queued, pending, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* A follower learns the mark from the leader, never past what it holds.
LearnHwm(b, f) ==
    /\ bgen[b] > 0 /\ f /= b
    /\ hwm[f] < hwm[b]
    /\ Len(log[f]) >= hwm[b]
    /\ SubSeq(log[f], 1, hwm[b]) = SubSeq(log[b], 1, hwm[b])
    /\ hwm' = [hwm EXCEPT ![f] = hwm[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

-----------------------------------------------------------------------------
(* Reports. The leader tells the control plane which followers hold every  *)
(* record it does, at which generation, and whether its log has stopped    *)
(* growing. The report travels on its own; it may arrive later than the    *)
(* acknowledgements it describes, or never. One from a generation the      *)
(* control plane has moved past is dropped on arrival, as the store does:  *)
(* it describes a leadership that has ended, and TLC finds what believing  *)
(* it does -- a drained report from the old leader, read as the new one's, *)
(* lets the next move skip its wait.                                       *)

\* `drained` counts claimed writes only. A write still waiting to be claimed
\* is invisible to it, as it is to the broker's fence -- which is why the
\* claim has to check the fence rather than trust the report to cover it.
\* A write that holds the fence from admission is counted from there.
\* Whether `m` holds the first `k` records of `b`'s log.
HoldsPrefix(m, b, k) ==
    Len(log[m]) >= k /\ \A j \in 1..k : Same(log[m][j], log[b][j])

\* Whether `b` counts `m` as holding its first `k` records in a report: by
\* `m`'s log, or with `ReportFromAnswers` by what `m` last answered, as
\* `quorum_offset_without` and `caught_up` read `FollowerCursor::confirmed`.
ReportCounts(m, b, k) ==
    IF ReportFromAnswers THEN confirmed[b][m] >= k ELSE HoldsPrefix(m, b, k)

\* How much of `b`'s log it inherited: the records written before its own
\* generation. Any record an earlier leader acknowledged is among them.
Inherited(b) == Cardinality({ i \in 1..Len(log[b]) : log[b][i].g < bgen[b] })

\* The most of `b`'s log a majority holds, `b` included and halted followers
\* not: `quorum_offset_without`. With `StartRecord`, only up to a record of
\* `b`'s own generation, as the mark.
MajorityLen(b) ==
    LET held == { k \in 0..Len(log[b]) :
                    /\ k > 0 => OwnGen(b, k)
                    /\ MajorityOf({ m \in Brokers \ halted : ReportCounts(m, b, k) } \cup {b},
                                  QuorumSet) }
    IN IF held = {} THEN 0 ELSE CHOOSE k \in held : \A j \in held : j <= k

\* What a follower must hold to be reported caught up. Under `Quorum`, the
\* records the mark sent with this report may release, and never below the
\* mark already out; otherwise, and once the leader has stopped for a move,
\* all of it.
\* With `ReportFloor`, never below the log the leader inherited either:
\* `acknowledged` in crates/server/felix-replication/src/driver/shard.rs.
Max(x, y) == IF x >= y THEN x ELSE y
ReportAt(b) ==
    IF ReportBound /= "tail" /\ Quorum /\ ~stopped[b]
    THEN Max(Max(MajorityLen(b), hwm[b]), IF ReportFloor THEN Inherited(b) ELSE 0)
    ELSE Len(log[b])

\* With follower acks and a promotion that does not read it, nothing reads the
\* report, so it is not sent: that keeps the fenced configurations small.
Report(b) ==
    /\ AckByFollowers => Promotion = "leader-report"
    /\ LeaseValid(b)
    \* Nothing runs the shard's replication pass until the fence is done.
    /\ ~fencing[b]
    /\ leader = b /\ bgen[b] = gen
    /\ inflight = <<>>
    /\ inflight' = << [holders |-> { f \in Brokers \ {b} :
                                        ReportCounts(f, b, ReportAt(b)) /\ f \notin halted },
                       len     |-> IF ReportBound = "unpaired" THEN Len(log[b])
                                                             ELSE ReportAt(b),
                       drained |-> stopped[b] /\ pending[b] = 0 /\ (Held => queued[b] = 0),
                       gen     |-> bgen[b]] >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* A stored report is answered to the leader that sent it, the one leading at
\* its generation. The answer can be lost, leaving the broker with an older one.
DeliverReport ==
    /\ inflight /= <<>>
    /\ report' = IF inflight[1].gen = gen THEN inflight[1] ELSE report
    /\ inflight' = <<>>
    /\ heard' \in IF AckOnResponse /\ inflight[1].gen = gen
                  THEN {[heard EXCEPT ![leader] = inflight[1]], heard}
                  ELSE {heard}
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

LoseReport ==
    /\ inflight /= <<>>
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

-----------------------------------------------------------------------------
(* Promotion. Once the lease has lapsed and the margin has passed, the      *)
(* control plane names a new leader at the next generation. Who qualifies  *)
(* is the rule under test.                                                 *)

\* A candidate under the design as written: reported caught up by the last
\* report the planner read, and not halted.
ByLeaderReport(r, f) == f \in r.holders /\ f \notin halted

\* A candidate under the log-order rule: among the live replicas, one whose
\* (last generation, length) is greatest.
ByLogOrder(old, f) ==
    /\ f \notin halted
    /\ \A o \in Brokers \ (halted \cup out \cup {old}) :
        \/ LastGen(o) < LastGen(f)
        \/ LastGen(o) = LastGen(f) /\ Len(log[o]) <= Len(log[f])

\* Any replica, halted or not: what placement can pick with no report to go
\* by, since only a report names a halt.
ByAny(f) == f \in ReplicaSet \cup joining

\* What a planner reads: the assignment and the last report. `lapsed` is
\* judged at the read and stays true: once the lease at a generation has
\* lapsed no heartbeat renews it, as a node marked down stays down.
Now == [ver       |-> ver,
        gen       |-> gen,
        leader    |-> leader,
        report    |-> report,
        draining  |-> draining,
        successor |-> successor,
        lapsed    |-> now >= cpExpiry + Margin]

\* A planner reads, and holds the read to decide from later.
Snapshot(p) ==
    /\ cpView' = [cpView EXCEPT ![p] = {Now}]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    draining, successor, stopped, moves, ver, staged, out, mine,
                    joining, leaving, joinedAt >>
    /\ UNCHANGED fenceVars

\* A write decided from read `v` lands only if nothing was written since,
\* when the store compares generations.
Cas(v) == CasWrites => v.ver = ver

\* Each decision below is made from a read `v` and leaves the planners'
\* reads as `views`: a held read is used up by the write it decided, one
\* write per shard per pass. Every write bumps the store's generation.
Promote(v, f, views) ==
    /\ f /= v.leader
    /\ v.lapsed
    /\ f \notin out
    \* With it off, a `Quorum` failover never names the destination: it ends
    \* the move instead (`promote` in services/felix-controlplane-service/src/
    \* cluster/placement/plan.rs).
    /\ Quorum /\ ~PromoteDestination => ~Incoming(f)
    /\ CASE Promotion = "leader-report" -> ByLeaderReport(v.report, f)
          [] Promotion = "log-order"     -> ByLogOrder(v.leader, f)
          [] Promotion = "any"           -> ByAny(f)
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ leader' = f
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![f] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = clock[f] + L]
    /\ queued' = [queued EXCEPT ![f] = 0]
    /\ pending' = [pending EXCEPT ![f] = 0]
    /\ report' = NoReport
    /\ draining' = FALSE
    /\ stopped' = [stopped EXCEPT ![f] = FALSE]
    \* A promoted destination leads, so it is part of the set from here.
    /\ staged' = staged \ {f}
    \* What `choose_replicas` in placement/plan.rs would write: the old set
    \* without the dead leader, plus a spare. `keep_replicas` keeps the set,
    \* without a replacement still joining unless it is `Counted`; failover
    \* ends the replacement either way.
    /\ IF ReplaceOnPromote /\ out /= {}
       THEN \E d \in out :
              /\ out' = (out \ {d}) \cup {v.leader}
              /\ mine' = [mine EXCEPT ![f] = (ReplicaSet \ {v.leader}) \cup {d}]
       ELSE /\ mine' = [mine EXCEPT ![f] = Counted \cup {f}]
            /\ out' = out \cup (joining \ (Counted \cup {f}))
    /\ joining' = {}
    /\ leaving' = {}
    /\ joinedAt' = 0
    \* The new leader persists its generation itself first; with
    \* `FenceOnPromote` it then fences the others before it serves.
    /\ promised' = IF Promises THEN [promised EXCEPT ![f] = gen + 1] ELSE promised
    /\ fencing' = [fencing EXCEPT ![f] = FenceOnPromote /\ ~Incoming(f)]
    /\ answered' = [answered EXCEPT ![f] = {}]
    \* Cursors belong to a generation: the new leader starts with none.
    /\ confirmed' = [confirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    \* A fenced leader may still take another log; it writes its start record
    \* when it opens.
    /\ log' = IF FenceOnPromote /\ ~Incoming(f) THEN log ELSE [log EXCEPT ![f] = Opened(f, gen + 1)]
    \* The counter log likewise; a cache shard is never a move's destination.
    /\ clog' = IF Counters /\ ~FenceOnPromote THEN [clog EXCEPT ![f] = COpened(f, gen + 1)] ELSE clog
    /\ cconfirmed' = [cconfirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    /\ canswered' = [canswered EXCEPT ![f] = {}]
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, acked, writes,
                    staleCommit, successor, moves, chwm, cacked >>

-----------------------------------------------------------------------------
(* The promotion fence, under `FenceOnPromote`. The promoted leader asks    *)
(* the replicas to take its generation; each that has not accepted a newer *)
(* one persists it, answers with its log, and from then on refuses the     *)
(* older leader. The new leader takes the tail of any answer ahead of its  *)
(* own log, and opens for writes once a majority, itself included, has     *)
(* answered. Any majority that acknowledged a record shares a replica with *)
(* that one, which either held the record when it answered or refused it  *)
(* after, so no clock is needed to keep the old leader out.                *)

\* Ahead by (last generation, length), the order promotion by log order uses.
Ahead(f, b) ==
    \/ LastGen(f) > LastGen(b)
    \/ LastGen(f) = LastGen(b) /\ Len(log[f]) > Len(log[b])

\* One replica answers the fence, and the leader takes its log if it is ahead.
\* That is the catch-up: whatever a majority acknowledged is in the answer
\* furthest ahead. The replica's half is `ReplicaHandler::fence` in
\* crates/server/felix-replication/src/replica.rs: the generation is fsynced
\* before the answer, which carries the log's end, its commit offset and its
\* last record's generation, the two halves of `Ahead`.
AnswerFence(b, f) ==
    /\ fencing[b] /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ f \in Members(b)
    /\ promised[f] < bgen[b]
    /\ promised' = [promised EXCEPT ![f] = bgen[b]]
    /\ answered' = [answered EXCEPT ![b] = @ \cup {f}]
    /\ log' = IF Ahead(f, b) THEN [log EXCEPT ![b] = log[f]] ELSE log
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, pending, acked, writes, staleCommit,
                    fencing, confirmed >>
    /\ UNCHANGED handoffVars

\* `open_promoted` in crates/server/felix-replication/src/driver.rs, on the
\* first majority `fence_shard` in promotion.rs gets; the tail that wins is
\* the answer furthest ahead by `Ahead`, taken before this.
OpenForWrites(b) ==
    /\ fencing[b]
    /\ LeaderMajority(b, answered[b] \cup {b})
    /\ Counters => LeaderMajority(b, canswered[b] \cup {b})
    /\ fencing' = [fencing EXCEPT ![b] = FALSE]
    /\ log' = [log EXCEPT ![b] = Opened(b, bgen[b])]
    /\ clog' = IF Counters THEN [clog EXCEPT ![b] = COpened(b, bgen[b])] ELSE clog
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, pending, acked, writes, staleCommit,
                    promised, answered, confirmed >>
    /\ UNCHANGED << handoffVars, chwm, cconfirmed, canswered, cacked >>

-----------------------------------------------------------------------------
(* The counter log of a cache shard, under `Counters`. A counter update is   *)
(* admitted and committed in one step: the gaps the cache log's write path  *)
(* has are covered there. The promise is shared with the cache log, so a    *)
(* follower that took a newer fence on either refuses the older leader on   *)
(* both: `accept_sender` and `ReplicaHandler::fence` in replica.rs.         *)

CommitCounter(b) ==
    /\ Counters
    /\ Serving(b)
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ clog' = [clog EXCEPT ![b] = Append(@, Record(bgen[b], writes + 1))]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars, chwm, cconfirmed, canswered, cacked >>

\* As `Ship`, on the counter log. A follower drops a divergent suffix only
\* for a newer generation than its last counter record's.
ShipCounter(b, f) ==
    /\ Counters
    /\ bgen[b] > 0 /\ f /= b
    /\ f \in Members(b)
    /\ bgen[f] = 0
    /\ bgen[b] >= CLastGen(f)
    /\ ~fencing[b]
    /\ promised[f] <= bgen[b]
    /\ promised' = [promised EXCEPT ![f] = bgen[b]]
    /\ LET i == Diverge(clog[b], clog[f]) IN
       /\ i <= Len(clog[b])
       /\ \/ /\ i > Len(clog[f])
             /\ clog' = [clog EXCEPT ![f] = Append(@, clog[b][i])]
             /\ cconfirmed' = [cconfirmed EXCEPT ![b][f] = i]
          \/ /\ i <= Len(clog[f])
             /\ bgen[b] > CLastGen(f)
             /\ clog' = [clog EXCEPT ![f] = SubSeq(@, 1, i - 1)]
             /\ cconfirmed' = [cconfirmed EXCEPT ![b][f] = i - 1]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered, confirmed, chwm, canswered, cacked >>

\* As `HeldAtGen`, on the counter log: the counter mark counts only past a
\* record of the leader's own generation (`quorum::counted_offset`).
CHeldAtGen(b, i) ==
    /\ StartRecord => clog[b][i].g = bgen[b]
    /\ LeaderMajority(b, { m \in Brokers \ {b} : cconfirmed[b][m] >= i }
                         \cup (IF promised[b] = bgen[b] THEN {b} ELSE {}))

AckCounters(b) ==
    /\ Counters
    /\ bgen[b] > 0
    /\ \E i \in (chwm[b] + 1)..Len(clog[b]) :
        /\ CHeldAtGen(b, i)
        /\ cacked' = cacked \cup ({ clog[b][j].id : j \in 1..i } \ {StartId})
        /\ chwm' = [chwm EXCEPT ![b] = i]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars, clog, cconfirmed, canswered >>

CAhead(f, b) ==
    \/ CLastGen(f) > CLastGen(b)
    \/ CLastGen(f) = CLastGen(b) /\ Len(clog[f]) > Len(clog[b])

\* The counter log's fence, at the generation the cache log's fence asked
\* for: a replica that already took it there answers again, one that took a
\* newer one refuses. With `CounterCatchUp` the leader takes the counter log
\* of an answer ahead of its own, as `fence_shard` does for every log the
\* shard has (promotion.rs).
AnswerCounterFence(b, f) ==
    /\ Counters
    /\ fencing[b] /\ bgen[b] > 0 /\ f /= b
    /\ f \in Members(b)
    /\ f \notin canswered[b]
    /\ promised[f] <= bgen[b]
    /\ promised' = [promised EXCEPT ![f] = bgen[b]]
    /\ canswered' = [canswered EXCEPT ![b] = @ \cup {f}]
    /\ clog' = IF CounterCatchUp /\ CAhead(f, b) THEN [clog EXCEPT ![b] = clog[f]] ELSE clog
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered, confirmed, chwm, cconfirmed, cacked >>

-----------------------------------------------------------------------------
(* Planned handoff. The control plane fences the leader so the shard can    *)
(* move to a follower the last report says is caught up. The leader keeps  *)
(* its lease and keeps shipping; it stops serving when it sees the fence,  *)
(* and a write it claimed before that still lands. Its next report says    *)
(* whether the log has stopped growing -- fenced, with no claimed write    *)
(* outstanding -- and the cut-over waits for that, or does not, which is   *)
(* the knob.                                                               *)

\* The fence names the leader that was read. If that is no longer the
\* leader -- only possible without `CasWrites` -- the write hands the shard
\* back to it at a new generation, fenced from the start.
\*
\* Placement fences once the destination is within a lag bound of the
\* leader's tail (`FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS`), not only when it
\* is level. The model allows any lag: a fence toward a destination holding
\* nothing is still safe, because the cut-over waits for a drained report
\* naming it level. So every bound the code may use is covered.
Fence(v, f, views) ==
    /\ Handoff
    /\ moves < MaxMoves
    /\ ~v.draining
    /\ f /= v.leader
    /\ f \notin halted
    /\ staged /= {} => f \in staged
    \* Placement fences only on a report from the generation it read
    \* (`ready_to_fence` in moves.rs), and a leader still in its promotion fence
    \* sends none, so a move never catches one mid-fence.
    /\ v.report.gen = v.gen
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ IF v.leader = leader
       THEN UNCHANGED << gen, leader, cpExpiry, report, bgen, bexpiry, queued, pending, stopped,
                         log >>
       ELSE /\ gen' = gen + 1
            /\ leader' = v.leader
            /\ cpExpiry' = now + L
            /\ bgen' = [bgen EXCEPT ![v.leader] = gen + 1]
            /\ bexpiry' = [bexpiry EXCEPT ![v.leader] = clock[v.leader] + L]
            /\ queued' = [queued EXCEPT ![v.leader] = 0]
            /\ pending' = [pending EXCEPT ![v.leader] = 0]
            /\ report' = NoReport
            /\ stopped' = [stopped EXCEPT ![v.leader] = TRUE]
            /\ log' = [log EXCEPT ![v.leader] = Opened(v.leader, gen + 1)]
    /\ draining' = TRUE
    /\ successor' = f
    /\ moves' = moves + 1
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, acked, writes,
                    staleCommit, staged, out, mine, joining, leaving, joinedAt >>
    /\ UNCHANGED fenceVars

\* The leader sees the fence. Modelled as the broker noticing; the cut-over
\* below does not rely on it noticing in time.
ObserveFence(b) ==
    /\ draining /\ leader = b /\ bgen[b] = gen
    /\ ~stopped[b]
    /\ stopped' = [stopped EXCEPT ![b] = TRUE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    draining, successor, moves, ver, cpView, staged, out, mine,
                    joining, leaving, joinedAt >>
    /\ UNCHANGED fenceVars

\* A leader named without the fence still persists its generation before its
\* start record (`accept_generation` in `open`, lifecycle.rs), and its cursors
\* start empty at the new generation.
Takes(f) ==
    /\ promised' = IF Promises THEN [promised EXCEPT ![f] = gen + 1] ELSE promised
    /\ confirmed' = [confirmed EXCEPT ![f] = [m \in Brokers |-> 0]]

CutOver(v, f, views) ==
    /\ v.draining /\ v.successor = f
    /\ f \notin halted
    /\ WaitForDrained => (v.report.gen = v.gen /\ v.report.drained /\ f \in v.report.holders)
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ leader' = f
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![f] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = clock[f] + L]
    /\ queued' = [queued EXCEPT ![f] = 0]
    /\ pending' = [pending EXCEPT ![f] = 0]
    /\ report' = NoReport
    /\ draining' = FALSE
    /\ stopped' = [stopped EXCEPT ![f] = FALSE]
    /\ staged' = staged \ {f}
    /\ log' = [log EXCEPT ![f] = Opened(f, gen + 1)]
    /\ Takes(f)
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, acked, writes,
                    staleCommit, successor, moves, out, mine, joining, leaving, joinedAt >>
    /\ UNCHANGED << fencing, answered >>

\* An operator cancels a fenced move (`cancel_move` in
\* services/felix-controlplane-service/src/cluster/placement/operator.rs): the
\* leader that was read serves again at a new generation. Unlike a promotion
\* it keeps what it has queued and claimed: those writes are inside its fence,
\* were admitted against the same log, and land in it. It keeps that log, and
\* with it the producer sequences its records carry, so a write re-sent after
\* the cancel is answered from there. The destination stays out of the
\* quorum, as the code drops it from the replicas.
Retake(v, f, views) ==
    /\ Cancel
    /\ v.draining
    /\ f = v.leader
    /\ CancelCas => Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ leader' = f
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![f] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = clock[f] + L]
    /\ report' = NoReport
    /\ draining' = FALSE
    /\ stopped' = [stopped EXCEPT ![f] = FALSE]
    /\ log' = [log EXCEPT ![f] = Opened(f, gen + 1)]
    /\ Takes(f)
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, queued, pending,
                    acked, writes, staleCommit, successor, moves, staged, out, mine,
                    joining, leaving, joinedAt >>
    /\ UNCHANGED << fencing, answered >>

-----------------------------------------------------------------------------
(* Replacing a follower (`reseat` and `replacement_step` in                 *)
(* services/felix-controlplane-service/src/cluster/placement/moves.rs). The *)
(* leader stays; each step is a new generation at which it ships to, and   *)
(* counts, the set it was given. It opens each with a start record, as it  *)
(* opens a promotion.                                                      *)

\* The most of `b`'s log a majority of the set it leads holds, `b` included
\* and halted followers not. Placement reads it from a report at the joining
\* generation, which is later, so what it reads is at least this.
OldSetLen(b) ==
    LET held == { k \in 0..Len(log[b]) :
                    MajorityOf({ m \in mine[b] \ halted : HoldsPrefix(m, b, k) } \cup {b},
                               mine[b]) }
    IN CHOOSE k \in held : \A j \in held : j <= k

\* The leader, still serving, moves to the next generation with `set`.
Regenerate(v, views, set) ==
    /\ v.leader = leader /\ ~v.lapsed
    /\ bgen[leader] = gen /\ ~fencing[leader]
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ bgen' = [bgen EXCEPT ![leader] = gen + 1]
    /\ promised' = [promised EXCEPT ![leader] = gen + 1]
    /\ confirmed' = [confirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
    /\ log' = [log EXCEPT ![leader] = Opened(leader, gen + 1)]
    /\ report' = NoReport
    /\ mine' = [mine EXCEPT ![leader] = set]
    /\ UNCHANGED << now, clock, leader, cpExpiry, inflight, bexpiry, hbOut, hbAt, hwm, halted,
                    queued, pending, acked, writes, staleCommit, draining, successor, stopped,
                    staged, fencing, answered >>

\* A spare joins beside the follower `o`, and counts toward the quorum from
\* here: a record is acknowledged on three of the four.
Reseat(v, o, views) ==
    /\ moves < MaxMoves
    /\ joining = {}
    /\ o \in ReplicaSet \ {leader}
    \* A restore grows a set short of the factor before it replaces anyone,
    \* so with `Grow` a replacement only ever joins an odd set.
    /\ Grow => Cardinality(ReplicaSet) % 2 = 1
    /\ \E d \in out :
        /\ Regenerate(v, views, mine[leader] \cup {d})
        /\ joining' = {d}
        /\ out' = out \ {d}
    /\ leaving' = {o}
    /\ joinedAt' = OldSetLen(leader)
    /\ moves' = moves + 1

\* A spare joins a set short of the replication factor, and counts toward
\* the quorum from here. Nobody leaves, so its seat changes nothing but the
\* generation.
GrowSet(v, d, views) ==
    /\ Grow
    /\ moves < MaxMoves
    /\ joining = {}
    /\ d \in out
    /\ Regenerate(v, views, mine[leader] \cup {d})
    /\ joining' = {d}
    /\ out' = out \ {d}
    /\ leaving' = {}
    /\ joinedAt' = OldSetLen(leader)
    /\ moves' = moves + 1

\* The leaving follower goes. With `SeatHoldsCopy`, only once the newcomer
\* holds what a majority of the set held when it joined; a record
\* acknowledged since is on three of the four, and survives losing one.
Seat(v, j, views) ==
    /\ j \in joining
    /\ SeatHoldsCopy => HoldsPrefix(j, leader, joinedAt)
    /\ Regenerate(v, views, mine[leader] \ leaving)
    /\ out' = out \cup leaving
    /\ joining' = {}
    /\ leaving' = {}
    /\ joinedAt' = 0
    /\ UNCHANGED moves

\* A placement write, from a read taken in the same step or from one a
\* planner has held since.
Decide(v, f, views) ==
    \/ Promote(v, f, views)
    \/ /\ \/ Fence(v, f, views) \/ CutOver(v, f, views) \/ Retake(v, f, views)
          \/ Reseat(v, f, views) \/ GrowSet(v, f, views) \/ Seat(v, f, views)
       /\ UNCHANGED counterVars

-----------------------------------------------------------------------------

\* Only a report's delivery changes what a broker has heard.
\* The counter log changes only in the actions that name it.
Step ==
    \/ Tick /\ UNCHANGED counterVars
    \/ \E b \in Brokers :
        \/ /\ \/ SendHeartbeat(b)
              \/ AcceptHeartbeat(b)
              \/ LoseHeartbeat(b)
              \/ StepDown(b)
              \/ Admit(b)
              \/ Resend(b)
              \/ Claim(b)
              \/ Commit(b)
              \/ AckQuorum(b)
              \/ Report(b)
              \/ ObserveFence(b)
              \/ \E f \in Brokers : Ship(b, f) \/ LearnHwm(b, f) \/ AnswerFence(b, f)
           /\ UNCHANGED counterVars
        \/ OpenForWrites(b)
        \/ Decide(Now, b, cpView)
        \/ \E p \in Planners : \E v \in cpView[p] : Decide(v, b, [cpView EXCEPT ![p] = {}])
        \/ CommitCounter(b)
        \/ AckCounters(b)
        \/ \E f \in Brokers : ShipCounter(b, f) \/ AnswerCounterFence(b, f)
    \/ LoseReport /\ UNCHANGED counterVars
    \/ \E p \in Planners : Snapshot(p) /\ UNCHANGED counterVars

Next == (Step /\ UNCHANGED heard) \/ (DeliverReport /\ UNCHANGED counterVars)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* What has to be true.                                                    *)

\* No two brokers serve the shard at once.
AtMostOneServing ==
    \A a, c \in Brokers : Serving(a) /\ Serving(c) => a = c

\* A record acknowledged to a client is held by whoever is serving. One
\* acknowledged on admission may still be on its way into that broker's log.
AckedSurvive ==
    \A b \in Brokers : Serving(b) =>
        \A id \in acked :
            \/ \E i \in 1..Len(log[b]) : log[b][i].id = id
            \/ AckOnAdmit /\ id \in {queued[b], pending[b]}

\* The leader at the current generation holds every acknowledged record once
\* it may serve. Unlike AckedSurvive this says nothing about the lease, so it
\* holds or fails whatever a deposed leader's clock tells it.
AckedHeldByLeader ==
    \A b \in Brokers : (bgen[b] = gen /\ ~fencing[b]) =>
        \A id \in acked : \E i \in 1..Len(log[b]) : log[b][i].id = id

\* Under `Quorum`, a report from a leader still serving names a follower that
\* may take over whenever a majority of the replicas is still replicating.
\* Otherwise a leader dying just after it leaves nobody to promote, however
\* many followers hold every record a client was promised.
QuorumReportNamesASuccessor ==
    (Quorum /\ inflight /= <<>> /\ ~inflight[1].drained /\ ~stopped[leader]
            /\ inflight[1].gen = bgen[leader])
        => \/ inflight[1].holders /= {}
           \/ ~Majority(Brokers \ halted)

\* Two brokers never hold different acknowledged records at one offset.
\* Compared by write rather than by (generation, write): a re-sent write
\* stored by a new leader where a deposed one still has its first copy is the
\* same record, and the deposed copy is truncated when it rejoins. Without
\* re-sends a write is stored once, at one generation, and the two agree.
AckedAgree ==
    \A a, c \in Brokers : \A i \in 1..Len(log[a]) :
        /\ i <= Len(log[c])
        /\ log[a][i].id \in acked
        /\ log[c][i].id \in acked
        => log[a][i].id = log[c][i].id

\* No log holds one write twice: a re-send of a write the shard already has
\* is answered, not appended. Start records are no one's write.
NoDuplicate ==
    \A b \in Brokers : \A i, j \in 1..Len(log[b]) :
        i /= j /\ log[b][i].id /= StartId => log[b][i].id /= log[b][j].id

\* No broker commits a write at a generation the control plane has superseded:
\* once the next leader is named, the old one's lease has run out by its own
\* clock, and its commit check says so. Without that check, a broker paused
\* between admitting and committing lands a write after its epoch ended.
NoStaleCommit == ~staleCommit

\* A follower never discards a record below its high-water mark.
NoTruncationBelowHwm ==
    \A b \in Brokers : Len(log[b]) >= hwm[b]

\* Every acknowledged record is on a majority of the stream's replica set, so
\* it survives any minority loss. The one set of four, a promoted replacement
\* leading the three it joined, needs two: every fence there takes three. A
\* copy joining an even set is counted with it (`Counted`).
AckedOnMajority ==
    Quorum =>
        \A id \in acked :
            LET holders == { b \in Counted : \E i \in 1..Len(log[b]) : log[b][i].id = id }
            IN \/ Cardinality(holders) * 2 > Cardinality(Counted)
               \/ Cardinality(Counted) = 4 /\ Cardinality(holders) = 2

\* A `Quorum` write is never held back by a destination's copy: whenever the
\* stream's own replica set would acknowledge it, the leader can. A latency
\* property, stated as the enabling condition of AckQuorum so TLC can check
\* it as an invariant.
StagedCopyNeverDelaysAck ==
    Quorum =>
        \A b \in Brokers : LeaseValid(b) =>
            \A i \in (hwm[b] + 1)..Len(log[b]) :
                AckReadyOver(b, i, ReplicaSet) => AckReadyOver(b, i, QuorumSet)

\* Under `Counters`, the leader at the current generation holds every
\* acknowledged counter update once it may serve, as AckedHeldByLeader.
CountersHeldByLeader ==
    \A b \in Brokers : (bgen[b] = gen /\ ~fencing[b]) =>
        \A id \in cacked : \E i \in 1..Len(clog[b]) : clog[b][i].id = id

\* Every acknowledged counter update is on a majority of the replicas.
CountersOnMajority ==
    \A id \in cacked :
        MajorityOf({ b \in Brokers : \E i \in 1..Len(clog[b]) : clog[b][i].id = id }, Brokers)

TypeOK ==
    /\ now \in 0..MaxTime
    /\ gen \in Nat
    /\ leader \in Brokers
    /\ halted \subseteq Brokers
    /\ writes \in 0..MaxWrites
    /\ draining \in BOOLEAN
    /\ successor \in Brokers
    /\ moves \in 0..MaxMoves
    /\ ver \in Nat
    /\ \A p \in Planners : Cardinality(cpView[p]) <= 1
    /\ staged \subseteq Brokers /\ Cardinality(staged) <= 1
    /\ heard \in [Brokers -> [holders : SUBSET Brokers, len : Nat, drained : BOOLEAN, gen : Nat]]
    /\ promised \in [Brokers -> Nat]
    /\ fencing \in [Brokers -> BOOLEAN]
    /\ answered \in [Brokers -> SUBSET Brokers]
    /\ confirmed \in [Brokers -> [Brokers -> Nat]]
    /\ out \subseteq Brokers
    /\ mine \in [Brokers -> SUBSET Brokers]
    /\ joining \subseteq Brokers /\ leaving \subseteq Brokers
    /\ joinedAt \in Nat
    /\ chwm \in [Brokers -> Nat]
    /\ cconfirmed \in [Brokers -> [Brokers -> Nat]]
    /\ canswered \in [Brokers -> SUBSET Brokers]
    /\ cacked \subseteq 1..MaxWrites

=============================================================================
