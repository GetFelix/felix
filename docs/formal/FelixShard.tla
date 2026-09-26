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
    Promotion,      \* "leader-report" or "log-order"
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
    ReportBound     \* what a follower must hold to be reported: "acknowledged", "tail", "unpaired"

ASSUME Promotion \in {"leader-report", "log-order"}
ASSUME ReportBeforeAck \in BOOLEAN
ASSUME Handoff \in BOOLEAN /\ WaitForDrained \in BOOLEAN /\ FenceAtClaim \in BOOLEAN
ASSUME AckOnAdmit \in BOOLEAN /\ FenceFromAdmit \in BOOLEAN
ASSUME CasWrites \in BOOLEAN
ASSUME StageMove \in BOOLEAN /\ LearnerVotes \in BOOLEAN
ASSUME Resends \in BOOLEAN /\ SequencesInLog \in BOOLEAN
ASSUME Cancel \in BOOLEAN /\ CancelCas \in BOOLEAN
ASSUME ReportBound \in {"acknowledged", "tail", "unpaired"}
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
    staged      \* a move's destination added to the replica set and still copying: {} or {f}

vars == << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
           hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
           draining, successor, stopped, moves, ver, cpView, staged >>

\* Placement's state, which only the control plane's decisions change.
handoffVars == << draining, successor, stopped, moves, ver, cpView, staged >>

NoReport == [holders |-> {}, len |-> 0, drained |-> FALSE, gen |-> 0]

\* The replica set the stream asked for: everyone but a destination still
\* copying. A quorum is a majority of this set, unless `LearnerVotes`.
ReplicaSet == Brokers \ staged
QuorumSet == IF LearnerVotes THEN Brokers ELSE ReplicaSet

MajorityOf(S, of) == Cardinality(S \cap of) * 2 > Cardinality(of)
Majority(S) == MajorityOf(S, QuorumSet)

\* Brokers are interchangeable, and so are planners, which lets TLC fold
\* their permutations.
Symm == Permutations(Brokers) \cup Permutations(Planners)

\* A broker's lease is good while it believes it leads and its own clock is
\* short of its own expiry by the margin it gives up.
LeaseValid(b) == bgen[b] > 0 /\ clock[b] + Eps < bexpiry[b]

\* It serves the shard on that lease until it has seen a fence.
Serving(b) == LeaseValid(b) /\ ~stopped[b]

Record(g, id) == [g |-> g, id |-> id]

LastGen(b) == IF Len(log[b]) = 0 THEN 0 ELSE log[b][Len(log[b])].g

-----------------------------------------------------------------------------

Init ==
    /\ now = 0
    /\ clock = [b \in Brokers |-> 0]
    /\ gen = 1
    /\ leader \in Brokers
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

-----------------------------------------------------------------------------
(* Time. Real time ticks, and with it each broker's clock moves by zero,   *)
(* one or two, staying within Drift of real time. A clock that stands      *)
(* still is slow; one that moves by two is fast. Both are the design's     *)
(* assumption, and moving them in the same step as real time keeps the     *)
(* state space to what the drift can actually produce.                     *)

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ clock' \in { c \in [Brokers -> 0..(MaxTime + Drift)] :
                     \A b \in Brokers : /\ c[b] >= clock[b]
                                        /\ c[b] <= clock[b] + 2
                                        /\ c[b] >= now + 1 - Drift
                                        /\ c[b] <= now + 1 + Drift }
    /\ UNCHANGED << gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED handoffVars

AcceptHeartbeat(b) ==
    /\ hbOut[b]
    /\ leader = b /\ bgen[b] = gen
    /\ now <= cpExpiry
    /\ cpExpiry' = now + L
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ bexpiry' = [bexpiry EXCEPT ![b] = hbAt[b] + L]
    /\ UNCHANGED << now, clock, gen, leader, report, inflight, bgen, hbAt,
                    log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

LoseHeartbeat(b) ==
    /\ hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit,
                    draining, successor, moves, ver, cpView, staged >>

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
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED handoffVars

-----------------------------------------------------------------------------
(* Replication. The leader ships the next record a follower is missing. A  *)
(* follower whose log disagrees with the leader's keeps what a newer       *)
(* generation than its own last accepted one says, provided the            *)
(* disagreement sits above its high-water mark; otherwise it halts. A      *)
(* follower refuses a leader older than one it has already heard from.     *)

\* The first offset at which two logs disagree, or one past the shorter.
Diverge(a, c) ==
    LET n == IF Len(a) < Len(c) THEN Len(a) ELSE Len(c)
        d == { i \in 1..n : a[i] /= c[i] }
    IN IF d = {} THEN n + 1 ELSE CHOOSE i \in d : \A j \in d : i <= j

Ship(b, f) ==
    /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ bgen[f] = 0
    /\ bgen[b] >= LastGen(f)
    /\ LET i == Diverge(log[b], log[f]) IN
       \/ /\ i > Len(log[f])
          /\ i <= Len(log[b])
          /\ log' = [log EXCEPT ![f] = Append(@, log[b][i])]
          /\ UNCHANGED halted
       \/ /\ i <= Len(log[f])
          /\ i <= Len(log[b])
          /\ IF bgen[b] > LastGen(f) /\ i > hwm[f]
             THEN /\ log' = [log EXCEPT ![f] = SubSeq(@, 1, i - 1)]
                  /\ UNCHANGED halted
             ELSE /\ halted' = halted \cup {f}
                  /\ UNCHANGED log
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

\* Under `Quorum`, a record is acknowledged once a majority including the
\* leader holds it, and the leader's mark moves up to it.
\*
\* With `ReportBeforeAck`, the mark may not move past what the control plane
\* has already been told: the leader reports who holds the record, waits for
\* that report to land, and only then releases the acknowledgement. This is
\* `publish_mark` in `services/felix-broker-service/src/replication/driver/shard.rs`, which moves
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
AckReadyOver(b, i, of) ==
    /\ MajorityOf({ m \in Brokers : Len(log[m]) >= i /\ log[m][i] = log[b][i] } \cup {b}, of)
    /\ ReportBeforeAck => /\ report.gen = bgen[b]
                          /\ i <= report.len
                          /\ MajorityOf(report.holders \cup {b}, of)

AckQuorum(b) ==
    /\ Quorum
    /\ LeaseValid(b)
    /\ \E i \in (hwm[b] + 1)..Len(log[b]) :
        /\ AckReadyOver(b, i, QuorumSet)
        /\ acked' = acked \cup { log[b][j].id : j \in 1..i }
        /\ hwm' = [hwm EXCEPT ![b] = i]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, queued, pending, writes, staleCommit >>
    /\ UNCHANGED handoffVars

\* A follower learns the mark from the leader, never past what it holds.
LearnHwm(b, f) ==
    /\ bgen[b] > 0 /\ f /= b
    /\ hwm[f] < hwm[b]
    /\ Len(log[f]) >= hwm[b]
    /\ SubSeq(log[f], 1, hwm[b]) = SubSeq(log[b], 1, hwm[b])
    /\ hwm' = [hwm EXCEPT ![f] = hwm[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

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
    Len(log[m]) >= k /\ SubSeq(log[m], 1, k) = SubSeq(log[b], 1, k)

\* The most of `b`'s log a majority holds, `b` included and halted followers
\* not: `quorum_offset_without`.
MajorityLen(b) ==
    LET held == { k \in 0..Len(log[b]) :
                    MajorityOf({ m \in Brokers \ halted : HoldsPrefix(m, b, k) } \cup {b},
                               QuorumSet) }
    IN IF held = {} THEN 0 ELSE CHOOSE k \in held : \A j \in held : j <= k

\* What a follower must hold to be reported caught up. Under `Quorum`, the
\* records the mark sent with this report may release, and never below the
\* mark already out; otherwise, and once the leader has stopped for a move,
\* all of it.
ReportAt(b) ==
    IF ReportBound /= "tail" /\ Quorum /\ ~stopped[b]
    THEN IF MajorityLen(b) >= hwm[b] THEN MajorityLen(b) ELSE hwm[b]
    ELSE Len(log[b])

Report(b) ==
    /\ LeaseValid(b)
    /\ leader = b /\ bgen[b] = gen
    /\ inflight = <<>>
    /\ inflight' = << [holders |-> { f \in Brokers \ {b} :
                                        HoldsPrefix(f, b, ReportAt(b)) /\ f \notin halted },
                       len     |-> IF ReportBound = "unpaired" THEN Len(log[b])
                                                             ELSE ReportAt(b),
                       drained |-> stopped[b] /\ pending[b] = 0 /\ (Held => queued[b] = 0),
                       gen     |-> bgen[b]] >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

DeliverReport ==
    /\ inflight /= <<>>
    /\ report' = IF inflight[1].gen = gen THEN inflight[1] ELSE report
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

LoseReport ==
    /\ inflight /= <<>>
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

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
    /\ \A o \in Brokers \ (halted \cup {old}) :
        \/ LastGen(o) < LastGen(f)
        \/ LastGen(o) = LastGen(f) /\ Len(log[o]) <= Len(log[f])

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
                    draining, successor, stopped, moves, ver, staged >>

\* A write decided from read `v` lands only if nothing was written since,
\* when the store compares generations.
Cas(v) == CasWrites => v.ver = ver

\* Each decision below is made from a read `v` and leaves the planners'
\* reads as `views`: a held read is used up by the write it decided, one
\* write per shard per pass. Every write bumps the store's generation.
Promote(v, f, views) ==
    /\ f /= v.leader
    /\ v.lapsed
    /\ IF Promotion = "leader-report" THEN ByLeaderReport(v.report, f)
                                      ELSE ByLogOrder(v.leader, f)
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
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit, successor, moves >>

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
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ IF v.leader = leader
       THEN UNCHANGED << gen, leader, cpExpiry, report, bgen, bexpiry, queued, pending, stopped >>
       ELSE /\ gen' = gen + 1
            /\ leader' = v.leader
            /\ cpExpiry' = now + L
            /\ bgen' = [bgen EXCEPT ![v.leader] = gen + 1]
            /\ bexpiry' = [bexpiry EXCEPT ![v.leader] = clock[v.leader] + L]
            /\ queued' = [queued EXCEPT ![v.leader] = 0]
            /\ pending' = [pending EXCEPT ![v.leader] = 0]
            /\ report' = NoReport
            /\ stopped' = [stopped EXCEPT ![v.leader] = TRUE]
    /\ draining' = TRUE
    /\ successor' = f
    /\ moves' = moves + 1
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit, staged >>

\* The leader sees the fence. Modelled as the broker noticing; the cut-over
\* below does not rely on it noticing in time.
ObserveFence(b) ==
    /\ draining /\ leader = b /\ bgen[b] = gen
    /\ ~stopped[b]
    /\ stopped' = [stopped EXCEPT ![b] = TRUE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    draining, successor, moves, ver, cpView, staged >>

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
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit, successor, moves >>

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
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, queued, pending,
                    acked, writes, staleCommit, successor, moves, staged >>

\* A placement write, from a read taken in the same step or from one a
\* planner has held since.
Decide(v, f, views) ==
    Promote(v, f, views) \/ Fence(v, f, views) \/ CutOver(v, f, views) \/ Retake(v, f, views)

-----------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E b \in Brokers :
        \/ SendHeartbeat(b)
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
        \/ Decide(Now, b, cpView)
        \/ \E p \in Planners : \E v \in cpView[p] : Decide(v, b, [cpView EXCEPT ![p] = {}])
        \/ \E f \in Brokers : Ship(b, f) \/ LearnHwm(b, f)
    \/ DeliverReport
    \/ LoseReport
    \/ \E p \in Planners : Snapshot(p)

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
\* is answered, not appended.
NoDuplicate ==
    \A b \in Brokers : \A i, j \in 1..Len(log[b]) :
        i /= j => log[b][i].id /= log[b][j].id

\* No broker commits a write at a generation the control plane has superseded:
\* once the next leader is named, the old one's lease has run out by its own
\* clock, and its commit check says so. Without that check, a broker paused
\* between admitting and committing lands a write after its epoch ended.
NoStaleCommit == ~staleCommit

\* A follower never discards a record below its high-water mark.
NoTruncationBelowHwm ==
    \A b \in Brokers : Len(log[b]) >= hwm[b]

\* Every acknowledged record is on a majority of the stream's replica set, so
\* it survives any minority loss.
AckedOnMajority ==
    Quorum =>
        \A id \in acked :
            MajorityOf({ b \in Brokers : \E i \in 1..Len(log[b]) : log[b][i].id = id },
                       ReplicaSet)

\* A `Quorum` write is never held back by a destination's copy: whenever the
\* stream's own replica set would acknowledge it, the leader can. A latency
\* property, stated as the enabling condition of AckQuorum so TLC can check
\* it as an invariant.
StagedCopyNeverDelaysAck ==
    Quorum =>
        \A b \in Brokers : LeaseValid(b) =>
            \A i \in (hwm[b] + 1)..Len(log[b]) :
                AckReadyOver(b, i, ReplicaSet) => AckReadyOver(b, i, QuorumSet)

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

=============================================================================
