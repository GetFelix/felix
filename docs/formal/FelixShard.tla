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
(* A seat into an even set fences the leader first (`SeatFence`) and       *)
(* waits for its drained report to name the newcomer holding its whole     *)
(* log: up to the seat a record can be on the leader and the leaving copy  *)
(* alone, and without the fence TLC finds `{a, c}` holding one on a alone  *)
(* (AckedOnMajority, #1616).                                               *)
(*                                                                         *)
(* A replacement given up beside an even set fences the leader first       *)
(* (`UndoFence`) and drops the copy joining only once the drained report   *)
(* has every other follower holding the leader's whole log (`Undo`).       *)
(* Dropped while the leader serves, TLC finds a record acknowledged on     *)
(* the leader and that copy alone left on the leader only                  *)
(* (AckedOnMajority, #1628).                                               *)
(*                                                                         *)
(* A failover right after a seat into two may keep the dropped copy        *)
(* (`SeatDropped`). A set above the factor is fenced (`TrimFence`) and,    *)
(* once its leader reports drained, drops a follower the rest can do       *)
(* without (`TrimSet`). `LoweredFactor` starts the set one above the       *)
(* factor, as after the factor is lowered, so the trim and the keep are    *)
(* checked without a seat.                                                 *)
(*                                                                         *)
(* `Grow` tops up a set a failover left short of the replication factor:   *)
(* a spare joins with nobody leaving, and `Seat` makes it a member. A set  *)
(* that starts with spares outside it is such a short set.                 *)
(*                                                                         *)
(* Without `FenceEveryChange` a planned move and a cancel name a leader    *)
(* without the fence: the cut-over waits for the drained leader's whole    *)
(* log, and a cancel hands the shard back to the leader that had it. A     *)
(* failover that names the move's destination is opened the same way,      *)
(* because its broker cannot tell it from the cut-over it expected. With   *)
(* `PromoteDestination` placement may name it, and TLC finds the           *)
(* destination opening without a record the old leader acknowledged after *)
(* its last report (AckedHeldByLeader). Without it a `Quorum` failover     *)
(* ends the move instead. With `FenceEveryChange` each of these fences and *)
(* catches up before it serves, as a promotion does, which is what the     *)
(* broker does once every replica offers the fence.                        *)
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
(*                                                                         *)
(* `Elections` lets a replica stand for leader with no control-plane step: *)
(* `Elect` takes the generation after the one it promised, persists its    *)
(* own promise, and fences the set it last knew, up to `Elections`         *)
(* generations past the control plane's. Two replicas can then pick the    *)
(* same generation, which the control plane never does. With `Ballots` a   *)
(* promise names the leader it was made to, and a replica answers a fence  *)
(* or a batch at a generation it already promised only from that leader   *)
(* (`DiskLog::accept_generation`). Without it, as replicas answered before *)
(* ballots, TLC finds two leaders opening at one generation               *)
(* (OneLeaderPerGeneration). A candidate fences the set it last knew. With *)
(* `SetCheck = FALSE` that is the set a follower started in, and with      *)
(* replacements TLC finds a candidate opening on the old set without a     *)
(* record the new one acknowledged (FelixShardElectStaleSet.cfg).          *)
(*                                                                         *)
(* `SetCheck` is the design for electing without the control plane         *)
(* (docs/replication-design.md, "Electing without the control plane").    *)
(* Each replica keeps the set it last learned and the generation of the    *)
(* assignment that wrote it (`setGen`). A batch and a fence carry the      *)
(* sender's pair; a replica holding a newer one refuses it, and otherwise  *)
(* adopts it. A refused candidate learns the newer set and stops; it may  *)
(* stand again at a later generation (RefuseStaleSet), so a generation is *)
(* fenced on one set only. A leader or candidate that answers a newer      *)
(* fence or batch stops leading. Only a member of its own set stands. A  *)
(* set change fences too, on the new set, and with `JointFence` on a       *)
(* majority of the old set: a move's cut-over can change several members  *)
(* at once (`Swap`), and without it TLC finds a record lost               *)
(* (FelixShardElectSwapNoJoint.cfg). `PreVote` adds pre-vote and          *)
(* stickiness, which liveness wants and safety must not need.             *)
(*                                                                         *)
(* `Adopt` lets the control plane take a leader it never named: a replica  *)
(* that opened above the control plane's generation, on the set the       *)
(* control plane records, becomes its leader (CpAdopt), up to generation   *)
(* `Adopt`. One on another set only moves the control plane's generation   *)
(* up, so it never issues a generation below one it heard was elected.    *)
(* That leaves the control plane naming its old leader at a generation it  *)
(* cannot serve. With `Renames` the control plane names it again one       *)
(* generation up, on its own set, and the leader fences there (CpRename).  *)
(* It does the same for a leader still fencing at its generation once too  *)
(* many of its set promised a candidate for that fence to win.             *)
(*                                                                         *)
(* Elections also break the cut-over and the hand-back. The control plane  *)
(* names them at a generation it picked without knowing of an election,    *)
(* and opened without the fence the leader it names is a second leader at  *)
(* a generation a candidate already opened (OneLeaderPerGeneration,        *)
(* FelixShardElectHandoffUnfenced.cfg). `FenceEveryChange` fences them,    *)
(* and the ballot refuses the second leader (FelixShardElectHandoff.cfg).  *)
(*                                                                         *)
(* `Suspicions` lets placement promote without waiting for the lease to    *)
(* lapse, as it does once a majority of the set says it cannot reach the   *)
(* leader, at generations up to `Suspicions`. A detector can be wrong, so  *)
(* the model promotes on any read, the old leader alive and renewing. With *)
(* follower acks and the fence that loses nothing                          *)
(* (FelixShardSuspicion.cfg). With the lease and the report instead, TLC   *)
(* finds the old leader still serving on its lease beside the new one      *)
(* (AtMostOneServing, FelixShardSuspicionLease.cfg), which is why          *)
(* placement acts on it only once the fleet acknowledges by its followers. *)
(*                                                                         *)
(* `Wipes` lets a follower lose its disk and come back empty, up to       *)
(* `Wipes` times. Counted by its answers (`ReportFromAnswers`), it stays  *)
(* in the leader's reports until it answers again, which on an idle shard *)
(* is never. With `StorageIds` each data directory has an identity a      *)
(* wipe changes (`sid`); the leader learns it in the peer handshake,       *)
(* starts the follower's count over when it changes (`Probe`), and its    *)
(* report says which identity each follower was counted under. Its mark   *)
(* counts a follower only under the identity it has now, so one wiped and *)
(* not yet probed releases nothing. Placement promotes a follower only    *)
(* under the identity it has now. Without that TLC finds the wiped        *)
(* follower promoted without a record the old leader acknowledged         *)
(* (AckedSurvive, FelixShardWipeNoIds.cfg).                               *)
(*                                                                         *)
(* Placement compares with the identity a follower last registered, not   *)
(* the one it has (`registered`). A wiped follower registers again        *)
(* (`Register`), and under `StripIds` the registration carries no         *)
(* identity, as through a control plane that dropped it. Such a follower  *)
(* is unverified; with `StampIncarnations` it counts only from a report   *)
(* naming its current incarnation (`incs`). Without that TLC finds it     *)
(* promoted on a report from before the wipe                              *)
(* (AckedSurvive, FelixShardWipeStrippedNoStamp.cfg). The leader learns   *)
(* each follower's incarnation in the handshake beside its identity       *)
(* (`seenInc`) and reports that. Stamped by the control plane as the      *)
(* report arrives instead (`StampOnArrival`), a leader still on the       *)
(* follower's old connection reports it under its old identity after it   *)
(* registered again, and the stamp counts it                              *)
(* (FelixShardWipeStrippedOnArrival.cfg).                                 *)
(*                                                                         *)
(* With `CrashLoses` the loss is a crash under `FsyncMode::Periodic` or    *)
(* `None` instead (`Crash`): the follower keeps its data directory but     *)
(* loses an unsynced suffix of its log. Keeping its identity, it stays     *)
(* counted on what it lost, and TLC finds it promoted without an           *)
(* acknowledged record (AckedSurvive, FelixShardCrashKeepsId.cfg). With    *)
(* `RotateOnCrash` it restarts under a new identity, as the broker does    *)
(* when it finds no clean stop recorded. On the report path that case is   *)
(* too large for the CI job with a partial suffix lost; on the fenced path *)
(* start records give the logs a second record, so a crash there can lose  *)
(* part of a log with one write (FelixShardFencedCrash.cfg).               *)
(*                                                                         *)
(* Wipes and crashes are also checked on the fenced, follower-acked path   *)
(* the broker ships. There a promoted leader counts a replica's fence      *)
(* answer only from the copy the report its promotion was decided on       *)
(* named (`expect`, `FenceChecksIds`). Counting the copy that came back    *)
(* empty or short, TLC finds the new leader opening on it without a record *)
(* the old one acknowledged (AckedHeldByLeader,                            *)
(* FelixShardFencedCrashCountsRotated.cfg).                                *)
(*                                                                         *)
(* With `ProcessCrashes` a crash may be the process's alone, which keeps   *)
(* the page cache and so the whole log; only a machine crash loses a       *)
(* suffix. With `KeepIdOnSameBoot` the identity rotates only after a       *)
(* machine crash, as the broker does by comparing boot ids (#1367): a      *)
(* process crash keeps it and passes                                       *)
(* (FelixShardProcessCrashKeepsId.cfg, FelixShardLeaderProcessCrash.cfg).  *)
(* With `BootCheckBlind` a machine crash is taken for a process crash and  *)
(* keeps its identity too, and TLC finds a record lost (AckedSurvive,      *)
(* FelixShardMachineCrashKeepsId.cfg). With `CleanStops` a follower back  *)
(* from a kept process crash later stops cleanly and the power goes while *)
(* it is down; without `SyncOnKeep` its clean stop vouched for pages only  *)
(* the kernel held and TLC finds a record lost (AckedSurvive,              *)
(* FelixShardCleanStopUnsynced.cfg).                                       *)
(*                                                                         *)
(* `CrashLeader` makes the crash the leader's instead (#1295). Keeping its *)
(* lease and its generation, it serves on without what it lost, and TLC    *)
(* finds an acknowledged record gone (AckedSurvive,                        *)
(* FelixShardLeaderCrashKeepsGen.cfg). With `GiveUpOnCrash` it restarts    *)
(* under a new identity and leads nothing from that copy, its report       *)
(* names the identity it reported from, and placement promotes a follower  *)
(* the report names caught up once the two differ, without waiting for the *)
(* lease (FelixShardLeaderCrashGivesUp.cfg). With `Reclaims` the leader    *)
(* measures every follower, placement promotes one that is furthest and    *)
(* not behind it, and names it again at the next generation only when      *)
(* every follower is behind (FelixShardLeaderCrashReclaim.cfg); without    *)
(* measuring the followers TLC finds the record gone                       *)
(* (FelixShardLeaderCrashReclaimBlind.cfg).                                *)
(*                                                                         *)
(* `LosePromises` lets a follower lose its data directory once on the      *)
(* fenced path, and with it the generation and ballot it promised          *)
(* (`Forget`, #1267). Its log stays: a deposed leader would copy its own   *)
(* back into the empty directory anyway, and what a wipe does to the       *)
(* records is FelixShardWipe.cfg's subject. Answering as if it had never   *)
(* promised, the follower takes a deposed leader's batch, and TLC finds a  *)
(* record acknowledged on the two of them that the new leader does not     *)
(* hold (AckedHeldByLeader, FelixShardSuspicionWipeUnbacked.cfg). With     *)
(* `PromiseFromRoutes` it answers only what its routing view backs, as the *)
(* broker does for a copy with no promise on disk                          *)
(* (FelixShardSuspicionWipeRoutes.cfg).                                    *)
(*                                                                         *)
(* With `Elections` the wipe also takes the replica's set, and a view      *)
(* cannot back a promise made to a candidate the control plane never       *)
(* heard of (#1331). The adopted rule is `WipedNonVoter`: the wiped        *)
(* replica does not vote until a re-admission it has caught up on.         *)
(* `WipedWaitsForAdopt` and the knobs beside it model guards that fail.    *)
(* FelixShardElectWipe*.cfg, and docs/formal/README.md, "A wiped voter in  *)
(* an election".                                                           *)
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
    CounterCatchUp, \* whether the fence takes the counter log furthest ahead too
    Ballots,        \* whether a promise names its leader, and refuses another at its generation
    Elections,      \* how many generations past the control plane's a replica may elect itself to
    FenceEveryChange, \* whether a cut-over, a hand-back and a promoted destination fence too
    Suspicions,     \* up to which generation placement may promote on the followers' word, lapsed or not
    SetCheck,       \* whether a fence and a batch carry the set's generation, and a newer one refuses them
    JointFence,     \* whether a set change also fences a majority of the old set
    Adopt,          \* up to which generation replicas elect and the control plane adopts them
    AdoptMatch,     \* whether the control plane adopts a leader only on the set it records
    Swaps,          \* how many cut-overs may change several members of the set at once
    PreVote,        \* whether a candidate needs a majority not hearing a leader, and a replica that hears one refuses it
    Wipes,          \* how many times a follower may lose its disk; bounds the state space
    StorageIds      \* whether the leader learns each follower's storage identity and placement checks it

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
\* Spares are allowed only where they join through a cut-over (`Swap`). The
\* cut-over waits for its destination to hold the old leader's counter log;
\* a joining spare gets the counter log only through `ShipCounter`.
ASSUME Counters => /\ AckByFollowers /\ ~LabelOnReceipt /\ ~Resends
                   /\ ~Handoff /\ ~Cancel /\ ~StageMove /\ MaxMoves = 0
                   /\ (Spares = {} \/ (Swaps > 0 /\ ~ReplaceOnPromote /\ ~Grow))
ASSUME Eps < L /\ Margin >= 0
\* Self-election is checked on the fenced, follower-acked path alone. A
\* staged copy is left out to keep the state space small.
ASSUME Ballots \in BOOLEAN /\ Elections \in Nat
ASSUME Elections > 0 => /\ AckByFollowers /\ FenceOnPromote /\ ~Counters /\ ~StageMove
ASSUME FenceEveryChange \in BOOLEAN /\ (FenceEveryChange => FenceOnPromote)
ASSUME Suspicions \in Nat
\* The set check and adoption exist for self-election.
ASSUME SetCheck \in BOOLEAN /\ JointFence \in BOOLEAN /\ (JointFence => SetCheck)
ASSUME Adopt \in Nat /\ (Adopt > 0 => Elections > 0) /\ AdoptMatch \in BOOLEAN
ASSUME PreVote \in BOOLEAN /\ (PreVote => Elections > 0)
\* A cut-over that changes the set is checked with the set check and spares.
ASSUME Swaps \in Nat /\ (Swaps > 0 => SetCheck /\ Spares /= {} /\ FenceEveryChange /\ MaxMoves = 0 /\ StartRecord)
\* Acknowledgements are recorded by generation where opens are tracked.
ASSUME (Elections > 0 \/ Suspicions > 0) => Quorum /\ ~Resends
\* A wipe is checked against the report counted from answers, with no other
\* set changes, and on the fenced path with promotion the only fence; the
\* short-copy refusal below is the exception to the first two.
ASSUME Wipes \in Nat /\ StorageIds \in BOOLEAN
ASSUME Wipes > 0 => /\ ~Counters /\ ~Handoff /\ ~Cancel
                   /\ Elections = 0 /\ Swaps = 0 /\ ~SetCheck

\* How placement reads a registration, as definitions rather than constants so
\* the configurations that leave them alone need not name them. One that
\* checks another sets it with `StripIds <- Yes` under CONSTANTS.
\* `StripIds`: a wiped follower registers again in a step of its own and
\* without an identity, as through a control plane that dropped it. Without
\* it the wipe registers the new identity at once.
\* `StampIncarnations`: placement counts a follower registered without an
\* identity only from a report naming its current incarnation.
\* `StampOnArrival`: the report's incarnations are the ones the control
\* plane reads as it takes the report, not the ones the leader measured on
\* its connections. The code does this only for a follower that predates the
\* handshake's incarnation.
\* `CrashLoses`: a follower loses data by crashing under a relaxed fsync
\* mode, which drops an unsynced suffix of its log and keeps its directory,
\* rather than by a wipe.
\* `RotateOnCrash`: after such a crash it starts under a new storage
\* identity: `felix_storage::run_state::begin`.
\* `ProcessCrashes`: a crash may be the process's alone, which keeps the
\* page cache and loses nothing; otherwise every crash is the machine's.
\* `KeepIdOnSameBoot`: under `RotateOnCrash`, only a machine crash rotates
\* the identity, and a leader gives its copy up only when it rotated
\* (`run_state::begin` compares the boot id it recorded with this boot's).
\* `BootCheckBlind`: the boot check takes a machine crash for a process
\* crash.
\* `CleanStops`: a follower back from a process crash later stops cleanly,
\* and the power may go while it is down (`CleanStop`).
\* `SyncOnKeep`: the start that kept the identity after the process crash
\* synced the filesystem first (`run_state::begin`, `synced_after_crash`).
\* `CrashLeader`: the crash is a leader's, not a follower's.
\* `GiveUpOnCrash`: a leader back from such a crash leads nothing from the
\* copy it led (`may_lead` in services/felix-broker-service/src/shards/
\* lifecycle.rs), and placement replaces it once its registered identity is
\* not the one its report names (`leader_replaced` in
\* services/felix-controlplane-service/src/cluster/placement/plan.rs).
\* `Reclaims`: placement promotes a follower the given-up leader's probe
\* finds not behind it (ByProbe), and names that leader again at the next
\* generation once every follower is behind it (Reclaim).
\* `ReclaimBlind`: it reclaims without measuring the followers.
\* `FenceChecksIds`: where wipes or crashes are modelled on the fenced path
\* (`FencedIds`), a promoted leader counts a fence answer only from the
\* identity the report its promotion was decided on named (`expect`), as
\* `fence_shard` in crates/server/felix-replication/src/promotion.rs does with
\* the identities the assignment carries.
\* `UnnamedCounts`: it also counts a member the report named no identity
\* for. The code names such a member with an empty identity, which nothing
\* matches, unless it is registered without one.
\* `NameByRegistration`: placement names such a replica by the identity it
\* is registered under, while it is at the incarnation the report was taken
\* at. The code does not: for a follower its leader had not asked yet, that
\* incarnation is the control plane's stamp as the report arrives, which can
\* follow a crash the report predates
\* (FelixShardFencedCrashNamesByRegistration.cfg).
\* `NameByInheritance`: for a replica the report names no identity for, its
\* leader's report also carries the identity that leader's own promotion
\* named it by (`inh`), and placement names the replica by that (#1574):
\* `inherited_storage` in crates/server/felix-replication/src/reporter.rs,
\* `report_storage_ids` in placement/replica_positions.rs. The leader has
\* not reached that replica at its generation, so every acknowledgement the
\* replica gave went to an earlier leader from that copy or one before it,
\* and identities never repeat.
Yes == TRUE
No == FALSE
Zero == 0
One == 1
Two == 2
StripIds == FALSE
StampIncarnations == TRUE
StampOnArrival == FALSE
CrashLoses == FALSE
RotateOnCrash == FALSE
ProcessCrashes == FALSE
KeepIdOnSameBoot == FALSE
BootCheckBlind == FALSE
CleanStops == FALSE
SyncOnKeep == FALSE
CrashLeader == FALSE
GiveUpOnCrash == FALSE
Reclaims == FALSE
ReclaimBlind == FALSE
\* `Renames`: under `Adopt`, the control plane names its leader again at the
\* next generation when that leader reports it cannot serve the recorded one:
\* an election the control plane recorded without adopting it moved past it,
\* its fence there cannot win a majority, or a replica promised it away
\* while the fence is still pending (CpRename).
\* `RenameUnfenced`: the renamed leader opens at once instead of fencing.
Renames == FALSE
RenameUnfenced == FALSE
\* `SetLossUpTo`: up to that generation the leader may lose the set its
\* generation leaves (`ForgetSet`, #1872): it learned the shard from a
\* control-plane snapshot, or restarted after it opened. `{}` in `oldSet`
\* stands for that unknown set: no majority of it is ever held, so the fence
\* cannot open. The broker then reports its generation refused with no node
\* named (`set_unknown` in promotion/joint.rs) and the control plane names it
\* again one up (`CpRename`), only when the broker already opened that
\* generation, so its fence there was taken. `UnknownRenamesAny` renames
\* whenever the set is unknown, the unsafe rule.
SetLossUpTo == 0
UnknownRenamesAny == FALSE
\* `SetLossAfterOpen`: the set is lost only where the broker can report it
\* (`UnknownRefusal`): by a leader that opened at its generation, named on a
\* set written there, with a change pending from an earlier one. Elsewhere
\* it has no safe way out (#1872), or no set being left to lose, so no
\* liveness is owed it.
SetLossAfterOpen == FALSE
\* `FirstAssignMark`: a broker that learned the shard from a snapshot with no
\* set kept reads the set before as empty, not unknown, when the snapshot
\* item carries the first-assignment mark. The control plane sets it only
\* when its change log holds the shard's `assigned` change at that same
\* generation, so only at the model's first generation. `MarkAnyGeneration`
\* trusts the mark at any generation, the unsafe rule.
FirstAssignMark == FALSE
MarkAnyGeneration == FALSE
\* `KeepsUnopened` (#1878): a failover also fences the set the promoted
\* broker was still leaving, unless its log holds a record of its own set's
\* generation, so that fence opened (`unopened_leaving` in
\* promotion/joint.rs). Off, a failover replaces it. `OneLeavingSet`: a
\* member answering a fence keeps only one of the sets it leaves, one it
\* votes in (`leaving_for`), as the broker's kept set has one `leaving`.
KeepsUnopened == FALSE
OneLeavingSet == FALSE
\* `LogDropsSets` (#1881): a member keeps the sets it leaves as the broker's
\* log does (`LearnSetFrom`), not as the leader last sent them.
\* `BatchDropsSets`: a batch that raises the set keeps none, the rule before
\* review. `KeptSetsCap`: how many earlier sets the set file holds.
LogDropsSets == FALSE
BatchDropsSets == FALSE
KeptSetsCap == 16
\* `RouteLags`: a promoted broker's routes land after the ballot its
\* promotion took (the lifecycle persists the ballot first), so a candidate
\* round there that read the route naming the old leader may still stand
\* (StandOnStaleRoute, #1933). `NamedHereWaits`: that round does not stand
\* while its copy's ballot is past its route and names this broker
\* (`named_here` in crates/server/felix-replication/src/election/candidate.rs).
\* The code holds that for 30 s (`NAMED_HERE_FOR`); the model holds it for
\* good, taking the routes to land within that.
\* `OwnBallotReported`: an open leader whose own ballot is past its
\* generation reports the generation refused, to be renamed one up
\* (`ballot_above` in crates/server/felix-replication/src/driver/shard.rs).
RouteLags == FALSE
NamedHereWaits == TRUE
OwnBallotReported == TRUE
\* `StaleFenceAnswered`: under `RouteLags`, a member answers the stale
\* round's fence before the round gives up, promising it the generation
\* above (#1940). `OpenChecksOwnBallot`: a leader the control plane named
\* opens only once its own copy takes that generation's ballot, which a newer
\* promise there refuses (`keep_own_set` in
\* crates/server/felix-replication/src/promotion.rs). `OwnBallotFenceRefused`:
\* a leader still fencing below its own copy's ballot reports its generation
\* refused (`fence_shard_as`, same file). All off by default, so the
\* configurations from before keep their state counts.
StaleFenceAnswered == FALSE
OpenChecksOwnBallot == FALSE
OwnBallotFenceRefused == FALSE
\* `ReportStore`: a refusal reaches the control plane in a report, and the
\* rename reads the report the control plane stored rather than the
\* broker's state. The store keeps one report per shard, and at the same
\* generation a later one replaces it (`ReplicaReport::supersedes` in
\* services/felix-controlplane-service/src/model/shard.rs), so a pass report
\* landing after the refusal hides it (#1969). `RefusalRepeats`: the
\* reporter repeats a refusal on every later report at its generation
\* (`Refusals` in crates/server/felix-replication/src/reporter.rs, #1971).
\* Off by default, so the configurations from before keep their state counts.
ReportStore == FALSE
RefusalRepeats == TRUE
FenceChecksIds == TRUE
UnnamedCounts == FALSE
NameByRegistration == FALSE
NameByInheritance == FALSE
\* `LosePromises`: a follower loses its data directory once, with what it
\* promised (`Forget`).
\* `PromiseFromRoutes`: a follower with no promise on disk answers only what
\* its routing view backs: `refuse_unbacked` and `check_role` in
\* crates/server/felix-replication/src/replica.rs.
LosePromises == FALSE
PromiseFromRoutes == FALSE
\* `WipedWaitsForAdopt`: under `Elections`, a follower that lost its
\* promises answers no candidate's fence, and does not stand, until the
\* control plane's generation, read linearizably (`gen`, not a planner's
\* view), is past the one it had when the follower lost them (#1331).
\* `WipedTakesBatches`: while it waits it still takes batches, a bootstrap
\* and a fetch from a leader the control plane has not adopted.
\* `WipedTakesNamed`: while it waits it still takes a fence and batches from
\* the leader the control plane names, which an election it has not heard
\* of may have deposed.
\* `WipedLearnsSet`: while it waits it learns the set a candidate's fence
\* carries, promising nothing.
\* `WipedAboveAdopted`: once the wait ends, until it holds a promise at the
\* control plane's generation, read the same way, it takes nothing below
\* that generation and only the named leader at it.
WipedWaitsForAdopt == FALSE
WipedTakesBatches == TRUE
WipedTakesNamed == TRUE
WipedLearnsSet == FALSE
WipedAboveAdopted == FALSE
\* `WipeBoundOracle`: the wait's bound is the newest generation the follower
\* had promised, not the control plane's. Nothing in a broker can know it;
\* it isolates what the other guards do from what the bound does.
WipeBoundOracle == FALSE
\* `WipedNonVoter`: a wiped replica is a new member that does not vote. It
\* answers no fence and takes no batch until a set change re-admits it
\* (`Readmit`), whose joint fence counts the old set's majority without it,
\* and it votes again only once it holds that leader's log through the
\* start record there (`Readmitted`). Until then it may learn a set, say
\* yes to a pre-vote and copy a leader's log (`CatchUp`), none of which
\* counts. The adopted rule of #1331.
\* `ReadmitCountsWiped`: the re-admission's old majority counts it too.
\* `ReadmitBeforeCatchUp`: it votes again once the leader opens, caught up
\* or not.
\* `PromoteNonVoter`: control-plane failover may promote it.
\* `WipeLeads`: a leader or candidate can be wiped, not only a follower.
\* `Forgets`: how many wipes, of one broker or two. `WipeLeads` with
\* `Forgets > 1` across distinct brokers is outside the failure budget: two
\* of three lost can lose an acknowledged record, which is not a defect. Such
\* a run sets `OneNonVoter`.
WipedNonVoter == FALSE
ReadmitCountsWiped == FALSE
ReadmitBeforeCatchUp == FALSE
PromoteNonVoter == FALSE
\* `WriteDropsLearners`: a placement write may name no learners, as one does
\* when its read of the assignment it replaces fails and is taken for no
\* assignment, or when it comes from a control plane that predates learners
\* (a mixed fleet on Postgres, which has no metadata level to stop it).
WriteDropsLearners == FALSE
WipeLeads == FALSE
Forgets == 1
\* `OneNonVoter`: a wipe is refused while it would leave two brokers non-voters.
OneNonVoter == FALSE
\* `Creates`: the shard starts as it is created, its first leader fencing at
\* generation 1 and no follower holding a promise or a set. A follower that
\* was not wiped votes there with an empty log: it holds every acknowledged
\* record, which is none. A wipe is still a wipe, whatever the generation.
\* `CreateCountsEmpty`: a copy with no set file votes, wiped or not, as if
\* an empty set could only mean a new shard. The wipe takes the set file too,
\* so only evidence of the wipe tells the two apart.
Creates == FALSE
CreateCountsEmpty == FALSE
\* `ReplacedIdLearner`: a broker the control plane last registered under
\* another storage id is a learner for every shard it held before, the
\* code's fallback when no set it learns yet names its old id. Every broker
\* here was registered before its wipe; a broker new to the control plane
\* held nothing and votes. Without it the only evidence is a set naming
\* another id for it (`NamesOther`), and a copy that first hears a set
\* written after its wipe votes with no promise and an empty log.
ReplacedIdLearner == TRUE
\* `SetlessFences`: the control plane's leader may send a member its fence
\* without a set, as `fence_log` in promotion.rs does to a replica whose
\* capabilities it could not learn. Such a fence is checked against no set,
\* so the copy's own mark is all that can refuse it.
\* `SetlessSeesMarker`: the copy refuses it on the replaced-storage marker
\* under its data root, which is the learner mark of a copy that holds no
\* set yet (`ReplacedIdLearner`). Off, only a mark kept in a set file
\* refuses it, and a copy wiped before it heard any set answers with an
\* empty log.
\* `CounterFenceSeesLearner`: a cache's counter fence, which carries no set,
\* is refused by a non-voter, judged on the cache log as
\* `refuse_non_voter_fence` does. Off, the counter log, which keeps no set,
\* decides, and a non-voter's empty counter log counts.
\* `ReadmitChecksCounters`: re-admission also waits for the counter log to
\* hold the start record of the set's generation.
\* `LearnerAcksCount`: the leader counts a non-voter's answers to its catch-up
\* batches toward its marks. The code's leader leaves them out (#1762): with
\* elections a learner's wipe took a promise to a candidate (twin
\* `FelixShardElectWipeNonVoterCreatesLearnerAcks.cfg`). Without elections
\* counting them is safe, and the model keeps it there because it only adds
\* acknowledgements the code would not make.
\* `CatchUpFromDeposed`: a non-voter takes a catch-up batch from a leader the
\* control plane named at that leader's generation after it moved on, as
\* the code's confirmed read, reused for 10 s or answered just before a
\* failover, lets it. The cache is in memory, so its reads came after the
\* wipe: at the generation then or a later one. Only without elections,
\* where every leader that opened was named.
\* `GroupLogs`: the second log stands for a stream shard's consumer-group
\* logs rather than a cache's counter log. They carry no start record, so
\* re-admission does not wait for them. The copy keeps its wiped mark
\* (`marked`) past re-admission instead, until its group log holds a record
\* written at its set's generation or later (`GroupsCaughtUp`). While marked
\* it answers no group fence, and as a leader its own group log does not
\* count toward the group fence's majority: `refuse_non_voter_fence`,
\* `admit_when_caught_up` and `fence_groups` in
\* crates/server/felix-replication/src.
\* `KeepSetDropsMark`: a promoted leader keeping its route's set once the
\* records' fence has its majority drops its own mark, as `keep_own_set` did
\* when it wrote the set without it (`KeepOwnSet`). A group take that fails
\* after that is retried reading the dropped mark.
\* `IdlessCreation`: the shard's generation-1 set names no storage ids, as a
\* shard created before sets named member ids keeps until a placement writes
\* another, so it is no evidence of a wipe.
\* `GenOneExempt`: a copy that holds no set and learns a generation-1 set
\* naming no other id for it votes, as `replaced_copy_of` let it whatever
\* that set named. Fixed, only a creation set naming its new id exempts it,
\* and no set written before the wipe does.
SetlessFences == FALSE
SetlessSeesMarker == TRUE
CounterFenceSeesLearner == TRUE
\* `CountersJoint`: under `JointFence`, while a set change is pending the
\* counter mark needs a majority of the old set too, as the cache log's does
\* (`counter_level` in crates/server/felix-replication/src/driver/shard.rs).
\* `FlipChecksCounters`: the control plane takes the new set only once the
\* counter log's start record is held as well (`CounterLevel::start_held`,
\* read by `joint_start`).
CountersJoint == TRUE
FlipChecksCounters == TRUE
GroupLogs == FALSE
KeepSetDropsMark == FALSE
IdlessCreation == FALSE
GenOneExempt == FALSE
ReadmitChecksCounters == ~GroupLogs
LearnerAcksCount == Elections = 0
CatchUpFromDeposed == TRUE
\* `RefuseShortCopy`: a leader that gave its copy up in a crash answers no
\* promotion fence from it until a newer leader has shipped to it, unless a
\* set of two acknowledged what it may have lost (`AckedByTwo`):
\* `refuse_short_copy` and `acknowledged_by_two` in
\* crates/server/felix-replication/src/replica.rs.
\* `ShortCopyChecksSet`: that exception needs the fencer's set to be the two,
\* with the fence ids naming exactly them. Without it, only the generation
\* and the last record are checked.
\* `ShortCopyChecksKeys`: of that, the fence ids naming exactly the two.
\* `FenceIdsSetOnly`: a failover's fence ids leave out the old set's staged
\* copy, the fix #1635 proposed. Unsafe beside a leader that counts that copy
\* (FelixShardShortCopyStagedVotesSetOnly.cfg).
RefuseShortCopy == FALSE
ShortCopyChecksSet == TRUE
ShortCopyChecksKeys == TRUE
FenceIdsSetOnly == FALSE
\* `FlipNewGeneration`: the control plane takes a pending set at a new
\* generation naming its leader again, as `TakePendingSet` in placement/
\* moves.rs writes it, and that leader fences again on the new set alone. It
\* flips on the leader's report of its start record. Off, the flip keeps the
\* generation, which the set-change pieces were measured with.
\* `NamedFenceSkipsBallot`: a replica answers the fence of the leader a flip
\* named at a generation it promised a candidate, checking the generation
\* alone. `accept_sender` in replica.rs checks the ballot for
\* every fence.
FlipNewGeneration == FALSE
NamedFenceSkipsBallot == FALSE
\* A continuing open: the leader that served `g - 1`, in the same process and
\* not draining, fences `g` (`RegenerateSet`, `Readmit`). It holds every
\* acknowledged record, so it neither checks a copy's identity before
\* counting its answer nor takes an answer's log.
\* `ContinuingTrustsBallot`: a copy the short-copy or non-voter refusal would
\* stop answers such a fence when its durable ballot names that leader at
\* `g - 1`. It promises; it stays doubted.
\* `ContinuingTrustsAnyCopy`: it answers whatever its ballot, which counts a
\* promise a wipe may have broken (FelixShardElectUncleanContinuingAnyCopy.cfg).
\* `ContinuingKeptBallot`: with `ContinuingTrustsBallot`, only a copy that
\* never lost its promises (`forgot`) is trusted. A ballot taken after a wipe,
\* from a batch, is not the promise the wipe lost
\* (FelixShardElectUncleanContinuingBatchBallot.cfg).
\* The leader counts an answer from the copy it expects, or one that attests
\* the trusted ballot (`FenceKeptOk`). A copy that lost its promises is
\* never the copy it expects: in the code its storage id rotated.
\* `ContinuingForgottenAnswers`: a copy that lost its promises answers a
\* continuing fence past the non-voter refusal, as a restored directory that
\* nothing marks does. Its answer must not count.
\* `ContinuingCountsForgotten`: the leader counts that answer as from the copy
\* it expects, which TLC finds unsafe
\* (FelixShardElectUncleanContinuingCountsForgotten.cfg).
ContinuingTrustsBallot == FALSE
ContinuingTrustsAnyCopy == FALSE
ContinuingKeptBallot == FALSE
ContinuingForgottenAnswers == FALSE
ContinuingCountsForgotten == FALSE
\* A continuing fence round starts fresh: a lost answer or a Pending round
\* drops every answer and the next round asks again. By then a replica that
\* answered the first round holds the ballot at the fence's own generation,
\* so the attestation also takes a ballot naming the leader at `g`.
\* `ContinuingRetries`: the leader retries its fence once after the first
\* answers came in (`RetryContinuingFence`).
\* `ContinuingCopiesNew`: every follower answers from a new copy, so the
\* leader counts only attested answers, as in the code after an unclean
\* restart of every follower.
\* `ContinuingAttestsBelowOnly`: the attestation takes only `g - 1`. A retry
\* then never completes (FelixShardElectUncleanContinuingRetryBelowOnly.cfg).
ContinuingRetries == FALSE
ContinuingCopiesNew == FALSE
ContinuingAttestsBelowOnly == FALSE
\* Liveness of elections (`ElectionSettles`, checked under `LiveSpec`).
\* `CpDown`: the control plane takes no step. `FailStops`: how many brokers
\* may stop for good, as `halted` ones. `PreVoteAsked`: the pre-vote counts
\* a yes only from a live member that leads nothing open, hears no leader and
\* holds no log ahead of the asker's, as `pre_vote` in felix-replication's
\* replica/election.rs does (#1815). Off, any live member that hears no
\* leader says yes, the leader itself included. The code's refusals on a stale generation or set are
\* not modelled. `Backoff`: a candidate whose round failed gives it up
\* (`GiveUpRound`), and at most `Splits` times in a whole behaviour does a
\* candidate stand while another's round is open. That is the randomised
\* backoff, which separates candidates within a few rounds; the model cannot
\* say how few, so it takes a bound below the generations `Elections`
\* allows.
CpDown == FALSE
\* `CpStops`: the control plane runs, then stops for good at some point
\* (`CpStop`), and the brokers carry on with what it last told them.
\* `BrokerSuspicion`: a replica stands only when the broker of the leader it
\* follows is down, as felix-replication's suspicion.rs pings the leader's
\* broker, not its shards. `ShardSuspicion` adds the per-shard trigger: it
\* also stands when that shard's leader makes no progress, which with writes
\* always pending is a leader it does not hear serving (`Hears`).
\* `SelfSuspicion`: under `ShardSuspicion`, a broker named the shard's leader
\* that holds no generation for it (its open refused for its own newer
\* ballot) stands too. The code never stands for a shard
\* whose route names its own node (`follows` in election/candidate.rs).
\* `PreVoteBounded`: under `StandBehindBallot`, a broker still fencing
\* refuses a pre-vote only while its fence can still complete; a refusal
\* that outlasts its bound is one whose fence cannot.
CpStops == FALSE
BrokerSuspicion == FALSE
ShardSuspicion == FALSE
SelfSuspicion == FALSE
PreVoteBounded == FALSE
\* `WithdrawsStuck`: under `Withdraws`, a candidate gives its standing up only
\* once its fence cannot complete. That assumes the stand timeout outlasts a
\* fence that can; without it a scheduler that always times the candidate out
\* first elects forever, which no bound on elections can show ending.
WithdrawsStuck == FALSE
\* `Withdraws`: a candidate still fencing may give its standing up at any
\* time, as one that has not opened within the stand timeout does
\* (`OPEN_WITHIN` in felix-replication's election/candidate.rs). It keeps
\* the ballot it persisted, on itself and on every replica that answered. The
\* control plane's own leader that stood while it held its assignment goes
\* back to it, still fencing at the control plane's generation, or finds that
\* assignment's open refused (bgen 0): the open and the standing race.
\* `Restarts`: a broker stopped by `FailStop` comes back (`Restart`) with its
\* log, promises and ballots, each at most once. Placement promotes away from
\* a stopped leader and names only live brokers, and a restarted leader opens
\* its assignment again.
\* `SupersededRefused`: a leader whose own ballot holds a generation above the
\* control plane's reports its open refused naming no node (#1916:
\* `Superseded` in services/felix-broker-service/src/shards/lifecycle.rs).
\* Off, it stays silent. A member answering with a newer ballot ends the
\* code's fence refused at once (`superseded_by` in promotion.rs); the model
\* drops such a member from the fence's count only under `OverlayRefused`.
\* `OwnBallotRefused`: the fence's step on the leader's own log reports the
\* same refusal (`keep_own_set` in promotion.rs); off, it stays pending. `AboveReported`: the control
\* plane's leader that is not open at its generation and whose own log holds
\* a newer one it does not stand at (it answered a candidate's fence, or
\* stood and withdrew, after opening there) reports the generation refused.
\* Off, such a leader reports nothing, and only a new standing moves the
\* shard on. The code reports from an open leader (#1951) and from one that
\* released the shard to the candidate's elected route (`note_released` in
\* driver/shards.rs); answering the fence sets `bgen` to 0 here, which is that
\* release. `RenamePastBallot`: a refusal carries the
\* ballot the leader lost to and placement renames above it, not at the next
\* generation, which may be that ballot's (#1951). The leader then also no
\* longer reports a generation its own copy promised away (#1872's wait).
\* The rest follow the list of dead ends in #1914, each off as on main.
\* `OverlayRefused`: a member that took a candidate's fence above the
\* fence's generation answers "at generation X" from its in-memory election
\* (replica.rs `check_role`), which the fence parses as a refusal and counts
\* as promised away. Off, that answer leaves the fence pending.
\* `HaltReported`: an open leader whose follower refuses its batches for a
\* newer ballot reports the generation refused with that ballot (`fenced_by`
\* in driver/ship.rs, reported from driver/shards.rs), and placement renames
\* it. Off, nothing reports the halt.
\* `DeposeOnAssigned`: one follower's newer ballot no longer deposes the
\* generation (fence.rs `depose`); off, the leader stops serving reads.
\* `StandBehindBallot`: a replica that promised its generation to another
\* candidate still standing there does not stand above it (candidate.rs
\* `stand`), and a broker still fencing answers no pre-vote
\* (replica/election.rs `pre_vote`). A candidate that stopped, withdrew or
\* came back without its standing frees the promise. `StandBehindDeadOnly`
\* frees it only once the candidate is down. `StandBehindBounded`: the
\* hold lasts a bound with no progress on the shard; one that outlasts it is
\* one whose candidate's fence cannot complete.
\* `CatchAllRename`: placement renames a live leader that is not serving and
\* cannot get there from where it stands, whatever it reported.
OverlayRefused == FALSE
HaltReported == FALSE
DeposeOnAssigned == FALSE
StandBehindBallot == FALSE
StandBehindDeadOnly == FALSE
StandBehindBounded == FALSE
CatchAllRename == FALSE
Withdraws == FALSE
Restarts == FALSE
\* `PriorMayBeUnknown`: under `Restarts`, a restarted leader may find no set
\* before its generation in the snapshot it reseeds from: the control plane's
\* change log no longer holds the change before it (`previous` on a snapshot
\* item is read from the retained log, #2024). Its fence then waits unless its
\* log keeps the set at the generation before, as before #2024 (#1989).
PriorMayBeUnknown == FALSE
\* `Stops`: under `Restarts`, how many brokers may stop in a behaviour. The
\* default is no bound with three brokers.
Stops == 3
SupersededRefused == TRUE
OwnBallotRefused == FALSE
AboveReported == FALSE
RenamePastBallot == FALSE
\* The refusals of #2046, each off as before the fix.
\* `UnknownCarriesBallot`: a leader that lost the set it leaves reports the
\* highest ballot its sets hold (`RefusedGeneration::above`), and placement
\* renames above it. Off, the refusal carries none and the rename takes the
\* next generation, which may be one a withdrawn candidate holds.
\* `AheadRefused`: a fence one of whose members took a newer generation ends
\* refused with that ballot. Off, it stays pending.
\* `StandsAboveCp`: a candidate stands above both the control plane's
\* generation, as its route shows it, and its own promise. Off, above its
\* promise alone. `StaleStandRoute`: that route may be one generation old.
\* `MaxStandings`: the model's bound on standings in a behaviour, 0 for none.
\* It stands for the backoff that separates candidates.
UnknownCarriesBallot == FALSE
AheadRefused == FALSE
StandsAboveCp == FALSE
StaleStandRoute == FALSE
MaxStandings == 0
\* `JointMinorityRefused`: a joint fence reports a refusal on one promise
\* away with the rest silent, as a single-set fence does. `RenameDropsChange`:
\* a rename names the pending set alone, dropping the set being left.
\* `JointRenamesAbove`: with a change pending, placement still renames above
\* a carried ballot. Off, it renames one up, as it must below the Raft level
\* that records the skipped generation's set.
JointMinorityRefused == FALSE
RenameDropsChange == FALSE
JointRenamesAbove == TRUE
\* Copy pins (`fence_storage_ids`), under `CopyPins`. A failover's fence
\* counts each member only from the copy its report named. A copy may change
\* (`ChangeCopy`), losing what it had not synced, and a pin naming the old
\* one then keeps it from counting. Until placement admits it again
\* (`ReadmitCopy`, `CopyAdmitted`) a changed copy answers only the leader the
\* control plane names.
\* `FailoverStart`: the run starts with the leader still fencing the
\* assignment a failover wrote, its pins naming every copy.
\* `PinsKeptOnRename`: a rename keeps the pins; off, it drops them.
\* `PinsDropAtOpen`: the leader drops them at its first fenced open.
\* `PinsDropOnAdmit`: a copy's pin drops once placement admits it.
\* `OtherCopiesExcluded`: a fence with one promise away and the rest silent
\* reports no refusal when a member answered from a copy it does not count
\* (`explained` in promotion.rs).
CopyPins == FALSE
FailoverStart == FALSE
CopyChanges == 0
PinsKeptOnRename == FALSE
PinsDropAtOpen == FALSE
PinsDropOnAdmit == FALSE
OtherCopiesExcluded == TRUE
\* `Reissues`: under `Adopt`, when the control plane named one leader at the
\* generation another was elected and opened at, it moves the shard to the
\* next generation under the elected one (CpReissue).
Reissues == FALSE
\* `ReissuePending`: when the set the reissued leader was elected on is not
\* the control plane's, the reissue records it as the set being left, with
\* the control plane's pending (`reissue_past_collision`). Off, the control
\* plane keeps its set and the reissued leader alone fences the elected one
\* beside it (`logged_besides` in
\* crates/server/felix-replication/src/promotion/joint.rs).
ReissuePending == TRUE
\* `ReissueOwnOnOtherSet`: under `AdoptMatch`, the control plane's own leader
\* elected on a set other than the recorded one is reissued on its elected set
\* like any other member (`adopting_elected_set` in services/
\* felix-controlplane-service/src/model/shard.rs, #1929). Off, only its
\* generation is taken, and the record names a set a majority of which may
\* lack what the leader acknowledged (AdoptedMatchesOpened).
ReissueOwnOnOtherSet == FALSE
\* `StandsAboveKeptSet`: a candidate stands no lower than the generation of
\* the set it keeps, since the joint fence refuses a set newer than its
\* generation (`set_before` in promotion/joint.rs). Only a set learned from a
\* refusal (RefuseStaleSet), which the code never keeps, can be newer. Off
\* elsewhere, which only adds behaviours.
StandsAboveKeptSet == FALSE
\* `StandsOnPendingSet`: a replica standing while a change to its kept set is
\* pending, which the code refuses (`stand` in election/candidate.rs). On only
\* in the witnesses that show what the refusal protects: the control plane
\* then has a leader to name again that never took its generation
\* (FelixShardElectRenameDue.cfg, FelixShardElectRenameFencingDue.cfg).
StandsOnPendingSet == FALSE
\* `RefusalLearnsSet`: a candidate whose fence is refused for a stale set
\* keeps the newer set the refusal names. The code only stops standing
\* (promotion.rs keeps no set from a refusal); learning it adds behaviours.
RefusalLearnsSet == TRUE
\* `LearnsLeavingSet`: a follower learning a leader's set during a change
\* takes the leader's set being left, as a broker does from a fence, or from
\* a batch with the control plane's set being left (replica/election.rs). Off,
\* it keeps its own old set as the one being left, which loses acknowledged
\* writes (FelixShardElectReissueReplaceBatchSet.cfg).
LearnsLeavingSet == TRUE
\* `DetectorMisfires`: a candidate may suspect a leader it still hears, as a
\* failure detector can be wrong. Only its own yes is affected.
DetectorMisfires == FALSE
FailStops == 0
PreVoteAsked == FALSE
Backoff == FALSE
Splits == 0
\* `StintStarts`: a leader opening again at a generation it already started
\* (elected there, then named there by a cut-over) begins a new stint, and
\* the flip waits for that stint's start record, as `own_start` in
\* felix-replication's driver/shard.rs counts from the current stint's
\* start. Off, the flip takes the generation's first start, and records the
\* elected stint acknowledged after that start need not be on a majority of
\* the new set (#1787).
\* `Readmitted` and `CpAdmit` still accept any start record at the
\* generation, not the named stint's; the configurations with `StintStarts`
\* on have no wipes.
\* `JoinLags`: a copy joining in a cut-over holds any prefix of the
\* destination's log. A top-up copy (`choose_replicas` in placement/moves.rs)
\* is not checked for what it holds. Off, a joining copy holds the
\* destination's whole log at the cut-over.
StintStarts == FALSE
JoinLags == FALSE

\* `AckWaits`: a `Quorum` write's client waits for the acknowledgement, keyed
\* by the generation and offset the write landed at, and the wait follows its
\* broker into a newer generation (`await_quorum`). `AckByOffset`: the wait is
\* answered by the offset alone, as before #2048. Off, only the records a
\* leader's log holds are acknowledged.
AckWaits == FALSE
AckByOffset == FALSE

\* The sets a failover's fence, or a regeneration's, takes a majority of
\* besides the new one. A recorded-only adoption (CpAdopt) can leave the
\* control plane's set (`CpSet`) and the set in the promoted broker's log
\* (`own`, its `mine`) different, with disjoint majorities, so the fence
\* needs a majority of each. `FenceOwnSet` alone is the fence that reads
\* the log, `FenceRecordedSet` alone the one that reads the control plane.
\* The broker's side is `set_before` in promotion/joint.rs.
FenceOwnSet == TRUE
FenceRecordedSet == FALSE
ASSUME FenceOwnSet \/ FenceRecordedSet

\* There only the leader loses records, so a report may count followers by
\* their logs.
ASSUME Wipes > 0 /\ ~RefuseShortCopy => ReportFromAnswers /\ ~StageMove /\ MaxMoves = 0 /\ Spares = {}
\* The refusal is checked with a crashed leader that gives its copy up, on
\* the fenced path where only a promotion fences.
ASSUME RefuseShortCopy => /\ CrashLeader /\ GiveUpOnCrash /\ CrashLoses /\ FenceOnPromote
                          /\ Elections = 0 /\ ~Handoff /\ ~Counters /\ ~Reclaims

\* The lost promise is checked on the fenced path, apart from the wipes of
\* the report path. With elections the set file goes with it, so only with
\* the set check.
ASSUME LosePromises => /\ Wipes = 0 /\ AckByFollowers /\ FenceOnPromote
                       /\ (Elections > 0 => SetCheck)
ASSUME PromiseFromRoutes => LosePromises
ASSUME WipedWaitsForAdopt => LosePromises /\ Elections > 0
ASSUME ~WipedTakesBatches \/ ~WipedTakesNamed \/ WipedAboveAdopted \/ WipeBoundOracle
       => WipedWaitsForAdopt
\* With `Elections = 0` only the control plane names leaders, as before
\* elections ship; `Counters` is checked there.
ASSUME WipedNonVoter => /\ LosePromises /\ ~WipedWaitsForAdopt /\ JointFence
                        /\ Spares = {} /\ Planners = {}
ASSUME SetlessFences => WipedNonVoter
ASSUME ReadmitCountsWiped \/ ReadmitBeforeCatchUp \/ PromoteNonVoter \/ WipeLeads \/ Forgets > 1
       => WipedNonVoter
ASSUME WipedLearnsSet => WipedWaitsForAdopt \/ WipedNonVoter
ASSUME CreateCountsEmpty => WipedNonVoter
ASSUME GroupLogs => Counters /\ WipedNonVoter
ASSUME KeepSetDropsMark => GroupLogs
ASSUME IdlessCreation \/ GenOneExempt => WipedNonVoter
ASSUME Creates => Elections > 0 /\ SetCheck /\ Spares = {}
ASSUME NamedFenceSkipsBallot => FlipNewGeneration

\* Wipes and crashes on the fenced path, where the promoted leader checks the
\* identities the report named. With `UnprobedStart` the first leader knows
\* neither follower's identity until it has asked (`Unnamed`, any value no
\* identity reaches), so a report made before then names none for it. The
\* code asks before it ships; the model lets it ship first, which only admits
\* more. Elsewhere every identity starts known, which keeps the state space
\* down.
FencedIds == FenceOnPromote /\ StorageIds /\ Wipes > 0
Unnamed == 99
UnprobedStart == FALSE

\* Copy pins stand apart from the storage-id model of wipes and crashes.
ASSUME CopyPins => /\ ~FencedIds /\ ~WipedNonVoter /\ SetCheck /\ JointFence
                   /\ Spares = {} /\ Planners = {}
ASSUME FailoverStart \/ CopyChanges > 0 => CopyPins
ASSUME StaleStandRoute => StandsAboveCp

\* Under `UnprobedStart` a promoted leader starts blind too: its cursors
\* belong to its generation, so it knows no follower's identity until it has
\* asked. Inheritance is modelled only there. A reclaimed or elected leader
\* is named by no promotion, and `expect` is not kept for one.
ASSUME NameByInheritance => /\ FencedIds /\ UnprobedStart /\ ~Reclaims /\ Elections = 0

\* `SplitAtCutOver`: the ghost `cutOver` records the first `Swap`, so that a
\* configuration too big for one run can be checked in pieces, each a
\* CONSTRAINT on how its cut-over went (`CutOverPiece`). Off, it stays empty
\* and adds no states. A piece takes the cut-overs that keep the old leader
\* in the new set or not (`PieceKeeps`), that come after `PieceWrites`
\* writes were admitted, and after a replica stood for a generation past the
\* control plane's or not (`PieceStood`). A piece that big can be split
\* again by how many had promised past it (`PieceStoodCounts`) and whether
\* the destination itself had stood (`PieceDestStood`).
SplitAtCutOver == FALSE
\* `SplitAtFailover`: the same ghost records the first failover (`Promote`)
\* instead, for a configuration with no cut-over (`FailoverTag`). The
\* selectors then read: `PieceKeeps` whether the promoted broker's own set is
\* the control plane's, which a recorded-only adoption can leave apart;
\* `PieceDestStood` whether the promoted broker had stood; `PieceGens` the
\* generations the failover may take (`Nat` for any). The rest as for a
\* cut-over.
SplitAtFailover == FALSE
\* The pieces check under QuietView, which is sound only under these.
ASSUME SplitAtCutOver \/ SplitAtFailover =>
    /\ MaxTime = 0 /\ ~Handoff /\ ~Reclaims /\ Wipes = 0 /\ ~CrashLoses
ASSUME SplitAtFailover => Swaps = 0 /\ ~SplitAtCutOver
PieceKeeps == TRUE
PieceWrites == 0
PieceStood == FALSE
PieceStoodCounts == Nat
PieceDestStood == BOOLEAN
PieceGens == Nat
OneStood == {1}
SeveralStood == 2..Cardinality(Brokers)
JustYes == {TRUE}
JustNo == {FALSE}
GenTwo == {2}
GenThree == {3}
GenFourOn == {n \in Nat : n >= 4}

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
    leaving,    \* the follower it replaces: {} or {o}; once seated, the one the seat dropped
    joinedAt,   \* how much of the leader's log a majority of the set held when it joined
    clog,       \* each broker's counter log, under `Counters`
    chwm,       \* each broker's counter mark, as leader
    cconfirmed, \* per leader, how far each follower answered that it holds the counter log
    canswered,  \* who has answered each broker's counter fence
    cacked,     \* counter updates acknowledged to a client
    ballot,     \* under `Ballots`, whom each broker's promise was made to: {} or {leader}
    opened,     \* history, under `Elections` or `Suspicions`: each <<generation, broker>> that opened for writes
    setGen,     \* under `SetCheck`, the generation of the assignment that wrote each broker's `mine`
    oldSet,     \* under `SetCheck`, the sets a pending set change is leaving, kept with `mine`: {} when none is pending
    ackGen,     \* history, where opens are tracked: each <<write, generation>> it was acknowledged at
    waits,      \* under `AckWaits`, each Quorum write waiting for its ack: <<write, broker, generation, offset>>
    installed,  \* history, under `SetCheck`: each <<set's generation, set, previous sets>> a set change opened, and its generation under `StintStarts`
    cpNext,     \* under `Swaps`, the set a cut-over is moving to while the old one is still the control plane's: {} when none
    cpNextGen,  \* the generation that set was written at
    cpLeft,     \* under `LogDropsSets`, the control plane's set right before each generation
    cutOver,    \* ghost, under `SplitAtCutOver`: <<>>, or what the first cut-over found (CutOverTag)
    splits,     \* ghost, under `Backoff`: how many candidates stood while another's round was open
    stands,     \* ghost, under `MaxStandings`: how many candidates stood
    sid,        \* each broker's storage identity, which a wipe changes
    seen,       \* per leader, the identity it last learned for each follower
    seenInc,    \* per leader, the incarnation it last learned for each follower: inc + 1, or 0 for none
    wipes,      \* how many wipes have happened
    checked,    \* per leader, the followers that answered this pass from the copy it knows
    registered, \* the identity each broker last registered: its sid + 1, or 0 for none
    inc,        \* how many times each broker has registered: its incarnation
    restarting, \* brokers wiped and not yet registered again
    expect,     \* per leader, the identity it counts each replica's fence answer from
    inherited,  \* history, per leader: the replicas its promotion named by inheritance
    leftDirty,  \* how much of its log a crashed process left to the page cache, until a clean stop
    gaveUp,     \* under `RefuseShortCopy`, the generation each broker gave its copy up at: 0 for none
    fenceIds,   \* under `RefuseShortCopy`, per leader, the members its promotion's fence ids name
    named,      \* under `RefuseShortCopy`, the members the last report the control plane stored named
    wipedAt,    \* per broker, 0 for never wiped or re-admitted; else the control plane's generation at the wipe
    readmit,    \* under `WipedNonVoter`, a pending re-admission: {} or {<<wiped broker, generation, leader>>}
    learner,    \* under `WipedNonVoter`, the learner mark each copy keeps in its set file
    marked,     \* the wiped mark kept beside it, which under `GroupLogs` outlasts it
    cpLearner,  \* under `WipedNonVoter`, the copies placement holds as learners
    cpRefused,  \* under `ReportStore`, the refusal the control plane's stored report carries: {} or {<<generation, leader>>}
    refusedAt,  \* under `ReportStore`, the generation each broker last reported refused: 0 for none
    contAt,     \* per broker, <<generation, incarnation, retried>> of its last continuing open, <<0, 0, 0>> for none
    forgot,     \* the brokers that ever lost their promises (`Wipe`, `Forget`)
    pinned,     \* under `CopyPins`, the leaders whose fence carries pins
    stalePin,   \* per leader, the members its pins name by a copy they no longer hold
    changed,    \* the copy that changed and that placement has not admitted again: {} or {m}
    changes,    \* how many copies have changed
    admitting,  \* a re-admission pending: {} or {<<copy, generation>>}
    cpGone      \* under `CpStops`, whether the control plane has stopped, for good

\* The counter log's state, which only a cache shard's actions change.
counterVars == << clog, chwm, cconfirmed, canswered, cacked >>

\* The set each broker knows, beside `mine`, and a set change's old set; the
\* control plane's pending set change, and the ghost that a cut-over sets.
nextVars == << cpNext, cpNextGen, cutOver, splits, stands, cpLeft >>
setVars == << setGen, oldSet, installed, nextVars >>

\* Storage identities and registrations: only a wipe, a crash, a leader
\* learning one, a registration and a promotion change them.
idVars == << sid, seen, seenInc, wipes, checked, registered, inc, restarting, expect, inherited,
             leftDirty, gaveUp, fenceIds >>
\* The refusal the control plane last stored, and what each broker's reporter
\* repeats.
refusalVars == << cpRefused, refusedAt >>
\* A wiped replica's guard, a re-admission the control plane has pending, and
\* the continuing opens, which a re-admission also starts.
wipeVars == << wipedAt, readmit, learner, marked, cpLearner, contAt, forgot >>
\* Copy pins and the changed copies they keep from counting.
pinVars == << pinned, stalePin, changed, changes, admitting >>
\* All of them but `expect`, `inherited` and the promoted leader's `seen`
\* and `seenInc`, which a promotion sets.
decideIdVars == << sid, wipes, checked, registered, inc, restarting, leftDirty, gaveUp,
                   wipedAt, readmit, learner, marked, cpLearner, forgot >>

vars == << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
           hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
           draining, successor, stopped, moves, ver, cpView, staged, heard,
           promised, fencing, answered, confirmed, out, mine, joining, leaving, joinedAt,
           counterVars, ballot, opened, setVars, ackGen, waits, idVars, named, refusalVars, wipeVars, pinVars,
           cpGone >>

\* Everything but the refusal reports.
notRefusalVars ==
    << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
       hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
       draining, successor, stopped, moves, ver, cpView, staged, heard,
       promised, fencing, answered, confirmed, out, mine, joining, leaving, joinedAt,
       counterVars, ballot, opened, setVars, ackGen, waits, idVars, named, wipeVars, pinVars, cpGone >>

\* A VIEW for a configuration where time stands still (`MaxTime = 0`), there
\* is no handoff (`~Handoff`), no leader is reclaimed (`~Reclaims`), and
\* nothing is wiped or crashes (`Wipes = 0`, `~CrashLoses`): it leaves out
\* what no step reads again. A heartbeat then renews nothing, so whether one
\* is in flight changes nothing else. A broker's cursors and the fence
\* answers it collected are read only while it leads, and reset when it next
\* leads. TLC checks one state of each group that differs only in those, and
\* reaches the same states otherwise. TLC cannot assume anything about a
\* VIEW, so every configuration using it sets `SplitAtCutOver` or
\* `SplitAtFailover`, which assume these.
QuietView ==
    << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
       log, hwm, halted, queued, pending, acked, writes, staleCommit,
       draining, successor, stopped, moves, ver, cpView, staged, heard,
       promised, fencing,
       [b \in Brokers |-> IF bgen[b] > 0 THEN answered[b] ELSE {}],
       [b \in Brokers |-> IF bgen[b] > 0 THEN confirmed[b] ELSE [m \in Brokers |-> 0]],
       out, mine, joining, leaving, joinedAt,
       counterVars, ballot, opened, setVars, ackGen, waits, idVars, named, refusalVars, wipeVars, pinVars,
       cpGone >>

\* Placement's state, which only the control plane's decisions change, but
\* `mine`, which a follower also learns under `SetCheck`.
placeVars == << draining, successor, stopped, moves, ver, cpView, staged, out,
                joining, leaving, joinedAt >>
handoffVars == << placeVars, mine >>

\* The promotion fence's state.
fenceVars == << promised, fencing, answered, confirmed, ballot, opened, setVars, ackGen, waits >>

\* Whether brokers keep and check `promised`: the fence needs it, and so do
\* follower acks.
Promises == AckByFollowers \/ FenceOnPromote

\* Whether `f` may promise `b` generation `g`. At the generation it already
\* promised, only to the same leader with `Ballots`, and to anyone without.
\* `{}` is a promise made before ballots, adopted by the first leader to ask.
MayPromise(f, b, g) ==
    \/ promised[f] < g
    \/ promised[f] = g /\ (Ballots => ballot[f] \subseteq {b})

\* `f` promises `b` generation `g`. The ballot is kept only under `Ballots`,
\* so a configuration without it has no more states than before.
Promise(f, b, g) ==
    /\ promised' = [promised EXCEPT ![f] = g]
    /\ ballot' = IF Ballots THEN [ballot EXCEPT ![f] = {b}] ELSE ballot

\* `b` opens for writes at `g`. Recorded only where a second leader at one
\* generation is in question, for the same reason: under `Elections`, and
\* under `Suspicions`, where placement replaces a leader that is still open.
TracksOpens == Elections > 0 \/ Suspicions > 0 \/ WipedNonVoter
Opens(b, g) == opened' = IF TracksOpens THEN opened \cup {<<g, b>>} ELSE opened

\* `incs` holds incarnation + 1 (0 is none), so a report seeded from this
\* names every broker at its first registration, as `sids` names its first
\* identity.
NoReport == [holders |-> {}, len |-> 0, drained |-> FALSE, gen |-> 0, sids |-> [m \in Brokers |-> 0],
             incs |-> [m \in Brokers |-> 1], inh |-> [m \in Brokers |-> Unnamed], names |-> {}]

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

\* `LoweredFactor`: the replication factor is one below the starting set.
\* A definition, so configurations without it need not name it.
LoweredFactor == FALSE

\* `Undoes`: placement may give a replacement up and drop the copy joining
\* (`undo_or_fence` in placement/moves.rs). `UndoUnfenced` drops it while
\* the leader serves, as placement did before #1628.
Undoes == FALSE
UndoUnfenced == FALSE

\* The replication factor, and a set above it with nothing else in progress.
\* Without `Grow` or `LoweredFactor` a set grows only by a failover: one that
\* keeps `SeatDropped`, or one that names the copy still joining.
Factor == Cardinality(Brokers \ Spares) - (IF LoweredFactor THEN 1 ELSE 0)
OverFactor == ~Grow /\ joining = {} /\ staged = {} /\ Cardinality(ReplicaSet) > Factor

\* A seat that leaves an even set: beside the odd set it joined, a record
\* can be acknowledged on the leader and the leaving copy alone at any time
\* up to the seat, after any report placement reads (#1616).
SeatsIntoEven == leaving /= {} /\ Cardinality(mine[leader] \ leaving) % 2 = 0

\* The leader fenced for such a seat (`SeatFence`).
SeatFenced == draining /\ successor = leader /\ joining /= {}

MajorityOf(S, of) == Cardinality(S \cap of) * 2 > Cardinality(of)
Majority(S) == MajorityOf(S, QuorumSet)

\* A leader ships to, fences and counts the set its own assignment named,
\* which a deposed leader still holds after the control plane has moved on.
\* With no spares every broker is in every set, as before.
Members(b) == IF Spares = {} THEN Brokers ELSE mine[b]
LeaderMajority(b, S) == IF Spares = {} THEN Majority(S) ELSE MajorityOf(S, mine[b])

\* Who a leader fences: its set, and while it changes sets with `JointFence`,
\* the old ones too.
FenceMembers(b) == Members(b) \cup (IF JointFence THEN UNION oldSet[b] ELSE {})

\* Whether `held` holds a majority of every set `b` is leaving.
MajorityOfOld(held, b) == \A o \in oldSet[b] : MajorityOf(held, o)

\* Whether `b` lost the set it leaves (`ForgetSet`).
SetUnknown(b) == {} \in oldSet[b]

\* The set the control plane's assignment names.
CpSet == ReplicaSet \cup joining

\* A step that writes the next generation records `set` as the one before
\* it, for `LogDropsSets` alone. Every other step that writes a generation
\* leaves it, which `LeftRecorded` catches.
RecordLeft(set) == cpLeft' = IF LogDropsSets THEN Append(cpLeft, set) ELSE cpLeft

\* What a batch raising the set teaches as the set being left: the control
\* plane's set before the batch's generation, as `check_batch_set` in
\* replica/election.rs takes it from `set_before` with no set of its own.
BatchLeaving(b) ==
    IF JointFence /\ setGen[b] \in 1..Len(cpLeft) THEN {cpLeft[setGen[b]]} \ {{}} ELSE {}

\* The sets a failover's fence leaves, `own` the promoted broker's.
FailoverOldSets(own) ==
    (IF FenceOwnSet THEN {own} ELSE {}) \cup (IF FenceRecordedSet THEN {CpSet} ELSE {})

\* Whether the control plane holds `b`'s set with no change pending, so `b`
\* no longer needs the old one.
Settled(b) == cpNext = {} /\ mine[b] = CpSet

\* Under `SetCheck`, `f` takes a fence or a batch from `b` only if it knows no
\* newer set than the one `b` carries, and then learns `b`'s. A set change
\* still pending travels with it, so whoever stands next on the new set
\* fences the old one too, as Raft's joint configuration does; a batch comes
\* only from a leader that opened, which ends it.
SetCurrent(f, b) == SetCheck => setGen[f] <= setGen[b]
\* Whether `b`'s log holds a record a leader wrote at generation `g`.
HoldsRecordOf(b, g) == \E i \in 1..Len(log[b]) : log[b][i].lg = g

\* Under `LogDropsSets` the sets are kept as `accept_set_keeping_earlier`
\* keeps them (disk_log/set.rs): a set at the generation `f` keeps changes
\* nothing. A newer one takes what it carries (from a batch, the control
\* plane's set before it, `BatchLeaving`) and, under `JointFence` and unless `f`'s
\* log holds a record of its own set's generation, that set and the sets it
\* kept. The model's logs are durable whole: no cfg with these sets loses a
\* tail. A raise that would hold more than `KeptSetsCap` + 1 distinct sets
\* is refused unless it adds none.
LearnSetFrom(f, b, batch) ==
    IF SetCheck
    THEN /\ setGen' = [setGen EXCEPT ![f] = setGen[b]]
         /\ mine' = [mine EXCEPT ![f] = mine[b]]
         /\ IF LogDropsSets
            THEN LET carried == IF JointFence /\ setGen[f] > 0 /\ ~HoldsRecordOf(f, setGen[f])
                                THEN ({mine[f]} \cup oldSet[f]) \ {{}} ELSE {}
                     sent == IF batch THEN BatchLeaving(b) ELSE oldSet[b]
                     kept == IF batch /\ BatchDropsSets THEN {}
                             ELSE (sent \cup carried) \ {mine[b]}
                     had == {mine[f]} \cup oldSet[f]
                 IN IF setGen[f] = setGen[b]
                    THEN UNCHANGED oldSet
                    ELSE /\ \/ Cardinality({mine[b]} \cup kept) <= KeptSetsCap + 1
                            \/ ({mine[b]} \cup kept) \subseteq had
                         /\ oldSet' = [oldSet EXCEPT ![f] = kept]
            ELSE IF OneLeavingSet /\ Cardinality(oldSet[b]) > 1
            THEN \E o \in oldSet[b] :
                   /\ f \in o \/ \A p \in oldSet[b] : f \notin p
                   /\ oldSet' = [oldSet EXCEPT ![f] = {o}]
            ELSE oldSet' = [oldSet EXCEPT ![f] = IF LearnsLeavingSet \/ oldSet[b] = {} THEN oldSet[b]
                                                ELSE IF setGen[f] < setGen[b] THEN {mine[f]}
                                                ELSE oldSet[f]]
    ELSE UNCHANGED << setGen, mine, oldSet >>
LearnSet(f, b) == LearnSetFrom(f, b, FALSE)

\* Whether a broker is a candidate rather than the leader the control plane
\* named at its generation.
Candidate(b) == ~(leader = b /\ bgen[b] = gen)

\* Whether non-voter `f` takes `b`'s batch: from the leader the control plane
\* confirms at its generation, and with `CatchUpFromDeposed` from one it
\* named there since `f`'s wipe and has replaced, which without elections is
\* every open leader at that generation or later.
ConfirmedSender(b, f) ==
    \/ b = leader /\ bgen[b] = gen
    \/ CatchUpFromDeposed /\ Elections = 0 /\ bgen[b] >= wipedAt[f]

\* Under `WipedWaitsForAdopt`, whether `f` lost its promises and the control
\* plane has not yet taken a generation past the one it had then. Until it
\* has, `f` may have promised any generation a candidate took to that
\* candidate. `gen` is the linearizable read; a planner's view can be older.
WipeWaits(f) == WipedWaitsForAdopt /\ wipedAt[f] > 0 /\ gen <= wipedAt[f]

\* Under `WipedNonVoter`, whether `f` lost its storage and is not yet
\* re-admitted. A ghost: no broker knows it.
Wiped(f) == WipedNonVoter /\ wipedAt[f] > 0

\* Whether `f` keeps the learner mark: the code's non-voter.
NonVoter(f) == WipedNonVoter /\ learner[f]

\* What the code can see of a wipe: `b`'s set names another storage id for
\* `f`. Every control-plane write names each member's registered id, so a
\* set written at or below the control plane's generation at the wipe names
\* the old one. Registering at once is the least evidence the code can get.
\* Under `IdlessCreation` the creation set names none.
NamesOther(f, b) ==
    /\ Wiped(f) /\ 0 < setGen[b] /\ setGen[b] <= wipedAt[f]
    /\ ~(IdlessCreation /\ setGen[b] = 1)

\* Under `GroupLogs`, whether `f`'s group log holds a record written at
\* generation `g` or later.
GroupHeld(f, g) == \E k \in 1..Len(clog[f]) : clog[f][k].g >= g

\* Whether `f` answers `b`'s fence or takes its batch as a voter: no mark,
\* and `b`'s set names no other copy for it. `CreateCountsEmpty` also lets a
\* wiped copy with no set file vote.
Votes(f, b) == (~NonVoter(f) /\ ~NamesOther(f, b)) \/ (CreateCountsEmpty /\ setGen[f] = 0)

\* What a fence without a set can see of `f`'s mark: the marker, which is
\* the mark of a copy holding no set (`setGen[f] = 0`), and a mark kept in
\* a set file.
SetlessNonVoter(f) == NonVoter(f) /\ (SetlessSeesMarker \/ setGen[f] > 0)

\* The credit given to a non-voter (`SetChangesJoint`, `AckedOnMajority`) is the
\* one loss a record or set already survived, so it holds only while at most one
\* distinct broker is a non-voter. A second wipe of the same broker is still
\* one failure; two distinct brokers wiped at once is two, beyond what a set
\* of three tolerates, and the plain checks apply.
NonVoterCredit == Cardinality({ m \in Brokers : Wiped(m) }) <= 1

\* Under `WipedAboveAdopted`, whether a wiped `f` that holds no promise at
\* the control plane's generation may promise `b` generation `g`: only above
\* it, or at it only to the leader the control plane names there.
WipeFloor(f, b, g) ==
    WipedAboveAdopted /\ wipedAt[f] > 0 /\ promised[f] < gen => g > gen \/ (g = gen /\ b = leader)

\* Whether `m` hears a leader: the one it promised its generation to is still
\* open there. The failure detector at its best; with `PreVote` it gates
\* elections, which safety must not depend on.
Hears(m) == \E l \in Brokers \ {m} :
              /\ bgen[l] > 0 /\ bgen[l] = promised[m] /\ ~fencing[l] /\ ~stopped[l]
              /\ Ballots => l \in ballot[m]

\* The acknowledgements recorded at `b`'s generation, where opens are tracked.
AckAt(b, ids) == ackGen' = IF TracksOpens THEN ackGen \cup { <<w, bgen[b]>> : w \in ids } ELSE ackGen

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

\* Ahead by (last generation, length), the order promotion by log order uses.
Ahead(f, b) ==
    \/ LastGen(f) > LastGen(b)
    \/ LastGen(f) = LastGen(b) /\ Len(log[f]) > Len(log[b])

\* A move's destination: staged and copying, or named successor by the fence.
\* Without `FenceEveryChange` it opens without the fence whichever write
\* names it leader, because a cut-over only names it once it holds the
\* drained leader's whole log.
Incoming(f) == f \in staged \/ (draining /\ successor = f)

\* Whether `f`, named leader by a promotion, opens without the fence.
\* `begin_open` in services/felix-broker-service/src/shards/lifecycle.rs
\* fences every leadership it takes.
PromotedUnfenced(f) == ~FenceOnPromote \/ (Incoming(f) /\ ~FenceEveryChange)

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

IdInit ==
    /\ sid = [m \in Brokers |-> 0]
    /\ seen = [m \in Brokers |-> [n \in Brokers |->
                 IF FencedIds /\ UnprobedStart /\ m = leader /\ n /= m THEN Unnamed ELSE 0]]
    /\ seenInc = [m \in Brokers |-> [n \in Brokers |->
                    IF FencedIds /\ UnprobedStart /\ m = leader /\ n /= m THEN 0 ELSE 1]]
    /\ wipes = 0
    /\ checked = [m \in Brokers |-> {}]
    /\ registered = [m \in Brokers |-> 1]
    /\ inc = [m \in Brokers |-> 0]
    /\ restarting = {}
    \* The first leader was promoted by nobody, so it inherits no name.
    /\ expect = [m \in Brokers |-> [n \in Brokers |->
                   IF FencedIds /\ UnprobedStart THEN Unnamed ELSE 0]]
    /\ inherited = [m \in Brokers |-> {}]
    /\ leftDirty = [b \in Brokers |-> 0]
    /\ gaveUp = [b \in Brokers |-> 0]
    /\ fenceIds = [b \in Brokers |-> {}]
    /\ named = {}
    /\ cpRefused = {}
    /\ refusedAt = [b \in Brokers |-> 0]
    /\ wipedAt = [b \in Brokers |-> 0]
    /\ readmit = {}
    /\ learner = [b \in Brokers |-> FALSE]
    /\ marked = [b \in Brokers |-> FALSE]
    /\ cpLearner = {}
    /\ contAt = [b \in Brokers |-> <<0, 0, 0>>]
    /\ forgot = {}
    /\ pinned = IF FailoverStart THEN {leader} ELSE {}
    /\ stalePin = [b \in Brokers |-> {}]
    /\ changed = {}
    /\ changes = 0
    /\ admitting = {}

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
    \* Under `Elections` the first leader starts as an elected one would, with
    \* its set's promise, so no candidate can take its generation from under it.
    \* With `Creates` it is still fencing its followers, which hold nothing.
    /\ promised = [b \in Brokers |-> IF b = leader \/ (Elections > 0 /\ b \notin Spares /\ ~Creates)
                                     THEN 1 ELSE 0]
    \* Under `FailoverStart` it is still fencing the assignment a failover wrote.
    /\ fencing = [b \in Brokers |-> (Creates \/ FailoverStart) /\ b = leader]
    /\ answered = [b \in Brokers |-> {}]
    /\ confirmed = [b \in Brokers |-> [f \in Brokers |-> 0]]
    /\ out = Spares
    /\ mine = [b \in Brokers |-> IF Creates /\ b /= leader THEN {} ELSE Brokers \ Spares]
    /\ joining = {}
    /\ leaving = {}
    /\ joinedAt = 0
    /\ cpGone = FALSE
    /\ CounterInit
    /\ ballot = [b \in Brokers |-> IF Ballots /\ promised[b] = 1 THEN {leader} ELSE {}]
    /\ opened = IF TracksOpens /\ ~Creates /\ ~FailoverStart THEN {<<1, leader>>} ELSE {}
    \* The first assignment wrote the set at generation 1; a spare knows none,
    \* and with `Creates` neither does a follower until the fence reaches it.
    /\ setGen = [b \in Brokers |-> IF SetCheck /\ b \notin Spares /\ (~Creates \/ b = leader)
                                   THEN 1 ELSE 0]
    /\ oldSet = [b \in Brokers |-> {}]
    /\ ackGen = {}
    /\ waits = {}
    /\ installed = {}
    /\ cpNext = {}
    /\ cpNextGen = 0
    /\ cpLeft = <<{}>>
    /\ cutOver = <<>>
    /\ splits = 0
    /\ stands = 0
    /\ IdInit

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

\* The pieces checked under QuietView send none. Time stands still there, so
\* every grant already set the expiries to L and a heartbeat renews nothing:
\* it changes only hbOut and hbAt, which the view leaves out, and every state
\* it leads to is one TLC already has (up to symmetry).
SendHeartbeat(b) ==
    /\ ~SplitAtCutOver
    /\ bgen[b] > 0
    /\ ~hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = TRUE]
    /\ hbAt' = [hbAt EXCEPT ![b] = clock[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

AcceptLeaderHeartbeat(b) ==
    /\ hbOut[b]
    /\ leader = b /\ bgen[b] = gen
    /\ now <= cpExpiry
    /\ cpExpiry' = now + L
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ bexpiry' = [bexpiry EXCEPT ![b] = hbAt[b] + L]
    /\ UNCHANGED << now, clock, gen, leader, report, inflight, bgen, hbAt,
                    log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* A heartbeat renews only while the control plane names the broker leader
\* at the generation it believes it leads. The two operators below take its
\* place in a configuration (`AcceptHeartbeat <- ...`) to check the code's
\* rule, which is per broker rather than per shard.
AcceptHeartbeat(b) == AcceptLeaderHeartbeat(b)

\* What the code did before #1955: the control plane accepts the heartbeat of
\* a broker it no longer names leader, and the broker renews on it at the
\* generation it still believes it leads, its routes not yet showing the
\* move. FelixShardLeaseAnyHeartbeat.cfg.
AcceptAnyHeartbeat(b) ==
    \/ AcceptLeaderHeartbeat(b)
    \/ /\ hbOut[b]
       /\ ~(leader = b /\ bgen[b] = gen)
       /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
       /\ bexpiry' = [bexpiry EXCEPT ![b] = hbAt[b] + L]
       /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, hbAt,
                       log, hwm, halted, queued, pending, acked, writes, staleCommit >>
       /\ UNCHANGED << handoffVars, fenceVars >>

\* The code's rule since: such a broker was marked down, and a renewal after
\* that counts only once its routes reach the assignment feed's head as of
\* the accept. Those routes name the new leader or generation, so the broker
\* stops believing it leads instead of renewing. That the routes show the
\* move is assumed here, not modelled: it is what the head check gives, and
\* the unit tests check the head (a lagging member's answer falls short).
\* FelixShardLeaseRoutesHeartbeat.cfg.
AcceptHeartbeatOnRoutes(b) ==
    \/ AcceptLeaderHeartbeat(b)
    \/ /\ hbOut[b]
       /\ ~(leader = b /\ bgen[b] = gen)
       /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
       /\ bgen' = [bgen EXCEPT ![b] = 0]
       /\ queued' = [queued EXCEPT ![b] = 0]
       /\ pending' = [pending EXCEPT ![b] = 0]
       /\ stopped' = [stopped EXCEPT ![b] = FALSE]
       /\ fencing' = [fencing EXCEPT ![b] = FALSE]
       /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                       hbAt, log, hwm, halted, acked, writes, staleCommit,
                       draining, successor, moves, ver, cpView, staged, promised, answered,
                       confirmed, out, mine, joining, leaving, joinedAt, ballot, opened, setVars,
                       ackGen, waits >>

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
                    confirmed, out, mine, joining, leaving, joinedAt, ballot, opened, setVars,
                    ackGen, waits >>

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
    \* The client's wait, keyed by where and at which generation the write landed.
    /\ waits' = IF AckWaits /\ Quorum
                THEN waits \cup {<< pending[b], b, bgen[b], Len(log[b]) + 1 >>}
                ELSE waits
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, writes >>
    /\ UNCHANGED << handoffVars, promised, fencing, answered, confirmed, ballot, opened, setVars, ackGen >>

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
\* leader older than the generation it persisted, and with `Ballots` another
\* leader at that generation, and persists the leader's:
\* `accept_sender` in crates/server/felix-replication/src/replica.rs. A leader still fencing
\* does not ship: it may yet take a tail from a follower it would truncate.
\*
\* With `LabelOnReceipt` the follower labels what it appends with the
\* sender's generation rather than the one the record was written at, as a
\* follower did when it recorded a new generation as starting where the
\* batch that brought it appended. A record the leader inherited then looks
\* newer on the follower than it is, and its fence answer overclaims.
\*
\* Under `SetCheck` a batch carries the leader's set, which the follower
\* refuses if it knows a newer one and otherwise learns. A broker still
\* leading at an older generation takes a newer leader's batch only by
\* stopping first, as it does for a newer fence (AnswerFence).
Ship(b, f) ==
    /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ f \in FenceMembers(b)
    /\ bgen[f] = 0 \/ (SetCheck /\ bgen[b] > bgen[f])
    /\ IF bgen[f] > 0
       THEN /\ bgen' = [bgen EXCEPT ![f] = 0]
            /\ fencing' = [fencing EXCEPT ![f] = FALSE]
            /\ queued' = [queued EXCEPT ![f] = 0]
            /\ pending' = [pending EXCEPT ![f] = 0]
       ELSE UNCHANGED << bgen, fencing, queued, pending >>
    /\ bgen[b] >= LastGen(f)
    /\ ~fencing[b]
    /\ WipeWaits(f) => IF Candidate(b) THEN WipedTakesBatches ELSE WipedTakesNamed
    /\ WipeFloor(f, b, bgen[b])
    /\ Votes(f, b)
    /\ Promises => MayPromise(f, b, bgen[b])
    /\ IF Promises THEN Promise(f, b, bgen[b]) ELSE UNCHANGED << promised, ballot >>
    /\ SetCurrent(f, b)
    /\ LearnSetFrom(f, b, TRUE)
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
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, hwm, acked, writes, staleCommit >>
    /\ UNCHANGED << placeVars, answered, opened, installed, nextVars, ackGen, waits >>

\* Whether `m` holds the first `k` records of `b`'s log.
HoldsPrefix(m, b, k) ==
    Len(log[m]) >= k /\ Len(log[b]) >= k /\ \A j \in 1..k : Same(log[m][j], log[b][j])

\* `m` holds all of `b`'s counter log.
CHoldsPrefix(m, b) ==
    Len(clog[m]) >= Len(clog[b]) /\ \A j \in 1..Len(clog[b]) : Same(clog[m][j], clog[b][j])

\* Whether `b`'s start at `g` is acknowledged for the flip. Under
\* `StintStarts`, the last start record at `g` is under `b`'s mark: a leader
\* opening at a generation it already started appends another, identical
\* start, and only that one was acknowledged on both sets. Otherwise it is
\* the generation's first start, which may have been acknowledged on the old
\* set alone, before the change. Under `JoinLags` that start must be held
\* now on a majority of the pending set and of the current one, as
\* `joint_start` in driver/shard.rs counts it live (`held_by_both`).
\* Without `JoinLags` the mark stands in: every joining copy holds the whole
\* log, so a start acknowledged before the change is already held on both
\* sets, and one written at the joint open is under the mark only once it is.
StartAcked(b, g) ==
    LET at == { i \in 1..Len(log[b]) : log[b][i] = Start(g) }
        first == CHOOSE i \in at : \A j \in at : i <= j
        holders == { m \in Brokers : HoldsPrefix(m, b, first) }
    IN /\ at /= {}
       /\ CASE StintStarts -> \A i \in at : i <= hwm[b]
              [] JoinLags -> MajorityOf(holders, cpNext) /\ MajorityOf(holders, CpSet)
              [] OTHER -> first <= hwm[b]

\* Whether `b` counts `m` as holding its first `k` records in a report: by
\* `m`'s log, or with `ReportFromAnswers` by what `m` last answered, as
\* `quorum_offset_without` and `caught_up` read `FollowerCursor::confirmed`.
ReportCounts(m, b, k) ==
    IF ReportFromAnswers THEN confirmed[b][m] >= k ELSE HoldsPrefix(m, b, k)

\* Whether `b` counts `m` toward its mark. Where wipes are modelled with
\* `StorageIds`, only once `m` has answered this pass from the copy `b` knows
\* (`Answer`; `checked_only` in
\* crates/server/felix-replication/src/driver/shard.rs). A wipe between that
\* answer and the count is not seen. The report still names `m`, under the
\* identity it was counted under, and placement checks that.
MarkCounts(m, b, k) ==
    /\ ReportCounts(m, b, k)
    /\ ReportFromAnswers /\ StorageIds /\ Wipes > 0 => m \in checked[b]

\* The most of `b`'s log a majority holds, `b` included and halted followers
\* not: `quorum_offset_without`. With `StartRecord`, only up to a record of
\* `b`'s own generation, as the mark.
MajorityLen(b) ==
    LET held == { k \in 0..Len(log[b]) :
                    /\ k > 0 => OwnGen(b, k)
                    /\ MajorityOf({ m \in Brokers \ halted : MarkCounts(m, b, k) } \cup {b},
                                  QuorumSet) }
    IN IF held = {} THEN 0 ELSE CHOOSE k \in held : \A j \in held : j <= k

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
    \* Where wipes are modelled with identities, by what the leader counts, as
    \* the code's mark is, rather than by the logs it cannot see.
    /\ IF Wipes > 0 /\ StorageIds
       THEN i <= MajorityLen(b)
       ELSE MajorityOf({ m \in Brokers : Len(log[m]) >= i /\ Same(log[m][i], log[b][i]) } \cup {b}, of)
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
\*
\* While a set change is pending under `JointFence`, a record also needs a
\* majority of the old set, as Raft's joint configuration commits.
HeldAtGen(b, i) ==
    LET held == { m \in Brokers \ {b} : confirmed[b][m] >= i }
                \cup (IF promised[b] = bgen[b] THEN {b} ELSE {})
    IN /\ OwnGen(b, i)
       /\ LeaderMajority(b, held)
       /\ JointFence => MajorityOfOld(held, b)

\* The waits `b`'s mark reaching `i` answers with an acknowledgement. A wait
\* follows its broker into any generation it leads next, so the mark may be
\* a newer generation's, counting a log a promotion's catch-up rewrote
\* (`wait_while_leading` in crates/server/felix-replication/src/quorum.rs).
\* The broker acknowledges only if its log's generation history still
\* labels the write's offset with the write's generation (`still_holds`),
\* which is `lg` here; one generation has one leader, so that record is the
\* write. A record the code holds with no known label (taken after a cut
\* from a replica that sent none) is refused there; the model always has a
\* label, `lg`, so it checks the case where the label is known. With `AckByOffset` it
\* acknowledges on the offset alone: FelixShardFigure8AckWaitByOffset.cfg. The
\* waits the mark passes and does not answer end as a lost leadership.
Answered(b, i) ==
    { wt[1] : wt \in { wt \in waits :
        /\ wt[2] = b
        /\ wt[4] <= i
        /\ AckByOffset \/ log[b][wt[4]].lg = wt[3] } }

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
        /\ LET ids == ({ log[b][j].id : j \in 1..i } \ {StartId}) \cup Answered(b, i)
           IN /\ acked' = acked \cup ids
              /\ AckAt(b, ids)
        /\ waits' = { wt \in waits : wt[2] /= b \/ wt[4] > i }
        /\ hwm' = [hwm EXCEPT ![b] = i]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, queued, pending, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, promised, fencing, answered, confirmed, ballot, opened, setVars >>

\* A follower learns the mark from the leader, never past what it holds.
LearnHwm(b, f) ==
    /\ bgen[b] > 0 /\ f /= b
    /\ hwm[f] < hwm[b]
    \* A leader that took a shorter log from a fence answer can hold less than
    \* its old mark, which NoTruncationBelowHwm flags; a twin that checks only
    \* its own invariant walks on from there.
    /\ Len(log[b]) >= hwm[b]
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
\* How much of `b`'s log it inherited: the records written before its own
\* generation. Any record an earlier leader acknowledged is among them.
Inherited(b) == Cardinality({ i \in 1..Len(log[b]) : log[b][i].g < bgen[b] })

\* What a follower must hold to be reported caught up. Under `Quorum`, the
\* records the mark sent with this report may release, and never below the
\* mark already out; otherwise, and once the leader has stopped for a move,
\* all of it.
\* With `ReportFloor`, never below the log the leader inherited either:
\* `acknowledged` in crates/server/felix-replication/src/driver/shard.rs.
\*
\* The code's floor is the mark it published, which it publishes as soon as
\* the report describing it lands. The model releases that acknowledgement
\* later, from `heard`, so the floor is also what `b` was told was stored.
\* Only a wipe lowers the leader's count of a majority below a report that
\* landed, so the floor is applied where wipes are and the other
\* configurations keep the state spaces they were checked with.
Max(x, y) == IF x >= y THEN x ELSE y
Landed(b) == IF Wipes > 0 /\ AckOnResponse /\ heard[b].gen = bgen[b] THEN heard[b].len ELSE 0
ReportAt(b) ==
    IF ReportBound /= "tail" /\ Quorum /\ ~stopped[b]
    THEN Max(Max(Max(MajorityLen(b), hwm[b]), Landed(b)),
             IF ReportFloor THEN Inherited(b) ELSE 0)
    ELSE Len(log[b])

\* With follower acks and a promotion that does not read it, nothing reads the
\* report but a trim or a fenced seat, so it is sent only for those: that
\* keeps the fenced configurations small.
Report(b) ==
    /\ AckByFollowers => Promotion = "leader-report" \/ OverFactor \/ SeatFenced
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
                       gen     |-> bgen[b],
                       \* With `CrashLeader` the leader names its own identity
                       \* too, as the code does. Otherwise `seen[b][b]` stands
                       \* in for it, 0, which is right only because there the
                       \* leader never crashes and keeps its first identity.
                       sids    |-> IF CrashLeader THEN [seen[b] EXCEPT ![b] = sid[b]]
                                                  ELSE seen[b],
                       incs    |-> seenInc[b],
                       \* What its own promotion named each replica by.
                       inh     |-> IF NameByInheritance THEN expect[b]
                                                        ELSE [m \in Brokers |-> Unnamed],
                       \* The members it names an identity for, itself included:
                       \* the keys of `follower_storage`.
                       names   |-> IF RefuseShortCopy
                                   THEN { m \in mine[b] : m = b \/ seen[b][m] /= Unnamed }
                                   ELSE {}] >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* A stored report is answered to the leader that sent it, the one leading at
\* its generation. The answer can be lost, leaving the broker with an older one.
\* Under `StampOnArrival`, or without `StorageIds` (a leader that learns no
\* identity learns no incarnation either), the control plane stamps it with
\* every broker's incarnation as it takes it (`registered_incarnations` in
\* services/felix-controlplane-service/src/api/nodes/reports.rs), as it does
\* for a follower that predates the handshake's incarnation.
DeliverReport ==
    /\ inflight /= <<>>
    /\ report' = IF inflight[1].gen /= gen THEN report
                 ELSE IF StampOnArrival \/ ~StorageIds
                      THEN [inflight[1] EXCEPT !.incs = [m \in Brokers |-> inc[m] + 1]]
                      \* On the fenced path a leader has measured no incarnation
                      \* for a follower it has not asked yet, and the control
                      \* plane stamps the registered one (`node_incarnations`).
                      ELSE IF FencedIds
                      THEN [inflight[1] EXCEPT !.incs =
                              [m \in Brokers |-> IF inflight[1].incs[m] = 0 THEN inc[m] + 1
                                                                             ELSE inflight[1].incs[m]]]
                      ELSE inflight[1]
    /\ inflight' = <<>>
    \* Kept across the generations that clear `report`, as placement keeps the
    \* last report it took.
    /\ named' = IF RefuseShortCopy /\ inflight[1].gen = gen THEN inflight[1].names ELSE named
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
\* With `StorageIds`, only under the identity the follower registered: a
\* follower counted under another was counted holding a log it has lost. One
\* registered without an identity is unverified, and with `StampIncarnations`
\* counts only from a report naming its current incarnation, never one naming none
\* (`counted` in services/felix-controlplane-service/src/cluster/placement/
\* replica_positions.rs). A follower wiped and not yet registered again is
\* left out: placement then still reads its old registration, which
\* docs/replication-design.md ("Who may be promoted") leaves open.
ByLeaderReport(r, f) ==
    /\ f \in r.holders /\ f \notin halted
    /\ f \notin restarting
    /\ IF StorageIds /\ registered[f] /= 0
       THEN r.sids[f] + 1 = registered[f]
       ELSE StampIncarnations => r.incs[f] = inc[f] + 1

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

\* Under `GiveUpOnCrash`, the leader of read `v` is registered under another
\* identity than its report names for it: it came back from a crash and
\* leads nothing from that copy. Read live, as ByLeaderReport reads
\* `registered`.
LeaderReplaced(v) ==
    /\ GiveUpOnCrash /\ StorageIds
    /\ v.report.gen > 0
    /\ registered[v.leader] /= 0
    /\ v.report.sids[v.leader] + 1 /= registered[v.leader]

\* Under `Reclaims`, the given-up leader's probe measures every follower as
\* it stands, and placement promotes one that is furthest among them and not
\* behind the leader: it holds whatever any copy holds (`given_up_report` in
\* crates/server/felix-replication/src/reclaim.rs, `is_caught_up` in
\* placement/replica_positions.rs). Reclaim is left for when every follower
\* is behind.
ByProbe(v, f) ==
    /\ Reclaims /\ ~ReclaimBlind
    /\ LeaderReplaced(v)
    /\ bgen[v.leader] = 0
    /\ f \in ReplicaSet \ {v.leader}
    /\ f \notin halted /\ f \notin restarting
    /\ ~Ahead(v.leader, f)
    /\ \A o \in ReplicaSet \ {v.leader} : ~Ahead(o, f)

\* The identity a promotion from report `r` hands the new leader for `m`: the
\* one `r` names. For one it names none, under `NameByInheritance` the one
\* `r`'s leader was promoted with for `m`, and under `NameByRegistration` the
\* one `m` is registered under while it is at the incarnation `r` was taken
\* at. A crash registers again with a new identity and incarnation.
Inherits(r, m) == r.sids[m] = Unnamed /\ NameByInheritance /\ r.inh[m] /= Unnamed

Named(r, m) ==
    IF Inherits(r, m) THEN r.inh[m]
    ELSE IF /\ r.sids[m] = Unnamed /\ NameByRegistration /\ registered[m] /= 0
            /\ r.incs[m] = inc[m] + 1
    THEN registered[m] - 1
    ELSE r.sids[m]

\* The copy a seat into a set of two dropped, which a failover may keep
\* (`left_unconfirmed` in placement/plan.rs): it does while the control plane
\* believes the leader's last report from before the seat, which named it, and
\* sees its broker serving. Either may not hold, so a failover keeps it or not.
SeatDropped == IF Cardinality(Brokers \ out) = 2 THEN leaving \cap out ELSE {}

\* The members a failover's fence ids name, `new` its set: those of the old
\* set and the new one the last report named, and every old member, which is
\* named by inheritance or with no identity where the report names none
\* (`report_storage_ids` in placement/replica_positions.rs, #1486). The old
\* set is the previous assignment's, a joining or staged copy included, or
\* without the staged copy under `FenceIdsSetOnly`. None when the last report
\* named none.
FenceIds(new) ==
    LET old == Brokers \ (out \cup (IF FenceIdsSetOnly THEN staged ELSE {}))
    IN IF named = {} THEN {}
       ELSE (named \cap (old \cup new)) \cup { m \in old : registered[m] /= 0 }

\* What a failover to `f` found, for `SplitAtFailover`, in `CutOverTag`'s
\* shape with the new generation last. Names no broker, so SYMMETRY holds.
FailoverTag(f) ==
    << IF mine[f] = CpSet THEN 1 ELSE 0, writes,
       Cardinality({ m \in Brokers : promised[m] > gen }),
       PieceDestStood /= BOOLEAN /\ promised[f] > gen, gen + 1 >>

\* Each decision below is made from a read `v` and leaves the planners'
\* reads as `views`: a held read is used up by the write it decided, one
\* write per shard per pass. Every write bumps the store's generation.
\* Whether the broker placement names leader at the next generation takes
\* it: it has not promised that generation, or a later one, to another.
TakesNaming(f) == Elections = 0 \/ MayPromise(f, f, gen + 1)

Promote(v, f, views) ==
    /\ f /= v.leader
    \* Under `Suspicions` the followers' word is enough, and a detector can
    \* be wrong, so any read will do: the leader may be alive, serving and
    \* heartbeating. Bounded by generation, or promotions never end.
    \* A leader that gave its copy up is replaced at once.
    /\ v.lapsed \/ v.gen <= Suspicions \/ LeaderReplaced(v) \/ (Restarts /\ v.leader \in halted)
    /\ Restarts => f \notin halted
    /\ f \notin out
    \* Failover never names a copy placement holds as a learner: it may hold
    \* none of what a majority acknowledged, and its own fence would count it.
    \* Placement reads only what it was told (`cpLearner`), never the copy's
    \* own mark; `PlacementKnowsLearners` checks the one covers the other.
    /\ ~PromoteNonVoter => f \notin cpLearner
    \* Nor a changed copy placement has not admitted again (`CopyPins`).
    /\ f \notin changed
    \* With it off, a `Quorum` failover never names the destination: it ends
    \* the move instead (`promote` in services/felix-controlplane-service/src/
    \* cluster/placement/plan.rs).
    /\ Quorum /\ ~PromoteDestination => ~Incoming(f)
    /\ CASE Promotion = "leader-report" -> ByLeaderReport(v.report, f) \/ ByProbe(v, f)
          [] Promotion = "log-order"     -> ByLogOrder(v.leader, f)
          [] Promotion = "any"           -> ByAny(f)
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ leader' = f
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![f] = IF TakesNaming(f) THEN gen + 1 ELSE @]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = IF TakesNaming(f) THEN clock[f] + L ELSE @]
    /\ queued' = [queued EXCEPT ![f] = IF TakesNaming(f) THEN 0 ELSE @]
    /\ pending' = [pending EXCEPT ![f] = IF TakesNaming(f) THEN 0 ELSE @]
    /\ report' = NoReport
    /\ draining' = FALSE
    \* Being deposed is per generation (`deposed_at` in fence.rs): a refused
    \* naming leaves it.
    /\ stopped' = [stopped EXCEPT ![f] = IF TakesNaming(f) THEN FALSE ELSE @]
    \* A promoted destination leads, so it is part of the set from here.
    /\ staged' = staged \ {f}
    \* What `choose_replicas` in placement/plan.rs would write: the old set
    \* without the dead leader, plus a spare. `keep_replicas` keeps the set,
    \* without a replacement still joining unless it is `Counted`, and with
    \* any of `SeatDropped`; failover ends the replacement either way.
    /\ IF ReplaceOnPromote /\ out /= {}
       THEN \E d \in out :
              /\ out' = (out \ {d}) \cup {v.leader}
              /\ mine' = [mine EXCEPT ![f] = IF TakesNaming(f) THEN (ReplicaSet \ {v.leader}) \cup {d} ELSE @]
              /\ leaving' = {}
       ELSE \E kept \in SUBSET SeatDropped :
              /\ mine' = [mine EXCEPT ![f] = IF TakesNaming(f) THEN Counted \cup {f} \cup kept ELSE @]
              /\ out' = (out \cup (joining \ (Counted \cup {f}))) \ kept
              \* One not kept may still be at the next failover.
              /\ leaving' = SeatDropped \ kept
    \* The assignment writes the set at the new generation; its fence carries it.
    /\ setGen' = IF SetCheck /\ TakesNaming(f) THEN [setGen EXCEPT ![f] = gen + 1] ELSE setGen
    \* A change still pending at `f` stays pending, and a set `f` did not know
    \* is a change from the one it did (`FailoverOldSets`).
    /\ LET kept == IF KeepsUnopened /\ ~HoldsRecordOf(f, setGen[f])
                   THEN oldSet[f] \ {{}} ELSE {}
           left == (FailoverOldSets(mine[f]) \cup kept) \ {mine'[f]}
       IN oldSet' = IF SetCheck /\ TakesNaming(f) /\ left /= {} THEN [oldSet EXCEPT ![f] = left] ELSE oldSet
    \* A failover writes the control plane's set at the new generation, which
    \* abandons a cut-over's pending change.
    /\ cpNext' = {} /\ cpNextGen' = 0
    /\ RecordLeft(CpSet)
    /\ cutOver' = IF SplitAtFailover /\ cutOver = <<>> THEN <<FailoverTag(f)>> ELSE cutOver
    /\ UNCHANGED << installed, ackGen, waits, splits, stands >>
    /\ joining' = {}
    /\ joinedAt' = 0
    \* The new leader persists its generation itself first; with
    \* `FenceOnPromote` it then fences the others before it serves. Under
    \* `Elections` it may already have promised that generation, or a later
    \* one, to a candidate: the assignment is written all the same, the
    \* broker's open is refused, and it reports that (`RenameDue`).
    /\ IF Promises /\ TakesNaming(f) THEN Promise(f, f, gen + 1) ELSE UNCHANGED << promised, ballot >>
    /\ IF PromotedUnfenced(f) /\ TakesNaming(f) THEN Opens(f, gen + 1) ELSE UNCHANGED opened
    /\ fencing' = [fencing EXCEPT ![f] = IF TakesNaming(f) THEN ~PromotedUnfenced(f) ELSE @]
    /\ answered' = [answered EXCEPT ![f] = IF TakesNaming(f) THEN {} ELSE @]
    \* Cursors belong to a generation: the new leader starts with none.
    /\ confirmed' = [confirmed EXCEPT ![f] = IF TakesNaming(f) THEN [m \in Brokers |-> 0] ELSE @]
    \* A fenced leader may still take another log; it writes its start record
    \* when it opens.
    /\ log' = IF PromotedUnfenced(f) /\ TakesNaming(f) THEN [log EXCEPT ![f] = Opened(f, gen + 1)] ELSE log
    \* The counter log likewise; a cache shard is never a move's destination.
    /\ clog' = IF Counters /\ ~FenceOnPromote THEN [clog EXCEPT ![f] = COpened(f, gen + 1)] ELSE clog
    /\ cconfirmed' = [cconfirmed EXCEPT ![f] = IF TakesNaming(f) THEN [m \in Brokers |-> 0] ELSE @]
    /\ canswered' = [canswered EXCEPT ![f] = IF TakesNaming(f) THEN {} ELSE @]
    \* The assignment carries the identities the report named, for the fence
    \* (`fence_storage_ids`). Kept only where identities can change.
    /\ expect' = IF FencedIds THEN [expect EXCEPT ![f] = [m \in Brokers |-> Named(v.report, m)]]
                                ELSE expect
    /\ inherited' = [inherited EXCEPT ![f] = { m \in Brokers : Inherits(v.report, m) }]
    /\ fenceIds' = IF RefuseShortCopy THEN [fenceIds EXCEPT ![f] = FenceIds(mine'[f])] ELSE fenceIds
    \* Under `CopyPins` the pins name the copies placement last admitted, so a
    \* changed one is not counted. They are the assignment's, so they stand
    \* even when `f` cannot take the naming and is renamed.
    /\ IF CopyPins
       THEN /\ pinned' = pinned \cup {f}
            /\ stalePin' = [stalePin EXCEPT ![f] = changed]
       ELSE UNCHANGED << pinned, stalePin >>
    /\ UNCHANGED << changed, changes, admitting >>
    \* The new leader's cursors are its generation's: under `UnprobedStart` it
    \* knows no follower's identity until it asks (`Probe`), like the first
    \* leader. The model lets it ask while it fences, which the code does only
    \* once open; a probe changes nothing the fence reads, so that adds no
    \* outcome.
    /\ IF FencedIds /\ UnprobedStart
       THEN /\ seen' = [seen EXCEPT ![f] = [m \in Brokers |-> IF m = f THEN seen[f][f] ELSE Unnamed]]
            /\ seenInc' = [seenInc EXCEPT ![f] = [m \in Brokers |-> IF m = f THEN seenInc[f][f] ELSE 0]]
       ELSE UNCHANGED << seen, seenInc >>
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

\* With `FenceChecksIds`, on the fenced path where identities can change,
\* whether `b` counts `f`'s fence answer: only from the identity it expects
\* for `f`. Another is a copy that may have lost what it answered for
\* before, so it is neither counted nor taken from. Its promise still refuses
\* the old leader, but cannot count alone: the majority that took the fence
\* is also what the catch-up trusts to hold every acknowledged record. A
\* replica the report named no identity for (`Unnamed`) gave its leader
\* every answer after that report, so it does not count either: the code
\* names it with an empty identity, which nothing matches. (It leaves one
\* registered without an identity unnamed, and counts it, which the model's
\* never are.) `UnnamedCounts` counts it, and TLC finds the record lost
\* (FelixShardFencedCrashCountsUnnamed.cfg), as it does naming it by its
\* registration (`NameByRegistration`,
\* FelixShardFencedCrashNamesByRegistration.cfg). Named by the identity the
\* report's leader was itself promoted with (`NameByInheritance`), it counts
\* safely (FelixShardFencedLeaderCrashTwiceNamesByInheritance.cfg, and the
\* walks of FelixShardFencedCrashNamesByInheritance.cfg). The code compares the
\* identity the answer's connection gave in its handshake.
FenceCounts(b, f) ==
    /\ \/ ~(FenceChecksIds /\ FencedIds)
       \/ expect[b][f] = sid[f]
       \/ UnnamedCounts /\ expect[b][f] = Unnamed
    \* Under `CopyPins`, not from a copy other than the one its pin names.
    /\ f \notin stalePin[b]

\* Under `RefuseShortCopy`, whether `f` holds the copy it gave up in a crash,
\* and no newer leader has had it promise since (a ship or a fence answer
\* would have).
ShortCopy(f) == gaveUp[f] > 0 /\ promised[f] = gaveUp[f] /\ bgen[f] = 0

\* Whether a set of two acknowledged everything `f`'s short copy may have
\* lost, `b` the other: `acknowledged_by_two`. The copy's last record is from
\* the generation it led, so its losses are all from there; `b` leads the
\* generation straight after; and `b`'s set is the two, with the fence ids
\* naming exactly them, the previous assignment's set. The code reads the
\* route from `f`'s routing view and ties it neither to the fence's
\* generation nor to `b` leading it; the model keys on `b`'s own, which is
\* narrower (README, "A crashed leader's own refusal").
AckedByTwo(b, f) ==
    /\ LastGen(f) = promised[f]
    /\ bgen[b] = promised[f] + 1
    /\ ShortCopyChecksSet =>
          /\ mine[b] = {b, f}
          /\ ShortCopyChecksKeys => fenceIds[b] = mine[b]

ShortCopyRefuses(b, f) == RefuseShortCopy /\ ShortCopy(f) /\ ~AckedByTwo(b, f)

\* Whether `b`'s fence is a continuing open that has not restarted since.
\* A crash or restart bumps `inc`.
Continuing(b) == contAt[b][1] = bgen[b] /\ contAt[b][2] = inc[b] /\ bgen[b] > 0
ContinuingFence(b) == (ContinuingTrustsBallot \/ ContinuingTrustsAnyCopy) /\ Continuing(b)
\* Whether `f`'s ballot, before it answers, names `b` at the generation `b`
\* served, or at the one it fences (a retried round): what `continues_from`
\* checks and `FenceKeptOk` attests.
ContinuingBallot(b, f) ==
    /\ ContinuingTrustsBallot
    /\ \/ promised[f] = bgen[b] - 1
       \/ ~ContinuingAttestsBelowOnly /\ promised[f] = bgen[b]
    /\ ballot[f] = {b}
    /\ ContinuingKeptBallot => f \notin forgot
\* Whether `f` answers `b`'s continuing fence past the short-copy and
\* non-voter refusals.
ContinuingTrusts(b, f) ==
    /\ Continuing(b)
    /\ ContinuingTrustsAnyCopy \/ ContinuingBallot(b, f)
\* Whether `b`'s continuing fence counts `f`'s answer: from the copy it
\* expects, or attesting the ballot.
ContinuingCounts(b, f) ==
    \/ FenceCounts(b, f) /\ ~ContinuingCopiesNew /\ (ContinuingCountsForgotten \/ f \notin forgot)
    \/ ContinuingTrustsAnyCopy
    \/ ContinuingBallot(b, f)
\* What `b` records when it fences `g` as a continuing open.
ContinuesAt(b, g) ==
    contAt' = IF ~draining THEN [contAt EXCEPT ![b] = <<g, inc[b], 0>>] ELSE contAt

\* Under `ContinuingRetries`, a continuing fence that has answers starts a
\* fresh round, as a lost answer or a Pending round does in the code. Once
\* per open, which `contAt` marks.
RetryContinuingFence(b) ==
    /\ ContinuingRetries
    /\ ContinuingFence(b) /\ fencing[b] /\ answered[b] /= {} /\ contAt[b][3] = 0
    /\ answered' = [answered EXCEPT ![b] = {}]
    /\ contAt' = [contAt EXCEPT ![b] = <<@[1], @[2], 1>>]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    promised, fencing, confirmed, ballot, opened, setVars, ackGen, waits >>
    /\ UNCHANGED handoffVars

\* Whether `b` fences as a leader the control plane named at its generation,
\* on a set written at it, with no change pending: the flip's leader
\* (`CpFlipNamed`), and a promotion that keeps the set, which the flip
\* configurations never reach (no lease lapses).
FlipFence(b) == ~Candidate(b) /\ setGen[b] = bgen[b] /\ oldSet[b] = {} /\ cpNext = {}

\* One replica answers the fence, and the leader takes its log if it is ahead.
\* That is the catch-up: whatever a majority acknowledged is in the answer
\* furthest ahead. The replica's half is `ReplicaHandler::fence` in
\* crates/server/felix-replication/src/replica.rs: the generation is fsynced
\* before the answer, which carries the log's end, its commit offset and its
\* last record's generation, the two halves of `Ahead`.
\*
\* The code also answers a fence at the generation it already promised, as
\* a leader confirming it still leads. With the control plane naming every
\* leader that is only ever the same leader asking again, so the model
\* leaves it out. Under `Elections` it can be a second candidate at that
\* generation, which only `Ballots` refuses.
\*
\* Under `SetCheck` the fence carries the leader's set, as a batch does, and
\* a member of the old set answers a set change's fence under `JointFence`.
\* A leader or candidate that answers stops leading there and then: it has
\* promised a newer generation away, and the set it just learned is not the
\* one its own generation counts. Leading on, it counted a majority of the
\* learned set and acknowledged a record the new set did not hold
\* (FelixShardElectAdopt.cfg, AckedOnMajority).
AnswerFence(b, f) ==
    /\ fencing[b] /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ f \in FenceMembers(b)
    \* A leader that lost the set it leaves asks no one: `fence_shard` in
    \* promotion.rs returns when `joint::sets` cannot find it, before
    \* `fence_log`.
    /\ ~SetUnknown(b)
    \* A changed copy answers only the leader the control plane names.
    /\ f \in changed => ~Candidate(b)
    /\ IF Elections > 0
       THEN \/ MayPromise(f, b, bgen[b])
            \/ NamedFenceSkipsBallot /\ FlipFence(b) /\ promised[f] <= bgen[b]
       ELSE promised[f] < bgen[b]
    /\ ~ShortCopyRefuses(b, f) \/ ContinuingTrusts(b, f)
    \* Stickiness: a replica that still hears its leader refuses a candidate.
    /\ PreVote /\ Candidate(b) => ~Hears(f)
    /\ WipeWaits(f) => ~Candidate(b) /\ WipedTakesNamed
    /\ WipeFloor(f, b, bgen[b])
    \* A non-voter answers no fence, its re-admission's included, as
    \* `refuse_non_voter_fence` does. `ReadmitCountsWiped` lets it answer
    \* that one.
    /\ ~Votes(f, b) =>
          \/ ReadmitCountsWiped /\ readmit = {<<f, bgen[b], b>>}
          \/ ContinuingTrusts(b, f)
          \/ ContinuingForgottenAnswers /\ Continuing(b) /\ f \in forgot
    /\ Promise(f, b, bgen[b])
    /\ SetCurrent(f, b)
    /\ LearnSet(f, b)
    \* A continuing leader takes no log: it already holds every acknowledged
    \* record.
    /\ answered' = IF \/ ContinuingFence(b) /\ ContinuingCounts(b, f)
                      \/ ~ContinuingFence(b) /\ FenceCounts(b, f)
                   THEN [answered EXCEPT ![b] = @ \cup {f}] ELSE answered
    /\ log' = IF FenceCounts(b, f) /\ ~ContinuingFence(b) /\ Ahead(f, b) THEN [log EXCEPT ![b] = log[f]] ELSE log
    /\ IF SetCheck /\ bgen[f] > 0
       THEN /\ bgen' = [bgen EXCEPT ![f] = 0]
            /\ fencing' = [fencing EXCEPT ![f] = FALSE]
            /\ queued' = [queued EXCEPT ![f] = 0]
            /\ pending' = [pending EXCEPT ![f] = 0]
       ELSE UNCHANGED << bgen, fencing, queued, pending >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, hwm, halted, acked, writes, staleCommit,
                    confirmed, opened, installed, nextVars, ackGen, waits >>
    /\ UNCHANGED placeVars

\* Under `SetlessFences`, a member answers the fence of the leader the
\* control plane named with no set in it: no set check and nothing learned.
\* A joint fence always carries its sets (`joint::sets`), so only a fence
\* with no change pending goes without.
AnswerSetlessFence(b, f) ==
    /\ SetlessFences
    /\ fencing[b] /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ ~Candidate(b) /\ oldSet[b] = {} /\ readmit = {}
    /\ f \in Members(b)
    /\ IF Elections > 0 THEN MayPromise(f, b, bgen[b]) ELSE promised[f] < bgen[b]
    /\ ~SetlessNonVoter(f)
    /\ Promise(f, b, bgen[b])
    /\ answered' = IF FenceCounts(b, f) THEN [answered EXCEPT ![b] = @ \cup {f}] ELSE answered
    /\ log' = IF FenceCounts(b, f) /\ Ahead(f, b) THEN [log EXCEPT ![b] = log[f]] ELSE log
    /\ IF SetCheck /\ bgen[f] > 0
       THEN /\ bgen' = [bgen EXCEPT ![f] = 0]
            /\ fencing' = [fencing EXCEPT ![f] = FALSE]
            /\ queued' = [queued EXCEPT ![f] = 0]
            /\ pending' = [pending EXCEPT ![f] = 0]
       ELSE UNCHANGED << bgen, fencing, queued, pending >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, hwm, halted, acked, writes, staleCommit,
                    confirmed, opened, setVars, mine, ackGen, waits >>
    /\ UNCHANGED placeVars

\* A replica that knows a newer set than the one a fence carries refuses it
\* and says which. The leader learns that set and stops standing; it may
\* stand again at a later generation (`Elect`), fenced on that set from the
\* start. Fencing on at the same generation would count one generation on two
\* sets, whose majorities need not meet: FelixShardElectAdopt.cfg found a
\* candidate doing so opening beside another at one generation.
RefuseStaleSet(b, f) ==
    /\ SetCheck
    /\ fencing[b] /\ bgen[b] > 0 /\ f /= b
    /\ f \in FenceMembers(b)
    /\ setGen[f] > setGen[b]
    /\ IF RefusalLearnsSet
       THEN /\ setGen' = [setGen EXCEPT ![b] = setGen[f]]
            /\ mine' = [mine EXCEPT ![b] = mine[f]]
            /\ oldSet' = [oldSet EXCEPT ![b] = oldSet[f]]
       ELSE UNCHANGED << setGen, mine, oldSet >>
    /\ answered' = [answered EXCEPT ![b] = {}]
    /\ bgen' = [bgen EXCEPT ![b] = 0]
    /\ fencing' = [fencing EXCEPT ![b] = FALSE]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit,
                    promised, confirmed, ballot, opened, installed, nextVars, ackGen, waits >>
    /\ UNCHANGED placeVars

\* What `installed` keeps of a set change `b` opens. Under `StintStarts` it
\* also keeps the generation, so a flip on a stale report can tell the named
\* stint's open from an elected stint's at a lower generation.
Installed(b) ==
    IF StintStarts THEN <<setGen[b], mine[b], oldSet[b], bgen[b]>>
    ELSE <<setGen[b], mine[b], oldSet[b]>>

\* `open_promoted` in crates/server/felix-replication/src/driver.rs, on the
\* first majority `fence_shard` in promotion.rs gets; the tail that wins is
\* the answer furthest ahead by `Ahead`, taken before this.
\*
\* A set change under `JointFence` also needs a majority of the old set.
\* Under `SetCheck` it is recorded, for SetChangesJoint, by the set's
\* generation: a leader standing again mid change fences higher on it. The
\* change stays pending, and acknowledgements joint, until the control plane
\* holds the new set (FinishChange).
OpenForWrites(b) ==
    /\ fencing[b]
    \* A leader whose own log took a newer ballot (a standing it withdrew)
    \* does not open (`keep_own_set` in promotion.rs).
    /\ Withdraws => promised[b] <= bgen[b]
    /\ LeaderMajority(b, answered[b] \cup {b})
    /\ JointFence => MajorityOfOld(answered[b] \cup {b}, b)
    /\ OpenChecksOwnBallot /\ ~Candidate(b) => promised[b] = bgen[b] /\ ballot[b] \subseteq {b}
    \* A re-admission counts the old set's majority without the wiped replica.
    /\ \A r \in readmit : r[2] = bgen[b] /\ r[3] = b /\ ~ReadmitCountsWiped =>
           MajorityOfOld((answered[b] \cup {b}) \ {r[1]}, b)
    \* A copy marked wiped does not count its own group log (`GroupLogs`).
    /\ Counters => LeaderMajority(b, canswered[b] \cup (IF GroupLogs /\ marked[b] THEN {} ELSE {b}))
    /\ fencing' = [fencing EXCEPT ![b] = FALSE]
    /\ oldSet' = IF Settled(b) THEN [oldSet EXCEPT ![b] = {}] ELSE oldSet
    /\ installed' = IF oldSet[b] /= {} THEN installed \cup {Installed(b)} ELSE installed
    /\ Opens(b, bgen[b])
    /\ log' = [log EXCEPT ![b] = Opened(b, bgen[b])]
    /\ clog' = IF Counters THEN [clog EXCEPT ![b] = COpened(b, bgen[b])] ELSE clog
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, pending, acked, writes, staleCommit,
                    promised, answered, confirmed, ballot, setGen, nextVars, ackGen, waits >>
    /\ UNCHANGED << handoffVars, chwm, cconfirmed, canswered, cacked >>

\* The open, with what it does to the pins: under `PinsDropAtOpen` the leader
\* drops them once its fence has counted a majority.
OpenStep(b) ==
    /\ OpenForWrites(b)
    /\ IF CopyPins /\ PinsDropAtOpen
       THEN /\ pinned' = pinned \ {b}
            /\ stalePin' = [stalePin EXCEPT ![b] = {}]
       ELSE UNCHANGED << pinned, stalePin >>
    /\ UNCHANGED << heard, idVars, named, refusalVars, wipeVars, changed, changes, admitting >>

\* A leader drops the old set once the control plane holds its set, with no
\* change pending: from here a majority of the new set is enough.
FinishChange(b) ==
    /\ oldSet[b] /= {} /\ bgen[b] > 0 /\ ~fencing[b]
    /\ Settled(b)
    /\ oldSet' = [oldSet EXCEPT ![b] = {}]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    promised, fencing, answered, confirmed, ballot, opened, setGen, installed,
                    nextVars, ackGen, waits >>
    /\ UNCHANGED handoffVars

\* Under `SetLossUpTo`, the leader no longer knows the set its generation
\* leaves: it learned the shard from a snapshot while fencing, or restarted
\* after it opened and fences the same generation again. Its log and
\* generation stay; the fence starts over and cannot open.
ASSUME SetLossUpTo > 0 => SetCheck /\ JointFence /\ TracksOpens
ForgetSet(b) ==
    /\ gen <= SetLossUpTo
    /\ b = leader /\ bgen[b] = gen /\ ~SetUnknown(b)
    /\ SetLossAfterOpen => /\ <<gen, b>> \in opened /\ setGen[b] = gen /\ oldSet[b] /= {}
                           /\ ~(cpNext /= {} /\ cpNextGen = gen)
    /\ fencing' = [fencing EXCEPT ![b] = TRUE]
    /\ answered' = [answered EXCEPT ![b] = {}]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    \* The model does not tell a lost kept set from a kept one, so the mark
    \* is read wherever the set is lost; at the first generation that is
    \* harmless, as nothing came before it.
    /\ oldSet' = [oldSet EXCEPT ![b] =
                     IF FirstAssignMark /\ (gen = 1 \/ (MarkAnyGeneration /\ <<gen, b>> \notin opened))
                     THEN {} ELSE {{}}]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit,
                    promised, confirmed, ballot, opened, setGen, installed,
                    nextVars, ackGen, waits >>
    /\ UNCHANGED handoffVars

\* A yes to `f`'s pre-vote from `m`, under `PreVoteAsked`.
\* The leader `f` follows: the one it last promised, or the control plane's
\* if that was its own standing. A broker never suspects itself.
Followed(f) == IF ballot[f] \subseteq {f} THEN {leader} ELSE ballot[f]

\* `b`'s fence cannot complete from where it stands: too few members can
\* still take its generation.
FenceStuck(b) ==
    LET can == { m \in FenceMembers(b) \ halted :
                   IF m = b THEN promised[m] <= bgen[b] ELSE MayPromise(m, b, bgen[b]) }
    IN \/ ~LeaderMajority(b, can)
       \/ JointFence /\ ~MajorityOfOld(can, b)

\* `c` still stands at `g`: up, fencing there, on its own election.
Standing(c, g) == c \notin halted /\ fencing[c] /\ bgen[c] = g /\ Candidate(c)

\* Under `StandBehindBallot`, `f` may stand above what it promised `c`. It
\* reads `c`'s state directly; code can only learn it late, by a timeout,
\* which `StandBehindBounded` stands for.
FreedFrom(f, c) ==
    \/ c = f \/ c \in halted \/ promised[f] <= gen
    \/ ~StandBehindDeadOnly /\ ~Standing(c, promised[f])
    \/ StandBehindBounded /\ FenceStuck(c)

PreVoteYes(m, f) ==
    /\ m \notin halted /\ ~Hears(m) /\ ~Ahead(m, f)
    /\ ~(bgen[m] > 0 /\ ~fencing[m])
    /\ StandBehindBallot => ~fencing[m] \/ (PreVoteBounded /\ FenceStuck(m))

\* Whether `f` would stand while another's round was open, under `Backoff`.
StandsBeside(f) == \E m \in Brokers \ ({f} \cup halted) : fencing[m] /\ Candidate(m)

\* Pre-vote: a majority of its set, itself included, no longer hears a leader.
\* Under `DetectorMisfires` the candidate's own suspicion may be wrong.
\* The candidate need not be in that majority, so one that still hears may
\* stand: more than the code allows, which only widens the safety checks.
PreVoteWon(f) ==
    PreVote => MajorityOf({ m \in Members(f) : (DetectorMisfires /\ m = f) \/
                             IF PreVoteAsked THEN PreVoteYes(m, f)
                             ELSE ~Hears(m) /\ (FailStops > 0 => m \notin halted) },
                          Members(f))

\* Under `Elections`, a replica stands with no control-plane step, at the
\* generation after the one it promised: it persists its own promise, then
\* fences the set it last knew, and opens as a promoted leader does. A
\* candidate still fencing may stand again higher. Any replica may stand at
\* any time, as a failure detector that is sometimes wrong would let it.
\* Under `SetCheck` only a member of the set it knows stands, and only once
\* it has learned one.
\* Under `StandsAboveCp` it stands above the control plane's generation too,
\* as its route shows it (`StandGens`), the highest of which is `StandTop`.
StandGens(f) ==
    IF StandsAboveCp
    THEN { Max(g, promised[f]) + 1 : g \in IF StaleStandRoute THEN {gen - 1, gen} ELSE {gen} }
    ELSE {promised[f] + 1}
StandTop(f) == IF StandsAboveCp THEN Max(gen, promised[f]) + 1 ELSE promised[f] + 1

\* The model's bound on elections, not the code's.
ElectBound(f) ==
    /\ StandTop(f) <= gen + Elections
    \* With adoption the control plane's generation follows elections up, so
    \* `Adopt` bounds them too, or neither ends.
    /\ Adopt > 0 => StandTop(f) <= Adopt
    /\ MaxStandings > 0 => stands < MaxStandings

ElectAt(f, s) ==
    /\ Elections > 0
    /\ f \notin halted
    \* A changed copy stands only once placement admits it again.
    /\ f \notin changed
    /\ bgen[f] = 0 \/ fencing[f]
    \* With `Backoff` a candidate stands again only once it gave its round up.
    /\ Backoff => bgen[f] = 0
    /\ SetCheck => f \in mine[f] /\ setGen[f] > 0
    /\ StandBehindBallot => \A c \in ballot[f] : FreedFrom(f, c)
    \* A replica with a round in flight does not start another (`in_flight`
    \* in election/candidate.rs).
    /\ BrokerSuspicion => bgen[f] = 0
    /\ BrokerSuspicion => \/ Followed(f) \subseteq halted
                          \/ ShardSuspicion /\ f \notin Followed(f) /\ ~Hears(f)
                          \/ SelfSuspicion /\ Followed(f) = {f} /\ bgen[f] = 0 /\ ~Hears(f)
    \* Not while a change to its kept set is pending: a set it is leaving may
    \* differ from it in learner flags only (`stand` in crates/server/
    \* felix-replication/src/election/candidate.rs refuses "the kept replica
    \* set has a change pending"; its "keeps earlier sets" refusal is not
    \* modelled, as no configuration keeps earlier sets). Across a change
    \* the control plane names the leader. The code takes the ballot only on
    \* the set those checks read (`accept_generation_on_set`), so the guard
    \* and the promise are one step, as here. Its further check, that the
    \* routes record the kept set, is left out: that only adds behaviours.
    /\ SetCheck => StandsOnPendingSet \/ oldSet[f] \subseteq {mine[f]}
    /\ StandsAboveKeptSet => setGen[f] <= s
    /\ ~WipeWaits(f) /\ WipeFloor(f, f, s) /\ ~NonVoter(f)
    /\ PreVoteWon(f)
    /\ Backoff /\ StandsBeside(f) => splits < Splits
    /\ splits' = IF Backoff /\ StandsBeside(f) THEN splits + 1 ELSE splits
    /\ stands' = IF MaxStandings > 0 THEN stands + 1 ELSE stands
    /\ bgen' = [bgen EXCEPT ![f] = s]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = clock[f] + L]
    /\ queued' = [queued EXCEPT ![f] = 0]
    /\ pending' = [pending EXCEPT ![f] = 0]
    /\ Promise(f, f, s)
    /\ fencing' = [fencing EXCEPT ![f] = TRUE]
    /\ answered' = [answered EXCEPT ![f] = {}]
    /\ confirmed' = [confirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    \* Counter-fence answers from an earlier generation do not count at this one.
    /\ canswered' = [canswered EXCEPT ![f] = {}]
    /\ cconfirmed' = [cconfirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    \* A replica standing while a set change is pending fences the old set too
    \* (under `SetCheck`, only one with the same copies stands).
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, hbOut, hbAt,
                    log, hwm, halted, acked, writes, staleCommit, opened, ackGen, waits,
                    setGen, oldSet, installed, cpNext, cpNextGen, cutOver, cpLeft >>
    /\ UNCHANGED handoffVars

ElectStep(f) == \E s \in StandGens(f) : ElectAt(f, s)

Elect(f) == ElectBound(f) /\ ElectStep(f)

\* Under `Backoff`, a candidate whose round failed gives it up and waits: the
\* answers it has and those still possible make no majority. A member can no
\* longer answer once it is down, promised past the candidate or hears a
\* leader. It keeps what it promised.
GiveUpRound(b) ==
    /\ Backoff
    /\ fencing[b] /\ bgen[b] > 0 /\ Candidate(b)
    /\ LET can == answered[b] \cup {b} \cup
                  { m \in FenceMembers(b) \ halted :
                      MayPromise(m, b, bgen[b]) /\ ~(PreVote /\ Hears(m)) }
       IN \/ ~LeaderMajority(b, can)
          \/ JointFence /\ ~MajorityOfOld(can, b)
    /\ bgen' = [bgen EXCEPT ![b] = 0]
    /\ fencing' = [fencing EXCEPT ![b] = FALSE]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry, hbOut, hbAt,
                    log, hwm, halted, acked, writes, staleCommit, promised, answered,
                    confirmed, ballot, opened, setVars, ackGen, waits >>
    /\ UNCHANGED handoffVars

\* Under `RouteLags`, a candidate round on a broker the control plane just
\* promoted, which read the route from before the promotion: that route names
\* another leader, so the round stands at one past the ballot the promotion
\* took, on its own copy, and withdraws when its fence gets nowhere, keeping
\* the promise. The leader role starts once the route lands, still at the
\* control plane's generation, now below its own copy's ballot (#1933). The
\* window is the promotion's fence before anyone has answered it. The stand
\* and the withdrawal are one step that fences nobody, the run where every
\* other member refused. Under `StaleFenceAnswered` one member may answer it
\* first, its reply arriving after the round gave up, which leaves that
\* member promised above the leader's generation too (#1940). Only a member
\* not leading and already on the round's set answers, so it learns nothing;
\* in the code it would learn the set. It is also allowed after a rename,
\* whose old route names this broker in the code, which only adds
\* behaviours; `Adopt` bounds the repeats.
StaleAnswerers(f) ==
    { m \in Members(f) \ ({f} \cup halted) :
        /\ bgen[m] = 0 /\ MayPromise(m, f, gen + 1)
        /\ PreVote => ~Hears(m)
        /\ setGen[m] = setGen[f] /\ mine[m] = mine[f] /\ oldSet[m] = oldSet[f] }

StandOnStaleRoute(f) ==
    /\ RouteLags /\ ~NamedHereWaits
    /\ Elections > 0
    /\ f = leader /\ f \notin halted
    /\ bgen[f] = gen /\ fencing[f] /\ answered[f] = {}
    /\ promised[f] = gen /\ ballot[f] = {f}
    /\ Adopt > 0 => gen < Adopt
    \* The round runs `stand`'s set checks, as `Elect` does.
    /\ SetCheck => StandsOnPendingSet \/ oldSet[f] \subseteq {mine[f]}
    /\ PreVoteWon(f)
    /\ \E took \in {{}} \cup (IF StaleFenceAnswered THEN { {m} : m \in StaleAnswerers(f) } ELSE {}) :
           /\ promised' = [m \in Brokers |-> IF m \in took \cup {f} THEN gen + 1 ELSE promised[m]]
           /\ ballot' = IF Ballots THEN [m \in Brokers |-> IF m \in took \cup {f} THEN {f} ELSE ballot[m]]
                        ELSE ballot
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered, confirmed, opened, setVars, ackGen, waits >>

\* Under `FailStops`, a broker stops for good, leader or not.
FailStop(b) ==
    /\ Cardinality(halted) < FailStops
    /\ b \notin halted
    /\ Restarts => inc[b] = 0
    /\ Cardinality({m \in Brokers : inc[m] > 0}) < Stops
    /\ halted' = halted \cup {b}
    /\ bgen' = [bgen EXCEPT ![b] = 0]
    /\ fencing' = [fencing EXCEPT ![b] = FALSE]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry, hbOut, hbAt,
                    log, hwm, acked, writes, staleCommit, promised, answered,
                    confirmed, ballot, opened, setVars, ackGen, waits >>
    /\ UNCHANGED handoffVars

\* Under `Withdraws`, a candidate that has not opened gives its standing up
\* (`OPEN_WITHIN`), keeping its ballot. The control plane's leader that
\* stood above its assignment and never opened there goes back to fencing
\* at it, or its open there was refused.
Withdraw(b) ==
    /\ Withdraws
    /\ fencing[b] /\ bgen[b] > 0 /\ Candidate(b)
    /\ WithdrawsStuck => FenceStuck(b)
    /\ \E back \in IF b = leader /\ gen < bgen[b] /\ <<gen, b>> \notin opened
                    THEN {0, gen} ELSE {0} :
         /\ bgen' = [bgen EXCEPT ![b] = back]
         /\ fencing' = [fencing EXCEPT ![b] = back > 0]
    /\ answered' = [answered EXCEPT ![b] = {}]
    /\ confirmed' = [confirmed EXCEPT ![b] = [m \in Brokers |-> 0]]
    /\ queued' = [queued EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry, hbOut, hbAt,
                    log, hwm, halted, acked, writes, staleCommit, promised,
                    ballot, opened, setVars, ackGen, waits >>
    /\ UNCHANGED handoffVars

\* Under `Withdraws`, unless `DeposeOnAssigned`: an open leader one of
\* whose followers holds a newer ballot stops serving the generation, though
\* the rest may still be a majority (`depose` in shards/lifecycle/fence.rs).
Depose(b) ==
    /\ Withdraws /\ ~DeposeOnAssigned
    /\ bgen[b] > 0 /\ ~fencing[b] /\ ~stopped[b]
    /\ \E m \in Members(b) \ {b} : promised[m] > bgen[b]
    /\ stopped' = [stopped EXCEPT ![b] = TRUE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry, hbOut, hbAt,
                    log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << draining, successor, moves, ver, cpView, staged, out, mine, joining, leaving,
                    joinedAt, fenceVars >>

\* Under `Restarts`, a stopped broker starts again with what it persisted, as
\* a new incarnation. The control plane's leader opens its assignment, which
\* it takes unless it promised the generation away; then it reports the
\* refusal (`RenameDue`). Being deposed is held in memory (`Gate` in
\* shards/lifecycle/fence.rs), so a restarted broker starts undeposed.
Restart(b) ==
    /\ Restarts
    /\ b \in halted
    /\ halted' = halted \ {b}
    /\ inc' = [inc EXCEPT ![b] = @ + 1]
    /\ IF b = leader /\ ~draining /\ MayPromise(b, b, gen)
       THEN /\ bgen' = [bgen EXCEPT ![b] = gen]
            /\ bexpiry' = [bexpiry EXCEPT ![b] = clock[b] + L]
            /\ fencing' = [fencing EXCEPT ![b] = TRUE]
            /\ answered' = [answered EXCEPT ![b] = {}]
            /\ confirmed' = [confirmed EXCEPT ![b] = [m \in Brokers |-> 0]]
            /\ Promise(b, b, gen)
            \* Its fence carries the assignment's set at the assignment's
            \* generation (`replica_set::view` in `fence_shard`, promotion.rs).
            \* Under `JointFence` it learns the set right before from the
            \* snapshot its watch reseeds from, which carries the control
            \* plane's assignment before the current one (`previous` on a
            \* snapshot item, #2024), so an older kept set no longer leaves
            \* the fence pending (#1989). Under `PriorMayBeUnknown` the
            \* snapshot may lack it, and then only a log keeping the set right
            \* before will do. A set kept at the generation is reported unknown.
            /\ \E known \in IF PriorMayBeUnknown THEN {TRUE, FALSE} ELSE {TRUE} :
                 IF SetCheck /\ setGen[b] < gen /\ (JointFence /\ ~known => setGen[b] = gen - 1)
                 THEN /\ mine' = [mine EXCEPT ![b] = IF cpNext = {} THEN CpSet ELSE cpNext]
                      /\ setGen' = [setGen EXCEPT ![b] = gen]
                      /\ oldSet' = [oldSet EXCEPT ![b] = IF cpNext = {} THEN {} ELSE {CpSet}]
                 ELSE UNCHANGED << mine, setGen, oldSet >>
       ELSE UNCHANGED << bgen, bexpiry, fencing, answered, confirmed, promised, ballot,
                         mine, setGen, oldSet >>
    /\ stopped' = [stopped EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, hbOut, hbAt,
                    log, hwm, queued, pending, acked, writes, staleCommit,
                    opened, installed, nextVars, ackGen, waits >>
    /\ UNCHANGED << draining, successor, moves, ver, cpView, staged, out, joining, leaving,
                    joinedAt >>
    /\ UNCHANGED << sid, seen, seenInc, wipes, checked, registered, restarting, expect, inherited,
                    leftDirty, gaveUp, fenceIds >>

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
    \* While a change is pending the model ships to the old set too, which
    \* only widens the check: the code ships a counter log to the route's set
    \* alone (`shard.rs`).
    /\ f \in FenceMembers(b)
    \* A non-voter copies the counter log in `CatchUp`, which counts nothing.
    /\ ~NonVoter(f)
    /\ bgen[f] = 0
    /\ bgen[b] >= CLastGen(f)
    /\ ~fencing[b]
    /\ MayPromise(f, b, bgen[b])
    /\ Promise(f, b, bgen[b])
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
    /\ UNCHANGED << handoffVars, fencing, answered, confirmed, opened, setVars, ackGen, waits, chwm, canswered, cacked >>

\* As `HeldAtGen`, on the counter log: the counter mark counts only past a
\* record of the leader's own generation (`quorum::counted_offset`).
CHeldAtGen(b, i) ==
    LET held == { m \in Brokers \ {b} : cconfirmed[b][m] >= i }
                \cup (IF promised[b] = bgen[b] THEN {b} ELSE {})
    IN /\ StartRecord => clog[b][i].g = bgen[b]
       /\ LeaderMajority(b, held)
       /\ CountersJoint /\ JointFence => MajorityOfOld(held, b)

\* Whether `b`'s counter log has its start record at generation `g` under
\* the counter mark, for a flip under `FlipChecksCounters`.
CStartHeld(b, g) ==
    Counters /\ FlipChecksCounters =>
        \E i \in 1..chwm[b] : i <= Len(clog[b]) /\ clog[b][i] = Start(g)

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
    /\ CounterFenceSeesLearner => ~NonVoter(f)
    /\ GroupLogs => ~marked[f]
    /\ MayPromise(f, b, bgen[b])
    /\ Promise(f, b, bgen[b])
    /\ canswered' = [canswered EXCEPT ![b] = @ \cup {f}]
    /\ clog' = IF CounterCatchUp /\ CAhead(f, b) THEN [clog EXCEPT ![b] = clog[f]] ELSE clog
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered, confirmed, opened, setVars, ackGen, waits, chwm, cconfirmed, cacked >>

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
    /\ cpNext = {}
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

\* `f` takes the shard at a cut-over or a hand-back. It persists its
\* generation first (`accept_generation` in `open`, lifecycle.rs), which under
\* `Elections` it cannot do once it promised that generation to a candidate,
\* and its cursors start empty at the new generation. Without
\* `FenceEveryChange` it then opens with its start record; with it, it
\* fences and catches up first, as a promoted leader does.
Takes(f) ==
    /\ Elections > 0 => MayPromise(f, f, gen + 1)
    /\ IF Promises THEN Promise(f, f, gen + 1) ELSE UNCHANGED << promised, ballot >>
    /\ confirmed' = [confirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    \* A move never changes the set: spares are checked without one.
    /\ UNCHANGED << setVars, ackGen, waits >>
    /\ IF FenceEveryChange
       THEN /\ fencing' = [fencing EXCEPT ![f] = TRUE]
            /\ answered' = [answered EXCEPT ![f] = {}]
            /\ UNCHANGED << log, opened >>
       ELSE /\ log' = [log EXCEPT ![f] = Opened(f, gen + 1)]
            /\ Opens(f, gen + 1)
            /\ UNCHANGED << fencing, answered >>

CutOver(v, f, views) ==
    /\ v.draining /\ v.successor = f
    /\ cpNext = {}
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
    /\ Takes(f)
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, acked, writes,
                    staleCommit, successor, moves, out, mine, joining, leaving, joinedAt >>

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
    /\ cpNext = {}
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
    /\ Takes(f)
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, queued, pending,
                    acked, writes, staleCommit, successor, moves, staged, out, mine,
                    joining, leaving, joinedAt >>

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
    IN IF held = {} THEN 0 ELSE CHOOSE k \in held : \A j \in held : j <= k

\* The leader, still serving, moves to the next generation with `set`. It
\* opens there at once, as the broker does. Under `SetCheck` it fences first,
\* on the new set, carrying the new set's generation, and with `JointFence`
\* on a majority of the old set too. Under `Elections` it may already have
\* promised that generation to a candidate, and then it does not take it.
RegenerateSet(v, views, set) ==
    /\ v.leader = leader /\ ~v.lapsed
    /\ bgen[leader] = gen /\ ~fencing[leader]
    \* Placement changes one set at a time; a cut-over's pending set comes first.
    /\ cpNext = {}
    /\ Elections > 0 => MayPromise(leader, leader, gen + 1)
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ bgen' = [bgen EXCEPT ![leader] = gen + 1]
    /\ Promise(leader, leader, gen + 1)
    /\ confirmed' = [confirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
    /\ report' = NoReport
    /\ mine' = [mine EXCEPT ![leader] = set]
    /\ ContinuesAt(leader, gen + 1)
    /\ IF SetCheck
       THEN /\ setGen' = [setGen EXCEPT ![leader] = gen + 1]
            /\ fencing' = [fencing EXCEPT ![leader] = TRUE]
            /\ answered' = [answered EXCEPT ![leader] = {}]
            /\ oldSet' = [oldSet EXCEPT ![leader] = {mine[leader]} \cup (IF FenceRecordedSet THEN {CpSet} \ {set} ELSE {})]
            /\ UNCHANGED << log, opened >>
       ELSE /\ Opens(leader, gen + 1)
            /\ log' = [log EXCEPT ![leader] = Opened(leader, gen + 1)]
            /\ UNCHANGED << setGen, oldSet, fencing, answered >>
    /\ UNCHANGED << installed, nextVars, ackGen, waits >>
    /\ UNCHANGED << now, clock, leader, cpExpiry, inflight, bexpiry, hbOut, hbAt, hwm, halted,
                    queued, pending, acked, writes, staleCommit, staged >>

Regenerate(v, views, set) ==
    /\ RegenerateSet(v, views, set)
    /\ UNCHANGED << draining, successor, stopped >>

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
    \* Short of the factor, which is odd: a full set does not grow.
    /\ Cardinality(ReplicaSet) % 2 = 0
    /\ moves < MaxMoves
    /\ joining = {}
    /\ d \in out
    /\ Regenerate(v, views, mine[leader] \cup {d})
    /\ joining' = {d}
    /\ out' = out \ {d}
    /\ leaving' = {}
    /\ joinedAt' = OldSetLen(leader)
    /\ moves' = moves + 1

\* With `SeatHoldsCopy`, a seat into an even set first fences the leader
\* (`seat_fence` in placement/moves.rs), so its log stops growing.
SeatFence(v, j, views) ==
    /\ SeatHoldsCopy /\ SeatsIntoEven
    /\ j \in joining
    /\ HoldsPrefix(j, leader, joinedAt)
    /\ ~v.draining /\ ~v.lapsed /\ v.leader = leader
    /\ cpNext = {}
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ draining' = TRUE
    /\ successor' = leader
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry, hbOut,
                    hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit, stopped,
                    moves, staged, out, mine, joining, leaving, joinedAt >>
    /\ UNCHANGED fenceVars

\* The leaving follower goes. With `SeatHoldsCopy`, only once the newcomer
\* holds what a majority of the set held when it joined; a record
\* acknowledged since is on three of the four, and survives losing one.
\* Into an even set, only once the fenced leader reports drained with the
\* newcomer holding its whole log (`seat_after_fence`), and the leader
\* serves again at the next generation.
Seat(v, j, views) ==
    /\ j \in joining
    /\ SeatHoldsCopy => HoldsPrefix(j, leader, joinedAt)
    /\ IF SeatHoldsCopy /\ SeatsIntoEven
       THEN /\ v.draining /\ v.successor = v.leader
            /\ v.report.gen = v.gen /\ v.report.drained /\ j \in v.report.holders
            /\ RegenerateSet(v, views, mine[leader] \ leaving)
            /\ draining' = FALSE
            /\ stopped' = [stopped EXCEPT ![leader] = FALSE]
            /\ UNCHANGED successor
       ELSE Regenerate(v, views, mine[leader] \ leaving)
    /\ out' = out \cup leaving
    /\ joining' = {}
    /\ joinedAt' = 0
    \* `leaving` stays, for `SeatDropped`.
    /\ UNCHANGED << moves, leaving >>

\* With `Undoes`, a replacement given up beside an even set first fences the
\* leader (`undo_or_fence` in placement/moves.rs): the leader and the copy
\* joining alone are a majority of the set it joined, so dropping the copy
\* while the leader serves can leave a record on the leader only (#1628).
UndoFence(v, j, views) ==
    /\ Undoes /\ ~UndoUnfenced
    /\ j \in joining
    /\ Cardinality(ReplicaSet) % 2 = 0
    /\ ~v.draining /\ ~v.lapsed /\ v.leader = leader
    /\ cpNext = {}
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ draining' = TRUE
    /\ successor' = leader
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry, hbOut,
                    hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit, stopped,
                    moves, staged, out, mine, joining, leaving, joinedAt >>
    /\ UNCHANGED fenceVars

\* The copy joining goes and the set is the one it joined. Beside an even
\* set, only once the fenced leader reports drained with every other
\* follower holding its whole log (`seat_after_fence`), and the leader serves
\* again at the next generation. A failover before the leader reports again
\* may keep the dropped copy, as after a seat.
Undo(v, j, views) ==
    /\ Undoes
    /\ j \in joining
    /\ IF Cardinality(ReplicaSet) % 2 = 0 /\ ~UndoUnfenced
       THEN /\ v.draining /\ v.successor = v.leader
            /\ v.report.gen = v.gen /\ v.report.drained
            /\ (mine[leader] \ {leader, j}) \subseteq v.report.holders
            /\ RegenerateSet(v, views, mine[leader] \ {j})
            /\ draining' = FALSE
            /\ stopped' = [stopped EXCEPT ![leader] = FALSE]
            /\ UNCHANGED successor
       ELSE /\ ~v.draining
            /\ Regenerate(v, views, mine[leader] \ {j})
    /\ out' = out \cup {j}
    /\ joining' = {}
    /\ leaving' = {j}
    /\ joinedAt' = 0
    /\ UNCHANGED moves

\* The followers report `r` lets a trim drop: every other follower is in it
\* caught up (`trim_candidate` in placement/moves.rs).
TrimDroppable(r) ==
    { o \in mine[leader] \ {leader} : (mine[leader] \ {leader, o}) \subseteq r.holders }

\* The leader of a set above the factor is fenced, as for a move, once a
\* report at this generation shows a follower the rest can do without
\* (`trim_fence`). The trim names no destination: `successor` is the leader.
TrimFence(v, views) ==
    /\ OverFactor
    /\ ~v.draining /\ ~v.lapsed /\ v.leader = leader
    /\ cpNext = {}
    /\ v.report.gen = v.gen
    /\ TrimDroppable(v.report) /= {}
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ draining' = TRUE
    /\ successor' = leader
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry, hbOut,
                    hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit, stopped,
                    moves, staged, out, mine, joining, leaving, joinedAt >>
    /\ UNCHANGED fenceVars

\* Once the fenced leader reports drained, a follower goes whose leaving
\* leaves every other one holding the leader's whole log, and the leader
\* serves again at the next generation (`trim`). Judged from the report, as
\* the code judges it: while the leader served, a record could be
\* acknowledged after the report on the leader and the copy that goes alone.
\* A failover before the leader reports again may keep `o`, as after a seat.
TrimSet(v, o, views) ==
    /\ OverFactor
    /\ v.draining /\ v.successor = v.leader
    /\ v.report.gen = v.gen /\ v.report.drained
    /\ o \in TrimDroppable(v.report)
    /\ RegenerateSet(v, views, mine[leader] \ {o})
    /\ draining' = FALSE
    /\ stopped' = [stopped EXCEPT ![leader] = FALSE]
    /\ out' = out \cup {o}
    /\ leaving' = {o}
    /\ UNCHANGED << successor, moves, joining, joinedAt >>

\* Whether `b` leads the set the control plane records, or while a cut-over
\* is pending, the pending set at its generation.
CpHoldsSetOf(b) ==
    IF cpNext = {} THEN mine[b] = CpSet
    ELSE mine[b] = cpNext /\ setGen[b] = cpNextGen

\* What a broker takes when the control plane names it at a new generation.
takeVars == << bgen, bexpiry, queued, pending, stopped, promised, ballot, fencing,
               answered, confirmed, mine, setGen, oldSet >>

\* The write naming `b`, elected at `g` on `E`, at `g + 1` in the control
\* plane's set `S` (`reissuing` in services/felix-controlplane-service/src/
\* model/shard.rs). With `pends` and `E /= S`, a majority of `E` may hold
\* records no majority of `S` does, so `E` becomes the recorded set and `S`
\* the pending one, as a cut-over writes them (Swap). The leader ships to
\* the pending set alone, so that change completes only if `S` and `b` hold
\* a majority of `E`; otherwise `E` is named alone. Without `pends` the
\* control plane keeps `S` and `b` fences `E` beside it (`logged_besides` in
\* crates/server/felix-replication/src/promotion/joint.rs).
\* `b` takes the new generation only if it may still promise it
\* (`accept_led_generation` in services/felix-broker-service/src/shards/
\* lifecycle.rs); otherwise it never opens there.
ReissueAt(b, g, E, S, pends) ==
    LET joint == pends /\ E /= S /\ 2 * Cardinality(E \cap (S \cup {b})) > Cardinality(E)
        alone == pends /\ E /= S /\ ~joint
    IN
    /\ gen' = g + 1
    /\ leader' = b
    /\ cpExpiry' = now + L
    /\ report' = NoReport
    /\ draining' = FALSE
    /\ IF pends /\ E /= S THEN out' = Brokers \ (E \cup staged) ELSE UNCHANGED out
    /\ IF joint THEN cpNext' = S /\ cpNextGen' = g + 1 /\ UNCHANGED << cutOver, splits, stands, cpLeft >>
       ELSE UNCHANGED nextVars
    /\ IF MayPromise(b, b, g + 1)
       THEN /\ bgen' = [bgen EXCEPT ![b] = g + 1]
            /\ bexpiry' = [bexpiry EXCEPT ![b] = clock[b] + L]
            /\ queued' = [queued EXCEPT ![b] = 0]
            /\ pending' = [pending EXCEPT ![b] = 0]
            /\ stopped' = [stopped EXCEPT ![b] = FALSE]
            /\ Promise(b, b, g + 1)
            /\ fencing' = [fencing EXCEPT ![b] = TRUE]
            /\ answered' = [answered EXCEPT ![b] = {}]
            /\ confirmed' = [confirmed EXCEPT ![b] = [m \in Brokers |-> 0]]
            /\ IF SetCheck
               THEN /\ mine' = [mine EXCEPT ![b] = IF alone THEN E ELSE S]
                    /\ setGen' = [setGen EXCEPT ![b] = g + 1]
                    /\ oldSet' = [oldSet EXCEPT ![b] = IF E /= S /\ ~alone THEN {E} ELSE {}]
               ELSE UNCHANGED << mine, setGen, oldSet >>
       ELSE UNCHANGED takeVars

\* Under `Adopt`, the control plane hears from a leader that opened above the
\* generation it read, one it never named, in one conditional write (`Cas`)
\* (`adopting_elected_set` in services/felix-controlplane-service/src/model/
\* shard.rs). With `AdoptMatch`:
\* - if the leader's set is the one the control plane records, or while a
\*   cut-over is pending the pending set at its generation, and the leader
\*   is in the recorded set and not a joining copy, it becomes the control
\*   plane's leader at that generation, the change staying pending until
\*   CpFlip;
\* - if it is the control plane's own leader, elected on the set being left
\*   while a change is pending, it opened past the change, which only it
\*   could take, so the change is dropped;
\* - if it is in the recorded set but was elected on another, that set is
\*   written as the set being left and the recorded one as pending from it,
\*   at the next generation (ReissueAt), because a majority of the elected
\*   set may hold records no majority of the recorded one does;
\* - if it is outside the recorded set, its set becomes the recorded set at
\*   its generation.
\* Under `ReissueOwnOnOtherSet` that includes the control plane's own leader.
\* Otherwise (the control plane's leader on another set, a change pending or
\* a copy joining) only the generation is taken, so every later one the
\* control plane issues is above it. A leader on the old set of a pending
\* change would be fenced by the new set's higher set generation.
\* Without `AdoptMatch` it adopts whatever set the leader has.
\* Bounded by `Adopt`, or elections and adoptions never end.

CpAdopt(v, b, views) ==
    /\ bgen[b] > v.gen /\ bgen[b] <= Adopt
    /\ ~fencing[b]
    /\ <<bgen[b], b>> \in opened
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ LET g == bgen[b]
           E == mine[b]
           S == CpSet
           \* The control plane's own leader opened past a pending change on
           \* the set being left. Only it could take the change, so the
           \* change is dropped.
           skipped == cpNext /= {} /\ leader = b /\ joining = {} /\ E = S
           recordedOnly == \/ leader = b /\ ~(ReissueOwnOnOtherSet /\ E /= S)
                           \/ cpNext /= {} \/ joining /= {}
                           \/ ~SetCheck \/ E \cap staged /= {}
       IN
       IF AdoptMatch => CpHoldsSetOf(b) /\ b \in CpSet /\ b \notin joining
       THEN /\ gen' = g
            /\ leader' = b
            /\ cpExpiry' = now + L
            /\ report' = NoReport
            /\ draining' = FALSE
            \* Without `AdoptMatch` the adopted set replaces a pending one.
            /\ (IF AdoptMatch THEN UNCHANGED nextVars
                ELSE cpNext' = {} /\ cpNextGen' = 0 /\ UNCHANGED << cutOver, splits, stands, cpLeft >>)
            /\ UNCHANGED << out, takeVars >>
       ELSE IF skipped
       THEN /\ gen' = g
            /\ cpNext' = {} /\ cpNextGen' = 0 /\ UNCHANGED << cutOver, splits, stands, cpLeft >>
            /\ UNCHANGED << leader, cpExpiry, report, draining, out, takeVars >>
       ELSE IF recordedOnly
       THEN /\ gen' = g
            /\ UNCHANGED << leader, cpExpiry, report, draining, nextVars, out, takeVars >>
       ELSE IF b \in S
       THEN ReissueAt(b, g, E, S, TRUE)
       ELSE /\ gen' = g
            /\ leader' = b
            /\ cpExpiry' = now + L
            /\ report' = NoReport
            /\ draining' = FALSE
            /\ out' = Brokers \ (E \cup staged)
            /\ UNCHANGED << nextVars, takeVars >>
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted,
                    acked, writes, staleCommit >>
    /\ UNCHANGED << successor, moves, staged, joining, leaving, joinedAt >>
    /\ UNCHANGED << opened, installed, ackGen, waits >>

\* Under `Reissues`, the control plane named a leader at the generation `b`
\* was elected at, and `b` opened there. Ballots let only one of the two open,
\* so the named one never will. On `b`'s report the control plane writes the
\* next generation with `b` as leader (ReissueAt), a conditional write like a
\* promotion (`reissue_past_collision` in
\* services/felix-controlplane-service/src/api/nodes/reports.rs). Only `b` is
\* named: it opened at the collided generation, so it holds everything
\* acknowledged up to it, on whichever set. Not while a change is pending or a
\* copy is joining, nor when `b` is outside the control plane's set. The
\* model takes `b`'s elected set to be the set it knows when the report is
\* read; the report says the set `b` opened on at the collided generation,
\* which differs only if `b` learned a set since. The report may be old: the
\* write checks only the generation, so `b` may since have stood again or
\* stepped down. Nothing bounds these but `Elections` and `Suspicions`, which
\* bound the generations that can collide. Successor and move fields are left
\* as they are: they mean something only while draining, which this ends.

CpReissue(v, b, views) ==
    /\ Reissues
    /\ v.leader /= b /\ v.gen = gen
    /\ <<gen, b>> \in opened
    /\ cpNext = {} /\ joining = {} /\ b \in CpSet
    /\ ReissuePending /\ SetCheck /\ mine[b] /= CpSet => mine[b] \cap staged = {}
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ ReissueAt(b, gen, mine[b], CpSet, ReissuePending /\ SetCheck)
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted,
                    acked, writes, staleCommit >>
    /\ UNCHANGED << successor, moves, staged, joining, leaving, joinedAt >>
    /\ UNCHANGED << opened, installed, ackGen, waits >>

\* What a cut-over found, for `SplitAtCutOver`: whether the new set keeps the
\* old leader, how many writes had been admitted, and how many replicas had
\* promised a generation past the control plane's, which only an election
\* does, and whether the destination `f` was one of them. Recorded only for
\* a piece that splits on it, so the other pieces keep their state counts.
\* It names no broker, so SYMMETRY still holds.
CutOverTag(keeps, f) ==
    << IF keeps THEN 1 ELSE 0, writes, Cardinality({ m \in Brokers : promised[m] > gen }),
       PieceDestStood /= BOOLEAN /\ promised[f] > gen >>

\* A piece of a split check: no cut-over yet, or one that went as the piece
\* says. Every tag has exactly one piece with each `PieceKeeps`,
\* `PieceWrites` in 0..MaxWrites and `PieceStood`, so those pieces together
\* reach every state the unsplit configuration does. A piece with
\* `PieceStood` split by `OneStood` and `SeveralStood`, each with `JustYes`
\* and `JustNo`, is likewise covered by the four, since at most every broker
\* promised past the control plane.
CutOverPiece ==
    \/ cutOver = <<>>
    \/ LET t == cutOver[1] IN
       /\ (t[1] = 1) = PieceKeeps
       /\ t[2] = PieceWrites
       /\ (t[3] > 0) = PieceStood
       /\ t[3] \in (IF PieceStood THEN PieceStoodCounts ELSE {0})
       /\ t[4] \in PieceDestStood
       /\ Len(t) < 5 \/ t[5] \in PieceGens

\* For a piece whose cut-overs no behaviour reaches: it holds only the states
\* before a cut-over, which every piece checks, so it is useful only as proof
\* that it is empty. In a set of three, no cut-over finds several replicas
\* stood with the destination not among them: `Swap` needs the old leader
\* still leading at the control plane's generation, a leader that answers a
\* fence stops leading (`AnswerFence`), and a spare outside the set neither
\* stands nor answers, so only the third member can have stood. TLC checks
\* invariants on states the CONSTRAINT then drops, hence `~CutOverPiece`.
NoCutOverInPiece == cutOver = <<>> \/ ~CutOverPiece

\* A cut-over that changes several members of the set at once, as
\* `replicas_after_cut_over` in services/felix-controlplane-service/src/
\* cluster/placement/moves.rs can: it keeps some of the staying replicas and
\* tops the set up from brokers outside it, so a majority of the new set need
\* share no replica with a majority of the old. The destination `f` leads at
\* the next generation on the new set, fenced, carrying the new set's
\* generation, and with `JointFence` it also needs a majority of the old set.
\* The control plane records the new set as pending (`cpNext`) and keeps the
\* old one as its set until a leader has opened jointly on the new one
\* (CpFlip): a replica of the old set that never heard of the change may still
\* stand and acknowledge on a majority of the old set until then.
\* A copy outside the set joins it here and not before: a staged copy is not
\* part of the persisted set. The cut-over waits for the drain, as CutOver
\* does: the old leader has stopped with nothing in flight, the destination
\* holds its whole log, and every copy joining holds the destination's.
\* Without the drain the old leader goes on acknowledging on the old set
\* after the control plane has moved to the new one (AckedOnMajority). The
\* destination can hold more than the old leader when it was elected past
\* it, and a copy that held only the old leader's log then missed what the
\* destination had acknowledged (FelixShardElectSwap.cfg, AckedOnMajority).
\* With `JoinLags` the joining copy holds any prefix of the destination's
\* log: nothing checks how far a top-up copy has got. A destination elected
\* past the old leader may also have acknowledged records on the old set
\* alone. The flip must then wait for the start of
\* the destination's named stint (`StintStarts`), which reaches the new set
\* only behind those records.
Swap(v, f, views) ==
    /\ Swaps > 0 /\ moves < Swaps
    /\ v.leader = leader /\ ~v.lapsed
    /\ bgen[leader] = gen /\ ~fencing[leader]
    /\ joining = {} /\ cpNext = {}
    /\ f \in CpSet \ {leader} /\ f \notin halted /\ f \notin cpLearner
    /\ queued[leader] = 0 /\ pending[leader] = 0
    /\ HoldsPrefix(f, leader, Len(log[leader]))
    \* The drained report needs the destination level on every log the shard
    \* has (`drain_ready`), the counter log included.
    /\ Counters => CHoldsPrefix(f, leader)
    /\ Elections > 0 => MayPromise(f, f, gen + 1)
    /\ Cas(v)
    /\ \E S \in SUBSET (CpSet \cup out) :
         /\ f \in S /\ Cardinality(S) = Cardinality(CpSet) /\ S /= CpSet
         /\ mine' = [mine EXCEPT ![f] = S]
         /\ cpNext' = S
         /\ \E k \in (IF JoinLags THEN 0 ELSE Len(log[f]))..Len(log[f]) :
              log' = [m \in Brokers |-> IF m \in S \ CpSet THEN SubSeq(log[f], 1, k) ELSE log[m]]
         /\ cutOver' = IF SplitAtCutOver /\ cutOver = <<>>
                       THEN <<CutOverTag(leader \in S, f)>> ELSE cutOver
         /\ UNCHANGED << splits, stands >>
    /\ cpNextGen' = gen + 1
    /\ RecordLeft(CpSet)
    /\ setGen' = [setGen EXCEPT ![f] = gen + 1]
    /\ oldSet' = [oldSet EXCEPT ![f] = {CpSet}]
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
    /\ stopped' = [stopped EXCEPT ![leader] = TRUE, ![f] = FALSE]
    /\ Promise(f, f, gen + 1)
    /\ fencing' = [fencing EXCEPT ![f] = TRUE]
    /\ answered' = [answered EXCEPT ![f] = {}]
    /\ confirmed' = [confirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    \* The counter fence's answers and cursors belong to a generation too.
    /\ canswered' = [canswered EXCEPT ![f] = {}]
    /\ cconfirmed' = [cconfirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
    /\ moves' = moves + 1
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, hwm, halted, acked, writes,
                    staleCommit, draining, successor, staged, joining, leaving, joinedAt,
                    out, opened, installed, ackGen, waits >>

\* The control plane takes the pending set as its own once a leader on it
\* reports its generation's start record acknowledged, which under
\* `JointFence` took a majority of the old set and of the new: every record
\* acknowledged before is then on a majority of the new set too. The leader's
\* report is modelled by reading its state. A conditional write like any other.
\* Liveness: only the control plane ends a pending change. While it is down,
\* leaders on the new set keep fencing and acknowledging on both sets, so
\* losing a majority of the old set stops writes until it comes back. Nothing
\* here checks liveness; safety does not depend on the flip happening.
CpFlip ==
    /\ ~FlipNewGeneration
    /\ cpNext /= {}
    /\ \E l \in Brokers :
        /\ mine[l] = cpNext /\ setGen[l] = cpNextGen /\ oldSet[l] /= {}
        /\ bgen[l] > 0 /\ ~fencing[l]
        /\ StartAcked(l, bgen[l])
        /\ CStartHeld(l, bgen[l])
    /\ out' = (out \cup CpSet) \ cpNext
    /\ cpNext' = {} /\ cpNextGen' = 0 /\ UNCHANGED << cutOver, splits, stands, cpLeft >>
    /\ ver' = ver + 1
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    draining, successor, stopped, moves, cpView, staged, mine,
                    joining, leaving, joinedAt,
                    promised, fencing, answered, confirmed, ballot, opened, setGen, oldSet,
                    installed, ackGen, waits >>

\* Under `FlipNewGeneration`, the flip as `TakePendingSet` writes it: the
\* pending set at the next generation, naming the leader again, which fences
\* again on the new set alone before it serves there. A candidate may have
\* taken that generation already, fencing both sets without the leader, so
\* two fences meet at one generation; the ballot keeps them apart, as it
\* does for any two leaders (`NamedFenceSkipsBallot` drops it for the named
\* one).
\*
\* The control plane flips on the report alone, which may be stale: since
\* sending it the leader may have stopped leading, or promised the next
\* generation to a candidate. The control plane still flips. A leader that
\* promised the generation away refuses it and stays as it is
\* (`a_generation_promised_to_another_node_is_not_led`); any other fences
\* at it.
FlipReported ==
    /\ StartAcked(leader, gen)
    /\ CStartHeld(leader, gen)
    /\ \/ /\ bgen[leader] = gen /\ ~fencing[leader]
          /\ mine[leader] = cpNext /\ setGen[leader] = cpNextGen /\ oldSet[leader] /= {}
       \* A stale report: the joint open happened. This also admits a report
       \* the leader never sent, when it learned the hwm from a candidate
       \* that took its log, so it is broader than the code. Under
       \* `StintStarts` the open must be at the control plane's generation: a
       \* leader elected past its named generation never reports that stint.
       \/ /\ ~(bgen[leader] = gen /\ ~fencing[leader])
          /\ \E r \in installed :
                /\ r[1] = cpNextGen /\ r[2] = cpNext /\ r[3] = {CpSet}
                /\ StintStarts => r[4] = gen

CpFlipNamed ==
    /\ FlipNewGeneration
    /\ cpNext /= {}
    /\ FlipReported
    /\ out' = (out \cup CpSet) \ cpNext
    /\ cpNext' = {} /\ cpNextGen' = 0 /\ UNCHANGED << cutOver, splits, stands >>
    /\ RecordLeft(cpNext)
    /\ ver' = ver + 1
    /\ gen' = gen + 1
    /\ cpExpiry' = now + L
    /\ report' = NoReport
    /\ IF MayPromise(leader, leader, gen + 1)
       THEN /\ bgen' = [bgen EXCEPT ![leader] = gen + 1]
            /\ bexpiry' = [bexpiry EXCEPT ![leader] = clock[leader] + L]
            /\ queued' = [queued EXCEPT ![leader] = 0]
            /\ pending' = [pending EXCEPT ![leader] = 0]
            /\ Promise(leader, leader, gen + 1)
            /\ fencing' = [fencing EXCEPT ![leader] = TRUE]
            /\ answered' = [answered EXCEPT ![leader] = {}]
            /\ confirmed' = [confirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
            /\ setGen' = [setGen EXCEPT ![leader] = gen + 1]
            /\ oldSet' = [oldSet EXCEPT ![leader] = {}]
            \* The counter log is fenced again too.
            /\ canswered' = [canswered EXCEPT ![leader] = {}]
            /\ cconfirmed' = [cconfirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
       ELSE UNCHANGED << bgen, bexpiry, queued, pending, promised, ballot, fencing, answered,
                         confirmed, setGen, oldSet, canswered, cconfirmed >>
    /\ UNCHANGED << now, clock, leader, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit, draining, successor, stopped, moves, cpView, staged, mine,
                    joining, leaving, joinedAt, opened, installed, ackGen, waits >>

\* Under `Reclaims`, placement names a leader that gave its copy up leader
\* again at the next generation, on the same set, when every follower's log
\* is behind its own: every follower measured as it stands now, as the given
\* up leader's probe does (`crate::reclaim` in felix-replication,
\* `reclaimable` in placement/replica_positions.rs). Nothing a client was
\* promised is then held beyond its copy, and no follower qualifies for
\* `ByProbe`. The model still lets it happen beside a promotion on the
\* report from before the crash, which admits more than the code does.
\* `ReclaimBlind` skips the measurement, and the promotion on it.
Reclaim(v, views) ==
    /\ Reclaims
    /\ LeaderReplaced(v)
    /\ bgen[v.leader] = 0
    /\ ReclaimBlind \/ \A f \in ReplicaSet \ {v.leader} : Ahead(v.leader, f)
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = gen + 1
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![v.leader] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![v.leader] = clock[v.leader] + L]
    /\ report' = NoReport
    /\ confirmed' = [confirmed EXCEPT ![v.leader] = [m \in Brokers |-> 0]]
    /\ UNCHANGED << now, clock, leader, inflight, hbOut, hbAt, log, hwm, halted,
                    queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << draining, successor, stopped, moves, staged, out, mine,
                    joining, leaving, joinedAt >>
    /\ UNCHANGED << promised, fencing, answered, ballot, opened, setVars, ackGen, waits >>

\* `m` promised generation `g` to a leader other than `b`.
PromisedAway(m, b, g) == promised[m] = g /\ ballot[m] /= {} /\ ballot[m] /= {b}

\* What the control plane's leader reports as refused at its generation
\* (`refused` in the replica report): its open there fails, since it promised
\* the generation to another node (`PromisedAway` in
\* services/felix-broker-service/src/shards/lifecycle.rs), or its fence there
\* can never reach a majority, since too many of its set did
\* (`Outcome::Refused` in crates/server/felix-replication/src/promotion.rs),
\* or one of its set did and the fence is still pending with every other
\* member that has not taken it silent. That last case is not a joint fence,
\* and not one where a member took the promise from a copy the fence does not
\* count (FenceCounts): a rename resets the copies the next fence checks.
\* That covers a leader that never took the generation and one that took it
\* and is still fencing; a joint fence is refused when either set is out of
\* reach. Read live, as `Reclaim` reads its measurement.
\*
\* The code also stays pending when a member declined for another reason
\* (a short copy): the model has no state for why a member did not answer,
\* so it renames there too. That over-approximates the
\* rename, which only adds behaviours, so every property checked still holds
\* of the code; it can overstate liveness for that case.
\* The control plane's leader is live, open at its generation, and has
\* published a mark over everything in its log.
LeaderServes ==
    /\ leader \notin halted
    /\ bgen[leader] = gen /\ ~fencing[leader]
    /\ hwm[leader] = Len(log[leader])
    /\ ~stopped[leader]

\* No step of the leader's or its set's can get it serving: it does not hold
\* the generation, stopped serving it, or too few members can still take it.
CannotServe ==
    LET can == { m \in FenceMembers(leader) \ halted :
                   IF m = leader THEN promised[m] <= gen ELSE MayPromise(m, leader, gen) }
    IN \/ bgen[leader] /= gen \/ stopped[leader] \/ promised[leader] > gen
       \/ ~LeaderMajority(leader, can)
       \/ JointFence /\ ~MajorityOfOld(can, leader)

\* `set_unknown` in promotion/joint.rs: the kept set is at exactly this
\* generation, no cut-over is pending at it, and the ballot names this leader
\* there, which `keep_own_set` writes only once `fence_log` succeeded, i.e.
\* it opened.
UnknownRefusal(stillFencing) ==
    /\ stillFencing /\ SetUnknown(leader)
    /\ ~UnknownRenamesAny =>
          /\ <<gen, leader>> \in opened
          /\ setGen[leader] = gen
          /\ ~(cpNext /= {} /\ cpNextGen = gen)

\* What the leader reports refused at the control plane's generation.
Refused(left, stillFencing) ==
    \* Its own ballot is newer: the open fails, or under `OwnBallotRefused`
    \* the fence's own step does. A leader that opened at the generation
    \* and lost it after reports neither, unless it restarted since: the
    \* refusals it keeps are in memory, and its reopen there is refused
    \* (`open_failed` in lifecycle.rs). A restarted leader that opened again
    \* and then lost the generation is counted too, which the code does not
    \* report.
    \* Its own copy promised the generation to another. Under
    \* `RenamePastBallot` a fenced open waits there instead (`keep_own_set` in
    \* promotion.rs, #1872) and reports nothing. A rename never lands there;
    \* a refused promotion can, and waits as the code does.
    \/ ~RenamePastBallot /\ PromisedAway(leader, leader, gen)
    \/ /\ SupersededRefused /\ promised[leader] > gen
       /\ bgen[leader] < gen
       /\ (<<gen, leader>> \notin opened \/ (Restarts /\ inc[leader] > 0))
    \/ OwnBallotRefused /\ stillFencing /\ promised[leader] > gen
    \/ ~LeaderMajority(leader, left \cap Members(leader))
    \/ /\ stillFencing /\ left /= FenceMembers(leader)
       /\ ~(JointFence /\ oldSet[leader] /= {}) \/ JointMinorityRefused
       \* A member that promised it and whose answer does not count (an
       \* other copy) keeps the refusal back, under `OtherCopiesExcluded`.
       /\ OtherCopiesExcluded =>
             \A m \in left :
                promised[m] = gen /\ ballot[m] = {leader} => m \in answered[leader]
    \/ /\ stillFencing /\ JointFence
       /\ \E o \in oldSet[leader] \ {{}} : ~MajorityOf(left \cup {leader}, o)
    \/ UnknownRefusal(stillFencing)

\* The control plane's leader is open at its generation and its own copy
\* holds its own ballot above it, with no standing under way there. It no
\* longer counts its own copy (HeldAtGen), so with one follower down nothing
\* it writes is acknowledged (#1933).
BelowOwnBallot ==
    /\ bgen[leader] = gen /\ ~fencing[leader]
    /\ promised[leader] > gen /\ ballot[leader] = {leader}

\* Under `OwnBallotReported`, it reports the generation refused once that
\* lasts (`ballot_above` in crates/server/felix-replication/src/driver/shard.rs
\* and driver/shards.rs).
OwnBallotAbove == OwnBallotReported /\ BelowOwnBallot

\* The control plane's leader is still fencing at its generation and its own
\* copy holds its own ballot above it. Under `OpenChecksOwnBallot` it never
\* opens there (#1940).
FencingBelowOwnBallot ==
    /\ bgen[leader] = gen /\ fencing[leader]
    /\ promised[leader] > gen /\ ballot[leader] = {leader}

\* Under `OwnBallotFenceRefused`, a leader still fencing whose own copy
\* promised a newer generation, to anyone, reports its generation refused
\* at once: that copy refuses the ballot its open needs, for good.
OwnBallotFenceAbove ==
    /\ OwnBallotFenceRefused
    /\ bgen[leader] = gen /\ fencing[leader] /\ promised[leader] > gen

\* Under `HaltReported`, the open leader's report that a follower refused its
\* batches for a newer ballot. It carries that ballot.
HaltRefusal ==
    /\ HaltReported /\ bgen[leader] = gen /\ ~fencing[leader]
    /\ \E m \in Members(leader) \ {leader} : promised[m] > gen

\* Under `AheadRefused`, the fence of the leader still fencing at its
\* generation ends refused once a live member answers that it took a newer
\* one. It carries that ballot.
AheadRefusal ==
    /\ AheadRefused /\ bgen[leader] = gen /\ fencing[leader]
    /\ \E m \in FenceMembers(leader) \ ({leader} \cup halted) : promised[m] > gen

\* A refusal other than `AboveReported`'s. `HaltRefusal`, `AheadRefusal` and
\* under `UnknownCarriesBallot` the lost set's carry a ballot.
OtherRefusal ==
    LET left == { m \in FenceMembers(leader) :
                    /\ ~PromisedAway(m, leader, gen)
                    \* The leader's own log answers through `keep_own_set`, not
                    \* `check_role`: that is `OwnBallotRefused`.
                    /\ ~(OverlayRefused /\ m /= leader /\ promised[m] > gen) }
        stillFencing == bgen[leader] = gen /\ fencing[leader]
        open == bgen[leader] = gen /\ ~fencing[leader]
    IN \/ HaltRefusal
       \/ AheadRefusal
       \/ OwnBallotAbove
       \/ OwnBallotFenceAbove
       \/ CatchAllRename /\ ~LeaderServes /\ CannotServe
       \/ /\ bgen[leader] < gen \/ stillFencing
          /\ Refused(left, stillFencing)

AboveRefusal == AboveReported /\ promised[leader] > gen /\ bgen[leader] < gen

\* What the control plane's leader reports refused at its generation, read
\* from its own state. A stopped broker reports nothing.
RefusalLive ==
    /\ leader \notin halted
    /\ OtherRefusal \/ AboveRefusal

\* Under `ReportStore`, the control plane renames on the refusal its stored
\* report carries, from the leader at its generation (`refused` in
\* services/felix-controlplane-service/src/cluster/placement/replica_positions.rs).
\* The model keeps a report for good; the code drops one past its TTL, which
\* only takes renames away.
RenameDue ==
    /\ Renames
    /\ IF ReportStore THEN <<gen, leader>> \in cpRefused ELSE RefusalLive

\* Under `ReportStore`, the leader reports its generation refused, and the
\* control plane stores that report over the last one. A refusal report has
\* no tail (`send_reports` in crates/server/felix-replication/src/reporter.rs),
\* so it replaces any report at its generation.
SendRefusal(b) ==
    /\ ReportStore
    /\ b = leader /\ RefusalLive
    /\ cpRefused' = {<<gen, b>>}
    /\ refusedAt' = [refusedAt EXCEPT ![b] = gen]
    /\ cpRefused' /= cpRefused \/ refusedAt' /= refusedAt

\* Under `ReportStore`, one of the open leader's pass reports lands. It has a
\* tail, so it replaces a refusal at its generation. Under `RefusalRepeats` it
\* carries the refusal the leader last reported there. A leader still fencing
\* runs no passes.
PassReport(b) ==
    /\ ReportStore
    /\ b = leader /\ bgen[b] = gen /\ ~fencing[b]
    /\ cpRefused' = IF RefusalRepeats /\ refusedAt[b] = gen THEN {<<gen, b>>} ELSE {}
    /\ cpRefused' /= cpRefused
    /\ UNCHANGED refusedAt

\* The generation a rename names. Under `RenamePastBallot` the
\* `AboveReported` refusal, when the ballot is another node's, carries it, and
\* placement names the generation above it (`RefusedGeneration::above` in
\* driver/shards.rs, #1951; placement's side is proposed there). A refused
\* open or fence is reported first and carries none (`Shards::refusal`), so
\* it renames to the next generation. A halt report (`HaltRefusal`) carries
\* the follower's ballot, and placement names the generation above the
\* highest (`follower_ballot` in driver/shard.rs, #1949). The code takes the
\* floor only inside the adoption window; the model always does. The model
\* also reads each follower's ballot now, where the code keeps the one the
\* halt carried: a ballot taken after the halt costs the code one more
\* refused rename and report, which the model skips.
\*
\* A lost set's refusal under `UnknownCarriesBallot`, and an ahead one, carry
\* the highest ballot the leader's sets hold, its own included, and placement
\* names the generation above it. Under `JointRenamesAbove` off, a rename
\* with a change pending takes the next generation whatever the ballot.
TopBallot ==
    LET ps == { promised[m] : m \in FenceMembers(leader) \cup {leader} }
    IN CHOOSE p \in ps : \A q \in ps : q <= p

CarriedAbove ==
    \/ UnknownCarriesBallot /\ UnknownRefusal(bgen[leader] = gen /\ fencing[leader])
    \/ AheadRefusal

RenameGen ==
    IF ~JointRenamesAbove /\ cpNext /= {}
    THEN gen + 1
    ELSE IF /\ RenamePastBallot /\ AboveRefusal /\ ~OtherRefusal
            /\ ballot[leader] \notin {{}, {leader}}
    THEN promised[leader] + 1
    ELSE IF RenamePastBallot /\ HaltRefusal
    THEN LET above == { promised[m] : m \in Members(leader) \ {leader} }
         IN (CHOOSE p \in above : \A q \in above : q <= p) + 1
    ELSE IF CarriedAbove /\ TopBallot > gen
    THEN TopBallot + 1
    ELSE gen + 1

\* Under `Renames`, the control plane names its leader again at the next
\* generation, the same set and any pending change kept, once that leader
\* reports it cannot serve the control plane's generation: a recorded
\* election moved the control plane past it, or it is still fencing there
\* and too many of its set promised a candidate, or one did and the rest
\* are silent, or it is open there below its own ballot, or still fencing
\* there below its own copy's promise (`election_refused`,
\* `move_step` in services/felix-controlplane-service/src/cluster/placement/moves.rs).
\* Not while a move drains its leader, which ends through its own steps. The
\* leader fences there as at any new term: its open failed, so the broker
\* reopens it fenced (`begin_open_renewed` in lifecycle.rs), on the set the
\* assignment names, and while a change is pending, jointly with the set
\* being left. Each one follows a stand, a restart or a promotion, and stands
\* stop at `Adopt`, so those bound them. A rename past a ballot skips
\* generations, and the leader still takes `CpSet` as the set it leaves: the
\* control plane recorded nothing in between and names the generation it
\* replaced (`previous_generation`, #2024), which the broker's watch reads.
CpRename(v, views) ==
    /\ RenameDue
    /\ ~v.draining
    /\ Cas(v)
    /\ ver' = ver + 1
    /\ cpView' = views
    /\ gen' = RenameGen
    /\ cpExpiry' = now + L
    /\ report' = NoReport
    /\ stopped' = [stopped EXCEPT ![leader] = IF MayPromise(leader, leader, RenameGen) THEN FALSE ELSE @]
    \* The leader may hold a ballot above this generation too, and then its
    \* open is refused again and reported, until a rename passes the ballot.
    /\ IF MayPromise(leader, leader, RenameGen)
       THEN /\ bgen' = [bgen EXCEPT ![leader] = RenameGen]
            /\ bexpiry' = [bexpiry EXCEPT ![leader] = clock[leader] + L]
            /\ queued' = [queued EXCEPT ![leader] = 0]
            /\ pending' = [pending EXCEPT ![leader] = 0]
            /\ Promise(leader, leader, RenameGen)
            /\ confirmed' = [confirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
            /\ IF SetCheck
               THEN /\ mine' = [mine EXCEPT ![leader] = IF cpNext = {} THEN CpSet ELSE cpNext]
                    /\ setGen' = [setGen EXCEPT ![leader] = RenameGen]
                    \* `RenameDropsChange` names the pending set alone.
                    /\ oldSet' = [oldSet EXCEPT ![leader] =
                                     IF cpNext = {} \/ RenameDropsChange THEN {} ELSE {CpSet}]
               ELSE UNCHANGED << mine, setGen, oldSet >>
            /\ IF RenameUnfenced
               THEN /\ log' = [log EXCEPT ![leader] = Opened(leader, RenameGen)]
                    /\ Opens(leader, RenameGen)
                    /\ fencing' = [fencing EXCEPT ![leader] = FALSE]
                    /\ UNCHANGED answered
               ELSE /\ fencing' = [fencing EXCEPT ![leader] = TRUE]
                    /\ answered' = [answered EXCEPT ![leader] = {}]
                    /\ UNCHANGED << log, opened >>
       ELSE UNCHANGED << bgen, bexpiry, queued, pending, promised, ballot, confirmed, mine, setGen,
                         oldSet, log, opened, fencing, answered >>
    /\ UNCHANGED << now, clock, leader, inflight, hbOut, hbAt, hwm, halted,
                    acked, writes, staleCommit >>
    /\ UNCHANGED << draining, successor, moves, staged, out, joining, leaving, joinedAt >>
    /\ UNCHANGED << installed, nextVars, ackGen, waits >>
    \* Under `CopyPins` a rename drops the leader's pins unless `PinsKeptOnRename`.
    /\ IF CopyPins /\ ~PinsKeptOnRename
       THEN /\ pinned' = pinned \ {leader}
            /\ stalePin' = [stalePin EXCEPT ![leader] = {}]
       ELSE UNCHANGED << pinned, stalePin >>
    /\ UNCHANGED << changed, changes, admitting >>

\* A placement write, from a read taken in the same step or from one a
\* planner has held since.
Decide(v, f, views) ==
    \/ Promote(v, f, views) /\ UNCHANGED contAt
    \/ /\ \/ /\ \/ Fence(v, f, views) \/ CutOver(v, f, views) \/ Retake(v, f, views)
                \/ SeatFence(v, f, views) \/ TrimFence(v, views) \/ UndoFence(v, f, views)
                \/ CpAdopt(v, f, views) \/ Reclaim(v, views) \/ CpReissue(v, f, views)
             /\ UNCHANGED << contAt, pinVars >>
          \/ CpRename(v, views) /\ UNCHANGED contAt
          \* These move the serving leader on through `RegenerateSet`.
          \/ /\ \/ Reseat(v, f, views) \/ GrowSet(v, f, views) \/ Seat(v, f, views)
                \/ TrimSet(v, f, views) \/ Undo(v, f, views)
             /\ UNCHANGED pinVars
       /\ UNCHANGED << counterVars, expect, inherited, seen, seenInc, fenceIds >>
    \/ /\ Swap(v, f, views)
       /\ UNCHANGED << clog, chwm, cacked, expect, inherited, seen, seenInc, fenceIds, contAt, pinVars >>

-----------------------------------------------------------------------------
(* Wipes. A follower loses its data directory and comes back empty, under *)
(* a new storage identity. Its high-water mark goes with the log.         *)

\* What it promised stays. On the fenced path the broker answers a copy with
\* no promise from its routing view (`Forget` under `PromiseFromRoutes`),
\* which names the promised generation or a later one, so keeping the promise
\* refuses no more than the code does.
Wipe(f) ==
    /\ ~CrashLoses
    /\ wipes < Wipes
    /\ bgen[f] = 0
    /\ log' = [log EXCEPT ![f] = <<>>]
    /\ hwm' = [hwm EXCEPT ![f] = 0]
    /\ sid' = [sid EXCEPT ![f] = @ + 1]
    /\ wipes' = wipes + 1
    /\ IF StripIds
       THEN /\ restarting' = restarting \cup {f}
            /\ UNCHANGED << registered, inc >>
       ELSE /\ registered' = [registered EXCEPT ![f] = sid[f] + 2]
            /\ inc' = [inc EXCEPT ![f] = @ + 1]
            /\ UNCHANGED restarting
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, halted, queued, pending, acked, writes, staleCommit >>
    \* The directory, and any ballot on it, is gone, though the model keeps
    \* `promised` (see above).
    /\ forgot' = forgot \cup {f}
    /\ UNCHANGED << handoffVars, fenceVars, seen, seenInc, checked, expect, inherited, leftDirty,
                    gaveUp, fenceIds, wipedAt, readmit, learner, marked, cpLearner, contAt >>

\* A follower crashes under `FsyncMode::Periodic` or `None` and restarts
\* without a suffix it had not synced, which it may already have answered
\* for. The suffix may be empty, and with `ProcessCrashes` the crash may be
\* the process's alone, which keeps the page cache and so the whole log; the
\* restart is unclean all the same. Its directory, and so its identity, is
\* the one it had, unless `RotateOnCrash` gives it a new one before it
\* registers again. It shares the wipes' budget.
\*
\* With `CrashLeader` the one that crashes is a broker that believes it
\* leads, and what it held in memory goes with the process: its queued and
\* pending writes, its cursors and what it heard of its reports. Without
\* `GiveUpOnCrash` it comes back still leading at its generation, on its
\* lease; with it, it leads nothing until placement names it again.
\* Whether a crash gives the directory a new identity. Without
\* `KeepIdOnSameBoot` every crash under `RotateOnCrash` does; with it only a
\* `machine` crash, unless `BootCheckBlind` takes that for the process's.
CrashRotates(machine) ==
    RotateOnCrash /\ (~KeepIdOnSameBoot \/ (machine /\ ~BootCheckBlind))

Crash(f) ==
    /\ CrashLoses
    /\ wipes < Wipes
    /\ IF CrashLeader THEN bgen[f] > 0 ELSE bgen[f] = 0
    \* Under `RefuseShortCopy` only the assignment's leader, which keeps the
    \* state space to what the seat needs.
    /\ RefuseShortCopy => f = leader
    /\ Len(log[f]) > 0
    /\ \E machine \in (IF ProcessCrashes THEN BOOLEAN ELSE {TRUE}) :
       LET rotates == CrashRotates(machine)
       IN
       /\ \E k \in (IF machine THEN 0..Len(log[f]) ELSE {Len(log[f])}) :
             /\ log' = [log EXCEPT ![f] = SubSeq(@, 1, k)]
             /\ hwm' = [hwm EXCEPT ![f] = IF @ > k THEN k ELSE @]
       /\ IF rotates
          THEN /\ sid' = [sid EXCEPT ![f] = @ + 1]
               /\ registered' = [registered EXCEPT ![f] = sid[f] + 2]
          ELSE UNCHANGED << sid, registered >>
       /\ leftDirty' = IF CleanStops /\ ~machine /\ ~CrashLeader
                       THEN [leftDirty EXCEPT ![f] = Len(log[f])]
                       ELSE leftDirty
       /\ IF CrashLeader
          THEN LET givesUp == GiveUpOnCrash /\ (rotates \/ ~KeepIdOnSameBoot)
               IN
               /\ queued' = [queued EXCEPT ![f] = 0]
               /\ pending' = [pending EXCEPT ![f] = 0]
               /\ confirmed' = [confirmed EXCEPT ![f] = [m \in Brokers |-> 0]]
               /\ checked' = [checked EXCEPT ![f] = {}]
               /\ heard' = [heard EXCEPT ![f] = NoReport]
               /\ bgen' = IF givesUp THEN [bgen EXCEPT ![f] = 0] ELSE bgen
               /\ fencing' = IF givesUp THEN [fencing EXCEPT ![f] = FALSE] ELSE fencing
               \* The copy it led, while it still holds its own generation.
               /\ gaveUp' = IF RefuseShortCopy /\ givesUp /\ promised[f] = bgen[f]
                            THEN [gaveUp EXCEPT ![f] = bgen[f]]
                            ELSE gaveUp
          ELSE UNCHANGED << queued, pending, confirmed, checked, heard, bgen, fencing, gaveUp >>
    /\ wipes' = wipes + 1
    /\ inc' = [inc EXCEPT ![f] = @ + 1]
    /\ UNCHANGED restarting
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, halted, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, promised, answered, ballot, opened, setVars, ackGen, waits,
                    seen, seenInc, expect, inherited, fenceIds, wipeVars >>

\* `f`, back from a process crash that kept its identity, stops cleanly and
\* starts again in the same boot. The stop syncs what this run wrote and
\* records a clean stop, so the next start keeps the identity whatever
\* happened in between, and here the power goes while it is down. Under
\* `SyncOnKeep` the start after the crash synced what the crashed process
\* had left to the page cache, so nothing is lost. Without it the clean
\* stop vouched for pages only the kernel held, and any suffix may go.
CleanStop(f) ==
    /\ CleanStops
    /\ leftDirty[f] > 0
    /\ bgen[f] = 0
    /\ \E k \in (IF SyncOnKeep THEN {Len(log[f])} ELSE 0..Len(log[f])) :
          /\ log' = [log EXCEPT ![f] = SubSeq(@, 1, k)]
          /\ hwm' = [hwm EXCEPT ![f] = IF @ > k THEN k ELSE @]
    /\ leftDirty' = [leftDirty EXCEPT ![f] = 0]
    /\ inc' = [inc EXCEPT ![f] = @ + 1]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars, sid, seen, seenInc, wipes, checked, registered,
                    restarting, expect, inherited, gaveUp, fenceIds, wipeVars >>

\* Under `StripIds`, the wiped broker registers again without an identity;
\* otherwise registering with one is the wipe's own step. Nothing orders the
\* registration against the leader's passes: a leader still on a crashed
\* follower's old connection goes on naming its old identity and incarnation
\* until QUIC's idle timeout.
Register(f) ==
    /\ f \in restarting
    /\ registered' = [registered EXCEPT ![f] = 0]
    /\ inc' = [inc EXCEPT ![f] = @ + 1]
    /\ restarting' = restarting \ {f}
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>
    /\ UNCHANGED << sid, seen, seenInc, wipes, checked, expect, inherited, leftDirty, gaveUp,
                    fenceIds, wipeVars >>

\* `f` loses its data directory, and with it the generation and ballot it
\* promised. Under `PromiseFromRoutes` it answers what its routing view
\* backs instead. The control plane issued every generation `f` promised
\* before the wipe, so the view it seeds on restart names the newest of them
\* or a later one, with the one leader named there: an older leader is
\* refused by the view, at its generation only the leader it names is
\* answered, and a newer generation was never promised. The model seeds the
\* view at the wipe, the oldest it can be; the code reads it when asked,
\* which refuses at least as much. Under `Elections` a candidate's
\* generation is in no view, and the guard is `WipedWaitsForAdopt`, whose
\* bound is taken here too: the control plane's generation at the wipe, the
\* lowest a registration can read.
Forget(f) ==
    /\ LosePromises
    /\ wipes < Forgets
    /\ wipes' = wipes + 1
    /\ IF PromiseFromRoutes
       THEN Promise(f, leader, gen)
       ELSE /\ promised' = [promised EXCEPT ![f] = 0]
            /\ ballot' = [ballot EXCEPT ![f] = {}]
    \* The set file goes with the directory.
    /\ IF SetCheck
       THEN /\ setGen' = [setGen EXCEPT ![f] = 0]
            /\ mine' = [mine EXCEPT ![f] = {}]
            /\ oldSet' = [oldSet EXCEPT ![f] = {}]
       ELSE UNCHANGED << setGen, mine, oldSet >>
    \* Under `WipedNonVoter` the logs go too, as with `Wipe`.
    /\ IF WipedNonVoter
       THEN /\ log' = [log EXCEPT ![f] = <<>>]
            /\ hwm' = [hwm EXCEPT ![f] = 0]
            /\ clog' = [clog EXCEPT ![f] = <<>>]
            /\ chwm' = [chwm EXCEPT ![f] = 0]
       ELSE UNCHANGED << log, hwm, clog, chwm >>
    \* A wiped leader or candidate restarts leading nothing.
    /\ IF WipeLeads
       THEN /\ bgen' = [bgen EXCEPT ![f] = 0]
            /\ fencing' = [fencing EXCEPT ![f] = FALSE]
            /\ queued' = [queued EXCEPT ![f] = 0]
            /\ pending' = [pending EXCEPT ![f] = 0]
       ELSE /\ bgen[f] = 0
            /\ UNCHANGED << bgen, fencing, queued, pending >>
    \* A replaced storage id is a learner from the start under
    \* `ReplacedIdLearner`; otherwise the mark went with the set file, and
    \* only `NamesOther` sets it.
    /\ learner' = IF WipedNonVoter THEN [learner EXCEPT ![f] = ReplacedIdLearner] ELSE learner
    /\ marked' = IF WipedNonVoter THEN [marked EXCEPT ![f] = ReplacedIdLearner] ELSE marked
    \* The control plane learns of the wipe from the registration under the
    \* new id: the assignment names another id for `f` from here
    \* (`ReplicaPositions` in services/felix-controlplane-service/src/cluster/
    \* placement/replica_positions.rs).
    /\ cpLearner' = IF WipedNonVoter THEN cpLearner \cup {f} ELSE cpLearner
    /\ wipedAt' =
          CASE WipedNonVoter -> [wipedAt EXCEPT ![f] = gen]
            [] WipedWaitsForAdopt ->
                 [wipedAt EXCEPT ![f] = IF WipeBoundOracle /\ promised[f] > gen THEN promised[f] ELSE gen]
            [] OTHER -> wipedAt
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, halted, acked, writes, staleCommit >>
    /\ UNCHANGED << placeVars, answered, confirmed, opened, installed, nextVars, ackGen, waits >>
    /\ UNCHANGED << sid, seen, seenInc, checked, registered, inc, restarting, expect, inherited,
                    leftDirty, gaveUp, fenceIds >>
    /\ OneNonVoter => NonVoterCredit'

\* Under `WipedLearnsSet`, a wiped replica the guard holds back still learns
\* the newer set a candidate's fence carries. It promises nothing, and the
\* candidate does not count it. Under `WipedNonVoter` a set naming another
\* id for it marks it a learner, from a fence or a batch, as
\* `refuse_non_voter_fence` and `mark_local` in
\* crates/server/felix-replication/src/replica/election.rs do.
WipedLearnSet(b, f) ==
    /\ WipedLearnsSet
    /\ fencing[b] \/ WipedNonVoter
    /\ bgen[b] > 0 /\ f /= b
    /\ f \in FenceMembers(b)
    /\ IF WipedNonVoter THEN NonVoter(f) \/ NamesOther(f, b) ELSE Candidate(b) /\ WipeWaits(f)
    /\ setGen[f] < setGen[b]
    /\ LearnSet(f, b)
    \* Under `GenOneExempt` a set-less copy takes a creation set naming no
    \* other id for it as a voter's.
    /\ LET mark == ~(GenOneExempt /\ setGen[f] = 0 /\ setGen[b] = 1 /\ ~NamesOther(f, b))
       IN /\ learner' = IF WipedNonVoter THEN [learner EXCEPT ![f] = mark] ELSE learner
          /\ marked' = IF WipedNonVoter THEN [marked EXCEPT ![f] = mark] ELSE marked
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << placeVars, promised, fencing, answered, confirmed, ballot, opened, installed,
                    nextVars, ackGen, waits >>

\* Under `WipedNonVoter`, the control plane re-admits wiped `w`: it writes the
\* set again, with `w` under its new identity, at the next generation, and
\* its leader fences that set jointly with the one before it, whose majority
\* it counts without `w` (OpenForWrites). `w` votes again only once it has
\* caught up (`Readmitted`). The control plane's own leader at its
\* generation does it, read live (no planners, so no store version to race),
\* and only up to `Adopt`, which bounds every generation here. A pending
\* re-admission the control plane has moved past is started again.
Readmit(w) ==
    /\ NonVoter(w) /\ w \in cpLearner
    /\ \A r \in readmit : r[2] < gen
    /\ leader /= w /\ bgen[leader] = gen /\ ~fencing[leader] /\ <<gen, leader>> \in opened
    /\ Elections > 0 => gen + 1 <= Adopt
    /\ MayPromise(leader, leader, gen + 1)
    /\ gen' = gen + 1
    /\ cpExpiry' = now + L
    /\ report' = NoReport
    /\ readmit' = {<<w, gen + 1, leader>>}
    /\ ContinuesAt(leader, gen + 1)
    /\ bgen' = [bgen EXCEPT ![leader] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![leader] = clock[leader] + L]
    /\ Promise(leader, leader, gen + 1)
    /\ fencing' = [fencing EXCEPT ![leader] = TRUE]
    /\ answered' = [answered EXCEPT ![leader] = {}]
    /\ confirmed' = [confirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
    /\ queued' = [queued EXCEPT ![leader] = 0]
    /\ pending' = [pending EXCEPT ![leader] = 0]
    /\ setGen' = [setGen EXCEPT ![leader] = gen + 1]
    /\ oldSet' = [oldSet EXCEPT ![leader] = {mine[leader]}]
    /\ UNCHANGED << now, clock, leader, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit >>
    /\ UNCHANGED << placeVars, mine, opened, installed, nextVars, ackGen, waits >>

\* The re-admission completes: `w`'s kept set names its new id (it was
\* written after the wipe, by `Readmit` or any later control-plane write) and
\* `w` holds the generation-start record of that set's generation, copied
\* from the leader the control plane confirms (`CatchUp`), as a Raft learner
\* is caught up before it is promoted. That leader fenced without `w`, which
\* answers no fence but its own re-admission's, so every record a majority
\* acknowledged precedes the record. `admit_when_caught_up` in
\* crates/server/felix-replication/src/replica/election.rs.
\* `ReadmitBeforeCatchUp` skips the catch-up: the confirmed leader having
\* opened at that generation or a later one is enough.
Readmitted(w) ==
    /\ NonVoter(w)
    /\ LET g == setGen[w]
       IN /\ g > wipedAt[w]
          /\ IF ReadmitBeforeCatchUp
             THEN gen >= g /\ <<gen, leader>> \in opened
             ELSE /\ \E k \in 1..Len(log[w]) : log[w][k] = Start(g)
                  /\ Counters /\ ReadmitChecksCounters =>
                        \E k \in 1..Len(clog[w]) : clog[w][k] = Start(g)
    /\ wipedAt' = [wipedAt EXCEPT ![w] = 0]
    /\ learner' = [learner EXCEPT ![w] = FALSE]
    /\ marked' = [marked EXCEPT ![w] = GroupLogs /\ ~GroupHeld(w, setGen[w])]
    /\ readmit' = { r \in readmit : r[1] /= w }
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* Under `WipedNonVoter`, a wiped replica copies a leader's log to catch up,
\* replacing its own, but never below its mark. With `LearnerAcksCount` the
\* leader counts its answer.
CatchUp(b, f) ==
    /\ NonVoter(f) /\ f /= b
    /\ bgen[b] > 0 /\ ~fencing[b]
    /\ ConfirmedSender(b, f)
    /\ log[f] /= log[b]
    /\ Len(log[b]) >= hwm[f] /\ SubSeq(log[f], 1, hwm[f]) = SubSeq(log[b], 1, hwm[f])
    /\ log' = [log EXCEPT ![f] = log[b]]
    /\ IF LearnerAcksCount THEN Confirm(b, f, Len(log[b])) ELSE UNCHANGED confirmed
    \* The batch takes the ballot, as every batch does (`accept_sender`):
    \* once re-admitted the copy never promises that generation, or one
    \* below it, to another leader. Before its wipe it may have promised
    \* any of them.
    /\ MayPromise(f, b, bgen[b])
    /\ Promise(f, b, bgen[b])
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered, opened, setVars, ackGen, waits >>

\* The same for the counter log, which the leader ships apart from the cache
\* log, so either can be behind the other.
CatchUpCounters(b, f) ==
    /\ Counters
    /\ NonVoter(f) /\ f /= b
    /\ bgen[b] > 0 /\ ~fencing[b]
    /\ ConfirmedSender(b, f)
    /\ clog[f] /= clog[b]
    /\ clog' = [clog EXCEPT ![f] = clog[b]]
    /\ cconfirmed' = IF LearnerAcksCount THEN [cconfirmed EXCEPT ![b][f] = Len(clog[b])]
                     ELSE cconfirmed
    /\ MayPromise(f, b, bgen[b])
    /\ Promise(f, b, bgen[b])
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fencing, answered, confirmed, opened, setVars, ackGen, waits >>

\* Placement stops holding `w` a learner once the report of the control
\* plane's leader shows `w` holding the start record of the control plane's
\* generation, written after the wipe so it names `w`'s new id, on every log
\* re-admission checks: `holds_start` in the report, read by
\* `ReplicaPositions` in services/felix-controlplane-service/src/cluster/
\* placement/replica_positions.rs. `w` re-admits itself before it answers
\* the batch that carried the record (`admit_when_caught_up` runs before the
\* `ReplicateOk` in crates/server/felix-replication/src/replica.rs), so by
\* the time the leader can report it `w` is no longer a non-voter: the
\* `~NonVoter(w)` conjunct stands for that order.
CpAdmit(w) ==
    /\ w \in cpLearner
    /\ gen > wipedAt[w] /\ ~NonVoter(w)
    /\ \E k \in 1..Len(log[w]) : log[w][k] = Start(gen)
    /\ Counters /\ ReadmitChecksCounters => \E k \in 1..Len(clog[w]) : clog[w][k] = Start(gen)
    /\ cpLearner' = cpLearner \ {w}
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* Under `GroupLogs`, a re-admitted copy's wiped mark ends once its group
\* log holds a record written at its set's generation or later: that record
\* follows everything the leader that wrote it took when it opened, and that
\* leader took no group answer from this copy while it was marked.
\* `admit_when_caught_up` with `settled` in
\* crates/server/felix-replication/src/replica/election.rs.
GroupsCaughtUp(w) ==
    /\ GroupLogs
    /\ marked[w] /\ ~learner[w]
    /\ GroupHeld(w, setGen[w])
    /\ marked' = [marked EXCEPT ![w] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* Under `CopyPins`, a member that leads nothing gets a new copy: a crash
\* that rotates its storage id, or a restore. It keeps its promises. With
\* writes it loses any suffix past its mark, and only while its log holds
\* nothing past the first generation: a later loss meets hazards these
\* configurations are not about. Every pin naming its old copy is now stale,
\* and no leader's cursor for it runs past what it still holds. One copy at
\* a time.
ChangeCopy(m) ==
    /\ CopyPins /\ changes < CopyChanges /\ changed = {}
    /\ m \notin halted /\ bgen[m] = 0
    /\ changes' = changes + 1
    /\ changed' = {m}
    /\ stalePin' = [b \in Brokers |-> IF b \in pinned THEN stalePin[b] \cup {m} ELSE stalePin[b]]
    /\ MaxWrites > 0 => LastGen(m) <= 1
    /\ \E k \in IF MaxWrites = 0 THEN {Len(log[m])} ELSE hwm[m]..Len(log[m]) :
          /\ log' = [log EXCEPT ![m] = SubSeq(@, 1, k)]
          /\ confirmed' = [b \in Brokers |-> [confirmed[b] EXCEPT ![m] = IF @ > k THEN k ELSE @]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, promised, fencing, answered, ballot, opened, setVars, ackGen, waits,
                    pinned, admitting >>

\* Placement admits the changed copy again, as it re-admits a wiped one: it
\* writes the set again at the next generation, and its open leader fences
\* that set jointly with the one before it, a change pending until the flip.
\* A pending re-admission it has moved past is started again.
ReadmitCopy(w) ==
    /\ w \in changed /\ w \notin halted
    /\ \A r \in admitting : r[2] < gen
    /\ leader /= w /\ leader \notin halted
    /\ bgen[leader] = gen /\ ~fencing[leader] /\ ~stopped[leader]
    /\ cpNext = {}
    /\ Adopt > 0 => gen + 1 <= Adopt
    /\ MayPromise(leader, leader, gen + 1)
    /\ gen' = gen + 1
    /\ cpExpiry' = now + L
    /\ report' = NoReport
    /\ admitting' = {<<w, gen + 1>>}
    /\ bgen' = [bgen EXCEPT ![leader] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![leader] = clock[leader] + L]
    /\ Promise(leader, leader, gen + 1)
    /\ fencing' = [fencing EXCEPT ![leader] = TRUE]
    /\ answered' = [answered EXCEPT ![leader] = {}]
    /\ confirmed' = [confirmed EXCEPT ![leader] = [m \in Brokers |-> 0]]
    /\ queued' = [queued EXCEPT ![leader] = 0]
    /\ pending' = [pending EXCEPT ![leader] = 0]
    /\ mine' = [mine EXCEPT ![leader] = CpSet]
    /\ setGen' = [setGen EXCEPT ![leader] = gen + 1]
    /\ oldSet' = [oldSet EXCEPT ![leader] = {CpSet}]
    /\ cpNext' = CpSet /\ cpNextGen' = gen + 1
    /\ RecordLeft(CpSet)
    /\ UNCHANGED << now, clock, leader, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit >>
    /\ UNCHANGED << placeVars, opened, installed, cutOver, splits, stands, ackGen, waits >>
    /\ UNCHANGED << pinned, stalePin, changed, changes >>

\* The re-admission completes once the copy holds the start record of its
\* generation. Under `PinsDropOnAdmit` placement drops the copy's pins.
CopyAdmitted(w) ==
    /\ \E r \in admitting :
          /\ r[1] = w
          /\ \E k \in 1..Len(log[w]) : log[w][k] = Start(r[2])
    /\ changed' = changed \ {w}
    /\ admitting' = {}
    /\ stalePin' = IF PinsDropOnAdmit THEN [b \in Brokers |-> stalePin[b] \ {w}] ELSE stalePin
    /\ UNCHANGED << pinned, changes >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* Under `KeepSetDropsMark`, a promoted leader keeps its route's set once
\* its records' fence has a majority, before its group fence is done, and
\* the write drops its own wiped mark. The code read the mark for the group
\* fence before the write, so only a retry after a failed take reads it
\* dropped; here `OpenForWrites` reads it when it opens, which is that retry.
KeepOwnSet(b) ==
    /\ KeepSetDropsMark
    /\ fencing[b] /\ bgen[b] > 0 /\ marked[b] /\ ~learner[b]
    /\ LeaderMajority(b, answered[b] \cup {b})
    /\ marked' = [marked EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars >>

\* The incarnation `f`'s handshake gives: none before it has registered.
\* Registering closes the connections opened before it, so a leader learns
\* the new one by dialling again (`Probe`).
Shown(f) == IF f \in restarting THEN 0 ELSE inc[f] + 1

\* The leader learns `f`'s identity and incarnation from the handshake. A
\* new identity starts `f`'s count over; a new incarnation on the same one
\* is the same copy and only updates it: `follow_storage` in
\* crates/server/felix-replication/src/driver/shard.rs, at the start of each
\* exchange, so an idle shard learns it too.
Probe(b, f) ==
    /\ StorageIds
    /\ bgen[b] > 0 /\ f /= b
    /\ (seen[b][f] /= sid[f] \/ seenInc[b][f] /= Shown(f))
    /\ seen' = [seen EXCEPT ![b][f] = sid[f]]
    /\ seenInc' = [seenInc EXCEPT ![b][f] = Shown(f)]
    /\ IF seen[b][f] /= sid[f]
       THEN /\ confirmed' = [confirmed EXCEPT ![b][f] = 0]
            /\ checked' = [checked EXCEPT ![b] = @ \ {f}]
       ELSE UNCHANGED << confirmed, checked >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, promised, fencing, answered, ballot, opened, setVars, ackGen, waits >>
    /\ UNCHANGED << sid, wipes, registered, inc, restarting, expect, inherited, leftDirty, gaveUp,
                    fenceIds, wipeVars >>

\* `f` answers one of `b`'s requests over a connection from the copy `b`
\* knows: the `Witness` in crates/server/felix-replication/src/driver/shard.rs.
\* A connection opened to the process before a wipe answers nothing. One
\* opened to the current copy answers whatever incarnation it named, since
\* the copy is what an answer vouches for. Only the current leader, and only
\* a follower whose answer would count, which keeps the state space down.
Answer(b, f) ==
    /\ StorageIds /\ Wipes > 0
    /\ leader = b /\ bgen[b] = gen
    /\ f /= b /\ f \notin checked[b] /\ confirmed[b][f] > 0
    /\ seen[b][f] = sid[f]
    /\ checked' = [checked EXCEPT ![b] = @ \cup {f}]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit >>
    /\ UNCHANGED << handoffVars, fenceVars, sid, seen, seenInc, wipes, registered, inc, restarting,
                    expect, inherited, leftDirty, gaveUp, fenceIds, wipeVars >>

\* A report ends `b`'s pass: what it heard before counts toward no later
\* mark.
EndPass(b) ==
    checked' = IF StorageIds /\ Wipes > 0 THEN [checked EXCEPT ![b] = {}] ELSE checked


-----------------------------------------------------------------------------

\* Only a report's delivery changes what a broker has heard.
\* The counter log changes only in the actions that name it.
\* Identities change only in a wipe, a crash, a probe, a registration and a
\* promotion.
quietVars == << counterVars, heard, idVars, named, refusalVars, wipeVars, pinVars >>
loudVars == << heard, idVars, named, refusalVars, wipeVars, pinVars >>

\* Whether the control plane takes steps.
CpUp == ~CpDown /\ ~cpGone

\* Every action is its own disjunct, with what it leaves alone conjoined
\* inside it. TLC splits a next-state relation only at disjunctions and
\* constant `\E`, and its simulation mode picks an action before a successor:
\* folded into one action, a walk would pick among successor states, and
\* Tick's clock choices would crowd out everything else.
Steps ==
    \/ Tick /\ UNCHANGED quietVars
    \/ \E b \in Brokers :
        \/ SendHeartbeat(b) /\ UNCHANGED quietVars
        \/ AcceptHeartbeat(b) /\ UNCHANGED quietVars
        \/ LoseHeartbeat(b) /\ UNCHANGED quietVars
        \/ StepDown(b) /\ UNCHANGED quietVars
        \/ Admit(b) /\ UNCHANGED quietVars
        \/ Resend(b) /\ UNCHANGED quietVars
        \/ Claim(b) /\ UNCHANGED quietVars
        \/ Commit(b) /\ UNCHANGED quietVars
        \/ AckQuorum(b) /\ UNCHANGED quietVars
        \/ Report(b) /\ EndPass(b)
           /\ UNCHANGED << counterVars, heard, sid, seen, seenInc, wipes, registered, inc, restarting,
                           expect, inherited, leftDirty, gaveUp, fenceIds, wipeVars, named, refusalVars,
                           pinVars >>
        \/ ObserveFence(b) /\ UNCHANGED quietVars
        \/ Elect(b) /\ UNCHANGED << clog, chwm, cacked, heard, idVars, named, refusalVars, wipeVars,
                                    pinVars >>
        \/ GiveUpRound(b) /\ UNCHANGED quietVars
        \/ StandOnStaleRoute(b) /\ UNCHANGED quietVars
        \/ FailStop(b) /\ UNCHANGED quietVars
        \/ Withdraw(b) /\ UNCHANGED quietVars
        \/ Depose(b) /\ UNCHANGED quietVars
        \* A restarted broker's reporter starts with no refusals.
        \/ Restart(b) /\ refusedAt' = [refusedAt EXCEPT ![b] = 0]
           /\ UNCHANGED << counterVars, heard, named, wipeVars, cpRefused, pinVars >>
        \/ FinishChange(b) /\ UNCHANGED quietVars
        \/ ForgetSet(b) /\ UNCHANGED quietVars
        \/ RetryContinuingFence(b)
           /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipedAt, readmit, learner,
                           marked, cpLearner, forgot >>
        \/ \E f \in Brokers :
            \/ Ship(b, f) /\ UNCHANGED quietVars
            \/ LearnHwm(b, f) /\ UNCHANGED quietVars
            \/ AnswerFence(b, f) /\ UNCHANGED quietVars
            \/ RefuseStaleSet(b, f) /\ UNCHANGED quietVars
            \/ WipedLearnSet(b, f) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipedAt, readmit, cpLearner, contAt, forgot, pinVars >>
            \/ AnswerSetlessFence(b, f) /\ UNCHANGED quietVars
            \/ CatchUp(b, f) /\ UNCHANGED quietVars
            \/ CatchUpCounters(b, f) /\ UNCHANGED << chwm, canswered, cacked, heard, idVars, named, refusalVars, wipeVars, pinVars >>
            \/ ShipCounter(b, f) /\ UNCHANGED loudVars
            \/ AnswerCounterFence(b, f) /\ UNCHANGED loudVars
            \/ Probe(b, f) /\ UNCHANGED << counterVars, heard, named, refusalVars, pinVars >>
            \/ Answer(b, f) /\ UNCHANGED << counterVars, heard, named, refusalVars, pinVars >>
        \/ Wipe(b) /\ UNCHANGED << counterVars, heard, named, refusalVars, pinVars >>
        \/ Forget(b) /\ forgot' = forgot \cup {b}
           /\ UNCHANGED << cconfirmed, canswered, cacked, heard, named, refusalVars, readmit, contAt, pinVars >>
        \/ Readmit(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipedAt, learner, marked, cpLearner, forgot, pinVars >>
        \/ Readmitted(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, cpLearner, contAt, forgot, pinVars >>
        \/ CpAdmit(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipedAt, readmit, learner, marked, contAt, forgot, pinVars >>
        \/ GroupsCaughtUp(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipedAt, readmit, learner, cpLearner, contAt, forgot, pinVars >>
        \/ KeepOwnSet(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipedAt, readmit, learner, cpLearner, contAt, forgot, pinVars >>
        \* The reporter's refusals live in the broker's memory.
        \/ Crash(b) /\ refusedAt' = [refusedAt EXCEPT ![b] = 0]
           /\ UNCHANGED << counterVars, named, cpRefused, pinVars >>
        \/ CleanStop(b) /\ UNCHANGED << counterVars, heard, named, refusalVars, pinVars >>
        \/ Register(b) /\ UNCHANGED << counterVars, heard, named, refusalVars, pinVars >>
        \/ OpenStep(b)
        \/ ChangeCopy(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipeVars >>
        \/ CpUp /\ ReadmitCopy(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipeVars >>
        \/ CpUp /\ CopyAdmitted(b) /\ UNCHANGED << counterVars, heard, idVars, named, refusalVars, wipeVars >>
        \/ CpUp /\ Decide(Now, b, cpView) /\ UNCHANGED << heard, decideIdVars, named, refusalVars >>
        \/ /\ CpUp /\ WriteDropsLearners
           /\ Decide(Now, b, cpView)
           /\ cpLearner' = {}
           /\ UNCHANGED << heard, sid, wipes, checked, registered, inc, restarting, leftDirty, gaveUp,
                          wipedAt, readmit, learner, marked, contAt, forgot, named, refusalVars >>
        \/ /\ CpUp
           /\ \E p \in Planners : \E v \in cpView[p] : Decide(v, b, [cpView EXCEPT ![p] = {}])
           /\ UNCHANGED << heard, decideIdVars, named, refusalVars >>
        \/ CommitCounter(b) /\ UNCHANGED loudVars
        \/ AckCounters(b) /\ UNCHANGED loudVars
    \/ LoseReport /\ UNCHANGED quietVars
    \/ CpUp /\ CpFlip /\ UNCHANGED quietVars
    \/ CpUp /\ CpFlipNamed /\ UNCHANGED << clog, chwm, cacked, heard, idVars, named, refusalVars, wipeVars, pinVars >>
    \/ CpUp /\ (\E p \in Planners : Snapshot(p)) /\ UNCHANGED quietVars
    \/ CpUp /\ DeliverReport /\ UNCHANGED << counterVars, idVars, wipeVars, refusalVars, pinVars >>
    \/ CpUp /\ \E b \in Brokers :
        \/ SendRefusal(b) /\ UNCHANGED notRefusalVars
        \/ PassReport(b) /\ UNCHANGED notRefusalVars

\* The control plane stops for good, under `CpStops`.
CpStop ==
    /\ CpStops /\ ~cpGone
    /\ cpGone' = TRUE
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
           hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
           draining, successor, stopped, moves, ver, cpView, staged, heard,
           promised, fencing, answered, confirmed, out, mine, joining, leaving, joinedAt,
           counterVars, ballot, opened, setVars, ackGen, waits, idVars, named, refusalVars, wipeVars, pinVars >>

Next ==
    \/ Steps /\ UNCHANGED cpGone
    \/ CpStop

Spec == Init /\ [][Next]_vars

\* What liveness assumes: every broker step an election needs is taken once
\* it stays possible. Nothing of the control plane's is.
Fairness ==
    /\ \A b \in Brokers :
        /\ WF_vars(Elect(b) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(GiveUpRound(b) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(OpenStep(b) /\ UNCHANGED cpGone)
    /\ \A b, f \in Brokers :
        /\ WF_vars(AnswerFence(b, f) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(RefuseStaleSet(b, f) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(Ship(b, f) /\ UNCHANGED << quietVars, cpGone >>)

LiveSpec == Spec /\ Fairness

\* With the control plane up: its decisions on a report or a stopped leader,
\* a candidate's stand timeout, a stopped broker's restart, and the leader's
\* mark, each taken once it stays possible.
CpFairness ==
    /\ \A b \in Brokers :
        /\ WF_vars(Promote(Now, b, cpView)
                   /\ UNCHANGED << counterVars, heard, decideIdVars, contAt, named, refusalVars, cpGone >>)
        /\ WF_vars(Withdraw(b) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(Depose(b) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(Restart(b) /\ refusedAt' = [refusedAt EXCEPT ![b] = 0]
                   /\ UNCHANGED << counterVars, heard, named, wipeVars, cpRefused, pinVars, cpGone >>)
        /\ WF_vars(AckQuorum(b) /\ UNCHANGED << quietVars, cpGone >>)
    /\ WF_vars(CpRename(Now, cpView)
               /\ UNCHANGED << counterVars, expect, inherited, seen, seenInc, fenceIds,
                               heard, decideIdVars, contAt, named, refusalVars, cpGone >>)

\* With the control plane stopped for good at some point and every stopped
\* broker back, elections alone get a leader serving: a candidate's stand
\* timeout, a restart and the leader's mark are taken once they stay
\* possible, and so is the control plane's stop. Nothing of the control
\* plane's is owed while it runs.
CpDownFairness ==
    /\ \A b \in Brokers :
        /\ WF_vars(Withdraw(b) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(Depose(b) /\ UNCHANGED << quietVars, cpGone >>)
        /\ WF_vars(Restart(b) /\ refusedAt' = [refusedAt EXCEPT ![b] = 0]
                   /\ UNCHANGED << counterVars, heard, named, wipeVars, cpRefused, pinVars, cpGone >>)
        /\ WF_vars(AckQuorum(b) /\ UNCHANGED << quietVars, cpGone >>)
    /\ WF_vars(CpStop)

\* The live members that hold `l`'s generation for `l`, and so take its batches.
Backers(l) == { m \in Members(l) \ halted :
                  promised[m] = bgen[l] /\ (Ballots => ballot[m] \subseteq {l}) }

\* A live broker leads, open and serving, with a mark over its whole log and a
\* majority that takes its batches. A member left promised past it is not
\* counted against it: it serves without that copy.
ServesWith(l) ==
    /\ l \notin halted /\ bgen[l] > 0 /\ ~fencing[l] /\ ~stopped[l]
    /\ hwm[l] = Len(log[l])
    /\ LeaderMajority(l, Backers(l))

ElectedServes == \E l \in Brokers : ServesWith(l)

\* With the control plane up and every stopped broker back, the leader it
\* names ends up serving, with a majority taking its batches, and stays.
CpLeaderServes == <>[](LeaderServes /\ ServesWith(leader))

CpDownServes == <>[]ElectedServes

\* The leader serving has every live member of its set behind it. Not owed:
\* with the control plane down, a member left promised past a leader that
\* goes on serving on the rest stays one copy short for good, since every
\* other member hears that leader and refuses its pre-vote. The witness is
\* FelixShardCpDownDegraded.cfg.
FullyBacked == \E l \in Brokers : ServesWith(l) /\ Members(l) \ halted \subseteq Backers(l)

\* Some live broker would stand, but the model's bound on elections stops it.
Exhausted == \E f \in Brokers : ~ElectBound(f) /\ ENABLED ElectStep(f)

\* `CpDownServes`, or elections stop only at the bound with a broker that
\* would stand again: what a bounded model can say of an unbounded run.
CpDownServesInBound == <>[]ElectedServes \/ <>[]Exhausted

CpDownFullyBacked == <>[]FullyBacked \/ <>[]Exhausted

\* Some live broker leads, open, at a generation no live broker promised past.
LeaderSettled ==
    \E l \in Brokers \ halted :
        /\ bgen[l] > 0 /\ ~fencing[l]
        /\ \A m \in Brokers \ halted : promised[m] <= bgen[l]

\* Elections end with a leader that stays: none is missing for good, and
\* candidates do not depose each other forever.
ElectionSettles == <>[]LeaderSettled

\* The control plane's leader can never open at the control plane's
\* generation: another broker was elected there, opened, and still leads.
Collided ==
    \E b \in Brokers :
        b /= leader /\ bgen[b] = gen /\ ~fencing[b] /\ <<gen, b>> \in opened

\* The control plane's steps on an elected leader's report, taken once they
\* stay possible: adopting a leader elected past its generation, and moving
\* past one it collided with.
ReportFairness ==
    \A b \in Brokers :
        /\ WF_vars(CpAdopt(Now, b, cpView)
                   /\ UNCHANGED << counterVars, expect, inherited, seen, seenInc, fenceIds,
                                   heard, decideIdVars, contAt, named, refusalVars, pinVars, cpGone >>)
        /\ WF_vars(CpReissue(Now, b, cpView)
                   /\ UNCHANGED << counterVars, expect, inherited, seen, seenInc, fenceIds,
                                   heard, decideIdVars, contAt, named, refusalVars, pinVars, cpGone >>)

ReissueSpec == Spec /\ Fairness /\ ReportFairness

\* Liveness once the control plane stops (FelixShardCpDown*.cfg).
CpDownSpec == Spec /\ Fairness /\ CpDownFairness

\* Liveness with the control plane up (FelixShardCpLive.cfg).
CpLiveSpec == Spec /\ Fairness /\ ReportFairness /\ CpFairness

\* Under `Renames`, the control plane's rename, taken once it stays possible.
RenameFairness ==
    /\ WF_vars(CpRename(Now, cpView)
               /\ UNCHANGED << counterVars, expect, inherited, seen, seenInc, fenceIds,
                               heard, decideIdVars, contAt, named, refusalVars, cpGone >>)
    \* Under `ReportStore`, a refusal the leader still has is reported again
    \* (`report_refusal` in crates/server/felix-replication/src/driver/shards.rs).
    /\ \A b \in Brokers : WF_vars(CpUp /\ SendRefusal(b) /\ UNCHANGED notRefusalVars)

\* Elections with the control plane up, renaming its leader on a refusal.
RenameSpec == Spec /\ Fairness /\ ReportFairness /\ RenameFairness

\* A generation whose leader can never open does not stay the control
\* plane's while the leader elected there leads. Once that leader is deposed
\* a later election moves the generation on, which the model bounds, so it
\* cannot ask for that.
CollisionSuperseded ==
    \A g \in 1..(Adopt + Suspicions + 1) :
        (gen = g /\ Collided) ~> (gen /= g \/ ~Collided)

\* The control plane's leader does not stay open below its own ballot.
OwnBallotCaughtUp == BelowOwnBallot ~> ~BelowOwnBallot

\* Nor does it stay fencing below its own ballot (#1940).
OwnBallotFenceCaughtUp == FencingBelowOwnBallot ~> ~FencingBelowOwnBallot

\* Nobody stands against an open leader that a majority of its set hears:
\* pre-vote keeps a working leader in place. With the control plane down, a
\* broker takes a new generation and fences only by standing, so this covers
\* a candidate standing again higher too.
NoStandAgainstHeardLeader ==
    [][\A f \in Brokers :
          fencing'[f] /\ bgen'[f] /= bgen[f] =>
              ~\E l \in Brokers \ halted :
                  /\ bgen[l] > 0 /\ ~fencing[l]
                  /\ MajorityOf({ m \in Members(l) : Hears(m) } \cup {l}, Members(l))]_vars

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

\* The newest generation anyone holds: the control plane's, or one a replica
\* elected itself to. Only `Elect` takes a broker past the control plane's.
Newest == LET gs == {gen} \cup { bgen[m] : m \in Brokers }
          IN CHOOSE g \in gs : \A h \in gs : h <= g

\* Placement never names a copy leader that refuses to vote: every
\* non-voter is one placement holds as a learner, so `Promote` and `Swap`,
\* which skip those, skip every non-voter.
PlacementKnowsLearners ==
    \A f \in Brokers : NonVoter(f) => f \in cpLearner

\* The leader at the newest generation holds every acknowledged record once
\* it may serve. Unlike AckedSurvive this says nothing about the lease, so it
\* holds or fails whatever a deposed leader's clock tells it.
\*
\* Where opens are tracked, generations are not one leader's after another,
\* so the check is per generation: every leader that opened holds every
\* record acknowledged at its generation or below. A newest generation
\* misleads there: an elected leader that steps down leaves a deposed one as
\* the newest (FelixShardElectStepDown.cfg), and a candidate's promise to
\* itself that reached nobody hides the leader below it.
\* Under `LogDropsSets`, every generation's set before it is recorded.
LeftRecorded == LogDropsSets => Len(cpLeft) = gen

AckedHeldByLeader ==
    IF TracksOpens
    THEN \A b \in Brokers : (bgen[b] > 0 /\ ~fencing[b] /\ <<bgen[b], b>> \in opened) =>
            \A a \in ackGen : a[2] <= bgen[b] => \E i \in 1..Len(log[b]) : log[b][i].id = a[1]
    ELSE \A b \in Brokers : (bgen[b] = Newest /\ ~fencing[b]) =>
            \A id \in acked : \E i \in 1..Len(log[b]) : log[b][i].id = id

\* A witness, not a safety property: no leader that opened counted a fence
\* answer from a replica its promotion named by inheritance. Under
\* `NameByInheritance` TLC finds one (FelixShardFencedCrashInheritedCounts.cfg,
\* FelixShardFencedWipeInheritedCounts.cfg,
\* FelixShardFencedLeaderCrashInheritedCounts.cfg and
\* FelixShardFencedLeaderCrashTwiceInheritedCounts.cfg), which shows the configurations that pass with it get that far.
NoInheritedCount ==
    \A b \in Brokers : (bgen[b] > 0 /\ ~fencing[b]) => answered[b] \cap inherited[b] = {}

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

\* Under `Elections` or `Suspicions`, no two brokers open for writes at one
\* generation.
OneLeaderPerGeneration ==
    \A x, y \in opened : x[1] = y[1] => x[2] = y[2]

\* The control plane's leader, once open at the control plane's generation,
\* leads the set the control plane records: an adopted leader included. While
\* a cut-over is pending, the control plane records the set it moves to as
\* well, and its leader leads that one. Only while it still holds that
\* generation: a leader that stepped down learns newer sets.
AdoptedMatchesOpened ==
    <<gen, leader>> \in opened /\ bgen[leader] = gen =>
        mine[leader] = IF cpNext /= {} THEN cpNext ELSE CpSet

\* A witness, not a safety property: the control plane never has a leader to
\* name again (`RenameDue`) that never took its generation. TLC finds one
\* under `Renames` (FelixShardElectRenameDue.cfg), which shows the
\* configuration that passes with it gets that far.
NeverRenameDue == ~(RenameDue /\ bgen[leader] < gen)

\* The same for a leader that took the control plane's generation and is
\* still fencing there (FelixShardElectRenameFencingDue.cfg).
NeverRenameWhileFencing == ~(RenameDue /\ bgen[leader] = gen)

\* The same for a leader that lost the set it leaves after it opened its
\* generation, past the first (FelixShardReplacedSetUnknownRenames.cfg): the
\* rename `SetLossUpTo` allows is reachable. The first generation is left out
\* because the model starts with its leader open there.
NeverRenameUnknownSet ==
    ~(/\ RenameDue /\ gen > 1 /\ bgen[leader] = gen /\ fencing[leader]
      /\ SetUnknown(leader) /\ <<gen, leader>> \in opened)
\* The same at the first generation, where no generation precedes it
\* (`NoSetBefore::Unknown(0)` in promotion/joint.rs: a log keeps no set at
\* generation 0, so the broker's own ballot there is the evidence). TLC finds
\* one under FelixShardReplacedSetUnknownFirstRenames.cfg.
NeverRenameFirstUnknownSet ==
    ~(/\ RenameDue /\ gen = 1 /\ bgen[leader] = gen /\ fencing[leader]
      /\ SetUnknown(leader) /\ <<gen, leader>> \in opened)

\* Under `SetCheck`, once a set change opened, a majority of the new set and
\* of the set before it know the new set, so no replica on the old set alone
\* can gather a majority. The joint fence is what makes the second half true.
\* A non-voter is counted as knowing it: it is in no majority, so it cannot
\* help an old-set replica gather one, and a second wipe would otherwise
\* break this by forgetting the set rather than by any majority forming.
SetChangesJoint ==
    \A r \in installed :
        LET knows == { m \in Brokers : setGen[m] >= r[1] \/ (NonVoterCredit /\ NonVoter(m)) }
        IN MajorityOf(knows, r[2]) /\ \A o \in r[3] : MajorityOf(knows, o)

\* Witnesses, not safety properties, under `WipedNonVoter`. TLC finds: a
\* leader at a generation taken after the wipe (above every generation
\* promised then) opened without the wiped replica while it is still a
\* non-voter (FelixShardElectWipeNonVoterElects.cfg); the wiped replica
\* re-admitted (FelixShardElectWipeNonVoterReadmits.cfg); and the control
\* plane failing over beside a non-voter, outside a re-admission
\* (FelixShardElectWipeNonVoterFailsOver.cfg). They show the paths the
\* passing configurations check are reached; that they are eventually taken
\* needs fairness the model does not have.
NoLeaderBesideNonVoter ==
    \A b \in Brokers : (<<bgen[b], b>> \in opened /\ bgen[b] > 0 /\ ~fencing[b]) =>
        ~\E w \in Brokers : NonVoter(w) /\ bgen[b] > wipedAt[w] /\ w \notin answered[b]
NeverReadmitted == wipes > 0 => \E m \in Brokers : wipedAt[m] > 0
\* Under `Creates`: a new shard opens without a copy wiped before it answered
\* (FelixShardElectWipeNonVoterCreatesOpens.cfg).
NoCreationBesideNonVoter ==
    \A b, w \in Brokers : ~(<<1, b>> \in opened /\ NonVoter(w) /\ w \notin answered[b])
\* Under `Creates`: the wiped copy re-admitted, a write acknowledged at the
\* re-admission's generation or later, then a leader opening above it
\* (FelixShardElectWipeNonVoterCreatesReadmits.cfg).
NoFailoverAfterReadmit ==
    ~(/\ wipes > 0 /\ \A m \in Brokers : ~Wiped(m)
      /\ \E r \in ackGen : \E o \in opened : r[2] >= 2 /\ o[1] > r[2])
\* A state constraint for the creation configurations at `Adopt = 3`: no
\* broker promises generation 3 until the wiped copy is re-admitted. What
\* comes before re-admission is checked as at `Adopt = 2`, which is all that
\* finishes, and the generation after it is still reached.
ThreeOnceReadmitted ==
    \A b \in Brokers : promised[b] <= 2 \/ (wipes > 0 /\ \A m \in Brokers : ~Wiped(m))
NoFailoverBesideNonVoter ==
    (\E w \in Brokers : NonVoter(w)) =>
        ~(fencing[leader] /\ bgen[leader] = gen /\ \A r \in readmit : r[2] /= gen)

\* No continuing open completes having counted a copy that was already a
\* non-voter when the fence began (wiped below the fence's generation), other
\* than the one it re-admits. Under `ContinuingKeptBallot` this holds: every
\* non-voter in the model lost its promises, so none is trusted
\* (FelixShardElectUncleanContinuing.cfg). Under `ContinuingTrustsBallot`
\* alone TLC finds one.
\* A canary: no continuing open completes after retrying its fence. Under
\* `ContinuingRetries` TLC should find one
\* (FelixShardElectUncleanContinuingRetry.cfg).
RetriedContinuingNeverOpens ==
    \A b \in Brokers : ~(Continuing(b) /\ contAt[b][3] = 1 /\ ~fencing[b])

NeverContinuingCountsNonVoter ==
    \A b \in Brokers : (bgen[b] > 0 /\ ~fencing[b] /\ <<bgen[b], b>> \in opened /\ Continuing(b)) =>
        \A f \in answered[b] : ~(NonVoter(f) /\ wipedAt[f] < bgen[b] /\ \A r \in readmit : r[1] /= f)

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
\*
\* Under `WipedNonVoter` a non-voter counts as holding every record: its
\* wipe is the loss the record already survived, and only voters' copies
\* can be counted by a fence or an acknowledgement. What is checked is that
\* no majority the voters can form lacks the record (README, "A wiped voter
\* in an election").
AckedOnMajority ==
    Quorum =>
        \A id \in acked :
            LET holders == { b \in Counted : (Wiped(b) /\ NonVoterCredit) \/ \E i \in 1..Len(log[b]) : log[b][i].id = id }
            IN \/ Cardinality(holders) * 2 > Cardinality(Counted)
               \/ Cardinality(Counted) = 4 /\ Cardinality(holders) = 2

\* The records a leader acknowledges in a step (raising its `hwm`, as
\* AckQuorum does) are held then by a majority of the copies that exist, a
\* wiped one holding nothing. A follower wiped after it answered the pass
\* that counted it is let off: to the leader that wipe is a fault after the
\* acknowledgement. Counting a follower's old answer without one in the pass
\* breaks it (FelixShardWipe.cfg).
AckedOnLiveMajority ==
    [][\A b \in Brokers :
          hwm'[b] > hwm[b] =>
              \A id \in acked' \ acked :
                  Majority({ m \in Brokers : \E i \in 1..Len(log'[m]) : log'[m][i].id = id }
                           \cup { f \in checked'[b] : seen'[b][f] /= sid'[f] })]_vars

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

\* Every acknowledged counter update is on a majority of the set the control
\* plane records, as AckedOnMajority for the cache log: the old set while a
\* change is pending, the new set after the flip.
CountersOnSetMajority ==
    Quorum =>
        \A id \in cacked :
            LET holders == { b \in Counted : \E i \in 1..Len(clog[b]) : clog[b][i].id = id }
            IN \/ Cardinality(holders) * 2 > Cardinality(Counted)
               \/ Cardinality(Counted) = 4 /\ Cardinality(holders) = 2

\* Every acknowledged counter update is on a majority of the replicas.
\* A non-voter is credited as `AckedOnMajority` credits it.
CountersOnMajority ==
    \A id \in cacked :
        MajorityOf({ b \in Brokers : (NonVoterCredit /\ (Wiped(b) \/ (GroupLogs /\ marked[b])))
                                     \/ \E i \in 1..Len(clog[b]) : clog[b][i].id = id }, Brokers)

TypeOK ==
    /\ now \in 0..MaxTime
    /\ cpGone \in BOOLEAN
    /\ gen \in Nat
    /\ leader \in Brokers
    /\ halted \subseteq Brokers
    /\ writes \in 0..MaxWrites
    /\ draining \in BOOLEAN
    /\ successor \in Brokers
    /\ moves \in 0..(MaxMoves + Swaps)
    /\ ver \in Nat
    /\ \A p \in Planners : Cardinality(cpView[p]) <= 1
    /\ staged \subseteq Brokers /\ Cardinality(staged) <= 1
    /\ heard \in [Brokers -> [holders : SUBSET Brokers, len : Nat, drained : BOOLEAN, gen : Nat,
                               sids : [Brokers -> Nat], incs : [Brokers -> Nat],
                               inh : [Brokers -> Nat], names : SUBSET Brokers]]
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
    /\ ballot \in [Brokers -> SUBSET Brokers]
    /\ \A o \in opened : o[1] \in Nat /\ o[2] \in Brokers
    /\ setGen \in [Brokers -> Nat]
    /\ oldSet \in [Brokers -> SUBSET SUBSET Brokers]
    /\ cpNext \subseteq Brokers /\ cpNextGen \in Nat
    /\ cpLeft \in Seq(SUBSET Brokers)
    /\ Len(cutOver) <= (IF SplitAtCutOver \/ SplitAtFailover THEN 1 ELSE 0)
    /\ splits \in 0..Splits
    /\ stands \in 0..MaxStandings
    /\ \A a \in ackGen : a[1] \in 1..MaxWrites /\ a[2] \in Nat
    /\ \A wt \in waits : wt[1] \in 1..MaxWrites /\ wt[2] \in Brokers /\ wt[3] \in Nat /\ wt[4] \in Nat
    /\ sid \in [Brokers -> Nat] /\ seen \in [Brokers -> [Brokers -> Nat]]
    /\ seenInc \in [Brokers -> [Brokers -> Nat]]
    /\ checked \in [Brokers -> SUBSET Brokers]
    /\ registered \in [Brokers -> Nat] /\ inc \in [Brokers -> Nat] /\ restarting \subseteq Brokers
    /\ expect \in [Brokers -> [Brokers -> Nat]]
    /\ inherited \in [Brokers -> SUBSET Brokers]
    /\ leftDirty \in [Brokers -> Nat]
    /\ gaveUp \in [Brokers -> Nat] /\ fenceIds \in [Brokers -> SUBSET Brokers] /\ named \subseteq Brokers
    /\ cpRefused \subseteq (Nat \X Brokers) /\ refusedAt \in [Brokers -> Nat]
    /\ wipedAt \in [Brokers -> Nat] /\ learner \in [Brokers -> BOOLEAN] /\ cpLearner \subseteq Brokers
    /\ marked \in [Brokers -> BOOLEAN]
    /\ \A r \in readmit : r[1] \in Brokers /\ r[2] \in Nat /\ r[3] \in Brokers
    /\ wipes \in 0..(IF LosePromises THEN Forgets ELSE Wipes)
    /\ contAt \in [Brokers -> Nat \X Nat \X (0..1)]
    /\ forgot \subseteq Brokers
    /\ pinned \subseteq Brokers /\ stalePin \in [Brokers -> SUBSET Brokers]
    /\ changed \subseteq Brokers /\ changes \in 0..CopyChanges
    /\ \A r \in admitting : r[1] \in Brokers /\ r[2] \in Nat

=============================================================================
