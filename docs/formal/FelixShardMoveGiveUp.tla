------------------------- MODULE FelixShardMoveGiveUp -------------------------
(***************************************************************************)
(* A fenced move whose leader crashes uncleanly before it reports drained. *)
(*                                                                         *)
(* The control plane fences a move: the leader takes a new generation,     *)
(* stops serving and ships to its successor until it can report drained. A *)
(* leader that crashes uncleanly in that window comes back having given    *)
(* its copy up: it leads nothing from it until a probe of every follower   *)
(* finds none ahead of it, and it never reports drained. Its probe reports *)
(* the give-up at the move's generation, with every follower's position.   *)
(*                                                                         *)
(* While the move is fenced, placement promotes nobody: a failover passes  *)
(* the successor over (`PromoteDestination` in FelixShard.tla) and ignores *)
(* the give-up (`replaced` in placement/plan.rs). So the move ends only    *)
(* through its own steps. With `RetakeGivenUp` the give-up at the move's   *)
(* generation is one of them: the leader is named again at once, as for    *)
(* the fence timeout (`move_step` in placement/moves.rs). It leads only if *)
(* its probe found no follower ahead (`may_lead` in services/felix-broker- *)
(* service/src/shards/lifecycle.rs); otherwise it gives that generation up *)
(* too, and with no move left placement promotes the follower the probe    *)
(* names furthest.                                                         *)
(*                                                                         *)
(* Without `RetakeGivenUp` only the fence timeout ends the wait. Its clock *)
(* is the placement instance's own and starts again on every control-plane *)
(* restart, so no bound holds, and the model owes it nothing: the shard    *)
(* waits for good (FelixShardMoveGiveUpStall.cfg). With `LeadsGivenUp` the *)
(* named leader leads its given-up copy without the probe's word, and TLC  *)
(* finds an acknowledged record gone (FelixShardMoveGiveUpLeads.cfg). The  *)
(* retake is safe because the broker refuses, not because of the timeout.  *)
(*                                                                         *)
(* Below three seats' worth of replication factor the set starts one       *)
(* above it, as with a staged destination. A move given up keeps its       *)
(* destination in a set of three (`keeps_destination` in moves.rs): a      *)
(* record can be acknowledged on the leader and the destination alone, and *)
(* the next probe measures only the set it is given. A later trim fence    *)
(* drops the extra copy once the rest hold the leader's whole log (`trim`  *)
(* in moves.rs). With `DropsDestination` the fence timeout's retake drops  *)
(* it instead, and with `UnstageDrops` a staged move given up before its   *)
(* fence does; TLC finds an acknowledged record gone either way            *)
(* (FelixShardMoveGiveUpDrop.cfg, FelixShardMoveGiveUpUnstage.cfg). Kept,  *)
(* the staged undo only names the leader again with the same set, and the  *)
(* model leaves it out.                                                    *)
(*                                                                         *)
(* Abstractions: the successor is one of the seats, already in the set. A  *)
(* cut-over keeps the set; the real one cuts it to the factor through a    *)
(* pending set change, which FelixShard.tla models.                        *)
(* Only a broker that leads, fences or drains crashes, and every crash is  *)
(* unclean: any suffix may go. A given-up copy answers no fence until a    *)
(* newer leader ships to it (`refuse_short_copy` in replica.rs). Placement *)
(* reads the brokers' state directly, and a probe measures every follower. *)
(* Leases are left out: a promoted leader fences a majority.               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Nodes,      \* the seats
    First,      \* the leader serving generation 1 at the start
    NoNode,
    MaxGen,     \* highest generation placement names
    MaxWrites,  \* client writes
    MaxCrashes, \* unclean crashes of a broker that leads, fences or drains
    MaxMoves    \* fenced moves

\* Knobs, overridden in a configuration with `<-`.
Yes == TRUE
No == FALSE
\* The fix: a give-up at the fenced move's generation retakes at once.
RetakeGivenUp == TRUE
\* Unsafe: the named leader leads a given-up copy without the probe.
LeadsGivenUp == FALSE
\* The replication factor. Below the seats, the set starts above it.
Replication == Cardinality(Nodes)
Two == 2
\* Unsafe: the fence timeout's retake drops the destination when the set
\* is above the factor.
DropsDestination == FALSE
\* Unsafe: a staged move given up before its fence drops the destination.
UnstageDrops == FALSE
\* Crashes only while a move with its destination staged above the factor is
\* fenced. In a set of two a leader's unclean crash stalls the shard whatever
\* the move rule, as its follower cannot fence a given-up copy, so a liveness
\* check at a factor of two uses this.
CrashesInMove == FALSE

ASSUME First \in Nodes /\ NoNode \notin Nodes

Start(g) == [g |-> g, w |-> 0]
Range(s) == {s[i] : i \in DOMAIN s}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min2(a, b) == IF a < b THEN a ELSE b
\* X holds a majority of the set S.
MajorityIn(X, S) == 2 * Cardinality(X \cap S) > Cardinality(S)
LastGen(s) == IF s = <<>> THEN 0 ELSE s[Len(s)].g
Prefix(s, j) == SubSeq(s, 1, j)
Common(s, t) == Max({j \in 0..Min2(Len(s), Len(t)) : Prefix(s, j) = Prefix(t, j)})
\* (last generation, length): the order a catch-up and a probe compare by.
Ahead(s, t) == LastGen(s) > LastGen(t) \/ (LastGen(s) = LastGen(t) /\ Len(s) > Len(t))
NotBehind(s, t) == ~Ahead(t, s)

VARIABLES
    log,        \* each seat's copy
    promised,   \* the newest generation each copy accepted
    bgen,       \* the generation each broker leads, fences or drains at; 0 for none
    mode,       \* "idle", "fencing", "open" (serving) or "draining" (fenced by a move)
    ans,        \* a fencing broker's answers: the logs it was sent
    gaveUp,     \* the generation each broker gave its copy up at; 0 for none
    reclaimAt,  \* the generation a probe found no follower ahead of it; 0 for none
    bset,       \* the set each broker took with its generation
    cpGen, cpLeader,
    cpSet,      \* the set placement names
    cpKind,     \* how the named leader takes cpGen: "fence", "cutover" or "move"
    cpDraining, cpSucc,
    drained,    \* the leader reported drained at cpGen, its successor level with it
    report,     \* the newest give-up report: [gen, pos], gen 0 for none
    moves, acked, opened, writes, crashes

vars == <<log, promised, bgen, mode, ans, gaveUp, reclaimAt, bset, cpGen, cpLeader, cpSet, cpKind,
          cpDraining, cpSucc, drained, report, moves, acked, opened, writes, crashes>>
cpVars == <<cpGen, cpLeader, cpSet, cpKind, cpDraining, cpSucc, drained, moves>>

NoAns == [n \in Nodes |-> <<>>]
NoReport == [gen |-> 0, pos |-> [n \in Nodes |-> <<>>]]

Init ==
    /\ log = [n \in Nodes |-> <<Start(1)>>]
    /\ promised = [n \in Nodes |-> 1]
    /\ bgen = [n \in Nodes |-> IF n = First THEN 1 ELSE 0]
    /\ mode = [n \in Nodes |-> IF n = First THEN "open" ELSE "idle"]
    /\ ans = [n \in Nodes |-> [has |-> {}, logs |-> NoAns]]
    /\ gaveUp = [n \in Nodes |-> 0]
    /\ reclaimAt = [n \in Nodes |-> 0]
    /\ bset = [n \in Nodes |-> Nodes]
    /\ cpGen = 1 /\ cpLeader = First /\ cpSet = Nodes /\ cpKind = "fence"
    /\ cpDraining = FALSE /\ cpSucc = NoNode /\ drained = FALSE
    /\ report = NoReport
    /\ moves = 0 /\ acked = {} /\ opened = {<<1, First>>} /\ writes = 0 /\ crashes = 0

(***************************************************************************)
(* Brokers.                                                                *)
(***************************************************************************)
Leading(l) == mode[l] \in {"open", "draining"}

Write(l) ==
    /\ mode[l] = "open" /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ log' = [log EXCEPT ![l] = Append(@, [g |-> bgen[l], w |-> writes + 1])]
    /\ UNCHANGED <<promised, bgen, mode, ans, gaveUp, reclaimAt, bset, report, acked, opened, crashes>>
    /\ UNCHANGED cpVars

\* A batch: the leader's log from where the two agree, or just its
\* generation. A broker leading below it stops; a given-up copy the batch
\* reached holds a prefix of a newer leader's log and is not given up any more.
Ship(l, m) ==
    /\ Leading(l) /\ m /= l /\ m \in bset[l]
    /\ promised[m] <= bgen[l]
    /\ \E k \in Common(log[m], log[l])..Len(log[l]) :
        /\ k > Common(log[m], log[l]) \/ promised[m] < bgen[l]
        /\ log' = [log EXCEPT ![m] = Prefix(log[l], k)]
    /\ promised' = [promised EXCEPT ![m] = bgen[l]]
    /\ IF bgen[m] < bgen[l] /\ mode[m] /= "idle"
       THEN /\ mode' = [mode EXCEPT ![m] = "idle"]
            /\ bgen' = [bgen EXCEPT ![m] = 0]
       ELSE UNCHANGED <<mode, bgen>>
    /\ gaveUp' = [gaveUp EXCEPT ![m] = IF @ < bgen[l] THEN 0 ELSE @]
    /\ UNCHANGED <<ans, reclaimAt, bset, report, acked, opened, writes, crashes>>
    /\ UNCHANGED cpVars

\* Acknowledge the client records of a prefix a majority holds at the
\* leader's generation, the leader included.
Ack(l) ==
    /\ Leading(l)
    /\ \E j \in 1..Len(log[l]), M \in SUBSET Nodes :
        /\ l \in M /\ M \subseteq bset[l] /\ MajorityIn(M, bset[l])
        /\ \A m \in M \ {l} : promised[m] = bgen[l] /\ Len(log[m]) >= j
                               /\ Prefix(log[m], j) = Prefix(log[l], j)
        /\ acked' = acked \cup {[r |-> log[l][i], at |-> bgen[l]] : i \in {x \in 1..j : log[l][x].w > 0}}
    /\ UNCHANGED <<log, promised, bgen, mode, ans, gaveUp, reclaimAt, bset, report, opened, writes, crashes>>
    /\ UNCHANGED cpVars

\* A broker that leads, fences or drains crashes uncleanly and restarts at
\* once. It may have lost any suffix, so it gives its copy up at the
\* generation it had accepted.
Crash(n) ==
    /\ crashes < MaxCrashes /\ mode[n] /= "idle"
    /\ CrashesInMove => cpDraining /\ cpSucc /= NoNode /\ Cardinality(cpSet) > Replication
    /\ \E k \in 0..Len(log[n]) : log' = [log EXCEPT ![n] = Prefix(@, k)]
    /\ mode' = [mode EXCEPT ![n] = "idle"]
    /\ bgen' = [bgen EXCEPT ![n] = 0]
    /\ ans' = [ans EXCEPT ![n] = [has |-> {}, logs |-> NoAns]]
    /\ gaveUp' = [gaveUp EXCEPT ![n] = promised[n]]
    /\ reclaimAt' = [reclaimAt EXCEPT ![n] = 0]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<promised, bset, report, acked, opened, writes>>
    /\ UNCHANGED cpVars

\* `may_lead`: a given-up copy leads only at a generation newer than a probe
\* that found no follower ahead of it.
MayLead(n) ==
    \/ gaveUp[n] = 0
    \/ LeadsGivenUp
    \/ reclaimAt[n] > 0 /\ cpGen > reclaimAt[n]

CanTake(n) ==
    CASE cpKind = "move" -> mode[n] = "open"
      [] cpKind = "fence" -> MayLead(n)
      [] OTHER -> TRUE

\* The broker placement names takes the generation: the move's leader
\* drains at it, a cut-over's successor opens at once on its log, and any
\* other leader fences a majority and catches up first.
Take(n) ==
    /\ cpLeader = n /\ promised[n] < cpGen /\ CanTake(n)
    /\ promised' = [promised EXCEPT ![n] = cpGen]
    /\ bgen' = [bgen EXCEPT ![n] = cpGen]
    /\ bset' = [bset EXCEPT ![n] = cpSet]
    /\ CASE cpKind = "move" ->
              /\ mode' = [mode EXCEPT ![n] = "draining"]
              /\ UNCHANGED <<log, ans, opened, gaveUp, reclaimAt>>
         [] cpKind = "cutover" ->
              /\ mode' = [mode EXCEPT ![n] = "open"]
              /\ log' = [log EXCEPT ![n] = Append(@, Start(cpGen))]
              /\ opened' = opened \cup {<<cpGen, n>>}
              /\ UNCHANGED <<ans, gaveUp, reclaimAt>>
         [] OTHER ->
              /\ mode' = [mode EXCEPT ![n] = "fencing"]
              /\ ans' = [ans EXCEPT ![n] = [has |-> {n}, logs |-> [NoAns EXCEPT ![n] = log[n]]]]
              /\ UNCHANGED <<log, opened, gaveUp, reclaimAt>>
    /\ UNCHANGED <<report, acked, writes, crashes>>
    /\ UNCHANGED cpVars

\* A member answers a fence at a newer generation than it accepted, with its
\* log, and stops leading. A given-up copy does not answer.
Answer(c, m) ==
    /\ mode[c] = "fencing" /\ m /= c /\ m \notin ans[c].has /\ m \in bset[c]
    /\ promised[m] < bgen[c] /\ gaveUp[m] = 0
    /\ promised' = [promised EXCEPT ![m] = bgen[c]]
    /\ ans' = [ans EXCEPT ![c] = [has |-> @.has \cup {m}, logs |-> [@.logs EXCEPT ![m] = log[m]]]]
    /\ mode' = [mode EXCEPT ![m] = "idle"]
    /\ bgen' = [bgen EXCEPT ![m] = 0]
    /\ UNCHANGED <<log, gaveUp, reclaimAt, bset, report, acked, opened, writes, crashes>>
    /\ UNCHANGED cpVars

Furthest(c) ==
    LET logs == {ans[c].logs[m] : m \in ans[c].has}
    IN CHOOSE s \in logs : \A t \in logs : NotBehind(s, t)

\* With a majority answered, the fencing broker catches up from the
\* furthest answer and opens with a start record.
Open(c) ==
    /\ mode[c] = "fencing" /\ MajorityIn(ans[c].has, bset[c])
    /\ log' = [log EXCEPT ![c] = Append(Furthest(c), Start(bgen[c]))]
    /\ mode' = [mode EXCEPT ![c] = "open"]
    /\ opened' = opened \cup {<<bgen[c], c>>}
    /\ ans' = [ans EXCEPT ![c] = [has |-> {}, logs |-> NoAns]]
    /\ gaveUp' = [gaveUp EXCEPT ![c] = 0]
    /\ reclaimAt' = [reclaimAt EXCEPT ![c] = 0]
    /\ UNCHANGED <<promised, bgen, bset, report, acked, writes, crashes>>
    /\ UNCHANGED cpVars

\* The given-up leader placement names, unable to take the generation,
\* probes every follower in the set it is given and reports the give-up at it with their positions
\* (`reclaim::probe`). Finding none ahead, it may lead a newer generation.
Probe(n) ==
    /\ cpLeader = n /\ gaveUp[n] > 0 /\ mode[n] = "idle"
    /\ report.gen /= cpGen
    /\ ~(promised[n] < cpGen /\ CanTake(n))
    /\ report' = [gen |-> cpGen, pos |-> log]
    /\ reclaimAt' = [reclaimAt EXCEPT ![n] =
                       IF \A m \in cpSet \ {n} : ~Ahead(log[m], log[n]) THEN cpGen ELSE @]
    /\ UNCHANGED <<log, promised, bgen, mode, ans, gaveUp, bset, acked, opened, writes, crashes>>
    /\ UNCHANGED cpVars

\* The move's leader reports drained once its successor is level with it.
Drained ==
    /\ cpDraining /\ ~drained /\ cpSucc /= NoNode
    /\ mode[cpLeader] = "draining" /\ bgen[cpLeader] = cpGen
    /\ promised[cpSucc] = cpGen /\ log[cpSucc] = log[cpLeader]
    /\ drained' = TRUE
    /\ UNCHANGED <<log, promised, bgen, mode, ans, gaveUp, reclaimAt, bset, cpGen, cpLeader, cpSet,
                   cpKind, cpDraining, cpSucc, report, moves, acked, opened, writes, crashes>>

(***************************************************************************)
(* Placement. Each write names a leader at the next generation.            *)
(***************************************************************************)
Name(n, kind) ==
    /\ cpGen < MaxGen
    /\ cpGen' = cpGen + 1
    /\ cpLeader' = n
    /\ cpKind' = kind
    /\ drained' = FALSE

Brokers == <<log, promised, bgen, mode, ans, gaveUp, reclaimAt, bset, report, acked, opened, writes,
             crashes>>

\* A move fences on a report from its leader serving at the generation.
CpFence(f) ==
    /\ moves < MaxMoves /\ ~cpDraining /\ f /= cpLeader /\ f \in cpSet
    /\ mode[cpLeader] = "open" /\ bgen[cpLeader] = cpGen
    /\ Name(cpLeader, "move")
    /\ cpDraining' = TRUE /\ cpSucc' = f /\ moves' = moves + 1
    /\ UNCHANGED <<cpSet>>
    /\ UNCHANGED Brokers

CpCutOver ==
    /\ cpDraining /\ drained
    /\ cpDraining' = FALSE /\ cpSucc' = NoNode
    /\ cpSucc /= NoNode /\ Name(cpSucc, "cutover")
    /\ UNCHANGED <<moves, cpSet>>
    /\ UNCHANGED Brokers

\* The leader serves again at a new generation and the move ends: a cancel,
\* an abandon, the fence timeout, or under `RetakeGivenUp` a give-up at the
\* move's generation. The destination stays in the set unless `drop`.
Retake(drop) ==
    /\ cpDraining /\ ~drained
    /\ Name(cpLeader, "fence")
    /\ cpDraining' = FALSE /\ cpSucc' = NoNode
    /\ cpSet' = IF drop /\ cpSucc /= NoNode /\ Cardinality(cpSet) > Replication
                THEN cpSet \ {cpSucc} ELSE cpSet
    /\ UNCHANGED <<moves>>
    /\ UNCHANGED Brokers

CpTimeout == Retake(DropsDestination)

CpRetakeGivenUp == RetakeGivenUp /\ report.gen = cpGen /\ Retake(FALSE)

\* A set above the factor fences its serving leader (`trim_fence`), and
\* drops a follower once every other one holds the leader's whole log at the
\* fence's generation (`trim`). The leader is named again; its timeout and a
\* give-up end the fence as for a move.
CpTrimFence ==
    /\ ~cpDraining /\ Cardinality(cpSet) > Replication
    /\ mode[cpLeader] = "open" /\ bgen[cpLeader] = cpGen
    /\ Name(cpLeader, "move")
    /\ cpDraining' = TRUE /\ cpSucc' = NoNode
    /\ UNCHANGED <<moves, cpSet>>
    /\ UNCHANGED Brokers

CpUnstage(f) ==
    /\ UnstageDrops /\ ~cpDraining /\ Cardinality(cpSet) > Replication
    /\ f \in cpSet \ {cpLeader}
    /\ Name(cpLeader, "fence")
    /\ cpSet' = cpSet \ {f}
    /\ UNCHANGED <<cpDraining, cpSucc, moves>>
    /\ UNCHANGED Brokers

CpTrim(t) ==
    /\ cpDraining /\ cpSucc = NoNode /\ t \in cpSet \ {cpLeader}
    /\ mode[cpLeader] = "draining" /\ bgen[cpLeader] = cpGen
    /\ \A m \in cpSet \ {cpLeader, t} : promised[m] = cpGen /\ log[m] = log[cpLeader]
    /\ Name(cpLeader, "fence")
    /\ cpDraining' = FALSE /\ cpSet' = cpSet \ {t}
    /\ UNCHANGED <<cpSucc, moves>>
    /\ UNCHANGED Brokers

\* With no move fenced, a give-up at the generation promotes the follower
\* the probe found furthest and not behind the leader, or names the leader
\* again when every follower is behind it (`reclaimable`).
CpFailover(f) ==
    /\ ~cpDraining /\ report.gen = cpGen /\ f /= cpLeader /\ f \in cpSet
    /\ NotBehind(report.pos[f], report.pos[cpLeader])
    /\ \A o \in cpSet \ {cpLeader} : NotBehind(report.pos[f], report.pos[o])
    /\ Name(f, "fence")
    /\ UNCHANGED <<cpSet, cpDraining, cpSucc, moves>>
    /\ UNCHANGED Brokers

CpReclaim ==
    /\ ~cpDraining /\ report.gen = cpGen
    /\ \A o \in cpSet \ {cpLeader} : Ahead(report.pos[cpLeader], report.pos[o])
    /\ Name(cpLeader, "fence")
    /\ UNCHANGED <<cpSet, cpDraining, cpSucc, moves>>
    /\ UNCHANGED Brokers

Next ==
    \/ \E l \in Nodes : Write(l) \/ Ack(l) \/ Crash(l) \/ Take(l) \/ Open(l) \/ Probe(l)
    \/ \E l, m \in Nodes : Ship(l, m) \/ Answer(l, m)
    \/ Drained
    \/ \E f \in Nodes : CpFence(f) \/ CpFailover(f) \/ CpTrim(f) \/ CpUnstage(f)
    \/ CpCutOver \/ CpTimeout \/ CpRetakeGivenUp \/ CpReclaim \/ CpTrimFence

Spec == Init /\ [][Next]_vars

\* Every step but the faults, the writes, a move's start and the fence
\* timeout, taken once it stays possible.
LiveSpec ==
    /\ Spec
    /\ \A l \in Nodes : WF_vars(Take(l)) /\ WF_vars(Open(l)) /\ WF_vars(Probe(l))
    /\ \A l, m \in Nodes : WF_vars(Ship(l, m)) /\ WF_vars(Answer(l, m))
    /\ WF_vars(Drained) /\ WF_vars(CpCutOver) /\ WF_vars(CpRetakeGivenUp) /\ WF_vars(CpReclaim)
    /\ WF_vars(\E f \in Nodes : CpFailover(f))
    /\ WF_vars(CpTrimFence) /\ WF_vars(\E t \in Nodes : CpTrim(t))

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)
OneLeaderPerGeneration ==
    \A x, y \in opened : x[1] = y[1] => x[2] = y[2]

\* A leader that opened holds every record acknowledged at its generation
\* or below.
AckedHeldByLeader ==
    \A n \in Nodes : (mode[n] = "open" /\ <<bgen[n], n>> \in opened) =>
        \A a \in acked : a.at <= bgen[n] => a.r \in Range(log[n])

\* Serving: open, with a majority still at its generation to take its
\* batches. Two never do at once.
Serving(n) == mode[n] = "open" /\ MajorityIn({n} \cup {m \in bset[n] : promised[m] = bgen[n]}, bset[n])
AtMostOneServing == \A a, c \in Nodes : Serving(a) /\ Serving(c) => a = c

\* Liveness: the leader placement names ends up serving, for good. At the
\* model's bound on generations placement can name nobody else, so a run
\* that reaches it is not held to this.
Serves == mode[cpLeader] = "open" /\ bgen[cpLeader] = cpGen
EventuallyServes == <>[](Serves \/ cpGen = MaxGen)
\* A set above the factor is trimmed back to it.
EventuallyAtFactor == <>[](Cardinality(cpSet) <= Replication \/ cpGen = MaxGen)
=============================================================================
