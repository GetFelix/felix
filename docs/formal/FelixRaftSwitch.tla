--------------------------- MODULE FelixRaftSwitch ---------------------------
(***************************************************************************)
(* Moving one shard from the hybrid protocol to Raft, as                   *)
(* docs/per-shard-raft.md decides it ("Moving a shard from the hybrid to   *)
(* Raft"; Questions, 6).                                                   *)
(*                                                                         *)
(* The model starts at the seal. The hybrid leader has fenced at           *)
(* generation G and appended the generation-start record at offset N. The  *)
(* record names a start offset S, the highest log start among the leader   *)
(* and the copies that answered the fence, and a digest of the records     *)
(* from S up to N. What the copies hold at that moment is an arbitrary     *)
(* choice in Init, bounded only by what the hybrid's fence guarantees:     *)
(* the leader holds every acknowledged record below N, and a record        *)
(* counts as acknowledged only if a majority holds it. A copy can lag,     *)
(* start later than the leader, carry generation 0 where another carries   *)
(* the real generation, or hold a different record below N.                *)
(*                                                                         *)
(* From there:                                                             *)
(*                                                                         *)
(*   FollowerAck -- a follower whose log reaches N appends the record and  *)
(*                  acknowledges with its copy id, if its log starts at or *)
(*                  below S and its digest from S matches.                 *)
(*   Report      -- with a majority of copy ids, the leader reports the    *)
(*                  base and those ids, once per seal.                     *)
(*   CpWrite     -- the control plane's compare-and-swap on generation and *)
(*                  set version: engine, base and voters in one write.     *)
(*   Start       -- a broker whose copy id is recorded cuts above N and    *)
(*                  starts with its log purged up to the base. Any other   *)
(*                  copy starts empty as a learner.                        *)
(*   Reseal      -- the hybrid opens again, acknowledges one write and is  *)
(*                  sealed at a later generation. The first seal's report  *)
(*                  may still arrive.                                      *)
(*   Wipe        -- a copy loses its disk and comes back under a new id.   *)
(*                                                                         *)
(* Then Raft: elections at terms above the base's generation, a blank      *)
(* entry, writes, a snapshot install for a learner (the records up to the  *)
(* base, copied) and its promotion by a membership entry. A member uses    *)
(* the latest membership in its log.                                       *)
(*                                                                         *)
(* A record's content is its offset, kind, value and membership. The       *)
(* `term` field is the generation field below the base and the Raft term   *)
(* above it. Content leaves it out, as the digest does.                    *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   LeaderCompleteness -- a Raft leader holds, by content, every record   *)
(*                         the hybrid acknowledged from S up and every     *)
(*                         record Raft acknowledged before its term.       *)
(*   StateMachineSafety -- above the base, every member applies the one    *)
(*                         committed record at each offset.                *)
(*   BaseAgree          -- members that vote hold the same content from S  *)
(*                         to N.                                           *)
(*   OneBase            -- no two brokers start from different bases.      *)
(*   OneLeaderPerTerm   -- no term elects two brokers.                     *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   AllVoters              -- the control plane records the whole set as  *)
(*                             voters. One that does not hold the record   *)
(*                             at N votes with the log it has.             *)
(*   TermsBelowBase         -- no copy purges or starts empty: the hybrid  *)
(*                             log is the Raft log and the generation      *)
(*                             field is the term, so Raft's log matching   *)
(*                             is trusted below N.                         *)
(*   NoDigest               -- a follower acknowledges N on reaching it.   *)
(*   DigestCoversGeneration -- the digest includes the generation field.   *)
(*   DigestFromOwnStart     -- a follower's digest runs from its own first *)
(*                             record, checked against the leader over     *)
(*                             that range, so a copy that starts above S   *)
(*                             acknowledges.                               *)
(*   CpBlindWrite           -- the control plane writes any report it      *)
(*                             receives, without comparing.                *)
(*   StaleIdVotes           -- a broker is matched to the recorded voters  *)
(*                             by name, so a wiped copy still votes.       *)
(*                                                                         *)
(* The implementation design on issue #2242 adds these, each off unless a  *)
(* config turns it on, so the configs above keep their state spaces:       *)
(*                                                                         *)
(*   InFlight   -- the seal leader has one write inside its fence that     *)
(*                 has not taken an offset. ClaimWrite gives it the next   *)
(*                 offset. As a `Leader` write, AnswerWrite acknowledges   *)
(*                 it on the leader's disk alone while the leader's lease  *)
(*                 holds. As a `Quorum` write, QuorumAnswer acknowledges   *)
(*                 it once a majority holds it, and it joins `acked`.      *)
(*                 SealClaim appends the record at N only once the write   *)
(*                 is answered or failed (the quiesce).                    *)
(*   Catchup    -- a hybrid follower takes the seal leader's records up to *)
(*                 N - 1, and cuts a longer tail to it. Only from the copy *)
(*                 id that sealed: in the code catch-up batches carry the  *)
(*                 leader's copy id and the sealed promise stores it. The  *)
(*                 sealed leader serves any copy without the sealed        *)
(*                 promise, a wiped follower under its new id included.    *)
(*   Failover   -- the control plane gives up the seal once the leader's   *)
(*                 lease has lapsed and seals again with another copy, the *)
(*                 one furthest ahead in a majority of copies that were    *)
(*                 never wiped.                                            *)
(*   MayUnsynced -- the copies whose log above `syn` is not on disk yet.   *)
(*                 Flush writes it. PowerLoss loses it, on any copy before *)
(*                 it starts its member, once per behaviour. A seal leader *)
(*                 that loses power before it reports gives up the seal.   *)
(*                 Every copy's log is fsync on commit, which the switch   *)
(*                 requires of every copy, not only the seal leader (the   *)
(*                 Init precondition): a record the hybrid acknowledged is *)
(*                 on disk on a majority.                                  *)
(*   Purge      -- a Raft member moves its purged id up to its commit.     *)
(*   MayDifferTag -- a follower's record differs only in the producer tag. *)
(*   MayDifferSide -- a follower's dead-letter and group-cursor logs lack  *)
(*                 the state the leader's hold (`side`).                   *)
(*                                                                         *)
(* And these checks:                                                       *)
(*                                                                         *)
(*   HybridAcksInBase   -- once the switch is recorded, every record the   *)
(*                         hybrid acknowledged from S up is in the base,   *)
(*                         by content, below N.                            *)
(*   LeaderAcksInBase   -- the same for `Leader` acknowledgements, while   *)
(*                         the writer is the seal leader. One made before  *)
(*                         a failover may be lost, as on the hybrid today. *)
(*   NoAckAfterHandOver -- no `Leader` acknowledgement once the control    *)
(*                         plane has handed the shard to another copy or   *)
(*                         to Raft.                                        *)
(*   SideComplete       -- a Raft leader holds the leader's side logs.     *)
(*   NeverReadsBelowBase -- a recorded voter starts with its purged id at  *)
(*                         N.                                              *)
(*   SwitchEnds         -- under FairSpec, a switch that started ends with *)
(*                         the switch recorded and every copy's member     *)
(*                         started. Its config has MaxCampaigns = 0, so    *)
(*                         "ends" does not include electing a leader or a  *)
(*                         learner catching up.                            *)
(*                                                                         *)
(* With one knob each that removes a rule:                                 *)
(*                                                                         *)
(*   ClaimBeforeQuiesce -- the record at N is appended before the write in *)
(*                         flight is answered.                             *)
(*   LeaseAtEntryOnly   -- a `Leader` write is acknowledged whatever the   *)
(*                         lease, as the hybrid does today.                *)
(*   ResealSameGen      -- a seal leader that lost its tail appends the    *)
(*                         record at N again, at its shorter end.          *)
(*   CopyFromAny        -- a learner copies the records below the base     *)
(*                         from any member, up to that member's last       *)
(*                         record, committed or not.                       *)
(*   DigestSkipsTag     -- the digest leaves out the producer tag.         *)
(*   DigestSkipsSide    -- the digest leaves out the side logs.            *)
(*   NoVoteFloor        -- a member starts with no vote, so it campaigns   *)
(*                         at term 1, below the base's term.               *)
(*   ReopenOnTimeout    -- a sealed leader opens again on its own and      *)
(*                         acknowledges a write.                           *)
(*   BallotIgnoresSeal  -- sealed copies elect a hybrid leader among       *)
(*                         themselves, which acknowledges a write.         *)
(*   MarkFirst          -- the member mark is written before the purge, so *)
(*                         a crash between them restarts the voter on the  *)
(*                         hybrid log.                                     *)
(*   WipedVotesInFailover -- a wiped copy answers the failover's fence     *)
(*                         before it is re-admitted.                       *)
(*   SealPeriodic       -- the switch seals a `Periodic` log: a record is  *)
(*                         acknowledged once a majority has written it,    *)
(*                         before it is on disk.                           *)
(*   AckBeforeSync      -- a follower acknowledges N, and the leader       *)
(*                         reports, before N is on disk.                   *)
(*                                                                         *)
(* The model's term is the record's generation field. A Raft term is one   *)
(* less in the code (`raft_log`: generation = term + 1), so the base's     *)
(* term G here is Raft term G - 1 there, and the vote at G in Start is the *)
(* vote at Raft term G - 1 the design calls the floor.                     *)
(*                                                                         *)
(* A broker starts its member in two steps: Start writes the log, purged   *)
(* id and vote floor, and StartMark writes the member mark. StartCrash is  *)
(* a crash between them; the broker then starts again from the record.     *)
(*                                                                         *)
(* Abstracted. Storage is durable at once, except the unsynced tails under *)
(* MayUnsynced: FelixRaft.tla covers the adapter's fsync rules once a      *)
(* member has started. A message is one atomic step between two brokers.   *)
(* The hybrid before the seal is the Init choice. Reseal and Failover are  *)
(* one step each, and a copy that was wiped is never re-admitted. One      *)
(* write is in flight at the seal. The digest and S travel in the `seal`   *)
(* variable; in the code they are in the record at N, which a follower     *)
(* appends as the leader sent it. The record at N carries its generation   *)
(* in its content, so two seals' records never compare equal. `cp.rfirst`  *)
(* and `cp.recs` are history for the checks: the control plane records     *)
(* only the base and the voters. The promotion adds one voter in one step; *)
(* FelixRaftMembership covers joint changes. Records below S are outside   *)
(* the check, as the design says: the switch proves nothing about them.    *)
(* Cache shards (a digest over live state) are not modelled.               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    Leader,         \* the hybrid leader that seals
    G,              \* the seal's generation, at least 2
    PreLen,         \* records below N on the leader, at least 2
    \* What Init may choose. `May` sets leave it open, `Must` sets force it.
    MayLag,         \* a log that ends below N - 1
    MustLag,
    MayStartLate,   \* a log that starts at offset 2
    MustStartLate,
    MayZero,        \* generation 0 on a prefix of the records
    MustZero,       \* generation 0 on every record held
    MayDiffer,      \* a different record at one offset below N
    Wipeable,
    MaxWipes,
    WriteAfterWipes,    \* the control plane's write waits for the wipes
    MaxReseals,
    MaxCampaigns,
    MaxWrites,
    MaxPromotes,
    AllVoters,
    TermsBelowBase,
    NoDigest,
    DigestCoversGeneration,
    DigestFromOwnStart,
    CpBlindWrite,
    StaleIdVotes,
    \* Added by the implementation design (#2242).
    InFlight,
    Catchup,
    MaxFailovers,
    MayUnsynced,    \* copies whose log may have a tail not on disk
    MaxPurges,
    MayDifferTag,
    MayDifferSide,
    ClaimBeforeQuiesce,
    LeaseAtEntryOnly,
    ResealSameGen,
    CopyFromAny,
    DigestSkipsTag,
    DigestSkipsSide,
    NoVoteFloor,
    ReopenOnTimeout,
    BallotIgnoresSeal,
    MarkFirst,
    WipedVotesInFailover,
    SealPeriodic,
    AckBeforeSync

VARIABLES
    copy,       \* the copy id's counter; a wipe raises it
    first,      \* the first offset a copy holds
    log,        \* its records from `first` on, hybrid and Raft alike
    base,       \* [idx, term] of the last purged entry; nothing at or below it is compared by term
    st,         \* "hybrid" until the broker starts its Raft member
    conf0,      \* the voters a member was started with
    startBase,  \* history: the base a member started from
    role,
    vote,
    granted,
    elected,    \* history: every <<term, broker>> that won an election
    match,      \* a leader's view of each follower's matching prefix
    commit,     \* how far a member has applied
    cp,         \* the control plane's record of the shard
    seal,       \* the seal in progress
    reports,    \* reports sent and not yet written; delivery can be late
    acked,      \* <<offset, content, term>> acknowledged; hybrid ones have term 0
    decided,    \* history: <<offset, content>> committed by Raft
    cutHeld,    \* history: a follower cut a record equal in content to the one it then took
    used,       \* budgets spent
    fly,        \* the seal leader's write in flight: its state and offset
    lease,      \* whether the first seal leader's lease still holds
    syn,        \* the last offset on each copy's disk; Durable when it has no tail
    side,       \* 1 if a copy's dead-letter and group-cursor logs hold the leader's state
    lacked,     \* `Leader` acknowledgements, <<offset, content>>
    lateAck     \* history: a `Leader` acknowledgement after the hand-over

raft == <<role, vote, granted, elected, match, commit>>
disk == <<copy, first, log, base>>
member == <<st, conf0, startBase>>
plane == <<cp, seal, reports>>
extra == <<fly, lease, syn, side, lacked, lateAck>>
vars == <<raft, disk, member, plane, acked, decided, cutHeld, used, extra>>

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Majority(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

\* A copy outside MayUnsynced has everything on disk.
Durable == 99

Rec(v, g) == [term |-> g, kind |-> "record", val |-> v, tag |-> 0, conf |-> {}]
StartRec(g) == [term |-> g, kind |-> "start", val |-> g, tag |-> 0, conf |-> {}]
Blank(t) == [term |-> t, kind |-> "blank", val |-> 0, tag |-> 0, conf |-> {}]
MemberRec(t, C) == [term |-> t, kind |-> "member", val |-> 0, tag |-> 0, conf |-> C]
\* What two copies of one record share: everything but the generation field.
\* The record at N names its generation in its payload too (`val`).
Content(e) == [e EXCEPT !.term = 0]

NoVote == [term |-> 0, node |-> Nobody, committed |-> FALSE]
NoBase == [gen |-> 0, n |-> 0, s |-> 0]
Empty == [idx |-> 0, term |-> 0]

VoteGE(a, b) ==
    \/ a.term > b.term
    \/ /\ a.term = b.term
       /\ \/ a = b
          \/ a.committed /\ ~b.committed
          \/ ~a.committed /\ ~b.committed /\ b.node \in {a.node, Nobody}

Me(s) == <<s, copy[s]>>
LastIdx(s) == first[s] + Len(log[s]) - 1
Holds(s, o) == first[s] <= o /\ o <= LastIdx(s)
At(s, o) == log[s][o - first[s] + 1]
TermAt(s, i) == IF i = base[s].idx THEN base[s].term ELSE At(s, i).term

\* Where two logs agree by (index, term), from the higher purged id up.
Common(i, j) ==
    {p \in Max(base[i].idx, base[j].idx)..Min(LastIdx(i), LastIdx(j)) :
        TermAt(i, p) = TermAt(j, p)}

\* The membership in force at offset k: the latest at or below it.
ConfAt(s, k) ==
    LET at == {o \in (base[s].idx + 1)..k : At(s, o).kind = "member"}
    IN IF at = {} THEN conf0[s] ELSE At(s, MaxOf(at)).conf

\* The membership a member acts on: the latest in its log, committed or not.
Conf(s) == ConfAt(s, LastIdx(s))

IsVoterIn(s, C) == IF StaleIdVotes THEN \E m \in C : m[1] = s ELSE Me(s) \in C

\* The voters of C among the brokers in Q, each under the copy id C names.
QuorumOf(Q, C) ==
    2 * Cardinality({m \in C : m[1] \in Q /\ (StaleIdVotes \/ copy[m[1]] = m[2])}) > Cardinality(C)

Following(i, j) ==
    i # j /\ st[i] = "raft" /\ st[j] = "raft" /\ role[i] = "leader" /\ vote[j] = vote[i]

\* A leader is open once an entry of its own term is committed.
Open(i) ==
    /\ role[i] = "leader"
    /\ Holds(i, commit[i])
    /\ commit[i] > base[i].idx
    /\ At(i, commit[i]).term = vote[i].term

----------------------------------------------------------------------------
\* What the digest compares of one record.
Digested(e) ==
    LET c == IF DigestCoversGeneration THEN e ELSE Content(e)
    IN IF DigestSkipsTag THEN [c EXCEPT !.tag = 0] ELSE c

\* The digest of a log `lg` that starts at offset `f`, over from..to.
Digest(lg, f, from, to) ==
    [x \in 1..(to - from + 1) |-> Digested(lg[from + x - f])]

Init ==
    \E f \in [Servers -> 1..2] :
      /\ \A s \in Servers :
            /\ f[s] = 2 => s \in MayStartLate \cup MustStartLate
            /\ s \in MustStartLate => f[s] = 2
      /\ \E e \in [Servers -> 0..PreLen] :
          /\ e[Leader] = PreLen
          /\ \A s \in Servers :
                /\ e[s] >= f[s] - 1
                /\ e[s] < PreLen => s \in MayLag \cup MustLag
                /\ s \in MustLag => e[s] < PreLen
          /\ \E z \in [Servers -> 0..PreLen], d \in [Servers -> 0..PreLen], Q \in SUBSET Servers :
              /\ \A s \in Servers :
                    /\ z[s] = 0 \/ (f[s] <= z[s] /\ z[s] <= e[s])
                    /\ z[s] > 0 => s \in MayZero \cup MustZero
                    /\ s \in MustZero => z[s] = (IF e[s] >= f[s] THEN e[s] ELSE 0)
                    /\ d[s] = 0 \/ (f[s] <= d[s] /\ d[s] <= e[s])
                    /\ d[s] > 0 => s \in MayDiffer \cup MayDifferTag /\ s # Leader
              /\ Leader \in Q
              /\ Majority(Q)
              /\ LET S == MaxOf({f[s] : s \in Q})
                     rec(s, o) ==
                        LET g == IF o <= z[s] THEN 0 ELSE G - 1
                        IN IF d[s] # o THEN Rec(o, g)
                           ELSE IF s \in MayDifferTag THEN [Rec(o, g) EXCEPT !.tag = 1]
                           ELSE Rec(90 + o, g)
                     recs(s) == [x \in 1..(e[s] - f[s] + 1) |-> rec(s, f[s] + x - 1)]
                     holders(k) == {s \in Servers : f[s] <= k /\ k <= e[s] /\ d[s] # k}
                 IN /\ first = f
                    /\ log = [s \in Servers |->
                                IF s = Leader /\ ~InFlight THEN Append(recs(s), StartRec(G)) ELSE recs(s)]
                    /\ seal = [gen |-> G, n |-> IF InFlight THEN 0 ELSE PreLen + 1, s |-> S,
                               lfirst |-> f[Leader], lrecs |-> recs(Leader),
                               acks |-> {<<Leader, 1>>}, reported |-> FALSE,
                               l |-> Leader, claimed |-> ~InFlight, void |-> FALSE]
                    /\ acked = {<<k, Content(Rec(k, 0)), 0>> :
                                    k \in {o \in S..PreLen : Majority(holders(o))}}
                    \* Fsync on commit: a record the hybrid acknowledged is on disk
                    \* on a majority. SealPeriodic drops it.
                    /\ syn \in {y \in [Servers -> (0..(PreLen + 1)) \cup {Durable}] :
                            /\ \A s \in Servers :
                                IF s \in MayUnsynced
                                THEN f[s] - 1 <= y[s] /\ y[s] <= f[s] + Len(log[s]) - 1
                                ELSE y[s] = Durable
                            /\ SealPeriodic \/
                                \A o \in {k \in S..PreLen : Majority(holders(k))} :
                                    Majority({s \in holders(o) : y[s] >= o})}
    /\ copy = [s \in Servers |-> 1]
    /\ base = [s \in Servers |-> Empty]
    /\ st = [s \in Servers |-> "hybrid"]
    /\ conf0 = [s \in Servers |-> {}]
    /\ startBase = [s \in Servers |-> NoBase]
    /\ role = [s \in Servers |-> "follower"]
    /\ vote = [s \in Servers |-> NoVote]
    /\ granted = [s \in Servers |-> {}]
    /\ elected = {}
    /\ match = [s \in Servers |-> [t \in Servers |-> 0]]
    /\ commit = [s \in Servers |-> 0]
    /\ cp = [gen |-> G, ver |-> 1, engine |-> "hybrid", base |-> NoBase, voters |-> {},
             rfirst |-> 1, recs |-> <<>>]
    /\ reports = {}
    /\ decided = {}
    /\ cutHeld = FALSE
    /\ used = [wipes |-> 0, reseals |-> 0, campaigns |-> 0, writes |-> 0, promotes |-> 0,
               failovers |-> 0, losses |-> 0, purges |-> 0, opens |-> 0]
    /\ fly = [st |-> IF InFlight THEN "admitted" ELSE "none", o |-> 0]
    /\ lease = TRUE
    /\ side \in {x \in [Servers -> {0, 1}] :
                    x[Leader] = 1 /\ \A s \in Servers \ MayDifferSide : x[s] = 1}
    /\ lacked = {}
    /\ lateAck = FALSE

----------------------------------------------------------------------------
\* The seal.

DigestOk(s) ==
    LET to == seal.n - 1
    IN /\ DigestSkipsSide \/ side[s] = side[seal.l]
       /\ IF DigestFromOwnStart
          THEN LET from == Max(first[s], seal.lfirst)
               IN Digest(log[s], first[s], from, to) = Digest(seal.lrecs, seal.lfirst, from, to)
          ELSE /\ first[s] <= seal.s
               /\ Digest(log[s], first[s], seal.s, to) = Digest(seal.lrecs, seal.lfirst, seal.s, to)

\* The record at N is appended like any other, so only a log that reaches
\* N - 1 can take it. The rule: N and everything below it are on disk
\* before the acknowledgement.
FollowerAck(s) ==
    /\ s # seal.l
    /\ st[s] = "hybrid"
    /\ cp.engine = "hybrid"
    /\ seal.claimed
    /\ ~seal.void
    /\ ~seal.reported
    /\ Me(s) \notin seal.acks
    /\ Holds(s, seal.n - 1)
    /\ LastIdx(s) = seal.n - 1
    /\ NoDigest \/ DigestOk(s)
    /\ log' = [log EXCEPT ![s] = Append(@, StartRec(seal.gen))]
    /\ seal' = [seal EXCEPT !.acks = @ \cup {Me(s)}]
    /\ syn' = [syn EXCEPT ![s] = IF AckBeforeSync THEN @ ELSE Max(@, seal.n)]
    /\ UNCHANGED <<raft, copy, first, base, member, cp, reports, acked, decided, cutHeld, used,
                   fly, lease, side, lacked, lateAck>>

\* The leader counts copy ids. It does not know if one was wiped since.
\* The rule: its own N is on disk before it reports.
Report ==
    /\ seal.claimed
    /\ ~seal.void
    /\ ~seal.reported
    /\ Me(seal.l) \in seal.acks
    /\ st[seal.l] = "hybrid"
    /\ Majority(seal.acks)
    /\ reports' = reports \cup
            {[gen |-> seal.gen, ver |-> cp.ver, n |-> seal.n, s |-> seal.s, voters |-> seal.acks,
              rfirst |-> seal.lfirst, recs |-> seal.lrecs]}
    /\ seal' = [seal EXCEPT !.reported = TRUE]
    /\ syn' = [syn EXCEPT ![seal.l] = IF AckBeforeSync THEN @ ELSE Max(@, seal.n)]
    /\ UNCHANGED <<raft, disk, member, cp, acked, decided, cutHeld, used,
                   fly, lease, side, lacked, lateAck>>

\* The switch. The write changes the set's version, so it happens once.
CpWrite(r) ==
    /\ r \in reports
    /\ CpBlindWrite \/ (cp.engine = "hybrid" /\ r.gen = cp.gen /\ r.ver = cp.ver)
    /\ WriteAfterWipes => used.wipes = MaxWipes
    /\ cp' = [cp EXCEPT !.ver = @ + 1,
                        !.engine = "raft",
                        !.base = [gen |-> r.gen, n |-> r.n, s |-> r.s],
                        !.voters = IF AllVoters THEN {Me(s) : s \in Servers} ELSE r.voters,
                        !.rfirst = r.rfirst,
                        !.recs = r.recs]
    /\ reports' = reports \ {r}
    /\ UNCHANGED <<raft, disk, member, seal, acked, decided, cutHeld, used, extra>>

\* The hybrid opens again at the next generation, acknowledges one write on
\* a majority that holds the sealed log, and is sealed again one generation
\* later. An earlier report stays in flight.
Reseal ==
    /\ used.reseals < MaxReseals
    /\ cp.engine = "hybrid"
    /\ seal.gen = cp.gen
    /\ seal.claimed
    /\ ~seal.void
    /\ Me(seal.l) \in seal.acks
    /\ st[seal.l] = "hybrid"
    /\ \E Q \in SUBSET Servers :
        /\ seal.l \in Q
        /\ Majority(Q)
        /\ \A s \in Q : Me(s) \in seal.acks /\ st[s] = "hybrid"
        /\ LET w == Rec(50, cp.gen + 1)
               l == seal.l
           IN /\ log' = [s \in Servers |->
                    IF s = l THEN log[s] \o <<w, StartRec(cp.gen + 2)>>
                    ELSE IF s \in Q THEN Append(log[s], w) ELSE log[s]]
              /\ acked' = acked \cup {<<seal.n + 1, Content(w), 0>>}
              /\ syn' = [s \in Servers |->
                    IF s \in Q /\ ~SealPeriodic THEN Max(syn[s], seal.n + 1) ELSE syn[s]]
              /\ seal' = [gen |-> cp.gen + 2, n |-> seal.n + 2, s |-> seal.s,
                          lfirst |-> first[l], lrecs |-> Append(log[l], w),
                          acks |-> {Me(l)}, reported |-> FALSE,
                          l |-> l, claimed |-> TRUE, void |-> FALSE]
    /\ cp' = [cp EXCEPT !.gen = @ + 2]
    /\ used' = [used EXCEPT !.reseals = @ + 1]
    /\ UNCHANGED <<raft, copy, first, base, member, reports, decided, cutHeld,
                   fly, lease, side, lacked, lateAck>>

\* The write in flight takes the writer's next offset. The writer is the
\* first seal leader, whose fence admitted it before the seal.
ClaimWrite ==
    /\ fly.st = "admitted"
    /\ st[Leader] = "hybrid"
    /\ copy[Leader] = 1
    /\ log' = [log EXCEPT ![Leader] = Append(@, Rec(80, G - 1))]
    /\ fly' = [fly EXCEPT !.st = "claimed", !.o = LastIdx(Leader) + 1]
    /\ UNCHANGED <<raft, copy, first, base, member, plane, acked, decided, cutHeld, used,
                   lease, syn, side, lacked, lateAck>>

\* A `Leader` write is acknowledged on the writer's disk alone, while its
\* lease holds (the rule) or whatever the lease (LeaseAtEntryOnly). On disk
\* means fsynced, unless the log is Periodic.
AnswerWrite ==
    /\ fly.st = "claimed"
    /\ lease \/ LeaseAtEntryOnly
    /\ st[Leader] = "hybrid"
    /\ copy[Leader] = 1
    /\ Holds(Leader, fly.o)
    /\ At(Leader, fly.o).val = 80
    /\ fly' = [fly EXCEPT !.st = "answered"]
    /\ lacked' = lacked \cup {<<fly.o, Content(Rec(80, 0))>>}
    /\ lateAck' = (lateAck \/ seal.l # Leader \/ cp.engine = "raft")
    /\ syn' = [syn EXCEPT ![Leader] = IF SealPeriodic THEN @ ELSE Max(@, fly.o)]
    /\ UNCHANGED <<raft, disk, member, plane, acked, decided, cutHeld, used, lease, side>>

\* A `Quorum` write is acknowledged once a majority holds it, on disk unless
\* the log is Periodic. It needs no lease, and it can come after a failover.
QuorumAnswer ==
    /\ fly.st = "claimed"
    /\ st[Leader] = "hybrid"
    /\ copy[Leader] = 1
    /\ Majority({s \in Servers : /\ Holds(s, fly.o)
                                 /\ At(s, fly.o).val = 80
                                 /\ SealPeriodic \/ syn[s] >= fly.o})
    /\ fly' = [fly EXCEPT !.st = "answered"]
    /\ acked' = acked \cup {<<fly.o, Content(Rec(80, 0)), 0>>}
    /\ UNCHANGED <<raft, disk, member, plane, decided, cutHeld, used, lease, syn, side, lacked, lateAck>>

\* Without a lease the write fails, and its client is told so.
FailWrite ==
    /\ fly.st \in {"admitted", "claimed"}
    /\ ~lease
    /\ fly' = [fly EXCEPT !.st = "failed"]
    /\ UNCHANGED <<raft, disk, member, plane, acked, decided, cutHeld, used,
                   lease, syn, side, lacked, lateAck>>

LeaseLapse ==
    /\ lease
    /\ InFlight \/ MaxFailovers > 0
    /\ lease' = FALSE
    /\ UNCHANGED <<raft, disk, member, plane, acked, decided, cutHeld, used,
                   fly, syn, side, lacked, lateAck>>

\* The seal leader appends the record at N. The rule: only once the write in
\* flight is answered or failed (ShardFence::quiesce).
SealClaim ==
    /\ ~seal.claimed
    /\ ~seal.void
    /\ cp.engine = "hybrid"
    /\ st[seal.l] = "hybrid"
    /\ Me(seal.l) \in seal.acks
    /\ ClaimBeforeQuiesce \/ fly.st \notin {"admitted", "claimed"}
    /\ log' = [log EXCEPT ![seal.l] = Append(@, StartRec(seal.gen))]
    /\ seal' = [seal EXCEPT !.claimed = TRUE, !.n = LastIdx(seal.l) + 1,
                            !.lfirst = first[seal.l], !.lrecs = log[seal.l]]
    /\ UNCHANGED <<raft, copy, first, base, member, cp, reports, acked, decided, cutHeld, used, extra>>

\* A hybrid follower takes the seal leader's next record below N, or cuts a
\* longer tail back to N - 1. The leader must be the copy that sealed: a
\* wiped leader comes back with no generation and leads nothing.
FollowerCatchUp(s) ==
    /\ Catchup
    /\ s # seal.l
    /\ st[s] = "hybrid"
    /\ st[seal.l] = "hybrid"
    /\ Me(seal.l) \in seal.acks
    /\ cp.engine = "hybrid"
    /\ ~seal.void
    /\ Me(s) \notin seal.acks
    /\ LET l == seal.l
           top == IF seal.claimed THEN seal.n - 1 ELSE LastIdx(l)
           same(o) == Holds(l, o) /\ Content(At(s, o)) = Content(At(l, o))
       IN \/ /\ LastIdx(s) < top
             /\ Holds(l, LastIdx(s) + 1)
             /\ \A o \in first[s]..LastIdx(s) : same(o)
             /\ log' = [log EXCEPT ![s] = Append(@, At(l, LastIdx(s) + 1))]
          \/ /\ LastIdx(s) > top
             /\ top >= first[s] - 1
             /\ \A o \in first[s]..top : same(o)
             /\ log' = [log EXCEPT ![s] = SubSeq(@, 1, top - first[s] + 1)]
    /\ UNCHANGED <<raft, copy, first, base, member, plane, acked, decided, cutHeld, used, extra>>

\* A copy writes its tail to disk.
Flush(s) ==
    /\ s \in MayUnsynced
    /\ st[s] = "hybrid"
    /\ syn[s] < LastIdx(s)
    /\ syn' = [syn EXCEPT ![s] = LastIdx(s)]
    /\ UNCHANGED <<raft, disk, member, plane, acked, decided, cutHeld, used,
                   fly, lease, side, lacked, lateAck>>

\* A copy loses power and its unsynced tail, at any point before it starts
\* its member, the seal leader after its report included. The rule: a seal
\* leader that has not reported gives up the seal. ResealSameGen: it
\* appends the record at N again.
PowerLoss(s) ==
    /\ s \in MayUnsynced
    /\ used.losses = 0
    /\ st[s] = "hybrid"
    /\ syn[s] < LastIdx(s)
    /\ log' = [log EXCEPT ![s] = SubSeq(@, 1, syn[s] - first[s] + 1)]
    /\ seal' = IF s = seal.l /\ ~seal.reported /\ ~seal.void
               THEN IF ResealSameGen
                    THEN [seal EXCEPT !.claimed = FALSE, !.acks = {Me(s)}]
                    ELSE [seal EXCEPT !.void = TRUE]
               ELSE seal
    /\ used' = [used EXCEPT !.losses = 1]
    /\ UNCHANGED <<raft, copy, first, base, member, cp, reports, acked, decided, cutHeld, extra>>

\* Once the seal leader's lease has lapsed the control plane seals again with
\* b, which the fence makes the furthest ahead of a majority. The rule: a
\* wiped copy does not answer the fence until it is re-admitted, which the
\* model never does.
Failover(b) ==
    /\ used.failovers < MaxFailovers
    /\ cp.engine = "hybrid"
    /\ b # seal.l
    /\ st[b] = "hybrid"
    /\ ~lease
    /\ \E Q \in SUBSET Servers :
        /\ b \in Q
        /\ Majority(Q)
        /\ \A q \in Q :
            /\ st[q] = "hybrid"
            /\ WipedVotesInFailover \/ copy[q] = 1
            /\ LastIdx(q) <= LastIdx(b)
            /\ \A o \in first[q]..LastIdx(q) : Holds(b, o) /\ Content(At(q, o)) = Content(At(b, o))
        /\ log' = [log EXCEPT ![b] = Append(@, StartRec(cp.gen + 1))]
        /\ seal' = [gen |-> cp.gen + 1, n |-> LastIdx(b) + 1, s |-> MaxOf({first[q] : q \in Q}),
                    lfirst |-> first[b], lrecs |-> log[b], acks |-> {Me(b)}, reported |-> FALSE,
                    l |-> b, claimed |-> TRUE, void |-> FALSE]
    /\ cp' = [cp EXCEPT !.gen = @ + 1]
    /\ used' = [used EXCEPT !.failovers = @ + 1]
    /\ UNCHANGED <<raft, copy, first, base, member, reports, acked, decided, cutHeld, extra>>

\* A sealed shard opens without the control plane: the seal leader on a
\* timeout (ReopenOnTimeout), or a follower elected by sealed copies
\* (BallotIgnoresSeal). Either acknowledges one write on a majority.
OpenSealed(s) ==
    /\ used.opens = 0
    /\ seal.claimed
    /\ ~seal.void
    /\ \/ ReopenOnTimeout /\ s = seal.l
       \/ BallotIgnoresSeal /\ s # seal.l /\ Me(s) \in seal.acks
    /\ \E Q \in SUBSET Servers :
        /\ s \in Q
        /\ Majority(Q)
        /\ \A q \in Q : st[q] = "hybrid" /\ Me(q) \in seal.acks /\ LastIdx(q) = seal.n
        /\ LET w == Rec(60, seal.gen)
           IN /\ log' = [q \in Servers |-> IF q \in Q THEN Append(log[q], w) ELSE log[q]]
              /\ acked' = acked \cup {<<seal.n + 1, Content(w), 0>>}
              /\ syn' = [q \in Servers |->
                    IF q \in Q /\ ~SealPeriodic THEN Max(syn[q], seal.n + 1) ELSE syn[q]]
    /\ used' = [used EXCEPT !.opens = 1]
    /\ UNCHANGED <<raft, copy, first, base, member, plane, decided, cutHeld,
                   fly, lease, side, lacked, lateAck>>

\* A broker reads the record and starts its member. A recorded voter cuts
\* above N and purges up to the base. If its log does not reach N (possible
\* only with a rule dropped) it votes with the log it has. Every other copy
\* starts empty. The vote floor is written with the rest and is on disk
\* before StartMark writes the member mark. Under MarkFirst a voter may
\* crash between the mark and the purge and restart on the hybrid log as it
\* is.
Start(s) ==
    /\ st[s] = "hybrid"
    /\ cp.engine = "raft"
    /\ \E crash \in (IF MarkFirst THEN BOOLEAN ELSE {FALSE}) :
        LET voter == IsVoterIn(s, cp.voters)
            bn == cp.base.n
            keep == SubSeq(log[s], 1, Min(Len(log[s]), bn - first[s] + 1))
            end == first[s] + Len(keep) - 1
        IN /\ IF TermsBelowBase \/ (crash /\ voter)
              THEN /\ log' = [log EXCEPT ![s] = keep]
                   /\ first' = first
                   /\ base' = [base EXCEPT ![s] = [idx |-> first[s] - 1, term |-> 0]]
                   /\ commit' = [commit EXCEPT ![s] = IF voter /\ end = bn THEN bn ELSE first[s] - 1]
              ELSE IF voter
              THEN /\ log' = [log EXCEPT ![s] = keep]
                   /\ first' = first
                   /\ base' = [base EXCEPT ![s] =
                          IF end = bn THEN [idx |-> bn, term |-> cp.base.gen]
                          ELSE [idx |-> end, term |-> IF keep = <<>> THEN 0 ELSE keep[Len(keep)].term]]
                   /\ commit' = [commit EXCEPT ![s] = end]
              ELSE /\ log' = [log EXCEPT ![s] = <<>>]
                   /\ first' = [first EXCEPT ![s] = 1]
                   /\ base' = [base EXCEPT ![s] = Empty]
                   /\ commit' = [commit EXCEPT ![s] = 0]
           /\ st' = [st EXCEPT ![s] = IF crash /\ voter THEN "raft" ELSE "floor"]
    /\ conf0' = [conf0 EXCEPT ![s] = cp.voters]
    /\ startBase' = [startBase EXCEPT ![s] = [cp.base EXCEPT !.s = 0]]
    /\ vote' = [vote EXCEPT ![s] =
            IF NoVoteFloor THEN NoVote ELSE [term |-> cp.base.gen, node |-> Nobody, committed |-> FALSE]]
    /\ side' = [side EXCEPT ![s] = IF IsVoterIn(s, cp.voters) \/ TermsBelowBase THEN @ ELSE 0]
    /\ UNCHANGED <<role, granted, elected, match, copy, plane, acked, decided, cutHeld, used,
                   fly, lease, syn, lacked, lateAck>>

\* The member mark. The member opens.
StartMark(s) ==
    /\ st[s] = "floor"
    /\ st' = [st EXCEPT ![s] = "raft"]
    /\ UNCHANGED <<raft, disk, conf0, startBase, plane, acked, decided, cutHeld, used, extra>>

\* A crash after the vote floor and before the mark. With no mark the broker
\* starts again from the record, on what it wrote. It is the behaviour's
\* one power loss, so only a copy in MayUnsynced has it.
StartCrash(s) ==
    /\ s \in MayUnsynced
    /\ st[s] = "floor"
    /\ used.losses = 0
    /\ st' = [st EXCEPT ![s] = "hybrid"]
    /\ used' = [used EXCEPT !.losses = 1]
    /\ UNCHANGED <<raft, disk, conf0, startBase, plane, acked, decided, cutHeld, extra>>

\* The disk is lost. The broker comes back empty under a new copy id and
\* reads the control plane's record again.
Wipe(s) ==
    /\ s \in Wipeable
    /\ used.wipes < MaxWipes
    /\ copy' = [copy EXCEPT ![s] = @ + 1]
    /\ first' = [first EXCEPT ![s] = 1]
    /\ log' = [log EXCEPT ![s] = <<>>]
    /\ base' = [base EXCEPT ![s] = Empty]
    /\ st' = [st EXCEPT ![s] = "hybrid"]
    /\ conf0' = [conf0 EXCEPT ![s] = {}]
    /\ startBase' = [startBase EXCEPT ![s] = NoBase]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ vote' = [vote EXCEPT ![s] = NoVote]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ match' = [i \in Servers |-> IF i = s THEN [t \in Servers |-> 0] ELSE [match[i] EXCEPT ![s] = 0]]
    /\ commit' = [commit EXCEPT ![s] = 0]
    /\ used' = [used EXCEPT !.wipes = @ + 1]
    /\ side' = [side EXCEPT ![s] = 0]
    /\ syn' = [syn EXCEPT ![s] = IF s \in MayUnsynced THEN 0 ELSE @]
    /\ UNCHANGED <<elected, plane, acked, decided, cutHeld, fly, lease, lacked, lateAck>>

----------------------------------------------------------------------------
\* Raft.

\* The first campaign is at the base's generation plus one.
Campaign(c) ==
    /\ st[c] = "raft"
    /\ IsVoterIn(c, Conf(c))
    /\ role[c] # "leader"
    /\ used.campaigns < MaxCampaigns
    /\ vote' = [vote EXCEPT ![c] = [term |-> vote[c].term + 1, node |-> c, committed |-> FALSE]]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ used' = [used EXCEPT !.campaigns = @ + 1]
    /\ UNCHANGED <<elected, match, commit, disk, member, plane, acked, decided, cutHeld, extra>>

\* Any member answers. Only a voter's answer is counted.
GrantVote(s, c) ==
    /\ s # c
    /\ st[s] = "raft"
    /\ st[c] = "raft"
    /\ role[c] = "candidate"
    /\ s \notin granted[c]
    /\ VoteGE(vote[c], vote[s])
    /\ \/ TermAt(c, LastIdx(c)) > TermAt(s, LastIdx(s))
       \/ /\ TermAt(c, LastIdx(c)) = TermAt(s, LastIdx(s))
          /\ LastIdx(c) >= LastIdx(s)
    /\ vote' = [vote EXCEPT ![s] = vote[c]]
    /\ role' = [role EXCEPT ![s] = IF vote[s].term < vote[c].term THEN "follower" ELSE @]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {s},
                                  ![s] = IF vote[s].term < vote[c].term THEN {} ELSE @]
    /\ UNCHANGED <<elected, match, commit, disk, member, plane, acked, decided, cutHeld, used, extra>>

BecomeLeader(c) ==
    /\ st[c] = "raft"
    /\ role[c] = "candidate"
    /\ QuorumOf(granted[c], Conf(c))
    /\ vote' = [vote EXCEPT ![c].committed = TRUE]
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<vote[c].term, c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(vote[c].term))]
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<granted, commit, copy, first, base, member, plane, acked, decided, cutHeld, used, extra>>

AcceptLeader(i, j) ==
    /\ i # j
    /\ st[i] = "raft"
    /\ st[j] = "raft"
    /\ role[i] = "leader"
    /\ vote[j] # vote[i]
    /\ VoteGE(vote[i], vote[j])
    /\ vote' = [vote EXCEPT ![j] = vote[i]]
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ UNCHANGED <<elected, match, commit, disk, member, plane, acked, decided, cutHeld, used, extra>>

LeaderAppend(i, e) ==
    /\ log' = [log EXCEPT ![i] = Append(@, e)]
    /\ UNCHANGED <<raft, copy, first, base, member, plane, acked, decided, cutHeld, extra>>

AppendRecord(i) ==
    /\ Open(i)
    /\ used.writes < MaxWrites
    /\ LeaderAppend(i, Rec(100 + used.writes + 1, vote[i].term))
    /\ used' = [used EXCEPT !.writes = @ + 1]

\* One entry after the highest point the two logs agree on by term. The
\* follower cuts whatever else it holds there.
Replicate(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET p == MaxOf(Common(i, j))
       IN /\ p < LastIdx(i)
          /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p - first[j] + 1), At(i, p + 1))]
          /\ commit' = [commit EXCEPT ![j] = Min(@, p)]
          /\ cutHeld' = (cutHeld \/ (p < LastIdx(j) /\ Content(At(j, p + 1)) = Content(At(i, p + 1))))
    /\ UNCHANGED <<role, vote, granted, elected, match, copy, first, base, member, plane, acked, decided, used, extra>>

\* A follower with no point of agreement takes the leader's snapshot: the
\* records up to the leader's purged id, copied, and that id as its own.
InstallSnapshot(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) = {}
    /\ first' = [first EXCEPT ![j] = first[i]]
    /\ log' = [log EXCEPT ![j] = SubSeq(log[i], 1, base[i].idx - first[i] + 1)]
    /\ base' = [base EXCEPT ![j] = base[i]]
    /\ commit' = [commit EXCEPT ![j] = base[i].idx]
    /\ conf0' = [conf0 EXCEPT ![j] = conf0[i]]
    /\ side' = [side EXCEPT ![j] = side[i]]
    /\ UNCHANGED <<role, vote, granted, elected, match, copy, st, startBase, plane, acked, decided, cutHeld, used,
                   fly, lease, syn, lacked, lateAck>>

\* CopyFromAny: a learner copies another member's log up to its last
\* record, committed or not, and takes that as its purged id.
InstallFromAny(i, j) ==
    /\ CopyFromAny
    /\ i # j
    /\ st[i] = "raft"
    /\ st[j] = "raft"
    /\ ~IsVoterIn(j, Conf(j))
    /\ Common(i, j) = {}
    /\ LastIdx(i) > base[i].idx
    /\ first' = [first EXCEPT ![j] = first[i]]
    /\ log' = [log EXCEPT ![j] = log[i]]
    /\ base' = [base EXCEPT ![j] = [idx |-> LastIdx(i), term |-> At(i, LastIdx(i)).term]]
    /\ commit' = [commit EXCEPT ![j] = LastIdx(i)]
    /\ conf0' = [conf0 EXCEPT ![j] = Conf(i)]
    /\ side' = [side EXCEPT ![j] = side[i]]
    /\ UNCHANGED <<role, vote, granted, elected, match, copy, st, startBase, plane, acked, decided, cutHeld, used,
                   fly, lease, syn, lacked, lateAck>>

\* A member moves its purged id up to what it has committed.
Purge(i) ==
    /\ st[i] = "raft"
    /\ used.purges < MaxPurges
    /\ commit[i] > base[i].idx
    /\ base' = [base EXCEPT ![i] = [idx |-> commit[i], term |-> At(i, commit[i]).term]]
    /\ conf0' = [conf0 EXCEPT ![i] = ConfAt(i, commit[i])]
    /\ used' = [used EXCEPT !.purges = @ + 1]
    /\ UNCHANGED <<raft, copy, first, log, st, startBase, plane, acked, decided, cutHeld, extra>>

Ack(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ MaxOf(Common(i, j)) > match[i][j]
    /\ match' = [match EXCEPT ![i][j] = MaxOf(Common(i, j))]
    /\ UNCHANGED <<role, vote, granted, elected, commit, disk, member, plane, acked, decided, cutHeld, used, extra>>

Agree(i, k) == {j \in Servers : IF j = i THEN LastIdx(i) >= k ELSE match[i][j] >= k}

Committable(i) ==
    {k \in (commit[i] + 1)..LastIdx(i) :
        /\ QuorumOf(Agree(i, k), Conf(i))
        /\ At(i, k).term = vote[i].term}

AdvanceCommit(i) ==
    /\ st[i] = "raft"
    /\ role[i] = "leader"
    /\ Committable(i) # {}
    /\ LET k == MaxOf(Committable(i))
           new == (commit[i] + 1)..k
       IN /\ commit' = [commit EXCEPT ![i] = k]
          /\ decided' = decided \cup {<<o, Content(At(i, o))>> : o \in new}
          /\ acked' = acked \cup
                {<<o, Content(At(i, o)), vote[i].term>> : o \in {x \in new : At(i, x).kind = "record"}}
    /\ UNCHANGED <<role, vote, granted, elected, match, disk, member, plane, cutHeld, used, extra>>

LearnCommit(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET k == Min(MaxOf(Common(i, j)), commit[i])
       IN /\ k > commit[j]
          /\ commit' = [commit EXCEPT ![j] = k]
    /\ UNCHANGED <<role, vote, granted, elected, match, disk, member, plane, acked, decided, cutHeld, used, extra>>

\* A learner that is level becomes a voter by a membership entry. One
\* change at a time, and only from an open leader.
Promote(i, j) ==
    /\ Open(i)
    /\ Following(i, j)
    /\ used.promotes < MaxPromotes
    /\ ~IsVoterIn(j, Conf(i))
    /\ match[i][j] = LastIdx(i)
    /\ \A o \in (commit[i] + 1)..LastIdx(i) : At(i, o).kind # "member"
    /\ LeaderAppend(i, MemberRec(vote[i].term, Conf(i) \cup {Me(j)}))
    /\ used' = [used EXCEPT !.promotes = @ + 1]

Next ==
    \/ \E s \in Servers :
        \/ FollowerAck(s)
        \/ FollowerCatchUp(s)
        \/ Failover(s)
        \/ OpenSealed(s)
        \/ Start(s)
        \/ StartMark(s)
        \/ StartCrash(s)
        \/ Flush(s)
        \/ PowerLoss(s)
        \/ Wipe(s)
        \/ Campaign(s)
        \/ BecomeLeader(s)
        \/ AppendRecord(s)
        \/ AdvanceCommit(s)
        \/ Purge(s)
    \/ Report
    \/ Reseal
    \/ SealClaim
    \/ ClaimWrite
    \/ AnswerWrite
    \/ FailWrite
    \/ LeaseLapse
    \/ QuorumAnswer
    \/ \E r \in reports : CpWrite(r)
    \/ \E i, j \in Servers :
        \/ GrantVote(i, j)
        \/ AcceptLeader(i, j)
        \/ Replicate(i, j)
        \/ InstallSnapshot(i, j)
        \/ InstallFromAny(i, j)
        \/ Ack(i, j)
        \/ LearnCommit(i, j)
        \/ Promote(i, j)

Spec == Init /\ [][Next]_vars

\* Weak fairness on the switch's own steps, not on faults, failovers,
\* reseals or Raft. SwitchEnds holds under it only where no fault needs the
\* control plane: see the README for the states left stuck.
FairSpec ==
    /\ Spec
    /\ \A s \in Servers :
        /\ WF_vars(FollowerAck(s))
        /\ WF_vars(FollowerCatchUp(s))
        /\ WF_vars(Start(s))
        /\ WF_vars(StartMark(s))
    /\ WF_vars(SealClaim) /\ WF_vars(ClaimWrite) /\ WF_vars(AnswerWrite) /\ WF_vars(FailWrite)
    /\ WF_vars(Report) /\ WF_vars(\E r \in reports : CpWrite(r))

----------------------------------------------------------------------------
TypeOK ==
    /\ \A s \in Servers :
        /\ st[s] \in {"hybrid", "floor", "raft"}
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ first[s] \in 1..2
        /\ st[s] = "raft" => base[s].idx >= first[s] - 1 /\ base[s].idx <= LastIdx(s)
        /\ commit[s] <= LastIdx(s)
        /\ st[s] # "raft" => role[s] = "follower"
        /\ side[s] \in {0, 1}
    /\ cp.engine \in {"hybrid", "raft"}
    /\ used.wipes <= MaxWipes
    /\ fly.st \in {"none", "admitted", "claimed", "answered", "failed"}

OneLeaderPerTerm == \A x, y \in elected : x[1] = y[1] => x[2] = y[2]

\* No acknowledged record is lost: whoever leads holds it, by content.
LeaderCompleteness ==
    \A l \in Servers : role[l] = "leader" =>
        \A a \in acked : a[3] <= vote[l].term =>
            Holds(l, a[1]) /\ Content(At(l, a[1])) = a[2]

StateMachineSafety ==
    /\ \A x, y \in decided : x[1] = y[1] => x = y
    /\ \A s \in Servers : st[s] = "raft" =>
        \A o \in (cp.base.n + 1)..commit[s] :
            Holds(s, o) /\ <<o, Content(At(s, o))>> \in decided

\* Members that vote hold the same records from S to N, whatever
\* generation each copy wrote on them.
BaseAgree ==
    \A s, t \in Servers :
        (st[s] = "raft" /\ st[t] = "raft" /\ IsVoterIn(s, Conf(s)) /\ IsVoterIn(t, Conf(t))) =>
            \A o \in cp.base.s..cp.base.n :
                Holds(s, o) /\ Holds(t, o) /\ Content(At(s, o)) = Content(At(t, o))

OneBase ==
    \A s, t \in Servers :
        (startBase[s] # NoBase /\ startBase[t] # NoBase) => startBase[s] = startBase[t]

\* The recorded base holds content c at offset o, below N.
InBase(o, c) ==
    /\ o < cp.base.n
    /\ cp.rfirst <= o
    /\ o - cp.rfirst + 1 <= Len(cp.recs)
    /\ Content(cp.recs[o - cp.rfirst + 1]) = c

\* Once the switch is recorded, every record the hybrid acknowledged from S
\* up is in the base. A hybrid acknowledgement above N, or one the base
\* lacks, would be cut by every voter.
HybridAcksInBase ==
    cp.engine = "raft" =>
        \A a \in acked : (a[3] = 0 /\ a[1] >= cp.base.s) => InBase(a[1], a[2])

\* The same for `Leader` acknowledgements while the writer is the seal
\* leader. One made before a failover may be lost, as on the hybrid today.
LeaderAcksInBase ==
    (cp.engine = "raft" /\ used.failovers = 0) =>
        \A a \in lacked : a[1] >= cp.base.s => InBase(a[1], a[2])

\* No `Leader` acknowledgement once the shard was handed to another seal
\* leader or to Raft.
NoAckAfterHandOver == ~lateAck

\* A Raft leader holds the leader's dead-letter and group-cursor logs.
SideComplete == \A l \in Servers : role[l] = "leader" => side[l] = 1

\* A recorded voter's purged id is at N: nothing at or below it is an entry.
NeverReadsBelowBase ==
    \A s \in Servers :
        (st[s] = "raft" /\ startBase[s] # NoBase /\ IsVoterIn(s, conf0[s])) => base[s].idx >= cp.base.n

\* A switch that started ends: the control plane records it and every copy
\* has started its member, as a voter or a learner.
SwitchEnds == <>[](cp.engine = "raft" /\ \A s \in Servers : st[s] = "raft")

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens. Most must be violated, to
\* show a passing config is not passing for want of behaviour. Two are
\* expected to hold in the configs that say so: they show a case that is
\* unavailable and not wrong.

\* The control plane records the switch.
NeverSwitches == cp.engine = "hybrid"

\* A Raft leader is elected.
NeverElects == elected = {}

\* Raft acknowledges a record above the base.
NeverCommitsAboveBase == \A a \in acked : a[3] = 0

\* A learner's promotion commits.
NeverPromotes == \A x \in decided : x[2].kind # "member"

\* A promoted learner leads.
NeverPromotedLeads ==
    \A l \in Servers : role[l] = "leader" => Me(l) \in conf0[l]

\* A learner is brought up by the records below the base.
NeverInstalls ==
    [][\A s \in Servers : (st[s] = "raft" /\ copy'[s] = copy[s]) => base'[s] = base[s]]_vars

\* The switch completes from a second seal, with the first one's report sent.
NeverSwitchesAfterReseal == ~(cp.engine = "raft" /\ cp.base.gen > G)

\* A follower cuts a record and takes the same content back (TermsBelowBase).
NeverCutsHeldRecord == ~cutHeld

\* The switch completes after the seal leader acknowledged a write in flight.
NeverSwitchesAfterLeaderAck == ~(cp.engine = "raft" /\ lacked # {})

\* A broker restarts between its vote floor and its member mark.
NeverRestartsBeforeMark == \A s \in Servers : ~(st[s] = "hybrid" /\ startBase[s] # NoBase)

\* The switch completes from a seal another copy took over.
NeverSwitchesAfterFailover == ~(cp.engine = "raft" /\ used.failovers > 0)

=============================================================================
