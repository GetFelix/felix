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
(* Abstracted. Storage is durable at once: FelixRaft.tla covers the        *)
(* adapter's fsync rules. A message is one atomic step between two         *)
(* brokers. The hybrid before the seal is the Init choice, and the         *)
(* hybrid's rebuild and catch-up during the seal are left out: a copy that *)
(* lags or was wiped stays so until Raft brings it up. Reseal is one step. *)
(* The digest and S travel in the `seal` variable, not in the record. The  *)
(* promotion adds one voter in one step; FelixRaftMembership covers joint  *)
(* changes. Records below S are outside the check, as the design says: the *)
(* switch proves nothing about them. Cache shards (a digest over live      *)
(* state) are not modelled.                                                *)
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
    StaleIdVotes

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
    used        \* budgets spent

raft == <<role, vote, granted, elected, match, commit>>
disk == <<copy, first, log, base>>
member == <<st, conf0, startBase>>
plane == <<cp, seal, reports>>
vars == <<raft, disk, member, plane, acked, decided, cutHeld, used>>

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Majority(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

Rec(v, g) == [term |-> g, kind |-> "record", val |-> v, conf |-> {}]
StartRec(g) == [term |-> g, kind |-> "start", val |-> 0, conf |-> {}]
Blank(t) == [term |-> t, kind |-> "blank", val |-> 0, conf |-> {}]
MemberRec(t, C) == [term |-> t, kind |-> "member", val |-> 0, conf |-> C]
\* What two copies of one record share: everything but the generation field.
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

\* The membership a member acts on: the latest in its log, committed or not.
Conf(s) ==
    LET at == {o \in (base[s].idx + 1)..LastIdx(s) : At(s, o).kind = "member"}
    IN IF at = {} THEN conf0[s] ELSE At(s, MaxOf(at)).conf

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
\* The digest of a log `lg` that starts at offset `f`, over from..to.
Digest(lg, f, from, to) ==
    [x \in 1..(to - from + 1) |->
        IF DigestCoversGeneration THEN lg[from + x - f] ELSE Content(lg[from + x - f])]

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
                    /\ d[s] > 0 => s \in MayDiffer /\ s # Leader
              /\ Leader \in Q
              /\ Majority(Q)
              /\ LET S == MaxOf({f[s] : s \in Q})
                     recs(s) == [x \in 1..(e[s] - f[s] + 1) |->
                                    LET o == f[s] + x - 1
                                    IN Rec(IF d[s] = o THEN 90 + o ELSE o,
                                           IF o <= z[s] THEN 0 ELSE G - 1)]
                     holders(k) == {s \in Servers : f[s] <= k /\ k <= e[s] /\ d[s] # k}
                 IN /\ first = f
                    /\ log = [s \in Servers |->
                                IF s = Leader THEN Append(recs(s), StartRec(G)) ELSE recs(s)]
                    /\ seal = [gen |-> G, n |-> PreLen + 1, s |-> S,
                               lfirst |-> f[Leader], lrecs |-> recs(Leader),
                               acks |-> {<<Leader, 1>>}, reported |-> FALSE]
                    /\ acked = {<<k, Content(Rec(k, 0)), 0>> :
                                    k \in {o \in S..PreLen : Majority(holders(o))}}
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
    /\ cp = [gen |-> G, ver |-> 1, engine |-> "hybrid", base |-> NoBase, voters |-> {}]
    /\ reports = {}
    /\ decided = {}
    /\ cutHeld = FALSE
    /\ used = [wipes |-> 0, reseals |-> 0, campaigns |-> 0, writes |-> 0, promotes |-> 0]

----------------------------------------------------------------------------
\* The seal.

DigestOk(s) ==
    LET to == seal.n - 1
    IN IF DigestFromOwnStart
       THEN LET from == Max(first[s], seal.lfirst)
            IN Digest(log[s], first[s], from, to) = Digest(seal.lrecs, seal.lfirst, from, to)
       ELSE /\ first[s] <= seal.s
            /\ Digest(log[s], first[s], seal.s, to) = Digest(seal.lrecs, seal.lfirst, seal.s, to)

\* The record at N is appended like any other, so only a log that reaches
\* N - 1 can take it.
FollowerAck(s) ==
    /\ s # Leader
    /\ st[s] = "hybrid"
    /\ cp.engine = "hybrid"
    /\ ~seal.reported
    /\ Me(s) \notin seal.acks
    /\ Holds(s, seal.n - 1)
    /\ LastIdx(s) = seal.n - 1
    /\ NoDigest \/ DigestOk(s)
    /\ log' = [log EXCEPT ![s] = Append(@, StartRec(seal.gen))]
    /\ seal' = [seal EXCEPT !.acks = @ \cup {Me(s)}]
    /\ UNCHANGED <<raft, copy, first, base, member, cp, reports, acked, decided, cutHeld, used>>

\* The leader counts copy ids. It does not know if one was wiped since.
Report ==
    /\ ~seal.reported
    /\ Me(Leader) \in seal.acks
    /\ st[Leader] = "hybrid"
    /\ Majority(seal.acks)
    /\ reports' = reports \cup
            {[gen |-> seal.gen, ver |-> cp.ver, n |-> seal.n, s |-> seal.s, voters |-> seal.acks]}
    /\ seal' = [seal EXCEPT !.reported = TRUE]
    /\ UNCHANGED <<raft, disk, member, cp, acked, decided, cutHeld, used>>

\* The switch. The write changes the set's version, so it happens once.
CpWrite(r) ==
    /\ r \in reports
    /\ CpBlindWrite \/ (cp.engine = "hybrid" /\ r.gen = cp.gen /\ r.ver = cp.ver)
    /\ WriteAfterWipes => used.wipes = MaxWipes
    /\ cp' = [cp EXCEPT !.ver = @ + 1,
                        !.engine = "raft",
                        !.base = [gen |-> r.gen, n |-> r.n, s |-> r.s],
                        !.voters = IF AllVoters THEN {Me(s) : s \in Servers} ELSE r.voters]
    /\ reports' = reports \ {r}
    /\ UNCHANGED <<raft, disk, member, seal, acked, decided, cutHeld, used>>

\* The hybrid opens again at the next generation, acknowledges one write on
\* a majority that holds the sealed log, and is sealed again one generation
\* later. An earlier report stays in flight.
Reseal ==
    /\ used.reseals < MaxReseals
    /\ cp.engine = "hybrid"
    /\ seal.gen = cp.gen
    /\ Me(Leader) \in seal.acks
    /\ st[Leader] = "hybrid"
    /\ \E Q \in SUBSET Servers :
        /\ Leader \in Q
        /\ Majority(Q)
        /\ \A s \in Q : Me(s) \in seal.acks /\ st[s] = "hybrid"
        /\ LET w == Rec(50, cp.gen + 1)
           IN /\ log' = [s \in Servers |->
                    IF s = Leader THEN log[s] \o <<w, StartRec(cp.gen + 2)>>
                    ELSE IF s \in Q THEN Append(log[s], w) ELSE log[s]]
              /\ acked' = acked \cup {<<seal.n + 1, Content(w), 0>>}
              /\ seal' = [gen |-> cp.gen + 2, n |-> seal.n + 2, s |-> seal.s,
                          lfirst |-> first[Leader], lrecs |-> Append(log[Leader], w),
                          acks |-> {Me(Leader)}, reported |-> FALSE]
    /\ cp' = [cp EXCEPT !.gen = @ + 2]
    /\ used' = [used EXCEPT !.reseals = @ + 1]
    /\ UNCHANGED <<raft, copy, first, base, member, reports, decided, cutHeld>>

\* A broker reads the record and starts its member. A recorded voter cuts
\* above N and purges up to the base. If its log does not reach N (possible
\* only with a rule dropped) it votes with the log it has. Every other copy
\* starts empty.
Start(s) ==
    /\ st[s] = "hybrid"
    /\ cp.engine = "raft"
    /\ LET voter == IsVoterIn(s, cp.voters)
           bn == cp.base.n
           keep == SubSeq(log[s], 1, Min(Len(log[s]), bn - first[s] + 1))
           end == first[s] + Len(keep) - 1
       IN IF TermsBelowBase
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
    /\ st' = [st EXCEPT ![s] = "raft"]
    /\ conf0' = [conf0 EXCEPT ![s] = cp.voters]
    /\ startBase' = [startBase EXCEPT ![s] = [cp.base EXCEPT !.s = 0]]
    /\ vote' = [vote EXCEPT ![s] = [term |-> cp.base.gen, node |-> Nobody, committed |-> FALSE]]
    /\ UNCHANGED <<role, granted, elected, match, copy, plane, acked, decided, cutHeld, used>>

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
    /\ UNCHANGED <<elected, plane, acked, decided, cutHeld>>

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
    /\ UNCHANGED <<elected, match, commit, disk, member, plane, acked, decided, cutHeld>>

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
    /\ UNCHANGED <<elected, match, commit, disk, member, plane, acked, decided, cutHeld, used>>

BecomeLeader(c) ==
    /\ st[c] = "raft"
    /\ role[c] = "candidate"
    /\ QuorumOf(granted[c], Conf(c))
    /\ vote' = [vote EXCEPT ![c].committed = TRUE]
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<vote[c].term, c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(vote[c].term))]
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<granted, commit, copy, first, base, member, plane, acked, decided, cutHeld, used>>

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
    /\ UNCHANGED <<elected, match, commit, disk, member, plane, acked, decided, cutHeld, used>>

LeaderAppend(i, e) ==
    /\ log' = [log EXCEPT ![i] = Append(@, e)]
    /\ UNCHANGED <<raft, copy, first, base, member, plane, acked, decided, cutHeld>>

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
    /\ UNCHANGED <<role, vote, granted, elected, match, copy, first, base, member, plane, acked, decided, used>>

\* A follower with no point of agreement takes the leader's snapshot: the
\* records up to the leader's purged id, copied, and that id as its own.
InstallSnapshot(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) = {}
    /\ first' = [first EXCEPT ![j] = first[i]]
    /\ log' = [log EXCEPT ![j] = SubSeq(log[i], 1, base[i].idx - first[i] + 1)]
    /\ base' = [base EXCEPT ![j] = base[i]]
    /\ commit' = [commit EXCEPT ![j] = base[i].idx]
    /\ UNCHANGED <<role, vote, granted, elected, match, copy, member, plane, acked, decided, cutHeld, used>>

Ack(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ MaxOf(Common(i, j)) > match[i][j]
    /\ match' = [match EXCEPT ![i][j] = MaxOf(Common(i, j))]
    /\ UNCHANGED <<role, vote, granted, elected, commit, disk, member, plane, acked, decided, cutHeld, used>>

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
    /\ UNCHANGED <<role, vote, granted, elected, match, disk, member, plane, cutHeld, used>>

LearnCommit(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET k == Min(MaxOf(Common(i, j)), commit[i])
       IN /\ k > commit[j]
          /\ commit' = [commit EXCEPT ![j] = k]
    /\ UNCHANGED <<role, vote, granted, elected, match, disk, member, plane, acked, decided, cutHeld, used>>

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
        \/ Start(s)
        \/ Wipe(s)
        \/ Campaign(s)
        \/ BecomeLeader(s)
        \/ AppendRecord(s)
        \/ AdvanceCommit(s)
    \/ Report
    \/ Reseal
    \/ \E r \in reports : CpWrite(r)
    \/ \E i, j \in Servers :
        \/ GrantVote(i, j)
        \/ AcceptLeader(i, j)
        \/ Replicate(i, j)
        \/ InstallSnapshot(i, j)
        \/ Ack(i, j)
        \/ LearnCommit(i, j)
        \/ Promote(i, j)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
TypeOK ==
    /\ \A s \in Servers :
        /\ st[s] \in {"hybrid", "raft"}
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ first[s] \in 1..2
        /\ st[s] = "raft" => base[s].idx >= first[s] - 1 /\ base[s].idx <= LastIdx(s)
        /\ commit[s] <= LastIdx(s)
        /\ st[s] = "hybrid" => role[s] = "follower"
    /\ cp.engine \in {"hybrid", "raft"}
    /\ used.wipes <= MaxWipes

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

=============================================================================
