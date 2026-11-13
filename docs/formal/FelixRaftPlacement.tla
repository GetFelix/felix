-------------------------- MODULE FelixRaftPlacement --------------------------
(***************************************************************************)
(* One Raft group whose voter set follows the control plane's placement,   *)
(* with disk loss, as docs/per-shard-raft.md decides it ("Membership",     *)
(* "Group and member identity") and as the metadata group does it today    *)
(* (docs/metadata-raft-design.md, "Rejoining after a lost volume").        *)
(*                                                                         *)
(* FelixRaftMembership.tla has one move with the write fence around it.    *)
(* FelixRaftIdentity.tla has the checks that decide when a copy lost what  *)
(* it promised. This module puts the two together and adds what neither    *)
(* has: the control plane as the proposer, changes of more than one seat,  *)
(* a smaller set, a removed copy's directory, and the offsets a subscriber *)
(* sees across all of it.                                                  *)
(*                                                                         *)
(* Placement. The control plane holds a target: the member ids the group   *)
(* should have. It moves a seat to another broker, removes a seat, and     *)
(* swaps a seat's id when the broker reports a new copy. The leader makes  *)
(* Raft follow the target it read, which may be an old one: it adds each   *)
(* missing member as a learner, then calls `change_membership` with the    *)
(* whole target. openraft writes a joint entry (old and new voters, both   *)
(* majorities needed) and, once that is committed, the entry that leaves   *)
(* it. One membership entry at a time. A new voter need not be level: the  *)
(* joint entry cannot commit without a majority of the new set holding the *)
(* log up to it.                                                           *)
(*                                                                         *)
(* Disk loss. A wipe empties a broker's directory: log, term and vote.     *)
(* Two rules bring such a broker back, and the model has both:             *)
(*                                                                         *)
(*   Rotate = TRUE   -- shard groups. The copy takes a new copy id. The    *)
(*                      old id is still in the membership and nothing      *)
(*                      answers for it. The control plane learns the new   *)
(*                      id and the leader swaps it in.                     *)
(*   Rotate = FALSE  -- the metadata group. The member keeps its id and    *)
(*                      withholds its vote: it grants none and does not    *)
(*                      campaign until it has applied up to an index a     *)
(*                      leader gave it after confirming its leadership     *)
(*                      with a quorum. Until then no leader counts what it *)
(*                      answers: it forgot the term it promised, so an old *)
(*                      leader could commit with it after a newer one was  *)
(*                      elected on its vote.                               *)
(*                                                                         *)
(* First start. With Forming the metadata group's first start is a step of *)
(* the model. A member with an empty directory withholds its vote and asks *)
(* every other member for its standing. It forms, which is to vote, only   *)
(* when every one of them answers and none holds a log entry. Silence is   *)
(* never read as empty, except from a slot the operator waived, and then   *)
(* only while the members that answer empty, this one included, are a      *)
(* majority. A waiver excuses silence only: a waived member that answers   *)
(* is counted like any other. Here a waived member that is up always       *)
(* answers, and Absent is the silent case. A member that lost its volume   *)
(* takes the same step, so wiped members cannot form a second group over   *)
(* one that holds the log, whether or not it answers. With FirstBoot every *)
(* member starts this way, so no leader exists until members have formed.  *)
(*                                                                         *)
(* An answer shows the log and not the vote. A volume lost after a vote and*)
(* before the first log entry meets only empty answers, so the answers     *)
(* alone let the member form again and vote a second time in the term it   *)
(* forgot (issue 2374). With FormRecord forming is three steps, each on    *)
(* disk before the next. Intend: the answers allow forming and none names  *)
(* this member, and it writes its own mark. Announce: another member       *)
(* records that this one set out to form. Form: it votes once every member *)
(* that answers holds the record. A wipe takes the mark and the records    *)
(* the directory held. A member that is named and holds no mark lost a     *)
(* volume that may have voted: it never forms, and votes again only by     *)
(* catching up. A group that starts formed starts with every mark and      *)
(* record written. EarlyWipes = FALSE keeps a wipe until the group has     *)
(* committed an entry, which is all the answers alone cover.               *)
(*                                                                         *)
(* Under fsync on commit a crash or a power cut loses nothing a peer       *)
(* counted, so an unclean restart is not a fault here. FelixRaft.tla has   *)
(* the adapter's fsync rules and FelixRaftIdentity.tla the restores.       *)
(*                                                                         *)
(* A removed copy. The control plane tells a broker to delete a copy that  *)
(* left the target. The rule: only once a committed membership that is not *)
(* joint has no vote for that id. The deletion is not counted as a fault.  *)
(* An id told to delete is never placed again. The rule has a gap (issue   *)
(* 2385): a leader on a replaced target can give the id its vote back      *)
(* between the control plane's check and the deletion                      *)
(* (FelixRaftPlacementStaleWipe.cfg).                                      *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   OneLeaderPerTerm   -- no term elects two members.                     *)
(*   LogMatching        -- two logs that agree on an entry's term agree on *)
(*                         everything up to it.                            *)
(*   LeaderCompleteness -- a leader holds every entry committed in an      *)
(*                         earlier term.                                   *)
(*   StateMachineSafety -- every broker's committed prefix is the one      *)
(*                         committed sequence.                             *)
(*   AckedSurvive       -- no member that could still win an election      *)
(*                         lacks an acknowledged record.                   *)
(*   AckedOnDisk        -- some directory holds every acknowledged record. *)
(*   OffsetNeverReused  -- no offset is delivered with two records, and no *)
(*                         record at two offsets.                          *)
(*   DeliveredAgree     -- what subscribers were given is what is          *)
(*                         committed at those offsets. A broker's          *)
(*                         subscribers get a record when the broker learns *)
(*                         it is committed.                                *)
(*   AckedHeldSurvive   -- AckedSurvive for the records some directory still*)
(*                         holds. Losing every copy is quorum loss, which no*)
(*                         first-start rule can see.                       *)
(*   CommitNeverLowers  -- no step lowers a broker's commit index, except  *)
(*                         one that empties its directory.                 *)
(*                                                                         *)
(* What a configuration's bounds leave unchecked: with no client write     *)
(* the Acked, Offset and Delivered checks have nothing to hold, and        *)
(* with one term LeaderCompleteness has no earlier term.                   *)
(* OffsetNeverReused needs two writes.                                     *)
(*                                                                         *)
(* An offset is the entry's index. Blank and membership entries hold an    *)
(* index that readers skip, so a jump is not checked here.                 *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   SkipJoint      -- `change_membership` writes the target in one entry. *)
(*   NoWithhold     -- Rotate = FALSE only: a wiped member votes at once.  *)
(*   WithheldCounts -- Rotate = FALSE only: a leader counts the appends    *)
(*                     and heartbeats a member answers while its vote is   *)
(*                     withheld. This is what the metadata group does      *)
(*                     today, and it loses a committed entry (issue        *)
(*                     2365). With the rule on, three members are safe and *)
(*                     five are not: the member can vote twice in the term *)
(*                     it forgot (FelixRaftPlacementSameIdFive.cfg).       *)
(*   AnswerAnyId    -- Rotate = TRUE only: a copy answers a request that   *)
(*                     names an older copy id of its broker.               *)
(*   DeleteOnTarget -- a copy is deleted as soon as the target drops it.   *)
(*   PlaceDoomed    -- the control plane may place a member id it told to  *)
(*                     delete.                                             *)
(*   FormMajority   -- Forming only: a member forms on a majority of empty *)
(*                     answers and skips a member that does not answer,    *)
(*                     which is what the metadata group did before issue   *)
(*                     2371.                                               *)
(*   FormAlias      -- Forming only: an answer is counted for the address  *)
(*                     it came from, not for the member that gave it. Two  *)
(*                     addresses can reach one member, so a member that    *)
(*                     does not answer is counted as empty on another      *)
(*                     member's answer, or on the asking member's own.     *)
(*   FormRecord = FALSE  -- Forming only: no mark and no records, a member *)
(*                     forms on the answers alone, which is what the       *)
(*                     metadata group did before issue 2374                *)
(*                     (FelixRaftPlacementFormEarlyWipeUnrecorded.cfg).    *)
(*   WaiverExcusesAnswer -- a waived member's answer is skipped too.       *)
(*   WaiverAnyNumber     -- a waiver may name a majority of the slots.     *)
(*                                                                         *)
(* FelixRaftMembership.tla has the twins for a change before the last one  *)
(* is committed and for a learner that counts.                             *)
(*                                                                         *)
(* Abstracted: storage, snapshots, the fence, the lease. A message is one  *)
(* atomic step between two brokers, so a record is written or not asked    *)
(* for. A leader counts the answers it has, whatever became of the copies  *)
(* that gave them.                                                         *)
(* The read-index round is one step that reads every broker's term at      *)
(* once, which is stricter than a round of answers collected over time.    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,
    Nobody,
    InitBrokers,
    MaxTerm,
    MaxWrites,
    MaxCalls,       \* membership entries a leader may propose (leaving a joint one is free)
    MaxTargets,     \* target changes the control plane may make
    MaxWipes,
    MinVoters,      \* the control plane never shrinks the target below this
    Rotate,
    Retain,         \* `change_membership(.., retain)`: removed voters stay as learners
    StaleTargets,   \* a leader may act on a target the control plane has replaced
    SkipJoint,
    NoWithhold,
    WithheldCounts,
    AnswerAnyId,
    DeleteOnTarget,
    PlaceDoomed,
    Forming,        \* a member whose vote is withheld may form from its peers' answers
    FormRecord,     \* forming is mark, record, vote
    FirstBoot,      \* the group has not formed: every member starts with its vote withheld
    FormMajority,
    FormAlias,
    WaiverExcusesAnswer,
    WaiverAnyNumber,
    Waived,         \* brokers a `form-without` waiver names: their silence does not block
    Absent,         \* brokers that never start
    EarlyWipes      \* a directory may be lost before the group has committed an entry

ASSUME FirstBoot => Forming
ASSUME FormRecord => Forming
ASSUME Absent # {} => FirstBoot
ASSUME Waived \subseteq InitBrokers /\ Absent \subseteq InitBrokers

VARIABLES
    role,       \* "follower", "candidate" or "leader"
    term,
    votedFor,
    granted,    \* the member ids that voted for a candidate
    elected,    \* history: every <<term, member id>> that won an election
    log,
    commitIdx,
    match,      \* a leader's view of each member id's matching prefix
    committed,  \* history: the committed sequence, each entry with the term that committed it
    copy,       \* the copy id in the broker's directory
    hold,       \* 0: votes. 1: vote withheld. n + 2: withheld until index n is applied
    mark,       \* the directory holds this member's own mark that it set out to form
    rec,        \* the members this directory records as having set out to form
    target,     \* control plane: the member ids the group should have
    known,      \* control plane: the copy id each broker last reported
    past,       \* history: targets the control plane replaced
    doomed,     \* control plane: member ids told to delete their directory
    delivered,  \* history: every <<offset, record>> given to a subscriber
    used        \* budgets spent

election == <<role, term, votedFor, granted, elected>>
raft == <<log, commitIdx, match, committed>>
ident == <<copy, hold, mark, rec>>
plane == <<target, known, past, doomed>>
vars == <<election, raft, ident, plane, delivered, used>>

\* The spare broker is told apart from the first voters, so only those
\* permute, and not the ones a configuration names.
Symm == Permutations(InitBrokers \ (Waived \cup Absent))

MaxCopy == MaxWipes + MaxTargets
Ids == Brokers \X (0..MaxCopy)

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

Me(s) == <<s, copy[s]>>
BrokersOf(S) == {m[1] : m \in S}

NoCfg == [old |-> {}, new |-> {}, learners |-> {}]
InitCfg == [old |-> InitBrokers \X {0}, new |-> InitBrokers \X {0}, learners |-> {}]
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
Majority(Q, V) == 2 * Cardinality(Q \cap V) > Cardinality(V)
Quorum(Q, c) == Majority(Q, c.old) /\ Majority(Q, c.new)

\* Broker s answers a request sent to member id m.
Is(s, m) == m[1] = s /\ (m[2] = copy[s] \/ (AnswerAnyId /\ m[2] <= copy[s]))

UpToDate(c, s) ==
    \/ LastTerm(c) > LastTerm(s)
    \/ LastTerm(c) = LastTerm(s) /\ LastIdx(c) >= LastIdx(s)

\* A subscriber of a broker is given each record as the broker learns it is
\* committed: the records of log l up to index n.
Given(l, n) ==
    delivered \cup {<<k, l[k].val>> : k \in {q \in 1..n : l[q].kind = "record"}}

Targets == IF StaleTargets THEN past \cup {target} ELSE {target}

\* Leaders in S stop leading.
Deposed(S) == role' = [s \in Brokers |-> IF s \in S THEN "follower" ELSE role[s]]

Init ==
    /\ role = [s \in Brokers |-> "follower"]
    /\ term = [s \in Brokers |-> 0]
    /\ votedFor = [s \in Brokers |-> Nobody]
    /\ granted = [s \in Brokers |-> {}]
    /\ elected = {}
    /\ log = [s \in Brokers |-> <<>>]
    /\ commitIdx = [s \in Brokers |-> 0]
    /\ match = [s \in Brokers |-> [m \in Ids |-> 0]]
    /\ committed = <<>>
    /\ copy = [s \in Brokers |-> 0]
    /\ hold = [s \in Brokers |-> IF FirstBoot THEN 1 ELSE 0]
    /\ mark = [s \in Brokers |-> FormRecord /\ ~FirstBoot /\ s \in InitBrokers]
    /\ rec = [s \in Brokers |->
                IF FormRecord /\ ~FirstBoot /\ s \in InitBrokers THEN InitBrokers \ {s} ELSE {}]
    /\ target = InitBrokers \X {0}
    /\ known = [s \in Brokers |-> 0]
    /\ past = {}
    /\ doomed = {}
    /\ delivered = {}
    /\ used = [writes |-> 0, calls |-> 0, targets |-> 0, wipes |-> 0, wiped |-> Nobody,
               deleted |-> FALSE, early |-> FALSE]

----------------------------------------------------------------------------
\* Raft.

\* Only a voter of the configuration it holds campaigns, and not while its
\* vote is withheld.
Campaign(c) ==
    /\ role[c] # "leader"
    /\ Me(c) \in Voters(Cfg(c))
    /\ hold[c] = 0
    /\ term[c] < MaxTerm
    /\ term' = [term EXCEPT ![c] = @ + 1]
    /\ votedFor' = [votedFor EXCEPT ![c] = c]
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {Me(c)}]
    /\ UNCHANGED <<elected, raft, ident, plane, delivered, used>>

\* The candidate asks the voters of its own configuration, by member id.
GrantVote(s, c) ==
    /\ s # c
    /\ role[c] = "candidate"
    /\ hold[s] = 0
    /\ term[s] < term[c]
    /\ UpToDate(c, s)
    /\ \E m \in Voters(Cfg(c)) :
        /\ Is(s, m)
        /\ granted' = [granted EXCEPT ![c] = @ \cup {m}, ![s] = {}]
    /\ term' = [term EXCEPT ![s] = term[c]]
    /\ votedFor' = [votedFor EXCEPT ![s] = c]
    /\ Deposed({s})
    /\ UNCHANGED <<elected, raft, ident, plane, delivered, used>>

BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c], Cfg(c))
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<term[c], Me(c)>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(term[c]))]
    /\ match' = [match EXCEPT ![c] = [m \in Ids |-> 0]]
    /\ UNCHANGED <<term, votedFor, granted, commitIdx, committed, ident, plane, delivered, used>>

\* A heartbeat: the receiver adopts the leader's term. A member whose vote
\* is withheld still follows.
Heartbeat(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] < term[i]
    /\ \E m \in Members(Cfg(i)) : Is(j, m)
    /\ term' = [term EXCEPT ![j] = term[i]]
    /\ votedFor' = [votedFor EXCEPT ![j] = i]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ Deposed({j})
    /\ UNCHANGED <<elected, raft, ident, plane, delivered, used>>

\* AppendEntries with one entry after the highest index the two logs agree
\* on, to a voter or a learner of the leader's configuration, by member id.
Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] <= term[i]
    /\ \E m \in Members(Cfg(i)) :
        /\ Is(j, m)
        /\ LET p == MaxOf({q \in 0..Min(LastIdx(i), LastIdx(j)) :
                            q = 0 \/ log[i][q].term = log[j][q].term})
           IN /\ p < LastIdx(i)
              /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p), log[i][p + 1])]
              /\ match' = [match EXCEPT ![i][m] =
                            IF hold[j] # 0 /\ ~WithheldCounts THEN 0 ELSE p + 1]
              /\ commitIdx' = [commitIdx EXCEPT ![j] = Max(Min(@, p), Min(commitIdx[i], p + 1))]
              /\ delivered' = Given(log'[j], commitIdx'[j])
    /\ term' = [term EXCEPT ![j] = term[i]]
    /\ votedFor' = [votedFor EXCEPT ![j] = IF term[j] < term[i] THEN i ELSE @]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ Deposed({j})
    /\ UNCHANGED <<elected, committed, ident, plane, used>>

\* The leader counts copies under the latest configuration in its log, and
\* only for an entry of its own term.
AdvanceCommit(i) ==
    /\ role[i] = "leader"
    /\ LET can == {n \in (commitIdx[i] + 1)..LastIdx(i) :
                    /\ log[i][n].term = term[i]
                    /\ Quorum({Me(i)} \cup {m \in Ids : match[i][m] >= n}, Cfg(i))}
       IN /\ can # {}
          /\ LET n == MaxOf(can)
                 have == Len(committed)
             IN /\ commitIdx' = [commitIdx EXCEPT ![i] = n]
                /\ committed' =
                    IF have >= n THEN committed
                    ELSE committed \o [k \in 1..(n - have) |->
                                        [e |-> log[i][have + k], by |-> term[i]]]
                /\ delivered' = Given(log[i], n)
    /\ UNCHANGED <<election, log, match, ident, plane, used>>

\* The follower learns the commit index for the prefix it shares with the
\* leader.
LearnCommit(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ \E m \in Members(Cfg(i)) :
        /\ Is(j, m)
        /\ LET n == Min(commitIdx[i], match[i][m])
           IN /\ commitIdx[j] < n
              /\ n <= LastIdx(j)
              /\ log[j][n].term = log[i][n].term
              /\ commitIdx' = [commitIdx EXCEPT ![j] = n]
              /\ delivered' = Given(log[j], n)
    /\ UNCHANGED <<election, log, match, committed, ident, plane, used>>

\* A member that votes again and is already level has nothing to be sent,
\* so the leader learns where it stands from a heartbeat's answer.
Probe(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ term[j] = term[i]
    /\ hold[j] = 0
    /\ LastIdx(j) > 0
    /\ LastIdx(j) <= LastIdx(i)
    /\ log[j][LastIdx(j)].term = log[i][LastIdx(j)].term
    /\ \E m \in Members(Cfg(i)) :
        /\ Is(j, m)
        /\ match[i][m] < LastIdx(j)
        /\ match' = [match EXCEPT ![i][m] = LastIdx(j)]
    /\ UNCHANGED <<election, log, commitIdx, committed, ident, plane, delivered, used>>

AppendRecord(i) ==
    /\ role[i] = "leader"
    /\ used.writes < MaxWrites
    /\ log' = [log EXCEPT ![i] = Append(@, Record(term[i], used.writes + 1))]
    /\ used' = [used EXCEPT !.writes = @ + 1]
    /\ UNCHANGED <<election, commitIdx, match, committed, ident, plane, delivered>>

----------------------------------------------------------------------------
\* The leader follows the target.

\* openraft refuses a membership entry while the last one is uncommitted.
Settled(i) ==
    /\ role[i] = "leader"
    /\ commitIdx[i] >= LastMember(i)
    /\ ~Joint(Cfg(i))

Propose(i, c) ==
    /\ used.calls < MaxCalls
    /\ log' = [log EXCEPT ![i] = Append(@, Member(term[i], c))]
    /\ used' = [used EXCEPT !.calls = @ + 1]
    /\ UNCHANGED <<election, commitIdx, match, committed, ident, plane, delivered>>

\* `add_learner` for a member the target names and the membership lacks.
AddLearner(i, m) ==
    /\ Settled(i)
    /\ \E T \in Targets : m \in T
    /\ m \notin Members(Cfg(i))
    /\ Propose(i, [Cfg(i) EXCEPT !.learners = @ \cup {m}])

\* `change_membership` with the whole target. Every new voter is already a
\* learner, which openraft requires.
Change(i, T) ==
    /\ Settled(i)
    /\ T \in Targets
    /\ T # Cfg(i).new
    /\ T \subseteq Members(Cfg(i))
    /\ LET c == Cfg(i)
       IN Propose(i, [old |-> IF SkipJoint THEN T ELSE c.new,
                      new |-> T,
                      learners |-> (c.learners \ T) \cup (IF Retain THEN c.new \ T ELSE {})])

\* openraft leaves the joint configuration once its entry is committed. A
\* new leader that finds one does the same.
LeaveJoint(i) ==
    /\ role[i] = "leader"
    /\ Joint(Cfg(i))
    /\ commitIdx[i] >= LastMember(i)
    /\ log' = [log EXCEPT ![i] = Append(@, Member(term[i], [Cfg(i) EXCEPT !.old = Cfg(i).new]))]
    /\ UNCHANGED <<election, commitIdx, match, committed, ident, plane, delivered, used>>

\* A leader outside the committed new set stops leading.
StepDown(s) ==
    /\ Settled(s)
    /\ Me(s) \notin Voters(Cfg(s))
    /\ Deposed({s})
    /\ UNCHANGED <<term, votedFor, granted, elected, raft, ident, plane, delivered, used>>

----------------------------------------------------------------------------
\* The control plane.

Retarget(T) ==
    /\ used.targets < MaxTargets
    /\ target' = T
    /\ past' = IF StaleTargets THEN past \cup {target} ELSE past
    /\ used' = [used EXCEPT !.targets = @ + 1]
    /\ UNCHANGED <<election, raft, ident, known, doomed, delivered>>

\* A move, a drain or a replaced node: seat x goes to broker y. An id the
\* control plane told to delete is never placed again: `doomed` only grows.
CpMove(x, y) ==
    /\ x \in target
    /\ y \notin BrokersOf(target)
    /\ PlaceDoomed \/ <<y, known[y]>> \notin doomed
    /\ Retarget((target \ {x}) \cup {<<y, known[y]>>})

\* A node removed with nowhere to put its seat.
CpRemove(x) ==
    /\ x \in target
    /\ Cardinality(target) > MinVoters
    /\ Retarget(target \ {x})

\* A broker reports the copy id it holds. A seat on it follows the id.
CpReport(s) ==
    /\ known[s] # copy[s]
    /\ known' = [known EXCEPT ![s] = copy[s]]
    /\ target' = IF s \in BrokersOf(target)
                 THEN {m \in target : m[1] # s} \cup {Me(s)}
                 ELSE target
    /\ past' = IF StaleTargets /\ target' # target THEN past \cup {target} ELSE past
    /\ UNCHANGED <<election, raft, ident, doomed, delivered, used>>

CommittedCfgs == {k \in 1..Len(committed) : committed[k].e.kind = "member"}
LastCommittedCfg ==
    IF CommittedCfgs = {} THEN InitCfg ELSE committed[MaxOf(CommittedCfgs)].e.cfg

\* The control plane tells a broker to delete a copy the target dropped.
CpDoom(s) ==
    /\ Me(s) \notin target
    /\ Me(s) \notin doomed
    /\ LastIdx(s) > 0
    /\ \/ DeleteOnTarget
       \/ ~Joint(LastCommittedCfg) /\ Me(s) \notin Voters(LastCommittedCfg)
    /\ doomed' = doomed \cup {Me(s)}
    /\ UNCHANGED <<election, raft, ident, target, known, past, delivered, used>>

----------------------------------------------------------------------------
\* Losing a directory.

Emptied(s) ==
    /\ log' = [log EXCEPT ![s] = <<>>]
    /\ commitIdx' = [commitIdx EXCEPT ![s] = 0]
    /\ term' = [term EXCEPT ![s] = 0]
    /\ votedFor' = [votedFor EXCEPT ![s] = Nobody]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ Deposed({s})
    /\ copy' = [copy EXCEPT ![s] = IF Rotate THEN @ + 1 ELSE @]
    /\ hold' = [hold EXCEPT ![s] = IF Rotate \/ NoWithhold THEN 0 ELSE 1]
    /\ mark' = [mark EXCEPT ![s] = FALSE]
    /\ rec' = [rec EXCEPT ![s] = {}]
    /\ UNCHANGED <<elected, match, committed, delivered>>

Wipe(s) ==
    /\ used.wipes < MaxWipes
    /\ EarlyWipes \/ committed # <<>>
    /\ Emptied(s)
    /\ used' = [used EXCEPT !.wipes = @ + 1, !.wiped = s,
                             !.early = @ \/ (Forming /\ committed = <<>>)]
    /\ UNCHANGED plane

Delete(s) ==
    /\ Me(s) \in doomed
    /\ LastIdx(s) > 0 \/ term[s] > 0
    /\ Emptied(s)
    /\ used' = [used EXCEPT !.deleted = TRUE]
    /\ UNCHANGED plane

\* The leader confirmed its leadership with a quorum in its term.
Confirmed(i) ==
    /\ role[i] = "leader"
    /\ Quorum({Me(j) : j \in {b \in Brokers :
                    term[b] = term[i] /\ (b = i \/ hold[b] = 0 \/ WithheldCounts)}}, Cfg(i))

\* `catch-up-target`: the leader's last log index, after a read-index round.
AskTarget(s, i) ==
    /\ hold[s] = 1
    /\ s # i
    /\ Confirmed(i)
    /\ hold' = [hold EXCEPT ![s] = LastIdx(i) + 2]
    /\ UNCHANGED <<election, raft, copy, mark, rec, plane, delivered, used>>

\* The member votes again once it has applied that far.
VoteAgain(s) ==
    /\ hold[s] >= 2
    /\ commitIdx[s] >= hold[s] - 2
    /\ hold' = [hold EXCEPT ![s] = 0]
    /\ UNCHANGED <<election, raft, copy, mark, rec, plane, delivered, used>>

\* First start. A is the set of members that answer, and it holds every
\* waived member that is up. Every other member answers, bar those a
\* waiver names. Each answer says which member gave it, so none stands in
\* for a member that is silent (FormAlias drops that).
Asked(s, A) ==
    /\ A \subseteq (InitBrokers \ Absent) \ {s}
    /\ (Waived \ Absent) \ {s} \subseteq A
    /\ FormMajority \/ FormAlias \/ (InitBrokers \ {s}) \ A \subseteq Waived

\* The answers allow forming: none shows a log entry, and the members that
\* answer empty, this one included, are a majority. The member decided its
\* own directory was empty before it asked and does not look again.
Standing(s, A) ==
    LET empty == {t \in A : LastIdx(t) = 0} IN
    /\ Asked(s, A)
    /\ \A t \in A \ empty : WaiverExcusesAnswer /\ t \in Waived
    /\ WaiverAnyNumber \/ FormAlias \/ Majority({s} \cup empty, InitBrokers)

\* The member sets out to form and writes its mark. An answer that names
\* it, when it holds no mark, means its directory once held the mark: it
\* lost a volume that may have voted.
Intend(s, A) ==
    /\ FormRecord
    /\ hold[s] = 1
    /\ ~mark[s]
    /\ Standing(s, A)
    /\ \A t \in A : s \notin rec[t]
    /\ mark' = [mark EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<election, raft, copy, hold, rec, plane, delivered, used>>

\* Another member records it, whatever that member's own state.
Announce(s, t) ==
    /\ FormRecord
    /\ s # t
    /\ t \in InitBrokers
    /\ hold[s] = 1
    /\ mark[s]
    /\ s \notin rec[t]
    /\ rec' = [rec EXCEPT ![t] = @ \cup {s}]
    /\ UNCHANGED <<election, raft, copy, hold, mark, plane, delivered, used>>

\* The member votes. With FormRecord: once every member that answers holds
\* the record. It does not read the answers again, which allows more than
\* the metadata group does: the mark shows this directory has not been lost,
\* and so has not voted, since it found the group empty. Without: on the
\* answers alone.
Form(s, A) ==
    /\ Forming
    /\ hold[s] = 1
    /\ IF FormRecord
         THEN mark[s] /\ Asked(s, A) /\ \A t \in A : s \in rec[t]
         ELSE Standing(s, A)
    /\ hold' = [hold EXCEPT ![s] = 0]
    /\ UNCHANGED <<election, raft, copy, mark, rec, plane, delivered, used>>

Up == Brokers \ Absent

Next ==
    \/ \E s \in Up : \E A \in SUBSET InitBrokers : Intend(s, A) \/ Form(s, A)
    \/ \E s, t \in Up : Announce(s, t)
    \/ \E s \in Up :
        \/ Campaign(s) \/ BecomeLeader(s) \/ AdvanceCommit(s) \/ AppendRecord(s)
        \/ LeaveJoint(s) \/ StepDown(s)
        \/ CpReport(s) \/ CpDoom(s) \/ Wipe(s) \/ Delete(s) \/ VoteAgain(s)
        \/ \E m \in Ids : AddLearner(s, m)
        \/ \E T \in Targets : Change(s, T)
    \/ \E i, j \in Up :
        \/ GrantVote(i, j) \/ Heartbeat(i, j) \/ Replicate(i, j) \/ LearnCommit(i, j) \/ Probe(i, j)
        \/ AskTarget(i, j)
    \/ \E x \in target :
        \/ CpRemove(x)
        \/ \E y \in Brokers : CpMove(x, y)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Brokers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ term[s] \in 0..MaxTerm
        /\ commitIdx[s] <= LastIdx(s)
        /\ copy[s] \in 0..MaxCopy
        /\ hold[s] \in Nat
        /\ mark[s] \in BOOLEAN
        /\ rec[s] \subseteq InitBrokers \ {s}
        /\ known[s] <= copy[s]
    /\ target \subseteq Ids
    /\ doomed \subseteq Ids
    /\ used.writes \in 0..MaxWrites
    /\ used.calls \in 0..MaxCalls
    /\ used.targets \in 0..MaxTargets
    /\ used.wipes \in 0..MaxWipes

OneLeaderPerTerm == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

LogMatching ==
    \A s, t \in Brokers : \A i \in 1..Min(LastIdx(s), LastIdx(t)) :
        log[s][i].term = log[t][i].term =>
            \A k \in 1..i : log[s][k] = log[t][k]

LeaderCompleteness ==
    \A l \in Brokers : role[l] = "leader" =>
        \A i \in 1..Len(committed) : committed[i].by < term[l] =>
            i <= LastIdx(l) /\ log[l][i] = committed[i].e

StateMachineSafety ==
    \A s \in Brokers : \A i \in 1..commitIdx[s] :
        i <= Len(committed) /\ log[s][i] = committed[i].e

Acked == {committed[i].e.val : i \in {k \in 1..Len(committed) : committed[k].e.kind = "record"}}
Holds(s, v) == \E i \in 1..LastIdx(s) : log[s][i].kind = "record" /\ log[s][i].val = v

\* With the logs as they stand, a later term could elect this broker: it is
\* a voter of the configuration it holds, and the copies that would vote
\* for it make a majority of each voter set.
Electable(s) ==
    /\ Me(s) \in Voters(Cfg(s))
    /\ hold[s] = 0
    /\ Quorum({m \in Voters(Cfg(s)) :
                \E t \in Brokers : Is(t, m) /\ (t = s \/ (hold[t] = 0 /\ UpToDate(s, t)))},
              Cfg(s))

AckedSurvive == \A s \in Brokers : Electable(s) => \A v \in Acked : Holds(s, v)

AckedHeldSurvive ==
    \A s \in Brokers : Electable(s) =>
        \A v \in Acked : (\E t \in Brokers : Holds(t, v)) => Holds(s, v)

AckedOnDisk == \A v \in Acked : \E s \in Brokers : Holds(s, v)

OffsetNeverReused ==
    \A a, b \in delivered : (a[1] = b[1] \/ a[2] = b[2]) => a = b

DeliveredAgree ==
    \A a \in delivered :
        /\ a[1] <= Len(committed)
        /\ committed[a[1]].e.kind = "record"
        /\ committed[a[1]].e.val = a[2]

\* Only an emptied directory lowers a commit index.
CommitNeverLowers ==
    [][\A s \in Brokers : commitIdx'[s] >= commitIdx[s] \/ log'[s] = <<>>]_vars

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens, and TLC must find it does.

IsMember(e) == e.kind = "member"

\* A write is acknowledged. With FirstBoot every vote starts withheld, so
\* this also says members formed.
NeverAcks == Acked = {}

\* No member of a first boot forms.
NobodyForms == \A s \in Brokers : hold[s] = 1

\* The committed memberships that are not joint.
FinalCfgs == {committed[k].e.cfg : k \in {n \in CommittedCfgs : ~Joint(committed[n].e.cfg)}}

\* A seat moved to the spare broker.
NeverMoves == \A c \in FinalCfgs : BrokersOf(c.new) \subseteq InitBrokers

\* The voter set got smaller.
NeverShrinks == \A c \in FinalCfgs : Cardinality(c.new) >= Cardinality(InitBrokers)

\* A rotated copy was swapped in for its old id.
NeverReseats == \A c \in FinalCfgs : \A m \in c.new : m[2] = 0

\* A member that kept its id through a wipe votes again.
NeverVotesAgain == ~(~Rotate /\ used.wiped # Nobody /\ hold[used.wiped] = 0)

\* A member that lost its volume before the group committed anything votes
\* again, and a write is acknowledged.
NeverEarlyWipedVotesAgain == ~(used.early /\ hold[used.wiped] = 0 /\ Acked # {})

\* A member that was wiped leads, under its old id or a new one.
NeverWipedLeads == ~(used.wiped # Nobody /\ role[used.wiped] = "leader")

\* A removed copy's directory was deleted.
NeverDeletes == ~used.deleted

=============================================================================
