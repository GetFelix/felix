------------------------------ MODULE FelixRaft ------------------------------
(***************************************************************************)
(* One Raft group on Felix's segment log, as docs/replication-engine.md    *)
(* decides it: Raft through openraft 0.10, with the log kept by the        *)
(* `raft_log` adapter over `DiskLog`. The adapter is not built yet (issue  *)
(* 2167); this model says what it has to guarantee.                        *)
(*                                                                         *)
(* The offset is the Raft index and the record's generation is its term.   *)
(* A leader's blank entry and a membership entry are records that readers  *)
(* skip, so they hold an index but no event. Three kinds of entry:         *)
(* "blank", "member" and "record".                                         *)
(*                                                                         *)
(* What a broker keeps, and what a crash does to it:                       *)
(*                                                                         *)
(*   log      -- what a reader returns. `append` returns once the entries  *)
(*               are here, before they are on disk.                        *)
(*   flushed  -- how far openraft believes the log is on disk: the flush   *)
(*               callback has fired, or the entry was there at startup.    *)
(*   synced   -- how far the log really is on disk. A restart keeps any    *)
(*               prefix from here up; a record cut short is a torn tail    *)
(*               and recovery drops it.                                    *)
(*   voteDisk -- the vote file (term, node, committed flag), replaced by   *)
(*               rename, so a crash leaves the old vote or the new one.    *)
(*   snap     -- the snapshot file, replaced by rename the same way.       *)
(*   base     -- the id of the last purged entry.                          *)
(*   inst     -- a snapshot a follower is installing. openraft does it as  *)
(*               three writes (cut a conflicting tail, replace the         *)
(*               snapshot, purge the log up to it) and a crash can fall    *)
(*               between any two.                                          *)
(*                                                                         *)
(* The state machine is rebuilt at a restart from the snapshot and the     *)
(* log up to the committed index read back, as openraft does for a state   *)
(* machine it is told is in memory. A shard's state machine is the log     *)
(* itself, which behaves the same way.                                     *)
(*                                                                         *)
(* A restarted broker is a follower even when its stored vote says it led. *)
(* openraft 0.10 lowers the flag in memory only; the model keeps the flag  *)
(* and drops the role, which the vote order cannot tell apart. A follower  *)
(* takes a leader's committed vote over its own uncommitted one of the     *)
(* same term.                                                              *)
(*                                                                         *)
(* A message is one atomic step between two brokers that reads both. The   *)
(* leader's knowledge of a follower (`match`) and the commit index it      *)
(* derives are kept, so a leader can act on an answer after the follower   *)
(* has moved on. One subscriber reads from brokers that believe they lead, *)
(* and each broker counts `skipped_before` from its own log.               *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   OneLeaderPerTerm        -- no term elects two brokers.                *)
(*   LogMatching             -- two logs that agree on an entry's term     *)
(*                              agree on everything up to it.              *)
(*   TermsMonotone           -- terms never go down along a log.           *)
(*   LeaderCompleteness      -- a leader holds every entry committed in    *)
(*                              an earlier term.                           *)
(*   StateMachineSafety      -- every broker applies the one committed     *)
(*                              sequence.                                  *)
(*   AppliedAgree            -- no two brokers differ on an applied entry. *)
(*   AppliedInLog            -- no cut reaches below what a broker         *)
(*                              applied, and the log holds what was        *)
(*                              applied except between a snapshot's        *)
(*                              install and its purge.                     *)
(*   AckedSurvive            -- an acknowledged record is on the disks of  *)
(*                              a majority.                                *)
(*   PurgedCoveredBySnapshot -- no log is purged past its stored snapshot. *)
(*   OffsetsDense            -- delivered offsets, less the skipped ones   *)
(*                              each serving broker reports from its own   *)
(*                              log, have no gap.                          *)
(*   OffsetNeverReused       -- a delivered offset holds that record in    *)
(*                              the committed sequence.                    *)
(*   DeliveredInOrder        -- the k-th delivery is the k-th committed    *)
(*                              record.                                    *)
(*   VoteNeverLowers         -- the stored vote only rises.                *)
(*   QuiescentServed         -- when nothing more can happen and a leader  *)
(*                              of the highest term stands, every record   *)
(*                              it holds is acknowledged, applied on every *)
(*                              broker and delivered.                      *)
(*                                                                         *)
(* Each knob removes one rule, and TLC finds the trace that rule prevents: *)
(*                                                                         *)
(*   AckBeforeFlush         -- the flush callback fires before the fsync.  *)
(*   VoteNotDurable         -- `save_vote` returns before the vote is on   *)
(*                             disk.                                       *)
(*   LeaderResumes          -- a restarted broker whose stored vote is     *)
(*                             committed for itself leads again in that    *)
(*                             term.                                       *)
(*   TornTailKept           -- recovery reads a torn record as an entry.   *)
(*   TruncateNotDurable     -- `truncate_after` returns before the cut is  *)
(*                             on disk. The next flush makes it durable,   *)
(*                             which is too late: the commit index can     *)
(*                             pass the cut first.                         *)
(*   RecoveredTailUnsynced  -- entries found at startup are reported as    *)
(*                             flushed without an fsync.                   *)
(*   SaveCommittedUnflushed -- the committed or applied index read back at *)
(*                             startup is past the recovered log, so the   *)
(*                             broker's state machine has a hole.          *)
(*   PurgeBeforeSnapshot    -- `purge` runs before the snapshot covering   *)
(*                             it is stored, so an acknowledged record     *)
(*                             leaves the disk.                            *)
(*   SnapshotTwoRenames     -- the snapshot's data and id are replaced by  *)
(*                             two renames, so a crash between them leaves *)
(*                             new data under the old id.                  *)
(*   CommitPriorTerm        -- a leader commits an earlier term's entry by *)
(*                             counting copies (Raft's Figure 8).          *)
(*   DeliverUncommitted     -- a subscriber is given entries above the     *)
(*                             commit index.                               *)
(*   SkipsNotCounted        -- a batch does not report the offsets that    *)
(*                             blank and membership entries hold.          *)
(*                                                                         *)
(* Not modelled yet (issue 2198 lists them): membership changes that alter *)
(* the voter set, the leader lease, disk loss and storage ids, the switch  *)
(* from the hybrid protocol, and `Leader`-level acknowledgements.          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    MaxTerm,
    MaxWrites,
    MaxMembers,
    MaxCrashes,
    MaxSnapshots,
    Consumer,
    AckBeforeFlush,
    VoteNotDurable,
    LeaderResumes,
    TornTailKept,
    TruncateNotDurable,
    RecoveredTailUnsynced,
    SaveCommittedUnflushed,
    PurgeBeforeSnapshot,
    SnapshotTwoRenames,
    CommitPriorTerm,
    DeliverUncommitted,
    SkipsNotCounted

VARIABLES
    role,       \* "follower", "candidate" or "leader"; lost in a crash
    vote,       \* the vote a broker acts on
    voteDisk,   \* the vote file
    granted,    \* the votes a candidate has
    elected,    \* history: every <<term, broker>> that won an election
    log,        \* entries after `base`, as a reader returns them
    base,       \* [idx, term] of the last purged entry
    flushed,    \* highest index openraft was told is on disk
    synced,     \* highest index that is on disk
    stale,      \* a tail cut off in memory but still on disk (TruncateNotDurable)
    match,      \* a leader's view of each follower's flushed, matching prefix
    applied,    \* the entries a broker's state machine has applied, from index 1
    snap,       \* the snapshot file: [idx, term, state]
    inst,       \* the snapshot being installed and its next write; lost in a crash
    committed,  \* history: the committed sequence, each entry with the term that committed it
    acked,      \* record values acknowledged to their publisher
    got,        \* what the subscriber received: [off, val, skipped, by]
    used        \* budgets spent: writes, members, crashes, snaps

election == <<role, vote, voteDisk, granted, elected>>
storage == <<log, base, flushed, synced, stale>>
machine == <<applied, snap, committed, acked>>
reader == <<got>>
vars == <<election, storage, machine, reader, match, used, inst>>

Symm == Permutations(Servers)

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

Blank(t) == [term |-> t, kind |-> "blank", val |-> 0]
NoVote == [term |-> 0, node |-> Nobody, committed |-> FALSE]
NoTail == [from |-> 0, tail |-> <<>>]
NoSnap == [idx |-> 0, term |-> 0, state |-> <<>>]
NoInstall == [step |-> "none", snap |-> NoSnap]
\* What a shard's state machine holds where its log lost an applied entry.
Lost == [term |-> 0, kind |-> "lost", val |-> 0]

\* openraft's order on votes with one leader per term (`leader_id_std`,
\* which docs/replication-engine.md pins). Two uncommitted votes
\* of one term for different brokers do not compare; a committed vote is above
\* every uncommitted vote of its term.
VoteGE(a, b) ==
    \/ a.term > b.term
    \/ /\ a.term = b.term
       /\ \/ a = b
          \/ a.committed /\ ~b.committed
          \/ ~a.committed /\ ~b.committed /\ b.node \in {a.node, Nobody}

LastIdx(s) == base[s].idx + Len(log[s])
Has(s, i) == base[s].idx < i /\ i <= LastIdx(s)
EntryAt(s, i) == log[s][i - base[s].idx]
\* Defined from the purged id up to the last entry.
TermAt(s, i) == IF i = base[s].idx THEN base[s].term ELSE EntryAt(s, i).term
\* The entries of `s` at indexes from + 1 .. to.
Entries(s, from, to) == [x \in 1..(to - from) |-> EntryAt(s, from + x)]

\* Where two logs agree by (index, term), which is all a follower checks.
Common(i, j) ==
    {p \in 0..Min(LastIdx(i), LastIdx(j)) :
        /\ p >= base[i].idx
        /\ p >= base[j].idx
        /\ TermAt(i, p) = TermAt(j, p)}

\* openraft moves its in-memory log state when it queues an install's three
\* writes, so the broker takes no other step until they land or it crashes.
Idle(s) == inst[s] = NoInstall

Following(i, j) == i # j /\ role[i] = "leader" /\ vote[j] = vote[i] /\ Idle(j)

Init ==
    /\ role = [s \in Servers |-> "follower"]
    /\ vote = [s \in Servers |-> NoVote]
    /\ voteDisk = [s \in Servers |-> NoVote]
    /\ granted = [s \in Servers |-> {}]
    /\ elected = {}
    /\ log = [s \in Servers |-> <<>>]
    /\ base = [s \in Servers |-> [idx |-> 0, term |-> 0]]
    /\ flushed = [s \in Servers |-> 0]
    /\ synced = [s \in Servers |-> 0]
    /\ stale = [s \in Servers |-> NoTail]
    /\ match = [s \in Servers |-> [t \in Servers |-> 0]]
    /\ applied = [s \in Servers |-> <<>>]
    /\ snap = [s \in Servers |-> NoSnap]
    /\ inst = [s \in Servers |-> NoInstall]
    /\ committed = <<>>
    /\ acked = {}
    /\ got = <<>>
    /\ used = [writes |-> 0, members |-> 0, crashes |-> 0, snaps |-> 0]

\* `save_vote`: on disk before it returns.
SaveVote(s, v) ==
    /\ vote' = [vote EXCEPT ![s] = v]
    /\ voteDisk' = IF VoteNotDurable THEN voteDisk ELSE [voteDisk EXCEPT ![s] = v]

Campaign(c) ==
    /\ role[c] # "leader"
    /\ Idle(c)
    /\ vote[c].term < MaxTerm
    /\ SaveVote(c, [term |-> vote[c].term + 1, node |-> c, committed |-> FALSE])
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<elected, storage, machine, reader, match, used, inst>>

\* A voter compares the candidate's last entry with its own, unflushed
\* entries included, and stores the vote before it answers.
GrantVote(s, c) ==
    /\ s # c
    /\ Idle(s)
    /\ role[c] = "candidate"
    /\ s \notin granted[c]
    /\ VoteGE(vote[c], vote[s])
    /\ \/ TermAt(c, LastIdx(c)) > TermAt(s, LastIdx(s))
       \/ /\ TermAt(c, LastIdx(c)) = TermAt(s, LastIdx(s))
          /\ LastIdx(c) >= LastIdx(s)
    /\ SaveVote(s, vote[c])
    /\ role' = [role EXCEPT ![s] = IF vote[s].term < vote[c].term THEN "follower" ELSE @]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {s},
                                  ![s] = IF vote[s].term < vote[c].term THEN {} ELSE @]
    /\ UNCHANGED <<elected, storage, machine, reader, match, used, inst>>

\* The winner stores its vote as committed, then appends its blank entry.
BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ Quorum(granted[c])
    /\ SaveVote(c, [vote[c] EXCEPT !.committed = TRUE])
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<vote[c].term, c>>}
    /\ log' = [log EXCEPT ![c] = Append(@, Blank(vote[c].term))]
    /\ flushed' = IF AckBeforeFlush THEN [flushed EXCEPT ![c] = LastIdx(c) + 1] ELSE flushed
    /\ match' = [match EXCEPT ![c] = [t \in Servers |-> 0]]
    /\ UNCHANGED <<granted, base, synced, stale, machine, reader, used, inst>>

\* A follower stores the leader's committed vote before it takes anything
\* from that leader.
AcceptLeader(i, j) ==
    /\ i # j
    /\ Idle(j)
    /\ role[i] = "leader"
    /\ vote[j] # vote[i]
    /\ VoteGE(vote[i], vote[j])
    /\ SaveVote(j, vote[i])
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ UNCHANGED <<elected, storage, machine, reader, match, used, inst>>

LeaderAppend(i, e) ==
    /\ role[i] = "leader"
    /\ log' = [log EXCEPT ![i] = Append(@, e)]
    /\ flushed' = IF AckBeforeFlush THEN [flushed EXCEPT ![i] = LastIdx(i) + 1] ELSE flushed
    /\ UNCHANGED <<election, base, synced, stale, machine, reader, match, inst>>

AppendRecord(i) ==
    /\ used.writes < MaxWrites
    /\ LeaderAppend(i, [term |-> vote[i].term, kind |-> "record", val |-> used.writes + 1])
    /\ used' = [used EXCEPT !.writes = @ + 1]

\* A membership entry holds an index like any other. The voter set stays as
\* it is: this model asks only what the entry does to offsets.
AppendMember(i) ==
    /\ used.members < MaxMembers
    /\ LeaderAppend(i, [term |-> vote[i].term, kind |-> "member", val |-> 0])
    /\ used' = [used EXCEPT !.members = @ + 1]

\* One fsync covers everything appended so far, and the callback fires.
\* It also makes a cut durable that TruncateNotDurable left in memory.
Flush(s) ==
    /\ Idle(s)
    /\ synced[s] < LastIdx(s)
    /\ synced' = [synced EXCEPT ![s] = LastIdx(s)]
    /\ flushed' = [flushed EXCEPT ![s] = LastIdx(s)]
    /\ stale' = [stale EXCEPT ![s] = NoTail]
    /\ UNCHANGED <<election, log, base, machine, reader, match, used, inst>>

\* AppendEntries with one entry, after the highest index the two logs agree
\* on. A follower holding something else there cuts it off first, and the
\* cut is on disk before `truncate_after` returns.
Replicate(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET p == MaxOf(Common(i, j))
           cut == p < LastIdx(j)
       IN /\ p < LastIdx(i)
          /\ log' = [log EXCEPT ![j] = Append(SubSeq(@, 1, p - base[j].idx), EntryAt(i, p + 1))]
          /\ flushed' = [flushed EXCEPT ![j] = IF AckBeforeFlush THEN p + 1 ELSE Min(@, p)]
          /\ synced' = [synced EXCEPT ![j] = Min(@, p)]
          /\ stale' = [stale EXCEPT ![j] =
                IF cut /\ TruncateNotDurable
                THEN [from |-> p, tail |-> SubSeq(log[j], p - base[j].idx + 1, Len(log[j]))]
                ELSE @]
    /\ UNCHANGED <<election, base, machine, reader, match, used, inst>>

\* The follower's answer: how far its log matches the leader's and is flushed.
Ack(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET p == Min(MaxOf(Common(i, j)), flushed[j])
       IN /\ p > match[i][j]
          /\ match' = [match EXCEPT ![i][j] = p]
    /\ UNCHANGED <<election, storage, machine, reader, used, inst>>

Agree(i, k) == {j \in Servers : IF j = i THEN flushed[i] >= k ELSE match[i][j] >= k}

\* An entry of the leader's own term on a majority's disks commits it and
\* everything before it. The leader's own copy counts once it is flushed.
Committable(i) ==
    {k \in (Len(applied[i]) + 1)..LastIdx(i) :
        /\ Quorum(Agree(i, k))
        /\ CommitPriorTerm \/ TermAt(i, k) = vote[i].term}

AdvanceCommit(i) ==
    /\ role[i] = "leader"
    /\ Committable(i) # {}
    /\ LET k == MaxOf(Committable(i))
           new == Entries(i, Len(applied[i]), k)
       IN /\ applied' = [applied EXCEPT ![i] = @ \o new]
          /\ committed' = IF k <= Len(committed) THEN committed
                ELSE committed \o [x \in 1..(k - Len(committed)) |->
                        [e |-> EntryAt(i, Len(committed) + x), by |-> vote[i].term]]
          /\ acked' = acked \cup {new[x].val : x \in {y \in DOMAIN new : new[y].kind = "record"}}
    /\ UNCHANGED <<election, storage, snap, reader, match, used, inst>>

\* A follower applies up to the leader's commit index, as far as its log
\* matches the leader's. Its own copy need not be flushed.
LearnCommit(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET k == Min(MaxOf(Common(i, j)), Len(applied[i]))
       IN /\ k > Len(applied[j])
          /\ applied' = [applied EXCEPT ![j] = @ \o Entries(j, Len(@), k)]
    /\ UNCHANGED <<election, storage, snap, committed, acked, reader, match, used, inst>>

\* The snapshot, data and id together, is replaced by one rename: a crash
\* leaves the old one or the new one, so the write is one step.
BuildSnapshot(s) ==
    /\ Idle(s)
    /\ used.snaps < MaxSnapshots
    /\ Len(applied[s]) > snap[s].idx
    /\ \E half \in BOOLEAN :
        /\ half => SnapshotTwoRenames
        /\ snap' = [snap EXCEPT ![s] =
                IF half THEN [@ EXCEPT !.state = applied[s]]
                ELSE [idx |-> Len(applied[s]),
                      term |-> applied[s][Len(applied[s])].term,
                      state |-> applied[s]]]
    /\ used' = [used EXCEPT !.snaps = @ + 1]
    /\ UNCHANGED <<election, storage, applied, committed, acked, reader, match, inst>>

\* Only up to the stored snapshot.
Purge(s) ==
    /\ Idle(s)
    /\ MaxSnapshots > 0
    /\ LET k == IF PurgeBeforeSnapshot THEN Len(applied[s]) ELSE snap[s].idx
       IN /\ k > base[s].idx
          /\ k <= LastIdx(s)
          /\ base' = [base EXCEPT ![s] = [idx |-> k, term |-> TermAt(s, k)]]
          /\ log' = [log EXCEPT ![s] = SubSeq(@, k - base[s].idx + 1, Len(@))]
    /\ UNCHANGED <<election, flushed, synced, stale, machine, reader, match, used, inst>>

\* A follower with no point of agreement at or above the leader's purged id
\* takes the leader's snapshot. openraft does it as three writes, and this is
\* the first: a log that holds another entry at the snapshot's index is cut
\* back to the commit index.
InstallCut(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) = {}
    /\ snap[i].idx >= base[i].idx
    /\ snap[i].idx = Len(snap[i].state)
    /\ snap[i].idx > Len(applied[j])
    /\ LET at == snap[i].idx
           keep == Len(applied[j])
           cut == /\ base[j].idx <= at
                  /\ at <= LastIdx(j)
                  /\ TermAt(j, at) # snap[i].term
       IN /\ log' = [log EXCEPT ![j] = IF cut THEN SubSeq(@, 1, keep - base[j].idx) ELSE @]
          /\ flushed' = [flushed EXCEPT ![j] = IF cut THEN Min(@, keep) ELSE @]
          /\ synced' = [synced EXCEPT ![j] = IF cut THEN Min(@, keep) ELSE @]
          /\ stale' = [stale EXCEPT ![j] =
                IF cut /\ TruncateNotDurable
                THEN [from |-> keep, tail |-> SubSeq(log[j], keep - base[j].idx + 1, Len(log[j]))]
                ELSE @]
    /\ inst' = [inst EXCEPT ![j] = [step |-> "install", snap |-> snap[i]]]
    /\ UNCHANGED <<election, base, machine, reader, match, used>>

\* The second write replaces the snapshot file and the state machine. Until
\* the third, the log ends below what the broker applied.
InstallState(j) ==
    /\ inst[j].step = "install"
    /\ snap' = [snap EXCEPT ![j] = inst[j].snap]
    /\ applied' = [applied EXCEPT ![j] = inst[j].snap.state]
    /\ inst' = [inst EXCEPT ![j].step = "purge"]
    /\ UNCHANGED <<election, storage, committed, acked, reader, match, used>>

\* The third purges up to the snapshot's id. The log ends below it (TypeOK),
\* so the purge is past the log's end and the log restarts there.
InstallPurge(j) ==
    /\ inst[j].step = "purge"
    /\ base' = [base EXCEPT ![j] = [idx |-> inst[j].snap.idx, term |-> inst[j].snap.term]]
    /\ log' = [log EXCEPT ![j] = <<>>]
    /\ flushed' = [flushed EXCEPT ![j] = inst[j].snap.idx]
    /\ synced' = [synced EXCEPT ![j] = inst[j].snap.idx]
    /\ stale' = [stale EXCEPT ![j] = NoTail]
    /\ inst' = [inst EXCEPT ![j] = NoInstall]
    /\ UNCHANGED <<election, machine, reader, match, used>>

\* A crash and the open that follows. The log keeps a prefix no shorter
\* than what was synced. The state machine is the snapshot plus the log up
\* to the committed index read back, which is never past the recovered log.
\* A snapshot past the recovered log makes the log restart at the snapshot:
\* openraft purges up to the applied index at startup, past the log's end.
\* That is also how a crash between an install's second and third write
\* opens. With SaveCommittedUnflushed the state machine reports an applied
\* index past the log and openraft restarts the log there, trusting it. A
\* shard's state machine is the log, so the entries between are lost.
\* openraft refuses to start a state machine that is behind the purged log.
Restart(s) ==
    /\ used.crashes < MaxCrashes
    /\ base[s].idx <= snap[s].idx
    /\ \E n \in Max(synced[s], base[s].idx)..LastIdx(s), torn \in BOOLEAN, back \in BOOLEAN :
        /\ torn => TornTailKept /\ n < LastIdx(s)
        /\ back => /\ TruncateNotDurable /\ ~torn
                   /\ n >= stale[s].from
                   /\ n - stale[s].from < Len(stale[s].tail)
        /\ LET kept == SubSeq(log[s], 1, n - base[s].idx)
               disk == IF torn THEN Append(kept, [EntryAt(s, n + 1) EXCEPT !.kind = "torn", !.val = 0])
                       ELSE IF back
                       THEN kept \o SubSeq(stale[s].tail, n - stale[s].from + 1, Len(stale[s].tail))
                       ELSE kept
               end == base[s].idx + Len(disk)
               saved == IF SaveCommittedUnflushed THEN Len(applied[s])
                        ELSE Min(Len(applied[s]), end)
               last == Max(snap[s].idx, saved)
               replay == [x \in 1..(Min(saved, end) - snap[s].idx) |->
                            disk[snap[s].idx + x - base[s].idx]]
           IN IF last > end
              THEN /\ base' = [base EXCEPT ![s] =
                          [idx |-> last,
                           term |-> IF last = snap[s].idx THEN snap[s].term
                                    ELSE applied[s][last].term]]
                   /\ log' = [log EXCEPT ![s] = <<>>]
                   /\ flushed' = [flushed EXCEPT ![s] = last]
                   /\ synced' = [synced EXCEPT ![s] = last]
                   /\ applied' = [applied EXCEPT ![s] =
                          IF snap[s].idx >= saved THEN snap[s].state
                          ELSE snap[s].state \o [x \in 1..(saved - snap[s].idx) |-> Lost]]
              ELSE /\ base' = base
                   /\ log' = [log EXCEPT ![s] = disk]
                   /\ flushed' = [flushed EXCEPT ![s] = end]
                   /\ synced' = [synced EXCEPT ![s] = IF RecoveredTailUnsynced THEN @ ELSE end]
                   /\ applied' = [applied EXCEPT ![s] = snap[s].state \o replay]
    /\ stale' = [stale EXCEPT ![s] = NoTail]
    /\ inst' = [inst EXCEPT ![s] = NoInstall]
    /\ vote' = [vote EXCEPT ![s] = voteDisk[s]]
    /\ role' = [role EXCEPT ![s] =
            IF LeaderResumes /\ voteDisk[s].committed /\ voteDisk[s].node = s
            THEN "leader" ELSE "follower"]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ match' = [match EXCEPT ![s] = [t \in Servers |-> 0]]
    /\ used' = [used EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<voteDisk, elected, snap, committed, acked, reader>>

\* The subscriber resumes after the last offset it received.
LastOff == IF got = <<>> THEN 0 ELSE got[Len(got)].off

\* The offsets directly below `o` that hold no event in `s`'s own log. The
\* serving broker knows nothing else: not what an earlier leader delivered.
SkippedBefore(s, o) ==
    Cardinality({i \in (base[s].idx + 1)..(o - 1) :
        \A k \in i..(o - 1) : EntryAt(s, k).kind # "record"})

\* A broker that believes it leads reads the subscriber the next record in
\* its log, up to the commit index. A blank or membership entry is not
\* delivered; the record's batch reports it in `skipped_before`.
Deliver(s) ==
    /\ Consumer
    /\ role[s] = "leader"
    /\ base[s].idx <= LastOff
    /\ LET bound == IF DeliverUncommitted THEN LastIdx(s) ELSE Min(Len(applied[s]), LastIdx(s))
           recs == {i \in (LastOff + 1)..bound : EntryAt(s, i).kind = "record"}
       IN /\ recs # {}
          /\ LET o == CHOOSE i \in recs : \A k \in recs : i <= k
             IN got' = Append(got, [off |-> o, val |-> EntryAt(s, o).val,
                                    skipped |-> IF SkipsNotCounted THEN 0 ELSE SkippedBefore(s, o),
                                    by |-> vote[s].term])
    /\ UNCHANGED <<election, storage, machine, match, used, inst>>

Next ==
    \/ \E s \in Servers :
        \/ Campaign(s) \/ BecomeLeader(s)
        \/ AppendRecord(s) \/ AppendMember(s)
        \/ Flush(s) \/ AdvanceCommit(s)
        \/ BuildSnapshot(s) \/ Purge(s)
        \/ InstallState(s) \/ InstallPurge(s)
        \/ Restart(s) \/ Deliver(s)
    \/ \E i, j \in Servers :
        \/ GrantVote(i, j) \/ AcceptLeader(i, j)
        \/ Replicate(i, j) \/ Ack(i, j) \/ LearnCommit(i, j)
        \/ InstallCut(i, j)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Servers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ vote[s].term \in 0..MaxTerm
        /\ synced[s] <= LastIdx(s)
        /\ flushed[s] <= LastIdx(s)
        /\ base[s].idx <= Len(applied[s])
        /\ inst[s].step \in {"none", "install", "purge"}
        /\ ~Idle(s) => LastIdx(s) < inst[s].snap.idx
    /\ acked \subseteq 1..MaxWrites
    /\ LastOff <= Len(committed) \/ DeliverUncommitted

OneLeaderPerTerm == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

LogMatching ==
    \A s, t \in Servers : \A i \in 1..Min(LastIdx(s), LastIdx(t)) :
        (Has(s, i) /\ Has(t, i) /\ EntryAt(s, i).term = EntryAt(t, i).term)
            => \A k \in 1..i : (Has(s, k) /\ Has(t, k)) => EntryAt(s, k) = EntryAt(t, k)

\* The open-time scan refuses a log that breaks this.
TermsMonotone ==
    \A s \in Servers : \A i \in 1..Len(log[s]) :
        /\ log[s][i].term >= base[s].term
        /\ i > 1 => log[s][i].term >= log[s][i - 1].term

LeaderCompleteness ==
    \A l \in Servers : role[l] = "leader" =>
        \A i \in 1..Len(committed) : committed[i].by < vote[l].term =>
            \/ i <= base[l].idx
            \/ Has(l, i) /\ EntryAt(l, i) = committed[i].e

StateMachineSafety ==
    \A s \in Servers : \A i \in 1..Len(applied[s]) :
        i <= Len(committed) /\ applied[s][i] = committed[i].e

AppliedAgree ==
    \A s, t \in Servers : \A i \in 1..Min(Len(applied[s]), Len(applied[t])) :
        applied[s][i] = applied[t][i]

\* `truncate_after` never reaches what a broker applied. DiskLog's own commit
\* offset is not modelled: it must be at or below this for `BelowCommit`
\* never to refuse a correct cut. Between a snapshot's install and its purge
\* the log is the old one: it ends below the snapshot and can hold entries
\* the snapshot replaced.
AppliedInLog ==
    \A s \in Servers : inst[s].step # "purge" =>
        /\ Len(applied[s]) <= LastIdx(s)
        /\ \A i \in 1..Len(applied[s]) : Has(s, i) => EntryAt(s, i) = applied[s][i]

IsRecord(e, v) == e.kind = "record" /\ e.val = v

OnDisk(s, v) ==
    \/ \E i \in (base[s].idx + 1)..synced[s] : IsRecord(EntryAt(s, i), v)
    \/ \E i \in DOMAIN snap[s].state : IsRecord(snap[s].state[i], v)

AckedSurvive ==
    \A v \in acked : \E Q \in SUBSET Servers : Quorum(Q) /\ \A s \in Q : OnDisk(s, v)

\* At startup openraft refuses a state machine that is behind the purged
\* log, and rebuilds a snapshot that is behind it from the state machine. A
\* shard's state machine is the log, which cannot give purged entries back,
\* so the stored snapshot has to cover every purge.
PurgedCoveredBySnapshot == \A s \in Servers : base[s].idx <= snap[s].idx

\* The client's drop check: the next event's offset is the last one's plus
\* one plus what the batch says was skipped. The count comes from the log of
\* whichever broker serves, so this fails if that log differs below the
\* record from what the subscriber was given.
OffsetsDense ==
    \A k \in 1..Len(got) :
        got[k].off = (IF k = 1 THEN 0 ELSE got[k - 1].off) + got[k].skipped + 1

OffsetNeverReused ==
    \A k \in 1..Len(got) : got[k].off <= Len(committed) =>
        IsRecord(committed[got[k].off].e, got[k].val)

\* Counting only records gives the dense numbering consumers rely on.
DeliveredInOrder ==
    LET recs == SelectSeq(committed, LAMBDA c : c.e.kind = "record")
    IN \A k \in 1..Len(got) : got[k].off <= Len(committed) =>
        k <= Len(recs) /\ recs[k].e.val = got[k].val

VoteNeverLowers == [][\A s \in Servers : VoteGE(voteDisk'[s], voteDisk[s])]_vars

\* Liveness, as a check on states where nothing more can happen: a leader
\* no broker has outvoted has committed its whole log, acknowledged every
\* record in it, brought every broker level and fed the subscriber.
TopLeader(l) == role[l] = "leader" /\ \A s \in Servers : vote[s].term <= vote[l].term

QuiescentServed ==
    ~ENABLED Next => \A l \in Servers : TopLeader(l) =>
        /\ used.writes = MaxWrites
        /\ Len(applied[l]) = LastIdx(l)
        /\ \A i \in 1..Len(log[l]) : log[l][i].kind = "record" => log[l][i].val \in acked
        /\ \A s \in Servers : applied[s] = applied[l]
        /\ (Consumer /\ MaxSnapshots = 0) =>
                \A i \in 1..Len(log[l]) : log[l][i].kind = "record" =>
                    \E k \in 1..Len(got) : got[k].off = i

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens and must be violated, to
\* show the checks above are not passing for want of behaviour.

\* An acknowledged record outlives a crash and a change of leader.
NeverFailover ==
    ~ \E l \in Servers :
        /\ role[l] = "leader"
        /\ vote[l].term > 1
        /\ used.crashes > 0
        /\ \E v \in acked : \E i \in 1..Len(applied[l]) :
            IsRecord(applied[l][i], v) /\ committed[i].by < vote[l].term

\* A follower cuts off a tail that conflicts with its leader.
NeverTruncates ==
    [][\A s \in Servers :
        (used'.crashes = used.crashes /\ base'[s] = base[s]) =>
            \A k \in 1..Min(Len(log[s]), Len(log'[s])) : log'[s][k] = log[s][k]]_vars

\* A crash loses entries that were appended and not flushed.
NeverLosesTail ==
    [][\A s \in Servers :
        base'[s] = base[s] => Len(log'[s]) >= Len(log[s]) \/ used'.crashes = used.crashes]_vars

\* A blank or membership entry sits between two delivered records.
NeverSkipsBetweenRecords == \A k \in 2..Len(got) : got[k].skipped = 0

\* A follower is brought up by a leader's snapshot.
NeverInstallsSnapshot ==
    [][\A s \in Servers : snap'[s] # snap[s] => snap'[s].idx = Len(applied[s])]_vars

\* A broker crashes between an install's second and third write and opens.
NeverCrashesMidInstall ==
    [][\A s \in Servers :
        (inst[s].step = "purge" /\ used'.crashes > used.crashes) => inst'[s] = inst[s]]_vars

\* A broker replaces a snapshot it already stored.
NeverReplacesSnapshot ==
    [][\A s \in Servers : snap'[s] # snap[s] => snap[s] = NoSnap]_vars

\* Two records in a row reach the subscriber from leaders of different terms.
NeverDeliversAcrossFailover == \A k \in 2..Len(got) : got[k].by = got[k - 1].by

=============================================================================
