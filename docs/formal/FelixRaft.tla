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
(* has moved on. One subscriber reads from brokers that believe they lead. *)
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
(*                              applied.                                   *)
(*   AckedSurvive            -- an acknowledged record is on the disks of  *)
(*                              a majority.                                *)
(*   PurgedCoveredBySnapshot -- no log is purged past its stored snapshot. *)
(*   OffsetsDense            -- delivered offsets, less the skipped ones   *)
(*                              each batch reports, have no gap.           *)
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
(*   TruncateNotDurable     -- `truncate_after` is not on disk before a    *)
(*                             later append is reported flushed.           *)
(*   RecoveredTailUnsynced  -- entries found at startup are reported as    *)
(*                             flushed without an fsync.                   *)
(*   SaveCommittedUnflushed -- the committed or applied index read back at *)
(*                             startup is past the recovered log.          *)
(*   PurgeBeforeSnapshot    -- `purge` runs before the snapshot covering   *)
(*                             it is stored.                               *)
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
    committed,  \* history: the committed sequence, each entry with the term that committed it
    acked,      \* record values acknowledged to their publisher
    got,        \* what the subscriber received: [off, val, skipped]
    cursor,     \* the last index read to the subscriber
    skips,      \* skipped offsets since the last delivered record
    used        \* budgets spent: writes, members, crashes, snaps

election == <<role, vote, voteDisk, granted, elected>>
storage == <<log, base, flushed, synced, stale>>
machine == <<applied, snap, committed, acked>>
reader == <<got, cursor, skips>>
vars == <<election, storage, machine, reader, match, used>>

Symm == Permutations(Servers)

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
Quorum(Q) == 2 * Cardinality(Q) > Cardinality(Servers)

Blank(t) == [term |-> t, kind |-> "blank", val |-> 0]
NoVote == [term |-> 0, node |-> Nobody, committed |-> FALSE]
NoTail == [from |-> 0, tail |-> <<>>]

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

Following(i, j) == i # j /\ role[i] = "leader" /\ vote[j] = vote[i]

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
    /\ snap = [s \in Servers |-> [idx |-> 0, term |-> 0, state |-> <<>>]]
    /\ committed = <<>>
    /\ acked = {}
    /\ got = <<>>
    /\ cursor = 0
    /\ skips = 0
    /\ used = [writes |-> 0, members |-> 0, crashes |-> 0, snaps |-> 0]

\* `save_vote`: on disk before it returns.
SaveVote(s, v) ==
    /\ vote' = [vote EXCEPT ![s] = v]
    /\ voteDisk' = IF VoteNotDurable THEN voteDisk ELSE [voteDisk EXCEPT ![s] = v]

Campaign(c) ==
    /\ role[c] # "leader"
    /\ vote[c].term < MaxTerm
    /\ SaveVote(c, [term |-> vote[c].term + 1, node |-> c, committed |-> FALSE])
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {c}]
    /\ UNCHANGED <<elected, storage, machine, reader, match, used>>

\* A voter compares the candidate's last entry with its own, unflushed
\* entries included, and stores the vote before it answers.
GrantVote(s, c) ==
    /\ s # c
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
    /\ UNCHANGED <<elected, storage, machine, reader, match, used>>

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
    /\ UNCHANGED <<granted, base, synced, stale, machine, reader, used>>

\* A follower stores the leader's committed vote before it takes anything
\* from that leader.
AcceptLeader(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ vote[j] # vote[i]
    /\ VoteGE(vote[i], vote[j])
    /\ SaveVote(j, vote[i])
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ UNCHANGED <<elected, storage, machine, reader, match, used>>

LeaderAppend(i, e) ==
    /\ role[i] = "leader"
    /\ log' = [log EXCEPT ![i] = Append(@, e)]
    /\ flushed' = IF AckBeforeFlush THEN [flushed EXCEPT ![i] = LastIdx(i) + 1] ELSE flushed
    /\ UNCHANGED <<election, base, synced, stale, machine, reader, match>>

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
Flush(s) ==
    /\ synced[s] < LastIdx(s)
    /\ synced' = [synced EXCEPT ![s] = LastIdx(s)]
    /\ flushed' = [flushed EXCEPT ![s] = LastIdx(s)]
    /\ UNCHANGED <<election, log, base, stale, machine, reader, match, used>>

\* AppendEntries with one entry, after the highest index the two logs agree
\* on. A follower holding something else there cuts it off first
\* (`truncate_after`), and the cut is on disk before the append.
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
    /\ UNCHANGED <<election, base, machine, reader, match, used>>

\* The follower's answer: how far its log matches the leader's and is flushed.
Ack(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET p == Min(MaxOf(Common(i, j)), flushed[j])
       IN /\ p > match[i][j]
          /\ match' = [match EXCEPT ![i][j] = p]
    /\ UNCHANGED <<election, storage, machine, reader, used>>

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
    /\ UNCHANGED <<election, storage, snap, reader, match, used>>

\* A follower applies up to the leader's commit index, as far as its log
\* matches the leader's. Its own copy need not be flushed.
LearnCommit(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) # {}
    /\ LET k == Min(MaxOf(Common(i, j)), Len(applied[i]))
       IN /\ k > Len(applied[j])
          /\ applied' = [applied EXCEPT ![j] = @ \o Entries(j, Len(@), k)]
    /\ UNCHANGED <<election, storage, snap, committed, acked, reader, match, used>>

\* The snapshot, data and id together, is replaced by one rename: a crash
\* leaves the old one or the new one, so the write is one step.
BuildSnapshot(s) ==
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
    /\ UNCHANGED <<election, storage, applied, committed, acked, reader, match>>

\* Only up to the stored snapshot.
Purge(s) ==
    /\ MaxSnapshots > 0
    /\ LET k == IF PurgeBeforeSnapshot THEN Len(applied[s]) ELSE snap[s].idx
       IN /\ k > base[s].idx
          /\ k <= LastIdx(s)
          /\ base' = [base EXCEPT ![s] = [idx |-> k, term |-> TermAt(s, k)]]
          /\ log' = [log EXCEPT ![s] = SubSeq(@, k - base[s].idx + 1, Len(@))]
    /\ UNCHANGED <<election, flushed, synced, stale, machine, reader, match, used>>

\* A follower whose log ends below the leader's purged id takes the
\* leader's snapshot and restarts its log there.
InstallSnapshot(i, j) ==
    /\ Following(i, j)
    /\ Common(i, j) = {}
    /\ snap[i].idx >= base[i].idx
    /\ snap[i].idx = Len(snap[i].state)
    /\ snap[i].idx > Len(applied[j])
    /\ snap' = [snap EXCEPT ![j] = snap[i]]
    /\ base' = [base EXCEPT ![j] = [idx |-> snap[i].idx, term |-> snap[i].term]]
    /\ log' = [log EXCEPT ![j] = <<>>]
    /\ flushed' = [flushed EXCEPT ![j] = snap[i].idx]
    /\ synced' = [synced EXCEPT ![j] = snap[i].idx]
    /\ stale' = [stale EXCEPT ![j] = NoTail]
    /\ applied' = [applied EXCEPT ![j] = snap[i].state]
    /\ UNCHANGED <<election, committed, acked, reader, match, used>>

\* A crash and the open that follows. The log keeps a prefix no shorter
\* than what was synced. The state machine is the snapshot plus the log up
\* to the committed index read back, which is never past the recovered log.
\* A snapshot past the recovered log makes the log restart at the snapshot.
\* With SaveCommittedUnflushed the state machine reports an applied index
\* past the log, and openraft restarts the log there, trusting it.
Restart(s) ==
    /\ used.crashes < MaxCrashes
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
                          IF snap[s].idx >= saved THEN snap[s].state ELSE SubSeq(@, 1, saved)]
              ELSE /\ base' = base
                   /\ log' = [log EXCEPT ![s] = disk]
                   /\ flushed' = [flushed EXCEPT ![s] = end]
                   /\ synced' = [synced EXCEPT ![s] = IF RecoveredTailUnsynced THEN @ ELSE end]
                   /\ applied' = [applied EXCEPT ![s] = snap[s].state \o replay]
    /\ stale' = [stale EXCEPT ![s] = NoTail]
    /\ vote' = [vote EXCEPT ![s] = voteDisk[s]]
    /\ role' = [role EXCEPT ![s] =
            IF LeaderResumes /\ voteDisk[s].committed /\ voteDisk[s].node = s
            THEN "leader" ELSE "follower"]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ match' = [match EXCEPT ![s] = [t \in Servers |-> 0]]
    /\ used' = [used EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<voteDisk, elected, snap, committed, acked, reader>>

\* The subscriber is read one index at a time, up to the commit index. A
\* blank or membership entry is not delivered; the next record's batch
\* reports it in `skipped_before`.
Deliver(s) ==
    /\ Consumer
    /\ role[s] = "leader"
    /\ Has(s, cursor + 1)
    /\ cursor + 1 <= IF DeliverUncommitted THEN LastIdx(s) ELSE Len(applied[s])
    /\ LET e == EntryAt(s, cursor + 1)
       IN IF e.kind = "record"
          THEN /\ got' = Append(got, [off |-> cursor + 1, val |-> e.val, skipped |-> skips])
               /\ skips' = 0
          ELSE /\ got' = got
               /\ skips' = IF SkipsNotCounted THEN skips ELSE skips + 1
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<election, storage, machine, match, used>>

Next ==
    \/ \E s \in Servers :
        \/ Campaign(s) \/ BecomeLeader(s)
        \/ AppendRecord(s) \/ AppendMember(s)
        \/ Flush(s) \/ AdvanceCommit(s)
        \/ BuildSnapshot(s) \/ Purge(s)
        \/ Restart(s) \/ Deliver(s)
    \/ \E i, j \in Servers :
        \/ GrantVote(i, j) \/ AcceptLeader(i, j)
        \/ Replicate(i, j) \/ Ack(i, j) \/ LearnCommit(i, j)
        \/ InstallSnapshot(i, j)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ \A s \in Servers :
        /\ role[s] \in {"follower", "candidate", "leader"}
        /\ vote[s].term \in 0..MaxTerm
        /\ synced[s] <= LastIdx(s)
        /\ flushed[s] <= LastIdx(s)
        /\ base[s].idx <= Len(applied[s])
    /\ acked \subseteq 1..MaxWrites
    /\ cursor <= Len(committed) \/ DeliverUncommitted

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
\* never to refuse a correct cut.
AppliedInLog ==
    \A s \in Servers :
        /\ Len(applied[s]) <= LastIdx(s)
        /\ \A i \in 1..Len(applied[s]) : Has(s, i) => EntryAt(s, i) = applied[s][i]

IsRecord(e, v) == e.kind = "record" /\ e.val = v

OnDisk(s, v) ==
    \/ \E i \in (base[s].idx + 1)..synced[s] : IsRecord(EntryAt(s, i), v)
    \/ \E i \in DOMAIN snap[s].state : IsRecord(snap[s].state[i], v)

AckedSurvive ==
    \A v \in acked : \E Q \in SUBSET Servers : Quorum(Q) /\ \A s \in Q : OnDisk(s, v)

\* A log that starts past its stored snapshot can be served only from a
\* state machine that kept the entries between. A shard's state machine is
\* the log, so there the entries are gone.
PurgedCoveredBySnapshot == \A s \in Servers : base[s].idx <= snap[s].idx

\* The client's drop check: the next event's offset is the last one's plus
\* one plus what the batch says was skipped.
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
        /\ (Consumer /\ MaxSnapshots = 0) => cursor = LastIdx(l)

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

=============================================================================
