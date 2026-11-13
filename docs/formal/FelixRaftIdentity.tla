-------------------------- MODULE FelixRaftIdentity --------------------------
(***************************************************************************)
(* Copy identity for one Raft group, as docs/per-shard-raft.md decides it  *)
(* under "Group and member identity". This is the model's side of issue    *)
(* 1331: a copy that lost or went back on what it promised must not vote   *)
(* under the id that made the promise.                                     *)
(*                                                                         *)
(* A member id is <<broker, copy id>>. What a broker keeps:                *)
(*                                                                         *)
(*   In the shard directory                                                *)
(*     copy    -- the copy id, minted when the member is first made there. *)
(*     minted  -- the storage id the copy id was minted under.             *)
(*     vote    -- the vote file. Always fsynced, so there is one variable. *)
(*     log     -- the Raft log.                                            *)
(*   At the data root                                                      *)
(*     sid     -- the storage id.                                          *)
(*     vmark   -- high-water mark of the shard's vote.                     *)
(*     lmark   -- high-water mark of the shard's last log id.              *)
(*     rcopy   -- the copy id the marks belong to.                         *)
(*     rseq    -- the root sequence. It rises with every synced write of   *)
(*                a mark.                                                  *)
(*   Outside the root                                                      *)
(*     reg     -- the incarnation the control plane holds. Every start     *)
(*                takes a new one and records it at the root.              *)
(*     cseq    -- the root sequence the control plane last heard in a      *)
(*                heartbeat. It trails.                                    *)
(*     The run witness holds the root sequence too. It is written with     *)
(*     every mark write and is in memory, so while the machine stays up    *)
(*     it holds what the root held before a restore, and after a machine   *)
(*     restart it holds nothing (WitnessLost).                             *)
(*                                                                         *)
(* When a member opens its directory the copy is rotated (new copy id,     *)
(* never voted, empty Raft log, a learner until a membership change swaps  *)
(* it in) if the directory is empty, if `minted` is not the root's storage *)
(* id, if the directory's copy id is not the one the marks name, or if its *)
(* vote or last log id is below its mark.                                  *)
(*                                                                         *)
(* Faults, all on one broker (a group of three survives one bad copy):     *)
(*                                                                         *)
(*   "wipe"    -- the shard directory is emptied.                          *)
(*   "lose"    -- the data root is lost: new storage id, no marks.         *)
(*   "relaxed" -- a crash under relaxed fsync with a machine restart. The  *)
(*                log keeps any prefix, the log mark is no better synced   *)
(*                than the log, and the storage id rotates.                *)
(*   "root"    -- the whole root goes back to a backup. The gate in front  *)
(*                of Raft compares the root's incarnation with the control *)
(*                plane's, and a root that is behind gets a new storage    *)
(*                id. A backup taken in the run the restore interrupts     *)
(*                carries the current incarnation and run token            *)
(*                (RootRestoreSameRun lets the model try that), so the     *)
(*                gate also fails a root whose sequence is below the       *)
(*                witness's copy or the control plane's. RestoreUnheard    *)
(*                lets a root with no witness go back before the control   *)
(*                plane heard its last sequence. RestoreYoung lets a root  *)
(*                go back while the batched log mark trails the log.       *)
(*   "dir"     -- one shard directory goes back to a backup under a        *)
(*                running root. RestoreYoung lets it happen while the      *)
(*                batched log mark still trails the log.                   *)
(*                                                                         *)
(* The log mark is exact (ExactMark: written with every append, which is   *)
(* the group-commit flush) or batched (a separate step copies the last log *)
(* id to the root once per interval).                                      *)
(*                                                                         *)
(* A rotated copy is replaced by a real membership change: once the new    *)
(* copy has the leader's whole log the leader appends a joint entry (old   *)
(* voters and new voters, both majorities needed), and when that is        *)
(* committed, the final entry. One change at a time. A member uses the     *)
(* latest membership in its log.                                           *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   OneLeaderPerTerm   -- no term elects two members.                     *)
(*   LeaderCompleteness -- a leader holds every entry committed in an      *)
(*                         earlier term.                                   *)
(*   StateMachineSafety -- no two leaders commit different entries at one  *)
(*                         index.                                          *)
(*   AckedSurvive       -- no set of copies that could elect a leader      *)
(*                         lacks an acknowledged record. With no fault     *)
(*                         this is "on a majority of the voters' disks".   *)
(*                         A copy id no directory holds any more cannot    *)
(*                         vote, so it is not counted either way.          *)
(*   AckedHeldByLeader  -- no leader is elected without a record that was  *)
(*                         acknowledged in an earlier term.                *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   WipeKeepsId      -- a member made in an empty directory takes the old *)
(*                       id.                                               *)
(*   NoRestoreCheck   -- no gate in front of Raft: a restored root keeps   *)
(*                       its storage id.                                   *)
(*   NoStorageIdCheck -- a directory is not compared with the root's       *)
(*                       storage id.                                       *)
(*   NoVoteMark       -- a directory's vote is not compared with its mark. *)
(*   VoteBeforeMark   -- the vote is answered before its mark is synced    *)
(*                       at the root.                                      *)
(*   MarkForgetsCopy  -- the marks do not name the copy id, so they are    *)
(*                       reset at a rotation and an older directory with   *)
(*                       the old copy id passes them.                      *)
(*   NoRootSequence   -- the gate does not compare the root sequence.      *)
(*                                                                         *)
(* Abstracted: the log is on disk when it is appended (FelixRaft.tla has   *)
(* the adapter's fsync rules), a message is one atomic step, a learner     *)
(* needs no membership entry, the logs are never purged, and a rotated     *)
(* copy starts from an empty log (the design keeps its records as plain    *)
(* records, which saves a transfer and counts for nothing). A follower     *)
(* cuts and appends in one step, so its last log id never goes down. The   *)
(* leader that stops when a follower goes backwards                        *)
(* (`allow_log_reversion` off) is left out: the harm needs no leader.      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Nobody,
    MaxTerm,
    MaxWrites,
    MaxChanges,
    MaxFaults,
    MaxRestarts,
    Faults,
    ExactMark,
    RestoreYoung,
    RootRestoreSameRun,
    WitnessLost,
    RestoreUnheard,
    WipeKeepsId,
    NoRestoreCheck,
    NoStorageIdCheck,
    NoVoteMark,
    VoteBeforeMark,
    MarkForgetsCopy,
    NoRootSequence

VARIABLES
    role,     \* "follower", "candidate" or "leader"
    vote,     \* the vote file in the shard directory
    granted,  \* the member ids that voted for a candidate
    elected,  \* history: every <<term, member id>> that won an election
    log,      \* the Raft log in the shard directory
    match,    \* a leader's view of each member id's matching prefix
    copy,     \* the copy id in the shard directory
    minted,   \* the storage id the copy id was minted under
    sid,      \* the root's storage id
    vmark,    \* the root's vote mark for the shard
    lmark,    \* the root's last log id mark for the shard
    rcopy,    \* the copy id the marks belong to
    rseq,     \* the root sequence
    cseq,     \* the root sequence the control plane last heard
    reg,      \* the incarnation the control plane holds for the broker
    nxt,      \* history: the next unused copy id and storage id per broker
    bak,      \* one backup of one broker's root and shard directory
    commits,  \* history: every [idx, e, by] a leader committed
    used      \* budgets spent, and the one broker faults hit

vars == <<role, vote, granted, elected, log, match, copy, minted, sid, vmark, lmark,
          rcopy, rseq, cseq, reg, nxt, bak, commits, used>>

Symm == Permutations(Servers)

Members == Servers \X (0..MaxFaults)
NoMember == <<Nobody, 0>>
NoVote == [term |-> 0, node |-> NoMember, committed |-> FALSE]
NoCfg == [old |-> {}, new |-> {}]
NoId == <<0, 0>>
NoBackup == [who |-> Nobody]

MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

Me(s) == <<s, copy[s]>>

FirstVoters == {<<s, 0>> : s \in Servers}

\* openraft's order on votes with one leader per term, as in FelixRaft.tla.
VoteGE(a, b) ==
    \/ a.term > b.term
    \/ /\ a.term = b.term
       /\ \/ a = b
          \/ a.committed /\ ~b.committed
          \/ ~a.committed /\ ~b.committed /\ b.node \in {a.node, NoMember}

LastId(l) == IF Len(l) = 0 THEN NoId ELSE <<l[Len(l)].term, Len(l)>>
IdBelow(x, y) == x[1] < y[1] \/ (x[1] = y[1] /\ x[2] < y[2])
TermAt(s, i) == IF i = 0 THEN 0 ELSE log[s][i].term

\* The latest membership in a log, committed or not.
CfgIdxOf(l) ==
    LET at == {k \in 1..Len(l) : l[k].kind = "member"}
    IN IF at = {} THEN 0 ELSE MaxOf(at)
CfgOf(l) == IF CfgIdxOf(l) = 0 THEN [old |-> FirstVoters, new |-> FirstVoters]
            ELSE l[CfgIdxOf(l)].cfg
Cfg(s) == CfgOf(log[s])

Maj(S, Q) == 2 * Cardinality(Q \cap S) > Cardinality(S)
QuorumIn(cfg, Q) == Maj(cfg.old, Q) /\ Maj(cfg.new, Q)
IsVoter(s) == Me(s) \in Cfg(s).old \cup Cfg(s).new

Entry(t, k, v, c) == [term |-> t, kind |-> k, val |-> v, cfg |-> c]

Common(i, j) ==
    {p \in 0..(IF Len(log[i]) <= Len(log[j]) THEN Len(log[i]) ELSE Len(log[j])) :
        TermAt(i, p) = TermAt(j, p)}

\* How far a leader knows its log is committed: entries of its own term
\* that a quorum of its latest membership holds.
CommitPoint(i) ==
    LET ok == {n \in 1..Len(log[i]) :
                /\ log[i][n].term = vote[i].term
                /\ QuorumIn(Cfg(i), {m \in Members : match[i][m] >= n} \cup {Me(i)})}
    IN IF ok = {} THEN 0 ELSE MaxOf(ok)

Init ==
    /\ role = [s \in Servers |-> "follower"]
    /\ vote = [s \in Servers |-> NoVote]
    /\ granted = [s \in Servers |-> {}]
    /\ elected = {}
    /\ log = [s \in Servers |-> <<>>]
    /\ match = [s \in Servers |-> [m \in Members |-> 0]]
    /\ copy = [s \in Servers |-> 0]
    /\ minted = [s \in Servers |-> 0]
    /\ sid = [s \in Servers |-> 0]
    /\ vmark = [s \in Servers |-> NoVote]
    /\ lmark = [s \in Servers |-> NoId]
    /\ rcopy = [s \in Servers |-> 0]
    /\ rseq = [s \in Servers |-> 0]
    /\ cseq = [s \in Servers |-> 0]
    /\ reg = [s \in Servers |-> 0]
    /\ nxt = [s \in Servers |-> [copy |-> 1, sid |-> 1]]
    /\ bak = NoBackup
    /\ commits = {}
    /\ used = [writes |-> 0, changes |-> 0, faults |-> 0, restarts |-> 0, faulty |-> Nobody]

identity == <<copy, minted, sid, rcopy, reg, nxt, bak>>

\* Broker `s` syncs `n` mark writes at its root. Only a restored root is
\* compared with the sequence, and a write before the backup is in the
\* backup, so only the backed-up broker's later writes are counted.
Counted(s) == bak.who = s /\ "root" \in Faults
MarkWrites(s, n) ==
    /\ rseq' = [rseq EXCEPT ![s] = IF Counted(s) THEN @ + n ELSE @]
    /\ UNCHANGED cseq
VoteWrites == IF VoteBeforeMark THEN 0 ELSE 1
LogWrites == IF ExactMark THEN 1 ELSE 0

\* `save_vote`: the mark is synced at the root before it returns.
SaveVote(s, v) ==
    /\ vote' = [vote EXCEPT ![s] = v]
    /\ vmark' = IF VoteBeforeMark THEN vmark ELSE [vmark EXCEPT ![s] = v]

SyncVoteMark(s) ==
    /\ VoteBeforeMark
    /\ vmark[s] # vote[s]
    /\ vmark' = [vmark EXCEPT ![s] = vote[s]]
    /\ MarkWrites(s, 1)
    /\ UNCHANGED <<role, vote, granted, elected, log, match, lmark, identity, commits, used>>

\* The exact mark rides the flush that makes the append durable.
SetLog(s, l) ==
    /\ log' = [log EXCEPT ![s] = l]
    /\ lmark' = IF ExactMark THEN [lmark EXCEPT ![s] = LastId(l)] ELSE lmark

\* The batched mark: one synced write per interval. Only a restored
\* directory is compared with it, and a mark written before the backup is
\* never above the backup, so only the backed-up broker's later marks count.
MarkLog(s) ==
    /\ ~ExactMark
    /\ bak.who = s
    /\ lmark[s] # LastId(log[s])
    /\ lmark' = [lmark EXCEPT ![s] = LastId(log[s])]
    /\ MarkWrites(s, 1)
    /\ UNCHANGED <<role, vote, granted, elected, log, match, vmark, identity, commits, used>>

\* The control plane hears the broker's last synced sequence. Its copy is
\* never above the witness's, so it decides nothing while there is one.
Heartbeat(s) ==
    /\ Counted(s)
    /\ WitnessLost
    /\ cseq[s] # rseq[s]
    /\ cseq' = [cseq EXCEPT ![s] = rseq[s]]
    /\ UNCHANGED <<role, vote, granted, elected, log, match, vmark, lmark, rseq, identity,
                   commits, used>>

Campaign(c) ==
    /\ role[c] # "leader"
    /\ IsVoter(c)
    /\ vote[c].term < MaxTerm
    /\ SaveVote(c, [term |-> vote[c].term + 1, node |-> Me(c), committed |-> FALSE])
    /\ MarkWrites(c, VoteWrites)
    /\ role' = [role EXCEPT ![c] = "candidate"]
    /\ granted' = [granted EXCEPT ![c] = {Me(c)}]
    /\ UNCHANGED <<elected, log, match, lmark, identity, commits, used>>

\* Only a voter of the candidate's membership is asked.
GrantVote(s, c) ==
    /\ s # c
    /\ role[c] = "candidate"
    /\ Me(s) \in Cfg(c).old \cup Cfg(c).new
    /\ Me(s) \notin granted[c]
    /\ VoteGE(vote[c], vote[s])
    /\ ~IdBelow(LastId(log[c]), LastId(log[s]))
    /\ SaveVote(s, vote[c])
    /\ MarkWrites(s, VoteWrites)
    /\ role' = [role EXCEPT ![s] = IF vote[s].term < vote[c].term THEN "follower" ELSE @]
    /\ granted' = [granted EXCEPT ![c] = @ \cup {Me(s)},
                                  ![s] = IF vote[s].term < vote[c].term THEN {} ELSE @]
    /\ UNCHANGED <<elected, log, match, lmark, identity, commits, used>>

BecomeLeader(c) ==
    /\ role[c] = "candidate"
    /\ QuorumIn(Cfg(c), granted[c])
    /\ SaveVote(c, [vote[c] EXCEPT !.committed = TRUE])
    /\ MarkWrites(c, VoteWrites + LogWrites)
    /\ role' = [role EXCEPT ![c] = "leader"]
    /\ elected' = elected \cup {<<vote[c].term, Me(c)>>}
    /\ SetLog(c, Append(log[c], Entry(vote[c].term, "blank", 0, NoCfg)))
    /\ match' = [match EXCEPT ![c] = [m \in Members |-> 0]]
    /\ UNCHANGED <<granted, identity, commits, used>>

\* A voter or a learner stores the leader's vote before it takes entries.
AcceptLeader(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ vote[j] # vote[i]
    /\ VoteGE(vote[i], vote[j])
    /\ SaveVote(j, vote[i])
    /\ MarkWrites(j, VoteWrites)
    /\ role' = [role EXCEPT ![j] = "follower"]
    /\ granted' = [granted EXCEPT ![j] = {}]
    /\ UNCHANGED <<elected, log, match, lmark, identity, commits, used>>

LeaderAppend(i, e) ==
    /\ role[i] = "leader"
    /\ SetLog(i, Append(log[i], e))
    /\ MarkWrites(i, LogWrites)
    /\ UNCHANGED <<role, vote, granted, elected, match, vmark, identity, commits>>

AppendRecord(i) ==
    /\ used.writes < MaxWrites
    /\ LeaderAppend(i, Entry(vote[i].term, "record", used.writes + 1, NoCfg))
    /\ used' = [used EXCEPT !.writes = @ + 1]

\* One entry after the highest index the two logs agree on, to a voter or
\* a learner. The follower cuts a conflicting tail in the same step.
Replicate(i, j) ==
    /\ i # j
    /\ role[i] = "leader"
    /\ vote[j] = vote[i]
    /\ LET p == MaxOf(Common(i, j))
       IN /\ p < Len(log[i])
          /\ SetLog(j, Append(SubSeq(log[j], 1, p), log[i][p + 1]))
          /\ match' = [match EXCEPT ![i][Me(j)] = p + 1]
    /\ MarkWrites(j, LogWrites)
    /\ UNCHANGED <<role, vote, granted, elected, vmark, identity, commits, used>>

\* The leader counts the answers it has, whatever became of the copies
\* that gave them.
Commit(i) ==
    /\ role[i] = "leader"
    /\ LET new == {[idx |-> k, e |-> log[i][k], by |-> vote[i].term] : k \in 1..CommitPoint(i)}
           fresh == {c \in new : ~\E d \in commits : d.idx = c.idx /\ d.e = c.e}
       IN /\ fresh # {}
          /\ commits' = commits \cup fresh
    /\ UNCHANGED <<role, vote, granted, elected, log, match, vmark, lmark, rseq, cseq,
                   identity, used>>

\* The joint entry that swaps a rotated copy's old id for its new one. The
\* new copy has caught up, the last membership is committed, and so is an
\* entry of this term.
ProposeJoint(i, j) ==
    /\ used.changes < MaxChanges
    /\ role[i] = "leader"
    /\ Cfg(i).old = Cfg(i).new
    /\ CommitPoint(i) >= 1
    /\ CommitPoint(i) >= CfgIdxOf(log[i])
    /\ Me(j) \notin Cfg(i).new
    /\ match[i][Me(j)] = Len(log[i])
    /\ \E m \in Cfg(i).new :
        /\ m[1] = j
        /\ LeaderAppend(i, Entry(vote[i].term, "member", 0,
                [old |-> Cfg(i).new, new |-> (Cfg(i).new \ {m}) \cup {Me(j)}]))
    /\ used' = [used EXCEPT !.changes = @ + 1]

\* The final entry, once the joint one is committed.
Finalize(i) ==
    /\ role[i] = "leader"
    /\ Cfg(i).old # Cfg(i).new
    /\ CommitPoint(i) >= CfgIdxOf(log[i])
    /\ LeaderAppend(i, Entry(vote[i].term, "member", 0,
            [old |-> Cfg(i).new, new |-> Cfg(i).new]))
    /\ UNCHANGED used

----------------------------------------------------------------------------
\* Faults and the checks a member makes when it opens its directory.

MayFail(s, kind) ==
    /\ kind \in Faults
    /\ used.faults < MaxFaults
    /\ used.faulty \in {Nobody, s}

Empty == [empty |-> TRUE, copy |-> 0, minted |-> 0, vote |-> NoVote, log |-> <<>>]
Dir(s) == [empty |-> FALSE, copy |-> copy[s], minted |-> minted[s], vote |-> vote[s],
           log |-> log[s]]
Root(s) == [sid |-> sid[s], vmark |-> vmark[s], lmark |-> lmark[s], rcopy |-> rcopy[s],
            seq |-> rseq[s]]

MustRotate(d, r) ==
    \/ d.empty
    \/ ~NoStorageIdCheck /\ d.minted # r.sid
    \/ ~MarkForgetsCopy /\ d.copy # r.rcopy
    \/ ~NoVoteMark /\ ~VoteGE(d.vote, r.vmark)
    \/ IdBelow(LastId(d.log), r.lmark)

\* Broker `s` opens directory `d` under root `r` after a fault. A new copy
\* id and its storage id are written together, with fresh marks, which is a
\* mark write. The control plane keeps a sequence per storage id.
Open(s, d, r) ==
    LET keep == d.empty /\ WipeKeepsId
        rotate == MustRotate(d, r) /\ ~keep
        fresh == keep \/ rotate
        id == IF rotate THEN nxt[s].copy ELSE IF keep THEN copy[s] ELSE d.copy
        seq == IF fresh /\ Counted(s) THEN r.seq + 1 ELSE r.seq
    IN /\ copy' = [copy EXCEPT ![s] = id]
       /\ rseq' = [rseq EXCEPT ![s] = seq]
       /\ cseq' = [cseq EXCEPT ![s] = IF r.sid # sid[s] THEN seq ELSE @]
       /\ minted' = [minted EXCEPT ![s] = IF fresh THEN r.sid ELSE d.minted]
       /\ vote' = [vote EXCEPT ![s] = IF fresh THEN NoVote ELSE d.vote]
       /\ log' = [log EXCEPT ![s] = IF fresh THEN <<>> ELSE d.log]
       /\ sid' = [sid EXCEPT ![s] = r.sid]
       /\ vmark' = [vmark EXCEPT ![s] = IF fresh THEN NoVote ELSE r.vmark]
       /\ lmark' = [lmark EXCEPT ![s] = IF fresh THEN NoId ELSE r.lmark]
       /\ rcopy' = [rcopy EXCEPT ![s] = IF fresh THEN id ELSE r.rcopy]
       /\ nxt' = [nxt EXCEPT ![s] = [copy |-> IF rotate THEN @.copy + 1 ELSE @.copy,
                                     sid |-> IF r.sid = @.sid THEN @.sid + 1 ELSE @.sid]]
       /\ role' = [role EXCEPT ![s] = "follower"]
       /\ granted' = [granted EXCEPT ![s] = {}]
       /\ used' = [used EXCEPT !.faults = @ + 1, !.faulty = s]
       /\ UNCHANGED <<elected, match, commits>>

Wipe(s) ==
    /\ MayFail(s, "wipe")
    /\ Open(s, Empty, Root(s))
    /\ UNCHANGED <<reg, bak>>

LoseRoot(s) ==
    /\ MayFail(s, "lose")
    /\ Open(s, Empty, [sid |-> nxt[s].sid, vmark |-> NoVote, lmark |-> NoId, rcopy |-> 0,
                       seq |-> 0])
    /\ reg' = [reg EXCEPT ![s] = @ + 1]
    /\ UNCHANGED bak

\* The vote survives. The log mark is written no more safely than the log,
\* so at worst it names only what is left.
RelaxedCrash(s) ==
    /\ MayFail(s, "relaxed")
    /\ \E k \in 0..Len(log[s]) :
        LET left == SubSeq(log[s], 1, k)
            mark == IF IdBelow(LastId(left), lmark[s]) THEN LastId(left) ELSE lmark[s]
        IN Open(s, [Dir(s) EXCEPT !.log = left],
                [Root(s) EXCEPT !.sid = nxt[s].sid, !.lmark = mark])
    /\ reg' = [reg EXCEPT ![s] = @ + 1]
    /\ UNCHANGED bak

Backup(s) ==
    /\ bak = NoBackup
    /\ Faults \cap {"root", "dir"} # {}
    /\ used.faults < MaxFaults
    /\ used.faulty \in {Nobody, s}
    /\ bak' = [who |-> s, dir |-> Dir(s), root |-> Root(s), inc |-> reg[s]]
    /\ UNCHANGED <<role, vote, granted, elected, log, match, copy, minted, sid, vmark,
                   lmark, rcopy, rseq, cseq, reg, nxt, commits, used>>

\* A clean stop and start, so that a backup can be from an earlier run.
Restart(s) ==
    /\ bak # NoBackup
    /\ bak.who = s
    /\ used.restarts < MaxRestarts
    /\ reg' = [reg EXCEPT ![s] = @ + 1]
    /\ role' = [role EXCEPT ![s] = "follower"]
    /\ granted' = [granted EXCEPT ![s] = {}]
    /\ used' = [used EXCEPT !.restarts = @ + 1]
    /\ UNCHANGED <<vote, elected, log, match, copy, minted, sid, vmark, lmark, rcopy,
                   rseq, cseq, nxt, bak, commits>>

\* The gate: a root whose recorded incarnation is behind the control
\* plane's, or whose sequence is below the witness's copy or the control
\* plane's, gets a new storage id, and every directory is judged against it.
\* Until the restore the witness holds what the root holds. Unless
\* RestoreYoung, a mark interval has passed since the last append; unless
\* RestoreUnheard, a root with no witness goes back only after a heartbeat.
RootRestore(s) ==
    /\ MayFail(s, "root")
    /\ bak # NoBackup
    /\ bak.who = s
    /\ RootRestoreSameRun \/ bak.inc < reg[s]
    /\ RestoreYoung \/ lmark[s] = LastId(log[s])
    /\ ~WitnessLost \/ RestoreUnheard \/ cseq[s] = rseq[s]
    /\ LET behind == \/ bak.root.seq < cseq[s]
                     \/ ~WitnessLost /\ bak.root.seq < rseq[s]
           caught == ~NoRestoreCheck /\ (bak.inc < reg[s] \/ (~NoRootSequence /\ behind))
       IN Open(s, bak.dir, [bak.root EXCEPT !.sid = IF caught THEN nxt[s].sid ELSE @])
    /\ reg' = [reg EXCEPT ![s] = @ + 1]
    /\ UNCHANGED bak

\* One directory goes back under a running root. Unless RestoreYoung, a
\* mark interval has passed since the last append, so the batched mark has
\* caught up with the log.
DirRestore(s) ==
    /\ MayFail(s, "dir")
    /\ bak # NoBackup
    /\ bak.who = s
    /\ RestoreYoung \/ lmark[s] = LastId(log[s])
    /\ Open(s, bak.dir, Root(s))
    /\ UNCHANGED <<reg, bak>>

Next ==
    \/ \E s \in Servers :
        \/ Campaign(s)
        \/ BecomeLeader(s)
        \/ AppendRecord(s)
        \/ Commit(s)
        \/ Finalize(s)
        \/ SyncVoteMark(s)
        \/ MarkLog(s)
        \/ Heartbeat(s)
        \/ Wipe(s)
        \/ LoseRoot(s)
        \/ RelaxedCrash(s)
        \/ Backup(s)
        \/ Restart(s)
        \/ RootRestore(s)
        \/ DirRestore(s)
    \/ \E s, t \in Servers :
        \/ GrantVote(s, t)
        \/ AcceptLeader(s, t)
        \/ Replicate(s, t)
        \/ ProposeJoint(s, t)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

Votes == [term : 0..MaxTerm, node : Members \cup {NoMember}, committed : BOOLEAN]

TypeOK ==
    /\ role \in [Servers -> {"follower", "candidate", "leader"}]
    /\ vote \in [Servers -> Votes]
    /\ vmark \in [Servers -> Votes]
    /\ granted \in [Servers -> SUBSET Members]
    /\ elected \subseteq (1..MaxTerm) \X Members
    /\ \A s \in Servers : \A k \in 1..Len(log[s]) :
        /\ log[s][k].term \in 1..MaxTerm
        /\ log[s][k].kind \in {"blank", "record", "member"}
        /\ log[s][k].val \in 0..MaxWrites
        /\ log[s][k].cfg.old \subseteq Members
        /\ log[s][k].cfg.new \subseteq Members
    /\ match \in [Servers -> [Members -> Nat]]
    /\ copy \in [Servers -> 0..MaxFaults]
    /\ rcopy \in [Servers -> 0..MaxFaults]
    /\ minted \in [Servers -> Nat]
    /\ sid \in [Servers -> Nat]
    /\ rseq \in [Servers -> Nat]
    /\ cseq \in [Servers -> Nat]
    /\ reg \in [Servers -> Nat]
    /\ bak.who \in Servers \cup {Nobody}
    /\ used.faulty \in Servers \cup {Nobody}

OneLeaderPerTerm == \A a, b \in elected : a[1] = b[1] => a[2] = b[2]

LeaderCompleteness ==
    \A l \in Servers : role[l] = "leader" =>
        \A c \in commits : c.by < vote[l].term =>
            Len(log[l]) >= c.idx /\ log[l][c.idx] = c.e

StateMachineSafety == \A c, d \in commits : c.idx = d.idx => c.e = d.e

Acked == {c.e.val : c \in {x \in commits : x.e.kind = "record"}}
Holds(s, v) == \E k \in 1..Len(log[s]) : log[s][k].kind = "record" /\ log[s][k].val = v

\* A candidate needs a quorum of its own membership, and only an id some
\* directory still holds can answer it.
AckedSurvive ==
    \A v \in Acked : \A s \in Servers : IsVoter(s) =>
        ~\E Q \in SUBSET {Me(t) : t \in Servers} :
            /\ QuorumIn(Cfg(s), Q)
            /\ \A m \in Q : ~Holds(m[1], v)

\* The harm itself: a leader elected without a record acknowledged in an
\* earlier term.
AckedHeldByLeader ==
    \A l \in Servers : role[l] = "leader" =>
        \A c \in commits : (c.e.kind = "record" /\ c.by < vote[l].term) => Holds(l, c.e.val)

----------------------------------------------------------------------------
\* Witnesses. Each says something never happens and must be violated.

\* A rotated copy is swapped in by a committed membership change.
NeverRejoins ==
    ~\E c \in commits : \E s \in Servers :
        /\ c.e.kind = "member"
        /\ c.e.cfg.old = c.e.cfg.new
        /\ copy[s] > 0
        /\ Me(s) \in c.e.cfg.new

\* A rotated copy, a voter again, wins an election.
NeverRotatedLeads == \A e \in elected : e[2][2] = 0

\* A copy is rotated. Under a config whose only fault is a restore, this is
\* a restore that was caught.
NeverRotates == \A s \in Servers : copy[s] = 0

=============================================================================
