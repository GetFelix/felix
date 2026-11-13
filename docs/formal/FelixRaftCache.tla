---------------------------- MODULE FelixRaftCache ----------------------------
(***************************************************************************)
(* The state machine of a cache shard's Raft group, as the second design   *)
(* on issue 2238 has it: one log holds cache writes, deletes and counter   *)
(* adds, every member applies it to a projection, and each member takes    *)
(* its own snapshot of that projection and purges its log behind it.       *)
(* Nothing here is built.                                                  *)
(*                                                                         *)
(* Compaction is that local snapshot. The leader proposes no compaction    *)
(* entries: no copies of live records and no counter checkpoints. The      *)
(* first design's `CopyUnchecked` and `CheckpointUnchecked` twins, and its *)
(* `CompactionInvisible` check, are retired for that reason: the entries   *)
(* they guarded do not exist, and a snapshot writes nothing to the log.    *)
(*                                                                         *)
(* Raft itself is abstracted to what FelixRaft.tla already checks: one     *)
(* committed sequence (`clog`) that only grows, a leader that proposes     *)
(* entries (`pend`), and a new leader that holds a prefix of the old       *)
(* leader's uncommitted tail and appends a blank entry. FelixRaft.tla's    *)
(* snapshot configs check the log side of a snapshot, install and purge.   *)
(* This module checks the projection side:                                 *)
(*                                                                         *)
(*   - Apply is deterministic. It checks expiry against the entry's time,  *)
(*     stamped by the leader, never a member's clock.                      *)
(*   - The projection holds the log's time: the stamp of the last stamped  *)
(*     entry it applied. The snapshot is the projection, so it carries     *)
(*     that time, and a member that installed it knows the log's time      *)
(*     with none of the log below it.                                      *)
(*   - The leader stamps an entry no earlier than the last stamped entry   *)
(*     in its log, the uncommitted tail it inherited included, or its      *)
(*     snapshot's time when its log holds none. Blank and membership       *)
(*     entries carry no stamp and are skipped.                             *)
(*   - Every client write carries a request id. The projection keeps each  *)
(*     applied id with the answer it gave, so a retry after an unknown     *)
(*     outcome changes nothing and gets the first answer. The snapshot     *)
(*     holds that table.                                                   *)
(*   - Expiry is a delete the leader proposes, conditional on the version  *)
(*     it saw expire. A key written again meanwhile keeps its value.       *)
(*   - A read takes the read-index path: the leader waits until it has     *)
(*     applied its commit index. It evaluates expiry at its read time,     *)
(*     the later of its clock and the log's time. A key that time finds    *)
(*     expired but the log's time does not is answered only after the      *)
(*     leader has committed and applied the conditional delete. So an idle *)
(*     group never reads an expired key as present, and an "absent" is     *)
(*     always backed by the log, which a later leader with a slower clock  *)
(*     cannot contradict. Answering "absent" from the leader's clock alone *)
(*     is the ReadsLocalClock twin, and answering at the log's time alone  *)
(*     is the ReadsLogTime twin.                                           *)
(*   - A snapshot is the projection and the id of the last entry it        *)
(*     applied, taken together. Restart and install both resume from it.   *)
(*     Purge goes up to the snapshot and no further.                       *)
(*   - An expired record is absent to every write, as it is to a read:     *)
(*     a put-if-absent takes the key, a delete answers "absent" and a      *)
(*     conditional write finds no version. Each judges that at its own     *)
(*     entry's time. So a snapshot may leave out a record that had expired *)
(*     at the log's time (`SnapshotDropsExpired`, which the design does),  *)
(*     and no answer can tell a member that restored from it from one that *)
(*     replayed the log.                                                   *)
(*   - A put or a delete may name a version (`cond`). It applies only if   *)
(*     the key holds that version, unexpired; otherwise it is refused.     *)
(*   - With `Trim`, the table of applied ids is trimmed by entries. Each   *)
(*     client entry carries its producer's floor: the lowest sequence the  *)
(*     producer has no answer for yet. Applying it drops the answers below *)
(*     the floor (never the entry's own). A retry below the floor that is  *)
(*     no longer in the table answers "forgotten" and is not applied. A    *)
(*     retire entry, sent once every request is answered, drops the        *)
(*     producer's answers and raises its floor past its last sequence, so *)
(*     the floor stays behind as a tombstone. A copy of a request may be   *)
(*     proposed after its answer came: a retry held in a queue or on the   *)
(*     network. That is what makes the tombstone necessary: dropping the   *)
(*     floor too (`RetireDropsFloor`) applies such a copy again.           *)
(*                                                                         *)
(* One key and one counter. Seven requests: an add (id 1), a put that      *)
(* expires one time unit after its stamp (id 2), a put-if-absent (id 3),   *)
(* which is refused while the key holds an unexpired value, a delete (id   *)
(* 4), a put that never expires (id 5), and a put (id 6) and a delete (id  *)
(* 7) that name a version. The version they name is the one the leader     *)
(* held, expired or not, when the client first sent the request; a retry   *)
(* names the same one. A configuration picks which requests exist. A       *)
(* request whose answer has not come may be proposed again: that is a      *)
(* retry. All requests come from one producer, and a request's sequence    *)
(* is its id.                                                              *)
(*                                                                         *)
(* For the implementation:                                                 *)
(*                                                                         *)
(*   - A request id must name the client durably, across connections and   *)
(*     leaders. The wire's `request_id` on cache operations is a           *)
(*     per-connection `Option<u64>`; it needs a producer id and sequence,  *)
(*     as PublishIdempotent has, behind a negotiated flag.                 *)
(*   - The table of applied ids is trimmed only by entries (the floor on   *)
(*     each write, and retire), with the floor and the kept answers in the *)
(*     snapshot. A retired producer keeps its floor: a retry still in      *)
(*     flight can commit after the retire entry.                           *)
(*   - Apply and snapshot building never read a clock.                     *)
(*                                                                         *)
(* Checked:                                                                *)
(*                                                                         *)
(*   MembersAgree     -- two members that applied the same prefix hold the *)
(*                       same projection.                                  *)
(*   ProjectionIsFold -- every member holds the fold of the committed log  *)
(*                       up to what it applied. With SnapshotDropsExpired  *)
(*                       these two compare what a request can observe: a   *)
(*                       record expired at the log's time counts as gone.  *)
(*   SumExact         -- a counter counts each request once.               *)
(*   AppliedOnce      -- a request id changes the projection once.         *)
(*   AnswersKept      -- an answer a client got is the answer the first    *)
(*                       commit of its request gave.                       *)
(*   ExpiryRemovesItsVersion -- an expiry delete removes only the version  *)
(*                       it names, and that version had expired. So a      *)
(*                       write is never removed by the expiry of an older  *)
(*                       version.                                          *)
(*   StampsMonotone   -- stamps never go back along the log.               *)
(*   ReadsFresh       -- a read answers what the committed log said when   *)
(*                       the read began.                                   *)
(*   ReadsNotStale    -- a read never answers "present" for a key that     *)
(*                       had expired at the leader's read time.            *)
(*   OneClock         -- a read that found the key absent is never         *)
(*                       followed, with no write between, by a write that  *)
(*                       finds it present: a refused put-if-absent, a      *)
(*                       delete that removes a version, or a conditional   *)
(*                       write that matches one.                           *)
(*   CondExact        -- a conditional write that applied replaced the     *)
(*                       version it named, and that version was unexpired. *)
(*   LiveReadable     -- a live key's record is in the member's log above  *)
(*                       its purge point or in its snapshot.               *)
(*   ForgetsOnlyDone  -- an entry answers "forgotten" only for a request   *)
(*                       whose answer the producer already had.            *)
(*   RetriesKeepAnswers -- a later commit of a request that is not         *)
(*                       forgotten answers what its first commit did.      *)
(*                                                                         *)
(* Each knob removes one rule:                                             *)
(*                                                                         *)
(*   NotDeduped         -- these operations are applied again on a retry.  *)
(*   SnapshotDropsDedup -- the snapshot leaves out the applied request ids.*)
(*   SnapshotDropsTime  -- the snapshot leaves out the log's time, so a    *)
(*                         member that installed it stamps from its clock. *)
(*   SnapshotIdAhead    -- the snapshot's id is read after its state, so   *)
(*                         it can name an entry the state does not hold.   *)
(*   PurgePastSnapshot  -- purge goes up to what the member applied.       *)
(*   ApplyLocalClock    -- apply checks expiry on the member's own clock.  *)
(*   ReadsLocalClock    -- a read checks expiry on the leader's own clock  *)
(*                         and answers without the delete.                 *)
(*   ReadsLogTime       -- a read checks expiry at the log's time only.    *)
(*   StampNotMonotone   -- the leader stamps its own clock, even if it is  *)
(*                         behind the entry before.                        *)
(*   FloorFromCommitted -- the stamp floor ignores the uncommitted tail.   *)
(*   FloorCountsBlank   -- the stamp floor reads a blank entry's time.     *)
(*   ExpiryUnconditional -- the expiry delete removes whatever the key     *)
(*                         holds.                                          *)
(*   ReadNoApplyWait    -- a read answers from what the leader has applied *)
(*                         without waiting for its commit index.           *)
(*   SeesExpired        -- these operations (delete, putAbsent, putIf,     *)
(*                         deleteIf) treat an expired record as present.   *)
(*   SnapshotDropsFloor -- the snapshot keeps the answers and leaves out   *)
(*                         the producer's floor.                           *)
(*   RetireDropsFloor   -- retire forgets the producer, floor included.    *)
(*   RetireLocal        -- the leader forgets the producer itself, with no *)
(*                         entry.                                          *)
(*   FloorPastUnanswered -- the producer sends its own sequence as the     *)
(*                         floor, past requests it has no answer for.      *)
(*                                                                         *)
(* SnapshotDropsExpired is not a dropped rule. It picks which of two       *)
(* allowed snapshots a member builds: with it, the snapshot holds live     *)
(* records only; without it, the whole projection.                         *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Servers,
    Requests,           \* the request ids clients send, out of 1..7
    MaxProposals,       \* client entries proposed, retries included
    MaxExpiries,        \* expiry deletes proposed
    MaxClock,           \* every clock runs 0..MaxClock
    MaxLeaderChanges,
    MaxCrashes,
    MaxReads,
    MaxSnapshots,       \* snapshots built, across all members
    NotDeduped,         \* a set of operation names
    SnapshotDropsDedup,
    SnapshotDropsTime,
    SnapshotIdAhead,
    PurgePastSnapshot,
    ApplyLocalClock,
    ReadsLocalClock,
    ReadsLogTime,
    StampNotMonotone,
    FloorFromCommitted,
    FloorCountsBlank,
    ExpiryUnconditional,
    ReadNoApplyWait,
    SeesExpired,        \* a set of operation names
    SnapshotDropsExpired,
    Trim,               \* entries carry a floor; retire and late copies exist
    MaxRetires,
    SnapshotDropsFloor,
    RetireDropsFloor,
    RetireLocal,
    FloorPastUnanswered

VARIABLES
    clog,       \* the committed entries
    pend,       \* the leader's uncommitted entries
    ldr,
    clock,      \* each broker's wall clock
    applied,    \* per member: the last entry applied
    st,         \* per member: the projection
    snap,       \* per member: [idx, st], its stored snapshot
    purged,     \* per member: its log is purged up to here
    given,      \* answers that reached a client: [rid, ans]
    reads,      \* history: every read served
    used,
    hist

vars == <<clog, pend, ldr, clock, applied, st, snap, purged, given, reads, used, hist>>

Symm == Permutations(Servers)

Max2(a, b) == IF a >= b THEN a ELSE b
Min2(a, b) == IF a <= b THEN a ELSE b
SetMin(S) == CHOOSE m \in S : \A x \in S : m <= x

TopSeq == CHOOSE m \in Requests : \A r \in Requests : r <= m

Op(r) == CASE r = 1 -> "add" [] r = 2 -> "put" [] r = 3 -> "putAbsent"
           [] r = 4 -> "delete" [] r = 5 -> "put"
           [] r = 6 -> "putIf" [] r = 7 -> "deleteIf"

CondOps == {"putIf", "deleteIf"}

NoAns == <<>>

Forgotten == <<"forgotten", 0>>

\* `t` is the log's time; `ans` the answer each applied request got that
\* is still kept; `floor` the producer's floor.
Empty == [val |-> 0, ver |-> 0, exp |-> 0, sum |-> 0, t |-> 0,
          ans |-> [r \in Requests |-> NoAns], floor |-> 0]

Blank == [rid |-> 0, op |-> "blank", t |-> 0, exp |-> 0, ver |-> 0, cond |-> 0,
          floor |-> 0]

\* Raise the floor to f, dropping the answers below it.
Raise(s, f) ==
    IF f > s.floor
    THEN [s EXCEPT !.floor = f,
                   !.ans = [r \in Requests |-> IF r < f THEN NoAns ELSE s.ans[r]]]
    ELSE s

\* A client entry whose answer was trimmed: it answers "forgotten".
Forgot(s, e) ==
    /\ Trim
    /\ e.op \notin NotDeduped
    /\ s.ans[e.rid] = NoAns
    /\ e.rid < s.floor

\* Present and not expired at time t. exp 0 is "never".
Live(s, t) == s.val # 0 /\ (s.exp = 0 \/ t < s.exp)

\* What a request can observe of s: a record expired at the log's time is
\* gone, and an absent key has no version.
Obs(s) == IF Live(s, s.t) THEN s ELSE [s EXCEPT !.val = 0, !.ver = 0, !.exp = 0]

\* Whether a write of this kind finds the key present at time t.
Present(s, t, op) == IF op \in SeesExpired THEN s.val # 0 ELSE Live(s, t)

\* A client entry applied at index i, checking expiry at time t.
Write(s, e, i, t) ==
    LET here == Present(s, t, e.op)
        put == [s EXCEPT !.val = e.rid, !.ver = i, !.exp = e.exp, !.ans[e.rid] = <<"ok", i>>]
        del == [s EXCEPT !.val = 0, !.ver = i, !.exp = 0, !.ans[e.rid] = <<"ok", s.ver>>]
        conflict == [s EXCEPT !.ans[e.rid] = <<"conflict", IF here THEN s.ver ELSE 0>>]
    IN CASE e.op = "add" ->
            [s EXCEPT !.sum = @ + 1, !.ans[e.rid] = <<"ok", s.sum + 1>>]
      [] e.op = "put" -> put
      [] e.op = "putAbsent" ->
            IF here THEN [s EXCEPT !.ans[e.rid] = <<"refused", s.ver>>] ELSE put
      [] e.op = "delete" ->
            IF here THEN del ELSE [s EXCEPT !.ans[e.rid] = <<"absent", 0>>]
      [] e.op = "putIf" -> IF here /\ s.ver = e.cond THEN put ELSE conflict
      [] e.op = "deleteIf" -> IF here /\ s.ver = e.cond THEN del ELSE conflict

\* Apply entry e at index i, checking expiry at time t.
Step(s, e, i, t) ==
    IF e.op = "blank" THEN s
    ELSE LET s1 == [s EXCEPT !.t = e.t]
         IN IF e.op = "expire"
            THEN IF s.val # 0 /\ (ExpiryUnconditional \/ s.ver = e.ver)
                 THEN [s1 EXCEPT !.val = 0, !.ver = i, !.exp = 0]
                 ELSE s1
            ELSE IF e.op = "retire"
            THEN IF RetireDropsFloor THEN [s1 EXCEPT !.ans = Empty.ans, !.floor = 0]
                 ELSE Raise(s1, e.floor)
            ELSE IF e.op \notin NotDeduped /\ s.ans[e.rid] # NoAns THEN s1
            ELSE IF Forgot(s, e) THEN s1
            \* Never past the request itself, whose answer was just kept.
            ELSE Raise(Write(s1, e, i, t), Min2(e.floor, e.rid))

\* The fold of the committed log, always at the entries' own times.
RECURSIVE FoldTo(_)
FoldTo(n) == IF n = 0 THEN Empty ELSE Step(FoldTo(n - 1), clog[n], n, clog[n].t)

All == clog \o pend

\* The last stamp in the leader's log at or below k. Its log starts above
\* its purge point; with no stamped entry there, its snapshot has the time.
RECURSIVE LastStamp(_)
LastStamp(k) ==
    IF k <= purged[ldr] THEN snap[ldr].st.t
    ELSE IF All[k].op # "blank" \/ FloorCountsBlank THEN All[k].t
    ELSE LastStamp(k - 1)

Floor == LastStamp(IF FloorFromCommitted THEN Len(clog) ELSE Len(All))

\* The leader's time: its clock, held up to the log's time.
Now == Max2(clock[ldr], Floor)

Stamp == IF StampNotMonotone THEN clock[ldr] ELSE Now

Init ==
    /\ clog = <<>>
    /\ pend = <<>>
    /\ ldr \in Servers
    /\ clock = [s \in Servers |-> 0]
    /\ applied = [s \in Servers |-> 0]
    /\ st = [s \in Servers |-> Empty]
    /\ snap = [s \in Servers |-> [idx |-> 0, st |-> Empty]]
    /\ purged = [s \in Servers |-> 0]
    /\ given = {}
    /\ reads = {}
    /\ used = [props |-> 0, exps |-> 0, changes |-> 0, crashes |-> 0, reads |-> 0, snaps |-> 0,
               retires |-> 0]
    /\ hist = [installed |-> {}, readAfterInstall |-> FALSE, snapFloor |-> FALSE,
               cond |-> [r \in Requests |-> 0], floor |-> [r \in Requests |-> 0]]

----------------------------------------------------------------------------

\* The leader installed a snapshot, holds no stamped entry above it, and
\* the snapshot's time is ahead of its clock.
SnapFloor ==
    /\ ldr \in hist.installed
    /\ \A k \in (purged[ldr] + 1)..Len(All) : All[k].op = "blank"
    /\ snap[ldr].st.t > clock[ldr]

Given(r) == \E g \in given : g.rid = r

\* The floor the producer sends with a request: the lowest sequence it has
\* no answer for. With FloorPastUnanswered, the request's own sequence.
ClientFloor(r) ==
    IF ~Trim THEN 0
    ELSE IF FloorPastUnanswered THEN r
    ELSE SetMin({q \in Requests : ~Given(q)} \cup {r})

\* A client request, or a retry of one whose answer has not come. A
\* conditional write names the version the leader held when the client
\* first sent it (`hist.cond`), and a retry names the same one, with the
\* same floor. With Trim, a copy sent before the answer came may still be
\* proposed after it: a retry held in a queue or on the network.
Propose(r) ==
    LET c == IF Op(r) \notin CondOps THEN 0
             ELSE IF hist.cond[r] # 0 THEN hist.cond[r] ELSE st[ldr].ver
        f == IF hist.floor[r] # 0 THEN hist.floor[r] ELSE ClientFloor(r)
    IN
    /\ ~Given(r) \/ (Trim /\ hist.floor[r] # 0)
    /\ used.props < MaxProposals
    /\ Op(r) \in CondOps => (hist.cond[r] # 0 \/ st[ldr].val # 0)
    /\ pend' = Append(pend, [rid |-> r, op |-> Op(r), t |-> Stamp,
                             exp |-> IF r = 2 THEN Stamp + 1 ELSE 0, ver |-> 0,
                             cond |-> c, floor |-> f])
    /\ used' = [used EXCEPT !.props = @ + 1]
    /\ hist' = [hist EXCEPT !.snapFloor = @ \/ SnapFloor, !.cond[r] = c, !.floor[r] = f]
    /\ UNCHANGED <<clog, ldr, clock, applied, st, snap, purged, given, reads>>

\* The leader's expiry timer: the key it applied has expired at its time.
\* It proposes a delete of that version.
Expire ==
    LET s == st[ldr]
    IN /\ used.exps < MaxExpiries
       /\ s.val # 0 /\ s.exp # 0 /\ Now >= s.exp
       /\ pend' = Append(pend, [rid |-> 0, op |-> "expire", t |-> Stamp, exp |-> 0,
                                ver |-> s.ver, cond |-> 0, floor |-> 0])
       /\ used' = [used EXCEPT !.exps = @ + 1]
       /\ UNCHANGED <<clog, ldr, clock, applied, st, snap, purged, given, reads, hist>>

\* The producer is done: every request has its answer. It retires, so its
\* answers can go. With RetireLocal the leader drops them itself.
Retire ==
    /\ Trim
    /\ used.retires < MaxRetires
    /\ \A r \in Requests : Given(r)
    /\ IF RetireLocal
       THEN /\ st' = [st EXCEPT ![ldr] = [@ EXCEPT !.ans = Empty.ans, !.floor = 0]]
            /\ UNCHANGED pend
       ELSE /\ pend' = Append(pend, [rid |-> 0, op |-> "retire", t |-> Stamp, exp |-> 0,
                                    ver |-> 0, cond |-> 0, floor |-> TopSeq + 1])
            /\ UNCHANGED st
    /\ used' = [used EXCEPT !.retires = @ + 1]
    /\ UNCHANGED <<clog, ldr, clock, applied, snap, purged, given, reads, hist>>

\* The first pending entry commits.
Commit ==
    /\ pend # <<>>
    /\ clog' = Append(clog, Head(pend))
    /\ pend' = Tail(pend)
    /\ UNCHANGED <<ldr, clock, applied, st, snap, purged, given, reads, used, hist>>

\* The leader answers a request it has applied, from its table, or
\* "forgotten" when the table no longer holds it. A request that never gets
\* this step had its answer lost. `at` is the log's length then.
Answer(r) ==
    /\ ~Given(r)
    /\ \E i \in 1..applied[ldr] : clog[i].rid = r
    /\ given' = given \cup {[rid |-> r, at |-> Len(clog),
                             ans |-> IF st[ldr].ans[r] = NoAns THEN Forgotten
                                     ELSE st[ldr].ans[r]]}
    /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, snap, purged, reads, used, hist>>

\* A new leader. It holds every committed entry and the first k of the old
\* leader's uncommitted ones, and appends a blank.
ChangeLeader(n, k) ==
    /\ n # ldr
    /\ used.changes < MaxLeaderChanges
    /\ ldr' = n
    /\ pend' = Append(SubSeq(pend, 1, k), Blank)
    /\ used' = [used EXCEPT !.changes = @ + 1]
    /\ UNCHANGED <<clog, clock, applied, st, snap, purged, given, reads, hist>>

Tick(s) ==
    /\ clock[s] < MaxClock
    /\ clock' = [clock EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<clog, pend, ldr, applied, st, snap, purged, given, reads, used, hist>>

Apply(s) ==
    LET i == applied[s] + 1
    IN /\ i <= Len(clog)
       /\ st' = [st EXCEPT ![s] =
                   Step(@, clog[i], i, IF ApplyLocalClock THEN clock[s] ELSE clog[i].t)]
       /\ applied' = [applied EXCEPT ![s] = i]
       /\ UNCHANGED <<clog, pend, ldr, clock, snap, purged, given, reads, used, hist>>

\* The projection and the id of the last entry it applied, together. With
\* SnapshotDropsExpired it holds live records only, judged at the log's time.
BuildSnapshot(s) ==
    LET live == IF SnapshotDropsExpired THEN Obs(st[s]) ELSE st[s]
        dedup == IF SnapshotDropsDedup THEN [live EXCEPT !.ans = Empty.ans] ELSE live
        kept == IF SnapshotDropsFloor THEN [dedup EXCEPT !.floor = 0] ELSE dedup
    IN /\ applied[s] > snap[s].idx
       /\ used.snaps < MaxSnapshots
       /\ used' = [used EXCEPT !.snaps = @ + 1]
       /\ snap' = [snap EXCEPT ![s] =
                     [idx |-> IF SnapshotIdAhead THEN Len(clog) ELSE applied[s],
                      st |-> IF SnapshotDropsTime THEN [kept EXCEPT !.t = 0] ELSE kept]]
       /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, purged, given, reads, hist>>

\* Only behind the stored snapshot (FelixRaft.tla's PurgeBeforeSnapshot
\* checks the log side of the other order).
Purge(s) ==
    LET to == IF PurgePastSnapshot THEN applied[s] ELSE snap[s].idx
    IN /\ purged[s] < to
       /\ purged' = [purged EXCEPT ![s] = to]
       /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, snap, given, reads, used, hist>>

\* A restart resumes from the snapshot and applies the log after it again.
Crash(s) ==
    /\ used.crashes < MaxCrashes
    /\ st' = [st EXCEPT ![s] = snap[s].st]
    /\ applied' = [applied EXCEPT ![s] = snap[s].idx]
    /\ pend' = IF s = ldr THEN <<>> ELSE pend
    /\ used' = [used EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<clog, ldr, clock, snap, purged, given, reads, hist>>

\* A member behind the leader's purge takes the leader's snapshot.
Install(s) ==
    /\ s # ldr
    /\ purged[ldr] > applied[s]
    /\ snap[ldr].idx > applied[s]
    /\ st' = [st EXCEPT ![s] = snap[ldr].st]
    /\ applied' = [applied EXCEPT ![s] = snap[ldr].idx]
    /\ snap' = [snap EXCEPT ![s] = snap[ldr]]
    /\ purged' = [purged EXCEPT ![s] = snap[ldr].idx]
    /\ hist' = [hist EXCEPT !.installed = @ \cup {s}]
    /\ UNCHANGED <<clog, pend, ldr, clock, given, reads, used>>

\* A read-index read on the leader. With the rule on, a key that the read
\* time finds expired and the log's time does not is not answered: Expire
\* is enabled instead, and the read waits for that delete to apply.
Read ==
    LET c == Len(clog)
        a == applied[ldr]
        s == st[ldr]
    IN /\ used.reads < MaxReads
       /\ ReadNoApplyWait \/ a = c
       /\ ReadsLocalClock \/ ReadsLogTime \/ Live(s, s.t) = Live(s, Now)
       /\ reads' = reads \cup
            {[c |-> c, rt |-> Now,
              present |-> Live(s, IF ReadsLocalClock THEN clock[ldr]
                                  ELSE IF ReadsLogTime THEN s.t ELSE Now)]}
       /\ used' = [used EXCEPT !.reads = @ + 1]
       /\ hist' = [hist EXCEPT !.readAfterInstall = @ \/ ldr \in hist.installed]
       /\ UNCHANGED <<clog, pend, ldr, clock, applied, st, snap, purged, given>>

Next ==
    \/ \E r \in Requests : Propose(r) \/ Answer(r)
    \/ Expire
    \/ Retire
    \/ Commit
    \/ Read
    \/ \E s \in Servers :
        \/ \E k \in 0..Len(pend) : ChangeLeader(s, k)
        \/ Tick(s) \/ Apply(s) \/ BuildSnapshot(s)
        \/ Purge(s) \/ Crash(s) \/ Install(s)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ ldr \in Servers
    /\ \A s \in Servers :
        /\ applied[s] \in 0..Len(clog)
        /\ purged[s] <= snap[s].idx
    /\ \A g \in given : g.rid \in Requests /\ g.ans # NoAns
    /\ \A s \in Servers : st[s].floor \in 0..(TopSeq + 1)

\* A snapshot that drops expired records leaves members with different
\* bytes for the same state, so they are compared by what can be observed.
View(s) == IF SnapshotDropsExpired THEN Obs(s) ELSE s

MembersAgree ==
    \A s, t \in Servers : applied[s] = applied[t] => View(st[s]) = View(st[t])

ProjectionIsFold == \A s \in Servers : View(st[s]) = View(FoldTo(applied[s]))

SumExact ==
    \A s \in Servers :
        st[s].sum = Cardinality({clog[i].rid : i \in {j \in 1..applied[s] : clog[j].op = "add"}})

\* A second commit of a request id moves the log's time and nothing else.
AppliedOnce ==
    \A i, j \in 1..Len(clog) :
        (i < j /\ clog[i].rid # 0 /\ clog[i].rid = clog[j].rid)
        => FoldTo(j) = [FoldTo(j - 1) EXCEPT !.t = clog[j].t]

First(r) == CHOOSE i \in 1..Len(clog) : clog[i].rid = r /\ \A j \in 1..(i - 1) : clog[j].rid # r

AnswersKept == \A g \in given : g.ans = FoldTo(First(g.rid)).ans[g.rid]

\* An expiry delete that removed a value removed the version it named, and
\* that version had expired by the delete's time.
ExpiryRemovesItsVersion ==
    \A j \in 1..Len(clog) :
        (clog[j].op = "expire" /\ FoldTo(j - 1).val # 0 /\ FoldTo(j).val = 0)
        => /\ FoldTo(j - 1).ver = clog[j].ver
           /\ FoldTo(j - 1).exp # 0
           /\ clog[j].t >= FoldTo(j - 1).exp

StampsMonotone ==
    \A i, j \in 1..Len(clog) :
        (i < j /\ clog[i].op # "blank" /\ clog[j].op # "blank") => clog[i].t <= clog[j].t

ReadsFresh == \A r \in reads : r.present = Live(FoldTo(r.c), FoldTo(r.c).t)

ReadsNotStale == \A r \in reads : r.present => Live(FoldTo(r.c), r.rt)

\* Nothing wrote the key between index a and index b.
Untouched(a, b) == FoldTo(a).ver = FoldTo(b).ver

\* Entry j is the first commit of a write that found the key present.
FoundPresent(j) ==
    LET a == FoldTo(j).ans[clog[j].rid]
    IN /\ clog[j].op \in {"putAbsent", "delete", "putIf", "deleteIf"}
       /\ FoldTo(j - 1).ans[clog[j].rid] = NoAns
       /\ a[1] = (IF clog[j].op = "putAbsent" THEN "refused" ELSE "ok")

OneClock ==
    \A r \in reads : \A j \in 1..Len(clog) :
        (~r.present /\ j > r.c /\ Untouched(r.c, j - 1)) => ~FoundPresent(j)

CondExact ==
    \A j \in 1..Len(clog) :
        (clog[j].op \in CondOps /\ FoundPresent(j))
        => /\ FoldTo(j - 1).ver = clog[j].cond
           /\ Live(FoldTo(j - 1), clog[j].t)

\* A live key's record is above the member's purge point or in its snapshot.
LiveReadable ==
    \A s \in Servers :
        (IF SnapshotDropsExpired THEN Live(st[s], st[s].t) ELSE st[s].val # 0) =>
            \/ st[s].ver > purged[s]
            \/ snap[s].st.val # 0 /\ snap[s].st.ver = st[s].ver

\* Entry j is a client entry that answered "forgotten".
ForgotAt(j) == clog[j].rid # 0 /\ Forgot(FoldTo(j - 1), clog[j])

\* What entry j answered.
Reply(j) == IF ForgotAt(j) THEN Forgotten ELSE FoldTo(j).ans[clog[j].rid]

\* "Forgotten" only for a request whose answer reached the producer before
\* the entry committed.
ForgetsOnlyDone ==
    \A j \in 1..Len(clog) :
        ForgotAt(j) => \E g \in given : g.rid = clog[j].rid /\ g.at < j

RetriesKeepAnswers ==
    \A j \in 1..Len(clog) :
        (clog[j].rid # 0 /\ j # First(clog[j].rid) /\ ~ForgotAt(j))
        => Reply(j) = FoldTo(First(clog[j].rid)).ans[clog[j].rid]

\* Witnesses. Each says something never happens and must be violated.

\* A member that installed a snapshot leads and serves a read.
NeverReadAfterInstall == ~hist.readAfterInstall

\* A retried add is committed twice and counted once.
NeverRetryApplied ==
    ~ \E s \in Servers :
        Cardinality({i \in 1..applied[s] : clog[i].op = "add"}) > 1

\* A request committed twice is answered.
NeverRetryAnswered ==
    ~ \E g \in given : Cardinality({i \in 1..Len(clog) : clog[i].rid = g.rid}) > 1

\* A read finds the expired key absent and a later put-if-absent applies.
NeverExpiredThenTaken ==
    ~ \E r \in reads : \E j \in 1..Len(clog) :
        /\ ~r.present /\ j > r.c /\ clog[j].op = "putAbsent"
        /\ FoldTo(r.c).val = 2
        /\ FoldTo(j).val = 3

\* An expiry delete commits after a newer write and leaves it alone.
NeverExpirySkipsNewer ==
    ~ \E j \in 1..Len(clog) :
        /\ clog[j].op = "expire"
        /\ FoldTo(j - 1).val # 0
        /\ FoldTo(j - 1).ver # clog[j].ver

\* A read answers "absent" after an expiry delete removed the key.
NeverReadAfterExpiry ==
    ~ \E r \in reads : \E j \in 1..r.c :
        /\ ~r.present
        /\ clog[j].op = "expire" /\ FoldTo(j - 1).val # 0 /\ FoldTo(j).val = 0

\* A leader that installed a snapshot stamps from the snapshot's time.
NeverStampFromSnapshot == ~hist.snapFloor

\* A delete finds an expired record that no expiry delete removed, and
\* answers "absent".
NeverDeleteOfExpired ==
    ~ \E j \in 1..Len(clog) :
        /\ clog[j].op = "delete"
        /\ FoldTo(j - 1).val # 0
        /\ FoldTo(j - 1).ans[clog[j].rid] = NoAns
        /\ FoldTo(j).ans[clog[j].rid] = <<"absent", 0>>

\* A member restores a snapshot that left out an expired record, and a
\* member that replayed the log still holds it at the same index.
NeverDroppedAndKept ==
    ~ \E s, t \in Servers :
        applied[s] = applied[t] /\ st[s].val = 0 /\ st[t].val # 0

\* A conditional write applies.
NeverCondApplied ==
    ~ \E j \in 1..Len(clog) : clog[j].op \in CondOps /\ FoundPresent(j)

\* A conditional write is refused because a newer write replaced the
\* version it named.
NeverCondStale ==
    ~ \E j \in 1..Len(clog) :
        /\ clog[j].op \in CondOps
        /\ FoldTo(j - 1).ans[clog[j].rid] = NoAns
        /\ Live(FoldTo(j - 1), clog[j].t)
        /\ FoldTo(j - 1).ver # clog[j].cond
        /\ FoldTo(j).ans[clog[j].rid][1] = "conflict"

\* A retry below the floor answers "forgotten".
NeverForgotten == ~ \E j \in 1..Len(clog) : ForgotAt(j)

\* A retry commits after the floor passed another request, and keeps its
\* answer.
NeverRetryAboveFloor ==
    ~ \E j \in 1..Len(clog) :
        /\ clog[j].rid # 0 /\ j # First(clog[j].rid) /\ ~ForgotAt(j)
        /\ FoldTo(j - 1).floor > SetMin(Requests)

\* A copy of a request commits after the retire entry.
NeverRetryAfterRetire ==
    ~ \E i, j \in 1..Len(clog) : i < j /\ clog[i].op = "retire" /\ clog[j].rid # 0

\* A member installs a snapshot whose floor passed a request.
NeverInstalledFloor ==
    ~ \E s \in hist.installed : snap[s].st.floor > SetMin(Requests)

=============================================================================
