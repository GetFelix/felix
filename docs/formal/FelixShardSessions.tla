---------------------------- MODULE FelixShardSessions ----------------------------
(***************************************************************************)
(* FelixShardReads with the long-lived sessions on a shard: a subscriber   *)
(* reading the stream, and a consumer group committing its state. The      *)
(* question is the same as for a read: what keeps a leader that has been   *)
(* deposed without knowing it from handing out something wrong, once the   *)
(* lease is no longer what stops it?                                       *)
(*                                                                         *)
(* A subscriber takes records in offset order from any broker that         *)
(* believes it leads, resuming where it left off when it moves, which is   *)
(* what a client does when its session ends and it resumes from the next   *)
(* offset somewhere else. `DeliverTo` is how far it may read:              *)
(*                                                                         *)
(*   "mark" -- up to the broker's committed mark, as the broker delivers   *)
(*             on a `Quorum` shard (`CommitHold`, `committed_bound`).      *)
(*   "log"  -- up to the end of the broker's log.                          *)
(*   "none" -- no subscriber.                                              *)
(*                                                                         *)
(* No lease and no round on the subscriber's path. NoLostDelivery says     *)
(* every record delivered is at its offset in the log of whoever leads at  *)
(* the current generation, once it may serve: nothing handed out is taken  *)
(* back, and the offsets a subscriber saw keep meaning what they meant. A  *)
(* deposed leader goes on delivering, but only up to its own mark, which   *)
(* covers records a majority held at its generation, and those every      *)
(* later leader has. "log" hands out a record past the mark, and TLC finds *)
(* the successor holding something else at that offset.                   *)
(*                                                                         *)
(* A group commit (a poll's claim, an ack, a dead-letter change) is        *)
(* written to the coordinator's own log and then confirmed as a read is,   *)
(* by `ReadConfirm`: a round of fences at its generation, the lease, or    *)
(* nothing. Group state is acknowledged on the leader's durability, not    *)
(* on a majority, so a commit the successor never received is redelivery, *)
(* not loss; that is unchanged. What must not happen is a commit           *)
(* acknowledged by a coordinator after a newer one had opened: then two    *)
(* coordinators of one group are both taking commits. NoStaleGroupCommit   *)
(* says no commit is acknowledged at a generation older than one that had  *)
(* opened before the commit began. "round" holds it with drifting clocks   *)
(* and no margin; "none" and "lease" do not.                               *)
(*                                                                         *)
(* A leader that learns it was deposed (a refused ship or round) ends its  *)
(* sessions so their clients find the new leader. That is liveness, and    *)
(* not modelled: the invariants hold whether or not it ever learns.        *)
(***************************************************************************)
EXTENDS FelixShardReads

CONSTANTS
    DeliverTo,      \* "mark", "log" or "none"
    MaxCommits      \* how many group commits the run begins

ASSUME DeliverTo \in {"mark", "log", "none"}
\* Serving is the lease-free condition only under follower acks, which is
\* the only way the broker serves a shard's sessions without the lease.
ASSUME AckByFollowers

VARIABLES
    snext,       \* the next offset the subscriber asks for
    sdel,        \* what it was handed: a set of << offset, record >>
    committing,  \* whether each broker has a group commit in flight
    cgen,        \* the generation it took that commit at
    cvotes,      \* who has answered the commit's round
    ctop,        \* the newest generation that had opened when the commit began
    commits,     \* how many group commits have begun
    topOpen,     \* history: the newest generation that has opened so far
    staleGroup   \* history: a commit acknowledged after a newer coordinator opened

sessionVars == << snext, sdel, committing, cgen, cvotes, ctop, commits, staleGroup >>
sessAllVars == << allVars, sessionVars, topOpen >>


\* The newest generation a broker serves at right now. `Serving` under
\* follower acks: it believes it leads and has finished its fence.
OpenGen ==
    LET open == { bgen[c] : c \in { c \in Brokers : Serving(c) } }
    IN IF open = {} THEN 0 ELSE CHOOSE g \in open : \A h \in open : h <= g

SessionsInit ==
    /\ Init
    /\ ReadsInit
    /\ snext = 1
    /\ sdel = {}
    /\ committing = [b \in Brokers |-> FALSE]
    /\ cgen = [b \in Brokers |-> 0]
    /\ cvotes = [b \in Brokers |-> {}]
    /\ ctop = [b \in Brokers |-> 0]
    /\ commits = 0
    /\ topOpen = 1
    /\ staleGroup = FALSE

\* How far the subscriber may read on `b`.
Upto(b) == IF DeliverTo = "mark" THEN hwm[b] ELSE Len(log[b])

\* One record, at the next offset. A generation-start record counts: the
\* broker skips it when it hands records out, but it takes its offset, and
\* the offset is what a subscriber resumes from.
Deliver(b) ==
    /\ DeliverTo /= "none"
    /\ bgen[b] > 0
    /\ snext <= Upto(b)
    /\ sdel' = sdel \cup {<< snext, log[b][snext] >>}
    /\ snext' = snext + 1
    /\ UNCHANGED << committing, cgen, cvotes, ctop, commits, staleGroup >>

\* The commit is written to the coordinator's own log first, so it may only
\* be confirmed after: `ctop` is what had opened by then.
BeginCommit(b) ==
    /\ commits < MaxCommits
    /\ Serving(b) /\ ~committing[b]
    /\ committing' = [committing EXCEPT ![b] = TRUE]
    /\ cgen' = [cgen EXCEPT ![b] = bgen[b]]
    /\ cvotes' = [cvotes EXCEPT ![b] = {}]
    /\ ctop' = [ctop EXCEPT ![b] = topOpen]
    /\ commits' = commits + 1
    /\ UNCHANGED << snext, sdel, staleGroup >>

\* `f` answers the commit's round, as `ConfirmRead`: the same fence, at the
\* commit's generation, refused once `f` took a newer one.
ConfirmCommit(b, f) ==
    /\ ReadConfirm = "round"
    /\ committing[b] /\ f \notin cvotes[b]
    /\ promised[f] <= cgen[b]
    /\ promised' = [promised EXCEPT ![f] = cgen[b]]
    /\ cvotes' = [cvotes EXCEPT ![b] = @ \cup {f}]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    handoffVars, fencing, answered, confirmed, heard >>
    /\ UNCHANGED << snext, sdel, committing, cgen, ctop, commits, staleGroup >>

CommitConfirmed(b) ==
    CASE ReadConfirm = "round" -> Majority(cvotes[b])
      [] ReadConfirm = "lease" -> LeaseValid(b)
      [] OTHER -> TRUE

\* The acknowledgement, from a coordinator that still believes it leads at
\* the commit's generation.
EndCommit(b) ==
    /\ committing[b]
    /\ bgen[b] = cgen[b]
    /\ CommitConfirmed(b)
    /\ staleGroup' = (staleGroup \/ ctop[b] > cgen[b])
    /\ committing' = [committing EXCEPT ![b] = FALSE]
    /\ cvotes' = [cvotes EXCEPT ![b] = {}]
    /\ UNCHANGED << snext, sdel, cgen, ctop, commits >>

SessionsStep ==
    \/ ReadsNext /\ UNCHANGED sessionVars
    \/ \E b \in Brokers :
        \/ Deliver(b) /\ UNCHANGED allVars
        \/ BeginCommit(b) /\ UNCHANGED allVars
        \/ EndCommit(b) /\ UNCHANGED allVars
        \/ \E f \in Brokers : ConfirmCommit(b, f) /\ UNCHANGED readVars

\* `topOpen` follows every step, so a leader that opened and then stepped
\* down is still remembered.
SessionsNext ==
    /\ SessionsStep
    /\ topOpen' = Max(topOpen, OpenGen')

SessionsSpec == SessionsInit /\ [][SessionsNext]_sessAllVars

\* Everything delivered is at its offset in the log of the leader at the
\* current generation, once it may serve.
NoLostDelivery ==
    \A c \in Brokers : (bgen[c] = gen /\ ~fencing[c]) =>
        \A p \in sdel : p[1] <= Len(log[c]) /\ Same(log[c][p[1]], p[2])

\* No group commit is acknowledged by a coordinator a newer one had already
\* replaced.
NoStaleGroupCommit == ~staleGroup

SessionsTypeOK ==
    /\ ReadsTypeOK
    /\ snext \in Nat
    /\ commits \in 0..MaxCommits
    /\ \A b \in Brokers : cvotes[b] \subseteq Brokers

=============================================================================
