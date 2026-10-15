---------------------------- MODULE FelixShardReads ----------------------------
(***************************************************************************)
(* FelixShard with `Quorum` reads, and the question a read has to answer:  *)
(* does this broker still lead, or has a newer leader acknowledged writes  *)
(* it never saw?                                                           *)
(*                                                                         *)
(* A read begins on a broker that believes it leads: it takes its value,   *)
(* the records in its log, and remembers which writes had been             *)
(* acknowledged by then. It is answered once its leadership is confirmed.  *)
(* `ReadConfirm` is how:                                                   *)
(*                                                                         *)
(*   "round"  -- read-index style. After the value is taken, the broker    *)
(*               sends the promotion fence at its own generation. A replica *)
(*               that has accepted no newer generation answers; the read   *)
(*               is answered once a majority has, the broker counting      *)
(*               itself only while its own log has taken no newer fence.   *)
(*               `ReadIndex` in crates/server/felix-replication/src/       *)
(*               leadership.rs.                                            *)
(*   "lease"  -- the broker's own lease, the opt-in fast path.             *)
(*   "none"   -- believing it leads is enough: the round skipped.          *)
(*                                                                         *)
(* NoStaleRead is linearizability for a read: every write acknowledged     *)
(* before the read began is in its answer. With follower acks and the      *)
(* promotion fence, "round" holds it under drifting clocks with no margin; *)
(* "none" and "lease" do not: TLC finds the deposed leader answering       *)
(* without a write its successor acknowledged.                             *)
(*                                                                         *)
(* The mark a read also waits for (the answer covers only records a        *)
(* majority holds) is not modelled: it is what keeps a read from           *)
(* returning a record a failover can take back, a different property.     *)
(***************************************************************************)
EXTENDS FelixShard

CONSTANTS
    ReadConfirm,    \* "round", "lease" or "none"
    MaxReads        \* how many reads the run begins

ASSUME ReadConfirm \in {"round", "lease", "none"}
\* The round is a fence, which only brokers that keep promises answer.
ASSUME ReadConfirm = "round" => Promises

VARIABLES
    reading,    \* whether each broker has a read in flight
    rgen,       \* the generation it serves that read at
    rseen,      \* the writes acknowledged before the read began
    rval,       \* the writes the read's value holds
    rvotes,     \* who has answered the read's round
    reads,      \* how many reads have begun
    staleRead   \* history: a read was answered without a write acknowledged before it began

readVars == << reading, rgen, rseen, rval, rvotes, reads, staleRead >>
allVars == << vars, readVars >>

ReadsInit ==
    /\ reading = [b \in Brokers |-> FALSE]
    /\ rgen = [b \in Brokers |-> 0]
    /\ rseen = [b \in Brokers |-> {}]
    /\ rval = [b \in Brokers |-> {}]
    /\ rvotes = [b \in Brokers |-> {}]
    /\ reads = 0
    /\ staleRead = FALSE

Ids(b) == { log[b][i].id : i \in 1..Len(log[b]) }

\* The read takes its value first, as the code reads the cache before it
\* waits: everything in the broker's log.
BeginRead(b) ==
    /\ reads < MaxReads
    /\ Serving(b) /\ ~reading[b]
    /\ reading' = [reading EXCEPT ![b] = TRUE]
    /\ rgen' = [rgen EXCEPT ![b] = bgen[b]]
    /\ rseen' = [rseen EXCEPT ![b] = acked]
    /\ rval' = [rval EXCEPT ![b] = Ids(b)]
    /\ rvotes' = [rvotes EXCEPT ![b] = {}]
    /\ reads' = reads + 1
    /\ UNCHANGED << vars, staleRead >>

\* `f` answers the round: it has accepted no generation newer than the
\* read's, and takes that one (`ReplicaHandler::fence`, which writes nothing
\* when it already had it). The broker answers for itself the same way, from
\* its own log's accepted generation. Only after the value was taken.
ConfirmRead(b, f) ==
    /\ ReadConfirm = "round"
    /\ reading[b] /\ f \notin rvotes[b]
    /\ MayPromise(f, b, rgen[b])
    /\ Promise(f, b, rgen[b])
    /\ rvotes' = [rvotes EXCEPT ![b] = @ \cup {f}]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, queued, pending, acked, writes, staleCommit,
                    handoffVars, fencing, answered, confirmed, opened, heard, counterVars >>
    /\ UNCHANGED << reading, rgen, rseen, rval, reads, staleRead >>

Confirmed(b) ==
    CASE ReadConfirm = "round" -> Majority(rvotes[b])
      [] ReadConfirm = "lease" -> LeaseValid(b)
      [] OTHER -> TRUE

\* The answer, from a broker that still believes it leads at the read's
\* generation.
EndRead(b) ==
    /\ reading[b]
    /\ bgen[b] = rgen[b]
    /\ Confirmed(b)
    /\ staleRead' = (staleRead \/ ~(rseen[b] \subseteq rval[b]))
    /\ reading' = [reading EXCEPT ![b] = FALSE]
    /\ rvotes' = [rvotes EXCEPT ![b] = {}]
    /\ UNCHANGED << vars, rgen, rseen, rval, reads >>

ReadsNext ==
    \/ Next /\ UNCHANGED readVars
    \/ \E b \in Brokers :
        \/ BeginRead(b)
        \/ EndRead(b)
        \/ \E f \in Brokers : ConfirmRead(b, f)

ReadsSpec == Init /\ ReadsInit /\ [][ReadsNext]_allVars

\* Every write acknowledged before a read began is in the read's answer.
NoStaleRead == ~staleRead

ReadsTypeOK ==
    /\ TypeOK
    /\ reads \in 0..MaxReads
    /\ \A b \in Brokers : rvotes[b] \subseteq Brokers

=============================================================================
