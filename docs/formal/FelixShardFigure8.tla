---------------------------- MODULE FelixShardFigure8 ----------------------------
(***************************************************************************)
(* FelixShard started from a history two leaderships in, which is how far *)
(* Raft's Figure 8 needs to reach and further than the time-bounded       *)
(* configurations can: each promotion costs a lapsed lease, and three of  *)
(* them from the start did not finish.                                    *)
(***************************************************************************)
EXTENDS FelixShard
CONSTANTS a, b, c
\* Generation 1 (a) wrote x and shipped it nowhere; generation 2 (b) fenced
\* c, wrote y and shipped it nowhere; b's lease has lapsed and b has stepped
\* down, and its last report names a and c as holding everything
\* acknowledged (nothing). With `StartRecord` each leader wrote its start
\* record first, and shipped that nowhere either. Starting b stepped down
\* keeps it from shipping its log late, which Figure 8 does not need and
\* which multiplies the states a pass has to cover.
SeededInit ==
    /\ now = 0
    /\ clock = [m \in Brokers |-> 0]
    /\ gen = 2
    /\ leader = b
    /\ cpExpiry = 0
    /\ report = [holders |-> {a, c}, len |-> 0, drained |-> FALSE, gen |-> 2]
    /\ inflight = <<>>
    /\ bgen = [m \in Brokers |-> 0]
    /\ bexpiry = [m \in Brokers |-> 0]
    /\ hbOut = [m \in Brokers |-> FALSE]
    /\ hbAt = [m \in Brokers |-> 0]
    /\ log = IF StartRecord
             THEN a :> <<Start(1), Record(1, 1)>> @@ b :> <<Start(2), Record(2, 2)>> @@ c :> <<>>
             ELSE a :> <<Record(1, 1)>> @@ b :> <<Record(2, 2)>> @@ c :> <<>>
    /\ hwm = [m \in Brokers |-> 0]
    /\ halted = {}
    /\ queued = [m \in Brokers |-> 0]
    /\ pending = [m \in Brokers |-> 0]
    /\ acked = {}
    /\ writes = 2
    /\ staleCommit = FALSE
    /\ draining = FALSE
    /\ successor = b
    /\ stopped = [m \in Brokers |-> FALSE]
    /\ moves = 0
    /\ ver = 0
    /\ cpView = [p \in Planners |-> {}]
    /\ staged = {}
    /\ heard = [m \in Brokers |-> NoReport]
    /\ promised = (a :> 1 @@ b :> 2 @@ c :> 2)
    /\ fencing = [m \in Brokers |-> FALSE]
    /\ answered = (a :> {} @@ b :> {c} @@ c :> {})
    /\ confirmed = [m \in Brokers |-> [n \in Brokers |-> 0]]
\* The same history one leadership further, with no start records in it: the
\* fleet finalized `generation_start` only after c's promotion. c was
\* promoted at 3, fenced a and took x, acknowledged nothing, and is now
\* stopped for a move to a; its own lease has run out, and its drained
\* report names a as holding its log. The next leadership is the cut-over to
\* a, or c taking the shard back when the move is cancelled, and whichever
\* it is inherits x.
SeededCutOverInit ==
    /\ now = 0
    /\ clock = [m \in Brokers |-> 0]
    /\ gen = 3
    /\ leader = c
    /\ cpExpiry = L
    /\ report = [holders |-> {a}, len |-> 1, drained |-> TRUE, gen |-> 3]
    /\ inflight = <<>>
    /\ bgen = (a :> 0 @@ b :> 0 @@ c :> 3)
    /\ bexpiry = (a :> 0 @@ b :> 0 @@ c :> Eps)
    /\ hbOut = [m \in Brokers |-> FALSE]
    /\ hbAt = [m \in Brokers |-> 0]
    /\ log = (a :> <<Record(1, 1)>> @@ b :> <<Record(2, 2)>> @@ c :> <<Record(1, 1)>>)
    /\ hwm = [m \in Brokers |-> 0]
    /\ halted = {}
    /\ queued = [m \in Brokers |-> 0]
    /\ pending = [m \in Brokers |-> 0]
    /\ acked = {}
    /\ writes = 2
    /\ staleCommit = FALSE
    /\ draining = TRUE
    /\ successor = a
    /\ stopped = (a :> FALSE @@ b :> FALSE @@ c :> TRUE)
    /\ moves = MaxMoves
    /\ ver = 0
    /\ cpView = [p \in Planners |-> {}]
    /\ staged = {}
    /\ heard = [m \in Brokers |-> NoReport]
    /\ promised = (a :> 3 @@ b :> 2 @@ c :> 3)
    /\ fencing = [m \in Brokers |-> FALSE]
    /\ answered = (a :> {} @@ b :> {c} @@ c :> {a})
    /\ confirmed = [m \in Brokers |-> [n \in Brokers |-> 0]]
\* The code moves no mark while it fences: nothing ships until it opens.
NoAckWhileFencing == \A m \in Brokers : fencing[m] => hwm'[m] = hwm[m]
SeededSpec == SeededInit /\ [][Next /\ NoAckWhileFencing]_vars
SeededCutOverSpec == SeededCutOverInit /\ [][Next /\ NoAckWhileFencing]_vars
=============================================================================
