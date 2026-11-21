---------------------------- MODULE FelixRaftFirstForm ----------------------------
(* How a Raft shard's group forms for the first time, with placement able   *)
(* to move before it has (issue 2794). The control plane stores a target    *)
(* for the set it places. Each broker forms only from its own view of       *)
(* placement, which may be stale: the lowest broker of the set it sees asks *)
(* every member of that set, and initializes with that set when each one   *)
(* is blank and the set is the target it sees. A blank member votes for    *)
(* any candidate of term 1. A group's leader needs a majority of the set   *)
(* it initialized with.                                                    *)
(*                                                                         *)
(* A re-form starts a new group in the next epoch, with a first form of    *)
(* its own. Every copy, vote and leader below belongs to one epoch, and a  *)
(* leader of an old epoch, cut off before the re-form, still reports.      *)
(*                                                                         *)
(* Hold = TRUE is the fix: the control plane refuses to move a set whose    *)
(* target is stored until a leader of the group has reported. Hold = FALSE  *)
(* finds two leaders of two different sets in term 1. PerEpoch = TRUE keys  *)
(* the formed mark on the epoch, as the code does; FALSE lets a mark from   *)
(* an earlier epoch release the new epoch's first target.                   *)
(*                                                                         *)
(* After the mark, Move is limited to a one-member swap (OneSwap), and   *)
(* replication factor 1 does not move. Changing a formed group's set,     *)
(* with joins, wipes and chains of swaps, is FelixRaftFormedSet.tla. The   *)
(* configs here check the first form: one set per shard, incarnation and   *)
(* epoch while the hold applies.                                           *)
EXTENDS FiniteSets, Naturals

CONSTANTS Brokers, Sets, Hold, PerEpoch, MaxEpoch

ASSUME Sets \subseteq SUBSET Brokers
ASSUME MaxEpoch \in Nat

Epochs == 0..MaxEpoch

VARIABLES
    epoch,      \* the control plane's reform epoch
    placed,     \* the set the control plane places
    target,     \* the set its stored target names, or {} for none
    marked,     \* the epochs a leader of the group has reported from
    view,       \* each broker's last read of [set, target, epoch]
    init,       \* per broker and epoch, the set it initialized, or {}
    votedFor,   \* per broker and epoch, its term-1 vote, or 0 for none
    leader      \* the <<broker, epoch>> pairs that won term 1

vars == <<epoch, placed, target, marked, view, init, votedFor, leader>>

Min(S) == CHOOSE b \in S : \A c \in S : b <= c
Majority(S, T) == 2 * Cardinality(S \cap T) > Cardinality(S)
Blank(b, e) == init[b][e] = {} /\ votedFor[b][e] = 0

\* The control plane holds the current epoch's group formed.
Formed == IF PerEpoch THEN epoch \in marked ELSE marked # {}

\* A set change placement makes on a formed group: one member swapped.
OneSwap(s) == Cardinality(placed \ s) <= 1 /\ s \cap placed # {}

Init ==
    /\ epoch = 0
    /\ placed \in Sets
    /\ target = {}
    /\ marked = {}
    /\ view = [b \in Brokers |-> [set |-> placed, target |-> {}, epoch |-> 0]]
    /\ init = [b \in Brokers |-> [e \in Epochs |-> {}]]
    /\ votedFor = [b \in Brokers |-> [e \in Epochs |-> 0]]
    /\ leader = {}

\* Every member of the placed set has named its copy.
StoreTarget ==
    /\ target # placed
    /\ target' = placed
    /\ UNCHANGED <<epoch, placed, marked, view, init, votedFor, leader>>

\* Placement moves. A move drops the stored target: the new set needs its
\* own.
Move(s) ==
    /\ s # placed
    /\ Hold => (target = {} \/ Formed)
    /\ Formed => OneSwap(s)
    /\ placed' = s
    /\ target' = {}
    /\ UNCHANGED <<epoch, marked, view, init, votedFor, leader>>

\* A re-form: the next epoch's group starts blank on any set, and needs a
\* target of its own. The mark stays, as the code keeps it.
Reform(s) ==
    /\ epoch < MaxEpoch
    /\ epoch' = epoch + 1
    /\ placed' = s
    /\ target' = {}
    /\ UNCHANGED <<marked, view, init, votedFor, leader>>

\* A broker reads the feed. One that does not is cut off and keeps its view.
Read(b) ==
    /\ view' = [view EXCEPT ![b] = [set |-> placed, target |-> target, epoch |-> epoch]]
    /\ UNCHANGED <<epoch, placed, target, marked, init, votedFor, leader>>

\* The ask and the initialize, with the check against its own view.
Form(b) ==
    LET s == view[b].set
        e == view[b].epoch
    IN  /\ b \in s
        /\ b = Min(s)
        /\ view[b].target = s
        /\ \A m \in s : Blank(m, e)
        /\ init' = [init EXCEPT ![b][e] = s]
        /\ UNCHANGED <<epoch, placed, target, marked, view, votedFor, leader>>

\* A blank member's vote is not gated on any target.
Vote(v, c, e) ==
    /\ init[c][e] # {}
    /\ votedFor[v][e] = 0
    /\ v \in init[c][e]
    /\ votedFor' = [votedFor EXCEPT ![v][e] = c]
    /\ UNCHANGED <<epoch, placed, target, marked, view, init, leader>>

Win(c, e) ==
    /\ init[c][e] # {}
    /\ <<c, e>> \notin leader
    /\ Majority(init[c][e], {v \in Brokers : votedFor[v][e] = c})
    /\ leader' = leader \cup {<<c, e>>}
    /\ UNCHANGED <<epoch, placed, target, marked, view, init, votedFor>>

\* A leader reports a floor at its epoch. Only a broker the target names is
\* counted, and a report from another epoch marks nothing.
Report(c, e) ==
    /\ <<c, e>> \in leader
    /\ target # {}
    /\ c \in target
    /\ e = epoch
    /\ marked' = marked \cup {e}
    /\ UNCHANGED <<epoch, placed, target, view, init, votedFor, leader>>

Next ==
    \/ StoreTarget
    \/ \E s \in Sets : Move(s) \/ Reform(s)
    \/ \E b \in Brokers : Read(b) \/ Form(b)
    \/ \E b \in Brokers, e \in Epochs : Win(b, e) \/ Report(b, e)
    \/ \E v, c \in Brokers, e \in Epochs : Vote(v, c, e)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ epoch \in Epochs
    /\ placed \in Sets
    /\ target \in Sets \cup {{}}
    /\ marked \subseteq Epochs
    /\ leader \subseteq Brokers \X Epochs
    /\ votedFor \in [Brokers -> [Epochs -> Brokers \cup {0}]]

\* Two brokers lead term 1 of one epoch only if they initialized the same
\* set, which is one group: Raft's single vote per term keeps that to one
\* leader.
OneGroup ==
    \A a, b \in Brokers, e \in Epochs :
        <<a, e>> \in leader /\ <<b, e>> \in leader => init[a][e] = init[b][e]

\* In each epoch every formed member formed the same set: one group per
\* shard, stream incarnation and epoch.
OneFirstSet ==
    \A a, b \in Brokers, e \in Epochs :
        init[a][e] # {} /\ init[b][e] # {} => init[a][e] = init[b][e]
==================================================================================
