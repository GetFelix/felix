- A first start of the metadata Raft group needs every configured member
  (#2371). Under `FELIX_RAFT_INITIAL_CLUSTER_STATE=new` an empty member
  formed the group when a majority of members answered empty and skipped
  the ones that did not answer, so two members that lost their volumes while
  the third was down formed a new group and the third's acknowledged writes
  were lost. Now every member must answer and be empty; an unreachable
  member, a 403 and an undecodable answer all block. For a machine lost
  before the group ever formed, `felix-controlplane raft form-without
  --data-dir <dir> --cluster-id <id> --slot <n>` lets the other members form
  without it, once.
- `FELIX_RAFT_PEERS` is refused when two ids share an address (#2371). One
  machine would answer for both, and a first start could form without asking
  the member the second id stands for.
