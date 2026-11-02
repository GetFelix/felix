- Creating many streams at once no longer makes the control plane move
  existing shard leaders. A new shard's leader now goes to a broker under the
  cluster's floor share before one under its ceiling, so a batch leaves the
  leader totals within one of even. Before, a batch could leave them two apart,
  and the balancer evened them by moving the first shard in key order, often
  the same busy cache each time, holding its writes for every fence.
