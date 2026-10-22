- A freed publish-queue slot now admits one waiting publisher, in arrival order, instead of waking
  every publisher blocked on a full queue (#1406). With `pub_ingress_wait` on and thousands of
  publishes in flight, each pop used to wake all of them to contend for one slot.
