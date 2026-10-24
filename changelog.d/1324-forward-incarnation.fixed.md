- A forwarded publish or cache op now names the incarnation of the stream or
  cache its route was for, and the owner refuses one for another incarnation
  (#1324). Before, a forward routed by a deleted stream's assignment could land
  in a stream recreated at the same generation, which a control-plane restore
  makes possible. Sent only to brokers that offer `FORWARD_INCARNATION`.
