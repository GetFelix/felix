- A Raft peer TLS reload now swaps the CA bundle, the server verifier and the
  member's certificate as one value, after checking them against each other
  (#2940). Before, a new-root-only bundle was stored first and put back if the
  leaf waiting beside it was refused, so for that moment the member trusted
  the new root while presenting the old leaf, and a connection a peer opened
  under the new root stayed up after the bundle was put back. A refused
  rotation now stores nothing, so no handshake sees material that was refused
  or not yet fully checked. A handshake in flight while a rotation is
  committed may still present the previous certificate and verify with the
  new CA, which costs at most that one handshake.
