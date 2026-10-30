- A client given several broker addresses no longer waits out a silent one
  before trying the next (#1232). `Client::connect_any` and a
  `ClusterClient`'s connect and reconnect start each next address 250 ms after
  the one before it, or at once when that one fails. The first to connect wins
  and the rest are dropped. The Python, TypeScript and C bindings inherit this.
