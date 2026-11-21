- Streams and connections no longer outlive their credentials (#2817). Every
  stream a token opened ends at the token's `exp` (plus its 60 s leeway) with
  code `revoked`, as lease-token streams already did. A QUIC client
  connection closes at its client certificate's `notAfter` (close code `2`,
  `credential expired`), and streams on it end then too. Internal
  broker-to-broker connections close when either side's certificate expires.
  Clients refresh their token or certificate and reconnect.
