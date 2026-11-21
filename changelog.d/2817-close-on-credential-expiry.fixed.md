- Streams and connections no longer outlive their credentials (#2817). Every
  stream a token opened ends at the token's `exp` (plus its 60 s leeway) with
  code `revoked`, as lease-token streams already did. A QUIC client
  connection closes at its client certificate's `notAfter` (close code `2`,
  `credential expired`), and streams on it end then too. Internal
  broker-to-broker connections close when either side's certificate expires,
  each up to 30 s early so they do not all drop together. Kafka connections
  close at the credential's end even when idle. A certificate chain is bounded
  by its leaf and the intermediates on its verified path, never the CA.
  Clients refresh their token or certificate and reconnect.
