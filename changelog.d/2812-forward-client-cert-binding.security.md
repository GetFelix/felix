- With `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT=true`, the owner of a shard now
  checks the client-certificate binding on publishes and cache operations
  another broker forwards to it (#2812). Before, only the broker the client
  connected to checked it, so a forwarding broker that did not bind, or a peer
  that skipped the check, could use a token without its certificate. The
  forward now carries the client's certificate chain in a new `ClientCertified`
  envelope (internal kind 66, capability `CLIENT_CERTS`), and the owner
  verifies it against its own `FELIX_TLS_CLIENT_CA`. A binding owner refuses a
  forward without one, including every forward from a broker on an older
  release, so upgrade all brokers of a binding cluster before relying on
  forwarding.
