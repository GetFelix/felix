- An MQTT connection under `FELIX_MQTT_ANONYMOUS_TENANT` whose client
  presented a certificate now closes at that certificate's `notAfter`, and a
  packet it sends after that is refused, as on an authenticated connection. A
  certificate that has already ended or cannot be read is refused at
  `CONNECT`. A connection with no certificate is unchanged (#3192).
