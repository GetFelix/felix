// A client certificate: the fixture's mutual TLS broker serves a client that
// presents one and refuses one that does not. Not a catalogued scenario, since
// the broker is a stub that checks the TLS handshake and nothing else.

import assert from "node:assert/strict";
import { it } from "node:test";

import { connect } from "../harness.mjs";

export default function register(ctx) {
  const mtls = () => {
    const { fixture } = ctx;
    assert.ok(
      fixture.mtls_addr && fixture.client_cert_file && fixture.client_key_file,
      "the fixture file has no mtls_addr, client_cert_file or client_key_file",
    );
    return { addrs: [fixture.mtls_addr], reconnectAttempts: 1 };
  };

  it("a client with a certificate connects to a broker that requires one", async () => {
    const { fixture } = ctx;
    const client = await connect(fixture, {
      ...mtls(),
      clientCertFile: fixture.client_cert_file,
      clientKeyFile: fixture.client_key_file,
    });
    client.close();
  });

  it("a client without a certificate is refused by it", async () => {
    await assert.rejects(connect(ctx.fixture, mtls()), (err) => err instanceof ctx.felix.FelixError);
  });
}
