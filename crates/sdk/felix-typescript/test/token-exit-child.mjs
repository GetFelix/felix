// Run by scenarios/token.mjs: connect with a token provider, publish, and
// leave. The parent checks the process exits by itself, so a provider that
// held the event loop open would show as a hang.
//
// argv: <fixture json> <close|leave>

import { readFileSync } from "node:fs";
import { loadClient } from "./harness.mjs";

const [fixturePath, mode] = process.argv.slice(2);
const fixture = JSON.parse(readFileSync(fixturePath, "utf8"));
const { Client } = loadClient();

let calls = 0;
const client = await Client.connect(
  fixture.addrs,
  fixture.tenant_id,
  undefined,
  undefined,
  fixture.ca_file,
  undefined,
  async () => {
    calls += 1;
    return fixture.token;
  },
);
await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("x"));
if (mode === "close") client.close();
console.log(`calls=${calls}`);
