// Run by scenarios/token.mjs: connect with a token provider, publish, and
// leave. The parent checks the process exits by itself, so a provider that
// held the event loop open would show as a hang.
//
// argv: <fixture json> <close|leave|slow|abandon>
//   close    publish, then close the client
//   leave    publish, then let the script end with the client open
//   slow     the provider waits on a timer before answering, and the script
//            awaits the connect at top level
//   abandon  the script ends while the provider's fetch is still pending

import { readFileSync } from "node:fs";
import { loadClient } from "./harness.mjs";

const [fixturePath, mode] = process.argv.slice(2);
const fixture = JSON.parse(readFileSync(fixturePath, "utf8"));
const { Client } = loadClient();

let calls = 0;
const options = {
  caFile: fixture.ca_file,
  tokenProvider: async () => {
    calls += 1;
    if (mode === "slow" || mode === "abandon") await new Promise((resolve) => setTimeout(resolve, 400));
    return fixture.token;
  },
};

if (mode === "abandon") {
  // Nothing awaits this, so the script's top level finishes with the fetch
  // pending. Node must still exit 0, not hang and not report an unsettled
  // top-level await.
  Client.connect(fixture.addrs, fixture.tenant_id, options).then(
    (client) => client.close(),
    () => {},
  );
} else {
  const client = await Client.connect(fixture.addrs, fixture.tenant_id, options);
  await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("x"));
  if (mode === "close") client.close();
  console.log(`calls=${calls}`);
}
