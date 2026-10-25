// The example in docs-site/src/content/docs/clients/typescript.md, kept here so
// `tsc --noEmit` checks it against index.d.ts. Keep the two in step.

import { Client } from "../index";

declare const oidcToken: string;
declare const caFile: string;

// <example>
const CONTROLPLANE = "https://controlplane:8443/v1/tenants/t1";

async function post(path: string, body: unknown, bearer?: string) {
  const res = await fetch(CONTROLPLANE + path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(20_000), // the client gives up at 30 s
  });
  if (!res.ok) throw new Error(`${path}: ${res.status}`); // 403: sign in again
  return res.json();
}

const exchanged = await post(
  "/token/exchange",
  { requested: ["stream.publish"], resources: ["stream:t1/default/*"] },
  oidcToken,
);

// The refresh token works once, so keep the newest. The client never runs
// this twice at the same time, so no lock is needed.
let refreshToken: string = exchanged.refresh_token;

const client = await Client.connect(
  "127.0.0.1:5000",
  "t1",
  exchanged.felix_token,
  "localhost",
  caFile,
  undefined,
  async () => {
    const minted = await post("/token/refresh", { refresh_token: refreshToken });
    refreshToken = minted.refresh_token;
    return minted.felix_token;
  },
);
// </example>

// A synchronous provider fits the type too, and so does a per-user handle.
const alice = await client.withIdentity("t1", null, () => "a-felix-token");
alice.close();
client.close();
