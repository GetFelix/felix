// The token provider. Not catalogue scenarios: the catalogue has no token
// entries yet, so these only have to pass, not be recorded.
//
// The fixture mints hour-long tokens, so nothing here waits for an expiry. What
// is covered is everything that happens at the first authentication: the
// provider is called lazily and one at a time, a refused token is fetched
// again once, and every kind of failure reaches the caller with the app's own
// message. Expiry-driven refresh and keeping a still-valid token across a
// failed fetch need short-lived tokens from the fixture.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";

import { CRATE_ROOT, sleep, unique } from "../harness.mjs";

const REFUSED = "not-a-felix-token";

export default function register(ctx) {
  /** A provider that counts its calls and the most that ran at once. */
  function counting(answer, delayMs = 0) {
    const state = { calls: 0, running: 0, peak: 0 };
    const provider = async () => {
      state.calls += 1;
      state.running += 1;
      state.peak = Math.max(state.peak, state.running);
      try {
        if (delayMs) await sleep(delayMs);
        return await answer(state.calls);
      } finally {
        state.running -= 1;
      }
    };
    return { state, provider };
  }

  function connectWith(provider, token) {
    const { fixture } = ctx;
    return ctx.felix.Client.connect(
      fixture.addrs,
      fixture.tenant_id,
      token,
      undefined,
      fixture.ca_file,
      undefined,
      provider,
    );
  }

  async function failure(promise) {
    return promise.then(
      (client) => {
        client.close();
        return null;
      },
      (err) => err,
    );
  }

  it("a provider with a token in hand is never called while the client is idle", async () => {
    const { fixture } = ctx;
    const { state, provider } = counting(async () => fixture.token);
    const client = await connectWith(provider, fixture.token);
    try {
      await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("a"));
      const subscription = await client.subscribe(fixture.tenant_id, fixture.namespace, fixture.durable_stream);
      await subscription.close();
      await sleep(300);
      assert.equal(state.calls, 0);
    } finally {
      client.close();
    }
  });

  it("without a token the first authentication calls the provider once, and streams opened together share it", async () => {
    const { fixture } = ctx;
    const { state, provider } = counting(async () => fixture.token, 150);
    const client = await connectWith(provider);
    try {
      const name = unique("token-fanout");
      await Promise.all(
        Array.from({ length: 8 }, (_, i) =>
          client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from(`${name}-${i}`)),
        ),
      );
      assert.equal(state.peak, 1, "the provider ran concurrently");
      assert.equal(state.calls, 1, "streams opened together did not share one fetch");
    } finally {
      client.close();
    }
  });

  it("a refused token is fetched again once, and the second one is used", async () => {
    const { fixture } = ctx;
    const { state, provider } = counting(async (call) => (call === 1 ? REFUSED : fixture.token));
    const client = await connectWith(provider);
    try {
      await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("retried"));
      assert.equal(state.calls, 2);
    } finally {
      client.close();
    }
  });

  it("a token refused twice fails with an AuthError after one retry, not more", async () => {
    const { state, provider } = counting(async () => REFUSED);
    const caught = await failure(connectWith(provider));
    assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
    assert.equal(state.calls, 2);
  });

  it("a throw reaches the caller's AuthError with the app's message", async () => {
    const caught = await failure(
      connectWith(() => {
        throw new Error("vault is sealed");
      }),
    );
    assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
    assert.match(caught.message, /vault is sealed/);
  });

  it("a rejected promise reaches the caller with the app's message", async () => {
    const caught = await failure(connectWith(async () => Promise.reject(new Error("refresh answered 403"))));
    assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
    assert.match(caught.message, /refresh answered 403/);
  });

  it("a result that is not a string fails the call and says so", async () => {
    const caught = await failure(connectWith(async () => ({ felix_token: ctx.fixture.token })));
    assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
    assert.match(caught.message, /non-empty string/);
  });

  it("a tokenProvider that is not a function is an invalid argument", async () => {
    const caught = await failure(connectWith("a-token"));
    assert.ok(caught instanceof TypeError, `got ${caught}`);
    assert.match(caught.message, /tokenProvider must be a function/);
  });

  it("connect without a token or a provider is an invalid argument", async () => {
    const caught = await failure(connectWith(undefined, undefined));
    assert.ok(caught instanceof ctx.felix.InvalidArgumentError, `got ${caught}`);
  });

  it("withIdentity takes a provider too", async () => {
    const { fixture } = ctx;
    const { state, provider } = counting(async () => fixture.token);
    const identity = await ctx.client.withIdentity(fixture.tenant_id, null, provider);
    try {
      await identity.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("as me"));
      assert.equal(state.calls, 1);
    } finally {
      identity.close();
    }
  });

  for (const mode of ["close", "leave"]) {
    it(`the process exits by itself with a provider set (${mode})`, async () => {
      const path = join(tmpdir(), `felix-ts-token-fixture-${process.pid}-${mode}.json`);
      writeFileSync(path, JSON.stringify(ctx.fixture));
      const child = spawn(process.execPath, [join(CRATE_ROOT, "test", "token-exit-child.mjs"), path, mode], {
        stdio: ["ignore", "pipe", "inherit"],
      });
      let out = "";
      child.stdout.on("data", (chunk) => (out += chunk));
      const timer = setTimeout(() => child.kill("SIGKILL"), 30_000);
      const code = await new Promise((resolve) => child.on("exit", (c, signal) => resolve(signal ?? c)));
      clearTimeout(timer);
      assert.equal(code, 0, `the child did not exit cleanly (${code}); it printed ${out}`);
      assert.match(out, /calls=1/);
    });
  }
}
