// The token provider: the `token.*` catalogue scenarios, plus what the binding
// adds (argument checks, the AbortSignal, process exit).
//
// The provider is called lazily and one at a time, a refused token is fetched
// again once, and every kind of failure reaches the caller with the app's own
// message. The scenarios about expiry take a short-lived token from the
// fixture's `POST /tokens` and wait for it.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it } from "node:test";

import { CRATE_ROOT, scenario, sleep, unique } from "../harness.mjs";

const REFUSED = "not-a-felix-token";

/** A token for the fixture tenant that lives `ttlSecs`, from the fixture. */
async function mint(fixture, ttlSecs, forged = false) {
  assert.ok(fixture.control_url, "this fixture has no control_url to mint tokens from");
  const response = await fetch(`${fixture.control_url}/tokens`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ttl_secs: ttlSecs, forged }),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`/tokens: ${response.status}: ${text}`);
  return JSON.parse(text).token;
}

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

  function connectWith(provider, token, addrs = ctx.fixture.addrs) {
    const { fixture } = ctx;
    return ctx.felix.Client.connect(
      addrs,
      fixture.tenant_id,
      { token, caFile: fixture.ca_file, tokenProvider: provider },
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

  it("a provider with a token in hand is never called while the client is idle", () =>
    scenario("token.lazy", async () => {
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
    }));

  it("without a token the first authentication calls the provider once, and streams opened together share it", () =>
    scenario(["token.lazy", "token.single_flight"], async () => {
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
    }));

  it("a refused token is fetched again once, and the second one is used", () =>
    scenario("token.refused_retry_once", async () => {
      const { fixture } = ctx;
      const { state, provider } = counting(async (call) => (call === 1 ? REFUSED : fixture.token));
      const client = await connectWith(provider);
      try {
        await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("retried"));
        assert.equal(state.calls, 2);
      } finally {
        client.close();
      }
    }));

  it("a token refused twice fails with an AuthError after one retry, not more", () =>
    scenario("token.refused_retry_once", async () => {
      const { state, provider } = counting(async () => REFUSED);
      // One broker: each broker's refusal is its own authentication with its own
      // retry, so a multi-broker connect would count every broker's pair.
      const caught = await failure(connectWith(provider, undefined, ctx.fixture.addrs.slice(0, 1)));
      assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
      assert.equal(state.calls, 2);
    }));

  it("a throw reaches the caller's AuthError with the app's message", () =>
    scenario("token.failure_message", async () => {
      const caught = await failure(
        connectWith(() => {
          throw new Error("vault is sealed");
        }),
      );
      assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
      assert.match(caught.message, /vault is sealed/);
    }));

  it("a rejected promise reaches the caller with the app's message", () =>
    scenario("token.failure_message", async () => {
      const caught = await failure(connectWith(async () => Promise.reject(new Error("refresh answered 403"))));
      assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
      assert.match(caught.message, /refresh answered 403/);
    }));

  it("a result that is not a string fails the call and says so", () =>
    scenario("token.failure_message", async () => {
      const caught = await failure(connectWith(async () => ({ felix_token: ctx.fixture.token })));
      assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
      assert.match(caught.message, /non-empty string/);
    }));

  it("a tokenProvider that is not a function is an invalid argument", async () => {
    const caught = await failure(connectWith("a-token"));
    assert.ok(caught instanceof TypeError, `got ${caught}`);
    assert.match(caught.message, /tokenProvider must be a function/);
  });

  it("connect without a token or a provider is an invalid argument", async () => {
    const caught = await failure(connectWith(undefined, undefined));
    assert.ok(caught instanceof ctx.felix.InvalidArgumentError, `got ${caught}`);
  });

  it("a provider that never answers fails the call with an AuthError after the fetch timeout", { timeout: 150_000 }, () =>
    scenario("token.timeout_joins", async () => {
      // Needs the client's 30 s fetch timeout (felix-client FETCH_TIMEOUT).
      let calls = 0;
      const never = () => {
        calls += 1;
        return new Promise(() => {});
      };
      const started = Date.now();
      const caught = await failure(connectWith(never));
      assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
      const took = Date.now() - started;
      assert.ok(took >= 25_000 && took < 60_000, `gave up after ${took} ms`);
      assert.equal(calls, 1, "the provider was called again while its first call was pending");
    }));

  it("a live-looking token the broker refuses is replaced once", () =>
    scenario("token.refused_retry_once", async () => {
      const { fixture } = ctx;
      const { state, provider } = counting(async () => fixture.token);
      // Signed with a key no broker trusts but not expired, so only the
      // broker's refusal can tell the client to fetch.
      const forged = await mint(fixture, 60, true);
      const client = await connectWith(provider, forged, fixture.addrs.slice(0, 1));
      try {
        await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("replaced"));
        assert.equal(state.calls, 1);
      } finally {
        client.close();
      }
    }));

  it("a stream opened after two thirds of a token's life gets a fresh one", { timeout: 60_000 }, () =>
    scenario("token.expiry_refresh", async () => {
      const { fixture } = ctx;
      const { state, provider } = counting(async () => fixture.token);
      const client = await connectWith(provider, await mint(fixture, 6));
      try {
        const open = async () => {
          const subscription = await client.subscribe(fixture.tenant_id, fixture.namespace, fixture.durable_stream);
          await subscription.close();
        };
        await open();
        assert.equal(state.calls, 0, "the token still had most of its life");
        await sleep(4500);
        await open();
        assert.equal(state.calls, 1);
        await client.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("after"));
        assert.equal(state.calls, 1, "the fresh token is held, so the next stream does not ask");
      } finally {
        client.close();
      }
    }));

  it("a failed refresh keeps the token that is still valid and is retried later", { timeout: 60_000 }, () =>
    scenario("token.keep_valid_on_failure", async () => {
      const { fixture } = ctx;
      let healthy = false;
      const calls = [];
      const provider = async () => {
        calls.push(Date.now());
        if (!healthy) throw new Error("idp is down");
        return fixture.token;
      };
      // Refresh is due at 6 s; the token expires at 9 s.
      const client = await connectWith(provider, await mint(fixture, 9));
      try {
        const open = async () => {
          const subscription = await client.subscribe(fixture.tenant_id, fixture.namespace, fixture.durable_stream);
          await subscription.close();
        };
        await sleep(6500);
        await open(); // still valid, so this works
        assert.equal(calls.length, 1);
        await open();
        assert.equal(calls.length, 1, "a failed refresh was retried on every open");
        // Past the retry delay and the token's expiry: asked again, and answered.
        healthy = true;
        await sleep(Math.max(0, calls[0] + 9500 - Date.now()));
        await open();
        assert.equal(calls.length, 2);
      } finally {
        client.close();
      }
    }));

  it("a provider past the timeout is joined by the retry, not called again", { timeout: 150_000 }, () =>
    scenario("token.timeout_joins", async () => {
      const { fixture } = ctx;
      // The held token expires at 6 s, so there is nothing to fall back on.
      // The provider answers 33 s after it is called. The wait on it fails at
      // 30 s, the cluster client retries the open (a fetch timeout carries no
      // broker code, so it is retried), and the retry joins the call still
      // running and gets its late answer at 33 s.
      const { state, provider } = counting(async () => fixture.token, 33_000);
      const client = await connectWith(provider, await mint(fixture, 6), fixture.addrs.slice(0, 1));
      try {
        const open = async () => {
          const subscription = await client.subscribe(fixture.tenant_id, fixture.namespace, fixture.durable_stream);
          await subscription.close();
        };
        await sleep(6500);
        const started = Date.now();
        await open();
        const took = Date.now() - started;
        assert.ok(took >= 30_000 && took < 60_000, `opened after ${took} ms`);
        assert.equal(state.calls, 1, "the retry started another call");
        assert.equal(state.peak, 1);
        await open();
        assert.equal(state.calls, 1, "the late answer was not held");
      } finally {
        client.close();
      }
    }));

  it("a provider that listens to its signal is aborted at the fetch timeout, and called once", { timeout: 150_000 }, async () => {
    // Needs the client's 30 s fetch timeout (felix-client FETCH_TIMEOUT).
    let calls = 0;
    let abortedAfter = null;
    const started = Date.now();
    const listening = (signal) => {
      calls += 1;
      return new Promise((_, reject) => {
        signal.addEventListener("abort", () => {
          abortedAfter = Date.now() - started;
          reject(new Error("fetch stopped by the client"));
        });
      });
    };
    const caught = await failure(connectWith(listening));
    assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
    // The abort is queued to the JS thread beside the rejection, so it can land
    // just after the connect settles.
    for (let i = 0; i < 100 && abortedAfter === null; i++) await sleep(50);
    assert.ok(abortedAfter !== null, "the signal was never aborted");
    assert.ok(abortedAfter >= 25_000 && abortedAfter < 60_000, `aborted after ${abortedAfter} ms`);
    assert.equal(calls, 1);
  });

  it("the app's message arrives unchanged, whatever the provider threw", () =>
    scenario("token.failure_message", async () => {
      const cases = [
        ["a thrown Error", () => { throw new Error("vault is sealed: code 7"); }, "vault is sealed: code 7"],
        ["a rejected Error", () => Promise.reject(new Error("refresh answered 403")), "refresh answered 403"],
        ["a thrown string", () => { throw "plain string reason"; }, "plain string reason"],
        ["a rejected string", () => Promise.reject("rejected reason"), "rejected reason"],
      ];
      for (const [label, provider, message] of cases) {
        const caught = await failure(connectWith(provider));
        assert.ok(caught instanceof ctx.felix.AuthError, `${label}: got ${caught}`);
        assert.ok(caught.message.includes(message), `${label}: ${caught.message}`);
      }
      for (const value of [undefined, 42, ""]) {
        const caught = await failure(connectWith(async () => value));
        assert.ok(caught instanceof ctx.felix.AuthError, `got ${caught}`);
        assert.match(caught.message, /tokenProvider must return a non-empty string/);
      }
    }));

  it("an unknown option or an extra argument is a TypeError, not dropped", async () => {
    const { fixture } = ctx;
    await assert.rejects(
      ctx.felix.Client.connect(fixture.addrs, fixture.tenant_id, { token: fixture.token, tokenprovider: () => "x" }),
      (err) => err instanceof TypeError && /unknown option tokenprovider/.test(err.message),
    );
    await assert.rejects(
      ctx.felix.Client.connect(fixture.addrs, fixture.tenant_id, { token: fixture.token }, "extra"),
      TypeError,
    );
    await assert.rejects(ctx.client.withIdentity(fixture.tenant_id, fixture.token), TypeError);
  });

  it("withIdentity takes a provider too", async () => {
    const { fixture } = ctx;
    const { state, provider } = counting(async () => fixture.token);
    const identity = await ctx.client.withIdentity(fixture.tenant_id, { tokenProvider: provider });
    try {
      await identity.publish(fixture.tenant_id, fixture.namespace, fixture.durable_stream, Buffer.from("as me"));
      assert.equal(state.calls, 1);
    } finally {
      identity.close();
    }
  });

  for (const mode of ["close", "leave", "slow", "abandon"]) {
    it(`the process exits by itself with a provider set (${mode})`, async () => {
      const path = join(tmpdir(), `felix-ts-token-fixture-${process.pid}-${mode}.json`);
      writeFileSync(path, JSON.stringify(ctx.fixture));
      const child = spawn(process.execPath, [join(CRATE_ROOT, "test", "token-exit-child.mjs"), path, mode], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      let out = "";
      child.stdout.on("data", (chunk) => (out += chunk));
      child.stderr.on("data", (chunk) => (out += chunk)); // exit 13 prints its warning here
      const timer = setTimeout(() => child.kill("SIGKILL"), 30_000);
      const code = await new Promise((resolve) => child.on("exit", (c, signal) => resolve(signal ?? c)));
      clearTimeout(timer);
      assert.equal(code, 0, `the child did not exit cleanly (${code}); it printed ${out}`);
      assert.doesNotMatch(out, /unsettled/);
      if (mode !== "abandon") assert.match(out, /calls=1/);
    });
  }
}
