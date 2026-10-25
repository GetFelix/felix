// The wrapper around the app's `tokenProvider`. No addon and no cluster: what
// the native side relies on is that it always gets a promise that resolves with
// a non-empty string or rejects with an Error carrying the app's message.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const { fetchFrom, connectArgs, withIdentityArgs } = require("../token.js");

test("no provider is no fetch function", () => {
  assert.equal(fetchFrom(undefined), undefined);
  assert.equal(fetchFrom(null), undefined);
});

test("a provider that is not a function is refused up front", () => {
  assert.throws(() => fetchFrom("a-token"), TypeError);
});

test("a string, or a promise of one, comes back as a promise of it", async () => {
  assert.equal(await fetchFrom(() => "sync")(1), "sync");
  assert.equal(await fetchFrom(async () => "async")(1), "async");
});

test("a synchronous throw becomes a rejection with the same message", async () => {
  const fetch = fetchFrom(() => {
    throw new Error("vault is sealed");
  });
  await assert.rejects(fetch(1), { message: "vault is sealed" });
});

test("a rejection keeps its message, and a non-Error rejection gets one", async () => {
  await assert.rejects(fetchFrom(async () => Promise.reject(new Error("403")))(1), { message: "403" });
  await assert.rejects(fetchFrom(async () => Promise.reject("plain string"))(1), {
    message: "plain string",
  });
});

test("a result that is not a non-empty string is a failure", async () => {
  for (const [value, word] of [
    [undefined, "undefined"],
    [null, "object"],
    [42, "number"],
    [{ felix_token: "x" }, "object"],
    ["", "empty string"],
  ]) {
    await assert.rejects(fetchFrom(async () => value)(1), (err) => {
      assert.ok(err instanceof Error);
      assert.match(err.message, /non-empty string/);
      assert.match(err.message, new RegExp(word));
      return true;
    });
  }
});

test("connect options become the addon's positional arguments", async () => {
  const provider = () => "t";
  const args = connectArgs(["h:1", "t1", { token: "tok", serverName: "s", caFile: "ca", offerAlpn: true, tokenProvider: provider }]);
  assert.deepEqual(args.slice(0, 6), ["h:1", "t1", "tok", "s", "ca", true]);
  assert.equal(await args[6](1), "t");
  assert.deepEqual(connectArgs(["h:1", "t1"]), ["h:1", "t1", undefined, undefined, undefined, undefined, undefined]);
  assert.deepEqual(withIdentityArgs(["t1", { token: "tok" }]), ["t1", "tok", undefined]);
});

test("an unknown option, an extra argument or a non-object is a TypeError", () => {
  assert.throws(() => connectArgs(["h:1", "t1", { tokenprovider: () => "x" }]), /unknown option tokenprovider/);
  assert.throws(() => connectArgs(["h:1", "t1", {}, "extra"]), TypeError);
  assert.throws(() => connectArgs(["h:1", "t1", "tok"]), TypeError);
  assert.throws(() => withIdentityArgs(["t1", "tok"]), TypeError);
  assert.throws(() => withIdentityArgs(["t1", { serverName: "s" }]), /unknown option serverName/);
});

test("the provider gets an AbortSignal that the native abort call fires", async () => {
  let seen;
  const call = fetchFrom((signal) => {
    seen = signal;
    return new Promise((_, reject) => signal.addEventListener("abort", () => reject(new Error("aborted"))));
  });
  const pending = call(7, false);
  assert.equal(seen.aborted, false);
  assert.equal(call(7, true), undefined);
  assert.equal(seen.aborted, true);
  await assert.rejects(pending, { message: "aborted" });
});

test("aborting one fetch leaves another's signal alone, and an unknown or settled id is ignored", async () => {
  const signals = {};
  const call = fetchFrom((signal) => {
    const id = Object.keys(signals).length + 1;
    signals[id] = signal;
    return id === 1 ? "done" : new Promise(() => {});
  });
  await call(1, false);
  call(1, true); // already settled
  call(99, true); // never started
  assert.equal(signals[1].aborted, false);
  call(2, false);
  call(3, false);
  call(2, true);
  assert.equal(signals[2].aborted, true);
  assert.equal(signals[3].aborted, false);
});

test("an abort that arrives before its fetch starts still aborts that fetch's signal", async () => {
  const seen = [];
  const call = fetchFrom((signal) => {
    seen.push(signal);
    return "t";
  });
  assert.equal(call(5, true), undefined);
  await call(5, false);
  assert.equal(seen[0].aborted, true);
  await call(6, false); // the record was used up: a later fetch starts clean
  assert.equal(seen[1].aborted, false);
  call(5, true); // settled id: ignored, not kept
  call(7, false);
  assert.equal(seen[2].aborted, false);
});

test("a provider that ignores the signal still completes after an abort", async () => {
  let release;
  const call = fetchFrom(() => new Promise((resolve) => (release = resolve)));
  const pending = call(1, false);
  call(1, true);
  release("late-token");
  assert.equal(await pending, "late-token");
});
