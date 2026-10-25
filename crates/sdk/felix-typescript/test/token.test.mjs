// The wrapper around the app's `tokenProvider`. No addon and no cluster: what
// the native side relies on is that it always gets a promise that resolves with
// a non-empty string or rejects with an Error carrying the app's message.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const { fetchFrom } = require("../token.js");

test("no provider is no fetch function", () => {
  assert.equal(fetchFrom(undefined), undefined);
  assert.equal(fetchFrom(null), undefined);
});

test("a provider that is not a function is refused up front", () => {
  assert.throws(() => fetchFrom("a-token"), TypeError);
});

test("a string, or a promise of one, comes back as a promise of it", async () => {
  assert.equal(await fetchFrom(() => "sync")(), "sync");
  assert.equal(await fetchFrom(async () => "async")(), "async");
});

test("a synchronous throw becomes a rejection with the same message", async () => {
  const fetch = fetchFrom(() => {
    throw new Error("vault is sealed");
  });
  await assert.rejects(fetch(), { message: "vault is sealed" });
});

test("a rejection keeps its message, and a non-Error rejection gets one", async () => {
  await assert.rejects(fetchFrom(async () => Promise.reject(new Error("403")))(), { message: "403" });
  await assert.rejects(fetchFrom(async () => Promise.reject("plain string"))(), {
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
    await assert.rejects(fetchFrom(async () => value)(), (err) => {
      assert.ok(err instanceof Error);
      assert.match(err.message, /non-empty string/);
      assert.match(err.message, new RegExp(word));
      return true;
    });
  }
});
