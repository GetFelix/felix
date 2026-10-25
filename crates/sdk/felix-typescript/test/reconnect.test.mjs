// The reconnect options are checked before anything is sent, so these need the
// addon but no broker.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const { Client, InvalidArgumentError } = require("../index.js");

const connect = (options) => Client.connect("127.0.0.1:1", "t", { token: "x", ...options });

test("reconnectAttempts must be a whole number of at least 1", async () => {
  for (const reconnectAttempts of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    await assert.rejects(connect({ reconnectAttempts }), (err) => {
      assert.ok(err instanceof InvalidArgumentError, `${reconnectAttempts}: ${err}`);
      assert.match(err.message, /reconnectAttempts/);
      return true;
    });
  }
});

test("reconnectDeadline must be a positive number of milliseconds", async () => {
  for (const reconnectDeadline of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    await assert.rejects(connect({ reconnectDeadline }), (err) => {
      assert.ok(err instanceof InvalidArgumentError, `${reconnectDeadline}: ${err}`);
      assert.match(err.message, /reconnectDeadline/);
      return true;
    });
  }
});
