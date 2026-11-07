// The client certificate options are checked before anything is sent, so these
// need the addon but no broker.

import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const { Client, InvalidArgumentError } = require("../index.js");

const dir = mkdtempSync(join(tmpdir(), "felix-ts-cert-"));
const notPem = join(dir, "not-pem.pem");
const empty = join(dir, "empty.pem");
writeFileSync(notPem, "this is not a certificate\n");
writeFileSync(empty, "");

const connect = (options) => Client.connect("127.0.0.1:1", "t", { token: "x", ...options });

async function refuses(options, pattern) {
  await assert.rejects(connect(options), (err) => {
    assert.ok(err instanceof InvalidArgumentError, `${err}`);
    assert.match(err.message, pattern);
    return true;
  });
}

test("a certificate without its key, or a key without its certificate, is refused", async () => {
  await refuses({ clientCertFile: notPem }, /clientCertFile and clientKeyFile must be given together/);
  await refuses({ clientKeyFile: notPem }, /clientCertFile and clientKeyFile must be given together/);
});

test("a missing certificate file is named", async () => {
  const missing = join(dir, "missing.pem");
  await refuses({ clientCertFile: missing, clientKeyFile: notPem }, /missing\.pem/);
});

test("a certificate file with no certificate in it is named", async () => {
  await refuses({ clientCertFile: empty, clientKeyFile: notPem }, /empty\.pem/);
  await refuses({ clientCertFile: notPem, clientKeyFile: notPem }, /not-pem\.pem/);
});

test("an unknown option is still refused", async () => {
  await assert.rejects(connect({ clientCert: notPem }), TypeError);
});
