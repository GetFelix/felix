// A real cluster for the tests to talk to, and the conformance ledger.
//
// Deliberately not a mock. The claim this binding makes is that Node gets the
// *same* client behaviour Rust does — reconnection, redirect-following, offset
// accounting — and a mock would only validate the mock. So these tests start
// the real thing: `felix-cluster client-fixture` runs a control plane and
// brokers, registers what the scenarios need, and writes a JSON file saying
// how to reach them.
//
// Every test that covers a catalogued scenario names it. What each one did is
// recorded and written out as a results document, which
// `felix-conformance verify` then checks against the catalogue. A test that
// fails, errors, or never runs becomes a non-passing outcome — so the claim
// "TypeScript is conformant" is a thing the suite earns rather than asserts.

import { spawn } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync, mkdtempSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";

// `require` is not defined in an ES module; this is the sanctioned way to get
// one, and loading a native addon is exactly what it is for.
const require = createRequire(import.meta.url);

const HERE = dirname(fileURLToPath(import.meta.url));
export const CRATE_ROOT = join(HERE, "..");
export const REPO_ROOT = join(CRATE_ROOT, "..", "..", "..");

const FIXTURE_WAIT_MS = 120_000;
const RESULTS_ENV = "FELIX_CONFORMANCE_RESULTS";

/**
 * The package's own wrapper, not the raw addon.
 *
 * It is what types the errors, and the error scenarios have to exercise the
 * layer users actually see. A plain `cargo build` is enough — `napi build` is
 * mostly a rename of the same cdylib — which is what keeps the suite runnable
 * without the napi CLI, and therefore without a registry.
 */
export function loadClient() {
  return require(join(CRATE_ROOT, "index.js"));
}

/** Binaries the fixture needs, built by `cargo build`. */
export function fixtureBinariesPresent() {
  return ["felix-broker", "felix-cluster"].every((binary) =>
    existsSync(join(REPO_ROOT, "target", "debug", binary)),
  );
}

/**
 * Run a conformance cluster, resolving to its fixture document.
 *
 * Returns a `stop()` alongside it: a fixture left running holds ports and a
 * control plane, and a suite that leaks one makes the next run fail in a way
 * that looks like a client bug.
 */
export async function startFixture() {
  const work = mkdtempSync(join(tmpdir(), "felix-ts-fixture-"));
  const fixturePath = join(work, "fixture.json");
  const caFile = join(work, "brokers.pem");

  const child = spawn(
    join(REPO_ROOT, "target", "debug", "felix-cluster"),
    ["client-fixture", "--out", fixturePath, "--ca-file", caFile],
    { cwd: REPO_ROOT, env: { ...process.env, RUST_LOG: "warn" }, stdio: ["ignore", "pipe", "pipe"] },
  );

  let output = "";
  child.stdout.on("data", (chunk) => (output += chunk));
  child.stderr.on("data", (chunk) => (output += chunk));

  let exited = false;
  child.on("exit", () => (exited = true));

  const deadline = Date.now() + FIXTURE_WAIT_MS;
  let body = null;
  while (Date.now() < deadline) {
    if (exited) throw new Error(`the fixture exited during startup:\n${output}`);
    if (existsSync(fixturePath) && existsSync(caFile)) {
      try {
        body = JSON.parse(readFileSync(fixturePath, "utf8"));
        break;
      } catch {
        await sleep(200); // being written right now
        continue;
      }
    }
    await sleep(200);
  }

  if (body === null) {
    child.kill("SIGTERM");
    throw new Error(`the fixture was not ready within ${FIXTURE_WAIT_MS}ms:\n${output}`);
  }

  const stop = async () => {
    if (exited) return;
    child.kill("SIGTERM");
    for (let i = 0; i < 100 && !exited; i++) await sleep(200);
    if (!exited) child.kill("SIGKILL");
  };

  return { fixture: body, stop, output: () => output };
}

/** A connected client for `fixture`, optionally against specific addresses. */
export async function connect(fixture, { addrs, token } = {}) {
  const { Client } = loadClient();
  return Client.connect(
    addrs ?? fixture.addrs,
    fixture.tenant_id,
    { token: token ?? fixture.token, caFile: fixture.ca_file },
  );
}

/** Run `body` with a client of its own, closed however it ends. */
export async function withClient(fixture, options, body) {
  const client = await connect(fixture, options);
  try {
    return await body(client);
  } finally {
    client.close();
  }
}

// ---------------------------------------------------------------------------
// Bounded reads
// ---------------------------------------------------------------------------

const TIMED_OUT = Symbol("timed out");

/** Race `promise` against a timer that is cancelled either way. */
async function within(promise, ms) {
  const controller = new AbortController();
  const timer = sleep(ms, TIMED_OUT, { signal: controller.signal, ref: false });
  // Aborting is how the timer is cancelled, not a failure to report.
  timer.catch(() => {});
  try {
    return await Promise.race([promise, timer]);
  } finally {
    controller.abort();
  }
}

/**
 * A bounded reader over a handle whose `method` resolves with the next item.
 *
 * An expired wait leaves the in-flight call *pending* and hands it back on the
 * next read rather than abandoning it. Abandoning would drop the item that
 * call is about to resolve with — the suite would then report a lost record
 * that the client had in fact delivered, which is the worst kind of false
 * failure to chase.
 */
export function reader(handle, method = "nextEvent") {
  let pending = null;
  return {
    /** The next item, or `null` on the end of the stream or on a timeout. */
    async next(timeoutMs = 10_000) {
      pending ??= handle[method]();
      const item = await within(pending, timeoutMs);
      if (item === TIMED_OUT) return null;
      pending = null;
      if (item !== null) assertTypedNulls(item);
      return item;
    },
  };
}

// The fields index.d.ts types `T | null`, by the object that carries them.
// napi leaves an absent `Option` field `undefined`, and a caller that follows
// the typings with `!== null` then misreads every item, so each one is checked.
const NULLABLE_FIELDS = {
  Event: ["offset"],
  ShardEvent: ["event", "lostError", "recovered", "shardMoved"],
  ShardMoved: ["resumeFrom", "nodeId", "addr"],
  CacheWatchItem: ["change", "laggedResumeFrom", "shardMoved"],
  CacheChange: ["value"],
};

function assertFieldsNotUndefined(object, kind) {
  for (const field of NULLABLE_FIELDS[kind]) {
    if (object[field] === undefined) {
      throw new Error(`${kind}.${field} is undefined; the typings say it is null when absent`);
    }
  }
}

/** Throws if a delivered item has `undefined` where the typings promise `null`. */
export function assertTypedNulls(item) {
  if ("shard" in item) {
    assertFieldsNotUndefined(item, "ShardEvent");
    if (item.event) assertFieldsNotUndefined(item.event, "Event");
    if (item.shardMoved) assertFieldsNotUndefined(item.shardMoved, "ShardMoved");
  } else if ("payload" in item) {
    assertFieldsNotUndefined(item, "Event");
  } else {
    assertFieldsNotUndefined(item, "CacheWatchItem");
    if (item.change) assertFieldsNotUndefined(item.change, "CacheChange");
    if (item.shardMoved) assertFieldsNotUndefined(item.shardMoved, "ShardMoved");
  }
}

/**
 * Up to `count` items, or fewer if the stream goes quiet.
 *
 * Single-shot: it builds its own reader, so draining the same handle twice
 * would abandon the read left in flight by the first call. Hold a `reader` and
 * call it repeatedly when a test needs more than one pass.
 */
export async function take(handle, count, timeoutMs = 10_000, method = "nextEvent") {
  const read = handle.next ? handle : reader(handle, method);
  const items = [];
  const deadline = Date.now() + timeoutMs;
  while (items.length < count) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    const item = await read.next(remaining);
    if (item === null || item === undefined) break;
    items.push(item);
  }
  return items;
}

/** A name unique to a test, so tests cannot collide on a shared fixture. */
export function unique(prefix) {
  return `${prefix}-${process.pid}-${(unique.counter = (unique.counter ?? 0) + 1)}`;
}

// ---------------------------------------------------------------------------
// The conformance ledger
// ---------------------------------------------------------------------------
//
// Recorded per scenario id rather than per test: several tests may cover one
// scenario, and the scenario passes only when all of them do. A scenario whose
// test errored during setup is not a pass either — the point of the ledger is
// that only a demonstrated semantic counts.

const outcomes = new Map();

function recordPass(id) {
  if (!outcomes.has(id)) outcomes.set(id, { id, status: "pass" });
}

function recordFail(id, detail) {
  // A failure overwrites a pass: a scenario covered by several tests holds
  // only if every one of them holds.
  outcomes.set(id, { id, status: "fail", detail: String(detail).slice(0, 300) });
}

function recordSkip(id, detail) {
  if (!outcomes.has(id) || outcomes.get(id).status === "pass") {
    outcomes.set(id, { id, status: "skip", detail: String(detail).slice(0, 300) });
  }
}

/**
 * Run `body` and record the scenarios it demonstrates.
 *
 * One test may demonstrate several at once — a resume proves both the resume
 * semantic and the reconnect one — and each is recorded separately.
 */
export async function scenario(ids, body) {
  const list = Array.isArray(ids) ? ids : [ids];
  try {
    await body();
    for (const id of list) recordPass(id);
  } catch (err) {
    for (const id of list) recordFail(id, err?.stack ?? err?.message ?? err);
    throw err;
  }
}

/** Record scenarios as skipped, with a reason. A skip without one is not reportable. */
export function skipScenarios(ids, detail) {
  for (const id of Array.isArray(ids) ? ids : [ids]) recordSkip(id, detail);
}

/** Write the results document `felix-conformance verify` reads. */
export function writeResults() {
  const destination = process.env[RESULTS_ENV];
  if (!destination) return null;
  let version = "unknown";
  try {
    version = JSON.parse(readFileSync(join(CRATE_ROOT, "package.json"), "utf8")).version;
  } catch {
    // Reporting must not depend on the package manifest being readable.
  }
  const document = {
    client: "felix-client (TypeScript)",
    version,
    outcomes: [...outcomes.values()].sort((a, b) => a.id.localeCompare(b.id)),
  };
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, `${JSON.stringify(document, null, 2)}\n`);
  return destination;
}

export { sleep };
