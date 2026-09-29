// The Felix client conformance suite for the Node binding.
//
// One file rather than one per area, because `node --test` gives each file its
// own process: separate files would each start their own three-node cluster,
// and the ledger would be written once per process with a fraction of the
// scenarios in each. So the areas are modules that register into this file's
// suite, sharing one fixture and one ledger.
//
// The ledger is the output that matters. `FELIX_CONFORMANCE_RESULTS` names a
// file to write it to, and `felix-conformance verify` reads that file and
// answers whether every required scenario passed. A green test run that quietly
// skipped a required scenario is not conformance, which is why the verdict
// comes from the catalogue rather than from this suite's exit code.

import { after, before, describe } from "node:test";

import {
  connect,
  fixtureBinariesPresent,
  loadClient,
  skipScenarios,
  startFixture,
  writeResults,
} from "./harness.mjs";

import registerPubsub from "./scenarios/pubsub.mjs";
import registerCache from "./scenarios/cache.mjs";
import registerErrors from "./scenarios/errors.mjs";
import registerQueues from "./scenarios/queues.mjs";
import registerSharded from "./scenarios/sharded.mjs";
import registerCluster from "./scenarios/cluster.mjs";
import registerRetry from "./scenarios/retry.mjs";
import registerFaults from "./scenarios/faults.mjs";
import registerCommit from "./scenarios/commit.mjs";

// Filled in by `before`, read by the tests at run time. The tests cannot close
// over a fixture that does not exist yet, so they close over this instead.
const ctx = { fixture: null, client: null, felix: null };

let running = null;

before(async () => {
  if (!fixtureBinariesPresent()) {
    throw new Error(
      "no felix-broker / felix-cluster binary — build them with " +
        "`cargo build -p felix-broker-service --bin felix-broker && " +
        "cargo build -p felix-cluster --bin felix-cluster`",
    );
  }
  ctx.felix = loadClient();
  running = await startFixture();
  ctx.fixture = running.fixture;
  // One client shared by the non-destructive tests, which is also the condition
  // the concurrency scenario is about.
  ctx.client = await connect(ctx.fixture);
}, { timeout: 180_000 });

after(async () => {
  ctx.client?.close();
  await running?.stop();
  const written = writeResults();
  if (written) console.log(`\nconformance results written to ${written}`);
}, { timeout: 60_000 });

describe("publish and subscribe", () => registerPubsub(ctx));
describe("cache, counters and watches", () => registerCache(ctx));
describe("errors and lifecycle", () => registerErrors(ctx));
describe("consumer groups", () => registerQueues(ctx));
describe("multi-shard streams", () => registerSharded(ctx));
describe("cluster semantics", () => registerCluster(ctx));
describe("retry classification", () => registerRetry(ctx));
describe("connection faults", () => registerFaults(ctx));
describe("atomic commits", () => registerCommit(ctx));

// Semantics this binding does not wrap. Recorded rather than left out, because
// `verify` distinguishes "not claimed" from "claimed and broken", and a reason
// is what makes the first one reportable.
skipScenarios(
  "retry.idempotent_producers_re_send_ambiguous_outcomes",
  "this binding does not wrap producerInit/publishIdempotent yet",
);
skipScenarios(
  "error.bad_offset_is_typed",
  "no way to trim the fixture's log from a client, so a discarded offset " +
    "cannot be produced to assert against",
);
