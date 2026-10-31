// Conditional head trims: hiding a durable shard's oldest records.
//
// A stale tail is an answer, not an error. The refusals that are errors are
// asserted by class: folding them into one would send a caller retrying a
// trim the broker will never take.

import assert from "node:assert/strict";
import { it } from "node:test";

import { connect, scenario, skipScenarios } from "../harness.mjs";

const STALE = 2n ** 64n - 1n;

export default function register(ctx) {
  /** The trim stream's next offset, learned from a stale trim's refusal. */
  async function tailOf(client) {
    const { tenant_id: tenant, namespace, trim_stream: stream } = ctx.fixture;
    const refused = await client.trimBefore(tenant, namespace, stream, 0, 0n, STALE);
    assert.equal(refused.trimmed, false);
    assert.equal(typeof refused.tail, "bigint");
    return refused.tail;
  }

  async function withTrimmer(body) {
    const { fixture } = ctx;
    if (!fixture.trimmer_token || !fixture.trim_stream) return false;
    const trimmer = await connect(fixture, { token: fixture.trimmer_token });
    try {
      await body(trimmer);
    } finally {
      trimmer.close();
    }
    return true;
  }

  it("a trim at the tail lands there, and a stale one is refused with the tail", () =>
    scenario(
      ["head_trim.lands_at_the_expected_offset", "head_trim.refused_with_the_tail"],
      async () => {
        const { fixture, felix } = ctx;
        const { tenant_id: tenant, namespace, trim_stream: stream } = fixture;
        const ran = await withTrimmer(async (trimmer) => {
          let tail = await tailOf(trimmer);
          const written = await trimmer.publishIfOnShard(
            tenant,
            namespace,
            stream,
            0,
            [Buffer.from("t-1"), Buffer.from("t-2"), Buffer.from("t-3")],
            tail,
          );
          assert.equal(written.written, true);
          tail += 3n;

          const trimmed = await trimmer.trimBefore(
            tenant, namespace, stream, 0, tail - 1n, tail, Buffer.from("who"),
          );
          assert.deepEqual(trimmed, { trimmed: true, offset: tail, oldest: tail - 1n, tail: null });

          // The trim took the offset the first one expected.
          const again = await trimmer.trimBefore(
            tenant, namespace, stream, 0, tail - 1n, tail, Buffer.from("who"),
          );
          assert.deepEqual(again, { trimmed: false, offset: null, oldest: null, tail: tail + 1n });

          // At or below the floor nothing is written, but the tail must still match.
          const noop = await trimmer.trimBefore(tenant, namespace, stream, 0, tail - 2n, tail + 1n);
          assert.deepEqual(noop, { trimmed: true, offset: null, oldest: tail - 1n, tail: null });

          await assert.rejects(
            trimmer.read(tenant, namespace, stream, 0, tail - 2n),
            (err) => err instanceof felix.CursorError,
          );
        });
        if (!ran) {
          skipScenarios(
            ["head_trim.lands_at_the_expected_offset", "head_trim.refused_with_the_tail"],
            "the fixture file has no trimmer_token or trim_stream",
          );
        }
      },
    ));

  it("a trim past its own record is a plain error", async () => {
    const { fixture, felix } = ctx;
    const { tenant_id: tenant, namespace, trim_stream: stream } = fixture;
    await withTrimmer(async (trimmer) => {
      const tail = await tailOf(trimmer);
      await assert.rejects(
        trimmer.trimBefore(tenant, namespace, stream, 0, tail + 5n, tail),
        (err) =>
          err.constructor === felix.FelixError &&
          err.code === "invalid_request" &&
          err.detail?.reason === "trim_past_tail",
      );
    });
  });

  it("a trim without the grant is an authorization failure", () =>
    scenario("head_trim.requires_permission", async () => {
      const { client, fixture, felix } = ctx;
      if (!fixture.trim_stream) {
        skipScenarios("head_trim.requires_permission", "the fixture file has no trim_stream");
        return;
      }
      await assert.rejects(
        client.trimBefore(fixture.tenant_id, fixture.namespace, fixture.trim_stream, 0, 0n, STALE),
        (err) => err instanceof felix.AuthError,
      );
    }));

  it("a broker that advertised no head trims is sent none", () =>
    scenario("head_trim.negotiated", async () => {
      const { fixture, felix } = ctx;
      if (!fixture.no_commit_addrs?.length || !fixture.control_url) {
        skipScenarios(
          "head_trim.negotiated",
          "the fixture file has no no_commit_addrs or control_url",
        );
        return;
      }
      const bare = await connect(fixture, { addrs: fixture.no_commit_addrs });
      try {
        await assert.rejects(
          bare.trimBefore(fixture.tenant_id, fixture.namespace, fixture.durable_stream, 0, 0n, 0n),
          (err) => err instanceof felix.TrimError,
        );
      } finally {
        bare.close();
      }
      const sent = await fetch(`${fixture.control_url}/no-commit/requests`);
      assert.equal(Number(await sent.text()), 0, "the broker was sent a trim");
    }));
}
