import assert from "node:assert/strict";
import test from "node:test";
import type { decodeUsageBuckets } from "../components/ocg/contracts/control-plane";
import { summarize } from "../components/ocg/dashboard/usage-chart";

test("usage buckets zero-fill days and rank models separately from Other", () => {
  const day = 86_400_000_000;
  const rows: ReturnType<typeof decodeUsageBuckets>["rows"] = [];
  for (let index = 0; index < 11; index++) {
    const model = index === 0 ? "Other" : index === 10 ? null : `model-${index}`;
    for (const bucket of [0, 2]) {
      rows.push({
        bucket_us: bucket * day, agent: bucket === 0 ? "profile:1" : "profile:2",
        provider: bucket === 0 ? "llm_provider:1" : "llm_provider:2", model,
        requests: 1, complete_requests: 1,
        input_tokens: bucket === 0 ? 11 - index : 0,
        output_tokens: bucket === 2 ? 11 - index : 0,
        cache_read_tokens: 100, cache_write_tokens: 100, reasoning_tokens: 100,
      });
    }
  }
  const summary = summarize(rows, 0, 3 * day, day);
  assert.deepEqual(summary.bars.map((bar) => [bar.bucketUs, bar.total]), [[0, 66], [day, 0], [2 * day, 66]]);
  assert.equal(summary.bars[1].totals.size, 0);
  assert.equal(summary.total, 132);
  assert.equal(summary.ranking.length, 10);
  assert.equal(summary.ranking.filter((entry) => !entry.other).length, 9);
  const namedOther = summary.ranking.find((entry) => !entry.other && entry.model === "Other");
  const remainder = summary.ranking.find((entry) => entry.other);
  assert.ok(namedOther);
  assert.ok(remainder);
  assert.equal(namedOther.total, 22);
  assert.equal(namedOther.unknown, false);
  assert.equal(remainder.total, 6);
  assert.deepEqual(remainder.keys, ["model-9", null]);
  assert.equal(remainder.unknown, false);
  assert.ok(Math.abs(summary.ranking.reduce((sum, entry) => sum + entry.share, 0) - 1) < 1e-9);
});
