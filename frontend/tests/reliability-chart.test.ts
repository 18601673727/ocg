import assert from "node:assert/strict";
import test from "node:test";
import type { decodeUsage, decodeUsageBuckets } from "../components/ocg/contracts/control-plane";
import { summarizeReliability } from "../components/ocg/dashboard/reliability-chart-data";

type Buckets = ReturnType<typeof decodeUsageBuckets>;
type Usage = ReturnType<typeof decodeUsage>;

test("reliability buckets zero-fill days and sum errors, retries, cache and latency", () => {
  const day = 86_400_000_000;
  const rows: Buckets["rows"] = [
    {
      bucket_us: 0, agent: "profile:1", provider: "llm_provider:1", model: "model-a",
      requests: 10, complete_requests: 7,
      input_tokens: 100, output_tokens: 50,
      cache_read_tokens: 30, cache_write_tokens: 5, reasoning_tokens: 0,
    },
    {
      bucket_us: 2 * day, agent: null, provider: "llm_provider:2", model: null,
      requests: 4, complete_requests: 4,
      input_tokens: 40, output_tokens: 20,
      cache_read_tokens: 0, cache_write_tokens: 8, reasoning_tokens: 2,
    },
  ];
  const retryRows: Buckets["retry_rows"] = [
    { bucket_us: 0, agent: "profile:1", provider: "llm_provider:1", model: "model-a", retries: 2 },
    { bucket_us: day, agent: null, provider: "llm_provider:2", model: null, retries: 1 },
  ];
  const observations: Usage["observations"] = [
    {
      id: "usage:1", job: "job:1", request: 1, observed_at_us: 1_000, model: "model-a", provider: "p",
      llm_profile: null, input_tokens: 10, output_tokens: 5, cache_read_tokens: null,
      cache_write_tokens: null, reasoning_tokens: null, ttfb_ms: 100, finished_in_ms: 500,
      finality: "final", eligibility: "eligible",
    },
    {
      id: "usage:2", job: "job:2", request: 1, observed_at_us: 2 * day + 1_000, model: null, provider: "p",
      llm_profile: null, input_tokens: null, output_tokens: null, cache_read_tokens: null,
      cache_write_tokens: null, reasoning_tokens: null, ttfb_ms: 300, finished_in_ms: null,
      finality: "cumulative", eligibility: null,
    },
  ];
  const summary = summarizeReliability(rows, retryRows, observations, 0, 3 * day, day);
  assert.equal(summary.buckets.length, 3);
  const [first, empty, last] = summary.buckets;
  assert.deepEqual([first.requests, first.errors, first.retries], [10, 3, 2]);
  assert.deepEqual([first.cacheHits, first.cacheMiss], [30, 5]);
  assert.deepEqual([first.ttfbCount, first.ttfbAvgMs], [1, 100]);
  assert.deepEqual([first.durationCount, first.durationAvgMs], [1, 500]);
  assert.deepEqual(
    [empty.requests, empty.errors, empty.retries, empty.cacheHits, empty.cacheMiss],
    [0, 0, 1, 0, 0],
  );
  assert.equal(empty.ttfbAvgMs, null);
  assert.deepEqual([last.requests, last.errors, last.retries], [4, 0, 0]);
  assert.deepEqual([last.cacheHits, last.cacheMiss], [0, 8]);
  assert.deepEqual([last.ttfbCount, last.ttfbAvgMs], [1, 300]);
  assert.equal(last.durationAvgMs, null);
  assert.deepEqual(
    [summary.totalRequests, summary.totalErrors, summary.totalRetries],
    [14, 3, 3],
  );
  assert.deepEqual([summary.totalCacheHits, summary.totalCacheMiss], [30, 13]);
  assert.deepEqual([summary.ttfbCount, summary.ttfbAvgMs], [2, 200]);
  assert.deepEqual([summary.durationCount, summary.durationAvgMs], [1, 500]);
});
