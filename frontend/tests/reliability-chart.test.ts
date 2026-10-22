import assert from "node:assert/strict";
import test from "node:test";
import type { decodeAvailability, decodeUsage, decodeUsageBuckets, decodeUsageRetries } from "../components/ocg/contracts/control-plane";
import { summarizeReliability } from "../components/ocg/dashboard/reliability-chart-data";

type Buckets = ReturnType<typeof decodeUsageBuckets>;
type Usage = ReturnType<typeof decodeUsage>;

test("reliability buckets zero-fill days and sum errors, retries, cache and latency", () => {
  const day = 86_400_000_000;
  const rows: Buckets["rows"] = [
    {
      bucket_us: 0, agent: "00000001-0000-7002-8000-000000000001", provider: "00000001-0000-7007-8000-000000000001", model: "model-a",
      requests: 10, complete_requests: 7,
      input_tokens: 100, output_tokens: 50,
      cache_read_tokens: 30, cache_write_tokens: 5, reasoning_tokens: 0,
    },
    {
      bucket_us: 2 * day, agent: null, provider: "00000002-0000-7007-8000-000000000002", model: null,
      requests: 4, complete_requests: 4,
      input_tokens: 40, output_tokens: 20,
      cache_read_tokens: 0, cache_write_tokens: 8, reasoning_tokens: 2,
    },
  ];
  const retryRows: ReturnType<typeof decodeUsageRetries>["retry_rows"] = [
    { bucket_us: 0, agent: "00000001-0000-7002-8000-000000000001", provider: "00000001-0000-7007-8000-000000000001", model: "model-a", retries: 2 },
    { bucket_us: day, agent: null, provider: "00000002-0000-7007-8000-000000000002", model: null, retries: 1 },
  ];
  const observations: Usage["observations"] = [
    {
      id: "00000001-0000-7008-8000-000000000001", job: "00000001-0000-7001-8000-000000000001", request: 1, observed_at_us: 1_000, model: "model-a", provider: "p",
      llm_profile: null, input_tokens: 10, output_tokens: 5, cache_read_tokens: null,
      cache_write_tokens: null, reasoning_tokens: null, ttfb_ms: 100, finished_in_ms: 500,
      finality: "final", eligibility: "eligible",
    },
    {
      id: "00000002-0000-7008-8000-000000000002", job: "00000002-0000-7001-8000-000000000002", request: 1, observed_at_us: 2 * day + 1_000, model: null, provider: "p",
      llm_profile: null, input_tokens: null, output_tokens: null, cache_read_tokens: null,
      cache_write_tokens: null, reasoning_tokens: null, ttfb_ms: 300, finished_in_ms: null,
      finality: "cumulative", eligibility: null,
    },
  ];
  const availability: ReturnType<typeof decodeAvailability>["observations"] = [
    { id: "00000001-0000-7009-8000-000000000001", job: "00000001-0000-7001-8000-000000000001", request: 1, observed_at_us: 1_000, provider: "p", model: "model-a", status_code: 200, finish_reason: "stop", classification: "available" },
    { id: "00000002-0000-7009-8000-000000000002", job: "00000002-0000-7001-8000-000000000002", request: 1, observed_at_us: 2_000, provider: "p", model: "model-a", status_code: 503, finish_reason: null, classification: "unavailable" },
    { id: "00000003-0000-7009-8000-000000000003", job: "00000002-0000-7001-8000-000000000002", request: 1, observed_at_us: 3_000, provider: "p", model: "model-a", status_code: 503, finish_reason: null, classification: "unavailable" },
    { id: "00000004-0000-7009-8000-000000000004", job: "00000002-0000-7001-8000-000000000002", request: 2, observed_at_us: 2 * day + 1_000, provider: "p", model: "model-a", status_code: 200, finish_reason: "tool_calls", classification: "unconfirmed" },
  ];
  const summary = summarizeReliability(rows, retryRows, observations, 0, 3 * day, day, availability);
  assert.equal(summary.buckets.length, 3);
  const [first, empty, last] = summary.buckets;
  assert.deepEqual([first.requests, first.errors, first.retries], [2, 1, 2]);
  assert.deepEqual([first.cacheReads, first.cacheWrites], [30, 5]);
  assert.deepEqual([first.ttfbCount, first.ttfbAvgMs], [1, 100]);
  assert.deepEqual([first.durationCount, first.durationAvgMs], [1, 500]);
  assert.deepEqual(
    [empty.requests, empty.errors, empty.retries, empty.cacheReads, empty.cacheWrites],
    [0, 0, 1, 0, 0],
  );
  assert.equal(empty.ttfbAvgMs, null);
  assert.deepEqual([last.requests, last.errors, last.retries], [1, 0, 0]);
  assert.deepEqual([last.cacheReads, last.cacheWrites], [0, 8]);
  assert.deepEqual([last.ttfbCount, last.ttfbAvgMs], [1, 300]);
  assert.equal(last.durationAvgMs, null);
  assert.deepEqual(
    [summary.totalRequests, summary.totalErrors, summary.totalRetries],
    [3, 1, 3],
  );
  assert.deepEqual([summary.totalCacheReads, summary.totalCacheWrites], [30, 13]);
  assert.deepEqual([summary.ttfbCount, summary.ttfbAvgMs], [2, 200]);
  assert.deepEqual([summary.durationCount, summary.durationAvgMs], [1, 500]);
});


test("missing token reports are not HTTP errors and cache writes are not inferred misses", () => {
  const rows: Buckets["rows"] = [{ bucket_us: 0, agent: null, provider: "00000001-0000-7007-8000-000000000001", model: "m", requests: 1, complete_requests: 0, input_tokens: 1000, output_tokens: 0, cache_read_tokens: 0, cache_write_tokens: 50, reasoning_tokens: 0 }];
  const availability: ReturnType<typeof decodeAvailability>["observations"] = [{ id: "00000001-0000-7009-8000-000000000001", job: "00000001-0000-7001-8000-000000000001", request: 1, observed_at_us: 1, provider: "p", model: "m", status_code: 200, finish_reason: "stop", classification: "available" }];
  const result = summarizeReliability(rows, [], [], 0, 100, 100, availability);
  assert.equal(result.totalRequests, 1);
  assert.equal(result.totalErrors, 0);
  assert.equal(result.totalCacheWrites, 50);
  assert.equal(result.totalCacheReads, 0);
});
