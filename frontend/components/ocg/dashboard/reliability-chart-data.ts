import type { decodeUsage, decodeUsageBuckets } from "../contracts";

type BucketRow = ReturnType<typeof decodeUsageBuckets>["rows"][number];
type RetryRow = ReturnType<typeof decodeUsageBuckets>["retry_rows"][number];
type UsageObservation = ReturnType<typeof decodeUsage>["observations"][number];

export type ReliabilityBucket = {
  bucketUs: number;
  requests: number;
  /** Requests that did not complete: requests minus complete requests, floored at zero. */
  errors: number;
  /** Turn retries attributed to this bucket. */
  retries: number;
  /** Tokens served from cache. */
  cacheHits: number;
  /** Tokens written to cache. */
  cacheMiss: number;
  ttfbCount: number;
  ttfbAvgMs: number | null;
  durationCount: number;
  durationAvgMs: number | null;
};

export type ReliabilitySummary = {
  buckets: ReliabilityBucket[];
  totalRequests: number;
  totalErrors: number;
  totalRetries: number;
  totalCacheHits: number;
  totalCacheMiss: number;
  ttfbCount: number;
  ttfbAvgMs: number | null;
  durationCount: number;
  durationAvgMs: number | null;
};

function average(sum: number, count: number): number | null {
  return count > 0 ? sum / count : null;
}

/**
 * Zero-fills one bucket per `widthUs` over `[fromUs, untilUs)`, then folds
 * usage-bucket counts (failed requests, retries, cache hits and misses) and
 * usage-observation latencies (average time to first byte and total
 * duration) into each bucket.
 */
export function summarizeReliability(
  bucketRows: readonly BucketRow[],
  retryRows: readonly RetryRow[],
  observations: readonly UsageObservation[],
  fromUs: number,
  untilUs: number,
  widthUs: number,
): ReliabilitySummary {
  const buckets: ReliabilityBucket[] = [];
  if (widthUs > 0) {
    for (let bucketUs = fromUs; bucketUs < untilUs; bucketUs += widthUs) {
      buckets.push({
        bucketUs,
        requests: 0,
        errors: 0,
        retries: 0,
        cacheHits: 0,
        cacheMiss: 0,
        ttfbCount: 0,
        ttfbAvgMs: null,
        durationCount: 0,
        durationAvgMs: null,
      });
    }
  }
  const at = (us: number) => Math.floor((us - fromUs) / widthUs);
  const inRange = (us: number) => widthUs > 0 && us >= fromUs && us < untilUs && at(us) < buckets.length;

  let ttfbSum = 0;
  let ttfbCount = 0;
  let durationSum = 0;
  let durationCount = 0;
  const ttfbSums = new Map<number, { sum: number; count: number }>();
  const durationSums = new Map<number, { sum: number; count: number }>();

  for (const row of bucketRows) {
    if (!inRange(row.bucket_us)) continue;
    const bucket = buckets[at(row.bucket_us)];
    bucket.requests += row.requests;
    bucket.errors += Math.max(0, row.requests - row.complete_requests);
    bucket.cacheHits += row.cache_read_tokens;
    bucket.cacheMiss += row.cache_write_tokens;
  }
  for (const row of retryRows) {
    if (!inRange(row.bucket_us)) continue;
    buckets[at(row.bucket_us)].retries += row.retries;
  }
  for (const observation of observations) {
    if (!inRange(observation.observed_at_us)) continue;
    const index = at(observation.observed_at_us);
    if (observation.ttfb_ms !== null) {
      const entry = ttfbSums.get(index) ?? { sum: 0, count: 0 };
      entry.sum += observation.ttfb_ms;
      entry.count += 1;
      ttfbSums.set(index, entry);
      ttfbSum += observation.ttfb_ms;
      ttfbCount += 1;
    }
    if (observation.finished_in_ms !== null) {
      const entry = durationSums.get(index) ?? { sum: 0, count: 0 };
      entry.sum += observation.finished_in_ms;
      entry.count += 1;
      durationSums.set(index, entry);
      durationSum += observation.finished_in_ms;
      durationCount += 1;
    }
  }

  let totalRequests = 0;
  let totalErrors = 0;
  let totalRetries = 0;
  let totalCacheHits = 0;
  let totalCacheMiss = 0;
  for (const [index, bucket] of buckets.entries()) {
    const ttfb = ttfbSums.get(index);
    if (ttfb) {
      bucket.ttfbCount = ttfb.count;
      bucket.ttfbAvgMs = average(ttfb.sum, ttfb.count);
    }
    const duration = durationSums.get(index);
    if (duration) {
      bucket.durationCount = duration.count;
      bucket.durationAvgMs = average(duration.sum, duration.count);
    }
    totalRequests += bucket.requests;
    totalErrors += bucket.errors;
    totalRetries += bucket.retries;
    totalCacheHits += bucket.cacheHits;
    totalCacheMiss += bucket.cacheMiss;
  }
  return {
    buckets,
    totalRequests,
    totalErrors,
    totalRetries,
    totalCacheHits,
    totalCacheMiss,
    ttfbCount,
    ttfbAvgMs: average(ttfbSum, ttfbCount),
    durationCount,
    durationAvgMs: average(durationSum, durationCount),
  };
}
