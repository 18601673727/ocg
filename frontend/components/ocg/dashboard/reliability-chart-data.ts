import type { decodeAvailability, decodeUsage, decodeUsageBuckets, decodeUsageRetries } from "../contracts";

type BucketRow = ReturnType<typeof decodeUsageBuckets>["rows"][number];
type RetryRow = ReturnType<typeof decodeUsageRetries>["retry_rows"][number];
type UsageObservation = ReturnType<typeof decodeUsage>["observations"][number];

export type ReliabilityBucket = {
  bucketUs: number;
  requests: number;
  /** Requests with a recorded HTTP error status (400 through 599). */
  errors: number;
  /** Turn retries attributed to this bucket. */
  retries: number;
  /** Tokens served from cache. */
  cacheReads: number;
  /** Tokens written to cache. */
  cacheWrites: number;
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
  totalCacheReads: number;
  totalCacheWrites: number;
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
 * recorded HTTP errors, turn retries, cache reads and writes, and
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
  availability: readonly ReturnType<typeof decodeAvailability>["observations"][number][],
): ReliabilitySummary {
  const buckets: ReliabilityBucket[] = [];
  if (widthUs > 0) {
    for (let bucketUs = fromUs; bucketUs < untilUs; bucketUs += widthUs) {
      buckets.push({
        bucketUs,
        requests: 0,
        errors: 0,
        retries: 0,
        cacheReads: 0,
        cacheWrites: 0,
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
    bucket.cacheReads += row.cache_read_tokens;
    bucket.cacheWrites += row.cache_write_tokens;
  }
  const requests = new Map<string, { at: number; error: boolean }>();
  for (const row of availability) {
    if (!inRange(row.observed_at_us)) continue;
    const key = `${row.job}:${row.request}`;
    const previous = requests.get(key);
    requests.set(key, {
      at: Math.min(previous?.at ?? row.observed_at_us, row.observed_at_us),
      error: (previous?.error ?? false) || (row.status_code !== null && row.status_code >= 400 && row.status_code <= 599),
    });
  }
  for (const request of requests.values()) {
    const bucket = buckets[at(request.at)];
    bucket.requests += 1;
    if (request.error) bucket.errors += 1;
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
  let totalCacheReads = 0;
  let totalCacheWrites = 0;
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
    totalCacheReads += bucket.cacheReads;
    totalCacheWrites += bucket.cacheWrites;
  }
  return {
    buckets,
    totalRequests,
    totalErrors,
    totalRetries,
    totalCacheReads,
    totalCacheWrites,
    ttfbCount,
    ttfbAvgMs: average(ttfbSum, ttfbCount),
    durationCount,
    durationAvgMs: average(durationSum, durationCount),
  };
}
