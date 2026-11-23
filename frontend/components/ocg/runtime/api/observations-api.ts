import { ContractError } from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { type Send } from "./transport";
import { identity, jobIdentity, sequence } from "./validators";

export type ObservationSettings = {
  fromUs?: number;
  untilUs?: number;
  after?: string;
  llmProfile?: string;
  limit?: number;
  signal?: AbortSignal;
};

function observationQuery(settings: ObservationSettings): string {
  const query: string[] = [];
  for (const [name, value] of [
    ["from_us", settings.fromUs],
    ["until_us", settings.untilUs],
    ["limit", settings.limit],
  ] as const) {
    if (value !== undefined) query.push(`${name}=${sequence(value)}`);
  }
  if (settings.after) {
    if (!/^[A-Za-z0-9_-]{1,400}$/.test(settings.after))
      throw new ContractError("after", "an observation cursor");
    query.push(`after=${settings.after}`);
  }
  if (settings.llmProfile)
    query.push(`llm_profile=${identity(settings.llmProfile, "llm_profile")}`);
  return query.length ? `?${query.join("&")}` : "";
}

export function createObservationsApi(send: Send) {
  return {
    async readUsage(settings: ObservationSettings = {}) {
      return (
        await send("GET", `/api/v1/usage${observationQuery(settings)}`, {
          signal: settings.signal,
        })
      ).input as Api.Usage;
    },

    async readUsageBuckets({
      fromUs,
      untilUs,
      bucket,
      tzOffsetMinutes,
      scope,
      after,
      signal,
    }: {
      fromUs: number;
      untilUs: number;
      bucket: "day" | "week";
      tzOffsetMinutes: number;
      scope?: "all" | "generative";
      after?: string;
      signal?: AbortSignal;
    }) {
      if (!Number.isSafeInteger(tzOffsetMinutes)) {
        throw new ContractError("tz_offset_minutes", "a signed safe integer");
      }
      const query = `from_us=${sequence(fromUs)}&until_us=${sequence(untilUs)}&bucket=${bucket}&tz_offset_minutes=${tzOffsetMinutes}${scope ? `&scope=${scope}` : ""}${after ? `&after=${encodeURIComponent(after)}` : ""}`;
      return (await send("GET", `/api/v1/usage/summary?${query}`, { signal }))
        .input as Api.Envelope<Api.UsageBuckets>;
    },

    async readUsageRetries({
      fromUs,
      untilUs,
      bucket,
      tzOffsetMinutes,
      after,
      signal,
    }: {
      fromUs: number;
      untilUs: number;
      bucket: "day" | "week";
      tzOffsetMinutes: number;
      after?: string;
      signal?: AbortSignal;
    }) {
      if (!Number.isSafeInteger(tzOffsetMinutes))
        throw new ContractError("tz_offset_minutes", "a signed safe integer");
      const query = `from_us=${sequence(fromUs)}&until_us=${sequence(untilUs)}&bucket=${bucket}&tz_offset_minutes=${tzOffsetMinutes}&limit=100${after ? `&after=${encodeURIComponent(after)}` : ""}`;
      return (await send("GET", `/api/v1/usage/retries?${query}`, { signal }))
        .input as Api.UsageRetries;
    },

    async readAvailability(settings: ObservationSettings = {}) {
      return (
        await send("GET", `/api/v1/availability${observationQuery(settings)}`, {
          signal: settings.signal,
        })
      ).input as Api.Availability;
    },

    async readLogs(
      settings: { file?: string; before?: number; limit?: number; signal?: AbortSignal } = {},
    ) {
      const query = [`limit=${settings.limit ?? 100}`];
      if (settings.file) {
        if (!/^\d{14}Z_runtime_\d+\.log$/.test(settings.file))
          throw new ContractError("file", "a runtime log file name");
        query.push(`file=${settings.file}`);
      }
      if (settings.before !== undefined) query.push(`before=${sequence(settings.before)}`);
      return (
        await send("GET", `/api/v1/logs?${query.join("&")}`, {
          signal: settings.signal,
        })
      ).input as Api.Logs;
    },

    async readHealth(signal?: AbortSignal, after?: string) {
      const query = after ? `?after=${encodeURIComponent(after)}` : "";
      return (await send("GET", `/api/v1/health${query}`, { signal })).input as Api.Health;
    },

    async releaseAllocation(item: NonNullable<Api.Health["quarantined"]>[number]) {
      jobIdentity(item.job);
      const { pool, executor, generation, epoch, job_revision } = item;
      await send("POST", `/api/v1/jobs/${item.job}/release`, {
        body: { pool, executor, generation, epoch, job_revision },
        sign: true,
      });
    },

    async readUpdate(latest: boolean, signal?: AbortSignal) {
      return (
        await send("GET", `/api/v1/update${latest ? "?latest=1" : ""}`, {
          signal,
        })
      ).input as Api.Update;
    },

    async startUpdate(version: string) {
      return (await send("POST", "/api/v1/update", { body: { version }, sign: true }))
        .input as Api.UpdateState;
    },
  };
}
