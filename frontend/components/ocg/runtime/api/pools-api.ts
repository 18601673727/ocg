import { API_VERSION, ContractError, type Pool, type Pools } from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { ApiError, type Send } from "./transport";
import { counter, identity } from "./validators";

export type PoolUpdate =
  | { concurrency_limit: number; enabled?: never }
  | { enabled: boolean; concurrency_limit?: never };

export type NativeExecutor = {
  id: string;
  pool: string;
  name: string;
  enabled: boolean;
  archived: boolean;
  revision: number;
  capabilities: {
    operation: "llm_request" | "decision";
    protocol: string | null;
  }[];
};

export type NativeExecutorChange =
  | { enabled: boolean; name?: never; capabilities?: never }
  | { name: string; enabled?: never; capabilities?: never }
  | {
      capabilities: NativeExecutor["capabilities"];
      enabled?: never;
      name?: never;
    };

export function isExecutorConflict(error: unknown): boolean {
  return error instanceof ApiError && error.code === "executor_revision_conflict";
}

function validateNativeExecutor(
  executor: NativeExecutor | undefined,
): asserts executor is NativeExecutor {
  if (
    !executor ||
    typeof executor.id !== "string" ||
    typeof executor.pool !== "string" ||
    typeof executor.name !== "string" ||
    typeof executor.enabled !== "boolean" ||
    executor.archived !== false ||
    !Number.isSafeInteger(executor.revision) ||
    executor.revision < 1 ||
    !Array.isArray(executor.capabilities)
  )
    throw new ContractError("executor", "a native Executor");
  identity(executor.id, "executor");
  identity(executor.pool, "pool");
  if (
    !executor.capabilities.every(
      (capability) =>
        capability &&
        ((capability.operation === "decision" && capability.protocol === null) ||
          (capability.operation === "llm_request" &&
            ["openai_chat_completions", "openai_responses", "anthropic_messages"].includes(
              capability.protocol ?? "",
            ))),
    )
  )
    throw new ContractError("executor.capabilities", "stored Executor capabilities");
}

function sameCapabilities(
  left: NativeExecutor["capabilities"],
  right: NativeExecutor["capabilities"],
): boolean {
  const keys = (list: NativeExecutor["capabilities"]) =>
    list.map((item) => `${item.operation}:${item.protocol ?? ""}`).sort();
  const first = keys(left);
  const second = keys(right);
  return first.length === second.length && first.every((key, index) => key === second[index]);
}

export type NativeAllocationPhase = "reserved" | "running" | "stop_requested" | "uncertain";

export type NativeAllocation = {
  pool: string;
  job: string;
  executor: string;
  generation: number;
  job_revision: number;
  phase: NativeAllocationPhase;
  holds_capacity: true;
  claimed_at_unix_micros: number;
  started_at_unix_micros: number | null;
};

type NativePoolOccupancy =
  | {
      available: true;
      held: number;
      reserved: number;
      running: number;
      stop_requested: number;
      uncertain: number;
    }
  | { available: false };

export type NativePoolAllocations = {
  pool: string;
  capacity: number;
  occupancy: NativePoolOccupancy;
  allocations: NativeAllocation[];
  has_more: boolean;
  next_after: string | null;
};

const ALLOCATION_PHASES: readonly string[] = ["reserved", "running", "stop_requested", "uncertain"];
function nativePool(value: unknown): Pool {
  if (typeof value !== "object" || value === null) throw new ContractError("pool", "a native Pool");
  const pool = value as Record<string, unknown>;
  if (
    typeof pool.id !== "string" ||
    typeof pool.name !== "string" ||
    typeof pool.enabled !== "boolean" ||
    typeof pool.capacity !== "number" ||
    !Number.isSafeInteger(pool.capacity) ||
    pool.capacity < 1 ||
    pool.capacity > 4294967295 ||
    typeof pool.archived !== "boolean" ||
    pool.archived ||
    typeof pool.revision !== "number" ||
    !Number.isSafeInteger(pool.revision) ||
    pool.revision < 1
  ) {
    throw new ContractError("pool", "a valid native Pool");
  }
  return {
    id: identity(pool.id, "pool"),
    name: pool.name,
    capacity: pool.capacity,
    enabled: pool.enabled,
    archived: pool.archived,
    revision: pool.revision,
  };
}

export function createPoolsApi(send: Send) {
  return {
    async readPools(settings: { before?: string; signal?: AbortSignal } = {}): Promise<Pools> {
      const query = settings.before ? `?before=${encodeURIComponent(settings.before)}` : "";
      const answer = await send("GET", `/api/v1/pools${query}`, {
        signal: settings.signal,
      });
      const page = answer.input as Partial<Api.Envelope<Api.Pools>>;
      if (
        page.api_version !== API_VERSION ||
        page.resource_model !== "independent" ||
        !Array.isArray(page.pools) ||
        page.pools.length > 20 ||
        typeof page.has_more !== "boolean"
      ) {
        throw new ContractError("pools", "an independent Pool page");
      }
      let nextBefore: string | null = null;
      if (page.has_more) {
        if (typeof page.next_before !== "string") {
          throw new ContractError("next_before", "an advancing Pool cursor");
        }
        identity(page.next_before, "next_before");
        if (settings.before !== undefined && page.next_before >= settings.before) {
          throw new ContractError("next_before", "an advancing Pool cursor");
        }
        nextBefore = page.next_before;
      } else if (page.next_before !== null) {
        throw new ContractError("next_before", "a terminal Pool cursor");
      }
      return {
        resource_model: page.resource_model,
        pools: page.pools.map(nativePool),
        has_more: page.has_more,
        next_before: nextBefore,
      };
    },

    async updatePool(
      poolId: string,
      expectedRevision: number,
      body: PoolUpdate,
      signal?: AbortSignal,
    ) {
      identity(poolId, "pool");
      if (
        !Number.isSafeInteger(expectedRevision) ||
        expectedRevision < 1 ||
        expectedRevision > 4294967295
      ) {
        throw new ContractError("expected_revision", "an integer from 1 to 4294967295");
      }
      if (body.concurrency_limit !== undefined) {
        if (
          !Number.isSafeInteger(body.concurrency_limit) ||
          body.concurrency_limit < 1 ||
          body.concurrency_limit > 4294967295
        ) {
          throw new ContractError("concurrency_limit", "an integer from 1 to 4294967295");
        }
      }
      const answer = await send("POST", `/api/v1/pools/${poolId}`, {
        body: { expected_revision: expectedRevision, ...body },
        sign: true,
        signal,
      });
      const response = answer.input as Partial<Api.Envelope<Api.PoolAnswer>>;
      if (response.api_version !== API_VERSION || response.resource_model !== "independent") {
        throw new ContractError("pool", "a native Pool response");
      }
      const pool = nativePool(response.pool);
      if (
        pool.id !== poolId ||
        pool.revision !== expectedRevision + 1 ||
        (body.concurrency_limit !== undefined && pool.capacity !== body.concurrency_limit) ||
        (body.enabled !== undefined && pool.enabled !== body.enabled)
      ) {
        throw new ContractError("pool", "the updated Pool");
      }
      return pool;
    },

    async createNativePool(id: string, name: string, capacity: number) {
      identity(id, "pool");
      if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4294967295)
        throw new ContractError("capacity", "an integer from 1 to 4294967295");
      const result = await send("POST", "/api/v1/pools", {
        body: { id, name, capacity, enabled: true },
        sign: true,
      });
      const response = result.input as Partial<Api.Envelope<Api.PoolAnswer>>;
      if (response.api_version !== API_VERSION || response.resource_model !== "independent")
        throw new ContractError("pool", "a native Pool response");
      return nativePool(response.pool);
    },

    async readNativeExecutors(settings: { before?: string; signal?: AbortSignal } = {}) {
      const suffix =
        settings.before === undefined ? "" : `?before=${identity(settings.before, "before")}`;
      const result = await send("GET", `/api/v1/executors${suffix}`, {
        signal: settings.signal,
      });
      const page = result.input as {
        api_version?: number;
        resource_model?: string;
        executors?: NativeExecutor[];
        has_more?: boolean;
        next_before?: string | null;
      };
      if (
        page.api_version !== 1 ||
        page.resource_model !== "independent" ||
        !Array.isArray(page.executors) ||
        page.executors.length > 20 ||
        typeof page.has_more !== "boolean"
      )
        throw new ContractError("executors", "a native Executor page");
      for (const executor of page.executors) validateNativeExecutor(executor);
      if (page.has_more) {
        if (typeof page.next_before !== "string")
          throw new ContractError("next_before", "an advancing Executor cursor");
        identity(page.next_before, "next_before");
        if (settings.before !== undefined && page.next_before >= settings.before)
          throw new ContractError("next_before", "an advancing Executor cursor");
      } else if (page.next_before !== null)
        throw new ContractError("next_before", "a terminal Executor cursor");
      return {
        executors: page.executors,
        has_more: page.has_more,
        next_before: page.next_before ?? null,
      };
    },

    async readNativePoolAllocations(
      pool: string,
      settings: { after?: string; signal?: AbortSignal } = {},
    ): Promise<NativePoolAllocations> {
      identity(pool, "pool");
      let suffix = "";
      if (settings.after !== undefined) {
        const [executor, job, ...rest] = settings.after.split(".");
        if (rest.length > 0 || executor === undefined || job === undefined)
          throw new ContractError("after", "an allocation cursor");
        identity(executor, "after");
        identity(job, "after");
        suffix = `?after=${settings.after}`;
      }
      const result = await send("GET", `/api/v1/pools/${pool}/allocations${suffix}`, {
        signal: settings.signal,
      });
      const page = result.input as Partial<Omit<NativePoolAllocations, "occupancy">> & {
        api_version?: number;
        occupancy?: Record<string, unknown>;
      };
      const occupancy = page.occupancy;
      if (
        page.api_version !== 1 ||
        page.pool !== pool ||
        typeof page.capacity !== "number" ||
        !Number.isSafeInteger(page.capacity) ||
        page.capacity < 1 ||
        !occupancy ||
        !Array.isArray(page.allocations) ||
        page.allocations.length > 100 ||
        typeof page.has_more !== "boolean"
      )
        throw new ContractError("allocations", "a native Pool allocation page");
      let checked: NativePoolOccupancy;
      if (occupancy.available === true) {
        const held = counter(occupancy.held, "occupancy.held");
        const reserved = counter(occupancy.reserved, "occupancy.reserved");
        const running = counter(occupancy.running, "occupancy.running");
        const stopRequested = counter(occupancy.stop_requested, "occupancy.stop_requested");
        const uncertain = counter(occupancy.uncertain, "occupancy.uncertain");
        if (held !== reserved + running + stopRequested + uncertain)
          throw new ContractError("occupancy", "phase counts that add up to the held total");
        checked = {
          available: true,
          held,
          reserved,
          running,
          stop_requested: stopRequested,
          uncertain,
        };
      } else if (occupancy.available === false) {
        checked = { available: false };
      } else throw new ContractError("occupancy", "an occupancy summary");
      let previousKey = settings.after;
      for (const allocation of page.allocations) {
        if (
          !allocation ||
          typeof allocation.generation !== "number" ||
          !Number.isSafeInteger(allocation.generation) ||
          allocation.generation < 1 ||
          typeof allocation.job_revision !== "number" ||
          !Number.isSafeInteger(allocation.job_revision) ||
          allocation.job_revision < 0 ||
          !ALLOCATION_PHASES.includes(allocation.phase) ||
          allocation.holds_capacity !== true ||
          !Number.isSafeInteger(allocation.claimed_at_unix_micros) ||
          (allocation.started_at_unix_micros !== null &&
            !Number.isSafeInteger(allocation.started_at_unix_micros))
        )
          throw new ContractError("allocation", "a held Allocation");
        identity(allocation.pool, "allocation.pool");
        if (allocation.pool !== pool)
          throw new ContractError("allocation.pool", "the requested Pool's identity");
        identity(allocation.job, "job");
        identity(allocation.executor, "executor");
        const key = `${allocation.executor}.${allocation.job}`;
        if (previousKey !== undefined && key <= previousKey)
          throw new ContractError(
            "allocations",
            "Allocations in strictly increasing Executor and Job order after the cursor",
          );
        previousKey = key;
      }
      if (page.has_more) {
        const last = page.allocations[page.allocations.length - 1];
        if (
          typeof page.next_after !== "string" ||
          page.next_after === settings.after ||
          !last ||
          page.next_after !== `${last.executor}.${last.job}`
        )
          throw new ContractError("next_after", "the final returned Allocation's cursor");
        const [executor, job, ...rest] = page.next_after.split(".");
        if (rest.length > 0 || executor === undefined || job === undefined)
          throw new ContractError("next_after", "an allocation cursor");
        identity(executor, "next_after");
        identity(job, "next_after");
      } else if (page.next_after !== null)
        throw new ContractError("next_after", "a terminal allocation cursor");
      return {
        pool,
        capacity: page.capacity,
        occupancy: checked,
        allocations: page.allocations,
        has_more: page.has_more,
        next_after: page.next_after ?? null,
      };
    },

    async createNativeExecutor(
      id: string,
      pool: string,
      name: string,
      capability: NativeExecutor["capabilities"][number],
    ) {
      identity(id, "executor");
      identity(pool, "pool");
      const result = await send("POST", "/api/v1/executors", {
        body: { id, pool, name, enabled: true, capabilities: [capability] },
        sign: true,
      });
      const answer = result.input as {
        api_version?: number;
        executor?: NativeExecutor;
      };
      if (
        answer.api_version !== 1 ||
        !answer.executor ||
        answer.executor.id !== id ||
        answer.executor.pool !== pool
      )
        throw new ContractError("executor", "the registered Executor");
      return answer.executor;
    },

    async updateNativeExecutor(
      executor: NativeExecutor,
      change: NativeExecutorChange,
      signal?: AbortSignal,
    ) {
      identity(executor.id, "executor");
      if (!Number.isSafeInteger(executor.revision) || executor.revision < 1)
        throw new ContractError("expected_revision", "a positive integer");
      const result = await send("POST", `/api/v1/executors/${executor.id}`, {
        body: { expected_revision: executor.revision, ...change },
        sign: true,
        signal,
      });
      const answer = result.input as {
        api_version?: number;
        executor?: NativeExecutor;
      };
      const updated = answer.executor;
      if (answer.api_version !== 1 || updated?.id !== executor.id)
        throw new ContractError("executor", "the updated Executor");
      validateNativeExecutor(updated);
      if (
        updated.pool !== executor.pool ||
        updated.revision !== executor.revision + 1 ||
        updated.enabled !== (change.enabled ?? executor.enabled) ||
        updated.name !== (change.name ?? executor.name) ||
        !sameCapabilities(updated.capabilities, change.capabilities ?? executor.capabilities)
      )
        throw new ContractError("executor", "the updated Executor");
      return updated;
    },

    async archiveNativeExecutor(id: string, expectedRevision: number, signal?: AbortSignal) {
      identity(id, "executor");
      if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 1)
        throw new ContractError("expected_revision", "a positive integer");
      const result = await send("POST", `/api/v1/executors/${id}/archive`, {
        body: { expected_revision: expectedRevision },
        sign: true,
        signal,
      });
      const answer = result.input as {
        api_version?: number;
        executor?: NativeExecutor;
      };
      const archived = answer.executor;
      if (
        answer.api_version !== 1 ||
        !archived ||
        archived.id !== id ||
        archived.archived !== true ||
        archived.enabled !== false ||
        archived.revision !== expectedRevision + 1
      )
        throw new ContractError("executor", "the archived Executor");
      return archived;
    },

    async archiveNativePool(id: string, expectedRevision: number, signal?: AbortSignal) {
      identity(id, "pool");
      if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 1)
        throw new ContractError("expected_revision", "a positive integer");
      const result = await send("POST", `/api/v1/pools/${id}/archive`, {
        body: { expected_revision: expectedRevision },
        sign: true,
        signal,
      });
      const answer = result.input as Partial<Api.Envelope<Api.PoolAnswer>>;
      const archived = answer.pool;
      if (
        answer.api_version !== API_VERSION ||
        answer.resource_model !== "independent" ||
        !archived ||
        archived.id !== id ||
        archived.archived !== true ||
        archived.enabled !== false ||
        archived.revision !== expectedRevision + 1
      )
        throw new ContractError("pool", "the archived Pool");
    },
  };
}
