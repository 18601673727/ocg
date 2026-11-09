import type { ControlProjects, ControlPools, ControlChildren } from "../contracts";
import type * as Api from "@/components/ocg/contracts/api";
import {
  projectJob,
  projectContextStatus,
  ContractError,
  TREE_MAX_DEPTH,
  projectAgentProfiles,
  type ControlApproval,
  projectJobDetails,
  projectConversation,
  projectJobEvent,
  projectProfiles,
  type ControlAttachment,
  type ControlOutput,
  type ControlCapability,
  type ControlJobEvent,
  type ControlJobToolsPage,
  type ControlProject,
  type ControlToolCall,
  type ControlProviderResponse,
} from "../contracts";
import { SseParser } from "./sse";

export type ControlProjectCommand = {
  name: string;
  argv: string[];
  owner?: "user" | "decision";
  status?: "allowed" | "review" | "blocked";
};

export type ControlProjectCreate = {
  name: string;
  root: string;
  description?: string | null;
  commands?: ControlProjectCommand[];
};

export type ControlProjectUpdate = {
  name: string;
  description: string | null;
  commands: ControlProjectCommand[];
};

export type ControlPoolUpdate =
  | { concurrency_limit: number; enabled?: never }
  | { enabled: boolean; concurrency_limit?: never }
  | { decision: boolean; concurrency_limit?: never; enabled?: never };

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

export function configurationIdentity(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let time = Date.now();
  for (let i = 5; i >= 0; i--) {
    bytes[i] = time % 256;
    time = Math.floor(time / 256);
  }
  bytes[6] = (bytes[6] & 15) | 0x70;
  bytes[8] = (bytes[8] & 63) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export type ControlConversationEdit =
  | { title: string; hidden?: never }
  | { hidden: true; title?: never }
  | {
      limits: Pick<Api.ConversationLimits, "max_depth" | "max_auto_turns" | "max_iterations">;
      title?: never;
      hidden?: never;
    };

export type JobStreamEvent = ControlJobEvent | { type: "heartbeat" };
export type JobStreamEnd = "settled" | "stream_error" | "aborted";
export type JobStreamSubscription = {
  closed: Promise<JobStreamEnd>;
  close(): void;
};

export class ControlTransportError extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
    readonly retryable: boolean,
    readonly detail: string = "",
  ) {
    // Only the decoded API error message is presented; never request data or fetch errors.
    super(detail ? `${detail} (${code}, ${status})` : `OCG request: ${code} (${status})`);
    this.name = "ControlTransportError";
  }
}

/** Whether a Profile save was refused because the Profile changed since the edit began. */
export function isProfileConflict(error: unknown): boolean {
  return (
    error instanceof ControlTransportError &&
    (error.code === "profile_revision_changed" || error.code === "profile_changed")
  );
}

export type HttpCanonicalControlClientOptions = {
  streamIdleMs?: number;
  baseUrl: string;
  fetch: typeof fetch;
  // Programmatic mode: supplied by an already authorized caller; no
  // acquisition or storage here.
  bearerToken?: () => string | Promise<string>;
  // Browser-session mode: the session cookie authenticates every request,
  // and the session's CSRF token, from the session route, signs the
  // state-changing ones. Never the bearer token.
  csrfToken?: () => string | Promise<string>;
};

export type MutableJobInput = {
  text: string;
  attachments: string[];
  configuration: {
    llm_profile: string;
    agent_profile: string | null;
    reasoning_effort: string | null;
  } | null;
  priority: "foreground" | "automatic" | "speculative";
};

export type MutableJob = {
  id: string;
  conversation: string;
  previous: string | null;
  project: string | null;
  context: string;
  revision: number;
  phase: "unsubmitted" | "queued" | "executing" | "completed" | "failed" | "cancelled" | "unknown";
  submitted_at: number | null;
  unsupported_decision_history?: boolean;
  input: MutableJobInput;
};

function mutableJob(value: unknown, context: string): MutableJob {
  const envelope = value as { api_version?: number; job?: MutableJob } | null;
  const job = envelope?.job;
  if (
    envelope?.api_version !== 1 ||
    !job ||
    job.context !== context ||
    !Number.isSafeInteger(job.revision) ||
    job.revision < 1 ||
    !["unsubmitted", "queued", "executing", "completed", "failed", "cancelled", "unknown"].includes(
      job.phase,
    ) ||
    !job.input ||
    typeof job.input.text !== "string" ||
    !Array.isArray(job.input.attachments) ||
    !job.input.attachments.every((id) => typeof id === "string") ||
    !["foreground", "automatic", "speculative"].includes(job.input.priority) ||
    !(
      job.unsupported_decision_history === undefined ||
      typeof job.unsupported_decision_history === "boolean"
    ) ||
    !(job.submitted_at === null || Number.isSafeInteger(job.submitted_at))
  ) {
    throw new ContractError("job", "a valid mutable Job");
  }
  identity(job.id, "job");
  identity(job.conversation, "conversation");
  if (job.previous !== null) identity(job.previous, "previous");
  if (job.project !== null) identity(job.project, "project");
  for (const id of job.input.attachments) identity(id, "attachment");
  if (job.input.configuration !== null) {
    const config = job.input.configuration;
    if (
      !config ||
      typeof config.llm_profile !== "string" ||
      !(config.agent_profile === null || typeof config.agent_profile === "string") ||
      !(config.reasoning_effort === null || typeof config.reasoning_effort === "string")
    )
      throw new ContractError("job.input.configuration", "a valid configuration");
    identity(config.llm_profile, "llm_profile");
    if (config.agent_profile) identity(config.agent_profile, "agent_profile");
  }
  return job;
}

// `GET /api/v1/jobs/{id}` answers the observation details with the editable state
// beside them; the editable Job is read out of that one body.
function editableJob(value: unknown): unknown {
  const body = value as {
    api_version?: number;
    job?: { id?: string; conversation?: string | null; previous?: string | null };
    project?: string | null;
    context?: string;
    revision?: number;
    phase?: MutableJob["phase"];
    submitted_at?: number | null;
    unsupported_decision_history?: boolean;
    input?: MutableJobInput;
  } | null;
  if (!body?.job) throw new ContractError("job", "a valid mutable Job");
  return {
    api_version: body.api_version,
    job: {
      id: body.job.id,
      conversation: body.job.conversation,
      previous: body.job.previous ?? null,
      project: body.project ?? null,
      context: body.context,
      revision: body.revision,
      phase: body.phase,
      submitted_at: body.submitted_at ?? null,
      unsupported_decision_history: body.unsupported_decision_history,
      input: body.input,
    },
  };
}

export type ControlProfileWriteRequest =
  | {
      kind: "llm";
      name: string;
      model: string;
      context_window?: number | null;
      protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
      endpoint?: string | null;
      credential_ref?: string | null;
      credential?: string | null;
      clear_credential?: boolean;
      declared_capabilities: ControlCapability[];
      /** Omitted keeps the stored headers; a list replaces them. */
      headers?: Api.OutboundHeaderInput[];
      /** The Profile's `edit_seq` when the form was opened; required to update one. */
      expected_edit_seq?: number;
      parameters?: {
        temperature?: number | null;
        top_p?: number | null;
        max_tokens?: number | null;
        max_completion_tokens?: number | null;
        reasoning_effort?: string | null;
        presence_penalty?: number | null;
        frequency_penalty?: number | null;
      };
    }
  | {
      kind: "agent";
      name: string;
      instructions?: string | null;
      capabilities: ("chat" | "coding" | "decision")[];
      llm_targets: string[];
      expected_edit_seq?: number;
    }
  | {
      kind: "decision_model";
      name: string;
      model: string;
      protocol: "typesafe_systemone";
      endpoint: string;
      credential?: string;
      credential_ref?: string | null;
      clear_credential?: boolean;
      declared_capabilities: string[];
      limits: Api.DecisionLimits;
      headers?: Api.OutboundHeaderInput[];
      expected_edit_seq?: number;
    }
  | {
      kind: "decision_agent";
      name: string;
      instructions?: string | null;
      models: string[];
      default_model?: string | null;
      expected_edit_seq?: number;
    };

function controlOrigin(raw: string, browserSession: boolean): string {
  const url = new URL(raw);
  const loopback =
    url.protocol === "http:" &&
    ["127.0.0.1", "localhost"].includes(url.hostname) &&
    Boolean(url.port);
  const secureBrowser =
    browserSession &&
    typeof window !== "undefined" &&
    url.protocol === "https:" &&
    window.location.origin === url.origin;
  if (
    (!loopback && !secureBrowser) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new ControlTransportError("invalid_control_origin", 0, false);
  }
  if (typeof window !== "undefined" && window.location.origin !== url.origin) {
    throw new ControlTransportError("same_origin_required", 0, false);
  }
  return url.origin;
}

function sequence(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new ContractError("after_seq", "a non-negative safe integer");
  return value;
}

function jobIdentity(value: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value))
    throw new ContractError("job", "a canonical Job UUID");
  return value;
}

function identity(value: string, path: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) {
    throw new ContractError(path, "a canonical UUIDv7 identity");
  }
  return value;
}

function nativePool(value: unknown): ControlPools["pools"][number] {
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
    enabled: pool.enabled,
    concurrency_limit: pool.capacity,
    operations: undefined,
  };
}

function submissionKey(value: string): string {
  if (!/^[A-Za-z0-9._~:+/=-]{16,128}$/.test(value)) {
    throw new ContractError("Idempotency-Key", "a well-formed idempotency key");
  }
  return value;
}

function pause(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
  });
}

export type HttpCanonicalControlClient = ReturnType<typeof createHttpCanonicalControlClient>;

/** Auxiliary evidence must not hide the authoritative stored outputs. */
export async function readTranscriptEvidence(
  client: Pick<HttpCanonicalControlClient, "readAllJobTools" | "readAllProviderResponses">,
  job: string,
  {
    signal = new AbortController().signal,
    previous = [],
    settled = false,
    readReplies = true,
  }: {
    signal?: AbortSignal;
    previous?: readonly ControlProviderResponse[];
    settled?: boolean;
    readReplies?: boolean;
  } = {},
) {
  const [tools, replies] = await Promise.allSettled([
    client.readAllJobTools(job, signal),
    readReplies
      ? client.readAllProviderResponses(job, signal, previous.at(-1)?.request).catch((error) => {
          // A Decision Job has no reply text; the backend says so rather than failing.
          if (
            error instanceof ControlTransportError &&
            error.code === "decision_response_unavailable"
          )
            return [];
          throw error;
        })
      : Promise.resolve([]),
  ]);
  signal.throwIfAborted();
  const responses = replies.status === "fulfilled" ? [...previous, ...replies.value] : null;
  let replyError = replies.status === "rejected" ? replies.reason : null;
  // Settlement precedes the final Received write. Reconcile that short gap
  // independently of the now-closed output stream, with bounded retries.
  for (const delay of [100, 300, 1000, 3000]) {
    if (
      !settled ||
      responses === null ||
      replyError ||
      (responses.length && !responses.at(-1)?.requests_tools)
    )
      break;
    await pause(delay, signal);
    signal.throwIfAborted();
    try {
      responses.push(
        ...(await client.readAllProviderResponses(job, signal, responses.at(-1)?.request)),
      );
    } catch (error) {
      replyError = error;
    }
  }
  const failures = [tools.status === "rejected" ? tools.reason : null, replyError].filter(
    (error) => error !== null,
  );
  return {
    tools: tools.status === "fulfilled" ? tools.value : null,
    responses,
    error: failures.length
      ? `Transcript evidence unavailable; showing stored output. ${failures.map((error) => (error instanceof Error ? error.message : String(error))).join("; ")}`
      : undefined,
  };
}

/** Reads a bounded sequence window without walking earlier output pages. */
export async function readOutputWindow(
  client: Pick<HttpCanonicalControlClient, "readOutputs" | "readLatestOutput">,
  job: string,
  {
    beforeSeq,
    limit = 200,
    signal,
  }: { beforeSeq?: number; limit?: number; signal?: AbortSignal } = {},
) {
  if (!Number.isSafeInteger(limit) || limit < 1)
    throw new ContractError("limit", "a positive output window");
  if (beforeSeq !== undefined && (!Number.isSafeInteger(beforeSeq) || beforeSeq < 1))
    throw new ContractError("beforeSeq", "a positive output sequence");
  const end =
    beforeSeq === undefined
      ? ((await client.readLatestOutput(job, signal))?.seq ?? 0)
      : beforeSeq - 1;
  const outputs: ControlOutput[] = [];
  let after = Math.max(0, end - limit);
  while (after < end) {
    const page = await client.readOutputs(job, {
      afterSeq: after,
      limit: Math.min(100, end - after),
      signal,
    });
    signal?.throwIfAborted();
    const incoming = page.outputs.filter((output) => output.seq <= end);
    if (!incoming.length || incoming[0].seq <= after)
      throw new ContractError("outputs.seq", "an advancing output window");
    outputs.push(...incoming);
    after = incoming.at(-1)!.seq;
    if (!page.has_more && after < end)
      throw new ContractError("outputs.seq", "the complete requested output window");
  }
  return { outputs, latestSeq: end };
}

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

// Retains the 0.4.5 canonical client's injected-fetch boundary and contracts
// entrypoint. None of its legacy lifecycle, chat or fixture stores are needed.
export function createHttpCanonicalControlClient(options: HttpCanonicalControlClientOptions) {
  const programmatic = options.bearerToken !== undefined;
  const base = controlOrigin(options.baseUrl, !programmatic);
  const fetchImpl = options.fetch.bind(globalThis);
  // Live subscriptions only; each owns its delivery acknowledgement.
  const active = new Set<AbortController>();
  const profileListeners = new Set<(change?: { availability: string }) => void>();
  const conversationListeners = new Set<() => void>();
  let closed = false;
  let locksUnavailable = false;
  function browserLocks(): LockManager | undefined {
    if (programmatic || locksUnavailable || typeof navigator === "undefined") return undefined;
    try {
      return navigator.locks;
    } catch {
      locksUnavailable = true;
      return undefined;
    }
  }
  let snapshotBusy = 0;
  let lockTurn = 0;
  const snapshotWaiters: Array<() => void> = [];
  const holders = new Set<{ started: number; check(): void }>();

  function checkWaiters() {
    if (!snapshotWaiters.length) return;
    let oldest: { started: number; check(): void } | undefined;
    for (const holder of holders) {
      if (!oldest || holder.started < oldest.started) oldest = holder;
    }
    oldest?.check();
  }

  async function snapshotSlot(signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    if (snapshotBusy >= 3) {
      await new Promise<void>((resolve, reject) => {
        const wake = () => {
          signal.removeEventListener("abort", abort);
          resolve();
        };
        const abort = () => {
          const index = snapshotWaiters.indexOf(wake);
          if (index >= 0) snapshotWaiters.splice(index, 1);
          reject(signal.reason);
        };
        snapshotWaiters.push(wake);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
        else checkWaiters();
      });
    } else snapshotBusy++;
    return () => {
      const next = snapshotWaiters.shift();
      if (next) next();
      else snapshotBusy--;
      checkWaiters();
    };
  }

  function streamAttempt(parent: AbortSignal, lockName?: string) {
    const controller = new AbortController();
    let rotated = false;
    let complete = programmatic;
    const started = performance.now();
    const rotate = () => {
      if (rotated || controller.signal.aborted) return;
      rotated = true;
      controller.abort();
    };
    const holder = {
      started,
      check() {
        if (!complete || performance.now() - started < 5000) return;
        if (snapshotWaiters.length) {
          let oldest: typeof holder | undefined;
          for (const other of holders) {
            if (!oldest || other.started < oldest.started) oldest = other;
          }
          if (oldest === holder) rotate();
        }
      },
    };
    const abort = () => controller.abort();
    parent.addEventListener("abort", abort, { once: true });
    if (parent.aborted) abort();
    if (!programmatic) holders.add(holder);
    const timer = programmatic ? undefined : setTimeout(() => holder.check(), 5000);
    const poll = lockName
      ? setInterval(() => {
          const locks = browserLocks();
          if (!locks || typeof locks.query !== "function") return;
          void locks
            .query()
            .then((state) => {
              if (
                complete &&
                performance.now() - started >= 5000 &&
                (state.pending ?? []).some((request) => request.name === lockName)
              )
                rotate();
            })
            .catch(() => {});
        }, 2000)
      : undefined;
    checkWaiters();
    return {
      signal: controller.signal,
      rotated: () => rotated,
      complete: () => {
        complete = true;
        holder.check();
      },
      abort: () => controller.abort(),
      dispose: () => {
        if (timer !== undefined) clearTimeout(timer);
        if (poll !== undefined) clearInterval(poll);
        holders.delete(holder);
        parent.removeEventListener("abort", abort);
      },
    };
  }

  async function withSnapshotLock<T>(
    locks: LockManager,
    signal: AbortSignal,
    run: (name: string) => Promise<T>,
  ): Promise<T> {
    const names = Array.from({ length: 3 }, (_, index) => `ocg-snapshot-stream-${index}`);
    let probingSupported = true;
    for (const name of names) {
      signal.throwIfAborted();
      let entered = false;
      try {
        const result = await locks.request(name, { ifAvailable: true }, (lock) => {
          if (!lock) return undefined;
          entered = true;
          return run(name);
        });
        if (entered) return result as T;
      } catch (error) {
        if (entered || !(error instanceof DOMException && error.name === "NotSupportedError"))
          throw error;
        probingSupported = false;
        break;
      }
      signal.throwIfAborted();
    }
    signal.throwIfAborted();
    const parked = new AbortController();
    const name = names[lockTurn++ % names.length];
    return new Promise<T>((resolve, reject) => {
      let claimed = false;
      let probing = false;
      let timer: ReturnType<typeof setInterval> | undefined;
      const cleanup = () => {
        if (timer !== undefined) clearInterval(timer);
        signal.removeEventListener("abort", abort);
      };
      const abort = () => {
        parked.abort();
        cleanup();
        reject(signal.reason);
      };
      const enter = (lockName: string, fromProbe: boolean) => {
        if (claimed || signal.aborted) return undefined;
        claimed = true;
        cleanup();
        if (fromProbe) parked.abort();
        const outcome = run(lockName);
        outcome.then(resolve, reject);
        return outcome;
      };
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) {
        abort();
        return;
      }
      void locks
        .request(name, { signal: parked.signal }, () => enter(name, false))
        .catch((error) => {
          if (!claimed && !signal.aborted) {
            cleanup();
            reject(error);
          }
        });
      if (probingSupported)
        timer = setInterval(() => {
          if (probing || claimed || signal.aborted) return;
          probing = true;
          void (async () => {
            for (const candidate of names) {
              if (claimed || signal.aborted) break;
              try {
                await locks.request(candidate, { ifAvailable: true }, (lock) =>
                  lock ? enter(candidate, true) : undefined,
                );
              } catch (error) {
                if (error instanceof DOMException && error.name === "NotSupportedError") {
                  if (timer !== undefined) clearInterval(timer);
                  break;
                }
                if (!claimed && !signal.aborted) {
                  parked.abort();
                  cleanup();
                  reject(error);
                }
                break;
              }
              if (signal.aborted) break;
            }
            probing = false;
          })();
        }, 2000);
    });
  }

  // The one credential this client sends: the bearer token of its caller,
  // or nothing at all where the browser's own session cookie applies.
  async function credentials(): Promise<Record<string, string>> {
    if (!options.bearerToken) return {};
    const token = await options.bearerToken();
    if (!/^[A-Za-z0-9._~+/=-]{32,512}$/.test(token)) {
      throw new ControlTransportError("credential_unavailable", 0, false);
    }
    return { Authorization: `Bearer ${token}` };
  }

  function retryable(status: number, code: string): boolean {
    return (
      status === 503 &&
      ["too_many_streams", "database_busy", "database_unavailable", "shutting_down"].includes(code)
    );
  }

  async function connection(
    jobId: string,
    signal: AbortSignal,
    listener: (event: JobStreamEvent) => void | Promise<void>,
    cursor: { sequence: number | undefined },
  ): Promise<Exclude<JobStreamEnd, "aborted"> | null> {
    const headers: Record<string, string> = {
      Accept: "text/event-stream",
      ...(await credentials()),
    };
    if (cursor.sequence !== undefined) headers["Last-Event-ID"] = String(cursor.sequence);
    let response: Response;
    try {
      // Entity IDs are canonical UUIDv7 values and are safe as literal path segments.
      response = await fetchImpl(`${base}/api/v1/jobs/${jobId}/events`, {
        method: "GET",
        headers,
        signal,
        mode: "same-origin",
        credentials: programmatic ? "omit" : "same-origin",
        cache: "no-store",
        redirect: "error",
      });
    } catch {
      throw new ControlTransportError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      try {
        code = ((await response.json()) as Api.ErrorResponse).error.code;
      } catch {
        /* An unrecognized body still retains its HTTP status. */
      }
      throw new ControlTransportError(code, response.status, retryable(response.status, code));
    }
    const mediaType = response.headers.get("content-type")?.split(";")[0].trim().toLowerCase();
    if (mediaType !== "text/event-stream" || !response.body) {
      await response.body?.cancel();
      throw new ControlTransportError("invalid_event_stream", response.status, false);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const parser = new SseParser();
    try {
      for (;;) {
        let chunk: ReadableStreamReadResult<Uint8Array>;
        let idle: ReturnType<typeof setTimeout> | undefined;
        try {
          chunk = await Promise.race([
            reader.read(),
            new Promise<never>((_, reject) => {
              idle = setTimeout(() => {
                reject(new ControlTransportError("stream_idle", 0, true));
                void reader.cancel().catch(() => {});
              }, options.streamIdleMs ?? 45000);
            }),
          ]);
        } catch (error) {
          throw error instanceof ControlTransportError
            ? error
            : new ControlTransportError("connection_lost", 0, true);
        } finally {
          if (idle) clearTimeout(idle);
        }
        signal.throwIfAborted();
        // Discard an incomplete frame or UTF-8 tail at EOF. Neither was
        // acknowledged; the stored cursor will replay the entire output.
        if (chunk.done) return null;
        for (const frame of parser.push(decoder.decode(chunk.value, { stream: true }))) {
          signal.throwIfAborted();
          if ("comment" in frame) {
            if (frame.comment === "heartbeat") await listener({ type: "heartbeat" });
            continue;
          }
          let input: unknown;
          try {
            input = JSON.parse(frame.data);
          } catch {
            throw new ContractError("data", "a complete JSON event");
          }
          const event = projectJobEvent(frame.event, input);
          if (event.type === "output") {
            const acknowledged = event.output.seq;
            const previous = cursor.sequence ?? 0;
            if (acknowledged <= previous) continue;
            if (acknowledged !== previous + 1)
              throw new ContractError("output.seq", "the next stored sequence without a gap");
            await listener(event);
            // A rejected listener never acknowledges an unprocessed output.
            cursor.sequence = acknowledged;
          } else {
            await listener(event);
          }
          if (event.type === "settlement") {
            conversationListeners.forEach((listener) => listener());
            return "settled";
          }
          if (event.type === "stream_error") return "stream_error";
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {
        /* The read failure is reported above; cleanup cannot replace it. */
      }
      reader.releaseLock();
    }
  }

  type Snapshot = {
    topic: string;
    key: string | null;
    first: boolean;
    last: boolean;
    value: unknown;
  };
  function subscribeSnapshots(
    path: string | (() => string),
    receive: (snapshot: Snapshot) => void,
    onError: (error: unknown) => void,
    initialTopics?: readonly string[],
  ) {
    const controller = new AbortController();
    active.add(controller);
    const signal = controller.signal;
    void (async () => {
      let delay = 250;
      const runAttempt = async (
        lockName?: string,
      ): Promise<{ keepGoing: boolean; wait: number }> => {
        const attempt = streamAttempt(signal, lockName);
        const remaining = initialTopics ? new Set(initialTopics) : null;
        const started = new Set<string>();
        let initialComplete = false;
        let observationReady = false;
        let stalled = false;
        let progressTimer: ReturnType<typeof setTimeout> | undefined;
        let lineKind: "start" | "comment" | "event" = "start";
        const progress = () => {
          if (programmatic || (initialComplete && !observationReady)) return;
          if (progressTimer !== undefined) clearTimeout(progressTimer);
          progressTimer = setTimeout(() => {
            stalled = true;
            attempt.abort();
          }, options.streamIdleMs ?? 45000);
        };
        progress();
        try {
          const response = await fetchImpl(`${base}${typeof path === "string" ? path : path()}`, {
            headers: {
              Accept: "text/event-stream",
              ...(await credentials()),
            },
            signal: attempt.signal,
            mode: "same-origin",
            credentials: programmatic ? "omit" : "same-origin",
            cache: "no-store",
            redirect: "error",
          });
          if (!response.ok) {
            let code = "http_error";
            try {
              code = ((await response.json()) as Api.ErrorResponse).error.code;
            } catch {
              /* Retain the HTTP status. */
            }
            throw new ControlTransportError(
              code,
              response.status,
              retryable(response.status, code),
            );
          }
          if (
            !response.body ||
            response.headers.get("content-type")?.split(";")[0].trim() !== "text/event-stream"
          ) {
            await response.body?.cancel();
            throw new ControlTransportError("invalid_event_stream", response.status, false);
          }
          const reader = response.body.getReader();
          const parser = new SseParser();
          const decoder = new TextDecoder("utf-8", { fatal: true });
          try {
            for (;;) {
              let idle: ReturnType<typeof setTimeout> | undefined;
              let chunk: ReadableStreamReadResult<Uint8Array>;
              try {
                chunk = await Promise.race([
                  reader.read(),
                  new Promise<never>((_, reject) => {
                    idle = setTimeout(() => {
                      reject(new ControlTransportError("stream_idle", 0, true));
                      void reader.cancel().catch(() => {});
                    }, options.streamIdleMs ?? 45000);
                  }),
                ]);
              } finally {
                if (idle) clearTimeout(idle);
              }
              attempt.signal.throwIfAborted();
              if (chunk.done) {
                if (initialComplete && !observationReady) {
                  delay = 250;
                  return { keepGoing: true, wait: 100 };
                }
                break;
              }
              const text = decoder.decode(chunk.value, { stream: true });
              let advanced = false;
              for (const character of text) {
                if (character === "\n" || character === "\r") lineKind = "start";
                else {
                  if (lineKind === "start") lineKind = character === ":" ? "comment" : "event";
                  if (lineKind === "event") advanced = true;
                }
              }
              if (advanced) progress();
              for (const frame of parser.push(text)) {
                if ("comment" in frame) {
                  // The observation feed emits a heartbeat only after it has
                  // checked the journal and found no more changes to replay.
                  if (observationReady) {
                    observationReady = false;
                    if (progressTimer !== undefined) clearTimeout(progressTimer);
                    attempt.complete();
                  }
                  continue;
                }
                const data = JSON.parse(frame.data);
                if (frame.event === "stream_error") {
                  const error = (data as Api.ErrorResponse).error;
                  throw new ControlTransportError(error.code, 503, retryable(503, error.code));
                }
                if (frame.event !== "snapshot") continue;
                const snapshot = data as Snapshot;
                const key = `${snapshot.topic}:${snapshot.key ?? ""}`;
                if (snapshot.first) started.add(key);
                if (
                  !started.has(key) &&
                  !(initialComplete && snapshot.topic === "observation_changes")
                )
                  throw new ContractError("snapshot.first", "the first page of a snapshot");
                receive(snapshot);
                if (
                  observationReady &&
                  snapshot.topic === "observation_changes" &&
                  !snapshot.first
                ) {
                  observationReady = false;
                  if (progressTimer !== undefined) clearTimeout(progressTimer);
                  attempt.complete();
                }
                if (!initialComplete && snapshot.last && started.has(key)) {
                  remaining?.delete(key);
                  if (!remaining || remaining.size === 0) {
                    initialComplete = true;
                    observationReady = snapshot.topic === "observation_changes" && snapshot.first;
                    if (!observationReady) {
                      if (progressTimer !== undefined) clearTimeout(progressTimer);
                      attempt.complete();
                    }
                  }
                }
                if (snapshot.last) started.delete(key);
                delay = 250;
              }
            }
          } finally {
            await reader.cancel().catch(() => {});
            reader.releaseLock();
          }
          throw new ControlTransportError("connection_lost", 0, true);
        } catch (error) {
          if (signal.aborted) return { keepGoing: false, wait: 0 };
          if (attempt.rotated()) {
            delay = 250;
            return { keepGoing: true, wait: 100 };
          }
          if (stalled) error = new ControlTransportError("stream_idle", 0, true);
          onError(error);
          if (
            error instanceof ContractError ||
            (error instanceof ControlTransportError && !error.retryable)
          )
            return { keepGoing: false, wait: 0 };
          const wait = delay;
          delay = Math.min(delay * 2, 10000);
          return { keepGoing: true, wait };
        } finally {
          if (progressTimer !== undefined) clearTimeout(progressTimer);
          attempt.dispose();
        }
      };
      while (!signal.aborted) {
        const release = programmatic ? () => {} : await snapshotSlot(signal);
        let outcome: { keepGoing: boolean; wait: number };
        try {
          signal.throwIfAborted();
          const previouslyUnavailable = locksUnavailable;
          const locks = browserLocks();
          if (!previouslyUnavailable && locksUnavailable) restartSidebar();
          if (locks) {
            let entered = false;
            try {
              outcome = await withSnapshotLock(locks, signal, (name) => {
                entered = true;
                return runAttempt(name);
              });
            } catch (error) {
              if (signal.aborted || entered) throw error;
              locksUnavailable = true;
              restartSidebar();
              outcome = await runAttempt();
            }
          } else outcome = await runAttempt();
        } finally {
          release();
        }
        if (!outcome.keepGoing) break;
        await pause(outcome.wait, signal);
      }
    })()
      .catch((error) => {
        if (!signal.aborted) onError(error);
      })
      .finally(() => active.delete(controller));
    return () => controller.abort();
  }

  type Conversations = Api.Conversations;
  type Projects = ControlProjects;
  type Children = ControlChildren;
  type Observer<T> = {
    receive: (value: T) => void;
    error: (error: unknown) => void;
  };
  const catalogObservers = new Set<Observer<void>>();
  let stopCatalog: (() => void) | undefined;
  let catalogReady = false;
  let catalogError: unknown;
  const sidebarObservers = new Set<Observer<Conversations>>();
  const projectObservers = new Set<Observer<Projects>>();
  const childObservers = new Map<string, Map<Observer<Children>, number>>();
  let stopSidebar: (() => void) | undefined;
  let restartPending = false;
  let latestProjects: Projects | undefined;
  let latestConversations: Conversations | undefined;

  function subscribeCatalogInternal(receive: () => void, error: (error: unknown) => void) {
    const observer = { receive, error };
    catalogObservers.add(observer);
    if (!stopCatalog && !closed) {
      stopCatalog = subscribeSnapshots(
        "/api/v1/catalog/events",
        () => {
          catalogReady = true;
          catalogError = undefined;
          catalogObservers.forEach((item) => item.receive());
        },
        (cause) => {
          catalogReady = false;
          catalogError = cause;
          catalogObservers.forEach((item) => item.error(cause));
        },
      );
    } else if (catalogReady || catalogError !== undefined) {
      queueMicrotask(() => {
        if (!catalogObservers.has(observer)) return;
        if (catalogReady) observer.receive();
        else if (catalogError !== undefined) observer.error(catalogError);
      });
    }
    return () => {
      catalogObservers.delete(observer);
      if (!catalogObservers.size) {
        stopCatalog?.();
        stopCatalog = undefined;
        catalogReady = false;
        catalogError = undefined;
      }
    };
  }

  function finiteSidebar() {
    const controller = new AbortController();
    const signal = controller.signal;
    let dirty = true;
    let running = false;
    let retryDelay = 750;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const report = (error: unknown) => {
      sidebarObservers.forEach((observer) => observer.error(error));
      projectObservers.forEach((observer) => observer.error(error));
      childObservers.forEach((observers) =>
        observers.forEach((_, observer) => observer.error(error)),
      );
    };
    const advance = (before: string | undefined, next: string | null | undefined) => {
      if (!next || (before !== undefined && next >= before))
        throw new ContractError("next_before", "an advancing sidebar cursor");
      return next;
    };
    const schedule = (delay = 750) => {
      if (signal.aborted || running || timer !== undefined) return;
      timer = setTimeout(() => {
        timer = undefined;
        void refresh();
      }, delay);
    };
    const refresh = async () => {
      if (signal.aborted || running || !dirty) return;
      running = true;
      dirty = false;
      let delay = 750;
      try {
        let before: string | undefined;
        let projects: Projects["projects"] = [];
        for (;;) {
          const page = await readProjectPage({ before, limit: 100, signal });
          projects = [...projects, ...page.projects];
          if (!page.has_more) {
            if (signal.aborted) return;
            latestProjects = { ...page, projects };
            projectObservers.forEach((observer) => observer.receive(latestProjects!));
            break;
          }
          before = advance(before, page.next_before);
        }
        before = undefined;
        let conversations: Conversations["conversations"] = [];
        for (;;) {
          const query = `?limit=100${before ? `&before=${jobIdentity(before)}` : ""}`;
          const page = (await send("GET", `/api/v1/conversations${query}`, { signal }))
            .input as Conversations;
          conversations = [...conversations, ...page.conversations];
          if (!page.has_more) {
            if (signal.aborted) return;
            latestConversations = { ...page, conversations };
            sidebarObservers.forEach((observer) => observer.receive(latestConversations!));
            break;
          }
          before = advance(before, page.next_before);
        }
        for (const [id, observers] of childObservers) {
          const maxPages = Math.max(...observers.values());
          let visiblePages = 0;
          let childBefore: string | undefined;
          let children: Children["children"] = [];
          for (;;) {
            const query = `?limit=20${childBefore ? `&before=${jobIdentity(childBefore)}` : ""}`;
            const page = (
              await send("GET", `/api/v1/conversations/${jobIdentity(id)}/children${query}`, {
                signal,
              })
            ).input as Children;
            children = [...children, ...page.children];
            if (page.children.some((child) => child.origin !== "system")) visiblePages++;
            if (!page.has_more || visiblePages >= maxPages) {
              if (signal.aborted) return;
              observers.forEach((_, observer) => observer.receive({ ...page, children }));
              break;
            }
            childBefore = advance(childBefore, page.next_before);
          }
        }
        retryDelay = 750;
      } catch (error) {
        if (!signal.aborted) {
          dirty = true;
          delay = retryDelay;
          retryDelay = Math.min(retryDelay * 2, 10000);
          report(error);
        }
      } finally {
        running = false;
        if (dirty) schedule(delay);
      }
    };
    const unsubscribe = subscribeCatalogInternal(() => {
      dirty = true;
      schedule();
    }, report);
    schedule(0);
    return () => {
      controller.abort();
      if (timer !== undefined) clearTimeout(timer);
      unsubscribe();
    };
  }

  function restartSidebar() {
    if (restartPending) return;
    restartPending = true;
    queueMicrotask(() => {
      restartPending = false;
      stopSidebar?.();
      stopSidebar = undefined;
      if (closed || !(sidebarObservers.size || projectObservers.size || childObservers.size))
        return;
      if (!programmatic && !browserLocks()) {
        stopSidebar = finiteSidebar();
        return;
      }
      let conversations: Conversations["conversations"] = [];
      let projects: Projects["projects"] = [];
      const children = new Map<string, Children["children"]>();
      const query = [...childObservers]
        .map(([id, observers]) => `child=${jobIdentity(id)}:${Math.max(...observers.values())}`)
        .join("&");
      stopSidebar = subscribeSnapshots(
        `/api/v1/sidebar/events${query ? `?${query}` : ""}`,
        (snapshot) => {
          if (snapshot.topic === "conversations") {
            const page = snapshot.value as Api.Conversations;
            conversations = snapshot.first
              ? page.conversations
              : [...conversations, ...page.conversations];
            if (snapshot.last) {
              latestConversations = { ...page, conversations };
              sidebarObservers.forEach((observer) => observer.receive(latestConversations!));
            }
          } else if (snapshot.topic === "projects") {
            const page = snapshot.value as ControlProjects;
            projects = snapshot.first ? page.projects : [...projects, ...page.projects];
            if (snapshot.last) {
              latestProjects = { ...page, projects };
              projectObservers.forEach((observer) => observer.receive(latestProjects!));
            }
          } else if (snapshot.topic === "children" && snapshot.key) {
            const page = snapshot.value as ControlChildren;
            const items = snapshot.first
              ? page.children
              : [...(children.get(snapshot.key) ?? []), ...page.children];
            children.set(snapshot.key, items);
            if (snapshot.last)
              childObservers
                .get(snapshot.key)
                ?.forEach((_, observer) => observer.receive({ ...page, children: items }));
          } else throw new ContractError("snapshot.topic", "a sidebar topic");
        },
        (error) => {
          sidebarObservers.forEach((observer) => observer.error(error));
          projectObservers.forEach((observer) => observer.error(error));
          childObservers.forEach((observers) =>
            observers.forEach((_, observer) => observer.error(error)),
          );
        },
        [
          "projects:",
          "conversations:",
          ...[...childObservers.keys()].map((id) => `children:${id}`),
        ],
      );
    });
  }

  // One JSON request to one route of this API: no open proxy, no other
  // upstream, and every answer checked against its contract.
  async function send(
    method: "GET" | "POST" | "DELETE",
    path: string,
    settings: {
      body?: unknown;
      // The request's own `Idempotency-Key`, where a route names one.
      key?: string;
      // Whether the request changes state and so carries the session's
      // CSRF token in browser mode.
      sign?: boolean;
      signal?: AbortSignal;
      context?: string;
      revision?: number;
    },
  ): Promise<{ status: number; input: unknown }> {
    const headers: Record<string, string> = { ...(await credentials()) };
    if (settings.body !== undefined) headers["Content-Type"] = "application/json";
    if (settings.key !== undefined) headers["Idempotency-Key"] = settings.key;
    if (settings.context !== undefined)
      headers["X-OCG-Composer-Context"] = identity(settings.context, "context");
    if (settings.revision !== undefined) headers["If-Match"] = String(sequence(settings.revision));
    if (settings.sign) {
      const csrf = await options.csrfToken?.();
      if (!csrf) throw new ControlTransportError("credential_unavailable", 0, false);
      headers["X-OCG-CSRF"] = csrf;
    }
    let response: Response;
    try {
      response = await fetchImpl(`${base}${path}`, {
        method,
        headers,
        signal: settings.signal,
        ...(settings.body === undefined ? {} : { body: JSON.stringify(settings.body) }),
        mode: "same-origin",
        credentials: programmatic ? "omit" : "same-origin",
        cache: "no-store",
        redirect: "error",
      });
    } catch {
      throw new ControlTransportError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      let detail = "";
      try {
        const decoded = ((await response.json()) as Api.ErrorResponse).error;
        code = decoded.code;
        detail = decoded.message;
      } catch {
        /* An unrecognized body still retains its HTTP status. */
      }
      throw new ControlTransportError(
        code,
        response.status,
        retryable(response.status, code),
        detail,
      );
    }
    let input: unknown;
    try {
      input = await response.json();
    } catch {
      throw new ContractError("response", "a complete JSON answer");
    }
    return { status: response.status, input };
  }

  const readJobToolsInternal = async (
    job: string,
    settings: { after?: string; limit?: number; signal?: AbortSignal } = {},
  ): Promise<ControlJobToolsPage> => {
    jobIdentity(job);
    const query: string[] = [];
    if (settings.after !== undefined && settings.after !== null) {
      if (!/^[1-9][0-9]*:[0-9]+$/.test(settings.after))
        throw new ContractError("after", "a tool cursor <request>:<index>");
      query.push(`after=${settings.after}`);
    }
    if (settings.limit !== undefined) {
      if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      query.push(`limit=${settings.limit}`);
    }
    const suffix = query.length ? `?${query.join("&")}` : "";
    const answer = await send("GET", `/api/v1/jobs/${job}/tools${suffix}`, {
      signal: settings.signal,
    });
    return answer.input as Api.Tools;
  };

  async function readProjectPage(
    settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
  ) {
    const query: string[] = [];
    if (settings.before !== undefined) query.push(`before=${identity(settings.before, "before")}`);
    if (settings.limit !== undefined) {
      if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      query.push(`limit=${settings.limit}`);
    }
    const suffix = query.length ? `?${query.join("&")}` : "";
    return (
      await send("GET", `/api/v1/projects${suffix}`, {
        signal: settings.signal,
      })
    ).input as ControlProjects;
  }

  async function readProfilePage(settings: { before?: string; signal?: AbortSignal } = {}) {
    const suffix =
      settings.before === undefined ? "" : `?before=${identity(settings.before, "before")}`;
    return projectProfiles(
      (
        await send("GET", `/api/v1/profiles${suffix}`, {
          signal: settings.signal,
        })
      ).input as Api.Profiles,
    );
  }

  async function readAllProfiles(signal?: AbortSignal) {
    const pages: Array<ReturnType<typeof projectProfiles>> = [];
    const providers = new Map<string, ReturnType<typeof projectProfiles>["providers"][number]>();
    const executors = new Map<
      string,
      ReturnType<typeof projectProfiles>["executor_protocols"][number]
    >();
    let before: string | undefined;
    for (;;) {
      const page = await readProfilePage({ before, signal });
      pages.push(page);
      for (const executor of page.executor_protocols) {
        const native =
          (page as typeof page & { resource_model?: string }).resource_model === "independent";
        executors.set(native ? `${executor.id}/${executor.protocol}` : executor.id, executor);
      }
      for (const provider of page.providers) {
        const key = provider.id;
        const merged = providers.get(key);
        if (!merged) {
          providers.set(key, {
            ...provider,
            profile_ids: [...provider.profile_ids],
            models: [...provider.models],
          });
          continue;
        }
        for (const id of provider.profile_ids) {
          if (!merged.profile_ids.includes(id)) merged.profile_ids.push(id);
        }
        for (const model of provider.models) {
          if (!merged.models.includes(model)) merged.models.push(model);
        }
        merged.has_credential ||= provider.has_credential;
      }
      if (!page.has_more) break;
      if (!page.next_before || (before !== undefined && page.next_before >= before)) {
        throw new ContractError("next_before", "an advancing Profile cursor");
      }
      before = page.next_before;
    }
    const first = pages[0];
    const native =
      (first as (typeof first & { resource_model?: string }) | undefined)?.resource_model ===
      "independent";
    return {
      profile_id: first?.profile_id ?? null,
      llm_profiles: pages
        .flatMap((page) => page.llm_profiles)
        .map((profile) => ({
          ...profile,
          executors:
            native && !profile.enabled
              ? []
              : [...executors.values()]
                  .filter((executor) => executor.protocol === profile.protocol)
                  .map((executor) => executor.id)
                  .sort(),
        })),
      agent_profiles: pages.flatMap((page) => page.agent_profiles),
      decision_profiles: pages.flatMap((page) => page.decision_profiles),
      decision_agent_profiles: pages.flatMap((page) => page.decision_agent_profiles),
      executor_protocols: [...executors.values()],
      providers: [...providers.values()],
      has_more: false,
      next_before: null,
    };
  }

  return {
    async currentMutableJob(context: string, signal?: AbortSignal): Promise<MutableJob | null> {
      try {
        return mutableJob(
          (
            await send("GET", "/api/v1/jobs/current", {
              context,
              signal,
            })
          ).input,
          context,
        );
      } catch (error) {
        if (
          error instanceof ControlTransportError &&
          error.status === 404 &&
          error.code === "not_found"
        )
          return null;
        throw error;
      }
    },
    async readMutableJob(context: string, job: string, signal?: AbortSignal): Promise<MutableJob> {
      return mutableJob(
        editableJob(
          (
            await send("GET", `/api/v1/jobs/${jobIdentity(job)}`, {
              context,
              signal,
            })
          ).input,
        ),
        context,
      );
    },
    async createMutableJob(
      context: string,
      input: MutableJobInput,
      key: string,
      association: { conversation?: string; project?: string } = {},
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", "/api/v1/jobs", {
            context,
            body: { ...input, ...association },
            key: submissionKey(key),
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async updateMutableJob(
      context: string,
      job: string,
      revision: number,
      input: MutableJobInput,
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}`, {
            context,
            body: { revision: sequence(revision), input },
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async submitMutableJob(
      context: string,
      job: string,
      revision: number,
      input: MutableJobInput,
      key: string,
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}/submit`, {
            context,
            body: { revision: sequence(revision), input },
            key: submissionKey(key),
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async discardMutableJob(context: string, job: string, revision: number): Promise<void> {
      await send("DELETE", `/api/v1/jobs/${jobIdentity(job)}`, {
        context,
        revision,
        sign: !programmatic,
      });
    },
    async cancelUnsupportedQueuedJob(
      context: string,
      job: string,
      revision: number,
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}/cancel-unsupported`, {
            context,
            body: { revision: sequence(revision) },
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    subscribeConversations(
      receive: Observer<Conversations>["receive"],
      error: Observer<Conversations>["error"],
    ) {
      const observer = { receive, error };
      sidebarObservers.add(observer);
      if (latestConversations) receive(latestConversations);
      restartSidebar();
      return () => {
        sidebarObservers.delete(observer);
        restartSidebar();
      };
    },
    subscribeProjects(receive: Observer<Projects>["receive"], error: Observer<Projects>["error"]) {
      const observer = { receive, error };
      projectObservers.add(observer);
      if (latestProjects) receive(latestProjects);
      restartSidebar();
      return () => {
        projectObservers.delete(observer);
        restartSidebar();
      };
    },
    subscribeChildren(
      conversation: string,
      pages: number,
      receive: Observer<Children>["receive"],
      error: Observer<Children>["error"],
    ) {
      jobIdentity(conversation);
      const observer = { receive, error };
      const observers = childObservers.get(conversation) ?? new Map<Observer<Children>, number>();
      observers.set(observer, Math.min(Math.max(pages, 1), 100));
      childObservers.set(conversation, observers);
      restartSidebar();
      return () => {
        observers.delete(observer);
        if (!observers.size) childObservers.delete(conversation);
        restartSidebar();
      };
    },
    subscribeLogs(
      file: string | undefined,
      receive: (value: Api.Logs) => void,
      error: (error: unknown) => void,
    ) {
      const query = file ? `?file=${encodeURIComponent(file)}` : "";
      return subscribeSnapshots(
        `/api/v1/logs/events${query}`,
        (snapshot) => {
          receive(snapshot.value as Api.Logs);
        },
        error,
      );
    },
    subscribeCatalog(receive: () => void, error: (error: unknown) => void) {
      return subscribeCatalogInternal(receive, error);
    },
    subscribeObservationChanges(
      receive: (value: {
        ready: boolean;
        usage: Api.Usage["observations"];
        availability: Api.Availability["observations"];
      }) => void,
      reset: () => void,
      error: (error: unknown) => void,
    ) {
      let cursor: string | undefined;
      let stopped = false;
      let stop: (() => void) | undefined;
      const start = () => {
        if (stopped) return;
        stop = subscribeSnapshots(
          () =>
            `/api/v1/observation-changes/events${cursor ? `?since=${encodeURIComponent(cursor)}` : ""}`,
          (snapshot) => {
            const value = snapshot.value as {
              usage: Api.Usage["observations"];
              availability: Api.Availability["observations"];
            };
            const { usage, availability } = value;
            receive({ ready: snapshot.first, usage, availability });
            cursor = snapshot.key ?? undefined;
          },
          (cause) => {
            if (
              cause instanceof ControlTransportError &&
              cause.code === "observation_replay_lost"
            ) {
              cursor = undefined;
              reset();
              queueMicrotask(start);
            } else error(cause);
          },
        );
      };
      start();
      return () => {
        stopped = true;
        stop?.();
      };
    },
    onProfilesChanged(listener: (change?: { availability: string }) => void) {
      profileListeners.add(listener);
      return () => {
        profileListeners.delete(listener);
      };
    },

    onConversationsChanged(listener: () => void) {
      conversationListeners.add(listener);
      return () => {
        conversationListeners.delete(listener);
      };
    },

    async readConversations(
      settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
    ) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      return (
        await send("GET", `/api/v1/conversations?${query.join("&")}`, {
          signal: settings.signal,
        })
      ).input as Api.Conversations;
    },

    /**
     * Full-text conversation search: eligible titles, messages, assistant replies and attachment names,
     * newest first. Terms are the whitespace-separated words of `query`,
     * combined by AND; matching is case-insensitive substring per term.
     */
    async searchConversations(settings: {
      query: string;
      before?: string;
      limit?: number;
      signal?: AbortSignal;
    }) {
      const trimmed = settings.query.trim();
      if (!trimmed) throw new ContractError("query", "a non-empty search query");
      if (new TextEncoder().encode(trimmed).length > 500)
        throw new ContractError("query", "a search query of at most 500 UTF-8 bytes");
      const query = [`query=${encodeURIComponent(trimmed)}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      query.push(`limit=${settings.limit ?? 50}`);
      return (
        await send("GET", `/api/v1/conversations/search?${query.join("&")}`, {
          signal: settings.signal,
        })
      ).input as Api.Conversations;
    },

    /** Renames a conversation or hides it for good. */
    async editConversation(root: string, edit: ControlConversationEdit, signal?: AbortSignal) {
      jobIdentity(root);
      const answer = await send("POST", `/api/v1/conversations/${root}`, {
        body: edit,
        sign: true,
        signal,
      });
      const result = (answer.input as Api.ConversationEdited).conversation;
      conversationListeners.forEach((listener) => listener());
      return result;
    },

    async readContextStatus(
      conversation: string,
      settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
    ) {
      const suffix = `?limit=${settings.limit ?? 50}${settings.before ? `&before=${jobIdentity(settings.before)}` : ""}`;
      return projectContextStatus(
        (
          await send("GET", `/api/v1/conversations/${jobIdentity(conversation)}/context${suffix}`, {
            signal: settings.signal,
          })
        ).input as Api.ContextStatus,
      );
    },
    async compactConversation(conversation: string, model: string, key: string) {
      return (
        await send("POST", `/api/v1/conversations/${jobIdentity(conversation)}/compact`, {
          body: { model: identity(model, "model") },
          key: submissionKey(key),
          sign: true,
        })
      ).input as Api.Compaction;
    },

    async readConversationLimits(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return (
        await send("GET", `/api/v1/conversations/${conversation}/limits`, {
          signal,
        })
      ).input as Api.ConversationLimits;
    },

    async readJobs(
      settings: { before?: string; limit?: number; query?: string; signal?: AbortSignal } = {},
    ) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      if (settings.query?.trim()) query.push(`query=${encodeURIComponent(settings.query.trim())}`);
      return (
        await send("GET", `/api/v1/job-history?${query.join("&")}`, {
          signal: settings.signal,
        })
      ).input as Api.Jobs;
    },

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

    /** What the host establishes about itself and its database, now. */
    async readHealth(signal?: AbortSignal) {
      return (await send("GET", "/api/v1/health", { signal })).input as Api.Health;
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

    /** The Job's persisted lifecycle record. */
    async readJob(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return projectJob(
        (await send("GET", `/api/v1/jobs/${job}`, { signal })).input as Api.JobDetails,
      );
    },

    /**
     * The Conversation an identity belongs to. A Conversation identity is read as a
     * Conversation and only an unknown one is then read as a Job: the two are different
     * records. An identity that is neither is refused.
     */
    async resolveConversation(id: string, signal?: AbortSignal): Promise<string> {
      jobIdentity(id);
      // The UI addresses a Conversation by its own identity, so that is read first; only an
      // identity the Conversation read does not know is looked up as a Job.
      try {
        await send("GET", `/api/v1/conversations/${id}/turns?limit=1`, { signal });
        return id;
      } catch (cause) {
        if (
          !(
            cause instanceof ControlTransportError &&
            cause.status === 404 &&
            cause.code === "conversation_not_found"
          )
        )
          throw cause;
      }
      const record = projectJob(
        (await send("GET", `/api/v1/jobs/${id}`, { signal })).input as Api.JobDetails,
      );
      if (record.kind === "conversation") return record.id;
      if (!record.conversation) throw new Error("This Job is a process turn with no conversation.");
      return record.conversation;
    },

    async readJobDetails(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return projectJobDetails(
        (await send("GET", `/api/v1/jobs/${job}`, { signal })).input as Api.JobDetails,
      );
    },

    /** The Job's frozen input: its objective as text, never inferred from its identity. */
    async readJobInput(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      const answer = (await send("GET", `/api/v1/jobs/${job}/input`, { signal }))
        .input as Api.JobInput;
      return { ...answer.input, job: answer.job };
    },

    /** The bounded conversation ending at `job`, reconstructed from persisted Job `previous` links. */
    async readConversationTurns(
      conversation: string,
      before?: string,
      limit = 20,
      signal?: AbortSignal,
    ) {
      jobIdentity(conversation);
      if (before) jobIdentity(before);
      const query = `?limit=${sequence(limit)}${before ? `&before=${before}` : ""}`;
      const answer = await send("GET", `/api/v1/conversations/${conversation}/turns${query}`, {
        signal,
      });
      return projectConversation(answer.input as Api.Turns);
    },

    async readChildren(conversation: string, before?: string, limit = 20, signal?: AbortSignal) {
      jobIdentity(conversation);
      if (before) jobIdentity(before);
      const query = `?limit=${sequence(limit)}${before ? `&before=${before}` : ""}`;
      return (
        await send("GET", `/api/v1/conversations/${conversation}/children${query}`, { signal })
      ).input as ControlChildren;
    },
    /** The tree of the work below a Job: its ancestors, and the nodes below it. */
    async readJobTree(
      job: string,
      settings: {
        after?: string;
        limit?: number;
        depth?: number;
        signal?: AbortSignal;
      } = {},
    ) {
      jobIdentity(job);
      const limit = settings.limit ?? 50;
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      const depth = settings.depth ?? 2;
      if (!Number.isSafeInteger(depth) || depth < 1 || depth > TREE_MAX_DEPTH) {
        throw new ContractError("depth", `an integer from 1 to ${TREE_MAX_DEPTH}`);
      }
      const query = [`limit=${limit}`, `depth=${depth}`];
      if (settings.after) query.push(`after=${jobIdentity(settings.after)}`);
      const answer = await send("GET", `/api/v1/jobs/${job}/tree?${query.join("&")}`, {
        signal: settings.signal,
      });
      return answer.input as import("../contracts").ControlJobTree;
    },
    async readCards(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return (
        await send("GET", `/api/v1/conversations/${conversation}/cards`, {
          signal,
        })
      ).input as Api.Cards;
    },
    async decideApproval(
      approval: ControlApproval,
      decision: { decision: "approve" | "reject" } | { answer: string },
      signal?: AbortSignal,
    ) {
      jobIdentity(approval[0]);
      sequence(approval[1]);
      sequence(approval[2]);
      const [turn, request, index] = approval;
      const result = (
        await send("POST", `/api/v1/approvals/${turn}/${request}/${index}`, {
          body: decision,
          sign: true,
          signal,
        })
      ).input as Api.ApprovalDecision;
      conversationListeners.forEach((listener) => listener());
      return result;
    },
    async releaseSlot(turn: string, confirm = false, signal?: AbortSignal) {
      jobIdentity(turn);
      const result = (
        await send("POST", `/api/v1/jobs/${turn}/release-slot`, {
          body: confirm ? { confirm: true } : {},
          sign: true,
          signal,
        })
      ).input as Api.ReleaseSlot;
      conversationListeners.forEach((listener) => listener());
      return result;
    },
    async holdConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const result = (
        await send("POST", `/api/v1/conversations/${conversation}/hold`, {
          body: {},
          sign: true,
          signal,
        })
      ).input as Api.Stop;
      conversationListeners.forEach((listener) => listener());
      return result;
    },
    async unholdConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const result = (
        await send("POST", `/api/v1/conversations/${conversation}/unhold`, {
          body: {},
          sign: true,
          signal,
        })
      ).input as Api.Stop;
      conversationListeners.forEach((listener) => listener());
      return result;
    },

    async amendQueuedMessage(
      conversation: string,
      amendment:
        | {
            action: "edit";
            turn: string;
            message: number;
            text: string;
            attachments: string[];
          }
        | { action: "withdraw"; turn: string; message: number },
      signal?: AbortSignal,
    ) {
      jobIdentity(conversation);
      jobIdentity(amendment.turn);
      sequence(amendment.message);
      const result = (
        await send("POST", `/api/v1/conversations/${conversation}/queue`, {
          body: amendment,
          sign: true,
          signal,
        })
      ).input as Api.Envelope<Api.Append>;
      return result;
    },
    async retryConversation(conversation: string, turn: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      jobIdentity(turn);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/retry`, {
        body: { turn },
        sign: true,
        signal,
      });
      const result = answer.input as Api.Envelope<Api.Append>;
      conversationListeners.forEach((listener) => listener());
      return result;
    },

    /** One page of Agent Profiles with their LLM targets and the Executors that can run each. */
    async readAgentProfiles(
      settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
    ) {
      const query: string[] = [];
      if (settings.before !== undefined)
        query.push(`before=${identity(settings.before, "before")}`);
      if (settings.limit !== undefined) {
        if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
          throw new ContractError("limit", "an integer from 1 to 100");
        }
        query.push(`limit=${settings.limit}`);
      }
      const suffix = query.length ? `?${query.join("&")}` : "";
      return projectAgentProfiles(
        (
          await send("GET", `/api/v1/agent-profiles${suffix}`, {
            signal: settings.signal,
          })
        ).input as Api.AgentProfiles,
      );
    },

    /** One page of Projects, newest first. */
    readProjects: readProjectPage,

    /** Every Project, newest first. */
    async readAllProjects(signal?: AbortSignal) {
      const projects: ControlProject[] = [];
      let before: string | undefined;
      for (;;) {
        const page = await readProjectPage({ before, limit: 100, signal });
        projects.push(...page.projects);
        if (!page.has_more) return projects;
        if (!page.next_before || page.next_before === before)
          throw new Error("Project cursor did not advance.");
        before = page.next_before;
      }
    },

    /** Creates a new Project. */
    async createProject(body: ControlProjectCreate, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/projects", {
        body,
        sign: true,
        signal,
      });
      return (answer.input as Api.ProjectAnswer).project as ControlProject;
    },

    /** Updates an existing Project. */
    async updateProject(projectId: string, body: ControlProjectUpdate, signal?: AbortSignal) {
      identity(projectId, "project");
      const answer = await send("POST", `/api/v1/projects/${projectId}`, {
        body,
        sign: true,
        signal,
      });
      return (answer.input as Api.ProjectAnswer).project as ControlProject;
    },

    async configureProjectDecision(
      projectId: string,
      decisionProfile: string | null,
      minimumConfidence: number,
      enabled: boolean,
      signal?: AbortSignal,
    ) {
      identity(projectId, "project");
      return (
        (
          await send("POST", `/api/v1/projects/${projectId}/decision`, {
            body: {
              enabled,
              decision_profile: decisionProfile,
              minimum_confidence: minimumConfidence,
            },
            sign: true,
            signal,
          })
        ).input as Api.ProjectAnswer
      ).project as ControlProject;
    },

    async readPools(settings: { before?: string; signal?: AbortSignal } = {}) {
      const query = settings.before ? `?before=${encodeURIComponent(settings.before)}` : "";
      const answer = await send("GET", `/api/v2/pools${query}`, {
        signal: settings.signal,
      });
      const page = answer.input as Api.Envelope<ControlPools>;
      if (
        page.api_version !== 2 ||
        !Array.isArray(page.pools) ||
        page.pools.length > 20 ||
        typeof page.has_more !== "boolean"
      ) {
        throw new ContractError("pools", "a version 2 Pool page");
      }
      if (page.has_more) {
        if (typeof page.next_before !== "string") {
          throw new ContractError("next_before", "an advancing Pool cursor");
        }
        identity(page.next_before, "next_before");
        if (settings.before !== undefined && page.next_before >= settings.before) {
          throw new ContractError("next_before", "an advancing Pool cursor");
        }
      } else if (page.next_before !== null) {
        throw new ContractError("next_before", "a terminal Pool cursor");
      }
      return (answer.input as { resource_model?: string }).resource_model === "independent"
        ? { ...page, pools: page.pools.map(nativePool) }
        : page;
    },

    /** Updates a Pool's concurrency limit or enabled state. */
    async updatePool(poolId: string, body: ControlPoolUpdate, signal?: AbortSignal) {
      identity(poolId, "pool");
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
        body,
        sign: true,
        signal,
      });
      const response = answer.input as Api.PoolAnswer & {
        resource_model?: string;
      };
      return response.resource_model === "independent"
        ? nativePool(response.pool)
        : (response.pool as ControlPools["pools"][number]);
    },

    async createNativePool(id: string, name: string, capacity: number) {
      identity(id, "pool");
      if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4294967295)
        throw new ContractError("capacity", "an integer from 1 to 4294967295");
      const result = await send("POST", "/api/v1/pools", {
        body: { id, name, capacity, enabled: true },
        sign: true,
      });
      const response = result.input as {
        api_version?: number;
        resource_model?: string;
        pool?: unknown;
      };
      if (response.api_version !== 1 || response.resource_model !== "independent")
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
      for (const executor of page.executors) {
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
          throw new ContractError("executor.capabilities", "supported Executor capabilities");
      }
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

    /** Every Profile (LLM Profiles, Agent Profiles) and the merged Provider projection. */
    async readProfiles(signal?: AbortSignal) {
      return readAllProfiles(signal);
    },

    async discoverProviderModels(
      connection: {
        protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
        endpoint: string;
        credential_ref?: string | null;
        credential?: string;
        headers?: Api.OutboundHeaderInput[];
        /** A saved Profile of this Provider, whose stored headers are probed. */
        profile_id?: string;
      },
      signal?: AbortSignal,
    ) {
      const answer = await send("POST", "/api/v1/provider-models", {
        body: connection,
        sign: true,
        signal,
      });
      const result = answer.input as Api.ProviderModels;
      profileListeners.forEach((listener) => listener());
      return result;
    },

    async importModels(
      source:
        | string
        | {
            protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
            name: string;
            endpoint: string;
            credential?: string;
            credential_ref?: string;
            headers?: Api.OutboundHeaderInput[];
          },
      models: Api.ProviderModel[],
      capabilities: ControlCapability[],
    ) {
      if (typeof source === "string") identity(source, "source_profile_id");
      const connection =
        typeof source === "string" ? { source_profile_id: source } : { provider: source };
      const answer = await send("POST", "/api/v1/profiles/import", {
        body: { ...connection, models, declared_capabilities: capabilities },
        sign: true,
      });
      const result = answer.input as Api.ImportedModels;
      profileListeners.forEach((listener) => listener());
      return result;
    },

    async readActiveSystemSettings(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/settings/system", { signal });
      return answer.input as Api.ActiveSystemSettings;
    },

    async readGlobalCompactionSettings(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/settings/compaction", {
        signal,
      });
      return answer.input as Api.GlobalCompactionSettings;
    },

    async saveGlobalCompactionSettings(settings: Api.GlobalCompactionSettings) {
      const answer = await send("POST", "/api/v1/settings/compaction", {
        body: {
          revision: settings.revision,
          prediction_enabled: settings.prediction_enabled,
          policy: settings.policy,
        },
        sign: true,
      });
      return answer.input as Api.GlobalCompactionSettings;
    },

    async readGlobalOutboundHeaders(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/settings/outbound-headers", {
        signal,
      });
      return answer.input as Api.GlobalOutboundHeaders;
    },

    /** Replaces the global headers; `revision` is the one last read. */
    async saveGlobalOutboundHeaders(revision: number, headers: Api.OutboundHeaderInput[]) {
      const answer = await send("POST", "/api/v1/settings/outbound-headers", {
        body: { revision, headers },
        sign: true,
      });
      return answer.input as Api.GlobalOutboundHeaders;
    },

    async deleteLlmProfile(profileId: string) {
      identity(profileId, "profile");
      const answer = await send("DELETE", `/api/v1/profiles/${profileId}`, {
        sign: true,
      });
      projectProfiles(answer.input as Api.Profiles);
      profileListeners.forEach((listener) => listener());
      return readAllProfiles();
    },

    async setModelEnabled(profileId: string, enabled: boolean) {
      identity(profileId, "profile");
      await send("POST", `/api/v1/profiles/${profileId}/availability`, {
        body: { enabled },
        sign: true,
      });
      profileListeners.forEach((listener) => listener({ availability: profileId }));
    },

    async removeProvider(providerId: string) {
      identity(providerId, "provider");
      await send("DELETE", `/api/v1/providers/${providerId}`, { sign: true });
      profileListeners.forEach((listener) => listener());
    },

    /** Creates an LLM Profile or Agent Profile and returns the refreshed catalog. */
    async createProfile(profile: ControlProfileWriteRequest, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/profiles", {
        body: profile,
        sign: true,
        signal,
      });
      const saved = projectProfiles(answer.input as Api.Profiles).profile_id;
      const snapshot = await readAllProfiles(signal);
      profileListeners.forEach((listener) => listener());
      return { ...snapshot, profile_id: saved };
    },

    /** Updates an existing LLM Profile or Agent Profile and returns the refreshed catalog. */
    async updateProfile(
      profileId: string,
      profile: ControlProfileWriteRequest,
      signal?: AbortSignal,
    ) {
      identity(profileId, "profile");
      const answer = await send("POST", `/api/v1/profiles/${profileId}`, {
        body: profile,
        sign: true,
        signal,
      });
      const saved = projectProfiles(answer.input as Api.Profiles).profile_id;
      const snapshot = await readAllProfiles(signal);
      profileListeners.forEach((listener) => listener());
      return { ...snapshot, profile_id: saved };
    },

    /** One stored page of the Job's outputs, in `seq` order. */
    async readOutputs(
      job: string,
      settings: {
        afterSeq?: number;
        limit?: number;
        signal?: AbortSignal;
      } = {},
    ) {
      jobIdentity(job);
      const query: string[] = [];
      if (settings.afterSeq !== undefined) query.push(`after_seq=${sequence(settings.afterSeq)}`);
      if (settings.limit !== undefined) {
        if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
          throw new ContractError("limit", "an integer from 1 to 100");
        }
        query.push(`limit=${settings.limit}`);
      }
      const suffix = query.length ? `?${query.join("&")}` : "";
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs${suffix}`, {
        signal: settings.signal,
      });
      return answer.input as Api.Outputs;
    },

    async readLatestOutput(job: string, signal?: AbortSignal): Promise<ControlOutput | null> {
      jobIdentity(job);
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs/latest`, {
        signal,
      });
      const page = answer.input as Api.Outputs;
      return page.outputs[0] ?? null;
    },

    async readJobTool(
      job: string,
      request: number,
      index: number,
      signal?: AbortSignal,
    ): Promise<ControlToolCall> {
      jobIdentity(job);
      if (sequence(request) === 0 || request > 0xffffffff || sequence(index) > 0xffffffff)
        throw new ContractError("tool", "a positive request and unsigned index");
      const answer = await send("GET", `/api/v1/jobs/${job}/tools/${request}/${index}`, { signal });
      const page = answer.input as Api.Tools;
      const tool = page.tools[0];
      if (
        page.tools.length !== 1 ||
        !tool ||
        tool.request !== request ||
        tool.index !== index ||
        tool.arguments_cut ||
        tool.result_cut
      )
        throw new ContractError("tool", "the complete requested tool evidence");
      return tool;
    },

    /** Reads the timestamp of the response that introduced a tool call. */
    async readToolResponseTime(job: string, request: number, signal?: AbortSignal) {
      jobIdentity(job);
      if (!Number.isSafeInteger(request) || request < 1)
        throw new ContractError("request", "a positive request number");
      const after = request === 1 ? "" : `&after=${request - 1}`;
      const page = (
        await send("GET", `/api/v1/jobs/${job}/responses?limit=1${after}`, {
          signal,
        })
      ).input as Api.ProviderResponses;
      const response = page.responses[0];
      return response?.request === request && response.requests_tools
        ? response.created_at_us
        : null;
    },

    /** Reassembles reply chunks; an incomplete 50-page walk is refused. */
    async readAllProviderResponses(
      job: string,
      signal?: AbortSignal,
      after?: number,
    ): Promise<ControlProviderResponse[]> {
      jobIdentity(job);
      const responses: ControlProviderResponse[] = [];
      let cursor = after;
      let offset = 0;
      let partial: ControlProviderResponse | null = null;
      let partialBytes = 0;
      for (let pageCount = 0; pageCount < 50; pageCount++) {
        const query = `?limit=100${cursor === undefined ? "" : `&after=${sequence(cursor)}`}${offset ? `&text_offset=${offset}` : ""}`;
        const page = (await send("GET", `/api/v1/jobs/${job}/responses${query}`, { signal }))
          .input as Api.ProviderResponses;
        for (const chunk of page.responses) {
          if (chunk.text_offset === 0) {
            if (partial || chunk.request <= (cursor ?? 0))
              throw new ContractError("responses.request", "a request after the cursor");
            partial = {
              request: chunk.request,
              text: "",
              requests_tools: chunk.requests_tools,
              created_at_us: chunk.created_at_us,
            };
            partialBytes = 0;
          }
          if (
            !partial ||
            chunk.request !== partial.request ||
            chunk.text_offset !== partialBytes ||
            chunk.requests_tools !== partial.requests_tools ||
            chunk.created_at_us !== partial.created_at_us
          )
            throw new ContractError("responses.text_offset", "contiguous chunks of the same reply");
          partial.text += chunk.text;
          partialBytes += new TextEncoder().encode(chunk.text).length;
          if (chunk.text_complete) {
            responses.push(partial);
            partial = null;
          }
        }
        if (!page.has_more) return responses;
        if (
          page.next_after === null ||
          page.next_text_offset === null ||
          page.next_after < (cursor ?? 0) ||
          (page.next_after === cursor && page.next_text_offset <= offset)
        )
          throw new ContractError("next_after", "an advancing reply cursor");
        cursor = page.next_after;
        offset = page.next_text_offset;
      }
      throw new ContractError("responses", "complete provider replies within 50 pages");
    },

    /** All recorded tool calls for a Job, paging through until complete. */
    async readAllJobTools(job: string, signal?: AbortSignal): Promise<ControlToolCall[]> {
      const tools: ControlToolCall[] = [];
      let cursor: string | undefined = undefined;
      for (let pageCount = 0; pageCount < 50; pageCount++) {
        const page = await readJobToolsInternal(job, {
          after: cursor,
          limit: 100,
          signal,
        });
        tools.push(...page.tools);
        if (!page.has_more || !page.next_after) {
          break;
        }
        cursor = page.next_after;
      }
      return tools;
    },

    /** Uploads a PNG, JPEG, GIF or WebP image to the Attachment store. */
    async uploadAttachment(
      payload: {
        mime: "image/png" | "image/jpeg" | "image/gif" | "image/webp";
        data_base64: string;
        display_name?: string;
      },
      signal?: AbortSignal,
    ): Promise<ControlAttachment> {
      const answer = await send("POST", "/api/v1/attachments", {
        body: payload,
        sign: true,
        signal,
      });
      return (answer.input as { attachment: ControlAttachment }).attachment;
    },

    /** Reads metadata of an image Attachment. */
    async readAttachment(attachmentId: string, signal?: AbortSignal): Promise<ControlAttachment> {
      identity(attachmentId, "attachment");
      const answer = await send("GET", `/api/v1/attachments/${attachmentId}`, {
        signal,
      });
      return (answer.input as { attachment: ControlAttachment }).attachment;
    },

    /** URL to fetch the raw image content of an Attachment. */
    attachmentContentUrl(attachmentId: string): string {
      identity(attachmentId, "attachment");
      return `${base}/api/v1/attachments/${attachmentId}/content`;
    },

    /** Requests the stop of one Job this listener admitted. */
    async stopConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/stop`, {
        body: {},
        sign: true,
        signal,
      });
      return answer.input as Api.Stop;
    },
    async closeConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/close`, {
        body: {},
        sign: true,
        signal,
      });
      return answer.input as Api.Stop;
    },

    /** Signs the browser in under this run's pairing code. */
    async createSession(pairingCode: string, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/session", {
        body: { pairing_code: pairingCode },
        signal,
      });
      const session = (answer.input as Api.SessionResponse).session;
      if (!session.authenticated) throw new ContractError("session", "a signed-in session");
      return {
        csrfToken: session.csrf_token,
        expiresAtUs: session.expires_at_us,
      };
    },

    /** The session this browser holds, if it is still authorized. */
    async readSession(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/session", { signal });
      const session = (answer.input as Api.SessionResponse).session;
      if (!session.authenticated) throw new ContractError("session", "a signed-in session");
      return {
        csrfToken: session.csrf_token,
        expiresAtUs: session.expires_at_us,
      };
    },

    /** Revokes this browser's session. */
    async deleteSession(signal?: AbortSignal) {
      await send("DELETE", "/api/v1/session", { sign: true, signal });
    },

    subscribeJobEvents(
      jobId: string,
      listener: (event: JobStreamEvent) => void | Promise<void>,
      settings: {
        signal?: AbortSignal;
        // Only seed from output the canonical consumer has already processed.
        afterSeq?: number;
        onReconnect?: (error: ControlTransportError) => void | Promise<void>;
      } = {},
    ): JobStreamSubscription {
      if (closed) throw new ControlTransportError("client_closed", 0, false);
      jobIdentity(jobId);
      // Each consumer acknowledges its own output; another view cannot skip it.
      const cursor = {
        sequence: settings.afterSeq === undefined ? undefined : sequence(settings.afterSeq),
      };
      const controller = new AbortController();
      const signal = controller.signal;
      const stop = () => controller.abort();
      settings.signal?.addEventListener("abort", stop, { once: true });
      if (settings.signal?.aborted) stop();
      active.add(controller);
      const completed = (async (): Promise<JobStreamEnd> => {
        let delay = 250;
        try {
          while (!signal.aborted) {
            let failure = new ControlTransportError("premature_end", 0, true);
            const attempt = streamAttempt(signal);
            try {
              const end = await connection(jobId, attempt.signal, listener, cursor);
              if (end) return end;
            } catch (error) {
              if (signal.aborted) return "aborted";
              if (attempt.rotated()) {
                delay = 250;
                await pause(100, signal);
                continue;
              }
              if (!(error instanceof ControlTransportError) || !error.retryable) throw error;
              failure = error;
            } finally {
              attempt.dispose();
            }
            if (signal.aborted) return "aborted";
            await settings.onReconnect?.(failure);
            await pause(delay, signal);
            delay = Math.min(delay * 2, 10_000);
          }
          return "aborted";
        } finally {
          controller.abort();
          settings.signal?.removeEventListener("abort", stop);
          active.delete(controller);
        }
      })();
      return { closed: completed, close: stop };
    },

    close(): void {
      closed = true;
      stopSidebar?.();
      sidebarObservers.clear();
      projectObservers.clear();
      childObservers.clear();
      profileListeners.clear();
      conversationListeners.clear();
      for (const controller of active.values()) controller.abort();
    },
  };
}
