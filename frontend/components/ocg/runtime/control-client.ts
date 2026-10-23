import {
  ContractError,
  TREE_MAX_DEPTH,
  decodeAdmission,
  decodeAgentProfiles,
  decodeAttachment,
  decodeControlError,
  decodeConversationEdit,
  decodeConversationLimits,
  decodeConversations,
  decodeChildren,
  decodeCards,
  decodeApprovalDecision,
  decodeReleaseSlot,
  type ControlApproval,
  decodeHealth,
  decodeJobs,
  decodeUsage,
  decodeUsageBuckets,
  decodeUsageRetries,
  decodeAvailability,
  decodeLogs,
  decodeJob,
  decodeJobDetails,
  decodeConversationTurns,
  decodeJobInput,
  decodeJobEvent,
  decodeJobTools,
  decodeJobTree,
  decodeOutputsPage,
  decodeProviderResponses,
  decodePoolAnswer,
  decodePools,
  decodeProfiles,
  decodeProviderModels,
  decodeImportedModels,
  type ControlProviderModel,
  decodeProjectAnswer,
  decodeProjects,
  decodeSession,
  decodeStop,
  decodeRetry,
  type ControlAttachment,
  type ControlCapability,
  type ControlJobEvent,
  type ControlJobToolsPage,
  type ControlProject,
  type ControlToolCall,
  type ControlProviderResponse,
} from "../contracts";
import { SseParser } from "./sse";

export type ControlProjectCommand = { name: string; argv: string[] };

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
  | { enabled: boolean; concurrency_limit?: never };

export type ControlConversationEdit =
  | { title: string; hidden?: never }
  | { hidden: true; title?: never }
  | { limits: Pick<ReturnType<typeof decodeConversationLimits>, "max_depth" | "max_auto_turns" | "max_iterations">; title?: never; hidden?: never };

export type JobStreamEvent = ControlJobEvent | { type: "heartbeat"; };
export type JobStreamEnd = "settled" | "stream_error" | "aborted";
export type JobStreamSubscription = { closed: Promise<JobStreamEnd>; close(): void; };

export class ControlTransportError extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
    readonly retryable: boolean,
    readonly detail: string = "",
  ) {
    // Do not echo fetch errors, request URLs, credentials or untrusted bodies.
    super(`OCG event transport: ${code} (${status})`);
    this.name = "ControlTransportError";
  }
}

export type HttpCanonicalControlClientOptions = {
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

export type ControlSubmission = {
  // An Agent Profile that targets `llm_profile`; its instructions are frozen with the Job.
  agent_profile?: string;
  llm_profile?: string;
  executor: string;
  objective: string;
  name?: string;
  // A completed predecessor Job whose conversation history precedes this turn.
  conversation?: string;
  // The Project the Job works in; an Agent with a Workspace grant requires one on a conversation's first turn.
  project?: string;
  // Image Attachments to send with the objective.
  attachments?: string[];
};

export type ControlProfileWriteRequest =
  | {
      kind: "llm";
      name: string;
      model: string;
      context_window?: number | null;
      protocol?: "openai_chat_completions" | "openai_responses";
      endpoint?: string | null;
      credential_ref?: string | null;
      credential?: string | null;
      clear_credential?: boolean;
      declared_capabilities: ControlCapability[];
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
      behavior?: "chat" | "general" | null;
      tool_grant?: "read" | "edit" | "code" | null;
      llm_targets: string[];
      dispatch_to?: string[];
      default_model?: string | null;
    };

export type ControlSubmissionResult = {
  conversation: string | null;
  seq: number | null;
  joined: boolean;
  model: string | null;
  kept: boolean;
  status: "admitted" | "pending";
  job: string | null;
  // Whether this request created the Job, rather than replay its key.
  created: boolean;
};

function controlOrigin(raw: string, browserSession: boolean): string {
  const url = new URL(raw);
  const loopback = url.protocol === "http:" && ["127.0.0.1", "localhost"].includes(url.hostname) && Boolean(url.port);
  const secureBrowser = browserSession && typeof window !== "undefined" &&
    url.protocol === "https:" && window.location.origin === url.origin;
  if ((!loopback && !secureBrowser) || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
    throw new ControlTransportError("invalid_control_origin", 0, false);
  }
  if (typeof window !== "undefined" && window.location.origin !== url.origin) {
    throw new ControlTransportError("same_origin_required", 0, false);
  }
  return url.origin;
}

function sequence(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new ContractError("after_seq", "a non-negative safe integer");
  return value;
}

function jobIdentity(value: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) throw new ContractError("job", "a canonical Job UUID");
  return value;
}

function identity(value: string, path: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) {
    throw new ContractError(path, "a canonical UUIDv7 identity");
  }
  return value;
}

function submissionKey(value: string): string {
  if (!/^[A-Za-z0-9._~:+/=-]{16,128}$/.test(value)) {
    throw new ContractError("Idempotency-Key", "a well-formed idempotency key");
  }
  return value;
}

function pause(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) { resolve(); return; }
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
  { signal = new AbortController().signal, previous = [], settled = false, readReplies = true }: {
    signal?: AbortSignal; previous?: readonly ControlProviderResponse[]; settled?: boolean; readReplies?: boolean;
  } = {},
) {
  const [tools, replies] = await Promise.allSettled([
    client.readAllJobTools(job, signal),
    readReplies ? client.readAllProviderResponses(job, signal, previous.at(-1)?.request) : Promise.resolve([]),
  ]);
  signal.throwIfAborted();
  const responses = replies.status === "fulfilled" ? [...previous, ...replies.value] : null;
  let replyError = replies.status === "rejected" ? replies.reason : null;
  // Settlement precedes the final Received write. Reconcile that short gap
  // independently of the now-closed output stream, with bounded retries.
  for (const delay of [100, 300, 1000, 3000]) {
    if (!settled || responses === null || replyError || (responses.length && !responses.at(-1)?.requests_tools)) break;
    await pause(delay, signal);
    signal.throwIfAborted();
    try {
      responses.push(...await client.readAllProviderResponses(job, signal, responses.at(-1)?.request));
    } catch (error) { replyError = error; }
  }
  const failures = [tools.status === "rejected" ? tools.reason : null, replyError].filter(error => error !== null);
  return {
    tools: tools.status === "fulfilled" ? tools.value : null,
    responses,
    error: failures.length ? `Transcript evidence unavailable; showing stored output. ${failures.map(error => error instanceof Error ? error.message : String(error)).join("; ")}` : undefined,
  };
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
  for (const [name, value] of [["from_us", settings.fromUs], ["until_us", settings.untilUs], ["limit", settings.limit]] as const) {
    if (value !== undefined) query.push(`${name}=${sequence(value)}`);
  }
  if (settings.after) {
    if (!/^[A-Za-z0-9_-]{1,400}$/.test(settings.after)) throw new ContractError("after", "an observation cursor");
    query.push(`after=${settings.after}`);
  }
  if (settings.llmProfile) query.push(`llm_profile=${identity(settings.llmProfile, "llm_profile")}`);
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
  const profileListeners = new Set<() => void>();
  const conversationListeners = new Set<() => void>();
  let closed = false;

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
    return status === 503 &&
      ["too_many_streams", "database_busy", "database_unavailable", "shutting_down"].includes(code);
  }

  async function connection(
    jobId: string,
    signal: AbortSignal,
    listener: (event: JobStreamEvent) => void | Promise<void>,
    cursor: { sequence: number | undefined },
  ): Promise<Exclude<JobStreamEnd, "aborted"> | null> {
    const headers: Record<string, string> = {
      Accept: "text/event-stream",
      ...await credentials(),
    };
    if (cursor.sequence !== undefined) headers["Last-Event-ID"] = String(cursor.sequence);
    let response: Response;
    try {
      // Entity IDs are canonical UUIDv7 values and are safe as literal path segments.
      response = await fetchImpl(`${base}/api/v1/jobs/${jobId}/events`, {
        method: "GET", headers, signal,
        mode: "same-origin", credentials: programmatic ? "omit" : "same-origin",
        cache: "no-store", redirect: "error",
      });
    } catch {
      throw new ControlTransportError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      try { code = decodeControlError(await response.json()).code; }
      catch { /* An unrecognized body still retains its HTTP status. */ }
      throw new ControlTransportError(code, response.status,
        retryable(response.status, code));
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
      for (; ;) {
        let chunk: ReadableStreamReadResult<Uint8Array>;
        try { chunk = await reader.read(); }
        catch { throw new ControlTransportError("connection_lost", 0, true); }
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
          try { input = JSON.parse(frame.data); }
          catch { throw new ContractError("data", "a complete JSON event"); }
          const event = decodeJobEvent(frame.event, input, jobId, frame.id);
          if (event.type === "output") {
            const acknowledged = event.output.seq;
            const previous = cursor.sequence ?? 0;
            if (acknowledged <= previous) continue;
            if (acknowledged !== previous + 1) throw new ContractError("output.seq", "the next stored sequence without a gap");
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
      try { await reader.cancel(); }
      catch { /* The read failure is reported above; cleanup cannot replace it. */ }
      reader.releaseLock();
    }
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
    },
  ): Promise<{ status: number; input: unknown }> {
    const headers: Record<string, string> = { ...await credentials() };
    if (settings.body !== undefined) headers["Content-Type"] = "application/json";
    if (settings.key !== undefined) headers["Idempotency-Key"] = settings.key;
    if (settings.sign) {
      const csrf = await options.csrfToken?.();
      if (!csrf) throw new ControlTransportError("credential_unavailable", 0, false);
      headers["X-OCG-CSRF"] = csrf;
    }
    let response: Response;
    try {
      response = await fetchImpl(`${base}${path}`, {
        method, headers, signal: settings.signal,
        ...(settings.body === undefined ? {} : { body: JSON.stringify(settings.body) }),
        mode: "same-origin", credentials: programmatic ? "omit" : "same-origin",
        cache: "no-store", redirect: "error",
      });
    } catch {
      throw new ControlTransportError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      let detail = "";
      try {
        const decoded = decodeControlError(await response.json());
        code = decoded.code;
        detail = decoded.message;
      } catch { /* An unrecognized body still retains its HTTP status. */ }
      throw new ControlTransportError(code, response.status, retryable(response.status, code), detail);
    }
    let input: unknown;
    try { input = await response.json(); }
    catch { throw new ContractError("response", "a complete JSON answer"); }
    return { status: response.status, input };
  }

  const readJobToolsInternal = async (
    job: string,
    settings: { after?: string; limit?: number; signal?: AbortSignal } = {},
  ): Promise<ControlJobToolsPage> => {
    jobIdentity(job);
    const query: string[] = [];
    if (settings.after !== undefined && settings.after !== null) {
      if (!/^[1-9][0-9]*:[0-9]+$/.test(settings.after)) throw new ContractError("after", "a tool cursor <request>:<index>");
      query.push(`after=${settings.after}`);
    }
    if (settings.limit !== undefined) {
      if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      query.push(`limit=${settings.limit}`);
    }
    const suffix = query.length ? `?${query.join("&")}` : "";
    const answer = await send("GET", `/api/v1/jobs/${job}/tools${suffix}`, { signal: settings.signal });
    return decodeJobTools(answer.input, job);
  };

  async function readProjectPage(settings: { before?: string; limit?: number; signal?: AbortSignal } = {}) {
    const query: string[] = [];
    if (settings.before !== undefined) query.push(`before=${identity(settings.before, "before")}`);
    if (settings.limit !== undefined) {
      if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      query.push(`limit=${settings.limit}`);
    }
    const suffix = query.length ? `?${query.join("&")}` : "";
    return decodeProjects((await send("GET", `/api/v1/projects${suffix}`, { signal: settings.signal })).input);
  }

  async function readProfilePage(settings: { before?: string; signal?: AbortSignal } = {}) {
    const suffix = settings.before === undefined ? "" : `?before=${identity(settings.before, "before")}`;
    return decodeProfiles((await send("GET", `/api/v1/profiles${suffix}`, { signal: settings.signal })).input);
  }

  async function readAllProfiles(signal?: AbortSignal) {
    const pages: Array<ReturnType<typeof decodeProfiles>> = [];
    const providers = new Map<string, ReturnType<typeof decodeProfiles>["providers"][number]>();
    const executors = new Map<string, ReturnType<typeof decodeProfiles>["executor_protocols"][number]>();
    let before: string | undefined;
    for (;;) {
      const page = await readProfilePage({ before, signal });
      pages.push(page);
      for (const executor of page.executor_protocols) {
        executors.set(executor.id, executor);
      }
      for (const provider of page.providers) {
        const key = JSON.stringify([provider.protocol, provider.endpoint, provider.credential_ref]);
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
    return {
      profile_id: first?.profile_id ?? null,
      llm_profiles: pages.flatMap((page) => page.llm_profiles).map((profile) => ({
        ...profile,
        executors: [...executors.values()].filter((executor) => executor.protocol === profile.protocol)
          .map((executor) => executor.id).sort(),
      })),
      agent_profiles: pages.flatMap((page) => page.agent_profiles),
      executor_protocols: [...executors.values()],
      providers: [...providers.values()],
      has_more: false,
      next_before: null,
    };
  }

  return {
    onProfilesChanged(listener: () => void) {
      profileListeners.add(listener);
      return () => { profileListeners.delete(listener); };
    },

    onConversationsChanged(listener: () => void) {
      conversationListeners.add(listener);
      return () => { conversationListeners.delete(listener); };
    },

    async readConversations(settings: { before?: string; limit?: number; signal?: AbortSignal } = {}) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      return decodeConversations((await send("GET", `/api/v1/conversations?${query.join("&")}`, { signal: settings.signal })).input);
    },

    /**
     * Full-text conversation search: titles and recent user/agent messages,
     * newest first. Terms are the whitespace-separated words of `query`,
     * combined by AND; matching is case-insensitive substring per term.
     */
    async searchConversations(settings: { query: string; before?: string; limit?: number; signal?: AbortSignal }) {
      const trimmed = settings.query.trim();
      if (!trimmed) throw new ContractError("query", "a non-empty search query");
      if (trimmed.length > 500) throw new ContractError("query", "a search query of at most 500 characters");
      const query = [`query=${encodeURIComponent(trimmed)}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      query.push(`limit=${settings.limit ?? 50}`);
      return decodeConversations((await send("GET", `/api/v1/conversations/search?${query.join("&")}`, { signal: settings.signal })).input);
    },

    /** Renames a conversation or hides it for good. */
    async editConversation(root: string, edit: ControlConversationEdit, signal?: AbortSignal) {
      jobIdentity(root);
      const answer = await send("POST", `/api/v1/conversations/${root}`, {
        body: edit,
        sign: true,
        signal,
      });
      const result = decodeConversationEdit(answer.input, root);
      conversationListeners.forEach((listener) => listener());
      return result;
    },

    async readConversationLimits(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return decodeConversationLimits((await send("GET", `/api/v1/conversations/${conversation}/limits`, { signal })).input, conversation);
    },

    async readJobs(settings: { before?: string; limit?: number; signal?: AbortSignal } = {}) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      return decodeJobs((await send("GET", `/api/v1/job-history?${query.join("&")}`, { signal: settings.signal })).input);
    },

    async readUsage(settings: ObservationSettings = {}) {
      return decodeUsage((await send("GET", `/api/v1/usage${observationQuery(settings)}`, { signal: settings.signal })).input);
    },

    async readUsageBuckets({ fromUs, untilUs, bucket, tzOffsetMinutes, after, signal }: {
      fromUs: number;
      untilUs: number;
      bucket: "day" | "week";
      tzOffsetMinutes: number;
      after?: string;
      signal?: AbortSignal;
    }) {
      if (!Number.isSafeInteger(tzOffsetMinutes)) {
        throw new ContractError("tz_offset_minutes", "a signed safe integer");
      }
      const query = `from_us=${sequence(fromUs)}&until_us=${sequence(untilUs)}&bucket=${bucket}&tz_offset_minutes=${tzOffsetMinutes}${after ? `&after=${encodeURIComponent(after)}` : ""}`;
      return decodeUsageBuckets((await send("GET", `/api/v1/usage/summary?${query}`, { signal })).input);
    },

    async readUsageRetries({ fromUs, untilUs, bucket, tzOffsetMinutes, after, signal }: {
      fromUs: number; untilUs: number; bucket: "day" | "week"; tzOffsetMinutes: number; after?: string; signal?: AbortSignal;
    }) {
      if (!Number.isSafeInteger(tzOffsetMinutes)) throw new ContractError("tz_offset_minutes", "a signed safe integer");
      const query = `from_us=${sequence(fromUs)}&until_us=${sequence(untilUs)}&bucket=${bucket}&tz_offset_minutes=${tzOffsetMinutes}&limit=100${after ? `&after=${encodeURIComponent(after)}` : ""}`;
      return decodeUsageRetries((await send("GET", `/api/v1/usage/retries?${query}`, { signal })).input);
    },

    async readAvailability(settings: ObservationSettings = {}) {
      return decodeAvailability((await send("GET", `/api/v1/availability${observationQuery(settings)}`, { signal: settings.signal })).input);
    },

    async readLogs(settings: { file?: string; limit?: number; signal?: AbortSignal } = {}) {
      const query = [`limit=${settings.limit ?? 100}`];
      if (settings.file) {
        if (!/^\d{14}Z_runtime_\d+\.log$/.test(settings.file)) throw new ContractError("file", "a runtime log file name");
        query.push(`file=${settings.file}`);
      }
      return decodeLogs((await send("GET", `/api/v1/logs?${query.join("&")}`, { signal: settings.signal })).input);
    },

    /** What the host establishes about itself and its database, now. */
    async readHealth(signal?: AbortSignal) {
      return decodeHealth((await send("GET", "/api/v1/health", { signal })).input);
    },

    /** The Job's persisted lifecycle record. */
    async readJob(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return decodeJob((await send("GET", `/api/v1/jobs/${job}`, { signal })).input);
    },

    async readJobDetails(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return decodeJobDetails((await send("GET", `/api/v1/jobs/${job}`, { signal })).input);
    },

    /** The Job's frozen input: its objective as text, never inferred from its identity. */
    async readJobInput(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return decodeJobInput((await send("GET", `/api/v1/jobs/${job}/input`, { signal })).input, job);
    },

    /** The bounded conversation ending at `job`, reconstructed from persisted Job `previous` links. */
    async readConversationTurns(conversation: string, before?: string, limit = 20, signal?: AbortSignal) {
      jobIdentity(conversation);
      if (before) jobIdentity(before);
      const query = `?limit=${sequence(limit)}${before ? `&before=${before}` : ""}`;
      const answer = await send("GET", `/api/v1/conversations/${conversation}/turns${query}`, { signal });
      return decodeConversationTurns(answer.input, conversation);
    },

    async readChildren(conversation: string, before?: string, limit = 20, signal?: AbortSignal) {
      jobIdentity(conversation);
      if (before) jobIdentity(before);
      const query = `?limit=${sequence(limit)}${before ? `&before=${before}` : ""}`;
      return decodeChildren((await send("GET", `/api/v1/conversations/${conversation}/children${query}`, { signal })).input, conversation);
    },
    /** The tree of the work below a Job: its ancestors, and the nodes below it. */
    async readJobTree(
      job: string,
      settings: { after?: string; limit?: number; depth?: number; signal?: AbortSignal } = {},
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
      const answer = await send("GET", `/api/v1/jobs/${job}/tree?${query.join("&")}`, { signal: settings.signal });
      return decodeJobTree(answer.input, job);
    },
    async readCards(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return decodeCards((await send("GET", `/api/v1/conversations/${conversation}/cards`, { signal })).input, conversation);
    },
    async decideApproval(approval: ControlApproval, decision: { decision: "approve" | "reject" } | { answer: string }, signal?: AbortSignal) {
      jobIdentity(approval[0]);
      sequence(approval[1]);
      sequence(approval[2]);
      const [turn, request, index] = approval;
      return decodeApprovalDecision((await send("POST", `/api/v1/approvals/${turn}/${request}/${index}`, { body: decision, sign: true, signal })).input, approval);
    },
    async releaseSlot(turn: string, confirm = false, signal?: AbortSignal) {
      jobIdentity(turn);
      return decodeReleaseSlot((await send("POST", `/api/v1/jobs/${turn}/release-slot`, { body: confirm ? { confirm: true } : {}, sign: true, signal })).input);
    },
    async holdConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return decodeStop((await send("POST", `/api/v1/conversations/${conversation}/hold`, { body: {}, sign: true, signal })).input, conversation);
    },
    async unholdConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return decodeStop((await send("POST", `/api/v1/conversations/${conversation}/unhold`, { body: {}, sign: true, signal })).input, conversation);
    },

    async retryConversation(conversation: string, turn: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      jobIdentity(turn);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/retry`, { body: { turn }, sign: true, signal });
      const result = decodeRetry(answer.input);
      if (result.conversation !== conversation) throw new ContractError("conversation", `matching ${conversation}`);
      return result;
    },

    /** One page of Agent Profiles with their LLM targets and the Executors that can run each. */
    async readAgentProfiles(
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
      return decodeAgentProfiles((await send("GET", `/api/v1/agent-profiles${suffix}`, { signal: settings.signal })).input);
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
        if (!page.next_before || page.next_before === before) throw new Error("Project cursor did not advance.");
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
      return decodeProjectAnswer(answer.input);
    },

    /** Updates an existing Project. */
    async updateProject(projectId: string, body: ControlProjectUpdate, signal?: AbortSignal) {
      identity(projectId, "project");
      const answer = await send("POST", `/api/v1/projects/${projectId}`, {
        body,
        sign: true,
        signal,
      });
      return decodeProjectAnswer(answer.input);
    },

    /** Every Pool. */
    async readPools(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/pools", { signal });
      return decodePools(answer.input);
    },

    /** Updates a Pool's concurrency limit or enabled state. */
    async updatePool(poolId: string, body: ControlPoolUpdate, signal?: AbortSignal) {
      identity(poolId, "pool");
      if (body.concurrency_limit !== undefined) {
        if (!Number.isSafeInteger(body.concurrency_limit) || body.concurrency_limit < 1 || body.concurrency_limit > 4294967295) {
          throw new ContractError("concurrency_limit", "an integer from 1 to 4294967295");
        }
      }
      const answer = await send("POST", `/api/v1/pools/${poolId}`, {
        body,
        sign: true,
        signal,
      });
      return decodePoolAnswer(answer.input);
    },

    /** Every Profile (LLM Profiles, Agent Profiles) and the merged Provider projection. */
    async readProfiles(signal?: AbortSignal) {
      return readAllProfiles(signal);
    },

    async discoverProviderModels(connection: { endpoint: string; credential_ref?: string | null; credential?: string }, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/provider-models", { body: connection, sign: true, signal });
      return decodeProviderModels(answer.input);
    },

    async importModels(source: string | { protocol?: "openai_chat_completions" | "openai_responses"; name: string; endpoint: string; credential?: string; credential_ref?: string }, models: ControlProviderModel[], capabilities: ControlCapability[]) {
      if (typeof source === "string") identity(source, "source_profile_id");
      const connection = typeof source === "string" ? { source_profile_id: source } : { provider: source };
      const answer = await send("POST", "/api/v1/profiles/import", {
        body: { ...connection, models, declared_capabilities: capabilities }, sign: true,
      });
      const result = decodeImportedModels(answer.input);
      profileListeners.forEach((listener) => listener());
      return result;
    },

    async deleteLlmProfile(profileId: string) {
      identity(profileId, "profile");
      const answer = await send("DELETE", `/api/v1/profiles/${profileId}`, { sign: true });
      decodeProfiles(answer.input);
      profileListeners.forEach((listener) => listener());
      return readAllProfiles();
    },

    /** Creates an LLM Profile or Agent Profile and returns the refreshed catalog. */
    async createProfile(profile: ControlProfileWriteRequest, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/profiles", {
        body: profile,
        sign: true,
        signal,
      });
      const saved = decodeProfiles(answer.input).profile_id;
      const snapshot = await readAllProfiles(signal);
      profileListeners.forEach((listener) => listener());
      return { ...snapshot, profile_id: saved };
    },

    /** Updates an existing LLM Profile or Agent Profile and returns the refreshed catalog. */
    async updateProfile(profileId: string, profile: ControlProfileWriteRequest, signal?: AbortSignal) {
      identity(profileId, "profile");
      const answer = await send("POST", `/api/v1/profiles/${profileId}`, {
        body: profile,
        sign: true,
        signal,
      });
      const saved = decodeProfiles(answer.input).profile_id;
      const snapshot = await readAllProfiles(signal);
      profileListeners.forEach((listener) => listener());
      return { ...snapshot, profile_id: saved };
    },

    /** One stored page of the Job's outputs, in `seq` order. */
    async readOutputs(
      job: string,
      settings: { afterSeq?: number; limit?: number; signal?: AbortSignal } = {},
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
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs${suffix}`, { signal: settings.signal });
      return decodeOutputsPage(answer.input, job);
    },

    /** One stored page of the Job's tool evidence. */
    async readJobTools(
      job: string,
      settings: { after?: string; limit?: number; signal?: AbortSignal } = {},
    ): Promise<ControlJobToolsPage> {
      return readJobToolsInternal(job, settings);
    },

    async readAllJobOutputs(job: string, signal?: AbortSignal) {
      const outputs: ReturnType<typeof decodeOutputsPage>["outputs"] = [];
      let after = 0;
      for (;;) {
        const page = decodeOutputsPage((await send("GET", `/api/v1/jobs/${jobIdentity(job)}/outputs?limit=100&after_seq=${after}`, { signal })).input, job);
        if (page.outputs.some(output => output.seq <= after)) throw new ContractError("outputs.seq", "a sequence after the cursor");
        outputs.push(...page.outputs);
        if (!page.has_more) return outputs;
        const next = page.outputs.at(-1)?.seq;
        if (next === undefined || next <= after) throw new ContractError("next_after_seq", "an advancing output cursor");
        after = next;
      }
    },

    /** Reassembles reply chunks; an incomplete 50-page walk is refused. */
    async readAllProviderResponses(job: string, signal?: AbortSignal, after?: number): Promise<ControlProviderResponse[]> {
      jobIdentity(job);
      const responses: ControlProviderResponse[] = [];
      let cursor = after;
      let offset = 0;
      let partial: ControlProviderResponse | null = null;
      let partialBytes = 0;
      for (let pageCount = 0; pageCount < 50; pageCount++) {
        const query = `?limit=100${cursor === undefined ? "" : `&after=${sequence(cursor)}`}${offset ? `&text_offset=${offset}` : ""}`;
        const page = decodeProviderResponses((await send("GET", `/api/v1/jobs/${job}/responses${query}`, { signal })).input, job);
        for (const chunk of page.responses) {
          if (chunk.text_offset === 0) {
            if (partial || chunk.request <= (cursor ?? 0)) throw new ContractError("responses.request", "a request after the cursor");
            partial = { request: chunk.request, text: "", requests_tools: chunk.requests_tools, created_at_us: chunk.created_at_us };
            partialBytes = 0;
          }
          if (!partial || chunk.request !== partial.request || chunk.text_offset !== partialBytes
            || chunk.requests_tools !== partial.requests_tools || chunk.created_at_us !== partial.created_at_us) throw new ContractError("responses.text_offset", "contiguous chunks of the same reply");
          partial.text += chunk.text;
          partialBytes += new TextEncoder().encode(chunk.text).length;
          if (chunk.text_complete) { responses.push(partial); partial = null; }
        }
        if (!page.has_more) return responses;
        if (page.next_after === null || page.next_text_offset === null || page.next_after < (cursor ?? 0)
          || (page.next_after === cursor && page.next_text_offset <= offset)) throw new ContractError("next_after", "an advancing reply cursor");
        cursor = page.next_after;
        offset = page.next_text_offset;
      }
      throw new ContractError("responses", "complete provider replies within 50 pages");
    },

    /** All recorded tool calls for a Job, paging through until complete. */
    async readAllJobTools(
      job: string,
      signal?: AbortSignal,
    ): Promise<ControlToolCall[]> {
      const tools: ControlToolCall[] = [];
      let cursor: string | undefined = undefined;
      for (let pageCount = 0; pageCount < 50; pageCount++) {
        const page = await readJobToolsInternal(job, { after: cursor, limit: 100, signal });
        tools.push(...page.tools);
        if (!page.has_more || !page.next_after) {
          break;
        }
        cursor = page.next_after;
      }
      return tools;
    },

    /**
     * Submits one Job under the request's `key`. The key names at most one
     * Job: a request that timed out or answered `pending` is never
     * submitted under a second key; the same key is looked up or replayed.
     */
    async submitJob(submission: ControlSubmission, key: string, signal?: AbortSignal) {
      if (submission.agent_profile !== undefined) identity(submission.agent_profile, "agent_profile");
      if (submission.llm_profile !== undefined) identity(submission.llm_profile, "llm_profile");
      identity(submission.executor, "executor");
      if (submission.conversation !== undefined) jobIdentity(submission.conversation);
      if (submission.project !== undefined) identity(submission.project, "project");
      if (submission.attachments !== undefined) {
        for (const [index, att] of submission.attachments.entries()) {
          identity(att, `attachments[${index}]`);
        }
      }
      submissionKey(key);
      const answer = await send("POST", "/api/v1/jobs", {
        body: submission,
        key,
        sign: true,
        signal,
      });
      const admission = decodeAdmission(answer.input);
      if (admission.status !== "admitted" && admission.status !== "pending") {
        throw new ContractError("admission.status", "admitted or pending from a submission");
      }
      conversationListeners.forEach((listener) => listener());
      return {
        status: admission.status,
        job: admission.job,
        conversation: admission.conversation, seq: admission.seq, joined: admission.joined, model: admission.model, kept: admission.kept,
        created: answer.status === 201,
      } satisfies ControlSubmissionResult;
    },

    /** Uploads a PNG or JPEG image to the Attachment store. */
    async uploadAttachment(
      payload: { mime: "image/png" | "image/jpeg"; data_base64: string; display_name?: string },
      signal?: AbortSignal,
    ): Promise<ControlAttachment> {
      const answer = await send("POST", "/api/v1/attachments", {
        body: payload,
        sign: true,
        signal,
      });
      return decodeAttachment(answer.input);
    },

    /** Reads metadata of an image Attachment. */
    async readAttachment(attachmentId: string, signal?: AbortSignal): Promise<ControlAttachment> {
      identity(attachmentId, "attachment");
      const answer = await send("GET", `/api/v1/attachments/${attachmentId}`, { signal });
      return decodeAttachment(answer.input);
    },

    /** URL to fetch the raw image content of an Attachment. */
    attachmentContentUrl(attachmentId: string): string {
      identity(attachmentId, "attachment");
      return `${base}/api/v1/attachments/${attachmentId}/content`;
    },

    /** What `key` created, however its submission ended. */
    async readAdmission(key: string, signal?: AbortSignal) {
      submissionKey(key);
      const answer = await send("GET", "/api/v1/admissions", { key, signal });
      return decodeAdmission(answer.input);
    },

    /** Requests the stop of one Job this listener admitted. */
    async stopConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/stop`, { body: {}, sign: true, signal });
      return decodeStop(answer.input, conversation);
    },
    async closeConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/close`, { body: {}, sign: true, signal });
      return decodeStop(answer.input, conversation);
    },

    /** Signs the browser in under this run's pairing code. */
    async createSession(pairingCode: string, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/session", {
        body: { pairing_code: pairingCode },
        signal,
      });
      const session = decodeSession(answer.input);
      if (!session.authenticated) throw new ContractError("session", "a signed-in session");
      return { csrfToken: session.csrf_token, expiresAtUs: session.expires_at_us };
    },

    /** The session this browser holds, if it is still authorized. */
    async readSession(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/session", { signal });
      const session = decodeSession(answer.input);
      if (!session.authenticated) throw new ContractError("session", "a signed-in session");
      return { csrfToken: session.csrf_token, expiresAtUs: session.expires_at_us };
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
      const cursor = { sequence: settings.afterSeq === undefined ? undefined : sequence(settings.afterSeq) };
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
            try {
              const end = await connection(jobId, signal, listener, cursor);
              if (end) return end;
            } catch (error) {
              if (signal.aborted) return "aborted";
              if (!(error instanceof ControlTransportError) || !error.retryable) throw error;
              failure = error;
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
      profileListeners.clear();
      conversationListeners.clear();
      for (const controller of active.values()) controller.abort();
    },
  };
}
