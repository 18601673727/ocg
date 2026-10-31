import type { ControlProjects, ControlPools, ControlChildren } from "../contracts";
import type * as Api from "@/components/ocg/contracts/api";
import {
  projectJob,
  projectContextStatus,
  ContractError,
  TREE_MAX_DEPTH,
  projectAdmission,
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
export type JobStreamSubscription = { closed: Promise<JobStreamEnd>; close(): void };

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

export type ControlSubmission = {
  reasoning_effort?: string;
  mode?: "queue" | "steer";
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
      protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
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
      capabilities: ("chat" | "coding" | "decision")[];
      llm_targets: string[];
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
    }
  | {
      kind: "decision_agent";
      name: string;
      instructions?: string | null;
      models: string[];
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
      ? client.readAllProviderResponses(job, signal, previous.at(-1)?.request)
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
  const profileListeners = new Set<() => void>();
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
  let snapshotBusy = false;
  const snapshotWaiters: Array<() => void> = [];
  async function snapshotSlot(signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    if (snapshotBusy) {
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
      });
    } else snapshotBusy = true;
    return () => {
      const next = snapshotWaiters.shift();
      if (next) next();
      else snapshotBusy = false;
    };
  }

  // Browsers share a small HTTP/1 connection pool across tabs. Rotate event
  // streams so ordinary requests can run even when several views are open.
  function streamAttempt(parent: AbortSignal, completeBeforeRotation = false) {
    const controller = new AbortController();
    let rotated = false;
    let rotationDue = false;
    let complete = !completeBeforeRotation;
    const rotate = () => {
      rotated = true;
      controller.abort();
    };
    const abort = () => controller.abort();
    parent.addEventListener("abort", abort, { once: true });
    if (parent.aborted) abort();
    const timer = programmatic
      ? undefined
      : setTimeout(() => {
          rotationDue = true;
          if (complete) rotate();
        }, 5000);
    return {
      signal: controller.signal,
      rotated: () => rotated,
      complete: () => {
        complete = true;
        if (rotationDue) rotate();
      },
      abort: () => controller.abort(),
      dispose: () => {
        if (timer !== undefined) clearTimeout(timer);
        parent.removeEventListener("abort", abort);
      },
    };
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
      const runAttempt = async (): Promise<{ keepGoing: boolean; wait: number }> => {
        const attempt = streamAttempt(signal, !programmatic);
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
            headers: { Accept: "text/event-stream", ...(await credentials()) },
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
              if (chunk.done) break;
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
              outcome = await locks.request("ocg-snapshot-stream", { signal }, () => {
                entered = true;
                return runAttempt();
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
  type Observer<T> = { receive: (value: T) => void; error: (error: unknown) => void };
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
    },
  ): Promise<{ status: number; input: unknown }> {
    const headers: Record<string, string> = { ...(await credentials()) };
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
    return (await send("GET", `/api/v1/projects${suffix}`, { signal: settings.signal }))
      .input as ControlProjects;
  }

  async function readProfilePage(settings: { before?: string; signal?: AbortSignal } = {}) {
    const suffix =
      settings.before === undefined ? "" : `?before=${identity(settings.before, "before")}`;
    return projectProfiles(
      (await send("GET", `/api/v1/profiles${suffix}`, { signal: settings.signal }))
        .input as Api.Profiles,
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
        executors.set(executor.id, executor);
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
    return {
      profile_id: first?.profile_id ?? null,
      llm_profiles: pages
        .flatMap((page) => page.llm_profiles)
        .map((profile) => ({
          ...profile,
          executors: [...executors.values()]
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
    onProfilesChanged(listener: () => void) {
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
        await send("GET", `/api/v1/conversations?${query.join("&")}`, { signal: settings.signal })
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
      return (await send("GET", `/api/v1/conversations/${conversation}/limits`, { signal }))
        .input as Api.ConversationLimits;
    },

    async readJobs(
      settings: { before?: string; limit?: number; query?: string; signal?: AbortSignal } = {},
    ) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      if (settings.query?.trim()) query.push(`query=${encodeURIComponent(settings.query.trim())}`);
      return (
        await send("GET", `/api/v1/job-history?${query.join("&")}`, { signal: settings.signal })
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
      return (await send("GET", `/api/v1/logs?${query.join("&")}`, { signal: settings.signal }))
        .input as Api.Logs;
    },

    /** What the host establishes about itself and its database, now. */
    async readHealth(signal?: AbortSignal) {
      return (await send("GET", "/api/v1/health", { signal })).input as Api.Health;
    },

    /** The Job's persisted lifecycle record. */
    async readJob(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return projectJob(
        (await send("GET", `/api/v1/jobs/${job}`, { signal })).input as Api.JobDetails,
      );
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
      const answer = await send("GET", `/api/v1/jobs/${job}/tree?${query.join("&")}`, {
        signal: settings.signal,
      });
      return answer.input as import("../contracts").ControlJobTree;
    },
    async readCards(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return (await send("GET", `/api/v1/conversations/${conversation}/cards`, { signal }))
        .input as Api.Cards;
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
        | { action: "edit"; turn: string; message: number; text: string; attachments: string[] }
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
        (await send("GET", `/api/v1/agent-profiles${suffix}`, { signal: settings.signal }))
          .input as Api.AgentProfiles,
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

    /** Every Pool. */
    async readPools(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/pools", { signal });
      return answer.input as ControlPools;
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
      return (answer.input as Api.PoolAnswer).pool as ControlPools["pools"][number];
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
      const answer = await send("GET", "/api/v1/settings/compaction", { signal });
      return answer.input as Api.GlobalCompactionSettings;
    },

    async saveGlobalCompactionSettings(settings: Api.GlobalCompactionSettings) {
      const answer = await send("POST", "/api/v1/settings/compaction", {
        body: settings,
        sign: true,
      });
      return answer.input as Api.GlobalCompactionSettings;
    },

    async deleteLlmProfile(profileId: string) {
      identity(profileId, "profile");
      const answer = await send("DELETE", `/api/v1/profiles/${profileId}`, { sign: true });
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
      profileListeners.forEach((listener) => listener());
      return readAllProfiles();
    },

    async removeProvider(providerId: string) {
      identity(providerId, "provider");
      await send("DELETE", `/api/v1/providers/${providerId}`, { sign: true });
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
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs${suffix}`, {
        signal: settings.signal,
      });
      return answer.input as Api.Outputs;
    },

    async readLatestOutput(job: string, signal?: AbortSignal): Promise<ControlOutput | null> {
      jobIdentity(job);
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs/latest`, { signal });
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
      if (submission.agent_profile !== undefined)
        identity(submission.agent_profile, "agent_profile");
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
      const admission = projectAdmission(answer.input as Api.AdmissionResponse);
      if (admission.status !== "admitted" && admission.status !== "pending") {
        throw new ContractError("admission.status", "admitted or pending from a submission");
      }
      conversationListeners.forEach((listener) => listener());
      return {
        status: admission.status,
        job: admission.job,
        conversation: admission.conversation,
        seq: admission.seq,
        joined: admission.joined,
        model: admission.model,
        kept: admission.kept,
        created: answer.status === 201,
      } satisfies ControlSubmissionResult;
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
      const answer = await send("GET", `/api/v1/attachments/${attachmentId}`, { signal });
      return (answer.input as { attachment: ControlAttachment }).attachment;
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
      return projectAdmission(answer.input as Api.AdmissionResponse);
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
      return { csrfToken: session.csrf_token, expiresAtUs: session.expires_at_us };
    },

    /** The session this browser holds, if it is still authorized. */
    async readSession(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/session", { signal });
      const session = (answer.input as Api.SessionResponse).session;
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
