import {
  type Attachment,
  ContractError,
  type Output,
  type ProviderResponse,
} from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { createConversationsApi } from "./api/conversations-api";
import { createJobEvidenceApi } from "./api/job-evidence";
import { createJobsApi } from "./api/jobs-api";
import { createObservationsApi } from "./api/observations-api";
import { createPoolsApi } from "./api/pools-api";
import { createProfilesApi } from "./api/profiles-api";
import { createProjectsApi } from "./api/projects-api";
import { createStreams, pause } from "./api/streams";
import { createSubscriptions } from "./api/subscriptions";
import { type ApiClientOptions, ApiError, createTransport } from "./api/transport";
import { identity } from "./api/validators";

export { type ConversationEdit } from "./api/conversations-api";
export { type MutableJob, type MutableJobInput } from "./api/jobs-api";
export { type ObservationSettings } from "./api/observations-api";
export {
  isExecutorConflict,
  type NativeAllocation,
  type NativeAllocationPhase,
  type NativeExecutor,
  type NativeExecutorChange,
  type NativePoolAllocations,
  type PoolUpdate,
} from "./api/pools-api";
export { isProfileConflict, type ProfileWriteRequest } from "./api/profiles-api";
export { type ProjectCommand, type ProjectCreate, type ProjectUpdate } from "./api/projects-api";
export { type JobStreamEnd, type JobStreamEvent, type JobStreamSubscription } from "./api/streams";
export { type ApiClientOptions, ApiError } from "./api/transport";
export { configurationIdentity } from "./api/validators";

export type ApiClient = ReturnType<typeof createApiClient>;

export async function readTranscriptEvidence(
  client: Pick<ApiClient, "readAllJobTools" | "readAllProviderResponses">,
  job: string,
  {
    signal = new AbortController().signal,
    previous = [],
    settled = false,
    readReplies = true,
  }: {
    signal?: AbortSignal;
    previous?: readonly ProviderResponse[];
    settled?: boolean;
    readReplies?: boolean;
  } = {},
) {
  const [tools, replies] = await Promise.allSettled([
    client.readAllJobTools(job, signal),
    readReplies
      ? client.readAllProviderResponses(job, signal, previous.at(-1)?.request).catch((error) => {
          if (error instanceof ApiError && error.code === "decision_response_unavailable")
            return [];
          throw error;
        })
      : Promise.resolve([]),
  ]);
  signal.throwIfAborted();
  const responses = replies.status === "fulfilled" ? [...previous, ...replies.value] : null;
  let replyError = replies.status === "rejected" ? replies.reason : null;
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

export async function readOutputWindow(
  client: Pick<ApiClient, "readOutputs" | "readLatestOutput">,
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
  const outputs: Output[] = [];
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

function browserSession(input: unknown) {
  const session = (input as Api.SessionResponse | null)?.session;
  if (session?.authenticated === false) return null;
  if (
    session?.authenticated !== true ||
    typeof session.csrf_token !== "string" ||
    !Number.isSafeInteger(session.expires_at_us)
  )
    throw new ContractError("session", "a browser session");
  return { csrfToken: session.csrf_token, expiresAtUs: session.expires_at_us };
}

function requireSession(session: ReturnType<typeof browserSession>) {
  if (!session) throw new ContractError("session", "a signed-in session");
  return session;
}

export function createApiClient(options: ApiClientOptions) {
  const transport = createTransport(options);
  const { send, base } = transport;
  const profileListeners = new Set<(change?: { availability: string }) => void>();
  const conversationListeners = new Set<() => void>();
  async function probeSession(signal?: AbortSignal) {
    return browserSession((await send("GET", "/api/v1/session", { signal })).input);
  }
  async function signOutBrowserSession(signal?: AbortSignal): Promise<"out" | "token"> {
    const { headers } = await send("DELETE", "/api/v1/session", { sign: true, signal });
    return headers.get("WWW-Authenticate")?.toLowerCase() === "bearer" ? "token" : "out";
  }
  const projects = createProjectsApi(send);
  const streams = createStreams(transport, conversationListeners, (): void =>
    subscriptions.restartSidebar(true),
  );
  const subscriptions = createSubscriptions(transport, streams, projects.readProjects);

  return {
    ...projects,
    ...createPoolsApi(send),
    ...createProfilesApi(send, profileListeners),
    ...createObservationsApi(send),
    ...createJobsApi(transport),
    ...createJobEvidenceApi(send),
    ...createConversationsApi(send, conversationListeners),
    ...subscriptions.methods,
    subscribeJobEvents: streams.subscribeJobEvents,
    async uploadAttachment(
      payload: {
        mime: "image/png" | "image/jpeg" | "image/gif" | "image/webp";
        data_base64: string;
        display_name?: string;
      },
      signal?: AbortSignal,
    ): Promise<Attachment> {
      const answer = await send("POST", "/api/v1/attachments", {
        body: payload,
        sign: true,
        signal,
      });
      return (answer.input as { attachment: Attachment }).attachment;
    },

    async readAttachment(attachmentId: string, signal?: AbortSignal): Promise<Attachment> {
      identity(attachmentId, "attachment");
      const answer = await send("GET", `/api/v1/attachments/${attachmentId}`, {
        signal,
      });
      return (answer.input as { attachment: Attachment }).attachment;
    },

    attachmentContentUrl(attachmentId: string): string {
      identity(attachmentId, "attachment");
      return `${base}/api/v1/attachments/${attachmentId}/content`;
    },
    probeSession,
    async readSession(signal?: AbortSignal) {
      return requireSession(await probeSession(signal));
    },
    async signInWithToken(token: string, signal?: AbortSignal) {
      return requireSession(browserSession((await transport.exchangeSession(token, signal)).input));
    },
    signOutBrowserSession,
    async deleteSession(signal?: AbortSignal) {
      await signOutBrowserSession(signal);
    },
    close(): void {
      subscriptions.close();
      profileListeners.clear();
      conversationListeners.clear();
      streams.close();
    },
  };
}
