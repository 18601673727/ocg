/**
 * Backend-backed control client for the OCG PWA.
 *
 * This is the PWA's only route to real OCG state. It carries stable command
 * identities, and every mutation returns a typed acknowledgement from the
 * backend. The client is transport-shaped only: it performs no authority
 * decision, derives no execution identity, and never edits a dispatched Call.
 *
 * Every payload is decoded against the Rust-generated contract in
 * `components/ocg/contracts` before it leaves this module. The previous
 * implementation asserted the server's JSON with `as` casts, which meant a
 * renamed or retyped Rust field arrived in the UI as `undefined` with nothing
 * reporting it. It also fabricated `ok: true` and echoed the client's own
 * command id, discarding the `accepted` flag and `command_id` the backend
 * actually returned; those now come from the response.
 */

import type { ProjectId } from "../project/domain";
import type { CanonicalBackendEvent } from "./canonical-store";
import {
  ContractError,
  decodeChatImage,
  type ChatImage,
  type ChatImageUploadRequest,
  decodeChatConversationsResponse,
  decodeChatMessagesResponse,
  type ChatConversationsResponse,
  type ChatMessagesResponse,
  decodeConfigurationAck,
  decodeConfigurationEnvelope,
  decodeDashboardResponse,
  decodeEventsEnvelope,
  decodeJobConfigResponse,
  decodeJobLaunchResponse,
  decodeJobSnapshot,
  decodeProjectResponse,
  decodeProjectsResponse,
  type CanonicalDashboardResponse,
  type CanonicalJobLaunchAck,
  type CanonicalJobSnapshot,
  type GlobalConfiguration,
  type ChatSendRequest,
  type JobLaunchRequest,
  type JsonValue,
  type ProjectConfigurationView,
  type ProjectRecord,
} from "../contracts";

export type {
  CanonicalJobLaunchAck,
  GlobalConfiguration as CanonicalGlobalConfiguration,
  ProjectConfigurationView as CanonicalConfigurationView,
  ProjectRecord as CanonicalProjectRecord,
} from "../contracts";

/** A canonical mutation the backend accepted. */
export type CanonicalProjectAck = {
  ok: true;
  /** The command identity the backend echoes back, not the one we hoped for. */
  commandId: string;
  accepted: boolean;
  project: ProjectRecord;
};

export type CanonicalConfigurationAck = {
  ok: true;
  commandId: string;
  accepted: boolean;
  projectId: string;
  revision: number;
  configuration: ProjectConfigurationView;
};

export type CanonicalJobConfigAck = {
  ok: true;
  commandId: string;
  accepted: boolean;
  jobId: string;
  revision: number;
  configuration: JsonValue;
};

export type CanonicalRejection = {
  ok: false;
  commandId: string;
  status: number;
  message: string;
};

export type CanonicalResult<T> = T | CanonicalRejection;

function isCanonicalRejection<T>(value: CanonicalResult<T>): value is CanonicalRejection {
  return (value as { ok?: boolean }).ok === false;
}

export { isCanonicalRejection };

export interface CanonicalControlClient {
  deleteChatConversation?(projectId: string, sessionId: string): Promise<CanonicalResult<ChatConversationsResponse>>;
  uploadChatImage?(request: ChatImageUploadRequest, signal?: AbortSignal): Promise<CanonicalResult<ChatImage>>;
  readChatConversations(projectId: string): Promise<CanonicalResult<ChatConversationsResponse>>;
  readChatMessages(projectId: string, sessionId: string): Promise<CanonicalResult<ChatMessagesResponse>>;
  listProjects(): Promise<ProjectRecord[]>;
  importProject(commandId: string, root: string): Promise<CanonicalResult<CanonicalProjectAck>>;
  readConfiguration(projectId: string): Promise<CanonicalResult<ProjectConfigurationView>>;
  writeGlobalConfiguration(
    commandId: string,
    configuration: GlobalConfiguration,
  ): Promise<CanonicalResult<CanonicalConfigurationAck>>;
  writeProjectDefaults(
    commandId: string,
    projectId: string,
    defaults: JsonValue,
  ): Promise<CanonicalResult<CanonicalConfigurationAck>>;
  writeJobConfiguration(
    commandId: string,
    jobId: string,
    configuration: JsonValue,
  ): Promise<CanonicalResult<CanonicalJobConfigAck>>;
  readJobSnapshot(
    projectId: string,
    jobId: string,
  ): Promise<CanonicalResult<CanonicalJobSnapshot>>;
  readJobEvents(
    projectId: string,
    jobId: string,
    after: number,
  ): Promise<CanonicalResult<CanonicalBackendEvent[]>>;
  readDashboard(
    projectId: string,
    jobId?: string,
  ): Promise<CanonicalResult<CanonicalDashboardResponse>>;
  /**
   * Launch a real Job on the loopback control plane.
   *
   * This is the only path that starts product execution. The payload is the
   * generated `JobLaunchRequest`, so a field the backend requires cannot be
   * forgotten; the response is decoded against `JobLaunchResponse` before it
   * can be read, so an unknown outcome is reported rather than rendered.
   */
  launchJob(request: JobLaunchRequest): Promise<CanonicalResult<CanonicalJobLaunchAck>>;
  /**
   * Start one plain chat turn on the canonical Job/Attempt/Call lane.
   *
   * The payload reuses `JobLaunchRequest` with `objective` carrying the plain
   * user message, so no second wire contract is introduced. The response is
   * the same `JobLaunchResponse` the Job lane returns; the live provider
   * deltas arrive on `chatStreamUrl`, never from a fixture.
   */
  sendChatMessage(request: ChatSendRequest): Promise<CanonicalResult<CanonicalJobLaunchAck>>;
  /** Revoke one active chat turn. Returns whether a turn was stopped. */
  cancelChatMessage(sessionId: string, projectId?: string): Promise<CanonicalResult<{ sessionId: string; cancelled: boolean }>>;
  /** SSE tail for one chat turn. Consumed with `EventSource`, not `fetch`. */
  chatStreamUrl(sessionId: string, jobId: string): string;
}

export type FetchLike = (
  input: string,
  init?: { method?: string; body?: string; headers?: Record<string, string>; signal?: AbortSignal },
) => Promise<{ status: number; ok: boolean; text(): Promise<string> }>;

export type HttpCanonicalControlClientOptions = {
  baseUrl: string;
  fetch: FetchLike;
};

/** A stable, collision-free command identity for one user intent. */
export function canonicalCommandId(kind: string, scope: string): string {
  return `cmd-${kind}-${scope}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function rejection(commandId: string, status: number, body: string): CanonicalRejection {
  let message = body;
  try {
    const parsed: unknown = JSON.parse(body);
    if (isRecord(parsed) && isRecord(parsed.error) && typeof parsed.error.message === "string") {
      message = parsed.error.message;
    }
  } catch {
    // A non-JSON body is reported verbatim (bounded by the caller).
  }
  return { ok: false, commandId, status, message: message.slice(0, 400) };
}

/** Report a decode failure as a rejection rather than a thrown exception, so
 * one malformed payload does not tear down the surrounding view. */
function contractRejection(commandId: string, error: unknown): CanonicalRejection {
  const message = error instanceof ContractError ? error.message : String(error);
  return { ok: false, commandId, status: 0, message: message.slice(0, 400) };
}

export function createHttpCanonicalControlClient(
  options: HttpCanonicalControlClientOptions,
): CanonicalControlClient {
  const base = options.baseUrl.replace(/\/$/, "");
  const fetch = options.fetch.bind(globalThis);
  async function send(
    method: string,
    path: string,
    body: unknown,
    signal?: AbortSignal,
  ): Promise<{ status: number; value: unknown; text: string }> {
    const response = await fetch(`${base}${path}`, {
      method,
      signal,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
    });
    const text = await response.text();
    let value: unknown = null;
    try {
      value = JSON.parse(text);
    } catch {
      value = null;
    }
    return { status: response.status, value, text };
  }

  /**
   * Both configuration writes answer with the same `CanonicalConfiguration`
   * acknowledgement, so they share one decoder and one shape.
   */
  function acknowledgeConfiguration(
    commandId: string,
    value: unknown,
  ): CanonicalResult<CanonicalConfigurationAck> {
    try {
      const ack = decodeConfigurationAck(value);
      return {
        ok: true,
        commandId: ack.command_id,
        accepted: ack.accepted,
        projectId: ack.project_id,
        revision: ack.revision,
        configuration: ack.configuration,
      };
    } catch (error) {
      return contractRejection(commandId, error);
    }
  }

  return {
    async deleteChatConversation(projectId, sessionId) {
      const { status, value, text } = await send("DELETE",
        "/api/v1/canonical/chat/conversations?project_id=" + encodeURIComponent(projectId) + "&session_id=" + encodeURIComponent(sessionId), undefined);
      if (status !== 200) return rejection("delete-conversation", status, text);
      return decodeChatConversationsResponse(value);
    },
    async uploadChatImage(request, signal) {
      const { status, value, text } = await send("POST", "/api/v1/canonical/chat/images", request, signal);
      if (status !== 200) return rejection("image-upload", status, text);
      return decodeChatImage(value);
    },
    async readChatConversations(projectId) {
      const { status, value, text } = await send("GET",
        `/api/v1/canonical/chat/conversations?project_id=${encodeURIComponent(projectId)}`, undefined);
      if (status !== 200) return rejection("conversations", status, text);
      return decodeChatConversationsResponse(value);
    },
    async readChatMessages(projectId, sessionId) {
      const { status, value, text } = await send("GET",
        `/api/v1/canonical/chat/messages?project_id=${encodeURIComponent(projectId)}&session_id=${encodeURIComponent(sessionId)}`, undefined);
      if (status !== 200) return rejection("messages", status, text);
      return decodeChatMessagesResponse(value);
    },
    async listProjects() {
      const { value } = await send("GET", "/api/v1/canonical/projects", undefined);
      return decodeProjectsResponse(value).projects;
    },

    async importProject(commandId, root) {
      const { status, value, text } = await send(
        "POST",
        "/api/v1/canonical/projects/import",
        { command_id: commandId, root },
      );
      if (status !== 200) return rejection(commandId, status, text);
      try {
        // The backend's own `command_id` and `accepted` flag, not the ones the
        // client assumed.
        const response = decodeProjectResponse(value);
        return {
          ok: true as const,
          commandId: response.command_id,
          accepted: response.accepted,
          project: response.project,
        };
      } catch (error) {
        return contractRejection(commandId, error);
      }
    },

    async readConfiguration(projectId) {
      const { status, value, text } = await send(
        "GET",
        `/api/v1/canonical/configuration?project_id=${encodeURIComponent(projectId)}`,
        undefined,
      );
      if (status !== 200) return rejection("read", status, text);
      try {
        return decodeConfigurationEnvelope(value).configuration;
      } catch (error) {
        return contractRejection("read", error);
      }
    },

    async writeGlobalConfiguration(commandId, configuration) {
      const { status, value, text } = await send(
        "PUT",
        "/api/v1/canonical/configuration",
        { command_id: commandId, configuration },
      );
      if (status !== 200) return rejection(commandId, status, text);
      return acknowledgeConfiguration(commandId, value);
    },

    async writeProjectDefaults(commandId, projectId, defaults) {
      const { status, value, text } = await send(
        "PUT",
        `/api/v1/canonical/configuration/projects/${encodeURIComponent(projectId)}`,
        { command_id: commandId, defaults },
      );
      if (status !== 200) return rejection(commandId, status, text);
      return acknowledgeConfiguration(commandId, value);
    },

    async writeJobConfiguration(commandId, jobId, configuration) {
      const { status, value, text } = await send(
        "PUT",
        `/api/v1/canonical/jobs/${encodeURIComponent(jobId)}/configuration`,
        { command_id: commandId, configuration },
      );
      if (status !== 200) return rejection(commandId, status, text);
      try {
        const ack = decodeJobConfigResponse(value);
        return {
          ok: true as const,
          commandId: ack.command_id,
          accepted: ack.accepted,
          jobId: ack.job_id,
          revision: ack.revision,
          configuration: ack.configuration,
        };
      } catch (error) {
        return contractRejection(commandId, error);
      }
    },

    async readJobSnapshot(projectId, jobId) {
      const { status, value, text } = await send(
        "GET",
        `/api/v1/canonical/jobs?project_id=${encodeURIComponent(projectId)}&job_id=${encodeURIComponent(jobId)}`,
        undefined,
      );
      if (status !== 200) return rejection("snapshot", status, text);
      // The full snapshot envelope is what the runtime store reconciles
      // against; the decoder checks the envelope without stripping it.
      try {
        return decodeJobSnapshot(value);
      } catch (error) {
        return contractRejection("snapshot", error);
      }
    },

    async readJobEvents(projectId, jobId, after) {
      const { status, value, text } = await send(
        "GET",
        `/api/v1/canonical/jobs/events?project_id=${encodeURIComponent(projectId)}&job_id=${encodeURIComponent(jobId)}&after=${after}`,
        undefined,
      );
      if (status !== 200) return rejection("events", status, text);
      try {
        // The store takes the generated event type directly, so the decoded
        // payload is handed over without re-declaring it here.
        return decodeEventsEnvelope(value).events;
      } catch (error) {
        return contractRejection("events", error);
      }
    },

    async readDashboard(projectId, jobId) {
      const suffix = jobId ? `&job_id=${encodeURIComponent(jobId)}` : "";
      const { status, value, text } = await send(
        "GET",
        `/api/v1/canonical/dashboard?project_id=${encodeURIComponent(projectId)}${suffix}`,
        undefined,
      );
      if (status !== 200) return rejection("dashboard", status, text);
      try {
        return decodeDashboardResponse(value);
      } catch (error) {
        return contractRejection("dashboard", error);
      }
    },

    async launchJob(request) {
      const { status, value, text } = await send(
        "POST",
        "/api/v1/canonical/jobs/launch",
        request,
      );
      if (status !== 200) return rejection(request.command_id, status, text);
      try {
        return decodeJobLaunchResponse(value);
      } catch (error) {
        return contractRejection(request.command_id, error);
      }
    },

    async sendChatMessage(request) {
      const { status, value, text } = await send(
        "POST",
        "/api/v1/canonical/chat/send",
        request,
      );
      if (status !== 200) return rejection(request.command_id, status, text);
      try {
        return decodeJobLaunchResponse(value);
      } catch (error) {
        return contractRejection(request.command_id, error);
      }
    },

    async cancelChatMessage(sessionId, projectId) {
      const { status, value, text } = await send(
        "POST",
        "/api/v1/canonical/chat/cancel",
        { session_id: sessionId, ...(projectId ? { project_id: projectId } : {}) },
      );
      if (status !== 200) return rejection(sessionId, status, text);
      try {
        if (!isRecord(value)) throw new ContractError("cancel", "object response");
        const returnedSession = value["session_id"];
        const cancelled = value["cancelled"];
        if (typeof returnedSession !== "string" || typeof cancelled !== "boolean") {
          throw new ContractError("cancel", "session_id/cancelled");
        }
        return { ok: true as const, sessionId: returnedSession, cancelled };
      } catch (error) {
        return contractRejection(sessionId, error);
      }
    },

    chatStreamUrl(sessionId, jobId) {
      return `${base}/api/v1/canonical/chat/stream?session_id=${encodeURIComponent(sessionId)}&job_id=${encodeURIComponent(jobId)}`;
    },
  };
}

/** Local Project scope label for a backend Project identity. */
export function projectLabelFor(project: ProjectRecord): string {
  const tail = project.project_id.replace(/^project-/, "");
  return `Project ${tail.slice(0, 8)}`;
}

/** The PWA scope id used to isolate a canonical projection. */
export function scopeIdFor(project: ProjectRecord): ProjectId {
  return project.project_id as ProjectId;
}
