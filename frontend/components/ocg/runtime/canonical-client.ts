/**
 * Backend-backed control client for the OCG PWA.
 *
 * This is the PWA's only route to real OCG state. It carries stable command
 * identities, and every mutation returns a typed acknowledgement from the
 * backend. The client is transport-shaped only: it performs no authority
 * decision, derives no execution identity, and never edits a dispatched Run.
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
  decodeConfigurationAck,
  decodeConfigurationEnvelope,
  decodeDashboardResponse,
  decodeEventsEnvelope,
  decodeMissionResponse,
  decodeProjectResponse,
  decodeProjectsResponse,
  decodeWorkSnapshot,
  type CanonicalDashboardResponse,
  type CanonicalWorkSnapshot,
  type GlobalConfiguration,
  type JsonValue,
  type ProjectConfigurationView,
  type ProjectRecord,
} from "../contracts";

export type {
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

export type CanonicalMissionConfigAck = {
  ok: true;
  commandId: string;
  accepted: boolean;
  missionId: string;
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
  writeMissionConfiguration(
    commandId: string,
    missionId: string,
    configuration: JsonValue,
  ): Promise<CanonicalResult<CanonicalMissionConfigAck>>;
  readWorkSnapshot(
    projectId: string,
    missionId: string,
  ): Promise<CanonicalResult<CanonicalWorkSnapshot>>;
  readWorkEvents(
    projectId: string,
    missionId: string,
    after: number,
  ): Promise<CanonicalResult<CanonicalBackendEvent[]>>;
  readDashboard(
    projectId: string,
    missionId?: string,
  ): Promise<CanonicalResult<CanonicalDashboardResponse>>;
}

export type FetchLike = (
  input: string,
  init?: { method?: string; body?: string; headers?: Record<string, string> },
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
  async function send(
    method: string,
    path: string,
    body: unknown,
  ): Promise<{ status: number; value: unknown; text: string }> {
    const response = await options.fetch(`${base}${path}`, {
      method,
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

    async writeMissionConfiguration(commandId, missionId, configuration) {
      const { status, value, text } = await send(
        "PUT",
        `/api/v1/canonical/missions/${encodeURIComponent(missionId)}/configuration`,
        { command_id: commandId, configuration },
      );
      if (status !== 200) return rejection(commandId, status, text);
      try {
        const ack = decodeMissionResponse(value);
        return {
          ok: true as const,
          commandId: ack.command_id,
          accepted: ack.accepted,
          missionId: ack.mission_id,
          revision: ack.revision,
          configuration: ack.configuration,
        };
      } catch (error) {
        return contractRejection(commandId, error);
      }
    },

    async readWorkSnapshot(projectId, missionId) {
      const { status, value, text } = await send(
        "GET",
        `/api/v1/canonical/work?project_id=${encodeURIComponent(projectId)}&mission_id=${encodeURIComponent(missionId)}`,
        undefined,
      );
      if (status !== 200) return rejection("snapshot", status, text);
      // The full snapshot envelope is what the runtime store reconciles
      // against; the decoder checks the envelope without stripping it.
      try {
        return decodeWorkSnapshot(value);
      } catch (error) {
        return contractRejection("snapshot", error);
      }
    },

    async readWorkEvents(projectId, missionId, after) {
      const { status, value, text } = await send(
        "GET",
        `/api/v1/canonical/work/events?project_id=${encodeURIComponent(projectId)}&mission_id=${encodeURIComponent(missionId)}&after=${after}`,
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

    async readDashboard(projectId, missionId) {
      const suffix = missionId ? `&mission_id=${encodeURIComponent(missionId)}` : "";
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
