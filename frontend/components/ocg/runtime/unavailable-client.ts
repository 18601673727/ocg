import type {
  ProjectUsageResponse, ConversationUsageResponse, JobUsageResponse, UsageWindow,
  ChatImage, ChatImageUploadRequest, ChatConversationsResponse, ChatMessagesResponse,
  CanonicalJobSpawnRequest, CanonicalJobSpawnResponse, CanonicalJobOperationRequest,
  CanonicalJobOperationResponse, CanonicalDashboardResponse, CanonicalJobLaunchAck,
  CanonicalJobSnapshot, GlobalConfiguration, ChatSendRequest, JobLaunchRequest,
  JsonValue, ProjectConfigurationView, ProjectRecord, Profile, ProfileView,
  SetupConnectResponse, SetupModelSelection, SetupModelsResponse, SetupBrowseResponse, SetupProjectResponse,
} from "../contracts";
import type { CanonicalBackendEvent } from "./canonical-store";
export type { Profile, ProfileView } from "../contracts";

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

export function isCanonicalRejection<T>(value: CanonicalResult<T>): value is CanonicalRejection {
  return (value as { ok?: boolean }).ok === false;
}


export interface CanonicalControlClient {
  readProjectUsage?(projectId: string, window: UsageWindow, signal?: AbortSignal): Promise<CanonicalResult<ProjectUsageResponse>>;
  readConversationUsage?(projectId: string, sessionId: string, signal?: AbortSignal): Promise<CanonicalResult<ConversationUsageResponse>>;
  readJobUsage?(projectId: string, jobId: string, signal?: AbortSignal): Promise<CanonicalResult<JobUsageResponse>>;
  deleteChatConversation?(projectId: string, sessionId: string): Promise<CanonicalResult<ChatConversationsResponse>>;
  uploadChatImage?(request: ChatImageUploadRequest, signal?: AbortSignal): Promise<CanonicalResult<ChatImage>>;
  readChatConversations(projectId: string): Promise<CanonicalResult<ChatConversationsResponse>>;
  readChatMessages(projectId: string, sessionId: string): Promise<CanonicalResult<ChatMessagesResponse>>;
  listProjects(): Promise<ProjectRecord[]>;
  importProject(commandId: string, root: string): Promise<CanonicalResult<CanonicalProjectAck>>;
  readConfiguration(projectId: string): Promise<CanonicalResult<ProjectConfigurationView>>;
  /** `expectedRevision` is the `global_revision` the change was computed from. */
  writeGlobalConfiguration(
    commandId: string,
    configuration: GlobalConfiguration,
    expectedRevision: number,
  ): Promise<CanonicalResult<CanonicalConfigurationAck>>;
  /** `expectedRevision` is the `project_revision` the change was computed from. */
  writeProjectDefaults(
    commandId: string,
    projectId: string,
    defaults: JsonValue,
    expectedRevision: number,
  ): Promise<CanonicalResult<CanonicalConfigurationAck>>;
  writeJobConfiguration(
    commandId: string,
    jobId: string,
    configuration: JsonValue,
  ): Promise<CanonicalResult<CanonicalJobConfigAck>>;
  spawnJob(parentJobId: string, request: CanonicalJobSpawnRequest): Promise<CanonicalResult<CanonicalJobSpawnResponse>>;
  cancelJob(jobId: string, request: CanonicalJobOperationRequest): Promise<CanonicalResult<CanonicalJobOperationResponse>>;
  retryJob(jobId: string, request: CanonicalJobOperationRequest): Promise<CanonicalResult<CanonicalJobOperationResponse>>;
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


function unavailable(): never {
  throw new Error("This 0.4.5 workflow has no 0.5 contract and is not available in this build.");
}

/** Type-compatible boundaries for retained, unrouted P2 components. No HTTP transport. */
export function createHttpCanonicalControlClient(options: HttpCanonicalControlClientOptions): CanonicalControlClient & Required<Pick<CanonicalControlClient, "readProjectUsage" | "readConversationUsage" | "readJobUsage">> {
  void options;
  return {
    readProjectUsage: unavailable,
    readConversationUsage: unavailable,
    readJobUsage: unavailable,
    deleteChatConversation: unavailable,
    uploadChatImage: unavailable,
    readChatConversations: unavailable,
    readChatMessages: unavailable,
    listProjects: unavailable,
    importProject: unavailable,
    readConfiguration: unavailable,
    writeGlobalConfiguration: unavailable,
    writeProjectDefaults: unavailable,
    writeJobConfiguration: unavailable,
    spawnJob: unavailable,
    cancelJob: unavailable,
    retryJob: unavailable,
    readJobSnapshot: unavailable,
    readJobEvents: unavailable,
    readDashboard: unavailable,
    launchJob: unavailable,
    sendChatMessage: unavailable,
    cancelChatMessage: unavailable,
    chatStreamUrl: unavailable,
  };
}

export function createProfileClient(baseUrl: string, fetchImpl: typeof fetch): {
  read(): Promise<ProfileView>;
  createNew(): Promise<ProfileView>;
  replace(revision: string, profile: Profile): Promise<ProfileView>;
  saveCredential(name: string, value: string): Promise<ProfileView>;
} {
  void baseUrl; void fetchImpl;
  return { read: unavailable, createNew: unavailable, replace: unavailable, saveCredential: unavailable };
}

export interface SetupClient {
  connectProvider(name: string, endpoint: string, apiKey: string): Promise<SetupConnectResponse>;
  refreshModels(providerKey: string, revision: string): Promise<SetupConnectResponse>;
  saveModels(providerKey: string, models: SetupModelSelection[], defaultModel: string | undefined, revision: string): Promise<SetupModelsResponse>;
  browseDirectory(path?: string): Promise<SetupBrowseResponse>;
  initProject(commandId: string, root: string): Promise<SetupProjectResponse>;
}

export function createSetupClient(baseUrl: string, fetchImpl: typeof fetch): SetupClient {
  void baseUrl; void fetchImpl;
  return { connectProvider: unavailable, refreshModels: unavailable, saveModels: unavailable, browseDirectory: unavailable, initProject: unavailable };
}

export function projectLabelFor(project: ProjectRecord): string {
  return `Project ${project.project_id.replace(/^project-/, "").slice(0, 8)}`;
}

export function scopeIdFor(project: ProjectRecord): string {
  return project.project_id;
}
