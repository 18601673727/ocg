/**
 * Backend-backed Chat and Job launch adapter.
 *
 * The shell runtime is a fixture projection, but a Chat turn and a Job launch
 * are real product side effects. This client extends `MockOcgRuntimeClient` so
 * the shell keeps its single runtime store, and overrides only the operations
 * that must reach the canonical backend.
 *
 * A launch is:
 *   1. `POST /api/v1/canonical/jobs/launch`, decoded against the generated
 *      `JobLaunchResponse` contract;
 *   2. on acceptance, a read of the authoritative canonical snapshot and its
 *      event tail for the Job the backend named;
 *   3. projection of that snapshot through the same
 *      `RuntimeStore`/`canonical-store` path the control surface uses, so the
 *      `JobExecution` the UI renders is assembled from backend entities by
 *      `assembleJobExecution` and never fabricated here.
 *
 * Chat turns reuse the same canonical Job/Attempt/Call lane:
 *   1. `POST /api/v1/canonical/chat/send` with a `JobLaunchRequest` whose
 *      `objective` is the plain user message;
 *   2. `EventSource` tail on `/api/v1/canonical/chat/stream` carrying only
 *      normalized provider deltas from the real provider path;
 *   3. `POST /api/v1/canonical/chat/cancel` revoking Attempt authority before
 *      stopping the provider transport.
 *
 * Chat is never simulated from a scenario name. Being this class at all means
 * the invocation is attached to a loopback control endpoint, so this adapter
 * always answers Chat from the backend: the scenario only seeds the shell
 * projection, never the runtime authority. The fixture timers in
 * `MockOcgRuntimeClient` stay reachable only where no control endpoint exists
 * to be canonical for.
 *
 * Whether chat can execute at all is the backend's answer, not a frontend
 * guess: the same `runnable_choices` readiness authority the bootstrap route
 * uses is read once on subscription and again per attempt, and projected as the
 * runtime status, so a workspace without an executable provider/model reports
 * configuration required instead of a ready-looking runtime.
 *
 * No credential, provider selection, or execution identity is decided in the
 * client: the backend freezes all of it and this adapter only reports what it
 * returned.
 */

import { decodeChatImage, type ChatImage, type ChatImageUploadRequest, type ChatMessagesResponse, type ChatSendRequest, type JobLaunchRequest } from "../contracts";
import type {
  JobLaunchCommand,
  JobLaunchResult,
  RuntimeAuthority,
  ScenarioId,
} from "./runtime-types";
import type { ChatMessage, ChatSession, OcgRuntimeEvent, RuntimeStatus, SendMessageInput } from "../types";
import type { CreateSessionInput } from "./runtime-types";
import { RuntimeClientBase } from "./mock-client";
import { createProfileClient } from "../profile/profile-client";
import {
  createHttpCanonicalControlClient,
  isCanonicalRejection,
  type CanonicalControlClient,
  type CanonicalJobLaunchAck,
} from "./canonical-client";
import { selectCanonical } from "./canonical-store";
import type { JobExecution } from "../execution/domain";
import { isNonEmptyString, isRecord } from "@/lib/narrow";
import { retryPresentation } from "../chat/retry";

/** Bounds the snapshot/event refetch loop when the backend keeps demanding a resync. */
const MAX_REFRESH_ROUNDS = 4;
const PROJECT_JOB_REFRESH_MS = 2_000;
/** How often a streaming turn re-reads its Job, so observable activity stays current. */
const CHAT_EXECUTION_REFRESH_MS = 1_500;

/** Backend-computed readiness for one chat turn, plus the status that reports it. */
type ChatAvailability = {
  available: boolean;
  status: RuntimeStatus;
};

export class CanonicalOcgRuntimeClient extends RuntimeClientBase {
  readonly authority: RuntimeAuthority = "canonical";

  private readonly control: CanonicalControlClient;
  private readonly profile: ReturnType<typeof createProfileClient>;
  private chatCounter = 0;
  private readonly pendingSends = new Map<string, Promise<void>>();
  private readonly historyEpochs = new Map<string, number>();
  private readonly chatStreams = new Map<string, { source: EventSource; assistantId: string; jobId: string; refresh: ReturnType<typeof setInterval> | null }>();
  /**
   * Where the committed presentation of the streaming message ends, per session.
   *
   * A provider round announces its boundary once, before its first HTTP attempt.
   * Every later HTTP attempt for that same round re-asserts this boundary rather
   * than taking a new one, which is what drops the failed attempt's provisional
   * tail instead of the rounds already committed ahead of it.
   */
  private readonly roundCommits = new Map<string, { committedContentLength: number; committedImageCount: number }>();
  private readonly sessionProjects = new Map<string, string>();
  private readonly projectHydrations = new Map<string, Promise<void>>();
  private readonly projectJobWatermarks = new Map<string, Map<string, string>>();
  private readonly projectJobRefreshes = new Map<string, Promise<void>>();
  private readonly chatJobsBySession = new Map<string, { projectId: string; jobId: string }>();
  private readonly deletedSessions = new Set<string>();
  private readonly deletingSessions = new Set<string>();
  /** Sessions created locally whose backend Conversation never materialized. */
  private readonly localDraftSessions = new Set<string>();
  private availabilityProbed = false;
  private reportedStatus: RuntimeStatus | null = null;
  private projectJobRefreshTimer: ReturnType<typeof setInterval> | null = null;
  private projectJobRefreshProject: string | null = null;
  private lastProjectJobRefreshError = new Map<string, string>();

  constructor(
    scenario: ScenarioId,
    control: CanonicalControlClient,
    profile: ReturnType<typeof createProfileClient>,
  ) {
    super(scenario, true, "canonical");
    this.control = control;
    this.profile = profile;
  }

  /** Build the adapter for a loopback control base URL. */
  static connect(scenario: ScenarioId, baseUrl: string, fetchImpl: typeof fetch): CanonicalOcgRuntimeClient {
    return new CanonicalOcgRuntimeClient(
      scenario,
      createHttpCanonicalControlClient({ baseUrl, fetch: fetchImpl }),
      createProfileClient(baseUrl, fetchImpl),
    );
  }

  /**
   * The runtime status the Chat surfaces render is the backend's, so the first
   * subscription asks for readiness once rather than reporting the seeded
   * fixture status as if it were authoritative.
   */
  override subscribe(listener: (event: OcgRuntimeEvent) => void): () => void {
    const stop = super.subscribe(listener);
    if (!this.availabilityProbed) {
      this.availabilityProbed = true;
      void this.syncChatAvailability();
    }
    return stop;
  }

  /**
   * The new setup wizard (SetupWizard) is a 3-step flow that completes without
   * ever reaching the legacy "ready" stage. Override to mark onboarding done
   * regardless of stage.
   */
  override async completeOnboarding(): Promise<void> {
    const bootstrap = this.store.getSnapshot().bootstrap;
    const onboarding = bootstrap.onboarding;
    if (!onboarding) return;
    this.updateBootstrap({
      ...bootstrap,
      ready: true,
      onboarding: {
        ...onboarding,
        completedStages: onboarding.completedStages.includes("ready")
          ? onboarding.completedStages
          : [...onboarding.completedStages, "ready"],
        failure: undefined,
      },
    });
  }

  async launchJob(command: JobLaunchCommand): Promise<JobLaunchResult> {
    const session = this.store.getSnapshot().sessions.find((item) => item.id === command.sessionId);
    const response = await this.control.launchJob(toLaunchRequest({ ...command, sessionId: session?.sessionId ?? command.sessionId }));
    if (isCanonicalRejection(response)) {
      const result = failedLaunch(command, response.message);
      this.emitLaunchResult(command, result);
      return result;
    }

    const result = launchResultFrom(command, response);
    this.emitLaunchResult(command, result);

    if (result.outcome === "accepted" && response.job_id !== null) {
      this.chatJobsBySession.set(command.sessionId, { projectId: response.project_id, jobId: response.job_id });
      const execution = await this.projectCanonicalExecution(response.project_id, response.job_id);
      if (execution !== null) {
        // The backend's Project identity scopes this Job update independently
        // of whether its Chat presentation session is currently attached.
        this.emit(
          { type: "job.execution-updated", sessionId: command.sessionId, execution, accounting: null },
          { projectId: response.project_id, commandId: command.commandId },
        );
      }
    }

    return result;
  }

  override async createSession(input: CreateSessionInput): Promise<ChatSession> {
    if (!input.projectId) throw new Error("New Chat requires a Project.");
    // An untouched local draft for the same Project means the user has not
    // started the conversation yet, so repeated New Chat must reuse it
    // instead of stacking another frontend-only ghost session.
    const existing = this.store.getSnapshot().sessions.find((session) =>
      session.projectId === input.projectId &&
      this.localDraftSessions.has(session.id) &&
      (this.store.getSnapshot().messagesBySession[session.id] ?? []).length === 0 &&
      !this.chatJobsBySession.has(session.id) &&
      !this.pendingSends.has(session.id) &&
      !this.chatStreams.has(session.id),
    );
    if (existing) return { ...existing };

    const sessionId = `chat-${crypto.randomUUID()}`;
    const id = chatSessionKey(input.projectId, sessionId);
    const session: ChatSession = {
      id,
      sessionId,
      projectId: input.projectId,
      title: input.title?.trim() || "",
      workType: input.workType,
      updatedAt: "now",
    };
    this.bindSessionProject(id, input.projectId);
    this.localDraftSessions.add(id);
    this.emit({ type: "conversation.session-created", session: { ...session } }, { projectId: input.projectId });
    return { ...session };
  }

  hydrateProject(projectId: string): Promise<void> {
    this.startProjectJobPolling(projectId);
    void this.refreshProjectJobs(projectId, true);
    const existing = this.projectHydrations.get(projectId);
    if (existing) return existing;
    const hydration = this.loadProjectHistory(projectId).catch((cause: unknown) => {
      this.projectHydrations.delete(projectId);
      throw cause;
    });
    this.projectHydrations.set(projectId, hydration);
    return hydration;
  }

  override dispose(): void {
    if (this.projectJobRefreshTimer !== null) clearInterval(this.projectJobRefreshTimer);
    this.projectJobRefreshTimer = null;
    this.projectJobRefreshProject = null;
    for (const tracked of this.chatStreams.values()) {
      tracked.source.close();
      if (tracked.refresh !== null) clearInterval(tracked.refresh);
    }
    this.chatStreams.clear();
    this.roundCommits.clear();
    super.dispose();
  }

  private startProjectJobPolling(projectId: string): void {
    if (this.projectJobRefreshProject === projectId && this.projectJobRefreshTimer !== null) return;
    if (this.projectJobRefreshTimer !== null) clearInterval(this.projectJobRefreshTimer);
    this.projectJobRefreshProject = projectId;
    const timer = setInterval(() => {
      void this.refreshProjectJobs(projectId);
    }, PROJECT_JOB_REFRESH_MS);
    this.projectJobRefreshTimer = timer;
    if (typeof timer === "object" && timer !== null && "unref" in timer && typeof timer.unref === "function") {
      timer.unref();
    }
  }

  override async deleteSession(sessionId: string): Promise<void> {
    const session = this.store.getSnapshot().sessions.find(item => item.id === sessionId);
    if (!session) return;
    if (this.pendingSends.has(sessionId) || this.chatStreams.has(sessionId) || this.streamingMessage(sessionId)) {
      throw new Error("Conversation is running. Stop it before deleting.");
    }
    // An untouched local draft never materialized a backend Conversation, so
    // deleting it must not invoke the canonical delete path.
    if (this.localDraftSessions.has(sessionId)) {
      this.localDraftSessions.delete(sessionId);
      this.sessionProjects.delete(sessionId);
      this.chatJobsBySession.delete(sessionId);
      this.historyEpochs.delete(sessionId);
      await super.deleteSession(sessionId);
      return;
    }
    if (!session.projectId || !session.sessionId || !this.control.deleteChatConversation) {
      throw new Error("This runtime does not support deleting chats.");
    }
    if (this.deletingSessions.has(sessionId)) throw new Error("Conversation deletion is already in progress.");
    this.deletingSessions.add(sessionId);
    try {
      const response = await this.control.deleteChatConversation(session.projectId, session.sessionId);
      if (isCanonicalRejection(response)) throw new Error(response.message);
      if (response.project_id !== session.projectId || response.conversations.some(item => item.session_id === session.sessionId)) {
        throw new Error("Conversation deletion was not confirmed by the backend.");
      }
      this.deletedSessions.add(sessionId);
      this.historyEpochs.set(sessionId, (this.historyEpochs.get(sessionId) ?? 0) + 1);
      await super.deleteSession(sessionId);
      this.sessionProjects.delete(sessionId);
      this.chatJobsBySession.delete(sessionId);
    } finally {
      this.deletingSessions.delete(sessionId);
    }
  }

  private async loadProjectHistory(projectId: string): Promise<void> {
    const response = await this.control.readChatConversations(projectId);
    if (isCanonicalRejection(response)) throw new Error(response.message);
    if (response.project_id !== projectId) throw new Error("Conversation Project mismatch.");
    const histories: ChatMessagesResponse[] = [];
    for (let offset = 0; offset < response.conversations.length; offset += 4) {
      histories.push(...await Promise.all(response.conversations.slice(offset, offset + 4).map((conversation) =>
        this.readSessionHistory(projectId, conversation.session_id))));
    }
    // Session creation prepends, so install oldest first to retain backend order.
    for (const history of histories.reverse()) this.installHistory(history);
  }

  private async readSessionHistory(projectId: string, sessionId: string): Promise<ChatMessagesResponse> {
    const response = await this.control.readChatMessages(projectId, sessionId);
    if (isCanonicalRejection(response)) throw new Error(response.message);
    if (response.project_id !== projectId || response.conversation.session_id !== sessionId) {
      throw new Error("Conversation scope mismatch.");
    }
    return response;
  }

  private installHistory(history: ChatMessagesResponse): void {
    const projectId = history.project_id;
    const id = chatSessionKey(projectId, history.conversation.session_id);
    if (this.deletedSessions.has(id)) return;
    // A Conversation with this ID exists on the backend, so the session is
    // canonical now — never treat it as a local draft again.
    this.localDraftSessions.delete(id);
    const session: ChatSession = {
      id, projectId, sessionId: history.conversation.session_id,
      title: history.conversation.title || history.messages.find(message => message.role === "user" && message.content.trim())?.content.trim().slice(0, 80) || history.messages.find(message => message.role === "user" && message.images.length)?.images[0]?.name || "",
      workType: "coding", updatedAt: chatTimestamp(history.conversation.updated_at),
    };
    this.bindSessionProject(id, projectId);
    this.emit({ type: "conversation.session-created", session }, { projectId });
    const messages: ChatMessage[] = history.messages.filter((message) => message.state !== "deleted").map((message) => ({
      id: message.message_id, commandId: message.command_id, jobId: message.job_id ?? undefined, role: message.role,
      images: message.images, content: message.content, failureReason: message.failure_reason ?? undefined, createdAt: chatTimestamp(message.created_at),
      status: message.state === "complete" ? "completed" :
        message.state === "failed" && message.attempt_state === "cancelled" ? "cancelled" :
        message.state === "failed" ? "failed" : "pending",
    }));
    this.emit({ type: "conversation.history-loaded", sessionId: id, messages }, { projectId });
    const jobId = [...history.messages].reverse().find(message => message.role === "assistant" && message.job_id)?.job_id;
    if (jobId) {
      this.chatJobsBySession.set(id, { projectId, jobId });
      void this.refreshExecution(id, projectId, jobId);
    } else {
      this.chatJobsBySession.delete(id);
    }
    if (this.chatStreams.has(id) || this.pendingSends.has(id)) return;
    const replay = history.messages.find((message) => message.replay_job_id !== null);
    const assistant = replay && this.store.getSnapshot().messagesBySession[id]?.find(message =>
      message.id === replay.message_id || (message.commandId === replay.command_id && message.role === replay.role));
    if (replay?.replay_job_id && assistant && (assistant.status === "pending" || assistant.status === "streaming")) {
      this.emit({ type: "conversation.message-started", sessionId: id, message: { ...assistant, status: "streaming" } }, { projectId });
      this.openChatStream(id, replay.replay_job_id, assistant.id);
    }
  }

  private async refreshSessionHistory(sessionId: string): Promise<void> {
    const session = this.store.getSnapshot().sessions.find((item) => item.id === sessionId);
    if (!session?.projectId || !session.sessionId) return;
    const epoch = this.historyEpochs.get(sessionId) ?? 0;
    try {
      const history = await this.readSessionHistory(session.projectId, session.sessionId);
      if (epoch === (this.historyEpochs.get(sessionId) ?? 0)) this.installHistory(history);
    } catch (cause) {
      this.emit({ type: "error", message: `Chat history refresh failed: ${cause instanceof Error ? cause.message : String(cause)}` });
    }
  }

  /**
   * Retry re-executes the message's own Job against its frozen target: same
   * turn, same assistant Message, a replacement Attempt. It never reads the
   * composer selection and never sends a new turn.
   */
  override async retryMessage(sessionId: string, messageId: string): Promise<void> {
    const snapshot = this.store.getSnapshot();
    const session = snapshot.sessions.find((item) => item.id === sessionId);
    const message = snapshot.messagesBySession[sessionId]?.find((item) => item.id === messageId);
    if (!session?.projectId || !message || message.role !== "assistant" || !message.jobId) {
      throw new Error("This canonical Chat message cannot be retried.");
    }

    const execution = await this.projectCanonicalExecution(session.projectId, message.jobId);
    if (!execution) throw new Error("Job snapshot is unavailable.");
    const response = await this.control.retryJob(message.jobId, { expected_generation: execution.generation });
    if (isCanonicalRejection(response)) throw new Error(response.message);
    if (response.job_id !== message.jobId) throw new Error("Job operation identity mismatch.");

    this.historyEpochs.set(sessionId, (this.historyEpochs.get(sessionId) ?? 0) + 1);
    this.chatJobsBySession.set(sessionId, { projectId: session.projectId, jobId: message.jobId });
    this.closeChatStream(sessionId, true);
    this.emit({ type: "conversation.message-started", sessionId, message: retryPresentation(message) }, { projectId: session.projectId });
    this.openChatStream(sessionId, message.jobId, message.id);
    await this.syncProjectJobs(session.projectId, true);
  }

  override async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    if (this.deletedSessions.has(sessionId) || this.deletingSessions.has(sessionId)) {
      throw new Error("Conversation has been deleted or is being deleted.");
    }
    if (this.enqueueIfBusy(sessionId, input)) return;
    const pending = this.pendingSends.get(sessionId);
    if (pending) await pending;
    const sending = this.sendChatTurn(sessionId, input);
    this.pendingSends.set(sessionId, sending);
    try {
      await sending;
    } finally {
      if (this.pendingSends.get(sessionId) === sending) this.pendingSends.delete(sessionId);
    }
  }

  private async sendChatTurn(sessionId: string, input: SendMessageInput): Promise<void> {
    this.historyEpochs.set(sessionId, (this.historyEpochs.get(sessionId) ?? 0) + 1);
    const content = input.content.trim();
    if (!content && !input.images?.length) return;

    const snapshot = this.store.getSnapshot();
    const session = snapshot.sessions.find((item) => item.id === sessionId);
    if (!session) {
      this.emit({ type: "error", message: `Unknown chat session: ${sessionId}` });
      return;
    }

    const commandId = `cmd-chat-${crypto.randomUUID()}`;
    const userMessage: ChatMessage = {
      images: input.images,
      commandId,
      optimistic: true,
      id: `chat-user-${Date.now().toString(36)}-${this.chatCounter++}`,
      role: "user",
      content,
      createdAt: chatClockLabel(),
      status: "completed",
    };
    this.emit({ type: "conversation.message-started", sessionId, message: { ...userMessage } });
    this.emit({ type: "conversation.session-updated", session: { ...session, title: session.title || content.slice(0, 80) || input.images?.[0]?.name || "", updatedAt: "now" } });

    const assistantId = `chat-assistant-${Date.now().toString(36)}-${this.chatCounter++}`;
    const assistant: ChatMessage = {
      commandId,
      optimistic: true,
      id: assistantId,
      role: "assistant",
      content: "",
      createdAt: chatClockLabel(),
      status: "streaming",
    };
    this.emit({ type: "conversation.message-started", sessionId, message: { ...assistant } });

    const projectId = await this.resolveChatProject(sessionId, input.projectId);
    if (!projectId) {
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: { ...assistant, content: "", failureCode: "project-missing", status: "failed" },
      });
      return;
    }

    // Backend readiness authority. Without an executable provider/model the
    // turn is reported as unavailable instead of being answered locally, and
    // the same verdict replaces the runtime status the Chat surfaces render.
    const availability = await this.syncChatAvailability();
    if (!availability.available) {
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: {
          ...assistant,
          content: "",
          failureReason: availability.status.detailCode ? undefined : availability.status.detail,
          failureCode: availability.status.detailCode === "configuration-required" ? "configuration-required" : undefined,
          status: "failed",
        },
      });
      return;
    }

    const request: ChatSendRequest = {
      image_ids: input.images?.map(image => image.id) ?? [],
      selection: input.selection ?? null,
      command_id: commandId,
      draft_id: commandId,
      project_id: projectId,
      session_id: session.sessionId ?? sessionId,
      objective: content,
      success_criteria: null,
      constraints: null,
      hard_budget_micros: 0,
      resource_commitment: null,
    };
    let response;
    try {
      response = await this.control.sendChatMessage(request);
    } catch (cause) {
      const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
      if (!current || current.status !== "streaming") return;
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: {
          ...current,
          content: "",
          failureReason: (cause instanceof Error ? cause.message : String(cause)).slice(0, 1024),
          status: "failed",
        },
      });
      return;
    }
    if (isCanonicalRejection(response)) {
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: { ...assistant, content: "", failureReason: response.message.slice(0, 1024), status: "failed" },
      });
      return;
    }
    if (response.outcome !== "accepted" || response.job_id === null) {
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: { ...assistant, content: "", failureReason: response.message.slice(0, 1024), status: "failed" },
      });
      return;
    }

    // Only an accepted turn supersedes the previous Attempt on the backend.
    // Keep its EventSource alive until then so failed sends leave it streaming.
    this.chatJobsBySession.set(sessionId, { projectId, jobId: response.job_id });
    this.localDraftSessions.delete(sessionId);
    this.emit({ type: "conversation.message-started", sessionId, message: { ...assistant, jobId: response.job_id } }, { projectId });
    this.closeChatStream(sessionId, true);
    this.openChatStream(sessionId, response.job_id, assistantId);
    void this.refreshExecution(sessionId, projectId, response.job_id);
  }

  async uploadChatImage(request: ChatImageUploadRequest, signal?: AbortSignal): Promise<ChatImage> {
    if (!this.control.uploadChatImage) throw new Error("Image upload is unavailable.");
    const result = await this.control.uploadChatImage(request, signal);
    if (isCanonicalRejection(result)) throw new Error(result.message);
    return result;
  }

  async cancelJob(jobId: string, expectedGeneration: number): Promise<void> {
    const response = await this.control.cancelJob(jobId, { expected_generation: expectedGeneration });
    if (isCanonicalRejection(response)) throw new Error(response.message);
    if (response.job_id !== jobId) throw new Error("Job operation identity mismatch.");
    await this.syncProjectJobs(response.snapshot.project_id, true);
  }

  async retryJob(jobId: string, expectedGeneration: number): Promise<void> {
    const response = await this.control.retryJob(jobId, { expected_generation: expectedGeneration });
    if (isCanonicalRejection(response)) throw new Error(response.message);
    if (response.job_id !== jobId) throw new Error("Job operation identity mismatch.");
    await this.syncProjectJobs(response.snapshot.project_id, true);
  }

  override async cancel(sessionId: string): Promise<void> {
    this.pauseQueue(sessionId);
    this.historyEpochs.set(sessionId, (this.historyEpochs.get(sessionId) ?? 0) + 1);
    // Admission can still be in flight when an automatically dequeued turn is stopped.
    await this.pendingSends.get(sessionId);
    const session = this.store.getSnapshot().sessions.find((item) => item.id === sessionId);
    const link = this.chatJobsBySession.get(sessionId);
    let cancelled: boolean;
    if (link) {
      const execution = await this.projectCanonicalExecution(link.projectId, link.jobId);
      if (!execution) throw new Error("Job snapshot is unavailable.");
      const response = await this.control.cancelJob(link.jobId, { expected_generation: execution.generation });
      if (isCanonicalRejection(response)) throw new Error(response.message);
      cancelled = response.accepted;
      await this.syncProjectJobs(link.projectId, true);
    } else {
      const response = await this.control.cancelChatMessage(session?.sessionId ?? sessionId, session?.projectId);
      if (isCanonicalRejection(response)) throw new Error(response.message);
      cancelled = response.cancelled;
    }
    if (!cancelled) {
      void this.refreshSessionHistory(sessionId);
      return;
    }
    const streaming = this.streamingMessage(sessionId);
    // Backend authority was revoked first; reflect it locally and stop the
    // SSE tail. A late provider `Failed("chat cancelled")` is ignored once
    // the message is terminal.
    this.closeChatStream(sessionId, false);
    if (streaming) {
      this.emit({ type: "cancelled", sessionId, messageId: streaming.id });
    } else {
      this.emit({ type: "cancelled", sessionId });
    }
    void this.refreshSessionHistory(sessionId);
  }

  /**
   * Read the one backend readiness authority and project it as the runtime
   * status.
   *
   * `runnable_choices` is the backend's own answer to "can this workspace
   * execute anything", already computed against the same selection, endpoint
   * and credential rules canonical launch enforces. The frontend never
   * re-derives it, so a workspace with no executable provider/model reports
   * configuration required instead of a runtime that looks ready.
   */
  private async syncChatAvailability(): Promise<ChatAvailability> {
    const availability = await this.probeChatAvailability();
    const current = this.reportedStatus;
    if (current === null || current.state !== availability.status.state || current.detail !== availability.status.detail) {
      this.reportedStatus = availability.status;
      this.emit({ type: "runtime.status-changed", status: { ...availability.status } });
    }
    return availability;
  }

  private async probeChatAvailability(): Promise<ChatAvailability> {
    try {
      const view = await this.profile.read();
      if (view.runnable_choices.length > 0) {
        return { available: true, status: { state: "connected", detailCode: "provider-ready", detail: "OCG provider runtime" } };
      }
      return {
        available: false,
        status: {
          state: "disconnected",
          detailCode: "configuration-required",
          detail: "no runnable provider or model: configuration required",
        },
      };
    } catch (cause) {
      return {
        available: false,
        status: {
          state: "failed",
          detail: `OCG control endpoint unreachable: ${cause instanceof Error ? cause.message : String(cause)}`,
        },
      };
    }
  }

  private async resolveChatProject(sessionId: string, activeProjectId?: string): Promise<string | null> {
    // Resolution order is explicit: the session's own recorded binding wins
    // so an old turn never migrates when the project list ordering changes;
    // otherwise the caller's current UI project applies and is recorded as
    // the session's binding. Anything else fails clearly — never a silent
    // first-project pick.
    const known = this.sessionProjects.get(sessionId) ?? this.store.getSync().sessionProjects[sessionId];
    if (typeof known === "string" && known.length > 0) return known;
    const active = activeProjectId?.trim();
    if (active) {
      if (!this.sessionProjects.has(sessionId)) {
        this.sessionProjects.set(sessionId, active);
      }
      return active;
    }
    return null;
  }

  /** Record the Project a session belongs to (called at session creation). */
  bindSessionProject(sessionId: string, projectId: string): void {
    if (!sessionId || !projectId) return;
    if (!this.sessionProjects.has(sessionId)) {
      this.sessionProjects.set(sessionId, projectId);
    }
  }

  private streamingMessage(sessionId: string): ChatMessage | null {
    const messages = this.store.getSnapshot().messagesBySession[sessionId] ?? [];
    const active = [...messages].reverse().find((message) => message.status === "streaming");
    return active ? { ...active } : null;
  }

  private openChatStream(sessionId: string, jobId: string, assistantId: string): void {
    const session = this.store.getSnapshot().sessions.find((item) => item.id === sessionId);
    const url = this.control.chatStreamUrl(session?.sessionId ?? sessionId, jobId);
    const source = new EventSource(url);
    // Native Tool Calls between provider rounds carry no stream event; the
    // canonical snapshot is what makes them observable while the turn runs.
    const projectId = session?.projectId;
    const refresh = projectId ? setInterval(() => {
      if (this.chatStreams.get(sessionId)?.source !== source) return;
      // A missed read is retried on the next tick; the stream stays the authority for the turn.
      void this.projectCanonicalExecution(projectId, jobId).then((execution) => {
        if (execution && this.chatStreams.get(sessionId)?.source === source) {
          this.emit({ type: "job.execution-updated", sessionId, execution, accounting: null }, { projectId });
        }
      }, () => undefined);
    }, CHAT_EXECUTION_REFRESH_MS) : null;
    if (refresh !== null && typeof refresh === "object" && "unref" in refresh && typeof refresh.unref === "function") refresh.unref();
    this.chatStreams.set(sessionId, { source, assistantId, jobId, refresh });
    source.onmessage = (event) => {
      let value: unknown = null;
      try {
        value = JSON.parse(event.data);
      } catch {
        return;
      }
      if (typeof value !== "object" || value === null) return;
      const record = value as Record<string, unknown>;
      if (record["image"] !== undefined) {
        try {
          const image = decodeChatImage(record["image"]);
          this.emit({ type: "conversation.message-image", sessionId, messageId: assistantId, image });
        } catch (cause) {
          const after = this.store.getSnapshot().messagesBySession[sessionId]?.find(item => item.id === assistantId);
          if (after?.status === "streaming") this.emit({ type: "conversation.message-completed", sessionId, message: { ...after, status: "failed", failureReason: cause instanceof Error ? cause.message : String(cause) } });
          this.closeChatStream(sessionId, false);
          void this.refreshSessionHistory(sessionId);
        }
        return;
      }
      if (typeof record["delta"] === "string") {
        const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
        if (!current || current.status !== "streaming") return;
        this.emit({ type: "conversation.message-delta", sessionId, messageId: assistantId, delta: record["delta"] as string });
        return;
      }
      if (typeof record["reasoning"] === "string") {
        const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
        if (!current || current.status !== "streaming") return;
        this.emit({ type: "conversation.message-reasoning-delta", sessionId, messageId: assistantId, delta: record["reasoning"] as string });
        return;
      }
      if (record["round_begin"] === true) {
        const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
        const boundary = {
          committedContentLength: current?.content.length ?? 0,
          committedImageCount: current?.images?.length ?? 0,
        };
        this.roundCommits.set(sessionId, boundary);
        this.emit({ type: "conversation.message-round-committed", sessionId, messageId: assistantId, ...boundary });
        return;
      }
      if (record["round_reset"] === true) {
        const boundary = this.roundCommits.get(sessionId);
        if (!boundary) return;
        // Re-asserting the round's own boundary drops exactly the provisional
        // output of the attempt that was replaced; the replacement attempt then
        // appends to clean presentation state on the same assistant message.
        this.emit({ type: "conversation.message-round-committed", sessionId, messageId: assistantId, ...boundary });
        return;
      }
      if (record["done"] === true) {
        const after = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
        if (after && after.status === "streaming") {
          this.emit({ type: "conversation.message-completed", sessionId, message: { ...after, status: "completed" } });
        }
        this.closeChatStream(sessionId, false);
        void this.refreshSessionHistory(sessionId);
        return;
      }
      if (typeof record["error"] === "string") {
        const message = (record["error"] as string).slice(0, 1024);
        if (message === "chat cancelled") {
          this.pauseQueue(sessionId);
          this.emit({ type: "cancelled", sessionId, messageId: assistantId });
        } else {
          const after = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
          this.emit({ type: "conversation.message-completed", sessionId, message: { ...(after ?? { id: assistantId, role: "assistant" as const, createdAt: chatClockLabel() }), content: "", failureReason: message, status: "failed" } });
        }
        this.closeChatStream(sessionId, false);
        void this.refreshSessionHistory(sessionId);
      }
    };
    source.onerror = () => {
      const tracked = this.chatStreams.get(sessionId);
      if (!tracked || tracked.jobId !== jobId) return;
      // A transient network error puts EventSource into CONNECTING and the
      // browser resumes the tail itself, resending the standard
      // `Last-Event-ID`. The runtime client must not close it or mark the
      // message failed: replay continues from the next unconsumed sequence.
      if (source.readyState === EventSource.CONNECTING) return;
      // CLOSED means the browser gave up (HTTP error, expired session, or a
      // server that closed without a resumable frame): this is terminal.
      const after = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
      if (after && after.status === "streaming") {
        this.emit({
          type: "conversation.message-completed",
          sessionId,
          message: { ...after, content: "", failureCode: "stream-closed", status: "failed" },
        });
      }
      this.closeChatStream(sessionId, false);
      void this.refreshSessionHistory(sessionId);
    };
  }

  private closeChatStream(sessionId: string, markCancelled: boolean): void {
    const tracked = this.chatStreams.get(sessionId);
    if (!tracked) return;
    try {
      tracked.source.close();
    } catch {
      // Closing a broken stream must not fail the new turn.
    }
    if (tracked.refresh !== null) clearInterval(tracked.refresh);
    this.chatStreams.delete(sessionId);
    this.roundCommits.delete(sessionId);
    if (markCancelled) {
      const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === tracked.assistantId);
      if (current && current.status === "streaming") {
        this.emit({ type: "cancelled", sessionId, messageId: tracked.assistantId });
      }
    }
  }

  private emitLaunchResult(command: JobLaunchCommand, result: JobLaunchResult): void {
    this.emit(
      { type: "job.launch-updated", sessionId: command.sessionId, result },
      { projectId: null, commandId: command.commandId },
    );
  }

  /**
   * Project Jobs are the operational collection. Chat history remains a
   * separate presentation projection and is never consulted to discover work.
   * The existing dashboard supplies canonical Job identities; each changed
   * identity is expanded through its authoritative Job snapshot and committed
   * through the one RuntimeStore event path.
   */
  private refreshProjectJobs(projectId: string, force = false): Promise<void> {
    const existing = this.projectJobRefreshes.get(projectId);
    if (existing) return existing;
    const refresh = this.syncProjectJobs(projectId, force)
      .then(() => { this.lastProjectJobRefreshError.delete(projectId); })
      .catch((cause: unknown) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        if (this.lastProjectJobRefreshError.get(projectId) === message) return;
        this.lastProjectJobRefreshError.set(projectId, message);
        this.emit({ type: "warning", message: `Project Jobs refresh failed: ${message}` }, { projectId });
      })
      .finally(() => {
        if (this.projectJobRefreshes.get(projectId) === refresh) this.projectJobRefreshes.delete(projectId);
      });
    this.projectJobRefreshes.set(projectId, refresh);
    return refresh;
  }

  private async syncProjectJobs(projectId: string, force: boolean): Promise<void> {
    const dashboard = await this.control.readDashboard(projectId);
    if (isCanonicalRejection(dashboard)) throw new Error(dashboard.message);
    if (dashboard.project_id !== projectId) throw new Error("Project Jobs response scope mismatch.");

    const summaries = dashboard.jobs.map((value, index) => {
      if (!isRecord(value) || !isNonEmptyString(value.job_id) || !isNonEmptyString(value.state) ||
          typeof value.updated_at !== "number" || !Number.isSafeInteger(value.updated_at) || value.updated_at < 0) {
        throw new Error(`Project Jobs response contains a malformed Job summary at index ${index}.`);
      }
      return { jobId: value.job_id, watermark: JSON.stringify(value) };
    });
    const previous = this.projectJobWatermarks.get(projectId) ?? new Map<string, string>();
    const next = new Map<string, string>();
    const snapshotFailures: string[] = [];

    for (const summary of summaries) {
      if (!force && previous.get(summary.jobId) === summary.watermark) {
        next.set(summary.jobId, summary.watermark);
        continue;
      }
      let execution: JobExecution | null;
      try {
        execution = await this.projectCanonicalExecution(projectId, summary.jobId);
      } catch (cause) {
        snapshotFailures.push(`${summary.jobId}: ${cause instanceof Error ? cause.message : String(cause)}`);
        continue;
      }
      if (!execution) {
        snapshotFailures.push(`${summary.jobId}: authoritative snapshot was not available`);
        continue;
      }
      if (execution.projectId !== projectId || execution.jobId !== summary.jobId) {
        throw new Error(`Canonical Job snapshot identity mismatch for Job "${summary.jobId}".`);
      }
      const chatSession = [...this.chatJobsBySession].find(([, link]) =>
        link.projectId === projectId && link.jobId === summary.jobId,
      )?.[0];
      this.emit({
        type: "job.execution-updated",
        ...(chatSession ? { sessionId: chatSession } : {}),
        execution,
        accounting: null,
      }, { projectId });
      next.set(summary.jobId, summary.watermark);
    }
    this.projectJobWatermarks.set(projectId, next);
    if (snapshotFailures.length > 0) throw new Error(snapshotFailures.join("; "));
  }

  /**
   * Read the authoritative canonical snapshot and project it.
   *
   * The canonical store projects whole snapshots and never advances its cursor
   * from a raw event, so an event tail means the installed snapshot is behind:
   * the loop refetches until the tail is empty. Each round advances the cursor
   * together with the projection it belongs to.
   */
  private async refreshExecution(sessionId: string, projectId: string, jobId: string): Promise<void> {
    try {
      const execution = await this.projectCanonicalExecution(projectId, jobId);
      if (execution) this.emit({ type: "job.execution-updated", sessionId, execution, accounting: null }, { projectId });
    } catch (cause) {
      this.emit({ type: "error", message: cause instanceof Error ? cause.message : String(cause) });
    }
  }

  private async projectCanonicalExecution(projectId: string, jobId: string): Promise<JobExecution | null> {
    for (let attempt = 0; attempt < MAX_REFRESH_ROUNDS; attempt += 1) {
      const snapshot = await this.control.readJobSnapshot(projectId, jobId);
      if (isCanonicalRejection(snapshot)) return null;
      const generation = this.store.getCanonical().generation + 1;
      const next = this.store.applyCanonicalSnapshot({ payload: snapshot, projectId, generation });
      if (next.projection === null || next.cursor !== snapshot.cursor) return null;
      const execution = selectCanonical(next).execution;
      const events = await this.control.readJobEvents(projectId, jobId, next.cursor);
      // Retain this read's projection: another Project may refresh while the
      // event tail is in flight. Never return that Project's current store view.
      if (isCanonicalRejection(events) || events.length === 0 || attempt === MAX_REFRESH_ROUNDS - 1) return execution;
    }
    return null;
  }

}

function toLaunchRequest(command: JobLaunchCommand): JobLaunchRequest {
  return {
    command_id: command.commandId,
    draft_id: command.draftId,
    project_id: command.projectId,
    session_id: command.sessionId,
    objective: command.objective,
    success_criteria: command.successCriteria ?? null,
    constraints: command.constraints ?? null,
    hard_budget_micros: command.hardBudgetMicros,
    resource_commitment: command.resourceCommitment ?? null,
  };
}

function launchResultFrom(command: JobLaunchCommand, response: CanonicalJobLaunchAck): JobLaunchResult {
  return {
    outcome: response.outcome,
    commandId: response.command_id,
    draftId: response.draft_id,
    projectId: response.project_id,
    sessionId: command.sessionId,
    ...(response.job_id !== null ? { jobId: response.job_id } : {}),
    message: response.message,
    duplicate: response.duplicate,
  };
}

function failedLaunch(command: JobLaunchCommand, message: string): JobLaunchResult {
  return {
    outcome: "failed",
    commandId: command.commandId,
    draftId: command.draftId,
    projectId: command.projectId,
    sessionId: command.sessionId,
    message,
    duplicate: false,
  };
}

function chatClockLabel(): string {
  const now = new Date();
  return `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
}

function chatSessionKey(projectId: string, sessionId: string): string {
  return JSON.stringify([projectId, sessionId]);
}

function chatTimestamp(timestamp: string): string {
  const seconds = Number(timestamp);
  return Number.isFinite(seconds) ? new Date(seconds * 1000).toLocaleString() : timestamp;
}
