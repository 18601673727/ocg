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

import type { ProjectId } from "../project/domain";
import type { ChatMessagesResponse, JobLaunchRequest } from "../contracts";
import type {
  JobLaunchCommand,
  JobLaunchResult,
  RuntimeAuthority,
  ScenarioId,
} from "./runtime-types";
import type { ChatMessage, ChatSession, OcgRuntimeEvent, RuntimeStatus, SendMessageInput } from "../types";
import type { CreateSessionInput } from "./runtime-types";
import { MockOcgRuntimeClient } from "./mock-client";
import { createProfileClient } from "../profile/profile-client";
import {
  createHttpCanonicalControlClient,
  isCanonicalRejection,
  type CanonicalControlClient,
  type CanonicalJobLaunchAck,
} from "./canonical-client";
import type { JobExecution } from "../execution/domain";

/** Bounds the snapshot/event refetch loop when the backend keeps demanding a resync. */
const MAX_REFRESH_ROUNDS = 4;

/** Backend-computed readiness for one chat turn, plus the status that reports it. */
type ChatAvailability = {
  available: boolean;
  status: RuntimeStatus;
};

export class CanonicalOcgRuntimeClient extends MockOcgRuntimeClient {
  readonly authority: RuntimeAuthority = "canonical";

  private readonly control: CanonicalControlClient;
  private readonly profile: ReturnType<typeof createProfileClient>;
  private chatCounter = 0;
  private readonly chatStreams = new Map<string, { source: EventSource; assistantId: string; jobId: string }>();
  private readonly sessionProjects = new Map<string, string>();
  private readonly projectHydrations = new Map<string, Promise<void>>();
  private availabilityProbed = false;
  private reportedStatus: RuntimeStatus | null = null;

  constructor(
    scenario: ScenarioId,
    control: CanonicalControlClient,
    profile: ReturnType<typeof createProfileClient>,
  ) {
    super(scenario, true);
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
      const execution = await this.projectCanonicalExecution(response.project_id, response.job_id);
      if (execution !== null) {
        // The canonical execution's Project is the backend's identity, which is
        // not necessarily a Project this fixture shell knows, so the update is
        // emitted unscoped: the reconciler must not drop it as foreign.
        this.emit(
          { type: "job.execution-updated", sessionId: command.sessionId, execution, accounting: null },
          { projectId: null, commandId: command.commandId },
        );
      }
    }

    return result;
  }

  override async createSession(input: CreateSessionInput): Promise<ChatSession> {
    if (!input.projectId) throw new Error("New Chat requires a Project.");
    const sessionId = `chat-${crypto.randomUUID()}`;
    const id = chatSessionKey(input.projectId, sessionId);
    const session: ChatSession = {
      id,
      sessionId,
      projectId: input.projectId,
      title: input.title?.trim() || "Untitled thread",
      workType: input.workType,
      updatedAt: "now",
    };
    this.bindSessionProject(id, input.projectId);
    this.emit({ type: "conversation.session-created", session: { ...session } }, { projectId: input.projectId });
    return { ...session };
  }

  hydrateProject(projectId: string): Promise<void> {
    const existing = this.projectHydrations.get(projectId);
    if (existing) return existing;
    const hydration = this.loadProjectHistory(projectId).catch((cause: unknown) => {
      this.projectHydrations.delete(projectId);
      throw cause;
    });
    this.projectHydrations.set(projectId, hydration);
    return hydration;
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
    const session: ChatSession = {
      id, projectId, sessionId: history.conversation.session_id,
      title: history.conversation.title || "Untitled thread",
      workType: "coding", updatedAt: chatTimestamp(history.conversation.updated_at),
    };
    this.bindSessionProject(id, projectId);
    this.emit({ type: "conversation.session-created", session }, { projectId });
    const messages: ChatMessage[] = history.messages.filter((message) => message.state !== "deleted").map((message) => ({
      id: message.message_id, commandId: message.command_id, role: message.role,
      content: message.content, createdAt: chatTimestamp(message.created_at),
      status: message.state === "complete" ? "completed" :
        message.state === "failed" && message.attempt_state === "cancelled" ? "cancelled" :
        message.state === "failed" ? "failed" : "pending",
    }));
    this.emit({ type: "conversation.history-loaded", sessionId: id, messages }, { projectId });
    if (this.chatStreams.has(id)) return;
    const replay = history.messages.find((message) => message.replay_job_id !== null);
    const assistant = replay && messages.find((message) => message.id === replay.message_id);
    if (replay?.replay_job_id && assistant) {
      this.emit({ type: "conversation.message-started", sessionId: id, message: { ...assistant, status: "streaming" } }, { projectId });
      this.openChatStream(id, replay.replay_job_id, assistant.id);
    }
  }

  private async refreshSessionHistory(sessionId: string): Promise<void> {
    const session = this.store.getSnapshot().sessions.find((item) => item.id === sessionId);
    if (!session?.projectId || !session.sessionId) return;
    try {
      this.installHistory(await this.readSessionHistory(session.projectId, session.sessionId));
    } catch (cause) {
      this.emit({ type: "error", message: `Chat history refresh failed: ${cause instanceof Error ? cause.message : String(cause)}` });
    }
  }

  override async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    const content = input.content.trim();
    if (!content) return;

    const snapshot = this.store.getSnapshot();
    const session = snapshot.sessions.find((item) => item.id === sessionId);
    if (!session) {
      this.emit({ type: "error", message: `Unknown chat session: ${sessionId}` });
      return;
    }

    const commandId = `cmd-chat-${crypto.randomUUID()}`;
    const userMessage: ChatMessage = {
      commandId,
      optimistic: true,
      id: `chat-user-${Date.now().toString(36)}-${this.chatCounter++}`,
      role: "user",
      content,
      createdAt: chatClockLabel(),
      status: "completed",
    };
    this.emit({ type: "conversation.message-started", sessionId, message: { ...userMessage } });
    this.emit({ type: "conversation.session-updated", session: { ...session, updatedAt: "now" } });

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
        message: { ...assistant, content: "Chat failed: this session has no Project binding.", status: "failed" },
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
          content: `Chat unavailable: ${availability.status.detail ?? "the runtime cannot execute chat."}`,
          status: "failed",
        },
      });
      return;
    }

    const request: JobLaunchRequest = {
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
          content: `Chat failed: ${cause instanceof Error ? cause.message : String(cause)}`,
          status: "failed",
        },
      });
      return;
    }
    if (isCanonicalRejection(response)) {
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: { ...assistant, content: `Chat failed: ${response.message}`, status: "failed" },
      });
      return;
    }
    if (response.outcome !== "accepted" || response.job_id === null) {
      this.emit({
        type: "conversation.message-completed",
        sessionId,
        message: { ...assistant, content: `Chat failed: ${response.message}`, status: "failed" },
      });
      return;
    }

    // Only an accepted turn supersedes the previous Attempt on the backend.
    // Keep its EventSource alive until then so failed sends leave it streaming.
    this.closeChatStream(sessionId, true);
    this.openChatStream(sessionId, response.job_id, assistantId);
  }

  override async cancel(sessionId: string): Promise<void> {
    const session = this.store.getSnapshot().sessions.find((item) => item.id === sessionId);
    const response = await this.control.cancelChatMessage(session?.sessionId ?? sessionId, session?.projectId);
    if (isCanonicalRejection(response)) {
      const streaming = this.streamingMessage(sessionId);
      if (streaming) {
        this.closeChatStream(sessionId, false);
        this.emit({
          type: "conversation.message-completed",
          sessionId,
          message: { ...streaming, content: streaming.content || response.message, status: "failed" },
        });
      }
      return;
    }
    if (!response.cancelled) return;
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
        return { available: true, status: { state: "connected", detail: "OCG provider runtime" } };
      }
      return {
        available: false,
        status: {
          state: "disconnected",
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
    this.chatStreams.set(sessionId, { source, assistantId, jobId });
    source.onmessage = (event) => {
      let value: unknown = null;
      try {
        value = JSON.parse(event.data);
      } catch {
        return;
      }
      if (typeof value !== "object" || value === null) return;
      const record = value as Record<string, unknown>;
      if (typeof record["delta"] === "string") {
        const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
        if (!current || current.status !== "streaming") return;
        this.emit({ type: "conversation.message-delta", sessionId, messageId: assistantId, delta: record["delta"] as string });
        return;
      }
      if (typeof record["reasoning"] === "string") return;
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
        const message = record["error"] as string;
        if (message === "chat cancelled") {
          this.emit({ type: "cancelled", sessionId, messageId: assistantId });
        } else {
          const after = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
          const content = after?.content || message;
          this.emit({ type: "conversation.message-completed", sessionId, message: { ...(after ?? { id: assistantId, role: "assistant" as const, createdAt: chatClockLabel() }), content, status: "failed" } });
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
          message: { ...after, content: after.content || "Chat stream failed.", status: "failed" },
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
    this.chatStreams.delete(sessionId);
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
   * Read the authoritative canonical snapshot and project it.
   *
   * The canonical store projects whole snapshots and never advances its cursor
   * from a raw event, so an event tail means the installed snapshot is behind:
   * the loop refetches until the tail is empty. Each round advances the cursor
   * together with the projection it belongs to.
   */
  private async projectCanonicalExecution(
    projectId: string,
    jobId: string,
  ): Promise<JobExecution | null> {
    const scoped: ProjectId = projectId;
    for (let attempt = 0; attempt < MAX_REFRESH_ROUNDS; attempt += 1) {
      const snapshot = await this.control.readJobSnapshot(projectId, jobId);
      if (isCanonicalRejection(snapshot)) return null;

      const generation = this.store.getCanonical().generation + 1;
      const next = this.store.applyCanonicalSnapshot({ payload: snapshot, projectId: scoped, generation });
      // A rejected or stale snapshot leaves the projection and its cursor
      // untouched, so there is no coherent new tail to read.
      if (next.projection === null || next.cursor !== snapshot.cursor) return null;

      const events = await this.control.readJobEvents(projectId, jobId, next.cursor);
      if (isCanonicalRejection(events)) return this.store.selectCanonical().execution;

      const applied = this.store.applyCanonicalEvents(events, { projectId: scoped, generation });
      if (!applied.resyncRequired) return this.store.selectCanonical().execution;
    }
    return this.store.selectCanonical().execution;
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
