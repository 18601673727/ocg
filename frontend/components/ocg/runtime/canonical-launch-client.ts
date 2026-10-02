/**
 * Backend-backed Job launch adapter.
 *
 * The shell runtime is a fixture projection, but a Job launch is a real
 * product side effect. This client extends `MockOcgRuntimeClient` so the shell
 * keeps its single runtime store, and overrides exactly one operation:
 * `launchJob`.
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
 * Fixture scenarios stay on the mock timers. Only ready/first-run workspaces
 * use the real chat lane; normal `ocg` startup must never default to mock.
 *
 * No credential, provider selection, or execution identity is decided in the
 * client: the backend freezes all of it and this adapter only reports what it
 * returned.
 */

import type { ProjectId } from "../project/domain";
import type { JobLaunchRequest } from "../contracts";
import type { JobLaunchCommand, JobLaunchResult, ScenarioId } from "./runtime-types";
import type { ChatMessage, ChatSession, SendMessageInput } from "../types";
import type { CreateSessionInput } from "./runtime-types";
import { MockOcgRuntimeClient } from "./mock-client";
import {
  createHttpCanonicalControlClient,
  isCanonicalRejection,
  type CanonicalControlClient,
  type CanonicalJobLaunchAck,
} from "./canonical-client";
import type { JobExecution } from "../execution/domain";

/** Bounds the snapshot/event refetch loop when the backend keeps demanding a resync. */
const MAX_REFRESH_ROUNDS = 4;

export class CanonicalOcgRuntimeClient extends MockOcgRuntimeClient {
  private readonly control: CanonicalControlClient;
  private readonly scenarioId: ScenarioId;
  private chatCounter = 0;
  private readonly chatStreams = new Map<string, { source: EventSource; assistantId: string; jobId: string }>();
  private readonly sessionProjects = new Map<string, string>();

  constructor(scenario: ScenarioId, control: CanonicalControlClient) {
    super(scenario);
    this.control = control;
    this.scenarioId = scenario;
  }

  /** Build the adapter for a loopback control base URL. */
  static connect(scenario: ScenarioId, baseUrl: string, fetchImpl: typeof fetch): CanonicalOcgRuntimeClient {
    return new CanonicalOcgRuntimeClient(
      scenario,
      createHttpCanonicalControlClient({ baseUrl, fetch: fetchImpl }),
    );
  }

  async launchJob(command: JobLaunchCommand): Promise<JobLaunchResult> {
    const response = await this.control.launchJob(toLaunchRequest(command));
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
    if (!isRealChatScenario(this.scenarioId)) return super.createSession(input);
    const id = `chat-${Date.now().toString(36)}-${this.chatCounter++}`;
    const session: ChatSession = {
      id,
      title: input.title?.trim() || "Untitled thread",
      workType: input.workType,
      updatedAt: "now",
    };
    this.emit({ type: "conversation.session-created", session: { ...session } });
    return { ...session };
  }

  override async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    if (!isRealChatScenario(this.scenarioId)) return super.sendMessage(sessionId, input);
    const content = input.content.trim();
    if (!content) return;

    const snapshot = this.store.getSnapshot();
    if (snapshot.status.state !== "connected") {
      this.emit({
        type: "warning",
        message: snapshot.status.detail ?? "The local runtime is not connected.",
      });
      return;
    }
    const session = snapshot.sessions.find((item) => item.id === sessionId);
    if (!session) {
      this.emit({ type: "error", message: `Unknown chat session: ${sessionId}` });
      return;
    }

    // Close a previous turn on the same session so only one provider stream
    // owns the conversation. The backend already cancels the previous Attempt
    // on send; this keeps the local EventSource from leaking.
    this.closeChatStream(sessionId, true);

    const userMessage: ChatMessage = {
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

    const commandId = `cmd-chat-${Date.now().toString(36)}-${this.chatCounter++}`;
    const request: JobLaunchRequest = {
      command_id: commandId,
      draft_id: commandId,
      project_id: projectId,
      session_id: sessionId,
      objective: content,
      success_criteria: null,
      constraints: null,
      hard_budget_micros: 0,
      resource_commitment: null,
    };
    const response = await this.control.sendChatMessage(request);
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

    this.openChatStream(sessionId, response.job_id, assistantId);
  }

  override async cancel(sessionId: string): Promise<void> {
    if (!isRealChatScenario(this.scenarioId)) return super.cancel(sessionId);
    const response = await this.control.cancelChatMessage(sessionId);
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
    const url = this.control.chatStreamUrl(sessionId, jobId);
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
    sessionId: response.session_id,
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

/** Real chat only for ready/first-run workspaces; fixtures keep mock timers. */
function isRealChatScenario(scenario: ScenarioId): boolean {
  return scenario.endsWith("-ready") || scenario.endsWith("-first-run");
}

function chatClockLabel(): string {
  const now = new Date();
  return `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
}
