import type {
  ChatImage,
  ChatImageUploadRequest,
  ChatModelSelection,
  ConversationTurn,
  Output,
} from "../contracts";
import type * as Api from "../contracts/api";
import type { ChatSession, OcgRuntimeEvent } from "../types";
import { type ApiClient, readOutputWindow, readTranscriptEvidence } from "./api-client";
import { TurnProjection } from "./conversation/projection";
import { PublishScheduler } from "./conversation/publish-scheduler";
import { TURN_PAGE_SIZE, type Turn, turnAttachments, turnCanRetry } from "./conversation/turn";
import { applyTranscriptEvidence, TurnReader } from "./conversation/turn-reader";
import { TurnStreams } from "./conversation/turn-stream";
import { pageTurns } from "./history-page";
import { reconcileJob } from "./job-status";
import { mergeOutputHistory } from "./output-history";
import { RuntimeClientBase } from "./runtime-client-base";
import type { CreateSessionInput, HideSessionOutcome } from "./runtime-types";

const ARCHIVE_STOP_TIMEOUT_MS = 15_000;

export class ConversationRuntimeClient extends RuntimeClientBase {
  private started = false;
  private disposed = false;
  private readonly lifetime = new AbortController();
  private readonly turns = new Map<string, Turn>();
  private readonly reader: TurnReader;
  private readonly projections: TurnProjection;
  private readonly streams: TurnStreams;
  private readonly publications = new PublishScheduler((id) => this.publishSnapshot(id));
  private readonly roots = new Map<string, string>();
  private listedRoots = new Set<string>();
  private readonly hiddenRoots = new Set<string>();
  private readonly unlisted = new Set<string>();
  private readonly hiding = new Set<string>();
  private readonly tips = new Map<string, string>();
  private readonly selections = new Map<string, ChatModelSelection>();
  private readonly sending = new Set<string>();
  private readonly stopping = new Set<string>();
  private readonly stopAccepted = new Set<string>();
  private readonly loading = new Map<string, Promise<string>>();
  private readonly earlier = new Map<string, string | null>();
  private readonly loadingEarlier = new Map<string, Promise<void>>();
  private conversationListLoaded = false;
  private unsubscribeConversations: (() => void) | null = null;

  constructor(readonly api: ApiClient) {
    super();
    this.reader = new TurnReader(api, this.lifetime.signal);
    this.projections = new TurnProjection(this.reader.images);
    this.streams = new TurnStreams(this.reader, this.turns, this.publications, (job) =>
      this.openConversation(job),
    );
  }

  override subscribe(listener: (event: OcgRuntimeEvent) => void) {
    const unsubscribe = super.subscribe(listener);
    if (!this.started) {
      this.started = true;
      this.startConversationSubscription();
    }
    return unsubscribe;
  }

  retryConversations(): void {
    this.conversationListLoaded = false;
    this.emit({ type: "conversation.list-state", state: "loading" });
    this.unsubscribeConversations?.();
    this.startConversationSubscription();
  }

  private startConversationSubscription() {
    this.unsubscribeConversations = this.api.subscribeConversations(
      (page) => {
        void this.applyConversations(page).catch((error) => {
          if (!this.conversationListLoaded) {
            this.emit({ type: "conversation.list-state", state: "failed" });
          }
          this.report(error);
        });
      },
      (error) => {
        if (!this.conversationListLoaded) {
          this.emit({ type: "conversation.list-state", state: "failed" });
        }
        this.report(error);
      },
    );
  }

  private report(error: unknown, sessionId?: string) {
    if (!this.disposed)
      this.emit({
        type: "runtime.status-changed",
        status: {
          state: "failed",
          sessionId,
          detail: error instanceof Error ? error.message : String(error),
        },
      });
  }

  private async applyConversations(page: Awaited<ReturnType<ApiClient["readConversations"]>>) {
    if (this.disposed) return;
    const updates: Promise<string>[] = [];
    const visible = new Set(page.conversations.map((item) => item.root_job));
    for (const root of this.hiddenRoots) {
      if (!visible.delete(root)) this.hiddenRoots.delete(root);
    }
    for (const session of this.getSnapshot().sessions) {
      if (
        session.conversationId &&
        !session.archived &&
        this.listedRoots.has(session.conversationId) &&
        !visible.has(session.conversationId)
      ) {
        this.evictSession(session.id, session.conversationId);
      }
    }
    this.listedRoots = visible;
    for (const item of page.conversations) {
      if (this.hiddenRoots.has(item.root_job)) continue;
      const id =
        this.roots.get(item.root_job) ??
        this.getSnapshot().sessions.find((session) => session.conversationId === item.root_job)
          ?.id ??
        item.root_job;
      this.roots.set(item.root_job, id);
      this.unlisted.delete(id);
      const known = this.tips.get(id);
      const local = known ? this.turns.get(known) : undefined;
      // A stale list can arrive after the client has accepted a newer tip.
      if (
        known &&
        known !== item.tip_job &&
        local &&
        (item.tip_created_at_us < local.job.created_at_us ||
          (item.tip_created_at_us === local.job.created_at_us && item.tip_job < known))
      )
        continue;
      this.publishCatalogSession(item, id, false);
      if (known && known !== item.tip_job && local && !this.loading.has(item.tip_job)) {
        updates.push(
          this.openConversation(item.tip_job).catch((error) => {
            if (this.tips.get(id) === item.tip_job) this.tips.set(id, known);
            throw error;
          }),
        );
      }
    }
    this.conversationListLoaded = true;
    this.emit({ type: "conversation.list-state", state: "loaded" });
    await Promise.all(updates);
    if (!this.disposed)
      this.emit({ type: "runtime.status-changed", status: { state: "connected" } });
  }

  private publishCatalogSession(item: Api.Conversation, id: string, archived: boolean) {
    const current = this.getSnapshot().sessions.find((session) => session.id === id);
    this.tips.set(id, item.tip_job);
    this.emit({
      type: current ? "conversation.session-updated" : "conversation.session-created",
      session: {
        id,
        conversationId: item.root_job,
        jobState: item.job_state,
        hasChildren: item.has_children,
        sessionId: item.tip_job,
        title: item.title,
        agentProfile: item.agent ?? undefined,
        llmProfile: current?.llmProfile,
        reasoningEffort: current?.reasoningEffort,
        model: current?.model,
        projectId: item.project ?? undefined,
        workType: "research",
        updatedAt: new Date(item.tip_created_at_us / 1000).toISOString(),
        archived,
        closed: item.state !== "open",
        settled: item.state === "settled",
      },
    });
  }

  override async createSession(input: CreateSessionInput): Promise<ChatSession> {
    const session: ChatSession = {
      id: `draft-${crypto.randomUUID()}`,
      title: input.title ?? "New conversation",
      workType: input.workType,
      updatedAt: new Date().toISOString(),
    };
    this.emit({ type: "conversation.session-created", session });
    return session;
  }

  override async deleteSession(sessionId: string): Promise<void> {
    if (!sessionId.startsWith("draft-")) {
      throw new Error("Persisted conversations cannot be deleted; hide the conversation instead.");
    }
    if (this.tips.has(sessionId) || this.sending.has(sessionId)) return;
    this.selections.delete(sessionId);
    this.removeSession(sessionId);
  }

  private removeSession(sessionId: string) {
    this.publications.cancel(sessionId);
    this.emit({ type: "conversation.session-deleted", sessionId });
  }

  private evictSession(sessionId: string, root: string) {
    this.selections.delete(sessionId);
    this.earlier.delete(sessionId);
    this.loadingEarlier.delete(sessionId);
    this.tips.delete(sessionId);
    this.roots.delete(root);
    this.unlisted.delete(sessionId);
    this.evictTurns((_, turn) => turn.sessionId === sessionId);
    this.removeSession(sessionId);
  }

  private sessionRoot(sessionId: string): string {
    for (const [root, id] of this.roots.entries()) {
      if (id === sessionId) return root;
    }
    throw new Error("This session has no admitted conversation.");
  }

  private evictTurns(shouldEvict: (jobId: string, turn: Turn) => boolean) {
    const attachments = new Set<string>();
    for (const [jobId, turn] of this.turns) {
      if (!shouldEvict(jobId, turn)) continue;
      for (const id of turnAttachments(turn)) attachments.add(id);
      this.streams.stop(jobId);
      this.turns.delete(jobId);
    }
    for (const turn of this.turns.values()) {
      for (const id of turnAttachments(turn)) attachments.delete(id);
    }
    for (const id of attachments) this.reader.images.delete(id);
  }

  private claimConversation(root: string, sessionId: string) {
    this.roots.set(root, sessionId);
    const stray = this.getSnapshot().sessions.find(
      (session) => session.id !== sessionId && session.conversationId === root,
    );
    if (!stray) return;
    this.tips.delete(stray.id);
    this.unlisted.delete(stray.id);
    this.earlier.delete(stray.id);
    this.loadingEarlier.delete(stray.id);
    for (const turn of this.turns.values()) {
      if (turn.sessionId === stray.id) turn.sessionId = sessionId;
    }
    this.removeSession(stray.id);
    this.publications.publish(sessionId);
  }

  async renameSession(sessionId: string, title: string): Promise<void> {
    const root = this.sessionRoot(sessionId);
    const trimmed = title.trim();
    await this.api.editConversation(root, { title: trimmed }, this.lifetime.signal);
    const session = this.getSnapshot().sessions.find((s) => s.id === sessionId);
    if (session) {
      this.emit({ type: "conversation.session-updated", session: { ...session, title: trimmed } });
    }
  }

  private async readArchiveTurns(root: string, signal: AbortSignal) {
    const unsettled: string[] = [];
    let before: string | undefined;
    do {
      const page = await this.api.readConversationTurns(root, before, 1, signal);
      const job = page.turns[0]?.job;
      if (!job || job.settlement.state !== "unsettled") return { unsettled, settled: job };
      unsettled.push(job.id);
      before = page.next_before ?? undefined;
    } while (before);
    return { unsettled };
  }

  async hideSession(sessionId: string, stopFirst: boolean): Promise<HideSessionOutcome> {
    const root = this.sessionRoot(sessionId);
    if (this.hiding.has(sessionId)) throw new Error("This conversation is being hidden.");
    this.hiding.add(sessionId);
    try {
      const signal = AbortSignal.any([
        this.lifetime.signal,
        AbortSignal.timeout(ARCHIVE_STOP_TIMEOUT_MS),
      ]);
      const { unsettled, settled } = await this.readArchiveTurns(root, signal);
      if (
        settled?.settlement.state === "unknown" &&
        (this.stopAccepted.has(settled.id) || (stopFirst && !unsettled.length))
      ) {
        return "stop-unconfirmed";
      }
      if (unsettled.length) {
        if (!stopFirst) return "stop-required";
        await this.api.stopConversation(root, signal);
        for (const job of unsettled) this.stopAccepted.add(job);
        for (const job of unsettled) {
          let state = await this.api.readExecutionState(job, { signal });
          while (state.settlement.state === "unsettled" && state.execution_phase !== "unknown") {
            await new Promise((resolve) => setTimeout(resolve, 250));
            signal.throwIfAborted();
            state = await this.api.readExecutionState(job, { signal });
          }
          if (state.settlement.state === "unknown" || state.execution_phase === "unknown")
            return "stop-unconfirmed";
        }
        if ((await this.readArchiveTurns(root, signal)).unsettled.length) return "stop-required";
      }
      await this.api.editConversation(root, { hidden: true }, this.lifetime.signal);
      this.hiddenRoots.add(root);
      this.listedRoots.delete(root);
      this.evictSession(sessionId, root);
    } finally {
      this.hiding.delete(sessionId);
    }
  }

  presentArchivedConversation(item: Api.Conversation): void {
    const root = item.root_job;
    const id = this.roots.get(root) ?? root;
    this.roots.set(root, id);
    this.listedRoots.delete(root);
    this.unlisted.add(id);
    this.publishCatalogSession(item, id, true);
  }

  async restoreConversation(item: Api.Conversation): Promise<void> {
    const root = item.root_job;
    await this.api.editConversation(root, { hidden: false }, this.lifetime.signal);
    this.hiddenRoots.delete(root);
    const id = this.roots.get(root) ?? root;
    this.roots.set(root, id);
    this.unlisted.delete(id);
    const current = this.getSnapshot().sessions.find((session) => session.id === id);
    if (!current || current.archived) this.publishCatalogSession(item, id, false);
  }

  setChatSelection(sessionId: string, selection: ChatModelSelection) {
    this.selections.set(sessionId, selection);
  }

  async uploadChatImage(request: ChatImageUploadRequest, signal?: AbortSignal): Promise<ChatImage> {
    const match =
      /^data:(image\/png|image\/jpeg|image\/gif|image\/webp);base64,([A-Za-z0-9+/=]+)$/.exec(
        request.data_url,
      );
    if (!match)
      throw new Error("Only PNG, JPEG, GIF and WebP attachments are supported by this host.");
    const [, mime, data] = match;
    if (
      mime !== "image/png" &&
      mime !== "image/jpeg" &&
      mime !== "image/gif" &&
      mime !== "image/webp"
    ) {
      throw new Error("Only PNG, JPEG, GIF and WebP attachments are supported by this host.");
    }
    const attachment = await this.api.uploadAttachment(
      { mime, data_base64: data, display_name: request.name },
      signal,
    );
    return {
      id: attachment.id,
      name: attachment.display_name ?? request.name,
      media_type: attachment.mime,
      url: this.api.attachmentContentUrl(attachment.id),
    };
  }

  isListedConversation(sessionId: string): boolean {
    return !this.unlisted.has(sessionId);
  }

  openConversation(job: string): Promise<string> {
    const existing = this.loading.get(job);
    if (existing) return existing;
    const task = this.readConversation(job).finally(() => this.loading.delete(job));
    this.loading.set(job, task);
    return task;
  }

  async presentMutableJob(
    sessionId: string,
    job: import("./api-client").MutableJob,
  ): Promise<void> {
    const session = this.getSnapshot().sessions.find((item) => item.id === sessionId);
    if (!session) throw new Error("Conversation presentation is no longer available.");
    if (session.conversationId && session.conversationId !== job.conversation)
      throw new Error("The Job belongs to another conversation.");
    this.claimConversation(job.conversation, sessionId);
    this.emit({
      type: "conversation.session-updated",
      session: {
        ...session,
        conversationId: job.conversation,
        projectId: job.project ?? undefined,
        sessionId: job.id,
      },
    });
    await this.openConversation(job.id);
  }

  private async readConversation(job: string): Promise<string> {
    const sessionsAtStart = this.getSnapshot().sessions;
    const root = await this.api.resolveConversation(job, this.lifetime.signal);
    const signal = this.lifetime.signal;
    const [page, state] = await Promise.all([
      this.api.readConversationTurns(root, undefined, TURN_PAGE_SIZE, signal),
      this.roots.has(root) ? undefined : this.api.readExecutionState(root, { signal }),
    ]);
    const original = sessionsAtStart.find((session) => session.conversationId === root);
    if (this.disposed || (original && !this.roots.has(root))) return original?.id ?? root;
    const conversation = { ...page, turns: [...page.turns].reverse() };
    const hiddenBefore = this.hiddenRoots.has(root);
    const id = this.roots.get(root) ?? root;
    if (!this.roots.has(root)) {
      this.unlisted.add(id);
      this.roots.set(root, id);
    }
    const lastTurn = conversation.turns.findLast((turn) => !turn.planning_target);
    job = lastTurn?.job.id ?? job;
    if (!this.getSnapshot().sessions.some((session) => session.id === id)) {
      this.emit({
        type: "conversation.session-created",
        session: {
          id,
          conversationId: root,
          sessionId: job,
          title: conversation.turns[0]?.input.objective?.slice(0, 80) || "Conversation",
          workType: "research",
          updatedAt: new Date().toISOString(),
          archived: state?.hidden_at_us != null,
          settled: state !== undefined && state.settlement.state !== "unsettled",
          closed:
            state !== undefined &&
            (state.closing_at_us !== null || state.settlement.state !== "unsettled"),
        },
      });
    }
    const isCurrent = () =>
      !this.disposed &&
      this.roots.get(root) === id &&
      (hiddenBefore || !this.hiddenRoots.has(root));
    const held = new Set(
      [...this.turns.values()].filter((turn) => turn.sessionId === id).map((turn) => turn.job.id),
    );
    await Promise.all(
      conversation.turns.map(async (turn) => {
        const loaded = await this.readHistoryTurn(id, turn, isCurrent);
        if (!loaded || !isCurrent() || !this.historyTurnIsCurrent(loaded)) return;
        for (const image of loaded.images) this.reader.images.set(image.id, image);
        this.turns.set(turn.job.id, loaded.state);
      }),
    );
    if (!isCurrent()) return id;
    if (!this.earlier.has(id)) {
      this.earlier.set(id, pageTurns(new Set(), undefined, page).next);
    } else if (
      held.size > 0 &&
      page.has_more &&
      !page.turns.some((turn) => held.has(turn.job.id))
    ) {
      const pageIds = new Set(page.turns.map((turn) => turn.job.id));
      this.evictTurns(
        (jobId, turn) => turn.sessionId === id && !pageIds.has(jobId) && !this.streams.has(jobId),
      );
      this.earlier.set(id, pageTurns(new Set(), undefined, page).next);
    }
    this.tips.set(id, job);
    const session = this.getSnapshot().sessions.find((session) => session.id === id);
    if (!session) return id;
    const lastConfig = lastTurn?.input.configuration;
    this.emit({
      type: "conversation.session-updated",
      session: {
        ...session,
        conversationId: root,
        sessionId: job,
        agentProfile: lastConfig?.agent_profile ?? undefined,
        llmProfile: lastConfig?.llm_profile ?? undefined,
        reasoningEffort: lastConfig ? lastConfig.reasoning_effort : undefined,
        model: lastConfig?.model,
      },
    });
    this.publications.publish(id);
    for (const turn of conversation.turns) this.streams.watch(turn.job.id);
    return id;
  }

  hasEarlierHistory(sessionId: string): boolean {
    return (
      !!this.earlier.get(sessionId) ||
      [...this.turns.values()].some(
        (turn) => turn.sessionId === sessionId && turn.outputs.firstSeq > 1,
      )
    );
  }

  loadEarlierHistory(sessionId: string): Promise<void> {
    const existing = this.loadingEarlier.get(sessionId);
    if (existing) return existing;
    const cursor = this.earlier.get(sessionId);
    if (this.disposed) return Promise.resolve();
    const partial = [...this.turns.values()]
      .filter((turn) => turn.sessionId === sessionId && turn.outputs.firstSeq > 1)
      .sort(
        (a, b) => b.job.created_at_us - a.job.created_at_us || b.job.id.localeCompare(a.job.id),
      )[0];
    if (!cursor && !partial) return Promise.resolve();
    const root = this.sessionRoot(sessionId);
    const isCurrent = () =>
      !this.disposed &&
      this.roots.get(root) === sessionId &&
      this.earlier.get(sessionId) === cursor;
    const task = (async () => {
      try {
        if (partial) {
          const held = partial.outputs;
          let before = held.firstSeq;
          let loaded: Output[] = [];
          while (true) {
            const earlier = await readOutputWindow(this.api, partial.job.id, {
              beforeSeq: before,
              signal: this.lifetime.signal,
            });
            if (!isCurrent() || this.turns.get(partial.job.id) !== partial) return;
            loaded = [
              ...new Map(
                [...earlier.outputs, ...loaded].map((output) => [output.seq, output]),
              ).values(),
            ].sort((a, b) => a.seq - b.seq);
            if (
              earlier.outputs.some((output) => output.kind !== "reasoning") ||
              earlier.outputs[0]?.seq === 1
            )
              break;
            const next = earlier.outputs[0]?.seq ?? 1;
            if (next >= before) break;
            before = next;
          }
          const previousFirstSeq = partial.outputs.firstSeq;
          partial.outputs = mergeOutputHistory(loaded, held, partial.outputs);
          if (partial.outputs.firstSeq === 1) {
            partial.partialToolTimes = undefined;
            partial.partialToolBoundaries = undefined;
            const evidenceRevision = partial.evidenceRevision;
            const evidence = await readTranscriptEvidence(this.api, partial.job.id, {
              signal: this.lifetime.signal,
            });
            if (!isCurrent() || this.turns.get(partial.job.id) !== partial) return;
            applyTranscriptEvidence(partial, evidenceRevision, evidence, true);
          } else {
            if (partial.outputs.firstSeq !== previousFirstSeq) {
              partial.partialToolBoundaries = undefined;
            }
            if (partial.job.settlement.state === "completed" && partial.tools.length) {
              const evidenceRevision = partial.evidenceRevision;
              const firstSeq = partial.outputs.firstSeq;
              const times = new Map(partial.partialToolTimes);
              const boundaries = await this.reader.readPartialToolBoundaries(partial, times);
              if (!isCurrent() || this.turns.get(partial.job.id) !== partial) return;
              if (
                partial.evidenceRevision === evidenceRevision &&
                partial.outputs.firstSeq === firstSeq
              ) {
                partial.partialToolTimes = times;
                partial.partialToolBoundaries = boundaries;
              }
            }
          }
          this.publications.publish(sessionId);
          return;
        }
        if (!cursor) return;
        const page = await this.api.readConversationTurns(
          root,
          cursor,
          TURN_PAGE_SIZE,
          this.lifetime.signal,
        );
        if (!isCurrent()) return;
        const held = new Set(
          [...this.turns.values()]
            .filter((turn) => turn.sessionId === sessionId)
            .map((turn) => turn.job.id),
        );
        const { add, next } = pageTurns(held, cursor, page);
        const loaded = await Promise.all(
          add.map((turn) => this.readHistoryTurn(sessionId, turn, isCurrent)),
        );
        if (!isCurrent()) return;
        for (const turn of loaded) {
          if (!turn || !this.historyTurnIsCurrent(turn)) continue;
          for (const image of turn.images) this.reader.images.set(image.id, image);
          this.turns.set(turn.state.job.id, turn.state);
          this.streams.watch(turn.state.job.id);
        }
        this.earlier.set(sessionId, next);
        this.publications.publish(sessionId);
      } catch (error) {
        if (isCurrent()) throw error;
      }
    })().finally(() => {
      if (this.loadingEarlier.get(sessionId) === task) this.loadingEarlier.delete(sessionId);
    });
    this.loadingEarlier.set(sessionId, task);
    return task;
  }

  private async readHistoryTurn(
    sessionId: string,
    turn: ConversationTurn,
    isCurrent: () => boolean,
  ) {
    if (this.streams.has(turn.job.id) || !isCurrent()) return null;
    const previous = this.turns.get(turn.job.id);
    const snapshot = previous ? { ...previous } : undefined;
    const window = turn.outputs_truncated
      ? (await readOutputWindow(this.api, turn.job.id, { signal: this.lifetime.signal })).outputs
      : turn.outputs;
    let outputs = mergeOutputHistory(window);
    if (previous?.outputs.length && window.length) {
      const last = previous.outputs.lastSeq;
      const gap = await this.reader.readMissingOutputs(turn.job.id, last, window[0].seq - 1);
      outputs = mergeOutputHistory(previous.outputs, gap, window);
    }
    const complete = outputs.firstSeq === 1;
    const needsDetails =
      turn.job.settlement.state === "unsettled" ||
      turn.job.settlement.state === "unknown" ||
      turn.job.settlement.state === "failed" ||
      turn.job.settlement.state === "cancelled" ||
      [...outputs].some((output) => output.code === "task_plan_scheduled");
    const details = needsDetails
      ? await this.api.readJobDetails(turn.job.id, this.lifetime.signal)
      : undefined;
    const evidence =
      turn.native_decision || details?.mutable_phase === "queued"
        ? { tools: [], responses: [], error: undefined }
        : await readTranscriptEvidence(this.api, turn.job.id, {
            signal: this.lifetime.signal,
            readReplies: complete,
          });
    if (!isCurrent()) return null;
    const state: Turn = {
      sessionId,
      ...turn,
      outputs,
      evidenceRevision: (previous?.evidenceRevision ?? 0) + 1,
      tools: evidence.tools ?? [],
      responses: evidence.responses ?? [],
      toolError: evidence.error,
    };
    if (!complete && turn.job.settlement.state === "completed" && state.tools.length) {
      const times = new Map(previous?.partialToolTimes);
      state.partialToolBoundaries = await this.reader.readPartialToolBoundaries(
        state,
        times,
        previous?.responses ?? [],
      );
      state.partialToolTimes = times;
    }
    if (details) {
      state.details = details;
      if (turn.job.settlement.state === "unknown")
        state.unknownReason = details.unknown_reason ?? null;
      state.job = reconcileJob(state.job, details);
    }
    const images = await this.reader.readImages(state);
    if (!isCurrent()) return null;
    return { state, images, previous, snapshot };
  }

  private historyTurnIsCurrent(loaded: {
    state: Turn;
    previous: Turn | undefined;
    snapshot: Turn | undefined;
  }) {
    const current = this.turns.get(loaded.state.job.id);
    if (this.streams.has(loaded.state.job.id) || current !== loaded.previous) return false;
    if (!current || !loaded.snapshot) return true;
    const snapshot = loaded.snapshot;
    return (
      Object.keys(current).length === Object.keys(snapshot).length &&
      (Object.keys(snapshot) as (keyof Turn)[]).every((key) => current[key] === snapshot[key])
    );
  }

  private publishSnapshot(sessionId: string) {
    if (!this.getSnapshot().sessions.some((session) => session.id === sessionId)) return;
    const turns = [...this.turns.values()]
      .filter((turn) => turn.sessionId === sessionId)
      .sort(
        (a, b) =>
          (a.input.submitted_at_us ?? a.job.created_at_us) -
            (b.input.submitted_at_us ?? b.job.created_at_us) ||
          (a.job.id < b.job.id ? -1 : a.job.id > b.job.id ? 1 : 0),
      );
    const messages = turns.flatMap((turn) => this.projections.messagesForTurn(turn));
    this.emit({ type: "conversation.history-loaded", sessionId, messages });
    // A listener may have disposed the client, removed this session, or published newer history.
    if (this.disposed || this.getSnapshot().messagesBySession[sessionId] !== messages) return;
    const queue = turns
      .filter(
        (turn) => turn.job.started_at_us === null && turn.job.settlement.state === "unsettled",
      )
      .flatMap((turn) =>
        (turn.entries ?? []).map((entry) => ({
          id: `${turn.job.id}:entry:${entry.seq}`,
          input: {
            content: entry.text ?? "",
            images: entry.attachments.flatMap((id) => this.reader.images.get(id) ?? []),
            mode: "queue" as const,
          },
        })),
      );
    const previous = this.getSnapshot().chatQueues?.[sessionId];
    if (
      previous &&
      !previous.paused &&
      previous.queue.length === queue.length &&
      queue.every((entry, index) => {
        const stored = previous.queue[index];
        const images = stored.input.images;
        return (
          entry.id === stored.id &&
          entry.input.content === stored.input.content &&
          stored.input.mode === "queue" &&
          images?.length === entry.input.images.length &&
          images.every((image, i) => {
            const next = entry.input.images[i];
            return (
              image === next ||
              (image.id === next.id &&
                image.name === next.name &&
                image.media_type === next.media_type &&
                image.url === next.url)
            );
          })
        );
      })
    )
      return;
    this.emit({ type: "conversation.queue-updated", sessionId, queue, paused: false });
  }

  private async trackTurn(sessionId: string, job: string) {
    const previous = this.turns.get(job);
    const snapshot = previous ? { ...previous } : undefined;
    const [record, frozen] = await Promise.all([
      this.api.readJobDetails(job),
      this.api.readJobInput(job),
    ]);
    const page = record.conversation
      ? await this.api.readConversationTurns(
          record.conversation,
          undefined,
          20,
          this.lifetime.signal,
        )
      : null;
    const stored = page?.turns.find((turn) => turn.job.id === job);
    const window =
      stored && !stored.outputs_truncated
        ? stored.outputs
        : (await readOutputWindow(this.api, job, { signal: this.lifetime.signal })).outputs;
    let outputs = mergeOutputHistory(window);
    if (previous?.outputs.length && window.length) {
      const gap = await this.reader.readMissingOutputs(
        job,
        previous.outputs.lastSeq,
        window[0].seq - 1,
      );
      outputs = mergeOutputHistory(previous.outputs, gap, window);
    }
    const evidence = stored?.native_decision
      ? { tools: [], responses: [], error: undefined }
      : await readTranscriptEvidence(this.api, job, {
          signal: this.lifetime.signal,
          readReplies: outputs.firstSeq === 1,
        });
    const turn: Turn = {
      content_kind: stored?.content_kind,
      compaction: stored?.compaction,
      entries: stored?.entries,
      native_decision: stored?.native_decision,
      planning_target: stored?.planning_target,
      details: record,
      sessionId,
      job: record,
      input: stored?.input ?? frozen,
      outputs,
      evidenceRevision: (previous?.evidenceRevision ?? 0) + 1,
      tools: evidence.tools ?? [],
      responses: evidence.responses ?? [],
      toolError: evidence.error,
      unknownReason: record.unknown_reason ?? null,
    };
    if (outputs.firstSeq > 1 && record.settlement.state === "completed" && turn.tools.length) {
      const times = new Map(previous?.partialToolTimes);
      turn.partialToolBoundaries = await this.reader.readPartialToolBoundaries(
        turn,
        times,
        previous?.responses ?? [],
      );
      turn.partialToolTimes = times;
    }
    if (!this.getSnapshot().sessions.some((s) => s.id === sessionId)) return;
    const current = this.turns.get(job);
    if (current && !this.historyTurnIsCurrent({ state: turn, previous, snapshot })) {
      current.content_kind = turn.content_kind;
      current.native_decision = turn.native_decision;
      current.planning_target = turn.planning_target;
      current.entries = turn.entries;
      current.input = turn.input;
      current.compaction = turn.compaction;
    } else {
      if (current) turn.job = reconcileJob(current.job, turn.job);
      this.turns.set(job, turn);
    }
    this.publications.publish(sessionId);
    this.streams.watch(job);
  }

  override async retryMessage(sessionId: string, messageId: string): Promise<void> {
    const message = this.getSnapshot().messagesBySession[sessionId]?.find(
      (message) => message.id === messageId,
    );
    const turn = message?.jobId ? this.turns.get(message.jobId) : undefined;
    if (
      !turn ||
      !turn.job.conversation ||
      !["failed", "cancelled"].includes(turn.job.settlement.state) ||
      this.tips.get(sessionId) !== turn.job.id
    )
      throw new Error("Only the latest failed or cancelled turn can be retried.");
    if (this.hiding.has(sessionId) || this.sending.has(sessionId))
      throw new Error("Wait for this conversation's current action to finish.");
    const session = this.getSnapshot().sessions.find((session) => session.id === sessionId);
    if (!session) throw new Error("Conversation presentation is no longer available.");
    this.sending.add(sessionId);
    try {
      const latest = await this.api.readJobDetails(turn.job.id, this.lifetime.signal);
      turn.job = reconcileJob(turn.job, latest);
      if (turn.job !== latest)
        throw new Error("The Job changed while reading its status; reload before retrying.");
      turn.details = latest;
      const conversation = turn.job.conversation;
      if (!conversation || !turnCanRetry(turn) || this.tips.get(sessionId) !== turn.job.id)
        throw new Error("Only the latest stopped Generative Job can be retried as a new Job.");
      const result = await this.api.retryConversation(
        conversation,
        turn.job.id,
        this.lifetime.signal,
      );
      this.tips.set(sessionId, result.turn);
      this.emit({
        type: "conversation.session-updated",
        session: { ...session, sessionId: result.turn, updatedAt: new Date().toISOString() },
      });
      await this.openConversation(result.turn);
    } finally {
      this.sending.delete(sessionId);
    }
  }

  override async cancel(sessionId: string): Promise<void> {
    const job = this.tips.get(sessionId);
    if (!job) throw new Error("No admitted turn to stop.");
    const native = this.turns.get(job)?.details;
    if (native?.mutable_phase !== undefined) {
      if (this.stopAccepted.has(job)) return;
      if (this.stopping.has(sessionId))
        throw new Error("Wait for this conversation's current stop request to finish.");
      const context = native.context;
      if (!context)
        throw new Error("This Job's Composer context is unavailable; the stop was not sent.");
      this.stopping.add(sessionId);
      try {
        const stop = await this.api.stopNativeJob(context, job, this.lifetime.signal);
        this.stopAccepted.add(job);
        const turn = this.turns.get(job);
        if (turn) {
          turn.error =
            stop.execution_phase === "failed"
              ? "The dispatch deadline expired before the Job started. Nothing was sent to the Provider."
              : stop.request === "ended_before_start"
                ? "Stopped before it started; the Job is Cancelled and nothing was sent to the Provider."
                : stop.request === "already_requested"
                  ? "A stop was already requested; the execution is not confirmed stopped."
                  : "Stop requested; the execution is not confirmed stopped.";
          this.publications.publish(sessionId);
        }
      } finally {
        this.stopping.delete(sessionId);
      }
      return;
    }
    const result = await this.api.stopConversation(
      this.turns.get(job)?.job.conversation ?? job,
      this.lifetime.signal,
    );
    const turn = this.turns.get(job);
    if (turn) {
      turn.error =
        result.status === "requested"
          ? "Stop requested; awaiting the host's settlement."
          : "The host reports this turn already settled.";
      this.publications.publish(sessionId);
    }
  }

  override dispose() {
    if (this.disposed) return;
    this.disposed = true;
    this.publications.dispose();
    this.lifetime.abort();
    this.api.close();
    if (this.unsubscribeConversations) {
      this.unsubscribeConversations();
      this.unsubscribeConversations = null;
    }
    this.streams.close();
    this.turns.clear();
    this.reader.images.clear();
    super.dispose();
  }
}
