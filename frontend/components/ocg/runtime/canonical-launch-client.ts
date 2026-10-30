import type {
  ChatImage,
  ChatImageUploadRequest,
  ChatModelSelection,
  ControlConversationTurn,
  ControlJobDetails,
  ControlJob,
  ControlOutput,
  ControlProviderResponse,
  ControlToolCall,
} from "../contracts";
import type { ChatMessage, ChatSession, OcgRuntimeEvent, SendMessageInput } from "../types";
import { RuntimeClientBase } from "./runtime-client-base";
import type { CreateSessionInput } from "./runtime-types";
import {
  readTranscriptEvidence,
  readOutputWindow,
  ControlTransportError,
  type ControlSubmission,
  type HttpCanonicalControlClient,
  type JobStreamSubscription,
} from "./control-client";
import {
  jobTranscript,
  TranscriptAccumulator,
  type PartialToolBoundary,
  type TranscriptTurn,
} from "../chat/job-transcript-model";
import { fileSnapshots } from "../markdown/source-snapshot";
import { reconcileJob, taskPlanActive } from "./job-status";
import { ContractError, materializeUserEntries } from "../contracts";
import { pageTurns } from "./history-page";

/** Turns read per conversation page: the newest page and each earlier page. */
const TURN_PAGE_SIZE = 20;
// Catalog changes can include every output write; keep one trailing refresh.
const CATALOG_REFRESH_DELAY_MS = 750;
const MAX_RECOVERY_DELAY_MS = 10_000;
const TOOL_TIME_CACHE_LIMIT = 128;
const TOOL_TIME_BATCH_SIZE = 4;

function reuseEvidence<T>(previous: T[], next: T[] | null | undefined): T[] {
  if (!next || next === previous) return previous;
  return JSON.stringify(previous) === JSON.stringify(next) ? previous : next;
}

class ToolTimeGate {
  private active = 0;
  private wake: () => void = () => {};
  private changed = new Promise<void>((resolve) => {
    this.wake = resolve;
  });

  async read<T>(signal: AbortSignal, operation: () => Promise<T>): Promise<T> {
    while (this.active >= TOOL_TIME_BATCH_SIZE) {
      signal.throwIfAborted();
      await new Promise<void>((resolve, reject) => {
        const abort = () => reject(signal.reason);
        signal.addEventListener("abort", abort, { once: true });
        this.changed.then(() => {
          signal.removeEventListener("abort", abort);
          resolve();
        });
        if (signal.aborted) abort();
      });
    }
    signal.throwIfAborted();
    this.active++;
    try {
      return await operation();
    } finally {
      this.active--;
      const wake = this.wake;
      this.changed = new Promise<void>((resolve) => {
        this.wake = resolve;
      });
      wake();
    }
  }
}

function rememberToolTime(times: Map<number, number>, request: number, at: number) {
  if (!times.has(request) && times.size >= TOOL_TIME_CACHE_LIMIT) {
    const oldest = Math.min(...times.keys());
    if (request <= oldest) return;
    times.delete(oldest);
  }
  times.set(request, at);
}

function mergeOutputs(...groups: readonly ControlOutput[][]): ControlOutput[] {
  return [...new Map(groups.flat().map((output) => [output.seq, output])).values()].sort(
    (a, b) => a.seq - b.seq,
  );
}

async function readMissingOutputs(
  control: HttpCanonicalControlClient,
  job: string,
  after: number,
  through: number,
  signal: AbortSignal,
): Promise<ControlOutput[]> {
  const outputs: ControlOutput[] = [];
  while (after < through) {
    const page = await control.readOutputs(job, { afterSeq: after, limit: 100, signal });
    signal.throwIfAborted();
    const next = page.outputs.filter((output) => output.seq <= through);
    for (const output of next) {
      if (output.seq !== after + 1) throw new Error("Stored output sequence has a gap.");
      outputs.push(output);
      after = output.seq;
    }
    if (!next.length || (!page.has_more && after < through))
      throw new Error("Stored output sequence could not be fully reconciled.");
  }
  return outputs;
}

async function readPartialToolBoundaries(
  control: HttpCanonicalControlClient,
  job: string,
  tools: readonly ControlToolCall[],
  outputs: readonly ControlOutput[],
  signal: AbortSignal,
  times: Map<number, number>,
  responses: readonly ControlProviderResponse[],
  gate?: ToolTimeGate,
): Promise<PartialToolBoundary[] | null> {
  const first = outputs[0]?.created_at_us;
  if (!tools.length || first === undefined) return null;
  try {
    const requests = [...new Set(tools.map((call) => call.request))].sort((a, b) => b - a);
    const boundaries: PartialToolBoundary[] = [];
    const recorded = new Map(
      responses
        .filter((response) => response.requests_tools)
        .map((response) => [response.request, response.created_at_us]),
    );
    for (let offset = 0; offset < requests.length; ) {
      // The first request often already precedes the window. Later batches
      // bound speculative reads while avoiding a serial walk through a long turn.
      const count = offset === 0 ? 1 : TOOL_TIME_BATCH_SIZE;
      let batch = requests.slice(offset, offset + count);
      const knownBoundary = batch.findIndex((request) => {
        const at = recorded.get(request) ?? times.get(request);
        return at !== undefined && at < first;
      });
      if (knownBoundary >= 0) batch = batch.slice(0, knownBoundary + 1);
      const controller = new AbortController();
      const abort = () => controller.abort();
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      try {
        const values = batch.map((request) => {
          const known = recorded.get(request) ?? times.get(request);
          return Promise.resolve(
            known === undefined
              ? gate
                ? gate.read(controller.signal, () =>
                    control.readToolResponseTime(job, request, controller.signal),
                  )
                : control.readToolResponseTime(job, request, controller.signal)
              : known,
          ).then(
            (value) => ({ value }),
            (error: unknown) => ({ error }),
          );
        });
        for (let i = 0; i < batch.length; i++) {
          const result = await values[i];
          signal.throwIfAborted();
          if ("error" in result) throw result.error;
          const at = result.value;
          if (at === null) return null;
          const request = batch[i];
          rememberToolTime(times, request, at);
          boundaries.push({ request, at });
          if (at < first) return boundaries.reverse();
        }
      } finally {
        controller.abort();
        signal.removeEventListener("abort", abort);
      }
      offset += batch.length;
    }
    return boundaries.reverse();
  } catch {
    signal.throwIfAborted();
    return null;
  }
}

type Turn = {
  compaction?: ControlConversationTurn["compaction"];
  details?: ControlJobDetails;
  entries?: ControlConversationTurn["entries"];
  sessionId: string;
  job: ControlJob;
  input: ControlConversationTurn["input"];
  outputs: ControlOutput[];
  // Advances when the live array is appended in place, including while an
  // asynchronous history read holds a shallow snapshot of this turn.
  outputRevision?: number;
  tools: ControlToolCall[];
  responses: ControlProviderResponse[];
  partialToolBoundaries?: PartialToolBoundary[] | null;
  partialToolTimes?: ReadonlyMap<number, number>;
  error?: string;
  toolError?: string;
  unknownReason?: string | null;
};
type Pending = {
  key: string;
  signature: string;
  submission: ControlSubmission;
  admittedJob?: string;
};
type StreamEntry = {
  subscription: JobStreamSubscription | null;
  unsubscribeCatalog: (() => void) | null;
  stopped: boolean;
  stop: () => void;
};

/** Projects and sessions are presentation scope only: every backend turn is a Job. */
export class CanonicalOcgRuntimeClient extends RuntimeClientBase {
  readonly authority = "canonical" as const;
  private started = false;
  private disposed = false;
  private readonly lifetime = new AbortController();
  private readonly toolTimeGate = new ToolTimeGate();
  private readonly turns = new Map<string, Turn>();
  private readonly projections = new WeakMap<
    Turn,
    {
      outputs: ControlOutput[];
      length: number;
      aggregate: TranscriptAccumulator;
      arguments?: readonly unknown[];
      transcript?: TranscriptTurn[];
    }
  >();
  private readonly roots = new Map<string, string>();
  private listedRoots = new Set<string>();
  // hiddenRoots and unlisted live as long as the client.
  private readonly hiddenRoots = new Set<string>();
  private readonly unlisted = new Set<string>();
  private readonly hiding = new Set<string>();
  private readonly tips = new Map<string, string>();
  private readonly selections = new Map<string, ChatModelSelection>();
  private readonly pending = new Map<string, Pending>();
  private readonly sending = new Set<string>();
  private readonly loading = new Map<string, Promise<string>>();
  private readonly earlier = new Map<string, string | null>();
  private readonly loadingEarlier = new Map<string, Promise<void>>();
  private readonly streams = new Map<string, StreamEntry>();
  private unsubscribeConversations: (() => void) | null = null;

  constructor(readonly control: HttpCanonicalControlClient) {
    super();
  }

  override subscribe(listener: (event: OcgRuntimeEvent) => void) {
    const unsubscribe = super.subscribe(listener);
    if (!this.started) {
      this.started = true;
      this.unsubscribeConversations = this.control.subscribeConversations(
        (page) => {
          void this.applyConversations(page).catch((error) => this.report(error));
        },
        (error) => this.report(error),
      );
    }
    return unsubscribe;
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

  private async applyConversations(
    page: Awaited<ReturnType<HttpCanonicalControlClient["readConversations"]>>,
  ) {
    if (this.disposed) return;
    const updates: Promise<string>[] = [];
    const visible = new Set(page.conversations.map((item) => item.root_job));
    for (const session of this.getSnapshot().sessions) {
      if (
        session.conversationId &&
        this.listedRoots.has(session.conversationId) &&
        !visible.has(session.conversationId)
      ) {
        this.emit({ type: "conversation.session-deleted", sessionId: session.id });
        this.roots.delete(session.conversationId);
      }
    }
    this.listedRoots = visible;
    for (const item of page.conversations) {
      if (this.hiddenRoots.has(item.root_job)) continue;
      // The conversation names its session, not the other way round: a draft
      // that already presents this conversation keeps presenting it, so a
      // list read that lands after admission cannot mint a second row for it.
      const id =
        this.roots.get(item.root_job) ??
        this.getSnapshot().sessions.find((session) => session.conversationId === item.root_job)
          ?.id ??
        item.root_job;
      this.roots.set(item.root_job, id);
      this.unlisted.delete(id);
      const current = this.getSnapshot().sessions.find((session) => session.id === id);
      // A stale list read must not roll an admitted local successor backwards.
      const known = this.tips.get(id);
      const local = known ? this.turns.get(known) : undefined;
      if (
        known &&
        known !== item.tip_job &&
        local &&
        (item.tip_created_at_us < local.job.created_at_us ||
          (item.tip_created_at_us === local.job.created_at_us && item.tip_job < known))
      )
        continue;
      this.tips.set(id, item.tip_job);
      const session: ChatSession = {
        id,
        conversationId: item.root_job,
        jobState: item.job_state,
        hasChildren: item.has_children,
        sessionId: item.tip_job,
        title: item.title,
        agentProfile: item.agent ?? undefined,
        projectId: item.project ?? undefined,
        workType: "research",
        updatedAt: new Date(item.tip_created_at_us / 1000).toISOString(),
      };
      this.emit({
        type: current ? "conversation.session-updated" : "conversation.session-created",
        session,
      });
      if (known && known !== item.tip_job && local && !this.loading.has(item.tip_job)) {
        updates.push(
          this.openConversation(item.tip_job).catch((error) => {
            if (this.tips.get(id) === item.tip_job) this.tips.set(id, known);
            throw error;
          }),
        );
      }
    }
    await Promise.all(updates);
    if (!this.disposed)
      this.emit({ type: "runtime.status-changed", status: { state: "connected" } });
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
      throw new Error("The 0.5 Control Plane does not provide conversation deletion.");
    }
    if (this.tips.has(sessionId) || this.sending.has(sessionId) || this.pending.has(sessionId))
      return;
    this.pending.delete(sessionId);
    this.selections.delete(sessionId);
    this.emit({ type: "conversation.session-deleted", sessionId });
  }

  private sessionRoot(sessionId: string): string {
    for (const [root, id] of this.roots.entries()) {
      if (id === sessionId) return root;
    }
    throw new Error("This session has no admitted conversation.");
  }

  /**
   * Records that `sessionId` presents the conversation `root`.
   *
   * Admitting a first turn answers with the conversation the server just
   * opened, while the same admission also makes the client re-read the
   * conversation list. A list read that lands first mints its own session for
   * that root, which would sit in the sidebar beside the draft that is already
   * showing it, under the title both derive from the same first message. The
   * root therefore claims the session that admitted it, and a second
   * presentation of that same root is dropped: both entries name one
   * conversation, so nothing distinct is hidden and no stored row is touched.
   */
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
    this.emit({ type: "conversation.session-deleted", sessionId: stray.id });
    this.publish(sessionId);
  }

  async renameSession(sessionId: string, title: string): Promise<void> {
    const root = this.sessionRoot(sessionId);
    const trimmed = title.trim();
    await this.control.editConversation(root, { title: trimmed }, this.lifetime.signal);
    const session = this.getSnapshot().sessions.find((s) => s.id === sessionId);
    if (session) {
      this.emit({ type: "conversation.session-updated", session: { ...session, title: trimmed } });
    }
  }

  async hideSession(sessionId: string): Promise<void> {
    const root = this.sessionRoot(sessionId);
    if (this.hiding.has(sessionId)) throw new Error("This conversation is being hidden.");
    this.hiding.add(sessionId);
    try {
      await this.control.editConversation(root, { hidden: true }, this.lifetime.signal);
      this.hiddenRoots.add(root);
      this.pending.delete(sessionId);
      this.selections.delete(sessionId);
      this.earlier.delete(sessionId);
      this.loadingEarlier.delete(sessionId);
      this.emit({ type: "conversation.session-deleted", sessionId });
      const attachments = new Set<string>();
      for (const [jobId, turn] of this.turns.entries()) {
        if (turn.sessionId !== sessionId) continue;
        for (const id of turn.input.attachments) attachments.add(id);
        const stream = this.streams.get(jobId);
        if (stream) {
          stream.stop();
        }
        this.turns.delete(jobId);
      }
      for (const turn of this.turns.values()) {
        for (const id of turn.input.attachments) attachments.delete(id);
      }
      for (const id of attachments) this.images.delete(id);
      this.tips.delete(sessionId);
      this.roots.delete(root);
    } finally {
      this.hiding.delete(sessionId);
    }
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
    const attachment = await this.control.uploadAttachment(
      { mime, data_base64: data, display_name: request.name },
      signal,
    );
    return {
      id: attachment.id,
      name: attachment.display_name ?? request.name,
      media_type: attachment.mime,
      url: this.control.attachmentContentUrl(attachment.id),
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

  private async readConversation(job: string): Promise<string> {
    const record = await this.control.readJob(job, this.lifetime.signal);
    const root = record.kind === "conversation" ? record.id : record.conversation;
    if (!root) throw new Error("This Job is a process turn with no conversation.");
    const page = await this.control.readConversationTurns(
      root,
      undefined,
      TURN_PAGE_SIZE,
      this.lifetime.signal,
    );
    const conversation = { ...page, turns: [...page.turns].reverse() };
    const hiddenBefore = this.hiddenRoots.has(root);
    const id = this.roots.get(root) ?? root;
    if (!this.roots.has(root)) {
      this.unlisted.add(id);
      this.roots.set(root, id);
    }
    job = conversation.turns.at(-1)?.job.id ?? job;
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
        },
      });
    }
    // Read each turn's attachments and tool evidence as well as its stored text.
    const isCurrent = () => !this.disposed && (hiddenBefore || !this.hiddenRoots.has(root));
    const held = new Set(
      [...this.turns.values()].filter((turn) => turn.sessionId === id).map((turn) => turn.job.id),
    );
    await Promise.all(
      conversation.turns.map(async (turn) => {
        const loaded = await this.readHistoryTurn(id, turn, isCurrent);
        if (!loaded || !isCurrent() || !this.historyTurnIsCurrent(loaded)) return;
        for (const image of loaded.images) this.images.set(image.id, image);
        this.turns.set(turn.job.id, loaded.state);
      }),
    );
    if (this.disposed || (!hiddenBefore && this.hiddenRoots.has(root))) return id;
    if (!this.earlier.has(id)) {
      this.earlier.set(id, pageTurns(new Set(), undefined, page).next);
    } else if (
      held.size > 0 &&
      page.has_more &&
      !page.turns.some((turn) => held.has(turn.job.id))
    ) {
      const pageIds = new Set(page.turns.map((turn) => turn.job.id));
      const attachments = new Set<string>();
      for (const [jobId, turn] of this.turns) {
        if (turn.sessionId !== id || pageIds.has(jobId) || this.streams.has(jobId)) continue;
        for (const imageId of turn.input.attachments) attachments.add(imageId);
        this.turns.delete(jobId);
      }
      for (const turn of this.turns.values()) {
        for (const imageId of turn.input.attachments) attachments.delete(imageId);
      }
      for (const imageId of attachments) this.images.delete(imageId);
      this.earlier.set(id, pageTurns(new Set(), undefined, page).next);
    }
    this.tips.set(id, job);
    const session = this.getSnapshot().sessions.find((session) => session.id === id);
    if (!session) return id;
    this.emit({
      type: "conversation.session-updated",
      session: {
        ...session,
        conversationId: root,
        sessionId: job,
        agentProfile: conversation.turns.at(-1)?.input.configuration?.agent_profile ?? undefined,
      },
    });
    this.publish(id);
    for (const turn of conversation.turns)
      if (
        turn.job.settlement.state === "unsettled" ||
        taskPlanActive(this.turns.get(turn.job.id)?.details?.task_plan)
      )
        this.watch(turn.job.id);
    return id;
  }

  hasEarlierHistory(sessionId: string): boolean {
    return (
      !!this.earlier.get(sessionId) ||
      [...this.turns.values()].some(
        (turn) => turn.sessionId === sessionId && (turn.outputs[0]?.seq ?? 1) > 1,
      )
    );
  }

  loadEarlierHistory(sessionId: string): Promise<void> {
    const existing = this.loadingEarlier.get(sessionId);
    if (existing) return existing;
    const cursor = this.earlier.get(sessionId);
    if (this.disposed) return Promise.resolve();
    const partial = [...this.turns.values()]
      .filter((turn) => turn.sessionId === sessionId && (turn.outputs[0]?.seq ?? 1) > 1)
      .sort(
        (a, b) => b.job.created_at_us - a.job.created_at_us || b.job.id.localeCompare(a.job.id),
      )[0];
    if (!cursor && !partial) return Promise.resolve();
    const root = this.sessionRoot(sessionId);
    const isCurrent = () =>
      !this.disposed && !this.hiddenRoots.has(root) && this.earlier.get(sessionId) === cursor;
    const task = (async () => {
      try {
        if (partial) {
          const held = partial.outputs;
          let before = held[0].seq;
          let loaded: typeof held = [];
          while (true) {
            const earlier = await readOutputWindow(this.control, partial.job.id, {
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
          const merged = new Map(
            [...loaded, ...held, ...partial.outputs].map((output) => [output.seq, output]),
          );
          partial.outputs = [...merged.values()].sort((a, b) => a.seq - b.seq);
          if (partial.outputs[0]?.seq === 1) {
            const evidence = await readTranscriptEvidence(this.control, partial.job.id, {
              signal: this.lifetime.signal,
            });
            if (!isCurrent()) return;
            partial.responses = reuseEvidence(partial.responses, evidence.responses ?? []);
            partial.tools = reuseEvidence(partial.tools, evidence.tools);
            partial.toolError = evidence.error;
            partial.partialToolTimes = undefined;
            partial.partialToolBoundaries = undefined;
          } else if (partial.job.settlement.state === "completed" && partial.tools.length) {
            const times = new Map(partial.partialToolTimes);
            const boundaries = await readPartialToolBoundaries(
              this.control,
              partial.job.id,
              partial.tools,
              partial.outputs,
              this.lifetime.signal,
              times,
              partial.responses,
              this.toolTimeGate,
            );
            if (!isCurrent() || this.turns.get(partial.job.id) !== partial) return;
            partial.partialToolTimes = times;
            partial.partialToolBoundaries = boundaries;
          }
          this.publish(sessionId);
          return;
        }
        if (!cursor) return;
        const page = await this.control.readConversationTurns(
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
          for (const image of turn.images) this.images.set(image.id, image);
          this.turns.set(turn.state.job.id, turn.state);
        }
        this.earlier.set(sessionId, next);
        this.publish(sessionId);
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
    turn: ControlConversationTurn,
    isCurrent: () => boolean,
  ) {
    if (this.streams.has(turn.job.id) || !isCurrent()) return null;
    const previous = this.turns.get(turn.job.id);
    const snapshot = previous ? { ...previous } : undefined;
    let outputs =
      turn.outputs_truncated || (turn.outputs.at(-1)?.seq ?? 0) > 200
        ? (await readOutputWindow(this.control, turn.job.id, { signal: this.lifetime.signal }))
            .outputs
        : turn.outputs;
    if (previous?.outputs.length && outputs.length) {
      const last = previous.outputs.at(-1)?.seq ?? 0;
      const gap = await readMissingOutputs(
        this.control,
        turn.job.id,
        last,
        outputs[0].seq - 1,
        this.lifetime.signal,
      );
      outputs = mergeOutputs(previous.outputs, gap, outputs);
    }
    const complete = (outputs[0]?.seq ?? 1) === 1;
    const evidence = await readTranscriptEvidence(this.control, turn.job.id, {
      signal: this.lifetime.signal,
      readReplies: complete,
    });
    if (!isCurrent()) return null;
    const state: Turn = {
      sessionId,
      ...turn,
      outputs,
      tools: evidence.tools ?? [],
      responses: evidence.responses ?? [],
      toolError: evidence.error,
    };
    if (!complete && turn.job.settlement.state === "completed" && state.tools.length) {
      const times = new Map(previous?.partialToolTimes);
      state.partialToolBoundaries = await readPartialToolBoundaries(
        this.control,
        turn.job.id,
        state.tools,
        outputs,
        this.lifetime.signal,
        times,
        previous?.responses ?? [],
        this.toolTimeGate,
      );
      state.partialToolTimes = times;
    }
    if (turn.job.settlement.state === "unknown") {
      const record = await this.control.readJob(turn.job.id, this.lifetime.signal);
      if (!isCurrent()) return null;
      state.unknownReason = record.unknown_reason ?? null;
    }
    if (
      turn.job.settlement.state === "unsettled" ||
      outputs.some((output) => output.code === "task_plan_scheduled")
    ) {
      state.details = await this.control.readJobDetails(turn.job.id, this.lifetime.signal);
      state.job = reconcileJob(state.job, state.details);
    }
    const images = await this.readImages(state);
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

  private readonly images = new Map<string, ChatImage>();
  private projectTurn(
    turn: Turn,
    partial: boolean,
  ): { transcript: TranscriptTurn[]; result: ControlOutput | null } {
    let cached = this.projections.get(turn);
    if (!cached || cached.outputs !== turn.outputs || cached.length > turn.outputs.length) {
      cached = { outputs: turn.outputs, length: 0, aggregate: new TranscriptAccumulator() };
      this.projections.set(turn, cached);
    }
    while (cached.length < turn.outputs.length)
      cached.aggregate.append(turn.outputs[cached.length++]);
    const args = [
      turn.outputRevision,
      turn.outputs,
      turn.outputs.length,
      turn.job,
      turn.input,
      turn.entries,
      turn.tools,
      turn.responses,
      turn.partialToolBoundaries,
      partial,
    ];
    if (!cached.arguments || args.some((value, index) => value !== cached!.arguments![index])) {
      cached.transcript = jobTranscript(
        turn.job.id,
        partial || turn.entries ? null : turn.input.objective,
        turn.outputs,
        turn.job.settlement,
        turn.job.settlement.state === "unsettled" && turn.job.started_at_us !== null,
        undefined,
        turn.tools,
        turn.job.previous === null,
        turn.responses,
        partial,
        turn.partialToolBoundaries,
        cached.aggregate,
      );
      cached.arguments = args;
    }
    return { transcript: cached.transcript ?? [], result: cached.aggregate.result };
  }

  private async readImages(turn: Turn) {
    return Promise.all(
      [
        ...new Set([
          ...turn.input.attachments,
          ...(turn.entries ?? []).flatMap((entry) => entry.attachments),
        ]),
      ].map(async (id) => {
        const cached = this.images.get(id);
        if (cached) return cached;
        const attachment = await this.control.readAttachment(id, this.lifetime.signal);
        return {
          id,
          name: attachment.display_name ?? id,
          media_type: attachment.mime,
          url: this.control.attachmentContentUrl(id),
        };
      }),
    );
  }

  private publish(sessionId: string) {
    if (this.disposed || !this.getSnapshot().sessions.some((session) => session.id === sessionId))
      return;
    const turns = [...this.turns.values()]
      .filter((turn) => turn.sessionId === sessionId)
      .sort(
        (a, b) => a.job.created_at_us - b.job.created_at_us || a.job.id.localeCompare(b.job.id),
      );
    // Keep the visible history contiguous: inputs and older turns precede any missing outputs.
    const lastPartial = turns.findLastIndex((turn) => (turn.outputs[0]?.seq ?? 1) > 1);
    const messages: ChatMessage[] = turns.slice(Math.max(lastPartial, 0)).flatMap((turn) => {
      const partial = (turn.outputs[0]?.seq ?? 1) > 1;
      const entries = materializeUserEntries(turn.entries ?? []);
      const queued =
        turn.job.previous !== null &&
        turn.job.started_at_us === null &&
        turn.job.settlement.state === "unsettled";
      const inputs: ChatMessage[] = entries.map((entry) => ({
        agentInstruction: entry.kind === "agent_message",
        intermediate: entry.kind === "agent_message",
        queued: entry.kind === "user_message" && queued,
        withdrawn: entry.withdrawn,
        queueHistory: entry.history,
        id: `${turn.job.id}:entry:${entry.seq}`,
        jobId: turn.job.id,
        commandId: turn.job.id,
        role: entry.kind === "agent_message" ? "assistant" : "user",
        content: entry.text ?? "",
        status: "completed",
        createdAt: new Date(entry.at_us / 1000).toISOString(),
        images: entry.attachments.flatMap((id) => this.images.get(id) ?? []),
      }));
      const boundary: ChatMessage[] = turn.compaction
        ? [
            {
              id: `${turn.job.id}:compaction`,
              role: "assistant",
              content: "",
              status: "completed",
              intermediate: true,
              createdAt: new Date(turn.compaction.at_us / 1000).toISOString(),
              compaction: turn.compaction,
            },
          ]
        : [];
      const withdrawnBeforeStart =
        turn.job.started_at_us === null &&
        turn.job.settlement.state === "cancelled" &&
        entries.length > 0 &&
        entries.every((entry) => entry.withdrawn) &&
        !turn.outputs.length &&
        !turn.tools.length &&
        !turn.responses.length;
      const projected = this.projectTurn(turn, partial);
      const transcript = withdrawnBeforeStart ? [] : projected.transcript;
      return [
        ...(partial ? [] : boundary),
        ...(partial ? [] : inputs),
        ...transcript.map((message) => ({
          id: message.id,
          jobId: turn.job.id,
          commandId: turn.job.id,
          role: message.role,
          content: message.content,
          status: message.status,
          intermediate: message.intermediate,
          activity: turn.details?.activity,
          taskPlan: turn.details?.task_plan,
          createdAt: new Date((message.createdAtUs ?? turn.job.created_at_us) / 1000).toISOString(),
          ...(message.role === "user"
            ? { images: turn.input.attachments.flatMap((id) => this.images.get(id) ?? []) }
            : {
                toolCalls: [...(message.tools ?? [])],
                reasoning: message.reasoning,
                sourceSnapshots: message.intermediate
                  ? undefined
                  : fileSnapshots(projected.result?.value),
                reasoningDurationMs: message.reasoningDurationMs,
                failureReason: message.intermediate
                  ? undefined
                  : (turn.error ??
                    turn.toolError ??
                    message.detail ??
                    (message.status === "unknown"
                      ? (turn.job.stop_confirmed_at_us === null
                          ? "Outcome unknown; stop is not yet confirmed."
                          : "Execution stopped; final outcome unknown.") +
                        (turn.unknownReason ? ` ${turn.unknownReason}` : "")
                      : undefined)),
                failureAt:
                  !message.intermediate &&
                  !turn.error &&
                  !turn.toolError &&
                  message.status !== "completed" &&
                  message.status !== "streaming" &&
                  message.status !== "pending" &&
                  turn.job.settlement.settled_at_us !== null
                    ? turn.job.settlement.settled_at_us / 1000
                    : undefined,
              }),
        })),
      ];
    });
    this.emit({ type: "conversation.history-loaded", sessionId, messages });
    const queue = turns
      .filter(
        (turn) => turn.job.started_at_us === null && turn.job.settlement.state === "unsettled",
      )
      .flatMap((turn) =>
        materializeUserEntries(turn.entries ?? [])
          .filter((entry) => entry.kind === "user_message" && !entry.withdrawn)
          .map((entry) => ({
            id: `${turn.job.id}:entry:${entry.seq}`,
            input: {
              content: entry.text ?? "",
              images: entry.attachments.flatMap((id) => this.images.get(id) ?? []),
              mode: "queue" as const,
            },
          })),
      );
    this.emit({ type: "conversation.queue-updated", sessionId, queue, paused: false });
  }

  private watch(jobId: string) {
    const turn = this.turns.get(jobId);
    if (!turn || this.streams.has(jobId) || this.disposed) return;
    const entry: StreamEntry = {
      subscription: null,
      unsubscribeCatalog: null,
      stopped: false,
      stop: () => {},
    };
    let inFlight = false;
    let queued = false;
    let catalogDirty = false;
    let catalogTimer: ReturnType<typeof setTimeout> | undefined;
    let needsReplay = false;
    let replayThrough = 0;
    let recoveryDelay = CATALOG_REFRESH_DELAY_MS;
    let recoveryAfter = 0;
    let loadedContinuation = false;
    const finish = () => {
      entry.stopped = true;
      if (catalogTimer !== undefined) clearTimeout(catalogTimer);
      entry.unsubscribeCatalog?.();
      entry.subscription?.close();
      if (this.streams.get(jobId) === entry) this.streams.delete(jobId);
    };
    entry.stop = finish;
    const requestTick = () => {
      if (entry.stopped || this.disposed) return;
      if (needsReplay && Date.now() < recoveryAfter) {
        scheduleCatalogTick();
        return;
      }
      if (inFlight) {
        queued = true;
        return;
      }
      void tick();
    };
    const scheduleCatalogTick = () => {
      catalogDirty = true;
      if (entry.stopped || this.disposed || inFlight || catalogTimer !== undefined) return;
      catalogTimer = setTimeout(
        () => {
          catalogTimer = undefined;
          if (inFlight) return;
          catalogDirty = false;
          requestTick();
        },
        needsReplay && recoveryAfter > Date.now()
          ? recoveryAfter - Date.now()
          : CATALOG_REFRESH_DELAY_MS,
      );
    };
    const tick = async () => {
      if (inFlight || entry.stopped || this.disposed) return;
      inFlight = true;
      try {
        const details = await this.control.readJobDetails(jobId, this.lifetime.signal);
        const readReplies = (turn.outputs[0]?.seq ?? 1) === 1;
        const evidence = await readTranscriptEvidence(this.control, jobId, {
          signal: this.lifetime.signal,
          previous: turn.responses,
          settled: readReplies && details.settlement.state !== "unsettled",
          readReplies,
        });
        if (entry.stopped || this.disposed) return;
        if (turn.job.conversation) {
          const page = await this.control.readConversationTurns(
            turn.job.conversation,
            undefined,
            20,
            this.lifetime.signal,
          );
          const stored = page.turns.find((item) => item.job.id === jobId);
          if (stored) {
            turn.entries = stored.entries;
            turn.compaction = stored.compaction;
            turn.input = stored.input;
            for (const image of await this.readImages(turn)) this.images.set(image.id, image);
          }
        }
        if (entry.stopped || this.disposed) return;
        turn.job = reconcileJob(turn.job, details);
        turn.details = details;
        turn.unknownReason = details.unknown_reason;
        const last = turn.outputs.at(-1)?.seq ?? 0;
        const through = Math.max(
          details.activity.last_output_seq,
          details.result?.seq ?? 0,
          replayThrough,
        );
        if (through > last) {
          let missing: ControlOutput[];
          try {
            missing = await readMissingOutputs(
              this.control,
              jobId,
              last,
              through,
              this.lifetime.signal,
            );
          } catch (error) {
            needsReplay = true;
            replayThrough = Math.max(replayThrough, through);
            entry.subscription?.close();
            throw error;
          }
          if (entry.stopped || this.disposed) return;
          turn.outputs = mergeOutputs(turn.outputs, missing);
        }
        turn.tools = reuseEvidence(turn.tools, evidence.tools);
        if (readReplies && (turn.outputs[0]?.seq ?? 1) === 1)
          turn.responses = reuseEvidence(turn.responses, evidence.responses);
        if (
          (turn.outputs[0]?.seq ?? 1) > 1 &&
          details.settlement.state === "completed" &&
          turn.tools.length &&
          turn.partialToolBoundaries == null
        ) {
          const times = new Map(turn.partialToolTimes);
          const boundaries = await readPartialToolBoundaries(
            this.control,
            jobId,
            turn.tools,
            turn.outputs,
            this.lifetime.signal,
            times,
            turn.responses,
            this.toolTimeGate,
          );
          if (entry.stopped || this.disposed) return;
          turn.partialToolTimes = times;
          turn.partialToolBoundaries = boundaries;
        }
        turn.toolError = evidence.error;
        turn.error = undefined;
        this.publish(turn.sessionId);
        if (details.task_plan && !loadedContinuation) {
          loadedContinuation = true;
          await this.openConversation(details.task_plan.continuation);
        }
        if (details.settlement.state !== "unsettled" && !taskPlanActive(details.task_plan)) {
          finish();
        } else if (needsReplay) {
          needsReplay = false;
          replayThrough = 0;
          recoveryAfter = 0;
          recoveryDelay = CATALOG_REFRESH_DELAY_MS;
          connect();
        }
      } catch (error) {
        if (!this.disposed && !entry.stopped) {
          if (needsReplay) {
            recoveryAfter = Date.now() + recoveryDelay;
            recoveryDelay = Math.min(recoveryDelay * 2, MAX_RECOVERY_DELAY_MS);
          }
          turn.error = `Status refresh unavailable: ${error instanceof Error ? error.message : String(error)}. Displayed state may be stale.${needsReplay ? " Reload to retry now." : ""}`;
          this.publish(turn.sessionId);
        }
      } finally {
        inFlight = false;
        if (queued) {
          queued = false;
          requestTick();
        } else if (catalogDirty || needsReplay) scheduleCatalogTick();
      }
    };
    this.streams.set(jobId, entry);
    const connect = () => {
      const subscription = this.control.subscribeJobEvents(
        jobId,
        (event) => {
          if (this.disposed || entry.stopped) return;
          if (event.type === "heartbeat") return;
          if (event.type === "job") turn.job = reconcileJob(turn.job, event.job);
          if (event.type === "output" && event.output.seq > (turn.outputs.at(-1)?.seq ?? 0)) {
            if (event.output.seq !== (turn.outputs.at(-1)?.seq ?? 0) + 1) {
              replayThrough = Math.max(replayThrough, event.output.seq);
              throw new ContractError("output.seq", "the next stored sequence without a gap");
            }
            turn.outputs.push(event.output);
            turn.outputRevision = (turn.outputRevision ?? 0) + 1;
          }
          if (event.type === "settlement") {
            turn.job = {
              ...turn.job,
              settlement: event.settlement,
              stop_confirmed_at_us: event.stop_confirmed_at_us,
            };
          }
          if (event.type === "stream_error")
            turn.error = `${event.error.code}: ${event.error.message}`;
          this.publish(turn.sessionId);
          if (event.type !== "output") requestTick();
        },
        {
          signal: this.lifetime.signal,
          afterSeq: turn.outputs.at(-1)?.seq ?? 0,
          onReconnect: (error) => {
            turn.error = `Reconnecting: ${error.code}`;
            this.publish(turn.sessionId);
            requestTick();
          },
        },
      );
      entry.subscription = subscription;
      void subscription.closed.then(requestTick).catch((error) => {
        if (this.disposed || entry.stopped) return;
        turn.error = `Stream unavailable: ${error instanceof Error ? error.message : String(error)}. Recovering stored status.`;
        this.publish(turn.sessionId);
        if (error instanceof ContractError && error.path === "output.seq") {
          needsReplay = true;
          scheduleCatalogTick();
        }
      });
    };
    connect();
    entry.unsubscribeCatalog = this.control.subscribeCatalog(scheduleCatalogTick, (error) => {
      if (this.disposed || entry.stopped) return;
      turn.error = `Catalog subscription unavailable: ${error instanceof Error ? error.message : String(error)}`;
      this.publish(turn.sessionId);
    });
    void tick();
  }

  override async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    await this.submit(
      sessionId,
      input,
      this.pending.get(sessionId)?.submission.conversation ??
        (this.pending.has(sessionId)
          ? undefined
          : [...this.roots].find(([, id]) => id === sessionId)?.[0]),
    );
  }

  private async submit(sessionId: string, input: SendMessageInput, conversation?: string) {
    if (this.hiding.has(sessionId))
      throw new Error("Wait for this conversation to finish hiding before sending.");
    if (!this.getSnapshot().sessions.some((session) => session.id === sessionId))
      throw new Error("Conversation presentation is no longer available.");
    if (this.sending.has(sessionId)) throw new Error("This turn is being submitted.");
    const selection = input.selection ?? this.selections.get(sessionId);
    if (!selection?.agentProfile || !selection.llmProfile || !selection.executor)
      throw new Error("Select an Agent, Model and Executor before sending.");
    const predecessor = this.turns.get(this.tips.get(sessionId) ?? "");
    if (
      predecessor?.input.configuration?.agent_profile &&
      predecessor.input.configuration.agent_profile !== selection.agentProfile
    )
      throw new Error(
        "A follow-up keeps the conversation's Agent. Start a new conversation to change Agents.",
      );
    const submission: ControlSubmission = {
      ...(selection.effort ? { reasoning_effort: selection.effort } : {}),
      ...(conversation && input.mode ? { mode: input.mode } : {}),
      agent_profile: selection.agentProfile,
      llm_profile: selection.llmProfile,
      executor: selection.executor,
      objective: input.content,
      ...(conversation
        ? { conversation }
        : selection.project
          ? { project: selection.project }
          : {}),
      attachments: input.images?.map((image) => image.id) ?? [],
    };
    const signature = JSON.stringify(submission);
    const held = this.pending.get(sessionId);
    if (held && held.signature !== signature)
      throw new Error(
        "The previous admission is unresolved. Resend the unchanged message to resolve its existing Idempotency-Key.",
      );
    const pending: Pending = held ?? { key: crypto.randomUUID(), signature, submission };
    this.pending.set(sessionId, pending);
    this.selections.set(sessionId, selection);
    this.sending.add(sessionId);
    try {
      // A resend of an ambiguous or pending admission uses the original payload and key.
      let job = pending.admittedJob;
      if (!job) {
        const admission = await this.control.submitJob(
          pending.submission,
          pending.key,
          this.lifetime.signal,
        );
        // The admission already names the conversation it opened, so the draft
        // claims it here instead of after another read.
        if (admission.conversation) this.claimConversation(admission.conversation, sessionId);
        job = admission.job ?? undefined;
      }
      if (!job)
        job =
          (await this.control.readAdmission(pending.key, this.lifetime.signal)).job ?? undefined;
      if (!job)
        throw new Error(
          "Admission is pending. Resend this unchanged message to check it with the same Idempotency-Key.",
        );
      pending.admittedJob = job;
      const session = this.getSnapshot().sessions.find((session) => session.id === sessionId);
      if (!session) throw new Error("Conversation presentation is no longer available.");
      this.tips.set(sessionId, job);
      for (const image of input.images ?? []) this.images.set(image.id, image);
      const admitted = await this.control.readJob(job);
      if (admitted.conversation) this.claimConversation(admitted.conversation, sessionId);
      this.emit({
        type: "conversation.session-updated",
        session: {
          ...session,
          conversationId: admitted.conversation ?? undefined,
          sessionId: job,
          agentProfile: selection.agentProfile,
          projectId: conversation ? session.projectId : selection.project,
          title: conversation ? session.title : input.content.slice(0, 80) || "Image conversation",
          updatedAt: new Date().toISOString(),
        },
      });
      await this.trackTurn(sessionId, job);
    } catch (error) {
      if (
        !pending.admittedJob &&
        error instanceof ControlTransportError &&
        ((error.status >= 400 && error.status < 500) || error.status === 503)
      )
        this.pending.delete(sessionId);
      throw error;
    } finally {
      this.sending.delete(sessionId);
    }
  }

  private async trackTurn(sessionId: string, job: string, clearPending = true) {
    const previous = this.turns.get(job);
    const snapshot = previous ? { ...previous } : undefined;
    const [record, frozen, window] = await Promise.all([
      this.control.readJob(job),
      this.control.readJobInput(job),
      readOutputWindow(this.control, job, { signal: this.lifetime.signal }),
    ]);
    let outputs = window.outputs;
    if (previous?.outputs.length && outputs.length) {
      const gap = await readMissingOutputs(
        this.control,
        job,
        previous.outputs.at(-1)?.seq ?? 0,
        outputs[0].seq - 1,
        this.lifetime.signal,
      );
      outputs = mergeOutputs(previous.outputs, gap, outputs);
    }
    const evidence = await readTranscriptEvidence(this.control, job, {
      signal: this.lifetime.signal,
      readReplies: (outputs[0]?.seq ?? 1) === 1,
    });
    const page = record.conversation
      ? await this.control.readConversationTurns(
          record.conversation,
          undefined,
          20,
          this.lifetime.signal,
        )
      : null;
    const stored = page?.turns.find((turn) => turn.job.id === job);
    const turn: Turn = {
      compaction: stored?.compaction,
      entries: stored?.entries,
      sessionId,
      job: record,
      input: stored?.input ?? frozen,
      outputs,
      tools: evidence.tools ?? [],
      responses: evidence.responses ?? [],
      toolError: evidence.error,
      unknownReason: record.unknown_reason ?? null,
    };
    if (
      (outputs[0]?.seq ?? 1) > 1 &&
      record.settlement.state === "completed" &&
      turn.tools.length
    ) {
      const times = new Map(previous?.partialToolTimes);
      turn.partialToolBoundaries = await readPartialToolBoundaries(
        this.control,
        job,
        turn.tools,
        outputs,
        this.lifetime.signal,
        times,
        previous?.responses ?? [],
        this.toolTimeGate,
      );
      turn.partialToolTimes = times;
    }
    if (!this.getSnapshot().sessions.some((s) => s.id === sessionId)) return;
    const current = this.turns.get(job);
    if (current && !this.historyTurnIsCurrent({ state: turn, previous, snapshot })) {
      // The watcher owns this object. Keep its live state and apply the
      // freshly read entries, including queue edits, without replacing it.
      current.entries = turn.entries;
      current.input = turn.input;
      current.compaction = turn.compaction;
    } else {
      if (current) turn.job = reconcileJob(current.job, turn.job);
      this.turns.set(job, turn);
    }
    if (clearPending) this.pending.delete(sessionId);
    this.publish(sessionId);
    if (record.settlement.state === "unsettled") this.watch(job);
  }

  override async removeQueuedMessage(sessionId: string, id: string): Promise<void> {
    await this.amendQueue(sessionId, id);
  }
  override async editQueuedMessage(sessionId: string, id: string, content: string): Promise<void> {
    await this.amendQueue(sessionId, id, content);
  }
  private async amendQueue(sessionId: string, id: string, content?: string) {
    const turn = [...this.turns.values()].find(
      (turn) => turn.sessionId === sessionId && id.startsWith(`${turn.job.id}:entry:`),
    );
    const entry = turn
      ? materializeUserEntries(turn.entries ?? []).find(
          (entry) => `${turn.job.id}:entry:${entry.seq}` === id && !entry.withdrawn,
        )
      : undefined;
    if (!turn?.job.conversation || !entry)
      throw new Error("Queued message is no longer available.");
    try {
      await this.control.amendQueuedMessage(
        turn.job.conversation,
        content === undefined
          ? { action: "withdraw", turn: turn.job.id, message: entry.seq }
          : {
              action: "edit",
              turn: turn.job.id,
              message: entry.seq,
              text: content,
              attachments: entry.attachments,
            },
        this.lifetime.signal,
      );
    } finally {
      await this.trackTurn(sessionId, turn.job.id, false);
    }
  }

  override async retryMessage(sessionId: string, messageId: string): Promise<void> {
    const message = this.getSnapshot().messagesBySession[sessionId]?.find(
      (message) => message.id === messageId,
    );
    const turn = message?.jobId ? this.turns.get(message.jobId) : undefined;
    if (
      !turn ||
      !turn.job.conversation ||
      !["failed", "unknown", "cancelled"].includes(turn.job.settlement.state) ||
      (this.tips.get(sessionId) !== turn.job.id &&
        this.pending.get(sessionId)?.submission.conversation !== turn.job.conversation)
    )
      throw new Error("Only the latest failed, unknown or cancelled turn can be retried.");
    if (this.hiding.has(sessionId) || this.sending.has(sessionId))
      throw new Error("Wait for this conversation's current action to finish.");
    const session = this.getSnapshot().sessions.find((session) => session.id === sessionId);
    if (!session) throw new Error("Conversation presentation is no longer available.");
    this.sending.add(sessionId);
    try {
      const result = await this.control.retryConversation(
        turn.job.conversation,
        turn.job.id,
        this.lifetime.signal,
      );
      this.tips.set(sessionId, result.turn);
      this.emit({
        type: "conversation.session-updated",
        session: { ...session, sessionId: result.turn, updatedAt: new Date().toISOString() },
      });
      await this.trackTurn(sessionId, result.turn);
    } finally {
      this.sending.delete(sessionId);
    }
  }

  override async cancel(sessionId: string): Promise<void> {
    const job = this.tips.get(sessionId);
    if (!job) throw new Error("No admitted turn to stop.");
    const result = await this.control.stopConversation(
      this.turns.get(job)?.job.conversation ?? job,
      this.lifetime.signal,
    );
    const turn = this.turns.get(job);
    if (turn) {
      turn.error =
        result.status === "requested"
          ? "Stop requested; awaiting the host's settlement."
          : "The host reports this turn already settled.";
      this.publish(sessionId);
    }
  }

  override dispose() {
    this.disposed = true;
    this.lifetime.abort();
    this.control.close();
    if (this.unsubscribeConversations) {
      this.unsubscribeConversations();
      this.unsubscribeConversations = null;
    }
    for (const entry of this.streams.values()) {
      entry.stop();
    }
    this.streams.clear();
    super.dispose();
  }
}
