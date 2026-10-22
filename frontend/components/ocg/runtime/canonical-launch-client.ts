import type { ChatImage, ChatImageUploadRequest, ChatModelSelection, ControlConversationTurn, ControlJob, ControlOutput, ControlProviderResponse, ControlToolCall } from "../contracts";
import type { ChatMessage, ChatSession, OcgRuntimeEvent, SendMessageInput } from "../types";
import { RuntimeClientBase } from "./mock-client";
import type { CreateSessionInput, ScenarioId } from "./runtime-types";
import { createHttpCanonicalControlClient, readTranscriptEvidence, ControlTransportError, type ControlSubmission, type HttpCanonicalControlClient, type JobStreamSubscription } from "./control-client";
import { jobTranscript } from "../chat/job-transcript-model";
import { pageTurns } from "./history-page";

type Turn = { entries?: ControlConversationTurn["entries"]; sessionId: string; job: ControlJob; input: ControlConversationTurn["input"]; outputs: ControlOutput[]; tools: ControlToolCall[]; responses: ControlProviderResponse[]; error?: string; toolError?: string; unknownReason?: string | null };
type Pending = { key: string; signature: string; submission: ControlSubmission; admittedJob?: string };
type StreamEntry = { subscription: JobStreamSubscription | null; timer: ReturnType<typeof setTimeout> | null; stopped: boolean };

/** Projects and sessions are presentation scope only: every backend turn is a Job. */
export class CanonicalOcgRuntimeClient extends RuntimeClientBase {
  readonly authority = "canonical" as const;
  private started = false;
  private disposed = false;
  private readonly lifetime = new AbortController();
  private readonly turns = new Map<string, Turn>();
  private readonly roots = new Map<string, string>();
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
  private refresh: Promise<void> | null = null;

  constructor(scenario: ScenarioId, private readonly control: HttpCanonicalControlClient) {
    super(scenario, true, "canonical");
  }

  static connect(scenario: ScenarioId, baseUrl: string, fetchImpl: typeof fetch) {
    const control = createHttpCanonicalControlClient({ baseUrl, fetch: fetchImpl });
    return new CanonicalOcgRuntimeClient(scenario, control);
  }

  override subscribe(listener: (event: OcgRuntimeEvent) => void) {
    const unsubscribe = super.subscribe(listener);
    if (!this.started) {
      this.started = true;
      void this.refreshConversations().catch(error => this.report(error));
    }
    return unsubscribe;
  }

  private report(error: unknown, sessionId?: string) {
    if (!this.disposed) this.emit({ type: "runtime.status-changed", status: { state: "failed", sessionId, detail: error instanceof Error ? error.message : String(error) } });
  }

  refreshConversations(): Promise<void> {
    if (this.refresh) return this.refresh;
    this.refresh = this.readConversations().finally(() => { this.refresh = null; });
    return this.refresh;
  }

  private async readConversations() {
    let before: string | undefined;
    for (;;) {
      const page = await this.control.readConversations({ before, limit: 100, signal: this.lifetime.signal });
      if (this.disposed) return;
      for (const item of page.conversations) {
        if (this.hiddenRoots.has(item.root_job)) continue;
        const id = this.roots.get(item.root_job) ?? item.root_job;
        this.roots.set(item.root_job, id);
        this.unlisted.delete(id);
        const current = this.getSnapshot().sessions.find(session => session.id === id);
        // A stale list read must not roll an admitted local successor backwards.
        const known = this.tips.get(id);
        const local = known ? this.turns.get(known) : undefined;
        if (known && known !== item.tip_job && local &&
          (item.tip_created_at_us < local.job.created_at_us ||
            (item.tip_created_at_us === local.job.created_at_us && Number(item.tip_job.slice(4)) < Number(known.slice(4))))) continue;
        this.tips.set(id, item.tip_job);
        const session: ChatSession = { id, conversationId: item.root_job, sessionId: item.tip_job, title: item.title, agentProfile: item.agent ?? undefined, projectId: item.project ?? undefined, workType: "research", updatedAt: new Date(item.tip_created_at_us / 1000).toISOString() };
        this.emit({ type: current ? "conversation.session-updated" : "conversation.session-created", session });
      }
      if (!page.has_more) break;
      if (!page.next_before || page.next_before === before) throw new Error("Conversation list returned no advancing cursor.");
      before = page.next_before;
    }
    this.emit({ type: "runtime.status-changed", status: { state: "connected" } });
  }

  override async createSession(input: CreateSessionInput): Promise<ChatSession> {
    const session: ChatSession = { id: `draft-${crypto.randomUUID()}`, title: input.title ?? "New conversation", workType: input.workType, updatedAt: new Date().toISOString() };
    this.emit({ type: "conversation.session-created", session });
    return session;
  }

  override async deleteSession(): Promise<void> {
    throw new Error("The 0.5 Control Plane does not provide conversation deletion.");
  }

  private sessionRoot(sessionId: string): string {
    for (const [root, id] of this.roots.entries()) {
      if (id === sessionId) return root;
    }
    throw new Error("This session has no admitted conversation.");
  }

  async renameSession(sessionId: string, title: string): Promise<void> {
    const root = this.sessionRoot(sessionId);
    const trimmed = title.trim();
    await this.control.editConversation(root, { title: trimmed }, this.lifetime.signal);
    const session = this.getSnapshot().sessions.find(s => s.id === sessionId);
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
          stream.stopped = true;
          if (stream.timer) clearTimeout(stream.timer);
          stream.subscription?.close();
          this.streams.delete(jobId);
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
    const match = /^data:(image\/png|image\/jpeg);base64,([A-Za-z0-9+/=]+)$/.exec(request.data_url);
    if (!match || (match[1] !== "image/png" && match[1] !== "image/jpeg")) throw new Error("Only PNG and JPEG attachments are supported by this host.");
    const attachment = await this.control.uploadAttachment({ mime: match[1], data_base64: match[2], display_name: request.name }, signal);
    return { id: attachment.id, name: attachment.display_name ?? request.name, media_type: attachment.mime, url: this.control.attachmentContentUrl(attachment.id) };
  }

  isListedConversation(sessionId: string): boolean { return !this.unlisted.has(sessionId); }

  openConversation(job: string): Promise<string> {
    const existing = this.loading.get(job);
    if (existing) return existing;
    const task = this.readConversation(job).finally(() => this.loading.delete(job));
    this.loading.set(job, task);
    return task;
  }

  private async readConversation(job: string): Promise<string> {
    await this.refreshConversations().catch(() => {});
    const record = await this.control.readJob(job, this.lifetime.signal);
    const root = record.kind === "conversation" ? record.id : record.conversation;
    if (!root) throw new Error("This Job is a process turn with no conversation.");
    const page = await this.control.readConversationTurns(root, undefined, 20, this.lifetime.signal);
    const conversation = { ...page, turns: [...page.turns].reverse() };
    const hiddenBefore = this.hiddenRoots.has(root);
    const id = this.roots.get(root) ?? root;
    if (!this.roots.has(root)) { this.unlisted.add(id); this.roots.set(root, id); }
    job = conversation.turns.at(-1)?.job.id ?? job;
    if (!this.getSnapshot().sessions.some(session => session.id === id)) {
      this.emit({ type: "conversation.session-created", session: { id, conversationId: root, sessionId: job, title: conversation.turns[0]?.input.objective?.slice(0, 80) || "Conversation", workType: "research", updatedAt: new Date().toISOString() } });
    }
    // Read each turn's attachments and tool evidence as well as its stored text.
    const isCurrent = () => !this.disposed && (hiddenBefore || !this.hiddenRoots.has(root));
    const held = new Set([...this.turns.values()].filter(turn => turn.sessionId === id).map(turn => turn.job.id));
    await Promise.all(conversation.turns.map(async turn => {
      const loaded = await this.readHistoryTurn(id, turn, isCurrent);
      if (!loaded || !isCurrent()) return;
      for (const image of loaded.images) this.images.set(image.id, image);
      this.turns.set(turn.job.id, loaded.state);
    }));
    if (this.disposed || (!hiddenBefore && this.hiddenRoots.has(root))) return id;
    if (!this.earlier.has(id)) {
      this.earlier.set(id, pageTurns(new Set(), undefined, page).next);
    } else if (held.size > 0 && page.has_more && !page.turns.some(turn => held.has(turn.job.id))) {
      const pageIds = new Set(page.turns.map(turn => turn.job.id));
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
    const session = this.getSnapshot().sessions.find(session => session.id === id);
    if (!session) return id;
    this.emit({ type: "conversation.session-updated", session: { ...session, conversationId: root, sessionId: job, agentProfile: conversation.turns.at(-1)?.input.configuration?.agent_profile ?? undefined } });
    this.publish(id);
    for (const turn of conversation.turns) if (turn.job.settlement.state === "unsettled") this.watch(turn.job.id);
    return id;
  }

  hasEarlierHistory(sessionId: string): boolean {
    return !!this.earlier.get(sessionId);
  }

  loadEarlierHistory(sessionId: string): Promise<void> {
    const existing = this.loadingEarlier.get(sessionId);
    if (existing) return existing;
    const cursor = this.earlier.get(sessionId);
    if (!cursor || this.disposed) return Promise.resolve();
    const root = this.sessionRoot(sessionId);
    const isCurrent = () => !this.disposed && !this.hiddenRoots.has(root) && this.earlier.get(sessionId) === cursor;
    const task = (async () => {
      try {
        const page = await this.control.readConversationTurns(root, cursor, 20, this.lifetime.signal);
        if (!isCurrent()) return;
        const held = new Set([...this.turns.values()].filter(turn => turn.sessionId === sessionId).map(turn => turn.job.id));
        const { add, next } = pageTurns(held, cursor, page);
        const loaded = await Promise.all(add.map(turn => this.readHistoryTurn(sessionId, turn, isCurrent)));
        if (!isCurrent()) return;
        for (const turn of loaded) {
          if (!turn) continue;
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

  private async readHistoryTurn(sessionId: string, turn: ControlConversationTurn, isCurrent: () => boolean) {
    if (this.streams.has(turn.job.id) || !isCurrent()) return null;
    const evidence = await readTranscriptEvidence(this.control, turn.job.id, { signal: this.lifetime.signal });
    if (!isCurrent()) return null;
    const outputs = evidence.responses === null
      ? await this.control.readAllJobOutputs(turn.job.id, this.lifetime.signal) : turn.outputs;
    if (!isCurrent()) return null;
    const state: Turn = { sessionId, ...turn, outputs, tools: evidence.tools ?? [], responses: evidence.responses ?? [], toolError: evidence.error };
    if (turn.job.settlement.state === "unknown") {
      try {
        const record = await this.control.readJob(turn.job.id, this.lifetime.signal);
        if (!isCurrent()) return null;
        state.unknownReason = record.unknown_reason ?? null;
      } catch {
        state.unknownReason = null;
      }
    }
    const images = await this.readImages(state);
    if (!isCurrent()) return null;
    return { state, images };
  }

  private readonly images = new Map<string, ChatImage>();
  private async readImages(turn: Turn) {
    return Promise.all(turn.input.attachments.map(async id => {
      const cached = this.images.get(id);
      if (cached) return cached;
      const attachment = await this.control.readAttachment(id, this.lifetime.signal);
      return { id, name: attachment.display_name ?? id, media_type: attachment.mime, url: this.control.attachmentContentUrl(id) };
    }));
  }

  private publish(sessionId: string) {
    if (this.disposed || !this.getSnapshot().sessions.some(session => session.id === sessionId)) return;
    const turns = [...this.turns.values()].filter(turn => turn.sessionId === sessionId).sort((a, b) => a.job.created_at_us - b.job.created_at_us || Number(a.job.id.slice(4)) - Number(b.job.id.slice(4)));
    const messages: ChatMessage[] = turns.flatMap(turn => {
      const users: ChatMessage[] = (turn.entries ?? []).filter(entry => entry.kind === "user_message" || entry.kind === "agent_message").map(entry => ({ id: `${turn.job.id}:entry:${entry.seq}`, jobId: turn.job.id, commandId: turn.job.id, role: "user", content: entry.text ?? "", status: "completed", createdAt: new Date(entry.at_us / 1000).toISOString(), images: entry.attachments.flatMap(id => this.images.get(id) ?? []) }));
      return [...users, ...jobTranscript(turn.job.id, turn.entries ? null : turn.input.objective, turn.outputs, turn.job.settlement, turn.job.settlement.state === "unsettled", undefined, turn.tools, turn.job.previous === null, turn.responses).map(message => ({
      id: message.id, jobId: turn.job.id, commandId: turn.job.id, role: message.role, content: message.content,
      status: message.status,
      intermediate: message.intermediate,
      createdAt: new Date((message.createdAtUs ?? turn.job.created_at_us) / 1000).toISOString(),
      ...(message.role === "user" ? { images: turn.input.attachments.flatMap(id => this.images.get(id) ?? []) } : { toolCalls: [...(message.tools ?? [])], reasoning: message.reasoning, reasoningDurationMs: message.reasoningDurationMs, failureReason: message.intermediate ? undefined : turn.error ?? turn.toolError ?? message.detail ?? (message.status === "unknown" ? ((turn.job.stop_confirmed_at_us === null ? "Outcome unknown; stop is not yet confirmed." : "Execution stopped; final outcome unknown.") + (turn.unknownReason ? ` ${turn.unknownReason}` : "")) : undefined) }),
    }))];
    });
    this.emit({ type: "conversation.history-loaded", sessionId, messages });
  }

  private watch(jobId: string) {
    const turn = this.turns.get(jobId);
    if (!turn || this.streams.has(jobId) || this.disposed) return;
    const entry: StreamEntry = { subscription: null, timer: null, stopped: false };
    let delay = 1500;
    let inFlight = false;
    let outputVersion = 0;
    let lastToolsJson = JSON.stringify(turn.tools);

    const rearm = () => {
      if (entry.stopped || this.disposed) return;
      if (entry.timer) clearTimeout(entry.timer);
      entry.timer = setTimeout(() => void tick(), delay);
    };

    const tick = async () => {
      entry.timer = null;
      if (entry.stopped || this.disposed) return;
      if (typeof document !== "undefined" && document.hidden) {
        rearm();
        return;
      }
      inFlight = true;
      const version = outputVersion;
      try {
        const evidence = await readTranscriptEvidence(this.control, jobId, { signal: this.lifetime.signal, previous: turn.responses });
        const fetched = evidence.tools ?? turn.tools;
        const responses = evidence.responses ?? turn.responses;
        if (entry.stopped || this.disposed) return;
        const currentJson = JSON.stringify(fetched);
        const changed = currentJson !== lastToolsJson || responses.length !== turn.responses.length || evidence.error !== turn.toolError;
        lastToolsJson = currentJson;
        turn.tools = fetched;
        turn.responses = responses;
        const recovered = turn.toolError !== undefined;
        turn.toolError = evidence.error;
        delay = changed || version !== outputVersion ? 1500 : Math.min(delay * 2, 12000);
        if (changed || recovered) this.publish(turn.sessionId);
      } catch (error) {
        if (!this.disposed && !entry.stopped) {
          turn.toolError = `Transcript evidence unavailable: ${error instanceof Error ? error.message : String(error)}`;
          this.publish(turn.sessionId);
        }
      } finally {
        inFlight = false;
        if (!entry.stopped && !this.disposed) {
          rearm();
        }
      }
    };

    const afterSeq = turn.outputs.at(-1)?.seq ?? 0;
    const subscription = this.control.subscribeJobEvents(jobId, event => {
      if (this.disposed || entry.stopped) return;
      if (event.type === "job") turn.job = event.job;
      if (event.type === "output" && !turn.outputs.some(output => output.seq === event.output.seq)) {
        turn.outputs.push(event.output);
        outputVersion += 1;
        const backedOff = delay !== 1500;
        delay = 1500;
        if (backedOff && !inFlight) rearm();
      }
      if (event.type === "settlement") turn.job = { ...turn.job, settlement: event.settlement, stop_confirmed_at_us: event.stop_confirmed_at_us };
      if (event.type === "stream_error") {
        turn.error = `${event.error.code}: ${event.error.message}. Reopen this conversation to recover persisted output.`;
        this.publish(turn.sessionId);
        return;
      }
      this.publish(turn.sessionId);
    }, { signal: this.lifetime.signal, afterSeq, onReconnect: error => { turn.error = `Reconnecting: ${error.code}`; this.publish(turn.sessionId); } });

    entry.subscription = subscription;
    this.streams.set(jobId, entry);
    rearm();

    void subscription.closed.then(async end => {
      if (this.disposed || entry.stopped) return;
      entry.stopped = true;
      if (entry.timer) clearTimeout(entry.timer);
      if (end === "settled") {
        turn.error = undefined;
        try {
          const evidence = await readTranscriptEvidence(this.control, jobId, {
            signal: this.lifetime.signal, previous: turn.responses, settled: turn.job.settlement.state === "completed",
          });
          turn.tools = evidence.tools ?? turn.tools;
          turn.responses = evidence.responses ?? turn.responses;
          turn.toolError = evidence.error;
        } catch (error) {
          turn.toolError = `Transcript evidence unavailable: ${error instanceof Error ? error.message : String(error)}`;
        }
        if (turn.job.settlement.state === "unknown") {
          try {
            const record = await this.control.readJob(jobId, this.lifetime.signal);
            turn.unknownReason = record.unknown_reason ?? null;
          } catch {
            turn.unknownReason = null;
          }
        }
        if (this.disposed || this.turns.get(jobId) !== turn) return;
        this.publish(turn.sessionId);
        await this.refreshConversations();
      } else if (end === "stream_error") {
        this.publish(turn.sessionId);
      }
    }).catch(error => {
      if (this.disposed || this.turns.get(jobId) !== turn) return;
      turn.error = `Stream unavailable: ${error instanceof Error ? error.message : String(error)}. Reopen this conversation to recover persisted output.`;
      this.publish(turn.sessionId);
      this.report(error, turn.sessionId);
    }).finally(() => {
      entry.stopped = true;
      if (entry.timer) clearTimeout(entry.timer);
      if (this.streams.get(jobId) === entry) this.streams.delete(jobId);
    });
  }

  override async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    await this.submit(sessionId, input, this.pending.get(sessionId)?.submission.conversation ?? (this.pending.has(sessionId) ? undefined : [...this.roots].find(([, id]) => id === sessionId)?.[0]));
  }

  private async submit(sessionId: string, input: SendMessageInput, conversation?: string) {
    if (this.hiding.has(sessionId)) throw new Error("Wait for this conversation to finish hiding before sending.");
    if (!this.getSnapshot().sessions.some(session => session.id === sessionId)) throw new Error("Conversation presentation is no longer available.");
    if (this.sending.has(sessionId)) throw new Error("This turn is being submitted.");
    const selection = input.selection ?? this.selections.get(sessionId);
    if (!selection?.agentProfile || !selection.llmProfile || !selection.executor) throw new Error("Select an Agent, Model and Executor before sending.");
    const predecessor = this.turns.get(this.tips.get(sessionId) ?? "");
    if (predecessor?.input.configuration?.agent_profile && predecessor.input.configuration.agent_profile !== selection.agentProfile) throw new Error("A follow-up keeps the conversation's Agent. Start a new conversation to change Agents.");
    const submission: ControlSubmission = { agent_profile: selection.agentProfile, llm_profile: selection.llmProfile, executor: selection.executor, objective: input.content, ...(conversation ? { conversation } : selection.project ? { project: selection.project } : {}), attachments: input.images?.map(image => image.id) ?? [] };
    const signature = JSON.stringify(submission);
    const held = this.pending.get(sessionId);
    if (held && held.signature !== signature) throw new Error("The previous admission is unresolved. Resend the unchanged message to resolve its existing Idempotency-Key.");
    const pending: Pending = held ?? { key: crypto.randomUUID(), signature, submission };
    this.pending.set(sessionId, pending);
    this.selections.set(sessionId, selection);
    this.sending.add(sessionId);
    try {
      // A resend of an ambiguous or pending admission uses the original payload and key.
      let job = pending.admittedJob;
      if (!job) job = (await this.control.submitJob(pending.submission, pending.key, this.lifetime.signal)).job ?? undefined;
      if (!job) job = (await this.control.readAdmission(pending.key, this.lifetime.signal)).job ?? undefined;
      if (!job) throw new Error("Admission is pending. Resend this unchanged message to check it with the same Idempotency-Key.");
      pending.admittedJob = job;
      const session = this.getSnapshot().sessions.find(session => session.id === sessionId);
      if (!session) throw new Error("Conversation presentation is no longer available.");
      this.tips.set(sessionId, job);
      for (const image of input.images ?? []) this.images.set(image.id, image);
      const admitted = await this.control.readJob(job);
      if (admitted.conversation) this.roots.set(admitted.conversation, sessionId);
      this.emit({ type: "conversation.session-updated", session: { ...session, conversationId: admitted.conversation ?? undefined, sessionId: job, agentProfile: selection.agentProfile, projectId: conversation ? session.projectId : selection.project, title: conversation ? session.title : input.content.slice(0, 80) || "Image conversation", updatedAt: new Date().toISOString() } });
      await this.trackTurn(sessionId, job);
    } catch (error) {
      if (!pending.admittedJob && error instanceof ControlTransportError && ((error.status >= 400 && error.status < 500) || error.status === 503)) this.pending.delete(sessionId);
      if (conversation && this.unlisted.has(sessionId) && error instanceof ControlTransportError && error.status === 422 && error.code === "configuration_refused") throw new Error("Open this conversation's latest turn from Jobs to continue.");
      throw error;
    } finally { this.sending.delete(sessionId); }
  }

  private async trackTurn(sessionId: string, job: string) {
    const [record, frozen, outputs, evidence] = await Promise.all([this.control.readJob(job), this.control.readJobInput(job), this.control.readOutputs(job, { limit: 100 }), readTranscriptEvidence(this.control, job, { signal: this.lifetime.signal })]);
    const page = record.conversation ? await this.control.readConversationTurns(record.conversation, undefined, 20, this.lifetime.signal) : null;
    const stored = page?.turns.find(turn => turn.job.id === job);
    const turn: Turn = { entries: stored?.entries, sessionId, job: record, input: stored?.input ?? frozen, outputs: outputs.outputs, tools: evidence.tools ?? [], responses: evidence.responses ?? [], toolError: evidence.error, unknownReason: record.unknown_reason ?? null };
    while (outputs.has_more) {
      const page = await this.control.readOutputs(job, { afterSeq: turn.outputs.at(-1)?.seq, limit: 100 });
      turn.outputs.push(...page.outputs);
      if (!page.has_more) break;
      if (!page.outputs.length) throw new Error("Output page did not advance.");
    }
    if (!this.getSnapshot().sessions.some(s => s.id === sessionId)) return;
    this.turns.set(job, turn);
    this.pending.delete(sessionId);
    this.publish(sessionId);
    if (record.settlement.state === "unsettled") this.watch(job);
    void this.refreshConversations().catch(error => this.report(error, sessionId));
  }

  override async retryMessage(sessionId: string, messageId: string): Promise<void> {
    const message = this.getSnapshot().messagesBySession[sessionId]?.find(message => message.id === messageId);
    const turn = message?.jobId ? this.turns.get(message.jobId) : undefined;
    if (!turn || !turn.job.conversation || !["failed", "unknown", "cancelled"].includes(turn.job.settlement.state) || (this.tips.get(sessionId) !== turn.job.id && this.pending.get(sessionId)?.submission.conversation !== turn.job.conversation)) throw new Error("Only the latest failed, unknown or cancelled turn can be retried.");
    if (this.hiding.has(sessionId) || this.sending.has(sessionId)) throw new Error("Wait for this conversation's current action to finish.");
    const session = this.getSnapshot().sessions.find(session => session.id === sessionId);
    if (!session) throw new Error("Conversation presentation is no longer available.");
    this.sending.add(sessionId);
    try {
      const result = await this.control.retryConversation(turn.job.conversation, turn.job.id, this.lifetime.signal);
      this.tips.set(sessionId, result.turn);
      this.emit({ type: "conversation.session-updated", session: { ...session, sessionId: result.turn, updatedAt: new Date().toISOString() } });
      await this.trackTurn(sessionId, result.turn);
    } finally { this.sending.delete(sessionId); }
  }

  override async cancel(sessionId: string): Promise<void> {
    const job = this.tips.get(sessionId);
    if (!job) throw new Error("No admitted turn to stop.");
    const result = await this.control.stopConversation(this.turns.get(job)?.job.conversation ?? job, this.lifetime.signal);
    const turn = this.turns.get(job);
    if (turn) { turn.error = result.status === "requested" ? "Stop requested; awaiting the host's settlement." : "The host reports this turn already settled."; this.publish(sessionId); }
  }

  override dispose() {
    this.disposed = true;
    this.lifetime.abort();
    for (const entry of this.streams.values()) {
      entry.stopped = true;
      if (entry.timer) clearTimeout(entry.timer);
      entry.subscription?.close();
    }
    this.streams.clear();
    super.dispose();
  }
}
