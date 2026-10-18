import type { ChatImage, ChatImageUploadRequest, ChatModelSelection, ControlConversationTurn, ControlJob, ControlOutput, ControlToolCall } from "../contracts";
import type { ChatMessage, ChatSession, OcgRuntimeEvent, SendMessageInput } from "../types";
import { RuntimeClientBase } from "./mock-client";
import type { CreateSessionInput, ScenarioId } from "./runtime-types";
import { createHttpCanonicalControlClient, ControlTransportError, type ControlSubmission, type HttpCanonicalControlClient, type JobStreamSubscription } from "./control-client";
import { jobTranscript } from "../chat/job-transcript-model";

type Turn = { sessionId: string; job: ControlJob; input: ControlConversationTurn["input"]; outputs: ControlOutput[]; tools: ControlToolCall[]; error?: string };
type Pending = { key: string; signature: string; submission: ControlSubmission; admittedJob?: string };

/** Projects and sessions are presentation scope only: every backend turn is a Job. */
export class CanonicalOcgRuntimeClient extends RuntimeClientBase {
  readonly authority = "canonical" as const;
  private started = false;
  private disposed = false;
  private readonly lifetime = new AbortController();
  private readonly turns = new Map<string, Turn>();
  private readonly roots = new Map<string, string>();
  private readonly tips = new Map<string, string>();
  private readonly selections = new Map<string, ChatModelSelection>();
  private readonly pending = new Map<string, Pending>();
  private readonly sending = new Set<string>();
  private readonly loading = new Map<string, Promise<string>>();
  private readonly streams = new Map<string, { subscription: JobStreamSubscription; timer: ReturnType<typeof setInterval> }>();
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

  private report(error: unknown) {
    if (!this.disposed) this.emit({ type: "runtime.status-changed", status: { state: "failed", detail: error instanceof Error ? error.message : String(error) } });
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
        const id = this.roots.get(item.root_job) ?? item.root_job;
        this.roots.set(item.root_job, id);
        const current = this.getSnapshot().sessions.find(session => session.id === id);
        // A stale list read must not roll an admitted local successor backwards.
        const known = this.tips.get(id);
        const local = known ? this.turns.get(known) : undefined;
        if (known && known !== item.tip_job && local &&
          (item.tip_created_at_us < local.job.created_at_us ||
            (item.tip_created_at_us === local.job.created_at_us && Number(item.tip_job.slice(4)) < Number(known.slice(4))))) continue;
        this.tips.set(id, item.tip_job);
        const session: ChatSession = { id, sessionId: item.tip_job, title: item.title, agentProfile: item.agent, workType: "research", updatedAt: new Date(item.tip_created_at_us / 1000).toISOString() };
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

  setChatSelection(sessionId: string, selection: ChatModelSelection) {
    this.selections.set(sessionId, selection);
  }

  async uploadChatImage(request: ChatImageUploadRequest, signal?: AbortSignal): Promise<ChatImage> {
    const match = /^data:(image\/png|image\/jpeg);base64,([A-Za-z0-9+/=]+)$/.exec(request.data_url);
    if (!match || (match[1] !== "image/png" && match[1] !== "image/jpeg")) throw new Error("Only PNG and JPEG attachments are supported by this host.");
    const attachment = await this.control.uploadAttachment({ mime: match[1], data_base64: match[2], display_name: request.name }, signal);
    return { id: attachment.id, name: attachment.display_name ?? request.name, media_type: attachment.mime, url: this.control.attachmentContentUrl(attachment.id) };
  }

  openConversation(job: string): Promise<string> {
    const existing = this.loading.get(job);
    if (existing) return existing;
    const task = this.readConversation(job).finally(() => this.loading.delete(job));
    this.loading.set(job, task);
    return task;
  }

  private async readConversation(job: string): Promise<string> {
    const conversation = await this.control.readJobConversation(job, this.lifetime.signal);
    const root = conversation.turns[0].job.id;
    const id = this.roots.get(root) ?? root;
    this.roots.set(root, id);
    if (!this.getSnapshot().sessions.some(session => session.id === id)) {
      this.emit({ type: "conversation.session-created", session: { id, sessionId: job, title: conversation.turns[0].input.objective?.slice(0, 80) || "Conversation", workType: "research", updatedAt: new Date().toISOString() } });
    }
    // Read each turn's attachments and tool evidence as well as its stored text.
    await Promise.all(conversation.turns.map(async turn => {
      if (this.streams.has(turn.job.id)) return;
      const tools = await this.control.readAllJobTools(turn.job.id, this.lifetime.signal);
      const state: Turn = { sessionId: id, ...turn, tools };
      await this.readImages(state);
      this.turns.set(turn.job.id, state);
    }));
    if (this.disposed) return id;
    this.tips.set(id, job);
    const session = this.getSnapshot().sessions.find(session => session.id === id)!;
    this.emit({ type: "conversation.session-updated", session: { ...session, sessionId: job, agentProfile: conversation.turns.at(-1)?.input.configuration?.agent_profile ?? undefined } });
    this.publish(id);
    for (const turn of conversation.turns) if (turn.job.settlement.state === "unsettled") this.watch(turn.job.id);
    return id;
  }

  private readonly images = new Map<string, ChatImage>();
  private async readImages(turn: Turn) {
    await Promise.all(turn.input.attachments.map(async id => {
      if (this.images.has(id)) return;
      const attachment = await this.control.readAttachment(id, this.lifetime.signal);
      this.images.set(id, { id, name: attachment.display_name ?? id, media_type: attachment.mime, url: this.control.attachmentContentUrl(id) });
    }));
  }

  private publish(sessionId: string) {
    if (this.disposed) return;
    const turns = [...this.turns.values()].filter(turn => turn.sessionId === sessionId).sort((a, b) => a.job.created_at_us - b.job.created_at_us || Number(a.job.id.slice(4)) - Number(b.job.id.slice(4)));
    const messages: ChatMessage[] = turns.flatMap(turn => jobTranscript(turn.job.id, turn.input.objective, turn.outputs, turn.job.settlement, turn.job.settlement.state === "unsettled").map(message => ({
      id: message.id, jobId: turn.job.id, commandId: turn.job.id, role: message.role, content: message.content,
      status: message.status,
      createdAt: new Date((message.createdAtUs ?? turn.job.created_at_us) / 1000).toISOString(),
      ...(message.role === "user" ? { images: turn.input.attachments.flatMap(id => this.images.get(id) ?? []) } : { toolCalls: turn.tools, failureReason: turn.error ?? message.detail ?? (message.status === "unknown" ? (turn.job.stop_confirmed_at_us === null ? "Outcome unknown; stop is not yet confirmed." : "Execution stopped; final outcome unknown.") : undefined) }),
    })));
    this.emit({ type: "conversation.history-loaded", sessionId, messages });
  }

  private watch(jobId: string) {
    const turn = this.turns.get(jobId);
    if (!turn || this.streams.has(jobId) || this.disposed) return;
    const tools = async () => {
      try { turn.tools = await this.control.readAllJobTools(jobId, this.lifetime.signal); this.publish(turn.sessionId); }
      catch (error) { if (!this.disposed) { turn.error = `Tool evidence unavailable: ${error instanceof Error ? error.message : String(error)}`; this.publish(turn.sessionId); } }
    };
    const afterSeq = this.control.lastOutputSequence(jobId) ?? turn.outputs.at(-1)?.seq ?? 0;
    const subscription = this.control.subscribeJobEvents(jobId, event => {
      if (this.disposed) return;
      if (event.type === "job") turn.job = event.job;
      if (event.type === "output" && !turn.outputs.some(output => output.seq === event.output.seq)) turn.outputs.push(event.output);
      if (event.type === "settlement") turn.job = { ...turn.job, settlement: event.settlement, stop_confirmed_at_us: event.stop_confirmed_at_us };
      if (event.type === "stream_error") throw new Error(`${event.error.code}: ${event.error.message}`);
      this.publish(turn.sessionId);
    }, { signal: this.lifetime.signal, afterSeq, onReconnect: error => { turn.error = `Reconnecting: ${error.code}`; this.publish(turn.sessionId); } });
    const timer = setInterval(() => void tools(), 1500);
    this.streams.set(jobId, { subscription, timer });
    void subscription.closed.then(async end => {
      if (end === "settled") {
        turn.error = undefined;
        await tools();
        await this.refreshConversations();
      }
    }).catch(error => {
      turn.error = `Stream unavailable: ${error instanceof Error ? error.message : String(error)}. Reopen this conversation to recover persisted output.`;
      this.publish(turn.sessionId);
      this.report(error);
    }).finally(() => { clearInterval(timer); this.streams.delete(jobId); });
  }

  override async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    await this.submit(sessionId, input, this.pending.get(sessionId)?.submission.previous ?? (this.pending.has(sessionId) ? undefined : this.tips.get(sessionId)));
  }

  private async submit(sessionId: string, input: SendMessageInput, previous?: string) {
    if (this.sending.has(sessionId)) throw new Error("This turn is being submitted.");
    if ([...this.turns.values()].some(turn => turn.sessionId === sessionId && turn.job.settlement.state === "unsettled")) throw new Error("Stop the running turn or wait for settlement before sending a follow-up.");
    const selection = input.selection ?? this.selections.get(sessionId);
    if (!selection?.agentProfile || !selection.llmProfile || !selection.executor) throw new Error("Select an Agent, Model and Executor before sending.");
    const predecessor = previous ? this.turns.get(previous) : undefined;
    if (predecessor?.input.configuration?.agent_profile && predecessor.input.configuration.agent_profile !== selection.agentProfile) throw new Error("A follow-up keeps the conversation's Agent. Start a new conversation to change Agents.");
    const submission: ControlSubmission = { agent_profile: selection.agentProfile, llm_profile: selection.llmProfile, executor: selection.executor, objective: input.content, ...(previous ? { previous } : {}), attachments: input.images?.map(image => image.id) ?? [] };
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
      this.tips.set(sessionId, job);
      for (const image of input.images ?? []) this.images.set(image.id, image);
      const session = this.getSnapshot().sessions.find(session => session.id === sessionId);
      if (!session) throw new Error("Conversation presentation is no longer available.");
      if (!previous) this.roots.set(job, sessionId);
      this.emit({ type: "conversation.session-updated", session: { ...session, sessionId: job, agentProfile: selection.agentProfile, title: previous ? session.title : input.content.slice(0, 80) || "Image conversation", updatedAt: new Date().toISOString() } });
      const [record, frozen, outputs, tools] = await Promise.all([this.control.readJob(job), this.control.readJobInput(job), this.control.readOutputs(job, { limit: 100 }), this.control.readAllJobTools(job)]);
      const turn: Turn = { sessionId, job: record, input: frozen, outputs: outputs.outputs, tools };
      while (outputs.has_more) {
        const page = await this.control.readOutputs(job, { afterSeq: turn.outputs.at(-1)?.seq, limit: 100 });
        turn.outputs.push(...page.outputs);
        if (!page.has_more) break;
        if (!page.outputs.length) throw new Error("Output page did not advance.");
      }
      this.turns.set(job, turn);
      this.pending.delete(sessionId);
      this.publish(sessionId);
      if (record.settlement.state === "unsettled") this.watch(job);
      void this.refreshConversations().catch(error => this.report(error));
    } catch (error) {
      if (!pending.admittedJob && error instanceof ControlTransportError && ((error.status >= 400 && error.status < 500) || error.status === 503)) this.pending.delete(sessionId);
      throw error;
    } finally { this.sending.delete(sessionId); }
  }

  override async retryMessage(sessionId: string, messageId: string): Promise<void> {
    const message = this.getSnapshot().messagesBySession[sessionId]?.find(message => message.id === messageId);
    const turn = message?.jobId ? this.turns.get(message.jobId) : undefined;
    if (!turn || turn.job.settlement.state !== "failed" || (this.tips.get(sessionId) !== turn.job.id && this.pending.get(sessionId)?.submission.previous !== turn.job.id)) throw new Error("Only the latest failed turn can be retried.");
    await this.submit(sessionId, { content: turn.input.objective ?? "", images: turn.input.attachments.flatMap(id => this.images.get(id) ?? []) }, turn.job.id);
  }

  override async cancel(sessionId: string): Promise<void> {
    const job = this.tips.get(sessionId);
    if (!job) throw new Error("No admitted turn to stop.");
    const result = await this.control.stopJob(job, this.lifetime.signal);
    const turn = this.turns.get(job);
    if (turn) { turn.error = result.status === "requested" ? "Stop requested; awaiting the host's settlement." : "The host reports this turn already settled."; this.publish(sessionId); }
  }

  override dispose() {
    this.disposed = true;
    this.lifetime.abort();
    for (const { subscription, timer } of this.streams.values()) { subscription.close(); clearInterval(timer); }
    this.streams.clear();
    super.dispose();
  }
}
