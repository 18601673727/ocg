import { ContractError, type Conversation, type Output } from "../../contracts";
import { ApiError, type JobStreamSubscription, readTranscriptEvidence } from "../api-client";
import { reconcileJob } from "../job-status";
import type { PublishScheduler } from "./publish-scheduler";
import { needsWatch, TURN_PAGE_SIZE, type Turn, turnAttachments } from "./turn";
import { applyTranscriptEvidence, type TurnReader } from "./turn-reader";

const CATALOG_REFRESH_DELAY_MS = 750;
const MAX_RECOVERY_DELAY_MS = 10_000;

type WatchPages = Map<string, Promise<Conversation>>;

type StreamEntry = {
  subscription: JobStreamSubscription | null;
  catalogChanged: (pages: WatchPages) => void;
  stopped: boolean;
  stop: () => void;
};

export class TurnStreams {
  private readonly streams = new Map<string, StreamEntry>();
  private unsubscribeWatchCatalog?: () => void;

  constructor(
    private readonly reader: TurnReader,
    private readonly turns: ReadonlyMap<string, Turn>,
    private readonly publications: PublishScheduler,
    private readonly openConversation: (job: string) => Promise<string>,
  ) {}

  has(jobId: string) {
    return this.streams.has(jobId);
  }

  stop(jobId: string) {
    this.streams.get(jobId)?.stop();
  }

  close() {
    for (const entry of this.streams.values()) entry.stop();
    this.streams.clear();
  }

  private readWatchPage(conversation: string, pages?: WatchPages) {
    const cached = pages?.get(conversation);
    if (cached) return cached;
    const page = this.reader.api.readConversationTurns(
      conversation,
      undefined,
      TURN_PAGE_SIZE,
      this.reader.signal,
    );
    pages?.set(conversation, page);
    return page;
  }

  private subscribeWatchCatalog() {
    this.unsubscribeWatchCatalog ??= this.reader.api.subscribeCatalog(
      () => {
        const pages: WatchPages = new Map();
        for (const stream of this.streams.values()) stream.catalogChanged(pages);
      },
      (error) => {
        for (const job of this.streams.keys()) {
          const turn = this.turns.get(job);
          if (!turn) continue;
          turn.error = `Catalog subscription unavailable: ${error instanceof Error ? error.message : String(error)}`;
          this.publications.publish(turn.sessionId);
        }
      },
    );
  }

  watch(jobId: string) {
    const { api, signal } = this.reader;
    const turn = this.turns.get(jobId);
    if (!turn || this.streams.has(jobId) || signal.aborted) return;
    if (!needsWatch(turn)) return;
    const entry: StreamEntry = {
      subscription: null,
      catalogChanged: (pages) => {
        pendingPages = pages;
        scheduleCatalogTick();
      },
      stopped: false,
      stop: () => {},
    };
    let pendingPages: WatchPages | undefined;
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
      entry.subscription?.close();
      if (this.streams.get(jobId) === entry) this.streams.delete(jobId);
      if (!this.streams.size) {
        this.unsubscribeWatchCatalog?.();
        this.unsubscribeWatchCatalog = undefined;
      }
    };
    entry.stop = finish;
    const requestTick = () => {
      if (entry.stopped || signal.aborted) return;
      if (Date.now() < recoveryAfter) {
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
      if (entry.stopped || signal.aborted || inFlight || catalogTimer !== undefined) return;
      catalogTimer = setTimeout(
        () => {
          catalogTimer = undefined;
          if (inFlight) return;
          catalogDirty = false;
          requestTick();
        },
        recoveryAfter > Date.now() ? recoveryAfter - Date.now() : CATALOG_REFRESH_DELAY_MS,
      );
    };
    const tick = async () => {
      if (inFlight || entry.stopped || signal.aborted) return;
      inFlight = true;
      try {
        this.publications.flushPending(turn.sessionId);
        if (entry.stopped || signal.aborted) return;
        const pages = pendingPages;
        pendingPages = undefined;
        const conversation = turn.job.conversation;
        const page = conversation ? await this.readWatchPage(conversation, pages) : undefined;
        if (entry.stopped || signal.aborted) return;
        const stored = page?.turns.find((item) => item.job.id === jobId);
        const details =
          pages &&
          !recoveryAfter &&
          !needsReplay &&
          turn.details?.mutable_phase === "queued" &&
          stored?.job.execution_phase === "queued" &&
          stored.job.event_seq === turn.details.event_seq
            ? turn.details
            : await api.readJobDetails(jobId, signal);
        const readReplies = turn.outputs.firstSeq === 1;
        const evidenceRevision = turn.evidenceRevision;
        const evidence =
          turn.native_decision || details.mutable_phase === "queued"
            ? null
            : await readTranscriptEvidence(api, jobId, {
                signal: signal,
                previous: turn.responses,
                settled:
                  readReplies &&
                  details.settlement.state !== "unsettled" &&
                  details.settlement.state !== "unknown",
                readReplies,
              });
        if (entry.stopped || signal.aborted) return;
        if (stored) {
          turn.content_kind = stored.content_kind;
          turn.native_decision = stored.native_decision;
          turn.planning_target = stored.planning_target;
          turn.entries = stored.entries;
          turn.compaction = stored.compaction;
          turn.input = stored.input;
          const images = await this.reader.readImages(turn);
          if (entry.stopped || signal.aborted) return;
          const attachments = new Set(turnAttachments(turn));
          for (const image of images) {
            if (attachments.has(image.id)) this.reader.images.set(image.id, image);
          }
        }
        if (entry.stopped || signal.aborted) return;
        turn.job = reconcileJob(turn.job, details);
        if (turn.job === details) {
          turn.details = details;
          turn.unknownReason = details.unknown_reason;
        }
        const last = turn.outputs.lastSeq;
        const through = Math.max(
          details.activity.last_output_seq,
          details.result?.seq ?? 0,
          replayThrough,
        );
        if (through > last) {
          let missing: Output[];
          try {
            missing = await this.reader.readMissingOutputs(jobId, last, through);
          } catch (error) {
            needsReplay = true;
            replayThrough = Math.max(replayThrough, through);
            entry.subscription?.close();
            throw error;
          }
          if (entry.stopped || signal.aborted) return;
          for (const output of missing) {
            if (output.seq <= turn.outputs.lastSeq) continue;
            turn.outputs.append(output);
            turn.outputRevision = (turn.outputRevision ?? 0) + 1;
          }
        }
        const evidenceApplied =
          evidence && applyTranscriptEvidence(turn, evidenceRevision, evidence, readReplies);
        if (
          turn.outputs.firstSeq > 1 &&
          details.settlement.state === "completed" &&
          turn.tools.length &&
          turn.partialToolBoundaries == null
        ) {
          const boundaryRevision = turn.evidenceRevision;
          const firstSeq = turn.outputs.firstSeq;
          const times = new Map(turn.partialToolTimes);
          const boundaries = await this.reader.readPartialToolBoundaries(turn, times);
          if (entry.stopped || signal.aborted) return;
          if (turn.evidenceRevision === boundaryRevision && turn.outputs.firstSeq === firstSeq) {
            turn.partialToolTimes = times;
            turn.partialToolBoundaries = boundaries;
          }
        }
        turn.error = undefined;
        this.publications.publish(turn.sessionId);
        if (details.task_plan && !loadedContinuation) {
          loadedContinuation = true;
          await this.openConversation(details.task_plan.continuation);
          if (entry.stopped || signal.aborted) return;
        }
        const active = needsWatch(turn);
        recoveryAfter = 0;
        recoveryDelay = CATALOG_REFRESH_DELAY_MS;
        if (active && details.mutable_phase !== "queued" && (needsReplay || !entry.subscription)) {
          needsReplay = false;
          replayThrough = 0;
          connect();
        }
        const evidenceCurrent =
          evidence === null ||
          (evidenceApplied &&
            turn.evidenceRevision === evidenceRevision + 1 &&
            (readReplies || turn.outputs.firstSeq > 1));
        if (!evidenceCurrent) {
          queued = true;
        } else if (!active) {
          finish();
        }
      } catch (error) {
        if (!signal.aborted && !entry.stopped) {
          const detail = error instanceof Error ? error.message : String(error);
          if (
            error instanceof ContractError ||
            (error instanceof ApiError &&
              (error.code === "not_found" ||
                (error.status >= 400 &&
                  error.status < 500 &&
                  ![401, 408, 429].includes(error.status))))
          ) {
            recoveryAfter = 0;
            finish();
            turn.error = `Status refresh stopped: ${detail}. Displayed state may be stale.`;
          } else {
            recoveryAfter = Date.now() + recoveryDelay;
            recoveryDelay = Math.min(recoveryDelay * 2, MAX_RECOVERY_DELAY_MS);
            turn.error = `Status refresh unavailable: ${detail}. Displayed state may be stale.${needsReplay ? " Reload to retry now." : ""}`;
          }
          this.publications.publish(turn.sessionId);
        }
      } finally {
        inFlight = false;
        if (queued) {
          queued = false;
          requestTick();
        } else if (catalogDirty || needsReplay || recoveryAfter) scheduleCatalogTick();
      }
    };
    this.streams.set(jobId, entry);
    const connect = () => {
      const subscription = api.subscribeJobEvents(
        jobId,
        (event) => {
          if (signal.aborted || entry.stopped) return;
          if (event.type === "heartbeat") return;
          if (event.type === "job") turn.job = reconcileJob(turn.job, event.job);
          if (event.type === "output") {
            if (event.output.seq <= turn.outputs.lastSeq) return;
            if (event.output.seq !== turn.outputs.lastSeq + 1) {
              replayThrough = Math.max(replayThrough, event.output.seq);
              throw new ContractError("output.seq", "the next stored sequence without a gap");
            }
            turn.outputs.append(event.output);
            turn.outputRevision = (turn.outputRevision ?? 0) + 1;
            this.publications.schedule(turn.sessionId);
            return;
          }
          if (event.type === "settlement") {
            turn.job = reconcileJob(turn.job, {
              ...turn.job,
              event_seq: event.event_seq,
              settlement: event.settlement,
              stop_confirmed_at_us: event.stop_confirmed_at_us,
            });
          }
          if (event.type === "stream_error")
            turn.error = `${event.error.code}: ${event.error.message}`;
          this.publications.publish(turn.sessionId);
          requestTick();
        },
        {
          signal: signal,
          afterSeq: turn.outputs.lastSeq,
          onReconnect: (error) => {
            if (signal.aborted || entry.stopped) return;
            turn.error = `Reconnecting: ${error.code}`;
            this.publications.publish(turn.sessionId);
            requestTick();
          },
        },
      );
      entry.subscription = subscription;
      void subscription.closed.then(requestTick).catch((error) => {
        if (signal.aborted || entry.stopped) return;
        turn.error = `Stream unavailable: ${error instanceof Error ? error.message : String(error)}. Recovering stored status.`;
        this.publications.publish(turn.sessionId);
        if (error instanceof ContractError && error.path === "output.seq") {
          needsReplay = true;
          scheduleCatalogTick();
        }
      });
    };
    // Queued turns share the catalog subscription until they have output to stream.
    if (turn.details?.mutable_phase !== "queued") connect();
    this.subscribeWatchCatalog();
    void tick();
  }
}
