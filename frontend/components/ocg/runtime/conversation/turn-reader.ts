import type { PartialToolBoundary } from "../../chat/job-transcript-model";
import type { ChatImage, Output } from "../../contracts";
import type { ApiClient, readTranscriptEvidence } from "../api-client";
import { type Turn, turnAttachments } from "./turn";

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

export function applyTranscriptEvidence(
  turn: Turn,
  baseRevision: number,
  evidence: Awaited<ReturnType<typeof readTranscriptEvidence>>,
  readReplies: boolean,
): boolean {
  if (turn.evidenceRevision !== baseRevision) return false;
  if (!readReplies && turn.outputs.firstSeq === 1) return false;
  turn.evidenceRevision++;
  const previousTools = turn.tools;
  turn.tools = reuseEvidence(turn.tools, evidence.tools);
  if (readReplies) {
    turn.responses = reuseEvidence(turn.responses, evidence.responses);
  }
  turn.toolError = evidence.error;
  if (turn.outputs.firstSeq === 1) {
    turn.partialToolTimes = undefined;
    turn.partialToolBoundaries = undefined;
  } else if (turn.tools !== previousTools) {
    turn.partialToolBoundaries = undefined;
  }
  return true;
}

export class TurnReader {
  readonly images = new Map<string, ChatImage>();
  private readonly toolTimeGate = new ToolTimeGate();

  constructor(
    readonly api: ApiClient,
    readonly signal: AbortSignal,
  ) {}

  async readImages(turn: Turn) {
    return Promise.all(
      [...new Set(turnAttachments(turn))].map(async (id) => {
        const cached = this.images.get(id);
        if (cached) return cached;
        const attachment = await this.api.readAttachment(id, this.signal);
        return {
          id,
          name: attachment.display_name ?? id,
          media_type: attachment.mime,
          url: this.api.attachmentContentUrl(id),
        };
      }),
    );
  }

  async readMissingOutputs(job: string, after: number, through: number): Promise<Output[]> {
    const { api, signal } = this;
    const outputs: Output[] = [];
    while (after < through) {
      const page = await api.readOutputs(job, { afterSeq: after, limit: 100, signal });
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

  async readPartialToolBoundaries(
    turn: Turn,
    times: Map<number, number>,
    responses = turn.responses,
  ): Promise<PartialToolBoundary[] | null> {
    const { tools, outputs } = turn;
    const job = turn.job.id;
    const { api, signal, toolTimeGate } = this;
    const first = outputs.at(0)?.created_at_us;
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
                ? toolTimeGate.read(controller.signal, () =>
                    api.readToolResponseTime(job, request, controller.signal),
                  )
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
}
