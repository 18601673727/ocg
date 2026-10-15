import { ContractError, decodeControlError, decodeJobEvent, type ControlJobEvent } from "../contracts";
import { SseParser } from "./sse";

export type JobStreamEvent = ControlJobEvent | { type: "heartbeat"; };
export type JobStreamEnd = "settled" | "stream_error" | "aborted";
export type JobStreamSubscription = { closed: Promise<JobStreamEnd>; close(): void; };

export class ControlTransportError extends Error {
  constructor(readonly code: string, readonly status: number, readonly retryable: boolean) {
    // Do not echo fetch errors, request URLs, credentials or untrusted bodies.
    super(`OCG event transport: ${code} (${status})`);
    this.name = "ControlTransportError";
  }
}

export type HttpCanonicalControlClientOptions = {
  baseUrl: string;
  fetch: typeof fetch;
  // Supplied by an already authorized caller; no acquisition or storage here.
  bearerToken: () => string | Promise<string>;
};

function controlOrigin(raw: string): string {
  const url = new URL(raw);
  if (url.protocol !== "http:" || !["127.0.0.1", "localhost"].includes(url.hostname) ||
    !url.port || url.username || url.password || url.pathname !== "/" || url.search || url.hash) {
    throw new ControlTransportError("invalid_control_origin", 0, false);
  }
  if (typeof window !== "undefined" && window.location.origin !== url.origin) {
    throw new ControlTransportError("same_origin_required", 0, false);
  }
  return url.origin;
}

function sequence(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new ContractError("after_seq", "a non-negative safe integer");
  return value;
}

function pause(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) { resolve(); return; }
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
  });
}

// Retains the 0.4.5 canonical client's injected-fetch boundary and contracts
// entrypoint. None of its legacy lifecycle, chat or fixture stores are needed.
export function createHttpCanonicalControlClient(options: HttpCanonicalControlClientOptions) {
  const base = controlOrigin(options.baseUrl);
  const fetchImpl = options.fetch.bind(globalThis);
  // Delivery acknowledgements only; output history and Job state live on server.
  const cursors = new Map<string, number>();
  const active = new Map<string, AbortController>();
  let closed = false;

  async function connection(
    jobId: string,
    signal: AbortSignal,
    listener: (event: JobStreamEvent) => void | Promise<void>,
  ): Promise<Exclude<JobStreamEnd, "aborted"> | null> {
    const token = await options.bearerToken();
    if (!/^[A-Za-z0-9._~+/=-]{32,512}$/.test(token)) {
      throw new ControlTransportError("credential_unavailable", 0, false);
    }
    signal.throwIfAborted();
    const headers: Record<string, string> = { Accept: "text/event-stream", Authorization: `Bearer ${token}` };
    const cursor = cursors.get(jobId);
    if (cursor !== undefined) headers["Last-Event-ID"] = String(cursor);
    let response: Response;
    try {
      // The route requires the canonical literal job:<n>; it does not decode %3A.
      response = await fetchImpl(`${base}/api/v1/jobs/${jobId}/events`, {
        method: "GET", headers, signal,
        mode: "same-origin", credentials: "omit", cache: "no-store", redirect: "error",
      });
    } catch {
      throw new ControlTransportError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      try { code = decodeControlError(await response.json()).code; }
      catch { /* An unrecognized body still retains its HTTP status. */ }
      throw new ControlTransportError(code, response.status,
        response.status === 503 && ["too_many_streams", "database_busy", "database_unavailable", "shutting_down"].includes(code));
    }
    const mediaType = response.headers.get("content-type")?.split(";")[0].trim().toLowerCase();
    if (mediaType !== "text/event-stream" || !response.body) {
      await response.body?.cancel();
      throw new ControlTransportError("invalid_event_stream", response.status, false);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const parser = new SseParser();
    try {
      for (; ;) {
        let chunk: ReadableStreamReadResult<Uint8Array>;
        try { chunk = await reader.read(); }
        catch { throw new ControlTransportError("connection_lost", 0, true); }
        signal.throwIfAborted();
        // Discard an incomplete frame or UTF-8 tail at EOF. Neither was
        // acknowledged; the stored cursor will replay the entire output.
        if (chunk.done) return null;
        for (const frame of parser.push(decoder.decode(chunk.value, { stream: true }))) {
          signal.throwIfAborted();
          if ("comment" in frame) {
            if (frame.comment === "heartbeat") await listener({ type: "heartbeat" });
            continue;
          }
          let input: unknown;
          try { input = JSON.parse(frame.data); }
          catch { throw new ContractError("data", "a complete JSON event"); }
          const event = decodeJobEvent(frame.event, input, jobId, frame.id);
          if (event.type === "output") {
            const acknowledged = event.output.seq;
            const previous = cursors.get(jobId) ?? 0;
            if (acknowledged <= previous) continue;
            if (acknowledged !== previous + 1) throw new ContractError("output.seq", "the next stored sequence without a gap");
            await listener(event);
            // A rejected listener never acknowledges an unprocessed output.
            cursors.set(jobId, acknowledged);
          } else {
            await listener(event);
          }
          if (event.type === "settlement") return "settled";
          if (event.type === "stream_error") return "stream_error";
        }
      }
    } finally {
      try { await reader.cancel(); }
      catch { /* The read failure is reported above; cleanup cannot replace it. */ }
      reader.releaseLock();
    }
  }

  return {
    lastOutputSequence(jobId: string): number | undefined { return cursors.get(jobId); },

    subscribeJobEvents(
      jobId: string,
      listener: (event: JobStreamEvent) => void | Promise<void>,
      settings: {
        signal?: AbortSignal;
        // Only seed from output the canonical consumer has already processed.
        afterSeq?: number;
        onReconnect?: (error: ControlTransportError) => void | Promise<void>;
      } = {},
    ): JobStreamSubscription {
      if (closed) throw new ControlTransportError("client_closed", 0, false);
      if (!/^job:(0|[1-9][0-9]*)$/.test(jobId)) throw new ContractError("job", "a canonical Job identity");
      if (active.has(jobId)) throw new ControlTransportError("job_stream_already_open", 0, false);
      if (settings.afterSeq !== undefined) {
        const supplied = sequence(settings.afterSeq);
        const previous = cursors.get(jobId);
        if (previous !== undefined && supplied !== previous) throw new ContractError("after_seq", "the acknowledged Job cursor");
        cursors.set(jobId, supplied);
      }
      const controller = new AbortController();
      const signal = controller.signal;
      const stop = () => controller.abort();
      settings.signal?.addEventListener("abort", stop, { once: true });
      if (settings.signal?.aborted) stop();
      active.set(jobId, controller);
      const completed = (async (): Promise<JobStreamEnd> => {
        let delay = 250;
        try {
          while (!signal.aborted) {
            let failure = new ControlTransportError("premature_end", 0, true);
            try {
              const end = await connection(jobId, signal, listener);
              if (end) return end;
            } catch (error) {
              if (signal.aborted) return "aborted";
              if (!(error instanceof ControlTransportError) || !error.retryable) throw error;
              failure = error;
            }
            if (signal.aborted) return "aborted";
            await settings.onReconnect?.(failure);
            await pause(delay, signal);
            delay = Math.min(delay * 2, 10_000);
          }
          return "aborted";
        } finally {
          controller.abort();
          settings.signal?.removeEventListener("abort", stop);
          active.delete(jobId);
        }
      })();
      return { closed: completed, close: stop };
    },

    close(): void {
      closed = true;
      for (const controller of active.values()) controller.abort();
    },
  };
}
