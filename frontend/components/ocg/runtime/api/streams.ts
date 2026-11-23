import { ContractError, type JobEvent, projectJobEvent } from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { SseParser } from "../sse";
import { ApiError, retryable, type Transport } from "./transport";
import { jobIdentity, sequence } from "./validators";

export type JobStreamEvent = JobEvent | { type: "heartbeat" };
export type JobStreamEnd = "settled" | "stream_error" | "aborted";
export type JobStreamSubscription = {
  closed: Promise<JobStreamEnd>;
  close(): void;
};

export function pause(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
  });
}

export function createStreams(
  transport: Transport,
  conversationListeners: Set<() => void>,
  onLocksUnavailable: () => void,
) {
  const { base, programmatic, fetchImpl, credentials, options } = transport;
  const active = new Set<AbortController>();
  let closed = false;
  let locksUnavailable = false;
  function browserLocks(): LockManager | undefined {
    if (programmatic || locksUnavailable || typeof navigator === "undefined") return undefined;
    try {
      return navigator.locks;
    } catch {
      locksUnavailable = true;
      return undefined;
    }
  }
  let snapshotBusy = 0;
  let lockTurn = 0;
  const snapshotWaiters: Array<() => void> = [];
  const holders = new Set<{ started: number; check(): void }>();

  function checkWaiters() {
    if (!snapshotWaiters.length) return;
    let oldest: { started: number; check(): void } | undefined;
    for (const holder of holders) {
      if (!oldest || holder.started < oldest.started) oldest = holder;
    }
    oldest?.check();
  }

  async function snapshotSlot(signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    if (snapshotBusy >= 3) {
      await new Promise<void>((resolve, reject) => {
        const wake = () => {
          signal.removeEventListener("abort", abort);
          resolve();
        };
        const abort = () => {
          const index = snapshotWaiters.indexOf(wake);
          if (index >= 0) snapshotWaiters.splice(index, 1);
          reject(signal.reason);
        };
        snapshotWaiters.push(wake);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
        else checkWaiters();
      });
    } else snapshotBusy++;
    return () => {
      const next = snapshotWaiters.shift();
      if (next) next();
      else snapshotBusy--;
      checkWaiters();
    };
  }

  function streamAttempt(parent: AbortSignal, holdsSnapshotSlot: boolean, lockName?: string) {
    const controller = new AbortController();
    let rotated = false;
    let complete = programmatic;
    const started = performance.now();
    const rotate = () => {
      if (rotated || controller.signal.aborted) return;
      rotated = true;
      controller.abort();
    };
    const holder = {
      started,
      check() {
        if (!complete || performance.now() - started < 5000) return;
        if (snapshotWaiters.length) {
          let oldest: typeof holder | undefined;
          for (const other of holders) {
            if (!oldest || other.started < oldest.started) oldest = other;
          }
          if (oldest === holder) rotate();
        }
      },
    };
    const abort = () => controller.abort();
    parent.addEventListener("abort", abort, { once: true });
    if (parent.aborted) abort();
    if (holdsSnapshotSlot) holders.add(holder);
    const timer = holdsSnapshotSlot ? setTimeout(() => holder.check(), 5000) : undefined;
    const poll = lockName
      ? setInterval(() => {
          const locks = browserLocks();
          if (!locks || typeof locks.query !== "function") return;
          void locks
            .query()
            .then((state) => {
              if (
                complete &&
                performance.now() - started >= 5000 &&
                (state.pending ?? []).some((request) => request.name === lockName)
              )
                rotate();
            })
            .catch(() => {});
        }, 2000)
      : undefined;
    checkWaiters();
    return {
      signal: controller.signal,
      rotated: () => rotated,
      complete: () => {
        complete = true;
        holder.check();
      },
      abort: () => controller.abort(),
      dispose: () => {
        if (timer !== undefined) clearTimeout(timer);
        if (poll !== undefined) clearInterval(poll);
        holders.delete(holder);
        parent.removeEventListener("abort", abort);
      },
    };
  }

  async function withSnapshotLock<T>(
    locks: LockManager,
    signal: AbortSignal,
    run: (name: string) => Promise<T>,
  ): Promise<T> {
    const names = Array.from({ length: 3 }, (_, index) => `ocg-snapshot-stream-${index}`);
    let probingSupported = true;
    for (const name of names) {
      signal.throwIfAborted();
      let entered = false;
      try {
        const result = await locks.request(name, { ifAvailable: true }, (lock) => {
          if (!lock) return undefined;
          entered = true;
          return run(name);
        });
        if (entered) return result as T;
      } catch (error) {
        if (entered || !(error instanceof DOMException && error.name === "NotSupportedError"))
          throw error;
        probingSupported = false;
        break;
      }
      signal.throwIfAborted();
    }
    signal.throwIfAborted();
    const parked = new AbortController();
    const name = names[lockTurn++ % names.length];
    return new Promise<T>((resolve, reject) => {
      let claimed = false;
      let probing = false;
      let timer: ReturnType<typeof setInterval> | undefined;
      const cleanup = () => {
        if (timer !== undefined) clearInterval(timer);
        signal.removeEventListener("abort", abort);
      };
      const abort = () => {
        parked.abort();
        cleanup();
        reject(signal.reason);
      };
      const enter = (lockName: string, fromProbe: boolean) => {
        if (claimed || signal.aborted) return undefined;
        claimed = true;
        cleanup();
        if (fromProbe) parked.abort();
        const outcome = run(lockName);
        outcome.then(resolve, reject);
        return outcome;
      };
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) {
        abort();
        return;
      }
      void locks
        .request(name, { signal: parked.signal }, () => enter(name, false))
        .catch((error) => {
          if (!claimed && !signal.aborted) {
            cleanup();
            reject(error);
          }
        });
      if (probingSupported)
        timer = setInterval(() => {
          if (probing || claimed || signal.aborted) return;
          probing = true;
          void (async () => {
            for (const candidate of names) {
              if (claimed || signal.aborted) break;
              try {
                await locks.request(candidate, { ifAvailable: true }, (lock) =>
                  lock ? enter(candidate, true) : undefined,
                );
              } catch (error) {
                if (error instanceof DOMException && error.name === "NotSupportedError") {
                  if (timer !== undefined) clearInterval(timer);
                  break;
                }
                if (!claimed && !signal.aborted) {
                  parked.abort();
                  cleanup();
                  reject(error);
                }
                break;
              }
              if (signal.aborted) break;
            }
            probing = false;
          })();
        }, 2000);
    });
  }
  async function connection(
    jobId: string,
    signal: AbortSignal,
    listener: (event: JobStreamEvent) => void | Promise<void>,
    cursor: { sequence: number | undefined },
  ): Promise<Exclude<JobStreamEnd, "aborted"> | null> {
    const headers: Record<string, string> = {
      Accept: "text/event-stream",
      ...(await credentials()),
    };
    if (cursor.sequence !== undefined) headers["Last-Event-ID"] = String(cursor.sequence);
    let response: Response;
    try {
      response = await fetchImpl(`${base}/api/v1/jobs/${jobId}/events`, {
        method: "GET",
        headers,
        signal,
        mode: "same-origin",
        credentials: programmatic ? "omit" : "same-origin",
        cache: "no-store",
        redirect: "error",
      });
    } catch {
      throw new ApiError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      try {
        code = ((await response.json()) as Api.ErrorResponse).error.code;
      } catch {}
      throw new ApiError(code, response.status, retryable(response.status, code));
    }
    const mediaType = response.headers.get("content-type")?.split(";")[0].trim().toLowerCase();
    if (mediaType !== "text/event-stream" || !response.body) {
      await response.body?.cancel();
      throw new ApiError("invalid_event_stream", response.status, false);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const parser = new SseParser();
    try {
      for (;;) {
        let chunk: ReadableStreamReadResult<Uint8Array>;
        let idle: ReturnType<typeof setTimeout> | undefined;
        try {
          chunk = await Promise.race([
            reader.read(),
            new Promise<never>((_, reject) => {
              idle = setTimeout(() => {
                reject(new ApiError("stream_idle", 0, true));
                void reader.cancel().catch(() => {});
              }, options.streamIdleMs ?? 45000);
            }),
          ]);
        } catch (error) {
          throw error instanceof ApiError ? error : new ApiError("connection_lost", 0, true);
        } finally {
          if (idle) clearTimeout(idle);
        }
        signal.throwIfAborted();
        if (chunk.done) return null;
        for (const frame of parser.push(decoder.decode(chunk.value, { stream: true }))) {
          signal.throwIfAborted();
          if ("comment" in frame) {
            if (frame.comment === "heartbeat") await listener({ type: "heartbeat" });
            continue;
          }
          let input: unknown;
          try {
            input = JSON.parse(frame.data);
          } catch {
            throw new ContractError("data", "a complete JSON event");
          }
          const event = projectJobEvent(frame.event, input);
          if (event.type === "output") {
            const acknowledged = event.output.seq;
            const previous = cursor.sequence ?? 0;
            if (acknowledged <= previous) continue;
            if (acknowledged !== previous + 1)
              throw new ContractError("output.seq", "the next stored sequence without a gap");
            await listener(event);
            cursor.sequence = acknowledged;
          } else {
            await listener(event);
          }
          if (event.type === "settlement") {
            conversationListeners.forEach((listener) => listener());
            return "settled";
          }
          if (event.type === "stream_error") return "stream_error";
        }
      }
    } finally {
      try {
        await reader.cancel();
      } catch {}
      reader.releaseLock();
    }
  }

  type Snapshot = {
    topic: string;
    key: string | null;
    first: boolean;
    last: boolean;
    value: unknown;
  };
  function subscribeSnapshots(
    path: string | (() => string),
    receive: (snapshot: Snapshot) => void,
    onError: (error: unknown) => void,
    initialTopics?: readonly string[],
  ) {
    const controller = new AbortController();
    active.add(controller);
    const signal = controller.signal;
    void (async () => {
      let delay = 250;
      const runAttempt = async (
        lockName?: string,
      ): Promise<{ keepGoing: boolean; wait: number }> => {
        const attempt = streamAttempt(signal, !programmatic, lockName);
        const remaining = initialTopics ? new Set(initialTopics) : null;
        const started = new Set<string>();
        let initialComplete = false;
        let observationReady = false;
        let stalled = false;
        let progressTimer: ReturnType<typeof setTimeout> | undefined;
        let lineKind: "start" | "comment" | "event" = "start";
        const progress = () => {
          if (programmatic || (initialComplete && !observationReady)) return;
          if (progressTimer !== undefined) clearTimeout(progressTimer);
          progressTimer = setTimeout(() => {
            stalled = true;
            attempt.abort();
          }, options.streamIdleMs ?? 45000);
        };
        progress();
        try {
          const response = await fetchImpl(`${base}${typeof path === "string" ? path : path()}`, {
            headers: {
              Accept: "text/event-stream",
              ...(await credentials()),
            },
            signal: attempt.signal,
            mode: "same-origin",
            credentials: programmatic ? "omit" : "same-origin",
            cache: "no-store",
            redirect: "error",
          });
          if (!response.ok) {
            let code = "http_error";
            try {
              code = ((await response.json()) as Api.ErrorResponse).error.code;
            } catch {}
            throw new ApiError(code, response.status, retryable(response.status, code));
          }
          if (
            !response.body ||
            response.headers.get("content-type")?.split(";")[0].trim() !== "text/event-stream"
          ) {
            await response.body?.cancel();
            throw new ApiError("invalid_event_stream", response.status, false);
          }
          const reader = response.body.getReader();
          const parser = new SseParser();
          const decoder = new TextDecoder("utf-8", { fatal: true });
          try {
            for (;;) {
              let idle: ReturnType<typeof setTimeout> | undefined;
              let chunk: ReadableStreamReadResult<Uint8Array>;
              try {
                chunk = await Promise.race([
                  reader.read(),
                  new Promise<never>((_, reject) => {
                    idle = setTimeout(() => {
                      reject(new ApiError("stream_idle", 0, true));
                      void reader.cancel().catch(() => {});
                    }, options.streamIdleMs ?? 45000);
                  }),
                ]);
              } finally {
                if (idle) clearTimeout(idle);
              }
              attempt.signal.throwIfAborted();
              if (chunk.done) {
                if (initialComplete && !observationReady) {
                  delay = 250;
                  return { keepGoing: true, wait: 100 };
                }
                break;
              }
              const text = decoder.decode(chunk.value, { stream: true });
              let advanced = false;
              for (const character of text) {
                if (character === "\n" || character === "\r") lineKind = "start";
                else {
                  if (lineKind === "start") lineKind = character === ":" ? "comment" : "event";
                  if (lineKind === "event") advanced = true;
                }
              }
              if (advanced) progress();
              for (const frame of parser.push(text)) {
                if ("comment" in frame) {
                  if (observationReady) {
                    observationReady = false;
                    if (progressTimer !== undefined) clearTimeout(progressTimer);
                    attempt.complete();
                  }
                  continue;
                }
                const data = JSON.parse(frame.data);
                if (frame.event === "stream_error") {
                  const error = (data as Api.ErrorResponse).error;
                  throw new ApiError(error.code, 503, retryable(503, error.code));
                }
                if (frame.event !== "snapshot") continue;
                const snapshot = data as Snapshot;
                const key = `${snapshot.topic}:${snapshot.key ?? ""}`;
                if (snapshot.first) started.add(key);
                if (
                  !started.has(key) &&
                  !(initialComplete && snapshot.topic === "observation_changes")
                )
                  throw new ContractError("snapshot.first", "the first page of a snapshot");
                receive(snapshot);
                if (
                  observationReady &&
                  snapshot.topic === "observation_changes" &&
                  !snapshot.first
                ) {
                  observationReady = false;
                  if (progressTimer !== undefined) clearTimeout(progressTimer);
                  attempt.complete();
                }
                if (!initialComplete && snapshot.last && started.has(key)) {
                  remaining?.delete(key);
                  if (!remaining || remaining.size === 0) {
                    initialComplete = true;
                    observationReady = snapshot.topic === "observation_changes" && snapshot.first;
                    if (!observationReady) {
                      if (progressTimer !== undefined) clearTimeout(progressTimer);
                      attempt.complete();
                    }
                  }
                }
                if (snapshot.last) started.delete(key);
                delay = 250;
              }
            }
          } finally {
            await reader.cancel().catch(() => {});
            reader.releaseLock();
          }
          throw new ApiError("connection_lost", 0, true);
        } catch (error) {
          if (signal.aborted) return { keepGoing: false, wait: 0 };
          if (attempt.rotated()) {
            delay = 250;
            return { keepGoing: true, wait: 100 };
          }
          if (stalled) error = new ApiError("stream_idle", 0, true);
          onError(error);
          if (error instanceof ContractError || (error instanceof ApiError && !error.retryable))
            return { keepGoing: false, wait: 0 };
          const wait = delay;
          delay = Math.min(delay * 2, 10000);
          return { keepGoing: true, wait };
        } finally {
          if (progressTimer !== undefined) clearTimeout(progressTimer);
          attempt.dispose();
        }
      };
      while (!signal.aborted) {
        const release = programmatic ? () => {} : await snapshotSlot(signal);
        let outcome: { keepGoing: boolean; wait: number };
        try {
          signal.throwIfAborted();
          const previouslyUnavailable = locksUnavailable;
          const locks = browserLocks();
          if (!previouslyUnavailable && locksUnavailable) onLocksUnavailable();
          if (locks) {
            let entered = false;
            try {
              outcome = await withSnapshotLock(locks, signal, (name) => {
                entered = true;
                return runAttempt(name);
              });
            } catch (error) {
              if (signal.aborted || entered) throw error;
              locksUnavailable = true;
              onLocksUnavailable();
              outcome = await runAttempt();
            }
          } else outcome = await runAttempt();
        } finally {
          release();
        }
        if (!outcome.keepGoing) break;
        await pause(outcome.wait, signal);
      }
    })()
      .catch((error) => {
        if (!signal.aborted) onError(error);
      })
      .finally(() => active.delete(controller));
    return () => controller.abort();
  }

  return {
    subscribeSnapshots,
    browserLocks,
    subscribeJobEvents(
      jobId: string,
      listener: (event: JobStreamEvent) => void | Promise<void>,
      settings: {
        signal?: AbortSignal;
        afterSeq?: number;
        onReconnect?: (error: ApiError) => void | Promise<void>;
      } = {},
    ): JobStreamSubscription {
      if (closed) throw new ApiError("client_closed", 0, false);
      jobIdentity(jobId);
      const cursor = {
        sequence: settings.afterSeq === undefined ? undefined : sequence(settings.afterSeq),
      };
      const controller = new AbortController();
      const signal = controller.signal;
      const stop = () => controller.abort();
      settings.signal?.addEventListener("abort", stop, { once: true });
      if (settings.signal?.aborted) stop();
      active.add(controller);
      const completed = (async (): Promise<JobStreamEnd> => {
        let delay = 250;
        try {
          while (!signal.aborted) {
            let failure = new ApiError("premature_end", 0, true);
            const attempt = streamAttempt(signal, false);
            try {
              const end = await connection(jobId, attempt.signal, listener, cursor);
              if (end) return end;
            } catch (error) {
              if (signal.aborted) return "aborted";
              if (attempt.rotated()) {
                delay = 250;
                await pause(100, signal);
                continue;
              }
              if (!(error instanceof ApiError) || !error.retryable) throw error;
              failure = error;
            } finally {
              attempt.dispose();
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
          active.delete(controller);
        }
      })();
      return { closed: completed, close: stop };
    },
    close() {
      closed = true;
      for (const controller of active.values()) controller.abort();
    },
  };
}
