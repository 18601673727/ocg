type PublicationWindow = {
  dirty: boolean;
  frame?: number;
};

export class PublishScheduler {
  private readonly windows = new Map<string, PublicationWindow>();
  private disposed = false;

  constructor(private readonly publishSnapshot: (id: string) => void) {}

  schedule(sessionId: string) {
    if (this.disposed) return;
    const active = this.windows.get(sessionId);
    if (active) {
      active.dirty = true;
      return;
    }
    const window: PublicationWindow = { dirty: false };
    this.windows.set(sessionId, window);
    const nextFrame = () => {
      if (this.windows.get(sessionId) !== window) return;
      this.windows.delete(sessionId);
      if (window.dirty) this.schedule(sessionId);
    };
    // Arm this session's next frame before publishing to synchronous listeners.
    if (typeof requestAnimationFrame === "function") {
      window.frame = requestAnimationFrame(nextFrame);
    } else {
      queueMicrotask(nextFrame);
    }
    this.publishSnapshot(sessionId);
  }

  cancel(sessionId: string): boolean {
    const active = this.windows.get(sessionId);
    if (!active) return false;
    this.windows.delete(sessionId);
    if (active.frame !== undefined) cancelAnimationFrame(active.frame);
    return active.dirty;
  }

  publish(sessionId: string) {
    this.cancel(sessionId);
    if (!this.disposed) this.publishSnapshot(sessionId);
  }

  flushPending(sessionId: string) {
    if (this.cancel(sessionId)) this.publishSnapshot(sessionId);
  }

  dispose() {
    this.disposed = true;
    for (const id of this.windows.keys()) this.cancel(id);
  }
}
