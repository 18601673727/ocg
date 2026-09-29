/**
 * The ordering rules both projection stores share.
 *
 * The event reconciler and the canonical store apply the same contract: a
 * generation never moves backwards, an unknown generation needs a snapshot, a
 * duplicate event identity is a no-op, a sequence at or below the cursor is
 * stale, and a sequence beyond `cursor + 1` is a gap that forces a resync.
 * Keeping the decision here is what stops the two stores drifting apart; each
 * store still decides what a decision *means* for its own state.
 */

/** Diagnostics and seen-id rings are bounded so a bad stream cannot grow state. */
export const MAX_DIAGNOSTICS = 50;
export const MAX_SEEN_IDS = 500;

export function boundDiagnostics<D>(diagnostics: readonly D[]): D[] {
  return diagnostics.length <= MAX_DIAGNOSTICS
    ? (diagnostics as D[])
    : diagnostics.slice(diagnostics.length - MAX_DIAGNOSTICS);
}

/** Append one id to the bounded seen ring, keeping the most recent entries. */
export function rememberId(seen: readonly string[], id?: string): string[] {
  const combined = id !== undefined ? [...seen, id] : [...seen];
  return combined.length <= MAX_SEEN_IDS ? combined : combined.slice(combined.length - MAX_SEEN_IDS);
}

export function isSeen(seen: readonly string[], id: string): boolean {
  return seen.includes(id);
}

export type GenerationDecision = "stale" | "current" | "unknown";

/** `stale` is a late replay, `unknown` is a reconnect that needs a baseline. */
export function compareGeneration(incoming: number, current: number): GenerationDecision {
  if (incoming < current) return "stale";
  if (incoming > current) return "unknown";
  return "current";
}

export type SequenceDecision = "stale" | "next" | "gap";

/** Classify one sequence against the applied cursor. */
export function compareSequence(sequence: number, cursor: number): SequenceDecision {
  if (sequence <= cursor) return "stale";
  if (sequence > cursor + 1) return "gap";
  return "next";
}
