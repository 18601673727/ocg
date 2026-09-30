/**
 * The ordering rules both projection stores share.
 *
 * The two stores see two different streams, so they classify a sequence
 * differently:
 *
 * - the event reconciler consumes an unfiltered stream, where a sequence beyond
 *   `cursor + 1` really is a gap that forces a resync;
 * - the canonical store consumes a stream already filtered to one Job, where
 *   the global journal sequence numbers skip everything belonging to other
 *   Jobs. A jump beyond `cursor + 1` is then the normal shape of the stream,
 *   not a gap, so [`compareFilteredSequence`] distinguishes only stale from
 *   next.
 *
 * Generation ordering and duplicate detection are identical for both, and
 * keeping every decision here is what stops the two stores drifting apart; each
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

export type FilteredSequenceDecision = "stale" | "next";

/**
 * Classify one sequence from a stream filtered to a single entity.
 *
 * A Job-filtered tail carries only the events of one Job, so the global journal
 * sequence numbers it returns skip whatever belongs to other Jobs. A jump
 * beyond `cursor + 1` is therefore the normal shape of the stream, not a gap:
 * anything at or below the cursor is a late event, and anything above it is
 * simply the next retained event for this Job.
 */
export function compareFilteredSequence(sequence: number, cursor: number): FilteredSequenceDecision {
  return sequence <= cursor ? "stale" : "next";
}
