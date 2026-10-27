import type { ControlOutput } from "../contracts";

/**
 * Resident output window policy for JobWatch.
 *
 * A long-output Job keeps its durable history on the server; the view
 * retains only the latest resident window in state and derives the
 * transcript from that window. Earlier outputs stay reachable through an
 * explicit "load earlier" walk from the first stored sequence, merged by
 * `seq` so neither gaps nor duplicates can appear. The processed stream
 * cursor (`processed` in the watcher) is maintained independently of the
 * visible rows: it only ever advances to the highest stored sequence, so
 * reconnects and replays resume exactly where they left off no matter how
 * small the resident window is.
 */

/** Automatically retained outputs in state; older rows need an explicit load. */
export const JOB_WATCH_RESIDENT_OUTPUTS = 200;

/** How many more outputs each explicit "load earlier" step retains. */
export const JOB_WATCH_WINDOW_STEP = 200;

/**
 * Merge stored outputs by `seq` without gaps or duplicates, retaining the
 * latest `window` rows. Both inputs may overlap arbitrarily: live appends
 * race history walks, and a re-walk rereads rows the view already holds.
 */
export function mergeOutputWindow(
  current: readonly ControlOutput[],
  incoming: readonly ControlOutput[],
  window: number,
): ControlOutput[] {
  const bySeq = new Map<number, ControlOutput>();
  for (const output of current) bySeq.set(output.seq, output);
  for (const output of incoming) bySeq.set(output.seq, output);
  const merged = [...bySeq.values()].sort((left, right) => left.seq - right.seq);
  return merged.slice(Math.max(0, merged.length - window));
}

/** Whether stored outputs exist beyond the resident rows. */
export function hasEvictedOutputs(total: number, resident: number): boolean {
  return total > resident;
}
