import type { ControlOutput } from "../contracts";

export const JOB_WATCH_RESIDENT_OUTPUTS = 200;

export const JOB_WATCH_WINDOW_STEP = 200;

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

export function hasEvictedOutputs(total: number, resident: number): boolean {
  return total > resident;
}
