import type { ControlOutput } from "@/components/ocg/contracts";

/** How a Job settled, as the Control Plane reports it. */
export type TranscriptSettlement = {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
};

export type TranscriptTurn = {
  id: string;
  content: string;
  status: "completed" | "streaming" | "pending" | "failed" | "cancelled" | "unknown";
  /** The Result output's own text where the Job did not complete. */
  detail: string | null;
  createdAtUs: number | null;
};

/**
 * Reads one Job's stored outputs as the Provider's answer: its Progress
 * outputs in `seq` order, and the Job's settlement alone deciding whether the
 * answer is complete, failed, cancelled or of unknown outcome. The Control
 * Plane reports a Job's input only as an identity (`job_input:<n>`), never as
 * text, so no objective turn is drawn: a Job is one answer, and there is no
 * Conversation entity.
 */
export function jobTranscript(
  jobId: string,
  outputs: readonly ControlOutput[],
  settlement: TranscriptSettlement | null,
  streaming: boolean,
): TranscriptTurn[] {
  const progress = outputs
    .filter((output) => output.kind === "progress")
    .map((output) => output.summary ?? "")
    .join("");
  const result = outputs.findLast((output) => output.kind === "result") ?? null;
  const state = settlement?.state ?? "unsettled";
  const base = {
    id: `${jobId}:answer`,
    createdAtUs: outputs.at(-1)?.created_at_us ?? null,
  };
  if (state === "completed") {
    // The Result carries the Provider's complete text; it supersedes the
    // streamed pieces it was assembled from.
    return [{ ...base, content: result?.summary ?? progress, status: "completed", detail: null }];
  }
  if (state === "unsettled") {
    return [{ ...base, content: progress, status: streaming ? "streaming" : "pending", detail: null }];
  }
  return [{ ...base, content: progress, status: state, detail: result?.summary ?? null }];
}
