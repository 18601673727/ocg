import type { ControlOutput, ControlToolCall } from "@/components/ocg/contracts";

/** How a Job settled, as the Control Plane reports it. */
export type TranscriptSettlement = {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
};

export type TranscriptTurn = {
  id: string;
  role: "user" | "assistant";
  content: string;
  /** The model's stored reasoning, shown apart from the answer. */
  reasoning?: string;
  attachments?: readonly string[];
  status: "completed" | "streaming" | "pending" | "failed" | "cancelled" | "unknown";
  /** The Result output's own text where the Job did not complete. */
  detail: string | null;
  createdAtUs: number | null;
  tools?: readonly ControlToolCall[];
};

/**
 * Reads one Job as a user turn and an answer. The user turn is the objective
 * text of the Job's frozen input, read from `GET /jobs/{job}/input`; it is
 * drawn only when that text was actually read, never inferred from the Job
 * record's input identity. The answer is the Job's stored Progress outputs in
 * `seq` order, the Job's stored Reasoning outputs are carried separately so a
 * view can show them as thinking, and the Job's settlement alone decides
 * whether it is complete, failed, cancelled or of unknown outcome. A Job is
 * one turn: there is no Conversation entity to read earlier turns from.
 */
export function jobTranscript(
  jobId: string,
  objective: string | null,
  outputs: readonly ControlOutput[],
  settlement: TranscriptSettlement | null,
  streaming: boolean,
  attachments?: readonly string[],
  tools?: readonly ControlToolCall[],
): TranscriptTurn[] {
  const answerOutputs = outputs.filter((output) => output.kind !== "reasoning");
  const progress = answerOutputs
    .filter((output) => output.kind === "progress")
    .map((output) => output.summary ?? "")
    .join("");
  const reasoning = outputs
    .filter((output) => output.kind === "reasoning")
    .map((output) => output.summary ?? "")
    .join("");
  const result = answerOutputs.findLast((output) => output.kind === "result") ?? null;
  const state = settlement?.state ?? "unsettled";
  const base = {
    id: `${jobId}:answer`,
    role: "assistant" as const,
    createdAtUs: outputs.at(-1)?.created_at_us ?? null,
    tools,
    ...(reasoning ? { reasoning } : {}),
  };
  const hasUserTurn = objective !== null || (attachments && attachments.length > 0);
  const user: TranscriptTurn[] = !hasUserTurn
    ? []
    : [
        {
          id: `${jobId}:objective`,
          role: "user",
          content: objective ?? "",
          attachments: attachments && attachments.length > 0 ? attachments : undefined,
          status: "completed",
          detail: null,
          createdAtUs: null,
        },
      ];
  if (state === "completed") {
    // The Result carries the Provider's complete text; it supersedes the
    // streamed pieces it was assembled from.
    return [...user, { ...base, content: result?.summary ?? progress, status: "completed", detail: null }];
  }
  if (state === "unsettled") {
    return [...user, { ...base, content: progress, status: streaming ? "streaming" : "pending", detail: null }];
  }
  return [...user, { ...base, content: progress, status: state, detail: result?.summary ?? null }];
}
