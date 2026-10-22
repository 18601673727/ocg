import type { ControlOutput, ControlProviderResponse, ControlToolCall } from "@/components/ocg/contracts";

/** How a Job settled, as the Control Plane reports it. */
export type TranscriptSettlement = {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
  settled_at_us?: number | null;
};

export type TranscriptTurn = {
  id: string;
  role: "user" | "assistant";
  content: string;
  /** The model's stored reasoning, shown apart from the answer. */
  reasoning?: string;
  reasoningDurationMs?: number;
  intermediate?: boolean;
  attachments?: readonly string[];
  status: "completed" | "streaming" | "pending" | "failed" | "cancelled" | "unknown";
  /** The Result output's own text where the Job did not complete. */
  detail: string | null;
  createdAtUs: number | null;
  tools?: readonly ControlToolCall[];
};

/**
 * Projects one Job's frozen input and stored outputs into chat messages.
 * Recorded provider replies preserve each request's text and tool calls;
 * progress beyond those replies remains a live tail. Reasoning is separate
 * because its stored chunks carry no request number. Missing reply evidence
 * falls back to the raw output text. Only the Job's settlement decides its
 * final status; an intermediate tool reply is never a completed Job.
 */
export function jobTranscript(
  jobId: string,
  objective: string | null,
  outputs: readonly ControlOutput[],
  settlement: TranscriptSettlement | null,
  streaming: boolean,
  attachments?: readonly string[],
  tools?: readonly ControlToolCall[],
  titleMetadata = false,
  responses?: readonly ControlProviderResponse[],
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
  const visibleAnswer = (text: string) => {
    if (!titleMetadata) return text;
    const start = text.trimStart();
    const prefix = "<ocg-title>";
    if (!start.startsWith(prefix)) return state === "unsettled" && prefix.startsWith(start) ? "" : text;
    const end = start.indexOf("</ocg-title>", prefix.length);
    if (end < 0) return state === "unsettled" && new TextEncoder().encode(start).length <= 1024 ? "" : text;
    const title = start.slice(prefix.length, end).trim();
    if (!title || [...title].length > 80 || new TextEncoder().encode(title).length > 256 || /[\p{Cc}<>]/u.test(title)) return text;
    return start.slice(end + "</ocg-title>".length).trimStart();
  };
  let reasoningStartedAt: number | null = null;
  let reasoningDurationUs = 0;
  for (const output of outputs) {
    if (output.kind === "reasoning" && output.summary) {
      reasoningStartedAt ??= output.created_at_us;
    } else if (output.kind !== "reasoning" && reasoningStartedAt !== null) {
      reasoningDurationUs += Math.max(0, output.created_at_us - reasoningStartedAt);
      reasoningStartedAt = null;
    }
  }
  if (reasoningStartedAt !== null) {
    const endedAt = (state !== "unsettled" ? settlement?.settled_at_us : null) ?? outputs.at(-1)?.created_at_us ?? reasoningStartedAt;
    reasoningDurationUs += Math.max(0, endedAt - reasoningStartedAt);
  }
  const base = {
    id: `${jobId}:answer`,
    role: "assistant" as const,
    createdAtUs: outputs.at(-1)?.created_at_us ?? null,
    tools,
    ...(reasoning ? { reasoning, reasoningDurationMs: reasoningDurationUs / 1000 } : {}),
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
  let answer: TranscriptTurn;
  if (state === "completed") {
    // The Result carries the Provider's complete text; it supersedes the
    // streamed pieces it was assembled from.
    answer = { ...base, content: visibleAnswer(result?.summary ?? progress), status: "completed", detail: null };
  } else if (state === "unsettled") {
    answer = { ...base, content: visibleAnswer(progress), status: streaming ? "streaming" : "pending", detail: null };
  } else {
    answer = { ...base, content: visibleAnswer(progress), status: state, detail: result?.summary ?? null };
  }
  return [...user, ...providerMessages(answer, progress, responses, visibleAnswer)];
}

function providerMessages(
  answer: TranscriptTurn,
  progress: string,
  responses: readonly ControlProviderResponse[] | undefined,
  visible: (text: string) => string,
): TranscriptTurn[] {
  if (!responses?.length) return fallbackMessages(answer, progress, visible);
  const recorded = responses.map(response => response.text).join("");
  if (!progress.startsWith(recorded) && !recorded.startsWith(progress)) {
    return fallbackMessages(answer, progress, visible);
  }
  const remainder = progress.startsWith(recorded) ? progress.slice(recorded.length) : "";
  const tools = answer.tools ?? [];
  const messages: TranscriptTurn[] = [];
  if (answer.reasoning) messages.push({
    ...answer,
    id: `${answer.id}:reasoning`,
    content: "",
    tools: undefined,
    status: answer.status === "streaming" || answer.status === "pending" ? answer.status : "completed",
    detail: null,
    intermediate: true,
  });
  const byRequest = new Map(responses.map(response => [response.request, response]));
  const toolsByRequest = new Map<number, ControlToolCall[]>();
  for (const call of tools) {
    const group = toolsByRequest.get(call.request) ?? [];
    group.push(call);
    toolsByRequest.set(call.request, group);
  }
  const requests = [...new Set([...byRequest.keys(), ...toolsByRequest.keys()])].sort((a, b) => a - b);
  let final = false;
  for (const request of requests) {
    const response = byRequest.get(request);
    const calls = toolsByRequest.get(request) ?? [];
    const isFinal = response === responses.at(-1) && !response?.requests_tools && answer.status === "completed" && !remainder;
    final ||= isFinal;
    const active = answer.status === "streaming" || answer.status === "pending";
    const waiting = active && calls.some(call => !call.outcome_kind && (call.decision === "awaiting_approval" || call.decision === "awaiting_answer"));
    const running = active && calls.some(call => !call.outcome_kind && call.decision === "admitted");
    messages.push({
      ...answer,
      id: isFinal ? answer.id : `${answer.id}:request:${request}`,
      content: response ? visible(response.text) : "",
      reasoning: undefined,
      reasoningDurationMs: undefined,
      createdAtUs: response?.created_at_us ?? answer.createdAtUs,
      tools: calls,
      status: waiting ? "pending" : running ? "streaming" : "completed",
      detail: null,
      intermediate: !isFinal,
    });
  }
  if (!final) {
    let content = visible(remainder);
    if (answer.status === "completed") {
      const last = visible(responses.at(-1)?.text ?? "");
      const suffix = answer.content.slice(last.length);
      const awaiting = responses.at(-1)?.requests_tools && !remainder && last
        && answer.content.startsWith(last) && /^\nAwaiting (?:approval|answer):/.test(suffix);
      content = awaiting ? suffix.trimStart() : answer.content;
    }
    messages.push({ ...answer, content, reasoning: undefined, reasoningDurationMs: undefined, tools: undefined });
  }
  return groupConsecutiveTools(messages);
}

function groupConsecutiveTools(messages: TranscriptTurn[]): TranscriptTurn[] {
  const grouped: TranscriptTurn[] = [];
  for (const message of messages) {
    if (message.intermediate && !message.content.trim() && !message.reasoning && !message.tools?.length && !message.detail) continue;
    const previous = grouped.at(-1);
    if (message.intermediate && !message.content.trim() && !message.reasoning && !message.detail
      && message.tools?.length && previous?.tools?.length && previous.role === "assistant") {
      previous.tools = [...previous.tools, ...message.tools];
      if (message.status === "streaming" || message.status === "pending") previous.status = message.status;
    } else grouped.push({ ...message });
  }
  return grouped;
}

function fallbackMessages(answer: TranscriptTurn, progress: string, visible: (text: string) => string): TranscriptTurn[] {
  if (answer.status !== "completed") return [answer];
  const text = visible(progress);
  const earlier = answer.content && text.endsWith(answer.content)
    ? text.slice(0, -answer.content.length) : text;
  if (!earlier || text === answer.content) return [answer];
  return [
    { ...answer, id: `${answer.id}:progress`, content: earlier, status: "completed", detail: null, intermediate: true },
    { ...answer, reasoning: undefined, tools: undefined },
  ];
}
