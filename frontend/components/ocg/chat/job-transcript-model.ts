import type {
  ControlOutput,
  ControlProviderResponse,
  ControlToolCall,
} from "@/components/ocg/contracts";

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
    if (!start.startsWith(prefix))
      return state === "unsettled" && prefix.startsWith(start) ? "" : text;
    const end = start.indexOf("</ocg-title>", prefix.length);
    if (end < 0)
      return state === "unsettled" && new TextEncoder().encode(start).length <= 1024 ? "" : text;
    const title = start.slice(prefix.length, end).trim();
    if (
      !title ||
      [...title].length > 80 ||
      new TextEncoder().encode(title).length > 256 ||
      /[\p{Cc}<>]/u.test(title)
    )
      return text;
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
    const endedAt =
      (state !== "unsettled" ? settlement?.settled_at_us : null) ??
      outputs.at(-1)?.created_at_us ??
      reasoningStartedAt;
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
    answer = {
      ...base,
      content: visibleAnswer(approvalAnswer(result?.summary ?? progress, tools ?? [])),
      status: "completed",
      detail: null,
    };
  } else if (state === "unsettled") {
    answer = {
      ...base,
      content: visibleAnswer(progress),
      status: streaming ? "streaming" : "pending",
      detail: null,
    };
  } else {
    answer = {
      ...base,
      content: visibleAnswer(progress),
      status: state,
      detail: providerFailureDetail(result),
    };
  }
  return [...user, ...providerMessages(answer, progress, responses, visibleAnswer, outputs)];
}

function providerFailureDetail(result: ControlOutput | null): string | null {
  const message = result?.summary ?? null;
  if (
    result?.code !== "provider_rejected" ||
    !result.value ||
    typeof result.value !== "object" ||
    Array.isArray(result.value)
  )
    return message;
  const value = result.value as Record<string, unknown>;
  const details = [
    typeof value.provider_code === "string" ? `Provider code: ${value.provider_code}` : null,
    typeof value.request_id === "string" ? `Request ID: ${value.request_id}` : null,
  ].filter((detail): detail is string => Boolean(detail));
  return [message, ...details].filter(Boolean).join("\n") || null;
}

function approvalAnswer(text: string, tools: readonly ControlToolCall[]): string {
  let answer = text;
  const pending = tools
    .filter((call) => call.decision === "awaiting_approval" || call.decision === "awaiting_answer")
    .sort((a, b) => b.request - a.request || b.index - a.index);
  for (const call of pending) {
    let marker: string;
    let payload: string;
    if (call.decision === "awaiting_answer") {
      try {
        const args: unknown = JSON.parse(call.arguments ?? "{}");
        if (
          !args ||
          typeof args !== "object" ||
          !("question" in args) ||
          typeof args.question !== "string"
        )
          continue;
        marker = "Awaiting answer: ";
        payload = args.question;
      } catch {
        continue;
      }
    } else {
      marker = `Awaiting approval: ${call.name} `;
      payload = call.arguments ?? "";
    }
    const start = answer.lastIndexOf(marker);
    if (start < 0 || (start > 0 && answer[start - 1] !== "\n")) continue;
    const suffix = answer.slice(start + marker.length);
    if (call.arguments_cut ? !payload || !suffix.startsWith(payload) : suffix !== payload) continue;
    answer = answer.slice(0, start > 0 ? start - 1 : 0);
  }
  return answer;
}

function providerMessages(
  answer: TranscriptTurn,
  progress: string,
  responses: readonly ControlProviderResponse[] | undefined,
  visible: (text: string) => string,
  outputs: readonly ControlOutput[],
): TranscriptTurn[] {
  if (!responses?.length) return fallbackMessages(answer, progress, visible);
  const recorded = responses.map((response) => response.text).join("");
  if (!progress.startsWith(recorded) && !recorded.startsWith(progress)) {
    return fallbackMessages(answer, progress, visible);
  }
  const remainder = progress.startsWith(recorded) ? progress.slice(recorded.length) : "";
  const tools = answer.tools ?? [];
  const messages: TranscriptTurn[] = [];
  let outputIndex = 0;
  const reasoningBefore = (through: number, id: string, live = false) => {
    let text = "";
    let started: number | null = null;
    let duration = 0;
    let at: number | null = null;
    while (outputIndex < outputs.length && outputs[outputIndex].created_at_us <= through) {
      const output = outputs[outputIndex++];
      if (output.kind === "reasoning" && output.summary) {
        text += output.summary;
        started ??= output.created_at_us;
        at ??= output.created_at_us;
      } else if (started !== null) {
        duration += Math.max(0, output.created_at_us - started);
        started = null;
      }
    }
    if (started !== null)
      duration += Math.max(
        0,
        Math.min(through, outputs.at(-1)?.created_at_us ?? started) - started,
      );
    if (text)
      messages.push({
        ...answer,
        id,
        content: "",
        reasoning: text,
        reasoningDurationMs: duration / 1000,
        createdAtUs: at,
        tools: undefined,
        status: live ? answer.status : "completed",
        detail: null,
        intermediate: true,
      });
  };
  const byRequest = new Map(responses.map((response) => [response.request, response]));
  const toolsByRequest = new Map<number, ControlToolCall[]>();
  for (const call of tools) {
    const group = toolsByRequest.get(call.request) ?? [];
    group.push(call);
    toolsByRequest.set(call.request, group);
  }
  const requests = [...new Set([...byRequest.keys(), ...toolsByRequest.keys()])].sort(
    (a, b) => a - b,
  );
  let final = false;
  for (const request of requests) {
    const response = byRequest.get(request);
    // Received is recorded after this response's streamed outputs are flushed.
    // Consume the stored sequence once, leaving the unfinished request at the tail.
    if (response)
      reasoningBefore(
        answer.status === "completed" && response === responses.at(-1)
          ? Number.POSITIVE_INFINITY
          : response.created_at_us,
        `${answer.id}:reasoning:${request}`,
      );
    const calls = toolsByRequest.get(request) ?? [];
    const isFinal =
      response === responses.at(-1) &&
      !response?.requests_tools &&
      answer.status === "completed" &&
      !remainder;
    final ||= isFinal;
    const active = answer.status === "streaming" || answer.status === "pending";
    const waiting =
      active &&
      calls.some(
        (call) =>
          !call.outcome_kind &&
          (call.decision === "awaiting_approval" || call.decision === "awaiting_answer"),
      );
    const running =
      active && calls.some((call) => !call.outcome_kind && call.decision === "admitted");
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
    reasoningBefore(
      Number.POSITIVE_INFINITY,
      `${answer.id}:reasoning:live`,
      answer.status === "streaming" || answer.status === "pending",
    );
    let content = visible(remainder);
    if (answer.status === "completed") {
      const last = visible(responses.at(-1)?.text ?? "");
      const suffix = answer.content.slice(last.length);
      const awaiting =
        responses.at(-1)?.requests_tools &&
        !remainder &&
        last &&
        answer.content.startsWith(last) &&
        /^\nAwaiting (?:approval|answer):/.test(suffix);
      const cardOnly =
        tools.some(
          (call) => call.decision === "awaiting_approval" || call.decision === "awaiting_answer",
        ) &&
        !remainder &&
        answer.content === last;
      content = cardOnly ? "" : awaiting ? suffix.trimStart() : answer.content;
    }
    messages.push({
      ...answer,
      content,
      reasoning: undefined,
      reasoningDurationMs: undefined,
      tools: undefined,
    });
  }
  return groupConsecutiveTools(messages);
}

function groupConsecutiveTools(messages: TranscriptTurn[]): TranscriptTurn[] {
  const grouped: TranscriptTurn[] = [];
  for (const message of messages) {
    if (
      message.intermediate &&
      !message.content.trim() &&
      !message.reasoning &&
      !message.tools?.length &&
      !message.detail
    )
      continue;
    const previous = grouped.at(-1);
    if (
      message.intermediate &&
      !message.content.trim() &&
      !message.reasoning &&
      !message.detail &&
      message.tools?.length &&
      previous?.tools?.length &&
      previous.role === "assistant"
    ) {
      previous.tools = [...previous.tools, ...message.tools];
      if (message.status === "streaming" || message.status === "pending")
        previous.status = message.status;
    } else grouped.push({ ...message });
  }
  return grouped;
}

function fallbackMessages(
  answer: TranscriptTurn,
  progress: string,
  visible: (text: string) => string,
): TranscriptTurn[] {
  if (answer.status !== "completed") return [answer];
  const text = visible(progress);
  const earlier =
    answer.content && text.endsWith(answer.content) ? text.slice(0, -answer.content.length) : text;
  if (!earlier || text === answer.content) return [answer];
  return [
    {
      ...answer,
      id: `${answer.id}:progress`,
      content: earlier,
      status: "completed",
      detail: null,
      intermediate: true,
    },
    { ...answer, reasoning: undefined, tools: undefined },
  ];
}
