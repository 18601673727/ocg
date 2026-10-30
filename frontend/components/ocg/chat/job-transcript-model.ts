import type { ControlProviderResponse, ControlToolCall } from "@/components/ocg/contracts";
import type { OutputSequence, OutputView } from "../runtime/output-history";

/** How a Job settled, as the Control Plane reports it. */
export type TranscriptSettlement = {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
  settled_at_us?: number | null;
};

export type PartialToolBoundary = { request: number; at: number };

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

export function outputText(output: OutputView | null | undefined): string | null {
  const value = output?.value;
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return "text" in value && typeof value.text === "string" ? value.text : null;
}

export function outputDisplayText(output: OutputView | null | undefined): string {
  return [outputText(output), output?.summary]
    .filter((text) => text != null && text !== "")
    .join("\n");
}

// Rebuildable presentation state. The stored outputs remain the authority for
// sequence and request boundaries; this only avoids rescanning their text.
export class TranscriptAccumulator {
  progress = "";
  reasoning = "";
  result: OutputView | null = null;
  lastAt: number | null = null;
  reasoningStartedAt: number | null = null;
  reasoningDurationUs = 0;
  provider?: ProviderProjection;

  append(output: OutputView) {
    if (output.kind === "progress") this.progress += outputDisplayText(output);
    if (output.kind === "reasoning") this.reasoning += outputDisplayText(output);
    if (output.kind === "result") this.result = output;
    if (output.kind === "reasoning" && outputText(output)) {
      this.reasoningStartedAt ??= output.created_at_us;
    } else if (output.kind !== "reasoning" && this.reasoningStartedAt !== null) {
      this.reasoningDurationUs += Math.max(0, output.created_at_us - this.reasoningStartedAt);
      this.reasoningStartedAt = null;
    }
    this.lastAt = output.created_at_us;
    this.provider?.append(output);
  }
}

type ReasoningInterval = {
  text: string;
  started: number | null;
  duration: number;
  at: number | null;
};

class ProviderProjection {
  readonly recorded: string;
  readonly byRequest: Map<number, ControlProviderResponse>;
  readonly toolsByRequest = new Map<number, ControlToolCall[]>();
  readonly requests: number[];
  readonly intervals: ReasoningInterval[];
  readonly through: number[];
  readonly requestMessages = new Map<number, TranscriptTurn>();
  private index = 0;
  private compared = 0;
  private compatible = true;

  constructor(
    readonly responses: readonly ControlProviderResponse[],
    readonly tools: readonly ControlToolCall[] | undefined,
    readonly completed: boolean,
    readonly answerId: string,
    readonly titleMetadata: boolean,
    public titleUnsettled: boolean,
    outputs: OutputSequence,
  ) {
    this.recorded = responses.map((response) => response.text).join("");
    this.byRequest = new Map(responses.map((response) => [response.request, response]));
    for (const call of tools ?? []) {
      const group = this.toolsByRequest.get(call.request) ?? [];
      group.push(call);
      this.toolsByRequest.set(call.request, group);
    }
    this.requests = [...new Set([...this.byRequest.keys(), ...this.toolsByRequest.keys()])].sort(
      (a, b) => a - b,
    );
    this.through = this.requests.flatMap((request) => {
      const response = this.byRequest.get(request);
      return response
        ? [completed && response === responses.at(-1) ? Infinity : response.created_at_us]
        : [];
    });
    this.through.push(Infinity);
    this.intervals = this.through.map(() => ({ text: "", started: null, duration: 0, at: null }));
    for (const output of outputs) this.append(output);
  }

  append(output: OutputView) {
    while (this.index < this.through.length - 1 && output.created_at_us > this.through[this.index])
      this.index++;
    const interval = this.intervals[this.index];
    const text = outputText(output);
    if (output.kind === "reasoning" && text) {
      interval.text += text;
      interval.started ??= output.created_at_us;
      interval.at ??= output.created_at_us;
    } else if (interval.started !== null) {
      interval.duration += Math.max(0, output.created_at_us - interval.started);
      interval.started = null;
    }
  }

  matches(progress: string): boolean {
    const end = Math.min(progress.length, this.recorded.length);
    if (this.compatible && end > this.compared) {
      this.compatible =
        progress.slice(this.compared, end) === this.recorded.slice(this.compared, end);
      this.compared = end;
    }
    return this.compatible;
  }
}

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
  outputs: OutputSequence,
  settlement: TranscriptSettlement | null,
  streaming: boolean,
  attachments?: readonly string[],
  tools?: readonly ControlToolCall[],
  titleMetadata = false,
  responses?: readonly ControlProviderResponse[],
  partialOutput = false,
  partialToolBoundaries?: readonly PartialToolBoundary[] | null,
  accumulated?: TranscriptAccumulator,
): TranscriptTurn[] {
  const aggregate = accumulated ?? new TranscriptAccumulator();
  if (!accumulated) for (const output of outputs) aggregate.append(output);
  const { progress, reasoning, result } = aggregate;
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
  const { reasoningStartedAt } = aggregate;
  let reasoningDurationUs = aggregate.reasoningDurationUs;
  if (reasoningStartedAt !== null) {
    const endedAt =
      (state !== "unsettled" ? settlement?.settled_at_us : null) ??
      aggregate.lastAt ??
      reasoningStartedAt;
    reasoningDurationUs += Math.max(0, endedAt - reasoningStartedAt);
  }
  const base = {
    id: `${jobId}:answer`,
    role: "assistant" as const,
    createdAtUs: aggregate.lastAt,
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
  const strippedPending = new Set<ControlToolCall>();
  if (state === "completed") {
    // The Result carries the Provider's complete text; it supersedes the
    // streamed pieces it was assembled from.
    answer = {
      ...base,
      content: visibleAnswer(
        approvalAnswer(result ? outputDisplayText(result) : progress, tools ?? [], strippedPending),
      ),
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
  return [
    ...user,
    ...providerMessages(
      answer,
      progress,
      responses,
      visibleAnswer,
      outputs,
      partialOutput,
      partialToolBoundaries,
      strippedPending,
      aggregate,
      titleMetadata,
    ),
  ];
}

function providerFailureDetail(result: OutputView | null): string | null {
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

function approvalAnswer(
  text: string,
  tools: readonly ControlToolCall[],
  strippedPending: Set<ControlToolCall>,
): string {
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
    strippedPending.add(call);
  }
  return answer;
}

function providerMessages(
  answer: TranscriptTurn,
  progress: string,
  responses: readonly ControlProviderResponse[] | undefined,
  visible: (text: string) => string,
  outputs: OutputSequence,
  partialOutput: boolean,
  partialToolBoundaries: readonly PartialToolBoundary[] | null | undefined,
  strippedPending: ReadonlySet<ControlToolCall>,
  aggregate: TranscriptAccumulator,
  titleMetadata: boolean,
): TranscriptTurn[] {
  if (!responses?.length) {
    aggregate.provider = undefined;
    return fallbackMessages(
      answer,
      progress,
      visible,
      outputs,
      partialOutput,
      partialToolBoundaries,
      strippedPending,
    );
  }
  let projection = aggregate.provider;
  if (
    !projection ||
    projection.responses !== responses ||
    projection.tools !== answer.tools ||
    projection.completed !== (answer.status === "completed") ||
    projection.answerId !== answer.id ||
    projection.titleMetadata !== titleMetadata
  ) {
    projection = new ProviderProjection(
      responses,
      answer.tools,
      answer.status === "completed",
      answer.id,
      titleMetadata,
      answer.status === "streaming" || answer.status === "pending",
      outputs,
    );
    aggregate.provider = projection;
  }
  const titleUnsettled = answer.status === "streaming" || answer.status === "pending";
  if (projection.titleMetadata && projection.titleUnsettled !== titleUnsettled) {
    projection.requestMessages.clear();
    projection.titleUnsettled = titleUnsettled;
  }
  const { recorded, byRequest, toolsByRequest, requests } = projection;
  if (!projection.matches(progress)) {
    return fallbackMessages(
      answer,
      progress,
      visible,
      outputs,
      partialOutput,
      partialToolBoundaries,
      strippedPending,
    );
  }
  const remainder = progress.length >= recorded.length ? progress.slice(recorded.length) : "";
  const tools = answer.tools ?? [];
  const messages: TranscriptTurn[] = [];
  let intervalIndex = 0;
  const reasoningBefore = (through: number, id: string, live = false) => {
    const { text, started, at } = projection.intervals[intervalIndex++];
    let { duration } = projection.intervals[intervalIndex - 1];
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
    const reusable = response && !isFinal && !waiting && !running;
    const cachedMessage = reusable ? projection.requestMessages.get(request) : undefined;
    const message: TranscriptTurn = cachedMessage ?? {
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
    };
    if (reusable && !cachedMessage) projection.requestMessages.set(request, message);
    messages.push(message);
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
  outputs: OutputSequence,
  partialOutput: boolean,
  partialToolBoundaries: readonly PartialToolBoundary[] | null | undefined,
  strippedPending: ReadonlySet<ControlToolCall>,
): TranscriptTurn[] {
  if (answer.status !== "completed") return [answer];
  const text = visible(progress);
  if (partialOutput && !answer.tools?.length && answer.content.endsWith(text)) return [answer];
  if (partialOutput && partialToolBoundaries?.length)
    return partialMessages(answer, outputs, visible, partialToolBoundaries, strippedPending);
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

function partialMessages(
  answer: TranscriptTurn,
  outputs: OutputSequence,
  visible: (text: string) => string,
  boundaries: readonly PartialToolBoundary[],
  strippedPending: ReadonlySet<ControlToolCall>,
): TranscriptTurn[] {
  const tools = new Map<number, ControlToolCall[]>();
  for (const call of answer.tools ?? []) {
    const group = tools.get(call.request) ?? [];
    group.push(call);
    tools.set(call.request, group);
  }
  const times = new Map(boundaries.map(({ request, at }) => [request, at]));
  const messages: TranscriptTurn[] = [];
  let position = 0;
  const take = (through: number) => {
    const start = position;
    while (
      position < outputs.length &&
      (outputs.at(position)?.created_at_us ?? Infinity) <= through
    )
      position++;
    const part: OutputView[] = [];
    for (let index = start; index < position; index++) {
      const output = outputs.at(index);
      if (output) part.push(output);
    }
    const text = part
      .filter((output) => output.kind === "progress")
      .map(outputDisplayText)
      .join("");
    const reasoning = part
      .filter((output) => output.kind === "reasoning")
      .map(outputDisplayText)
      .join("");
    return { text, reasoning, createdAtUs: part[0]?.created_at_us ?? null };
  };
  for (const request of [...tools.keys()].sort((a, b) => a - b)) {
    const at = times.get(request);
    // Requests older than the first boundary precede the retained output window.
    const part = at === undefined ? { text: "", reasoning: "", createdAtUs: null } : take(at);
    messages.push({
      ...answer,
      id: `${answer.id}:request:${request}`,
      content: visible(part.text),
      reasoning: part.reasoning || undefined,
      reasoningDurationMs: undefined,
      createdAtUs: part.createdAtUs,
      tools: tools.get(request),
      status: "completed",
      intermediate: true,
      detail: null,
    });
  }
  const tail = take(Number.POSITIVE_INFINITY);
  const streamed = visible(tail.text);
  const separateTail = Boolean(streamed && !answer.content.endsWith(streamed));
  if (separateTail) {
    messages.push({
      ...answer,
      id: `${answer.id}:progress`,
      content: streamed,
      reasoning: tail.reasoning || undefined,
      reasoningDurationMs: undefined,
      tools: undefined,
      intermediate: true,
      detail: null,
    });
  }
  const lastToolMessage = messages.at(-1);
  const cardOnly =
    !streamed &&
    !!answer.content &&
    lastToolMessage?.content === answer.content &&
    lastToolMessage.tools?.some((call) => strippedPending.has(call));
  if (!cardOnly || tail.reasoning) {
    messages.push({
      ...answer,
      ...(cardOnly ? { id: `${answer.id}:reasoning:tail`, content: "", intermediate: true } : {}),
      reasoning: separateTail ? undefined : tail.reasoning || undefined,
      reasoningDurationMs: undefined,
      tools: undefined,
    });
  }
  return messages;
}
