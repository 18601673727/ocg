import type * as Api from "./api";
import type { ControlTreeNode } from "./control-shared";
import type { ControlDecisionAnswer } from "./control-settings";

export type NativeDecisionTurn = {
  kind: "decision";
  request: {
    schema_version: number;
    state: unknown;
    question: {
      kind: "choice" | "binary_probability" | "ordinal_score";
      prompt: string;
      options?: { id: string; description: string }[];
      levels?: { value: number; description: string }[];
    };
  };
  result: null | {
    outcome:
      | { status: "answer"; answer: ControlDecisionAnswer }
      | { status: "uncertain" | "unusable"; reason: string };
  };
  failure: string | null;
};

function nativeDecision(value: unknown): NativeDecisionTurn | undefined {
  if (value === undefined) return undefined;
  const object = (item: unknown): item is Record<string, unknown> =>
    item !== null && typeof item === "object" && !Array.isArray(item);
  if (
    !object(value) ||
    value.kind !== "decision" ||
    !object(value.request) ||
    value.request.schema_version !== 1 ||
    !object(value.request.question)
  )
    throw new Error("Invalid Decision history record.");
  if (value.failure !== null && typeof value.failure !== "string")
    throw new Error("Invalid Decision failure.");
  const question = value.request.question;
  if (
    !["choice", "binary_probability", "ordinal_score"].includes(String(question.kind)) ||
    typeof question.prompt !== "string"
  )
    throw new Error("Unsupported Decision question.");
  if (
    question.kind === "choice" &&
    (!Array.isArray(question.options) ||
      !question.options.every(
        (option: unknown) =>
          object(option) && typeof option.id === "string" && typeof option.description === "string",
      ))
  )
    throw new Error("Invalid Decision options.");
  if (
    question.kind === "ordinal_score" &&
    (!Array.isArray(question.levels) ||
      !question.levels.every(
        (level: unknown) =>
          object(level) && typeof level.value === "number" && typeof level.description === "string",
      ))
  )
    throw new Error("Invalid Decision levels.");
  if (value.result !== null) {
    if (!object(value.result) || !object(value.result.outcome))
      throw new Error("Invalid Decision result.");
    const outcome = value.result.outcome;
    if (outcome.status === "answer") {
      if (!object(outcome.answer) || outcome.answer.kind !== question.kind)
        throw new Error("Decision answer does not match its question.");
    } else if (
      !(
        ["uncertain", "unusable"].includes(String(outcome.status)) &&
        typeof outcome.reason === "string"
      )
    )
      throw new Error("Unsupported Decision outcome.");
  }
  return value as NativeDecisionTurn;
}

export type ProjectedEntry = {
  seq: number;
  at_us: number;
  kind: Api.TurnEntry["kind"];
  message: number | null;
  text: string | null;
  attachments: string[];
};
export type ControlConversationTurn = Omit<
  Api.Turn,
  "entries" | "configuration" | "uncertain_calls" | "native_decision"
> & {
  native_decision?: NativeDecisionTurn;
  entries: ProjectedEntry[];
  input: Api.Input & { job: string };
};
export type ControlConversation = Omit<Api.Turns, "turns"> & { turns: ControlConversationTurn[] };
export type ControlChildConversation = Omit<
  Api.Child,
  "state" | "origin" | "job_state" | "recurrence"
> & {
  state: "open" | "closing" | "settled";
  origin?: "user" | "system" | "agent";
  job_state:
    | "failed"
    | "unknown"
    | "cancelled"
    | "running"
    | "completed"
    | "queued"
    | "waiting_for_user"
    | "waiting";
  recurrence:
    | (Omit<Api.Recurrence, "phase"> & { phase: "active" | "waiting_due" | "halted" })
    | null;
};
export type ControlChildren = Omit<Api.Children, "children"> & {
  children: ControlChildConversation[];
};
export type ControlConversationCard = Api.Card;
export type ControlJobTree = Omit<Api.Tree, "root"> & { root: ControlTreeNode };

export function materializeUserEntries(entries: readonly ProjectedEntry[]) {
  const visible = new Map<
    number,
    ProjectedEntry & { withdrawn: boolean; history: { content: string; at: string }[] }
  >();
  for (const entry of entries) {
    if (entry.kind === "user_message" || entry.kind === "agent_message") {
      visible.set(entry.seq, { ...entry, withdrawn: false, history: [] });
    } else if (entry.kind === "user_message_edit" || entry.kind === "user_message_withdraw") {
      const original = entry.message === null ? undefined : visible.get(entry.message);
      if (!original) continue;
      if (entry.kind === "user_message_edit") {
        original.history.push({
          content: original.text ?? "",
          at: new Date(entry.at_us / 1000).toISOString(),
        });
        original.text = entry.text;
        original.attachments = entry.attachments;
      } else {
        original.withdrawn = true;
      }
    }
  }
  return [...visible.values()];
}

export function projectConversation(page: Api.Turns): ControlConversation {
  return {
    ...page,
    turns: page.turns.map((turn) => {
      const decision = nativeDecision(turn.native_decision);
      if (
        (turn.content_kind === "decision" && !decision) ||
        (turn.content_kind === "generative" && decision)
      )
        throw new Error("Inconsistent Conversation content kind.");
      const entries = turn.entries.map((entry) => ({
        seq: entry.seq,
        kind: entry.kind,
        at_us: entry.at_us,
        message: "message" in entry ? entry.message : null,
        text: "text" in entry ? entry.text : null,
        attachments: "attachments" in entry ? entry.attachments : [],
      }));
      const messages = materializeUserEntries(entries).filter((entry) => !entry.withdrawn);
      return {
        job: turn.job,
        native_decision: decision,
        content_kind: turn.content_kind,
        entries,
        compaction: turn.compaction,
        input: {
          job: turn.job.id,
          id: turn.job.input,
          objective: messages.length ? messages.map((entry) => entry.text ?? "").join("\n") : null,
          submitted_at_us: turn.submitted_at_us ?? turn.job.created_at_us,
          attachments: messages.flatMap((entry) => entry.attachments),
          configuration: turn.configuration,
        },
        outputs: turn.outputs,
        outputs_truncated: turn.outputs_truncated,
      };
    }),
  };
}
