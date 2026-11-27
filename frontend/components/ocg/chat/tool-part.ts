import type { Component } from "solid-js";
import type { ToolCall } from "../contracts";
import type { TranslateFn } from "../i18n";

export type ToolCallMessagePartStatus =
  | { type: "running" | "complete" | "requires-action" }
  | { type: "incomplete"; reason: "error" | "cancelled"; error?: string };

export type ToolCallPart = {
  toolCallId: string;
  toolName: string;
  argsText: string | null;
  result?: unknown;
  status: ToolCallMessagePartStatus;
  durationMs?: number;
  truncated?: boolean;
  call?: ToolCall;
  jobId?: string;
};
export type ToolCallMessagePartComponent = Component<ToolCallPart>;

export function presentToolCall(
  call: ToolCall,
  jobId: string | undefined,
  t: TranslateFn,
): ToolCallPart {
  const refused =
    !call.outcome_kind &&
    call.decision !== "admitted" &&
    call.decision !== "awaiting_approval" &&
    call.decision !== "awaiting_answer";
  const failed =
    Boolean(call.child_notice_drop) ||
    refused ||
    call.outcome_kind === "not_applied" ||
    call.outcome_kind === "not_invoked" ||
    call.outcome_kind === "uncertain";
  const result =
    call.child_notice_drop ??
    (refused
      ? `${t("transcript.outcome.refused")}: ${call.decision}`
      : call.outcome_kind && call.outcome_kind !== "completed_elided"
        ? (call.result ?? call.outcome_kind)
        : undefined);
  let status: ToolCallMessagePartStatus = { type: "running" };
  if (call.child_help_status === "answered") {
    status = { type: "complete" };
  } else if (failed) {
    status = { type: "incomplete", reason: "error" };
  } else if (call.child_help_status === "dismissed") {
    status = { type: "incomplete", reason: "cancelled" };
  } else if (call.outcome_kind) {
    status = { type: "complete" };
  } else if (call.decision === "awaiting_approval" || call.decision === "awaiting_answer") {
    status = { type: "requires-action" };
  }
  return {
    toolCallId: call.tool_call_id,
    toolName: call.name,
    argsText: call.arguments,
    result,
    status,
    durationMs: call.duration_ms,
    truncated: call.arguments_cut || call.result_cut,
    call,
    jobId,
  };
}
