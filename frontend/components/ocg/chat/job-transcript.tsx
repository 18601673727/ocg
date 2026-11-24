import { ChevronRight } from "@lucide/solid";
import { createMemo, createSignal } from "solid-js";

import {
  estimateReasoningTokens,
  ReasoningContent,
  ReasoningRoot,
  ReasoningTrigger,
} from "@/components/ocg/chat/ui/reasoning";
import type { ToolCall, ToolOutcomeKind } from "@/components/ocg/contracts";
import { TEXT_TONE } from "@/components/ocg/primitives";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { cn } from "@/lib/utils";
import { type I18nKey, type TranslateFn, useI18n } from "../i18n";
import { MarkdownBody } from "../markdown/markdown-body";
import type { ApiClient } from "../runtime/api-client";
import type { TranscriptTurn } from "./job-transcript-model";
import { ToolActivityLabel } from "./tool-activity-label";
import { formatToolJson, jobPlanTitles } from "./tool-activity-summary";
import { WorkspaceTool } from "./workspace-tool";

const STATUS_NOTE: Partial<Record<TranscriptTurn["status"], I18nKey>> = {
  streaming: "transcript.status.streaming",
  pending: "transcript.status.pending",
  failed: "transcript.status.failed",
  cancelled: "transcript.status.cancelled",
  unknown: "transcript.status.unknown",
};

function TurnRow(props: {
  jobId?: string;
  client?: ApiClient;
  turn: TranscriptTurn;
  userLabel: string;
  attachmentContentUrl?: (id: string) => string;
}) {
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const i18n = useI18n();
  const isUser = createMemo(() => props.turn.role === "user");
  const note = createMemo(() => (isUser() ? undefined : STATUS_NOTE[props.turn.status]));
  const reasoning = createMemo(() =>
    !isUser() && props.turn.reasoning ? (
      <ReasoningRoot defaultOpen={false}>
        <ReasoningTrigger
          active={props.turn.status === "streaming" || props.turn.status === "pending"}
          duration={
            props.turn.reasoningDurationMs === undefined
              ? undefined
              : props.turn.reasoningDurationMs / 1000
          }
          tokens={estimateReasoningTokens(props.turn.reasoning)}
        />
        <ReasoningContent>
          <p class="whitespace-pre-wrap break-words">{props.turn.reasoning}</p>
        </ReasoningContent>
      </ReasoningRoot>
    ) : null,
  );
  const toolCalls = createMemo(() =>
    !isUser() && props.turn.tools?.length ? (
      <TurnToolCalls
        tools={props.turn.tools}
        jobId={props.jobId}
        client={props.client}
        isLive={props.turn.status === "streaming" || props.turn.status === "pending"}
      />
    ) : null,
  );
  return (
    <>
      {(() => {
        if (
          !isUser() &&
          !props.turn.content &&
          !props.turn.detail &&
          !props.turn.attachments?.length
        ) {
          if (reasoning() || toolCalls())
            return (
              <>
                {reasoning()}
                {toolCalls()}
              </>
            );
          if (props.turn.intermediate && props.turn.status === "completed") return null;
        }
        return (
          <div class="flex gap-2.5">
            <span
              class={cn(
                "flex size-6 shrink-0 items-center justify-center rounded-md border border-border text-[10px] font-semibold",
                isUser() ? "bg-muted text-muted-foreground" : "bg-primary text-primary-foreground",
              )}
              aria-hidden="true"
            >
              {isUser() ? "U" : "A"}
            </span>
            <div class="min-w-0 flex-1">
              <div class="mb-1 flex items-center gap-2">
                <span class="text-xs font-semibold">
                  {isUser() ? props.userLabel : i18n.t("transcript.answer")}
                </span>
                {props.turn.createdAtUs === null ? null : (
                  <span class="text-xs text-muted-foreground">
                    {new Date(props.turn.createdAtUs / 1000).toLocaleTimeString()}
                  </span>
                )}
                <button
                  type="button"
                  onClick={() => copyToClipboard(props.turn.content)}
                  disabled={props.turn.content.length === 0}
                  class="ml-auto inline-flex shrink-0 items-center gap-1 rounded px-1.5 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring disabled:cursor-not-allowed disabled:opacity-40"
                  aria-label={
                    isUser() ? i18n.t("transcript.copyMessage") : i18n.t("transcript.copyAnswer")
                  }
                >
                  <span role="status">
                    {isCopied() ? i18n.t("common.copied") : i18n.t("common.copy")}
                  </span>
                </button>
              </div>
              <div
                class={cn(
                  "text-base leading-7 text-foreground/90",
                  isUser() && "rounded-md border border-border bg-muted/30 px-3 py-2",
                  props.turn.status === "failed" && TEXT_TONE.red,
                  props.turn.status === "cancelled" && "text-muted-foreground italic",
                )}
              >
                {props.turn.attachments && props.turn.attachments.length > 0 ? (
                  <div class="mb-3 flex flex-wrap gap-2">
                    {props.turn.attachments.map((id) => {
                      const url = props.attachmentContentUrl
                        ? props.attachmentContentUrl(id)
                        : `/api/v1/attachments/${encodeURIComponent(id)}/content`;
                      return (
                        <a
                          href={url}
                          target="_blank"
                          rel="noreferrer"
                          class="group relative block overflow-hidden rounded-md border border-border bg-background transition-shadow hover:shadow-md"
                        >
                          <img
                            src={url}
                            alt={id}
                            class="max-h-48 max-w-xs rounded object-contain"
                            loading="lazy"
                          />
                          <span class="absolute bottom-1 right-1 rounded bg-background/80 px-1 py-0.5 font-mono text-[10px] text-muted-foreground">
                            {id}
                          </span>
                        </a>
                      );
                    })}
                  </div>
                ) : null}
                {reasoning()}
                {isUser() ? (
                  <p class="whitespace-pre-wrap break-words">{props.turn.content}</p>
                ) : (
                  <MarkdownBody content={props.turn.content} class="break-words" />
                )}
                {props.turn.detail ? (
                  <DismissibleNotice
                    message={props.turn.detail}
                    class="mt-2 whitespace-pre-wrap break-words text-xs"
                  >
                    {props.turn.detail}
                  </DismissibleNotice>
                ) : null}
                {note() ? (
                  <span class="mt-1 block text-xs text-muted-foreground">{i18n.t(note()!)}</span>
                ) : null}
              </div>
              {toolCalls()}
            </div>
          </div>
        );
      })()}
    </>
  );
}

function argumentSummary(name: string, raw: string | null): string {
  if (!raw) return "";
  try {
    const parsed = JSON.parse(raw);
    if (name === "job_plan") return jobPlanTitles(parsed);
    if (typeof parsed === "object" && parsed !== null) {
      if (typeof parsed.path === "string") return parsed.path;
      if (typeof parsed.preset === "string") return parsed.preset;
      if (typeof parsed.command === "string") return parsed.command;
      if (typeof parsed.query === "string") return parsed.query;
      if (typeof parsed.pattern === "string") return parsed.pattern;
      for (const val of Object.values(parsed)) {
        if (typeof val === "string") return val.length > 60 ? `${val.slice(0, 57)}…` : val;
      }
    }
  } catch {
    const line = raw.trim().split("\n")[0] ?? "";
    return line.length > 60 ? `${line.slice(0, 57)}…` : line;
  }
  return "";
}

function outcomeLabel(
  kind: ToolOutcomeKind | null | undefined,
  decision: ToolCall["decision"],
  isLive: boolean,
): string {
  if (!kind && (decision === "awaiting_approval" || decision === "awaiting_answer"))
    return decision;
  if (
    !kind &&
    decision !== "admitted" &&
    decision !== "awaiting_approval" &&
    decision !== "awaiting_answer"
  )
    return "refused";
  if (!kind) return isLive ? "running" : "uncertain";
  return kind;
}

function outcomeText(
  t: TranslateFn,
  kind: ToolOutcomeKind | null | undefined,
  decision: ToolCall["decision"],
  isLive: boolean,
): string {
  if (!kind && decision === "awaiting_approval") return t("transcript.outcome.awaitingApproval");
  if (!kind && decision === "awaiting_answer") return t("transcript.outcome.awaitingAnswer");
  if (outcomeLabel(kind, decision, isLive) === "refused") return t("transcript.outcome.refused");
  if (!kind) return isLive ? t("transcript.outcome.running") : t("transcript.outcome.uncertain");
  switch (kind) {
    case "completed":
      return t("state.completed");
    case "completed_withheld":
      return t("transcript.outcome.completedWithheld");
    case "completed_elided":
      return t("state.completed");
    case "not_applied":
      return t("transcript.outcome.notApplied");
    case "not_invoked":
      return t("transcript.outcome.notInvoked");
    case "uncertain":
      return t("transcript.outcome.uncertain");
  }
}

function TurnToolCalls(props: {
  tools: readonly ToolCall[];
  isLive: boolean;
  jobId?: string;
  client?: ApiClient;
}) {
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  const [expanded, setExpanded] = createSignal<Record<string, boolean>>({});
  return (
    <details
      onToggle={(event) => {
        if (event.target === event.currentTarget) setOpen(event.currentTarget.open);
      }}
      class="group mt-2.5 rounded-md border border-border bg-muted/20 text-xs"
    >
      <summary class="flex cursor-pointer list-none items-center gap-2 px-3 py-2 font-medium text-muted-foreground hover:text-foreground [&::-webkit-details-marker]:hidden">
        <ChevronRight
          aria-hidden="true"
          class="size-3.5 shrink-0 transition-transform group-open:rotate-90"
        />
        <ToolActivityLabel
          tools={props.tools.map((tool) => ({
            name: tool.name,
            arguments: tool.arguments,
            result: tool.result,
            durationMs: tool.duration_ms,
            truncated: tool.arguments_cut || tool.result_cut,
          }))}
        />
      </summary>
      <div class="flex flex-col gap-2 p-3 pt-0">
        {open() &&
          props.tools.map((tool) => {
            const key = `${tool.request}:${tool.index}`;
            const summary = argumentSummary(tool.name, tool.arguments);
            const outcome = outcomeLabel(tool.outcome_kind, tool.decision, props.isLive);
            const isCompleted = outcome === "completed";
            const isRunning = outcome === "running";
            const isUncertain =
              outcome === "uncertain" ||
              outcome === "awaiting_approval" ||
              outcome === "awaiting_answer";
            return (
              <details
                onToggle={(event) => {
                  if (event.target !== event.currentTarget) return;
                  const value = event.currentTarget.open;
                  setExpanded((previous) => ({ ...previous, [key]: value }));
                }}
                class="group rounded border border-border bg-card p-2 text-card-foreground"
              >
                <summary class="flex cursor-pointer select-none flex-wrap items-center justify-between gap-2 text-xs">
                  <div class="flex min-w-0 items-center gap-2 font-mono">
                    <span class="font-semibold text-foreground">{tool.name}</span>
                    {summary ? (
                      <span class="truncate text-[11px] text-muted-foreground" title={summary}>
                        {summary}
                      </span>
                    ) : null}
                  </div>
                  <span
                    class={cn(
                      "rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider",
                      isCompleted &&
                        "border border-emerald-500/20 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
                      isRunning &&
                        "border border-sky-500/20 bg-sky-500/10 text-sky-600 dark:text-sky-400",
                      isUncertain &&
                        "border border-amber-500/20 bg-amber-500/10 text-amber-600 dark:text-amber-400",
                      !isCompleted &&
                        !isRunning &&
                        !isUncertain &&
                        "border border-border bg-muted text-muted-foreground",
                    )}
                  >
                    {outcomeText(i18n.t, tool.outcome_kind, tool.decision, props.isLive)}
                  </span>
                </summary>
                {expanded()[key] && (
                  <div class="mt-2 space-y-2 border-t border-border pt-2 font-mono text-xs">
                    {tool.decision !== "admitted" ? (
                      <div class="font-sans text-xs text-destructive">
                        {i18n.t("transcript.decision", { decision: tool.decision })}
                      </div>
                    ) : null}
                    <WorkspaceTool call={tool} jobId={props.jobId} client={props.client}>
                      <div>
                        <div class="mb-1 flex items-center justify-between font-sans text-[11px] text-muted-foreground">
                          <span>{i18n.t("transcript.arguments")}</span>
                          {tool.arguments_cut ? (
                            <span class="text-[10px] font-semibold text-amber-500">
                              {i18n.t("transcript.cut")}
                            </span>
                          ) : null}
                        </div>
                        <pre class="ocg-subtle-scrollbar max-h-48 overflow-auto overscroll-contain whitespace-pre-wrap break-all rounded bg-muted p-2 text-[11px]">
                          {tool.arguments === null ? "—" : formatToolJson(tool.arguments)}
                        </pre>
                      </div>
                      {tool.outcome_kind !== "completed_elided" && (
                        <div>
                          <div class="mb-1 flex items-center justify-between font-sans text-[11px] text-muted-foreground">
                            <span>{i18n.t("transcript.result")}</span>
                            {tool.result_cut ? (
                              <span class="text-[10px] font-semibold text-amber-500">
                                {i18n.t("transcript.cut")}
                              </span>
                            ) : null}
                          </div>
                          <pre class="ocg-subtle-scrollbar max-h-48 overflow-auto overscroll-contain whitespace-pre-wrap break-all rounded bg-muted p-2 text-[11px]">
                            {tool.result === null ? "—" : formatToolJson(tool.result)}
                          </pre>
                        </div>
                      )}
                    </WorkspaceTool>
                  </div>
                )}
              </details>
            );
          })}
      </div>
    </details>
  );
}

export function JobTranscript(props: {
  turns: readonly TranscriptTurn[];
  jobId?: string;
  client?: ApiClient;
  userLabel?: string;
  attachmentContentUrl?: (id: string) => string;
}) {
  return (
    <div class="flex flex-col gap-4" aria-live="polite">
      {props.turns.map((turn) => (
        <TurnRow
          turn={turn}
          jobId={props.jobId}
          client={props.client}
          userLabel={props.userLabel === undefined ? "Objective" : props.userLabel}
          attachmentContentUrl={props.attachmentContentUrl}
        />
      ))}
    </div>
  );
}
