"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { TEXT_TONE } from "@/components/ocg/primitives";
import type { ControlToolCall, ControlToolOutcomeKind } from "@/components/ocg/contracts";
import type { TranscriptTurn } from "./job-transcript-model";
import { useI18n, type I18nKey, type TranslateFn } from "../i18n";
import { ToolActivityLabel } from "./tool-activity-label";
import { formatToolJson, jobPlanTitles } from "./tool-activity-summary";
import { MarkdownBody } from "../markdown/markdown-body";
import {
  ReasoningRoot,
  ReasoningTrigger,
  ReasoningContent,
  estimateReasoningTokens,
} from "@/components/assistant-ui/elements/reasoning";

const STATUS_NOTE: Partial<Record<TranscriptTurn["status"], I18nKey>> = {
  streaming: "transcript.status.streaming",
  pending: "transcript.status.pending",
  failed: "transcript.status.failed",
  cancelled: "transcript.status.cancelled",
  unknown: "transcript.status.unknown",
};

function TurnRow({
  turn,
  userLabel,
  attachmentContentUrl,
}: {
  turn: TranscriptTurn;
  userLabel: string;
  attachmentContentUrl?: (id: string) => string;
}) {
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const { t } = useI18n();
  const isUser = turn.role === "user";
  const note = isUser ? undefined : STATUS_NOTE[turn.status];
  const reasoning =
    !isUser && turn.reasoning ? (
      <ReasoningRoot defaultOpen={false}>
        <ReasoningTrigger
          active={turn.status === "streaming" || turn.status === "pending"}
          duration={
            turn.reasoningDurationMs === undefined ? undefined : turn.reasoningDurationMs / 1000
          }
          tokens={estimateReasoningTokens(turn.reasoning)}
        />
        <ReasoningContent>
          <p className="whitespace-pre-wrap break-words">{turn.reasoning}</p>
        </ReasoningContent>
      </ReasoningRoot>
    ) : null;
  const toolCalls =
    !isUser && turn.tools?.length ? (
      <TurnToolCalls
        tools={turn.tools}
        isLive={turn.status === "streaming" || turn.status === "pending"}
      />
    ) : null;
  if (!isUser && !turn.content && !turn.detail && !turn.attachments?.length) {
    if (reasoning || toolCalls)
      return (
        <>
          {reasoning}
          {toolCalls}
        </>
      );
    if (turn.intermediate && turn.status === "completed") return null;
  }
  return (
    <div className="flex gap-2.5">
      <span
        className={cn(
          "flex size-6 shrink-0 items-center justify-center rounded-md border border-border text-[10px] font-semibold",
          isUser ? "bg-muted text-muted-foreground" : "bg-primary text-primary-foreground",
        )}
        aria-hidden="true"
      >
        {isUser ? "U" : "A"}
      </span>
      <div className="min-w-0 flex-1">
        <div className="mb-1 flex items-center gap-2">
          <span className="text-xs font-semibold">
            {isUser ? userLabel : t("transcript.answer")}
          </span>
          {turn.createdAtUs === null ? null : (
            <span className="text-xs text-muted-foreground">
              {new Date(turn.createdAtUs / 1000).toLocaleTimeString()}
            </span>
          )}
          <button
            type="button"
            onClick={() => copyToClipboard(turn.content)}
            disabled={turn.content.length === 0}
            className="ml-auto inline-flex shrink-0 items-center gap-1 rounded px-1.5 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring disabled:cursor-not-allowed disabled:opacity-40"
            aria-label={isUser ? t("transcript.copyMessage") : t("transcript.copyAnswer")}
          >
            <span role="status">{isCopied ? t("common.copied") : t("common.copy")}</span>
          </button>
        </div>
        <div
          className={cn(
            "text-base leading-7 text-foreground/90",
            isUser && "rounded-md border border-border bg-muted/30 px-3 py-2",
            turn.status === "failed" && TEXT_TONE.red,
            turn.status === "cancelled" && "text-muted-foreground italic",
          )}
        >
          {turn.attachments && turn.attachments.length > 0 ? (
            <div className="mb-3 flex flex-wrap gap-2">
              {turn.attachments.map((id) => {
                const url = attachmentContentUrl
                  ? attachmentContentUrl(id)
                  : `/api/v1/attachments/${encodeURIComponent(id)}/content`;
                return (
                  <a
                    key={id}
                    href={url}
                    target="_blank"
                    rel="noreferrer"
                    className="group relative block overflow-hidden rounded-md border border-border bg-background transition-shadow hover:shadow-md"
                  >
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img
                      src={url}
                      alt={id}
                      className="max-h-48 max-w-xs rounded object-contain"
                      loading="lazy"
                    />
                    <span className="absolute bottom-1 right-1 rounded bg-background/80 px-1 py-0.5 font-mono text-[10px] text-muted-foreground">
                      {id}
                    </span>
                  </a>
                );
              })}
            </div>
          ) : null}
          {reasoning}
          {isUser ? (
            <p className="whitespace-pre-wrap break-words">{turn.content}</p>
          ) : (
            <MarkdownBody content={turn.content} className="break-words" />
          )}
          {turn.detail ? (
            <DismissibleNotice
              message={turn.detail}
              className="mt-2 whitespace-pre-wrap break-words text-xs"
            >
              {turn.detail}
            </DismissibleNotice>
          ) : null}
          {note ? (
            <span className="mt-1 block text-xs text-muted-foreground">{t(note)}</span>
          ) : null}
        </div>
        {toolCalls}
      </div>
    </div>
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
  kind: ControlToolOutcomeKind | null | undefined,
  decision: ControlToolCall["decision"],
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
  kind: ControlToolOutcomeKind | null | undefined,
  decision: ControlToolCall["decision"],
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
    case "not_applied":
      return t("transcript.outcome.notApplied");
    case "not_invoked":
      return t("transcript.outcome.notInvoked");
    case "uncertain":
      return t("transcript.outcome.uncertain");
  }
}

function TurnToolCalls({ tools, isLive }: { tools: readonly ControlToolCall[]; isLive: boolean }) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  return (
    <details
      onToggle={(event) => {
        if (event.target === event.currentTarget) setOpen(event.currentTarget.open);
      }}
      className="group mt-2.5 rounded-md border border-border bg-muted/20 text-xs"
    >
      <summary className="flex cursor-pointer list-none items-center gap-2 px-3 py-2 font-medium text-muted-foreground hover:text-foreground [&::-webkit-details-marker]:hidden">
        <ChevronRight
          aria-hidden="true"
          className="size-3.5 shrink-0 transition-transform group-open:rotate-90"
        />
        <ToolActivityLabel
          tools={tools.map((tool) => ({
            name: tool.name,
            arguments: tool.arguments,
            result: tool.result,
            durationMs: tool.duration_ms,
            truncated: tool.arguments_cut || tool.result_cut,
          }))}
        />
      </summary>
      <div className="flex flex-col gap-2 p-3 pt-0">
        {open &&
          tools.map((tool) => {
            const summary = argumentSummary(tool.name, tool.arguments);
            const outcome = outcomeLabel(tool.outcome_kind, tool.decision, isLive);
            const isCompleted = outcome === "completed";
            const isRunning = outcome === "running";
            const isUncertain =
              outcome === "uncertain" ||
              outcome === "awaiting_approval" ||
              outcome === "awaiting_answer";
            return (
              <details
                key={`${tool.request}:${tool.index}`}
                className="group rounded border border-border bg-card p-2 text-card-foreground"
              >
                <summary className="flex cursor-pointer select-none flex-wrap items-center justify-between gap-2 text-xs">
                  <div className="flex min-w-0 items-center gap-2 font-mono">
                    <span className="font-semibold text-foreground">{tool.name}</span>
                    {summary ? (
                      <span className="truncate text-[11px] text-muted-foreground" title={summary}>
                        {summary}
                      </span>
                    ) : null}
                  </div>
                  <span
                    className={cn(
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
                    {outcomeText(t, tool.outcome_kind, tool.decision, isLive)}
                  </span>
                </summary>
                <div className="mt-2 space-y-2 border-t border-border pt-2 font-mono text-xs">
                  {tool.decision !== "admitted" ? (
                    <div className="font-sans text-xs text-destructive">
                      {t("transcript.decision", { decision: tool.decision })}
                    </div>
                  ) : null}
                  <div>
                    <div className="mb-1 flex items-center justify-between font-sans text-[11px] text-muted-foreground">
                      <span>{t("transcript.arguments")}</span>
                      {tool.arguments_cut ? (
                        <span className="text-[10px] font-semibold text-amber-500">
                          {t("transcript.cut")}
                        </span>
                      ) : null}
                    </div>
                    <pre className="ocg-subtle-scrollbar max-h-48 overflow-auto overscroll-contain whitespace-pre-wrap break-all rounded bg-muted p-2 text-[11px]">
                      {tool.arguments === null ? "—" : formatToolJson(tool.arguments)}
                    </pre>
                  </div>
                  <div>
                    <div className="mb-1 flex items-center justify-between font-sans text-[11px] text-muted-foreground">
                      <span>{t("transcript.result")}</span>
                      {tool.result_cut ? (
                        <span className="text-[10px] font-semibold text-amber-500">
                          {t("transcript.cut")}
                        </span>
                      ) : null}
                    </div>
                    <pre className="ocg-subtle-scrollbar max-h-48 overflow-auto overscroll-contain whitespace-pre-wrap break-all rounded bg-muted p-2 text-[11px]">
                      {tool.result === null ? "—" : formatToolJson(tool.result)}
                    </pre>
                  </div>
                </div>
              </details>
            );
          })}
      </div>
    </details>
  );
}

/** One Job as its objective and answer; see `jobTranscript` for what each is made of. */
export function JobTranscript({
  turns,
  userLabel = "Objective",
  attachmentContentUrl,
}: {
  turns: readonly TranscriptTurn[];
  userLabel?: string;
  attachmentContentUrl?: (id: string) => string;
}) {
  return (
    <div className="flex flex-col gap-4" aria-live="polite">
      {turns.map((turn) => (
        <TurnRow
          key={turn.id}
          turn={turn}
          userLabel={userLabel}
          attachmentContentUrl={attachmentContentUrl}
        />
      ))}
    </div>
  );
}
