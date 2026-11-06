"use client";

import { type ReactNode } from "react";
import { Check, Copy, Terminal } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import type { ControlToolCall } from "../contracts";
import { useI18n } from "../i18n";
import { highlightCode } from "../markdown/markdown-pipeline";
import { toolObject } from "./artifact-model";

function commandArgument(value: string): string {
  if (/^[a-zA-Z0-9_@%+=:,./-]+$/.test(value)) return value;
  return "'" + value.replace(/'/g, "'\"'\"'") + "'";
}

function commandLine(value: unknown): string | null {
  if (!Array.isArray(value) || !value.length || !value.every((item) => typeof item === "string")) {
    return null;
  }
  return value.map(commandArgument).join(" ");
}

function jsonOutput(text: string): string | null {
  try {
    const value: unknown = JSON.parse(text);
    return value !== null && typeof value === "object" ? JSON.stringify(value, null, 2) : null;
  } catch {
    return null;
  }
}

function formattedOutput(text: string): string | null {
  const json = jsonOutput(text);
  if (json !== null) return json;
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return null;
  const records = lines.map(jsonOutput);
  return records.every((record) => record !== null) ? records.join("\n\n") : null;
}

function CommandOutput({ title, text }: { title: string; text: string }) {
  const { t } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const formatted = formattedOutput(text);
  const highlighted = formatted === null ? null : highlightCode(formatted, "json");
  return (
    <section className="min-w-0 border-t border-border" data-slot="command-output">
      <header className="flex items-center justify-between gap-2 bg-muted/20 px-3 py-1.5 text-xs text-muted-foreground">
        <h3 className="font-medium">{title}</h3>
        {text && (
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={t("chat.command.copyOutput", { stream: title })}
            onClick={() => copyToClipboard(text)}
          >
            {isCopied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
          </Button>
        )}
      </header>
      {text ? (
        <pre
          className="ocg-subtle-scrollbar max-h-80 overflow-auto whitespace-pre-wrap break-words px-3 py-3 font-mono text-xs leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring"
          tabIndex={0}
          role="region"
          aria-label={title}
        >
          {highlighted ? (
            <code
              className="hljs bg-transparent! p-0!"
              dangerouslySetInnerHTML={{ __html: highlighted.html }}
            />
          ) : (
            <code>{formatted ?? text}</code>
          )}
        </pre>
      ) : (
        <p className="px-3 py-3 text-xs text-muted-foreground">{t("chat.command.emptyOutput")}</p>
      )}
    </section>
  );
}

function nonnegative(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

export function CommandTool({ call, children }: { call: ControlToolCall; children: ReactNode }) {
  const { t, locale } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const args = toolObject(call.arguments);
  const result = toolObject(call.result);
  const command = typeof result?.command === "string" ? result.command : args?.command;
  const stdout = typeof result?.stdout === "string" ? result.stdout : undefined;
  const stderr = typeof result?.stderr === "string" ? result.stderr : undefined;
  if (!result || typeof command !== "string" || (stdout === undefined && stderr === undefined))
    return children;
  if (
    call.outcome_kind !== "completed" &&
    call.outcome_kind !== "not_applied" &&
    call.outcome_kind !== "uncertain"
  )
    return children;
  const requestedArguments = args?.arguments;
  const requested = Array.isArray(requestedArguments)
    ? [command, ...requestedArguments]
    : [command];
  const input = commandLine(result.argv) ?? commandLine(requested);
  if (input === null) return children;
  const exitCode =
    typeof result.exit_code === "number" && Number.isInteger(result.exit_code)
      ? result.exit_code
      : null;
  const signal =
    typeof result.signal === "number" && Number.isInteger(result.signal) && result.signal > 0
      ? result.signal
      : null;
  const number = new Intl.NumberFormat(locale, { maximumFractionDigits: 1 });
  const time = (value: number) =>
    value >= 1000 ? `${number.format(value / 1000)} s` : `${number.format(value)} ms`;
  const wall = nonnegative(result.wall_ms);
  const duration = wall ?? nonnegative(call.duration_ms);
  const cpuUser = nonnegative(result.cpu_user_ms);
  const cpuSystem = nonnegative(result.cpu_system_ms);
  const memory = nonnegative(result.peak_rss_bytes);
  let memoryValue: string | null = null;
  if (memory !== null) {
    if (memory >= 1024 * 1024) memoryValue = `${number.format(memory / (1024 * 1024))} MiB`;
    else if (memory >= 1024) memoryValue = `${number.format(memory / 1024)} KiB`;
    else memoryValue = `${number.format(memory)} B`;
  }
  const stats = [
    {
      label: t(wall === null ? "chat.command.toolTime" : "chat.command.wallTime"),
      value: duration === null ? null : time(duration),
    },
    { label: t("chat.command.cpuUser"), value: cpuUser === null ? null : time(cpuUser) },
    { label: t("chat.command.cpuSystem"), value: cpuSystem === null ? null : time(cpuSystem) },
    { label: t("chat.command.peakRss"), value: memoryValue },
  ].filter((stat) => stat.value !== null);
  return (
    <section
      data-slot="workspace-command"
      className="min-w-0 overflow-hidden rounded-lg border border-border bg-background font-sans"
    >
      <header className="flex min-w-0 items-center gap-2 border-b border-border bg-muted/40 px-3 py-2 text-xs">
        <Terminal className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span className="min-w-0 flex-1 truncate font-mono" title={command}>
          {command}
        </span>
        {exitCode !== null && (
          <span
            data-slot="command-exit-code"
            className={`shrink-0 rounded border px-2 py-0.5 tabular-nums ${exitCode === 0 ? "border-diff-added/25 bg-diff-added/10 text-diff-added" : "border-destructive/25 bg-destructive/10 text-destructive"}`}
          >
            {t("chat.command.exitCode", { code: exitCode })}
          </span>
        )}
        {signal !== null && (
          <span className="shrink-0 text-destructive">{t("chat.command.signal", { signal })}</span>
        )}
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          aria-label={t("chat.command.copyCommand")}
          onClick={() => copyToClipboard(input)}
        >
          {isCopied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </header>
      <pre
        data-slot="command-input"
        className="ocg-subtle-scrollbar overflow-x-auto px-3 py-3 font-mono text-xs leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring"
        tabIndex={0}
        role="region"
        aria-label={command}
      >
        <span className="text-muted-foreground select-none" aria-hidden="true">
          ${" "}
        </span>
        <code>{input}</code>
      </pre>
      {stats.length > 0 && (
        <dl className="flex flex-wrap gap-x-5 gap-y-2 border-t border-border px-3 py-2 text-xs">
          {stats.map((stat) => (
            <div key={stat.label} className="flex flex-wrap items-baseline gap-1.5">
              <dt className="text-muted-foreground">{stat.label}</dt>
              <dd className="tabular-nums">{stat.value}</dd>
            </div>
          ))}
        </dl>
      )}
      {stdout !== undefined && <CommandOutput title={t("chat.command.stdout")} text={stdout} />}
      {stderr !== undefined && <CommandOutput title={t("chat.command.stderr")} text={stderr} />}
      {result.truncated === true && (
        <p className="border-t border-border px-3 py-2 text-xs text-muted-foreground">
          {t("chat.command.truncated")}
        </p>
      )}
    </section>
  );
}
