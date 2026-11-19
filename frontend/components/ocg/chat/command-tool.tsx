import { Check, Copy, Terminal } from "@lucide/solid";
import { createMemo } from "solid-js";
import { Button } from "@/components/ui/button";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { type View } from "@/lib/ui-state";
import type { ToolCall } from "../contracts";
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

function CommandOutput(props: { title: string; text: string }) {
  const i18n = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const formatted = createMemo(() => formattedOutput(props.text));
  const highlighted = createMemo(() =>
    formatted() === null ? null : highlightCode(formatted()!, "json"),
  );
  return (
    <section class="min-w-0 border-t border-border" data-slot="command-output">
      <header class="flex items-center justify-between gap-2 bg-muted/20 px-3 py-1.5 text-xs text-muted-foreground">
        <h3 class="font-medium">{props.title}</h3>
        {props.text && (
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={i18n.t("chat.command.copyOutput", { stream: props.title })}
            onClick={() => copyToClipboard(props.text)}
          >
            {isCopied() ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
          </Button>
        )}
      </header>
      {props.text ? (
        <pre
          class="ocg-subtle-scrollbar max-h-80 overflow-auto whitespace-pre-wrap break-words px-3 py-3 font-mono text-xs leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring"
          tabindex={0}
          role="region"
          aria-label={props.title}
        >
          {highlighted() ? (
            <code class="hljs bg-transparent! p-0!" innerHTML={highlighted()!.html} />
          ) : (
            <code>{formatted() ?? props.text}</code>
          )}
        </pre>
      ) : (
        <p class="px-3 py-3 text-xs text-muted-foreground">{i18n.t("chat.command.emptyOutput")}</p>
      )}
    </section>
  );
}

function nonnegative(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

export function CommandTool(props: { call: ToolCall; children: View }) {
  const i18n = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const args = createMemo(() => toolObject(props.call.arguments));
  const result = createMemo(() => toolObject(props.call.result));
  const command = createMemo(() => {
    const value = result()?.command ?? args()?.command;
    return typeof value === "string" ? value : null;
  });
  const stdout = createMemo(() => {
    const value = result()?.stdout;
    return typeof value === "string" ? value : undefined;
  });
  const stderr = createMemo(() => {
    const value = result()?.stderr;
    return typeof value === "string" ? value : undefined;
  });
  return (
    <>
      {(() => {
        if (
          !result() ||
          typeof command() !== "string" ||
          (stdout() === undefined && stderr() === undefined)
        )
          return props.children;
        if (
          props.call.outcome_kind !== "completed" &&
          props.call.outcome_kind !== "not_applied" &&
          props.call.outcome_kind !== "uncertain"
        )
          return props.children;
        const requestedArguments = createMemo(() => args()?.arguments);
        const requested = createMemo(() =>
          Array.isArray(requestedArguments())
            ? [
                command(),
                ...(Array.isArray(requestedArguments()) ? (requestedArguments() as unknown[]) : []),
              ]
            : [command()],
        );
        const input = createMemo(() => commandLine(result()!.argv) ?? commandLine(requested()));
        if (input() === null) return props.children;
        const exitCode = createMemo(() =>
          typeof result()!.exit_code === "number" && Number.isInteger(result()!.exit_code)
            ? Number(result()!.exit_code)
            : null,
        );
        const signal = createMemo(() =>
          typeof result()!.signal === "number" &&
          Number.isInteger(result()!.signal) &&
          Number(result()!.signal) > 0
            ? Number(result()!.signal)
            : null,
        );
        const number = new Intl.NumberFormat(i18n.locale, { maximumFractionDigits: 1 });
        const time = (value: number) =>
          value >= 1000 ? `${number.format(value / 1000)} s` : `${number.format(value)} ms`;
        const wall = createMemo(() => nonnegative(result()!.wall_ms));
        const duration = createMemo(() => wall() ?? nonnegative(props.call.duration_ms));
        const cpuUser = createMemo(() => nonnegative(result()!.cpu_user_ms));
        const cpuSystem = createMemo(() => nonnegative(result()!.cpu_system_ms));
        const memory = createMemo(() => nonnegative(result()!.peak_rss_bytes));
        let memoryValue: string | null = null;
        const bytes = memory();
        if (bytes !== null) {
          if (bytes >= 1024 * 1024) memoryValue = `${number.format(bytes / (1024 * 1024))} MiB`;
          else if (bytes >= 1024) memoryValue = `${number.format(bytes / 1024)} KiB`;
          else memoryValue = `${number.format(bytes)} B`;
        }
        const stats = createMemo(() =>
          [
            {
              label: i18n.t(wall() === null ? "chat.command.toolTime" : "chat.command.wallTime"),
              value: duration() === null ? null : time(duration()!),
            },
            {
              label: i18n.t("chat.command.cpuUser"),
              value: cpuUser() === null ? null : time(cpuUser()!),
            },
            {
              label: i18n.t("chat.command.cpuSystem"),
              value: cpuSystem() === null ? null : time(cpuSystem()!),
            },
            { label: i18n.t("chat.command.peakRss"), value: memoryValue },
          ].filter((stat) => stat.value !== null),
        );
        return (
          <section
            data-slot="workspace-command"
            class="min-w-0 overflow-hidden rounded-lg border border-border bg-background font-sans"
          >
            <header class="flex min-w-0 items-center gap-2 border-b border-border bg-muted/40 px-3 py-2 text-xs">
              <Terminal class="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
              <span class="min-w-0 flex-1 truncate font-mono" title={command() ?? undefined}>
                {command()}
              </span>
              {exitCode() !== null && (
                <span
                  data-slot="command-exit-code"
                  class={`shrink-0 rounded border px-2 py-0.5 tabular-nums ${exitCode() === 0 ? "border-diff-added/25 bg-diff-added/10 text-diff-added" : "border-destructive/25 bg-destructive/10 text-destructive"}`}
                >
                  {i18n.t("chat.command.exitCode", { code: exitCode()! })}
                </span>
              )}
              {signal() !== null && (
                <span class="shrink-0 text-destructive">
                  {i18n.t("chat.command.signal", { signal: signal()! })}
                </span>
              )}
              <Button
                type="button"
                variant="ghost"
                size="icon-xs"
                aria-label={i18n.t("chat.command.copyCommand")}
                onClick={() => copyToClipboard(input()!)}
              >
                {isCopied() ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
              </Button>
            </header>
            <pre
              data-slot="command-input"
              class="ocg-subtle-scrollbar overflow-x-auto px-3 py-3 font-mono text-xs leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring"
              tabindex={0}
              role="region"
              aria-label={command() ?? undefined}
            >
              <span class="text-muted-foreground select-none" aria-hidden="true">
                ${" "}
              </span>
              <code>{input()}</code>
            </pre>
            {stats().length > 0 && (
              <dl class="flex flex-wrap gap-x-5 gap-y-2 border-t border-border px-3 py-2 text-xs">
                {stats().map((stat) => (
                  <div class="flex flex-wrap items-baseline gap-1.5">
                    <dt class="text-muted-foreground">{stat.label}</dt>
                    <dd class="tabular-nums">{stat.value}</dd>
                  </div>
                ))}
              </dl>
            )}
            {stdout() !== undefined && (
              <CommandOutput title={i18n.t("chat.command.stdout")} text={stdout()!} />
            )}
            {stderr() !== undefined && (
              <CommandOutput title={i18n.t("chat.command.stderr")} text={stderr()!} />
            )}
            {result()!.truncated === true && (
              <p class="border-t border-border px-3 py-2 text-xs text-muted-foreground">
                {i18n.t("chat.command.truncated")}
              </p>
            )}
          </section>
        );
      })()}
    </>
  );
}
