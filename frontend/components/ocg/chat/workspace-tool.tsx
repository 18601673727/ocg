"use client";

import { useEffect, useState, type ReactNode } from "react";
import { useAuiState, type ToolCallMessagePartComponent } from "@assistant-ui/react";
import { Check, Copy, FileCode2 } from "lucide-react";
import {
  ToolFallback,
  toolArgumentSummary,
} from "@/components/assistant-ui/elements/tool-fallback.aui";
import { Button } from "@/components/ui/button";
import { DiffViewer } from "@/components/ui/diff-viewer";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import type { ControlToolCall } from "../contracts";
import { useI18n } from "../i18n";
import { highlightCode } from "../markdown/markdown-pipeline";
import { sourceLanguage } from "../markdown/source-snapshot";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useOcgRuntime } from "../runtime/runtime-context";
import { artifactTitle, fileArtifact, toolObject } from "./artifact-model";
import { CommandTool } from "./command-tool";
import { SearchTool } from "./search-tool";

const SUPPORTED_TOOLS = new Set([
  "workspace_read_file",
  "workspace_write_file",
  "workspace_run_command",
  "workspace_search",
  "workspace_fts_search",
]);

function FileCode({
  path,
  content,
  startLine = 1,
  totalLines,
  truncated = false,
}: {
  path: string;
  content: string;
  startLine?: number;
  totalLines?: number;
  truncated?: boolean;
}) {
  const { t } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const highlighted = highlightCode(content, sourceLanguage(path));
  const lines = content ? content.split("\n") : [];
  if (lines.at(-1) === "") lines.pop();
  const endLine = startLine + lines.length - 1;
  return (
    <section
      data-slot="workspace-file-content"
      className="min-w-0 overflow-hidden rounded-lg border border-border bg-background"
    >
      <header className="flex min-w-0 items-center gap-2 border-b border-border bg-muted/40 px-3 py-2 text-xs">
        <FileCode2 className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span className="min-w-0 flex-1 truncate font-mono" title={path}>
          {path}
        </span>
        {lines.length > 0 && (
          <span className="shrink-0 tabular-nums text-muted-foreground">
            {startLine}–{endLine}
            {totalLines === undefined ? "" : ` / ${totalLines}`}
          </span>
        )}
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          aria-label={t("common.copySource")}
          onClick={() => copyToClipboard(content)}
        >
          {isCopied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </header>
      {lines.length ? (
        <div
          className="ocg-subtle-scrollbar max-h-96 overflow-auto outline-none focus-visible:ring-2 focus-visible:ring-ring"
          tabIndex={0}
          role="region"
          aria-label={path}
        >
          <div className="grid w-max min-w-full grid-cols-[auto_minmax(0,1fr)] font-mono text-xs leading-5">
            <pre
              aria-hidden="true"
              className="sticky left-0 border-r border-border bg-muted px-3 py-3 text-right text-muted-foreground select-none"
            >
              {lines.map((_, index) => startLine + index).join("\n")}
            </pre>
            <pre className="px-3 py-3">
              {highlighted ? (
                <code
                  className="hljs bg-transparent! p-0!"
                  dangerouslySetInnerHTML={{ __html: highlighted.html }}
                />
              ) : (
                <code>{content}</code>
              )}
            </pre>
          </div>
        </div>
      ) : (
        <p className="px-3 py-4 text-sm text-muted-foreground">{t("chat.fileTool.empty")}</p>
      )}
      {truncated && (
        <p className="border-t border-border px-3 py-2 text-xs text-muted-foreground">
          {t("chat.fileTool.truncated")}
        </p>
      )}
    </section>
  );
}

export function WorkspaceTool({
  call,
  jobId,
  client,
  children,
}: {
  call: ControlToolCall;
  jobId?: string;
  client?: Pick<HttpCanonicalControlClient, "readJobTool">;
  children: ReactNode;
}) {
  const { t } = useI18n();
  const [loaded, setLoaded] = useState<{
    key: string;
    call: ControlToolCall;
  } | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const key = `${jobId}:${call.request}:${call.index}`;
  const supportedTool = SUPPORTED_TOOLS.has(call.name);
  const recordedResult =
    call.outcome_kind === "completed" ||
    (call.name === "workspace_run_command" &&
      (call.outcome_kind === "not_applied" || call.outcome_kind === "uncertain"));
  const needsRead = supportedTool && recordedResult && (call.arguments_cut || call.result_cut);
  useEffect(() => {
    if (!needsRead || !client || !jobId || loaded?.key === key) return;
    const controller = new AbortController();
    void client
      .readJobTool(jobId, call.request, call.index, controller.signal)
      .then((full) => {
        if (!controller.signal.aborted) setLoaded({ key, call: full });
      })
      .catch(() => {
        if (!controller.signal.aborted) setFailure(key);
      });
    return () => controller.abort();
  }, [client, jobId, call.request, call.index, key, needsRead, loaded?.key, attempt]);
  if (needsRead && client && jobId && loaded?.key !== key) {
    if (failure !== key)
      return (
        <p role="status" className="text-xs text-muted-foreground">
          {t("common.loading")}
        </p>
      );
    return (
      <div className="space-y-2">
        <p role="status" className="text-xs text-muted-foreground">
          {t("chat.fileTool.loadFailed")}
        </p>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => {
            setFailure(null);
            setAttempt((value) => value + 1);
          }}
        >
          {t("common.retry")}
        </Button>
        {children}
      </div>
    );
  }
  const current = loaded?.key === key ? loaded.call : call;
  if (!supportedTool || current.arguments_cut || current.result_cut) return children;
  if (current.name === "workspace_run_command") {
    return <CommandTool call={current}>{children}</CommandTool>;
  }
  if (current.outcome_kind !== "completed") return children;
  if (current.name === "workspace_search" || current.name === "workspace_fts_search") {
    return <SearchTool call={current}>{children}</SearchTool>;
  }
  const args = toolObject(current.arguments);
  const result = toolObject(current.result);
  const path =
    typeof args?.path === "string"
      ? args.path
      : typeof result?.path === "string"
        ? result.path
        : null;
  if (!path) return children;
  if (current.name === "workspace_read_file") {
    if (typeof result?.content !== "string") return children;
    const start = result.start_line ?? args?.start_line ?? 1;
    const total = result.total_lines;
    return (
      <FileCode
        path={path}
        content={result.content}
        startLine={
          typeof start === "number" && Number.isSafeInteger(start) && start > 0 ? start : 1
        }
        totalLines={
          typeof total === "number" && Number.isSafeInteger(total) && total >= 0 ? total : undefined
        }
        truncated={result.line_truncated === true || result.truncated === true}
      />
    );
  }
  const file = fileArtifact(current);
  if (file?.before !== undefined && file.after !== undefined) {
    return (
      <div className="min-w-0" data-slot="workspace-file-diff">
        {file.excerpt && (
          <p className="mb-2 text-xs text-muted-foreground">{t("chat.artifacts.excerpt")}</p>
        )}
        <div
          className="ocg-subtle-scrollbar max-h-96 overflow-auto"
          tabIndex={0}
          role="region"
          aria-label={path}
        >
          <DiffViewer
            oldFile={{ name: path, content: file.before }}
            newFile={{ name: path, content: file.after }}
            viewMode="unified"
            size="sm"
            showLineNumbers={!file.excerpt}
            maxCollapsedLines={80}
            className="max-w-full rounded-lg"
          />
        </div>
      </div>
    );
  }
  if (file?.after !== undefined)
    return (
      <div className="space-y-2">
        <p className="text-xs text-muted-foreground">{t("chat.artifacts.beforeUnavailable")}</p>
        <FileCode path={path} content={file.after} />
      </div>
    );
  if (args?.operation === "delete")
    return (
      <div
        data-slot="workspace-file-deleted"
        className="flex min-w-0 items-center justify-between gap-3 rounded-lg border border-border bg-muted/30 px-3 py-3 text-xs"
      >
        <span className="min-w-0 truncate font-mono" title={path}>
          {path}
        </span>
        <span className="shrink-0 text-destructive">{t("state.deleted")}</span>
      </div>
    );
  return children;
}

export const WorkspaceToolFallback: ToolCallMessagePartComponent = (props) => {
  const { control } = useOcgRuntime();
  const call = useAuiState((state) => {
    const metrics = state.message.metadata.custom.toolMetrics as
      | Record<string, { call?: ControlToolCall }>
      | undefined;
    return metrics?.[props.toolCallId]?.call;
  });
  const jobId = useAuiState((state) => state.message.metadata.custom.jobId);
  if (!call || !SUPPORTED_TOOLS.has(call.name)) return <ToolFallback {...props} />;
  const search = call.name === "workspace_search" || call.name === "workspace_fts_search";
  return (
    <ToolFallback.Root>
      <ToolFallback.Trigger
        toolName={props.toolName}
        summary={
          search ? toolArgumentSummary(call.name, call.arguments ?? "") : artifactTitle(call)
        }
        status={props.status}
      />
      <ToolFallback.Content>
        <ToolFallback.Error status={props.status} />
        <WorkspaceTool
          call={call}
          jobId={typeof jobId === "string" ? jobId : undefined}
          client={control ?? undefined}
        >
          <ToolFallback.Args argsText={props.argsText} />
          <ToolFallback.Result result={props.result} />
        </WorkspaceTool>
      </ToolFallback.Content>
    </ToolFallback.Root>
  );
};
