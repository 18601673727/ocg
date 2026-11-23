import { Check, Copy, FileCode2 } from "@lucide/solid";
import { createEffect, createMemo, createSignal, Loading, lazy } from "solid-js";
import { ToolFallback, toolArgumentSummary } from "@/components/ocg/chat/ui/tool-fallback";
import { Button } from "@/components/ui/button";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { type View } from "@/lib/ui-state";
import type { ToolCall } from "../contracts";
import { useI18n } from "../i18n";
import { highlightCode } from "../markdown/markdown-pipeline";
import type { ApiClient } from "../runtime/api-client";
import { useOcgRuntime } from "../runtime/runtime-context";
import { artifactTitle, fileArtifact, toolObject } from "./artifact-model";
import { CommandTool } from "./command-tool";
import { SearchTool } from "./search-tool";
import type { ToolCallMessagePartComponent } from "./tool-part";

const DiffViewer = lazy(() =>
  import("@/components/ui/diff-viewer").then((module) => ({ default: module.DiffViewer })),
);

const SUPPORTED_TOOLS = new Set([
  "workspace_read_file",
  "workspace_write_file",
  "workspace_run_command",
  "workspace_search",
  "workspace_fts_search",
]);

function FileCode(props: {
  path: string;
  content: string;
  startLine?: number;
  totalLines?: number;
  truncated?: boolean;
}) {
  const i18n = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const language = createMemo(() => props.path.match(/[^./\\]+$/)?.[0] ?? "");
  const highlighted = createMemo(() => highlightCode(props.content, language()));
  const lines = createMemo(() => (props.content ? props.content.split("\n") : []));
  if (lines().at(-1) === "") lines().pop();
  const endLine = createMemo(
    () => (props.startLine === undefined ? 1 : props.startLine) + lines().length - 1,
  );
  return (
    <section
      data-slot="workspace-file-content"
      class="min-w-0 overflow-hidden rounded-lg border border-border bg-background"
    >
      <header class="flex min-w-0 items-center gap-2 border-b border-border bg-muted/40 px-3 py-2 text-xs">
        <FileCode2 class="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span class="min-w-0 flex-1 truncate font-mono" title={props.path}>
          {props.path}
        </span>
        {lines().length > 0 && (
          <span class="shrink-0 tabular-nums text-muted-foreground">
            {props.startLine === undefined ? 1 : props.startLine}–{endLine()}
            {props.totalLines === undefined ? "" : ` / ${props.totalLines}`}
          </span>
        )}
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          aria-label={i18n.t("common.copySource")}
          onClick={() => copyToClipboard(props.content)}
        >
          {isCopied() ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </header>
      {lines().length ? (
        <div
          class="ocg-subtle-scrollbar max-h-96 overflow-auto outline-none focus-visible:ring-2 focus-visible:ring-ring"
          tabindex={0}
          role="region"
          aria-label={props.path}
        >
          <div class="grid w-max min-w-full grid-cols-[auto_minmax(0,1fr)] font-mono text-xs leading-5">
            <pre
              aria-hidden="true"
              class="sticky left-0 border-r border-border bg-muted px-3 py-3 text-right text-muted-foreground select-none"
            >
              {lines()
                .map((_, index) => (props.startLine === undefined ? 1 : props.startLine) + index)
                .join("\n")}
            </pre>
            <pre class="px-3 py-3">
              {highlighted() ? (
                <code class="hljs bg-transparent! p-0!" innerHTML={highlighted()!.html} />
              ) : (
                <code>{props.content}</code>
              )}
            </pre>
          </div>
        </div>
      ) : (
        <p class="px-3 py-4 text-sm text-muted-foreground">{i18n.t("chat.fileTool.empty")}</p>
      )}
      {(props.truncated === undefined ? false : props.truncated) && (
        <p class="border-t border-border px-3 py-2 text-xs text-muted-foreground">
          {i18n.t("chat.fileTool.truncated")}
        </p>
      )}
    </section>
  );
}

export function WorkspaceTool(props: {
  call: ToolCall;
  jobId?: string;
  client?: Pick<ApiClient, "readJobTool">;
  children: View;
}) {
  const i18n = useI18n();
  const [loaded, setLoaded] = createSignal<{
    key: string;
    call: ToolCall;
  } | null>(null);
  const [failure, setFailure] = createSignal<string | null>(null);
  const [attempt, setAttempt] = createSignal(0);
  const key = createMemo(() => `${props.jobId}:${props.call.request}:${props.call.index}`);
  const supportedTool = createMemo(() => SUPPORTED_TOOLS.has(props.call.name));
  const recordedResult = createMemo(
    () =>
      props.call.outcome_kind === "completed" ||
      (props.call.name === "workspace_run_command" &&
        (props.call.outcome_kind === "not_applied" || props.call.outcome_kind === "uncertain")),
  );
  const needsRead = createMemo(
    () =>
      supportedTool() && recordedResult() && (props.call.arguments_cut || props.call.result_cut),
  );
  createEffect(
    () => [
      props.client,
      props.jobId,
      props.call.request,
      props.call.index,
      key(),
      needsRead(),
      loaded()?.key,
      attempt(),
    ],
    () => {
      if (!needsRead() || !props.client || !props.jobId || loaded()?.key === key()) return;
      const controller = new AbortController();
      void props.client
        .readJobTool(props.jobId, props.call.request, props.call.index, controller.signal)
        .then((full) => {
          if (!controller.signal.aborted) setLoaded({ key: key(), call: full });
        })
        .catch(() => {
          if (!controller.signal.aborted) setFailure(key());
        });
      return () => controller.abort();
    },
  );
  return (
    <>
      {(() => {
        if (needsRead() && props.client && props.jobId && loaded()?.key !== key()) {
          if (failure() !== key())
            return (
              <p role="status" class="text-xs text-muted-foreground">
                {i18n.t("common.loading")}
              </p>
            );
          return (
            <div class="space-y-2">
              <p role="status" class="text-xs text-muted-foreground">
                {i18n.t("chat.fileTool.loadFailed")}
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
                {i18n.t("common.retry")}
              </Button>
              {props.children}
            </div>
          );
        }
        const current = createMemo(() => (loaded()?.key === key() ? loaded()!.call : props.call));
        if (!supportedTool() || current().arguments_cut || current().result_cut)
          return props.children;
        if (current().name === "workspace_run_command") {
          return <CommandTool call={current()}>{props.children}</CommandTool>;
        }
        if (current().outcome_kind !== "completed") return props.children;
        if (current().name === "workspace_search" || current().name === "workspace_fts_search") {
          return <SearchTool call={current()}>{props.children}</SearchTool>;
        }
        const args = createMemo(() => toolObject(current().arguments));
        const result = createMemo(() => toolObject(current().result));
        const path = createMemo(() => {
          const value = args()?.path ?? result()?.path;
          return typeof value === "string" ? value : null;
        });
        if (!path()) return props.children;
        if (current().name === "workspace_read_file") {
          const content = result()?.content;
          if (typeof content !== "string") return props.children;
          const start = result()!.start_line ?? args()?.start_line ?? 1;
          const total = result()!.total_lines;
          return (
            <FileCode
              path={path()!}
              content={content}
              startLine={
                typeof start === "number" && Number.isSafeInteger(start) && start > 0 ? start : 1
              }
              totalLines={
                typeof total === "number" && Number.isSafeInteger(total) && total >= 0
                  ? total
                  : undefined
              }
              truncated={result()!.line_truncated === true || result()!.truncated === true}
            />
          );
        }
        const file = createMemo(() => fileArtifact(current()));
        if (file()?.before !== undefined && file()!.after !== undefined) {
          return (
            <div class="min-w-0" data-slot="workspace-file-diff">
              {file()!.excerpt && (
                <p class="mb-2 text-xs text-muted-foreground">{i18n.t("chat.artifacts.excerpt")}</p>
              )}
              <div
                class="ocg-subtle-scrollbar max-h-96 overflow-auto"
                tabindex={0}
                role="region"
                aria-label={path() ?? undefined}
              >
                <Loading fallback={null}>
                  <DiffViewer
                    oldFile={{ name: path()!, content: file()!.before! }}
                    newFile={{ name: path()!, content: file()!.after! }}
                    viewMode="unified"
                    size="sm"
                    showLineNumbers={!file()!.excerpt}
                    maxCollapsedLines={80}
                    class="max-w-full rounded-lg"
                  />
                </Loading>
              </div>
            </div>
          );
        }
        if (file()?.after !== undefined)
          return (
            <div class="space-y-2">
              <p class="text-xs text-muted-foreground">
                {i18n.t("chat.artifacts.beforeUnavailable")}
              </p>
              <FileCode path={path()!} content={file()!.after!} />
            </div>
          );
        if (args()?.operation === "delete")
          return (
            <div
              data-slot="workspace-file-deleted"
              class="flex min-w-0 items-center justify-between gap-3 rounded-lg border border-border bg-muted/30 px-3 py-3 text-xs"
            >
              <span class="min-w-0 truncate font-mono" title={path() ?? undefined}>
                {path()}
              </span>
              <span class="shrink-0 text-destructive">{i18n.t("state.deleted")}</span>
            </div>
          );
        return props.children;
      })()}
    </>
  );
}

export const WorkspaceToolFallback: ToolCallMessagePartComponent = (props) => {
  const { api } = useOcgRuntime();
  return (
    <>
      {(() => {
        const call = props.call;
        if (!call || !SUPPORTED_TOOLS.has(call.name)) return <ToolFallback {...props} />;
        const search = createMemo(
          () => call.name === "workspace_search" || call.name === "workspace_fts_search",
        );
        return (
          <ToolFallback.Root>
            <ToolFallback.Trigger
              toolName={props.toolName}
              durationMs={props.durationMs}
              summary={
                search()
                  ? toolArgumentSummary(call.name, call.arguments ?? "")
                  : artifactTitle(call)
              }
              status={props.status}
            />
            <ToolFallback.Content>
              <ToolFallback.Error status={props.status} />
              <WorkspaceTool call={call} jobId={props.jobId} client={api ?? undefined}>
                <ToolFallback.Args argsText={props.argsText} />
                <ToolFallback.Result result={props.result} />
              </WorkspaceTool>
            </ToolFallback.Content>
          </ToolFallback.Root>
        );
      })()}
    </>
  );
};
