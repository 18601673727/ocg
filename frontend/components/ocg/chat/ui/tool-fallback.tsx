import {
  AlertCircleIcon,
  CheckIcon,
  ChevronDownIcon,
  LoaderIcon,
  type LucideIcon,
  XCircleIcon,
} from "@lucide/solid";
import { Dynamic } from "@solidjs/web";
import { createMemo, createSignal } from "solid-js";
import { formatToolJson, jobPlanTitles } from "@/components/ocg/chat/tool-activity-summary";
import type {
  ToolCallMessagePartComponent,
  ToolCallMessagePartStatus,
} from "@/components/ocg/chat/tool-part";
import {
  bindElementRef,
  type CSSProperties,
  type ElementProps,
  elementRef,
  splitProps,
} from "@/lib/ui-state";
import { cn } from "@/lib/utils";

import {
  CollapsibleRoot as Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "./collapsible-root";

const ANIMATION_DURATION = 200;

type ToolFallbackRootProps = Omit<ElementProps<typeof Collapsible>, "open" | "onOpenChange"> & {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  defaultOpen?: boolean;
};

function ToolFallbackRoot(viewProps: ToolFallbackRootProps) {
  const [, props] = splitProps(viewProps, [
    "class",
    "open",
    "onOpenChange",
    "defaultOpen",
    "children",
  ]);

  const collapsibleRef = elementRef<HTMLDivElement>(null);
  const [uncontrolledOpen, setUncontrolledOpen] = createSignal(
    viewProps.defaultOpen === undefined ? false : viewProps.defaultOpen,
  );

  const isControlled = createMemo(() => viewProps.open !== undefined);
  const isOpen = createMemo(() => (isControlled() ? viewProps.open : uncontrolledOpen()));

  const handleOpenChange = (open: boolean) => {
    if (!isControlled()) {
      setUncontrolledOpen(open);
    }
    viewProps.onOpenChange?.(open);
  };

  return (
    <Collapsible
      ref={bindElementRef(collapsibleRef)}
      data-slot="tool-fallback-root"
      open={isOpen()}
      onOpenChange={handleOpenChange}
      class={cn("aui-tool-fallback-root group/tool-fallback-root w-full", viewProps.class)}
      style={
        {
          "--animation-duration": `${ANIMATION_DURATION}ms`,
        } as CSSProperties
      }
      {...props}
    >
      {viewProps.children}
    </Collapsible>
  );
}

type ToolStatus = ToolCallMessagePartStatus["type"];

const statusIconMap: Record<ToolStatus, LucideIcon> = {
  running: LoaderIcon,
  complete: CheckIcon,
  incomplete: XCircleIcon,
  "requires-action": AlertCircleIcon,
};

const formatToolDuration = (ms: number) => {
  if (ms < 1000) return "<1s";
  const seconds = ms / 1000;
  if (seconds < 10) return `${(Math.floor(seconds * 10) / 10).toFixed(1)}s`;
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
};

function ToolFallbackDuration(viewProps: ElementProps<"span"> & { durationMs?: number }) {
  const [, props] = splitProps(viewProps, ["class", "durationMs"]);

  if (viewProps.durationMs === undefined) return null;

  return (
    <span
      data-slot="tool-fallback-duration"
      class={cn(
        "aui-tool-fallback-duration shrink-0 text-muted-foreground tabular-nums",
        viewProps.class,
      )}
      {...props}
    >
      {formatToolDuration(viewProps.durationMs!)}
    </span>
  );
}

export function toolArgumentSummary(toolName: string, argsText: string): string {
  try {
    const args: unknown = JSON.parse(argsText);
    if (!args || typeof args !== "object") return "";
    const value = (key: string) => Reflect.get(args, key);
    const text = (key: string) => {
      const item: unknown = value(key);
      return typeof item === "string" ? item.replace(/\s+/g, " ").trim() : "";
    };
    switch (toolName) {
      case "job_plan":
        return jobPlanTitles(args);
      case "workspace_search":
        return [text("path"), text("name"), text("text")].filter(Boolean).join(" · ");
      case "workspace_fts_search":
        return text("query");
      case "web_fetch":
        return text("url");
      case "workspace_read_file":
      case "workspace_list_directory":
        return text("path");
      case "workspace_write_file":
        return [text("operation"), text("path")].filter(Boolean).join(" · ");
      case "workspace_run_command": {
        const argumentsValue: unknown = value("arguments");
        const argumentsSummary = Array.isArray(argumentsValue)
          ? argumentsValue.filter((item): item is string => typeof item === "string").join(" ")
          : "";
        return [text("command"), argumentsSummary].filter(Boolean).join(" ");
      }
      case "workspace_code": {
        const queries: unknown = value("queries");
        if (!Array.isArray(queries)) return "";
        return queries
          .map((query: unknown) => {
            if (!query || typeof query !== "object") return "";
            const op: unknown = Reflect.get(query, "op");
            const target: unknown = Reflect.get(query, "name") || Reflect.get(query, "path");
            return [op, target].filter((item) => typeof item === "string").join(" ");
          })
          .filter(Boolean)
          .join(" · ");
      }
      default:
        return "";
    }
  } catch {
    return "";
  }
}

function ToolFallbackTrigger(
  viewProps: ElementProps<typeof CollapsibleTrigger> & {
    toolName: string;
    durationMs?: number;
    summary?: string;
    status?: ToolCallMessagePartStatus;
  },
) {
  const [, props] = splitProps(viewProps, ["toolName", "summary", "status", "class", "durationMs"]);

  const statusType = createMemo(() => viewProps.status?.type ?? "complete");
  const isRunning = createMemo(() => statusType() === "running");
  const isCancelled = createMemo(
    () => viewProps.status?.type === "incomplete" && viewProps.status.reason === "cancelled",
  );

  const Icon = createMemo(() => statusIconMap[statusType()]);
  const label = createMemo(() =>
    statusType() === "running"
      ? "Running tool"
      : statusType() === "requires-action"
        ? "Waiting on tool"
        : statusType() === "incomplete"
          ? `${isCancelled() ? "Cancelled" : "Failed"} tool`
          : "Used tool",
  );

  return (
    <CollapsibleTrigger
      data-slot="tool-fallback-trigger"
      class={cn(
        "aui-tool-fallback-trigger group/trigger text-muted-foreground hover:text-foreground flex w-full min-w-0 origin-left items-center gap-2 py-1.5 text-xs leading-5 transition-[color,scale] active:scale-[0.98]",
        viewProps.class,
      )}
      {...props}
    >
      <span data-slot="tool-fallback-trigger-icon" class="contents" aria-hidden="true">
        <Dynamic
          component={Icon()}
          class={cn(
            "aui-tool-fallback-trigger-icon size-4 shrink-0",
            isCancelled() && "text-muted-foreground",
            isRunning() && "animate-spin [animation-duration:0.6s]",
          )}
        />
      </span>
      <span
        data-slot="tool-fallback-trigger-label"
        class={cn(
          "aui-tool-fallback-trigger-label-wrapper min-w-0 text-start wrap-anywhere",
          isCancelled() && "text-muted-foreground line-through",
          isRunning() && "shimmer motion-reduce:animate-none",
        )}
      >
        {label()}: <b class="font-medium">{viewProps.toolName}</b>
      </span>
      {viewProps.summary ? (
        <span
          class="min-w-0 flex-1 truncate text-start text-muted-foreground"
          title={viewProps.summary}
        >
          {viewProps.summary}
        </span>
      ) : (
        <span class="flex-1" />
      )}
      <ToolFallbackDuration durationMs={viewProps.durationMs} />
      <span class="flex size-5 shrink-0 items-center justify-center" aria-hidden="true">
        <ChevronDownIcon
          data-slot="tool-fallback-trigger-chevron"
          class={cn(
            "aui-tool-fallback-trigger-chevron size-3.5 shrink-0",
            "transition-transform duration-(--animation-duration) ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:transition-none",
            "-rotate-90",
            "group-data-open/trigger:rotate-0",
            "group-data-panel-open/trigger:rotate-0",
          )}
        />
      </span>
    </CollapsibleTrigger>
  );
}

function ToolFallbackContent(viewProps: ElementProps<typeof CollapsibleContent>) {
  const [, props] = splitProps(viewProps, ["class", "children"]);

  return (
    <CollapsibleContent
      data-slot="tool-fallback-content"
      class={cn(
        "aui-tool-fallback-content relative min-w-0 text-xs leading-5 outline-none",
        "group/collapsible-content ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:animate-none",
        "data-closed:animate-collapsible-up",
        "data-open:animate-collapsible-down",
        "data-closed:fill-mode-forwards",
        "data-closed:pointer-events-none",
        "[--tw-duration:var(--animation-duration)]",
        viewProps.class,
      )}
      {...props}
    >
      <div
        class={cn(
          "flex flex-col gap-2 ps-6 pt-1 pb-2 ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:animate-none",
          "group-data-open/collapsible-content:animate-in group-data-open/collapsible-content:fade-in-0 group-data-open/collapsible-content:blur-in-[2px] group-data-open/collapsible-content:slide-in-from-top-1",
          "group-data-closed/collapsible-content:animate-out group-data-closed/collapsible-content:fade-out-0 group-data-closed/collapsible-content:blur-out-[2px] group-data-closed/collapsible-content:slide-out-to-top-1",
          "group-data-closed/collapsible-content:animation-duration-(--animation-duration) group-data-open/collapsible-content:animation-duration-(--animation-duration)",
        )}
      >
        {viewProps.children}
      </div>
    </CollapsibleContent>
  );
}

function ToolFallbackArgs(
  viewProps: ElementProps<"div"> & {
    argsText?: string;
  },
) {
  const [, props] = splitProps(viewProps, ["argsText", "class"]);

  return (
    <>
      {(() => {
        if (!viewProps.argsText) return null;

        return (
          <div
            data-slot="tool-fallback-args"
            class={cn("aui-tool-fallback-args", viewProps.class)}
            {...props}
          >
            <pre class="aui-tool-fallback-args-value bg-muted/50 text-foreground/90 overflow-x-visible rounded-md p-2.5 whitespace-pre-wrap wrap-anywhere">
              {formatToolJson(viewProps.argsText)}
            </pre>
          </div>
        );
      })()}
    </>
  );
}

const formatUnknownValue = (value: unknown, space?: number): string => {
  if (typeof value === "string") return value;

  try {
    if (value instanceof Error) return String(value);

    const json = JSON.stringify(value, null, space);
    if (json !== undefined) return json;
  } catch {}

  try {
    return String(value);
  } catch {
    return "[Unserializable value]";
  }
};

function ToolFallbackResult(
  viewProps: ElementProps<"div"> & {
    result?: unknown;
  },
) {
  const [, props] = splitProps(viewProps, ["result", "class"]);

  return (
    <>
      {(() => {
        if (viewProps.result === undefined) return null;

        return (
          <div
            data-slot="tool-fallback-result"
            class={cn("aui-tool-fallback-result", viewProps.class)}
            {...props}
          >
            <p class="aui-tool-fallback-result-header text-muted-foreground text-xs font-medium">
              Result:
            </p>
            <pre class="aui-tool-fallback-result-content bg-muted/50 text-foreground/90 mt-1 overflow-x-visible rounded-md p-2.5 whitespace-pre-wrap wrap-anywhere">
              {formatToolJson(formatUnknownValue(viewProps.result, 2))}
            </pre>
          </div>
        );
      })()}
    </>
  );
}

function ToolFallbackError(
  viewProps: ElementProps<"div"> & {
    status?: ToolCallMessagePartStatus;
  },
) {
  const [, props] = splitProps(viewProps, ["status", "class"]);

  return (
    <>
      {(() => {
        const status = viewProps.status;
        if (status?.type !== "incomplete") return null;

        const error = createMemo(() => status.error);
        const errorText = createMemo(() =>
          error() === undefined || error() === null ? null : formatUnknownValue(error()),
        );

        if (!errorText()) return null;

        const isCancelled = createMemo(() => status.reason === "cancelled");
        const headerText = createMemo(() => (isCancelled() ? "Cancelled reason:" : "Error:"));

        return (
          <div
            data-slot="tool-fallback-error"
            class={cn("aui-tool-fallback-error", viewProps.class)}
            {...props}
          >
            <p class="aui-tool-fallback-error-header text-muted-foreground font-semibold">
              {headerText()}
            </p>
            <p class="aui-tool-fallback-error-reason text-muted-foreground break-words whitespace-pre-line">
              {errorText()}
            </p>
          </div>
        );
      })()}
    </>
  );
}

const ToolFallbackImpl: ToolCallMessagePartComponent = (props) => {
  const isCancelled = createMemo(
    () => props.status?.type === "incomplete" && props.status.reason === "cancelled",
  );

  return (
    <ToolFallbackRoot>
      <ToolFallbackTrigger
        toolName={props.toolName}
        summary={toolArgumentSummary(props.toolName, props.argsText)}
        status={props.status}
      />
      <ToolFallbackContent>
        <ToolFallbackError status={props.status} />
        <ToolFallbackArgs argsText={props.argsText} class={cn(isCancelled() && "opacity-60")} />
        <ToolFallbackResult result={props.result} />
      </ToolFallbackContent>
    </ToolFallbackRoot>
  );
};

const ToolFallback = Object.assign(ToolFallbackImpl, {
  Root: ToolFallbackRoot,
  Trigger: ToolFallbackTrigger,
  Content: ToolFallbackContent,
  Args: ToolFallbackArgs,
  Result: ToolFallbackResult,
  Error: ToolFallbackError,
});

export { ToolFallback };
