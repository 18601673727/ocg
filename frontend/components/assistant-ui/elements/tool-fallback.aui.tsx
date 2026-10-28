"use client";

import { memo, useCallback, useRef, useState } from "react";
import {
  AlertCircleIcon,
  CheckIcon,
  ChevronDownIcon,
  type LucideIcon,
  LoaderIcon,
  XCircleIcon,
} from "lucide-react";
import {
  useScrollLock,
  useToolCallElapsed,
  type ToolCallMessagePartStatus,
  type ToolCallMessagePartComponent,
} from "@assistant-ui/react";
import { cn } from "@/lib/utils";
import { formatToolJson, jobPlanTitles } from "@/components/ocg/chat/tool-activity-summary";
import {
  CollapsibleRoot as Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "./collapsible-root";

const ANIMATION_DURATION = 200;

export type ToolFallbackRootProps = Omit<
  React.ComponentProps<typeof Collapsible>,
  "open" | "onOpenChange"
> & {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  defaultOpen?: boolean;
};

function ToolFallbackRoot({
  className,
  open: controlledOpen,
  onOpenChange: controlledOnOpenChange,
  defaultOpen = false,
  children,
  ...props
}: ToolFallbackRootProps) {
  const collapsibleRef = useRef<HTMLDivElement>(null);
  const [uncontrolledOpen, setUncontrolledOpen] = useState(defaultOpen);
  const lockScroll = useScrollLock(collapsibleRef, ANIMATION_DURATION);

  const isControlled = controlledOpen !== undefined;
  const isOpen = isControlled ? controlledOpen : uncontrolledOpen;

  const handleOpenChange = useCallback(
    (open: boolean) => {
      lockScroll();
      if (!isControlled) {
        setUncontrolledOpen(open);
      }
      controlledOnOpenChange?.(open);
    },
    [lockScroll, isControlled, controlledOnOpenChange],
  );

  return (
    <Collapsible
      ref={collapsibleRef}
      data-slot="tool-fallback-root"
      open={isOpen}
      onOpenChange={handleOpenChange}
      className={cn("aui-tool-fallback-root group/tool-fallback-root w-full", className)}
      style={
        {
          "--animation-duration": `${ANIMATION_DURATION}ms`,
        } as React.CSSProperties
      }
      {...props}
    >
      {children}
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

function ToolFallbackDuration({ className, ...props }: React.ComponentProps<"span">) {
  const elapsedMs = useToolCallElapsed();
  if (elapsedMs === undefined) return null;

  return (
    <span
      data-slot="tool-fallback-duration"
      className={cn(
        "aui-tool-fallback-duration text-muted-foreground text-xs tabular-nums",
        className,
      )}
      {...props}
    >
      {formatToolDuration(elapsedMs)}
    </span>
  );
}

function toolArgumentSummary(toolName: string, argsText: string): string {
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

function ToolFallbackTrigger({
  toolName,
  summary,
  status,
  className,
  ...props
}: React.ComponentProps<typeof CollapsibleTrigger> & {
  toolName: string;
  summary?: string;
  status?: ToolCallMessagePartStatus;
}) {
  const statusType = status?.type ?? "complete";
  const isRunning = statusType === "running";
  const isCancelled = status?.type === "incomplete" && status.reason === "cancelled";

  const Icon = statusIconMap[statusType];
  const label =
    statusType === "running"
      ? "Running tool"
      : statusType === "requires-action"
        ? "Waiting on tool"
        : statusType === "incomplete"
          ? `${isCancelled ? "Cancelled" : "Failed"} tool`
          : "Used tool";

  return (
    <CollapsibleTrigger
      data-slot="tool-fallback-trigger"
      className={cn(
        "aui-tool-fallback-trigger group/trigger text-muted-foreground hover:text-foreground flex w-full min-w-0 origin-left items-center gap-2 py-1.5 text-sm transition-[color,scale] active:scale-[0.98]",
        className,
      )}
      {...props}
    >
      <span data-slot="tool-fallback-trigger-icon" className="contents" aria-hidden="true">
        <Icon
          className={cn(
            "aui-tool-fallback-trigger-icon size-4 shrink-0",
            isCancelled && "text-muted-foreground",
            isRunning && "animate-spin [animation-duration:0.6s]",
          )}
        />
      </span>
      <span
        data-slot="tool-fallback-trigger-label"
        className={cn(
          "aui-tool-fallback-trigger-label-wrapper inline-block min-w-0 text-start leading-none wrap-anywhere",
          isCancelled && "text-muted-foreground line-through",
          isRunning && "shimmer motion-reduce:animate-none",
        )}
      >
        {label}: <b>{toolName}</b>
      </span>
      {summary ? (
        <span
          className="min-w-0 flex-1 truncate text-start text-xs text-muted-foreground"
          title={summary}
        >
          {summary}
        </span>
      ) : (
        <span className="flex-1" />
      )}
      <ToolFallbackDuration />
      <ChevronDownIcon
        data-slot="tool-fallback-trigger-chevron"
        className={cn(
          "aui-tool-fallback-trigger-chevron size-4 shrink-0",
          "transition-transform duration-(--animation-duration) ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:transition-none",
          "-rotate-90",
          "group-data-open/trigger:rotate-0",
          "group-data-panel-open/trigger:rotate-0",
        )}
      />
    </CollapsibleTrigger>
  );
}

function ToolFallbackContent({
  className,
  children,
  ...props
}: React.ComponentProps<typeof CollapsibleContent>) {
  return (
    <CollapsibleContent
      data-slot="tool-fallback-content"
      className={cn(
        "aui-tool-fallback-content relative overflow-hidden text-sm outline-none",
        "group/collapsible-content ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:animate-none",
        "data-closed:animate-collapsible-up",
        "data-open:animate-collapsible-down",
        "data-closed:fill-mode-forwards",
        "data-closed:pointer-events-none",
        "[--tw-duration:var(--animation-duration)]",
        className,
      )}
      {...props}
    >
      <div
        className={cn(
          "flex flex-col gap-2 ps-6 pt-1 pb-2 ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:animate-none",
          "group-data-open/collapsible-content:animate-in group-data-open/collapsible-content:fade-in-0 group-data-open/collapsible-content:blur-in-[2px] group-data-open/collapsible-content:slide-in-from-top-1",
          "group-data-closed/collapsible-content:animate-out group-data-closed/collapsible-content:fade-out-0 group-data-closed/collapsible-content:blur-out-[2px] group-data-closed/collapsible-content:slide-out-to-top-1",
          "group-data-closed/collapsible-content:animation-duration-(--animation-duration) group-data-open/collapsible-content:animation-duration-(--animation-duration)",
        )}
      >
        {children}
      </div>
    </CollapsibleContent>
  );
}

function ToolFallbackArgs({
  argsText,
  className,
  ...props
}: React.ComponentProps<"div"> & {
  argsText?: string;
}) {
  if (!argsText) return null;

  return (
    <div
      data-slot="tool-fallback-args"
      className={cn("aui-tool-fallback-args", className)}
      {...props}
    >
      <pre className="aui-tool-fallback-args-value bg-muted/50 text-foreground/90 rounded-md p-2.5 text-[11px] leading-relaxed break-words whitespace-pre-wrap">
        {formatToolJson(argsText)}
      </pre>
    </div>
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

function ToolFallbackResult({
  result,
  className,
  ...props
}: React.ComponentProps<"div"> & {
  result?: unknown;
}) {
  if (result === undefined) return null;

  return (
    <div
      data-slot="tool-fallback-result"
      className={cn("aui-tool-fallback-result", className)}
      {...props}
    >
      <p className="aui-tool-fallback-result-header text-muted-foreground text-xs font-medium">
        Result:
      </p>
      <pre className="aui-tool-fallback-result-content bg-muted/50 text-foreground/90 mt-1 rounded-md p-2.5 text-[11px] leading-relaxed break-words whitespace-pre-wrap">
        {formatToolJson(formatUnknownValue(result, 2))}
      </pre>
    </div>
  );
}

function ToolFallbackError({
  status,
  className,
  ...props
}: React.ComponentProps<"div"> & {
  status?: ToolCallMessagePartStatus;
}) {
  if (status?.type !== "incomplete") return null;

  const error = status.error;
  const errorText = error === undefined || error === null ? null : formatUnknownValue(error);

  if (!errorText) return null;

  const isCancelled = status.reason === "cancelled";
  const headerText = isCancelled ? "Cancelled reason:" : "Error:";

  return (
    <div
      data-slot="tool-fallback-error"
      className={cn("aui-tool-fallback-error", className)}
      {...props}
    >
      <p className="aui-tool-fallback-error-header text-muted-foreground font-semibold">
        {headerText}
      </p>
      <p className="aui-tool-fallback-error-reason text-muted-foreground break-words whitespace-pre-line">
        {errorText}
      </p>
    </div>
  );
}

const ToolFallbackImpl: ToolCallMessagePartComponent = ({ toolName, argsText, result, status }) => {
  const isCancelled = status?.type === "incomplete" && status.reason === "cancelled";

  return (
    <ToolFallbackRoot>
      <ToolFallbackTrigger
        toolName={toolName}
        summary={toolArgumentSummary(toolName, argsText)}
        status={status}
      />
      <ToolFallbackContent>
        <ToolFallbackError status={status} />
        <ToolFallbackArgs argsText={argsText} className={cn(isCancelled && "opacity-60")} />
        <ToolFallbackResult result={result} />
      </ToolFallbackContent>
    </ToolFallbackRoot>
  );
};

const ToolFallback = Object.assign(memo(ToolFallbackImpl), {
  Root: ToolFallbackRoot,
  Trigger: ToolFallbackTrigger,
  Content: ToolFallbackContent,
  Args: ToolFallbackArgs,
  Result: ToolFallbackResult,
  Error: ToolFallbackError,
});

ToolFallback.displayName = "ToolFallback";

export {
  formatUnknownValue,
  ToolFallback,
  ToolFallbackRoot,
  ToolFallbackTrigger,
  ToolFallbackContent,
  ToolFallbackArgs,
  ToolFallbackResult,
  ToolFallbackError,
};
