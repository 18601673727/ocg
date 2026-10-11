"use client";

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  ArrowUp,
  Square,
  ListPlus,
  X,
  Bot,
  Brain,
  Check,
  ChevronDown,
  Command,
  Copy,
  Loader2,
  Paperclip,
  Sparkles,
  User,
  Wrench,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { BORDER_TONE, StatusDot, TEXT_TONE, TOOL_STATUS } from "@/components/ocg/primitives";
import { ActivityPulse } from "../activity-pulse";
import { useI18n, runtimeStateLabel, runtimeStatusDetail, chatFailureReason } from "../i18n";
import type { ChatMessage, ChatSession, RuntimeStatus } from "../types";
import { ModelSelector } from "./model-selector";
import type { ChatModelSelection } from "../contracts";
import type { QueuedChatMessage } from "../types";
import { retryInput } from "./retry";
import { activeExecutionTarget, currentActivity, executionActivity, isExecutionActive, type ActiveExecutionTarget, type ActivityStep, type ExecutionPhase } from "./execution-status";
import { readModelPreference, writeModelPreference } from "./model-preference";
import { BOUNDED_MESSAGE_CLASS, boundedToggle, followAfterScroll } from "./message-bounds";
import type { JobExecution } from "../execution/domain";
import { parseMarkdownTable, type MarkdownTable } from "./markdown-table";
import { AttachmentStaging, ImageGallery, useImageAttachments } from "./image-attachments";
import {
  applyComposerSuggestion,
  matchComposerSuggestions,
  moveComposerSuggestionIndex,
  parseComposerIntent,
  type ComposerIntent,
  type ComposerSuggestion,
} from "../composer/domain";

/* ---------- lightweight markdown rendering (no new deps) ---------- */

function renderInline(text: string, keyPrefix: string): React.ReactNode[] {
  const parts: React.ReactNode[] = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  let i = 0;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) parts.push(text.slice(last, m.index));
    const token = m[0];
    if (token.startsWith("**")) {
      parts.push(
        <strong key={`${keyPrefix}-b${i}`} className="font-semibold text-foreground">
          {token.slice(2, -2)}
        </strong>,
      );
    } else {
      parts.push(
        <code
          key={`${keyPrefix}-c${i}`}
          className="rounded border border-border bg-muted px-1 py-px font-mono text-[12px] text-foreground"
        >
          {token.slice(1, -1)}
        </code>,
      );
    }
    last = m.index + token.length;
    i += 1;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts;
}

function CodeBlock({ language, code }: { language: string; code: string }) {
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const { t } = useI18n();
  return (
    <div className="overflow-hidden rounded-md border border-border bg-muted/40">
      <div className="flex items-center gap-2 border-b border-border px-2.5 py-1.5">
        <span className="font-mono text-[11px] text-muted-foreground">
          {language || t("chat.code")}
        </span>
        <button
          type="button"
          onClick={() => copyToClipboard(code)}
          className="ml-auto flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px] text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          aria-label={isCopied ? t("common.copied") : t("chat.copyCodeToClipboard")}
          title={t("chat.copyCode")}
        >
          {isCopied ? <Check className="size-3" /> : <Copy className="size-3" />}
          {isCopied ? t("common.copied") : t("common.copy")}
        </button>
      </div>
      <pre className="overflow-x-auto p-2.5 font-mono text-[12px] leading-5 text-foreground">
        <code>{code}</code>
      </pre>
    </div>
  );
}

function TableBlock({ table }: { table: MarkdownTable }) {
  const { t } = useI18n();
  return (
    <div role="region" aria-label={t("chat.markdownTable")} tabIndex={0} className="max-w-full overflow-x-auto rounded-md border border-border focus-visible:outline-ring">
      <table className="w-full min-w-max border-collapse text-[12px] leading-5">
        <thead className="bg-muted/50">
          <tr>
            {table.headers.map((header, index) => (
              <th key={index} scope="col" style={{ textAlign: table.alignments[index] }} className="border-r border-border px-3 py-2 font-semibold last:border-r-0">
                {renderInline(header, `table-header-${index}`)}
              </th>
            ))}
          </tr>
        </thead>
        {table.rows.length > 0 && <tbody>
          {table.rows.map((row, rowIndex) => (
            <tr key={rowIndex} className="border-t border-border">
              {row.map((cell, columnIndex) => (
                <td key={columnIndex} style={{ textAlign: table.alignments[columnIndex] }} className="border-r border-border px-3 py-2 align-top last:border-r-0">
                  {renderInline(cell, `table-${rowIndex}-${columnIndex}`)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>}
      </table>
    </div>
  );
}

function Markdown({ content }: { content: string }) {
  const blocks: React.ReactNode[] = [];
  const fence = /```(\w*)\n([\s\S]*?)```/g;
  let last = 0;
  let m: RegExpExecArray | null;
  let bi = 0;
  const pushText = (text: string, key: string) => {
    const lines = text.split("\n");
    let li = 0;
    let listBuffer: string[] = [];
    const flushList = () => {
      if (listBuffer.length === 0) return;
      blocks.push(
        <ul key={`${key}-ul${li}`} className="flex list-disc flex-col gap-1 pl-5">
          {listBuffer.map((item, idx) => (
            <li key={idx}>{renderInline(item, `${key}-li${idx}`)}</li>
          ))}
        </ul>,
      );
      listBuffer = [];
    };
    for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
      const table = parseMarkdownTable(lines, lineIndex);
      if (table) {
        flushList();
        blocks.push(<TableBlock key={`${key}-table${li}`} table={table} />);
        lineIndex = table.nextLine - 1;
        li += 1;
        continue;
      }
      const line = lines[lineIndex];
      const trimmed = line.trim();
      const embeddedImages = [...line.matchAll(/!\[([^\]]*)\]\((https?:\/\/[^\s)]+|data:image\/(?:png|jpeg|gif|webp);base64,[A-Za-z0-9+/=]+)\)/g)];
      if (embeddedImages.length) {
        flushList();
        const text = line.replace(/!\[([^\]]*)\]\((https?:\/\/[^\s)]+|data:image\/(?:png|jpeg|gif|webp);base64,[A-Za-z0-9+/=]+)\)/g, "").trim();
        if (text) blocks.push(<p key={`${key}-p${li}`} className="leading-6">{renderInline(text, `${key}-p${li}`)}</p>);
        blocks.push(<ImageGallery key={`${key}-images${li}`} images={embeddedImages.map((match, index) => ({ id: `${key}-${li}-${index}`, name: match[1] || "Image", media_type: "image/*", url: match[2] }))} />);
      } else if (trimmed.startsWith("- ")) {
        listBuffer.push(trimmed.slice(2));
      } else {
        flushList();
        if (trimmed === "") {
          // paragraph break — no node needed
        } else if (trimmed.startsWith("### ")) {
          blocks.push(
            <h4 key={`${key}-h${li}`} className="pt-1 text-[13px] font-semibold">
              {renderInline(trimmed.slice(4), `${key}-h${li}`)}
            </h4>,
          );
        } else if (trimmed.startsWith("## ")) {
          blocks.push(
            <h3 key={`${key}-h${li}`} className="pt-1 text-[13px] font-semibold">
              {renderInline(trimmed.slice(3), `${key}-h${li}`)}
            </h3>,
          );
        } else if (/^\d+\.\s/.test(trimmed)) {
          listBuffer.push(trimmed.replace(/^\d+\.\s/, ""));
        } else {
          blocks.push(
            <p key={`${key}-p${li}`} className="leading-6">
              {renderInline(line, `${key}-p${li}`)}
            </p>,
          );
        }
      }
      li += 1;
    }
    flushList();
  };

  while ((m = fence.exec(content)) !== null) {
    if (m.index > last) pushText(content.slice(last, m.index), `t${bi}`);
    blocks.push(<CodeBlock key={`c${bi}`} language={m[1]} code={m[2].replace(/\n$/, "")} />);
    last = m.index + m[0].length;
    bi += 1;
  }
  if (last < content.length) pushText(content.slice(last), `t${bi}`);
  return <div className="flex flex-col gap-2">{blocks}</div>;
}

/* ---------- tool / activity block ---------- */

function ToolBlock({ message }: { message: ChatMessage }) {
  const tool = message.tool;
  const [open, setOpen] = useState(true);
  const { t } = useI18n();
  if (!tool) return null;
  const tone = Object.entries(TOOL_STATUS).find(([status]) => status === tool.status)?.[1].tone ?? "slate";
  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="overflow-hidden rounded-md border border-border bg-muted/30">
        <CollapsibleTrigger
          className="flex w-full items-center gap-2 px-2.5 py-2 text-left"
          aria-label={t("chat.toolToggleDetails", { name: tool.name, status: tool.status })}
        >
          <span className="flex size-6 shrink-0 items-center justify-center rounded border border-border bg-background">
            <Wrench className="size-3.5 text-muted-foreground" aria-hidden="true" />
          </span>
          <span className="min-w-0 flex-1">
            <span className="block truncate font-mono text-[12px] font-medium">
              {tool.name}
            </span>
            <span className="block truncate text-[12px] text-muted-foreground">
              {tool.summary}
            </span>
          </span>
          <span
           className={cn(
              "flex shrink-0 items-center gap-1 rounded-md border px-1.5 py-0.5 text-[11px]",
              BORDER_TONE[tone],
              TEXT_TONE[tone],
            )}
          >
            {tool.status === "running" || tool.status === "retrying" ? (
              <Loader2 className="size-3 animate-spin" aria-hidden="true" />
            ) : (
              <StatusDot tone={tone} />
            )}
            {runtimeStateLabel(t, tool.status)} · {tool.durationMs}ms
          </span>
          <ChevronDown
            className={cn("size-3.5 shrink-0 text-muted-foreground transition-transform", !open && "-rotate-90")}
            aria-hidden="true"
          />
        </CollapsibleTrigger>
        <CollapsibleContent>
          <pre className="overflow-x-auto border-t border-border bg-background/60 p-2.5 font-mono text-[12px] leading-5 text-muted-foreground">
            {tool.detail}
          </pre>
        </CollapsibleContent>
      </div>
    </Collapsible>
  );
}

/* ---------- message row ---------- */

/**
 * A long message scrolls inside its own bounded viewport until the reader
 * expands it. A streaming message keeps its newest output in view until the
 * reader scrolls up inside it.
 */
function BoundedMessage({ streaming, children }: { streaming: boolean; children: React.ReactNode }) {
  const { t } = useI18n();
  const viewport = useRef<HTMLDivElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  const lastTop = useRef(0);
  const [expanded, setExpanded] = useState(false);
  const [overflowing, setOverflowing] = useState(false);

  useEffect(() => {
    const el = viewport.current;
    const inner = content.current;
    if (!el || !inner) return;
    const observer = new ResizeObserver(() => {
      if (!expanded && streaming && following.current) {
        el.scrollTop = el.scrollHeight;
        lastTop.current = el.scrollTop;
      }
      setOverflowing(el.scrollHeight - el.clientHeight > 1);
    });
    observer.observe(inner);
    observer.observe(el);
    return () => observer.disconnect();
  }, [expanded, streaming]);

  const toggle = boundedToggle(overflowing, expanded);
  return (
    <>
      <div
        ref={viewport}
        data-message-bounds={expanded ? "expanded" : "bounded"}
        onScroll={() => {
          const el = viewport.current;
          if (!el || expanded) return;
          following.current = followAfterScroll(following.current, lastTop.current, el);
          lastTop.current = el.scrollTop;
        }}
        className={cn("min-w-0", !expanded && BOUNDED_MESSAGE_CLASS)}
      >
        <div ref={content}>{children}</div>
      </div>
      {toggle && <button
        type="button"
        aria-expanded={expanded}
        onClick={() => setExpanded(value => !value)}
        className="mt-1 inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px] font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring"
      >
        <ChevronDown className={cn("size-3 transition-transform", expanded && "rotate-180")} aria-hidden="true" />
        {t(toggle === "collapse" ? "chat.collapseMessage" : "chat.expandMessage")}
      </button>}
    </>
  );
}

function MessageRow({ message, toolMessages, onRetry, retryDisabled }: { message: ChatMessage; toolMessages?: ChatMessage[]; onRetry?: () => void; retryDisabled?: boolean }) {
  const { t } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  // Open by default: thinking must be observable as it streams without a
  // click. The user's explicit toggle still wins for the mounted message.
  const [reasoningOpen, setReasoningOpen] = useState(true);
  
  if (message.role === "tool") {
    return null; // Tool messages are now rendered within their parent assistant message
  }
  const isUser = message.role === "user";
  return (
    <div className="flex flex-col gap-2.5">
      <div className="flex gap-2.5">
        <span
          className={cn(
            "flex size-6 shrink-0 items-center justify-center rounded-md border",
            isUser
              ? "border-border bg-muted text-muted-foreground"
              : "border-border bg-primary text-primary-foreground",
          )}
          aria-hidden="true"
        >
          {isUser ? <User className="size-3.5" /> : <Bot className="size-3.5" />}
        </span>
        <div className="min-w-0 flex-1">
          <div className="mb-1 flex items-center gap-2">
            <span className="text-[12px] font-semibold">{isUser ? t("chat.you") : t("chat.assistant")}</span>
            <span className="text-[11px] text-muted-foreground">{message.createdAt}</span>
            <button
              type="button"
              onClick={() => copyToClipboard(message.content)}
              disabled={message.content.length === 0}
              className="ml-auto inline-flex shrink-0 items-center gap-1 rounded px-1.5 py-1 text-[11px] text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring disabled:cursor-not-allowed disabled:opacity-40"
              aria-label={isUser ? t("chat.copyRawUserMessage") : t("chat.copyRawAssistantMessage")}
              title={t("chat.copyRawMessage")}
            >
              {isCopied ? <Check className="size-3" aria-hidden="true" /> : <Copy className="size-3" aria-hidden="true" />}
              <span role="status">{isCopied ? t("common.copied") : t("common.copy")}</span>
            </button>
          </div>
          {!isUser && message.reasoning && (
            <Collapsible open={reasoningOpen} onOpenChange={setReasoningOpen}>
              <div className="mb-2 overflow-hidden rounded-md border border-border bg-muted/20">
                <CollapsibleTrigger
                  className="flex w-full items-center gap-2 px-2.5 py-2 text-left"
                  aria-label={t("chat.toggleThinking")}
                >
                  <span className="flex size-6 shrink-0 items-center justify-center rounded border border-border bg-background">
                    <Brain className="size-3.5 text-muted-foreground" aria-hidden="true" />
                  </span>
                  <span className="min-w-0 flex-1 text-[12px] font-medium text-foreground">
                    {t("chat.thinking")}
                  </span>
                  <ChevronDown
                    className={cn("size-3.5 shrink-0 text-muted-foreground transition-transform", !reasoningOpen && "-rotate-90")}
                    aria-hidden="true"
                  />
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <div className="border-t border-border bg-background/40 px-2.5 py-2 text-[13px] leading-relaxed text-muted-foreground">
                    <Markdown content={message.reasoning} />
                  </div>
                </CollapsibleContent>
              </div>
            </Collapsible>
          )}
           <div
             className={cn(
               "text-sm text-foreground/90",
               isUser && "rounded-md border border-border bg-muted/30 px-3 py-2",
               message.status === "failed" && "text-red-600 dark:text-red-400",
               message.status === "cancelled" && "text-muted-foreground italic",
             )}
           >
             <BoundedMessage streaming={message.status === "streaming"}>
               <Markdown content={message.content} />
               <ImageGallery images={message.images} />
             </BoundedMessage>
             {message.status === "failed" && <p role="alert" className="mt-2 whitespace-pre-wrap break-words text-[12px]">
               {chatFailureReason(t, message)}
             </p>}
             {/* A stop the runtime made for a recorded cause, such as the
                 execution deadline, names that cause; an operator's stop has none. */}
             {message.status === "cancelled" && message.failureReason && <p className="mt-2 whitespace-pre-wrap break-words text-[12px]">
               {message.failureReason}
             </p>}
             {onRetry && <Button className="mt-2" size="xs" variant="outline" disabled={retryDisabled} onClick={onRetry}>{t("common.retry")}</Button>}
             {message.status !== "completed" && message.status !== "pending" && (
               <span className="mt-1 block text-[11px] text-muted-foreground">
                 {runtimeStateLabel(t, message.status)}
               </span>
             )}
           </div>
        </div>
      </div>
      {toolMessages && toolMessages.length > 0 && (
        <div className="flex gap-2.5">
          <div className="w-6 shrink-0" aria-hidden="true" />
          <div className="min-w-0 flex-1 space-y-2">
            {toolMessages.map(toolMsg => (
              <ToolBlock key={toolMsg.id} message={toolMsg} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/* ---------- composer ---------- */

function Composer({
  projectId,
  draft,
  onDraftChange,
  onIntent,
  runtimeStatus,
  busy,
  queueing,
  selection,
  onSelectionChange,
  onPreference,
  active,
  activity,
  onCancel,
}: {
  projectId?: string;
  busy: boolean;
  queueing: boolean;
  selection?: ChatModelSelection;
  onSelectionChange: (selection: ChatModelSelection) => void;
  onPreference: (selection: ChatModelSelection) => void;
  active: ActiveExecutionTarget | null;
  activity?: { current: ActivityStep | ExecutionPhase | null; steps: ActivityStep[] };
  onCancel?: () => Promise<void>;
  draft: string;
  onDraftChange: (v: string) => void;
  onIntent: (intent: ComposerIntent) => void | Promise<void>;
  runtimeStatus: RuntimeStatus;
}) {
  const ref = useRef<HTMLTextAreaElement>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const images = useImageAttachments(projectId);
  const [dragging, setDragging] = useState(false);
  const dragDepth = useRef(0);
  const { t } = useI18n();
  const [highlight, setHighlight] = useState<{ raw: string; index: number } | null>(null);
  const [dismissedFor, setDismissedFor] = useState<string | null>(null);
  const [commandError, setCommandError] = useState<{ raw: string; message: string } | null>(null);
  const chatReady = runtimeStatus.state === "connected";
  const actionLock = useRef(false);
  const [submitting, setSubmitting] = useState(false);

  // A drop that misses the composer must not navigate the page to the file.
  useEffect(() => {
    const block = (event: DragEvent) => {
      if (event.dataTransfer?.types.includes("Files")) event.preventDefault();
    };
    window.addEventListener("dragover", block);
    window.addEventListener("drop", block);
    return () => {
      window.removeEventListener("dragover", block);
      window.removeEventListener("drop", block);
    };
  }, []);
  const [cancelling, setCancelling] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 160)}px`;
  }, [draft]);

  const suggestions = matchComposerSuggestions(draft);
  const suggestionsOpen = suggestions.length > 0 && dismissedFor !== draft;
  const activeIndex = suggestionsOpen && highlight?.raw === draft && highlight.index < suggestions.length
    ? highlight.index
    : -1;
  const error = commandError && commandError.raw === draft ? commandError.message : null;
  const canSend = (draft.trim().length > 0 || images.attachments.length > 0) && images.ready;

  const closeSuggestions = () => {
    setHighlight(null);
    setDismissedFor(draft);
  };

  const applySuggestion = (suggestion: ComposerSuggestion) => {
    const applied = applyComposerSuggestion(suggestion);
    setHighlight(null);
    setDismissedFor(null);
    setCommandError(null);
    if (applied.action === "create-job") {
      onDraftChange("");
      onIntent(parseComposerIntent(applied.text));
      return;
    }
    onDraftChange(applied.text);
    ref.current?.focus();
  };

  const submit = async (mode: "queue" | "steer" = "queue") => {
    if (actionLock.current || !images.ready) return;
    const intent = parseComposerIntent(draft);
    if (intent.kind === "unknown-command") {
      setDismissedFor(draft);
      setCommandError({ raw: draft, message: intent.reason });
      return;
    }
    if (intent.kind !== "chat" && images.attachments.length) {
      setCommandError({ raw: draft, message: t("chat.imagesRequireChat") });
      return;
    }
    if (intent.kind === "chat" && intent.text.length === 0 && !images.images.length) return;
    if (intent.kind === "chat" && !chatReady) return;
    actionLock.current = true;
    setSubmitting(true);
    setCommandError(null);
    try {
      await onIntent(intent.kind === "chat" ? { ...intent, selection, mode, images: images.images } : intent);
      onDraftChange("");
      images.clear();
    } catch (cause) {
      setCommandError({ raw: draft, message: cause instanceof Error ? cause.message : String(cause) });
    } finally {
      actionLock.current = false;
      setSubmitting(false);
    }
  };

  const cancel = async () => {
    if (!onCancel || actionLock.current) return;
    actionLock.current = true;
    setCancelling(true);
    try {
      await onCancel();
    } catch (cause) {
      setCommandError({ raw: draft, message: cause instanceof Error ? cause.message : String(cause) });
    } finally {
      actionLock.current = false;
      setCancelling(false);
    }
  };

  return (
    <div className="shrink-0 bg-background px-3 pt-2 pb-[max(0.75rem,env(safe-area-inset-bottom))] sm:px-5">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
        onDragEnter={(event) => {
          if (!event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          dragDepth.current += 1;
          setDragging(true);
        }}
        onDragOver={(event) => {
          if (!event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          event.dataTransfer.dropEffect = submitting || cancelling ? "none" : "copy";
        }}
        onDragLeave={(event) => {
          event.preventDefault();
          dragDepth.current = Math.max(0, dragDepth.current - 1);
          if (dragDepth.current === 0) setDragging(false);
        }}
        onDrop={(event) => {
          if (!event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          dragDepth.current = 0;
          setDragging(false);
          if (!submitting && !cancelling) images.add(Array.from(event.dataTransfer.files));
        }}
        className="relative mx-auto max-w-3xl"
      >
        {dragging && <div className="pointer-events-none absolute inset-0 z-30 flex items-center justify-center rounded-lg border-2 border-dashed border-primary bg-background/95 text-sm font-medium" role="status">{t("chat.dropImages")}</div>}
        <input ref={fileInput} type="file" accept="image/png,image/jpeg,image/gif,image/webp" multiple hidden disabled={submitting || cancelling} aria-label={t("chat.attachFile")} onChange={(event) => {
          images.add(Array.from(event.target.files ?? []));
          event.target.value = "";
        }} />
        <ModelSelector selection={selection} onChange={onSelectionChange} onPreference={onPreference} busy={busy || submitting || cancelling} active={active} activity={activity} />
        <div className="rounded-lg border border-border bg-card shadow-[0_8px_30px_-12px_rgba(0,0,0,0.25)] transition-colors focus-within:border-ring">
          <AttachmentStaging state={images} disabled={submitting || cancelling} />
          <div className="relative">
            {suggestionsOpen && (
              <ul
                id="composer-suggestions"
                role="listbox"
                aria-label={t("chat.composerSuggestions")}
                className="absolute inset-x-1 bottom-full z-20 mb-1 overflow-hidden rounded-md border border-border bg-popover text-popover-foreground shadow-md"
              >
                {suggestions.map((suggestion, index) => {
                  const selected = activeIndex === index;
                  return (
                    <li key={suggestion.id} role="presentation">
                      <button
                        id={`composer-suggestion-${suggestion.id}`}
                        type="button"
                        role="option"
                        aria-selected={selected}
                        onMouseDown={(event) => event.preventDefault()}
                        onClick={() => applySuggestion(suggestion)}
                        className={cn(
                          "flex w-full items-start gap-2 px-2.5 py-2 text-left transition-colors hover:bg-muted",
                          selected && "bg-muted",
                        )}
                      >
                        <span className="mt-0.5 text-muted-foreground" aria-hidden="true">
                          {suggestion.action === "create-job"
                            ? <Sparkles className="size-3.5" />
                            : <Command className="size-3.5" />}
                        </span>
                        <span className="min-w-0 flex-1">
                          <span className="block text-[12px] font-medium">{suggestion.label}</span>
                          <span className="block truncate text-[11px] text-muted-foreground">{suggestion.description}</span>
                        </span>
                        {suggestion.command !== suggestion.label && (
                          <span className="mt-0.5 shrink-0 font-mono text-[10px] text-muted-foreground">{suggestion.command.trim()}</span>
                        )}
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
            <textarea
              ref={ref}
              value={draft}
              readOnly={submitting || cancelling}
              onChange={(e) => {
                setDismissedFor(null);
                onDraftChange(e.target.value);
              }}
              onPaste={(e) => {
                if (submitting || cancelling) return;
                const clipboard = e.clipboardData;
                if (!clipboard) return;
                // One image can arrive as both an item and a file. Collect once
                // and let the shared attachment state drop the duplicate.
                const pasted = [
                  ...Array.from(clipboard.items).flatMap(item => item.type.startsWith("image/") ? [item.getAsFile()] : []),
                  ...Array.from(clipboard.files),
                ].filter((file): file is File => file !== null && file.type.startsWith("image/"));
                if (pasted.length > 0) {
                  e.preventDefault();
                  images.add(pasted);
                }
              }}
              onKeyDown={(e) => {
                if (e.nativeEvent.isComposing || e.nativeEvent.keyCode === 229) return;
                if (e.key === "Escape" && suggestionsOpen) {
                  e.preventDefault();
                  closeSuggestions();
                  return;
                }
                if (suggestionsOpen && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
                  e.preventDefault();
                  setHighlight({
                    raw: draft,
                    index: moveComposerSuggestionIndex(activeIndex, e.key === "ArrowDown" ? 1 : -1, suggestions.length),
                  });
                  return;
                }
                if (e.key === "Enter" && !e.shiftKey) {
                  if (suggestionsOpen && activeIndex >= 0) {
                    e.preventDefault();
                    applySuggestion(suggestions[activeIndex]);
                    return;
                  }
                  if (window.matchMedia("(pointer: coarse)").matches && !e.ctrlKey && !e.metaKey) return;
                  e.preventDefault();
                  void submit();
                }
              }}
              rows={1}
              placeholder={t("chat.messagePlaceholder")}
              aria-label={t("chat.messageLabel")}
              role="combobox"
              aria-autocomplete="list"
              aria-expanded={suggestionsOpen}
              aria-controls={suggestionsOpen ? "composer-suggestions" : undefined}
              aria-activedescendant={activeIndex >= 0 ? `composer-suggestion-${suggestions[activeIndex].id}` : undefined}
              aria-describedby={chatReady ? "composer-hint" : "composer-hint composer-status"}
              aria-invalid={Boolean(error)}
              className="max-h-[min(10rem,25dvh)] min-h-11 w-full resize-none bg-transparent px-3 pt-2.5 pb-1 text-base outline-none placeholder:text-muted-foreground sm:text-sm [@media(max-height:600px)]:max-h-[18dvh]"
            />
          </div>
          {error && (
            <p role="alert" className="border-t border-border px-3 py-1.5 text-[11px] text-red-600 dark:text-red-400">
              {error}
            </p>
          )}
          <div className="flex flex-wrap items-center gap-1 px-2 pb-2">
            <Button
              type="button"
              variant="ghost"
              size="icon-xs"
              className="size-11 rounded-md sm:size-9"
              aria-label={t("chat.attachFile")}
              title={t("chat.attachFile")}
              disabled={submitting || cancelling}
              onClick={() => fileInput.current?.click()}
            >
              <Paperclip className="size-4" />
            </Button>
            <span id="composer-hint" className="sr-only sm:ml-1 sm:not-sr-only sm:text-[11px] sm:text-muted-foreground">
              <span className="hidden pointer-coarse:inline">{t("chat.touchHint")}</span>
              <span className="pointer-coarse:hidden">{busy ? t("chat.queueHint") : t("chat.hint")}</span>
            </span>
            <div className="ml-auto flex flex-wrap items-center justify-end gap-1">
              {busy && onCancel && <Button type="button" variant="outline" size="xs" className="h-11 rounded-md px-2 tracking-normal normal-case sm:h-9" disabled={submitting || cancelling} onClick={() => void cancel()} title={t("chat.cancel")}>
                {cancelling ? <Loader2 className="size-3 animate-spin" /> : <Square className="size-3" />}{t("chat.cancel")}
              </Button>}
              {busy && <Button type="button" variant="outline" size="xs" className="h-11 rounded-md px-2 tracking-normal normal-case sm:h-9" disabled={!canSend || !chatReady || submitting || cancelling} onClick={() => void submit("steer")} title={t("chat.steerHint")}>
                {t("chat.steer")}
              </Button>}
              <Button type="submit" size={queueing ? "xs" : "icon-sm"} className={cn("h-11 rounded-md tracking-normal normal-case sm:h-9", !queueing && "w-11 sm:w-9")} disabled={!canSend || submitting || cancelling || (!chatReady && parseComposerIntent(draft).kind === "chat")} aria-label={t(queueing ? "chat.queue" : "chat.send")} title={t(busy ? "chat.queueHint" : "chat.send")}>
                {submitting ? <Loader2 className="size-4 animate-spin" /> : busy ? <ListPlus className="size-4" /> : <ArrowUp className="size-4" />}
                {queueing && t("chat.queue")}
              </Button>
            </div>
          </div>
        </div>
        {!chatReady && <p id="composer-status" role="status" className="mt-1.5 text-center text-xs text-muted-foreground">
          {`${t("chat.unavailable")} · ${runtimeStatusDetail(t, runtimeStatus) ?? t("chat.unavailableFallback")}`}
        </p>}
      </form>
    </div>
  );
}

/* ---------- chat view ---------- */

type ChatViewProps = {
  session: ChatSession;
  messages: ChatMessage[];
  /** The canonical execution of this Chat's latest Job, when one is known. */
  execution?: JobExecution | null;
  runtimeStatus: RuntimeStatus;
  onComposerIntent: (intent: ComposerIntent) => void | Promise<void>;
  onCancel?: () => Promise<void>;
  queueState?: { queue: QueuedChatMessage[]; paused: boolean };
  onRemoveQueued?: (id: string) => void;
  onResumeQueue?: () => void;
  onRetryMessage: (messageId: string) => Promise<void>;
  /** Structured command surfaces are composed by the shell, not selected here. */
  composerSurface?: React.ReactNode;
  composerSurfaceKey?: string | null;
};

const SUGGESTIONS = [
  "chat.suggestion.summary", "chat.suggestion.plan", "chat.suggestion.review",
] as const;

export function ChatView({
  session,
  messages,
  execution,
  runtimeStatus,
  onComposerIntent,
  onRetryMessage,
  onCancel,
  queueState,
  onRemoveQueued,
  onResumeQueue,
  composerSurface,
  composerSurfaceKey,
}: ChatViewProps) {
  const [draft, setDraft] = useState("");
  // ChatView is keyed per Chat, so the saved next-turn preference is read once per Chat.
  const [selection, setSelection] = useState<ChatModelSelection | undefined>(() => readModelPreference(session.projectId, session.sessionId ?? session.id));
  const live = messages.findLast(message => message.role === "assistant" && (message.status === "streaming" || message.status === "pending"));
  // The running turn's own Job is the only source of what is executing.
  const liveExecution = isExecutionActive(execution) && (!live?.jobId || live.jobId === execution.jobId) ? execution : null;
  const busy = Boolean(live) || liveExecution !== null;
  const active = activeExecutionTarget(liveExecution);
  const activity = useMemo(() => {
    if (!liveExecution) return undefined;
    const steps = executionActivity(liveExecution);
    return { steps, current: currentActivity(liveExecution, steps) };
  }, [liveExecution]);
  const { t } = useI18n();
  const scrollRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  const lastScrollTop = useRef(0);
  const [showLatest, setShowLatest] = useState(false);
  const retryLock = useRef(false);
  const [retrying, setRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);

  async function retry(messageId: string) {
    if (retryLock.current || !retryInput(messages, messageId)) return;
    retryLock.current = true;
    setRetrying(true);
    setRetryError(null);
    try {
      await onRetryMessage(messageId);
    } catch (cause) {
      setRetryError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      retryLock.current = false;
      setRetrying(false);
    }
  }

  const empty = messages.length === 0 && !composerSurface;
  const scrollToLatest = () => {
    following.current = true;
    setShowLatest(false);
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
      lastScrollTop.current = el.scrollTop;
    }
  };

  useLayoutEffect(() => {
    following.current = true;
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
      lastScrollTop.current = el.scrollTop;
    }
  }, [session.id]);

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (el && following.current) {
      el.scrollTop = el.scrollHeight;
      lastScrollTop.current = el.scrollTop;
    }
  }, [messages, composerSurfaceKey]);

  useEffect(() => {
    const el = scrollRef.current;
    const content = contentRef.current;
    if (!el || !content) return;
    const followContent = () => {
      if (following.current) {
        el.scrollTop = el.scrollHeight;
        lastScrollTop.current = el.scrollTop;
      }
      setShowLatest(!following.current && el.scrollHeight - el.clientHeight - el.scrollTop > 80);
    };
    const observer = new ResizeObserver(followContent);
    observer.observe(content);
    observer.observe(el);
    followContent();
    return () => observer.disconnect();
  }, [session.id, empty, composerSurfaceKey]);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="relative flex min-h-0 flex-1 flex-col">
        <div ref={scrollRef} onScroll={() => {
          const el = scrollRef.current;
          if (!el) return;
          const atBottom = el.scrollHeight - el.clientHeight - el.scrollTop <= 80;
          if (atBottom) following.current = true;
          else if (el.scrollTop < lastScrollTop.current) following.current = false;
          lastScrollTop.current = el.scrollTop;
          setShowLatest(!following.current && !atBottom);
        }} className="min-h-0 flex-1 overflow-y-auto overscroll-contain [overflow-anchor:none]" role="log" aria-label={t("chat.conversation", { title: session.title })}>
          {empty ? (
            <div ref={contentRef} className="relative mx-auto flex min-h-full max-w-3xl flex-col items-center justify-center px-5 py-8 text-center">
              <ActivityPulse className="size-20 opacity-80" label={t("chat.idleIllustration")} />
              <h2 className="mt-5 text-[15px] font-semibold tracking-tight">
                {t("chat.newThread")}
              </h2>
              <p className="mt-1 max-w-md text-[13px] leading-6 text-muted-foreground">
                {t("chat.emptyPrefix")} <span className="font-medium text-foreground">{session.title}</span>{t("chat.emptySuffix")}
              </p>
              <div className="mt-4 flex w-full max-w-md flex-col gap-1.5">
                {SUGGESTIONS.map((s) => (
                  <button
                    key={s}
                    type="button"
                    onClick={() => setDraft(t(s))}
                    className="min-h-11 rounded-md border border-border bg-muted/30 px-3 py-2 text-left text-sm text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30"
                  >
                    {t(s)}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div ref={contentRef} className="mx-auto flex max-w-3xl flex-col gap-5 px-3 py-5 sm:px-5">
              {messages.map((m, index) => {
                if (m.role === "tool") return null; // Skip tool messages, they'll be rendered with their parent
                // Find tool messages that follow this assistant message
                const toolMessages: ChatMessage[] = [];
                if (m.role === "assistant") {
                  let i = index + 1;
                  while (i < messages.length && messages[i].role === "tool") {
                    toolMessages.push(messages[i]);
                    i++;
                  }
                }
                return (
                  <MessageRow key={m.id} message={m}
                    toolMessages={toolMessages.length > 0 ? toolMessages : undefined}
                    onRetry={retryInput(messages, m.id) ? () => void retry(m.id) : undefined}
                    retryDisabled={retrying || runtimeStatus.state !== "connected"}
                  />
                );
              })}
              {retryError && <p role="alert" className="text-sm text-destructive">{retryError}</p>}
              {composerSurface}
            </div>
          )}
        </div>
        {showLatest && <Button type="button" size="sm" variant="secondary" onClick={scrollToLatest} className="absolute bottom-3 left-1/2 h-11 -translate-x-1/2 rounded-md border border-border tracking-normal normal-case shadow-md">
          <ChevronDown className="size-4" />{t("chat.jumpToLatest")}
        </Button>}
      </div>
      {!!queueState?.queue.length && (
        <div className="mx-auto w-full max-w-3xl shrink-0 border-t border-border px-3 py-2 sm:px-5" aria-label={t("chat.queue")}>
          <div className="mb-1 flex items-center gap-2 text-[11px] text-muted-foreground" role="status">
            <ListPlus className="size-3.5" />{t("chat.queueCount", { count: queueState.queue.length })}
            {queueState.paused && <><span>· {t("chat.queuePaused")}</span><Button type="button" size="xs" variant="ghost" disabled={runtimeStatus.state !== "connected"} onClick={onResumeQueue}>{t("chat.resumeQueue")}</Button></>}
          </div>
          <ol className="max-h-[min(7rem,15dvh)] overflow-y-auto">
            {queueState.queue.map((item, index) => <li key={item.id} className="flex items-center gap-2 py-1 text-xs">
              <span className="text-muted-foreground">{index + 1}.</span><span className="min-w-0 flex-1 truncate">{item.input.content || t("chat.imageCount", { count: item.input.images?.length ?? 0 })}</span>
              <ImageGallery images={item.input.images} compact />
              <Button type="button" size="icon-xs" variant="ghost" className="size-11 rounded-md sm:size-7" aria-label={t("chat.removeQueued")} onClick={() => onRemoveQueued?.(item.id)}><X className="size-3" /></Button>
            </li>)}
          </ol>
        </div>
      )}
      <Composer
        key={`${session.id}:${session.projectId ?? ""}`}
        projectId={session.projectId}
        busy={busy}
        queueing={busy || Boolean(queueState?.queue.length)}
        selection={selection}
        onSelectionChange={setSelection}
        onPreference={next => writeModelPreference(session.projectId, session.sessionId ?? session.id, next)}
        active={active}
        activity={activity}
        onCancel={onCancel}
        draft={draft}
        onDraftChange={setDraft}
        onIntent={async intent => {
          scrollToLatest();
          await onComposerIntent(intent);
          scrollToLatest();
        }}
        runtimeStatus={runtimeStatus}
      />
    </div>
  );
}
