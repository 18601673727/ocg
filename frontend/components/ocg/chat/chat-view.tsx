"use client";

import { useEffect, useRef, useState } from "react";
import {
  ArrowUp,
  Bot,
  Check,
  ChevronDown,
  Command,
  Copy,
  Loader2,
  Mic,
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
import { ActivityPulse } from "../activity-pulse";
import type { ChatMessage, ChatSession, Mission, RuntimeStatus } from "../types";
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
  return (
    <div className="overflow-hidden rounded-md border border-border bg-muted/40">
      <div className="flex items-center gap-2 border-b border-border px-2.5 py-1.5">
        <span className="font-mono text-[11px] text-muted-foreground">
          {language || "code"}
        </span>
        <button
          type="button"
          onClick={() => copyToClipboard(code)}
          className="ml-auto flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px] text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          aria-label={isCopied ? "Copied" : "Copy code to clipboard"}
          title="Copy code"
        >
          {isCopied ? <Check className="size-3" /> : <Copy className="size-3" />}
          {isCopied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre className="overflow-x-auto p-2.5 font-mono text-[12px] leading-5 text-foreground">
        <code>{code}</code>
      </pre>
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
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.startsWith("- ")) {
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
  if (!tool) return null;
  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="overflow-hidden rounded-md border border-border bg-muted/30">
        <CollapsibleTrigger
          className="flex w-full items-center gap-2 px-2.5 py-2 text-left"
          aria-label={`${tool.name} ${tool.status}. Toggle details.`}
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
              "flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[11px]",
              (tool.status === "success" || tool.status === "pending") && "border-border text-muted-foreground",
              tool.status === "running" && "border-amber-500/30 text-amber-600 dark:text-amber-400",
              tool.status === "retrying" && "border-sky-500/30 text-sky-600 dark:text-sky-400",
              tool.status === "waiting-approval" && "border-violet-500/30 text-violet-600 dark:text-violet-400",
              tool.status === "failure" && "border-red-500/30 text-red-600 dark:text-red-400",
            )}
          >
            {tool.status === "running" || tool.status === "retrying" ? (
              <Loader2 className="size-3 animate-spin" aria-hidden="true" />
            ) : tool.status === "failure" ? (
              <span className="size-1.5 rounded-full bg-red-500" aria-hidden="true" />
            ) : tool.status === "waiting-approval" ? (
              <span className="size-1.5 rounded-full bg-violet-500" aria-hidden="true" />
            ) : (
              <span className="size-1.5 rounded-full bg-emerald-500" aria-hidden="true" />
            )}
            {tool.status === "success" ? "success" : tool.status} · {tool.durationMs}ms
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

function MessageRow({ message }: { message: ChatMessage }) {
  if (message.role === "tool") {
    return (
      <div className="flex gap-2.5">
        <div className="w-6 shrink-0" aria-hidden="true" />
        <div className="min-w-0 flex-1">
          <ToolBlock message={message} />
        </div>
      </div>
    );
  }
  const isUser = message.role === "user";
  return (
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
        <p className="mb-1 flex items-baseline gap-2">
          <span className="text-[12px] font-semibold">{isUser ? "You" : "Assistant"}</span>
          <span className="text-[11px] text-muted-foreground">{message.createdAt}</span>
          {!isUser && (
            <span className="text-[11px] text-muted-foreground">· mock</span>
          )}
        </p>
         <div
           className={cn(
             "text-[13.5px] text-foreground/90",
             isUser && "rounded-md border border-border bg-muted/30 px-3 py-2",
             message.status === "failed" && "text-red-600 dark:text-red-400",
             message.status === "cancelled" && "text-muted-foreground italic",
           )}
         >
           <Markdown content={message.content} />
           {message.status !== "completed" && message.status !== "pending" && (
             <span className="mt-1 block text-[11px] text-muted-foreground">
               {message.status === "streaming" ? "streaming…" : message.status}
             </span>
           )}
         </div>
      </div>
    </div>
  );
}

/* ---------- composer ---------- */

function Composer({
  draft,
  onDraftChange,
  onIntent,
}: {
  draft: string;
  onDraftChange: (v: string) => void;
  onIntent: (intent: ComposerIntent) => void;
}) {
  const ref = useRef<HTMLTextAreaElement>(null);
  const [highlight, setHighlight] = useState<{ raw: string; index: number } | null>(null);
  const [dismissedFor, setDismissedFor] = useState<string | null>(null);
  const [commandError, setCommandError] = useState<{ raw: string; message: string } | null>(null);

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
  const canSend = draft.trim().length > 0;

  const closeSuggestions = () => {
    setHighlight(null);
    setDismissedFor(draft);
  };

  const applySuggestion = (suggestion: ComposerSuggestion) => {
    const applied = applyComposerSuggestion(suggestion);
    setHighlight(null);
    setDismissedFor(null);
    setCommandError(null);
    if (applied.action === "create-mission") {
      onDraftChange("");
      onIntent(parseComposerIntent(applied.text));
      return;
    }
    onDraftChange(applied.text);
    ref.current?.focus();
  };

  const submit = () => {
    const intent = parseComposerIntent(draft);
    if (intent.kind === "unknown-command") {
      setDismissedFor(draft);
      setCommandError({ raw: draft, message: intent.reason });
      return;
    }
    if (intent.kind === "chat" && intent.text.length === 0) return;
    setCommandError(null);
    onDraftChange("");
    onIntent(intent);
  };

  return (
    <div className="shrink-0 border-t border-border bg-background px-3 pt-2 pb-2 sm:px-5 sm:pb-3">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
        className="mx-auto max-w-3xl"
      >
        <div className="rounded-lg border border-border bg-background shadow-[0_1px_2px_rgba(0,0,0,0.04)] transition-colors focus-within:border-ring">
          <div className="relative">
            {suggestionsOpen && (
              <ul
                id="composer-suggestions"
                role="listbox"
                aria-label="Composer suggestions"
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
                          {suggestion.action === "create-mission"
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
              onChange={(e) => {
                setDismissedFor(null);
                onDraftChange(e.target.value);
              }}
              onKeyDown={(e) => {
                if (e.nativeEvent.isComposing) return;
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
                  e.preventDefault();
                  if (suggestionsOpen && activeIndex >= 0) {
                    applySuggestion(suggestions[activeIndex]);
                    return;
                  }
                  submit();
                }
              }}
              rows={1}
              placeholder="Message OCG… (type / for Mission commands)"
              aria-label="Message OCG"
              role="combobox"
              aria-autocomplete="list"
              aria-expanded={suggestionsOpen}
              aria-controls={suggestionsOpen ? "composer-suggestions" : undefined}
              aria-activedescendant={activeIndex >= 0 ? `composer-suggestion-${suggestions[activeIndex].id}` : undefined}
              className="max-h-40 min-h-11 w-full resize-none bg-transparent px-3 pt-2.5 pb-1 text-[13.5px] outline-none placeholder:text-muted-foreground"
            />
          </div>
          {error && (
            <p role="alert" className="border-t border-border px-3 py-1.5 text-[11px] text-red-600 dark:text-red-400">
              {error}
            </p>
          )}
          <div className="flex items-center gap-1 px-2 pb-2">
            <Button
              type="button"
              variant="ghost"
              size="icon-xs"
              aria-label="Attach file (placeholder)"
              title="Attach file (placeholder)"
            >
              <Paperclip className="size-4" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon-xs"
              aria-label="Voice input (placeholder)"
              title="Voice input (placeholder)"
            >
              <Mic className="size-4" />
            </Button>
            <span className="ml-1 hidden text-[11px] text-muted-foreground sm:inline">
              / for Mission · Enter to send · Shift+Enter for newline
            </span>
            <Button
              type="submit"
              size="icon-sm"
              disabled={!canSend}
              aria-label="Send message"
              title="Send message"
              className="ml-auto rounded-md"
            >
              <ArrowUp className="size-4" />
            </Button>
          </div>
        </div>
        <p className="mt-1.5 text-center text-[11px] text-muted-foreground">
          Mock composer — messages stay local. No backend, SSE, or model calls in Phase 1.
        </p>
      </form>
    </div>
  );
}

/* ---------- chat view ---------- */

type ChatViewProps = {
  session: ChatSession;
  messages: ChatMessage[];
  runtimeStatus: RuntimeStatus;
  mission?: Mission | null;
  onComposerIntent: (intent: ComposerIntent) => void;
  /** Structured command surfaces are composed by the shell, not selected here. */
  composerSurface?: React.ReactNode;
  composerSurfaceKey?: string | null;
};

const SUGGESTIONS = [
  "Summarize the open decisions in this thread",
  "Draft the layout grid for Sidebar | Chat | Mission",
  "List what Phase 2 needs from this mock state",
];

export function ChatView({
  session,
  messages,
  runtimeStatus,
  mission,
  onComposerIntent,
  composerSurface,
  composerSurfaceKey,
}: ChatViewProps) {
  const [draft, setDraft] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages.length, session.id, composerSurfaceKey]);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto" role="log" aria-label={`Conversation: ${session.title}`}>
        {messages.length === 0 && !composerSurface ? (
          <div className="relative mx-auto flex h-full max-w-3xl flex-col items-center justify-center px-5 py-10 text-center">
            <ActivityPulse className="size-20 opacity-80" label="OCG idle illustration" />
            <h2 className="mt-5 text-[15px] font-semibold tracking-tight">
              Start a new thread
            </h2>
            <p className="mt-1 max-w-md text-[13px] leading-6 text-muted-foreground">
              This is the mock ChatView for <span className="font-medium text-foreground">{session.title}</span>.
              Nothing leaves the browser — pick a suggestion or write below.
            </p>
            <div className="mt-4 flex w-full max-w-md flex-col gap-1.5">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  type="button"
                  onClick={() => setDraft(s)}
                  className="rounded-md border border-border bg-muted/30 px-3 py-2 text-left text-[13px] text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="mx-auto flex max-w-3xl flex-col gap-5 px-3 py-5 sm:px-5">
            {messages.length > 0 && (
              <div className="flex items-center gap-2 text-[11px] text-muted-foreground" aria-hidden="true">
                <span className="h-px flex-1 bg-border" />
                <span>Today · mock history</span>
                <span className="h-px flex-1 bg-border" />
              </div>
            )}
            {messages.map((m) => (
              <MessageRow key={m.id} message={m} />
            ))}
            {messages.length > 0 && (
              <div className="flex items-center gap-2 rounded-md border border-dashed border-border bg-muted/20 px-2.5 py-2 text-[12px] text-muted-foreground">
                {runtimeStatus.state === "connected" ? (
                  <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />
                ) : (
                  <span className="size-1.5 rounded-full bg-muted-foreground" aria-hidden="true" />
                )}
                <span>
                  Runtime <span className="font-medium">{runtimeStatus.state}</span>
                  {mission ? ` · Mission ${mission.status}` : " · no Mission loaded"}
                  {runtimeStatus.detail ? ` · ${runtimeStatus.detail}` : ""}
                </span>
              </div>
            )}
            {composerSurface}
          </div>
        )}
      </div>
      <Composer
        draft={draft}
        onDraftChange={setDraft}
        onIntent={onComposerIntent}
      />
    </div>
  );
}
