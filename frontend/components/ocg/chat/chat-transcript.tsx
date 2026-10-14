"use client";

import { useState } from "react";
import { Bot, Brain, Check, ChevronDown, Copy, Loader2, User, Wrench } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { BORDER_TONE, StatusDot, STATUS_TEXT, TEXT_TONE, TOOL_STATUS } from "@/components/ocg/primitives";
import { useI18n, runtimeStateLabel, chatFailureReason } from "../i18n";
import type { ChatMessage } from "../types";
import { BoundedMessage } from "./bounded-message";
import { ImageGallery } from "./image-attachments";
import { MarkdownRenderer } from "./markdown-renderer";

/* ---------- tool / activity block ---------- */

function ActivityBlock({ message }: { message: ChatMessage }) {
  const activity = message.role === "call" ? message.call : message.tool;
  const name = activity?.name ?? message.role;
  const status = activity?.status ?? message.status;
  const summary = activity?.summary ?? message.content;
  const [open, setOpen] = useState(true);
  const { t } = useI18n();
  const visualStatus = status === "completed" || status === "succeeded" ? "success"
    : status === "failed" ? "failure" : status;
  const tone = Object.entries(TOOL_STATUS).find(([key]) => key === visualStatus)?.[1].tone ?? "slate";
  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="overflow-hidden rounded-md border border-border bg-muted/30">
        <CollapsibleTrigger
          className="flex w-full items-center gap-2 px-2.5 py-2 text-left"
          aria-label={t("chat.toolToggleDetails", { name, status })}
        >
          <span className="flex size-6 shrink-0 items-center justify-center rounded border border-border bg-background">
            <Wrench className="size-3.5 text-muted-foreground" aria-hidden="true" />
          </span>
          <span className="min-w-0 flex-1">
            <span className="block truncate font-mono text-xs font-medium">
              {name}
            </span>
            <span className="block truncate text-xs text-muted-foreground">
              {summary}
            </span>
          </span>
          <span
           className={cn(
              "flex shrink-0 items-center gap-1 rounded-md border px-1.5 py-0.5 text-xs",
              BORDER_TONE[tone],
              TEXT_TONE[tone],
            )}
          >
            {status === "running" || status === "retrying" ? (
              <Loader2 className="size-3 animate-spin" aria-hidden="true" />
            ) : (
              <StatusDot tone={tone} />
            )}
            {runtimeStateLabel(t, status === "succeeded" ? "completed" : status)}
            {activity?.durationMs != null && <> · {activity.durationMs}ms</>}
          </span>
          <ChevronDown
            className={cn("size-3.5 shrink-0 text-muted-foreground transition-transform", !open && "-rotate-90")}
            aria-hidden="true"
          />
        </CollapsibleTrigger>
        <CollapsibleContent>
          {activity?.detail && <pre className="overflow-x-auto border-t border-border bg-background/60 p-2.5 font-mono text-xs leading-5 text-muted-foreground">
            {activity.detail}
          </pre>}
        </CollapsibleContent>
      </div>
    </Collapsible>
  );
}

/* ---------- message row ---------- */

function MessageRow({ message, activityMessages, onRetry, retryDisabled }: { message: ChatMessage; activityMessages?: ChatMessage[]; onRetry?: () => void; retryDisabled?: boolean }) {
  const { t } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  // Open by default: thinking must be observable as it streams without a
  // click. The user's explicit toggle still wins for the mounted message.
  const [reasoningOpen, setReasoningOpen] = useState(true);

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
            <span className="text-xs font-semibold">{isUser ? t("chat.you") : t("chat.assistant")}</span>
            <span className="text-xs text-muted-foreground">{message.createdAt}</span>
            <button
              type="button"
              onClick={() => copyToClipboard(message.content)}
              disabled={message.content.length === 0}
              className="ml-auto inline-flex shrink-0 items-center gap-1 rounded px-1.5 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring disabled:cursor-not-allowed disabled:opacity-40"
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
                  <span className="min-w-0 flex-1 text-xs font-medium text-foreground">
                    {t("chat.thinking")}
                  </span>
                  <ChevronDown
                    className={cn("size-3.5 shrink-0 text-muted-foreground transition-transform", !reasoningOpen && "-rotate-90")}
                    aria-hidden="true"
                  />
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <div className="border-t border-border bg-background/40 px-2.5 py-2 text-[13px] leading-6 text-muted-foreground">
                    <MarkdownRenderer content={message.reasoning} inMuted={true} />
                  </div>
                </CollapsibleContent>
              </div>
            </Collapsible>
          )}
           <div
             className={cn(
               "text-base leading-7 text-foreground/90",
               isUser && "rounded-md border border-border bg-muted/30 px-3 py-2",
               message.status === "failed" && STATUS_TEXT['error'],
               message.status === "cancelled" && "text-muted-foreground italic",
             )}
           >
             <BoundedMessage streaming={message.status === "streaming"}>
               <MarkdownRenderer content={message.content} />
               <ImageGallery images={message.images} />
             </BoundedMessage>
             {message.status === "failed" && <p role="alert" className="mt-2 whitespace-pre-wrap break-words text-xs">
               {chatFailureReason(t, message)}
             </p>}
             {/* A stop the runtime made for a recorded cause, such as the
                 execution deadline, names that cause; an operator's stop has none. */}
             {message.status === "cancelled" && message.failureReason && <p className="mt-2 whitespace-pre-wrap break-words text-xs">
               {message.failureReason}
             </p>}
             {onRetry && <Button className="mt-2" size="xs" variant="outline" disabled={retryDisabled} onClick={onRetry}>{t("common.retry")}</Button>}
             {message.status !== "completed" && message.status !== "pending" && (
               <span className="mt-1 block text-xs text-muted-foreground">
                 {runtimeStateLabel(t, message.status)}
               </span>
             )}
           </div>
        </div>
      </div>
      {activityMessages && activityMessages.length > 0 && <ActivityGroup messages={activityMessages} />}
    </div>
  );
}

function ActivityGroup({ messages }: { messages: ChatMessage[] }) {
  return (
    <div className="flex gap-2.5">
      <div className="w-6 shrink-0" aria-hidden="true" />
      <div className="min-w-0 flex-1 space-y-2">
        {messages.map(message => <ActivityBlock key={message.id} message={message} />)}
      </div>
    </div>
  );
}

export function ChatTranscript({ messages, retryableMessageIds, onRetryMessage, retryDisabled }: {
  messages: readonly ChatMessage[];
  retryableMessageIds?: ReadonlySet<string>;
  onRetryMessage?: (messageId: string) => void;
  retryDisabled?: boolean;
}) {
  const rows: { message: ChatMessage; activities: ChatMessage[] }[] = [];
  for (const message of messages) {
    const previous = rows.at(-1);
    if ((message.role === "call" || message.role === "tool") && previous?.message.role === "assistant") {
      previous.activities.push(message);
    } else {
      rows.push({ message, activities: [] });
    }
  }

  return rows.map(({ message, activities }) => (
    message.role === "call" || message.role === "tool"
      ? <ActivityGroup key={message.id} messages={[message]} />
      : <MessageRow
          key={message.id}
          message={message}
          activityMessages={activities}
          onRetry={onRetryMessage && retryableMessageIds?.has(message.id) ? () => onRetryMessage(message.id) : undefined}
          retryDisabled={retryDisabled}
        />
  ));
}
