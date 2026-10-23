"use client";

import { createContext, useCallback, useContext, useEffect, useId, useMemo, useRef, useState, type ReactNode } from "react";
import { AlertCircle, ChevronDown, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import { dismissNotice, isNoticeDismissed, noticeIdentity } from "@/lib/notice-dismissal";

type Retry = { label: string; run: () => void };
type ConversationError = { id: number; message: string; at: number; retry?: Retry; identity?: string; dismissalKey: string; dismissed?: boolean };
type Histories = Record<string, readonly ConversationError[]>;
type ErrorStore = {
  histories: Histories;
  report: (conversation: string, cause: unknown, retry?: Retry, identity?: string) => void;
  dismiss: (conversation: string, through: number) => void;
  transfer: (from: string, to: string) => void;
};

const ErrorContext = createContext<ErrorStore | null>(null);
const ConversationContext = createContext<string | null>(null);

export function ConversationErrorsProvider({ children }: { children: ReactNode }) {
  const [histories, setHistories] = useState<Histories>({});
  const sequence = useRef(0);
  const report = useCallback((conversation: string, cause: unknown, retry?: Retry, identity?: string) => {
    const message = cause instanceof Error ? cause.message : String(cause);
    if (!message.trim()) return;
    const error = { id: ++sequence.current, message, at: Date.now(), retry, identity };
    void noticeIdentity(identity ? `error:${identity}` : `conversation:${conversation}`, message).then(dismissalKey => {
      const dismissed = isNoticeDismissed(dismissalKey);
      setHistories(current => {
        const previous = current[conversation] ?? [];
        const existing = previous.find(item => item.dismissalKey === dismissalKey);
        if (existing) {
          if (retry && !existing.retry) return { ...current, [conversation]: previous.map(item => item === existing ? { ...item, retry } : item) };
          return current;
        }
        return { ...current, [conversation]: [...previous, { ...error, dismissalKey, dismissed }].sort((a, b) => a.id - b.id) };
      });
    });
  }, []);
  const dismiss = useCallback((conversation: string, through: number) => {
    for (const error of histories[conversation] ?? []) {
      if (error.id <= through) dismissNotice(error.dismissalKey);
    }
    setHistories(current => {
      const history = current[conversation];
      if (!history?.some(error => error.id <= through && !error.dismissed)) return current;
      return { ...current, [conversation]: history.map(error => error.id <= through ? { ...error, dismissed: true } : error) };
    });
  }, [histories]);
  const transfer = useCallback((from: string, to: string) => {
    if (from === to) return;
    setHistories(current => {
      if (!current[from]?.length) return current;
      const next = { ...current, [to]: [...(current[to] ?? []), ...current[from]].sort((a, b) => a.id - b.id) };
      delete next[from];
      return next;
    });
  }, []);
  const value = useMemo(() => ({ histories, report, dismiss, transfer }), [histories, report, dismiss, transfer]);
  return <ErrorContext.Provider value={value}>{children}</ErrorContext.Provider>;
}

export const ConversationErrorScope = ConversationContext.Provider;

export function useConversationErrors() {
  return useContext(ErrorContext);
}

export function useConversationError(message: string | null | undefined, identity?: string) {
  const store = useConversationErrors();
  const conversation = useContext(ConversationContext);
  const report = store?.report;
  useEffect(() => {
    if (conversation && report && message) report(conversation, message, undefined, identity);
  }, [conversation, report, message, identity]);
  return Boolean(conversation && report);
}

export function ConversationErrorHeader() {
  const store = useConversationErrors();
  const conversation = useContext(ConversationContext);
  const { t, locale } = useI18n();
  const [expanded, setExpanded] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const panelId = useId();
  const history = conversation ? store?.histories[conversation] : undefined;
  const latest = history?.at(-1);
  const active = history?.findLast(error => !error.dismissed);

  useEffect(() => {
    if (!expanded) return;
    const dismiss = (event: PointerEvent) => {
      if (event.target instanceof Node && !root.current?.contains(event.target)) setExpanded(false);
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, [expanded]);

  if (!latest || !history || !active) return null;
  return <div ref={root} className="pointer-events-none relative w-full min-w-0 max-w-3xl" data-slot="conversation-errors"
    onKeyDown={event => {
      if (event.key === "Escape" && expanded) {
        event.stopPropagation(); setExpanded(false); trigger.current?.focus();
      }
    }}>
    <div className={cn("pointer-events-auto flex h-11 min-w-0 items-center rounded-xl border bg-background/90 text-xs shadow-sm backdrop-blur-sm", active ? "border-destructive/20 text-destructive" : "ml-auto w-fit max-w-full border-border text-muted-foreground")}>
    <button ref={trigger} type="button" className="flex h-full min-w-0 flex-1 items-center gap-2 rounded-xl px-3 text-left hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
      aria-expanded={expanded} aria-controls={panelId} aria-label={t(expanded ? "chat.errors.hide" : "chat.errors.show", { count: history.length })}
      onClick={() => setExpanded(value => !value)}>
      <AlertCircle className="size-4 shrink-0" aria-hidden="true" />
      {active ? <span className="min-w-0 flex-1 truncate" role="status" title={active.message}>{active.message}</span> : <span>{t("chat.errors.label")}</span>}
      <span className="shrink-0 tabular-nums">{history.length}</span>
      <ChevronDown aria-hidden="true" className={cn("size-4 shrink-0 transition-transform duration-200 motion-reduce:transition-none", !expanded && "-rotate-90")} />
    </button>
    {active && <button type="button" className="flex h-full w-11 shrink-0 items-center justify-center rounded-xl hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring" aria-label={t("chat.errors.dismiss")} title={t("chat.errors.dismiss")} onClick={() => {
      if (conversation) store?.dismiss(conversation, latest.id);
      setExpanded(false);
    }}><X className="size-4" aria-hidden="true" /></button>}
    </div>
    {expanded && <section id={panelId} aria-label={t("chat.errors.history")} className="pointer-events-auto absolute inset-x-0 top-full z-30 mt-2 overflow-hidden rounded-lg border border-border bg-popover text-popover-foreground shadow-lg">
      <h2 className="border-b border-border px-3 py-2 text-xs font-medium">{t("chat.errors.history")}</h2>
      <ol className="ocg-subtle-scrollbar max-h-[min(24rem,60dvh)] overflow-y-auto overscroll-contain divide-y divide-border">
        {[...history].reverse().map((error, index) => <li key={error.id} className="space-y-1 px-3 py-2.5 text-xs">
          <time dateTime={new Date(error.at).toISOString()} className="text-muted-foreground">{new Date(error.at).toLocaleTimeString(locale)}</time>
          <p className="whitespace-pre-wrap break-words text-destructive">{error.message}</p>
          {index === 0 && error.retry && <button type="button" className="mt-1 underline underline-offset-2" onClick={error.retry.run}>{error.retry.label}</button>}
        </li>)}
      </ol>
    </section>}
  </div>;
}
