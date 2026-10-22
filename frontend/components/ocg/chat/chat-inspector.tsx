"use client";

import { ExternalLink, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ChatMessage, ChatSession } from "../types";
import { runtimeStateLabel, useI18n } from "../i18n";

export function ChatInspector({ session, messages, onClose, onOpenJob }: {
  session?: ChatSession;
  messages: ChatMessage[];
  onClose: () => void;
  onOpenJob: (job: string) => void;
}) {
  const { t, locale } = useI18n();
  const turns = [...new Map(messages.filter(message => message.role === "assistant").map(message => [message.jobId ?? message.id, message])).values()];

  return (
    <aside id="chat-inspector" aria-labelledby="chat-inspector-title" className="fixed inset-y-3 right-3 z-40 flex min-h-0 w-[min(24rem,calc(100%-1.5rem))] min-w-0 flex-col overflow-hidden rounded-3xl border-0 bg-muted text-foreground shadow-lg lg:relative lg:inset-auto lg:z-auto lg:m-3 lg:h-[calc(100%-1.5rem)] lg:w-80 lg:shrink-0 lg:shadow-none xl:w-96">
      <header className="flex min-h-14 shrink-0 items-center gap-3 py-4 pr-14 pl-5">
        <h2 id="chat-inspector-title" className="min-w-0 flex-1 text-sm font-semibold">{t("usage.conversation")}</h2>
        <Button id="chat-inspector-close" type="button" variant="ghost" size="icon" className="absolute top-0 right-0 size-11 rounded-full" onClick={onClose} aria-label={t("topbar.collapseInspector")} title={t("topbar.collapseInspector")}><X className="size-4" /></Button>
      </header>
      <div className="ocg-subtle-scrollbar min-h-0 flex-1 space-y-6 overflow-x-hidden overflow-y-auto overscroll-contain px-5 pb-6 [overflow-wrap:anywhere]" data-chat-inspector-scroll="">
        {session ? <>
          <section className="space-y-3">
            <h3 className="text-base font-medium">{session.title || t("sidebar.newChat")}</h3>
            <dl className="space-y-3 text-xs">
              <div className="flex justify-between gap-4"><dt className="text-muted-foreground">{t("usage.turns")}</dt><dd className="tabular-nums">{turns.length}</dd></div>
              <div className="flex justify-between gap-4"><dt className="text-muted-foreground">{t("usage.updated")}</dt><dd className="text-right">{new Date(session.updatedAt).toLocaleString(locale)}</dd></div>
            </dl>
          </section>
          {turns.length > 0 ? <section className="space-y-3">
            <h3 className="text-xs font-semibold text-muted-foreground">{t("usage.execution")}</h3>
            <ol className="space-y-3">
              {turns.map((message, index) => <li key={message.id} className="space-y-2 rounded-2xl bg-background/60 p-4 text-xs">
                <div className="flex items-center justify-between gap-3"><span className="font-medium">{t("usage.turns")} {index + 1}</span><span className="text-muted-foreground">{runtimeStateLabel(t, message.status)}</span></div>
                {message.jobId && <button type="button" className="flex max-w-full items-center gap-2 rounded-md py-1 text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring" onClick={() => { if (message.jobId) onOpenJob(message.jobId); }}><span className="truncate font-mono">{message.jobId}</span><ExternalLink className="size-3.5 shrink-0" /><span className="sr-only">{t("chat.openInJobs")}</span></button>}
                {message.failureReason && <p className="text-destructive">{message.failureReason}</p>}
              </li>)}
            </ol>
          </section> : <p className="text-sm text-muted-foreground">{t("execution.noExecution")}</p>}
        </> : <p className="text-sm text-muted-foreground">{t("execution.noExecution")}</p>}
      </div>
    </aside>
  );
}
