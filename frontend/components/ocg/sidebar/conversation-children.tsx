"use client";

import { useEffect, useState } from "react";
import { MessageSquare } from "lucide-react";
import { cn } from "@/lib/utils";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { useI18n } from "../i18n";
import type { ControlChildConversation } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";

export function SidebarConversationChildren({ conversation, control, activeChild, onOpen }: {
  conversation: string;
  control: HttpCanonicalControlClient;
  activeChild?: string;
  onOpen: (conversation: string) => void;
}) {
  const { t } = useI18n();
  const [children, setChildren] = useState<ControlChildConversation[]>([]);
  const [pages, setPages] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let inFlight = false;
    const refresh = async () => {
      if (controller.signal.aborted || inFlight) return;
      if (!document.hidden) {
        inFlight = true;
        try {
          const items: ControlChildConversation[] = [];
          let before: string | undefined;
          let more = false;
          for (let page = 0; page < pages; page++) {
            const result = await control.readChildren(conversation, before, 20, controller.signal);
            items.push(...result.children.filter(child => child.origin !== "system"));
            more = result.has_more;
            before = result.next_before ?? undefined;
            if (!more) break;
          }
          if (!controller.signal.aborted) { setChildren(items); setHasMore(more); setError(null); }
        } catch (cause) {
          if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : String(cause));
        } finally { inFlight = false; }
      }
      if (!controller.signal.aborted) timer = setTimeout(() => void refresh(), 15000);
    };
    const changed = () => {
      if (timer !== undefined) clearTimeout(timer);
      void refresh();
    };
    void refresh();
    const unsubscribe = control.onConversationsChanged(changed);
    return () => { controller.abort(); if (timer !== undefined) clearTimeout(timer); unsubscribe(); };
  }, [control, conversation, pages]);
  return <>
    {error && <DismissibleNotice message={error} className="ml-6 px-2 py-1 text-xs" />}
    {(children.length > 0 || hasMore) && <ul className="ml-5 border-l border-border pl-2" aria-label={t("chat.children")}>
      {children.map(child => <li key={child.id} data-sidebar-level="2">
        <button type="button" onClick={() => onOpen(child.id)} aria-current={activeChild === child.id ? "true" : undefined}
          title={child.title || t("chat.child.untitled")}
          className={cn("flex w-full min-w-0 items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm leading-5", activeChild === child.id ? "bg-muted font-medium text-foreground" : "text-muted-foreground hover:bg-muted/60 hover:text-foreground")}>
          <MessageSquare className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate">{child.title || t("chat.child.untitled")}</span>
        </button>
      </li>)}
      {hasMore && <li><button type="button" className="px-2 py-1.5 text-xs text-muted-foreground hover:text-foreground" onClick={() => setPages(value => value + 1)}>{t("chat.child.more")}</button></li>}
    </ul>}
  </>;
}
