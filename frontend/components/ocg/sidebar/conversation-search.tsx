"use client";

/**
 * Full-screen conversation search.
 *
 * The overlay searches across projects within this account only: an empty
 * query shows the sidebar's own list, while a non-empty query asks the
 * control server's conversation search (titles plus recent user/agent
 * messages, terms combined by AND) through the same account's control
 * client and keeps only the sessions the sidebar already renders.
 * Selecting a result goes through the same session-selection callback the
 * sidebar list uses, so navigation, URL state and attention handling stay
 * identical. The server reads only this account's database, so one
 * account's search never sees another account's conversations. Without a
 * control client, or when the search request fails, it falls back to the
 * local title filter.
 *
 * It follows the Project switcher's overlay conventions: the same Dialog
 * primitive, so Escape and dismissal return focus to the trigger, and the same
 * `listbox` / `option` markup for the selectable entries.
 */

import { useEffect, useMemo, useRef, useState, type ReactElement } from "react";
import { MessageSquare, Search, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import type { ChatSession } from "../types";
import { useI18n } from "../i18n";
import type { HttpCanonicalControlClient } from "../runtime/control-client";

export type ConversationSearchProps = {
  /** Element used to open the overlay; receives the dialog's trigger props. */
  trigger: ReactElement;
  /** This account's session list, in the sidebar's own order. */
  sessions: readonly ChatSession[];
  activeSessionId: string;
  /** The same selection callback the sidebar list uses. */
  onSelect: (id: string) => void;
  /** This account's control client for full-text search; omitting it keeps title filtering. */
  control?: HttpCanonicalControlClient;
};

function sessionKeys(session: ChatSession): string[] {
  const keys = [session.id];
  if (session.conversationId && session.conversationId !== session.id) keys.push(session.conversationId);
  return keys;
}

export function ConversationSearch({
  trigger,
  sessions,
  activeSessionId,
  onSelect,
  control,
}: ConversationSearchProps) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [serverKeys, setServerKeys] = useState<ReadonlySet<string> | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const needle = query.trim();
  useEffect(() => {
    if (!open || !control || !needle) return;
    let cancelled = false;
    const controller = new AbortController();
    const timer = setTimeout(() => {
      void control
        .searchConversations({ query: needle, limit: 50, signal: controller.signal })
        .then(page => {
          if (cancelled) return;
          const keys = new Set<string>();
          for (const conversation of page.conversations) {
            keys.add(conversation.root_job);
            keys.add(conversation.tip_job);
          }
          setServerKeys(keys);
        })
        .catch(() => {
          // A failed search keeps the local title filter, never an empty list.
          if (!cancelled) setServerKeys(null);
        });
    }, 200);
    return () => {
      cancelled = true;
      clearTimeout(timer);
      controller.abort();
    };
  }, [open, control, needle]);

  // An untitled session is shown as "New chat", so it is matched under that
  // name too rather than only against its empty title. Same rule as the list.
  const untitledLabel = t("sidebar.newChat");
  const matches = useMemo(() => {
    if (!needle) return sessions;
    if (serverKeys) {
      // The server already matched titles and messages across projects;
      // keep the sidebar's own order, including hidden-session handling
      // upstream. The keys come from this account's control client only.
      return sessions.filter(session => sessionKeys(session).some(key => serverKeys.has(key)));
    }
    const lowered = needle.toLowerCase();
    return sessions.filter(session =>
      (session.title || untitledLabel).toLowerCase().includes(lowered),
    );
  }, [sessions, needle, serverKeys, untitledLabel]);

  const close = () => setOpen(false);
  const select = (id: string) => {
    close();
    onSelect(id);
  };

  return (
    <Dialog
      open={open}
      onOpenChange={next => {
        setOpen(next);
        // A reopened dialog starts from the unfiltered list, like the sidebar.
        if (!next) {
          setQuery("");
          setServerKeys(null);
        }
      }}
    >
      <DialogTrigger render={trigger} />
      <DialogContent
        initialFocus={inputRef}
        showCloseButton={false}
        className="flex max-h-[min(36rem,85dvh)] flex-col gap-0 p-0"
      >
        <DialogHeader className="gap-0 border-b border-border px-4 pt-[max(1rem,env(safe-area-inset-top))] pb-3 sm:px-6">
          <div className="flex items-center gap-2">
            <DialogTitle className="min-w-0 flex-1 truncate font-sans normal-case tracking-normal">
              {t("sidebar.searchConversations")}
            </DialogTitle>
            <DialogClose
              render={
                <Button variant="ghost" size="icon-sm" aria-label={t("common.close")} />
              }
            >
              <X aria-hidden="true" />
            </DialogClose>
          </div>
          <DialogDescription>{t("sidebar.searchConversationsBody")}</DialogDescription>
        </DialogHeader>

        <div className="px-4 pt-3 pb-2 sm:px-6">
          <div className="relative">
            <Search
              className="pointer-events-none absolute top-3 left-3 size-4 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              ref={inputRef}
              type="search"
              placeholder={t("sidebar.searchChats")}
              aria-label={t("sidebar.searchChatsLabel")}
              value={query}
              onChange={event => {
                setQuery(event.target.value);
                // A new needle invalidates the previous server result.
                setServerKeys(null);
              }}
              onKeyDown={event => {
                // Enter takes the first match; Escape belongs to the dialog.
                if (event.key !== "Enter" || event.nativeEvent.isComposing) return;
                event.preventDefault();
                const first = matches[0];
                if (first) select(first.id);
              }}
              className="h-11 pl-9 text-base sm:h-10 sm:text-sm"
            />
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-[max(1rem,env(safe-area-inset-bottom))] sm:px-6">
          <ul role="listbox" aria-label={t("nav.chatSessions")} className="flex flex-col gap-px pb-2">
            {matches.map(session => {
              const active = session.id === activeSessionId;
              return (
                <li key={session.id}>
                  <button
                    type="button"
                    role="option"
                    aria-selected={active}
                    onClick={() => select(session.id)}
                    className={cn(
                      "flex w-full min-w-0 items-center gap-2 rounded-md border border-transparent px-2 py-2 text-left",
                      TYPOGRAPHY_SCALE.body_sm,
                      "transition-colors",
                      active
                        ? "bg-muted font-medium text-foreground"
                        : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                    )}
                  >
                    <MessageSquare className="size-3.5 shrink-0" aria-hidden="true" />
                    <span className="min-w-0 flex-1 truncate">
                      {session.title || untitledLabel}
                    </span>
                  </button>
                </li>
              );
            })}
            {matches.length === 0 && (
              <li
                role="status"
                className={cn("px-2 py-6 text-center text-muted-foreground", TYPOGRAPHY_SCALE.caption)}
              >
                {sessions.length === 0 ? t("sidebar.searchNoConversations") : t("sidebar.noSearchMatches")}
              </li>
            )}
          </ul>
        </div>
      </DialogContent>
    </Dialog>
  );
}
