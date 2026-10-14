"use client";

/**
 * Full-screen conversation search.
 *
 * The overlay filters the *current* Project's conversation titles and nothing
 * else: there is no message-body or full-text search, no index, and no request
 * to the control server. It reads the same project-filtered session list the
 * sidebar already renders, and selecting a result goes through the same
 * session-selection callback the sidebar list uses, so navigation, URL state
 * and attention handling stay identical.
 *
 * It follows the Project switcher's overlay conventions: the same Dialog
 * primitive, so Escape and dismissal return focus to the trigger, and the same
 * `listbox` / `option` markup for the selectable entries.
 */

import { useMemo, useRef, useState, type ReactElement } from "react";
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

export type ConversationSearchProps = {
  /** Element used to open the overlay; receives the dialog's trigger props. */
  trigger: ReactElement;
  /** The already project-filtered session list, in the sidebar's own order. */
  sessions: readonly ChatSession[];
  activeSessionId: string;
  /** Name of the Project the search is scoped to, shown as the scope line. */
  projectName?: string;
  /** The same selection callback the sidebar list uses. */
  onSelect: (id: string) => void;
};

export function ConversationSearch({
  trigger,
  sessions,
  activeSessionId,
  projectName,
  onSelect,
}: ConversationSearchProps) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  // An untitled session is shown as "New chat", so it is matched under that
  // name too rather than only against its empty title. Same rule as the list.
  const untitledLabel = t("sidebar.newChat");
  const matches = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return sessions;
    return sessions.filter(session =>
      (session.title || untitledLabel).toLowerCase().includes(needle),
    );
  }, [sessions, query, untitledLabel]);

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
        // A reopened overlay starts from the unfiltered list, like the sidebar.
        if (!next) setQuery("");
      }}
    >
      <DialogTrigger render={trigger} />
      <DialogContent
        initialFocus={inputRef}
        showCloseButton={false}
        className="top-0 left-0 h-dvh w-full max-w-none translate-x-0 translate-y-0 flex flex-col gap-0 rounded-none p-0 sm:max-w-none"
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
              onChange={event => setQuery(event.target.value)}
              onKeyDown={event => {
                // Enter takes the first title match; Escape belongs to the dialog.
                if (event.key !== "Enter" || event.nativeEvent.isComposing) return;
                event.preventDefault();
                const first = matches[0];
                if (first) select(first.id);
              }}
              className="h-11 pl-9 text-base sm:h-10 sm:text-sm"
            />
          </div>
          {projectName && (
            <p className={cn("mt-2 truncate text-muted-foreground", TYPOGRAPHY_SCALE.caption)}>
              {t("sidebar.currentProject", { name: projectName })}
            </p>
          )}
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
