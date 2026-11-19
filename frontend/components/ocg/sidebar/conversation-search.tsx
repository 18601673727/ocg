import { MessageSquare, Search, X } from "@lucide/solid";
import { createEffect, createMemo, createSignal, createUniqueId } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { ariaState, bindElementRef, elementRef, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import { SELECTED_ROW } from "../primitives/selection";
import type { ApiClient } from "../runtime/api-client";
import type { ChatSession } from "../types";
import {
  appendSearchResults,
  conversationSearchMatches,
  type SearchConversation,
  type SearchMatch,
} from "./conversation-search-results";

export type ConversationSearchProps = {
  trigger: View;
  sessions: readonly ChatSession[];
  activeSessionId: string;
  onSelect: (id: string) => void;
  onOpenConversation?: (conversation: string) => void;
  api?: ApiClient;
  conversationList?: "loading" | "loaded" | "failed";
  onRetryConversations?: () => void;
};

export function ConversationSearch(props: ConversationSearchProps) {
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  const [query, setQuery] = createSignal("");
  const [serverResults, setServerResults] = createSignal<readonly SearchConversation[] | null>(
    null,
  );
  const [nextBefore, setNextBefore] = createSignal<string | null>(null);
  const [loading, setLoading] = createSignal(false);
  const [failed, setFailed] = createSignal(false);
  const [before, setBefore] = createSignal<string | undefined>();
  const [attempt, setAttempt] = createSignal(0);
  const [highlightedId, setHighlightedId] = createSignal<string | null>(null);
  const inputRef = elementRef<HTMLInputElement>(null);
  const listId = createUniqueId();
  const hintId = createUniqueId();
  const needle = createMemo(() => query().trim());

  const changeOpen = (next: boolean) => {
    setOpen(next);
    if (!next) {
      setQuery("");
      setServerResults(null);
      setNextBefore(null);
      setLoading(false);
      setFailed(false);
      setBefore(undefined);
      setHighlightedId(null);
    }
  };

  createEffect(
    () => [open(), changeOpen],
    () => {
      const onKeyDown = (event: KeyboardEvent) => {
        if (
          event.defaultPrevented ||
          event.isComposing ||
          event.repeat ||
          event.altKey ||
          event.shiftKey ||
          !(event.metaKey || event.ctrlKey) ||
          event.key.toLowerCase() !== "k"
        )
          return;
        if (!open() && document.querySelector('[role="dialog"]')) return;
        event.preventDefault();
        changeOpen(!open());
      };
      window.addEventListener("keydown", onKeyDown);
      return () => window.removeEventListener("keydown", onKeyDown);
    },
  );

  createEffect(
    () => [open(), props.api, needle(), before(), attempt()],
    () => {
      if (!open() || !props.api || !needle()) return;
      let cancelled = false;
      const controller = new AbortController();
      const timer = setTimeout(
        () => {
          setLoading(true);
          setFailed(false);
          void props
            .api!.searchConversations({
              query: needle(),
              before: before(),
              limit: 50,
              signal: controller.signal,
            })
            .then((answer) => {
              if (cancelled) return;
              setServerResults((previous) =>
                appendSearchResults(before() ? (previous ?? []) : [], answer.conversations),
              );
              setNextBefore(answer.has_more ? answer.next_before : null);
            })
            .catch(() => {
              if (!cancelled) setFailed(true);
            })
            .finally(() => {
              if (!cancelled) setLoading(false);
            });
        },
        before() === undefined ? 200 : 0,
      );
      return () => {
        cancelled = true;
        clearTimeout(timer);
        controller.abort();
      };
    },
  );

  const untitledLabel = i18n.t("sidebar.newChat");
  const matches = createMemo<readonly SearchMatch[]>(() => {
    if (!needle()) return props.sessions;
    if (serverResults()) return conversationSearchMatches(props.sessions, serverResults()!);
    const lowered = needle().toLowerCase();
    return props.sessions.filter((session) =>
      (session.title || untitledLabel).toLowerCase().includes(lowered),
    );
  });

  const highlightedIndex = createMemo(() =>
    matches().findIndex((match) => match.id === highlightedId()),
  );
  const highlightedOption = createMemo(() =>
    highlightedIndex() >= 0 ? `${listId}-option-${highlightedIndex()}` : undefined,
  );
  createEffect(
    () => [open(), highlightedOption()],
    () => {
      if (open() && highlightedOption())
        document.getElementById(highlightedOption()!)?.scrollIntoView({ block: "nearest" });
    },
  );

  const close = () => setOpen(false);
  const select = (match: SearchMatch) => {
    close();
    if (match.conversation && props.onOpenConversation)
      props.onOpenConversation(match.conversation);
    else props.onSelect(match.id);
  };

  return (
    <Dialog open={open()} onOpenChange={changeOpen}>
      <DialogTrigger render={props.trigger} />
      <DialogContent
        initialFocus={() => inputRef.current}
        showCloseButton={false}
        class="flex max-h-[min(36rem,85dvh)] flex-col gap-0 p-0"
      >
        <DialogHeader class="gap-0 border-b border-border px-4 pt-[max(1rem,env(safe-area-inset-top))] pb-3 sm:px-6">
          <div class="flex items-center gap-2">
            <DialogTitle class="min-w-0 flex-1 truncate font-sans normal-case tracking-normal">
              {i18n.t("sidebar.searchConversations")}
            </DialogTitle>
            <DialogClose
              render={<Button variant="ghost" size="icon-sm" aria-label={i18n.t("common.close")} />}
            >
              <X aria-hidden="true" />
            </DialogClose>
          </div>
          <DialogDescription>{i18n.t("sidebar.searchConversationsBody")}</DialogDescription>
        </DialogHeader>

        <div class="px-4 pt-3 pb-2 sm:px-6">
          <div class="relative">
            <Search
              class="pointer-events-none absolute top-3 left-3 size-4 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              ref={bindElementRef(inputRef)}
              type="search"
              role="combobox"
              placeholder={i18n.t("sidebar.searchChats")}
              aria-label={i18n.t("sidebar.searchChatsLabel")}
              aria-autocomplete="list"
              aria-expanded="true"
              aria-controls={listId}
              aria-activedescendant={highlightedOption()}
              aria-describedby={hintId}
              value={query()}
              onInput={(event) => {
                setQuery(event.target.value);
                setHighlightedId(null);
                if (event.target.value.trim() === needle()) return;
                setServerResults(null);
                setNextBefore(null);
                setFailed(false);
                setLoading(Boolean(props.api && event.target.value.trim()));
                setBefore(undefined);
              }}
              onKeyDown={(event) => {
                if (event.isComposing) return;
                if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                  if (!matches().length) return;
                  event.preventDefault();
                  const next =
                    highlightedIndex() < 0
                      ? event.key === "ArrowDown"
                        ? 0
                        : matches().length - 1
                      : Math.max(
                          0,
                          Math.min(
                            matches().length - 1,
                            highlightedIndex() + (event.key === "ArrowDown" ? 1 : -1),
                          ),
                        );
                  setHighlightedId(matches()[next].id);
                } else if (event.key === "Enter") {
                  event.preventDefault();
                  const match = matches()[highlightedIndex() < 0 ? 0 : highlightedIndex()];
                  if (match) select(match);
                }
              }}
              class="h-11 pl-9 text-base sm:h-10 sm:text-sm"
            />
          </div>
          <span id={hintId} class="sr-only">
            {i18n.t("sidebar.searchKeyboardHint")}
          </span>
        </div>

        <ScrollArea
          class="flex min-h-0 flex-1 flex-col"
          viewportProps={{
            class: "ocg-subtle-scrollbar min-h-0 flex-1",
            style: { "overflow-x": "hidden" },
          }}
          contentProps={{
            class: "px-4 pb-[max(1rem,env(safe-area-inset-bottom))] sm:px-6",
            style: { "min-width": `${0}px`, width: "100%" },
          }}
        >
          {loading() && (
            <p role="status" class="px-2 py-2 text-sm text-muted-foreground">
              {i18n.t("common.loading")}
            </p>
          )}
          {failed() && (
            <div role="alert" class="px-2 py-2 text-sm text-destructive">
              <p>{i18n.t("sidebar.searchFailed")}</p>
              <Button
                variant="outline"
                size="sm"
                disabled={loading()}
                onClick={() => {
                  setLoading(true);
                  setAttempt((value) => value + 1);
                }}
              >
                {i18n.t("common.retry")}
              </Button>
            </div>
          )}
          <ul
            id={listId}
            aria-busy={ariaState(loading())}
            role="listbox"
            aria-label={i18n.t("nav.chatSessions")}
            class="flex flex-col gap-px pb-2"
          >
            {matches().map((session, index) => {
              const active = session.id === props.activeSessionId;
              const highlighted = index === highlightedIndex();
              return (
                <li role="presentation">
                  <button
                    id={`${listId}-option-${index}`}
                    type="button"
                    role="option"
                    tabindex={-1}
                    aria-selected={ariaState(highlighted)}
                    aria-current={active ? "page" : undefined}
                    onClick={() => select(session)}
                    class={cn(
                      "flex w-full min-w-0 items-center gap-2 rounded-md border border-transparent px-2 py-2 text-left",
                      TYPOGRAPHY_SCALE.body_sm,
                      "transition-colors",
                      highlighted || (active && highlightedIndex() < 0)
                        ? cn(SELECTED_ROW, "font-medium")
                        : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                    )}
                  >
                    <MessageSquare class="size-3.5 shrink-0" aria-hidden="true" />
                    <span class="line-clamp-2 min-w-0 flex-1 break-words">
                      {session.title || untitledLabel}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
          {matches().length === 0 &&
            !loading() &&
            !failed() &&
            !nextBefore() &&
            (props.sessions.length === 0 ? (
              props.conversationList === "loading" ? (
                <div class="flex flex-col gap-1 py-2">
                  {Array.from({ length: 5 }, (_, i) => (
                    <Skeleton class="h-8 w-full rounded-md" />
                  ))}
                </div>
              ) : props.conversationList === "failed" ? (
                <div
                  role="alert"
                  class="flex items-center justify-between gap-2 rounded-lg border border-destructive/20 bg-destructive/10 px-3 py-2 text-xs text-destructive"
                >
                  <span class="truncate">{i18n.t("sidebar.loadFailed")}</span>
                  {props.onRetryConversations && (
                    <Button
                      variant="outline"
                      size="sm"
                      class="h-7 shrink-0 px-2 text-xs"
                      onClick={props.onRetryConversations}
                    >
                      {i18n.t("common.retry")}
                    </Button>
                  )}
                </div>
              ) : (
                <p
                  role="status"
                  class={cn(
                    "px-2 py-6 text-center text-muted-foreground",
                    TYPOGRAPHY_SCALE.caption,
                  )}
                >
                  {i18n.t("sidebar.searchNoConversations")}
                </p>
              )
            ) : (
              <p
                role="status"
                class={cn("px-2 py-6 text-center text-muted-foreground", TYPOGRAPHY_SCALE.caption)}
              >
                {i18n.t("sidebar.noSearchMatches")}
              </p>
            ))}
          {nextBefore() && (
            <Button
              variant="outline"
              size="sm"
              disabled={loading() || failed()}
              onClick={() => {
                setLoading(true);
                setBefore(nextBefore() ?? undefined);
              }}
            >
              {i18n.t("sidebar.searchMore")}
            </Button>
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
