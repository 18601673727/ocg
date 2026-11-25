import { ChevronDown, MessageSquare, Search, X } from "@lucide/solid";
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
import { Select, SelectItem } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { ariaState, bindElementRef, elementRef, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import type { Project } from "../contracts";
import { useI18n } from "../i18n";
import { SELECTED_ROW } from "../primitives/selection";
import type { ApiClient, ConversationSearchScope } from "../runtime/api-client";
import type { ChatSession } from "../types";
import {
  appendSearchResults,
  conversationSearchMatches,
  type SearchConversation,
  type SearchMatch,
  sessionMatchesScope,
} from "./conversation-search-results";

export type ConversationSearchProps = {
  trigger: View;
  sessions: readonly ChatSession[];
  projects?: readonly Project[];
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
  const [scope, setScope] = createSignal<ConversationSearchScope>("all");
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

  const projectNames = createMemo(() => new Map(props.projects?.map(({ id, name }) => [id, name])));
  const resetPage = () => {
    setHighlightedId(null);
    setServerResults(null);
    setNextBefore(null);
    setFailed(false);
    setBefore(undefined);
  };
  const changeScope = (next: ConversationSearchScope | null) => {
    if (!next || next === scope()) return;
    resetPage();
    setScope(next);
    setLoading(Boolean(props.api && (needle() || next !== "all")));
  };
  const changeOpen = (next: boolean) => {
    setOpen(next);
    if (!next) {
      resetPage();
      setQuery("");
      setScope("all");
      setLoading(false);
    }
  };

  createEffect(
    () => [scope(), props.projects] as const,
    ([selected, projects]) => {
      if (
        selected.startsWith("project:") &&
        projects &&
        !projects.some(({ id }) => selected === `project:${id}`)
      )
        changeScope("all");
    },
  );

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
    () => ({
      open: open(),
      api: props.api,
      query: needle(),
      scope: scope(),
      before: before(),
      attempt: attempt(),
    }),
    (current, previous) => {
      const { api, query, scope, before } = current;
      if (!current.open || !api || (!query && scope === "all")) return;
      const controller = new AbortController();
      const settings = { scope, before, limit: 50, signal: controller.signal };
      const debounce =
        query && before === undefined && query !== previous?.query && scope === previous?.scope;
      const timer = setTimeout(
        () => {
          setLoading(true);
          setFailed(false);
          const request = query
            ? api.searchConversations({ ...settings, query })
            : api.listConversations(settings);
          void request
            .then((answer) => {
              if (controller.signal.aborted) return;
              setServerResults((previous) =>
                appendSearchResults(before ? (previous ?? []) : [], answer.conversations),
              );
              setNextBefore(answer.has_more ? answer.next_before : null);
            })
            .catch(() => {
              if (!controller.signal.aborted) setFailed(true);
            })
            .finally(() => {
              if (!controller.signal.aborted) setLoading(false);
            });
        },
        debounce ? 200 : 0,
      );
      return () => {
        clearTimeout(timer);
        controller.abort();
      };
    },
  );

  const untitledLabel = i18n.t("sidebar.newChat");
  const matches = createMemo<readonly SearchMatch[]>(() => {
    if (!needle() && scope() === "all") return props.sessions;
    const results = serverResults();
    if (results) return conversationSearchMatches(props.sessions, results);
    const lowered = needle().toLowerCase();
    return props.sessions.filter(
      (session) =>
        sessionMatchesScope(session, scope()) &&
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

  const select = (match: SearchMatch) => {
    changeOpen(false);
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
        class="flex max-h-[min(36rem,85dvh)] flex-col gap-0 p-0 sm:max-w-xl"
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

        <div class="flex flex-col gap-2 px-4 pt-3 pb-2 sm:flex-row sm:items-center sm:px-6">
          <div class="relative min-w-0 flex-1">
            <Search
              class="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
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
                const next = event.currentTarget.value;
                if (next.trim() !== needle()) {
                  resetPage();
                  setLoading(Boolean(props.api && (next.trim() || scope() !== "all")));
                }
                setQuery(next);
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
          <div class="relative inline-flex w-fit min-w-0 max-w-full sm:max-w-44">
            <Select<ConversationSearchScope>
              value={scope()}
              onValueChange={changeScope}
              aria-label={i18n.t("sidebar.searchScopeLabel")}
              class="max-w-full not-supports-[appearance:base-select]:appearance-none sm:data-[size=default]:h-10"
            >
              <SelectItem value="all">{i18n.t("sidebar.searchScopeAll")}</SelectItem>
              <SelectItem value="conversations">
                {i18n.t("sidebar.searchScopeConversations")}
              </SelectItem>
              {(props.projects ?? []).map((project) => (
                <SelectItem value={`project:${project.id}`}>{project.name}</SelectItem>
              ))}
            </Select>
            <ChevronDown
              class="pointer-events-none absolute end-3 top-1/2 size-4 -translate-y-1/2 text-foreground supports-[appearance:base-select]:hidden"
              aria-hidden="true"
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
              const project =
                scope() === "all" && session.projectId && projectNames().get(session.projectId);
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
                    <span class="min-w-0 flex-1">
                      <span class="flex min-w-0 items-center gap-2">
                        <span class="min-w-0 truncate">{session.title || untitledLabel}</span>
                        {project && (
                          <span class="ml-auto max-w-[45%] shrink-0 truncate rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground">
                            {project}
                          </span>
                        )}
                      </span>
                      {session.snippet && (
                        <span class="line-clamp-2 text-xs text-muted-foreground">
                          {session.snippet}
                        </span>
                      )}
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
            <div class="pt-1 pb-2">
              <Button
                variant="outline"
                size="sm"
                class="w-full"
                disabled={loading() || failed()}
                onClick={() => {
                  setLoading(true);
                  setBefore(nextBefore() ?? undefined);
                }}
              >
                {i18n.t("sidebar.searchMore")}
              </Button>
            </div>
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
