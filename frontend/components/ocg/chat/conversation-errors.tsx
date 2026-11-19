import { AlertCircle, ChevronDown, X } from "@lucide/solid";
import {
  type Accessor,
  createContext,
  createEffect,
  createMemo,
  createSignal,
  createUniqueId,
  Show,
  useContext,
} from "solid-js";
import { ScrollArea } from "@/components/ui/scroll-area";
import { dismissNotice, isNoticeDismissed, noticeIdentity } from "@/lib/notice-dismissal";
import { ariaState, bindElementRef, elementRef, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";

type Retry = { label: string; run: () => void };
type ConversationError = {
  id: number;
  message: string;
  at: number;
  retry?: Retry;
  identity?: string;
  dismissalKey: string;
  dismissed?: boolean;
};
type Histories = Record<string, readonly ConversationError[]>;
type ErrorStore = {
  histories: Histories;
  report: (
    conversation: string,
    cause: unknown,
    retry?: Retry,
    identity?: string,
    at?: number,
  ) => void;
  dismiss: (conversation: string, through: number) => void;
  transfer: (from: string, to: string) => void;
};

const ErrorContext = createContext<ErrorStore | null>(null);
const ConversationContext = createContext<Accessor<string | null>>(() => null);

export function ConversationErrorsProvider(props: { children: View }) {
  const [histories, setHistories] = createSignal<Histories>({});
  const sequence = elementRef(0);
  const report = (
    conversation: string,
    cause: unknown,
    retry?: Retry,
    identity?: string,
    at?: number,
  ) => {
    const message = cause instanceof Error ? cause.message : String(cause);
    if (!message.trim()) return;
    const error = { id: ++sequence.current, message, at: at ?? Date.now(), retry, identity };
    void noticeIdentity(
      identity ? `error:${identity}` : `conversation:${conversation}`,
      message,
    ).then((dismissalKey) => {
      const dismissed = isNoticeDismissed(dismissalKey);
      setHistories((current) => {
        const previous = current[conversation] ?? [];
        const existing = previous.find((item) => item.dismissalKey === dismissalKey);
        if (existing) {
          if ((retry && !existing.retry) || (at !== undefined && existing.at !== at))
            return {
              ...current,
              [conversation]: previous.map((item) =>
                item === existing
                  ? { ...item, retry: item.retry ?? retry, at: at ?? item.at }
                  : item,
              ),
            };
          return current;
        }
        return {
          ...current,
          [conversation]: [...previous, { ...error, dismissalKey, dismissed }].sort(
            (a, b) => a.id - b.id,
          ),
        };
      });
    });
  };
  const dismiss = (conversation: string, through: number) => {
    for (const error of histories()[conversation] ?? []) {
      if (error.id <= through) dismissNotice(error.dismissalKey);
    }
    setHistories((current) => {
      const history = current[conversation];
      if (!history?.some((error) => error.id <= through && !error.dismissed)) return current;
      return {
        ...current,
        [conversation]: history.map((error) =>
          error.id <= through ? { ...error, dismissed: true } : error,
        ),
      };
    });
  };
  const transfer = (from: string, to: string) => {
    if (from === to) return;
    setHistories((current) => {
      if (!current[from]?.length) return current;
      const next = {
        ...current,
        [to]: [...(current[to] ?? []), ...current[from]].sort((a, b) => a.id - b.id),
      };
      delete next[from];
      return next;
    });
  };
  const value: ErrorStore = {
    get histories() {
      return histories();
    },
    report,
    dismiss,
    transfer,
  };
  return <ErrorContext value={value}>{props.children}</ErrorContext>;
}

export const ConversationErrorScope = ConversationContext;

export function useConversationErrors() {
  return useContext(ErrorContext);
}

export function useConversationError(
  message: Accessor<string | null | undefined>,
  identity?: Accessor<string>,
  at?: Accessor<number>,
) {
  const store = useConversationErrors();
  const conversation = useContext(ConversationContext);
  createEffect(
    () => [conversation(), message(), identity?.(), at?.()] as const,
    ([key, text, errorId, time]) => {
      if (key && text) store?.report(key, text, undefined, errorId, time);
    },
  );
  return () => Boolean(conversation() && store);
}

export function ConversationErrorHeader() {
  const store = useConversationErrors();
  const conversation = useContext(ConversationContext);
  const i18n = useI18n();
  const [expanded, setExpanded] = createSignal(false);
  const root = elementRef<HTMLDivElement>(null);
  const trigger = elementRef<HTMLButtonElement>(null);
  const panelId = createUniqueId();
  const history = createMemo(() => {
    const id = conversation();
    return id ? store?.histories[id] : undefined;
  });
  const latest = createMemo(() => history()?.at(-1));
  const active = createMemo(() => history()?.findLast((error) => !error.dismissed));

  createEffect(
    () => [expanded()],
    () => {
      if (!expanded()) return;
      const dismiss = (event: PointerEvent) => {
        if (event.target instanceof Node && !root.current?.contains(event.target))
          setExpanded(false);
      };
      document.addEventListener("pointerdown", dismiss);
      return () => document.removeEventListener("pointerdown", dismiss);
    },
  );

  return (
    <Show when={latest() && active()}>
      <div
        ref={bindElementRef(root)}
        class="pointer-events-none relative w-full min-w-0 max-w-3xl"
        data-slot="conversation-errors"
        onKeyDown={(event) => {
          if (event.key === "Escape" && expanded()) {
            event.stopPropagation();
            setExpanded(false);
            trigger.current?.focus();
          }
        }}
      >
        <div
          class={cn(
            "pointer-events-auto flex h-11 min-w-0 items-center rounded-xl border bg-background/90 text-xs shadow-sm backdrop-blur-sm",
            active()
              ? "border-destructive/20 text-destructive"
              : "ml-auto w-fit max-w-full border-border text-muted-foreground",
          )}
        >
          <button
            ref={bindElementRef(trigger)}
            type="button"
            class="flex h-full min-w-0 flex-1 items-center gap-2 rounded-xl px-3 text-left hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            aria-expanded={ariaState(expanded())}
            aria-controls={panelId}
            aria-label={i18n.t(expanded() ? "chat.errors.hide" : "chat.errors.show", {
              count: history()?.length ?? 0,
            })}
            onClick={() => setExpanded((value) => !value)}
          >
            <AlertCircle class="size-4 shrink-0" aria-hidden="true" />
            {active() ? (
              <span class="min-w-0 flex-1 truncate" role="status" title={active()!.message}>
                {active()!.message}
              </span>
            ) : (
              <span>{i18n.t("chat.errors.label")}</span>
            )}
            <span class="shrink-0 tabular-nums">{history()?.length ?? 0}</span>
            <ChevronDown
              aria-hidden="true"
              class={cn(
                "size-4 shrink-0 transition-transform duration-200 motion-reduce:transition-none",
                !expanded() && "-rotate-90",
              )}
            />
          </button>
          {active() && (
            <button
              type="button"
              class="flex h-full w-11 shrink-0 items-center justify-center rounded-xl hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
              aria-label={i18n.t("chat.errors.dismiss")}
              title={i18n.t("chat.errors.dismiss")}
              onClick={() => {
                const key = conversation();
                const error = latest();
                if (key && error) store?.dismiss(key, error.id);
                setExpanded(false);
              }}
            >
              <X class="size-4" aria-hidden="true" />
            </button>
          )}
        </div>
        {expanded() && (
          <section
            id={panelId}
            aria-label={i18n.t("chat.errors.history")}
            class="pointer-events-auto absolute inset-x-0 top-full z-30 mt-2 overflow-hidden rounded-lg border border-border bg-popover text-popover-foreground shadow-lg"
          >
            <h2 class="border-b border-border px-3 py-2 text-xs font-medium">
              {i18n.t("chat.errors.history")}
            </h2>
            <ScrollArea
              class="max-h-[min(24rem,60dvh)]"
              viewportProps={{ class: "ocg-subtle-scrollbar overscroll-contain" }}
            >
              <ol class="divide-y divide-border">
                {[...(history() ?? [])].reverse().map((error, index) => (
                  <li class="space-y-1 px-3 py-2.5 text-xs">
                    <time datetime={new Date(error.at).toISOString()} class="text-muted-foreground">
                      {new Date(error.at).toLocaleString(i18n.locale)}
                    </time>
                    <p class="whitespace-pre-wrap break-words text-destructive">{error.message}</p>
                    {index === 0 && error.retry && (
                      <button
                        type="button"
                        class="mt-1 underline underline-offset-2"
                        onClick={error.retry.run}
                      >
                        {error.retry.label}
                      </button>
                    )}
                  </li>
                ))}
              </ol>
            </ScrollArea>
          </section>
        )}
      </div>
    </Show>
  );
}
