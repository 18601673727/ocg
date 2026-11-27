import { ArrowDown, ArrowUpRight, Check, Copy, Download, Ellipsis, RefreshCw } from "@lucide/solid";
import { Dynamic, type JSX } from "@solidjs/web";
import {
  type Component,
  createEffect,
  createMemo,
  createSignal,
  For,
  onCleanup,
  Show,
} from "solid-js";
import {
  estimateReasoningTokens,
  ReasoningContent,
  ReasoningRoot,
  ReasoningText,
  ReasoningTrigger,
} from "@/components/ocg/chat/ui/reasoning";
import {
  ToolGroupContent,
  ToolGroupRoot,
  ToolGroupTrigger,
} from "@/components/ocg/chat/ui/tool-group";
import {
  FloatingIconButton,
  TooltipIconButton,
} from "@/components/ocg/chat/ui/tooltip-icon-button";
import { Button } from "@/components/ui/button";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { ariaState } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import type { ChildNotice, ChildResult } from "../contracts/api";
import { runtimeStateLabel, useI18n } from "../i18n";
import { WorkspaceLink } from "../layout/workspace-link";
import { MarkdownBody } from "../markdown/markdown-body";
import { providerRejectionText } from "../runtime/provider-rejection";
import { MessageContext, useConversationView, useThreadMessage } from "./conversation-view";
import { ImageGallery } from "./image-attachments";
import { followAfterScroll } from "./message-bounds";
import {
  captureThreadPosition,
  readThreadPosition,
  savedMessageElement,
  searchMessageElement,
  type ThreadSearchTarget,
  writeThreadPosition,
} from "./thread-position";
import { ToolActivityLabel } from "./tool-activity-label";
import { presentToolCall } from "./tool-part";
import { WorkspaceToolFallback } from "./workspace-tool";

export type ThreadProps = {
  conversationId?: string;
  searchTarget?: ThreadSearchTarget;
  Assistant: Component;
  Composer: Component<{ autoFocus: boolean }>;
  Welcome: Component;
  afterMessages?: JSX.Element;
  onOpenConversation: (conversation: string) => void;
};

export function Thread(props: ThreadProps) {
  const { t } = useI18n();
  const view = useConversationView();
  const empty = createMemo(() => view.messages().length === 0);
  const conversationId = createMemo(() => props.conversationId);
  const [following, setFollowing] = createSignal(true);
  const [positioning, setPositioning] = createSignal(false);
  const [historyError, setHistoryError] = createSignal<string>();
  let viewport!: HTMLDivElement;
  const [body, setBody] = createSignal<HTMLDivElement>();
  let lastTop = 0;
  let frame = 0;
  let saveTimer: number | undefined;
  let conversation: string | undefined;

  const atBottom = () => viewport.scrollHeight - viewport.clientHeight - viewport.scrollTop <= 8;

  function rememberPosition() {
    if (!conversation || positioning() || !viewport?.isConnected || empty()) return;
    if (following()) {
      writeThreadPosition(conversation);
    } else {
      const position = captureThreadPosition(viewport);
      if (position) writeThreadPosition(conversation, position);
    }
  }

  function rememberAfterScroll() {
    if (saveTimer !== undefined) return;
    saveTimer = window.setTimeout(() => {
      saveTimer = undefined;
      rememberPosition();
    }, 120);
  }

  function follow() {
    cancelAnimationFrame(frame);
    frame = requestAnimationFrame(() => {
      if (!following()) return;
      viewport.scrollTop = viewport.scrollHeight;
      lastTop = viewport.scrollTop;
    });
  }

  createEffect(() => view.messages().length, follow);
  createEffect(body, (content) => {
    if (!content) return;
    const observer = new ResizeObserver(follow);
    observer.observe(content);
    return () => observer.disconnect();
  });
  let positionedKey: string | undefined;
  let positionedTarget: ThreadSearchTarget | undefined;
  createEffect(
    () => [body(), conversationId(), props.searchTarget, empty()] as const,
    ([content, id, target, isEmpty]) => {
      conversation = id;
      setPositioning(false);
      if (!content || !id || isEmpty) return;
      if (positionedKey === id && positionedTarget === target) return;
      positionedKey = id;
      positionedTarget = target;
      const position = target ? undefined : readThreadPosition(id);
      if (!target && !position) return;

      let active = true;
      setPositioning(true);
      setFollowing(false);
      const findRow = () => {
        if (target) return searchMessageElement(viewport, view.messages(), target);
        return position ? savedMessageElement(viewport, position) : null;
      };
      const nextFrame = () =>
        new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));

      void (async () => {
        await nextFrame();
        if (!active) return;
        let row = findRow();
        while (!row) {
          const advanced = await view.loadEarlier("turns");
          await nextFrame();
          if (!active) return;
          row = findRow();
          if (!advanced) break;
        }
        if (!row) {
          setFollowing(true);
          follow();
          return;
        }
        viewport.scrollTop +=
          row.getBoundingClientRect().top -
          viewport.getBoundingClientRect().top -
          (position?.offset ?? 64);
        lastTop = viewport.scrollTop;
        setFollowing(atBottom());
        if (target) row.focus({ preventScroll: true });
        setPositioning(false);
        rememberPosition();
      })()
        .catch((error) => {
          if (active) setHistoryError(error instanceof Error ? error.message : String(error));
        })
        .finally(() => {
          if (active) setPositioning(false);
        });
      return () => {
        active = false;
      };
    },
  );
  window.addEventListener("pagehide", rememberPosition);
  onCleanup(() => {
    window.removeEventListener("pagehide", rememberPosition);
    clearTimeout(saveTimer);
    cancelAnimationFrame(frame);
    rememberPosition();
  });

  async function loadEarlier(button: HTMLButtonElement) {
    const rows = viewport.querySelector("[data-slot='aui_message-group']");
    const anchor = rows?.firstElementChild;
    const before = anchor?.getBoundingClientRect().top;
    setHistoryError(undefined);
    setFollowing(false);
    try {
      await view.loadEarlier();
      requestAnimationFrame(() => {
        if (anchor && anchor.isConnected && before !== undefined) {
          viewport.scrollTop += anchor.getBoundingClientRect().top - before;
          lastTop = viewport.scrollTop;
        }
        if (!button.isConnected && document.activeElement === document.body) {
          rows?.querySelector<HTMLElement>("button:not([disabled]), a[href]")?.focus();
        }
      });
    } catch (error) {
      setHistoryError(error instanceof Error ? error.message : String(error));
    }
  }

  return (
    <div
      class="aui-root aui-thread-root bg-background @container flex h-full min-h-0 flex-col"
      style={{
        "--thread-max-width": "56rem",
        "--composer-bg": "color-mix(in oklab, var(--color-muted) 30%, transparent)",
        "--composer-radius": "1rem",
        "--composer-padding": "8px",
      }}
    >
      <div
        ref={viewport}
        data-slot="aui_thread-viewport"
        tabindex={0}
        role="region"
        aria-label={t("nav.chat")}
        class="ocg-subtle-scrollbar relative flex min-h-0 flex-1 flex-col overflow-x-auto overflow-y-auto overscroll-contain"
        style={{ "overflow-anchor": following() ? "none" : "auto" }}
        onScroll={() => {
          if (positioning()) return;
          setFollowing(atBottom() || followAfterScroll(following(), lastTop, viewport));
          lastTop = viewport.scrollTop;
          rememberAfterScroll();
        }}
      >
        <div
          ref={setBody}
          class={cn(
            "mx-auto flex w-full max-w-(--thread-max-width) flex-1 flex-col px-4 pt-16",
            empty() && "justify-center",
          )}
        >
          <Show when={empty()}>
            <Dynamic component={props.Welcome} />
          </Show>
          <Show when={view.hasEarlier() || view.loadingEarlier()}>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              data-slot="aui_thread-load-earlier"
              class="text-muted-foreground mb-6 self-center rounded-full"
              disabled={view.loadingEarlier() || positioning()}
              onClick={(event) => void loadEarlier(event.currentTarget)}
            >
              {view.loadingEarlier() ? t("aui.loadingEarlier") : t("aui.loadEarlier")}
            </Button>
          </Show>
          <Show when={historyError()}>
            <p role="alert" class="text-destructive">
              {historyError()}
            </p>
          </Show>
          <div data-slot="aui_message-group" class="mb-14 flex flex-col gap-y-6 empty:hidden">
            <For each={view.messages()} keyed={(message) => message.id}>
              {(message) => (
                <MessageContext value={message}>
                  <Show
                    when={message().role === "user"}
                    fallback={<Dynamic component={props.Assistant} />}
                  >
                    <Show
                      when={message().childResults}
                      fallback={
                        <Show when={message().childNotice} fallback={<UserMessage />}>
                          <ChildNoticeMessage
                            notice={message().childNotice!}
                            onOpen={props.onOpenConversation}
                          />
                        </Show>
                      }
                    >
                      <ChildResultsMessage
                        onOpen={props.onOpenConversation}
                        expand={
                          !props.searchTarget?.answer &&
                          props.searchTarget?.turn === message().jobId
                        }
                      />
                    </Show>
                  </Show>
                </MessageContext>
              )}
            </For>
            {props.afterMessages}
          </div>
          <footer
            class={cn(
              "aui-thread-viewport-footer pointer-events-none flex flex-col gap-4 overflow-visible bg-transparent pb-4 [&>*]:pointer-events-auto md:pb-6",
              empty() ? "relative" : "sticky bottom-0 mt-auto rounded-t-(--composer-radius)",
            )}
          >
            <Show when={positioning() && view.loadingEarlier()}>
              <p
                role="status"
                class="absolute -top-9 self-center rounded-full bg-card px-3 py-1 text-sm text-muted-foreground shadow-sm"
              >
                {t("aui.loadingEarlier")}
              </p>
            </Show>
            <Show when={!following() && !positioning()}>
              <FloatingIconButton
                tooltip={t("aui.scrollBottom")}
                class="absolute -top-12 z-10 self-center"
                onClick={() => {
                  setFollowing(true);
                  follow();
                  rememberPosition();
                }}
              >
                <ArrowDown />
              </FloatingIconButton>
            </Show>
            <Dynamic component={props.Composer} autoFocus={true} />
          </footer>
        </div>
      </div>
    </div>
  );
}

function MessageActions(props: { assistant?: boolean }) {
  const { t } = useI18n();
  const message = useThreadMessage();
  const { retry } = useConversationView();
  const clipboard = useCopyToClipboard();
  const [retryError, setRetryError] = createSignal<string>();

  async function retryTurn() {
    setRetryError(undefined);
    try {
      await retry(message().id);
    } catch (error) {
      setRetryError(error instanceof Error ? error.message : String(error));
    }
  }

  function exportMarkdown() {
    const text = message().content;
    const blob = new Blob([text], { type: "text/markdown;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.download = "message.md";
    link.href = url;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }

  return (
    <div class="flex items-center gap-1 text-muted-foreground">
      <TooltipIconButton
        tooltip={t(props.assistant ? "common.copy" : "common.copySource")}
        onClick={() => void clipboard.copyToClipboard(message().content)}
      >
        <Show when={clipboard.isCopied()} fallback={<Copy />}>
          <Check />
        </Show>
      </TooltipIconButton>
      <Show when={props.assistant && message().canRetry}>
        <TooltipIconButton tooltip={t("aui.retryTurn")} onClick={() => void retryTurn()}>
          <RefreshCw />
        </TooltipIconButton>
      </Show>
      <Show when={props.assistant}>
        <details class="group relative">
          <summary
            title={t("aui.more")}
            aria-label={t("aui.more")}
            class="flex size-6 cursor-pointer items-center justify-center rounded hover:bg-muted"
          >
            <Ellipsis class="size-4" />
          </summary>
          <Button
            type="button"
            variant="outline"
            size="sm"
            class="absolute left-0 top-full z-10 bg-popover"
            onClick={(event) => {
              exportMarkdown();
              event.currentTarget.closest("details")?.removeAttribute("open");
            }}
          >
            <Download />
            {t("aui.exportMarkdown")}
          </Button>
        </details>
      </Show>
      <Show when={retryError()}>
        <p role="alert" class="text-destructive">
          {retryError()}
        </p>
      </Show>
    </div>
  );
}

function ChildNoticeMessage(props: {
  notice: ChildNotice;
  onOpen: (conversation: string) => void;
}) {
  const { t } = useI18n();
  const message = useThreadMessage();
  const label = () =>
    t(props.notice.kind === "help" ? "chat.child.helpRequest" : "chat.child.progressReport");
  return (
    <section
      aria-label={label()}
      data-slot="conversation-child-notice"
      data-message-id={message().id}
      data-turn-id={message().jobId}
      data-role="user"
      tabindex={-1}
      class={cn(
        "min-w-0 space-y-2 rounded-xl border border-border bg-muted/20 p-3 focus-visible:outline-2 focus-visible:outline-ring",
        props.notice.kind === "help" && "border-amber-500/40",
      )}
    >
      <div class="flex items-center justify-between gap-3">
        <h2 class="text-sm font-semibold">{label()}</h2>
        <MessageActions />
      </div>
      <button
        type="button"
        class="inline-flex min-w-0 max-w-full items-center gap-1 rounded text-left text-sm font-medium text-primary hover:underline focus-visible:outline-2 focus-visible:outline-ring"
        title={props.notice.title}
        onClick={() => props.onOpen(props.notice.conversation)}
      >
        <span class="truncate">{props.notice.title || t("chat.child.untitled")}</span>
        <ArrowUpRight class="size-3.5 shrink-0" aria-hidden="true" />
      </button>
      <p class="whitespace-pre-wrap wrap-break-word text-sm">
        {message().content.slice(props.notice.text_start_utf16)}
      </p>
    </section>
  );
}

function ChildResultsMessage(props: { onOpen: (conversation: string) => void; expand: boolean }) {
  const { t } = useI18n();
  const message = useThreadMessage();
  const answer = (result: ChildResult) =>
    message().content.slice(result.answer_start_utf16, result.answer_end_utf16).trim();
  return (
    <section
      aria-label={t("chat.child.results")}
      data-slot="conversation-child-results"
      data-message-id={message().id}
      data-turn-id={message().jobId}
      data-role="user"
      tabindex={-1}
      class="min-w-0 space-y-2 rounded-xl border border-border bg-muted/20 p-3 focus-visible:outline-2 focus-visible:outline-ring"
    >
      <div class="flex items-center justify-between gap-3">
        <h2 class="text-sm font-semibold">{t("chat.child.results")}</h2>
        <MessageActions />
      </div>
      <ul class="divide-y divide-border">
        <For each={message().childResults ?? []} keyed={(result) => result.conversation}>
          {(result) => (
            <li class="space-y-1 py-2 last:pb-0">
              <div class="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
                <button
                  type="button"
                  class="inline-flex min-w-0 max-w-full items-center gap-1 rounded text-left text-sm font-medium text-primary hover:underline focus-visible:outline-2 focus-visible:outline-ring"
                  title={result().title}
                  onClick={() => props.onOpen(result().conversation)}
                >
                  <span class="truncate">{result().title || t("chat.child.untitled")}</span>
                  <ArrowUpRight class="size-3.5 shrink-0" aria-hidden="true" />
                </button>
                <span
                  class={cn(
                    "text-xs text-muted-foreground",
                    (result().state === "failed" || result().state === "unknown") &&
                      "text-destructive",
                  )}
                >
                  {runtimeStateLabel(t, result().state)}
                </span>
              </div>
              <Show when={answer(result())}>
                <details class="text-sm" open={props.expand}>
                  <summary class="w-fit cursor-pointer rounded text-xs text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring">
                    {t("chat.child.viewResult")}
                  </summary>
                  <MarkdownBody content={answer(result())} class="mt-2 break-words" />
                </details>
              </Show>
            </li>
          )}
        </For>
      </ul>
    </section>
  );
}

function UserMessage() {
  const { t } = useI18n();
  const message = useThreadMessage();
  return (
    <div
      data-slot="aui_user-message-root"
      data-message-id={message().id}
      data-turn-id={message().jobId}
      data-role="user"
      tabindex={-1}
      style={{ contain: message().queued ? undefined : "layout style" }}
      class="grid auto-rows-auto grid-cols-[minmax(72px,1fr)_auto] content-start gap-y-2 [contain-intrinsic-size:auto_80px] focus-visible:outline-2 focus-visible:outline-ring [&>*]:col-start-2"
    >
      <ImageGallery images={message().images} compact />
      <Show when={message().content.trim()}>
        <div class="group/user-message relative col-start-2 min-w-0 max-w-full justify-self-end">
          <MarkdownBody
            content={message().content}
            class="bg-muted text-foreground rounded-(--composer-radius) px-4 py-2 wrap-break-word [&>:first-child]:mt-0 [&>:last-child]:mb-0"
          />
          <div class="pointer-events-none absolute start-0 top-1/2 -translate-x-full -translate-y-1/2 pe-2 opacity-0 transition-opacity group-hover/user-message:pointer-events-auto group-hover/user-message:opacity-100 group-focus-within/user-message:pointer-events-auto group-focus-within/user-message:opacity-100 motion-reduce:transition-none rtl:translate-x-full">
            <MessageActions />
          </div>
        </div>
      </Show>
      <Show when={message().queued}>
        <p
          role="status"
          data-slot="queued-message-status"
          class="text-right text-xs text-muted-foreground"
        >
          {t("chat.queuedMessage")}
        </p>
      </Show>
    </div>
  );
}

export function AssistantMessage(props: { footer?: JSX.Element }) {
  const { t } = useI18n();
  const message = useThreadMessage();
  const { retry } = useConversationView();
  const running = createMemo(
    () => message().status === "streaming" || message().status === "pending",
  );
  const parts = createMemo(() =>
    (message().toolCalls ?? []).map((call) => presentToolCall(call, message().jobId, t)),
  );
  const visible = createMemo(() =>
    Boolean(
      message().content.trim() || message().reasoning || parts().length || message().images?.length,
    ),
  );
  const failure = createMemo(() => {
    const rejection = message().failureRejection;
    if (rejection) return providerRejectionText(t, rejection);
    const code = message().failureCode;
    const key =
      code === "outcome-held"
        ? "chat.outcome.held"
        : code === "outcome-unconfirmed"
          ? "chat.outcome.unconfirmed"
          : code === "outcome-stopped"
            ? "chat.outcome.stopped"
            : undefined;
    return key
      ? [t(key), message().failureReason].filter(Boolean).join(" ")
      : message().failureReason;
  });

  return (
    <Show
      when={message().compaction?.kind}
      fallback={
        <Show
          when={!message().intermediate || visible() || message().failureReason || props.footer}
        >
          <div
            data-slot="aui_assistant-message-root"
            data-message-id={message().id}
            data-turn-id={message().jobId}
            data-role="assistant"
            tabindex={-1}
            style={{ contain: running() ? undefined : "layout style" }}
            data-intermediate={message().intermediate || undefined}
            class="relative -mb-7.5 pb-7.5 focus-visible:outline-2 focus-visible:outline-ring"
          >
            <div
              data-slot="aui_assistant-message-content"
              class={cn(
                "leading-relaxed wrap-break-word",
                message().intermediate ? "text-sm text-muted-foreground" : "text-foreground",
              )}
            >
              <Show
                when={
                  running() &&
                  message().showActivityIndicator &&
                  !visible() &&
                  !message().failureReason
                }
              >
                <p
                  role="status"
                  class="text-sm text-muted-foreground shimmer motion-reduce:animate-none"
                >
                  {t("chat.thinking")}
                </p>
              </Show>
              <Show when={Boolean(message().reasoning)}>
                <ReasoningRoot streaming={running()}>
                  <ReasoningTrigger
                    active={running()}
                    duration={
                      message().reasoningDurationMs == null
                        ? undefined
                        : message().reasoningDurationMs! / 1000
                    }
                    tokens={estimateReasoningTokens(message().reasoning ?? "")}
                  />
                  <ReasoningContent
                    streaming={running()}
                    aria-busy={ariaState(String(running()) as "true" | "false")}
                  >
                    <ReasoningText>
                      <MarkdownBody content={message().reasoning ?? ""} />
                    </ReasoningText>
                  </ReasoningContent>
                </ReasoningRoot>
              </Show>
              <Show when={Boolean(message().content)}>
                <MarkdownBody
                  content={message().content}
                  sourceSnapshots={message().sourceSnapshots}
                />
              </Show>
              <Show when={parts().length > 0}>
                <ToolGroupRoot variant="ghost">
                  <ToolGroupTrigger
                    count={parts().length}
                    active={running()}
                    summary={
                      <ToolActivityLabel
                        tools={parts().map((part) => ({
                          name: part.toolName,
                          arguments: part.argsText,
                          result: part.result,
                          durationMs: part.durationMs,
                          truncated: part.truncated,
                        }))}
                      />
                    }
                  />
                  <ToolGroupContent streaming={running()}>
                    <For
                      each={parts()}
                      keyed={(part) => `${part.jobId}:${part.call?.request}:${part.call?.index}`}
                    >
                      {(part) => <WorkspaceToolFallback {...part()} />}
                    </For>
                  </ToolGroupContent>
                </ToolGroupRoot>
              </Show>
              <ImageGallery images={message().images} />
              {props.footer}
              <Show
                when={failure() || message().status === "failed" || message().status === "unknown"}
              >
                <p role="alert" class="text-destructive mt-2 text-sm whitespace-pre-wrap">
                  {failure() ??
                    (message().status === "unknown" ? t("job.activity.unknown") : t("chat.failed"))}
                </p>
              </Show>
              <Show
                when={message().failureLink}
                fallback={
                  <Show
                    when={
                      failure() || message().status === "failed" || message().status === "unknown"
                    }
                  >
                    <p class="mt-1.5 text-sm">
                      <Show
                        when={message().canRetry}
                        fallback={
                          <WorkspaceLink
                            href={
                              message().jobId
                                ? `/?view=jobs&job=${encodeURIComponent(message().jobId!)}`
                                : "/?view=jobs"
                            }
                            class="underline underline-offset-2"
                          >
                            {t("chat.openJobs")}
                          </WorkspaceLink>
                        }
                      >
                        <Button
                          type="button"
                          size="xs"
                          variant="outline"
                          onClick={() => void retry(message().id)}
                        >
                          {t("common.retry")}
                        </Button>
                      </Show>
                    </p>
                  </Show>
                }
              >
                {(link) => (
                  <p class="mt-1 text-sm">
                    <WorkspaceLink href={link().href} class="underline underline-offset-2">
                      {link().label}
                    </WorkspaceLink>
                  </p>
                )}
              </Show>
              <Show when={message().failureDetail}>
                {(detail) => (
                  <p class="mt-1 text-xs whitespace-pre-wrap text-muted-foreground">
                    {t("chat.failureDetails", { text: detail() })}
                  </p>
                )}
              </Show>
            </div>
            <Show when={!message().intermediate && !running()}>
              <div
                data-slot="aui_assistant-message-footer"
                class="flex min-h-7.5 items-center pt-1.5"
              >
                <MessageActions assistant />
              </div>
            </Show>
          </div>
        </Show>
      }
    >
      {(kind) => (
        <div
          data-slot="context-compacted"
          data-message-id={message().id}
          data-turn-id={message().jobId}
          tabindex={-1}
          class="my-4 flex items-center gap-3 px-2 text-xs text-muted-foreground"
          role="status"
        >
          <span class="h-px flex-1 bg-border" />
          <span>
            {t("chat.compacted")} ·{" "}
            {t(
              kind() === "predictive"
                ? "chat.compactionPredictive"
                : kind() === "rule_based"
                  ? "chat.compactionRule"
                  : "chat.compactionManual",
            )}
          </span>
          <span class="h-px flex-1 bg-border" />
        </div>
      )}
    </Show>
  );
}
