import { ArrowDown, Check, Copy, Download, Ellipsis, RefreshCw } from "@lucide/solid";
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
import { useI18n } from "../i18n";
import { MarkdownBody } from "../markdown/markdown-body";
import { MessageContext, useConversationView, useThreadMessage } from "./conversation-view";
import { ImageGallery } from "./image-attachments";
import { followAfterScroll } from "./message-bounds";
import { ToolActivityLabel } from "./tool-activity-label";
import { presentToolCall } from "./tool-part";
import { WorkspaceToolFallback } from "./workspace-tool";

export type ThreadProps = {
  Assistant: Component;
  Composer: Component<{ autoFocus: boolean }>;
  Welcome: Component;
  afterMessages?: JSX.Element;
  footerAction?: JSX.Element;
};

export function Thread(props: ThreadProps) {
  const { t } = useI18n();
  const view = useConversationView();
  const empty = createMemo(() => view.messages().length === 0);
  const [following, setFollowing] = createSignal(true);
  const [historyError, setHistoryError] = createSignal<string>();
  let viewport!: HTMLDivElement;
  const [body, setBody] = createSignal<HTMLDivElement>();
  let lastTop = 0;
  let frame = 0;

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
  onCleanup(() => cancelAnimationFrame(frame));

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
          const atBottom = viewport.scrollHeight - viewport.clientHeight - viewport.scrollTop <= 8;
          setFollowing(atBottom || followAfterScroll(following(), lastTop, viewport));
          lastTop = viewport.scrollTop;
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
              disabled={view.loadingEarlier()}
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
                    <UserMessage />
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
            <Show when={props.footerAction}>
              <div class="absolute -top-12 z-10 -translate-x-12 self-center">
                {props.footerAction}
              </div>
            </Show>
            <Show when={!following()}>
              <FloatingIconButton
                tooltip={t("aui.scrollBottom")}
                class="absolute -top-12 z-10 self-center"
                onClick={() => {
                  setFollowing(true);
                  follow();
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

function UserMessage() {
  const { t } = useI18n();
  const message = useThreadMessage();
  return (
    <div
      data-slot="aui_user-message-root"
      data-role="user"
      class="grid auto-rows-auto grid-cols-[minmax(72px,1fr)_auto] content-start gap-y-2 [contain-intrinsic-size:auto_80px] [&>*]:col-start-2"
    >
      <ImageGallery images={message().images} compact />
      <div class="relative col-start-2 min-w-0 justify-self-end">
        <div class="peer bg-muted text-foreground rounded-(--composer-radius) px-4 py-2 whitespace-pre-wrap wrap-break-word empty:hidden">
          {message().content}
        </div>
        <div class="absolute start-0 top-1/2 -translate-x-full -translate-y-1/2 pe-2 peer-empty:hidden rtl:translate-x-full">
          <MessageActions />
        </div>
      </div>
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

  return (
    <Show
      when={message().compaction?.kind}
      fallback={
        <Show
          when={!message().intermediate || visible() || message().failureReason || props.footer}
        >
          <div
            data-slot="aui_assistant-message-root"
            data-role="assistant"
            data-intermediate={message().intermediate || undefined}
            class="relative -mb-7.5 pb-7.5"
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
              <Show when={message().reasoning}>
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
              <Show when={message().content}>
                <MarkdownBody
                  content={message().content}
                  sourceSnapshots={message().sourceSnapshots}
                />
              </Show>
              <Show when={parts().length}>
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
                    <For each={parts()} keyed={(part) => part.toolCallId}>
                      {(part) => <WorkspaceToolFallback {...part()} />}
                    </For>
                  </ToolGroupContent>
                </ToolGroupRoot>
              </Show>
              <ImageGallery images={message().images} />
              {props.footer}
              <Show when={message().failureReason || message().status === "failed"}>
                <p role="alert" class="text-destructive mt-2 text-sm whitespace-pre-wrap">
                  {message().failureReason ?? t("chat.failed")}
                </p>
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
