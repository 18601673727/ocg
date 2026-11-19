import { Activity, CalendarDays, Folder, MessageSquare } from "@lucide/solid";
import { type JSX, Portal } from "@solidjs/web";
import { createMemo, createSignal, onCleanup, Show } from "solid-js";
import { composeButton } from "@/lib/ui-state";
import { useI18n } from "../i18n";

const PREVIEW_DELAY_MS = 600;

export type ConversationPreviewDetails = {
  project?: string | null;
  status?: string;
  updatedAt?: string;
};

export function ConversationPreview(
  props: ConversationPreviewDetails & {
    title: string;
    trigger: JSX.Element;
    children?: JSX.Element;
    suppressed?: boolean;
  },
) {
  const i18n = useI18n();
  const [bounds, setBounds] = createSignal<DOMRect>();
  const element = composeButton(props.trigger, {
    children: props.children,
    attrs: { "data-slot": "conversation-preview-trigger" },
  });
  const date = createMemo(() => (props.updatedAt ? new Date(props.updatedAt) : undefined));
  let timer: ReturnType<typeof setTimeout>;
  if (element instanceof HTMLElement) {
    const enter = () => {
      clearTimeout(timer);
      timer = setTimeout(() => {
        if (!props.suppressed) setBounds(element.getBoundingClientRect());
      }, PREVIEW_DELAY_MS);
    };
    const leave = () => {
      clearTimeout(timer);
      setBounds(undefined);
    };
    element.addEventListener("pointerenter", enter);
    element.addEventListener("pointerleave", leave);
    onCleanup(() => {
      clearTimeout(timer);
      element.removeEventListener("pointerenter", enter);
      element.removeEventListener("pointerleave", leave);
    });
  }
  return (
    <>
      {element}
      <Show when={!props.suppressed && bounds()}>
        {(anchor) => (
          <Portal>
            <div
              data-slot="conversation-preview"
              class="pointer-events-none fixed z-40 w-80 max-w-[calc(100vw-1.5rem)] space-y-3 rounded-2xl border border-border bg-popover p-4 text-popover-foreground shadow-lg"
              style={{
                left: `${Math.min(anchor().right + 8, window.innerWidth - 332)}px`,
                top: `${Math.max(12, anchor().top)}px`,
              }}
            >
              <p class="text-sm leading-snug font-semibold [overflow-wrap:anywhere]">
                {props.title}
              </p>
              <div class="space-y-2 text-sm text-muted-foreground">
                <Show when={props.project !== undefined}>
                  <div class="flex items-center gap-2">
                    <Show
                      when={props.project}
                      fallback={<MessageSquare class="size-4 shrink-0" aria-hidden="true" />}
                    >
                      <Folder class="size-4 shrink-0" aria-hidden="true" />
                    </Show>
                    <span class="min-w-0 [overflow-wrap:anywhere]">
                      {props.project ?? i18n.t("chat.pureConversation")}
                    </span>
                  </div>
                </Show>
                <Show when={props.status}>
                  <div class="flex items-center gap-2">
                    <Activity class="size-4 shrink-0" aria-hidden="true" />
                    {props.status}
                  </div>
                </Show>
                <Show when={Number.isFinite(date()?.getTime())}>
                  <div class="flex items-center gap-2">
                    <CalendarDays class="size-4 shrink-0" aria-hidden="true" />
                    <time datetime={props.updatedAt} class="tabular-nums">
                      {i18n.t("usage.updated")} {date()!.toLocaleString(i18n.locale)}
                    </time>
                  </div>
                </Show>
              </div>
            </div>
          </Portal>
        )}
      </Show>
    </>
  );
}
