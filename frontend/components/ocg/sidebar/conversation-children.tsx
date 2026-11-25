import { createEffect, createSignal } from "solid-js";
import { cn } from "@/lib/utils";
import type { ChildConversation } from "../contracts";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { SELECTED_ROW } from "../primitives/selection";
import type { ApiClient } from "../runtime/api-client";
import { childConversationStatus } from "../runtime/job-status";
import { ConversationPreview } from "./conversation-preview";
import { ConversationStatus } from "./conversation-status";
import { HoverMarquee } from "./hover-marquee";

export function SidebarConversationChildren(props: {
  conversation: string;
  api: ApiClient;
  activeChild?: string;
  project?: string | null;
  expanded: boolean;
  sessionId: string;
  onAvailabilityChange: (sessionId: string, available: boolean) => void;
  onOpen: (conversation: string) => void;
  onRename?: (child: ChildConversation) => void;
}) {
  const i18n = useI18n();
  const [children, setChildren] = createSignal<ChildConversation[]>([]);
  const [pages, setPages] = createSignal(1);
  const [hasMore, setHasMore] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  createEffect(
    () => [
      props.api,
      props.conversation,
      props.onAvailabilityChange,
      pages(),
      props.sessionId,
      props.expanded,
    ],
    () => {
      if (!props.expanded) return;
      return props.api.subscribeChildren(
        props.conversation,
        pages(),
        (result) => {
          const items = result.children.filter((child) => child.origin !== "system");
          setChildren(items);
          setHasMore(result.has_more);
          props.onAvailabilityChange(props.sessionId, items.length > 0);
          setError(null);
        },
        (cause) => setError(cause instanceof Error ? cause.message : String(cause)),
      );
    },
  );
  return (
    <>
      {(() => {
        if (!props.expanded) return null;
        return (
          <>
            {error() && <DismissibleNotice message={error()!} class="ml-6 px-2 py-1 text-xs" />}
            {(children().length > 0 || hasMore()) && (
              <ul
                class="ml-5 min-w-0 max-w-full border-l border-border pl-2"
                aria-label={i18n.t("chat.children")}
              >
                {children().map((child) => {
                  const jobState = child.job_state;
                  const title = child.title || i18n.t("chat.child.untitled");
                  const stateLabel = childConversationStatus(i18n.t, child);
                  const status =
                    child.state === "open" &&
                    !child.on_hold &&
                    (jobState === "queued" || jobState === "running")
                      ? jobState
                      : null;
                  return (
                    <li data-sidebar-level="2" class="min-w-0 max-w-full">
                      <ConversationPreview
                        title={title}
                        project={props.project}
                        status={stateLabel}
                        trigger={
                          <button
                            type="button"
                            onClick={() => props.onOpen(child.id)}
                            onDblClick={() => props.onRename?.(child)}
                            aria-current={props.activeChild === child.id ? "true" : undefined}
                            class={cn(
                              "grid w-full min-w-0 max-w-full grid-cols-[minmax(0,1fr)_auto] items-center overflow-hidden rounded-md px-2 py-1.5 text-left text-sm leading-5",
                              props.activeChild === child.id
                                ? cn(SELECTED_ROW, "font-medium")
                                : "text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-foreground",
                            )}
                          />
                        }
                      >
                        <span class="flex min-w-0 items-center gap-1.5">
                          {status && <ConversationStatus state={status} />}
                          <HoverMarquee>{title}</HoverMarquee>
                        </span>
                        {!status && (
                          <span
                            class={cn(
                              "ml-2 shrink-0 text-xs",
                              jobState === "failed" && "text-destructive",
                            )}
                          >
                            {stateLabel}
                          </span>
                        )}
                      </ConversationPreview>
                    </li>
                  );
                })}
                {hasMore() && (
                  <li>
                    <button
                      type="button"
                      class="px-2 py-1.5 text-xs text-muted-foreground hover:text-foreground"
                      onClick={() => setPages((value) => value + 1)}
                    >
                      {i18n.t("chat.child.more")}
                    </button>
                  </li>
                )}
              </ul>
            )}
          </>
        );
      })()}
    </>
  );
}
