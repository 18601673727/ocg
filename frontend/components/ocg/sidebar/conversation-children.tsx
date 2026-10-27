"use client";

import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { LoaderCircle } from "lucide-react";
import { runtimeStateLabel, useI18n } from "../i18n";
import type { ControlChildConversation } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";

export function SidebarConversationChildren({
  conversation,
  control,
  activeChild,
  expanded,
  sessionId,
  onAvailabilityChange,
  onOpen,
}: {
  conversation: string;
  control: HttpCanonicalControlClient;
  activeChild?: string;
  expanded: boolean;
  sessionId: string;
  onAvailabilityChange: (sessionId: string, available: boolean) => void;
  onOpen: (conversation: string) => void;
}) {
  const { t } = useI18n();
  const [children, setChildren] = useState<ControlChildConversation[]>([]);
  const [pages, setPages] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!expanded) return;
    return control.subscribeChildren(
      conversation,
      pages,
      (result) => {
        const items = result.children.filter((child) => child.origin !== "system");
        setChildren(items);
        setHasMore(result.has_more);
        onAvailabilityChange(sessionId, items.length > 0);
        setError(null);
      },
      (cause) => setError(cause instanceof Error ? cause.message : String(cause)),
    );
  }, [control, conversation, onAvailabilityChange, pages, sessionId, expanded]);
  if (!expanded) return null;
  return (
    <>
      {error && <DismissibleNotice message={error} className="ml-6 px-2 py-1 text-xs" />}
      {(children.length > 0 || hasMore) && (
        <ul
          className="ml-5 min-w-0 max-w-full border-l border-border pl-2"
          aria-label={t("chat.children")}
        >
          {children.map((child) => (
            <li key={child.id} data-sidebar-level="2" className="min-w-0 max-w-full">
              <button
                type="button"
                onClick={() => onOpen(child.id)}
                aria-current={activeChild === child.id ? "true" : undefined}
                title={`${child.title || t("chat.child.untitled")} · ${child.state !== "settled" && child.on_hold ? t(`chat.child.hold.${child.on_hold.by.kind}`) : runtimeStateLabel(t, child.job_state)}`}
                className={cn(
                  "grid w-full min-w-0 max-w-full grid-cols-[minmax(0,1fr)_auto] items-center overflow-hidden rounded-md px-2 py-1.5 text-left text-sm leading-5",
                  activeChild === child.id
                    ? "bg-sidebar-accent font-medium text-sidebar-accent-foreground"
                    : "text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-foreground",
                )}
              >
                <span className="min-w-0 overflow-hidden whitespace-nowrap [mask-image:linear-gradient(to_right,black_calc(100%-1rem),transparent)]">
                  {child.title || t("chat.child.untitled")}
                </span>
                <span
                  className={cn(
                    "ml-2 shrink-0 text-xs",
                    child.job_state === "failed" && "text-destructive",
                  )}
                >
                  {child.job_state === "running" && !child.on_hold ? (
                    <LoaderCircle
                      className="size-3.5 animate-spin motion-reduce:animate-none text-primary"
                      aria-label={runtimeStateLabel(t, child.job_state)}
                    />
                  ) : child.state !== "settled" && child.on_hold ? (
                    t(`chat.child.hold.${child.on_hold.by.kind}`)
                  ) : (
                    runtimeStateLabel(t, child.job_state)
                  )}
                </span>
              </button>
            </li>
          ))}
          {hasMore && (
            <li>
              <button
                type="button"
                className="px-2 py-1.5 text-xs text-muted-foreground hover:text-foreground"
                onClick={() => setPages((value) => value + 1)}
              >
                {t("chat.child.more")}
              </button>
            </li>
          )}
        </ul>
      )}
    </>
  );
}
