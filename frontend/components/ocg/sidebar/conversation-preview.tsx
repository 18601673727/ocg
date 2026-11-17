"use client";

import { PreviewCard } from "@base-ui/react/preview-card";
import { Activity, CalendarDays, Folder, MessageSquare } from "lucide-react";
import { type ReactElement, type ReactNode, useEffect, useRef } from "react";
import { useI18n } from "../i18n";

const PREVIEW_DELAY_MS = 600;

export type ConversationPreviewDetails = {
  project?: string | null;
  status?: string;
  updatedAt?: string;
};

export function ConversationPreview({
  title,
  project,
  status,
  updatedAt,
  trigger,
  children,
  suppressed = false,
}: ConversationPreviewDetails & {
  title: string;
  trigger: ReactElement;
  children?: ReactNode;
  suppressed?: boolean;
}) {
  const { locale, t } = useI18n();
  const actions = useRef<PreviewCard.Root.Actions>(null);
  useEffect(() => {
    if (suppressed) actions.current?.close();
  }, [suppressed]);
  const date = updatedAt && new Date(updatedAt);
  const ProjectIcon = project ? Folder : MessageSquare;
  return (
    <PreviewCard.Root
      actionsRef={actions}
      onOpenChange={(open, details) => {
        if (open && suppressed) details.cancel();
      }}
    >
      <PreviewCard.Trigger
        data-slot="conversation-preview-trigger"
        delay={PREVIEW_DELAY_MS}
        closeDelay={160}
        render={trigger}
      >
        {children}
      </PreviewCard.Trigger>
      <PreviewCard.Portal>
        <PreviewCard.Positioner
          side="right"
          align="start"
          sideOffset={8}
          collisionPadding={12}
          className="z-40"
        >
          <PreviewCard.Popup
            data-slot="conversation-preview"
            className="w-80 max-w-(--available-width) space-y-3 rounded-2xl border border-border bg-popover p-4 text-popover-foreground shadow-lg outline-none"
          >
            <p className="text-sm leading-snug font-semibold [overflow-wrap:anywhere]">{title}</p>
            <div className="space-y-2 text-sm text-muted-foreground">
              {project !== undefined && (
                <div className="flex items-center gap-2">
                  <ProjectIcon className="size-4 shrink-0" aria-hidden="true" />
                  <span className="min-w-0 [overflow-wrap:anywhere]">
                    {project ?? t("chat.pureConversation")}
                  </span>
                </div>
              )}
              {status && (
                <div className="flex items-center gap-2">
                  <Activity className="size-4 shrink-0" aria-hidden="true" />
                  <span>{status}</span>
                </div>
              )}
              {date && Number.isFinite(date.getTime()) && (
                <div className="flex items-center gap-2">
                  <CalendarDays className="size-4 shrink-0" aria-hidden="true" />
                  <time dateTime={updatedAt} className="tabular-nums">
                    {t("usage.updated")} {date.toLocaleString(locale)}
                  </time>
                </div>
              )}
            </div>
          </PreviewCard.Popup>
        </PreviewCard.Positioner>
      </PreviewCard.Portal>
    </PreviewCard.Root>
  );
}
