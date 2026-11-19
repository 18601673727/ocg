import { X } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { dismissNotice, isNoticeDismissed, noticeIdentity } from "@/lib/notice-dismissal";
import { type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";

export function DismissibleNotice(props: {
  message: string;
  children?: View;
  class?: string;
  tone?: "error" | "warning";
  id?: string;
}) {
  const i18n = useI18n();
  const signature = createMemo(() => JSON.stringify([props.id ?? "notice", props.message]));
  const [notice, setNotice] = createSignal<{
    signature: string;
    identity: string;
    dismissed: boolean;
  } | null>(null);
  createEffect(
    () => [signature()],
    () => {
      let active = true;
      void noticeIdentity("notice", signature()).then((identity) => {
        if (active)
          setNotice({ signature: signature(), identity, dismissed: isNoticeDismissed(identity) });
      });
      return () => {
        active = false;
      };
    },
  );
  return (
    <>
      {(() => {
        if (!notice() || notice()!.signature !== signature() || notice()!.dismissed) return null;
        return (
          <div
            id={props.id}
            role={
              (props.tone === undefined ? "error" : props.tone) === "error" ? "alert" : "status"
            }
            class={cn(
              "flex items-start gap-2 text-sm",
              (props.tone === undefined ? "error" : props.tone) === "error"
                ? "text-destructive"
                : "text-muted-foreground",
              props.class,
            )}
          >
            <div class="min-w-0 flex-1 break-words">{props.children ?? props.message}</div>
            <button
              type="button"
              onClick={() => {
                dismissNotice(notice()!.identity);
                setNotice((current) => (current ? { ...current, dismissed: true } : current));
              }}
              aria-label={i18n.t("common.dismiss")}
              title={i18n.t("common.dismiss")}
              class="-my-1 flex size-7 shrink-0 items-center justify-center rounded-md hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              <X class="size-3.5" aria-hidden="true" />
            </button>
          </div>
        );
      })()}
    </>
  );
}
