"use client";

import { useEffect, useState, type ReactNode } from "react";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import { dismissNotice, isNoticeDismissed, noticeIdentity } from "@/lib/notice-dismissal";

export function DismissibleNotice({ message, children, className, tone = "error", id }: {
  message: string; children?: ReactNode; className?: string; tone?: "error" | "warning"; id?: string;
}) {
  const { t } = useI18n();
  const signature = JSON.stringify([id ?? "notice", message]);
  const [notice, setNotice] = useState<{ signature: string; identity: string; dismissed: boolean } | null>(null);
  useEffect(() => {
    let active = true;
    void noticeIdentity("notice", signature).then(identity => {
      if (active) setNotice({ signature, identity, dismissed: isNoticeDismissed(identity) });
    });
    return () => { active = false; };
  }, [signature]);
  if (!notice || notice.signature !== signature || notice.dismissed) return null;
  return <div id={id} role={tone === "error" ? "alert" : "status"} className={cn("flex items-start gap-2 text-sm", tone === "error" ? "text-destructive" : "text-muted-foreground", className)}>
    <div className="min-w-0 flex-1 break-words">{children ?? message}</div>
    <button type="button" onClick={() => {
      dismissNotice(notice.identity);
      setNotice({ ...notice, dismissed: true });
    }} aria-label={t("common.dismiss")} title={t("common.dismiss")} className="-my-1 flex size-7 shrink-0 items-center justify-center rounded-md hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"><X className="size-3.5" aria-hidden="true" /></button>
  </div>;
}
