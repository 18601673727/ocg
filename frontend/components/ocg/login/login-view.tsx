"use client";

import { Cloud, Loader2, TriangleAlert } from "lucide-react";
import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { TONE_CLASS } from "@/components/ocg/primitives";
import { cn } from "@/lib/utils";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";

/**
 * Restrained access surface. This is a mock handoff: no credential form, no
 * provider sign-in, and no redirect to an external identity system.
 */
export function LoginView() {
  const router = useRouter();
  const { t } = useI18n();
  const { snapshot, requestAccessHandoff } = useOcgRuntime();
  const access = snapshot.bootstrap.access;
  const copyTitle = t(
    access.state === "local"
      ? "login.access.localTitle"
      : access.state === "authenticated"
        ? "login.access.grantedTitle"
        : access.state === "unauthenticated"
          ? "login.access.requiredTitle"
          : access.state === "session-expired"
            ? "login.access.expiredTitle"
            : access.state === "denied"
              ? "login.access.deniedTitle"
              : "login.access.authRequiredTitle",
  );
  const copyDetail = t(
    access.state === "local"
      ? "login.access.localDetail"
      : access.state === "authenticated"
        ? "login.access.grantedDetail"
        : access.state === "unauthenticated"
          ? "login.access.requiredDetail"
          : access.state === "session-expired"
            ? "login.access.expiredDetail"
            : access.state === "denied"
              ? "login.access.deniedDetail"
              : "login.access.authRequiredDetail",
  );
  const copyButton = t(
    access.state === "local" || access.state === "authenticated"
      ? "login.access.continue"
      : access.state === "session-expired"
        ? "login.access.refresh"
        : access.state === "denied"
          ? "login.access.retry"
          : "login.access.start",
  );
  const pending = access.handoffState === "pending";
  const complete = access.handoffState === "complete";
  const failed = access.handoffState === "failed";
  const denied = access.state === "denied";

  useEffect(() => {
    if (!complete) return;
    // The mock handoff changes the normalized bootstrap snapshot. Move through
    // the same entry boundary a real backend will eventually own.
    router.replace(
      snapshot.bootstrap.onboarding
        ? "/?scenario=remote-authenticated-first-run"
        : "/?scenario=remote-authenticated-ready",
    );
  }, [complete, router, snapshot.bootstrap.onboarding]);

  return (
    <div className="flex min-h-dvh items-center justify-center bg-background px-4 py-10 text-foreground">
      <section
        aria-label={t("login.workspaceAccess")}
        className="w-full max-w-md rounded-lg border border-border bg-muted/10 p-6"
      >
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="flex size-7 items-center justify-center rounded-md border border-border bg-muted text-[10px] font-bold tracking-[0.16em]" aria-label="OCG">
              OCG
            </span>
            <div>
              <p className="text-[10px] font-medium tracking-[0.16em] text-muted-foreground uppercase">{t("login.remoteAccess")}</p>
              <h1 className="text-[15px] font-semibold tracking-tight">{copyTitle}</h1>
            </div>
          </div>
          <span className="rounded border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground">{t("login.remote")}</span>
        </div>

        <p className="mt-2 text-[12px] leading-5 text-muted-foreground">
          {access.detail ?? copyDetail}
        </p>

        <div
          role="status"
          className={cn(
            "mt-4 flex items-center gap-2 rounded-md border px-2.5 py-2 text-[11px]",
            failed ? TONE_CLASS.red : "bg-background text-muted-foreground",
          )}
        >
          {pending ? (
            <Loader2 className="size-3.5 animate-spin" aria-hidden="true" />
          ) : failed ? (
            <TriangleAlert className="size-3.5" aria-hidden="true" />
          ) : (
            <Cloud className="size-3.5" aria-hidden="true" />
          )}
          <span>
            {pending
              ? t("login.waiting")
              : complete
                ? t("login.complete")
                : failed
                  ? t("login.notCompleted")
                  : t("login.available")}
          </span>
        </div>

        <div className="mt-5">
          <Button
            className="w-full"
            onClick={() => {
              void requestAccessHandoff();
            }}
            disabled={pending || complete || denied}
            aria-busy={pending}
          >
            {pending ? t("login.waitingShort") : denied ? t("login.denied") : copyButton}
          </Button>
        </div>

        <p className="mt-4 text-[11px] leading-4 text-muted-foreground">
          {t("login.mockNote")}
        </p>
      </section>
    </div>
  );
}
