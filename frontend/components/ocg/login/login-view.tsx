"use client";

import { Cloud, Loader2, TriangleAlert } from "lucide-react";
import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ACCESS_STATE_COPY } from "../bootstrap/presentation";

/**
 * Restrained access surface. This is a mock handoff: no credential form, no
 * provider sign-in, and no redirect to an external identity system.
 */
export function LoginView() {
  const router = useRouter();
  const { snapshot, requestAccessHandoff } = useOcgRuntime();
  const access = snapshot.bootstrap.access;
  const copy = ACCESS_STATE_COPY[access.state];
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
        aria-label="Workspace access"
        className="w-full max-w-md rounded-lg border border-border bg-muted/10 p-6"
      >
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="flex size-7 items-center justify-center rounded-md border border-border bg-muted text-[10px] font-bold tracking-[0.16em]" aria-label="OCG">
              OCG
            </span>
            <div>
              <p className="text-[10px] font-medium tracking-[0.16em] text-muted-foreground uppercase">Remote access</p>
              <h1 className="text-[15px] font-semibold tracking-tight">{copy.title}</h1>
            </div>
          </div>
          <span className="rounded border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground">Remote</span>
        </div>

        <p className="mt-2 text-[12px] leading-5 text-muted-foreground">
          {access.detail ?? copy.detail}
        </p>

        <div
          role="status"
          className={cn(
            "mt-4 flex items-center gap-2 rounded-md border px-2.5 py-2 text-[11px]",
            failed ? "border-red-500/40 bg-red-500/5 text-red-700 dark:text-red-300" : "border-border bg-background text-muted-foreground",
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
              ? "Waiting for Cloudflare Access handoff"
              : complete
                ? "Cloudflare Access handoff complete"
                : failed
                  ? "Cloudflare Access handoff not completed"
                  : "Cloudflare Access handoff available"}
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
            {pending ? "Waiting" : denied ? "Access denied" : copy.button}
          </Button>
        </div>

        <p className="mt-4 text-[11px] leading-4 text-muted-foreground">
          Mock access surface. No credential is entered, stored, or transmitted in this phase.
        </p>
      </section>
    </div>
  );
}
