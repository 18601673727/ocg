"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { createProfileClient } from "../profile/profile-client";
import { useOcgControlUrl } from "../profile/control-url";

/**
 * Neutral launch route: the PWA opener lands here and the frontend decides,
 * from the one backend readiness authority (`GET /api/v1/profile` decoded by
 * the profile client + `runnableChoices`), whether setup is incomplete.
 *
 * Exactly one readiness semantics exists: the same `runnableChoices` the
 * onboarding/profile UI uses. The launcher never decides locally, and no
 * fixture scenario is involved.
 */
export function BootstrapEntry() {
  const router = useRouter();
  const controlUrl = useOcgControlUrl();
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (!controlUrl) {
        if (!cancelled) setError("No loopback control endpoint is available for this session.");
        return;
      }
      try {
        const client = createProfileClient(controlUrl, fetch);
        const view = await client.read();
        if (cancelled) return;
        // The one readiness authority is backend-computed: model keys that
        // satisfy the same selection/endpoint/credential rules as canonical
        // launch. No fixture scenario and no local re-derivation here.
        const ready = view.runnable_choices.length > 0;
        router.replace(ready ? "/?scenario=local-ready" : "/onboarding?scenario=local-first-run");
      } catch (cause) {
        if (!cancelled) {
          setError(cause instanceof Error ? cause.message : "Could not read the OCG Profile endpoint.");
        }
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [controlUrl, router, attempt]);

  if (error !== null) {
    return (
      <main className="flex min-h-screen flex-col items-center justify-center gap-3 p-6 text-sm">
        <p role="alert">OCG startup could not determine readiness: {error}</p>
        <button type="button" onClick={() => { setError(null); setAttempt((value) => value + 1); }}>
          Retry
        </button>
      </main>
    );
  }
  return (
    <main className="flex min-h-screen items-center justify-center p-6 text-sm text-muted-foreground">
      Starting OCG…
    </main>
  );
}
