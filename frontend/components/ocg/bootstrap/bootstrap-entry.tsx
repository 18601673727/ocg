"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { createProfileClient } from "../profile/profile-client";
import { useOcgControlUrl } from "../profile/control-url";
import { resolveProjectParam, withProjectParam } from "../project/domain";
import { useI18n } from "../i18n";

/**
 * Neutral launch route: the PWA opener lands here and the frontend decides,
 * from the one backend readiness authority (`GET /api/v1/profile` decoded by
 * the profile client + backend `runnable_choices`), whether setup is incomplete.
 *
 * Exactly one readiness semantics exists: the same backend `runnable_choices` the
 * onboarding/profile UI uses. The launcher never decides locally, and no
 * fixture scenario is involved.
 *
 * The launcher hands the Project OCG was launched with as an explicit
 * `project` parameter. It is forwarded verbatim to the workspace entry, which
 * reads it as the explicit Project of this entry, so a Project the browser
 * persisted earlier never silently wins over the launched one. No Project is
 * chosen here: an absent parameter stays absent.
 */
export function BootstrapEntry() {
  const router = useRouter();
  const controlUrl = useOcgControlUrl();
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const { t } = useI18n();

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (!controlUrl) {
        if (!cancelled) setError(t("shell.noControlEndpoint"));
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
        const target = ready ? "/?scenario=local-ready" : "/onboarding?scenario=local-first-run";
        // The launched Project travels with the entry so the workspace sees the
        // same explicit identity the launcher registered.
        const project = resolveProjectParam(new URLSearchParams(window.location.search).get("project"));
        router.replace(project ? withProjectParam(target, project) : target);
      } catch (cause) {
        if (!cancelled) {
          setError(cause instanceof Error ? cause.message : t("shell.profileReadFailed"));
        }
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [controlUrl, router, attempt, t]);

  if (error !== null) {
    return (
      <main className="flex min-h-screen flex-col items-center justify-center gap-3 p-6 text-sm">
        <p role="alert">{t("shell.readinessFailed", { error })}</p>
        <button type="button" onClick={() => { setError(null); setAttempt((value) => value + 1); }}>
          {t("shell.retry")}
        </button>
      </main>
    );
  }
  return (
    <main className="flex min-h-screen items-center justify-center p-6 text-sm text-muted-foreground">
      {t("shell.starting")}
    </main>
  );
}
