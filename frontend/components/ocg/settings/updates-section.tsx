"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import type { Update, UpdateState } from "../contracts/api";
import { useI18n } from "../i18n";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { Section, describeError } from "./settings-parts";

const seenKey = "ocg-settings-update";
const active = (state: UpdateState | null) =>
  state?.phase === "waiting" || state?.phase === "installing";
const identity = (state: UpdateState) => `${state.started_at}:${state.to}`;

export function UpdatesSection({ client }: { client: HttpCanonicalControlClient }) {
  const { t } = useI18n();
  const [view, setView] = useState<Update | null>(null);
  const [error, setError] = useState<unknown>();
  const [busy, setBusy] = useState(false);
  const [now, setNow] = useState(0);
  const [installingAt, setInstallingAt] = useState<number | null>(null);
  const running = useRef<string | null>(null);
  const installing = useRef<{ id: string; started: number } | null>(null);
  const openedAt = useRef(0);
  const [seen, setSeen] = useState<string | null>(null);

  const read = useCallback(
    async (latest: boolean, signal?: AbortSignal) => {
      try {
        const next = await client.readUpdate(latest, signal);
        if (signal?.aborted) return;
        if (next.state && (active(next.state) || next.state.started_at >= openedAt.current)) {
          const current = identity(next.state);
          try {
            sessionStorage.setItem(seenKey, current);
          } catch {
            // The current page can still track the update without storage.
          }
          setSeen(current);
          if (next.state.phase === "installing") {
            const key = `${seenKey}-installing`;
            const fallback =
              installing.current?.id === current ? installing.current.started : Date.now();
            let started: number;
            try {
              const saved = sessionStorage.getItem(key)?.split("|");
              started = saved?.[0] === current ? Number(saved[1]) : fallback;
            } catch {
              started = fallback;
            }
            installing.current = { id: current, started };
            try {
              sessionStorage.setItem(key, `${current}|${started}`);
            } catch {
              // Keep the timer in memory when storage is unavailable.
            }
            setInstallingAt(started);
          }
        } else {
          try {
            const saved = sessionStorage.getItem(seenKey);
            if (saved !== null) setSeen(saved);
          } catch {
            // Keep the current page's seen update.
          }
        }
        if (running.current && running.current !== next.running) {
          window.location.reload();
          return;
        }
        running.current = next.running;
        setNow(Date.now());
        setView((previous) =>
          latest
            ? next
            : {
                ...next,
                latest: previous?.latest ?? null,
                latest_error: previous?.latest_error ?? null,
                available: previous?.available ?? false,
              },
        );
        setError(undefined);
      } catch (cause) {
        if (!signal?.aborted && latest) setError(cause);
      }
    },
    [client],
  );

  useEffect(() => {
    const controller = new AbortController();
    openedAt.current = Date.now();
    void (async () => {
      await read(true, controller.signal);
    })();
    return () => controller.abort();
  }, [read]);

  const polling = active(view?.state ?? null);
  useEffect(() => {
    if (!polling) return;
    const controller = new AbortController();
    const timer = window.setInterval(() => {
      setNow(Date.now());
      void read(false, controller.signal);
    }, 2000);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [polling, read]);

  const state = view?.state;
  const showState = state && (active(state) || seen === identity(state));
  let progress: string | undefined;
  if (showState && state) {
    switch (state.phase) {
      case "waiting":
        progress = state.running_jobs
          ? t(state.running_jobs === 1 ? "updates.waitOne" : "updates.waitMany", {
              count: state.running_jobs,
            })
          : t("updates.waiting");
        break;
      case "installing":
        progress =
          installingAt !== null && now - installingAt > 300_000
            ? t("updates.still", { path: view?.log_path ?? "update.log" })
            : t("updates.installing", { version: state.to });
        break;
      case "succeeded":
        progress = t("updates.succeeded", { version: state.to });
        break;
      case "failed":
        progress = state.message ?? t("updates.failed");
        break;
    }
  }
  return (
    <Section
      id="settings-updates"
      title={t("updates.title")}
      description={t("updates.description")}
    >
      {view && (
        <p className="text-sm">
          {view.latest
            ? view.available
              ? t("updates.available", { running: view.running, latest: view.latest })
              : t("updates.current", { running: view.running })
            : t("updates.running", { running: view.running })}
        </p>
      )}
      {view?.release_page && (
        <a className="text-sm underline" href={view.release_page} target="_blank" rel="noreferrer">
          {t("updates.release")}
        </a>
      )}
      {progress && (
        <p className="text-sm" role="status">
          {progress}
        </p>
      )}
      {(error !== undefined || view?.latest_error) && (
        <p className="text-sm text-destructive">
          {error !== undefined ? describeError(error, t) : view?.latest_error}
        </p>
      )}
      {view && !view.can_update && <p className="text-sm">{t("updates.terminal")}</p>}
      <div className="flex gap-2">
        <Button
          variant="outline"
          disabled={busy || !!(view && active(view.state))}
          onClick={() => void read(true)}
        >
          {t("updates.check")}
        </Button>
        {view?.can_update && view.available && view.latest && (
          <Button
            disabled={busy || active(view.state)}
            onClick={() => {
              setBusy(true);
              void client
                .startUpdate(view.latest!)
                .then(
                  (state) => {
                    const current = identity(state);
                    setSeen(current);
                    try {
                      sessionStorage.setItem(seenKey, current);
                    } catch {
                      // The current page can still track the update without storage.
                    }
                    setView((previous) => previous && { ...previous, state });
                  },
                  (cause) => setError(cause),
                )
                .finally(() => setBusy(false));
            }}
          >
            {t("updates.update")}
          </Button>
        )}
      </div>
    </Section>
  );
}
