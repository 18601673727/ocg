import { createEffect, createMemo, createSignal } from "solid-js";
import { Button } from "@/components/ui/button";
import { elementRef } from "@/lib/ui-state";
import type { Update, UpdateState } from "../contracts/api";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";
import { describeError, Section } from "./settings-parts";

const seenKey = "ocg-settings-update";
const active = (state: UpdateState | null) =>
  state?.phase === "waiting" || state?.phase === "installing";
const identity = (state: UpdateState) => `${state.started_at}:${state.to}`;

export function UpdatesSection(props: { client: ApiClient }) {
  const i18n = useI18n();
  const [view, setView] = createSignal<Update | null>(null);
  const [error, setError] = createSignal<unknown>();
  const [busy, setBusy] = createSignal(false);
  const [now, setNow] = createSignal(0);
  const [installingAt, setInstallingAt] = createSignal<number | null>(null);
  const running = elementRef<string | null>(null);
  const installing = elementRef<{ id: string; started: number } | null>(null);
  const openedAt = elementRef(0);
  const [seen, setSeen] = createSignal<string | null>(null);

  const read = async (latest: boolean, signal?: AbortSignal) => {
    try {
      const next = await props.client.readUpdate(latest, signal);
      if (signal?.aborted) return;
      if (next.state && (active(next.state) || next.state.started_at >= openedAt.current)) {
        const current = identity(next.state);
        try {
          sessionStorage.setItem(seenKey, current);
        } catch {}
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
          } catch {}
          setInstallingAt(started);
        }
      } else {
        try {
          const saved = sessionStorage.getItem(seenKey);
          if (saved !== null) setSeen(saved);
        } catch {}
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
              release_page: previous?.release_page ?? null,
              latest_error: previous?.latest_error ?? null,
              available: previous?.available ?? false,
            },
      );
      setError(undefined);
    } catch (cause) {
      if (!signal?.aborted && latest) setError(cause);
    }
  };

  createEffect(
    () => [read],
    () => {
      const controller = new AbortController();
      openedAt.current = Date.now();
      void (async () => {
        await read(true, controller.signal);
      })();
      return () => controller.abort();
    },
  );

  const polling = createMemo(() => active(view()?.state ?? null));
  createEffect(
    () => [polling(), read],
    () => {
      if (!polling()) return;
      const controller = new AbortController();
      const timer = window.setInterval(() => {
        setNow(Date.now());
        void read(false, controller.signal);
      }, 2000);
      return () => {
        controller.abort();
        window.clearInterval(timer);
      };
    },
  );

  const state = createMemo(() => view()?.state);
  const showState = createMemo(
    () => state() && (active(state()!) || seen() === identity(state()!)),
  );
  let progress: string | undefined;
  if (showState() && state()) {
    switch (state()!.phase) {
      case "waiting":
        progress = state()!.running_jobs
          ? i18n.t(state()!.running_jobs === 1 ? "updates.waitOne" : "updates.waitMany", {
              count: state()!.running_jobs!,
            })
          : i18n.t("updates.waiting");
        break;
      case "installing":
        progress =
          installingAt() !== null && now() - installingAt()! > 300_000
            ? i18n.t("updates.still", { path: view()?.log_path ?? "update.log" })
            : i18n.t("updates.installing", { version: state()!.to });
        break;
      case "succeeded":
        progress = i18n.t("updates.succeeded", { version: state()!.to });
        break;
      case "failed":
        progress = state()!.message ?? i18n.t("updates.failed");
        break;
    }
  }
  return (
    <Section
      id="settings-updates"
      title={i18n.t("updates.title")}
      description={i18n.t("updates.description")}
    >
      {view() && (
        <p class="text-sm">
          {view()!.latest
            ? view()!.available
              ? i18n.t("updates.available", { running: view()!.running, latest: view()!.latest! })
              : i18n.t("updates.current", { running: view()!.running })
            : i18n.t("updates.running", { running: view()!.running })}
        </p>
      )}
      {view()?.release_page && (
        <a class="text-sm underline" href={view()!.release_page!} target="_blank" rel="noreferrer">
          {i18n.t("updates.release")}
        </a>
      )}
      {progress && (
        <p class="text-sm" role="status">
          {progress}
        </p>
      )}
      {(error() !== undefined || view()?.latest_error) && (
        <p class="text-sm text-destructive">
          {error() !== undefined ? describeError(error(), i18n.t) : view()?.latest_error}
        </p>
      )}
      {view() && !view()!.can_update && <p class="text-sm">{i18n.t("updates.terminal")}</p>}
      <div class="flex gap-2">
        <Button
          variant="outline"
          disabled={busy() || !!(view() && active(view()!.state))}
          onClick={() => void read(true)}
        >
          {i18n.t("updates.check")}
        </Button>
        {view()?.can_update && view()!.available && view()!.latest && (
          <Button
            disabled={busy() || active(view()!.state)}
            onClick={() => {
              setBusy(true);
              void props.client
                .startUpdate(view()!.latest!)
                .then(
                  (state) => {
                    const current = identity(state);
                    setSeen(current);
                    try {
                      sessionStorage.setItem(seenKey, current);
                    } catch {}
                    setView((previous) => previous && { ...previous, state });
                  },
                  (cause) => setError(cause),
                )
                .finally(() => setBusy(false));
            }}
          >
            {i18n.t("updates.update")}
          </Button>
        )}
      </div>
    </Section>
  );
}
