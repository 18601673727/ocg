"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { PageSurface, Pill } from "../primitives";
import { runtimeStateLabel, useI18n, type I18nKey } from "../i18n";
import { createHttpCanonicalControlClient, isCanonicalRejection } from "../runtime/canonical-client";
import type { Tone } from "../primitives";
import { healthMatrixOf, type HealthEntry, type HealthStatus } from "./matrix";

const STATUS_TONE: Record<HealthStatus, Tone> = {
  available: "emerald",
  unavailable: "red",
  unknown: "slate",
  "in-progress": "amber",
};

const STATUS_KEY: Record<HealthStatus, I18nKey> = {
  available: "health.available",
  unavailable: "health.unavailable",
  unknown: "health.unknown",
  "in-progress": "health.inProgress",
};

function checkedAt(epochSeconds: number): string {
  const date = new Date(epochSeconds * 1000);
  return Number.isNaN(date.getTime()) ? String(epochSeconds) : date.toISOString();
}

function HealthRow({ entry }: { entry: HealthEntry }) {
  const { t } = useI18n();
  return (
    <li className="min-w-0 border-t border-border/70 py-3 first:border-t-0 first:pt-0">
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="break-words text-[13px] font-medium">{entry.model}</p>
          <p className="mt-0.5 break-words text-[11px] text-muted-foreground">
            {entry.effort ?? t("health.noEffort")}
          </p>
        </div>
        <Pill tone={STATUS_TONE[entry.status]} dot>{t(STATUS_KEY[entry.status])}</Pill>
      </div>
      <dl className="mt-2 space-y-1 text-[11px] text-muted-foreground">
        <div className="flex min-w-0 flex-wrap gap-x-2">
          <dt>{t("health.checked")}</dt>
          <dd><time dateTime={checkedAt(entry.updatedAt)}>{checkedAt(entry.updatedAt)}</time></dd>
        </div>
        <div className="flex min-w-0 flex-wrap gap-x-2">
          <dt>{t("health.jobState")}</dt>
          <dd>{runtimeStateLabel(t, entry.jobState)}</dd>
        </div>
        <div className="min-w-0">
          <dt className="inline">{t("health.probeJob")}</dt>
          {" "}
          <dd className="inline break-all font-mono text-[10px]">{entry.jobId}</dd>
        </div>
        {entry.failure && (
          <div className="min-w-0">
            <dt>{t("health.failure")}</dt>
            <dd className="mt-0.5 break-words">
              <span className="font-mono text-[10px]">{entry.failure.code}</span>
              {entry.failure.message ? <span className="mt-0.5 block whitespace-pre-wrap">{entry.failure.message}</span> : null}
            </dd>
          </div>
        )}
      </dl>
    </li>
  );
}

export function HealthSurface({ baseUrl, projectId }: { baseUrl: string | null; projectId: string }) {
  const { t } = useI18n();
  const [revision, setRevision] = useState(0);
  const [result, setResult] = useState<{ key: string; revision: number; matrix: ReturnType<typeof healthMatrixOf> | null; error: string | null } | null>(null);
  const client = useMemo(
    () => (baseUrl ? createHttpCanonicalControlClient({ baseUrl, fetch: globalThis.fetch }) : null),
    [baseUrl],
  );
  const key = JSON.stringify([baseUrl, projectId]);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    if (!client || !projectId) return;
    let cancelled = false;
    void client.readDashboard(projectId).then((value) => {
      if (cancelled) return;
      if (isCanonicalRejection(value)) setResult({ key, revision, matrix: null, error: value.message });
      else setResult({ key, revision, matrix: healthMatrixOf(value.jobs), error: null });
    }).catch((cause: unknown) => {
      if (!cancelled) setResult({ key, revision, matrix: null, error: cause instanceof Error ? cause.message : String(cause) });
    });
    return () => {
      cancelled = true;
    };
  }, [client, projectId, key, revision]);

  const current = result?.key === key ? result : null;
  const loading = Boolean(client && projectId && (!current || current.revision !== revision));
  const error = current?.error ?? null;
  const matrix = current?.matrix ?? null;

  const summary = matrix?.summary;
  return (
    <PageSurface>
      <div className="mx-auto min-w-0 max-w-3xl space-y-5">
        <header className="flex min-w-0 flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <h1 className="text-lg font-semibold tracking-tight">{t("health.title")}</h1>
            <p className="mt-1 max-w-2xl text-[12px] leading-5 text-muted-foreground">{t("health.subtitle")}</p>
          </div>
          <Button variant="outline" size="sm" disabled={!baseUrl || !projectId || loading} onClick={refresh}>
            <RefreshCw className="size-3.5" />{t("common.refresh")}
          </Button>
        </header>

        {!baseUrl || !projectId ? (
          <p className="text-sm text-muted-foreground">{t("health.noEndpoint")}</p>
        ) : loading && !matrix ? (
          <p role="status" className="text-sm text-muted-foreground">{t("common.loading")}</p>
        ) : error ? (
          <div role="alert" className="flex flex-wrap items-center gap-2 text-sm text-destructive">
            <span>{t("health.loadFailed")}</span>
            <Button size="xs" variant="outline" onClick={refresh}>{t("common.retry")}</Button>
          </div>
        ) : null}

        {summary && summary.total > 0 && (
          <p className="text-[12px] text-muted-foreground">
            {t("health.summary", {
              total: summary.total,
              available: summary.available,
              unavailable: summary.unavailable,
              unknown: summary.unknown,
              inProgress: summary.inProgress,
            })}
          </p>
        )}

        {matrix && matrix.groups.length === 0 && !loading && !error && (
          <p className="text-sm text-muted-foreground">{t("health.empty")}</p>
        )}

        {matrix && matrix.groups.map((group) => (
          <section key={group.provider} aria-label={group.provider} className="min-w-0 rounded-lg border border-border bg-card p-4">
            <h2 className="break-words text-[13px] font-semibold">{group.provider}</h2>
            <ul className="mt-3">
              {group.entries.map((entry) => (
                <HealthRow key={`${entry.provider}:${entry.model}:${entry.effort ?? ""}:${entry.jobId}`} entry={entry} />
              ))}
            </ul>
          </section>
        ))}

        <p className="text-[11px] leading-5 text-muted-foreground">{t("health.note")}</p>
      </div>
    </PageSurface>
  );
}
