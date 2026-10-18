"use client";

import type { ReactNode } from "react";
import { RefreshCw } from "lucide-react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { Button } from "@/components/ui/button";
import { PageSurface, Pill } from "../primitives";
import { runtimeStateLabel, useI18n, type I18nKey } from "../i18n";
import type { Tone } from "../primitives";
import type { AvailabilityEntry, AvailabilityMatrix, AvailabilityStatus } from "./availability-types";

const STATUS_TONE: Record<AvailabilityStatus, Tone> = {
  available: "emerald",
  unavailable: "red",
  unknown: "slate",
  "in-progress": "amber",
};

const STATUS_KEY: Record<AvailabilityStatus, I18nKey> = {
  available: "health.available",
  unavailable: "health.unavailable",
  unknown: "health.unknown",
  "in-progress": "health.inProgress",
};

function checkedAt(epochSeconds: number): string {
  const date = new Date(epochSeconds * 1000);
  return Number.isNaN(date.getTime()) ? String(epochSeconds) : date.toISOString();
}

function HealthRow({ entry }: { entry: AvailabilityEntry }) {
  const { t } = useI18n();
  return (
    <li className="min-w-0 border-t border-border/70 py-3 first:border-t-0 first:pt-0">
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className={cn(TYPOGRAPHY_SCALE.body_sm, "break-words font-medium")}>{entry.model}</p>
          <p className="mt-0.5 break-words text-xs text-muted-foreground">
            {entry.effort ?? t("health.noEffort")}
          </p>
        </div>
        <div className="flex flex-wrap gap-2"><Pill tone={STATUS_TONE[entry.status]} dot>{t(STATUS_KEY[entry.status])}</Pill>{entry.stale && <Pill tone="amber">{t("health.stale")}</Pill>}</div>
      </div>
      <dl className="mt-2 space-y-1 text-xs text-muted-foreground">
        <div className="flex min-w-0 flex-wrap gap-x-2">
          <dt>{t("health.checked")}</dt>
          <dd>{entry.updatedAt != null ? <time dateTime={checkedAt(entry.updatedAt)}>{checkedAt(entry.updatedAt)}</time> : t("common.notReported")}</dd>
        </div>
        {entry.jobState != null && <div className="flex min-w-0 flex-wrap gap-x-2">
          <dt>{t("health.jobState")}</dt>
          <dd>{runtimeStateLabel(t, entry.jobState)}</dd>
        </div>}
        {entry.jobId != null && <div className="min-w-0">
          <dt className="inline">{t("health.probeJob")}</dt>
          {" "}
          <dd className="inline break-all font-mono text-xs">{entry.jobId}</dd>
        </div>}
        {entry.failure && (
          <div className="min-w-0">
            <dt>{t("health.failure")}</dt>
            <dd className="mt-0.5 break-words">
              <span className="font-mono text-xs">{entry.failure.code}</span>
              {entry.failure.message ? <span className="mt-0.5 block whitespace-pre-wrap">{entry.failure.message}</span> : null}
            </dd>
          </div>
        )}
      </dl>
    </li>
  );
}

export type AvailabilityMatrixViewProps = {
  matrix: AvailabilityMatrix | null;
  loading: boolean;
  error: string | null;
  unavailable: boolean;
  onRefresh: () => void;
  description?: ReactNode;
  note?: ReactNode;
};

export function AvailabilityMatrixView({ matrix, loading, error, unavailable, onRefresh, description, note }: AvailabilityMatrixViewProps) {
  const { t } = useI18n();
  const summary = matrix?.summary;
  return (
    <PageSurface>
      <div className="mx-auto min-w-0 max-w-6xl space-y-5">
        <header className="flex min-w-0 flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <h1 className="text-lg font-semibold tracking-tight">{t("health.title")}</h1>
            <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">{description ?? t("health.observationSubtitle")}</p>
          </div>
          <Button variant="outline" size="sm" disabled={unavailable || loading} onClick={onRefresh}>
            <RefreshCw className="size-3.5" />{t("common.refresh")}
          </Button>
        </header>

        {unavailable ? (
          <p className="text-sm text-muted-foreground">{t("health.noEndpoint")}</p>
        ) : loading && !matrix ? (
          <p role="status" className="text-sm text-muted-foreground">{t("common.loading")}</p>
        ) : error ? (
          <div role="alert" className="flex flex-wrap items-center gap-2 text-sm text-destructive">
            <span>{t("health.loadFailed")}</span>
            <Button size="xs" variant="outline" onClick={onRefresh}>{t("common.retry")}</Button>
          </div>
        ) : null}

        {summary && summary.total > 0 && (
          <p className="text-xs text-muted-foreground">
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
            <h2 className={cn(TYPOGRAPHY_SCALE.body_sm, "break-words font-semibold")}>{group.provider}</h2>
            <ul className="mt-3">
              {group.entries.map((entry) => (
                <HealthRow key={`${entry.provider}:${entry.model}:${entry.effort ?? ""}:${entry.jobId}`} entry={entry} />
              ))}
            </ul>
          </section>
        ))}

        <p className="text-xs leading-5 text-muted-foreground">{note ?? t("health.observationNote")}</p>
      </div>
    </PageSurface>
  );
}
