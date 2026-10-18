"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { createHttpCanonicalControlClient, isCanonicalRejection } from "../runtime/unavailable-client";
import { healthMatrixOf } from "./matrix";
import { useI18n } from "../i18n";
import { AvailabilityMatrixView } from "./availability-matrix-view";

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

  return <AvailabilityMatrixView
    matrix={matrix}
    loading={loading}
    error={error}
    unavailable={!baseUrl || !projectId}
    onRefresh={refresh}
    description={t("health.subtitle")}
    note={<>{t("health.note")} {t("health.observationSubtitle")}</>}
  />;
}
