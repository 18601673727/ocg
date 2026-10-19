"use client";

import { useCallback, useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Pill } from "@/components/ocg/primitives";
import type { ControlPool } from "@/components/ocg/contracts";
import type {
  ControlPoolUpdate,
  HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { Section, describeError, refusalText } from "./settings-parts";
import { useI18n, type I18nKey } from "../i18n";

const POOL_REFUSALS: Partial<Record<string, I18nKey>> = {
  pool_not_found: "settings.pools.refusal.notFound",
  invalid_request: "settings.pools.refusal.invalidRequest",
};

function PoolRow({
  pool,
  onUpdate,
  disabled,
}: {
  pool: ControlPool;
  onUpdate: (poolId: string, update: ControlPoolUpdate) => Promise<void>;
  disabled: boolean;
}) {
  const { t } = useI18n();
  const [limit, setLimit] = useState(String(pool.concurrency_limit));

  const parsed = Number(limit.trim());
  const isIntegerGte1 =
    Number.isSafeInteger(parsed) && parsed >= 1 && /^\d+$/.test(limit.trim());
  const isDifferent = isIntegerGte1 && parsed !== pool.concurrency_limit;
  const saveDisabled = disabled || !isIntegerGte1 || !isDifferent;

  return (
    <div className="flex flex-col gap-2 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="min-w-0 space-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-semibold">{pool.name}</span>
          <Pill tone="slate" className="text-xs normal-case">
            {pool.id}
          </Pill>
          <Pill
            tone={pool.enabled ? "emerald" : "amber"}
            className="text-xs normal-case"
          >
            {t(pool.enabled ? "settings.pools.enabled" : "settings.pools.disabled")}
          </Pill>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <div className="flex items-center gap-1.5">
          <label
            htmlFor={`pool-limit-${pool.id}`}
            className="text-xs text-muted-foreground whitespace-nowrap"
          >
            {t("settings.pools.concurrencyLimit")}
          </label>
          <Input
            id={`pool-limit-${pool.id}`}
            type="number"
            min={1}
            step={1}
            value={limit}
            onChange={(e) => setLimit(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !saveDisabled) {
                e.preventDefault();
                void onUpdate(pool.id, { concurrency_limit: parsed });
              }
            }}
            disabled={disabled}
            className="h-8 w-20 text-xs"
          />
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={saveDisabled}
            onClick={() => void onUpdate(pool.id, { concurrency_limit: parsed })}
          >
            {t("settings.pools.saveLimit")}
          </Button>
        </div>
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={disabled}
          onClick={() => void onUpdate(pool.id, { enabled: !pool.enabled })}
        >
          {t(pool.enabled ? "settings.pools.disable" : "settings.pools.enable")}
        </Button>
      </div>
    </div>
  );
}

export function PoolsSection({ client }: { client: HttpCanonicalControlClient }) {
  const { t } = useI18n();
  const [pools, setPools] = useState<ControlPool[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [updatingPoolId, setUpdatingPoolId] = useState<string | null>(null);

  const refresh = useCallback(
    (signal?: AbortSignal) => client.readPools(signal)
      .then((loaded) => {
        if (!signal?.aborted) {
          setPools(loaded.pools);
          setLoadError(null);
        }
      })
      .catch((err: unknown) => {
        if (!signal?.aborted) setLoadError(describeError(err));
      })
      .finally(() => {
        if (!signal?.aborted) setLoading(false);
      }),
    [client]
  );

  useEffect(() => {
    const controller = new AbortController();
    void refresh(controller.signal);
    return () => controller.abort();
  }, [refresh]);

  async function handleUpdate(poolId: string, update: ControlPoolUpdate) {
    setActionError(null);
    setUpdatingPoolId(poolId);
    try {
      const updated = await client.updatePool(poolId, update);
      setPools((prev) => prev.map((p) => (p.id === updated.id ? updated : p)));
    } catch (err) {
      setActionError(refusalText(t, err, POOL_REFUSALS));
    } finally {
      setUpdatingPoolId(null);
    }
  }

  return (
    <Section
      id="settings-pools"
      title={t("settings.pools.title")}
      description={t("settings.pools.description")}
      action={
        <Button
          variant="ghost"
          size="sm"
          className="h-9 rounded-md tracking-normal normal-case"
          disabled={loading}
          onClick={() => { setLoading(true); setLoadError(null); void refresh(); }}
        >
          {loading ? t("settings.pools.refreshing") : t("settings.pools.refresh")}
        </Button>
      }
    >
      {loadError ? (
        <p role="alert" className="py-2 text-sm text-destructive">
          {loadError}
        </p>
      ) : null}
      {actionError ? (
        <p role="alert" className="py-2 text-sm text-destructive">
          {actionError}
        </p>
      ) : null}

      <div className="divide-y divide-border/70 py-2">
        {pools.length === 0 ? (
          <p className="py-2 text-sm text-muted-foreground">
            {t("settings.pools.empty")}
          </p>
        ) : (
          pools.map((pool) => (
            <PoolRow
              key={`${pool.id}-${pool.concurrency_limit}`}
              pool={pool}
              onUpdate={handleUpdate}
              disabled={updatingPoolId === pool.id}
            />
          ))
        )}
      </div>

      <p className="border-t border-border/70 py-2 text-xs text-muted-foreground">
        {t("settings.pools.note")}
      </p>
    </Section>
  );
}
