"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Pill } from "@/components/ocg/primitives";
import type { ControlPool } from "@/components/ocg/contracts";
import {
  configurationIdentity,
  type NativeExecutor,
} from "@/components/ocg/runtime/control-client";
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
  const isIntegerGte1 = Number.isSafeInteger(parsed) && parsed >= 1 && /^\d+$/.test(limit.trim());
  const isDifferent = isIntegerGte1 && parsed !== pool.concurrency_limit;
  const saveDisabled = disabled || !isIntegerGte1 || !isDifferent;

  return (
    <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3 py-3">
      <div className="min-w-0 flex-1 space-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <Tooltip>
            <TooltipTrigger
              render={
                <button type="button" className="text-left text-sm font-semibold break-all" />
              }
            >
              {pool.name}
            </TooltipTrigger>
            <TooltipContent className="max-w-none font-mono">{pool.id}</TooltipContent>
          </Tooltip>
          <Pill tone={pool.enabled ? "emerald" : "amber"} className="text-xs normal-case">
            {t(pool.enabled ? "settings.pools.enabled" : "settings.pools.disabled")}
          </Pill>
          {pool.operations === undefined ? (
            <span className="text-xs text-muted-foreground">
              {t("settings.pools.operationsUnknown")}
            </span>
          ) : pool.operations.length === 0 ? (
            <span className="text-xs text-muted-foreground">
              {t("settings.pools.operationsNone")}
            </span>
          ) : (
            pool.operations.map((operation) => (
              <Pill key={operation} tone="violet" className="text-[11px] normal-case">
                {t(
                  operation === "decision"
                    ? "settings.pools.operationDecision"
                    : "settings.pools.operationLlmRequest",
                )}
              </Pill>
            ))
          )}
        </div>
      </div>
      <div className="flex flex-wrap items-center justify-end gap-2">
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
  const [loadError, setLoadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [updatingPoolId, setUpdatingPoolId] = useState<string | null>(null);
  const [before, setBefore] = useState<string | undefined>();
  const [previous, setPrevious] = useState<(string | undefined)[]>([]);
  const [nextBefore, setNextBefore] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [native, setNative] = useState(false);
  const [poolName, setPoolName] = useState("");
  const [poolCapacity, setPoolCapacity] = useState("1");
  const poolIdentity = useRef<string | null>(null);
  const executorIdentity = useRef<string | null>(null);
  const [executorName, setExecutorName] = useState("");
  const [executorPool, setExecutorPool] = useState("");
  const [capability, setCapability] = useState("openai_chat_completions");
  const [executors, setExecutors] = useState<NativeExecutor[]>([]);
  const [executorBefore, setExecutorBefore] = useState<string | undefined>();
  const [executorPrevious, setExecutorPrevious] = useState<(string | undefined)[]>([]);
  const [executorNext, setExecutorNext] = useState<string | null>(null);
  const [executorLoading, setExecutorLoading] = useState(false);
  const [executorError, setExecutorError] = useState<string | null>(null);
  const [failedExecutorPage, setFailedExecutorPage] = useState<{
    cursor: string | undefined;
    history: (string | undefined)[];
  } | null>(null);
  const [creating, setCreating] = useState(false);
  const executorRequest = useRef<AbortController | null>(null);
  const [failedPage, setFailedPage] = useState<{
    cursor: string | undefined;
    history: (string | undefined)[];
  } | null>(null);
  const request = useRef<AbortController | null>(null);

  const loadPools = useCallback(
    async (cursor: string | undefined, history: (string | undefined)[], signal: AbortSignal) => {
      try {
        const page = await client.readPools({ before: cursor, signal });
        if (!signal.aborted) {
          setNative(
            (page as typeof page & { resource_model?: string }).resource_model === "independent",
          );
          setPools(page.pools);
          setBefore(cursor);
          setPrevious(history);
          setNextBefore(page.next_before);
          setLoadError(null);
          setFailedPage(null);
        }
      } catch (err: unknown) {
        if (!signal.aborted) {
          setLoadError(describeError(err, t));
          setFailedPage({ cursor, history });
        }
      } finally {
        if (!signal.aborted) setLoading(false);
      }
    },
    [client, t],
  );

  useEffect(() => {
    const controller = new AbortController();
    request.current = controller;
    void Promise.resolve().then(() => {
      if (!controller.signal.aborted) {
        setLoading(true);
        return loadPools(undefined, [], controller.signal);
      }
    });
    return () => request.current?.abort();
  }, [loadPools]);

  const loadExecutors = useCallback(
    async (cursor: string | undefined, history: (string | undefined)[], signal: AbortSignal) => {
      try {
        const page = await client.readNativeExecutors({ before: cursor, signal });
        if (!signal.aborted) {
          setExecutors(page.executors);
          setExecutorBefore(cursor);
          setExecutorPrevious(history);
          setExecutorNext(page.next_before);
          setExecutorError(null);
          setFailedExecutorPage(null);
        }
      } catch (error) {
        if (!signal.aborted) {
          setExecutorError(describeError(error, t));
          setFailedExecutorPage({ cursor, history });
        }
      } finally {
        if (!signal.aborted) setExecutorLoading(false);
      }
    },
    [client, t],
  );

  useEffect(() => {
    if (!native) return;
    const controller = new AbortController();
    executorRequest.current = controller;
    void Promise.resolve().then(() => {
      if (!controller.signal.aborted) {
        setExecutorLoading(true);
        return loadExecutors(undefined, [], controller.signal);
      }
    });
    return () => executorRequest.current?.abort();
  }, [native, loadExecutors]);

  function navigateExecutors(cursor: string | undefined, history: (string | undefined)[]) {
    executorRequest.current?.abort();
    const controller = new AbortController();
    executorRequest.current = controller;
    setExecutorLoading(true);
    void loadExecutors(cursor, history, controller.signal);
  }

  async function createPool() {
    setActionError(null);
    setCreating(true);
    try {
      const id = poolIdentity.current ?? configurationIdentity();
      poolIdentity.current = id;
      await client.createNativePool(id, poolName.trim(), Number(poolCapacity));
      poolIdentity.current = null;
      setPoolName("");
      setPoolCapacity("1");
      navigate(undefined, []);
    } catch (error) {
      setActionError(describeError(error, t));
    } finally {
      setCreating(false);
    }
  }

  async function createExecutor() {
    setActionError(null);
    setCreating(true);
    try {
      const id = executorIdentity.current ?? configurationIdentity();
      executorIdentity.current = id;
      await client.createNativeExecutor(
        id,
        executorPool,
        executorName.trim(),
        capability === "decision"
          ? { operation: "decision", protocol: null }
          : { operation: "llm_request", protocol: capability },
      );
      executorIdentity.current = null;
      setExecutorName("");
      navigateExecutors(undefined, []);
    } catch (error) {
      setActionError(describeError(error, t));
    } finally {
      setCreating(false);
    }
  }

  function navigate(cursor: string | undefined, history: (string | undefined)[]) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setLoading(true);
    void loadPools(cursor, history, controller.signal);
  }

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
    >
      {loadError ? (
        <DismissibleNotice message={loadError} className="py-2 text-sm text-destructive">
          {loadError}
        </DismissibleNotice>
      ) : null}
      {loadError ? (
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={loading}
          onClick={() => {
            if (failedPage) navigate(failedPage.cursor, failedPage.history);
          }}
        >
          {t("common.retry")}
        </Button>
      ) : null}
      {actionError ? (
        <DismissibleNotice message={actionError} className="py-2 text-sm text-destructive">
          {actionError}
        </DismissibleNotice>
      ) : null}

      {native ? (
        <div className="flex flex-wrap items-end gap-2">
          <label className="text-xs">
            {t("settings.pools.nativeName")}
            <Input value={poolName} onChange={(event) => setPoolName(event.target.value)} />
          </label>
          <label className="text-xs">
            {t("settings.pools.concurrencyLimit")}
            <Input
              type="number"
              min={1}
              max={4294967295}
              value={poolCapacity}
              onChange={(event) => setPoolCapacity(event.target.value)}
            />
          </label>
          <Button
            type="button"
            size="xs"
            disabled={creating || !poolName.trim()}
            onClick={() => void createPool()}
          >
            {t("settings.pools.nativeCreate")}
          </Button>
        </div>
      ) : null}

      <TooltipProvider delay={100}>
        <div className="divide-y divide-border/70 py-2">
          {pools.length === 0 && nextBefore === null && !loading && !loadError ? (
            <p className="py-2 text-sm text-muted-foreground">{t("settings.pools.empty")}</p>
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
      </TooltipProvider>

      <div className="flex gap-2 py-2">
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={loading || previous.length === 0}
          onClick={() => navigate(previous[previous.length - 1], previous.slice(0, -1))}
        >
          {t("settings.pools.previousPage")}
        </Button>
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={loading || nextBefore === null}
          onClick={() => {
            if (nextBefore !== null) navigate(nextBefore, [...previous, before]);
          }}
        >
          {loading ? t("common.loading") : t("settings.pools.nextPage")}
        </Button>
      </div>

      {native ? (
        <div className="space-y-3 border-t border-border/70 pt-3">
          <h3 className="font-semibold">{t("settings.pools.executors")}</h3>
          {executorError ? (
            <DismissibleNotice message={executorError} className="text-sm text-destructive">
              {executorError}
            </DismissibleNotice>
          ) : null}
          {executorError && failedExecutorPage ? (
            <Button
              type="button"
              size="xs"
              variant="outline"
              onClick={() =>
                navigateExecutors(failedExecutorPage.cursor, failedExecutorPage.history)
              }
            >
              {t("common.retry")}
            </Button>
          ) : null}
          <div className="divide-y divide-border/70">
            {executors.map((executor) => (
              <div key={executor.id} className="py-2 text-sm">
                <strong>{executor.name}</strong>{" "}
                <span className="text-muted-foreground">
                  {executor.id} · {executor.pool} ·{" "}
                  {t(executor.enabled ? "settings.pools.enabled" : "settings.pools.disabled")} ·{" "}
                  {executor.capabilities.map((item) => item.protocol ?? item.operation).join(", ")}
                </span>
              </div>
            ))}
            {!executors.length && !executorLoading && !executorError ? (
              <p className="text-sm text-muted-foreground">{t("settings.pools.executorEmpty")}</p>
            ) : null}
          </div>
          <div className="flex gap-2">
            <Button
              type="button"
              size="xs"
              variant="outline"
              disabled={executorLoading || !executorPrevious.length}
              onClick={() =>
                navigateExecutors(
                  executorPrevious[executorPrevious.length - 1],
                  executorPrevious.slice(0, -1),
                )
              }
            >
              {t("settings.pools.previousPage")}
            </Button>
            <Button
              type="button"
              size="xs"
              variant="outline"
              disabled={executorLoading || executorNext === null}
              onClick={() => {
                if (executorNext !== null)
                  navigateExecutors(executorNext, [...executorPrevious, executorBefore]);
              }}
            >
              {t("settings.pools.nextPage")}
            </Button>
          </div>
          <div className="flex flex-wrap items-end gap-2">
            <label className="text-xs">
              {t("settings.pools.executorName")}
              <Input
                value={executorName}
                onChange={(event) => setExecutorName(event.target.value)}
              />
            </label>
            <label className="text-xs">
              {t("settings.pools.executorPool")}
              <select
                className="block h-9 rounded border bg-background px-2"
                value={executorPool}
                onChange={(event) => setExecutorPool(event.target.value)}
              >
                <option value="">—</option>
                {executorPool && !pools.some((pool) => pool.id === executorPool) ? (
                  <option value={executorPool}>{executorPool}</option>
                ) : null}
                {pools.map((pool) => (
                  <option key={pool.id} value={pool.id}>
                    {pool.name} · {pool.id}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-xs">
              {t("settings.pools.executorCapability")}
              <select
                className="block h-9 rounded border bg-background px-2"
                value={capability}
                onChange={(event) => setCapability(event.target.value)}
              >
                <option value="openai_chat_completions">OpenAI Chat</option>
                <option value="openai_responses">OpenAI Responses</option>
                <option value="anthropic_messages">Anthropic Messages</option>
                <option value="decision">{t("settings.pools.operationDecision")}</option>
              </select>
            </label>
            <Button
              type="button"
              size="xs"
              disabled={creating || !executorName.trim() || !executorPool}
              onClick={() => void createExecutor()}
            >
              {t("settings.pools.executorCreate")}
            </Button>
          </div>
        </div>
      ) : null}

      <p className="border-t border-border/70 py-2 text-xs text-muted-foreground">
        {t("settings.pools.note")}
      </p>
    </Section>
  );
}
