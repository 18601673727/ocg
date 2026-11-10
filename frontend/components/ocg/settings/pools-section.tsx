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
  ControlTransportError,
  isExecutorConflict,
  type NativeAllocation,
  type NativeAllocationPhase,
  type NativeExecutor,
  type NativeExecutorChange,
  type NativePoolAllocations,
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

const POOL_ARCHIVE_REFUSALS: Partial<Record<string, I18nKey>> = {
  pool_not_found: "settings.pools.refusal.notFound",
  invalid_request: "settings.pools.refusal.invalidRequest",
  pool_revision_conflict: "settings.pools.refusal.poolChanged",
  pool_archived: "settings.pools.refusal.poolArchived",
  pool_holds_allocation: "settings.pools.refusal.poolHoldsAllocation",
  pool_has_active_executor: "settings.pools.refusal.poolHasExecutor",
  pool_child_scan_unavailable: "settings.pools.refusal.poolScanUnavailable",
  allocation_index_inconsistent: "settings.pools.refusal.poolIndexInconsistent",
  pool_storage_inconsistent: "settings.pools.refusal.poolStorageInconsistent",
};

/** Pool archive refusals after which the page of Pools must be read again. */
const POOL_ARCHIVE_STALE_CODES: ReadonlySet<string> = new Set([
  "pool_revision_conflict",
  "pool_not_found",
  "pool_archived",
]);

const EXECUTOR_REFUSALS: Partial<Record<string, I18nKey>> = {
  executor_not_found: "settings.pools.refusal.executorNotFound",
  executor_revision_conflict: "settings.pools.refusal.executorChanged",
  invalid_executor: "settings.pools.refusal.invalidExecutor",
  invalid_request: "settings.pools.refusal.invalidExecutor",
  executor_archived: "settings.pools.refusal.executorArchived",
  executor_holds_allocation: "settings.pools.refusal.executorHoldsAllocation",
  allocation_index_inconsistent: "settings.pools.refusal.executorIndexInconsistent",
};

/** Archive refusals after which the Executor must be read again before anything else. */
const ARCHIVE_STALE_CODES: ReadonlySet<string> = new Set([
  "executor_revision_conflict",
  "executor_not_found",
  "executor_archived",
]);

const ALLOCATION_REFUSALS: Partial<Record<string, I18nKey>> = {
  pool_not_found: "settings.pools.refusal.notFound",
  invalid_cursor: "settings.pools.refusal.invalidCursor",
  allocation_index_inconsistent: "settings.pools.refusal.allocationInconsistent",
  database_busy: "settings.pools.refusal.allocationUnavailable",
  database_unavailable: "settings.pools.refusal.allocationUnavailable",
};

/** How each phase of a held Allocation looks. Reserved is not Running; Uncertain is unresolved. */
const ALLOCATION_PHASE: Record<
  NativeAllocationPhase,
  { tone: "sky" | "emerald" | "amber" | "red"; label: I18nKey; pulse: boolean }
> = {
  reserved: {
    tone: "sky",
    label: "settings.pools.phase.reserved",
    pulse: false,
  },
  running: {
    tone: "emerald",
    label: "settings.pools.phase.running",
    pulse: false,
  },
  stop_requested: {
    tone: "amber",
    label: "settings.pools.phase.stopRequested",
    pulse: false,
  },
  uncertain: {
    tone: "red",
    label: "settings.pools.phase.uncertain",
    pulse: true,
  },
};

/** The capabilities an Executor can declare, in the order they are sent. */
const EXECUTOR_CAPABILITIES: {
  key: string;
  label: string | null;
  capability: NativeExecutor["capabilities"][number];
}[] = [
  {
    key: "openai_chat_completions",
    label: "OpenAI Chat",
    capability: {
      operation: "llm_request",
      protocol: "openai_chat_completions",
    },
  },
  {
    key: "openai_responses",
    label: "OpenAI Responses",
    capability: { operation: "llm_request", protocol: "openai_responses" },
  },
  {
    key: "anthropic_messages",
    label: "Anthropic Messages",
    capability: { operation: "llm_request", protocol: "anthropic_messages" },
  },
  {
    key: "decision",
    label: null,
    capability: { operation: "decision", protocol: null },
  },
];

function capabilityKey(capability: NativeExecutor["capabilities"][number]): string {
  return capability.protocol ?? capability.operation;
}

function ExecutorRow({
  executor,
  pool,
  busy,
  stale,
  onUpdate,
  onArchive,
  onReload,
}: {
  executor: NativeExecutor;
  pool: ControlPool | undefined;
  /** An Executor update is pending; no other may be submitted. */
  busy: boolean;
  /** The Executor changed since it was read; a fresh read is needed first. */
  stale: boolean;
  onUpdate: (executor: NativeExecutor, change: NativeExecutorChange) => Promise<boolean>;
  /** Archives the Executor; the refusal text, or null when it was archived. */
  onArchive: (executor: NativeExecutor) => Promise<string | null>;
  onReload: () => void;
}) {
  const { t } = useI18n();
  const [confirmingArchive, setConfirmingArchive] = useState(false);
  const [archiving, setArchiving] = useState(false);
  const [archiveRefusal, setArchiveRefusal] = useState<string | null>(null);
  const [name, setName] = useState<string | null>(null);
  const [draft, setDraft] = useState<ReadonlySet<string> | null>(null);
  const disabled = busy || stale;
  const current = new Set(executor.capabilities.map(capabilityKey));
  const state = t(executor.enabled ? "settings.pools.enabled" : "settings.pools.disabled");
  const nameValid = name !== null && name.trim() !== "" && name.trim() !== executor.name;
  const draftValid =
    draft !== null && (draft.size !== current.size || [...draft].some((key) => !current.has(key)));

  return (
    <div className="space-y-2 py-2 text-sm">
      <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2">
        <div className="min-w-0 flex-1 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <strong className="break-all">{executor.name}</strong>
            <Pill tone={executor.enabled ? "emerald" : "amber"} className="text-xs normal-case">
              {t("settings.pools.executorState", { state })}
            </Pill>
            {pool ? (
              <Pill tone={pool.enabled ? "emerald" : "amber"} className="text-xs normal-case">
                {t("settings.pools.executorPoolState", {
                  pool: pool.name,
                  state: t(pool.enabled ? "settings.pools.enabled" : "settings.pools.disabled"),
                })}
              </Pill>
            ) : (
              <Pill tone="violet" className="text-xs normal-case">
                {t("settings.pools.executorPoolElsewhere", {
                  pool: executor.pool,
                })}
              </Pill>
            )}
          </div>
          <div className="text-xs break-all text-muted-foreground">
            {executor.id} · {executor.pool} ·{" "}
            {executor.capabilities.map((item) => item.protocol ?? item.operation).join(", ")}
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled}
            onClick={() => void onUpdate(executor, { enabled: !executor.enabled })}
          >
            {t(executor.enabled ? "settings.pools.disable" : "settings.pools.enable")}
          </Button>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled}
            onClick={() => setName(name === null ? executor.name : null)}
          >
            {t("settings.pools.executorRename")}
          </Button>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled}
            onClick={() => setDraft(draft === null ? current : null)}
          >
            {t("settings.pools.executorCapabilities")}
          </Button>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled || archiving}
            onClick={() => {
              setArchiveRefusal(null);
              setConfirmingArchive(!confirmingArchive);
            }}
          >
            {t("settings.pools.executorArchive")}
          </Button>
        </div>
      </div>
      {confirmingArchive || archiveRefusal ? (
        <div
          role="alertdialog"
          aria-label={t("settings.pools.executorArchiveTitle")}
          className="space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-xs"
        >
          {confirmingArchive ? (
            <>
              <p className="font-semibold">{t("settings.pools.executorArchiveTitle")}</p>
              <ul className="list-disc space-y-1 pl-4">
                <li>{t("settings.pools.executorArchiveActiveList")}</li>
                <li>{t("settings.pools.executorArchiveHistory")}</li>
                <li>{t("settings.pools.executorArchiveNotStop")}</li>
                <li>{t("settings.pools.executorArchiveNotRelease")}</li>
                <li>{t("settings.pools.executorArchiveIrreversible")}</li>
              </ul>
            </>
          ) : null}
          {archiveRefusal ? (
            <p role="alert" className="text-destructive">
              {archiveRefusal}
            </p>
          ) : null}
          {confirmingArchive ? (
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="destructive"
                size="xs"
                disabled={disabled || archiving}
                onClick={() => {
                  setArchiving(true);
                  setArchiveRefusal(null);
                  void onArchive(executor).then((refusal) => {
                    setArchiving(false);
                    if (refusal === null) return;
                    setArchiveRefusal(refusal);
                    setConfirmingArchive(false);
                  });
                }}
              >
                {t(
                  archiving
                    ? "settings.pools.executorArchiving"
                    : "settings.pools.executorArchiveConfirm",
                )}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="xs"
                disabled={archiving}
                onClick={() => setConfirmingArchive(false)}
              >
                {t("common.cancel")}
              </Button>
            </div>
          ) : (
            <Button
              type="button"
              variant="outline"
              size="xs"
              onClick={() => setArchiveRefusal(null)}
            >
              {t("common.close")}
            </Button>
          )}
        </div>
      ) : null}
      {stale ? (
        <div className="flex flex-wrap items-center gap-2 text-xs text-destructive">
          <span>{t("settings.pools.executorStale")}</span>
          <Button type="button" variant="outline" size="xs" onClick={onReload}>
            {t("settings.pools.executorReload")}
          </Button>
        </div>
      ) : null}
      {name !== null ? (
        <div className="flex flex-wrap items-center gap-2">
          <Input
            aria-label={t("settings.pools.executorName")}
            value={name}
            disabled={disabled}
            onChange={(event) => setName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && nameValid && !disabled) {
                event.preventDefault();
                void onUpdate(executor, { name: name.trim() });
              }
            }}
            className="h-8 w-64 text-xs"
          />
          <Button
            type="button"
            size="xs"
            disabled={disabled || !nameValid}
            onClick={() => void onUpdate(executor, { name: name.trim() })}
          >
            {t("settings.pools.executorSaveName")}
          </Button>
          <Button type="button" variant="outline" size="xs" onClick={() => setName(null)}>
            {t("common.cancel")}
          </Button>
        </div>
      ) : null}
      {draft !== null ? (
        <div className="flex flex-wrap items-center gap-3">
          {EXECUTOR_CAPABILITIES.map((option) => (
            <label key={option.key} className="flex items-center gap-1.5 text-xs">
              <input
                type="checkbox"
                disabled={disabled}
                checked={draft.has(option.key)}
                onChange={(event) => {
                  const next = new Set(draft);
                  if (event.target.checked) next.add(option.key);
                  else next.delete(option.key);
                  setDraft(next);
                }}
              />
              {option.label ?? t("settings.pools.operationDecision")}
            </label>
          ))}
          <Button
            type="button"
            size="xs"
            disabled={disabled || !draftValid}
            onClick={() =>
              void onUpdate(executor, {
                capabilities: EXECUTOR_CAPABILITIES.filter((option) => draft.has(option.key)).map(
                  (option) => option.capability,
                ),
              })
            }
          >
            {t("settings.pools.executorSaveCapabilities")}
          </Button>
          <Button type="button" variant="outline" size="xs" onClick={() => setDraft(null)}>
            {t("common.cancel")}
          </Button>
        </div>
      ) : null}
    </div>
  );
}

function PoolRow({
  pool,
  onUpdate,
  onArchive,
  disabled,
}: {
  pool: ControlPool;
  onUpdate: (poolId: string, update: ControlPoolUpdate) => Promise<void>;
  /** Archives the Pool; the refusal text, or null when it was archived. Absent when archival is not offered. */
  onArchive: ((pool: ControlPool) => Promise<string | null>) | undefined;
  disabled: boolean;
}) {
  const { t } = useI18n();
  const [limit, setLimit] = useState(String(pool.concurrency_limit));
  const [confirmingArchive, setConfirmingArchive] = useState(false);
  const [archiving, setArchiving] = useState(false);
  const [archiveRefusal, setArchiveRefusal] = useState<string | null>(null);

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
        {onArchive ? (
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled || archiving}
            onClick={() => {
              setArchiveRefusal(null);
              setConfirmingArchive(!confirmingArchive);
            }}
          >
            {t("settings.pools.poolArchive")}
          </Button>
        ) : null}
      </div>
      {onArchive && (confirmingArchive || archiveRefusal) ? (
        <div
          role="alertdialog"
          aria-label={t("settings.pools.poolArchiveTitle")}
          className="w-full space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-xs"
        >
          {confirmingArchive ? (
            <>
              <p className="font-semibold">{t("settings.pools.poolArchiveTitle")}</p>
              <ul className="list-disc space-y-1 pl-4">
                <li>{t("settings.pools.poolArchiveChildren")}</li>
                <li>{t("settings.pools.poolArchiveActiveList")}</li>
                <li>{t("settings.pools.poolArchiveHistory")}</li>
                <li>{t("settings.pools.poolArchiveNotStop")}</li>
                <li>{t("settings.pools.poolArchiveNotRelease")}</li>
                <li>{t("settings.pools.poolArchiveIrreversible")}</li>
              </ul>
            </>
          ) : null}
          {archiveRefusal ? (
            <p role="alert" className="text-destructive">
              {archiveRefusal}
            </p>
          ) : null}
          {confirmingArchive ? (
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="destructive"
                size="xs"
                disabled={disabled || archiving}
                onClick={() => {
                  setArchiving(true);
                  setArchiveRefusal(null);
                  void onArchive(pool).then((refusal) => {
                    setArchiving(false);
                    if (refusal === null) return;
                    setArchiveRefusal(refusal);
                    setConfirmingArchive(false);
                  });
                }}
              >
                {t(
                  archiving ? "settings.pools.poolArchiving" : "settings.pools.poolArchiveConfirm",
                )}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="xs"
                disabled={archiving}
                onClick={() => setConfirmingArchive(false)}
              >
                {t("common.cancel")}
              </Button>
            </div>
          ) : (
            <Button
              type="button"
              variant="outline"
              size="xs"
              onClick={() => setArchiveRefusal(null)}
            >
              {t("common.close")}
            </Button>
          )}
        </div>
      ) : null}
    </div>
  );
}

/**
 * The Allocations holding capacity in one native Pool. Nothing is requested
 * until the operator expands the Pool, and one Pool's failure never touches
 * another's state.
 */
function PoolAllocations({
  client,
  pool,
}: {
  client: HttpCanonicalControlClient;
  pool: ControlPool;
}) {
  const { t } = useI18n();
  const [expanded, setExpanded] = useState(false);
  const [snapshot, setSnapshot] = useState<NativePoolAllocations | null>(null);
  // Each page is its own read of the database. `read` numbers the reads so a
  // row can be told apart from the read that produced `snapshot`.
  const [items, setItems] = useState<{ allocation: NativeAllocation; read: number }[]>([]);
  const [latestRead, setLatestRead] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const request = useRef<AbortController | null>(null);
  const attempted = useRef<string | undefined>(undefined);
  const reads = useRef(0);

  useEffect(() => () => request.current?.abort(), []);

  /** Loads the first page, or the page after `after` and appends it. */
  function load(after: string | undefined) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    attempted.current = after;
    setLoading(true);
    setError(null);
    client
      .readNativePoolAllocations(pool.id, { after, signal: controller.signal })
      .then((page) => {
        if (controller.signal.aborted) return;
        const read = ++reads.current;
        setSnapshot(page);
        setLatestRead(read);
        setItems((previous) => {
          const fresh = page.allocations.map((allocation) => ({ allocation, read }));
          if (after === undefined) return fresh;
          const seen = new Set(
            previous.map(({ allocation }) => `${allocation.executor}.${allocation.job}`),
          );
          return [
            ...previous,
            ...fresh.filter(
              ({ allocation }) => !seen.has(`${allocation.executor}.${allocation.job}`),
            ),
          ];
        });
      })
      .catch((reason: unknown) => {
        // A failed read keeps what was loaded; it is never shown as zero occupancy.
        if (!controller.signal.aborted) setError(refusalText(t, reason, ALLOCATION_REFUSALS));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
  }

  function toggle() {
    if (expanded) {
      request.current?.abort();
      setLoading(false);
      setExpanded(false);
      return;
    }
    setExpanded(true);
    load(undefined);
  }

  const occupancy = snapshot?.occupancy;
  const capacity = snapshot?.capacity ?? pool.concurrency_limit;
  const indicator = !snapshot
    ? t("settings.pools.capacityOnly", { capacity })
    : occupancy?.available
      ? t("settings.pools.capacityHeld", { held: occupancy.held, capacity })
      : t("settings.pools.capacityHeldUnavailable", { capacity });
  const overCapacity = occupancy?.available === true && occupancy.held > capacity;

  return (
    <div className="space-y-2 pb-3 text-xs">
      <div className="flex flex-wrap items-center gap-2">
        <Pill
          tone={overCapacity ? "amber" : "slate"}
          className="text-xs normal-case"
          title={t("settings.pools.capacityHint")}
        >
          {indicator}
        </Pill>
        {occupancy?.available ? (
          <span className="text-muted-foreground">
            {t("settings.pools.occupancyBreakdown", {
              reserved: occupancy.reserved,
              running: occupancy.running,
              stopRequested: occupancy.stop_requested,
              uncertain: occupancy.uncertain,
            })}
          </span>
        ) : null}
        <Button type="button" variant="outline" size="xs" onClick={toggle}>
          {t(expanded ? "settings.pools.allocationsHide" : "settings.pools.allocationsShow")}
        </Button>
        {expanded ? (
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={loading}
            onClick={() => load(undefined)}
          >
            {t("settings.pools.allocationsRefresh")}
          </Button>
        ) : null}
      </div>
      {expanded ? (
        <div className="space-y-2">
          {error ? (
            <div className="flex flex-wrap items-center gap-2 text-destructive" role="alert">
              <span>{error}</span>
              <Button
                type="button"
                variant="outline"
                size="xs"
                disabled={loading}
                onClick={() => load(attempted.current)}
              >
                {t("common.retry")}
              </Button>
            </div>
          ) : null}
          {snapshot && occupancy && !occupancy.available ? (
            <p className="text-muted-foreground">{t("settings.pools.occupancyUnavailable")}</p>
          ) : null}
          {overCapacity ? (
            <p className="text-muted-foreground">{t("settings.pools.overCapacity")}</p>
          ) : null}
          {snapshot && snapshot.capacity !== pool.concurrency_limit ? (
            <p className="text-muted-foreground">
              {t("settings.pools.capacityChanged", {
                capacity: pool.concurrency_limit,
              })}
            </p>
          ) : null}
          {loading && !snapshot ? (
            <p className="text-muted-foreground">{t("common.loading")}</p>
          ) : null}
          {snapshot && !items.length && !error ? (
            <p className="text-muted-foreground">{t("settings.pools.allocationsNone")}</p>
          ) : null}
          {items.some(({ read }) => read !== latestRead) ? (
            <p className="text-muted-foreground">{t("settings.pools.allocationsPagesNote")}</p>
          ) : null}
          {items.length ? (
            <ul className="divide-y divide-border/50 rounded border border-border/60">
              {items.map(({ allocation, read }) => {
                const phase = ALLOCATION_PHASE[allocation.phase];
                const earlier = read !== latestRead;
                return (
                  <li
                    key={`${allocation.executor}.${allocation.job}`}
                    className={`flex flex-wrap items-center gap-x-4 gap-y-1 px-2 py-1.5 ${
                      allocation.phase === "uncertain" && !earlier ? "bg-red-500/5" : ""
                    }`}
                  >
                    <Pill tone={phase.tone} dot pulse={phase.pulse} className="text-xs normal-case">
                      {t(phase.label)}
                    </Pill>
                    <span className="break-all">
                      <span className="text-muted-foreground">
                        {t("settings.pools.allocationJob")}{" "}
                      </span>
                      <code>{allocation.job}</code>
                    </span>
                    <span className="break-all">
                      <span className="text-muted-foreground">
                        {t("settings.pools.allocationExecutor")}{" "}
                      </span>
                      <code>{allocation.executor}</code>
                    </span>
                    <span className="text-muted-foreground">
                      {t(
                        earlier
                          ? "settings.pools.allocationEarlierRead"
                          : "settings.pools.allocationHolds",
                      )}
                    </span>
                    {allocation.phase === "uncertain" && !earlier ? (
                      <span className="font-medium text-destructive">
                        {t("settings.pools.allocationUnresolved")}
                      </span>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          ) : null}
          {snapshot?.has_more ? (
            <Button
              type="button"
              variant="outline"
              size="xs"
              disabled={loading}
              onClick={() => load(snapshot.next_after ?? undefined)}
            >
              {loading ? t("common.loading") : t("settings.pools.allocationsMore")}
            </Button>
          ) : null}
        </div>
      ) : null}
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
  const [updatingExecutorId, setUpdatingExecutorId] = useState<string | null>(null);
  const [staleExecutors, setStaleExecutors] = useState<ReadonlySet<string>>(new Set());
  const executorUpdating = useRef(false);
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
        const page = await client.readNativeExecutors({
          before: cursor,
          signal,
        });
        if (!signal.aborted) {
          setExecutors(page.executors);
          setStaleExecutors(new Set());
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

  /** Applies one change from the revision read; true when it was applied. */
  async function handleExecutorUpdate(
    executor: NativeExecutor,
    change: NativeExecutorChange,
  ): Promise<boolean> {
    if (executorUpdating.current) return false;
    executorUpdating.current = true;
    setActionError(null);
    setUpdatingExecutorId(executor.id);
    try {
      const updated = await client.updateNativeExecutor(executor.id, executor.revision, change);
      setExecutors((previous) => previous.map((item) => (item.id === updated.id ? updated : item)));
      return true;
    } catch (error) {
      if (
        isExecutorConflict(error) ||
        (error instanceof ControlTransportError && error.code === "executor_not_found")
      )
        setStaleExecutors((previous) => new Set(previous).add(executor.id));
      setActionError(refusalText(t, error, EXECUTOR_REFUSALS));
      return false;
    } finally {
      executorUpdating.current = false;
      setUpdatingExecutorId(null);
    }
  }

  /**
   * Archives one Executor from the revision read. The refusal text, or null
   * when it was archived, after which the current page is read again.
   */
  async function handleExecutorArchive(executor: NativeExecutor): Promise<string | null> {
    if (executorUpdating.current) return t("settings.pools.executorStale");
    executorUpdating.current = true;
    setActionError(null);
    setUpdatingExecutorId(executor.id);
    try {
      await client.archiveNativeExecutor(executor.id, executor.revision);
      navigateExecutors(executorBefore, executorPrevious);
      return null;
    } catch (error) {
      if (error instanceof ControlTransportError && ARCHIVE_STALE_CODES.has(error.code))
        setStaleExecutors((previous) => new Set(previous).add(executor.id));
      return refusalText(t, error, EXECUTOR_REFUSALS);
    } finally {
      executorUpdating.current = false;
      setUpdatingExecutorId(null);
    }
  }

  function navigate(cursor: string | undefined, history: (string | undefined)[]) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setLoading(true);
    void loadPools(cursor, history, controller.signal);
  }

  /**
   * Archives one Pool from the revision read. The refusal text, or null when
   * it was archived, after which the current page of Pools is read again.
   */
  async function handlePoolArchive(pool: ControlPool): Promise<string | null> {
    if (pool.revision === undefined) return t("settings.pools.refusal.poolChanged");
    setActionError(null);
    setUpdatingPoolId(pool.id);
    try {
      await client.archiveNativePool(pool.id, pool.revision);
      navigate(before, previous);
      return null;
    } catch (error) {
      const text = refusalText(t, error, POOL_ARCHIVE_REFUSALS);
      if (error instanceof ControlTransportError && POOL_ARCHIVE_STALE_CODES.has(error.code)) {
        navigate(before, previous);
        setActionError(text);
      }
      return text;
    } finally {
      setUpdatingPoolId(null);
    }
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
              <div key={pool.id}>
                <PoolRow
                  key={`${pool.id}-${pool.concurrency_limit}`}
                  pool={pool}
                  onUpdate={handleUpdate}
                  onArchive={native && pool.revision !== undefined ? handlePoolArchive : undefined}
                  disabled={updatingPoolId === pool.id}
                />
                {native ? <PoolAllocations client={client} pool={pool} /> : null}
              </div>
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
              <ExecutorRow
                key={`${executor.id}-${executor.revision}`}
                executor={executor}
                pool={pools.find((pool) => pool.id === executor.pool)}
                busy={updatingExecutorId !== null}
                stale={staleExecutors.has(executor.id)}
                onUpdate={handleExecutorUpdate}
                onArchive={handleExecutorArchive}
                onReload={() => navigateExecutors(executorBefore, executorPrevious)}
              />
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
