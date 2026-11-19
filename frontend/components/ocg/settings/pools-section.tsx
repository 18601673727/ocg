import { createEffect, createMemo, createSignal } from "solid-js";
import type { Pool } from "@/components/ocg/contracts";
import { Pill } from "@/components/ocg/primitives";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import type { ApiClient, PoolUpdate } from "@/components/ocg/runtime/api-client";
import {
  ApiError,
  configurationIdentity,
  isExecutorConflict,
  type NativeAllocation,
  type NativeAllocationPhase,
  type NativeExecutor,
  type NativeExecutorChange,
  type NativePoolAllocations,
} from "@/components/ocg/runtime/api-client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { elementRef } from "@/lib/ui-state";
import { type I18nKey, useI18n } from "../i18n";
import { describeError, refusalText, Section } from "./settings-parts";

const POOL_REFUSALS: Partial<Record<string, I18nKey>> = {
  pool_not_found: "settings.pools.refusal.notFound",
  pool_revision_conflict: "settings.pools.refusal.poolUpdateChanged",
  invalid_request: "settings.pools.refusal.invalidRequest",
  storage_refused: "settings.pools.refusal.storageRefused",
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
  storage_refused: "settings.pools.refusal.storageRefused",
};

type RowStale = "changed" | "updateUnconfirmed" | "archiveUnconfirmed";

type StaleLock = { reason: RowStale; at: number };

function useStaleRows() {
  const locks = elementRef<ReadonlyMap<string, StaleLock>>(new Map());
  const clock = elementRef(0);
  const [view, setView] = createSignal<ReadonlyMap<string, RowStale>>(new Map());
  const actions = createMemo(() => {
    const publish = (next: ReadonlyMap<string, StaleLock>) => {
      locks.current = next;
      setView(new Map([...next].map(([id, lock]) => [id, lock.reason])));
    };
    return {
      has: (id: string) => locks.current.has(id),
      mark: (id: string, reason: RowStale) =>
        publish(new Map(locks.current).set(id, { reason, at: ++clock.current })),
      sendRead: () => ++clock.current,
      readSucceeded: (sentAt: number) => {
        const kept = new Map([...locks.current].filter(([, lock]) => lock.at > sentAt));
        if (kept.size !== locks.current.size) publish(kept);
      },
    };
  });
  return [view(), actions()] as const;
}

const POOL_STALE_TEXT: Record<RowStale, I18nKey> = {
  changed: "settings.pools.poolStale",
  updateUnconfirmed: "settings.pools.poolStaleUnconfirmed",
  archiveUnconfirmed: "settings.pools.poolStaleArchiveUnconfirmed",
};

const EXECUTOR_STALE_TEXT: Record<RowStale, I18nKey> = {
  changed: "settings.pools.executorStale",
  updateUnconfirmed: "settings.pools.executorStaleUnconfirmed",
  archiveUnconfirmed: "settings.pools.executorStaleArchiveUnconfirmed",
};

const NOT_COMMITTED_CODES: ReadonlySet<string> = new Set([
  "credential_unavailable",
  "invalid_request",
  "database_busy",
  "shutting_down",
  "storage_refused",
  "pool_not_found",
  "pool_revision_conflict",
  "pool_archived",
  "pool_holds_allocation",
  "pool_has_active_executor",
  "pool_child_scan_unavailable",
  "pool_storage_inconsistent",
  "allocation_index_inconsistent",
  "invalid_pool",
  "executor_not_found",
  "executor_revision_conflict",
  "executor_archived",
  "executor_holds_allocation",
  "invalid_executor",
]);

function outcomeUnconfirmed(error: unknown): boolean {
  return !(error instanceof ApiError && NOT_COMMITTED_CODES.has(error.code));
}

function poolUpdateStale(error: unknown): RowStale | null {
  if (
    error instanceof ApiError &&
    (error.code === "pool_revision_conflict" || error.code === "pool_not_found")
  )
    return "changed";
  return outcomeUnconfirmed(error) ? "updateUnconfirmed" : null;
}

const POOL_ARCHIVE_STALE_CODES: ReadonlySet<string> = new Set([
  "pool_revision_conflict",
  "pool_not_found",
  "pool_archived",
]);

type ArchiveOutcome =
  | { kind: "archived" }
  | { kind: "refused"; text: string }
  | { kind: "unconfirmed" };

const EXECUTOR_REFUSALS: Partial<Record<string, I18nKey>> = {
  executor_not_found: "settings.pools.refusal.executorNotFound",
  executor_revision_conflict: "settings.pools.refusal.executorChanged",
  invalid_executor: "settings.pools.refusal.invalidExecutor",
  invalid_request: "settings.pools.refusal.invalidExecutor",
  executor_archived: "settings.pools.refusal.executorArchived",
  executor_holds_allocation: "settings.pools.refusal.executorHoldsAllocation",
  allocation_index_inconsistent: "settings.pools.refusal.executorIndexInconsistent",
  storage_refused: "settings.pools.refusal.storageRefused",
};

const NATIVE_CREATE_REFUSALS: Partial<Record<string, I18nKey>> = {
  storage_refused: "settings.pools.refusal.storageRefused",
};

const ARCHIVE_STALE_CODES: ReadonlySet<string> = new Set([
  "executor_revision_conflict",
  "executor_not_found",
  "executor_archived",
]);

function executorUpdateStale(error: unknown): RowStale | null {
  if (
    isExecutorConflict(error) ||
    (error instanceof ApiError && error.code === "executor_not_found")
  )
    return "changed";
  return outcomeUnconfirmed(error) ? "updateUnconfirmed" : null;
}

const ALLOCATION_REFUSALS: Partial<Record<string, I18nKey>> = {
  pool_not_found: "settings.pools.refusal.notFound",
  invalid_cursor: "settings.pools.refusal.invalidCursor",
  allocation_index_inconsistent: "settings.pools.refusal.allocationInconsistent",
  database_busy: "settings.pools.refusal.allocationUnavailable",
  database_unavailable: "settings.pools.refusal.allocationUnavailable",
};

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

const EXECUTOR_CAPABILITIES: {
  key: string;
  label: string | null;
  capability: NativeExecutor["capabilities"][number];
}[] = [
  {
    key: "openai_chat_completions",
    label: "OpenAI Chat Completions",
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
    key: "decision",
    label: null,
    capability: { operation: "decision", protocol: null },
  },
];

function capabilityKey(capability: NativeExecutor["capabilities"][number]): string {
  return capability.protocol ?? capability.operation;
}

function ExecutorRow(props: {
  executor: NativeExecutor;
  pool: Pool | undefined;
  busy: boolean;
  stale: RowStale | undefined;
  onUpdate: (executor: NativeExecutor, change: NativeExecutorChange) => Promise<boolean>;
  onArchive: (executor: NativeExecutor) => Promise<ArchiveOutcome>;
  onReload: () => void;
}) {
  const i18n = useI18n();
  const [confirmingArchive, setConfirmingArchive] = createSignal(false);
  const [archiving, setArchiving] = createSignal(false);
  const [archiveRefusal, setArchiveRefusal] = createSignal<string | null>(null);
  const [name, setName] = createSignal<string | null>(null);
  const [draft, setDraft] = createSignal<ReadonlySet<string> | null>(null);
  const disabled = createMemo(() => props.busy || props.stale !== undefined);
  const current = new Set(props.executor.capabilities.map(capabilityKey));
  const legacyOptions = createMemo(() =>
    props.executor.capabilities.flatMap((capability) => {
      const key = capabilityKey(capability);
      if (EXECUTOR_CAPABILITIES.some((option) => option.key === key)) return [];
      return [
        { key, label: `${key} (${i18n.t("settings.llm.executionUnavailable")})`, capability },
      ];
    }),
  );
  const options = createMemo(() => [...EXECUTOR_CAPABILITIES, ...legacyOptions()]);
  const state = createMemo(() =>
    i18n.t(props.executor.enabled ? "settings.pools.enabled" : "settings.pools.disabled"),
  );
  const nameValid = createMemo(
    () => name() !== null && name()!.trim() !== "" && name()!.trim() !== props.executor.name,
  );
  const draftValid = createMemo(
    () =>
      draft() !== null &&
      (draft()!.size !== current.size || [...(draft() ?? [])].some((key) => !current.has(key))),
  );

  return (
    <div class="space-y-2 py-2 text-sm">
      <div class="flex flex-wrap items-center justify-between gap-x-6 gap-y-2">
        <div class="min-w-0 flex-1 space-y-1">
          <div class="flex flex-wrap items-center gap-2">
            <strong class="break-all">{props.executor.name}</strong>
            <Pill tone={props.executor.enabled ? "emerald" : "amber"} class="text-xs normal-case">
              {i18n.t("settings.pools.executorState", { state: state() })}
            </Pill>
            {props.pool ? (
              <Pill tone={props.pool.enabled ? "emerald" : "amber"} class="text-xs normal-case">
                {i18n.t("settings.pools.executorPoolState", {
                  pool: props.pool.name,
                  state: i18n.t(
                    props.pool.enabled ? "settings.pools.enabled" : "settings.pools.disabled",
                  ),
                })}
              </Pill>
            ) : (
              <Pill tone="violet" class="text-xs normal-case">
                {i18n.t("settings.pools.executorPoolElsewhere", {
                  pool: props.executor.pool,
                })}
              </Pill>
            )}
          </div>
          <div class="text-xs break-all text-muted-foreground">
            {props.executor.id} · {props.executor.pool} ·{" "}
            {[...current]
              .map((key) => legacyOptions().find((option) => option.key === key)?.label ?? key)
              .join(", ")}
          </div>
        </div>
        <div class="flex flex-wrap items-center justify-end gap-2">
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled()}
            onClick={() =>
              void props.onUpdate(props.executor, { enabled: !props.executor.enabled })
            }
          >
            {i18n.t(props.executor.enabled ? "settings.pools.disable" : "settings.pools.enable")}
          </Button>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled()}
            onClick={() => setName(name() === null ? props.executor.name : null)}
          >
            {i18n.t("settings.pools.executorRename")}
          </Button>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled()}
            onClick={() => setDraft(draft() === null ? current : null)}
          >
            {i18n.t("settings.pools.executorCapabilities")}
          </Button>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={disabled() || archiving()}
            onClick={() => {
              setArchiveRefusal(null);
              setConfirmingArchive(!confirmingArchive());
            }}
          >
            {i18n.t("settings.pools.executorArchive")}
          </Button>
        </div>
      </div>
      {confirmingArchive() || archiveRefusal() ? (
        <div
          role="alertdialog"
          aria-label={i18n.t("settings.pools.executorArchiveTitle")}
          class="space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-xs"
        >
          {confirmingArchive() ? (
            <>
              <p class="font-semibold">{i18n.t("settings.pools.executorArchiveTitle")}</p>
              <ul class="list-disc space-y-1 pl-4">
                <li>{i18n.t("settings.pools.executorArchiveActiveList")}</li>
                <li>{i18n.t("settings.pools.executorArchiveHistory")}</li>
                <li>{i18n.t("settings.pools.executorArchiveNotStop")}</li>
                <li>{i18n.t("settings.pools.executorArchiveNotRelease")}</li>
                <li>{i18n.t("settings.pools.executorArchiveIrreversible")}</li>
              </ul>
            </>
          ) : null}
          {archiveRefusal() ? (
            <p role="alert" class="text-destructive">
              {archiveRefusal()}
            </p>
          ) : null}
          {confirmingArchive() ? (
            <div class="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="destructive"
                size="xs"
                disabled={disabled() || archiving()}
                onClick={() => {
                  setArchiving(true);
                  setArchiveRefusal(null);
                  void props.onArchive(props.executor).then((outcome) => {
                    setArchiving(false);
                    if (outcome.kind === "archived") return;
                    setArchiveRefusal(outcome.kind === "refused" ? outcome.text : null);
                    setConfirmingArchive(false);
                  });
                }}
              >
                {i18n.t(
                  archiving()
                    ? "settings.pools.executorArchiving"
                    : "settings.pools.executorArchiveConfirm",
                )}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="xs"
                disabled={archiving()}
                onClick={() => setConfirmingArchive(false)}
              >
                {i18n.t("common.cancel")}
              </Button>
            </div>
          ) : (
            <Button
              type="button"
              variant="outline"
              size="xs"
              onClick={() => setArchiveRefusal(null)}
            >
              {i18n.t("common.close")}
            </Button>
          )}
        </div>
      ) : null}
      {props.stale ? (
        <div role="alert" class="flex flex-wrap items-center gap-2 text-xs">
          <p class="text-destructive">{i18n.t(EXECUTOR_STALE_TEXT[props.stale])}</p>
          <Button type="button" variant="outline" size="xs" onClick={props.onReload}>
            {i18n.t("settings.pools.executorReload")}
          </Button>
        </div>
      ) : null}
      {name() !== null ? (
        <div class="flex flex-wrap items-center gap-2">
          <Input
            aria-label={i18n.t("settings.pools.executorName")}
            value={name()!}
            disabled={disabled()}
            onInput={(event) => setName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && nameValid() && !disabled()) {
                event.preventDefault();
                void props.onUpdate(props.executor, { name: name()!.trim() });
              }
            }}
            class="h-8 w-64 text-xs"
          />
          <Button
            type="button"
            size="xs"
            disabled={disabled() || !nameValid()}
            onClick={() => void props.onUpdate(props.executor, { name: name()!.trim() })}
          >
            {i18n.t("settings.pools.executorSaveName")}
          </Button>
          <Button type="button" variant="outline" size="xs" onClick={() => setName(null)}>
            {i18n.t("common.cancel")}
          </Button>
        </div>
      ) : null}
      {draft() !== null ? (
        <div class="flex flex-wrap items-center gap-3">
          {options().map((option) => (
            <label class="flex items-center gap-1.5 text-xs">
              <input
                type="checkbox"
                disabled={disabled()}
                checked={draft()!.has(option.key)}
                onChange={(event) => {
                  const next = new Set(draft());
                  if (event.target.checked) next.add(option.key);
                  else next.delete(option.key);
                  setDraft(next);
                }}
              />
              {option.label ?? i18n.t("settings.pools.operationDecision")}
            </label>
          ))}
          <Button
            type="button"
            size="xs"
            disabled={disabled() || !draftValid()}
            onClick={() =>
              void props.onUpdate(props.executor, {
                capabilities: options()
                  .filter((option) => draft()!.has(option.key))
                  .map((option) => option.capability),
              })
            }
          >
            {i18n.t("settings.pools.executorSaveCapabilities")}
          </Button>
          <Button type="button" variant="outline" size="xs" onClick={() => setDraft(null)}>
            {i18n.t("common.cancel")}
          </Button>
        </div>
      ) : null}
    </div>
  );
}

function PoolRow(props: {
  pool: Pool;
  onUpdate: (pool: Pool, update: PoolUpdate) => Promise<void>;
  onArchive: (pool: Pool) => Promise<ArchiveOutcome>;
  disabled: boolean;
  stale: RowStale | undefined;
  reloading: boolean;
  onReload: () => void;
}) {
  const i18n = useI18n();
  const [limit, setLimit] = createSignal(String(props.pool.capacity));
  const [confirmingArchive, setConfirmingArchive] = createSignal(false);
  const [archiving, setArchiving] = createSignal(false);
  const [archiveRefusal, setArchiveRefusal] = createSignal<string | null>(null);
  const locked = createMemo(() => props.disabled || props.stale !== undefined);

  const parsed = createMemo(() => Number(limit().trim()));
  const isIntegerGte1 = createMemo(
    () => Number.isSafeInteger(parsed()) && parsed() >= 1 && /^\d+$/.test(limit().trim()),
  );
  const isDifferent = createMemo(() => isIntegerGte1() && parsed() !== props.pool.capacity);
  const saveDisabled = createMemo(() => locked() || !isIntegerGte1() || !isDifferent());

  return (
    <div class="flex flex-wrap items-center justify-between gap-x-6 gap-y-3 py-3">
      <div class="min-w-0 flex-1 space-y-1">
        <div class="flex flex-wrap items-center gap-2">
          <Tooltip>
            <TooltipTrigger
              render={<button type="button" class="text-left text-sm font-semibold break-all" />}
            >
              {props.pool.name}
            </TooltipTrigger>
            <TooltipContent class="max-w-none font-mono">{props.pool.id}</TooltipContent>
          </Tooltip>
          <Pill tone={props.pool.enabled ? "emerald" : "amber"} class="text-xs normal-case">
            {i18n.t(props.pool.enabled ? "settings.pools.enabled" : "settings.pools.disabled")}
          </Pill>
        </div>
      </div>
      <div class="flex flex-wrap items-center justify-end gap-2">
        <div class="flex items-center gap-1.5">
          <label
            for={`pool-limit-${props.pool.id}`}
            class="text-xs text-muted-foreground whitespace-nowrap"
          >
            {i18n.t("settings.pools.concurrencyLimit")}
          </label>
          <Input
            id={`pool-limit-${props.pool.id}`}
            type="number"
            min={1}
            step={1}
            value={limit()}
            onInput={(e) => setLimit(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !saveDisabled()) {
                e.preventDefault();
                void props.onUpdate(props.pool, { concurrency_limit: parsed() });
              }
            }}
            disabled={locked()}
            class="h-8 w-20 text-xs"
          />
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={saveDisabled()}
            onClick={() => void props.onUpdate(props.pool, { concurrency_limit: parsed() })}
          >
            {i18n.t("settings.pools.saveLimit")}
          </Button>
        </div>
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={locked()}
          onClick={() => void props.onUpdate(props.pool, { enabled: !props.pool.enabled })}
        >
          {i18n.t(props.pool.enabled ? "settings.pools.disable" : "settings.pools.enable")}
        </Button>
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={locked() || archiving()}
          onClick={() => {
            setArchiveRefusal(null);
            setConfirmingArchive(!confirmingArchive());
          }}
        >
          {i18n.t("settings.pools.poolArchive")}
        </Button>
      </div>
      {props.stale ? (
        <div role="alert" class="flex w-full flex-wrap items-center gap-2 text-xs">
          <p class="text-destructive">{i18n.t(POOL_STALE_TEXT[props.stale])}</p>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={props.reloading}
            onClick={props.onReload}
          >
            {i18n.t("settings.pools.poolReload")}
          </Button>
        </div>
      ) : null}
      {confirmingArchive() || archiveRefusal() ? (
        <div
          role="alertdialog"
          aria-label={i18n.t("settings.pools.poolArchiveTitle")}
          class="w-full space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-xs"
        >
          {confirmingArchive() ? (
            <>
              <p class="font-semibold">{i18n.t("settings.pools.poolArchiveTitle")}</p>
              <ul class="list-disc space-y-1 pl-4">
                <li>{i18n.t("settings.pools.poolArchiveChildren")}</li>
                <li>{i18n.t("settings.pools.poolArchiveActiveList")}</li>
                <li>{i18n.t("settings.pools.poolArchiveHistory")}</li>
                <li>{i18n.t("settings.pools.poolArchiveNotStop")}</li>
                <li>{i18n.t("settings.pools.poolArchiveNotRelease")}</li>
                <li>{i18n.t("settings.pools.poolArchiveIrreversible")}</li>
              </ul>
            </>
          ) : null}
          {archiveRefusal() ? (
            <p role="alert" class="text-destructive">
              {archiveRefusal()}
            </p>
          ) : null}
          {confirmingArchive() ? (
            <div class="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="destructive"
                size="xs"
                disabled={locked() || archiving()}
                onClick={() => {
                  setArchiving(true);
                  setArchiveRefusal(null);
                  void props.onArchive(props.pool).then((outcome) => {
                    setArchiving(false);
                    if (outcome.kind === "archived") return;
                    setArchiveRefusal(outcome.kind === "refused" ? outcome.text : null);
                    setConfirmingArchive(false);
                  });
                }}
              >
                {i18n.t(
                  archiving()
                    ? "settings.pools.poolArchiving"
                    : "settings.pools.poolArchiveConfirm",
                )}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="xs"
                disabled={archiving()}
                onClick={() => setConfirmingArchive(false)}
              >
                {i18n.t("common.cancel")}
              </Button>
            </div>
          ) : (
            <Button
              type="button"
              variant="outline"
              size="xs"
              onClick={() => setArchiveRefusal(null)}
            >
              {i18n.t("common.close")}
            </Button>
          )}
        </div>
      ) : null}
    </div>
  );
}

function PoolAllocations(props: { client: ApiClient; pool: Pool }) {
  const i18n = useI18n();
  const [expanded, setExpanded] = createSignal(false);
  const [snapshot, setSnapshot] = createSignal<NativePoolAllocations | null>(null);
  const [items, setItems] = createSignal<{ allocation: NativeAllocation; read: number }[]>([]);
  const [latestRead, setLatestRead] = createSignal(0);
  const [error, setError] = createSignal<string | null>(null);
  const [loading, setLoading] = createSignal(false);
  const request = elementRef<AbortController | null>(null);
  const attempted = elementRef<string | undefined>(undefined);
  const reads = elementRef(0);

  createEffect(
    () => [],
    () => () => request.current?.abort(),
  );

  function load(after: string | undefined) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    attempted.current = after;
    setLoading(true);
    setError(null);
    props.client
      .readNativePoolAllocations(props.pool.id, { after, signal: controller.signal })
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
        if (!controller.signal.aborted) setError(refusalText(i18n.t, reason, ALLOCATION_REFUSALS));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
  }

  function toggle() {
    if (expanded()) {
      request.current?.abort();
      setLoading(false);
      setExpanded(false);
      return;
    }
    setExpanded(true);
    load(undefined);
  }

  const occupancy = createMemo(() => {
    const data = snapshot()?.occupancy;
    return data?.available ? data : undefined;
  });
  const capacity = createMemo(() => snapshot()?.capacity ?? props.pool.capacity);
  const indicator = createMemo(() =>
    !snapshot()
      ? i18n.t("settings.pools.capacityOnly", { capacity: capacity() })
      : occupancy()?.available
        ? i18n.t("settings.pools.capacityHeld", { held: occupancy()!.held, capacity: capacity() })
        : i18n.t("settings.pools.capacityHeldUnavailable", { capacity: capacity() }),
  );
  const overCapacity = createMemo(
    () => occupancy()?.available === true && occupancy()!.held > capacity(),
  );

  return (
    <div class="space-y-2 pb-3 text-xs">
      <div class="flex flex-wrap items-center gap-2">
        <Pill
          tone={overCapacity() ? "amber" : "slate"}
          class="text-xs normal-case"
          title={i18n.t("settings.pools.capacityHint")}
        >
          {indicator()}
        </Pill>
        {occupancy()?.available ? (
          <span class="text-muted-foreground">
            {i18n.t("settings.pools.occupancyBreakdown", {
              reserved: occupancy()!.reserved,
              running: occupancy()!.running,
              stopRequested: occupancy()!.stop_requested,
              uncertain: occupancy()!.uncertain,
            })}
          </span>
        ) : null}
        <Button type="button" variant="outline" size="xs" onClick={toggle}>
          {i18n.t(expanded() ? "settings.pools.allocationsHide" : "settings.pools.allocationsShow")}
        </Button>
        {expanded() ? (
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={loading()}
            onClick={() => load(undefined)}
          >
            {i18n.t("settings.pools.allocationsRefresh")}
          </Button>
        ) : null}
      </div>
      {expanded() ? (
        <div class="space-y-2">
          {error() ? (
            <div class="flex flex-wrap items-center gap-2 text-destructive" role="alert">
              <span>{error()}</span>
              <Button
                type="button"
                variant="outline"
                size="xs"
                disabled={loading()}
                onClick={() => load(attempted.current)}
              >
                {i18n.t("common.retry")}
              </Button>
            </div>
          ) : null}
          {snapshot() && occupancy() && !occupancy()!.available ? (
            <p class="text-muted-foreground">{i18n.t("settings.pools.occupancyUnavailable")}</p>
          ) : null}
          {overCapacity() ? (
            <p class="text-muted-foreground">{i18n.t("settings.pools.overCapacity")}</p>
          ) : null}
          {snapshot() && snapshot()!.capacity !== props.pool.capacity ? (
            <p class="text-muted-foreground">
              {i18n.t("settings.pools.capacityChanged", {
                capacity: props.pool.capacity,
              })}
            </p>
          ) : null}
          {loading() && !snapshot() ? (
            <p class="text-muted-foreground">{i18n.t("common.loading")}</p>
          ) : null}
          {snapshot() && !items().length && !error() ? (
            <p class="text-muted-foreground">{i18n.t("settings.pools.allocationsNone")}</p>
          ) : null}
          {items().some(({ read }) => read !== latestRead()) ? (
            <p class="text-muted-foreground">{i18n.t("settings.pools.allocationsPagesNote")}</p>
          ) : null}
          {items().length ? (
            <ul class="divide-y divide-border/50 rounded border border-border/60">
              {items().map(({ allocation, read }) => {
                const phase = ALLOCATION_PHASE[allocation.phase];
                const earlier = read !== latestRead();
                return (
                  <li
                    class={`flex flex-wrap items-center gap-x-4 gap-y-1 px-2 py-1.5 ${
                      allocation.phase === "uncertain" && !earlier ? "bg-red-500/5" : ""
                    }`}
                  >
                    <Pill tone={phase.tone} dot pulse={phase.pulse} class="text-xs normal-case">
                      {i18n.t(phase.label)}
                    </Pill>
                    <span class="break-all">
                      <span class="text-muted-foreground">
                        {i18n.t("settings.pools.allocationJob")}{" "}
                      </span>
                      <code>{allocation.job}</code>
                    </span>
                    <span class="break-all">
                      <span class="text-muted-foreground">
                        {i18n.t("settings.pools.allocationExecutor")}{" "}
                      </span>
                      <code>{allocation.executor}</code>
                    </span>
                    <span class="text-muted-foreground">
                      {i18n.t(
                        earlier
                          ? "settings.pools.allocationEarlierRead"
                          : "settings.pools.allocationHolds",
                      )}
                    </span>
                    {allocation.phase === "uncertain" && !earlier ? (
                      <span class="font-medium text-destructive">
                        {i18n.t("settings.pools.allocationUnresolved")}
                      </span>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          ) : null}
          {snapshot()?.has_more ? (
            <Button
              type="button"
              variant="outline"
              size="xs"
              disabled={loading()}
              onClick={() => load(snapshot()!.next_after ?? undefined)}
            >
              {loading() ? i18n.t("common.loading") : i18n.t("settings.pools.allocationsMore")}
            </Button>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export function PoolsSection(props: { client: ApiClient }) {
  const i18n = useI18n();
  const [pools, setPools] = createSignal<Pool[] | null>(null);
  const [loadError, setLoadError] = createSignal<string | null>(null);
  const [actionError, setActionError] = createSignal<string | null>(null);
  const [updatingPoolId, setUpdatingPoolId] = createSignal<string | null>(null);
  const [before, setBefore] = createSignal<string | undefined>();
  const [previous, setPrevious] = createSignal<(string | undefined)[]>([]);
  const [nextBefore, setNextBefore] = createSignal<string | null>(null);
  const [loading, setLoading] = createSignal(true);
  const [poolName, setPoolName] = createSignal("");
  const [poolCapacity, setPoolCapacity] = createSignal("1");
  const poolIdentity = elementRef<string | null>(null);
  const executorIdentity = elementRef<string | null>(null);
  const [executorName, setExecutorName] = createSignal("");
  const [executorPool, setExecutorPool] = createSignal("");
  const [capability, setCapability] = createSignal("openai_chat_completions");
  const [executors, setExecutors] = createSignal<NativeExecutor[]>([]);
  const [executorBefore, setExecutorBefore] = createSignal<string | undefined>();
  const [executorPrevious, setExecutorPrevious] = createSignal<(string | undefined)[]>([]);
  const [executorNext, setExecutorNext] = createSignal<string | null>(null);
  const [executorLoading, setExecutorLoading] = createSignal(false);
  const [executorError, setExecutorError] = createSignal<string | null>(null);
  const [failedExecutorPage, setFailedExecutorPage] = createSignal<{
    cursor: string | undefined;
    history: (string | undefined)[];
  } | null>(null);
  const [creating, setCreating] = createSignal(false);
  const [updatingExecutorId, setUpdatingExecutorId] = createSignal<string | null>(null);
  const [staleExecutors, executorStale] = useStaleRows();
  const executorUpdating = elementRef(false);
  const executorRequest = elementRef<AbortController | null>(null);
  const [failedPage, setFailedPage] = createSignal<{
    cursor: string | undefined;
    history: (string | undefined)[];
  } | null>(null);
  const request = elementRef<AbortController | null>(null);
  const [stalePools, poolStale] = useStaleRows();
  const poolMutating = elementRef(false);
  const archivedPools = elementRef(new Set<string>());
  const hasPoolPage = createMemo(() => pools() !== null);

  const loadPools = async (
    cursor: string | undefined,
    history: (string | undefined)[],
    signal: AbortSignal,
  ) => {
    const sentAt = poolStale.sendRead();
    try {
      const page = await props.client.readPools({ before: cursor, signal });
      if (!signal.aborted) {
        setPools(page.pools);
        poolStale.readSucceeded(sentAt);
        setBefore(cursor);
        setPrevious(history);
        setNextBefore(page.next_before);
        setLoadError(null);
        setFailedPage(null);
      }
    } catch (err: unknown) {
      if (!signal.aborted) {
        setLoadError(describeError(err, i18n.t));
        setFailedPage({ cursor, history });
      }
    } finally {
      if (!signal.aborted) setLoading(false);
    }
  };

  createEffect(
    () => [loadPools],
    () => {
      const controller = new AbortController();
      request.current = controller;
      void Promise.resolve().then(() => {
        if (!controller.signal.aborted) {
          setLoading(true);
          return loadPools(undefined, [], controller.signal);
        }
      });
      return () => request.current?.abort();
    },
  );

  const loadExecutors = async (
    cursor: string | undefined,
    history: (string | undefined)[],
    signal: AbortSignal,
  ) => {
    const sentAt = executorStale.sendRead();
    try {
      const page = await props.client.readNativeExecutors({
        before: cursor,
        signal,
      });
      if (!signal.aborted) {
        setExecutors(page.executors);
        executorStale.readSucceeded(sentAt);
        setExecutorBefore(cursor);
        setExecutorPrevious(history);
        setExecutorNext(page.next_before);
        setExecutorError(null);
        setFailedExecutorPage(null);
      }
    } catch (error) {
      if (!signal.aborted) {
        setExecutorError(describeError(error, i18n.t));
        setFailedExecutorPage({ cursor, history });
      }
    } finally {
      if (!signal.aborted) setExecutorLoading(false);
    }
  };

  createEffect(
    () => [hasPoolPage(), loadExecutors],
    () => {
      if (!hasPoolPage()) return;
      const controller = new AbortController();
      executorRequest.current = controller;
      void Promise.resolve().then(() => {
        if (!controller.signal.aborted) {
          setExecutorLoading(true);
          return loadExecutors(undefined, [], controller.signal);
        }
      });
      return () => executorRequest.current?.abort();
    },
  );

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
      await props.client.createNativePool(id, poolName().trim(), Number(poolCapacity()));
      poolIdentity.current = null;
      setPoolName("");
      setPoolCapacity("1");
      navigate(undefined, []);
    } catch (error) {
      setActionError(refusalText(i18n.t, error, NATIVE_CREATE_REFUSALS));
    } finally {
      setCreating(false);
    }
  }

  async function createExecutor() {
    const option = EXECUTOR_CAPABILITIES.find(({ key }) => key === capability());
    if (!option) return;
    setActionError(null);
    setCreating(true);
    try {
      const id = executorIdentity.current ?? configurationIdentity();
      executorIdentity.current = id;
      await props.client.createNativeExecutor(
        id,
        executorPool(),
        executorName().trim(),
        option.capability,
      );
      executorIdentity.current = null;
      setExecutorName("");
      navigateExecutors(undefined, []);
    } catch (error) {
      setActionError(refusalText(i18n.t, error, NATIVE_CREATE_REFUSALS));
    } finally {
      setCreating(false);
    }
  }

  async function handleExecutorUpdate(
    executor: NativeExecutor,
    change: NativeExecutorChange,
  ): Promise<boolean> {
    if (executorUpdating.current || executorStale.has(executor.id)) return false;
    executorUpdating.current = true;
    setActionError(null);
    setUpdatingExecutorId(executor.id);
    try {
      const updated = await props.client.updateNativeExecutor(executor, change);
      setExecutors((previous) => previous.map((item) => (item.id === updated.id ? updated : item)));
      return true;
    } catch (error) {
      const stale = executorUpdateStale(error);
      if (stale) executorStale.mark(executor.id, stale);
      setActionError(
        stale === "updateUnconfirmed"
          ? i18n.t("settings.pools.refusal.executorUpdateUnconfirmed")
          : refusalText(i18n.t, error, EXECUTOR_REFUSALS),
      );
      return false;
    } finally {
      executorUpdating.current = false;
      setUpdatingExecutorId(null);
    }
  }

  async function handleExecutorArchive(executor: NativeExecutor): Promise<ArchiveOutcome> {
    if (executorStale.has(executor.id)) return { kind: "unconfirmed" };
    if (executorUpdating.current)
      return { kind: "refused", text: i18n.t("settings.pools.executorBusy") };
    executorUpdating.current = true;
    setActionError(null);
    setUpdatingExecutorId(executor.id);
    try {
      await props.client.archiveNativeExecutor(executor.id, executor.revision);
      setExecutors((current) => current.filter((item) => item.id !== executor.id));
      navigateExecutors(executorBefore(), executorPrevious());
      return { kind: "archived" };
    } catch (error) {
      if (outcomeUnconfirmed(error)) {
        executorStale.mark(executor.id, "archiveUnconfirmed");
        setActionError(i18n.t("settings.pools.refusal.executorArchiveUnconfirmed"));
        return { kind: "unconfirmed" };
      }
      if (error instanceof ApiError && ARCHIVE_STALE_CODES.has(error.code))
        executorStale.mark(executor.id, "changed");
      return { kind: "refused", text: refusalText(i18n.t, error, EXECUTOR_REFUSALS) };
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

  async function handlePoolArchive(pool: Pool): Promise<ArchiveOutcome> {
    if (poolStale.has(pool.id)) return { kind: "unconfirmed" };
    if (archivedPools.current.has(pool.id)) return { kind: "archived" };
    if (poolMutating.current) return { kind: "refused", text: i18n.t("settings.pools.poolBusy") };
    poolMutating.current = true;
    setActionError(null);
    setUpdatingPoolId(pool.id);
    try {
      await props.client.archiveNativePool(pool.id, pool.revision);
      archivedPools.current.add(pool.id);
      setPools((current) => current?.filter((item) => item.id !== pool.id) ?? null);
      navigate(before(), previous());
      return { kind: "archived" };
    } catch (error) {
      if (outcomeUnconfirmed(error)) {
        poolStale.mark(pool.id, "archiveUnconfirmed");
        setActionError(i18n.t("settings.pools.refusal.poolArchiveUnconfirmed"));
        return { kind: "unconfirmed" };
      }
      const text = refusalText(i18n.t, error, POOL_ARCHIVE_REFUSALS);
      if (error instanceof ApiError && POOL_ARCHIVE_STALE_CODES.has(error.code)) {
        poolStale.mark(pool.id, "changed");
        navigate(before(), previous());
        setActionError(text);
      }
      return { kind: "refused", text };
    } finally {
      poolMutating.current = false;
      setUpdatingPoolId(null);
    }
  }

  async function handleUpdate(pool: Pool, update: PoolUpdate) {
    if (poolStale.has(pool.id) || archivedPools.current.has(pool.id) || poolMutating.current)
      return;
    poolMutating.current = true;
    setActionError(null);
    setUpdatingPoolId(pool.id);
    try {
      const updated = await props.client.updatePool(pool.id, pool.revision, update);
      setPools((prev) => prev?.map((p) => (p.id === updated.id ? updated : p)) ?? null);
    } catch (err) {
      const stale = poolUpdateStale(err);
      if (stale) poolStale.mark(pool.id, stale);
      setActionError(
        stale === "updateUnconfirmed"
          ? i18n.t("settings.pools.refusal.poolUpdateUnconfirmed")
          : refusalText(i18n.t, err, POOL_REFUSALS),
      );
    } finally {
      poolMutating.current = false;
      setUpdatingPoolId(null);
    }
  }

  const emptyPage = createMemo(() => pools()?.length === 0 && !loading() && !loadError());

  return (
    <Section
      id="settings-pools"
      title={i18n.t("settings.pools.title")}
      description={i18n.t("settings.pools.description")}
    >
      {loadError() ? (
        <DismissibleNotice message={loadError()!} class="py-2 text-sm text-destructive">
          {loadError()}
        </DismissibleNotice>
      ) : null}
      {loadError() ? (
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={loading()}
          onClick={() => {
            if (failedPage()) navigate(failedPage()!.cursor, failedPage()!.history);
          }}
        >
          {i18n.t("common.retry")}
        </Button>
      ) : null}
      {actionError() ? (
        <DismissibleNotice message={actionError()!} class="py-2 text-sm text-destructive">
          {actionError()}
        </DismissibleNotice>
      ) : null}

      {hasPoolPage() ? (
        <div class="flex flex-wrap items-end gap-2">
          <label class="text-xs">
            {i18n.t("settings.pools.nativeName")}
            <Input value={poolName()} onInput={(event) => setPoolName(event.target.value)} />
          </label>
          <label class="text-xs">
            {i18n.t("settings.pools.concurrencyLimit")}
            <Input
              type="number"
              min={1}
              max={4294967295}
              value={poolCapacity()}
              onInput={(event) => setPoolCapacity(event.target.value)}
            />
          </label>
          <Button
            type="button"
            size="xs"
            disabled={creating() || !poolName().trim()}
            onClick={() => void createPool()}
          >
            {i18n.t("settings.pools.nativeCreate")}
          </Button>
        </div>
      ) : null}

      <TooltipProvider delay={100}>
        <div class="divide-y divide-border/70 py-2">
          {emptyPage() && before() === undefined && nextBefore() === null ? (
            <p class="py-2 text-sm text-muted-foreground">{i18n.t("settings.pools.empty")}</p>
          ) : null}
          {emptyPage() && nextBefore() !== null ? (
            <p class="py-2 text-sm text-muted-foreground">{i18n.t("settings.pools.emptyPage")}</p>
          ) : null}
          {emptyPage() && before() !== undefined && nextBefore() === null ? (
            <p class="py-2 text-sm text-muted-foreground">
              {i18n.t("settings.pools.emptyLastPage")}
            </p>
          ) : null}
          {pools()?.map((pool) => (
            <div>
              <PoolRow
                pool={pool}
                onUpdate={handleUpdate}
                onArchive={handlePoolArchive}
                disabled={updatingPoolId() !== null}
                stale={stalePools.get(pool.id)}
                reloading={loading()}
                onReload={() => {
                  setActionError(null);
                  navigate(before(), previous());
                }}
              />
              <PoolAllocations client={props.client} pool={pool} />
            </div>
          ))}
        </div>
      </TooltipProvider>

      <div class="flex gap-2 py-2">
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={loading() || previous().length === 0}
          onClick={() => navigate(previous()[previous().length - 1], previous().slice(0, -1))}
        >
          {i18n.t("settings.pools.previousPage")}
        </Button>
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={loading() || nextBefore() === null}
          onClick={() => {
            if (nextBefore() !== null) navigate(nextBefore()!, [...previous(), before()]);
          }}
        >
          {loading() ? i18n.t("common.loading") : i18n.t("settings.pools.nextPage")}
        </Button>
      </div>

      {hasPoolPage() ? (
        <div class="space-y-3 border-t border-border/70 pt-3">
          <h3 class="font-semibold">{i18n.t("settings.pools.executors")}</h3>
          {executorError() ? (
            <DismissibleNotice message={executorError()!} class="text-sm text-destructive">
              {executorError()}
            </DismissibleNotice>
          ) : null}
          {executorError() && failedExecutorPage() ? (
            <Button
              type="button"
              size="xs"
              variant="outline"
              onClick={() =>
                navigateExecutors(failedExecutorPage()!.cursor, failedExecutorPage()!.history)
              }
            >
              {i18n.t("common.retry")}
            </Button>
          ) : null}
          <div class="divide-y divide-border/70">
            {executors().map((executor) => (
              <ExecutorRow
                executor={executor}
                pool={pools()?.find((pool) => pool.id === executor.pool)}
                busy={updatingExecutorId() !== null}
                stale={staleExecutors.get(executor.id)}
                onUpdate={handleExecutorUpdate}
                onArchive={handleExecutorArchive}
                onReload={() => {
                  setActionError(null);
                  navigateExecutors(executorBefore(), executorPrevious());
                }}
              />
            ))}
            {!executors().length && !executorLoading() && !executorError() ? (
              <p class="text-sm text-muted-foreground">{i18n.t("settings.pools.executorEmpty")}</p>
            ) : null}
          </div>
          <div class="flex gap-2">
            <Button
              type="button"
              size="xs"
              variant="outline"
              disabled={executorLoading() || !executorPrevious().length}
              onClick={() =>
                navigateExecutors(
                  executorPrevious()[executorPrevious().length - 1],
                  executorPrevious().slice(0, -1),
                )
              }
            >
              {i18n.t("settings.pools.previousPage")}
            </Button>
            <Button
              type="button"
              size="xs"
              variant="outline"
              disabled={executorLoading() || executorNext() === null}
              onClick={() => {
                if (executorNext() !== null)
                  navigateExecutors(executorNext()!, [...executorPrevious(), executorBefore()]);
              }}
            >
              {i18n.t("settings.pools.nextPage")}
            </Button>
          </div>
          <div class="flex flex-wrap items-end gap-2">
            <label class="text-xs">
              {i18n.t("settings.pools.executorName")}
              <Input
                value={executorName()}
                onInput={(event) => setExecutorName(event.target.value)}
              />
            </label>
            <label class="text-xs">
              {i18n.t("settings.pools.executorPool")}
              <select
                class="block h-9 rounded border bg-background px-2"
                value={executorPool()}
                onChange={(event) => setExecutorPool(event.target.value)}
              >
                <option value="">—</option>
                {executorPool() && !pools()?.some((pool) => pool.id === executorPool()) ? (
                  <option value={executorPool()}>{executorPool()}</option>
                ) : null}
                {pools()?.map((pool) => (
                  <option value={pool.id}>
                    {pool.name} · {pool.id}
                  </option>
                ))}
              </select>
            </label>
            <label class="text-xs">
              {i18n.t("settings.pools.executorCapability")}
              <select
                class="block h-9 rounded border bg-background px-2"
                value={capability()}
                onChange={(event) => setCapability(event.target.value)}
              >
                {EXECUTOR_CAPABILITIES.map(({ key, label }) => (
                  <option value={key}>{label ?? i18n.t("settings.pools.operationDecision")}</option>
                ))}
              </select>
            </label>
            <Button
              type="button"
              size="xs"
              disabled={creating() || !executorName().trim() || !executorPool()}
              onClick={() => void createExecutor()}
            >
              {i18n.t("settings.pools.executorCreate")}
            </Button>
          </div>
        </div>
      ) : null}

      <p class="border-t border-border/70 py-2 text-xs text-muted-foreground">
        {i18n.t("settings.pools.note")}
      </p>
    </Section>
  );
}
