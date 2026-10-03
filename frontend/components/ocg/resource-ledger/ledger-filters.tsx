"use client";

import { useId } from "react";
import { RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { FilterOption, LedgerFilter, LedgerFilterOptions, TimeWindow } from "./types";
import { ALL_FILTER_VALUE, TIME_WINDOWS } from "./types";
import { useI18n, type I18nKey } from "../i18n";

const TIME_WINDOW_KEY: Record<TimeWindow, I18nKey> = {
  all: "ledger.filter.allTime",
  today: "ledger.window.today",
  last15m: "ledger.window.last15m",
  last1h: "ledger.window.last1h",
  last6h: "ledger.window.last6h",
  last24h: "ledger.window.last24h",
  last7d: "ledger.window.last7d",
};

function FilterSelect({
  label,
  value,
  options,
  allLabel,
  onChange,
}: {
  label: string;
  value: string;
  options: FilterOption[];
  allLabel: string;
  onChange: (value: string) => void;
}) {
  const id = useId();
  const items: FilterOption[] = [{ value: ALL_FILTER_VALUE, label: allLabel }, ...options];
  const selected = items.find((item) => item.value === value);
  return (
    <label htmlFor={id} className="flex min-w-0 flex-col gap-0.5">
      <span className="text-[10px] font-medium tracking-wider text-muted-foreground uppercase">{label}</span>
      <select
        id={id}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        title={selected ? `${label}: ${selected.label}${selected.detail ? ` · ${selected.detail}` : ""}` : label}
        className="h-7 w-full min-w-0 max-w-[13rem] truncate rounded border border-border bg-background px-1.5 text-[11px] text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30"
      >
        {items.map((item) => (
          <option key={item.value} value={item.value} title={item.detail}>
            {item.detail ? `${item.label} · ${item.detail}` : item.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export type LedgerFiltersProps = {
  filter: LedgerFilter;
  options: LedgerFilterOptions;
  active: boolean;
  resultCount: number;
  totalCount: number;
  onChange: (next: LedgerFilter) => void;
  onReset: () => void;
};

/** Compact, dependency-free filter bar. Every choice comes from the full dataset. */
export function LedgerFilters({
  filter,
  options,
  active,
  resultCount,
  totalCount,
  onChange,
  onReset,
}: LedgerFiltersProps) {
  const { t } = useI18n();
  const timeWindowOptions: FilterOption[] = TIME_WINDOWS.map((window) => ({
    value: window,
    label: t(TIME_WINDOW_KEY[window]),
  }));
  return (
    <section aria-label={t("ledger.filters")} className="border-b border-border px-3 py-2">
      <div className="flex flex-wrap items-end gap-x-2 gap-y-1.5">
        <FilterSelect
          label={t("ledger.filter.window")}
          value={filter.window}
          options={timeWindowOptions}
          allLabel={t("ledger.filter.allTime")}
          onChange={(window) => onChange({ ...filter, window: window as TimeWindow })}
        />
        <FilterSelect
          label={t("ledger.filter.job")}
          value={filter.jobId}
          options={options.jobs}
          allLabel={t("ledger.filter.allJobs")}
          onChange={(jobId) => onChange({ ...filter, jobId })}
        />
        <FilterSelect
          label={t("ledger.filter.worker")}
          value={filter.workerId}
          options={options.workers}
          allLabel={t("ledger.filter.allWorkers")}
          onChange={(workerId) => onChange({ ...filter, workerId })}
        />
        <FilterSelect
          label={t("ledger.filter.provider")}
          value={filter.provider}
          options={options.providers}
          allLabel={t("ledger.filter.allProviders")}
          onChange={(provider) => onChange({ ...filter, provider })}
        />
        <FilterSelect
          label={t("ledger.filter.model")}
          value={filter.modelKey}
          options={options.models}
          allLabel={t("ledger.filter.allModels")}
          onChange={(modelKey) => onChange({ ...filter, modelKey })}
        />
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={!active}
          onClick={onReset}
          title={t("ledger.reset")}
        >
          <RotateCcw className="size-3" data-icon="inline-start" aria-hidden />
          {t("common.reset")}
        </Button>
        <span className="ml-auto shrink-0 self-end pb-1 text-[10px] tabular-nums text-muted-foreground">
          {t("ledger.filter.count", { result: resultCount, total: totalCount })}
        </span>
      </div>
    </section>
  );
}
