"use client";

import { useId } from "react";
import { RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { FilterOption, LedgerFilter, LedgerFilterOptions, TimeWindow } from "./types";
import { ALL_FILTER_VALUE, TIME_WINDOWS, TIME_WINDOW_LABEL } from "./types";

const TIME_WINDOW_OPTIONS: FilterOption[] = TIME_WINDOWS.map((window) => ({
  value: window,
  label: TIME_WINDOW_LABEL[window],
}));

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
  return (
    <section aria-label="Ledger filters" className="border-b border-border px-3 py-2">
      <div className="flex flex-wrap items-end gap-x-2 gap-y-1.5">
        <FilterSelect
          label="Window"
          value={filter.window}
          options={TIME_WINDOW_OPTIONS}
          allLabel="All time"
          onChange={(window) => onChange({ ...filter, window: window as TimeWindow })}
        />
        <FilterSelect
          label="Mission"
          value={filter.missionId}
          options={options.missions}
          allLabel="All missions"
          onChange={(missionId) => onChange({ ...filter, missionId })}
        />
        <FilterSelect
          label="Worker"
          value={filter.workerId}
          options={options.workers}
          allLabel="All workers"
          onChange={(workerId) => onChange({ ...filter, workerId })}
        />
        <FilterSelect
          label="Provider"
          value={filter.provider}
          options={options.providers}
          allLabel="All providers"
          onChange={(provider) => onChange({ ...filter, provider })}
        />
        <FilterSelect
          label="Model"
          value={filter.modelKey}
          options={options.models}
          allLabel="All models"
          onChange={(modelKey) => onChange({ ...filter, modelKey })}
        />
        <Button
          type="button"
          variant="outline"
          size="xs"
          disabled={!active}
          onClick={onReset}
          title="Reset all ledger filters"
        >
          <RotateCcw className="size-3" data-icon="inline-start" aria-hidden />
          Reset
        </Button>
        <span className="ml-auto shrink-0 self-end pb-1 text-[10px] tabular-nums text-muted-foreground">
          {resultCount} / {totalCount} calls
        </span>
      </div>
    </section>
  );
}
