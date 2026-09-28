"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { cn } from "@/lib/utils";
import {
  LEDGER_SERIES,
  USAGE_COMPONENT_LABEL,
  USAGE_COMPONENTS,
  type LedgerSeriesKey,
} from "./types";
import { formatTokens } from "./format";
import { describeTimeSeries, type ComponentTraffic, type LedgerTimePoint } from "./selectors";
import { SectionTitle } from "./ledger-primitives";

const CHART_GRID = "var(--border)";

/** Horizontal stacked composition of the filtered token traffic. */
export function StackedCompositionBar({ traffic }: { traffic: ComponentTraffic }) {
  const segments = USAGE_COMPONENTS.map((component) => ({
    component,
    value: traffic[component],
    color: LEDGER_SERIES.find((series) => series.key === component)?.color ?? "var(--chart-3)",
  })).filter((segment): segment is { component: typeof segment.component; value: number; color: string } =>
    segment.value !== null && segment.value > 0,
  );
  const total = traffic.total;

  if (total === null || total <= 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
        Component traffic is unavailable for this selection.
      </p>
    );
  }

  return (
    <div className="min-w-0">
      <div
        className="flex h-3 w-full overflow-hidden rounded-sm border border-border bg-muted"
        role="img"
        aria-label={`Token composition of ${formatTokens(total)} total tokens: ${segments
          .map((segment) => `${USAGE_COMPONENT_LABEL[segment.component]} ${formatTokens(segment.value)}`)
          .join(", ")}`}
      >
        {segments.map((segment) => (
          <div
            key={segment.component}
            className="h-full"
            style={{ width: `${(segment.value / total) * 100}%`, backgroundColor: segment.color }}
            title={`${USAGE_COMPONENT_LABEL[segment.component]}: ${formatTokens(segment.value)} tokens (${(
              (segment.value / total) *
              100
            ).toFixed(1)}%)`}
          />
        ))}
      </div>
      <ul className="mt-1.5 flex flex-wrap gap-x-3 gap-y-1">
        {USAGE_COMPONENTS.map((component) => {
          const value = traffic[component];
          const color = LEDGER_SERIES.find((series) => series.key === component)?.color ?? "var(--chart-3)";
          return (
            <li key={component} className="flex min-w-0 items-center gap-1 text-[10px] text-muted-foreground">
              <span className="size-2 shrink-0 rounded-sm" style={{ backgroundColor: color }} aria-hidden />
              <span>{USAGE_COMPONENT_LABEL[component]}</span>
              <strong className="font-medium tabular-nums text-foreground">{formatTokens(value)}</strong>
            </li>
          );
        })}
        <li className="flex items-center gap-1 text-[10px] text-muted-foreground">
          <span>Total</span>
          <strong className="font-medium tabular-nums text-foreground">{formatTokens(total)}</strong>
        </li>
      </ul>
    </div>
  );
}

const DEFAULT_VISIBLE: LedgerSeriesKey[] = ["freshInput", "cacheRead", "output", "total"];

/** Bounded traffic-over-time chart with explicit series toggles. */
export function TrafficChart({ points }: { points: LedgerTimePoint[] }) {
  const [visible, setVisible] = useState<LedgerSeriesKey[]>(DEFAULT_VISIBLE);
  const summary = useMemo(() => describeTimeSeries(points), [points]);
  const active = LEDGER_SERIES.filter((series) => visible.includes(series.key));

  const toggle = (key: LedgerSeriesKey) => {
    setVisible((current) =>
      current.includes(key) ? current.filter((item) => item !== key) : [...current, key],
    );
  };

  if (points.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
        No timestamped usage matches the current filters.
      </p>
    );
  }

  return (
    <div className="min-w-0">
      <div className="mb-1.5 flex flex-wrap gap-1" role="group" aria-label="Traffic chart series">
        {LEDGER_SERIES.map((series) => {
          const on = visible.includes(series.key);
          return (
            <button
              key={series.key}
              type="button"
              aria-pressed={on}
              onClick={() => toggle(series.key)}
              title={`${on ? "Hide" : "Show"} ${series.label} series`}
              className={cn(
                "inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] transition-colors focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring",
                on ? "border-border bg-muted font-medium text-foreground" : "border-border/60 text-muted-foreground",
              )}
            >
              <span className="size-2 shrink-0 rounded-full" style={{ backgroundColor: series.color }} aria-hidden />
              {series.label}
            </button>
          );
        })}
      </div>

      <div
        className="h-44 min-h-[160px] w-full min-w-0 overflow-hidden"
        role="img"
        aria-label={`Token traffic over time. ${summary}`}
      >
        <ResponsiveContainer
          width="100%"
          height="100%"
          initialDimension={{ width: 360, height: 176 }}
          minWidth={48}
          minHeight={120}
          debounce={50}
        >
          <LineChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={CHART_GRID} vertical={false} />
            <XAxis
              dataKey="timestamp"
              tick={{ fontSize: 9, fill: "var(--muted-foreground)" }}
              axisLine={false}
              tickLine={false}
              minTickGap={24}
              tickFormatter={(value: string) => value.slice(11, 16)}
            />
            <YAxis
              domain={[0, "auto"]}
              tick={{ fontSize: 9, fill: "var(--muted-foreground)" }}
              axisLine={false}
              tickLine={false}
              width={40}
              tickFormatter={(value: number) => formatTokens(Number(value))}
            />
            <Tooltip
              formatter={(value, name) => [
                value === null || value === undefined ? "—" : `${formatTokens(Number(value))} tokens`,
                LEDGER_SERIES.find((series) => series.key === name)?.label ?? String(name),
              ]}
              labelFormatter={(label) => `${String(label).slice(11, 16)}Z`}
            />
            {active.map((series) => (
              <Line
                key={series.key}
                type="monotone"
                dataKey={series.key}
                name={series.key}
                stroke={series.color}
                strokeWidth={series.key === "total" ? 2 : 1.5}
                strokeDasharray={series.key === "total" ? undefined : "4 3"}
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <p className="mt-1 text-[10px] text-muted-foreground" aria-live="polite">
        {summary} · {active.length} of {LEDGER_SERIES.length} series shown. Missing values break the line instead
        of plotting zero.
      </p>
    </div>
  );
}

/** Small helper for the Overview composition header. */
export function CompositionSection({ traffic }: { traffic: ComponentTraffic }) {
  return (
    <section aria-label="Token composition">
      <SectionTitle detail="filtered dataset">Component traffic</SectionTitle>
      <StackedCompositionBar traffic={traffic} />
    </section>
  );
}
