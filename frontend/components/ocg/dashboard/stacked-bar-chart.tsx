"use client";

import { useState } from "react";
import { useI18n } from "../i18n";

export type ChartBar = { key: string; label: string; values: number[]; tooltip: string };
export type ChartSeries = { key: string; label: string; color: string; share: number };

export function StackedBarChart({ bars, series, metricLabel, ariaLabel, integerTicks = false }: {
  bars: readonly ChartBar[];
  series: readonly ChartSeries[];
  metricLabel: string;
  ariaLabel: string;
  integerTicks?: boolean;
}) {
  const { locale } = useI18n();
  const [activeBar, setActiveBar] = useState<number | null>(null);
  const compact = new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 });
  const percent = new Intl.NumberFormat(locale, { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const largest = Math.max(0, ...bars.map(bar => bar.values.reduce((sum, value) => sum + value, 0)));
  if (!bars.length || largest === 0) return null;
  const maximum = integerTicks ? Math.ceil(largest / 3) * 3 : largest;
  const left = 60, top = 16, plotWidth = 660, plotHeight = 230;
  const step = plotWidth / bars.length;
  const active = activeBar === null ? undefined : bars[activeBar];
  return <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_16rem]">
    <div className="relative min-w-0">
      <p className="mb-2 text-xs text-muted-foreground">{metricLabel}</p>
      <svg viewBox="0 0 740 290" width="100%" role="img" aria-label={ariaLabel}>
        {[0, 1, 2, 3].map(tick => {
          const y = top + plotHeight - plotHeight * tick / 3;
          return <g key={tick}>
            <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
            <text x={left - 8} y={y + 4} textAnchor="end" fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{compact.format(maximum * tick / 3)}</text>
          </g>;
        })}
        {bars.map((bar, index) => {
          let stacked = 0;
          return <g key={bar.key} tabIndex={0} aria-label={bar.tooltip}
            onMouseEnter={() => setActiveBar(index)} onMouseLeave={() => setActiveBar(null)}
            onFocus={() => setActiveBar(index)} onBlur={() => setActiveBar(null)}
            onKeyDown={event => { if (event.key === "Escape") setActiveBar(null); }}
            className="outline-none focus-visible:stroke-foreground">
            <title>{bar.tooltip}</title>
            <rect x={left + index * step} y={top} width={step} height={plotHeight} fill="transparent" />
            {series.map((entry, rank) => {
              const value = bar.values[rank] ?? 0;
              stacked += value;
              return value > 0 ? <rect key={entry.key} x={left + index * step + step * 0.1} y={top + plotHeight - stacked / maximum * plotHeight}
                width={step * 0.8} height={value / maximum * plotHeight} fill={entry.color} /> : null;
            })}
          </g>;
        })}
        <text x={left} y={top + plotHeight + 26} fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{bars[0].label}</text>
        <text x={left + plotWidth} y={top + plotHeight + 26} textAnchor="end" fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{bars[bars.length - 1].label}</text>
      </svg>
      {active ? <div role="tooltip" className="pointer-events-none absolute left-16 top-8 z-10 max-h-64 max-w-[calc(100%-4rem)] overflow-y-auto whitespace-pre-line rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">{active.tooltip}</div> : null}
    </div>
    <ol className="min-w-0 space-y-3 text-sm">
      {series.map((entry, rank) => <li key={entry.key} className="flex items-center gap-2">
        <span className="w-4 shrink-0 text-right text-xs text-muted-foreground">{rank + 1}</span>
        <span className="h-3 w-3 shrink-0 rounded-sm" style={{ backgroundColor: entry.color }} />
        <span className="min-w-0 flex-1 break-words">{entry.label}</span>
        <span className="shrink-0 tabular-nums text-muted-foreground">{percent.format(entry.share)}</span>
      </li>)}
    </ol>
  </div>;
}
