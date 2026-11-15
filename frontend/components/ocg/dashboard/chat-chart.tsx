"use client";

import { useI18n } from "../i18n";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

export type ChartType = "line" | "bar" | "pie" | "combo";
export type ChatChartSeries = {
  label: string;
  data: readonly number[];
  type?: "line" | "bar";
  axis?: "left" | "right";
};
export type ChatChartDefinition = {
  type: ChartType;
  labels: readonly string[];
  series: readonly ChatChartSeries[];
  ariaLabel?: string;
};

const left = 62;
const top = 16;
const plotWidth = 650;
const plotHeight = 230;
const width = 790;
const height = 290;
const colors = [
  "var(--usage-1)",
  "var(--usage-3)",
  "var(--usage-2)",
  "var(--usage-4)",
  "var(--usage-5)",
];

function format(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 8 }).format(value);
}
function compact(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(
    value,
  );
}
function scale(value: number, minimum: number, maximum: number): number {
  return maximum === minimum
    ? top + plotHeight / 2
    : top + plotHeight - ((value - minimum) / (maximum - minimum)) * plotHeight;
}

function Legend({ series }: { series: readonly ChatChartSeries[] }) {
  return (
    <div className="mb-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {series.map((item, index) => (
        <span key={`${item.label}-${index}`}>
          <i
            className="mr-1 inline-block h-2.5 w-2.5 rounded-sm"
            style={{ background: colors[index % colors.length] }}
          />
          {item.label}
        </span>
      ))}
    </div>
  );
}

function CartesianChart({ definition }: { definition: ChatChartDefinition }) {
  const { locale } = useI18n();
  const axisFor = (series: ChatChartSeries) =>
    definition.type === "combo" ? series.axis || "left" : "left";
  const valuesFor = (axis: "left" | "right") =>
    definition.series.filter((series) => axisFor(series) === axis).flatMap((series) => series.data);
  const domain = (axis: "left" | "right"): [number, number] => {
    const values = valuesFor(axis);
    if (values.length === 0) return [0, 1];
    const hasBars = definition.series.some(
      (series) => axisFor(series) === axis && (series.type || definition.type) === "bar",
    );
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    return [
      hasBars ? Math.min(0, minimum) : minimum,
      hasBars ? Math.max(0, maximum) : maximum === minimum ? minimum + 1 : maximum,
    ];
  };
  const hasRightAxis =
    definition.type === "combo" && definition.series.some((series) => series.axis === "right");
  const [leftMinimum, leftMaximum] = domain("left");
  const [rightMinimum, rightMaximum] = domain("right");
  const axisScale = (series: ChatChartSeries, value: number) => {
    const [minimum, maximum] =
      axisFor(series) === "right" ? [rightMinimum, rightMaximum] : [leftMinimum, leftMaximum];
    return scale(value, minimum, maximum);
  };
  const step = plotWidth / Math.max(definition.labels.length, 1);
  const barSeries = definition.series.filter(
    (series) => (series.type || definition.type) === "bar",
  );
  const barWidth = Math.max(3, (step * 0.7) / Math.max(barSeries.length, 1));
  const pointAt = (series: ChatChartSeries, index: number) =>
    `${left + index * step + step / 2},${axisScale(series, series.data[index])}`;
  const tooltip = (index: number) => (
    <div className="whitespace-pre text-left">
      <div>{definition.labels[index]}</div>
      {definition.series.map((item) => (
        <div key={item.label}>
          {item.label}: {format(item.data[index], locale)}
        </div>
      ))}
    </div>
  );

  return (
    <div className="my-3 min-w-0">
      <Legend series={definition.series} />
      <div className="relative min-w-0 overflow-x-auto">
        <TooltipProvider delay={0}>
          <svg
            viewBox={`0 0 ${width} ${height}`}
            width="100%"
            role="img"
            aria-label={definition.ariaLabel || "Chart"}
          >
            {[0, 1, 2, 3].map((level) => {
              const leftValue = leftMinimum + ((leftMaximum - leftMinimum) * level) / 3;
              const rightValue = rightMinimum + ((rightMaximum - rightMinimum) * level) / 3;
              const y = top + plotHeight - (plotHeight * level) / 3;
              return (
                <g key={level}>
                  <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
                  <text
                    x={left - 8}
                    y={y + 4}
                    textAnchor="end"
                    fill="var(--muted-foreground)"
                    className="text-[11px]"
                  >
                    {compact(leftValue, locale)}
                  </text>
                  {hasRightAxis && (
                    <text
                      x={left + plotWidth + 12}
                      y={y + 4}
                      fill="var(--muted-foreground)"
                      className="text-[11px]"
                    >
                      {compact(rightValue, locale)}
                    </text>
                  )}
                </g>
              );
            })}
            {definition.series.map((series, seriesIndex) =>
              (series.type || definition.type) === "bar" ? (
                series.data.map((value, index) => {
                  const zero = axisScale(series, 0);
                  const valueY = axisScale(series, value);
                  const barIndex = barSeries.indexOf(series);
                  return (
                    <rect
                      key={`${series.label}-${index}`}
                      x={
                        left +
                        index * step +
                        step / 2 -
                        (barSeries.length * barWidth) / 2 +
                        barIndex * barWidth
                      }
                      y={Math.min(valueY, zero)}
                      width={barWidth}
                      height={Math.abs(valueY - zero)}
                      fill={colors[seriesIndex % colors.length]}
                      opacity="0.7"
                    />
                  );
                })
              ) : (
                <polyline
                  key={series.label}
                  points={series.data.map((_, index) => pointAt(series, index)).join(" ")}
                  fill="none"
                  stroke={colors[seriesIndex % colors.length]}
                  strokeWidth="3"
                  strokeLinejoin="round"
                  strokeLinecap="round"
                />
              ),
            )}
            {definition.series.map((series, seriesIndex) =>
              (series.type || definition.type) === "line"
                ? series.data.map((value, index) => (
                    <circle
                      key={`${series.label}-${index}`}
                      cx={left + index * step + step / 2}
                      cy={axisScale(series, value)}
                      r="3.5"
                      fill={colors[seriesIndex % colors.length]}
                    />
                  ))
                : null,
            )}
            {definition.labels.map((label, index) => (
              <Tooltip key={`${label}-${index}`}>
                <TooltipTrigger
                  render={
                    <rect
                      x={left + index * step}
                      y={top}
                      width={step}
                      height={plotHeight}
                      fill="transparent"
                    />
                  }
                />
                <TooltipContent side="top" className="max-w-none">
                  {tooltip(index)}
                </TooltipContent>
              </Tooltip>
            ))}
            <text
              x={left}
              y={top + plotHeight + 26}
              fill="var(--muted-foreground)"
              className="text-[11px]"
            >
              {definition.labels[0]}
            </text>
            <text
              x={left + plotWidth}
              y={top + plotHeight + 26}
              textAnchor="end"
              fill="var(--muted-foreground)"
              className="text-[11px]"
            >
              {definition.labels[definition.labels.length - 1]}
            </text>
          </svg>
        </TooltipProvider>
      </div>
    </div>
  );
}

function PieChart({ definition }: { definition: ChatChartDefinition }) {
  const { locale } = useI18n();
  const series = definition.series[0];
  const total = series.data.reduce((sum, value) => sum + value, 0);
  if (total <= 0) return null;
  const radius = 105;
  const center = 145;
  const path = (start: number, end: number) => {
    const x1 = center + radius * Math.cos(start);
    const y1 = center + radius * Math.sin(start);
    const x2 = center + radius * Math.cos(end);
    const y2 = center + radius * Math.sin(end);
    return `M ${center} ${center} L ${x1} ${y1} A ${radius} ${radius} 0 ${end - start > Math.PI ? 1 : 0} 1 ${x2} ${y2} Z`;
  };
  return (
    <div className="my-3 min-w-0">
      <Legend series={[series]} />
      <div className="flex flex-wrap items-center gap-4">
        <svg
          viewBox="0 0 290 290"
          width="290"
          role="img"
          aria-label={definition.ariaLabel || "Pie chart"}
        >
          {series.data.map((value, index) => {
            const start =
              -Math.PI / 2 +
              series.data
                .slice(0, index)
                .reduce((sum, item) => sum + (item / total) * Math.PI * 2, 0);
            const end = start + (value / total) * Math.PI * 2;
            return (
              <path
                key={`${definition.labels[index]}-${index}`}
                d={path(start, end)}
                fill={colors[index % colors.length]}
                stroke="var(--background)"
                strokeWidth="2"
              >
                <title>{`${definition.labels[index]}: ${format(value, locale)}`}</title>
              </path>
            );
          })}
        </svg>
        <div className="text-xs text-muted-foreground">
          {series.data.map((value, index) => (
            <div key={`${definition.labels[index]}-${index}`}>
              <i
                className="mr-1 inline-block h-2.5 w-2.5 rounded-sm"
                style={{ background: colors[index % colors.length] }}
              />
              {definition.labels[index]}: {format(value, locale)}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function Chart({ definition }: { definition: ChatChartDefinition }) {
  if (definition.type === "pie") return <PieChart definition={definition} />;
  return <CartesianChart definition={definition} />;
}
