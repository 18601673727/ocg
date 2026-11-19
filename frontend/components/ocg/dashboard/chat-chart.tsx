import { createMemo } from "solid-js";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useI18n } from "../i18n";

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

function Legend(props: { series: readonly ChatChartSeries[] }) {
  return (
    <div class="mb-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {props.series.map((item, index) => (
        <span>
          <i
            class="mr-1 inline-block h-2.5 w-2.5 rounded-sm"
            style={{ background: colors[index % colors.length] }}
          />
          {item.label}
        </span>
      ))}
    </div>
  );
}

function CartesianChart(props: { definition: ChatChartDefinition }) {
  const i18n = useI18n();
  const axisFor = (series: ChatChartSeries) =>
    props.definition.type === "combo" ? series.axis || "left" : "left";
  const valuesFor = (axis: "left" | "right") =>
    props.definition.series
      .filter((series) => axisFor(series) === axis)
      .flatMap((series) => series.data);
  const domain = (axis: "left" | "right"): [number, number] => {
    const values = valuesFor(axis);
    if (values.length === 0) return [0, 1];
    const hasBars = props.definition.series.some(
      (series) => axisFor(series) === axis && (series.type || props.definition.type) === "bar",
    );
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    return [
      hasBars ? Math.min(0, minimum) : minimum,
      hasBars ? Math.max(0, maximum) : maximum === minimum ? minimum + 1 : maximum,
    ];
  };
  const hasRightAxis = createMemo(
    () =>
      props.definition.type === "combo" &&
      props.definition.series.some((series) => series.axis === "right"),
  );
  const [leftMinimum, leftMaximum] = domain("left");
  const [rightMinimum, rightMaximum] = domain("right");
  const axisScale = (series: ChatChartSeries, value: number) => {
    const [minimum, maximum] =
      axisFor(series) === "right" ? [rightMinimum, rightMaximum] : [leftMinimum, leftMaximum];
    return scale(value, minimum, maximum);
  };
  const step = createMemo(() => plotWidth / Math.max(props.definition.labels.length, 1));
  const barSeries = createMemo(() =>
    props.definition.series.filter((series) => (series.type || props.definition.type) === "bar"),
  );
  const barWidth = createMemo(() => Math.max(3, (step() * 0.7) / Math.max(barSeries().length, 1)));
  const pointAt = (series: ChatChartSeries, index: number) =>
    `${left + index * step() + step() / 2},${axisScale(series, series.data[index])}`;
  const tooltip = (index: number) => (
    <div class="whitespace-pre text-left">
      <div>{props.definition.labels[index]}</div>
      {props.definition.series.map((item) => (
        <div>
          {item.label}: {format(item.data[index], i18n.locale)}
        </div>
      ))}
    </div>
  );

  return (
    <div class="my-3 min-w-0">
      <Legend series={props.definition.series} />
      <div class="relative min-w-0 overflow-x-auto">
        <TooltipProvider delay={0}>
          <svg
            viewBox={`0 0 ${width} ${height}`}
            width="100%"
            role="img"
            aria-label={props.definition.ariaLabel || "Chart"}
          >
            {[0, 1, 2, 3].map((level) => {
              const leftValue = leftMinimum + ((leftMaximum - leftMinimum) * level) / 3;
              const rightValue = rightMinimum + ((rightMaximum - rightMinimum) * level) / 3;
              const y = top + plotHeight - (plotHeight * level) / 3;
              return (
                <g>
                  <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
                  <text
                    x={left - 8}
                    y={y + 4}
                    text-anchor="end"
                    fill="var(--muted-foreground)"
                    class="text-[11px]"
                  >
                    {compact(leftValue, i18n.locale)}
                  </text>
                  {hasRightAxis() && (
                    <text
                      x={left + plotWidth + 12}
                      y={y + 4}
                      fill="var(--muted-foreground)"
                      class="text-[11px]"
                    >
                      {compact(rightValue, i18n.locale)}
                    </text>
                  )}
                </g>
              );
            })}
            {props.definition.series.map((series, seriesIndex) =>
              (series.type || props.definition.type) === "bar" ? (
                series.data.map((value, index) => {
                  const zero = axisScale(series, 0);
                  const valueY = axisScale(series, value);
                  const barIndex = barSeries().indexOf(series);
                  return (
                    <rect
                      x={
                        left +
                        index * step() +
                        step() / 2 -
                        (barSeries().length * barWidth()) / 2 +
                        barIndex * barWidth()
                      }
                      y={Math.min(valueY, zero)}
                      width={barWidth()}
                      height={Math.abs(valueY - zero)}
                      fill={colors[seriesIndex % colors.length]}
                      opacity="0.7"
                    />
                  );
                })
              ) : (
                <polyline
                  points={series.data.map((_, index) => pointAt(series, index)).join(" ")}
                  fill="none"
                  stroke={colors[seriesIndex % colors.length]}
                  stroke-width="3"
                  stroke-linejoin="round"
                  stroke-linecap="round"
                />
              ),
            )}
            {props.definition.series.map((series, seriesIndex) =>
              (series.type || props.definition.type) === "line"
                ? series.data.map((value, index) => (
                    <circle
                      cx={left + index * step() + step() / 2}
                      cy={axisScale(series, value)}
                      r="3.5"
                      fill={colors[seriesIndex % colors.length]}
                    />
                  ))
                : null,
            )}
            {props.definition.labels.map((label, index) => (
              <Tooltip>
                <TooltipTrigger
                  render={
                    <rect
                      x={left + index * step()}
                      y={top}
                      width={step()}
                      height={plotHeight}
                      fill="transparent"
                    />
                  }
                />
                <TooltipContent side="top" class="max-w-none">
                  {tooltip(index)}
                </TooltipContent>
              </Tooltip>
            ))}
            <text
              x={left}
              y={top + plotHeight + 26}
              fill="var(--muted-foreground)"
              class="text-[11px]"
            >
              {props.definition.labels[0]}
            </text>
            <text
              x={left + plotWidth}
              y={top + plotHeight + 26}
              text-anchor="end"
              fill="var(--muted-foreground)"
              class="text-[11px]"
            >
              {props.definition.labels[props.definition.labels.length - 1]}
            </text>
          </svg>
        </TooltipProvider>
      </div>
    </div>
  );
}

function PieChart(props: { definition: ChatChartDefinition }) {
  const i18n = useI18n();
  const series = createMemo(() => props.definition.series[0]);
  const total = createMemo(() => series().data.reduce((sum, value) => sum + value, 0));
  return (
    <>
      {(() => {
        if (total() <= 0) return null;
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
          <div class="my-3 min-w-0">
            <Legend series={[series()]} />
            <div class="flex flex-wrap items-center gap-4">
              <svg
                viewBox="0 0 290 290"
                width="290"
                role="img"
                aria-label={props.definition.ariaLabel || "Pie chart"}
              >
                {series().data.map((value, index) => {
                  const start =
                    -Math.PI / 2 +
                    series()
                      .data.slice(0, index)
                      .reduce((sum, item) => sum + (item / total()) * Math.PI * 2, 0);
                  const end = start + (value / total()) * Math.PI * 2;
                  return (
                    <path
                      d={path(start, end)}
                      fill={colors[index % colors.length]}
                      stroke="var(--background)"
                      stroke-width="2"
                    >
                      <title>{`${props.definition.labels[index]}: ${format(value, i18n.locale)}`}</title>
                    </path>
                  );
                })}
              </svg>
              <div class="text-xs text-muted-foreground">
                {series().data.map((value, index) => (
                  <div>
                    <i
                      class="mr-1 inline-block h-2.5 w-2.5 rounded-sm"
                      style={{ background: colors[index % colors.length] }}
                    />
                    {props.definition.labels[index]}: {format(value, i18n.locale)}
                  </div>
                ))}
              </div>
            </div>
          </div>
        );
      })()}
    </>
  );
}

export function Chart(props: { definition: ChatChartDefinition }) {
  return (
    <>
      {(() => {
        if (props.definition.type === "pie") return <PieChart definition={props.definition} />;
        return <CartesianChart definition={props.definition} />;
      })()}
    </>
  );
}
