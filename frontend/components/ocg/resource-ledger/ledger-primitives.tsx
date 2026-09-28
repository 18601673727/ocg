"use client";

import type { ComponentType, ReactNode } from "react";
import { cn } from "@/lib/utils";
import {
  ATTRIBUTION_CONFIDENCE_DEFINITION,
  ATTRIBUTION_CONFIDENCE_LABEL,
  COST_PROVENANCE_DEFINITION,
  COST_PROVENANCE_LABEL,
  COST_PROVENANCES,
  RECONCILIATION_DEFINITION,
  RECONCILIATION_LABEL,
  USAGE_AUTHORITY_DEFINITION,
  USAGE_AUTHORITY_LABEL,
  USAGE_COMPONENT_DEFINITION,
  USAGE_COMPONENT_LABEL,
  USAGE_COMPONENTS,
  type AttributionConfidence,
  type CostProvenance,
  type ReconciliationStatus,
  type UsageAuthority,
} from "./types";
import { UNKNOWN } from "./format";

export function SectionTitle({ children, detail }: { children: ReactNode; detail?: ReactNode }) {
  return (
    <div className="mb-1.5 flex min-w-0 items-center gap-1.5">
      <h3 className="truncate text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">
        {children}
      </h3>
      {detail !== undefined && (
        <span className="min-w-0 truncate text-[10px] text-muted-foreground">{detail}</span>
      )}
    </div>
  );
}

/** Renders the canonical unknown glyph. */
export function Unknown() {
  return <span title="Unavailable">{UNKNOWN}</span>;
}

export function Metric({
  label,
  value,
  detail,
  title,
  icon: Icon,
}: {
  label: string;
  value: string;
  detail?: string;
  title?: string;
  icon?: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
}) {
  return (
    <div
      className="min-w-0 rounded-md border border-border bg-background px-2 py-1.5"
      title={title}
    >
      <div className="flex items-center gap-1 text-[10px] text-muted-foreground">
        {Icon && <Icon className="size-3 shrink-0" aria-hidden />}
        <span className="truncate">{label}</span>
      </div>
      <p className="mt-0.5 truncate text-[13px] font-semibold tabular-nums">{value}</p>
      {detail && <p className="truncate text-[10px] text-muted-foreground">{detail}</p>}
    </div>
  );
}

type Tone = "emerald" | "sky" | "amber" | "red" | "violet" | "slate";

const TONE_CLASS: Record<Tone, string> = {
  emerald: "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400",
  sky: "border-sky-500/30 bg-sky-500/10 text-sky-700 dark:text-sky-400",
  amber: "border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-400",
  red: "border-red-500/30 bg-red-500/10 text-red-700 dark:text-red-400",
  violet: "border-violet-500/30 bg-violet-500/10 text-violet-700 dark:text-violet-400",
  slate: "border-border bg-muted/50 text-muted-foreground",
};

export const AUTHORITY_TONE: Record<UsageAuthority, Tone> = {
  reportedCall: "emerald",
  reconciled: "sky",
  fallback: "amber",
  estimated: "violet",
};

export const ATTRIBUTION_TONE: Record<AttributionConfidence, Tone> = {
  exact: "emerald",
  inferred: "sky",
  fallback: "amber",
  unknown: "slate",
};

export const COST_TONE: Record<CostProvenance, Tone> = {
  reported: "emerald",
  stored: "sky",
  estimated: "amber",
  unavailable: "slate",
};

export const RECONCILIATION_TONE: Record<ReconciliationStatus, Tone> = {
  reconciled: "emerald",
  partial: "amber",
  pending: "amber",
  mismatch: "red",
  notApplicable: "slate",
  unknown: "slate",
};

export function Pill({
  children,
  tone = "slate",
  title,
}: {
  children: ReactNode;
  tone?: Tone;
  title?: string;
}) {
  return (
    <span
      title={title}
      className={cn(
        "inline-flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-medium capitalize",
        TONE_CLASS[tone],
      )}
    >
      {children}
    </span>
  );
}

export function AuthorityPill({ value }: { value: UsageAuthority }) {
  return (
    <Pill tone={AUTHORITY_TONE[value]} title={USAGE_AUTHORITY_DEFINITION[value]}>
      {USAGE_AUTHORITY_LABEL[value]}
    </Pill>
  );
}

export function AttributionPill({ value }: { value: AttributionConfidence }) {
  return (
    <Pill tone={ATTRIBUTION_TONE[value]} title={ATTRIBUTION_CONFIDENCE_DEFINITION[value]}>
      {ATTRIBUTION_CONFIDENCE_LABEL[value]}
    </Pill>
  );
}

export function CostPill({ value }: { value: CostProvenance }) {
  return (
    <Pill tone={COST_TONE[value]} title={COST_PROVENANCE_DEFINITION[value]}>
      {COST_PROVENANCE_LABEL[value]}
    </Pill>
  );
}

export function ReconciliationPill({ value }: { value: ReconciliationStatus }) {
  return (
    <Pill tone={RECONCILIATION_TONE[value]} title={RECONCILIATION_DEFINITION[value]}>
      {RECONCILIATION_LABEL[value]}
    </Pill>
  );
}

/** Compact cost-provenance counts used by the Overview provenance mix. */
export function CostsMix({ cost }: { cost: Record<CostProvenance, number> }) {
  return (
    <>
      {COST_PROVENANCES.map((key) => (
        <span
          key={key}
          className={cn(
            "inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] capitalize",
            TONE_CLASS[COST_TONE[key]],
            cost[key] === 0 && "opacity-50",
          )}
          title={`${COST_PROVENANCE_LABEL[key]}: ${cost[key]} call(s)`}
        >
          {COST_PROVENANCE_LABEL[key]}
          <strong className="font-semibold tabular-nums">{cost[key]}</strong>
        </span>
      ))}
    </>
  );
}

const RECONCILIATION_ORDER: ReconciliationStatus[] = [
  "reconciled",
  "partial",
  "pending",
  "mismatch",
  "notApplicable",
  "unknown",
];

/** Compact reconciliation indicator; mismatch is always surfaced even at zero. */
export function ReconciliationIndicator({
  counts,
  className,
}: {
  counts: Record<ReconciliationStatus, number>;
  className?: string;
}) {
  return (
    <div className={cn("flex min-w-0 flex-wrap items-center gap-1", className)} aria-label="Reconciliation status">
      {RECONCILIATION_ORDER.map((status) => (
        <span
          key={status}
          className={cn(
            "inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px]",
            RECONCILIATION_TONE[status],
            counts[status] === 0 && "opacity-50",
          )}
          title={`${RECONCILIATION_LABEL[status]}: ${counts[status]} call(s). ${RECONCILIATION_DEFINITION[status]}`}
        >
          <span className="capitalize">{RECONCILIATION_LABEL[status]}</span>
          <span className="font-semibold tabular-nums">{counts[status]}</span>
        </span>
      ))}
    </div>
  );
}

/** Collapsible explanation of every normalized term used by the inspector. */
export function DefinitionsDetails() {
  return (
    <details className="rounded-md border border-border bg-muted/20 px-2.5 py-2 text-[11px] open:bg-muted/30">
      <summary className="cursor-pointer list-none font-medium text-muted-foreground marker:hidden">
        Metric definitions
      </summary>
      <div className="mt-2 grid gap-2 sm:grid-cols-2">
        <div>
          <p className="font-semibold">Usage components</p>
          <ul className="mt-0.5 text-muted-foreground">
            {USAGE_COMPONENTS.map((component) => (
              <li key={component}>
                <strong className="font-medium text-foreground">{USAGE_COMPONENT_LABEL[component]}:</strong>{" "}
                {USAGE_COMPONENT_DEFINITION[component]}
              </li>
            ))}
          </ul>
        </div>
        <div className="space-y-2">
          <div>
            <p className="font-semibold">Usage authority</p>
            <ul className="text-muted-foreground">
              {(Object.keys(USAGE_AUTHORITY_LABEL) as UsageAuthority[]).map((key) => (
                <li key={key}>
                  <strong className="font-medium text-foreground">{USAGE_AUTHORITY_LABEL[key]}:</strong>{" "}
                  {USAGE_AUTHORITY_DEFINITION[key]}
                </li>
              ))}
            </ul>
          </div>
          <div>
            <p className="font-semibold">Cost provenance</p>
            <ul className="text-muted-foreground">
              {(Object.keys(COST_PROVENANCE_LABEL) as CostProvenance[]).map((key) => (
                <li key={key}>
                  <strong className="font-medium text-foreground">{COST_PROVENANCE_LABEL[key]}:</strong>{" "}
                  {COST_PROVENANCE_DEFINITION[key]}
                </li>
              ))}
            </ul>
          </div>
          <div>
            <p className="font-semibold">Attribution confidence</p>
            <ul className="text-muted-foreground">
              {(Object.keys(ATTRIBUTION_CONFIDENCE_LABEL) as AttributionConfidence[]).map((key) => (
                <li key={key}>
                  <strong className="font-medium text-foreground">{ATTRIBUTION_CONFIDENCE_LABEL[key]}:</strong>{" "}
                  {ATTRIBUTION_CONFIDENCE_DEFINITION[key]}
                </li>
              ))}
            </ul>
          </div>
          <div>
            <p className="font-semibold">Derived metrics</p>
            <ul className="text-muted-foreground">
              <li>
                 <strong className="font-medium text-foreground">Cache share:</strong> cache read ÷ component
                 traffic. Unavailable when cache read or component traffic is unknown, or the denominator is zero.
              </li>
              <li>
                <strong className="font-medium text-foreground">Cache leverage:</strong> cache read ÷ fresh input.
                Unavailable when fresh input is unknown or zero.
              </li>
              <li>
                <strong className="font-medium text-foreground">Cost:</strong> integer micro-units of USD
                (1,000,000 micros = $1). An explicit $0.000000 is a known free call, not unknown.
              </li>
            </ul>
          </div>
        </div>
      </div>
    </details>
  );
}
