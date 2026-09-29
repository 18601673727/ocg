"use client";

import { UNKNOWN } from "@/lib/format";
import { cn } from "@/lib/utils";
import { Pill, TONE_CLASS, type Tone } from "@/components/ocg/primitives";
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
  type LedgerCallStatus,
  type ReconciliationStatus,
  type UsageAuthority,
} from "./types";

export { Metric, SectionTitle } from "@/components/ocg/primitives";

/** Renders the canonical unknown glyph. */
export function Unknown() {
  return <span title="Unavailable">{UNKNOWN}</span>;
}

/**
 * Provenance tones for the ledger.
 *
 * These say how much to trust a number, which is a ledger idea rather than a
 * runtime status, so they live here instead of in the shared status maps.
 */
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

/** A single provider call either worked, failed, or is being retried. */
export const CALL_STATUS_TONE: Record<LedgerCallStatus, Tone> = {
  success: "emerald",
  failure: "red",
  retrying: "amber",
};

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
    <div
      className={cn("flex min-w-0 flex-wrap items-center gap-1", className)}
      aria-label="Reconciliation status"
    >
      {RECONCILIATION_ORDER.map((status) => (
        <span
          key={status}
          className={`inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] ${TONE_CLASS[RECONCILIATION_TONE[status]]}${counts[status] === 0 ? " opacity-50" : ""}`}
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
          <DefinitionList title="Usage authority" labels={USAGE_AUTHORITY_LABEL} definitions={USAGE_AUTHORITY_DEFINITION} />
          <DefinitionList title="Cost provenance" labels={COST_PROVENANCE_LABEL} definitions={COST_PROVENANCE_DEFINITION} />
          <DefinitionList
            title="Attribution confidence"
            labels={ATTRIBUTION_CONFIDENCE_LABEL}
            definitions={ATTRIBUTION_CONFIDENCE_DEFINITION}
          />
          <div>
            <p className="font-semibold">Derived metrics</p>
            <ul className="text-muted-foreground">
              <li>
                <strong className="font-medium text-foreground">Cache share:</strong> cache read ÷ component traffic.
                Unavailable when cache read or component traffic is unknown, or the denominator is zero.
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

function DefinitionList({
  title,
  labels,
  definitions,
}: {
  title: string;
  labels: Record<string, string>;
  definitions: Record<string, string>;
}) {
  return (
    <div>
      <p className="font-semibold">{title}</p>
      <ul className="text-muted-foreground">
        {Object.keys(labels).map((key) => (
          <li key={key}>
            <strong className="font-medium text-foreground">{labels[key]}:</strong> {definitions[key]}
          </li>
        ))}
      </ul>
    </div>
  );
}
