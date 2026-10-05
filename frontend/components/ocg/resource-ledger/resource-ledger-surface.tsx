"use client";

import { useMemo, useState } from "react";
import {
  Activity,
  Coins,
  Database,
  Gauge,
  GitBranch,
  Layers,
  Users,
  Zap,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DEFAULT_LEDGER_FILTER,
  LEDGER_TABS,
  type LedgerFilter,
  type LedgerTab,
  type ResourceLedger,
  type ResourceLedgerEntry,
} from "./types";
import {
  filterEntries,
  filterOptions,
  groupEntries,
  isFilterActive,
  latestTimestampMs,
  summarize,
  toLedgerTimeSeries,
  UNKNOWN_JOB_KEY,
} from "./selectors";
import {
  formatCostMicros,
  formatCount,
  formatPercent,
  formatRatio,
  formatTimestamp,
  formatTokens,
} from "@/lib/format";
import {
  EmptyState,
  Metric,
  Pill,
  SectionTitle,
  SegmentedTabs,
  TONE_CLASS,
  type TabItem,
  type Tone,
} from "@/components/ocg/primitives";
import {
  ATTRIBUTION_TONE,
  AUTHORITY_TONE,
  CostsMix,
  DefinitionsDetails,
  ReconciliationIndicator,
} from "./ledger-primitives";
import { CompositionSection, TrafficChart } from "./ledger-charts";
import { LedgerFilters } from "./ledger-filters";
import { useI18n, type I18nKey } from "../i18n";
import {
  AggregateGroupList,
  AttributionBreakdown,
  AttributionSummary,
  LedgerCallRow,
} from "./ledger-tables";
import { CallDetail } from "./ledger-detail";

const TAB_LABEL_KEY: Record<LedgerTab, I18nKey> = {
  overview: "ledger.overview",
  ledger: "ledger.ledger",
  attribution: "ledger.attribution",
  providers: "ledger.providers",
  models: "ledger.models",
};

function EmptyLedger() {
  const { t } = useI18n();
  return (
    <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-6 text-center">
      <h2 className="text-[13px] font-semibold tracking-tight">{t("ledger.title")}</h2>
      <p className="max-w-md text-[11px] text-muted-foreground">
        This scenario has no normalized resource ledger. Open{" "}
        <code className="rounded border border-border bg-muted px-1">?scenario=resource-ledger</code> to inspect
        the deterministic fixture dataset.
      </p>
    </div>
  );
}

/**
 * One provenance family as count chips.
 *
 * The chips are tinted by the same tone the pills use, so a `fallback` number
 * looks the same here as it does on a row.
 */
function ProvenanceChips({
  counts,
  tones,
}: {
  counts: Record<string, number>;
  tones: Record<string, Tone>;
}) {
  return (
    <div className="flex flex-wrap gap-1">
      {Object.entries(counts).map(([key, count]) => (
        <span
          key={key}
          className={cn(
            "inline-flex items-center gap-1 rounded-md border px-1.5 py-0.5 text-[10px] capitalize",
            TONE_CLASS[tones[key] ?? "slate"],
            count === 0 && "opacity-50",
          )}
        >
          {key} <strong className="font-semibold tabular-nums">{count}</strong>
        </span>
      ))}
    </div>
  );
}

function ProvenanceMix({
  authority,
  cost,
  attribution,
}: {
  authority: Record<string, number>;
  cost: Record<string, number>;
  attribution: Record<string, number>;
}) {
  return (
    <div className="grid gap-2 sm:grid-cols-3">
      <div>
        <SectionTitle>Usage authority</SectionTitle>
        <ProvenanceChips counts={authority} tones={AUTHORITY_TONE} />
      </div>
      <div>
        <SectionTitle>Cost provenance</SectionTitle>
        <div className="flex flex-wrap gap-1">
          <CostsMix cost={cost} />
        </div>
      </div>
      <div>
        <SectionTitle>Attribution confidence</SectionTitle>
        <ProvenanceChips counts={attribution} tones={ATTRIBUTION_TONE} />
      </div>
    </div>
  );
}

export function ResourceLedgerSurface({ ledger }: { ledger: ResourceLedger | null }) {
  const { t } = useI18n();
  const [tab, setTab] = useState<LedgerTab>("overview");
  const [filter, setFilter] = useState<LedgerFilter>(DEFAULT_LEDGER_FILTER);
  const [selectedEntry, setSelectedEntry] = useState<ResourceLedgerEntry | null>(null);
  const tabs: TabItem<LedgerTab>[] = useMemo(
    () => LEDGER_TABS.map((id) => ({ id, label: t(TAB_LABEL_KEY[id]) })),
    [t],
  );

  const entries = useMemo(() => ledger?.entries ?? [], [ledger]);
  const options = useMemo(() => filterOptions(entries), [entries]);
  const nowMs = useMemo(() => latestTimestampMs(entries), [entries]);
  const filtered = useMemo(() => filterEntries(entries, filter, { nowMs }), [entries, filter, nowMs]);
  const summary = useMemo(() => summarize(filtered), [filtered]);
  const timeSeries = useMemo(
    () => toLedgerTimeSeries(filtered, { bucketMs: 15 * 60_000, limit: 24 }),
    [filtered],
  );
  const providerGroups = useMemo(() => groupEntries(filtered, "provider"), [filtered]);
  const modelGroups = useMemo(() => groupEntries(filtered, "model"), [filtered]);
  const modelVariantGroups = useMemo(() => groupEntries(filtered, "modelVariant"), [filtered]);
  const jobGroups = useMemo(
    () => groupEntries(filtered, "job").filter((group) => group.key !== UNKNOWN_JOB_KEY),
    [filtered],
  );
  const unknownEntries = useMemo(
    () => filtered.filter((entry) => entry.attributionConfidence === "unknown" || entry.attributedJobId === null),
    [filtered],
  );

  if (!ledger) return <EmptyLedger />;

  const filterActive = isFilterActive(filter);

  return (
    <div className="flex h-full min-h-0 w-full min-w-0 flex-col">
      <header className="shrink-0 border-b border-border px-3 py-2.5">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h2 className="text-[13px] font-semibold tracking-tight">{t("ledger.title")}</h2>
          <Pill tone="slate" title={t("ledger.filters")}>
            {t("ledger.header.calls", { count: formatCount(summary.entryCount) })}
          </Pill>
          <span className="text-[10px] text-muted-foreground">
            {t("ledger.header.jobsWorkers", { jobs: summary.jobCount, workers: summary.workerCount })}
          </span>
          <span
            className="ml-auto shrink-0 text-[10px] text-muted-foreground"
            title={ledger.generatedAt}
          >
            generated {formatTimestamp(ledger.generatedAt)}
          </span>
        </div>
      </header>

      <nav className="shrink-0 border-b border-border px-3 py-1.5">
        <SegmentedTabs
          tabs={tabs}
          value={tab}
          onSelect={setTab}
          ariaLabel={t("ledger.surfaces")}
          panelIdBase="resource-ledger"
          className="grid-cols-5"
          size="sm"
        />
      </nav>

      <LedgerFilters
        filter={filter}
        options={options}
        active={filterActive}
        resultCount={filtered.length}
        totalCount={entries.length}
        onChange={setFilter}
        onReset={() => setFilter(DEFAULT_LEDGER_FILTER)}
      />

      <div
        id={`resource-ledger-${tab}`}
        role="tabpanel"
        aria-label={t(TAB_LABEL_KEY[tab])}
        className="min-h-0 flex-1 overflow-y-auto px-3 py-3"
      >
        {tab === "overview" && (
          <div className="flex min-w-0 flex-col gap-3">
            <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3 xl:grid-cols-4">
              <Metric
                label="Component traffic"
                value={formatTokens(summary.totalTokens)}
                detail="fresh + cache + output + reasoning"
                icon={Gauge}
              />
              <Metric
                label={t("ledger.cost")}
                value={formatCostMicros(summary.costMicros)}
                detail={`${summary.costProvenance} provenance`}
                icon={Coins}
                title="Integer micro-units of USD. $0.000000 is a known free call; — is unknown."
              />
              <Metric
                label={t("ledger.calls")}
                value={formatCount(summary.entryCount)}
                detail={`${summary.successCount} ok · ${summary.failureCount} failed · ${summary.retryCount} retries`}
                icon={Layers}
              />
              <Metric
                label="Jobs / workers"
                value={`${summary.jobCount} / ${summary.workerCount}`}
                detail={`${summary.leadEntryCount} lead calls`}
                icon={Users}
              />
              <Metric label="Fresh input" value={formatTokens(summary.traffic.freshInput)} icon={Activity} />
              <Metric label="Cache read" value={formatTokens(summary.traffic.cacheRead)} icon={Zap} />
              <Metric
                label="Cache share"
                value={formatPercent(summary.cacheShare)}
                detail="cache read ÷ component traffic"
                icon={Database}
                 title="Unavailable when cache read or component traffic is unknown, or the denominator is zero."
              />
              <Metric label="Cache write" value={formatTokens(summary.traffic.cacheWrite)} icon={Zap} />
              <Metric label="Output" value={formatTokens(summary.traffic.output)} icon={Activity} />
              <Metric label="Reasoning" value={formatTokens(summary.traffic.reasoning)} icon={GitBranch} />
              <Metric
                label="Cache leverage"
                value={formatRatio(summary.cacheLeverage)}
                detail="cache read ÷ fresh input"
                icon={GitBranch}
                title="Unavailable when fresh input is unknown or zero."
              />
            </div>

            <CompositionSection traffic={summary.traffic} />

            <section aria-label="Traffic over time">
              <SectionTitle detail="bounded · filtered dataset">Traffic over time</SectionTitle>
              <TrafficChart points={timeSeries} />
            </section>

            <section aria-label="Provenance mix">
              <SectionTitle>Provenance mix</SectionTitle>
              <ProvenanceMix
                authority={summary.byAuthority}
                cost={summary.byCostProvenance}
                attribution={summary.byAttribution}
              />
            </section>

            <ReconciliationIndicator counts={summary.byReconciliation} />

            <DefinitionsDetails />
          </div>
        )}

        {tab === "ledger" && (
          <div className="flex min-w-0 flex-col gap-2">
            <p className="text-[10px] text-muted-foreground">
              {filtered.length} call(s) in the filtered dataset. Select a row to open its full detail sheet; all
              tabs share this same filtered dataset.
            </p>
            {filtered.length === 0 ? (
              <EmptyState className="px-2 py-4">{t("ledger.noMatch")}</EmptyState>
            ) : (
              <ul className="flex flex-col gap-1.5">
                {filtered.map((entry) => (
                  <LedgerCallRow key={entry.id} entry={entry} onSelect={setSelectedEntry} />
                ))}
              </ul>
            )}
          </div>
        )}

        {tab === "attribution" && (
          <div className="flex min-w-0 flex-col gap-3">
            <AttributionSummary summary={summary} />
            <AttributionBreakdown
              jobs={jobGroups}
              unknownEntries={unknownEntries}
              onSelectEntry={setSelectedEntry}
            />
          </div>
        )}

        {tab === "providers" && (
          <div className="flex min-w-0 flex-col gap-3">
            <section aria-label="Provider aggregates">
              <SectionTitle detail={`${providerGroups.length} provider(s) · filtered`}>Provider aggregates</SectionTitle>
              <AggregateGroupList groups={providerGroups} emptyLabel="No provider calls match the current filters." />
            </section>
          </div>
        )}

        {tab === "models" && (
          <div className="flex min-w-0 flex-col gap-3">
            <section aria-label="Model aggregates">
              <SectionTitle detail={`${modelGroups.length} provider+model group(s)`}>Model rollup</SectionTitle>
              <AggregateGroupList groups={modelGroups} emptyLabel="No model calls match the current filters." />
            </section>
            <section aria-label="Model and variant aggregates">
              <SectionTitle detail={`${modelVariantGroups.length} model+variant group(s)`}>
                Model + variant
              </SectionTitle>
              <AggregateGroupList
                groups={modelVariantGroups}
                emptyLabel="No model variant calls match the current filters."
              />
            </section>
          </div>
        )}
      </div>

      <Dialog
        open={selectedEntry !== null}
        onOpenChange={(open) => {
          if (!open) setSelectedEntry(null);
        }}
      >
        <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Call detail</DialogTitle>
            <DialogDescription>
              {selectedEntry
                ? `${selectedEntry.workerLabel} · ${selectedEntry.provider} · ${selectedEntry.model}`
                : "Select a call to inspect it."}
            </DialogDescription>
          </DialogHeader>
          {selectedEntry && <CallDetail entry={selectedEntry} />}
        </DialogContent>
      </Dialog>
    </div>
  );
}
