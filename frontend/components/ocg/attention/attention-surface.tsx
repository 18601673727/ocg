"use client";

import { useMemo, useState } from "react";
import {
  ArrowRight,
  Check,
  CircleCheck,
  Clock3,
  Inbox,
  Search,
  ShieldAlert,
  ShieldCheck,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  EmptyPanel,
  PageSurface,
  Pill,
  SegmentedTabs,
  StatusDot,
  TEXT_TONE,
  type Tone,
} from "@/components/ocg/primitives";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type {
  AttentionDestination,
  AttentionItem,
  AttentionKind,
  AttentionSeverity,
  AttentionSource,
  AttentionTab,
} from "./domain";
import {
  ATTENTION_KINDS,
  ATTENTION_KIND_LABELS,
  ATTENTION_SEVERITY_LABELS,
  ATTENTION_SOURCE_LABELS,
  ATTENTION_STATUS_LABELS,
  ATTENTION_TABS,
  APPROVAL_TYPE_LABELS,
  isApprovalItem,
  isBlockedItem,
  isUnresolved,
} from "./domain";
import type { AttentionFilters, AttentionQueue } from "./selectors";
import {
  ATTENTION_RESULT_LIMIT,
  acknowledgeAttentionItem,
  applyAttentionDecision,
  filterAttentionItems,
  resolveAttentionItem,
  selectAttentionItems,
  selectAttentionSummary,
} from "./selectors";
import { createAttentionQueue } from "./fixtures";
import type { WorkspaceView } from "../layout/view-domain";
import { useI18n } from "../i18n";

type AttentionSurfaceProps = {
  snapshot: RuntimeSnapshot;
  initialTab?: AttentionTab;
  /**
   * Optional project-scoped fixture queue. When omitted the scenario queue is
   * used, preserving the standalone behavior.
   */
  queue?: AttentionQueue;
  /** Opens the workspace an item points at; the shell decides the address. */
  onNavigate: (view: WorkspaceView) => void;
  onSelectSession?: (sessionId: string) => void;
  onSelectJob?: (jobId: string) => void;
};

const TAB_LABEL_KEY = {
  overview: "attention.overview",
  approvals: "attention.approvals",
  blocked: "attention.blocked",
  resolved: "attention.resolved",
} as const;

/**
 * Attention's own scales, expressed in the shared tone vocabulary. There is no
 * orange tone, so `high` and `critical` share red; the severity word beside the
 * dot keeps them distinguishable.
 */
const SEVERITY_TONE: Record<AttentionSeverity, Tone> = {
  info: "sky",
  warning: "amber",
  high: "red",
  critical: "red",
};

const KIND_TONE: Record<AttentionKind, Tone> = {
  approval: "violet",
  budget: "amber",
  policy: "slate",
  permission: "sky",
  blocked: "amber",
  "runtime-failure": "red",
  "resource-degraded": "amber",
  configuration: "slate",
  retry: "sky",
  escalation: "violet",
};

const DESTINATION_LABELS: Record<AttentionDestination, string> = {
  "job-execution": "Job Execution",
  "control-center": "Control Center",
  "resource-ledger": "Resource Ledger",
  logs: "Logs",
  settings: "Settings",
  chat: "Chat",
};

/** Where each destination is opened. Only `resource-ledger` names a view differently. */
const DESTINATION_VIEWS: Record<AttentionDestination, WorkspaceView> = {
  "job-execution": "job-execution",
  "control-center": "control-center",
  "resource-ledger": "ledger",
  logs: "logs",
  settings: "settings",
  chat: "chat",
};

/** Local fixture decisions use a fixed clock so UI state stays deterministic. */
const DECISION_CLOCK = "2026-09-25T10:00:00Z";

function CanonicalAttentionSurface({ snapshot, onNavigate, onSelectJob }: AttentionSurfaceProps) {
  const { t } = useI18n();
  const executions = snapshot.executionsByProject !== undefined
    ? Object.values(snapshot.executionsByProject).flatMap((jobs) => Object.values(jobs))
    : [...new Map(Object.values(snapshot.executionBySession)
      .filter((execution) => execution !== null)
      .map((execution) => [execution.jobId, execution])).values()];
  const failed = executions.filter((execution) => execution.state === "failed");
  return <PageSurface>
    <h1 className="text-xl font-semibold">{t("attention.title")}</h1>
    <p className="mt-1 text-sm text-muted-foreground">{t("attention.localReview")}</p>
    {failed.length === 0 ? <p className="mt-6 text-sm text-muted-foreground">{t("attention.noFailedJobs")}</p> :
      <ul className="mt-4 space-y-3">{failed.map((execution) => <li key={execution.jobId} className="rounded-lg border border-border p-3">
        <p className="text-sm font-medium">{t("home.failedJob", { id: execution.jobId })}</p>
        <p className="mt-1 text-xs text-muted-foreground">{t("home.failedSummary")}</p>
        <Button className="mt-3" size="xs" variant="outline" onClick={() => onSelectJob ? onSelectJob(execution.jobId) : onNavigate("job-execution")}>{t("execution.title")}</Button>
      </li>)}</ul>}
  </PageSurface>;
}

export function AttentionSurface(props: AttentionSurfaceProps) {
  return props.snapshot.authority === "canonical" ? <CanonicalAttentionSurface {...props} /> : <MockAttentionSurface {...props} />;
}

function MockAttentionSurface(props: AttentionSurfaceProps) {
  const { snapshot, initialTab = "overview", queue: queueProp, onNavigate } = props;
  const { t } = useI18n();
  const tabLabel = (tab: AttentionTab) => t(TAB_LABEL_KEY[tab]);

  // Fixture queue is stable per scenario; decisions mutate local state only.
  const queue = useMemo(
    () => queueProp ?? createAttentionQueue(snapshot.scenario),
    [queueProp, snapshot.scenario],
  );
  const baseItems = useMemo(() => selectAttentionItems(snapshot, queue), [snapshot, queue]);
  const [overrides, setOverrides] = useState<Record<string, AttentionItem>>({});
  const items = useMemo(
    () => baseItems.map((item) => overrides[item.id] ?? item),
    [baseItems, overrides],
  );

  const [filters, setFilters] = useState<AttentionFilters>({
    tab: initialTab,
    query: "",
    kind: "all",
    severity: "all",
    source: "all",
  });
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const visible = useMemo(() => filterAttentionItems(items, filters), [items, filters]);
  const summary = useMemo(() => selectAttentionSummary(items), [items]);
  const tabbed = useMemo(
    () => ({
      overview: items.filter(isUnresolved).length,
      approvals: items.filter((i) => isUnresolved(i) && isApprovalItem(i)).length,
      blocked: items.filter((i) => isUnresolved(i) && isBlockedItem(i)).length,
      resolved: items.filter((i) => !isUnresolved(i)).length,
    }),
    [items],
  );

  const selected = selectedId ? items.find((item) => item.id === selectedId) ?? null : null;

  const patchItems = (next: AttentionItem[]) => {
    setOverrides((prev) => {
      const merged = { ...prev };
      for (const item of next) {
        const base = baseItems.find((b) => b.id === item.id);
        if (!base || JSON.stringify(base) !== JSON.stringify(item)) merged[item.id] = item;
        else delete merged[item.id];
      }
      return merged;
    });
  };

  const handleDecide = (id: string, decision: "approved" | "rejected") => {
    patchItems(applyAttentionDecision(items, id, decision, DECISION_CLOCK));
  };
  const handleAcknowledge = (id: string) => {
    patchItems(acknowledgeAttentionItem(items, id, DECISION_CLOCK));
  };
  const handleResolve = (id: string) => {
    patchItems(resolveAttentionItem(items, id, DECISION_CLOCK));
  };

  const truncated = visible.length >= ATTENTION_RESULT_LIMIT;

  return (
    <div className="flex min-h-0 flex-1 overflow-hidden">
      {/* List column */}
      <div className={cn("flex min-h-0 w-full min-w-0 flex-1 flex-col", selected && "lg:border-r lg:border-border")}>
        <div className="shrink-0 px-4 pt-4 sm:px-6 sm:pt-6">
          <h1 className="text-[20px] font-semibold tracking-tight">{t("attention.title")}</h1>
          <p className="mt-0.5 text-[13px] text-muted-foreground">
            {t("attention.subtitle")}
          </p>
          <SummaryStrip summary={summary} />
          <SegmentedTabs
            tabs={ATTENTION_TABS.map((tab) => ({ id: tab, label: tabLabel(tab), count: tabbed[tab] }))}
            value={filters.tab}
            onSelect={(tab) => {
              setFilters((f) => ({ ...f, tab }));
              setSelectedId(null);
            }}
            ariaLabel="Attention views"
            panelId="attention-panel"
            className="mt-3 grid-cols-4 border border-border bg-muted/30 p-1"
          />
          <div className="mt-3 flex flex-col gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
              <Input
                value={filters.query}
                onChange={(event) => setFilters((f) => ({ ...f, query: event.target.value }))}
                placeholder={t("attention.searchPlaceholder")}
                aria-label={t("attention.searchLabel")}
                className="pl-8"
              />
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
                {t("attention.kind")}
                <select
                  value={filters.kind}
                  onChange={(event) => setFilters((f) => ({ ...f, kind: event.target.value as AttentionFilters["kind"] }))}
                  aria-label={t("attention.filterKind")}
                  className="rounded-md border border-border bg-background px-1.5 py-1 text-[12px] text-foreground"
                >
                  <option value="all">{t("common.all")}</option>
                  {ATTENTION_KINDS.map((kind) => (
                    <option key={kind} value={kind}>{ATTENTION_KIND_LABELS[kind]}</option>
                  ))}
                </select>
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
                {t("attention.severity")}
                <select
                  value={filters.severity}
                  onChange={(event) => setFilters((f) => ({ ...f, severity: event.target.value as AttentionFilters["severity"] }))}
                  aria-label={t("attention.filterSeverity")}
                  className="rounded-md border border-border bg-background px-1.5 py-1 text-[12px] text-foreground"
                >
                  <option value="all">{t("common.all")}</option>
                  <option value="critical">Critical</option>
                  <option value="high">High</option>
                  <option value="warning">Warning</option>
                  <option value="info">Info</option>
                </select>
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
                {t("attention.source")}
                <select
                  value={filters.source}
                  onChange={(event) => setFilters((f) => ({ ...f, source: event.target.value as AttentionFilters["source"] }))}
                  aria-label={t("attention.filterSource")}
                  className="rounded-md border border-border bg-background px-1.5 py-1 text-[12px] text-foreground"
                >
                  <option value="all">{t("common.all")}</option>
                  {(Object.keys(ATTENTION_SOURCE_LABELS) as AttentionSource[]).map((source) => (
                    <option key={source} value={source}>{ATTENTION_SOURCE_LABELS[source]}</option>
                  ))}
                </select>
              </label>
              {(filters.query || filters.kind !== "all" || filters.severity !== "all" || filters.source !== "all") && (
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => setFilters((f) => ({ ...f, query: "", kind: "all", severity: "all", source: "all" }))}
                >
                  {t("common.clear")}
                </Button>
              )}
            </div>
          </div>
        </div>

        <div id="attention-panel" role="tabpanel" aria-label={t("attention.detailsFor", { title: tabLabel(filters.tab) })} className="min-h-0 flex-1 overflow-y-auto px-4 py-3 sm:px-6">
          {visible.length === 0 ? (
            <EmptyState tab={filters.tab} hasItems={items.length > 0} />
          ) : (
            <ul className="flex flex-col gap-2 pb-4">
              {visible.map((item) => (
                <li key={item.id}>
                  <AttentionRow
                    item={item}
                    selected={selectedId === item.id}
                    onSelect={() => setSelectedId(item.id)}
                  />
                </li>
              ))}
            </ul>
          )}
          {truncated && (
            <p className="pb-4 text-center text-[11px] text-muted-foreground">
              Showing the first {ATTENTION_RESULT_LIMIT} matches.
            </p>
          )}
        </div>
      </div>

      {/* Inspector column: inline pane on desktop, overlay drawer on mobile */}
      {selected && (
        <>
          <div
            className="fixed inset-0 z-40 bg-black/40 lg:hidden"
            onClick={() => setSelectedId(null)}
            aria-hidden="true"
          />
          <aside
            aria-label={t("attention.detailsFor", { title: selected.title })}
            className="fixed inset-y-0 right-0 z-50 flex w-full max-w-none flex-col border-l border-border bg-background sm:w-[480px] sm:max-w-[90vw] lg:static lg:z-auto lg:flex lg:min-h-0 lg:w-[420px] lg:max-w-none lg:shrink-0"
          >
            <AttentionInspector
              item={selected}
              onNavigate={onNavigate}
              onClose={() => setSelectedId(null)}
              onDecide={handleDecide}
              onAcknowledge={handleAcknowledge}
              onResolve={handleResolve}
            />
          </aside>
        </>
      )}
    </div>
  );
}

function SummaryStrip({ summary }: { summary: ReturnType<typeof selectAttentionSummary> }) {
  const metrics = [
    { label: "Needs action", value: summary.needsAction },
    { label: "Awaiting approval", value: summary.awaitingApproval },
    { label: "Blocked", value: summary.blocked },
    { label: "Critical", value: summary.critical, alert: summary.critical > 0 },
    { label: "High", value: summary.high },
  ];
  return (
    <dl className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border border-border bg-card px-3 py-2">
      {metrics.map((metric) => (
        <div key={metric.label} className="flex items-baseline gap-1.5">
          <dd className={cn("text-[15px] font-semibold tabular-nums", metric.alert ? "text-red-600 dark:text-red-400" : "text-foreground")}>
            {metric.value}
          </dd>
          <dt className="text-[11px] text-muted-foreground">{metric.label}</dt>
        </div>
      ))}
    </dl>
  );
}

function AttentionRow({ item, selected, onSelect }: { item: AttentionItem; selected: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-current={selected ? "true" : undefined}
      aria-label={`${item.title} · ${ATTENTION_KIND_LABELS[item.kind]} · ${ATTENTION_SEVERITY_LABELS[item.severity]} · ${ATTENTION_STATUS_LABELS[item.status]}`}
      className={cn(
        "flex w-full items-start gap-3 rounded-lg border p-3 text-left transition-colors",
        selected
          ? "border-foreground/30 bg-muted/50"
          : "border-border bg-card hover:bg-muted/30",
      )}
    >
      <StatusDot tone={SEVERITY_TONE[item.severity]} size="md" className="mt-1.5" />
      <span className="min-w-0 flex-1">
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="text-[13px] font-semibold">{item.title}</span>
          <Pill tone={KIND_TONE[item.kind]}>{ATTENTION_KIND_LABELS[item.kind]}</Pill>
          <span className={cn("text-[10px] font-semibold uppercase", TEXT_TONE[SEVERITY_TONE[item.severity]])}>
            {ATTENTION_SEVERITY_LABELS[item.severity]}
          </span>
        </span>
        <span className="mt-0.5 block truncate text-[12px] text-muted-foreground">{item.summary}</span>
        <span className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] text-muted-foreground">
          <span className="inline-flex items-center gap-1">
            <Clock3 className="size-3" aria-hidden="true" />{item.createdAt}
          </span>
          {item.jobTitle && <span className="truncate">· {item.jobTitle}</span>}
          <span>· {ATTENTION_STATUS_LABELS[item.status]}</span>
        </span>
      </span>
      <ArrowRight className="mt-1 size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
    </button>
  );
}

function EmptyState({ tab, hasItems }: { tab: AttentionTab; hasItems: boolean }) {
  const { t } = useI18n();
  if (!hasItems) {
    return (
      <EmptyPanel
        icon={ShieldCheck}
        iconClassName="text-emerald-500"
        title={t("attention.allClear")}
        hint={t("attention.allClearBody")}
        className="px-6 py-12"
      />
    );
  }
  return (
    <EmptyPanel
      icon={Inbox}
      title={tab === "resolved" ? t("attention.emptyResolved") : t("attention.emptyFiltered", { tab: t(TAB_LABEL_KEY[tab]) })}
      hint={t("attention.emptyHint")}
      className="px-6 py-12"
    />
  );
}

function AttentionInspector({
  item,
  onNavigate,
  onClose,
  onDecide,
  onAcknowledge,
  onResolve,
}: {
  item: AttentionItem;
  onNavigate: (view: WorkspaceView) => void;
  onSelectSession?: (sessionId: string) => void;
  onClose: () => void;
  onDecide: (id: string, decision: "approved" | "rejected") => void;
  onAcknowledge: (id: string) => void;
  onResolve: (id: string) => void;
}) {
  const openDestination = () => onNavigate(DESTINATION_VIEWS[item.destination]);
  const unresolved = isUnresolved(item);
  const approval = item.approval;
  const { t } = useI18n();

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex shrink-0 items-start gap-2 border-b border-border px-4 py-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <Pill tone={KIND_TONE[item.kind]}>{ATTENTION_KIND_LABELS[item.kind]}</Pill>
            <span className={cn("text-[10px] font-semibold uppercase", TEXT_TONE[SEVERITY_TONE[item.severity]])}>
              {ATTENTION_SEVERITY_LABELS[item.severity]}
            </span>
            <span className="rounded-md bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
              {ATTENTION_STATUS_LABELS[item.status]}
            </span>
          </div>
          <h2 className="mt-1 text-[15px] font-semibold tracking-tight">{item.title}</h2>
          <p className="mt-0.5 text-[11px] text-muted-foreground">
            {ATTENTION_SOURCE_LABELS[item.source]} · created {item.createdAt} · updated {item.updatedAt}
          </p>
        </div>
        <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label={t("attention.closeDetails")}>
          <X className="size-4" />
        </Button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-4 py-3">
        <div className="flex flex-col gap-3 text-[13px]">
          <InspectorAnswer question="What happened?" answer={item.whatHappened} />
          <InspectorAnswer question="Why does OCG need me?" answer={item.whyNeeded} />
          <InspectorAffected item={item} />
          {approval && <ApprovalDetail approval={approval} />}
          {item.blocked && <BlockedDetail item={item} />}
          <InspectorAnswer question="What happens if I do nothing?" answer={item.inactionConsequence} />
          {item.resolution && (
            <section aria-label="Resolution" className="rounded-lg border border-border bg-muted/30 p-3">
              <h3 className="text-[12px] font-semibold">Resolution</h3>
              <p className="mt-1 text-[12px] text-muted-foreground">
                {ATTENTION_STATUS_LABELS[item.resolution.outcome]} · {item.resolution.at}
                {item.resolution.note ? ` — ${item.resolution.note}` : ""}
              </p>
            </section>
          )}
        </div>
      </div>

      <div className="shrink-0 border-t border-border px-4 py-3">
        {approval && approval.decision === "pending" ? (
          <div className="flex items-center gap-2">
            <Button
              variant="default"
              size="sm"
              className="flex-1"
              onClick={() => onDecide(item.id, "approved")}
              aria-label={`Approve: ${approval.requestedAction}`}
            >
              <Check className="size-3.5" data-icon="inline-start" /> {t("attention.approve")}
            </Button>
            <Button
              variant="destructive"
              size="sm"
              className="flex-1"
              onClick={() => onDecide(item.id, "rejected")}
              aria-label={`Reject: ${approval.requestedAction}`}
            >
              <X className="size-3.5" data-icon="inline-start" /> {t("attention.reject")}
            </Button>
          </div>
        ) : (
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="outline" size="sm" onClick={openDestination}>
              {t("common.open")} {DESTINATION_LABELS[item.destination]} <ArrowRight className="size-3.5" data-icon="inline-end" />
            </Button>
            {unresolved && item.status === "pending" && (
              <Button variant="ghost" size="sm" onClick={() => onAcknowledge(item.id)} aria-label={`Acknowledge ${item.title}`}>
                {t("attention.acknowledge")}
              </Button>
            )}
            {unresolved && !approval && (
              <Button variant="ghost" size="sm" onClick={() => onResolve(item.id)} aria-label={t("attention.detailsFor", { title: item.title })}>
                <CircleCheck className="size-3.5" data-icon="inline-start" /> {t("attention.markResolved")}
              </Button>
            )}
            {!unresolved && (
              <span className="inline-flex items-center gap-1 text-[12px] text-muted-foreground">
                <ShieldAlert className="size-3.5" aria-hidden="true" />
                {ATTENTION_STATUS_LABELS[item.status]} — no further action
              </span>
            )}
          </div>
        )}
        {approval && approval.decision === "pending" && (
          <p className="mt-2 text-[11px] text-muted-foreground">
            Decision applies to this workspace view only; nothing is persisted or sent anywhere.
          </p>
        )}
      </div>
    </div>
  );
}

function InspectorAnswer({ question, answer }: { question: string; answer: string }) {
  return (
    <section aria-label={question}>
      <h3 className="text-[12px] font-semibold">{question}</h3>
      <p className="mt-1 text-[12px] leading-relaxed text-muted-foreground">{answer}</p>
    </section>
  );
}

function InspectorAffected({ item }: { item: AttentionItem }) {
  const rows: Array<[string, string]> = [];
  if (item.jobTitle) rows.push(["Job", item.jobTitle]);
  if (item.taskTitle) rows.push(["Task", item.taskTitle]);
  if (item.providerLabel) rows.push(["Provider", item.providerLabel + (item.model ? ` · ${item.model}` : "")]);
  if (item.blocked?.workerLabel) rows.push(["Worker", item.blocked.workerLabel]);
  if (rows.length === 0) return null;
  return (
    <section aria-label="What is affected?">
      <h3 className="text-[12px] font-semibold">What is affected?</h3>
      <dl className="mt-1 rounded-lg border border-border bg-muted/20 px-3 py-2">
        {rows.map(([label, value]) => (
          <div key={label} className="flex items-baseline justify-between gap-3 py-0.5 text-[12px]">
            <dt className="shrink-0 text-muted-foreground">{label}</dt>
            <dd className="min-w-0 truncate text-right font-medium">{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

function ApprovalDetail({ approval }: { approval: NonNullable<AttentionItem["approval"]> }) {
  return (
    <section aria-label="Approval request" className="rounded-lg border border-border p-3">
      <h3 className="text-[12px] font-semibold">
        Approval · {APPROVAL_TYPE_LABELS[approval.type]}
      </h3>
      <dl className="mt-2 flex flex-col gap-1.5 text-[12px]">
        <DetailRow label="Requested" value={approval.requestedAction} />
        <DetailRow label="Reason" value={approval.reason} />
        <DetailRow label="Requester" value={`${approval.requester} · ${approval.requestedAt}`} />
        {approval.expiresAt && <DetailRow label="Expires" value={approval.expiresAt} />}
        {approval.estimatedImpact && <DetailRow label="Impact" value={approval.estimatedImpact} />}
        {approval.requestedSpendMicros !== undefined && (
          <DetailRow label="Spend" value={`$${(approval.requestedSpendMicros / 1_000_000).toFixed(2)}`} />
        )}
        {(approval.requestedProvider || approval.requestedModel) && (
          <DetailRow label="Resource" value={[approval.requestedProvider, approval.requestedModel].filter(Boolean).join(" · ")} />
        )}
        <DetailRow label="If approved" value={approval.approveConsequence} />
        <DetailRow label="If rejected" value={approval.rejectConsequence} />
      </dl>
    </section>
  );
}

function BlockedDetail({ item }: { item: AttentionItem }) {
  const blocked = item.blocked;
  if (!blocked) return null;
  return (
    <section aria-label="Blocked context" className="rounded-lg border border-border p-3">
      <h3 className="text-[12px] font-semibold">Blocked — not an approval</h3>
      <dl className="mt-2 flex flex-col gap-1.5 text-[12px]">
        <DetailRow label="Reason" value={blocked.reason} />
        <DetailRow label="Unblocks when" value={blocked.unblocksWhen} />
      </dl>
    </section>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-3">
      <dt className="shrink-0 text-muted-foreground">{label}</dt>
      <dd className="min-w-0 text-right font-medium">{value}</dd>
    </div>
  );
}
