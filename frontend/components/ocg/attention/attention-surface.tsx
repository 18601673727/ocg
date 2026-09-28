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

export type AttentionNavigate = {
  onOpenChat: () => void;
  onOpenMissionControl: () => void;
  onOpenControlCenter: () => void;
  onOpenLedger: () => void;
  onOpenLogs: () => void;
  onOpenSettings: () => void;
};

type AttentionSurfaceProps = {
  snapshot: RuntimeSnapshot;
  initialTab?: AttentionTab;
  /**
   * Optional project-scoped fixture queue. When omitted the scenario queue is
   * used, preserving the standalone behavior.
   */
  queue?: AttentionQueue;
} & AttentionNavigate;

const TAB_LABELS: Record<AttentionTab, string> = {
  overview: "Overview",
  approvals: "Approvals",
  blocked: "Blocked",
  resolved: "Resolved",
};

const SEVERITY_DOT: Record<AttentionSeverity, string> = {
  info: "bg-sky-500",
  warning: "bg-amber-500",
  high: "bg-orange-500",
  critical: "bg-red-500",
};

const SEVERITY_TEXT: Record<AttentionSeverity, string> = {
  info: "text-sky-700 dark:text-sky-400",
  warning: "text-amber-700 dark:text-amber-400",
  high: "text-orange-700 dark:text-orange-400",
  critical: "text-red-700 dark:text-red-400",
};

const KIND_TONE: Record<AttentionKind, string> = {
  approval: "bg-violet-500/10 text-violet-700 dark:text-violet-400",
  budget: "bg-amber-500/10 text-amber-700 dark:text-amber-400",
  policy: "bg-slate-500/10 text-slate-600 dark:text-slate-300",
  permission: "bg-sky-500/10 text-sky-700 dark:text-sky-400",
  blocked: "bg-orange-500/10 text-orange-700 dark:text-orange-400",
  "runtime-failure": "bg-red-500/10 text-red-700 dark:text-red-400",
  "resource-degraded": "bg-amber-500/10 text-amber-700 dark:text-amber-400",
  configuration: "bg-slate-500/10 text-slate-600 dark:text-slate-300",
  retry: "bg-sky-500/10 text-sky-700 dark:text-sky-400",
  escalation: "bg-violet-500/10 text-violet-700 dark:text-violet-400",
};

const DESTINATION_LABELS: Record<AttentionDestination, string> = {
  "mission-control": "Mission Control",
  "control-center": "Control Center",
  "resource-ledger": "Resource Ledger",
  logs: "Logs",
  settings: "Settings",
  chat: "Chat",
};

function destinationAction(destination: AttentionDestination, navigate: AttentionNavigate): () => void {
  switch (destination) {
    case "mission-control": return navigate.onOpenMissionControl;
    case "control-center": return navigate.onOpenControlCenter;
    case "resource-ledger": return navigate.onOpenLedger;
    case "logs": return navigate.onOpenLogs;
    case "settings": return navigate.onOpenSettings;
    case "chat": return navigate.onOpenChat;
  }
}

/** Local fixture decisions use a fixed clock so UI state stays deterministic. */
const DECISION_CLOCK = "2026-09-25T10:00:00Z";

export function AttentionSurface(props: AttentionSurfaceProps) {
  const { snapshot, initialTab = "overview", queue: queueProp } = props;
  const navigate: AttentionNavigate = {
    onOpenChat: props.onOpenChat,
    onOpenMissionControl: props.onOpenMissionControl,
    onOpenControlCenter: props.onOpenControlCenter,
    onOpenLedger: props.onOpenLedger,
    onOpenLogs: props.onOpenLogs,
    onOpenSettings: props.onOpenSettings,
  };

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
      <div className="flex min-h-0 w-full min-w-0 flex-1 flex-col lg:max-w-[560px] lg:border-r lg:border-border">
        <div className="shrink-0 px-4 pt-4 sm:px-6 sm:pt-6">
          <h1 className="text-[20px] font-semibold tracking-tight">Attention</h1>
          <p className="mt-0.5 text-[13px] text-muted-foreground">
            Items that need your decision or intervention.
          </p>
          <SummaryStrip summary={summary} />
          <div
            role="tablist"
            aria-label="Attention views"
            className="mt-3 flex items-center gap-1 rounded-lg border border-border bg-muted/30 p-1"
          >
            {ATTENTION_TABS.map((tab) => {
              const active = filters.tab === tab;
              return (
                <button
                  key={tab}
                  type="button"
                  role="tab"
                  aria-selected={active}
                  aria-controls="attention-panel"
                  onClick={() => { setFilters((f) => ({ ...f, tab })); setSelectedId(null); }}
                  className={cn(
                    "flex min-w-0 flex-1 items-center justify-center gap-1.5 rounded-md px-2 py-1.5 text-[12px] font-medium transition-colors",
                    active ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground",
                  )}
                >
                  <span className="truncate">{TAB_LABELS[tab]}</span>
                  <span className={cn(
                    "rounded-full px-1.5 text-[10px] tabular-nums",
                    active ? "bg-muted text-foreground" : "bg-muted/60 text-muted-foreground",
                  )}>
                    {tabbed[tab]}
                  </span>
                </button>
              );
            })}
          </div>
          <div className="mt-3 flex flex-col gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden="true" />
              <Input
                value={filters.query}
                onChange={(event) => setFilters((f) => ({ ...f, query: event.target.value }))}
                placeholder="Search title, Mission, provider…"
                aria-label="Search attention items"
                className="pl-8"
              />
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
                Kind
                <select
                  value={filters.kind}
                  onChange={(event) => setFilters((f) => ({ ...f, kind: event.target.value as AttentionFilters["kind"] }))}
                  aria-label="Filter by kind"
                  className="rounded-md border border-border bg-background px-1.5 py-1 text-[12px] text-foreground"
                >
                  <option value="all">All</option>
                  {ATTENTION_KINDS.map((kind) => (
                    <option key={kind} value={kind}>{ATTENTION_KIND_LABELS[kind]}</option>
                  ))}
                </select>
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
                Severity
                <select
                  value={filters.severity}
                  onChange={(event) => setFilters((f) => ({ ...f, severity: event.target.value as AttentionFilters["severity"] }))}
                  aria-label="Filter by severity"
                  className="rounded-md border border-border bg-background px-1.5 py-1 text-[12px] text-foreground"
                >
                  <option value="all">All</option>
                  <option value="critical">Critical</option>
                  <option value="high">High</option>
                  <option value="warning">Warning</option>
                  <option value="info">Info</option>
                </select>
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
                Source
                <select
                  value={filters.source}
                  onChange={(event) => setFilters((f) => ({ ...f, source: event.target.value as AttentionFilters["source"] }))}
                  aria-label="Filter by source"
                  className="rounded-md border border-border bg-background px-1.5 py-1 text-[12px] text-foreground"
                >
                  <option value="all">All</option>
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
                  Clear
                </Button>
              )}
            </div>
          </div>
        </div>

        <div id="attention-panel" role="tabpanel" aria-label={`${TAB_LABELS[filters.tab]} items`} className="min-h-0 flex-1 overflow-y-auto px-4 py-3 sm:px-6">
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
            aria-label={`Details for ${selected.title}`}
            className="fixed inset-y-0 right-0 z-50 flex w-full max-w-none flex-col border-l border-border bg-background sm:w-[480px] sm:max-w-[90vw] lg:static lg:z-auto lg:flex lg:min-h-0 lg:w-[420px] lg:max-w-none lg:shrink-0"
          >
            <AttentionInspector
              item={selected}
              navigate={navigate}
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
      <span className={cn("mt-1.5 size-2 shrink-0 rounded-full", SEVERITY_DOT[item.severity])} aria-hidden="true" />
      <span className="min-w-0 flex-1">
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="text-[13px] font-semibold">{item.title}</span>
          <span className={cn("rounded-full px-1.5 py-0.5 text-[10px] font-medium", KIND_TONE[item.kind])}>
            {ATTENTION_KIND_LABELS[item.kind]}
          </span>
          <span className={cn("text-[10px] font-semibold uppercase", SEVERITY_TEXT[item.severity])}>
            {ATTENTION_SEVERITY_LABELS[item.severity]}
          </span>
        </span>
        <span className="mt-0.5 block truncate text-[12px] text-muted-foreground">{item.summary}</span>
        <span className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] text-muted-foreground">
          <span className="inline-flex items-center gap-1">
            <Clock3 className="size-3" aria-hidden="true" />{item.createdAt}
          </span>
          {item.missionTitle && <span className="truncate">· {item.missionTitle}</span>}
          <span>· {ATTENTION_STATUS_LABELS[item.status]}</span>
        </span>
      </span>
      <ArrowRight className="mt-1 size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
    </button>
  );
}

function EmptyState({ tab, hasItems }: { tab: AttentionTab; hasItems: boolean }) {
  if (!hasItems) {
    return (
      <div className="flex flex-col items-center justify-center rounded-lg border border-dashed border-border bg-muted/10 px-6 py-12 text-center">
        <ShieldCheck className="mb-2 size-6 text-emerald-500" aria-hidden="true" />
        <p className="text-sm font-medium">All clear</p>
        <p className="mt-1 max-w-[280px] text-[12px] text-muted-foreground">
          OCG has progressed as far as it safely can and nothing needs your judgment right now.
        </p>
      </div>
    );
  }
  return (
    <div className="flex flex-col items-center justify-center rounded-lg border border-dashed border-border bg-muted/10 px-6 py-12 text-center">
      <Inbox className="mb-2 size-6 text-muted-foreground" aria-hidden="true" />
      <p className="text-sm font-medium">
        {tab === "resolved" ? "No resolved history yet" : `Nothing in ${TAB_LABELS[tab].toLowerCase()}`}
      </p>
      <p className="mt-1 max-w-[280px] text-[12px] text-muted-foreground">
        Try a different tab or clear the filters.
      </p>
    </div>
  );
}

function AttentionInspector({
  item,
  navigate,
  onClose,
  onDecide,
  onAcknowledge,
  onResolve,
}: {
  item: AttentionItem;
  navigate: AttentionNavigate;
  onClose: () => void;
  onDecide: (id: string, decision: "approved" | "rejected") => void;
  onAcknowledge: (id: string) => void;
  onResolve: (id: string) => void;
}) {
  const openDestination = destinationAction(item.destination, navigate);
  const unresolved = isUnresolved(item);
  const approval = item.approval;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex shrink-0 items-start gap-2 border-b border-border px-4 py-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className={cn("rounded-full px-1.5 py-0.5 text-[10px] font-medium", KIND_TONE[item.kind])}>
              {ATTENTION_KIND_LABELS[item.kind]}
            </span>
            <span className={cn("text-[10px] font-semibold uppercase", SEVERITY_TEXT[item.severity])}>
              {ATTENTION_SEVERITY_LABELS[item.severity]}
            </span>
            <span className="rounded-full bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
              {ATTENTION_STATUS_LABELS[item.status]}
            </span>
          </div>
          <h2 className="mt-1 text-[15px] font-semibold tracking-tight">{item.title}</h2>
          <p className="mt-0.5 text-[11px] text-muted-foreground">
            {ATTENTION_SOURCE_LABELS[item.source]} · created {item.createdAt} · updated {item.updatedAt}
          </p>
        </div>
        <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label="Close details">
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
              <Check className="size-3.5" data-icon="inline-start" /> Approve
            </Button>
            <Button
              variant="destructive"
              size="sm"
              className="flex-1"
              onClick={() => onDecide(item.id, "rejected")}
              aria-label={`Reject: ${approval.requestedAction}`}
            >
              <X className="size-3.5" data-icon="inline-start" /> Reject
            </Button>
          </div>
        ) : (
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="outline" size="sm" onClick={openDestination}>
              Open {DESTINATION_LABELS[item.destination]} <ArrowRight className="size-3.5" data-icon="inline-end" />
            </Button>
            {unresolved && item.status === "pending" && (
              <Button variant="ghost" size="sm" onClick={() => onAcknowledge(item.id)} aria-label={`Acknowledge ${item.title}`}>
                Acknowledge
              </Button>
            )}
            {unresolved && !approval && (
              <Button variant="ghost" size="sm" onClick={() => onResolve(item.id)} aria-label={`Mark resolved: ${item.title}`}>
                <CircleCheck className="size-3.5" data-icon="inline-start" /> Mark resolved
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
  if (item.missionTitle) rows.push(["Mission", item.missionTitle]);
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
