"use client";

import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { formatTimestamp } from "@/lib/format";
import { EmptyState, JOB_STATE, KeyValue, KeyValueList, Panel, Pill, SectionTitle } from "../primitives";
import { recoveryLabel, runtimeStateLabel, useI18n, type TranslateFn } from "../i18n";
import type { CanonicalJobState, ChildPolicy, JobOrigin } from "../contracts";
import { jobTreeOf, type JobExecution, type JobTreeNode } from "./domain";

/**
 * One canonical Job id, rendered as a navigation control when the surface
 * can navigate and as plain text otherwise. Ids are shown in full — wrapping,
 * never truncating into horizontal scroll — because they are the one fact a
 * reader can always trust.
 */
function JobLink({ jobId, onSelectJob, children }: { jobId: string; onSelectJob?: (jobId: string) => void; children?: ReactNode }) {
  if (!onSelectJob) {
    return <span className="break-all font-mono text-[11px]">{children ?? jobId}</span>;
  }
  return (
    <Button
      size="xs"
      variant="outline"
      className="h-auto max-w-full gap-1.5 whitespace-normal break-all text-left font-mono text-[11px]"
      onClick={() => onSelectJob(jobId)}
    >
      {children ?? jobId}
    </Button>
  );
}

const JOIN_KEY = { required: "relations.joinRequired", not_required: "relations.joinNotRequired" } as const;
const CANCELLATION_KEY = { cascade: "relations.cancellationCascade", independent: "relations.cancellationIndependent" } as const;
const FAILURE_KEY = {
  observe: "relations.failureObserve",
  block_parent: "relations.failureBlockParent",
  fail_parent: "relations.failureFailParent",
} as const;

function ChildPolicyFacts({ policy, t }: { policy: ChildPolicy; t: TranslateFn }) {
  return (
    <p className="text-[11px] text-muted-foreground">
      {t(JOIN_KEY[policy.join])} · {t(CANCELLATION_KEY[policy.cancellation])} · {t(FAILURE_KEY[policy.failure])}
    </p>
  );
}

/** The durable origin facts the backend recorded for a spawned Job. */
function OriginFacts({ origin, t }: { origin: JobOrigin; t: TranslateFn }) {
  return (
    <KeyValueList className="mt-2">
      <KeyValue label={t("relations.spawnAttempt")} mono>
        {origin.attempt_id} · {t("relations.spawnGeneration", { generation: String(origin.generation) })}
      </KeyValue>
      {origin.spawn_key && <KeyValue label={t("relations.spawnKey")} mono>{origin.spawn_key}</KeyValue>}
      {origin.call_id && <KeyValue label={t("relations.spawnCallId")} mono>{origin.call_id}</KeyValue>}
      {origin.policy && (
        <KeyValue label={t("relations.spawnPolicy")}>
          <ChildPolicyFacts policy={origin.policy} t={t} />
        </KeyValue>
      )}
    </KeyValueList>
  );
}

function JobChip({ jobId, state, onSelectJob, t }: {
  jobId: string;
  /** `undefined` when this Job has not been loaded into this Project's view. */
  state: CanonicalJobState | undefined;
  onSelectJob?: (jobId: string) => void;
  t: TranslateFn;
}) {
  return (
    <JobLink jobId={jobId} onSelectJob={onSelectJob}>
      <span className="max-w-48 truncate">{jobId}</span>
      {state ? (
        <Pill tone={JOB_STATE[state].tone} dot pulse={JOB_STATE[state].pulse}>{runtimeStateLabel(t, state)}</Pill>
      ) : (
        <Pill tone="slate">{t("relations.notLoaded")}</Pill>
      )}
    </JobLink>
  );
}

/**
 * The selected Job's direct relationships: Parent/Origin, Children,
 * Prerequisites, and Dependents. Each stays its own panel because the
 * backend keeps them as distinct relations — merging them would lose that
 * distinction.
 */
export function JobRelationsPanel({ execution, executions, onSelectJob }: {
  execution: JobExecution;
  executions: readonly JobExecution[];
  onSelectJob?: (jobId: string) => void;
}) {
  const { t } = useI18n();
  const stateById = new Map(executions.map((item) => [item.jobId, item.state] as const));
  stateById.set(execution.jobId, execution.state);

  const hasTreeContext = execution.parentJobId !== null || execution.childJobIds.length > 0 || execution.depth > 0;
  const hasRecursionContext = hasTreeContext || execution.waitingForChildren || execution.recursiveLimits !== undefined;

  return (
    <section aria-label={t("relations.title")} className="space-y-3">
      <SectionTitle>{t("relations.title")}</SectionTitle>

      <Panel title={t("relations.parent")}>
        {execution.parentJobId === null ? (
          <EmptyState compact>{t("relations.noParent")}</EmptyState>
        ) : (
          <div>
            <JobChip jobId={execution.parentJobId} state={stateById.get(execution.parentJobId)} onSelectJob={onSelectJob} t={t} />
            {execution.origin && <OriginFacts origin={execution.origin} t={t} />}
          </div>
        )}
      </Panel>

      <Panel title={t("relations.children")} detail={execution.childJobIds.length > 0 ? String(execution.childJobIds.length) : undefined}>
        {execution.childJobIds.length === 0 ? (
          <EmptyState compact>{t("relations.noChildren")}</EmptyState>
        ) : (
          <ul className="flex flex-wrap gap-2">
            {execution.childJobIds.map((childId) => (
              <li key={childId}><JobChip jobId={childId} state={stateById.get(childId)} onSelectJob={onSelectJob} t={t} /></li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel title={t("relations.prerequisites")} detail={execution.dependsOn.length > 0 ? String(execution.dependsOn.length) : undefined}>
        {execution.dependsOn.length === 0 ? (
          <EmptyState compact>{t("relations.noPrerequisites")}</EmptyState>
        ) : (
          <div className="space-y-2">
            {execution.blockedBy.length > 0 && (
              <p className="text-[11px] text-muted-foreground">{t("relations.blockedNotice", { count: String(execution.blockedBy.length) })}</p>
            )}
            <ul className="flex flex-wrap gap-2">
              {execution.dependsOn.map((depId) => (
                <li key={depId}>
                  <JobLink jobId={depId} onSelectJob={onSelectJob}>
                    <span className="max-w-48 truncate">{depId}</span>
                    {stateById.has(depId) && <Pill tone={JOB_STATE[stateById.get(depId)!].tone}>{runtimeStateLabel(t, stateById.get(depId)!)}</Pill>}
                    <Pill tone={execution.blockedBy.includes(depId) ? "amber" : "emerald"}>
                      {execution.blockedBy.includes(depId) ? t("relations.waitingOn") : t("relations.satisfied")}
                    </Pill>
                  </JobLink>
                </li>
              ))}
            </ul>
          </div>
        )}
      </Panel>

      <Panel title={t("relations.dependents")} detail={execution.blocks.length > 0 ? String(execution.blocks.length) : undefined}>
        {execution.blocks.length === 0 ? (
          <EmptyState compact>{t("relations.noDependents")}</EmptyState>
        ) : (
          <div className="space-y-2">
            {execution.state !== "completed" && (
              <p className="text-[11px] text-muted-foreground">{t("relations.dependentsNotice", { count: String(execution.blocks.length) })}</p>
            )}
            <ul className="flex flex-wrap gap-2">
              {execution.blocks.map((depId) => (
                <li key={depId}><JobChip jobId={depId} state={stateById.get(depId)} onSelectJob={onSelectJob} t={t} /></li>
              ))}
            </ul>
          </div>
        )}
      </Panel>

      {hasRecursionContext && <RecursionFacts execution={execution} t={t} />}
      {hasTreeContext && <JobTreePanel execution={execution} executions={executions} onSelectJob={onSelectJob} />}
    </section>
  );
}

function RecursionFacts({ execution, t }: { execution: JobExecution; t: TranslateFn }) {
  const limits = execution.recursiveLimits;
  return (
    <Panel title={t("recursion.title")}>
      <KeyValueList>
        <KeyValue label={t("recursion.depth")}>{limits ? `${execution.depth} / ${limits.max_depth}` : execution.depth}</KeyValue>
        <KeyValue label={t("recursion.descendants")}>
          {limits ? `${execution.descendantJobIds.length} / ${limits.max_total_descendants_per_root}` : execution.descendantJobIds.length}
        </KeyValue>
        {limits && <KeyValue label={t("recursion.maxChildren")}>{limits.max_children_per_job}</KeyValue>}
      </KeyValueList>
      {!limits && <p className="mt-2 text-[11px] text-muted-foreground">{t("recursion.limitsNotRecorded")}</p>}
      {execution.waitingForChildren && (
        <p className="mt-2"><Pill tone="amber" dot pulse>{t("recursion.waitingForChildren")}</Pill></p>
      )}
    </Panel>
  );
}

function JobTreeRow({ node, depth, onSelectJob, t }: {
  node: JobTreeNode;
  depth: number;
  onSelectJob?: (jobId: string) => void;
  t: TranslateFn;
}) {
  return (
    <li>
      <div className="flex min-w-0 flex-wrap items-center gap-1.5 py-0.5" style={{ paddingLeft: `${depth * 14}px` }}>
        {depth > 0 && <span aria-hidden className="text-muted-foreground">└─</span>}
        {node.loaded ? (
          <JobLink jobId={node.jobId} onSelectJob={onSelectJob}><span className="max-w-48 truncate">{node.jobId}</span></JobLink>
        ) : (
          <span className="break-all font-mono text-[11px] text-muted-foreground">{node.jobId}</span>
        )}
        {node.state && <Pill tone={JOB_STATE[node.state].tone} dot pulse={JOB_STATE[node.state].pulse}>{runtimeStateLabel(t, node.state)}</Pill>}
        {!node.loaded && <Pill tone="slate">{t("relations.notLoaded")}</Pill>}
        {node.isCurrent && <Pill tone="violet" dot>{t("tree.current")}</Pill>}
      </div>
      {node.children.length > 0 && (
        <ul>{node.children.map((child) => <JobTreeRow key={child.jobId} node={child} depth={depth + 1} onSelectJob={onSelectJob} t={t} />)}</ul>
      )}
    </li>
  );
}

/**
 * A compact ownership/spawn tree, bounded to the Project Jobs this view has
 * already loaded. It never issues a request of its own: an ancestor or
 * descendant outside `executions` renders as an unresolved stub.
 */
function JobTreePanel({ execution, executions, onSelectJob }: {
  execution: JobExecution;
  executions: readonly JobExecution[];
  onSelectJob?: (jobId: string) => void;
}) {
  const { t } = useI18n();
  const projection = jobTreeOf(execution, executions);
  const summaryEntries = Object.entries(execution.descendantSummary);
  return (
    <Panel
      title={t("tree.title")}
      detail={execution.descendantJobIds.length > 0 ? t("tree.descendantsRecorded", { count: String(execution.descendantJobIds.length) }) : undefined}
    >
      {summaryEntries.length > 0 && (
        <ul className="mb-2 flex flex-wrap gap-1.5">
          {summaryEntries.map(([state, count]) => (
            <li key={state}>
              <Pill tone={(JOB_STATE as Record<string, { tone: "emerald" | "sky" | "amber" | "red" | "violet" | "slate" }>)[state]?.tone ?? "slate"}>
                {runtimeStateLabel(t, state)} · {count}
              </Pill>
            </li>
          ))}
        </ul>
      )}
      {!projection.reachedRoot && <p className="mb-2 text-[11px] text-muted-foreground">{t("tree.rootNotLoaded")}</p>}
      {projection.cyclic && <p role="alert" className="mb-2 text-[11px] text-destructive">{t("tree.cyclic")}</p>}
      <ul>
        <JobTreeRow node={projection.root} depth={0} onSelectJob={onSelectJob} t={t} />
      </ul>
    </Panel>
  );
}

/**
 * Watchdog recovery evidence for this Job's Attempts. The Job stays the same
 * Job across every entry here — recovery replaces or fences an Attempt, it
 * never spawns a Job of its own.
 */
export function JobRecoveryPanel({ execution }: { execution: JobExecution }) {
  const { t } = useI18n();
  return (
    <section aria-label={t("recovery.title")} className="rounded-lg border border-border bg-card p-4">
      <SectionTitle>{t("recovery.title")}</SectionTitle>
      {execution.recovery.length === 0 ? (
        <EmptyState className="mt-3">{t("recovery.empty")}</EmptyState>
      ) : (
        <ul className="mt-3 space-y-2">
          {execution.recovery.map((event) => (
            <li key={event.id} className="rounded border border-border p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="font-medium">{recoveryLabel(t, event.classification)}</span>
                <time dateTime={new Date(event.createdAt * 1000).toISOString()} className="text-[10px] text-muted-foreground">
                  {formatTimestamp(new Date(event.createdAt * 1000).toISOString())}
                </time>
              </div>
              <p className="mt-1 text-[11px] text-muted-foreground">
                {recoveryLabel(t, event.action)} → {recoveryLabel(t, event.outcome)}
              </p>
              <dl className="mt-2 space-y-0.5 text-[10px] text-muted-foreground">
                {event.attemptId && (
                  <div className="break-all">
                    <span className="font-sans">{t("recovery.attempt")}: </span>
                    <span className="font-mono">{event.attemptId}</span>
                    <span>
                      {" · "}
                      {event.supersededAttempt
                        ? t("recovery.superseded")
                        : execution.authoritativeAttemptId !== null
                          ? t("recovery.current")
                          : t("recovery.producing")}
                    </span>
                  </div>
                )}
                {event.executorId && (
                  <div className="break-all"><span className="font-sans">{t("recovery.executor")}: </span><span className="font-mono">{event.executorId}</span></div>
                )}
                {event.callId && (
                  <div className="break-all"><span className="font-sans">{t("recovery.call")}: </span><span className="font-mono">{event.callId}</span></div>
                )}
                {event.replacementAttemptId && (
                  <div className="break-all"><span className="font-sans">{t("recovery.replacement")}: </span><span className="font-mono">{event.replacementAttemptId}</span></div>
                )}
              </dl>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
