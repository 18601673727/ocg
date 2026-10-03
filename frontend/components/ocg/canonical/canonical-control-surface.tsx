"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { FolderGit2, GitCommitHorizontal, Lock, Save, ShieldCheck, Upload } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { Panel } from "@/components/ocg/primitives";
import {
  createHttpCanonicalControlClient,
  isCanonicalRejection,
  projectLabelFor,
  scopeIdFor,
  type CanonicalConfigurationView,
  type CanonicalControlClient,
  type CanonicalProjectRecord,
} from "../runtime/canonical-client";
import { RuntimeStore } from "../runtime/runtime-store";
import { createUninitializedRuntimeState } from "../runtime/reconciler";
import { type CanonicalState, selectCanonical } from "../runtime/canonical-store";
import { JobExecutionSurface } from "../execution/job-execution-surface";
import { useI18n, type I18nKey } from "../i18n";
import {
  DEFAULT_CONFIGURATION_DRAFT,
  MIN_HARD_BUDGET,
  describeAcknowledgement,
  draftFromConfiguration,
  globalConfigurationCommandId,
  preRunConfigurationState,
  projectDefaultsCommandId,
  projectImportCommandId,
  selectRegisteredProjects,
  toGlobalConfiguration,
  validateConfigurationDraft,
  validateImportRoot,
  type ConfigurationDraft,
  type DraftIssue,
  type DraftIssueCode,
} from "./canonical-control-domain";

/**
 * Backend-backed OCG control surface.
 *
 * This is a projection and control surface only. It registers/imports real
 * repositories, persists supported global and project configuration, edits an
 * undispatched Job, and renders a running canonical Job from durable backend
 * state. It never dispatches, completes or replaces a Call, and a dispatched
 * Call's frozen effect contract is read-only here.
 */
export type CanonicalControlSurfaceProps = {
  /** Loopback OCG control base URL, e.g. http://127.0.0.1:8710 */
  baseUrl: string;
  fetchImpl: typeof fetch;
  /** Backend project to preselect, when one is already known. */
  initialRoot?: string;
};

function IssueList({ issues }: { issues: readonly DraftIssue[] }) {
  const { t } = useI18n();
  if (issues.length === 0) return null;
  return (
    <ul className="mt-2 space-y-1">
      {issues.map((issue) => (
        <li key={`${issue.field}:${issue.code}`} className="text-[10px] text-amber-700 dark:text-amber-300">
          {issueText(t, issue)}
        </li>
      ))}
    </ul>
  );
}

/**
 * Static validation copy lives in the i18n table keyed by issue code; the
 * domain message is only a fallback. Contract/backend error strings stay raw
 * at their call sites. `root-required` covers two domain messages, so the
 * absolute-path variant selects its own key by content.
 */
const ISSUE_I18N_KEY: Record<DraftIssueCode, I18nKey> = {
  "provider-required": "canonical.issue.provider-required",
  "model-required": "canonical.issue.model-required",
  "budget-invalid": "canonical.issue.budget-invalid",
  "budget-below-minimum": "canonical.issue.budget-below-minimum",
  "profile-invalid": "canonical.issue.profile-invalid",
  "routing-invalid": "canonical.issue.routing-invalid",
  "root-required": "canonical.issue.root-required",
  "job-required": "canonical.issue.job-required",
  "job-dispatched": "canonical.issue.job-dispatched",
};

function issueText(t: ReturnType<typeof useI18n>["t"], issue: DraftIssue): string {
  if (issue.code === "root-required" && issue.message.includes("absolute")) {
    return t("canonical.issue.root-format");
  }
  if (issue.code === "budget-below-minimum") {
    return t(ISSUE_I18N_KEY[issue.code], { min: MIN_HARD_BUDGET });
  }
  return t(ISSUE_I18N_KEY[issue.code]);
}

export function CanonicalControlSurface({
  baseUrl,
  fetchImpl,
  initialRoot,
}: CanonicalControlSurfaceProps) {
  const client = useMemo<CanonicalControlClient>(
    () => createHttpCanonicalControlClient({ baseUrl, fetch: fetchImpl }),
    [baseUrl, fetchImpl],
  );
  // The existing RuntimeStore owns the canonical projection so a control
  // acknowledgement and a runtime commit share one listener notification.
  const store = useMemo(
    () => new RuntimeStore(createUninitializedRuntimeState("local-ready")),
    [],
  );
  const [state, setState] = useState<CanonicalState>(() => store.getCanonical());
  const [projects, setProjects] = useState<CanonicalProjectRecord[]>([]);
  const [configuration, setConfiguration] = useState<CanonicalConfigurationView | null>(null);
  const [root, setRoot] = useState(initialRoot ?? "");
  const [jobId, setJobId] = useState("");
  const [draft, setDraft] = useState<ConfigurationDraft>({ ...DEFAULT_CONFIGURATION_DRAFT });
  const [ack, setAck] = useState<string>("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { t } = useI18n();

  const activeProjectId = state.projectId;
  const selected = useMemo(() => selectCanonical(state), [state]);
  const preRun = preRunConfigurationState(state);
  const draftIssues = validateConfigurationDraft(draft);
  const importIssues = validateImportRoot(root);

  const refresh = useCallback(
    async (project: CanonicalProjectRecord, job: string) => {
      const projectId = scopeIdFor(project);
      // The store projects whole authoritative snapshots and never advances its
      // cursor from a raw event, so a canonical event tail means the installed
      // snapshot is behind: refetch it until the tail is empty. Each round
      // advances the cursor together with the projection it belongs to.
      for (let attempt = 0; attempt < 4; attempt += 1) {
        const snapshot = await client.readJobSnapshot(project.project_id, job);
        if (isCanonicalRejection(snapshot)) {
          setError(snapshot.message);
          return;
        }
        const generation = store.getCanonical().generation + 1;
        const next = store.applyCanonicalSnapshot({ payload: snapshot, projectId, generation });
        setState(next);
        // A rejected or stale snapshot leaves the projection and its cursor
        // untouched, so there is no coherent new tail to read.
        if (next.projection === null || next.cursor !== snapshot.cursor) return;
        const events = await client.readJobEvents(project.project_id, job, next.cursor);
        if (isCanonicalRejection(events)) return;
        const applied = store.applyCanonicalEvents(events, { projectId, generation });
        setState(applied);
        if (!applied.resyncRequired) return;
      }
    },
    [client, store],
  );

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const registered = await client.listProjects();
        if (cancelled) return;
        setProjects(registered);
      } catch (cause) {
        if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [client]);

  const run = useCallback(async (operation: () => Promise<void>) => {
    setBusy(true);
    setError(null);
    try {
      await operation();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }, []);

  const importProject = () =>
    run(async () => {
      const result = await client.importProject(projectImportCommandId(root.trim()), root.trim());
      setAck(describeAcknowledgement(result));
      if (isCanonicalRejection(result)) return;
      setState(
        store.applyCanonicalCommandAck({
          commandId: result.commandId,
          kind: "project-import",
          accepted: true,
          message: "Project registered",
        }),
      );
      const registered = await client.listProjects();
      setProjects(registered);
      const view = await client.readConfiguration(result.project.project_id);
      if (!isCanonicalRejection(view)) {
        setConfiguration(view);
        setDraft(draftFromConfiguration(view));
      }
    });

  const saveGlobal = () =>
    run(async () => {
      const project = projects.find((item) => item.project_id === activeProjectId) ?? projects[0];
      if (!project) {
        setError(t("canonical.error.needProjectGlobal"));
        return;
      }
      const commandId = globalConfigurationCommandId(configurationRevision(configuration) + 1);
      const result = await client.writeGlobalConfiguration(commandId, toGlobalConfiguration(draft));
      setAck(describeAcknowledgement(result));
      if (isCanonicalRejection(result)) return;
      setConfiguration(result.configuration);
      setState(
        store.applyCanonicalCommandAck({
          commandId: result.commandId,
          kind: "global-config",
          accepted: true,
          revision: result.revision,
          message: "Global configuration persisted",
        }),
      );
    });

  const saveProjectDefaults = () =>
    run(async () => {
      const project = projects.find((item) => item.project_id === activeProjectId) ?? projects[0];
      if (!project) {
        setError(t("canonical.error.needProjectDefaults"));
        return;
      }
      const result = await client.writeProjectDefaults(
        projectDefaultsCommandId(project.project_id, configurationRevision(configuration) + 1),
        project.project_id,
        { profile: draft.profile, routing: draft.routing, hard_budget: draft.hardBudget },
      );
      setAck(describeAcknowledgement(result));
      if (!isCanonicalRejection(result)) {
        setConfiguration(result.configuration);
      }
    });

  const saveJobConfig = () =>
    run(async () => {
      if (preRun.jobId === null) return;
      const result = await client.writeJobConfiguration(
        `cmd-job-config-${preRun.jobId}-${Date.now()}`,
        preRun.jobId,
        { profile: draft.profile, routing: draft.routing, hard_budget: draft.hardBudget },
      );
      setAck(describeAcknowledgement(result));
      if (!isCanonicalRejection(result)) {
        setState(
          store.applyCanonicalCommandAck({
            commandId: result.commandId,
            kind: "job-config",
            accepted: true,
            message: "Pre-run Job configuration persisted",
          }),
        );
      }
    });

  const loadJob = () =>
    run(async () => {
      const project = projects.find((item) => item.project_id === activeProjectId) ?? projects[0];
      if (!project) {
        setError(t("canonical.error.selectProjectFirst"));
        return;
      }
      await refresh(project, jobId.trim());
    });

  return (
    <div className="flex min-h-0 w-full min-w-0 flex-1 flex-col overflow-auto">
      <div className="space-y-3 p-3">
        {error ? (
          <p className="rounded-md border border-amber-500/40 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-700 dark:text-amber-300">
            {error}
          </p>
        ) : null}
        {ack ? <p className="text-[10px] text-muted-foreground">{ack}</p> : null}

        <div className="grid gap-3 xl:grid-cols-2">
          <Panel className="bg-background p-3" title="Project Manager" detail="backend identity">
            <div className="flex flex-wrap items-end gap-2">
              <label className="flex-1 text-[10px] text-muted-foreground">
                Repository root
                <Input
                  className="mt-1 h-7 text-[11px]"
                  value={root}
                  placeholder="/path/to/repository"
                  onChange={(event) => setRoot(event.target.value)}
                />
              </label>
              <Button size="xs" disabled={busy || importIssues.length > 0} onClick={importProject}>
                <Upload className="size-3.5" /> Import
              </Button>
            </div>
            <IssueList issues={importIssues} />
            <ul className="mt-2 space-y-1">
              {selectRegisteredProjects(projects, activeProjectId).map((project) => (
                <li key={project.project_id}>
                  <button
                    type="button"
                    aria-pressed={project.active}
                    onClick={() => {
                      if (jobId.trim().length > 0) void refresh(project, jobId.trim());
                    }}
                    className={cn(
                      "flex w-full items-center gap-2 rounded border px-2 py-1 text-left text-[10px]",
                      project.active ? "border-foreground/30 bg-muted" : "border-border hover:bg-muted/50",
                    )}
                  >
                    <FolderGit2 className="size-3" aria-hidden="true" />
                    <span className="font-medium">{projectLabelFor(project)}</span>
                    <span className="truncate text-muted-foreground">{project.root}</span>
                    {project.marker ? (
                      <span className="ml-auto inline-flex items-center gap-1 text-muted-foreground">
                        <ShieldCheck className="size-3" /> boundary
                      </span>
                    ) : (
                      <span className="ml-auto text-muted-foreground">explicit</span>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          </Panel>

          <Panel className="bg-background p-3" title="Global Configurator" detail="persisted OCG configuration">
            <div className="grid gap-2 sm:grid-cols-2">
              <label className="text-[10px] text-muted-foreground">
                Provider
                <Input
                  className="mt-1 h-7 text-[11px]"
                  value={draft.provider}
                  onChange={(event) => setDraft({ ...draft, provider: event.target.value })}
                />
              </label>
              <label className="text-[10px] text-muted-foreground">
                Model
                <Input
                  className="mt-1 h-7 text-[11px]"
                  value={draft.model}
                  onChange={(event) => setDraft({ ...draft, model: event.target.value })}
                />
              </label>
              <label className="text-[10px] text-muted-foreground">
                Profile
                <select
                  className="mt-1 h-7 w-full rounded-md border border-border bg-background px-2 text-[11px]"
                  value={draft.profile}
                  onChange={(event) =>
                    setDraft({ ...draft, profile: event.target.value as ConfigurationDraft["profile"] })
                  }
                >
                  <option value="fast">fast</option>
                  <option value="careful">careful</option>
                  <option value="balanced">balanced</option>
                </select>
              </label>
              <label className="text-[10px] text-muted-foreground">
                Routing
                <select
                  className="mt-1 h-7 w-full rounded-md border border-border bg-background px-2 text-[11px]"
                  value={draft.routing}
                  onChange={(event) =>
                    setDraft({ ...draft, routing: event.target.value as ConfigurationDraft["routing"] })
                  }
                >
                  <option value="direct">direct</option>
                  <option value="balanced">balanced</option>
                  <option value="review">review</option>
                </select>
              </label>
              <label className="text-[10px] text-muted-foreground">
                Hard budget (USD)
                <Input
                  className="mt-1 h-7 text-[11px]"
                  type="number"
                  value={draft.hardBudget}
                  onChange={(event) => setDraft({ ...draft, hardBudget: Number(event.target.value) })}
                />
              </label>
            </div>
            <IssueList issues={draftIssues} />
            <div className="mt-2 flex flex-wrap gap-2">
              <Button size="xs" disabled={busy || draftIssues.length > 0} onClick={saveGlobal}>
                <Save className="size-3.5" /> {t("canonical.saveGlobal")}
              </Button>
              <Button size="xs" variant="outline" disabled={busy || draftIssues.length > 0} onClick={saveProjectDefaults}>
                {t("canonical.saveProjectDefaults")}
              </Button>
            </div>
          </Panel>

          <Panel
            className="bg-background p-3"
            title="Job pre-run configuration"
            detail={preRun.jobId ? `job ${preRun.jobId}` : t("canonical.noJob")}
          >
            <div className="flex flex-wrap items-end gap-2">
              <label className="flex-1 text-[10px] text-muted-foreground">
                {t("canonical.jobId")}
                <Input
                  className="mt-1 h-7 text-[11px]"
                  value={jobId}
                  placeholder="job-..."
                  onChange={(event) => setJobId(event.target.value)}
                />
              </label>
              <Button size="xs" variant="outline" disabled={busy || jobId.trim().length === 0} onClick={loadJob}>
                <GitCommitHorizontal className="size-3.5" /> {t("common.load")}
              </Button>
            </div>
            {preRun.reason ? (
              <p className="mt-2 flex items-start gap-1 text-[10px] text-muted-foreground">
                <Lock className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
                {preRun.reason}
              </p>
            ) : null}
            <div className="mt-2 flex flex-wrap gap-2">
              <Button
                size="xs"
                disabled={busy || draftIssues.length > 0 || !preRun.editable}
                onClick={saveJobConfig}
              >
                <Save className="size-3.5" /> {t("canonical.savePreRun")}
              </Button>
            </div>
            <dl className="mt-3 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[10px]">
              <dt className="text-muted-foreground">Attempts</dt>
              <dd>{state.projection?.attempts.length ?? 0}</dd>
              <dt className="text-muted-foreground">Calls</dt>
              <dd>{state.projection?.calls.length ?? 0}</dd>
              <dt className="text-muted-foreground">Attempt history</dt>
              <dd>{selected.attemptHistory.length}</dd>
              <dt className="text-muted-foreground">Event cursor</dt>
              <dd>{state.cursor}</dd>
            </dl>
          </Panel>

          <Panel className="bg-background p-3" title={t("canonical.runningJob")} detail="Runtime state">
            {selected.execution ? (
              <div className="h-[420px] overflow-hidden rounded border border-border">
                <JobExecutionSurface execution={selected.execution} />
              </div>
            ) : (
              <p className="text-[11px] text-muted-foreground">
                Load a Job to inspect its execution graph, Attempt history, Call states, and frozen
                contracts.
              </p>
            )}
          </Panel>
        </div>
      </div>
    </div>
  );
}

function configurationRevision(view: CanonicalConfigurationView | null): number {
  if (!view) return 0;
  const budget = view.global.resource_budget;
  return budget ? Math.trunc(budget.hard_limit) : 0;
}
