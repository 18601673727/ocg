"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { FolderGit2, GitCommitHorizontal, Lock, Save, ShieldCheck, Upload } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
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
import { MissionControlSurface } from "../mission-control/mission-control-surface";
import {
  DEFAULT_CONFIGURATION_DRAFT,
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
} from "./canonical-control-domain";

/**
 * Backend-backed OCG control surface.
 *
 * This is a projection and control surface only. It registers/imports real
 * repositories, persists supported global and project configuration, edits an
 * undispatched Mission, and renders a running canonical Mission from durable
 * backend state. It never dispatches, completes or replaces a Run, and a
 * dispatched Run's frozen executor contract is read-only here.
 */
export type CanonicalControlSurfaceProps = {
  /** Loopback OCG control base URL, e.g. http://127.0.0.1:8710 */
  baseUrl: string;
  fetchImpl: typeof fetch;
  /** Backend project to preselect, when one is already known. */
  initialRoot?: string;
};

function SectionCard({
  title,
  detail,
  children,
}: {
  title: string;
  detail?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-md border border-border bg-background p-3">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{title}</h2>
        {detail ? <span className="text-[10px] text-muted-foreground">{detail}</span> : null}
      </div>
      <div className="mt-2">{children}</div>
    </section>
  );
}

function IssueList({ issues }: { issues: readonly DraftIssue[] }) {
  if (issues.length === 0) return null;
  return (
    <ul className="mt-2 space-y-1">
      {issues.map((issue) => (
        <li key={`${issue.field}:${issue.code}`} className="text-[10px] text-amber-700 dark:text-amber-300">
          {issue.message}
        </li>
      ))}
    </ul>
  );
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
  const [missionId, setMissionId] = useState("");
  const [draft, setDraft] = useState<ConfigurationDraft>({ ...DEFAULT_CONFIGURATION_DRAFT });
  const [ack, setAck] = useState<string>("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const activeProjectId = state.projectId;
  const selected = selectCanonical(state, missionId || "Canonical Mission");
  const preRun = preRunConfigurationState(state);
  const draftIssues = validateConfigurationDraft(draft);
  const importIssues = validateImportRoot(root);

  const refresh = useCallback(
    async (project: CanonicalProjectRecord, mission: string) => {
      const snapshot = await client.readWorkSnapshot(project.project_id, mission);
      if (isCanonicalRejection(snapshot as never)) {
        setError((snapshot as { message: string }).message);
        return;
      }
      const generation = store.getCanonical().generation + 1;
      const next = store.applyCanonicalSnapshot({
        payload: snapshot,
        projectId: scopeIdFor(project),
        generation,
      });
      setState(next);
      const events = await client.readWorkEvents(project.project_id, mission, next.cursor);
      if (!isCanonicalRejection(events as never)) {
        setState(
          store.applyCanonicalEvents(events as never, {
            projectId: scopeIdFor(project),
            generation,
          }),
        );
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
      setAck(describeAcknowledgement(result as never));
      if (isCanonicalRejection(result as never)) return;
      const ackOk = result as { ok: true; commandId: string };
      setState(
        store.applyCanonicalCommandAck({
          commandId: ackOk.commandId,
          kind: "project-import",
          accepted: true,
          message: "Project registered",
        }),
      );
      const registered = await client.listProjects();
      setProjects(registered);
      const view = await client.readConfiguration((result as { project: CanonicalProjectRecord }).project.project_id);
      if (!isCanonicalRejection(view as never)) {
        setConfiguration(view as CanonicalConfigurationView);
        setDraft(draftFromConfiguration(view as CanonicalConfigurationView));
      }
    });

  const saveGlobal = () =>
    run(async () => {
      const project = projects.find((item) => item.project_id === activeProjectId) ?? projects[0];
      if (!project) {
        setError("Register a Project before changing global configuration.");
        return;
      }
      const commandId = globalConfigurationCommandId(configurationRevision(configuration) + 1);
      const result = await client.writeGlobalConfiguration(commandId, toGlobalConfiguration(draft));
      setAck(describeAcknowledgement(result as never));
      if (isCanonicalRejection(result as never)) return;
      const ok = result as { ok: true; commandId: string; configuration: CanonicalConfigurationView };
      setConfiguration(ok.configuration);
      setState(
        store.applyCanonicalCommandAck({
          commandId: ok.commandId,
          kind: "global-config",
          accepted: true,
          revision: (result as { revision?: number }).revision,
          message: "Global configuration persisted",
        }),
      );
    });

  const saveProjectDefaults = () =>
    run(async () => {
      const project = projects.find((item) => item.project_id === activeProjectId) ?? projects[0];
      if (!project) {
        setError("Register a Project before changing project defaults.");
        return;
      }
      const result = await client.writeProjectDefaults(
        projectDefaultsCommandId(project.project_id, configurationRevision(configuration) + 1),
        project.project_id,
        { profile: draft.profile, routing: draft.routing, hard_budget: draft.hardBudget },
      );
      setAck(describeAcknowledgement(result as never));
      if (!isCanonicalRejection(result as never)) {
        setConfiguration((result as { configuration: CanonicalConfigurationView }).configuration);
      }
    });

  const saveMissionConfig = () =>
    run(async () => {
      if (preRun.missionId === null) return;
      const result = await client.writeMissionConfiguration(
        `cmd-mission-config-${preRun.missionId}-${Date.now()}`,
        preRun.missionId,
        { profile: draft.profile, routing: draft.routing, hard_budget: draft.hardBudget },
      );
      setAck(describeAcknowledgement(result as never));
      if (!isCanonicalRejection(result as never)) {
        setState(
        store.applyCanonicalCommandAck({
            commandId: (result as { commandId: string }).commandId,
            kind: "mission-config",
            accepted: true,
            message: "Pre-run Mission configuration persisted",
          }),
        );
      }
    });

  const loadMission = () =>
    run(async () => {
      const project = projects.find((item) => item.project_id === activeProjectId) ?? projects[0];
      if (!project) {
        setError("Select a Project first.");
        return;
      }
      await refresh(project, missionId.trim());
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
          <SectionCard title="Project Manager" detail="backend identity">
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
                      if (missionId.trim().length > 0) void refresh(project, missionId.trim());
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
          </SectionCard>

          <SectionCard title="Global Configurator" detail="persisted OCG configuration">
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
                <Save className="size-3.5" /> Save global
              </Button>
              <Button size="xs" variant="outline" disabled={busy || draftIssues.length > 0} onClick={saveProjectDefaults}>
                Save project defaults
              </Button>
            </div>
          </SectionCard>

          <SectionCard
            title="Mission pre-run configuration"
            detail={preRun.missionId ? `mission ${preRun.missionId}` : "no Mission selected"}
          >
            <div className="flex flex-wrap items-end gap-2">
              <label className="flex-1 text-[10px] text-muted-foreground">
                Mission id
                <Input
                  className="mt-1 h-7 text-[11px]"
                  value={missionId}
                  placeholder="wn-..."
                  onChange={(event) => setMissionId(event.target.value)}
                />
              </label>
              <Button size="xs" variant="outline" disabled={busy || missionId.trim().length === 0} onClick={loadMission}>
                <GitCommitHorizontal className="size-3.5" /> Load
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
                onClick={saveMissionConfig}
              >
                <Save className="size-3.5" /> Save pre-run configuration
              </Button>
            </div>
            <dl className="mt-3 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[10px]">
              <dt className="text-muted-foreground">Dispatched Runs</dt>
              <dd>{state.projection?.runs.length ?? 0}</dd>
              <dt className="text-muted-foreground">Fenced generations</dt>
              <dd>{selected.fencedRuns.length}</dd>
              <dt className="text-muted-foreground">Verification evidence</dt>
              <dd>{state.projection?.verifications.length ?? 0}</dd>
              <dt className="text-muted-foreground">Canonical cursor</dt>
              <dd>{state.cursor}</dd>
            </dl>
          </SectionCard>

          <SectionCard title="Running Mission" detail="canonical projection">
            {selected.execution ? (
              <div className="h-[420px] overflow-hidden rounded border border-border">
                <MissionControlSurface execution={selected.execution} />
              </div>
            ) : (
              <p className="text-[11px] text-muted-foreground">
                Load a canonical Mission to inspect its WorkNode tree, dependency DAG, Run generations, frozen
                contracts, and verification evidence.
              </p>
            )}
          </SectionCard>
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
