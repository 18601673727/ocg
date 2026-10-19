"use client";

import { useEffect, useState } from "react";
import type { ChatModelSelection, ControlAgentProfile, ControlProject } from "../contracts";
import type { ActiveExecutionTarget, ActivityStep, ExecutionPhase } from "./execution-status";
import { ModelSelectorView } from "./model-selector-view";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";

type Catalog = { agents: ControlAgentProfile[]; projects: ControlProject[] };

export function ModelSelector({ sessionId, selection, onChange, onPreference, busy }: {
  sessionId?: string;
  selection?: ChatModelSelection;
  onChange: (selection: ChatModelSelection) => void;
  onPreference?: (selection: ChatModelSelection) => void;
  busy: boolean;
  active?: ActiveExecutionTarget | null;
  activity?: { current: ActivityStep | ExecutionPhase | null; steps: ActivityStep[] };
}) {
  const { control, client, snapshot } = useOcgRuntime();
  const { t } = useI18n();
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string>();
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!control) return;
    const controller = new AbortController();
    const load = async () => {
      const [profiles, projects] = await Promise.all([control.readProfiles(controller.signal), control.readAllProjects(controller.signal)]);
      const agents: ControlAgentProfile[] = [];
      let before: string | undefined;
      for (;;) {
        const page = await control.readAgentProfiles({ before, limit: 100, signal: controller.signal });
        agents.push(...page.agent_profiles);
        if (!page.has_more) break;
        if (!page.next_before || page.next_before === before) throw new Error("Agent Profile cursor did not advance.");
        before = page.next_before;
      }
      const listed = new Set(profiles.agent_profiles.map(profile => profile.id));
      if (agents.some(agent => !listed.has(agent.id))) throw new Error("Profile catalog changed while loading. Refresh the selection.");
      if (!controller.signal.aborted) { setCatalog({ agents, projects }); setError(undefined); }
    };
    void load().catch(cause => { if (!controller.signal.aborted) { setCatalog(null); setError(cause instanceof Error ? cause.message : String(cause)); } });
    return () => controller.abort();
  }, [control, attempt]);
  useEffect(() => control?.onProfilesChanged(() => setAttempt(value => value + 1)), [control]);

  const fixedAgent = snapshot.sessions.find(session => session.id === sessionId)?.agentProfile;
  const agents = (catalog?.agents ?? []).filter(agent => agent.llm_targets.some(target => target.declared_capabilities.includes("chat") && target.executors.length));
  const agent = agents.find(agent => agent.id === (fixedAgent ?? selection?.agentProfile)) ?? (!fixedAgent ? agents[0] : undefined);
  const targets = agent?.llm_targets.filter(target => target.declared_capabilities.includes("chat") && target.executors.length) ?? [];
  const llm = targets.find(target => target.id === selection?.llmProfile) ?? targets[0];
  const executor = llm?.executors.find(executor => executor === selection?.executor) ?? llm?.executors[0];
  const agentId = agent?.id;
  const llmId = llm?.id;
  const model = llm?.model;
  const disabled = busy || !catalog || !llm || !executor;
  // A conversation keeps the Project of its first turn, so only a new one chooses.
  const projects = agent?.tool_grant && !fixedAgent ? catalog?.projects ?? [] : [];
  const projectId = (projects.find(project => project.id === selection?.project) ?? projects[0])?.id;

  useEffect(() => {
    if (!agentId || !llmId || !executor || !model) return;
    const next = { model, effort: null, agentProfile: agentId, llmProfile: llmId, executor, ...(projectId ? { project: projectId } : {}) };
    if (selection?.agentProfile !== agentId || selection?.llmProfile !== llmId || selection?.executor !== executor || selection?.model !== model || selection?.project !== projectId) onChange(next);
    if (sessionId) client.setChatSelection?.(sessionId, next);
  }, [agentId, llmId, executor, model, projectId, selection, onChange, client, sessionId]);

  function choose(agentProfile: string, llmProfile?: string, executor?: string) {
    const agent = agents.find(agent => agent.id === agentProfile);
    const choices = agent?.llm_targets.filter(target => target.declared_capabilities.includes("chat") && target.executors.length) ?? [];
    const llm = choices.find(target => target.id === llmProfile) ?? choices[0];
    if (!agent || !llm) return;
    const next = { model: llm.model, effort: null, agentProfile, llmProfile: llm.id, executor: executor ?? llm.executors[0] };
    onChange(next);
    onPreference?.(next);
  }

  function chooseProject(project: string) {
    if (!selection) return;
    const next = { ...selection, project };
    onChange(next);
    onPreference?.(next);
  }

  return <div className="min-w-0 flex-1">
    <ModelSelectorView title={model ?? t("chat.noModels")} subtitle={agent?.name ?? null}
      status={busy ? t("chat.settingsLocked") : t("chat.settingsNextTurn")} active={busy} locked={busy} disabled={disabled} loading={!catalog && !error}
      error={error ?? (catalog && fixedAgent && !agent ? t("chat.agentUnavailable") : undefined)} refresh={{ disabled: busy, onRefresh: () => setAttempt(value => value + 1) }}
      providerLabel={t("chat.agent")} effortLabel={t("chat.executor")}
      provider={{ value: agentId ?? "", options: agents.filter(option => !fixedAgent || option.id === fixedAgent).map(agent => ({ value: agent.id, label: agent.name })), onChange: value => choose(value) }}
      model={{ value: llmId ?? "", options: targets.map(target => ({ value: target.id, label: `${target.model} (${target.id})` })), onChange: value => { if (agentId) choose(agentId, value); } }}
      effort={{ value: executor ?? "", options: (llm?.executors ?? []).map(id => ({ value: id, label: id })), onChange: value => { if (agentId) choose(agentId, llmId, value); } }} effortUnsupported={false} />
    {agent && <p className="mt-1 px-1 text-xs text-muted-foreground">{agent.tool_grant ? t("chat.workspaceAccess", { grant: agent.tool_grant }) : t("chat.noWorkspaceAccess")}</p>}
    {agent?.tool_grant && !fixedAgent && catalog && (projects.length
      ? <label className="mt-1 flex items-center gap-2 px-1 text-xs text-muted-foreground">{t("chat.project")}
        <select className="h-7 min-w-0 rounded-md border border-input bg-background px-2 text-xs text-foreground" value={projectId ?? ""} disabled={busy}
          onChange={event => chooseProject(event.target.value)}>
          {projects.map(project => <option key={project.id} value={project.id}>{project.name} ({project.id})</option>)}
        </select>
      </label>
      : <p role="status" className="mt-1 px-1 text-xs text-muted-foreground">{t("chat.projectNone")}</p>)}
    {llm && !llm.declared_capabilities.includes("tools") && <p role="status" className="mt-1 px-1 text-xs text-muted-foreground">{t("chat.toolsWarning")}</p>}
    {llm && !llm.declared_capabilities.includes("vision") && <p role="status" className="mt-1 px-1 text-xs text-muted-foreground">{t("chat.visionWarning")}</p>}
  </div>;
}
