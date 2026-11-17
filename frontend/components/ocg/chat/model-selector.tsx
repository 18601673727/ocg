"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { ExternalLink } from "lucide-react";
import { useEffect, useState, type Ref } from "react";
import type { ChatModelSelection, AgentProfile, Project } from "../contracts";
import { ModelSelectorView } from "./model-selector-view";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";
import { settingsPath } from "../settings/settings-sections";

const WIRE_EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];

type Catalog = { agents: AgentProfile[]; projects: Project[] };

export type ModelImageSupport = { llmProfile: string | undefined; supported: boolean };

export function ModelSelector({
  sessionId,
  selection,
  onChange,
  onPreference,
  onImageSupportChange,
  busy,
  projectLocked = false,
  lockedProject,
  statusText,
  triggerRef,
}: {
  sessionId?: string;
  selection?: ChatModelSelection;
  onChange: (selection: ChatModelSelection) => void;
  onPreference?: (selection: ChatModelSelection) => void;
  onImageSupportChange: (support: ModelImageSupport) => void;
  busy: boolean;
  projectLocked?: boolean;
  lockedProject?: string | null;
  statusText?: string;
  triggerRef?: Ref<HTMLButtonElement>;
}) {
  const { api, client, snapshot } = useOcgRuntime();
  const { t } = useI18n();
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [error, setError] = useState<string>();
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!api) return;
    const controller = new AbortController();
    const load = async () => {
      const [profiles, projects] = await Promise.all([
        api.readProfiles(controller.signal),
        api.readAllProjects(controller.signal),
      ]);
      const agents: AgentProfile[] = [];
      let before: string | undefined;
      for (;;) {
        const page = await api.readAgentProfiles({
          before,
          limit: 100,
          signal: controller.signal,
        });
        agents.push(...page.agent_profiles);
        if (!page.has_more) break;
        if (!page.next_before || page.next_before === before)
          throw new Error("Agent Profile cursor did not advance.");
        before = page.next_before;
      }
      const listed = new Set(profiles.agent_profiles.map((profile) => profile.id));
      if (agents.some((agent) => !listed.has(agent.id)))
        throw new Error("Profile catalog changed while loading. Refresh the selection.");
      if (!controller.signal.aborted) {
        setCatalog({ agents, projects });
        setError(undefined);
      }
    };
    void load().catch((cause) => {
      if (!controller.signal.aborted) {
        setCatalog(null);
        setError(cause instanceof Error ? cause.message : String(cause));
      }
    });
    return () => controller.abort();
  }, [api, attempt]);
  useEffect(() => api?.onProfilesChanged(() => setAttempt((value) => value + 1)), [api]);

  const session = snapshot.sessions.find((session) => session.id === sessionId);
  const fixedAgent = session?.agentProfile;
  const agents = (catalog?.agents ?? []).filter(
    (agent) =>
      (agent.capabilities.includes("chat") || agent.capabilities.includes("coding")) &&
      agent.llm_targets.some((target) => target.declared_capabilities.includes("chat")),
  );
  const agent =
    agents.find((agent) => agent.id === (fixedAgent ?? selection?.agentProfile)) ??
    (!fixedAgent ? agents[0] : undefined);
  const targets =
    agent?.llm_targets.filter((target) => target.declared_capabilities.includes("chat")) ?? [];
  const llm = targets.find((target) => target.id === selection?.llmProfile) ?? targets[0];
  const agentId = agent?.id;
  const llmId = llm?.id;
  const supportsImages = llm?.declared_capabilities.includes("vision") ?? false;
  useEffect(() => {
    onImageSupportChange({ llmProfile: llmId, supported: supportsImages });
  }, [llmId, supportsImages, onImageSupportChange]);
  const model = llm?.model;
  const efforts =
    llm?.reasoning_efforts ??
    (llm?.declared_capabilities.includes("reasoning") ? WIRE_EFFORTS : []);
  const effort =
    selection?.llmProfile === llmId &&
    selection?.effort &&
    selection.effort !== llm?.default_effort &&
    efforts.includes(selection.effort)
      ? selection.effort
      : null;
  const disabled = busy || !catalog || !llm;
  const needsSetup = Boolean(
    session && !session.conversationId && !fixedAgent && !busy && catalog && !error && !agent,
  );
  useEffect(() => {
    if (!needsSetup) return;
    const refresh = () => setAttempt((value) => value + 1);
    window.addEventListener("focus", refresh);
    return () => window.removeEventListener("focus", refresh);
  }, [needsSetup]);
  const projects = !fixedAgent ? (catalog?.projects ?? []) : [];
  const projectId = projectLocked
    ? (lockedProject ?? undefined)
    : projects.find((project) => project.id === selection?.project)?.id;

  useEffect(() => {
    if (!agentId || !llmId || !model) return;
    const next = {
      model,
      effort,
      agentProfile: agentId,
      llmProfile: llmId,
      ...(projectId ? { project: projectId } : {}),
    };
    if (
      selection?.agentProfile !== agentId ||
      selection?.llmProfile !== llmId ||
      selection?.model !== model ||
      selection?.project !== projectId ||
      (selection?.effort ?? null) !== effort
    )
      onChange(next);
    if (sessionId) client.setChatSelection?.(sessionId, next);
  }, [agentId, llmId, model, projectId, effort, selection, onChange, client, sessionId]);

  function choose(agentProfile: string, llmProfile?: string) {
    const agent = agents.find((agent) => agent.id === agentProfile);
    const choices =
      agent?.llm_targets.filter((target) => target.declared_capabilities.includes("chat")) ?? [];
    const llm = choices.find((target) => target.id === llmProfile) ?? choices[0];
    if (!agent || !llm) return;
    const next = {
      model: llm.model,
      effort: llm.id === selection?.llmProfile ? effort : null,
      agentProfile,
      llmProfile: llm.id,
      ...(selection?.project ? { project: selection.project } : {}),
    };
    onChange(next);
    onPreference?.(next);
  }

  function chooseProject(project: string) {
    if (!selection) return;
    const next = { ...selection, project: project || undefined };
    onChange(next);
    onPreference?.(next);
  }

  return (
    <div className="min-w-0 flex-1">
      <ModelSelectorView
        triggerRef={triggerRef}
        status={statusText ?? (busy ? t("chat.settingsLocked") : t("chat.settingsNextTurn"))}
        disabled={disabled}
        loading={!catalog && !error}
        error={error ?? (catalog && fixedAgent && !agent ? t("chat.agentUnavailable") : undefined)}
        providerLabel={t("chat.agent")}
        defaultEffort={llm?.default_effort}
        project={{
          value: projectLocked
            ? (lockedProject ?? "")
            : fixedAgent
              ? (session?.projectId ?? "")
              : (projectId ?? ""),
          options: (fixedAgent || projectLocked ? (catalog?.projects ?? []) : projects).map(
            (project) => ({
              value: project.id,
              label: project.name,
            }),
          ),
          disabled: busy || !catalog || Boolean(fixedAgent) || projectLocked,
          placeholder: t("chat.pureConversation"),
          onChange: chooseProject,
        }}
        provider={{
          value: agentId ?? "",
          options: agents
            .filter((option) => !fixedAgent || option.id === fixedAgent)
            .map((agent) => ({ value: agent.id, label: agent.name })),
          onChange: (value) => choose(value),
        }}
        model={{
          value: llmId ?? "",
          options: targets.map((target) => ({
            value: target.id,
            label: target.name.trim() || target.model,
          })),
          onChange: (value) => {
            if (agentId) choose(agentId, value);
          },
        }}
        effort={{
          value: effort ?? "",
          options: efforts.map((value) => ({ value, label: value })),
          onChange: (value) => {
            if (!selection) return;
            const next = { ...selection, effort: value || null };
            onChange(next);
            onPreference?.(next);
          },
        }}
      />
      {needsSetup && (
        <a
          href={settingsPath("models")}
          target="_blank"
          rel="noopener noreferrer"
          className="mt-2 inline-flex items-start gap-1.5 rounded-sm px-1 text-xs leading-5 text-primary underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-ring"
        >
          <ExternalLink className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
          <span>
            {t("chat.setupModels")}
            <span className="sr-only"> ({t("chat.setupNewTab")})</span>
          </span>
        </a>
      )}
      {llm && !llm.declared_capabilities.includes("tools") && (
        <DismissibleNotice
          tone="warning"
          message={t("chat.toolsWarning")}
          className="mt-1 px-1 text-xs text-muted-foreground"
        />
      )}
    </div>
  );
}
