import { createEffect, createMemo, createSignal } from "solid-js";
import { type Ref } from "@/lib/ui-state";
import type { AgentProfile, ChatModelSelection, Project } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgRuntime, useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import { ModelSelectorView } from "./model-selector-view";

const WIRE_EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];

type Catalog = { agents: AgentProfile[]; projects: Project[] };

export type ModelSupport = {
  llmProfile: string | undefined;
  images: boolean;
  notice?: string;
};

export function ModelSelector(props: {
  sessionId?: string;
  selection?: ChatModelSelection;
  onChange: (selection: ChatModelSelection) => void;
  onPreference?: (selection: ChatModelSelection) => void;
  onSupportChange: (support: ModelSupport) => void;
  busy: boolean;
  projectLocked?: boolean;
  lockedProject?: string | null;
  statusText?: string;
  triggerRef?: Ref<HTMLButtonElement>;
}) {
  const { api, client } = useOcgRuntime();
  const i18n = useI18n();
  const [catalog, setCatalog] = createSignal<Catalog | null>(null);
  const [error, setError] = createSignal<string>();
  const [attempt, setAttempt] = createSignal(0);
  createEffect(
    () => [api, attempt()],
    () => {
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
    },
  );
  createEffect(
    () => [api],
    () => api?.onProfilesChanged(() => setAttempt((value) => value + 1)),
  );

  const session = useOcgRuntimeSnapshot((snapshot) =>
    snapshot.sessions.find((session) => session.id === props.sessionId),
  );
  const fixedAgent = createMemo(() => session()?.agentProfile);
  const agents = createMemo(() =>
    (catalog()?.agents ?? []).filter(
      (agent) =>
        (agent.capabilities.includes("chat") || agent.capabilities.includes("coding")) &&
        agent.llm_targets.some((target) => target.declared_capabilities.includes("chat")),
    ),
  );
  const agent = createMemo(
    () =>
      agents().find((agent) => agent.id === (fixedAgent() ?? props.selection?.agentProfile)) ??
      (!fixedAgent() ? agents()[0] : undefined),
  );
  const targets = createMemo(
    () =>
      agent()?.llm_targets.filter((target) => target.declared_capabilities.includes("chat")) ?? [],
  );
  const llm = createMemo(
    () => targets().find((target) => target.id === props.selection?.llmProfile) ?? targets()[0],
  );
  const agentId = createMemo(() => agent()?.id);
  const llmId = createMemo(() => llm()?.id);
  const supportsImages = createMemo(() => llm()?.declared_capabilities.includes("vision") ?? false);
  const model = createMemo(() => llm()?.model);
  const efforts = createMemo(
    () =>
      llm()?.reasoning_efforts ??
      (llm()?.declared_capabilities.includes("reasoning") ? WIRE_EFFORTS : []),
  );
  const effort = createMemo(() =>
    props.selection?.llmProfile === llmId() &&
    props.selection?.effort &&
    props.selection.effort !== llm()?.default_effort &&
    efforts().includes(props.selection.effort)
      ? props.selection.effort
      : null,
  );
  const disabled = createMemo(() => props.busy || !catalog() || !llm());
  const needsSetup = createMemo(() =>
    Boolean(
      session() &&
        !session()!.conversationId &&
        !fixedAgent() &&
        !props.busy &&
        catalog() &&
        !error() &&
        !agent(),
    ),
  );
  const notice = createMemo(
    () =>
      error() ??
      (catalog() && fixedAgent() && !agent()
        ? i18n.t("chat.agentUnavailable")
        : needsSetup()
          ? i18n.t("chat.setupModels")
          : llm() && !llm().declared_capabilities.includes("tools")
            ? i18n.t("chat.toolsWarning")
            : undefined),
  );
  createEffect(
    () => [llmId(), supportsImages(), notice()],
    () => {
      props.onSupportChange({
        llmProfile: llmId(),
        images: supportsImages(),
        notice: notice(),
      });
    },
  );
  createEffect(
    () => [needsSetup()],
    () => {
      if (!needsSetup()) return;
      const refresh = () => setAttempt((value) => value + 1);
      window.addEventListener("focus", refresh);
      return () => window.removeEventListener("focus", refresh);
    },
  );
  const projects = createMemo(() => (!fixedAgent() ? (catalog()?.projects ?? []) : []));
  const projectId = createMemo(() =>
    (props.projectLocked === undefined ? false : props.projectLocked)
      ? (props.lockedProject ?? undefined)
      : projects().find((project) => project.id === props.selection?.project)?.id,
  );

  createEffect(
    () => [
      agentId(),
      llmId(),
      model(),
      projectId(),
      effort(),
      props.selection,
      props.onChange,
      client,
      props.sessionId,
    ],
    () => {
      if (!agentId() || !llmId() || !model()) return;
      const next = {
        model: model(),
        effort: effort(),
        agentProfile: agentId(),
        llmProfile: llmId(),
        ...(projectId() ? { project: projectId() } : {}),
      };
      if (
        props.selection?.agentProfile !== agentId() ||
        props.selection?.llmProfile !== llmId() ||
        props.selection?.model !== model() ||
        props.selection?.project !== projectId() ||
        (props.selection?.effort ?? null) !== effort()
      )
        props.onChange(next);
      if (props.sessionId) client.setChatSelection?.(props.sessionId, next);
    },
  );

  function choose(agentProfile: string, llmProfile?: string) {
    const agent = agents().find((agent) => agent.id === agentProfile);
    const choices =
      agent?.llm_targets.filter((target) => target.declared_capabilities.includes("chat")) ?? [];
    const llm = choices.find((target) => target.id === llmProfile) ?? choices[0];
    if (!agent || !llm) return;
    const next = {
      model: llm.model,
      effort: llm.id === props.selection?.llmProfile ? effort() : null,
      agentProfile,
      llmProfile: llm.id,
      ...(props.selection?.project ? { project: props.selection.project } : {}),
    };
    props.onChange(next);
    props.onPreference?.(next);
  }

  function chooseProject(project: string) {
    if (!props.selection) return;
    const next = { ...props.selection, project: project || undefined };
    props.onChange(next);
    props.onPreference?.(next);
  }

  return (
    <div class="min-w-0 flex-1">
      <ModelSelectorView
        triggerRef={props.triggerRef}
        status={
          props.statusText ??
          (props.busy ? i18n.t("chat.settingsLocked") : i18n.t("chat.settingsNextTurn"))
        }
        disabled={disabled()}
        loading={!catalog() && !error()}
        providerLabel={i18n.t("chat.agent")}
        defaultEffort={llm()?.default_effort}
        project={{
          value: (props.projectLocked === undefined ? false : props.projectLocked)
            ? (props.lockedProject ?? "")
            : fixedAgent()
              ? (session()?.projectId ?? "")
              : (projectId() ?? ""),
          options: (fixedAgent() ||
          (props.projectLocked === undefined ? false : props.projectLocked)
            ? (catalog()?.projects ?? [])
            : projects()
          ).map((project) => ({
            value: project.id,
            label: project.name,
          })),
          disabled:
            props.busy ||
            !catalog() ||
            Boolean(fixedAgent()) ||
            (props.projectLocked === undefined ? false : props.projectLocked),
          placeholder: i18n.t("chat.pureConversation"),
          onChange: chooseProject,
        }}
        provider={{
          value: agentId() ?? "",
          options: agents()
            .filter((option) => !fixedAgent() || option.id === fixedAgent())
            .map((agent) => ({ value: agent.id, label: agent.name })),
          onChange: (value) => choose(value),
        }}
        model={{
          value: llmId() ?? "",
          options: targets().map((target) => ({
            value: target.id,
            label: target.name.trim() || target.model,
          })),
          onChange: (value) => {
            if (agentId()) choose(agentId()!, value);
          },
        }}
        effort={{
          value: effort() ?? "",
          options: efforts().map((value) => ({ value, label: value })),
          onChange: (value) => {
            if (!props.selection) return;
            const next = { ...props.selection, effort: value || null };
            props.onChange(next);
            props.onPreference?.(next);
          },
        }}
      />
    </div>
  );
}
