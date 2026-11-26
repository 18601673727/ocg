import type * as Api from "./api";

export type { Pool, Pools } from "./api";

export type Capability = "chat" | "tools" | "vision" | "reasoning";
export type Project = Omit<Api.Project, "commands"> & {
  commands: (Omit<Api.ProjectCommand, "owner" | "status"> & {
    owner: "user" | "decision";
    status: "allowed" | "review" | "blocked";
  })[];
};
export type Projects = Omit<Api.Projects, "projects"> & { projects: Project[] };
export type AgentProfile = Omit<Api.AgentProfile, "llm_targets"> & {
  llm_targets: (Omit<Api.LlmTarget, "parameters" | "declared_capabilities"> & {
    default_effort: string | null;
    declared_capabilities: Capability[];
  })[];
};
export type AgentProfiles = Omit<Api.AgentProfiles, "agent_profiles"> & {
  agent_profiles: AgentProfile[];
};
type Protocol = "openai_chat_completions" | "openai_responses" | "anthropic_messages";
export type ProfilesSnapshot = Omit<
  Api.Profiles,
  | "profile_id"
  | "llm_profiles"
  | "decision_profiles"
  | "decision_agent_profiles"
  | "providers"
  | "executor_protocols"
> & {
  profile_id: string | null;
  llm_profiles: (Omit<Api.LlmProfile, "protocol" | "declared_capabilities"> & {
    protocol: Protocol;
    declared_capabilities: Capability[];
    executors: string[];
  })[];
  providers: (Omit<Api.Provider, "protocol"> & { protocol: Protocol })[];
  executor_protocols: (Omit<Api.ProfileExecutor, "protocol"> & { protocol: Protocol })[];
  decision_profiles: Api.DecisionProfile[];
  decision_agent_profiles: Api.DecisionAgentProfile[];
};
export type LlmProfile = ProfilesSnapshot["llm_profiles"][number];
export type ManagedAgentProfile = ProfilesSnapshot["agent_profiles"][number];

const WIRE_EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];

export function reasoningEffortOptions(model?: {
  reasoning_efforts: string[] | null;
  declared_capabilities: Capability[];
}) {
  return (
    model?.reasoning_efforts ??
    (model?.declared_capabilities.includes("reasoning") ? WIRE_EFFORTS : [])
  );
}

export function projectAgentProfiles(page: Api.AgentProfiles): AgentProfiles {
  return {
    ...page,
    agent_profiles: page.agent_profiles.map((agent) => ({
      ...agent,
      llm_targets: agent.llm_targets.map(({ parameters, ...target }) => ({
        ...target,
        declared_capabilities: target.declared_capabilities as Capability[],
        default_effort: parameters.reasoning_effort,
      })),
    })),
  };
}

export function projectProfiles(page: Api.Profiles): ProfilesSnapshot {
  return {
    ...page,
    profile_id: page.profile_id ?? null,
    llm_profiles: page.llm_profiles.map((profile) => ({
      ...profile,
      protocol: profile.protocol as Protocol,
      declared_capabilities: profile.declared_capabilities as Capability[],
      executors: [],
    })),
    providers: page.providers as ProfilesSnapshot["providers"],
    executor_protocols: page.executor_protocols as ProfilesSnapshot["executor_protocols"],
    decision_profiles: page.decision_profiles ?? [],
    decision_agent_profiles: page.decision_agent_profiles ?? [],
  };
}
