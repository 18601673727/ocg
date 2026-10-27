import type * as Api from "./api";

export type ControlCapability = "chat" | "tools" | "vision" | "reasoning";
export type ControlPool = Omit<Api.Pool, "operations"> & {
  operations: ("llm_request" | "decision")[] | undefined;
};
export type ControlPools = { pools: ControlPool[] };
export type ControlProject = Omit<Api.Project, "commands"> & {
  commands: (Omit<Api.ProjectCommand, "owner" | "status"> & {
    owner: "user" | "decision";
    status: "allowed" | "review" | "blocked";
  })[];
};
export type ControlProjects = Omit<Api.Projects, "projects"> & { projects: ControlProject[] };
export type ControlAgentProfile = Omit<Api.AgentProfile, "llm_targets"> & {
  llm_targets: (Omit<Api.LlmTarget, "parameters" | "declared_capabilities"> & {
    default_effort: string | null;
    declared_capabilities: ControlCapability[];
  })[];
};
export type ControlAgentProfiles = Omit<Api.AgentProfiles, "agent_profiles"> & {
  agent_profiles: ControlAgentProfile[];
};
type Protocol = "openai_chat_completions" | "openai_responses" | "anthropic_messages";
export type ControlProfilesSnapshot = Omit<
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
    declared_capabilities: ControlCapability[];
    executors: string[];
  })[];
  providers: (Omit<Api.Provider, "protocol"> & { protocol: Protocol })[];
  executor_protocols: (Omit<Api.ProfileExecutor, "protocol"> & { protocol: Protocol })[];
  decision_profiles: Api.DecisionProfile[];
  decision_agent_profiles: Api.DecisionAgentProfile[];
};
export type ControlProviderModel = Api.ProviderModel;
export type ControlLlmProfile = ControlProfilesSnapshot["llm_profiles"][number];
export type ControlManagedAgentProfile = ControlProfilesSnapshot["agent_profiles"][number];

export function projectAgentProfiles(page: Api.AgentProfiles): ControlAgentProfiles {
  return {
    ...page,
    agent_profiles: page.agent_profiles.map((agent) => ({
      ...agent,
      llm_targets: agent.llm_targets.map(({ parameters, ...target }) => ({
        ...target,
        declared_capabilities: target.declared_capabilities as ControlCapability[],
        default_effort: parameters.reasoning_effort,
      })),
    })),
  };
}

export function projectProfiles(page: Api.Profiles): ControlProfilesSnapshot {
  return {
    ...page,
    profile_id: page.profile_id ?? null,
    llm_profiles: page.llm_profiles.map((profile) => ({
      ...profile,
      protocol: profile.protocol as Protocol,
      declared_capabilities: profile.declared_capabilities as ControlCapability[],
      executors: [],
    })),
    providers: page.providers as ControlProfilesSnapshot["providers"],
    executor_protocols: page.executor_protocols as ControlProfilesSnapshot["executor_protocols"],
    decision_profiles: page.decision_profiles ?? [],
    decision_agent_profiles: page.decision_agent_profiles ?? [],
  };
}
