import {
  type Capability,
  ContractError,
  projectAgentProfiles,
  projectProfiles,
} from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { ApiError, type Send } from "./transport";
import { identity } from "./validators";

export function isProfileConflict(error: unknown): boolean {
  return (
    error instanceof ApiError &&
    (error.code === "profile_revision_changed" || error.code === "profile_changed")
  );
}

export type ProfileWriteRequest =
  | {
      kind: "llm";
      name: string;
      model: string;
      context_window?: number | null;
      protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
      endpoint?: string | null;
      credential_ref?: string | null;
      credential?: string | null;
      clear_credential?: boolean;
      declared_capabilities: Capability[];
      headers?: Api.OutboundHeaderInput[];
      expected_edit_seq?: number;
      parameters?: {
        temperature?: number | null;
        top_p?: number | null;
        max_tokens?: number | null;
        max_completion_tokens?: number | null;
        reasoning_effort?: string | null;
        presence_penalty?: number | null;
        frequency_penalty?: number | null;
      };
    }
  | {
      kind: "agent";
      name: string;
      instructions?: string | null;
      capabilities: ("chat" | "coding" | "decision")[];
      llm_targets: string[];
      expected_edit_seq?: number;
    }
  | {
      kind: "decision_model";
      name: string;
      model: string;
      protocol: "typesafe_systemone";
      endpoint: string;
      credential?: string;
      credential_ref?: string | null;
      clear_credential?: boolean;
      declared_capabilities: string[];
      limits: Api.DecisionLimits;
      headers?: Api.OutboundHeaderInput[];
      expected_edit_seq?: number;
    }
  | {
      kind: "decision_agent";
      name: string;
      instructions?: string | null;
      models: string[];
      default_model?: string | null;
      expected_edit_seq?: number;
    };

export function createProfilesApi(
  send: Send,
  profileListeners: Set<(change?: { availability: string }) => void>,
) {
  async function readProfilePage(settings: { before?: string; signal?: AbortSignal } = {}) {
    const suffix =
      settings.before === undefined ? "" : `?before=${identity(settings.before, "before")}`;
    return projectProfiles(
      (
        await send("GET", `/api/v1/profiles${suffix}`, {
          signal: settings.signal,
        })
      ).input as Api.Profiles,
    );
  }

  async function readAllProfiles(signal?: AbortSignal) {
    const pages: Array<ReturnType<typeof projectProfiles>> = [];
    const providers = new Map<string, ReturnType<typeof projectProfiles>["providers"][number]>();
    const executors = new Map<
      string,
      ReturnType<typeof projectProfiles>["executor_protocols"][number]
    >();
    let before: string | undefined;
    for (;;) {
      const page = await readProfilePage({ before, signal });
      pages.push(page);
      for (const executor of page.executor_protocols) {
        const native =
          (page as typeof page & { resource_model?: string }).resource_model === "independent";
        executors.set(native ? `${executor.id}/${executor.protocol}` : executor.id, executor);
      }
      for (const provider of page.providers) {
        const key = provider.id;
        const merged = providers.get(key);
        if (!merged) {
          providers.set(key, {
            ...provider,
            profile_ids: [...provider.profile_ids],
            models: [...provider.models],
          });
          continue;
        }
        for (const id of provider.profile_ids) {
          if (!merged.profile_ids.includes(id)) merged.profile_ids.push(id);
        }
        for (const model of provider.models) {
          if (!merged.models.includes(model)) merged.models.push(model);
        }
        merged.has_credential ||= provider.has_credential;
      }
      if (!page.has_more) break;
      if (!page.next_before || (before !== undefined && page.next_before >= before)) {
        throw new ContractError("next_before", "an advancing Profile cursor");
      }
      before = page.next_before;
    }
    const first = pages[0];
    const native =
      (first as (typeof first & { resource_model?: string }) | undefined)?.resource_model ===
      "independent";
    return {
      profile_id: first?.profile_id ?? null,
      llm_profiles: pages
        .flatMap((page) => page.llm_profiles)
        .map((profile) => ({
          ...profile,
          executors:
            native && !profile.enabled
              ? []
              : [...executors.values()]
                  .filter((executor) => executor.protocol === profile.protocol)
                  .map((executor) => executor.id)
                  .sort(),
        })),
      agent_profiles: pages.flatMap((page) => page.agent_profiles),
      decision_profiles: pages.flatMap((page) => page.decision_profiles),
      decision_agent_profiles: pages.flatMap((page) => page.decision_agent_profiles),
      executor_protocols: [...executors.values()],
      providers: [...providers.values()],
      has_more: false,
      next_before: null,
    };
  }

  return {
    onProfilesChanged(listener: (change?: { availability: string }) => void) {
      profileListeners.add(listener);
      return () => {
        profileListeners.delete(listener);
      };
    },
    async readAgentProfiles(
      settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
    ) {
      const query: string[] = [];
      if (settings.before !== undefined)
        query.push(`before=${identity(settings.before, "before")}`);
      if (settings.limit !== undefined) {
        if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
          throw new ContractError("limit", "an integer from 1 to 100");
        }
        query.push(`limit=${settings.limit}`);
      }
      const suffix = query.length ? `?${query.join("&")}` : "";
      return projectAgentProfiles(
        (
          await send("GET", `/api/v1/agent-profiles${suffix}`, {
            signal: settings.signal,
          })
        ).input as Api.AgentProfiles,
      );
    },
    async readProfiles(signal?: AbortSignal) {
      return readAllProfiles(signal);
    },

    async discoverProviderModels(
      connection: {
        protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
        endpoint: string;
        credential_ref?: string | null;
        credential?: string;
        headers?: Api.OutboundHeaderInput[];
        profile_id?: string;
      },
      signal?: AbortSignal,
    ) {
      const answer = await send("POST", "/api/v1/provider-models", {
        body: connection,
        sign: true,
        signal,
      });
      const result = answer.input as Api.ProviderModels;
      profileListeners.forEach((listener) => listener());
      return result;
    },

    async importModels(
      source:
        | string
        | {
            protocol?: "openai_chat_completions" | "openai_responses" | "anthropic_messages";
            name: string;
            endpoint: string;
            credential?: string;
            credential_ref?: string;
            headers?: Api.OutboundHeaderInput[];
          },
      models: Api.ProviderModel[],
      capabilities: Capability[],
    ) {
      if (typeof source === "string") identity(source, "source_profile_id");
      const connection =
        typeof source === "string" ? { source_profile_id: source } : { provider: source };
      const answer = await send("POST", "/api/v1/profiles/import", {
        body: { ...connection, models, declared_capabilities: capabilities },
        sign: true,
      });
      const result = answer.input as Api.ImportedModels;
      profileListeners.forEach((listener) => listener());
      return result;
    },

    async readActiveSystemSettings(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/settings/system", { signal });
      return answer.input as Api.ActiveSystemSettings;
    },

    async readGlobalCompactionSettings(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/settings/compaction", {
        signal,
      });
      return answer.input as Api.GlobalCompactionSettings;
    },

    async saveGlobalCompactionSettings(settings: Api.GlobalCompactionSettings) {
      const answer = await send("POST", "/api/v1/settings/compaction", {
        body: {
          revision: settings.revision,
          prediction_enabled: settings.prediction_enabled,
          policy: settings.policy,
        },
        sign: true,
      });
      return answer.input as Api.GlobalCompactionSettings;
    },

    async readGlobalOutboundHeaders(signal?: AbortSignal) {
      const answer = await send("GET", "/api/v1/settings/outbound-headers", {
        signal,
      });
      return answer.input as Api.GlobalOutboundHeaders;
    },

    async saveGlobalOutboundHeaders(revision: number, headers: Api.OutboundHeaderInput[]) {
      const answer = await send("POST", "/api/v1/settings/outbound-headers", {
        body: { revision, headers },
        sign: true,
      });
      return answer.input as Api.GlobalOutboundHeaders;
    },

    async deleteLlmProfile(profileId: string) {
      identity(profileId, "profile");
      const answer = await send("DELETE", `/api/v1/profiles/${profileId}`, {
        sign: true,
      });
      projectProfiles(answer.input as Api.Profiles);
      profileListeners.forEach((listener) => listener());
      return readAllProfiles();
    },

    async setModelEnabled(profileId: string, enabled: boolean) {
      identity(profileId, "profile");
      await send("POST", `/api/v1/profiles/${profileId}/availability`, {
        body: { enabled },
        sign: true,
      });
      profileListeners.forEach((listener) => listener({ availability: profileId }));
    },

    async removeProvider(providerId: string) {
      identity(providerId, "provider");
      await send("DELETE", `/api/v1/providers/${providerId}`, { sign: true });
      profileListeners.forEach((listener) => listener());
    },

    async createProfile(profile: ProfileWriteRequest, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/profiles", {
        body: profile,
        sign: true,
        signal,
      });
      const saved = projectProfiles(answer.input as Api.Profiles).profile_id;
      const snapshot = await readAllProfiles(signal);
      profileListeners.forEach((listener) => listener());
      return { ...snapshot, profile_id: saved };
    },

    async updateProfile(profileId: string, profile: ProfileWriteRequest, signal?: AbortSignal) {
      identity(profileId, "profile");
      const answer = await send("POST", `/api/v1/profiles/${profileId}`, {
        body: profile,
        sign: true,
        signal,
      });
      const saved = projectProfiles(answer.input as Api.Profiles).profile_id;
      const snapshot = await readAllProfiles(signal);
      profileListeners.forEach((listener) => listener());
      return { ...snapshot, profile_id: saved };
    },
  };
}
