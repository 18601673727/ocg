/**
 * HTTP client for the setup / first-run control endpoints.
 *
 * All requests go through the loopback control server; the frontend never
 * talks to a provider directly. The client validates the base URL with the
 * same loopback guard as the profile client.
 */

import { isLoopbackControlUrl } from "../profile/profile-client";
import {
  decodeSetupConnectResponse,
  decodeSetupModelsResponse,
  decodeSetupBrowseResponse,
  decodeSetupProjectResponse,
} from "../contracts";
import type {
  SetupConnectResponse,
  SetupModelSelection,
  SetupModelsResponse,
  SetupBrowseResponse,
  SetupProjectResponse,
} from "../contracts";

function errorMessage(value: unknown, fallback: string): string {
  if (typeof value === "object" && value !== null) {
    const body = value as Record<string, unknown>;
    const error = body["error"];
    if (typeof error === "object" && error !== null) {
      const inner = error as Record<string, unknown>;
      if (typeof inner["message"] === "string") return inner["message"];
    }
  }
  return fallback;
}

export interface SetupClient {
  connectProvider(name: string, endpoint: string, apiKey: string): Promise<SetupConnectResponse>;
  refreshModels(providerKey: string, revision: string): Promise<SetupConnectResponse>;
  /** An absent `defaultModel` keeps the Profile's current default. */
  saveModels(providerKey: string, models: SetupModelSelection[], defaultModel: string | undefined, revision: string): Promise<SetupModelsResponse>;
  browseDirectory(path?: string): Promise<SetupBrowseResponse>;
  initProject(commandId: string, root: string): Promise<SetupProjectResponse>;
}

export function createSetupClient(baseUrl: string, fetchImpl: typeof fetch): SetupClient {
  if (!isLoopbackControlUrl(baseUrl)) {
    throw new Error("OCG setup endpoint must be an HTTP loopback URL");
  }
  const base = baseUrl.replace(/\/$/, "");

  async function post<T>(path: string, body: unknown, decoder: (input: unknown) => T): Promise<T> {
    const response = await fetchImpl(`${base}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const value: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(errorMessage(value, `Setup request failed (${response.status})`));
    }
    return decoder(value);
  }

  return {
    connectProvider(name, endpoint, apiKey) {
      return post("/api/v1/setup/connect", { name, endpoint, api_key: apiKey }, decodeSetupConnectResponse);
    },
    refreshModels(providerKey, revision) {
      return post("/api/v1/setup/refresh", { provider_key: providerKey, revision }, decodeSetupConnectResponse);
    },
    saveModels(providerKey, models, defaultModel, revision) {
      return post("/api/v1/setup/models", {
        provider_key: providerKey,
        models,
        default_model: defaultModel ?? null,
        revision,
      }, decodeSetupModelsResponse);
    },
    browseDirectory(path) {
      return post("/api/v1/setup/browse", { path: path ?? null }, decodeSetupBrowseResponse);
    },
    initProject(commandId, root) {
      return post("/api/v1/setup/project", { command_id: commandId, root }, decodeSetupProjectResponse);
    },
  };
}
