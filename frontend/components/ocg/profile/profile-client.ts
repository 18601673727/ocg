/** The backend's OCG-owned Profile projection.
 *
 * The types are the generated projection of `src/contracts.rs`, so Rust is the
 * single source of truth for this schema: a field added, renamed or retyped on
 * the Rust side surfaces here as a type error rather than as `undefined` in a
 * rendered Profile. */
export type { Model, Origin, Profile, Provider, ProfileView } from "../contracts";

import type { Profile, ProfileView } from "../contracts";
import { decodeProfileView, ContractError } from "../contracts";
import { PROFILE_API_VERSION } from "../contracts";

export function runnableChoices(profile: Profile): string[] {
  return Object.entries(profile.models)
    .filter(([, model]) => !model.placeholder && profile.providers[model.provider]?.placeholder === false)
    .map(([key]) => key);
}

export function isLoopbackControlUrl(raw: string): boolean {
  try {
    const url = new URL(raw);
    return url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) && !url.username && !url.password && url.pathname === "/" && !url.search && !url.hash;
  } catch {
    return false;
  }
}

/** The message a failed control request should surface. */
function errorMessage(body: unknown, fallback: string): string {
  if (body && typeof body === "object" && "error" in body) {
    const envelope = (body as { error?: unknown }).error;
    if (envelope && typeof envelope === "object" && "message" in envelope) {
      const message = (envelope as { message?: unknown }).message;
      if (typeof message === "string") return message;
    }
  }
  return fallback;
}

export function createProfileClient(baseUrl: string, fetchImpl: typeof fetch) {
  if (!isLoopbackControlUrl(baseUrl)) throw new Error("OCG control endpoint must be an HTTP loopback URL");
  async function request(path: string, init?: RequestInit): Promise<ProfileView> {
    const response = await fetchImpl(`${baseUrl.replace(/\/$/, "")}${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", ...init?.headers },
    });
    const value: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(errorMessage(value, `OCG Profile request failed (${response.status})`));
    }
    // Every field is checked against the Rust contract before the Profile is
    // handed to a component. A payload from a different backend, or one whose
    // shape has drifted, is a visible error rather than a half-rendered view.
    try {
      return decodeProfileView(value);
    } catch (error) {
      if (error instanceof ContractError) {
        throw new Error(`Unsupported OCG Profile response (${PROFILE_API_VERSION}): ${error.message}`);
      }
      throw error;
    }
  }
  return {
    read: () => request("/api/v1/profile"),
    createNew: () => request("/api/v1/profile/bootstrap", { method: "POST", body: JSON.stringify({ choice: "new" }) }),
    replace: (revision: string, profile: Profile) => request("/api/v1/profile", {
      method: "PUT", body: JSON.stringify({ revision, profile }),
    }),
  };
}
