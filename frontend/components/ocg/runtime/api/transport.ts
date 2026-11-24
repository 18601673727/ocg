import { ContractError } from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { identity, sequence } from "./validators";

export class ApiError extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
    readonly retryable: boolean,
    readonly detail: string = "",
  ) {
    super(detail ? `${detail} (${code}, ${status})` : `OCG request: ${code} (${status})`);
    this.name = "ApiError";
  }
}

export type ApiClientOptions = {
  streamIdleMs?: number;
  baseUrl: string;
  fetch: typeof fetch;
  bearerToken?: () => string | Promise<string>;
  csrfToken?: () => string | Promise<string>;
};

function apiOrigin(raw: string, browserSession: boolean): string {
  const url = new URL(raw);
  const loopback =
    url.protocol === "http:" &&
    ["127.0.0.1", "localhost"].includes(url.hostname) &&
    Boolean(url.port);
  const secureBrowser =
    browserSession &&
    typeof window !== "undefined" &&
    url.protocol === "https:" &&
    window.location.origin === url.origin;
  if (
    (!loopback && !secureBrowser) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new ApiError("invalid_api_origin", 0, false);
  }
  if (typeof window !== "undefined" && window.location.origin !== url.origin) {
    throw new ApiError("same_origin_required", 0, false);
  }
  return url.origin;
}

export function retryable(status: number, code: string): boolean {
  return (
    status === 503 &&
    ["too_many_streams", "database_busy", "database_unavailable", "shutting_down"].includes(code)
  );
}

export function createTransport(options: ApiClientOptions) {
  const programmatic = options.bearerToken !== undefined;
  const base = apiOrigin(options.baseUrl, !programmatic);
  const fetchImpl = options.fetch.bind(globalThis);
  function bearerCredentials(token: string) {
    if (!/^[A-Za-z0-9._~+/=-]{32,512}$/.test(token)) {
      throw new ApiError("credential_unavailable", 0, false);
    }
    return { Authorization: `Bearer ${token}` };
  }
  async function credentials(): Promise<Record<string, string>> {
    return options.bearerToken ? bearerCredentials(await options.bearerToken()) : {};
  }
  type RequestSettings = {
    body?: unknown;
    key?: string;
    sign?: boolean;
    signal?: AbortSignal;
    context?: string;
    revision?: number;
    onUploadProgress?: (progress: number) => void;
  };
  function upload(
    url: string,
    body: string,
    headers: Record<string, string>,
    progress: (value: number) => void,
    signal?: AbortSignal,
  ): Promise<Response> {
    return new Promise((resolve, reject) => {
      signal?.throwIfAborted();
      const xhr = new XMLHttpRequest();
      xhr.open("POST", url);
      xhr.withCredentials = !programmatic;
      for (const [name, value] of Object.entries(headers)) xhr.setRequestHeader(name, value);
      const abort = () => xhr.abort();
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable && event.total > 0) progress(event.loaded / event.total);
      };
      xhr.upload.onload = () => progress(1);
      xhr.onload = () => {
        if (xhr.responseURL !== url) {
          reject(new ApiError("unexpected_redirect", 0, false));
          return;
        }
        const responseHeaders = new Headers();
        for (const line of xhr
          .getAllResponseHeaders()
          .trim()
          .split(/[\r\n]+/)) {
          const colon = line.indexOf(":");
          if (colon > 0) responseHeaders.append(line.slice(0, colon), line.slice(colon + 1).trim());
        }
        resolve(
          new Response(xhr.responseText || null, {
            status: xhr.status,
            headers: responseHeaders,
          }),
        );
      };
      xhr.onerror = () => reject(new ApiError("connection_lost", 0, true));
      xhr.onabort = () => reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
      xhr.onloadend = () => signal?.removeEventListener("abort", abort);
      signal?.addEventListener("abort", abort, { once: true });
      xhr.send(body);
    });
  }
  async function request(
    method: "GET" | "POST" | "DELETE",
    path: string,
    settings: RequestSettings,
    headers: Record<string, string>,
  ) {
    if (settings.body !== undefined) headers["Content-Type"] = "application/json";
    if (settings.key !== undefined) headers["Idempotency-Key"] = settings.key;
    if (settings.context !== undefined)
      headers["X-OCG-Composer-Context"] = identity(settings.context, "context");
    if (settings.revision !== undefined) headers["If-Match"] = String(sequence(settings.revision));
    if (settings.sign) {
      const csrf = await options.csrfToken?.();
      if (!csrf) throw new ApiError("credential_unavailable", 0, false);
      headers["X-OCG-CSRF"] = csrf;
    }
    let response: Response;
    try {
      const body = settings.body === undefined ? undefined : JSON.stringify(settings.body);
      response =
        method === "POST" && body !== undefined && settings.onUploadProgress
          ? await upload(
              `${base}${path}`,
              body,
              headers,
              settings.onUploadProgress,
              settings.signal,
            )
          : await fetchImpl(`${base}${path}`, {
              method,
              headers,
              signal: settings.signal,
              ...(body === undefined ? {} : { body }),
              mode: "same-origin",
              credentials: programmatic ? "omit" : "same-origin",
              cache: "no-store",
              redirect: "error",
            });
    } catch {
      throw new ApiError("connection_lost", 0, true);
    }
    if (!response.ok) {
      let code = "http_error";
      let detail = "";
      try {
        const decoded = ((await response.json()) as Api.ErrorResponse).error;
        code = decoded.code;
        detail = decoded.message;
      } catch {}
      throw new ApiError(code, response.status, retryable(response.status, code), detail);
    }
    let input: unknown;
    try {
      input = await response.json();
    } catch {
      throw new ContractError("response", "a complete JSON answer");
    }
    return { status: response.status, input, headers: response.headers };
  }
  async function send(method: "GET" | "POST" | "DELETE", path: string, settings: RequestSettings) {
    return request(method, path, settings, await credentials());
  }
  function exchangeSession(token: string, signal?: AbortSignal) {
    if (programmatic || typeof window === "undefined" || window.location.origin !== base) {
      throw new ApiError("browser_session_required", 0, false);
    }
    return request("GET", "/api/v1/session", { signal }, bearerCredentials(token.trim()));
  }
  return { options, base, programmatic, fetchImpl, credentials, send, exchangeSession };
}

export type Transport = ReturnType<typeof createTransport>;
export type Send = Transport["send"];
