"use client";

import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { PageSurface, Pill } from "@/components/ocg/primitives";
import type {
  ControlAgentBehavior,
  ControlToolGrant,
  ControlCapability,
  ControlLlmProfile,
  ControlManagedAgentProfile,
  ControlProfilesSnapshot,
} from "@/components/ocg/contracts";
import {
  ControlTransportError,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/canonical-client";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";

type Health = {
  host: string;
  database: { status: string; accepting_commands: boolean; readable: boolean };
};

const ALL_CAPABILITIES: readonly { value: ControlCapability; label: string }[] = [
  { value: "chat", label: "Chat" },
  { value: "vision", label: "Vision" },
  { value: "tools", label: "Tools" },
  { value: "reasoning", label: "Reasoning" },
];

function ChoiceGroup<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: readonly { value: T; label: string }[];
  onChange: (value: T) => void;
}) {
  return (
    <div className="flex flex-wrap gap-2" role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={value === option.value}
          onClick={() => onChange(option.value)}
          className={cn(
            "min-h-11 rounded-md border px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
            value === option.value
              ? "border-primary/50 bg-primary/10 font-medium text-foreground"
              : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground",
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

function SettingRow({
  label,
  description,
  origin,
  children,
}: {
  label: string;
  description: string;
  origin: "browser" | "runtime";
  children: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h3 className="text-sm font-medium">{label}</h3>
          <Pill tone={origin === "browser" ? "violet" : "sky"} className="text-xs normal-case">
            {origin === "browser" ? "Browser only" : "Reported by OCG"}
          </Pill>
        </div>
        <p className="mt-1 max-w-2xl text-[13px] leading-5 text-muted-foreground">{description}</p>
      </div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">{children}</div>
    </div>
  );
}

function Section({
  id,
  title,
  description,
  action,
  children,
}: {
  id: string;
  title: string;
  description: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section
      aria-labelledby={id}
      className="rounded-lg border border-border bg-background px-3 py-2.5 sm:px-4"
    >
      <div className="flex items-start justify-between gap-3 border-b border-border pb-3">
        <div className="min-w-0">
          <h2 id={id} className="text-base font-semibold tracking-tight">{title}</h2>
          <p className="mt-1 text-[13px] leading-5 text-muted-foreground">{description}</p>
        </div>
        {action}
      </div>
      <div>{children}</div>
    </section>
  );
}

const value = "[overflow-wrap:anywhere] text-[13px] font-medium text-foreground";

function describeError(error: unknown): string {
  if (error instanceof ControlTransportError) {
    return error.detail
      ? `${error.detail} (${error.code})`
      : `Request refused (${error.code}, HTTP ${error.status}).`;
  }
  return error instanceof Error ? error.message : "The request could not be completed.";
}

export function SettingsSurface({
  client,
  health,
  sessionExpiresAtUs,
  signingOut,
  onSignOut,
}: {
  client?: HttpCanonicalControlClient;
  health: Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  onSignOut: () => void;
}) {
  const theme = useTheme();
  const [snapshot, setSnapshot] = useState<ControlProfilesSnapshot | null>(null);
  const [loadingProfiles, setLoadingProfiles] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  // LLM Profile form state
  const [editingLlmId, setEditingLlmId] = useState<string | null>(null);
  const [llmName, setLlmName] = useState("");
  const [llmModel, setLlmModel] = useState("");
  const [llmEndpoint, setLlmEndpoint] = useState("");
  const [llmCredentialRef, setLlmCredentialRef] = useState("");
  // Write-only secret input: never populated from server responses and cleared after save.
  const [llmSecret, setLlmSecret] = useState("");
  const [llmCapabilities, setLlmCapabilities] = useState<ControlCapability[]>(["chat"]);
  const [llmTemperature, setLlmTemperature] = useState("");
  const [llmTopP, setLlmTopP] = useState("");
  const [llmMaxTokens, setLlmMaxTokens] = useState("");
  const [llmSaving, setLlmSaving] = useState(false);
  const [llmError, setLlmError] = useState<string | null>(null);
  const [llmNotice, setLlmNotice] = useState<string | null>(null);

  // Agent Profile form state
  const [editingAgentId, setEditingAgentId] = useState<string | null>(null);
  const [agentName, setAgentName] = useState("");
  const [agentInstructions, setAgentInstructions] = useState("");
  const [agentBehavior, setAgentBehavior] = useState<ControlAgentBehavior>("chat");
  const [agentToolGrant, setAgentToolGrant] = useState<Exclude<ControlToolGrant, null> | "none">("none");
  const [agentTargets, setAgentTargets] = useState<string[]>([]);
  const [agentSaving, setAgentSaving] = useState(false);
  const [agentError, setAgentError] = useState<string | null>(null);
  const [agentNotice, setAgentNotice] = useState<string | null>(null);

  const refreshProfiles = useCallback(async (signal?: AbortSignal) => {
    if (!client) return;
    setLoadingProfiles(true);
    setLoadError(null);
    try {
      const loaded = await client.readProfiles(signal);
      if (!signal?.aborted) {
        setSnapshot(loaded);
      }
    } catch (error) {
      if (!signal?.aborted) {
        setLoadError(describeError(error));
      }
    } finally {
      if (!signal?.aborted) {
        setLoadingProfiles(false);
      }
    }
  }, [client]);

  useEffect(() => {
    if (!client) return;
    const controller = new AbortController();
    let aborted = false;
    (async () => {
      try {
        const loaded = await client.readProfiles(controller.signal);
        if (!aborted) {
          setSnapshot(loaded);
          setLoadingProfiles(false);
        }
      } catch (error) {
        if (!aborted) {
          setLoadError(describeError(error));
          setLoadingProfiles(false);
        }
      }
    })();
    return () => {
      aborted = true;
      controller.abort();
    };
  }, [client]);

  function resetLlmForm() {
    setEditingLlmId(null);
    setLlmName("");
    setLlmModel("");
    setLlmEndpoint("");
    setLlmCredentialRef("");
    setLlmSecret("");
    setLlmCapabilities(["chat"]);
    setLlmTemperature("");
    setLlmTopP("");
    setLlmMaxTokens("");
    setLlmError(null);
  }

  function startEditLlm(profile: ControlLlmProfile) {
    setEditingLlmId(profile.id);
    setLlmName(profile.name);
    setLlmModel(profile.model);
    setLlmEndpoint(profile.endpoint ?? "");
    setLlmCredentialRef(profile.credential_ref ?? "");
    // Never populate raw credentials back into the browser.
    setLlmSecret("");
    setLlmCapabilities(profile.declared_capabilities.length > 0 ? [...profile.declared_capabilities] : ["chat"]);
    setLlmTemperature(profile.parameters.temperature !== null ? String(profile.parameters.temperature) : "");
    setLlmTopP(profile.parameters.top_p !== null ? String(profile.parameters.top_p) : "");
    setLlmMaxTokens(profile.parameters.max_tokens !== null ? String(profile.parameters.max_tokens) : "");
    setLlmError(null);
    setLlmNotice(null);
  }

  function toggleCapability(cap: ControlCapability) {
    setLlmCapabilities((current) =>
      current.includes(cap) ? current.filter((entry) => entry !== cap) : [...current, cap],
    );
  }

  async function handleSaveLlm(event: FormEvent) {
    event.preventDefault();
    if (!client) return;
    setLlmError(null);
    setLlmNotice(null);

    const trimmedName = llmName.trim();
    const trimmedModel = llmModel.trim();
    if (!trimmedName) {
      setLlmError("Profile name is required.");
      return;
    }
    if (!trimmedModel) {
      setLlmError("Model identifier is required.");
      return;
    }
    if (llmCapabilities.length === 0) {
      setLlmError("Select at least one model capability.");
      return;
    }
    const parsedTemp = llmTemperature.trim() === "" ? null : Number(llmTemperature.trim());
    if (parsedTemp !== null && (!Number.isFinite(parsedTemp) || parsedTemp < 0 || parsedTemp > 2)) {
      setLlmError("Temperature must be a number between 0 and 2.");
      return;
    }
    const parsedTopP = llmTopP.trim() === "" ? null : Number(llmTopP.trim());
    if (parsedTopP !== null && (!Number.isFinite(parsedTopP) || parsedTopP < 0 || parsedTopP > 1)) {
      setLlmError("Top-p must be a number between 0 and 1.");
      return;
    }
    const parsedMaxTokens = llmMaxTokens.trim() === "" ? null : Number(llmMaxTokens.trim());
    if (
      parsedMaxTokens !== null &&
      (!Number.isSafeInteger(parsedMaxTokens) || parsedMaxTokens < 1)
    ) {
      setLlmError("Max tokens must be a positive integer.");
      return;
    }

    setLlmSaving(true);
    try {
      const payload = {
        kind: "llm" as const,
        name: trimmedName,
        model: trimmedModel,
        protocol: "openai_chat_completions" as const,
        endpoint: llmEndpoint.trim() ? llmEndpoint.trim() : null,
        credential_ref: llmCredentialRef.trim() ? llmCredentialRef.trim() : null,
        credential: llmSecret.trim() ? llmSecret.trim() : null,
        declared_capabilities: llmCapabilities,
        parameters: {
          temperature: parsedTemp,
          top_p: parsedTopP,
          max_tokens: parsedMaxTokens,
        },
      };
      const updated = editingLlmId
        ? await client.updateProfile(editingLlmId, payload)
        : await client.createProfile(payload);
      setSnapshot(updated);
      const savedId = updated.profile_id ?? editingLlmId ?? "LLM Profile";
      const wasEditing = editingLlmId !== null;
      resetLlmForm();
      setLlmNotice(`${savedId} ${wasEditing ? "updated" : "created"}.`);
    } catch (error) {
      setLlmError(describeError(error));
      if (error instanceof ControlTransportError && error.code === "credential_not_saved") {
        // The Profile was committed even though its key was not; refresh so
        // the catalog shows the saved Profile, and keep the form open so the
        // key can be saved again.
        await refreshProfiles();
      }
    } finally {
      setLlmSaving(false);
    }
  }

  function resetAgentForm() {
    setEditingAgentId(null);
    setAgentName("");
    setAgentInstructions("");
    setAgentBehavior("chat");
    setAgentToolGrant("none");
    setAgentTargets([]);
    setAgentError(null);
  }

  function startEditAgent(agent: ControlManagedAgentProfile) {
    setEditingAgentId(agent.id);
    setAgentName(agent.name);
    setAgentInstructions(agent.instructions ?? "");
    setAgentBehavior(agent.behavior);
    setAgentToolGrant(agent.tool_grant ?? "none");
    setAgentTargets([...agent.llm_targets]);
    setAgentError(null);
    setAgentNotice(null);
  }

  function toggleAgentTarget(profileId: string) {
    setAgentTargets((current) =>
      current.includes(profileId)
        ? current.filter((entry) => entry !== profileId)
        : [...current, profileId],
    );
  }

  async function handleSaveAgent(event: FormEvent) {
    event.preventDefault();
    if (!client) return;
    setAgentError(null);
    setAgentNotice(null);

    const trimmedName = agentName.trim();
    if (!trimmedName) {
      setAgentError("Agent Profile name is required.");
      return;
    }
    if (agentTargets.length === 0) {
      setAgentError("Select at least one target LLM Profile.");
      return;
    }

    setAgentSaving(true);
    try {
      const payload = {
        kind: "agent" as const,
        name: trimmedName,
        instructions: agentInstructions.trim() ? agentInstructions.trim() : null,
        behavior: agentBehavior,
        tool_grant: agentToolGrant === "none" ? null : agentToolGrant,
        llm_targets: agentTargets,
      };
      const updated = editingAgentId
        ? await client.updateProfile(editingAgentId, payload)
        : await client.createProfile(payload);
      setSnapshot(updated);
      const savedId = updated.profile_id ?? editingAgentId ?? "Agent Profile";
      const wasEditing = editingAgentId !== null;
      resetAgentForm();
      setAgentNotice(`${savedId} ${wasEditing ? "updated" : "created"}.`);
    } catch (error) {
      setAgentError(describeError(error));
    } finally {
      setAgentSaving(false);
    }
  }

  const llmProfiles = snapshot?.llm_profiles ?? [];
  const agentProfiles = snapshot?.agent_profiles ?? [];
  const providers = snapshot?.providers ?? [];

  return (
    <PageSurface className="mx-auto w-full max-w-4xl">
      <header className="mb-6 flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
          <p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
            Manage LLM Profiles, Agent Profiles, credentials, and browser preferences.
          </p>
        </div>
        <div role="status" className="rounded-lg border border-border bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
          <p className="font-medium text-foreground">
            {theme.resolvedTheme === "dark" ? "Dark" : "Light"} theme active
          </p>
          <p className="mt-1">{theme.hydrated ? "Saved in this browser" : "Loading preferences…"}</p>
        </div>
      </header>
      <div className="flex min-w-0 flex-col gap-4">
        {client ? (
          <>
            <Section
              id="settings-llm-profiles"
              title="LLM Profiles"
              description="Persisted model configurations, protocol endpoints, capabilities, and write-only credentials."
              action={
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-9 rounded-md tracking-normal normal-case"
                  disabled={loadingProfiles}
                  onClick={() => void refreshProfiles()}
                >
                  {loadingProfiles ? "Refreshing…" : "Refresh"}
                </Button>
              }
            >
              {loadError ? (
                <p role="alert" className="py-2 text-sm text-destructive">
                  {loadError}
                </p>
              ) : null}
              <div className="divide-y divide-border/70 py-2">
                {llmProfiles.length === 0 ? (
                  <p className="py-2 text-sm text-muted-foreground">
                    No LLM Profile is configured yet. Create one below to configure a model and Provider settings.
                  </p>
                ) : (
                  llmProfiles.map((profile) => (
                    <div key={profile.id} className="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between">
                      <div className="min-w-0 space-y-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="text-sm font-semibold">{profile.name}</span>
                          <Pill tone="slate" className="text-xs normal-case">{profile.id}</Pill>
                          <Pill tone="sky" className="text-xs normal-case">{profile.model}</Pill>
                          <Pill tone={profile.has_credential ? "emerald" : "amber"} className="text-xs normal-case">
                            {profile.has_credential ? "Credential ready" : "No credential"}
                          </Pill>
                          {!profile.editable ? (
                            <Pill tone="amber" className="text-xs normal-case">
                              Locked ({profile.blocking_jobs.join(", ")})
                            </Pill>
                          ) : null}
                        </div>
                        <p className="text-xs text-muted-foreground">
                          Protocol: {profile.protocol} · Endpoint: {profile.endpoint ?? "https://api.openai.com/v1/chat/completions (default)"}
                          {profile.credential_ref ? ` · Credential ref: ${profile.credential_ref}` : ""}
                        </p>
                        <div className="flex flex-wrap items-center gap-1.5 pt-0.5">
                          {profile.declared_capabilities.map((cap) => (
                            <Pill key={cap} tone="violet" className="text-[11px] normal-case">
                              {cap}
                            </Pill>
                          ))}
                          {profile.executors.length > 0 ? (
                            <span className="text-xs text-muted-foreground">
                              · Executors: {profile.executors.join(", ")}
                            </span>
                          ) : null}
                        </div>
                      </div>
                      <div className="shrink-0">
                        <Button
                          type="button"
                          variant="outline"
                          size="xs"
                          disabled={!profile.editable}
                          onClick={() => startEditLlm(profile)}
                        >
                          {editingLlmId === profile.id ? "Editing" : "Edit"}
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>

              <form onSubmit={(e) => void handleSaveLlm(e)} className="mt-2 flex flex-col gap-3 border-t border-border pt-4">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">
                    {editingLlmId ? `Edit LLM Profile (${editingLlmId})` : "Create LLM Profile"}
                  </h3>
                  {editingLlmId ? (
                    <Button type="button" variant="ghost" size="xs" onClick={resetLlmForm}>
                      Cancel edit
                    </Button>
                  ) : null}
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    Profile Name
                    <Input
                      value={llmName}
                      onChange={(e) => setLlmName(e.target.value)}
                      placeholder="OpenAI GPT-4o"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    Model Identifier
                    <Input
                      value={llmModel}
                      onChange={(e) => setLlmModel(e.target.value)}
                      placeholder="gpt-4o"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
                    API Endpoint (HTTPS, optional — leave blank for default OpenAI endpoint)
                    <Input
                      value={llmEndpoint}
                      onChange={(e) => setLlmEndpoint(e.target.value)}
                      placeholder="https://api.openai.com/v1/chat/completions"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    Credential Reference (optional env/key name)
                    <Input
                      value={llmCredentialRef}
                      onChange={(e) => setLlmCredentialRef(e.target.value)}
                      placeholder="OPENAI_API_KEY"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    API Key Secret (write-only, never shown after save)
                    <Input
                      type="password"
                      value={llmSecret}
                      onChange={(e) => setLlmSecret(e.target.value)}
                      placeholder={editingLlmId ? "Leave blank to keep existing credential" : "sk-..."}
                      autoComplete="new-password"
                      spellCheck={false}
                    />
                  </label>
                </div>

                <fieldset className="flex flex-col gap-1.5">
                  <legend className="text-xs font-medium text-muted-foreground">Declared Capabilities</legend>
                  <div className="flex flex-wrap gap-3 pt-1">
                    {ALL_CAPABILITIES.map((cap) => (
                      <label key={cap.value} className="flex items-center gap-1.5 text-sm">
                        <input
                          type="checkbox"
                          checked={llmCapabilities.includes(cap.value)}
                          onChange={() => toggleCapability(cap.value)}
                          className="size-4 accent-primary"
                        />
                        {cap.label}
                      </label>
                    ))}
                  </div>
                </fieldset>

                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    Temperature (0..2, optional)
                    <Input
                      value={llmTemperature}
                      onChange={(e) => setLlmTemperature(e.target.value)}
                      placeholder="0.7"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    Top-P (0..1, optional)
                    <Input
                      value={llmTopP}
                      onChange={(e) => setLlmTopP(e.target.value)}
                      placeholder="1.0"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    Max Tokens (optional)
                    <Input
                      value={llmMaxTokens}
                      onChange={(e) => setLlmMaxTokens(e.target.value)}
                      placeholder="4096"
                      spellCheck={false}
                    />
                  </label>
                </div>

                {llmError ? (
                  <p role="alert" className="text-sm text-destructive">
                    {llmError}
                  </p>
                ) : null}
                {llmNotice ? (
                  <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
                    {llmNotice}
                  </p>
                ) : null}

                <div className="flex justify-end gap-2">
                  <Button type="submit" disabled={llmSaving}>
                    {llmSaving
                      ? "Saving…"
                      : editingLlmId
                        ? "Update LLM Profile"
                        : "Create LLM Profile"}
                  </Button>
                </div>
              </form>
            </Section>

            <Section
              id="settings-agent-profiles"
              title="Agent Profiles"
              description="Persisted Agent Profiles with system instructions and allowed target LLM Profiles."
            >
              <div className="divide-y divide-border/70 py-2">
                {agentProfiles.length === 0 ? (
                  <p className="py-2 text-sm text-muted-foreground">
                    No Agent Profile is configured yet. Create an LLM Profile first, then create an Agent Profile below.
                  </p>
                ) : (
                  agentProfiles.map((agent) => (
                    <div key={agent.id} className="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between">
                      <div className="min-w-0 space-y-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="text-sm font-semibold">{agent.name}</span>
                          <Pill tone="slate" className="text-xs normal-case">{agent.id}</Pill>
                          <Pill tone="sky" className="text-xs capitalize">{agent.behavior}</Pill>
                          {agent.tool_grant ? (
                            <Pill tone="sky" className="text-xs capitalize">Workspace: {agent.tool_grant}</Pill>
                          ) : null}
                          {!agent.editable ? (
                            <Pill tone="amber" className="text-xs normal-case">
                              Locked ({agent.blocking_jobs.join(", ")})
                            </Pill>
                          ) : null}
                        </div>
                        <p className="text-xs text-muted-foreground">
                          Target LLM Profiles: {agent.llm_targets.join(", ")}
                        </p>
                        {agent.instructions ? (
                          <p className="line-clamp-2 whitespace-pre-wrap text-xs text-muted-foreground">
                            Instructions: {agent.instructions}
                          </p>
                        ) : (
                          <p className="text-xs italic text-muted-foreground">No system instructions</p>
                        )}
                      </div>
                      <div className="shrink-0">
                        <Button
                          type="button"
                          variant="outline"
                          size="xs"
                          disabled={!agent.editable}
                          onClick={() => startEditAgent(agent)}
                        >
                          {editingAgentId === agent.id ? "Editing" : "Edit"}
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>

              <form onSubmit={(e) => void handleSaveAgent(e)} className="mt-2 flex flex-col gap-3 border-t border-border pt-4">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">
                    {editingAgentId ? `Edit Agent Profile (${editingAgentId})` : "Create Agent Profile"}
                  </h3>
                  {editingAgentId ? (
                    <Button type="button" variant="ghost" size="xs" onClick={resetAgentForm}>
                      Cancel edit
                    </Button>
                  ) : null}
                </div>
                <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                  Agent Name
                  <Input
                    value={agentName}
                    onChange={(e) => setAgentName(e.target.value)}
                    placeholder="Default Assistant"
                    spellCheck={false}
                  />
                </label>
                <div className="flex flex-col gap-1.5">
                  <span className="text-xs font-medium text-muted-foreground">Behavior</span>
                  <ChoiceGroup
                    label="Agent Behavior"
                    value={agentBehavior}
                    onChange={setAgentBehavior}
                    options={[
                      { value: "chat", label: "Chat" },
                      { value: "general", label: "General" },
                    ]}
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <span className="text-xs font-medium text-muted-foreground">Workspace access</span>
                  <ChoiceGroup
                    label="Workspace access"
                    value={agentToolGrant}
                    onChange={setAgentToolGrant}
                    options={[
                      { value: "none", label: "None" },
                      { value: "read", label: "Read" },
                      { value: "edit", label: "Edit" },
                      { value: "code", label: "Code" },
                    ]}
                  />
                </div>
                <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                  System Instructions (optional)
                  <Textarea
                    value={agentInstructions}
                    onChange={(e) => setAgentInstructions(e.target.value)}
                    placeholder="You are a concise, accurate assistant."
                    rows={3}
                  />
                </label>
                <fieldset className="flex flex-col gap-1.5">
                  <legend className="text-xs font-medium text-muted-foreground">
                    Target LLM Profiles
                  </legend>
                  {llmProfiles.length === 0 ? (
                    <p className="text-xs text-muted-foreground">
                      Create an LLM Profile above before creating an Agent Profile.
                    </p>
                  ) : (
                    <div className="flex flex-wrap gap-3 pt-1">
                      {llmProfiles.map((llm) => (
                        <label key={llm.id} className="flex items-center gap-1.5 text-sm">
                          <input
                            type="checkbox"
                            checked={agentTargets.includes(llm.id)}
                            onChange={() => toggleAgentTarget(llm.id)}
                            className="size-4 accent-primary"
                          />
                          {llm.name} ({llm.model}, {llm.id})
                        </label>
                      ))}
                    </div>
                  )}
                </fieldset>

                {agentError ? (
                  <p role="alert" className="text-sm text-destructive">
                    {agentError}
                  </p>
                ) : null}
                {agentNotice ? (
                  <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
                    {agentNotice}
                  </p>
                ) : null}

                <div className="flex justify-end gap-2">
                  <Button type="submit" disabled={agentSaving || llmProfiles.length === 0}>
                    {agentSaving
                      ? "Saving…"
                      : editingAgentId
                        ? "Update Agent Profile"
                        : "Create Agent Profile"}
                  </Button>
                </div>
              </form>
            </Section>

            <Section
              id="settings-computed-providers"
              title="Computed Providers"
              description="Read-only projection derived from LLM Profile configurations. Providers are not managed as a separate CRUD entity."
            >
              {providers.length === 0 ? (
                <p className="py-3 text-sm text-muted-foreground">
                  No Provider projection yet — create an LLM Profile above.
                </p>
              ) : (
                <div className="divide-y divide-border/70 py-2">
                  {providers.map((provider, idx) => (
                    <div key={`${provider.protocol}-${provider.endpoint ?? "default"}-${provider.credential_ref ?? "none"}-${idx}`} className="flex flex-col gap-1 py-2.5 text-xs">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-semibold text-foreground">
                          {provider.endpoint ?? "https://api.openai.com/v1/chat/completions (default)"}
                        </span>
                        <Pill tone="sky" className="text-[11px] normal-case">{provider.protocol}</Pill>
                        <Pill tone={provider.has_credential ? "emerald" : "amber"} className="text-[11px] normal-case">
                          {provider.has_credential ? "Credential ready" : "No credential"}
                        </Pill>
                      </div>
                      <p className="text-muted-foreground">
                        Models: {provider.models.join(", ")} · Derived from Profiles: {provider.profile_ids.join(", ")}
                        {provider.credential_ref ? ` · Credential ref: ${provider.credential_ref}` : ""}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </Section>
          </>
        ) : null}

        <Section
          id="settings-appearance"
          title="Appearance"
          description="Presentation preferences, kept in this browser alone."
          action={
            <Button
              variant="ghost"
              size="sm"
              className="h-11 rounded-md tracking-normal normal-case"
              onClick={theme.resetAppearance}
            >
              Reset
            </Button>
          }
        >
          <SettingRow label="Theme" description="Follow the system, or fix light or dark." origin="browser">
            <ChoiceGroup
              label="Theme"
              value={theme.preferences.theme}
              onChange={theme.setTheme}
              options={[
                { value: "system", label: "System" },
                { value: "light", label: "Light" },
                { value: "dark", label: "Dark" },
              ]}
            />
          </SettingRow>
          <SettingRow label="Density" description="How tightly surfaces are spaced." origin="browser">
            <ChoiceGroup
              label="Density"
              value={theme.preferences.density}
              onChange={theme.setDensity}
              options={[
                { value: "comfortable", label: "Comfortable" },
                { value: "compact", label: "Compact" },
              ]}
            />
          </SettingRow>
          <SettingRow label="Accent" description="One of the predefined accents." origin="browser">
            <ChoiceGroup
              label="Accent"
              value={theme.preferences.accent}
              onChange={theme.setAccent}
              options={[
                { value: "ochre", label: "Ochre" },
                { value: "slate", label: "Slate" },
                { value: "teal", label: "Teal" },
              ]}
            />
          </SettingRow>
        </Section>
        <Section
          id="settings-session"
          title="Session"
          description="This browser's sign-in to the local Control Plane."
          action={
            <Button
              variant="outline"
              size="sm"
              className="h-11 rounded-md tracking-normal normal-case"
              disabled={signingOut}
              onClick={onSignOut}
            >
              {signingOut ? "Signing out…" : "Sign out"}
            </Button>
          }
        >
          <SettingRow
            label="Signed in until"
            description="A session lasts eight hours and ends with the OCG process."
            origin="runtime"
          >
            <span className={value}>
              {sessionExpiresAtUs === null ? "—" : new Date(sessionExpiresAtUs / 1000).toLocaleString()}
            </span>
          </SettingRow>
        </Section>
        <Section
          id="settings-control-plane"
          title="Control Plane"
          description="What the host establishes about itself, read every few seconds."
        >
          <SettingRow label="Host" description="Whether the OCG host answers." origin="runtime">
            <span className={value}>{health ? health.host : "unreachable"}</span>
          </SettingRow>
          <SettingRow label="Database" description="The state of the persisted store." origin="runtime">
            <span className={value}>{health ? health.database.status : "unknown"}</span>
          </SettingRow>
          <SettingRow label="Reads" description="Whether Job records can be read." origin="runtime">
            <span className={value}>{health ? (health.database.readable ? "available" : "unavailable") : "unknown"}</span>
          </SettingRow>
          <SettingRow label="Commands" description="Whether new Jobs are accepted." origin="runtime">
            <span className={value}>
              {health ? (health.database.accepting_commands ? "accepting" : "not accepting") : "unknown"}
            </span>
          </SettingRow>
        </Section>
        <Section
          id="settings-unavailable"
          title="Not available in this build"
          description="0.4.5 settings that have no Control Plane contract in 0.5, so they are not shown as controls."
        >
          <ul className="flex list-disc flex-col gap-1.5 py-3 pl-5 text-[13px] leading-5 text-muted-foreground">
            <li>Workspace and startup preferences: no route owns them.</li>
            <li>Interface language: the 0.4.5 dictionaries describe the legacy surfaces and were not migrated.</li>
          </ul>
        </Section>
      </div>
    </PageSurface>
  );
}
