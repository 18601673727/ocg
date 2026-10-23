"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
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
} from "@/components/ocg/runtime/control-client";
import { cn } from "@/lib/utils";
import { Section, describeError } from "./settings-parts";
import { ProjectsSection } from "./projects-section";
import { ProviderModelImport } from "./provider-model-import";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { ProtocolSelect, type LlmProtocol } from "./protocol-select";
import { PoolsSection } from "./pools-section";
import { useTheme } from "../appearance/theme-provider";
import { LanguageSwitcher, useI18n, type I18nKey } from "../i18n";
import { PageTitle } from "../layout/page-title";

type Health = {
  host: string;
  database: { status: string; accepting_commands: boolean; readable: boolean };
};

const ALL_CAPABILITIES: readonly { value: ControlCapability; labelKey: I18nKey }[] = [
  { value: "chat", labelKey: "settings.llm.capabilityChat" },
  { value: "vision", labelKey: "settings.llm.capabilityVision" },
  { value: "tools", labelKey: "settings.llm.capabilityTools" },
  { value: "reasoning", labelKey: "settings.llm.capabilityReasoning" },
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
  const { t } = useI18n();
  return (
    <div className="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h3 className="text-sm font-medium">{label}</h3>
          <Pill tone={origin === "browser" ? "violet" : "sky"} className="text-xs normal-case">
            {origin === "browser" ? t("settings.origin.browser") : t("settings.origin.runtime")}
          </Pill>
        </div>
        <p className="mt-1 max-w-2xl text-[13px] leading-5 text-muted-foreground">{description}</p>
      </div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">{children}</div>
    </div>
  );
}

const value = "[overflow-wrap:anywhere] text-[13px] font-medium text-foreground";

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
  const { t } = useI18n();
  const theme = useTheme();
  const [snapshot, setSnapshot] = useState<ControlProfilesSnapshot | null>(null);
  const [loadingProfiles, setLoadingProfiles] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  // LLM Profile form state
  const [editingLlmId, setEditingLlmId] = useState<string | null>(null);
  const [llmName, setLlmName] = useState("");
  const [llmModel, setLlmModel] = useState("");
  const [llmProtocol, setLlmProtocol] = useState<LlmProtocol>("openai_chat_completions");
  const [llmEndpoint, setLlmEndpoint] = useState("");
  const [llmCredentialRef, setLlmCredentialRef] = useState("");
  // Write-only secret input: never populated from server responses and cleared after save.
  const [llmSecret, setLlmSecret] = useState("");
  const [llmCapabilities, setLlmCapabilities] = useState<ControlCapability[]>(["chat"]);
  const [llmTemperature, setLlmTemperature] = useState("");
  const [llmTopP, setLlmTopP] = useState("");
  const [llmMaxTokens, setLlmMaxTokens] = useState("");
  const [llmContextWindow, setLlmContextWindow] = useState("");
  const [deletingModel, setDeletingModel] = useState<ControlLlmProfile | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [llmSaving, setLlmSaving] = useState(false);
  const [llmError, setLlmError] = useState<string | null>(null);
  const [llmNotice, setLlmNotice] = useState<string | null>(null);
  const [catalogNotice, setCatalogNotice] = useState<string | null>(null);

  // Agent Profile form state
  const [editingAgentId, setEditingAgentId] = useState<string | null>(null);
  const [agentName, setAgentName] = useState("");
  const [agentInstructions, setAgentInstructions] = useState("");
  const [agentBehavior, setAgentBehavior] = useState<ControlAgentBehavior>("chat");
  const [agentToolGrant, setAgentToolGrant] = useState<Exclude<ControlToolGrant, null> | "none">("none");
  const [agentTargets, setAgentTargets] = useState<string[]>([]);
  const [agentDispatchTo, setAgentDispatchTo] = useState<string[] | undefined>();
  const [agentDefaultModel, setAgentDefaultModel] = useState<string | null | undefined>();
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
        setLoadError(describeError(error, t));
      }
    } finally {
      if (!signal?.aborted) {
        setLoadingProfiles(false);
      }
    }
  }, [client, t]);

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
          setLoadError(describeError(error, t));
          setLoadingProfiles(false);
        }
      }
    })();
    return () => {
      aborted = true;
      controller.abort();
    };
  }, [client, t]);

  function resetLlmForm() {
    setEditingLlmId(null);
    setLlmName("");
    setLlmModel("");
    setLlmProtocol("openai_chat_completions");
    setLlmEndpoint("");
    setLlmCredentialRef("");
    setLlmSecret("");
    setLlmCapabilities(["chat"]);
    setLlmTemperature("");
    setLlmTopP("");
    setLlmMaxTokens("");
    setLlmContextWindow("");
    setLlmError(null);
  }

  function startEditLlm(profile: ControlLlmProfile) {
    setEditingLlmId(profile.id);
    setLlmContextWindow(profile.context_window === null ? "" : String(profile.context_window));
    setLlmName(profile.name);
    setLlmModel(profile.model);
    setLlmProtocol(profile.protocol);
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
      setLlmError(t("settings.llm.validation.name"));
      return;
    }
    if (!trimmedModel) {
      setLlmError(t("settings.llm.validation.model"));
      return;
    }
    if (llmCapabilities.length === 0) {
      setLlmError(t("settings.llm.validation.capabilities"));
      return;
    }
    const parsedTemp = llmTemperature.trim() === "" ? null : Number(llmTemperature.trim());
    if (parsedTemp !== null && (!Number.isFinite(parsedTemp) || parsedTemp < 0 || parsedTemp > 2)) {
      setLlmError(t("settings.llm.validation.temperature"));
      return;
    }
    const parsedTopP = llmTopP.trim() === "" ? null : Number(llmTopP.trim());
    if (parsedTopP !== null && (!Number.isFinite(parsedTopP) || parsedTopP < 0 || parsedTopP > 1)) {
      setLlmError(t("settings.llm.validation.topP"));
      return;
    }
    const parsedMaxTokens = llmMaxTokens.trim() === "" ? null : Number(llmMaxTokens.trim());
    if (
      parsedMaxTokens !== null &&
      (!Number.isSafeInteger(parsedMaxTokens) || parsedMaxTokens < 1)
    ) {
      setLlmError(t("settings.llm.validation.maxTokens"));
      return;
    }

    const parsedContextWindow = llmContextWindow.trim() === "" ? null : Number(llmContextWindow.trim());
    if (parsedContextWindow !== null && (!Number.isSafeInteger(parsedContextWindow) || parsedContextWindow < 1 || parsedContextWindow > 4294967295)) {
      setLlmError(t("settings.llm.invalidContext")); return;
    }
    const priorParameters = snapshot?.llm_profiles.find((profile) => profile.id === editingLlmId)?.parameters;
    setLlmSaving(true);
    try {
      const payload = {
        kind: "llm" as const,
        name: trimmedName,
        model: trimmedModel,
        context_window: parsedContextWindow,
        protocol: llmProtocol,
        endpoint: llmEndpoint.trim() ? llmEndpoint.trim() : null,
        credential_ref: llmCredentialRef.trim() ? llmCredentialRef.trim() : null,
        credential: llmSecret.trim() ? llmSecret.trim() : null,
        declared_capabilities: llmCapabilities,
        parameters: {
          ...priorParameters,
          max_completion_tokens: parsedMaxTokens === null ? priorParameters?.max_completion_tokens : null,
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
      setLlmNotice(wasEditing ? t("settings.notice.updated", { id: savedId }) : t("settings.notice.created", { id: savedId }));
    } catch (error) {
      setLlmError(describeError(error, t));
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
    setAgentDispatchTo(undefined);
    setAgentDefaultModel(undefined);
    setAgentError(null);
  }

  function startEditAgent(agent: ControlManagedAgentProfile) {
    setEditingAgentId(agent.id);
    setAgentName(agent.name);
    setAgentInstructions(agent.instructions ?? "");
    setAgentBehavior(agent.behavior);
    setAgentToolGrant(agent.tool_grant ?? "none");
    setAgentTargets([...agent.llm_targets]);
    setAgentDispatchTo(agent.dispatch_to);
    setAgentDefaultModel(agent.default_model);
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
      setAgentError(t("settings.agent.validation.name"));
      return;
    }
    if (agentTargets.length === 0) {
      setAgentError(t("settings.agent.validation.targets"));
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
        dispatch_to: agentDispatchTo,
        default_model: agentDefaultModel && !agentTargets.includes(agentDefaultModel) ? null : agentDefaultModel,
      };
      const updated = editingAgentId
        ? await client.updateProfile(editingAgentId, payload)
        : await client.createProfile(payload);
      setSnapshot(updated);
      const savedId = updated.profile_id ?? editingAgentId ?? "Agent Profile";
      const wasEditing = editingAgentId !== null;
      resetAgentForm();
      setAgentNotice(wasEditing ? t("settings.notice.updated", { id: savedId }) : t("settings.notice.created", { id: savedId }));
    } catch (error) {
      setAgentError(describeError(error, t));
    } finally {
      setAgentSaving(false);
    }
  }

  const llmProfiles = snapshot?.llm_profiles ?? [];
  const agentProfiles = snapshot?.agent_profiles ?? [];
  const providers = snapshot?.providers ?? [];

  return (
    <PageSurface className="mx-auto w-full max-w-4xl">
      <Dialog open={deletingModel !== null} onOpenChange={(open) => { if (!open && !deleting) setDeletingModel(null); }}>
        <DialogContent showCloseButton={!deleting}>
          <DialogHeader>
            <DialogTitle>{t("settings.llm.deleteTitle")}</DialogTitle>
            <DialogDescription>{t("settings.llm.deleteDescription", { name: deletingModel?.name ?? "" })}</DialogDescription>
          </DialogHeader>
          {deleteError && <p role="alert" className="text-sm text-destructive">{deleteError}</p>}
          <DialogFooter>
            <Button variant="outline" disabled={deleting} onClick={() => setDeletingModel(null)}>{t("common.cancel")}</Button>
            <Button variant="destructive" disabled={deleting} onClick={() => {
              if (!client || !deletingModel) return;
              const id = deletingModel.id;
              setDeleting(true); setDeleteError(null);
              void client.deleteLlmProfile(id).then((updated) => {
                setSnapshot(updated);
                if (editingLlmId === id) resetLlmForm();
                setAgentTargets((current) => current.filter((target) => target !== id));
                setAgentDefaultModel((current) => current === id ? "" : current);
                setDeletingModel(null); setCatalogNotice(t("settings.llm.deleted"));
              }).catch((error) => setDeleteError(describeError(error, t))).finally(() => setDeleting(false));
            }}>{deleting ? t("settings.saving") : t("common.remove")}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <header className="mb-6 flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <PageTitle className="text-2xl font-semibold tracking-tight">{t("settings.title")}</PageTitle>
          <p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
            {t("settings.manage")}
          </p>
        </div>
      </header>
      <div className="flex min-w-0 flex-col gap-4">
        {client ? (
          <>
            <Section
              id="settings-llm-profiles"
              title={t("settings.llm.title")}
              description={t("settings.llm.description")}
              action={
                <div className="flex shrink-0 flex-wrap justify-end gap-2">
                <ProviderModelImport client={client} snapshot={snapshot} onImported={(message) => { setCatalogNotice(message); void refreshProfiles(); }} />
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-9 rounded-md tracking-normal normal-case"
                  disabled={loadingProfiles}
                  onClick={() => void refreshProfiles()}
                >
                  {loadingProfiles ? t("settings.llm.refreshing") : t("common.refresh")}
                </Button>
                </div>
              }
            >
              {loadError ? (
                <DismissibleNotice message={loadError}  className="py-2 text-sm text-destructive">
                  {loadError}
                </DismissibleNotice>
              ) : null}
              {catalogNotice && <p role="status" className="pt-3 text-sm text-emerald-700 dark:text-emerald-400">{catalogNotice}</p>}
              <div className="divide-y divide-border/70 py-2">
                {llmProfiles.length === 0 ? (
                  <p className="py-2 text-sm text-muted-foreground">
                    {t("settings.llm.empty")}
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
                            {profile.has_credential ? t("settings.credentialReady") : t("settings.noCredential")}
                          </Pill>
                          {!profile.editable ? (
                            <Pill tone="amber" className="text-xs normal-case">
                              {t("settings.locked", { jobs: profile.blocking_jobs.join(", ") })}
                            </Pill>
                          ) : null}
                        </div>
                        <p className="text-xs text-muted-foreground">
                          {t("settings.llm.protocol", {
                            protocol: profile.protocol,
                            endpoint: profile.endpoint ?? t("settings.llm.defaultEndpoint"),
                          })}
                          {profile.credential_ref
                            ? ` · ${t("settings.credentialRef", { ref: profile.credential_ref })}`
                            : ""}
                        </p>
                        <div className="flex flex-wrap items-center gap-1.5 pt-0.5">
                          {profile.declared_capabilities.map((cap) => (
                            <Pill key={cap} tone="violet" className="text-[11px] normal-case">
                              {cap}
                            </Pill>
                          ))}
                          {profile.executors.length > 0 ? (
                            <span className="text-xs text-muted-foreground">
                              · {t("settings.llm.executors", { executors: profile.executors.join(", ") })}
                            </span>
                          ) : null}
                        </div>
                      </div>
                      <div className="flex shrink-0 gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          size="xs"
                          disabled={!profile.editable}
                          onClick={() => startEditLlm(profile)}
                        >
                          {editingLlmId === profile.id ? t("settings.editing") : t("common.edit")}
                        </Button>
                        <Button type="button" variant="ghost" size="xs" disabled={llmSaving || deleting} onClick={() => { setDeletingModel(profile); setDeleteError(null); }}>{t("common.remove")}</Button>
                      </div>
                    </div>
                  ))
                )}
              </div>

              <form onSubmit={(e) => void handleSaveLlm(e)} className="mt-2 flex flex-col gap-3 border-t border-border pt-4">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">
                    {editingLlmId
                      ? t("settings.llm.editTitle", { id: editingLlmId })
                      : t("settings.llm.createTitle")}
                  </h3>
                  {editingLlmId ? (
                    <Button type="button" variant="ghost" size="xs" onClick={resetLlmForm}>
                      {t("settings.cancelEdit")}
                    </Button>
                  ) : null}
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.name")}
                    <Input
                      value={llmName}
                      onChange={(e) => setLlmName(e.target.value)}
                      placeholder={t("settings.llm.namePlaceholder")}
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.model")}
                    <Input
                      value={llmModel}
                      onChange={(e) => setLlmModel(e.target.value)}
                      placeholder={t("settings.llm.modelPlaceholder")}
                      spellCheck={false}
                    />
                  </label>
                  <ProtocolSelect value={llmProtocol} onChange={setLlmProtocol} disabled={llmSaving} />
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
                    {t("settings.llm.endpoint")}
                    <Input
                      value={llmEndpoint}
                      onChange={(e) => setLlmEndpoint(e.target.value)}
                      placeholder={t("settings.llm.endpointPlaceholder")}
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.credentialReference")}
                    <Input
                      value={llmCredentialRef}
                      onChange={(e) => setLlmCredentialRef(e.target.value)}
                      placeholder={t("settings.llm.credentialPlaceholder")}
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.apiKey")}
                    <Input
                      type="password"
                      value={llmSecret}
                      onChange={(e) => setLlmSecret(e.target.value)}
                      placeholder={
                        editingLlmId
                          ? t("settings.llm.apiKeyKeep")
                          : t("settings.llm.apiKeyPlaceholder")
                      }
                      autoComplete="new-password"
                      spellCheck={false}
                    />
                  </label>
                </div>

                <fieldset className="flex flex-col gap-1.5">
                  <legend className="text-xs font-medium text-muted-foreground">{t("settings.llm.capabilities")}</legend>
                  <div className="flex flex-wrap gap-3 pt-1">
                    {ALL_CAPABILITIES.map((cap) => (
                      <label key={cap.value} className="flex items-center gap-1.5 text-sm">
                        <input
                          type="checkbox"
                          checked={llmCapabilities.includes(cap.value)}
                          onChange={() => toggleCapability(cap.value)}
                          className="size-4 accent-primary"
                        />
                        {t(cap.labelKey)}
                      </label>
                    ))}
                  </div>
                </fieldset>

                <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                  {t("settings.llm.contextWindow")}
                  <Input inputMode="numeric" value={llmContextWindow} onChange={(event) => setLlmContextWindow(event.target.value)} placeholder="128000" />
                </label>

                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.temperature")}
                    <Input
                      value={llmTemperature}
                      onChange={(e) => setLlmTemperature(e.target.value)}
                      placeholder="0.7"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.topP")}
                    <Input
                      value={llmTopP}
                      onChange={(e) => setLlmTopP(e.target.value)}
                      placeholder="1.0"
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                    {t("settings.llm.maxTokens")}
                    <Input
                      value={llmMaxTokens}
                      onChange={(e) => setLlmMaxTokens(e.target.value)}
                      placeholder="4096"
                      spellCheck={false}
                    />
                  </label>
                </div>

                {llmError ? (
                  <DismissibleNotice message={llmError}  className="text-sm text-destructive">
                    {llmError}
                  </DismissibleNotice>
                ) : null}
                {llmNotice ? (
                  <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
                    {llmNotice}
                  </p>
                ) : null}

                <div className="flex justify-end gap-2">
                  <Button type="submit" disabled={llmSaving}>
                    {llmSaving
                      ? t("settings.saving")
                      : editingLlmId
                        ? t("settings.llm.saveUpdate")
                        : t("settings.llm.saveCreate")}
                  </Button>
                </div>
              </form>
            </Section>

            <Section
              id="settings-agent-profiles"
              title={t("settings.agent.title")}
              description={t("settings.agent.description")}
            >
              <div className="divide-y divide-border/70 py-2">
                {agentProfiles.length === 0 ? (
                  <p className="py-2 text-sm text-muted-foreground">
                    {t("settings.agent.empty")}
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
                            <Pill tone="sky" className="text-xs capitalize">
                              {t("settings.agent.workspace", { grant: agent.tool_grant })}
                            </Pill>
                          ) : null}
                          {!agent.editable ? (
                            <Pill tone="amber" className="text-xs normal-case">
                              {t("settings.locked", { jobs: agent.blocking_jobs.join(", ") })}
                            </Pill>
                          ) : null}
                        </div>
                        <p className="text-xs text-muted-foreground">
                          {t("settings.agent.targets", { targets: agent.llm_targets.join(", ") })}
                        </p>
                        {agent.instructions ? (
                          <p className="line-clamp-2 whitespace-pre-wrap text-xs text-muted-foreground">
                            {t("settings.agent.instructions", { instructions: agent.instructions })}
                          </p>
                        ) : (
                          <p className="text-xs italic text-muted-foreground">{t("settings.agent.noInstructions")}</p>
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
                          {editingAgentId === agent.id ? t("settings.editing") : t("common.edit")}
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>

              <form onSubmit={(e) => void handleSaveAgent(e)} className="mt-2 flex flex-col gap-3 border-t border-border pt-4">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">
                    {editingAgentId
                      ? t("settings.agent.editTitle", { id: editingAgentId })
                      : t("settings.agent.createTitle")}
                  </h3>
                  {editingAgentId ? (
                    <Button type="button" variant="ghost" size="xs" onClick={resetAgentForm}>
                      {t("settings.cancelEdit")}
                    </Button>
                  ) : null}
                </div>
                <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                  {t("settings.agent.name")}
                  <Input
                    value={agentName}
                    onChange={(e) => setAgentName(e.target.value)}
                    placeholder={t("settings.agent.namePlaceholder")}
                    spellCheck={false}
                  />
                </label>
                <div className="flex flex-col gap-1.5">
                  <span className="text-xs font-medium text-muted-foreground">{t("settings.agent.behavior")}</span>
                  <ChoiceGroup
                    label={t("settings.agent.behavior")}
                    value={agentBehavior}
                    onChange={setAgentBehavior}
                    options={[
                      { value: "chat", label: t("settings.agent.behaviorChat") },
                      { value: "general", label: t("settings.agent.behaviorGeneral") },
                    ]}
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <span className="text-xs font-medium text-muted-foreground">{t("settings.agent.workspaceAccess")}</span>
                  <ChoiceGroup
                    label={t("settings.agent.workspaceAccess")}
                    value={agentToolGrant}
                    onChange={setAgentToolGrant}
                    options={[
                      { value: "none", label: t("settings.agent.grantNone") },
                      { value: "read", label: t("settings.agent.grantRead") },
                      { value: "edit", label: t("settings.agent.grantEdit") },
                      { value: "code", label: t("settings.agent.grantCode") },
                    ]}
                  />
                </div>
                <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                  {t("settings.agent.instructionsLabel")}
                  <Textarea
                    value={agentInstructions}
                    onChange={(e) => setAgentInstructions(e.target.value)}
                    placeholder={t("settings.agent.instructionsPlaceholder")}
                    rows={3}
                  />
                </label>
                <fieldset className="flex flex-col gap-1.5">
                  <legend className="text-xs font-medium text-muted-foreground">
                    {t("settings.agent.targetsLabel")}
                  </legend>
                  {llmProfiles.length === 0 ? (
                    <p className="text-xs text-muted-foreground">
                      {t("settings.agent.createLlmFirst")}
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
                  <DismissibleNotice message={agentError}  className="text-sm text-destructive">
                    {agentError}
                  </DismissibleNotice>
                ) : null}
                {agentNotice ? (
                  <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
                    {agentNotice}
                  </p>
                ) : null}

                <div className="flex justify-end gap-2">
                  <Button type="submit" disabled={agentSaving || llmProfiles.length === 0}>
                    {agentSaving
                      ? t("settings.saving")
                      : editingAgentId
                        ? t("settings.agent.saveUpdate")
                        : t("settings.agent.saveCreate")}
                  </Button>
                </div>
              </form>
            </Section>

            <Section
              id="settings-computed-providers"
              title={t("settings.providers.title")}
              description={t("settings.providers.description")}
            >
              {providers.length === 0 ? (
                <p className="py-3 text-sm text-muted-foreground">
                  {t("settings.providers.empty")}
                </p>
              ) : (
                <div className="divide-y divide-border/70 py-2">
                  {providers.map((provider, idx) => (
                    <div key={`${provider.protocol}-${provider.endpoint ?? "default"}-${provider.credential_ref ?? "none"}-${idx}`} className="flex flex-col gap-1 py-2.5 text-xs">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-semibold text-foreground">
                          {provider.endpoint ?? t("settings.llm.defaultEndpoint")}
                        </span>
                        <Pill tone="sky" className="text-[11px] normal-case">{provider.protocol}</Pill>
                        <Pill tone={provider.has_credential ? "emerald" : "amber"} className="text-[11px] normal-case">
                          {provider.has_credential ? t("settings.credentialReady") : t("settings.noCredential")}
                        </Pill>
                      </div>
                      <p className="text-muted-foreground">
                        {t("settings.providers.derivation", {
                          models: provider.models.join(", "),
                          profiles: provider.profile_ids.join(", "),
                        })}
                        {provider.credential_ref
                          ? ` · ${t("settings.credentialRef", { ref: provider.credential_ref })}`
                          : ""}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </Section>

            <ProjectsSection client={client} />
            <PoolsSection client={client} />
          </>
        ) : null}

        <Section
          id="settings-appearance"
          title={t("settings.section.appearance")}
          description={t("settings.appearance.description")}
          action={
            <Button
              variant="ghost"
              size="sm"
              className="h-11 rounded-md tracking-normal normal-case"
              onClick={theme.resetAppearance}
            >
              {t("common.reset")}
            </Button>
          }
        >
          <SettingRow label={t("settings.language.label")} description={t("settings.language.desc")} origin="browser"><LanguageSwitcher /></SettingRow>
          <SettingRow label={t("settings.theme.label")} description={t("settings.theme.desc")} origin="browser">
            <ChoiceGroup
              label={t("settings.theme.label")}
              value={theme.preferences.theme}
              onChange={theme.setTheme}
              options={[
                { value: "system", label: t("settings.theme.system") },
                { value: "light", label: t("settings.theme.light") },
                { value: "dark", label: t("settings.theme.dark") },
              ]}
            />
          </SettingRow>
          <SettingRow label={t("settings.accent.label")} description={t("settings.accent.desc")} origin="browser">
            <ChoiceGroup
              label={t("settings.accent.label")}
              value={theme.preferences.accent}
              onChange={theme.setAccent}
              options={[
                { value: "ochre", label: t("settings.accent.ochre") },
                { value: "slate", label: t("settings.accent.slate") },
                { value: "teal", label: t("settings.accent.teal") },
              ]}
            />
          </SettingRow>
        </Section>
        <Section
          id="settings-session"
          title={t("settings.session.title")}
          description={t("settings.session.description")}
          action={
            <Button
              variant="outline"
              size="sm"
              className="h-11 rounded-md tracking-normal normal-case"
              disabled={signingOut}
              onClick={onSignOut}
            >
              {signingOut ? t("settings.session.signingOut") : t("settings.session.signOut")}
            </Button>
          }
        >
          <SettingRow
            label={t("settings.session.expires")}
            description={t("settings.session.expiresDescription")}
            origin="runtime"
          >
            <span className={value}>
              {sessionExpiresAtUs === null ? "—" : new Date(sessionExpiresAtUs / 1000).toLocaleString()}
            </span>
          </SettingRow>
        </Section>
        <Section
          id="settings-control-plane"
          title={t("settings.control.title")}
          description={t("settings.control.description")}
        >
          <SettingRow label={t("settings.control.host")} description={t("settings.control.hostDescription")} origin="runtime">
            <span className={value}>{health ? health.host : t("settings.control.unreachable")}</span>
          </SettingRow>
          <SettingRow label={t("settings.control.database")} description={t("settings.control.databaseDescription")} origin="runtime">
            <span className={value}>{health ? health.database.status : t("common.unknown")}</span>
          </SettingRow>
          <SettingRow label={t("settings.control.reads")} description={t("settings.control.readsDescription")} origin="runtime">
            <span className={value}>
              {health
                ? health.database.readable
                  ? t("settings.control.available")
                  : t("settings.control.unavailable")
                : t("common.unknown")}
            </span>
          </SettingRow>
          <SettingRow label={t("settings.control.commands")} description={t("settings.control.commandsDescription")} origin="runtime">
            <span className={value}>
              {health
                ? health.database.accepting_commands
                  ? t("settings.control.accepting")
                  : t("settings.control.notAccepting")
                : t("common.unknown")}
            </span>
          </SettingRow>
        </Section>
      </div>
    </PageSurface>
  );
}
