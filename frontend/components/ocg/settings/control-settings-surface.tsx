"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { PageSurface, Pill, SELECTED_OPTION } from "@/components/ocg/primitives";
import type {
  ControlAgentCapability,
  ControlCapability,
  ControlLlmProfile,
  ControlManagedAgentProfile,
  ControlProfilesSnapshot,
} from "@/components/ocg/contracts";
import type { DecisionLimits as ControlDecisionLimits } from "@/components/ocg/contracts/api";
import {
  ControlTransportError,
  type ControlProfileWriteRequest,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { cn } from "@/lib/utils";
import { Section, describeError } from "./settings-parts";
import { ProjectsSection } from "./projects-section";
import { ProviderModelImport } from "./provider-model-import";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ProtocolSelect, type LlmProtocol } from "./protocol-select";
import { GlobalCompactionSection } from "./global-compaction-section";
import { UpdatesSection } from "./updates-section";
import { DecisionProfilesSection } from "./decision-profiles-section";
import { PoolsSection } from "./pools-section";
import { useTheme } from "../appearance/theme-provider";
import { LanguageSwitcher, useI18n, type I18nKey } from "../i18n";
import { PageTitle } from "../layout/page-title";
import { SettingsNavigation } from "./settings-navigation";
import type { SettingsSection } from "./settings-sections";

const ALL_CAPABILITIES: readonly { value: ControlCapability; labelKey: I18nKey }[] = [
  { value: "chat", labelKey: "settings.llm.capabilityChat" },
  { value: "vision", labelKey: "settings.llm.capabilityVision" },
  { value: "tools", labelKey: "settings.llm.capabilityTools" },
  { value: "reasoning", labelKey: "settings.llm.capabilityReasoning" },
];
const DECISION_CAPABILITIES = ["choice", "binary_probability", "ordinal_score"] as const;
const EMPTY_DECISION_LIMITS: ControlDecisionLimits = {
  max_input_tokens_per_request: null,
  max_output_tokens_per_request: null,
  max_input_tokens_per_question: null,
  max_output_tokens_per_question: null,
};
type ModelKind = "llm" | "decision_model";
type DecisionModelProfile = ControlProfilesSnapshot["decision_profiles"][number];

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
              ? cn(SELECTED_OPTION, "font-medium")
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
  sessionExpiresAtUs,
  signingOut,
  onSignOut,
  section,
  onNavigate,
}: {
  client?: HttpCanonicalControlClient;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  onSignOut: () => void;
  section: SettingsSection;
  onNavigate(section: SettingsSection): void;
}) {
  const { t } = useI18n();
  const theme = useTheme();
  const [snapshot, setSnapshot] = useState<ControlProfilesSnapshot | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  // Model form state
  const [editingLlmId, setEditingLlmId] = useState<string | null>(null);
  const [llmFormOpen, setLlmFormOpen] = useState(false);
  const [modelKind, setModelKind] = useState<ModelKind>("llm");
  const [llmName, setLlmName] = useState("");
  const [llmModel, setLlmModel] = useState("");
  const [llmProtocol, setLlmProtocol] = useState<LlmProtocol>("openai_chat_completions");
  const [llmEndpoint, setLlmEndpoint] = useState("");
  const [llmCredentialRef, setLlmCredentialRef] = useState("");
  // Write-only secret input: never populated from server responses and cleared after save.
  const [llmSecret, setLlmSecret] = useState("");
  const [llmCapabilities, setLlmCapabilities] = useState<ControlCapability[]>(["chat"]);
  const [decisionCapabilities, setDecisionCapabilities] = useState<string[]>(["choice"]);
  const [decisionLimits, setDecisionLimits] =
    useState<ControlDecisionLimits>(EMPTY_DECISION_LIMITS);
  const [llmTemperature, setLlmTemperature] = useState("");
  const [llmTopP, setLlmTopP] = useState("");
  const [llmMaxTokens, setLlmMaxTokens] = useState("");
  const [llmReasoningEffort, setLlmReasoningEffort] = useState("");
  const [llmContextWindow, setLlmContextWindow] = useState("");
  const [deletingModel, setDeletingModel] = useState<
    ControlLlmProfile | DecisionModelProfile | null
  >(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [llmSaving, setLlmSaving] = useState(false);
  const [llmError, setLlmError] = useState<string | null>(null);
  const [catalogNotice, setCatalogNotice] = useState<string | null>(null);
  const [availabilityId, setAvailabilityId] = useState<string | null>(null);

  // Agent Profile form state
  const [editingAgentId, setEditingAgentId] = useState<string | null>(null);
  const [agentName, setAgentName] = useState("");
  const [agentInstructions, setAgentInstructions] = useState("");
  const [agentCapabilities, setAgentCapabilities] = useState<ControlAgentCapability[]>(["chat"]);
  const [agentTargets, setAgentTargets] = useState<string[]>([]);
  const [agentSaving, setAgentSaving] = useState(false);
  const [agentError, setAgentError] = useState<string | null>(null);
  const [agentNotice, setAgentNotice] = useState<string | null>(null);

  const refreshProfiles = useCallback(
    async (signal?: AbortSignal) => {
      if (!client) return;
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
      }
    },
    [client, t],
  );

  useEffect(() => {
    if (!client) return;
    const controller = new AbortController();
    let aborted = false;
    (async () => {
      try {
        const loaded = await client.readProfiles(controller.signal);
        if (!aborted) {
          setSnapshot(loaded);
        }
      } catch (error) {
        if (!aborted) {
          setLoadError(describeError(error, t));
        }
      }
    })();
    return () => {
      aborted = true;
      controller.abort();
    };
  }, [client, t]);

  useEffect(
    () =>
      client?.onProfilesChanged(() => {
        void refreshProfiles();
      }),
    [client, refreshProfiles],
  );

  function resetLlmForm() {
    setLlmFormOpen(false);
    setEditingLlmId(null);
    setModelKind("llm");
    setLlmName("");
    setLlmModel("");
    setLlmProtocol("openai_chat_completions");
    setLlmEndpoint("");
    setLlmCredentialRef("");
    setLlmSecret("");
    setLlmCapabilities(["chat"]);
    setDecisionCapabilities(["choice"]);
    setDecisionLimits(EMPTY_DECISION_LIMITS);
    setLlmTemperature("");
    setLlmTopP("");
    setLlmMaxTokens("");
    setLlmReasoningEffort("");
    setLlmContextWindow("");
    setLlmError(null);
  }

  function startEditLlm(profile: ControlLlmProfile) {
    setModelKind("llm");
    setEditingLlmId(profile.id);
    setLlmContextWindow(profile.context_window === null ? "" : String(profile.context_window));
    setLlmName(profile.name);
    setLlmModel(profile.model);
    setLlmProtocol(profile.protocol);
    setLlmEndpoint(profile.endpoint ?? "");
    setLlmCredentialRef(profile.credential_ref ?? "");
    // Never populate raw credentials back into the browser.
    setLlmSecret("");
    setLlmCapabilities(
      profile.declared_capabilities.length > 0 ? [...profile.declared_capabilities] : ["chat"],
    );
    setLlmTemperature(
      profile.parameters.temperature !== null ? String(profile.parameters.temperature) : "",
    );
    setLlmTopP(profile.parameters.top_p !== null ? String(profile.parameters.top_p) : "");
    setLlmMaxTokens(
      profile.parameters.max_tokens !== null ? String(profile.parameters.max_tokens) : "",
    );
    setLlmReasoningEffort(profile.parameters.reasoning_effort ?? "");
    setLlmError(null);
    setLlmFormOpen(true);
  }

  function startEditDecisionModel(profile: DecisionModelProfile) {
    resetLlmForm();
    setModelKind("decision_model");
    setEditingLlmId(profile.id);
    setLlmName(profile.name);
    setLlmModel(profile.model);
    setLlmEndpoint(profile.endpoint ?? "");
    setLlmCredentialRef(profile.credential_ref ?? "");
    setDecisionCapabilities(profile.declared_capabilities);
    setDecisionLimits(profile.limits);
    setLlmFormOpen(true);
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
    let payload: ControlProfileWriteRequest;
    if (modelKind === "decision_model") {
      if (decisionCapabilities.length === 0) {
        setLlmError(t("settings.llm.validation.capabilities"));
        return;
      }
      payload = {
        kind: "decision_model",
        name: trimmedName,
        model: trimmedModel,
        protocol: "typesafe_systemone",
        endpoint: llmEndpoint.trim(),
        credential_ref: llmCredentialRef.trim() ? llmCredentialRef.trim() : null,
        ...(llmSecret.trim() ? { credential: llmSecret.trim() } : {}),
        declared_capabilities: decisionCapabilities,
        limits: decisionLimits,
      };
    } else {
      if (llmCapabilities.length === 0) {
        setLlmError(t("settings.llm.validation.capabilities"));
        return;
      }
      const parsedTemp = llmTemperature.trim() === "" ? null : Number(llmTemperature.trim());
      if (
        parsedTemp !== null &&
        (!Number.isFinite(parsedTemp) || parsedTemp < 0 || parsedTemp > 2)
      ) {
        setLlmError(t("settings.llm.validation.temperature"));
        return;
      }
      const parsedTopP = llmTopP.trim() === "" ? null : Number(llmTopP.trim());
      if (
        parsedTopP !== null &&
        (!Number.isFinite(parsedTopP) || parsedTopP < 0 || parsedTopP > 1)
      ) {
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

      const parsedContextWindow =
        llmContextWindow.trim() === "" ? null : Number(llmContextWindow.trim());
      if (
        parsedContextWindow !== null &&
        (!Number.isSafeInteger(parsedContextWindow) ||
          parsedContextWindow < 1 ||
          parsedContextWindow > 4294967295)
      ) {
        setLlmError(t("settings.llm.invalidContext"));
        return;
      }
      const priorParameters = snapshot?.llm_profiles.find(
        (profile) => profile.id === editingLlmId,
      )?.parameters;
      payload = {
        kind: "llm",
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
          max_completion_tokens:
            parsedMaxTokens === null ? priorParameters?.max_completion_tokens : null,
          temperature: parsedTemp,
          top_p: parsedTopP,
          max_tokens: parsedMaxTokens,
          reasoning_effort: llmReasoningEffort || null,
        },
      };
    }
    setLlmSaving(true);
    try {
      const updated = editingLlmId
        ? await client.updateProfile(editingLlmId, payload)
        : await client.createProfile(payload);
      setSnapshot(updated);
      const savedId = updated.profile_id ?? editingLlmId ?? "Model";
      const wasEditing = editingLlmId !== null;
      resetLlmForm();
      setCatalogNotice(
        wasEditing
          ? t("settings.notice.updated", { id: savedId })
          : t("settings.notice.created", { id: savedId }),
      );
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
    setAgentCapabilities(["chat"]);
    setAgentTargets([]);
    setAgentError(null);
  }

  function startEditAgent(agent: ControlManagedAgentProfile) {
    setEditingAgentId(agent.id);
    setAgentName(agent.name);
    setAgentInstructions(agent.instructions ?? "");
    setAgentCapabilities([...agent.capabilities]);
    setAgentTargets([...agent.llm_targets]);
    setAgentError(null);
    setAgentNotice(null);
  }

  const assignableModels = [
    ...(snapshot?.llm_profiles ?? []).map((profile) => ({
      ...profile,
      kind: "generative" as const,
    })),
    ...(snapshot?.decision_profiles ?? []).map((profile) => ({
      ...profile,
      kind: "decision" as const,
    })),
  ];
  function supportsCapabilities(
    model: (typeof assignableModels)[number],
    capabilities: ControlAgentCapability[],
  ) {
    return (
      capabilities.length > 0 &&
      capabilities.every((capability) =>
        model.kind === "decision"
          ? capability === "decision" && model.declared_capabilities.length > 0
          : model.declared_capabilities.includes("chat") &&
            (capability !== "coding" || model.declared_capabilities.includes("tools")),
      )
    );
  }
  const eligibleModels = assignableModels.filter((model) =>
    supportsCapabilities(model, agentCapabilities),
  );
  const assignedModels = agentTargets.filter((id) =>
    eligibleModels.some((model) => model.id === id),
  );
  const unassignedModels = eligibleModels.filter(
    (model) => !assignedModels.includes(model.id) && (model.kind === "decision" || model.enabled),
  );
  function toggleAgentCapability(capability: ControlAgentCapability) {
    const next = agentCapabilities.includes(capability)
      ? agentCapabilities.filter((value) => value !== capability)
      : [...agentCapabilities, capability];
    setAgentCapabilities(next);
    setAgentTargets((current) =>
      current.filter((id) =>
        assignableModels.some((model) => model.id === id && supportsCapabilities(model, next)),
      ),
    );
  }
  function moveAssignedModel(index: number, offset: number) {
    const next = [...assignedModels];
    const destination = index + offset;
    if (destination < 0 || destination >= next.length) return;
    [next[index], next[destination]] = [next[destination], next[index]];
    setAgentTargets(next);
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
    if (!agentCapabilities.length || assignedModels.length === 0) {
      setAgentError(t("settings.agent.validation.targets"));
      return;
    }

    setAgentSaving(true);
    try {
      const payload = {
        kind: "agent" as const,
        name: trimmedName,
        instructions: agentInstructions.trim() ? agentInstructions.trim() : null,
        capabilities: agentCapabilities,
        llm_targets: assignedModels,
      };
      const updated = editingAgentId
        ? await client.updateProfile(editingAgentId, payload)
        : await client.createProfile(payload);
      setSnapshot(updated);
      const savedId = updated.profile_id ?? editingAgentId ?? "Agent Profile";
      const wasEditing = editingAgentId !== null;
      resetAgentForm();
      setAgentNotice(
        wasEditing
          ? t("settings.notice.updated", { id: savedId })
          : t("settings.notice.created", { id: savedId }),
      );
    } catch (error) {
      setAgentError(describeError(error, t));
    } finally {
      setAgentSaving(false);
    }
  }

  const llmProfiles = snapshot?.llm_profiles ?? [];
  const modelRows = [
    ...llmProfiles.map((profile) => ({ kind: "llm" as const, profile })),
    ...(snapshot?.decision_profiles ?? []).map((profile) => ({
      kind: "decision_model" as const,
      profile,
    })),
  ];
  const agentProfiles = snapshot?.agent_profiles ?? [];

  return (
    <PageSurface className="mx-auto w-full max-w-7xl">
      <Dialog
        open={deletingModel !== null}
        onOpenChange={(open) => {
          if (!open && !deleting) setDeletingModel(null);
        }}
      >
        <DialogContent showCloseButton={!deleting}>
          <DialogHeader>
            <DialogTitle>{t("settings.llm.deleteTitle")}</DialogTitle>
            <DialogDescription>
              {t("settings.llm.deleteDescription", { name: deletingModel?.name ?? "" })}
            </DialogDescription>
          </DialogHeader>
          {deleteError && (
            <p role="alert" className="text-sm text-destructive">
              {deleteError}
            </p>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={deleting} onClick={() => setDeletingModel(null)}>
              {t("common.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={deleting}
              onClick={() => {
                if (!client || !deletingModel) return;
                const id = deletingModel.id;
                setDeleting(true);
                setDeleteError(null);
                void client
                  .deleteLlmProfile(id)
                  .then((updated) => {
                    setSnapshot(updated);
                    if (editingLlmId === id) resetLlmForm();
                    setAgentTargets((current) => current.filter((target) => target !== id));
                    setDeletingModel(null);
                    setCatalogNotice(t("settings.llm.deleted"));
                  })
                  .catch((error) => setDeleteError(describeError(error, t)))
                  .finally(() => setDeleting(false));
              }}
            >
              {deleting ? t("settings.saving") : t("common.remove")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <header className="mb-8 flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <PageTitle className="text-2xl font-semibold tracking-tight">
            {t("settings.title")}
          </PageTitle>
          <p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
            {t("settings.manage")}
          </p>
        </div>
      </header>
      <div className="grid min-w-0 gap-6 lg:grid-cols-[13rem_minmax(0,1fr)] lg:gap-8">
        <SettingsNavigation section={section} onNavigate={onNavigate} />
        <div id="settings-content" className="flex min-w-0 scroll-mt-6 flex-col gap-4">
          {loadError && ["models", "agents", "decisions"].includes(section) && (
            <DismissibleNotice message={loadError} />
          )}
          {client ? (
            <>
              {section === "models" && (
                <Section
                  id="settings-llm-profiles"
                  title={t("settings.llm.importModels")}
                  description={t("settings.llm.importDescription")}
                  action={
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        resetLlmForm();
                        setLlmFormOpen(true);
                      }}
                    >
                      {t("settings.llm.add")}
                    </Button>
                  }
                >
                  <ProviderModelImport
                    client={client}
                    snapshot={snapshot}
                    onImported={(message) => {
                      setCatalogNotice(message);
                      void refreshProfiles();
                    }}
                  />
                  <h3 className="border-t border-border pt-4 text-sm font-semibold">
                    {t("settings.llm.title")}
                  </h3>
                  {catalogNotice && (
                    <p
                      role="status"
                      className="pt-3 text-sm text-emerald-700 dark:text-emerald-400"
                    >
                      {catalogNotice}
                    </p>
                  )}
                  <div className="divide-y divide-border/70 py-2">
                    {modelRows.length === 0 ? (
                      <p className="py-2 text-sm text-muted-foreground">
                        {t("settings.llm.empty")}
                      </p>
                    ) : (
                      modelRows.map(({ kind, profile }) => (
                        <div
                          key={profile.id}
                          className="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between"
                        >
                          <div className="min-w-0 space-y-1">
                            <div className="flex flex-wrap items-center gap-2">
                              <span className="text-sm font-semibold">{profile.name}</span>
                              <Pill tone="slate" className="text-xs normal-case">
                                {profile.id}
                              </Pill>
                              <Pill tone="sky" className="text-xs normal-case">
                                {profile.model}
                              </Pill>
                              {kind === "llm" && (
                                <Pill
                                  tone={profile.enabled ? "emerald" : "amber"}
                                  className="text-xs normal-case"
                                >
                                  {t(
                                    profile.enabled
                                      ? "settings.llm.enabled"
                                      : "settings.llm.disabled",
                                  )}
                                </Pill>
                              )}
                              <Pill
                                tone={kind === "llm" ? "violet" : "amber"}
                                className="text-xs normal-case"
                              >
                                {t(
                                  kind === "llm"
                                    ? "settings.llm.generativeModel"
                                    : "settings.llm.decisionModel",
                                )}
                              </Pill>
                              <Pill
                                tone={profile.has_credential ? "emerald" : "amber"}
                                className="text-xs normal-case"
                              >
                                {profile.has_credential
                                  ? t("settings.credentialReady")
                                  : t("settings.noCredential")}
                              </Pill>
                              {kind === "llm" && !profile.editable ? (
                                <Pill tone="amber" className="text-xs normal-case">
                                  {t("settings.locked", { jobs: profile.blocking_jobs.join(", ") })}
                                </Pill>
                              ) : null}
                            </div>
                            <p className="text-xs text-muted-foreground">
                              {t("settings.llm.protocol", {
                                protocol: profile.protocol,
                                endpoint:
                                  profile.endpoint ??
                                  (kind === "llm" ? t("settings.llm.defaultEndpoint") : ""),
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
                              {kind === "llm" && profile.executors.length > 0 ? (
                                <span className="text-xs text-muted-foreground">
                                  ·{" "}
                                  {t("settings.llm.executors", {
                                    executors: profile.executors.join(", "),
                                  })}
                                </span>
                              ) : null}
                            </div>
                          </div>
                          <div className="flex shrink-0 gap-2">
                            {kind === "llm" && (
                              <Button
                                type="button"
                                variant="outline"
                                size="xs"
                                disabled={availabilityId !== null}
                                onClick={() => {
                                  setAvailabilityId(profile.id);
                                  setLoadError(null);
                                  void client
                                    .setModelEnabled(profile.id, !profile.enabled)
                                    .then(setSnapshot)
                                    .catch((error) => setLoadError(describeError(error, t)))
                                    .finally(() => setAvailabilityId(null));
                                }}
                              >
                                {t(
                                  profile.enabled ? "settings.llm.disable" : "settings.llm.enable",
                                )}
                              </Button>
                            )}
                            <Button
                              type="button"
                              variant="outline"
                              size="xs"
                              disabled={!profile.editable}
                              onClick={() =>
                                kind === "llm"
                                  ? startEditLlm(profile)
                                  : startEditDecisionModel(profile)
                              }
                            >
                              {t("common.edit")}
                            </Button>
                            <Button
                              type="button"
                              variant="ghost"
                              size="xs"
                              disabled={llmSaving || deleting || !profile.editable}
                              onClick={() => {
                                setDeletingModel(profile);
                                setDeleteError(null);
                              }}
                            >
                              {t("common.remove")}
                            </Button>
                          </div>
                        </div>
                      ))
                    )}
                  </div>

                  <Dialog
                    open={llmFormOpen}
                    onOpenChange={(open) => {
                      if (!open && !llmSaving) resetLlmForm();
                    }}
                  >
                    <DialogContent
                      className="max-h-[90dvh] overflow-y-auto sm:max-w-3xl"
                      showCloseButton={!llmSaving}
                    >
                      <DialogHeader>
                        <DialogTitle>
                          {editingLlmId
                            ? t("settings.llm.editTitle", { id: editingLlmId })
                            : t("settings.llm.createTitle")}
                        </DialogTitle>
                      </DialogHeader>
                      <form onSubmit={(e) => void handleSaveLlm(e)} className="flex flex-col gap-3">
                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                          <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
                            {t("settings.llm.modelType")}
                            <Select
                              value={modelKind}
                              items={[
                                { value: "llm", label: t("settings.llm.generativeModel") },
                                {
                                  value: "decision_model",
                                  label: t("settings.llm.decisionModel"),
                                },
                              ]}
                              disabled={llmSaving || editingLlmId !== null}
                              onValueChange={(next) => {
                                if (next !== null) setModelKind(next as ModelKind);
                              }}
                            >
                              <SelectTrigger className="h-10 w-full rounded-md border-input bg-background px-3 text-sm text-foreground">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="llm">
                                  {t("settings.llm.generativeModel")}
                                </SelectItem>
                                <SelectItem value="decision_model">
                                  {t("settings.llm.decisionModel")}
                                </SelectItem>
                              </SelectContent>
                            </Select>
                          </label>
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
                          {modelKind === "llm" ? (
                            <ProtocolSelect
                              value={llmProtocol}
                              onChange={setLlmProtocol}
                              disabled={llmSaving}
                            />
                          ) : null}
                          <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
                            {t(modelKind === "llm" ? "settings.llm.endpoint" : "decision.endpoint")}
                            <Input
                              required={modelKind === "decision_model"}
                              value={llmEndpoint}
                              onChange={(e) => setLlmEndpoint(e.target.value)}
                              placeholder={
                                modelKind === "llm"
                                  ? t("settings.llm.endpointPlaceholder")
                                  : undefined
                              }
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

                        {modelKind === "llm" ? (
                          <>
                            <fieldset className="flex flex-col gap-1.5">
                              <legend className="text-xs font-medium text-muted-foreground">
                                {t("settings.llm.capabilities")}
                              </legend>
                              <div className="flex flex-wrap gap-3 pt-1">
                                {ALL_CAPABILITIES.map((cap) => (
                                  <label
                                    key={cap.value}
                                    className="flex items-center gap-1.5 text-sm"
                                  >
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
                              <Input
                                inputMode="numeric"
                                value={llmContextWindow}
                                onChange={(event) => setLlmContextWindow(event.target.value)}
                                placeholder="128000"
                              />
                            </label>

                            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
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
                              <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                                {t("settings.llm.reasoningEffort")}
                                <Select
                                  value={llmReasoningEffort}
                                  items={[
                                    { value: "", label: t("chat.providerDefault") },
                                    ...(llmReasoningEffort &&
                                    snapshot?.llm_profiles
                                      .find((profile) => profile.id === editingLlmId)
                                      ?.reasoning_efforts?.every(
                                        (value) => value !== llmReasoningEffort,
                                      )
                                      ? [
                                          {
                                            value: llmReasoningEffort,
                                            label: `${llmReasoningEffort} (${t("chat.effortUnsupported")})`,
                                          },
                                        ]
                                      : []),
                                    ...(
                                      snapshot?.llm_profiles.find(
                                        (profile) => profile.id === editingLlmId,
                                      )?.reasoning_efforts ?? [
                                        "none",
                                        "minimal",
                                        "low",
                                        "medium",
                                        "high",
                                        "xhigh",
                                        "max",
                                      ]
                                    ).map((value) => ({ value, label: value })),
                                  ]}
                                  disabled={llmSaving}
                                  onValueChange={(next) => {
                                    if (next !== null) setLlmReasoningEffort(next);
                                  }}
                                >
                                  <SelectTrigger className="h-10 w-full rounded-md border-input bg-background px-3 text-sm text-foreground">
                                    <SelectValue />
                                  </SelectTrigger>
                                  <SelectContent>
                                    <SelectItem value="">{t("chat.providerDefault")}</SelectItem>
                                    {llmReasoningEffort &&
                                      snapshot?.llm_profiles
                                        .find((profile) => profile.id === editingLlmId)
                                        ?.reasoning_efforts?.every(
                                          (value) => value !== llmReasoningEffort,
                                        ) && (
                                        <SelectItem value={llmReasoningEffort} disabled>
                                          {llmReasoningEffort} ({t("chat.effortUnsupported")})
                                        </SelectItem>
                                      )}
                                    {(
                                      snapshot?.llm_profiles.find(
                                        (profile) => profile.id === editingLlmId,
                                      )?.reasoning_efforts ?? [
                                        "none",
                                        "minimal",
                                        "low",
                                        "medium",
                                        "high",
                                        "xhigh",
                                        "max",
                                      ]
                                    ).map((value) => (
                                      <SelectItem key={value} value={value}>
                                        {value}
                                      </SelectItem>
                                    ))}
                                  </SelectContent>
                                </Select>
                              </label>
                            </div>
                          </>
                        ) : (
                          <>
                            <fieldset className="flex flex-col gap-1.5">
                              <legend className="text-xs font-medium text-muted-foreground">
                                {t("decision.capabilities")}
                              </legend>
                              <div className="flex flex-wrap gap-3 pt-1">
                                {DECISION_CAPABILITIES.map((capability) => (
                                  <label
                                    key={capability}
                                    className="flex items-center gap-1.5 text-sm"
                                  >
                                    <input
                                      type="checkbox"
                                      checked={decisionCapabilities.includes(capability)}
                                      onChange={(event) =>
                                        setDecisionCapabilities((current) =>
                                          event.target.checked
                                            ? [...current, capability]
                                            : current.filter((value) => value !== capability),
                                        )
                                      }
                                      className="size-4 accent-primary"
                                    />
                                    {t(`decision.${capability}`)}
                                  </label>
                                ))}
                              </div>
                            </fieldset>
                            <details>
                              <summary className="cursor-pointer text-xs font-medium text-muted-foreground">
                                {t("decision.limits")}
                              </summary>
                              <div className="mt-2 grid grid-cols-2 gap-2">
                                {(
                                  [
                                    "max_input_tokens_per_request",
                                    "max_output_tokens_per_request",
                                    "max_input_tokens_per_question",
                                    "max_output_tokens_per_question",
                                  ] as const
                                ).map((field) => (
                                  <label
                                    key={field}
                                    className="flex flex-col gap-1 text-xs font-medium text-muted-foreground"
                                  >
                                    {t(`decision.${field}`)}
                                    <Input
                                      type="number"
                                      min={1}
                                      max={1_000_000}
                                      step={1}
                                      value={decisionLimits[field] ?? ""}
                                      onChange={(event) =>
                                        setDecisionLimits({
                                          ...decisionLimits,
                                          [field]: event.target.value
                                            ? event.currentTarget.valueAsNumber
                                            : null,
                                        })
                                      }
                                    />
                                  </label>
                                ))}
                              </div>
                            </details>
                          </>
                        )}

                        {llmError ? (
                          <DismissibleNotice
                            message={llmError}
                            className="text-sm text-destructive"
                          >
                            {llmError}
                          </DismissibleNotice>
                        ) : null}
                        <DialogFooter>
                          <Button
                            type="button"
                            variant="outline"
                            disabled={llmSaving}
                            onClick={resetLlmForm}
                          >
                            {t("common.cancel")}
                          </Button>
                          <Button type="submit" disabled={llmSaving}>
                            {llmSaving
                              ? t("settings.saving")
                              : editingLlmId
                                ? t("settings.llm.saveUpdate")
                                : t("settings.llm.saveCreate")}
                          </Button>
                        </DialogFooter>
                      </form>
                    </DialogContent>
                  </Dialog>
                </Section>
              )}

              {section === "agents" && (
                <Section
                  id="settings-agent-profiles"
                  title={t("settings.agent.title")}
                  description={t("settings.agent.description")}
                >
                  <form
                    onSubmit={(e) => void handleSaveAgent(e)}
                    className="mb-2 flex flex-col gap-3 border-b border-border pb-4"
                  >
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
                    <fieldset disabled={agentSaving} className="flex flex-col gap-2">
                      <legend className="mb-2 text-xs font-medium text-muted-foreground">
                        {t("settings.agent.capabilities")}
                      </legend>
                      <div className="flex flex-wrap gap-4">
                        {(["chat", "coding", "decision"] as const).map((capability) => (
                          <label key={capability} className="flex items-center gap-2 text-sm">
                            <input
                              type="checkbox"
                              className="size-4 accent-primary"
                              checked={agentCapabilities.includes(capability)}
                              onChange={() => toggleAgentCapability(capability)}
                            />
                            {t(`settings.agent.capability.${capability}`)}
                          </label>
                        ))}
                      </div>
                    </fieldset>
                    <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                      {t("settings.agent.instructionsLabel")}
                      <Textarea
                        value={agentInstructions}
                        onChange={(e) => setAgentInstructions(e.target.value)}
                        placeholder={t("settings.agent.instructionsPlaceholder")}
                        rows={3}
                      />
                    </label>
                    <fieldset disabled={agentSaving} className="flex min-w-0 flex-col gap-2">
                      <legend className="text-xs font-medium text-muted-foreground">
                        {t("settings.agent.targetsLabel")}
                      </legend>
                      <p className="text-xs text-muted-foreground">
                        {t("settings.agent.modelsHint")}
                      </p>
                      <div className="grid min-w-0 grid-cols-2 gap-3">
                        <section className="flex min-h-48 min-w-0 flex-col rounded-md border border-border">
                          <h4 className="border-b border-border px-3 py-2 text-sm font-medium">
                            {t("settings.agent.availableModels")}
                          </h4>
                          <div className="ocg-subtle-scrollbar max-h-80 min-w-0 overflow-y-auto overscroll-contain">
                            <ul className="p-2">
                              {unassignedModels.map((model) => (
                                <li
                                  key={model.id}
                                  className="flex items-center gap-2 rounded-md p-2 text-sm"
                                >
                                  <span className="min-w-0 flex-1">
                                    <span className="block break-words">{model.name}</span>
                                    <span
                                      className="block truncate text-xs text-muted-foreground"
                                      title={model.model}
                                    >
                                      {model.model}
                                    </span>
                                  </span>
                                  <Button
                                    type="button"
                                    size="icon-xs"
                                    variant="outline"
                                    aria-label={t("settings.agent.assignModel", {
                                      model: model.name,
                                    })}
                                    onClick={() => setAgentTargets([...assignedModels, model.id])}
                                  >
                                    →
                                  </Button>
                                </li>
                              ))}
                              {!unassignedModels.length && (
                                <li className="p-2 text-xs text-muted-foreground">
                                  {t("settings.agent.noEligibleModels")}
                                </li>
                              )}
                            </ul>
                          </div>
                        </section>
                        <section className="flex min-h-48 min-w-0 flex-col rounded-md border border-border">
                          <h4 className="border-b border-border px-3 py-2 text-sm font-medium">
                            {t("settings.agent.assignedModels")}
                          </h4>
                          <div className="ocg-subtle-scrollbar max-h-80 min-w-0 overflow-y-auto overscroll-contain">
                            <ol className="p-2">
                              {assignedModels.map((id, index) => {
                                const model = eligibleModels.find((model) => model.id === id)!;
                                return (
                                  <li
                                    key={id}
                                    className="flex items-center gap-2 rounded-md p-2 text-sm"
                                  >
                                    <Button
                                      type="button"
                                      size="icon-xs"
                                      variant="outline"
                                      aria-label={t("settings.agent.unassignModel", {
                                        model: model.name,
                                      })}
                                      onClick={() =>
                                        setAgentTargets(
                                          assignedModels.filter((value) => value !== id),
                                        )
                                      }
                                    >
                                      ←
                                    </Button>
                                    <span className="min-w-0 flex-1">
                                      <span className="block break-words">{model.name}</span>
                                      <span
                                        className="block truncate text-xs text-muted-foreground"
                                        title={model.model}
                                      >
                                        {model.model}
                                      </span>
                                      {index === 0 && (
                                        <span className="text-xs text-primary">
                                          {t("settings.agent.defaultModel")}
                                        </span>
                                      )}
                                    </span>
                                    <div className="flex shrink-0 flex-col gap-1">
                                      <Button
                                        type="button"
                                        size="icon-xs"
                                        variant="ghost"
                                        disabled={agentSaving || index === 0}
                                        aria-label={t("settings.agent.moveModelUp", {
                                          model: model.name,
                                        })}
                                        onClick={() => moveAssignedModel(index, -1)}
                                      >
                                        ↑
                                      </Button>
                                      <Button
                                        type="button"
                                        size="icon-xs"
                                        variant="ghost"
                                        disabled={
                                          agentSaving || index === assignedModels.length - 1
                                        }
                                        aria-label={t("settings.agent.moveModelDown", {
                                          model: model.name,
                                        })}
                                        onClick={() => moveAssignedModel(index, 1)}
                                      >
                                        ↓
                                      </Button>
                                    </div>
                                  </li>
                                );
                              })}
                              {!assignedModels.length && (
                                <li className="p-2 text-xs text-muted-foreground">
                                  {t("settings.agent.noAssignedModels")}
                                </li>
                              )}
                            </ol>
                          </div>
                        </section>
                      </div>
                    </fieldset>

                    {agentError ? (
                      <DismissibleNotice message={agentError} className="text-sm text-destructive">
                        {agentError}
                      </DismissibleNotice>
                    ) : null}
                    {agentNotice ? (
                      <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
                        {agentNotice}
                      </p>
                    ) : null}

                    <div className="flex justify-end gap-2">
                      <Button
                        type="submit"
                        disabled={
                          agentSaving || !agentCapabilities.length || !assignedModels.length
                        }
                      >
                        {agentSaving
                          ? t("settings.saving")
                          : editingAgentId
                            ? t("settings.agent.saveUpdate")
                            : t("settings.agent.saveCreate")}
                      </Button>
                    </div>
                  </form>
                  <div className="divide-y divide-border/70 py-2">
                    {agentProfiles.length === 0 ? (
                      <p className="py-2 text-sm text-muted-foreground">
                        {t("settings.agent.empty")}
                      </p>
                    ) : (
                      agentProfiles.map((agent) => (
                        <div
                          key={agent.id}
                          className="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between"
                        >
                          <div className="min-w-0 space-y-1">
                            <div className="flex flex-wrap items-center gap-2">
                              <span className="text-sm font-semibold">{agent.name}</span>
                              <Pill tone="slate" className="text-xs normal-case">
                                {agent.id}
                              </Pill>
                              {agent.capabilities.map((capability) => (
                                <Pill key={capability} tone="sky" className="text-xs">
                                  {t(`settings.agent.capability.${capability}`)}
                                </Pill>
                              ))}
                              {!agent.editable ? (
                                <Pill tone="amber" className="text-xs normal-case">
                                  {t("settings.locked", { jobs: agent.blocking_jobs.join(", ") })}
                                </Pill>
                              ) : null}
                            </div>
                            <p className="text-xs text-muted-foreground">
                              {t("settings.agent.targets", {
                                targets: agent.llm_targets.join(", "),
                              })}
                            </p>
                            {agent.instructions ? (
                              <p className="line-clamp-2 whitespace-pre-wrap text-xs text-muted-foreground">
                                {t("settings.agent.instructions", {
                                  instructions: agent.instructions,
                                })}
                              </p>
                            ) : (
                              <p className="text-xs italic text-muted-foreground">
                                {t("settings.agent.noInstructions")}
                              </p>
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
                              {editingAgentId === agent.id
                                ? t("settings.editing")
                                : t("common.edit")}
                            </Button>
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </Section>
              )}

              {section === "decisions" && snapshot && (
                <DecisionProfilesSection
                  client={client}
                  profiles={snapshot}
                  onSaved={setSnapshot}
                  onManageModels={() => onNavigate("models")}
                />
              )}
              {section === "projects" && <ProjectsSection client={client} />}
              {section === "execution" && <PoolsSection client={client} />}
            </>
          ) : null}

          {section === "appearance" && (
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
              <SettingRow
                label={t("settings.language.label")}
                description={t("settings.language.desc")}
                origin="browser"
              >
                <LanguageSwitcher />
              </SettingRow>
              <SettingRow
                label={t("settings.theme.label")}
                description={t("settings.theme.desc")}
                origin="browser"
              >
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
              <SettingRow
                label={t("settings.accent.label")}
                description={t("settings.accent.desc")}
                origin="browser"
              >
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
          )}
          {section === "session" && (
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
                  {sessionExpiresAtUs === null
                    ? "—"
                    : new Date(sessionExpiresAtUs / 1000).toLocaleString()}
                </span>
              </SettingRow>
            </Section>
          )}
          {section === "system" && client && (
            <>
              <UpdatesSection client={client} />
              <GlobalCompactionSection client={client} profiles={snapshot} />
            </>
          )}
        </div>
      </div>
    </PageSurface>
  );
}
