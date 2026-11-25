import { Moon, Sun } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import type {
  AgentCapability,
  Capability,
  LlmProfile,
  ManagedAgentProfile,
  ProfilesSnapshot,
} from "@/components/ocg/contracts";
import type { DecisionLimits as DecisionLimits } from "@/components/ocg/contracts/api";
import { PageSurface, Pill, SELECTED_OPTION } from "@/components/ocg/primitives";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import {
  type ApiClient,
  ApiError,
  isProfileConflict,
  type ProfileWriteRequest,
} from "@/components/ocg/runtime/api-client";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Select, SelectItem } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { ariaState, elementRef, type FormEvent, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";
import { type I18nKey, LanguageSwitcher, useI18n } from "../i18n";
import { ContentHeader } from "../layout/content-container";
import { DecisionProfilesSection } from "./decision-profiles-section";
import { GlobalCompactionSection } from "./global-compaction-section";
import { GlobalHeadersSection } from "./global-headers-section";
import {
  draftsToInputs,
  type HeaderDraft,
  type HeaderDraftError,
  headersToDrafts,
} from "./header-drafts";
import { HeadersEditor } from "./headers-editor";
import { PoolsSection } from "./pools-section";
import { ProjectsSection } from "./projects-section";
import { type LlmProtocol, ProtocolSelect } from "./protocol-select";
import { ProviderModelImport } from "./provider-model-import";
import { SettingsNavigation } from "./settings-navigation";
import { ApiKeyInput, describeError, Section } from "./settings-parts";
import type { SettingsSection } from "./settings-sections";
import { UpdatesSection } from "./updates-section";

const ALL_CAPABILITIES: readonly { value: Capability; labelKey: I18nKey }[] = [
  { value: "chat", labelKey: "settings.llm.capabilityChat" },
  { value: "vision", labelKey: "settings.llm.capabilityVision" },
  { value: "tools", labelKey: "settings.llm.capabilityTools" },
  { value: "reasoning", labelKey: "settings.llm.capabilityReasoning" },
];
const DECISION_CAPABILITIES = ["choice", "binary_probability", "ordinal_score"] as const;
const EMPTY_DECISION_LIMITS: DecisionLimits = {
  max_input_tokens_per_request: null,
  max_output_tokens_per_request: null,
  max_input_tokens_per_question: null,
  max_output_tokens_per_question: null,
};
type ModelKind = "llm" | "decision_model";
type DecisionModelProfile = ProfilesSnapshot["decision_profiles"][number];

function ChoiceGroup<T extends string>(props: {
  label: string;
  value: T;
  options: readonly { value: T; label: string; preview: View }[];
  onChange: (value: T) => void;
}) {
  return (
    <div class="flex flex-wrap gap-2" role="group" aria-label={props.label}>
      {props.options.map((option) => (
        <button
          type="button"
          aria-pressed={ariaState(props.value === option.value)}
          onClick={() => props.onChange(option.value)}
          class={cn(
            "inline-flex min-h-11 items-center gap-2 rounded-md border px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring",
            props.value === option.value
              ? cn(SELECTED_OPTION, "font-medium")
              : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground",
          )}
        >
          {option.preview}
          {option.label}
        </button>
      ))}
    </div>
  );
}

function SettingRow(props: {
  label: string;
  description: string;
  origin: "browser" | "runtime";
  children: View;
}) {
  const i18n = useI18n();
  return (
    <div class="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div class="min-w-0">
        <div class="flex flex-wrap items-center gap-2">
          <h3 class="text-sm font-medium">{props.label}</h3>
          <Pill tone={props.origin === "browser" ? "violet" : "sky"} class="text-xs normal-case">
            {props.origin === "browser"
              ? i18n.t("settings.origin.browser")
              : i18n.t("settings.origin.runtime")}
          </Pill>
        </div>
        <p class="mt-1 max-w-2xl text-[13px] leading-5 text-muted-foreground">
          {props.description}
        </p>
      </div>
      <div class="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">{props.children}</div>
    </div>
  );
}

const value = "[overflow-wrap:anywhere] text-[13px] font-medium text-foreground";

export function SettingsSurface(props: {
  client?: ApiClient;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  onSignOut: () => void;
  section: SettingsSection;
  onNavigate(section: SettingsSection): void;
}) {
  const i18n = useI18n();
  const theme = useTheme();
  const [snapshot, setSnapshot] = createSignal<ProfilesSnapshot | null>(null);
  const [loadError, setLoadError] = createSignal<string | null>(null);

  const [editingLlmId, setEditingLlmId] = createSignal<string | null>(null);
  const [editingEditSeq, setEditingEditSeq] = createSignal<number | null>(null);
  const [llmFormOpen, setLlmFormOpen] = createSignal(false);
  const [modelKind, setModelKind] = createSignal<ModelKind>("llm");
  const [llmName, setLlmName] = createSignal("");
  const [llmModel, setLlmModel] = createSignal("");
  const [llmProtocol, setLlmProtocol] = createSignal<LlmProtocol>("openai_chat_completions");
  const isAnthropic = () => llmProtocol() === "anthropic_messages";
  const maxTemperature = () => (isAnthropic() ? 1 : 2);
  const [llmEndpoint, setLlmEndpoint] = createSignal("");
  const [llmCredentialRef, setLlmCredentialRef] = createSignal("");
  const [llmSecret, setLlmSecret] = createSignal("");
  const [llmHeaders, setLlmHeaders] = createSignal<HeaderDraft[]>([]);
  const [llmHeaderError, setLlmHeaderError] = createSignal<HeaderDraftError>();
  const [llmCapabilities, setLlmCapabilities] = createSignal<Capability[]>(["chat"]);
  const [decisionCapabilities, setDecisionCapabilities] = createSignal<string[]>(["choice"]);
  const [decisionLimits, setDecisionLimits] = createSignal<DecisionLimits>(EMPTY_DECISION_LIMITS);
  const [llmTemperature, setLlmTemperature] = createSignal("");
  const [llmTopP, setLlmTopP] = createSignal("");
  const [llmMaxTokens, setLlmMaxTokens] = createSignal("");
  const [llmReasoningEffort, setLlmReasoningEffort] = createSignal("");
  const [llmContextWindow, setLlmContextWindow] = createSignal("");
  const [focusContext, setFocusContext] = createSignal(false);
  const [deletingModel, setDeletingModel] = createSignal<LlmProfile | DecisionModelProfile | null>(
    null,
  );
  const [deleting, setDeleting] = createSignal(false);
  const [deleteError, setDeleteError] = createSignal<string | null>(null);
  const [llmSaving, setLlmSaving] = createSignal(false);
  const [llmError, setLlmError] = createSignal<string | null>(null);
  const [catalogNotice, setCatalogNotice] = createSignal<string | null>(null);
  const [inFlight, setInFlight] = createSignal<Set<string>>((() => new Set())());
  const ownAvailabilityToggles = elementRef(0);
  const missedProfileChange = elementRef(false);

  const [editingAgentId, setEditingAgentId] = createSignal<string | null>(null);
  const [editingAgentSeq, setEditingAgentSeq] = createSignal<number | null>(null);
  const [agentName, setAgentName] = createSignal("");
  const [agentInstructions, setAgentInstructions] = createSignal("");
  const [agentCapabilities, setAgentCapabilities] = createSignal<AgentCapability[]>(["chat"]);
  const [agentTargets, setAgentTargets] = createSignal<string[]>([]);
  const [agentSaving, setAgentSaving] = createSignal(false);
  const [agentError, setAgentError] = createSignal<string | null>(null);
  const [agentNotice, setAgentNotice] = createSignal<string | null>(null);

  const refreshProfiles = async (signal?: AbortSignal) => {
    if (!props.client) return;
    setLoadError(null);
    try {
      const loaded = await props.client.readProfiles(signal);
      if (!signal?.aborted) {
        setSnapshot(loaded);
      }
    } catch (error) {
      if (!signal?.aborted) {
        setLoadError(describeError(error, i18n.t));
      }
    }
  };

  createEffect(
    () => [props.client, i18n.t],
    () => {
      if (!props.client) return;
      const controller = new AbortController();
      let aborted = false;
      (async () => {
        try {
          const loaded = await props.client!.readProfiles(controller.signal);
          if (!aborted) {
            setSnapshot(loaded);
          }
        } catch (error) {
          if (!aborted) {
            setLoadError(describeError(error, i18n.t));
          }
        }
      })();
      return () => {
        aborted = true;
        controller.abort();
      };
    },
  );

  createEffect(
    () => [props.client, refreshProfiles],
    () =>
      props.client?.onProfilesChanged((change) => {
        if (change?.availability) return;
        if (ownAvailabilityToggles.current > 0) {
          missedProfileChange.current = true;
          return;
        }
        void refreshProfiles();
      }),
  );

  createEffect(
    () => [snapshot(), props.section],
    () => {
      const loaded = snapshot();
      const params = new URLSearchParams(window.location.search);
      const target = params.get("edit");
      if (!loaded || props.section !== "models" || !target) return;
      const field = params.get("field");
      params.delete("edit");
      params.delete("field");
      const query = params.toString();
      window.history.replaceState(
        null,
        "",
        `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`,
      );
      const profile = loaded.llm_profiles.find((item) => item.id === target);
      if (!profile?.editable) return;
      startEditLlm(profile);
      setFocusContext(field === "context_window");
    },
  );

  function resetLlmForm() {
    setLlmFormOpen(false);
    setFocusContext(false);
    setEditingLlmId(null);
    setEditingEditSeq(null);
    setModelKind("llm");
    setLlmName("");
    setLlmModel("");
    setLlmProtocol("openai_chat_completions");
    setLlmEndpoint("");
    setLlmCredentialRef("");
    setLlmSecret("");
    setLlmHeaders([]);
    setLlmHeaderError(undefined);
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

  function startEditLlm(profile: LlmProfile) {
    setModelKind("llm");
    setEditingLlmId(profile.id);
    setEditingEditSeq(profile.edit_seq);
    setLlmContextWindow(profile.context_window === null ? "" : String(profile.context_window));
    setLlmName(profile.name);
    setLlmModel(profile.model);
    setLlmProtocol(profile.protocol);
    setLlmEndpoint(profile.endpoint ?? "");
    setLlmCredentialRef(profile.credential_ref ?? "");
    setLlmSecret("");
    setLlmHeaders(headersToDrafts(profile.headers));
    setLlmHeaderError(undefined);
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
    setEditingEditSeq(profile.edit_seq);
    setLlmName(profile.name);
    setLlmModel(profile.model);
    setLlmEndpoint(profile.endpoint ?? "");
    setLlmCredentialRef(profile.credential_ref ?? "");
    setLlmHeaders(headersToDrafts(profile.headers));
    setLlmHeaderError(undefined);
    setDecisionCapabilities(profile.declared_capabilities);
    setDecisionLimits(profile.limits);
    setLlmFormOpen(true);
  }

  function toggleCapability(cap: Capability) {
    setLlmCapabilities((current) =>
      current.includes(cap) ? current.filter((entry) => entry !== cap) : [...current, cap],
    );
  }

  async function handleSaveLlm(event: FormEvent) {
    event.preventDefault();
    if (!props.client) return;
    setLlmError(null);

    const trimmedName = llmName().trim();
    const trimmedModel = llmModel().trim();
    if (!trimmedName) {
      setLlmError(i18n.t("settings.llm.validation.name"));
      return;
    }
    if (!trimmedModel) {
      setLlmError(i18n.t("settings.llm.validation.model"));
      return;
    }
    const headerRequest = draftsToInputs(llmHeaders());
    if ("error" in headerRequest) {
      setLlmHeaderError(headerRequest.error);
      setLlmError(
        i18n.t(
          headerRequest.error.kind === "name"
            ? "headers.invalidName"
            : headerRequest.error.kind === "duplicate"
              ? "headers.duplicate"
              : "headers.invalidValue",
        ),
      );
      return;
    }
    setLlmHeaderError(undefined);
    let payload: ProfileWriteRequest;
    if (modelKind() === "decision_model") {
      if (decisionCapabilities().length === 0) {
        setLlmError(i18n.t("settings.llm.validation.capabilities"));
        return;
      }
      payload = {
        kind: "decision_model",
        name: trimmedName,
        model: trimmedModel,
        protocol: "typesafe_systemone",
        endpoint: llmEndpoint().trim(),
        credential_ref: llmCredentialRef().trim() ? llmCredentialRef().trim() : null,
        ...(llmSecret().trim() ? { credential: llmSecret().trim() } : {}),
        declared_capabilities: decisionCapabilities(),
        limits: decisionLimits(),
        headers: headerRequest.inputs,
        ...(editingEditSeq() === null ? {} : { expected_edit_seq: editingEditSeq()! }),
      };
    } else {
      if (llmCapabilities().length === 0) {
        setLlmError(i18n.t("settings.llm.validation.capabilities"));
        return;
      }
      const parsedTemp = llmTemperature().trim() === "" ? null : Number(llmTemperature().trim());
      if (
        parsedTemp !== null &&
        (!Number.isFinite(parsedTemp) || parsedTemp < 0 || parsedTemp > maxTemperature())
      ) {
        setLlmError(i18n.t("settings.llm.validation.temperature", { max: maxTemperature() }));
        return;
      }
      const parsedTopP = llmTopP().trim() === "" ? null : Number(llmTopP().trim());
      if (
        parsedTopP !== null &&
        (!Number.isFinite(parsedTopP) || parsedTopP < 0 || parsedTopP > 1)
      ) {
        setLlmError(i18n.t("settings.llm.validation.topP"));
        return;
      }
      const parsedMaxTokens = llmMaxTokens().trim() === "" ? null : Number(llmMaxTokens().trim());
      if (
        parsedMaxTokens !== null &&
        (!Number.isSafeInteger(parsedMaxTokens) || parsedMaxTokens < 1)
      ) {
        setLlmError(i18n.t("settings.llm.validation.maxTokens"));
        return;
      }

      const parsedContextWindow =
        llmContextWindow().trim() === "" ? null : Number(llmContextWindow().trim());
      if (
        parsedContextWindow !== null &&
        (!Number.isSafeInteger(parsedContextWindow) ||
          parsedContextWindow < 1 ||
          parsedContextWindow > 4294967295)
      ) {
        setLlmError(i18n.t("settings.llm.invalidContext"));
        return;
      }
      const priorParameters = snapshot()?.llm_profiles.find(
        (profile) => profile.id === editingLlmId(),
      )?.parameters;
      payload = {
        kind: "llm",
        name: trimmedName,
        model: trimmedModel,
        context_window: parsedContextWindow,
        protocol: llmProtocol(),
        endpoint: llmEndpoint().trim() ? llmEndpoint().trim() : null,
        credential_ref: llmCredentialRef().trim() ? llmCredentialRef().trim() : null,
        credential: llmSecret().trim() ? llmSecret().trim() : null,
        declared_capabilities: llmCapabilities(),
        headers: headerRequest.inputs,
        ...(editingEditSeq() === null ? {} : { expected_edit_seq: editingEditSeq()! }),
        parameters: {
          ...priorParameters,
          max_completion_tokens:
            parsedMaxTokens === null ? priorParameters?.max_completion_tokens : null,
          temperature: parsedTemp,
          top_p: parsedTopP,
          max_tokens: parsedMaxTokens,
          reasoning_effort: isAnthropic() ? null : llmReasoningEffort() || null,
          ...(isAnthropic() ? { presence_penalty: null, frequency_penalty: null } : {}),
        },
      };
    }
    setLlmSaving(true);
    try {
      const updated = editingLlmId()
        ? await props.client.updateProfile(editingLlmId()!, payload)
        : await props.client.createProfile(payload);
      setSnapshot(updated);
      const wasEditing = editingLlmId() !== null;
      resetLlmForm();
      setCatalogNotice(
        wasEditing
          ? i18n.t("settings.notice.updated", { name: trimmedName })
          : i18n.t("settings.notice.created", { name: trimmedName }),
      );
    } catch (error) {
      if (isProfileConflict(error) && editingLlmId()) {
        setLlmError(i18n.t("settings.llm.conflict"));
        try {
          const latest = await props.client.readProfiles();
          setSnapshot(latest);
          const current = [...latest.llm_profiles, ...(latest.decision_profiles ?? [])].find(
            (profile) => profile.id === editingLlmId(),
          );
          if (current) setEditingEditSeq(current.edit_seq);
        } catch {}
        return;
      }
      setLlmError(describeError(error, i18n.t));
      if (error instanceof ApiError && error.code === "credential_not_saved") {
        await refreshProfiles();
      }
    } finally {
      setLlmSaving(false);
    }
  }

  function resetAgentForm() {
    setEditingAgentId(null);
    setEditingAgentSeq(null);
    setAgentName("");
    setAgentInstructions("");
    setAgentCapabilities(["chat"]);
    setAgentTargets([]);
    setAgentError(null);
  }

  function startEditAgent(agent: ManagedAgentProfile) {
    setEditingAgentId(agent.id);
    setEditingAgentSeq(agent.edit_seq);
    setAgentName(agent.name);
    setAgentInstructions(agent.instructions ?? "");
    setAgentCapabilities([...agent.capabilities]);
    setAgentTargets([...agent.llm_targets]);
    setAgentError(null);
    setAgentNotice(null);
  }

  const assignableModels = createMemo(() => [
    ...(snapshot()?.llm_profiles ?? []).map((profile) => ({
      ...profile,
      kind: "generative" as const,
    })),
    ...(snapshot()?.decision_profiles ?? []).map((profile) => ({
      ...profile,
      kind: "decision" as const,
    })),
  ]);
  function supportsCapabilities(
    model: ReturnType<typeof assignableModels>[number],
    capabilities: AgentCapability[],
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
  const eligibleModels = createMemo(() =>
    assignableModels().filter((model) => supportsCapabilities(model, agentCapabilities())),
  );
  const assignedModels = createMemo(() =>
    agentTargets().filter((id) => eligibleModels().some((model) => model.id === id)),
  );
  const unassignedModels = createMemo(() =>
    eligibleModels().filter(
      (model) =>
        !assignedModels().includes(model.id) && (model.kind === "decision" || model.enabled),
    ),
  );
  function toggleAgentCapability(capability: AgentCapability) {
    const next = agentCapabilities().includes(capability)
      ? agentCapabilities().filter((value) => value !== capability)
      : [...agentCapabilities(), capability];
    setAgentCapabilities(next);
    setAgentTargets((current) =>
      current.filter((id) =>
        assignableModels().some((model) => model.id === id && supportsCapabilities(model, next)),
      ),
    );
  }
  function moveAssignedModel(index: number, offset: number) {
    const next = [...assignedModels()];
    const destination = index + offset;
    if (destination < 0 || destination >= next.length) return;
    [next[index], next[destination]] = [next[destination], next[index]];
    setAgentTargets(next);
  }

  async function handleSaveAgent(event: FormEvent) {
    event.preventDefault();
    if (!props.client) return;
    setAgentError(null);
    setAgentNotice(null);

    const trimmedName = agentName().trim();
    if (!trimmedName) {
      setAgentError(i18n.t("settings.agent.validation.name"));
      return;
    }
    if (!agentCapabilities().length || assignedModels().length === 0) {
      setAgentError(i18n.t("settings.agent.validation.targets"));
      return;
    }

    setAgentSaving(true);
    try {
      const payload = {
        kind: "agent" as const,
        name: trimmedName,
        instructions: agentInstructions().trim() ? agentInstructions().trim() : null,
        capabilities: agentCapabilities(),
        llm_targets: assignedModels(),
        ...(editingAgentSeq() === null ? {} : { expected_edit_seq: editingAgentSeq()! }),
      };
      const updated = editingAgentId()
        ? await props.client.updateProfile(editingAgentId()!, payload)
        : await props.client.createProfile(payload);
      setSnapshot(updated);
      const wasEditing = editingAgentId() !== null;
      resetAgentForm();
      setAgentNotice(
        wasEditing
          ? i18n.t("settings.notice.updated", { name: trimmedName })
          : i18n.t("settings.notice.created", { name: trimmedName }),
      );
    } catch (error) {
      if (isProfileConflict(error) && editingAgentId()) {
        setAgentError(i18n.t("settings.agent.conflict"));
        try {
          const latest = await props.client.readProfiles();
          setSnapshot(latest);
          const current = latest.agent_profiles.find((profile) => profile.id === editingAgentId());
          if (current) setEditingAgentSeq(current.edit_seq);
        } catch {}
      } else {
        setAgentError(describeError(error, i18n.t));
      }
    } finally {
      setAgentSaving(false);
    }
  }

  const llmProfiles = createMemo(() => snapshot()?.llm_profiles ?? []);
  const modelRows = createMemo(() => [
    ...llmProfiles().map((profile) => ({ kind: "llm" as const, profile })),
    ...(snapshot()?.decision_profiles ?? []).map((profile) => ({
      kind: "decision_model" as const,
      profile,
    })),
  ]);
  const agentProfiles = createMemo(() => snapshot()?.agent_profiles ?? []);

  return (
    <PageSurface class="mx-auto w-full max-w-7xl">
      <Dialog
        open={deletingModel() !== null}
        onOpenChange={(open) => {
          if (!open && !deleting()) setDeletingModel(null);
        }}
      >
        <DialogContent showCloseButton={!deleting()}>
          <DialogHeader>
            <DialogTitle>{i18n.t("settings.llm.deleteTitle")}</DialogTitle>
            <DialogDescription>
              {i18n.t("settings.llm.deleteDescription", { name: deletingModel()?.name ?? "" })}
            </DialogDescription>
          </DialogHeader>
          {deleteError() && (
            <p role="alert" class="text-sm text-destructive">
              {deleteError()}
            </p>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={deleting()} onClick={() => setDeletingModel(null)}>
              {i18n.t("common.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={deleting()}
              onClick={() => {
                if (!props.client || !deletingModel()) return;
                const id = deletingModel()!.id;
                setDeleting(true);
                setDeleteError(null);
                void props.client
                  .deleteLlmProfile(id)
                  .then((updated) => {
                    setSnapshot(updated);
                    if (editingLlmId() === id) resetLlmForm();
                    setAgentTargets((current) => current.filter((target) => target !== id));
                    setDeletingModel(null);
                    setCatalogNotice(i18n.t("settings.llm.deleted"));
                  })
                  .catch((error) => setDeleteError(describeError(error, i18n.t)))
                  .finally(() => setDeleting(false));
              }}
            >
              {deleting() ? i18n.t("settings.saving") : i18n.t("common.remove")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <header class="mb-6">
        <ContentHeader title={i18n.t("settings.title")} description={i18n.t("settings.manage")} />
      </header>
      <div class="grid min-w-0 gap-6 lg:grid-cols-[13rem_minmax(0,1fr)] lg:gap-8">
        <SettingsNavigation section={props.section} onNavigate={props.onNavigate} />
        <div id="settings-content" class="flex min-w-0 scroll-mt-6 flex-col gap-4">
          {loadError() && ["models", "agents", "decisions"].includes(props.section) && (
            <DismissibleNotice message={loadError()!} />
          )}
          {props.client ? (
            <>
              {props.section === "models" && (
                <Section
                  id="settings-llm-profiles"
                  title={i18n.t("settings.llm.importModels")}
                  description={i18n.t("settings.llm.importDescription")}
                  action={
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        resetLlmForm();
                        setLlmFormOpen(true);
                      }}
                    >
                      {i18n.t("settings.llm.add")}
                    </Button>
                  }
                >
                  <ProviderModelImport
                    client={props.client}
                    snapshot={snapshot()}
                    onImported={setCatalogNotice}
                  />
                  <h3 class="border-t border-border pt-4 text-sm font-semibold">
                    {i18n.t("settings.llm.title")}
                  </h3>
                  {catalogNotice() && (
                    <p role="status" class="pt-3 text-sm text-emerald-700 dark:text-emerald-400">
                      {catalogNotice()}
                    </p>
                  )}
                  <div class="divide-y divide-border/70 py-2">
                    {modelRows().length === 0 ? (
                      <p class="py-2 text-sm text-muted-foreground">
                        {i18n.t("settings.llm.empty")}
                      </p>
                    ) : (
                      modelRows().map(({ kind, profile }) => (
                        <div class="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between">
                          <div class="min-w-0 space-y-1">
                            <div class="flex flex-wrap items-center gap-2">
                              <span class="text-sm font-semibold">{profile.name}</span>
                              <Pill tone="slate" class="text-xs normal-case">
                                {profile.id}
                              </Pill>
                              <Pill tone="sky" class="text-xs normal-case">
                                {profile.model}
                              </Pill>
                              {kind === "llm" && (
                                <Pill
                                  tone={profile.enabled ? "emerald" : "amber"}
                                  class="text-xs normal-case"
                                >
                                  {i18n.t(
                                    profile.enabled
                                      ? "settings.llm.enabled"
                                      : "settings.llm.disabled",
                                  )}
                                </Pill>
                              )}
                              <Pill
                                tone={kind === "llm" ? "violet" : "amber"}
                                class="text-xs normal-case"
                              >
                                {i18n.t(
                                  kind === "llm"
                                    ? "settings.llm.generativeModel"
                                    : "settings.llm.decisionModel",
                                )}
                              </Pill>
                              <Pill
                                tone={profile.has_credential ? "emerald" : "amber"}
                                class="text-xs normal-case"
                              >
                                {profile.has_credential
                                  ? i18n.t("settings.credentialReady")
                                  : i18n.t("settings.noCredential")}
                              </Pill>
                              {kind === "llm" && !profile.editable ? (
                                <Pill tone="amber" class="text-xs normal-case">
                                  {i18n.t("settings.locked", {
                                    jobs: profile.blocking_jobs.join(", "),
                                  })}
                                </Pill>
                              ) : null}
                            </div>
                            <p class="text-xs text-muted-foreground">
                              {i18n.t("settings.llm.protocol", {
                                protocol: profile.protocol,
                                endpoint:
                                  profile.endpoint ??
                                  (kind === "llm" ? i18n.t("settings.llm.defaultEndpoint") : ""),
                              })}
                              {profile.credential_ref
                                ? ` · ${i18n.t("settings.credentialRef", { ref: profile.credential_ref })}`
                                : ""}
                            </p>
                            <div class="flex flex-wrap items-center gap-1.5 pt-0.5">
                              {profile.declared_capabilities.map((cap) => (
                                <Pill tone="violet" class="text-[11px] normal-case">
                                  {cap}
                                </Pill>
                              ))}
                              {kind === "llm" && profile.executors.length > 0 ? (
                                <span class="text-xs text-muted-foreground">
                                  ·{" "}
                                  {i18n.t("settings.llm.executors", {
                                    executors: profile.executors.join(", "),
                                  })}
                                </span>
                              ) : null}
                            </div>
                          </div>
                          <div class="flex shrink-0 gap-2">
                            {kind === "llm" && (
                              <Button
                                type="button"
                                variant="outline"
                                size="xs"
                                disabled={inFlight().has(profile.id)}
                                onClick={() => {
                                  const profileId = profile.id;
                                  const enabled = !profile.enabled;
                                  setInFlight((current) => new Set(current).add(profileId));
                                  setLoadError(null);
                                  ownAvailabilityToggles.current += 1;
                                  void props
                                    .client!.setModelEnabled(profileId, enabled)
                                    .then(() => {
                                      setSnapshot((current) =>
                                        current
                                          ? {
                                              ...current,
                                              llm_profiles: current.llm_profiles.map((item) =>
                                                item.id === profileId ? { ...item, enabled } : item,
                                              ),
                                            }
                                          : current,
                                      );
                                    })
                                    .catch((error) => {
                                      setLoadError(describeError(error, i18n.t));
                                    })
                                    .finally(() => {
                                      ownAvailabilityToggles.current -= 1;
                                      if (
                                        ownAvailabilityToggles.current === 0 &&
                                        missedProfileChange.current
                                      ) {
                                        missedProfileChange.current = false;
                                        void refreshProfiles();
                                      }
                                      setInFlight((current) => {
                                        const next = new Set(current);
                                        next.delete(profileId);
                                        return next;
                                      });
                                    });
                                }}
                              >
                                {i18n.t(
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
                              {i18n.t("common.edit")}
                            </Button>
                            <Button
                              type="button"
                              variant="ghost"
                              size="xs"
                              disabled={llmSaving() || deleting() || !profile.editable}
                              onClick={() => {
                                setDeletingModel(profile);
                                setDeleteError(null);
                              }}
                            >
                              {i18n.t("common.remove")}
                            </Button>
                          </div>
                        </div>
                      ))
                    )}
                  </div>

                  <Dialog
                    open={llmFormOpen()}
                    onOpenChange={(open) => {
                      if (!open && !llmSaving()) resetLlmForm();
                    }}
                  >
                    <DialogContent
                      class="max-h-[90dvh] overflow-y-auto sm:max-w-3xl"
                      showCloseButton={!llmSaving()}
                      initialFocus={() =>
                        focusContext() ? document.getElementById("llm-context-window") : null
                      }
                    >
                      <DialogHeader>
                        <DialogTitle>
                          {editingLlmId()
                            ? i18n.t("settings.llm.editTitle", { id: editingLlmId()! })
                            : i18n.t("settings.llm.createTitle")}
                        </DialogTitle>
                      </DialogHeader>
                      <form onSubmit={(e) => void handleSaveLlm(e)} class="flex flex-col gap-3">
                        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
                          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
                            {i18n.t("settings.llm.modelType")}
                            <Select
                              value={modelKind()}
                              items={[
                                { value: "llm", label: i18n.t("settings.llm.generativeModel") },
                                {
                                  value: "decision_model",
                                  label: i18n.t("settings.llm.decisionModel"),
                                },
                              ]}
                              disabled={llmSaving() || editingLlmId() !== null}
                              onValueChange={(next) => {
                                if (next !== null) setModelKind(next as ModelKind);
                              }}
                              class="h-10 w-full rounded-md border-input bg-background px-3 text-sm text-foreground"
                            >
                              <SelectItem value="llm">
                                {i18n.t("settings.llm.generativeModel")}
                              </SelectItem>
                              <SelectItem value="decision_model">
                                {i18n.t("settings.llm.decisionModel")}
                              </SelectItem>
                            </Select>
                          </label>
                          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                            {i18n.t("settings.llm.name")}
                            <Input
                              value={llmName()}
                              onInput={(e) => setLlmName(e.target.value)}
                              placeholder={i18n.t("settings.llm.namePlaceholder")}
                              spellcheck="false"
                            />
                          </label>
                          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                            {i18n.t("settings.llm.model")}
                            <Input
                              value={llmModel()}
                              onInput={(e) => setLlmModel(e.target.value)}
                              placeholder={i18n.t("settings.llm.modelPlaceholder")}
                              spellcheck="false"
                            />
                          </label>
                          {modelKind() === "llm" ? (
                            <ProtocolSelect
                              value={llmProtocol()}
                              onChange={setLlmProtocol}
                              disabled={llmSaving()}
                            />
                          ) : null}
                          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
                            {i18n.t(
                              modelKind() === "llm" ? "settings.llm.endpoint" : "decision.endpoint",
                            )}
                            <Input
                              required={modelKind() === "decision_model"}
                              value={llmEndpoint()}
                              onInput={(e) => setLlmEndpoint(e.target.value)}
                              placeholder={
                                modelKind() === "llm"
                                  ? i18n.t("settings.llm.endpointPlaceholder")
                                  : undefined
                              }
                              spellcheck="false"
                            />
                          </label>
                          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                            {i18n.t("settings.llm.credentialReference")}
                            <Input
                              value={llmCredentialRef()}
                              onInput={(e) => setLlmCredentialRef(e.target.value)}
                              placeholder={i18n.t("settings.llm.credentialPlaceholder")}
                              spellcheck="false"
                            />
                          </label>
                          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                            {i18n.t("settings.llm.apiKey")}
                            <ApiKeyInput
                              value={llmSecret()}
                              onChange={(e) => setLlmSecret(e.target.value)}
                              placeholder={
                                editingLlmId()
                                  ? i18n.t("settings.llm.apiKeyKeep")
                                  : i18n.t("settings.llm.apiKeyPlaceholder")
                              }
                            />
                          </label>
                        </div>

                        <HeadersEditor
                          legend={i18n.t("headers.title")}
                          hint={i18n.t("headers.connectionHint")}
                          value={llmHeaders()}
                          onChange={setLlmHeaders}
                          disabled={llmSaving()}
                          allowSecret
                          error={llmHeaderError()}
                        />

                        {modelKind() === "llm" ? (
                          <>
                            <fieldset class="flex flex-col gap-1.5">
                              <legend class="text-xs font-medium text-muted-foreground">
                                {i18n.t("settings.llm.capabilities")}
                              </legend>
                              <div class="flex flex-wrap gap-3 pt-1">
                                {ALL_CAPABILITIES.map((cap) => (
                                  <label class="flex items-center gap-1.5 text-sm">
                                    <input
                                      type="checkbox"
                                      checked={llmCapabilities().includes(cap.value)}
                                      onChange={() => toggleCapability(cap.value)}
                                      class="size-4 accent-primary"
                                    />
                                    {i18n.t(cap.labelKey)}
                                  </label>
                                ))}
                              </div>
                            </fieldset>

                            <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                              {i18n.t("settings.llm.contextWindow")}
                              <Input
                                id="llm-context-window"
                                inputmode="numeric"
                                value={llmContextWindow()}
                                onInput={(event) => setLlmContextWindow(event.target.value)}
                                placeholder="e.g. 128000"
                              />
                            </label>

                            <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                              <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                                {i18n.t("settings.llm.temperature", { max: maxTemperature() })}
                                <Input
                                  type="number"
                                  min={0}
                                  max={maxTemperature()}
                                  step="any"
                                  value={llmTemperature()}
                                  onInput={(e) => setLlmTemperature(e.target.value)}
                                  placeholder="e.g. 0.7"
                                  spellcheck="false"
                                />
                              </label>
                              <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                                {i18n.t("settings.llm.topP")}
                                <Input
                                  value={llmTopP()}
                                  onInput={(e) => setLlmTopP(e.target.value)}
                                  placeholder="e.g. 1.0"
                                  spellcheck="false"
                                />
                              </label>
                              <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                                {i18n.t("settings.llm.maxTokens")}
                                <Input
                                  value={llmMaxTokens()}
                                  onInput={(e) => setLlmMaxTokens(e.target.value)}
                                  placeholder={isAnthropic() ? undefined : "e.g. 4096"}
                                  spellcheck="false"
                                />
                              </label>
                              <label
                                hidden={isAnthropic()}
                                class={cn(
                                  "flex-col gap-1 text-xs font-medium text-muted-foreground",
                                  !isAnthropic() && "flex",
                                )}
                              >
                                {i18n.t("settings.llm.reasoningEffort")}
                                <Select
                                  value={llmReasoningEffort()}
                                  items={[
                                    { value: "", label: i18n.t("chat.providerDefault") },
                                    ...(llmReasoningEffort() &&
                                    snapshot()
                                      ?.llm_profiles.find(
                                        (profile) => profile.id === editingLlmId(),
                                      )
                                      ?.reasoning_efforts?.every(
                                        (value) => value !== llmReasoningEffort(),
                                      )
                                      ? [
                                          {
                                            value: llmReasoningEffort(),
                                            label: `${llmReasoningEffort()} (${i18n.t("chat.effortUnsupported")})`,
                                          },
                                        ]
                                      : []),
                                    ...(
                                      snapshot()?.llm_profiles.find(
                                        (profile) => profile.id === editingLlmId(),
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
                                  disabled={llmSaving()}
                                  onValueChange={(next) => {
                                    if (next !== null) setLlmReasoningEffort(next);
                                  }}
                                  class="h-10 w-full rounded-md border-input bg-background px-3 text-sm text-foreground"
                                >
                                  <SelectItem value="">{i18n.t("chat.providerDefault")}</SelectItem>
                                  {llmReasoningEffort() &&
                                    snapshot()
                                      ?.llm_profiles.find(
                                        (profile) => profile.id === editingLlmId(),
                                      )
                                      ?.reasoning_efforts?.every(
                                        (value) => value !== llmReasoningEffort(),
                                      ) && (
                                      <SelectItem value={llmReasoningEffort()} disabled>
                                        {llmReasoningEffort()} ({i18n.t("chat.effortUnsupported")})
                                      </SelectItem>
                                    )}
                                  {(
                                    snapshot()?.llm_profiles.find(
                                      (profile) => profile.id === editingLlmId(),
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
                                    <SelectItem value={value}>{value}</SelectItem>
                                  ))}
                                </Select>
                              </label>
                            </div>
                            {isAnthropic() ? (
                              <div class="text-xs text-muted-foreground">
                                <p>{i18n.t("settings.llm.anthropicSamplingHelp")}</p>
                                <p>{i18n.t("settings.llm.anthropicTokenLimitHelp")}</p>
                              </div>
                            ) : null}
                          </>
                        ) : (
                          <>
                            <fieldset class="flex flex-col gap-1.5">
                              <legend class="text-xs font-medium text-muted-foreground">
                                {i18n.t("decision.capabilities")}
                              </legend>
                              <div class="flex flex-wrap gap-3 pt-1">
                                {DECISION_CAPABILITIES.map((capability) => (
                                  <label class="flex items-center gap-1.5 text-sm">
                                    <input
                                      type="checkbox"
                                      checked={decisionCapabilities().includes(capability)}
                                      onChange={(event) =>
                                        setDecisionCapabilities((current) =>
                                          event.target.checked
                                            ? [...current, capability]
                                            : current.filter((value) => value !== capability),
                                        )
                                      }
                                      class="size-4 accent-primary"
                                    />
                                    {i18n.t(`decision.${capability}`)}
                                  </label>
                                ))}
                              </div>
                            </fieldset>
                            <details>
                              <summary class="cursor-pointer text-xs font-medium text-muted-foreground">
                                {i18n.t("decision.limits")}
                              </summary>
                              <div class="mt-2 grid grid-cols-2 gap-2">
                                {(
                                  [
                                    "max_input_tokens_per_request",
                                    "max_output_tokens_per_request",
                                    "max_input_tokens_per_question",
                                    "max_output_tokens_per_question",
                                  ] as const
                                ).map((field) => (
                                  <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                                    {i18n.t(`decision.${field}`)}
                                    <Input
                                      type="number"
                                      min={1}
                                      max={1_000_000}
                                      step={1}
                                      value={decisionLimits()[field] ?? ""}
                                      onInput={(event) =>
                                        setDecisionLimits({
                                          ...decisionLimits(),
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

                        {llmError() ? (
                          <DismissibleNotice message={llmError()!} class="text-sm text-destructive">
                            {llmError()}
                          </DismissibleNotice>
                        ) : null}
                        <DialogFooter>
                          <Button
                            type="button"
                            variant="outline"
                            disabled={llmSaving()}
                            onClick={resetLlmForm}
                          >
                            {i18n.t("common.cancel")}
                          </Button>
                          <Button type="submit" disabled={llmSaving()}>
                            {llmSaving()
                              ? i18n.t("settings.saving")
                              : editingLlmId()
                                ? i18n.t("settings.llm.saveUpdate")
                                : i18n.t("settings.llm.saveCreate")}
                          </Button>
                        </DialogFooter>
                      </form>
                    </DialogContent>
                  </Dialog>
                </Section>
              )}

              {props.section === "agents" && (
                <Section
                  id="settings-agent-profiles"
                  title={i18n.t("settings.agent.title")}
                  description={i18n.t("settings.agent.description")}
                >
                  <form
                    onSubmit={(e) => void handleSaveAgent(e)}
                    class="mb-2 flex flex-col gap-3 border-b border-border pb-4"
                  >
                    <div class="flex items-center justify-between gap-2">
                      <h3 class="text-sm font-semibold">
                        {editingAgentId()
                          ? i18n.t("settings.agent.editTitle", { id: editingAgentId()! })
                          : i18n.t("settings.agent.createTitle")}
                      </h3>
                      {editingAgentId() ? (
                        <Button type="button" variant="ghost" size="xs" onClick={resetAgentForm}>
                          {i18n.t("settings.cancelEdit")}
                        </Button>
                      ) : null}
                    </div>
                    <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                      {i18n.t("settings.agent.name")}
                      <Input
                        value={agentName()}
                        onInput={(e) => setAgentName(e.target.value)}
                        placeholder={i18n.t("settings.agent.namePlaceholder")}
                        spellcheck="false"
                      />
                    </label>
                    <fieldset disabled={agentSaving()} class="flex flex-col gap-2">
                      <legend class="mb-2 text-xs font-medium text-muted-foreground">
                        {i18n.t("settings.agent.capabilities")}
                      </legend>
                      <div class="flex flex-wrap gap-4">
                        {(["chat", "coding", "decision"] as const).map((capability) => (
                          <label class="flex items-center gap-2 text-sm">
                            <input
                              type="checkbox"
                              class="size-4 accent-primary"
                              checked={agentCapabilities().includes(capability)}
                              onChange={() => toggleAgentCapability(capability)}
                            />
                            {i18n.t(`settings.agent.capability.${capability}`)}
                          </label>
                        ))}
                      </div>
                    </fieldset>
                    <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                      {i18n.t("settings.agent.instructionsLabel")}
                      <Textarea
                        value={agentInstructions()}
                        onInput={(e) => setAgentInstructions(e.target.value)}
                        placeholder={i18n.t("settings.agent.instructionsPlaceholder")}
                        rows={3}
                      />
                    </label>
                    <fieldset disabled={agentSaving()} class="flex min-w-0 flex-col gap-2">
                      <legend class="text-xs font-medium text-muted-foreground">
                        {i18n.t("settings.agent.targetsLabel")}
                      </legend>
                      <p class="text-xs text-muted-foreground">
                        {i18n.t("settings.agent.modelsHint")}
                      </p>
                      <div class="grid min-w-0 grid-cols-2 gap-3">
                        <section class="flex min-h-48 min-w-0 flex-col rounded-md border border-border">
                          <h4 class="border-b border-border px-3 py-2 text-sm font-medium">
                            {i18n.t("settings.agent.availableModels")}
                          </h4>
                          <div class="ocg-subtle-scrollbar max-h-80 min-w-0 overflow-y-auto overscroll-contain">
                            <ul class="p-2">
                              {unassignedModels().map((model) => (
                                <li class="flex items-center gap-2 rounded-md p-2 text-sm">
                                  <span class="min-w-0 flex-1">
                                    <span class="block break-words">{model.name}</span>
                                    <span
                                      class="block truncate text-xs text-muted-foreground"
                                      title={model.model}
                                    >
                                      {model.model}
                                    </span>
                                  </span>
                                  <Button
                                    type="button"
                                    size="icon-xs"
                                    variant="outline"
                                    aria-label={i18n.t("settings.agent.assignModel", {
                                      model: model.name,
                                    })}
                                    onClick={() => setAgentTargets([...assignedModels(), model.id])}
                                  >
                                    →
                                  </Button>
                                </li>
                              ))}
                              {!unassignedModels().length && (
                                <li class="p-2 text-xs text-muted-foreground">
                                  {i18n.t("settings.agent.noEligibleModels")}
                                </li>
                              )}
                            </ul>
                          </div>
                        </section>
                        <section class="flex min-h-48 min-w-0 flex-col rounded-md border border-border">
                          <h4 class="border-b border-border px-3 py-2 text-sm font-medium">
                            {i18n.t("settings.agent.assignedModels")}
                          </h4>
                          <div class="ocg-subtle-scrollbar max-h-80 min-w-0 overflow-y-auto overscroll-contain">
                            <ol class="p-2">
                              {assignedModels().map((id, index) => {
                                const model = eligibleModels().find((model) => model.id === id)!;
                                return (
                                  <li class="flex items-center gap-2 rounded-md p-2 text-sm">
                                    <Button
                                      type="button"
                                      size="icon-xs"
                                      variant="outline"
                                      aria-label={i18n.t("settings.agent.unassignModel", {
                                        model: model.name,
                                      })}
                                      onClick={() =>
                                        setAgentTargets(
                                          assignedModels().filter((value) => value !== id),
                                        )
                                      }
                                    >
                                      ←
                                    </Button>
                                    <span class="min-w-0 flex-1">
                                      <span class="block break-words">
                                        {model.name}
                                        {model.kind === "generative" &&
                                          !model.enabled &&
                                          ` (${i18n.t("settings.llm.disabled")})`}
                                      </span>
                                      <span
                                        class="block truncate text-xs text-muted-foreground"
                                        title={model.model}
                                      >
                                        {model.model}
                                      </span>
                                      {index === 0 && (
                                        <span class="text-xs text-primary">
                                          {i18n.t("settings.agent.defaultModel")}
                                        </span>
                                      )}
                                    </span>
                                    <div class="flex shrink-0 flex-col gap-1">
                                      <Button
                                        type="button"
                                        size="icon-xs"
                                        variant="ghost"
                                        disabled={agentSaving() || index === 0}
                                        aria-label={i18n.t("settings.agent.moveModelUp", {
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
                                          agentSaving() || index === assignedModels().length - 1
                                        }
                                        aria-label={i18n.t("settings.agent.moveModelDown", {
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
                              {!assignedModels().length && (
                                <li class="p-2 text-xs text-muted-foreground">
                                  {i18n.t("settings.agent.noAssignedModels")}
                                </li>
                              )}
                            </ol>
                          </div>
                        </section>
                      </div>
                    </fieldset>

                    {agentError() ? (
                      <DismissibleNotice message={agentError()!} class="text-sm text-destructive">
                        {agentError()}
                      </DismissibleNotice>
                    ) : null}
                    {agentNotice() ? (
                      <p role="status" class="text-sm text-emerald-700 dark:text-emerald-400">
                        {agentNotice()}
                      </p>
                    ) : null}

                    <div class="flex justify-end gap-2">
                      <Button
                        type="submit"
                        disabled={
                          agentSaving() || !agentCapabilities().length || !assignedModels().length
                        }
                      >
                        {agentSaving()
                          ? i18n.t("settings.saving")
                          : editingAgentId()
                            ? i18n.t("settings.agent.saveUpdate")
                            : i18n.t("settings.agent.saveCreate")}
                      </Button>
                    </div>
                  </form>
                  <div class="divide-y divide-border/70 py-2">
                    {agentProfiles().length === 0 ? (
                      <p class="py-2 text-sm text-muted-foreground">
                        {i18n.t("settings.agent.empty")}
                      </p>
                    ) : (
                      agentProfiles().map((agent) => (
                        <div class="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between">
                          <div class="min-w-0 space-y-1">
                            <div class="flex flex-wrap items-center gap-2">
                              <span class="text-sm font-semibold">{agent.name}</span>
                              <Pill tone="slate" class="text-xs normal-case">
                                {agent.id}
                              </Pill>
                              {agent.capabilities.map((capability) => (
                                <Pill tone="sky" class="text-xs">
                                  {i18n.t(`settings.agent.capability.${capability}`)}
                                </Pill>
                              ))}
                              {!agent.editable ? (
                                <Pill tone="amber" class="text-xs normal-case">
                                  {i18n.t("settings.locked", {
                                    jobs: agent.blocking_jobs.join(", "),
                                  })}
                                </Pill>
                              ) : null}
                            </div>
                            <p class="text-xs text-muted-foreground">
                              {i18n.t("settings.agent.targets", {
                                targets: agent.llm_targets.join(", "),
                              })}
                            </p>
                            {agent.instructions ? (
                              <p class="line-clamp-2 whitespace-pre-wrap text-xs text-muted-foreground">
                                {i18n.t("settings.agent.instructions", {
                                  instructions: agent.instructions,
                                })}
                              </p>
                            ) : (
                              <p class="text-xs italic text-muted-foreground">
                                {i18n.t("settings.agent.noInstructions")}
                              </p>
                            )}
                          </div>
                          <div class="shrink-0">
                            <Button
                              type="button"
                              variant="outline"
                              size="xs"
                              disabled={!agent.editable}
                              onClick={() => startEditAgent(agent)}
                            >
                              {editingAgentId() === agent.id
                                ? i18n.t("settings.editing")
                                : i18n.t("common.edit")}
                            </Button>
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </Section>
              )}

              {props.section === "decisions" && snapshot() && (
                <DecisionProfilesSection
                  client={props.client}
                  profiles={snapshot()!}
                  onSaved={setSnapshot}
                  onManageModels={() => props.onNavigate("models")}
                />
              )}
              {props.section === "projects" && <ProjectsSection client={props.client} />}
              {props.section === "execution" && <PoolsSection client={props.client} />}
            </>
          ) : null}

          {props.section === "appearance" && (
            <Section
              id="settings-appearance"
              title={i18n.t("settings.section.appearance")}
              description={i18n.t("settings.appearance.description")}
              action={
                <Button
                  variant="ghost"
                  size="sm"
                  class="h-11 rounded-md tracking-normal normal-case"
                  onClick={theme.resetAppearance}
                >
                  {i18n.t("common.reset")}
                </Button>
              }
            >
              <SettingRow
                label={i18n.t("settings.language.label")}
                description={i18n.t("settings.language.desc")}
                origin="browser"
              >
                <LanguageSwitcher />
              </SettingRow>
              <SettingRow
                label={i18n.t("settings.theme.label")}
                description={i18n.t("settings.theme.desc")}
                origin="browser"
              >
                <ChoiceGroup
                  label={i18n.t("settings.theme.label")}
                  value={theme.preferences.theme}
                  onChange={theme.setTheme}
                  options={[
                    {
                      value: "light",
                      label: i18n.t("settings.theme.light"),
                      preview: <Sun aria-hidden="true" class="size-4" />,
                    },
                    {
                      value: "dark",
                      label: i18n.t("settings.theme.dark"),
                      preview: <Moon aria-hidden="true" class="size-4" />,
                    },
                  ]}
                />
              </SettingRow>
              <SettingRow
                label={i18n.t("settings.accent.label")}
                description={i18n.t("settings.accent.desc")}
                origin="browser"
              >
                <ChoiceGroup
                  label={i18n.t("settings.accent.label")}
                  value={theme.preferences.accent}
                  onChange={theme.setAccent}
                  options={[
                    {
                      value: "ochre",
                      label: i18n.t("settings.accent.ochre"),
                      preview: (
                        <span aria-hidden="true" class="size-3 rounded-full bg-(--accent-ochre)" />
                      ),
                    },
                    {
                      value: "slate",
                      label: i18n.t("settings.accent.slate"),
                      preview: (
                        <span aria-hidden="true" class="size-3 rounded-full bg-(--accent-slate)" />
                      ),
                    },
                    {
                      value: "teal",
                      label: i18n.t("settings.accent.teal"),
                      preview: (
                        <span aria-hidden="true" class="size-3 rounded-full bg-(--accent-teal)" />
                      ),
                    },
                  ]}
                />
              </SettingRow>
            </Section>
          )}
          {props.section === "session" && (
            <Section
              id="settings-session"
              title={i18n.t("settings.session.title")}
              description={i18n.t("settings.session.description")}
              action={
                <Button
                  variant="outline"
                  size="sm"
                  class="h-11 rounded-md tracking-normal normal-case"
                  disabled={props.signingOut}
                  onClick={props.onSignOut}
                >
                  {props.signingOut
                    ? i18n.t("settings.session.signingOut")
                    : i18n.t("settings.session.signOut")}
                </Button>
              }
            >
              <SettingRow
                label={i18n.t("settings.session.expires")}
                description={i18n.t("settings.session.expiresDescription")}
                origin="runtime"
              >
                <span class={value}>
                  {props.sessionExpiresAtUs === null
                    ? "—"
                    : new Date(props.sessionExpiresAtUs / 1000).toLocaleString()}
                </span>
              </SettingRow>
            </Section>
          )}
          {props.section === "system" && props.client && (
            <>
              <UpdatesSection client={props.client} />
              <GlobalCompactionSection client={props.client} profiles={snapshot()} />
              <GlobalHeadersSection client={props.client} />
            </>
          )}
        </div>
      </div>
    </PageSurface>
  );
}
