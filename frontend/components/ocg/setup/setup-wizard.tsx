"use client";

import { useI18n, type I18nKey } from "../i18n";
import { useState, useCallback, useEffect, useRef } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { ArrowLeft, ArrowRight, Check, Loader2, FolderOpen, ChevronRight, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";
import { createHttpCanonicalControlClient } from "../runtime/canonical-client";
import { createSetupClient } from "./setup-client";
import { PROVIDER_PRESETS } from "./provider-presets";
import { resolveProjectParam, withProjectParam, type ProjectId } from "../project/domain";
import type { ProfileView, SetupModel, SetupBrowseResponse } from "../contracts";

/**
 * First-run and repair setup: Welcome → Connect model → Verify → Enter Chat.
 *
 * Readiness is never decided here. `GET /api/v1/profile` returns the backend's
 * `runnable_choices` (selection + usable endpoint + Vault credential), which is
 * the same authority the shell gate and canonical launch use; Chat additionally
 * needs one registered Project, read from the canonical Project registry.
 */
type SetupStep = "checking" | "welcome" | "connect" | "models" | "verified";

type Progress = "connect" | "verify" | "chat";

const PROGRESS: readonly { id: Progress; label: I18nKey }[] = [
  { id: "connect", label: "setup.stepConnect" },
  { id: "verify", label: "setup.stepVerify" },
  { id: "chat", label: "setup.stepChat" },
];

interface ProviderState {
  name: string;
  endpoint: string;
}

interface ConnectResult {
  providerKey: string;
  models: SetupModel[];
}

/** What the verified summary shows, all read back from the canonical Profile view. */
interface Verified {
  provider: string | null;
  model: string;
  runnable: number;
}

/**
 * The Project setup enters the workspace with.
 *
 * An explicit Project is carried through exactly as given, registered or not:
 * dropping it would let a Project the browser persisted earlier win over the
 * one OCG was launched with. An unknown explicit Project is forwarded so the
 * workspace and backend reject or report it, never silently swapped for a
 * different one. List position is consulted only when no Project was named.
 */
function entryProject(requested: ProjectId | undefined, registered: ReadonlyArray<{ id: string }>): ProjectId {
  if (requested !== undefined) return requested;
  return registered[0]?.id ?? "";
}

/** The workspace entry URL for the Project setup resolved. Canonical product
 * URLs carry only `project=`; fixture `scenario=` never leaks here. */
function workspaceHref(projectId: ProjectId): string {
  return projectId ? withProjectParam("/", projectId) : "/";
}

function verifiedFrom(view: ProfileView): Verified | null {
  const profile = view.profile;
  if (!profile || view.runnable_choices.length === 0) return null;
  // The default model is what a new Chat uses; fall back to the first
  // backend-runnable key only for presentation when no default is recorded.
  const key = profile.defaultModel && view.runnable_choices.includes(profile.defaultModel)
    ? profile.defaultModel
    : view.runnable_choices[0];
  const model = profile.models[key];
  return {
    provider: model ? profile.providers[model.provider]?.label ?? model.provider : null,
    model: model?.label ?? model?.id ?? key,
    runnable: view.runnable_choices.length,
  };
}

export function SetupWizard() {
  const { t } = useI18n();
  const router = useRouter();
  const searchParams = useSearchParams();
  const controlUrl = useOcgControlUrl();
  // The explicit Project the launcher entered with, if any. It survives setup
  // and still addresses the workspace once setup is done. Absent stays absent:
  // setup never invents a Project.
  const requestedProject = resolveProjectParam(searchParams.get("project"));

  const [step, setStep] = useState<SetupStep>("checking");
  const [checkAttempt, setCheckAttempt] = useState(0);
  const [checkError, setCheckError] = useState<string | null>(null);
  const [repair, setRepair] = useState(false);
  const [busy, setBusy] = useState<"connect" | "refresh" | "verify" | "project" | null>(null);
  const loading = busy !== null;
  const [error, setError] = useState<string | null>(null);

  const [provider, setProvider] = useState<ProviderState>({ name: "", endpoint: "" });
  // The API key lives only in the uncontrolled input and is cleared after each
  // attempt; it never enters React state, storage, or a URL.
  const apiKeyInput = useRef<HTMLInputElement>(null);
  const [connectResult, setConnectResult] = useState<ConnectResult | null>(null);

  const [selectedModels, setSelectedModels] = useState<Set<string>>(new Set());
  const [defaultModel, setDefaultModel] = useState<string>("");
  const [profileRevision, setProfileRevision] = useState<string>("");

  const [verified, setVerified] = useState<Verified | null>(null);
  const [projects, setProjects] = useState<Array<{ id: string; name: string; root: string }>>([]);

  const [browseResult, setBrowseResult] = useState<SetupBrowseResponse | null>(null);
  const [selectedFolders, setSelectedFolders] = useState<string[]>([]);
  const [manualPath, setManualPath] = useState<string>("");

  // Decide where setup starts from canonical state: a runnable installation
  // with a Project skips setup entirely; a saved but non-runnable connection
  // is a repair, not a fresh install.
  useEffect(() => {
    if (!controlUrl) return;
    let cancelled = false;
    void (async () => {
      try {
        const view = await createProfileClient(controlUrl, fetch).read();
        const registered = await createHttpCanonicalControlClient({ baseUrl: controlUrl, fetch }).listProjects();
        if (cancelled) return;
        const ready = verifiedFrom(view);
        const known = registered.map((record) => ({ id: record.project_id, name: record.root, root: record.root }));
        setProjects(known);
        setCheckError(null);
        if (ready && known.length > 0) {
          // An explicit Project outranks list order: a runnable installation
          // enters the Project it was launched with.
          router.replace(workspaceHref(entryProject(requestedProject, known)));
          return;
        }
        if (ready) {
          setVerified(ready);
          setStep("verified");
          return;
        }
        const configured = Object.keys(view.profile?.providers ?? {}).length > 0;
        setRepair(configured);
        setStep(configured ? "connect" : "welcome");
      } catch (cause) {
        if (!cancelled) setCheckError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
    return () => { cancelled = true; };
  }, [controlUrl, requestedProject, router, checkAttempt]);

  useEffect(() => {
    if (step !== "verified" || projects.length > 0 || browseResult || !controlUrl) return;
    let cancelled = false;
    createSetupClient(controlUrl, fetch).browseDirectory().then((value) => {
      if (!cancelled) setBrowseResult(value);
    }).catch(() => {
      // Browsing is a convenience; a typed path still works.
    });
    return () => { cancelled = true; };
  }, [step, projects.length, browseResult, controlUrl]);

  const handleConnectProvider = useCallback(async () => {
    if (!controlUrl) {
      setError(t("setup.noEndpoint"));
      return;
    }
    const apiKey = apiKeyInput.current?.value.trim() ?? "";
    if (!provider.name.trim() || !provider.endpoint.trim() || !apiKey) {
      setError(t("setup.required"));
      return;
    }

    setBusy("connect");
    setError(null);
    try {
      // The Profile must exist before a provider can be filed under it. On a
      // fresh machine there is nothing yet, so bootstrap it first.
      const profileClient = createProfileClient(controlUrl, fetch);
      const existing = await profileClient.read();
      if (!existing?.profile) {
        await profileClient.createNew();
      }

      // The backend owns the whole provider step: it lists `/models` before
      // writing anything, writes the credential to the Vault, persists the
      // Provider, and returns the catalog it actually found.
      const result = await createSetupClient(controlUrl, fetch).connectProvider(
        provider.name.trim(),
        provider.endpoint.trim(),
        apiKey,
      );

      setProfileRevision(result.revision);
      setConnectResult({ providerKey: result.provider_key, models: result.models });
      const first = result.models[0];
      setSelectedModels(first ? new Set([first.key]) : new Set());
      setDefaultModel(first?.key ?? "");
      setStep("models");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.connectFailed"));
    } finally {
      if (apiKeyInput.current) apiKeyInput.current.value = "";
      setBusy(null);
    }
  }, [controlUrl, provider, t]);

  const handleRefreshModels = useCallback(async () => {
    if (!controlUrl || !connectResult) return;
    setBusy("refresh");
    setError(null);
    try {
      const result = await createSetupClient(controlUrl, fetch).refreshModels(connectResult.providerKey, profileRevision);
      const retained = new Set(result.models.filter((model) => selectedModels.has(model.key)).map((model) => model.key));
      if (retained.size === 0 && result.models[0]) retained.add(result.models[0].key);
      setConnectResult({ providerKey: result.provider_key, models: result.models });
      setProfileRevision(result.revision);
      setSelectedModels(retained);
      setDefaultModel(retained.has(defaultModel) ? defaultModel : retained.values().next().value ?? "");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.refreshFailed"));
    } finally {
      setBusy(null);
    }
  }, [controlUrl, connectResult, profileRevision, selectedModels, defaultModel, t]);

  const handleVerify = useCallback(async () => {
    if (!controlUrl || !connectResult) return;
    if (selectedModels.size === 0) {
      setError(t("setup.selectModel"));
      return;
    }
    if (!defaultModel || !selectedModels.has(defaultModel)) {
      setError(t("setup.chooseDefault"));
      return;
    }

    setBusy("verify");
    setError(null);
    try {
      const modelsToSave = Array.from(selectedModels).map((key) => {
        const model = connectResult.models.find((m) => m.key === key);
        if (!model) throw new Error(t("setup.refreshCatalog"));
        return { key: model.key, id: model.id };
      });
      // The backend refuses a default model that is not executable.
      const result = await createSetupClient(controlUrl, fetch).saveModels(
        connectResult.providerKey,
        modelsToSave,
        defaultModel,
        profileRevision,
      );
      setProfileRevision(result.revision);
      setSelectedModels(new Set(result.selected_models));
      setDefaultModel(result.default_model);

      // Read readiness back from the same authority the shell gate uses.
      const ready = verifiedFrom(await createProfileClient(controlUrl, fetch).read());
      if (!ready) {
        setError(t("setup.noRunnable"));
        return;
      }
      setVerified(ready);
      setStep("verified");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.saveFailed"));
    } finally {
      setBusy(null);
    }
  }, [controlUrl, connectResult, selectedModels, defaultModel, profileRevision, t]);

  const handleBrowse = useCallback(async (path: string) => {
    if (!controlUrl) return;
    setBusy("project");
    try {
      setBrowseResult(await createSetupClient(controlUrl, fetch).browseDirectory(path));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.browseFailed"));
    } finally {
      setBusy(null);
    }
  }, [controlUrl, t]);

  const handleSelectFolder = useCallback((path: string) => {
    setSelectedFolders((current) =>
      current.includes(path) ? current.filter((p) => p !== path) : [...current, path],
    );
  }, []);

  const handleAddManualPath = useCallback(() => {
    const trimmed = manualPath.trim();
    if (trimmed && !selectedFolders.includes(trimmed)) {
      setSelectedFolders((current) => [...current, trimmed]);
      setManualPath("");
    }
  }, [manualPath, selectedFolders]);

  const handleImportProjects = useCallback(async () => {
    if (!controlUrl) return;
    if (selectedFolders.length === 0) {
      setError(t("setup.selectFolder"));
      return;
    }
    setBusy("project");
    setError(null);
    try {
      const setupClient = createSetupClient(controlUrl, fetch);
      const imported: Array<{ id: string; name: string; root: string }> = [];
      for (const folder of selectedFolders) {
        const commandId = `cmd-setup-project-${Date.now().toString(36)}-${imported.length}`;
        const result = await setupClient.initProject(commandId, folder);
        imported.push({ id: result.project_id, name: result.name, root: result.root });
      }
      setProjects(imported);
      setSelectedFolders([]);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.importFailed"));
    } finally {
      setBusy(null);
    }
  }, [controlUrl, selectedFolders, t]);

  const enterChat = useCallback(() => {
    router.push(workspaceHref(entryProject(requestedProject, projects)));
  }, [projects, requestedProject, router]);

  const progress: Progress = step === "verified" ? projects.length > 0 ? "chat" : "verify" : "connect";
  const progressIndex = PROGRESS.findIndex((item) => item.id === progress);

  if (!controlUrl || step === "checking") {
    return (
      <main className="flex min-h-dvh flex-col items-center justify-center gap-3 bg-background p-6 text-center text-[13px] text-foreground">
        {!controlUrl ? (
          <p role="alert" className="text-destructive">{t("setup.noEndpoint")}</p>
        ) : checkError ? (
          <>
            <p role="alert" className="max-w-md break-words text-destructive">{t("setup.checkFailed", { error: checkError })}</p>
            <Button size="sm" variant="outline" onClick={() => { setCheckError(null); setCheckAttempt((value) => value + 1); }}>{t("common.retry")}</Button>
          </>
        ) : (
          <p role="status" className="text-muted-foreground">{t("setup.checking")}</p>
        )}
      </main>
    );
  }

  if (step === "welcome") {
    return (
      <main className="flex min-h-dvh items-center justify-center bg-background px-4 py-8 text-foreground">
        <section className="w-full max-w-md space-y-4 rounded-lg border border-border p-6">
          <h1 className="text-[18px] font-semibold tracking-tight">{t("setup.welcomeTitle")}</h1>
          <p className="text-[13px] leading-6 text-muted-foreground">{t("setup.welcomeBody")}</p>
          <Button onClick={() => setStep("connect")}>
            {t("setup.stepConnect")}
            <ArrowRight className="size-3.5" aria-hidden="true" />
          </Button>
        </section>
      </main>
    );
  }

  return (
    <main className="flex min-h-dvh justify-center bg-background px-4 py-8 text-foreground sm:py-12">
      <div className="flex w-full min-w-0 max-w-xl flex-col">
        <header>
          <h1 className="text-[18px] font-semibold tracking-tight">{t(step !== "verified" ? "setup.stepConnect" : connectResult ? "setup.verifiedTitle" : "setup.readyTitle")}</h1>
          <ol className="mt-4 grid grid-cols-3 gap-1" aria-label={t("setup.steps")}>
            {PROGRESS.map((item, index) => {
              const isCurrent = index === progressIndex;
              const isDone = index < progressIndex;
              return (
                <li key={item.id} className="min-w-0" aria-current={isCurrent ? "step" : undefined}>
                  <div className={cn(
                    "flex w-full items-center gap-1.5 rounded-md border px-2 py-1.5",
                    isCurrent ? "border-foreground/40 bg-muted" : "border-border",
                    !isCurrent && !isDone && "opacity-60",
                  )}>
                    <span className={cn(
                      "flex size-4 shrink-0 items-center justify-center rounded-full border text-[9px]",
                      isDone ? "border-emerald-600 bg-emerald-600 text-white" : "border-border text-muted-foreground",
                    )} aria-hidden="true">
                      {isDone ? <Check className="size-2.5" /> : index + 1}
                    </span>
                    <span className="truncate text-[10px] text-muted-foreground">{t(item.label)}</span>
                  </div>
                </li>
              );
            })}
          </ol>
        </header>

        <div className="mt-5 flex flex-col gap-3">
          {repair && step === "connect" && (
            <p className="rounded-md border border-border p-3 text-[12px] text-muted-foreground">{t("setup.repairNotice")}</p>
          )}
          {error && (
            <div role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/5 p-3">
              <AlertCircle className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden="true" />
              <p className="min-w-0 break-words text-[12px] text-destructive">{error}</p>
            </div>
          )}

          <section className="min-w-0 rounded-lg border border-border bg-muted/10 p-4">
            {step === "connect" && (
              <ConnectProviderPanel provider={provider} onChange={setProvider} loading={loading} apiKeyInput={apiKeyInput} />
            )}
            {step === "models" && connectResult && (
              <ChooseModelsPanel
                onRefresh={() => void handleRefreshModels()}
                loading={loading}
                models={connectResult.models}
                selectedModels={selectedModels}
                defaultModel={defaultModel}
                onToggleModel={(key) => {
                  const next = new Set(selectedModels);
                  if (next.has(key)) next.delete(key); else next.add(key);
                  setSelectedModels(next);
                  if (!next.has(defaultModel)) setDefaultModel(next.values().next().value ?? "");
                }}
                onSetDefault={setDefaultModel}
              />
            )}
            {step === "verified" && verified && <VerifiedSummary verified={verified} checkedNow={connectResult !== null} />}
          </section>

          {step === "verified" && (
            <section className="min-w-0 rounded-lg border border-border bg-muted/10 p-4">
              {projects.length > 0 ? (
                <dl className="text-[12px]">
                  <dt className="text-[11px] text-muted-foreground">{t("setup.project")}</dt>
                  <dd className="mt-0.5 break-all font-medium">{projects[0].root}</dd>
                </dl>
              ) : (
                <AddProjectsPanel
                  browseResult={browseResult}
                  selectedFolders={selectedFolders}
                  manualPath={manualPath}
                  onBrowse={handleBrowse}
                  onSelectFolder={handleSelectFolder}
                  onManualPathChange={setManualPath}
                  onAddManualPath={handleAddManualPath}
                  loading={loading}
                />
              )}
            </section>
          )}
        </div>

        <footer className="mt-5 flex flex-wrap items-center justify-between gap-2">
          {step === "models" ? (
            <Button size="sm" variant="ghost" disabled={loading} onClick={() => { setError(null); setStep("connect"); }}>
              <ArrowLeft className="size-3.5" aria-hidden="true" />{t("setup.editConnection")}
            </Button>
          ) : step === "verified" && connectResult ? (
            <Button size="sm" variant="ghost" disabled={loading} onClick={() => { setError(null); setStep("models"); }}>
              <ArrowLeft className="size-3.5" aria-hidden="true" />{t("setup.changeModel")}
            </Button>
          ) : <span />}

          {step === "connect" && (
            <Button size="sm" onClick={() => void handleConnectProvider()} disabled={loading}>
              {busy === "connect" ? <Loader2 className="size-3.5 animate-spin" /> : null}
              {t(busy === "connect" ? "setup.connecting" : "setup.connect")}
            </Button>
          )}
          {step === "models" && (
            <Button size="sm" onClick={() => void handleVerify()} disabled={loading || selectedModels.size === 0}>
              {busy === "verify" ? <Loader2 className="size-3.5 animate-spin" /> : null}
              {t(busy === "verify" ? "setup.verifying" : "setup.verify")}
            </Button>
          )}
          {step === "verified" && projects.length === 0 && (
            <Button size="sm" onClick={() => void handleImportProjects()} disabled={loading || selectedFolders.length === 0}>
              {busy === "project" ? <Loader2 className="size-3.5 animate-spin" /> : null}
              {t("setup.addProject")}
            </Button>
          )}
          {step === "verified" && projects.length > 0 && (
            <Button size="sm" onClick={enterChat} disabled={loading || !verified}>
              {t("setup.enterChat")}
              <ArrowRight className="size-3.5" aria-hidden="true" />
            </Button>
          )}
        </footer>
      </div>
    </main>
  );
}

// -- Step panels -------------------------------------------------------------

/**
 * `checkedNow` is true only when this session connected the endpoint and listed
 * its models; a configuration found already runnable claims readiness alone.
 */
function VerifiedSummary({ verified, checkedNow }: { verified: Verified; checkedNow: boolean }) {
  const { t } = useI18n();
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <dl className="grid gap-2 text-[12px]">
        <div className="min-w-0">
          <dt className="text-[11px] text-muted-foreground">{t("setup.summaryProvider")}</dt>
          <dd className="mt-0.5 break-words font-medium">{verified.provider ?? t("common.notReported")}</dd>
        </div>
        <div className="min-w-0">
          <dt className="text-[11px] text-muted-foreground">{t("setup.summaryModel")}</dt>
          <dd className="mt-0.5 break-all font-medium">{verified.model}</dd>
        </div>
        <div>
          <dt className="text-[11px] text-muted-foreground">{t("setup.summaryRunnable")}</dt>
          <dd className="mt-0.5 font-medium tabular-nums">{verified.runnable}</dd>
        </div>
      </dl>
      <p className="text-[11px] leading-5 text-muted-foreground">{t(checkedNow ? "setup.verifiedFacts" : "setup.readyFacts")}</p>
    </div>
  );
}

function ConnectProviderPanel({
  provider,
  onChange,
  loading,
  apiKeyInput,
}: {
  apiKeyInput: React.RefObject<HTMLInputElement | null>;
  provider: ProviderState;
  onChange: (state: ProviderState) => void;
  loading: boolean;
}) {
  const { t } = useI18n();
  return (
    <div className="flex flex-col gap-4">
      <p className="text-[12px] text-muted-foreground">{t("setup.connectHint")}</p>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-[12px] font-medium">{t("setup.presets")}</span>
        {PROVIDER_PRESETS.map((preset) => (
          <button
            key={preset.id}
            type="button"
            className="rounded-md border border-border px-2.5 py-1 text-[12px] hover:bg-muted"
            onClick={() => onChange({ name: preset.label, endpoint: preset.endpoint })}
            disabled={loading}
          >
            {preset.label}
          </button>
        ))}
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor="setup-provider-name" className="text-[12px] font-medium">
          {t("setup.providerName")}
        </label>
        <input
          id="setup-provider-name"
          type="text"
          className="rounded-md border border-border bg-background px-3 py-2 text-[13px]"
          placeholder={t("setup.providerName")}
          value={provider.name}
          onChange={(event) => onChange({ ...provider, name: event.target.value })}
          disabled={loading}
        />
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor="setup-provider-endpoint" className="text-[12px] font-medium">
          {t("setup.providerEndpoint")}
        </label>
        <input
          id="setup-provider-endpoint"
          type="url"
          className="rounded-md border border-border bg-background px-3 py-2 text-[13px]"
          placeholder="https://api.example.com/v1"
          value={provider.endpoint}
          onChange={(event) => onChange({ ...provider, endpoint: event.target.value })}
          disabled={loading}
        />
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor="setup-provider-apikey" className="text-[12px] font-medium">
          {t("setup.apiKey")}
        </label>
        <input
          id="setup-provider-apikey"
          type="password"
          className="rounded-md border border-border bg-background px-3 py-2 text-[13px]"
          placeholder="••••••••••••••"
          ref={apiKeyInput}
          disabled={loading}
          autoComplete="off"
        />
      </div>
    </div>
  );
}

function ChooseModelsPanel({
  models,
  onRefresh,
  loading,
  selectedModels,
  defaultModel,
  onToggleModel,
  onSetDefault,
}: {
  models: SetupModel[];
  onRefresh: () => void;
  loading: boolean;
  selectedModels: Set<string>;
  defaultModel: string;
  onToggleModel: (key: string) => void;
  onSetDefault: (key: string) => void;
}) {
  const { t } = useI18n();
  if (models.length === 0) {
    return <p className="text-[12px] text-muted-foreground">{t("setup.noModels")}</p>;
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="min-w-0 flex-1 text-[12px] text-muted-foreground">{t("setup.modelsHint")}</p>
        <Button size="xs" variant="outline" onClick={onRefresh} disabled={loading}>{t("setup.refreshModels")}</Button>
      </div>
      <ul className="max-h-[50dvh] divide-y divide-border overflow-y-auto rounded-md border border-border">
        {models.map((model) => {
          const selected = selectedModels.has(model.key);
          const isDefault = defaultModel === model.key;
          return (
            <li key={model.key} className="flex items-center gap-2 px-3 py-2.5">
              <input
                type="checkbox"
                className="size-4 shrink-0"
                checked={selected}
                disabled={loading}
                onChange={() => onToggleModel(model.key)}
                aria-label={t("setup.selectNamedModel", { model: model.label })}
              />
              <div className="min-w-0 flex-1">
                <p className="break-all text-[12px] font-medium">{model.label}</p>
              </div>
              {selected && (
                <Button
                  size="xs"
                  variant={isDefault ? "default" : "outline"}
                  disabled={loading}
                  onClick={() => onSetDefault(model.key)}
                >
                  {t(isDefault ? "setup.default" : "setup.setDefault")}
                </Button>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function AddProjectsPanel({
  browseResult,
  selectedFolders,
  manualPath,
  onBrowse,
  onSelectFolder,
  onManualPathChange,
  onAddManualPath,
  loading,
}: {
  browseResult: SetupBrowseResponse | null;
  selectedFolders: string[];
  manualPath: string;
  onBrowse: (path: string) => void;
  onSelectFolder: (path: string) => void;
  onManualPathChange: (path: string) => void;
  onAddManualPath: () => void;
  loading: boolean;
}) {
  const { t } = useI18n();
  return (
    <div className="flex flex-col gap-4">
      <p className="text-[12px] text-muted-foreground">
        {t("setup.projectsHint")}
      </p>

      {/* Manual path entry */}
      <div className="flex gap-2">
        <input
          type="text"
          className="min-w-0 flex-1 rounded-md border border-border bg-background px-3 py-2 text-[13px]"
          placeholder="/path/to/project"
          value={manualPath}
          onChange={(event) => onManualPathChange(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              onAddManualPath();
            }
          }}
          disabled={loading}
        />
        <Button size="sm" variant="outline" onClick={onAddManualPath} disabled={loading || !manualPath.trim()}>
          {t("common.add")}
        </Button>
      </div>

      {/* Selected folders */}
      {selectedFolders.length > 0 && (
        <div className="flex flex-col gap-1">
          <p className="text-[11px] font-medium text-muted-foreground">{t("setup.selected", { count: selectedFolders.length })}</p>
          <ul className="divide-y divide-border rounded-md border border-border">
            {selectedFolders.map((folder) => (
              <li key={folder} className="flex items-center gap-2 px-3 py-2">
                <FolderOpen className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span className="min-w-0 flex-1 break-all text-[12px]">{folder}</span>
                <Button
                  size="xs"
                  variant="ghost"
                  onClick={() => onSelectFolder(folder)}
                  disabled={loading}
                >
                  {t("common.remove")}
                </Button>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Filesystem browser */}
      {browseResult && (
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2">
            <p className="text-[11px] font-medium text-muted-foreground">{t("setup.browse")}</p>
            {browseResult.parent && (
              <Button
                size="xs"
                variant="ghost"
                onClick={() => browseResult.parent && onBrowse(browseResult.parent)}
                disabled={loading}
              >
                <ArrowLeft className="size-3" /> {t("setup.up")}
              </Button>
            )}
          </div>
          <p className="break-all text-[11px] text-muted-foreground">{browseResult.current}</p>
          <ul className="max-h-[200px] divide-y divide-border overflow-y-auto rounded-md border border-border">
            {browseResult.entries.map((entry) => {
              const isSelected = selectedFolders.includes(entry.path);
              return (
                <li key={entry.path} className="flex items-center gap-2 px-3 py-1.5">
                  <FolderOpen className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                  <button
                    type="button"
                    className="min-w-0 flex-1 truncate text-left text-[12px] hover:underline"
                    onClick={() => onBrowse(entry.path)}
                    disabled={loading}
                  >
                    {entry.name}
                  </button>
                  <Button
                    size="xs"
                    variant={isSelected ? "default" : "outline"}
                    onClick={(event) => {
                      event.stopPropagation();
                      onSelectFolder(entry.path);
                    }}
                    disabled={loading}
                  >
                    {isSelected ? <Check className="size-3" /> : t("common.select")}
                  </Button>
                  <button
                    type="button"
                    className="shrink-0 text-muted-foreground hover:text-foreground"
                    onClick={() => onBrowse(entry.path)}
                    disabled={loading}
                    aria-label={t("setup.openFolder", { name: entry.name })}
                  >
                    <ChevronRight className="size-3.5" />
                  </button>
                </li>
              );
            })}
            {browseResult.entries.length === 0 && (
              <li className="px-3 py-2 text-[11px] text-muted-foreground">
                {t("setup.noDirectories")}
              </li>
            )}
          </ul>
        </div>
      )}
    </div>
  );
}
