"use client";

import { useState, useCallback, useRef } from "react";
import { useRouter } from "next/navigation";
import { ArrowLeft, ArrowRight, Check, Loader2, FolderOpen, ChevronRight, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";
import { createSetupClient } from "./setup-client";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useProject } from "../project/project-context";
import type { SetupModel, SetupBrowseResponse } from "../contracts";

type SetupStep = "provider" | "models" | "projects";

const STEP_LABELS: Record<SetupStep, string> = {
  provider: "Connect Provider",
  models: "Choose Models",
  projects: "Add Projects",
};

const STEPS: SetupStep[] = ["provider", "models", "projects"];

interface ProviderState {
  name: string;
  endpoint: string;
}

interface ConnectResult {
  providerKey: string;
  models: SetupModel[];
}

export function SetupWizard() {
  const router = useRouter();
  const controlUrl = useOcgControlUrl();
  const { completeOnboarding } = useOcgRuntime();
  const { setActiveProject } = useProject();

  const [step, setStep] = useState<SetupStep>("provider");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Provider state
  const [provider, setProvider] = useState<ProviderState>({
    name: "",
    endpoint: "",
  });
  const apiKeyInput = useRef<HTMLInputElement>(null);
  const [connectResult, setConnectResult] = useState<ConnectResult | null>(null);

  // Models state
  const [selectedModels, setSelectedModels] = useState<Set<string>>(new Set());
  const [defaultModel, setDefaultModel] = useState<string>("");
  const [profileRevision, setProfileRevision] = useState<string>("");

  // Projects state
  const [browseResult, setBrowseResult] = useState<SetupBrowseResponse | null>(null);
  const [selectedFolders, setSelectedFolders] = useState<string[]>([]);
  const [manualPath, setManualPath] = useState<string>("");
  const [importedProjects, setImportedProjects] = useState<Array<{ id: string; name: string; root: string }>>([]);

  const currentIndex = STEPS.indexOf(step);

  const handleConnectProvider = useCallback(async () => {
    if (!controlUrl) {
      setError("No loopback control endpoint available.");
      return;
    }
    const apiKey = apiKeyInput.current?.value.trim() ?? "";
    if (!provider.name.trim() || !provider.endpoint.trim() || !apiKey) {
      setError("All fields are required.");
      return;
    }

    setLoading(true);
    setError(null);
    try {
      // The Profile must exist before a provider can be filed under it. On a
      // fresh machine there is nothing yet, so bootstrap it first.
      const profileClient = createProfileClient(controlUrl, fetch);
      const existing = await profileClient.read();
      if (!existing?.profile) {
        await profileClient.createNew();
      }

      // The backend owns the whole provider step: it derives both the
      // `/models` URL and the canonical chat endpoint, writes the credential to
      // the Vault, persists the Provider, and returns what it actually found.
      const setupClient = createSetupClient(controlUrl, fetch);
      const result = await setupClient.connectProvider(
        provider.name.trim(),
        provider.endpoint.trim(),
        apiKey,
      );

      setProfileRevision(result.revision);
      setConnectResult({
        providerKey: result.provider_key,
        models: result.models,
      });

      // Default: the first model is both selected and the default, so a first
      // run that only presses through still reaches a runnable configuration.
      const first = result.models[0];
      if (first) {
        setSelectedModels(new Set([first.key]));
        setDefaultModel(first.key);
      }

      setStep("models");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Failed to connect provider.");
    } finally {
      if (apiKeyInput.current) apiKeyInput.current.value = "";
      setLoading(false);
    }
  }, [controlUrl, provider]);

  const handleRefreshModels = useCallback(async () => {
    if (!controlUrl || !connectResult) return;
    setLoading(true);
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
      setError(cause instanceof Error ? cause.message : "Failed to refresh provider models.");
    } finally {
      setLoading(false);
    }
  }, [controlUrl, connectResult, profileRevision, selectedModels, defaultModel]);

  const handleSaveModels = useCallback(async () => {
    if (!controlUrl || !connectResult) return;
    if (selectedModels.size === 0) {
      setError("Select at least one model.");
      return;
    }
    if (!defaultModel || !selectedModels.has(defaultModel)) {
      setError("Choose a default model.");
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const setupClient = createSetupClient(controlUrl, fetch);
      const modelsToSave = Array.from(selectedModels).map((key) => {
        const model = connectResult.models.find((m) => m.key === key);
        if (!model) throw new Error("Refresh the provider model catalog before selecting.");
        return { key: model.key, id: model.id };
      });
      const result = await setupClient.saveModels(
        connectResult.providerKey,
        modelsToSave,
        defaultModel,
        profileRevision,
      );

      if (result.runnable_choices.length === 0) {
        setError("No runnable models after save. Check provider configuration.");
        return;
      }

      setProfileRevision(result.revision);
      setSelectedModels(new Set(result.selected_models));
      setDefaultModel(result.default_model);

      // Browse home directory for project selection
      try {
        const browse = await setupClient.browseDirectory();
        setBrowseResult(browse);
      } catch {
        // Browsing may fail, proceed anyway
      }

      setStep("projects");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Failed to save models.");
    } finally {
      setLoading(false);
    }
  }, [controlUrl, connectResult, selectedModels, defaultModel, profileRevision]);

  const handleBrowse = useCallback(async (path: string) => {
    if (!controlUrl) return;
    setLoading(true);
    try {
      const setupClient = createSetupClient(controlUrl, fetch);
      const result = await setupClient.browseDirectory(path);
      setBrowseResult(result);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Failed to browse directory.");
    } finally {
      setLoading(false);
    }
  }, [controlUrl]);

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
      setError("Select at least one folder.");
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const setupClient = createSetupClient(controlUrl, fetch);
      const imported: Array<{ id: string; name: string; root: string }> = [];

      for (const folder of selectedFolders) {
        const commandId = `cmd-setup-project-${Date.now().toString(36)}-${imported.length}`;
        const result = await setupClient.initProject(commandId, folder);
        imported.push({
          id: result.project_id,
          name: result.name,
          root: result.root,
        });
      }

      setImportedProjects(imported);

      // Set first project as active
      if (imported.length > 0) {
        setActiveProject(imported[0].id);
      }

      // Complete onboarding and enter workspace
      await completeOnboarding();
      const projectParam = imported.length > 0 ? `&project=${encodeURIComponent(imported[0].id)}` : "";
      router.push(`/?scenario=local-ready${projectParam}`);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Failed to import projects.");
    } finally {
      setLoading(false);
    }
  }, [controlUrl, selectedFolders, completeOnboarding, router, setActiveProject]);

  return (
    <div className="flex min-h-dvh justify-center bg-background px-4 py-8 text-foreground sm:py-12">
      <div className="flex w-full max-w-2xl flex-col">
        <header>
          <div className="flex items-center gap-2">
            <span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-[10px] font-semibold tracking-wider text-muted-foreground uppercase">
              OCG setup
            </span>
          </div>
          <h1 className="mt-3 text-[18px] font-semibold tracking-tight">{STEP_LABELS[step]}</h1>

          {/* Step indicator */}
          <ol className="mt-4 grid grid-cols-3 gap-1" aria-label="Setup steps">
            {STEPS.map((s, i) => {
              const isCurrent = s === step;
              const isDone = i < currentIndex;
              return (
                <li key={s} className="min-w-0">
                  <div
                    className={cn(
                      "flex w-full items-center gap-1.5 rounded-md border px-2 py-1.5",
                      isCurrent ? "border-foreground/40 bg-muted" : "border-border",
                      !isCurrent && !isDone && "opacity-60",
                    )}
                  >
                    <span
                      className={cn(
                        "flex size-4 shrink-0 items-center justify-center rounded-full border text-[9px]",
                        isDone ? "border-emerald-600 bg-emerald-600 text-white" : "border-border text-muted-foreground",
                      )}
                      aria-hidden="true"
                    >
                      {isDone ? <Check className="size-2.5" /> : i + 1}
                    </span>
                    <span className="truncate text-[10px] text-muted-foreground">{STEP_LABELS[s]}</span>
                  </div>
                </li>
              );
            })}
          </ol>
        </header>

        <div className="mt-5 flex flex-col gap-3">
          {error && (
            <div role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/5 p-3">
              <AlertCircle className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden="true" />
              <p className="text-[12px] text-destructive">{error}</p>
            </div>
          )}

          <section className="rounded-lg border border-border bg-muted/10 p-4">
            {step === "provider" && (
              <ConnectProviderPanel
                provider={provider}
                onChange={setProvider}
                loading={loading}
                apiKeyInput={apiKeyInput}
              />
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
            {step === "projects" && (
              <AddProjectsPanel
                browseResult={browseResult}
                selectedFolders={selectedFolders}
                manualPath={manualPath}
                importedProjects={importedProjects}
                onBrowse={handleBrowse}
                onSelectFolder={handleSelectFolder}
                onManualPathChange={setManualPath}
                onAddManualPath={handleAddManualPath}
                loading={loading}
              />
            )}
          </section>
        </div>

        <footer className="mt-5 flex items-center justify-between gap-2">
          <Button
            variant="ghost"
            size="sm"
            disabled={currentIndex === 0 || loading}
            onClick={() => {
              setError(null);
              setStep(STEPS[currentIndex - 1]);
            }}
          >
            <ArrowLeft className="size-3.5" aria-hidden="true" />
            Back
          </Button>

          <span className="text-[11px] text-muted-foreground">
            Step {currentIndex + 1} of {STEPS.length}
          </span>

          {step === "provider" && (
            <Button size="sm" onClick={() => void handleConnectProvider()} disabled={loading}>
              {loading ? <Loader2 className="size-3.5 animate-spin" /> : null}
              Connect & Discover Models
              <ArrowRight className="size-3.5" aria-hidden="true" />
            </Button>
          )}
          {step === "models" && (
            <Button size="sm" onClick={() => void handleSaveModels()} disabled={loading}>
              {loading ? <Loader2 className="size-3.5 animate-spin" /> : null}
              Save & Continue
              <ArrowRight className="size-3.5" aria-hidden="true" />
            </Button>
          )}
          {step === "projects" && (
            <Button size="sm" onClick={() => void handleImportProjects()} disabled={loading || selectedFolders.length === 0}>
              {loading ? <Loader2 className="size-3.5 animate-spin" /> : null}
              Import & Start
              <ArrowRight className="size-3.5" aria-hidden="true" />
            </Button>
          )}
        </footer>
      </div>
    </div>
  );
}

// -- Step panels -------------------------------------------------------------

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
  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-1.5">
        <label htmlFor="setup-provider-name" className="text-[12px] font-medium">
          Provider Name
        </label>
        <input
          id="setup-provider-name"
          type="text"
          className="rounded-md border border-border bg-background px-3 py-2 text-[13px]"
          placeholder="Provider"
          value={provider.name}
          onChange={(event) => onChange({ ...provider, name: event.target.value })}
          disabled={loading}
        />
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor="setup-provider-endpoint" className="text-[12px] font-medium">
          Provider Endpoint
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
          API Key
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
  if (models.length === 0) {
    return <p className="text-[12px] text-muted-foreground">No models discovered from this provider.</p>;
  }

  return (
    <div className="flex flex-col gap-3">
      <p className="text-[12px] text-muted-foreground">
        Select the models you want to use. The default model is used for new chats.
      </p>
      <Button size="sm" variant="outline" onClick={onRefresh} disabled={loading}>Refresh models</Button>
      <ul className="divide-y divide-border rounded-md border border-border">
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
                aria-label={`Select ${model.label}`}
              />
              <div className="min-w-0 flex-1">
                <p className="truncate text-[12px] font-medium">{model.label}</p>
              </div>
              {selected && (
                <Button
                  size="xs"
                  variant={isDefault ? "default" : "outline"}
                  disabled={loading}
                  onClick={() => onSetDefault(model.key)}
                >
                  {isDefault ? "Default" : "Set default"}
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
  importedProjects,
  onBrowse,
  onSelectFolder,
  onManualPathChange,
  onAddManualPath,
  loading,
}: {
  browseResult: SetupBrowseResponse | null;
  selectedFolders: string[];
  manualPath: string;
  importedProjects: Array<{ id: string; name: string; root: string }>;
  onBrowse: (path: string) => void;
  onSelectFolder: (path: string) => void;
  onManualPathChange: (path: string) => void;
  onAddManualPath: () => void;
  loading: boolean;
}) {
  return (
    <div className="flex flex-col gap-4">
      <p className="text-[12px] text-muted-foreground">
        Select local folders to import as Projects. Each folder becomes an OCG Project.
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
          Add
        </Button>
      </div>

      {/* Selected folders */}
      {selectedFolders.length > 0 && (
        <div className="flex flex-col gap-1">
          <p className="text-[11px] font-medium text-muted-foreground">Selected ({selectedFolders.length})</p>
          <ul className="divide-y divide-border rounded-md border border-border">
            {selectedFolders.map((folder) => (
              <li key={folder} className="flex items-center gap-2 px-3 py-2">
                <FolderOpen className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span className="min-w-0 flex-1 truncate text-[12px]">{folder}</span>
                <Button
                  size="xs"
                  variant="ghost"
                  onClick={() => onSelectFolder(folder)}
                  disabled={loading}
                >
                  Remove
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
            <p className="text-[11px] font-medium text-muted-foreground">Browse</p>
            {browseResult.parent && (
              <Button
                size="xs"
                variant="ghost"
                onClick={() => browseResult.parent && onBrowse(browseResult.parent)}
                disabled={loading}
              >
                <ArrowLeft className="size-3" /> Up
              </Button>
            )}
          </div>
          <p className="truncate text-[11px] text-muted-foreground">{browseResult.current}</p>
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
                    {isSelected ? <Check className="size-3" /> : "Select"}
                  </Button>
                  <button
                    type="button"
                    className="shrink-0 text-muted-foreground hover:text-foreground"
                    onClick={() => onBrowse(entry.path)}
                    disabled={loading}
                    aria-label={`Open ${entry.name}`}
                  >
                    <ChevronRight className="size-3.5" />
                  </button>
                </li>
              );
            })}
            {browseResult.entries.length === 0 && (
              <li className="px-3 py-2 text-[11px] text-muted-foreground">
                No subdirectories found.
              </li>
            )}
          </ul>
        </div>
      )}

      {/* Imported projects feedback */}
      {importedProjects.length > 0 && (
        <div className="rounded-md border border-emerald-500/40 bg-emerald-500/5 p-3">
          <p className="text-[12px] font-medium text-emerald-700 dark:text-emerald-400">
            {importedProjects.length} project{importedProjects.length > 1 ? "s" : ""} imported
          </p>
        </div>
      )}
    </div>
  );
}
