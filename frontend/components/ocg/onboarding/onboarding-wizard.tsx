"use client";

import { ArrowLeft, ArrowRight, Check, TriangleAlert } from "lucide-react";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ONBOARDING_STAGES, type BootstrapFailure } from "../bootstrap/types";
import {
  canResumeOnboarding,
  normalizeOnboardingMode,
  selectActiveOnboardingStage,
  selectNextStage,
  selectOnboardingProgress,
  selectPreviousStage,
  selectStageGate,
} from "../bootstrap/selectors";
import {
  BOOTSTRAP_MODE_DESCRIPTION,
  BOOTSTRAP_MODE_LABEL,
  ONBOARDING_STAGE_LABEL,
  ONBOARDING_STAGE_SUMMARY,
} from "../bootstrap/presentation";
import { StagePanel } from "./stage-panels";
import { createProfileClient } from "../profile/profile-client";
import {
  createHttpCanonicalControlClient,
  isCanonicalRejection,
} from "../runtime/canonical-client";
import { useOcgControlUrl } from "../profile/control-url";
import { useProject } from "../project/project-context";
import { ProjectSwitcher } from "../project/project-switcher";
import type { Profile } from "../contracts";

function FailureBanner({
  failure,
  onAction,
}: {
  failure: BootstrapFailure;
  onAction: (failure: BootstrapFailure) => void;
}) {
  return (
    <div role="alert" className="rounded-md border border-amber-500/40 bg-amber-500/5 p-3">
      <div className="flex items-start gap-2">
        <TriangleAlert className="mt-0.5 size-4 shrink-0 text-amber-600 dark:text-amber-400" aria-hidden="true" />
        <div className="min-w-0 flex-1">
          <p className="text-[12px] font-medium">{failure.summary}</p>
          <p className="mt-0.5 text-[11px] text-muted-foreground">
            Setup is paused until this is resolved.
          </p>
        </div>
        <Button
          size="xs"
          variant="outline"
          onClick={() => onAction(failure)}
          disabled={!failure.retryable && failure.action === "continue"}
        >
          {failure.actionLabel}
        </Button>
      </div>
      {failure.detail && (
        <details className="mt-2 text-[11px] text-muted-foreground">
          <summary className="cursor-pointer select-none">Advanced details</summary>
          <p className="mt-1 break-words">{failure.detail}</p>
        </details>
      )}
    </div>
  );
}

function StageStepper({ current, completed, onSelect }: { current: number; completed: string[]; onSelect: (stage: (typeof ONBOARDING_STAGES)[number]) => void }) {
  return (
    <ol className="mt-4 hidden grid-cols-7 gap-1 sm:grid" aria-label="Setup stages">
      {ONBOARDING_STAGES.map((stage, index) => {
        const isCurrent = index + 1 === current;
        const isDone = completed.includes(stage);
        return (
          <li key={stage} className="min-w-0">
            <button
              type="button"
              className={cn(
                "flex w-full items-center gap-1.5 rounded-md border px-2 py-1.5 text-left",
                isCurrent ? "border-foreground/40 bg-muted" : "border-border",
                !isCurrent && !isDone && "cursor-not-allowed opacity-60",
              )}
              aria-current={isCurrent ? "step" : undefined}
              aria-label={`${index + 1}. ${ONBOARDING_STAGE_LABEL[stage]}${isDone ? " (completed)" : isCurrent ? " (current)" : " (locked)"}`}
              disabled={!isCurrent && !isDone}
              onClick={() => onSelect(stage)}
            >
              <span
                className={cn(
                  "flex size-4 shrink-0 items-center justify-center rounded-full border text-[9px]",
                  isDone ? "border-emerald-600 bg-emerald-600 text-white" : "border-border text-muted-foreground",
                )}
                aria-hidden="true"
              >
                {isDone ? <Check className="size-2.5" /> : index + 1}
              </span>
              <span className="truncate text-[10px] text-muted-foreground">{ONBOARDING_STAGE_LABEL[stage]}</span>
            </button>
          </li>
        );
      })}
    </ol>
  );
}

/** The user's explicit defaultModel, only when the backend confirms it executable. */
function executableDefaultModel(
  profile: Profile | null,
  runnable: readonly string[],
): { provider: string; model: string } | null {
  const key = profile?.defaultModel ?? null;
  if (!key || !runnable.includes(key)) return null;
  const record = profile?.models[key];
  const provider = record?.provider;
  if (!record || !provider) return null;
  return { provider, model: key };
}

/** A persisted Project defaults value that names a backend-executable pair. */
function isExecutablePair(
  profile: Profile | null,
  runnable: readonly string[],
  defaults: unknown,
): boolean {
  if (typeof defaults !== "object" || defaults === null) return false;
  const { provider, model } = defaults as Record<string, unknown>;
  if (typeof provider !== "string" || !provider || typeof model !== "string" || !model) {
    return false;
  }
  if (!runnable.includes(model)) return false;
  const record = profile?.models[model];
  return !!record && record.provider === provider;
}

export function OnboardingWizard() {
  const router = useRouter();
  const controlUrl = useOcgControlUrl();
  const {
    snapshot,
    setOnboardingStage,
    completeOnboarding,
    requestAccessHandoff,
    retryBootstrap,
  } = useOcgRuntime();
  const bootstrap = snapshot.bootstrap;
  const onboarding = bootstrap.onboarding;
  // Backend readiness for the final Enter: the workspace opens only when the
  // backend reports a real executable choice. Kept beside the hooks so the
  // early return below never reorders them.
  const [enterError, setEnterError] = useState<string | null>(null);
  const [entering, setEntering] = useState(false);
  const { activeProjectId, projects, setActiveProject } = useProject();
  const projectResolved = projects.some((project) => project.id === activeProjectId);

  if (!onboarding) return null;

  const stage = selectActiveOnboardingStage(bootstrap);
  const gate = selectStageGate(bootstrap, stage);
  const next = selectNextStage(stage);
  const previous = selectPreviousStage(stage);
  const progress = selectOnboardingProgress(bootstrap);
  const mode = normalizeOnboardingMode(onboarding.mode);
  const resumable = canResumeOnboarding(bootstrap);

  function handleFailureAction(failure: BootstrapFailure) {
    if (failure.action === "handoff") {
      void requestAccessHandoff();
      return;
    }
    void retryBootstrap();
  }

  function handleNext() {
    if (!gate.canAdvance || entering) return;
    if (!next) {
      // The final Enter is backend-gated in two steps: a backend-confirmed
      // executable provider/model, then an explicit Project defaults
      // selection persisted for the current Project. Nothing is guessed.
      if (!controlUrl) {
        setEnterError("No loopback control endpoint is available; cannot confirm an executable provider.");
        return;
      }
      setEntering(true);
      setEnterError(null);
      void (async () => {
        const profileClient = createProfileClient(controlUrl, fetch);
        const control = createHttpCanonicalControlClient({ baseUrl: controlUrl, fetch });
        const view = await profileClient.read();
        if (view.runnable_choices.length === 0) {
          setEnterError("No executable provider/model yet: set an HTTPS endpoint, a credential reference, and save the matching Vault credential first.");
          return;
        }
        // The model is the user's explicit defaultModel, and only when the
        // backend confirms it executable. The provider always comes from
        // that model's own provider identity, never a label.
        const choice = executableDefaultModel(view.profile, view.runnable_choices);
        if (!choice) {
          setEnterError("Choose an executable default model in the Profile panel before entering the workspace.");
          return;
        }
        const listed = await control.listProjects();
        const projectId = listed.some((project) => project.project_id === activeProjectId)
          ? activeProjectId
          : null;
        if (!projectId) {
          setEnterError("Select a project to continue setup.");
          return;
        }
        const stored = await control.readConfiguration(projectId);
        if (isCanonicalRejection(stored)) {
          setEnterError(`Could not read the current Project configuration: ${stored.message}`);
          return;
        }
        if (!isExecutablePair(view.profile, view.runnable_choices, stored.project_defaults.defaults)) {
          const commandId = `cmd-onboard-defaults-${Date.now().toString(36)}`;
          const written = await control.writeProjectDefaults(commandId, projectId, {
            provider: choice.provider,
            model: choice.model,
          });
          if (isCanonicalRejection(written)) {
            setEnterError(`Could not persist the Project provider/model selection: ${written.message}`);
            return;
          }
          if (!isExecutablePair(view.profile, view.runnable_choices, written.configuration.project_defaults.defaults)) {
            setEnterError("The Project provider/model selection did not persist; refresh and try again.");
            return;
          }
        }
        void completeOnboarding();
        router.push(bootstrap.access.remote ? "/?scenario=remote-authenticated-ready" : "/?scenario=local-ready");
      })()
        .catch((cause: unknown) => {
          setEnterError(cause instanceof Error ? cause.message : "Could not confirm an executable provider.");
        })
        .finally(() => {
          setEntering(false);
        });
      return;
    }
    void setOnboardingStage(next);
  }

  return (
    <div className="flex min-h-dvh justify-center bg-background px-4 py-8 text-foreground sm:py-12">
      <div className="flex w-full max-w-3xl flex-col">
        <header>
          <div className="flex items-center gap-2">
            <span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-[10px] font-semibold tracking-wider text-muted-foreground uppercase">
              OCG setup
            </span>
            <span className="text-[11px] text-muted-foreground">{BOOTSTRAP_MODE_LABEL[mode]}</span>
          </div>
          <h1 className="mt-3 text-[18px] font-semibold tracking-tight">{ONBOARDING_STAGE_LABEL[stage]}</h1>
          <p className="mt-1 text-[12px] leading-5 text-muted-foreground">{ONBOARDING_STAGE_SUMMARY[stage]}</p>
          <p className="mt-1 text-[11px] leading-4 text-muted-foreground">{BOOTSTRAP_MODE_DESCRIPTION[mode]}</p>
          {resumable && (
            <p role="status" className="mt-2 text-[11px] text-muted-foreground">
              Saved progress found. Setup resumes where it stopped.
            </p>
          )}
           <StageStepper
             current={progress.current}
             completed={onboarding.completedStages}
             onSelect={(selectedStage) => {
               if (selectedStage !== stage && onboarding.completedStages.includes(selectedStage)) {
                 void setOnboardingStage(selectedStage);
               }
             }}
           />
          <p className="mt-3 text-[11px] text-muted-foreground sm:hidden">
            Step {progress.current} of {progress.total} · {ONBOARDING_STAGE_LABEL[stage]}
          </p>
        </header>

        <div className="mt-5 flex flex-col gap-3">
          <Button size="sm" onClick={() => router.push("/onboarding?scenario=local-first-run")}>Connect Provider & Choose Models</Button>
          {onboarding.failure && (
            <FailureBanner failure={onboarding.failure} onAction={handleFailureAction} />
          )}
          <section
            aria-label={ONBOARDING_STAGE_LABEL[stage]}
            className="rounded-lg border border-border bg-muted/10 p-4"
          >
            <StagePanel
              bootstrap={bootstrap}
              onRequestHandoff={() => {
                void requestAccessHandoff();
              }}
            />
          </section>
          {!projectResolved && (
            <section aria-label="Select project" className="rounded-lg border border-border bg-muted/10 p-4">
              <p className="mb-2 text-[12px] text-muted-foreground">
                Entering the workspace also needs an explicit project. Nothing is selected automatically.
              </p>
              <ProjectSwitcher
                projects={projects}
                activeProjectId={activeProjectId}
                onChange={setActiveProject}
              />
            </section>
          )}
        </div>

        <footer className="mt-5 flex items-center justify-between gap-2">
          <Button
            variant="ghost"
            size="sm"
            disabled={!previous}
            onClick={() => {
              if (previous) void setOnboardingStage(previous);
            }}
          >
            <ArrowLeft className="size-3.5" aria-hidden="true" />
            Back
          </Button>
          <span className="text-[11px] text-muted-foreground">
            {progress.completed} of {progress.total} stages complete
          </span>
          <Button
            size="sm"
            onClick={handleNext}
            disabled={!gate.canAdvance || entering}
            title={!gate.canAdvance ? `Blocked: ${gate.blockers.join(", ")}` : undefined}
          >
            {next ? "Next" : "Enter workspace"}
            <ArrowRight className="size-3.5" aria-hidden="true" />
          </Button>
        </footer>

        {enterError && (
          <p role="alert" className="mt-2 text-right text-[11px] text-destructive">
            {enterError}
          </p>
        )}

        {!gate.canAdvance && (
          <p className="mt-2 text-right text-[11px] text-muted-foreground">
            {gate.blockers.join(" · ")}
          </p>
        )}
      </div>
    </div>
  );
}
