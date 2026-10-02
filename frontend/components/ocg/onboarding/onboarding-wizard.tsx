"use client";

import { ArrowLeft, ArrowRight, Check, TriangleAlert } from "lucide-react";
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
import { ProfilePanel } from "../profile/profile-panel";
import { useOcgControlUrl } from "../profile/control-url";

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
    if (!gate.canAdvance) return;
    if (!next) {
      void completeOnboarding();
      router.push(bootstrap.access.remote ? "/?scenario=remote-authenticated-ready" : "/?scenario=local-ready");
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
          {controlUrl && <ProfilePanel />}
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
            disabled={!gate.canAdvance}
            title={!gate.canAdvance ? `Blocked: ${gate.blockers.join(", ")}` : undefined}
          >
            {next ? "Next" : "Enter workspace"}
            <ArrowRight className="size-3.5" aria-hidden="true" />
          </Button>
        </footer>

        {!gate.canAdvance && (
          <p className="mt-2 text-right text-[11px] text-muted-foreground">
            {gate.blockers.join(" · ")}
          </p>
        )}
      </div>
    </div>
  );
}
