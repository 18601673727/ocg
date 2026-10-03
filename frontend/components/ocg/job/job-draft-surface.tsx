"use client";

/**
 * Compact inline Job draft surface.
 *
 * Rendered inside the Chat column (not a new page and not a modal). It uses the
 * existing OCG primitives and visual language, keeps a single scroll owner, and
 * never selects a Project itself — the shell owns the active Project.
 */

import { useRef, useState } from "react";
import { AlertTriangle, Check, Rocket, Target, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import type { ProjectSummary } from "../project/domain";
import type { JobLaunchResult } from "../runtime/runtime-types";
import {
  FIXTURE_RECOMMENDED_BUDGET_MICROS,
  HARD_BUDGET_MIN_MICROS,
  microsToUsd,
  jobDraftHasErrors,
  usdToMicros,
  type HardBudgetSource,
  type JobDraft,
  type JobDraftTextField,
} from "./draft-domain";
import { useI18n, type TranslateFn } from "../i18n";

type JobDraftSurfaceProps = {
  project: ProjectSummary;
  draft: JobDraft;
  launchResult?: JobLaunchResult | null;
  onFieldChange: (field: JobDraftTextField, value: string) => void;
  onBudgetChange: (micros: number | null, source: HardBudgetSource) => void;
  onCommitmentChange: (value: number) => void;
  onValidate: () => void;
  onLaunch: () => void;
  onClose: () => void;
};

const OUTCOME_TONE: Record<JobLaunchResult["outcome"], string> = {
  accepted: "border-emerald-500/40 bg-emerald-500/5 text-emerald-800 dark:text-emerald-200",
  rejected: "border-amber-500/40 bg-amber-500/5 text-amber-800 dark:text-amber-200",
  "requires-attention": "border-violet-500/40 bg-violet-500/5 text-violet-800 dark:text-violet-200",
  failed: "border-red-500/40 bg-red-500/5 text-red-800 dark:text-red-200",
};

const KNOWN_VALIDATION_FAILED = "Draft validation failed before launch.";
const KNOWN_LAUNCH_FAILED = "The runtime adapter failed while launching this Job.";

/**
 * Reducer and adapter messages stay English at the domain boundary so the
 * runtime logic never depends on locale. Localize the two known messages at
 * render time; unknown backend messages stay raw so nothing is hidden.
 */
function localizeKnownFailureMessage(message: string, t: TranslateFn): string {
  if (message === KNOWN_VALIDATION_FAILED) return t("job.validationFailed");
  if (message === KNOWN_LAUNCH_FAILED) return t("job.launchFailed");
  return message;
}

export function JobDraftSurface({
  project,
  draft,
  launchResult,
  onFieldChange,
  onBudgetChange,
  onCommitmentChange,
  onValidate,
  onLaunch,
  onClose,
}: JobDraftSurfaceProps) {
  const { t } = useI18n();
  const summaryRef = useRef<HTMLDivElement>(null);
  const [budgetText, setBudgetText] = useState(() =>
    draft.hardBudgetMicros === null ? "" : String(microsToUsd(draft.hardBudgetMicros)),
  );

  const isLaunching = draft.lifecycle === "launching";
  const hasErrors = jobDraftHasErrors(draft.issues);
  const commitmentPercent = Math.round(draft.resourceCommitment * 100);
  const errorCount = draft.issues.filter((issue) => issue.severity === "error").length;

  const setFixtureBudget = () => {
    setBudgetText(String(microsToUsd(FIXTURE_RECOMMENDED_BUDGET_MICROS)));
    onBudgetChange(FIXTURE_RECOMMENDED_BUDGET_MICROS, "fixture-recommended");
  };

  const handleBudgetChange = (value: string) => {
    setBudgetText(value);
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      onBudgetChange(null, "user");
      return;
    }
    if (!/^\d+(\.\d{0,6})?$/.test(trimmed)) {
      // Do not leave the previous valid amount in the domain while the field
      // visibly contains an invalid amount. That would launch with a value
      // different from what the operator reviewed.
      onBudgetChange(null, "user");
      return;
    }
    onBudgetChange(usdToMicros(Number(trimmed)), "user");
  };

  const attemptLaunch = () => {
    if (isLaunching) return;
    onValidate();
    if (hasErrors) {
      summaryRef.current?.focus();
      return;
    }
    onLaunch();
  };

  return (
    <section
      aria-label={t("job.draft")}
      className="flex flex-col gap-3 rounded-lg border border-border bg-background px-3 py-3 shadow-[0_1px_2px_rgba(0,0,0,0.04)] sm:px-4"
    >
      <header className="flex items-start gap-2">
        <span className="mt-0.5 flex size-6 shrink-0 items-center justify-center rounded border border-border bg-muted" aria-hidden="true">
          <Target className="size-3.5 text-muted-foreground" />
        </span>
        <div className="min-w-0 flex-1">
          <h2 className="text-[13px] font-semibold tracking-tight">{t("job.draft")}</h2>
          <p className="mt-0.5 text-[11px] text-muted-foreground">
            {t("job.activeProject")} <span className="font-medium text-foreground">{project.name}</span> · {t("job.activeProjectNote")}
          </p>
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          onClick={onClose}
          aria-label={t("job.closeDraft")}
          title={t("job.closeDraft")}
        >
          <X className="size-4" />
        </Button>
      </header>

      <div
        ref={summaryRef}
        tabIndex={-1}
        role={hasErrors ? "alert" : "status"}
        aria-live="polite"
        className={cn(
          "rounded-md border px-2.5 py-1.5 text-[11px] outline-none focus-visible:ring-2 focus-visible:ring-ring/40",
          hasErrors
            ? "border-red-500/40 bg-red-500/5 text-red-800 dark:text-red-200"
            : "border-border bg-muted/20 text-muted-foreground",
        )}
      >
        {hasErrors
          ? t("job.draftAttention", { count: errorCount })
          : draft.lifecycle === "ready"
            ? t("job.draftReady")
            : t("job.fillHint")}
      </div>

      <div className="flex flex-col gap-1">
        <label htmlFor="job-draft-objective" className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
          {t("job.objective")}
        </label>
        <Textarea
          id="job-draft-objective"
          value={draft.objective}
          onChange={(event) => onFieldChange("objective", event.target.value)}
          onBlur={onValidate}
          placeholder={t("job.objectivePlaceholder")}
          aria-invalid={draft.issues.some((issue) => issue.field === "objective" && issue.severity === "error")}
          disabled={isLaunching}
          className="min-h-11 text-[13px]"
        />
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <div className="flex flex-col gap-1">
          <label htmlFor="job-draft-criteria" className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            {t("job.successCriteria")}
          </label>
          <Textarea
            id="job-draft-criteria"
            value={draft.successCriteria}
            onChange={(event) => onFieldChange("successCriteria", event.target.value)}
            onBlur={onValidate}
            placeholder={t("job.successCriteriaPlaceholder")}
            aria-invalid={draft.issues.some((issue) => issue.field === "successCriteria" && issue.severity === "error")}
            disabled={isLaunching}
            className="min-h-16 text-[13px]"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="job-draft-constraints" className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            {t("job.constraints")}
          </label>
          <Textarea
            id="job-draft-constraints"
            value={draft.constraints}
            onChange={(event) => onFieldChange("constraints", event.target.value)}
            onBlur={onValidate}
            placeholder={t("job.constraintsPlaceholder")}
            disabled={isLaunching}
            className="min-h-16 text-[13px]"
          />
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <div className="flex flex-col gap-1">
          <label htmlFor="job-draft-budget" className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            {t("job.budget")}
          </label>
          <div className="flex items-center gap-2">
            <span aria-hidden="true" className="text-[13px] text-muted-foreground">$</span>
            <Input
              id="job-draft-budget"
              type="text"
              inputMode="decimal"
              value={budgetText}
              onChange={(event) => handleBudgetChange(event.target.value)}
              onBlur={onValidate}
              aria-invalid={draft.issues.some((issue) => issue.field === "hardBudgetMicros" && issue.severity === "error")}
              disabled={isLaunching}
              className="h-8 text-[13px]"
            />
            <Button type="button" variant="outline" size="xs" onClick={setFixtureBudget} disabled={isLaunching}>
              {t("job.useFixture")}
            </Button>
          </div>
          <p className="text-[10px] leading-4 text-muted-foreground">
            {t("job.budgetHelp", { value: draft.hardBudgetMicros === null ? t("job.budgetNotSet") : draft.hardBudgetMicros.toLocaleString() })}
          </p>
          <p
            className={cn(
              "text-[10px] leading-4",
              draft.hardBudgetSource === "fixture-recommended" ? "text-muted-foreground" : "text-foreground/70",
            )}
          >
            {draft.hardBudgetSource === "fixture-recommended"
              ? t("job.fixtureNote")
              : t("job.budgetCustomNote")}
          </p>
        </div>

        <div className="flex flex-col gap-1">
          <label htmlFor="job-draft-commitment" className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            {t("job.resourceCommitment")}
          </label>
          <input
            id="job-draft-commitment"
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={draft.resourceCommitment}
            onChange={(event) => onCommitmentChange(Number(event.target.value))}
            onBlur={onValidate}
            disabled={isLaunching}
            aria-valuetext={t("job.commitmentValue", { percent: commitmentPercent })}
            className="h-8 w-full accent-foreground"
          />
          <p className="text-[11px] tabular-nums text-foreground/80">{t("job.commitmentValue", { percent: commitmentPercent })}</p>
          <p className="text-[10px] leading-4 text-muted-foreground">
            {t("job.commitmentHelp")}
          </p>
        </div>
      </div>

      {hasErrors && (
        <ul className="flex flex-col gap-1" aria-label={t("job.validationIssues")}>
          {draft.issues.filter((issue) => issue.severity === "error").map((issue) => (
            <li key={`${issue.code}-${issue.field}`} className="flex items-start gap-1.5 text-[11px] leading-4 text-red-700 dark:text-red-300">
              <AlertTriangle className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
              <span>{localizeKnownFailureMessage(issue.message, t)}</span>
            </li>
          ))}
        </ul>
      )}

      {draft.lifecycle === "launch-failed" && draft.launchMessage && (!launchResult || launchResult.message !== draft.launchMessage) && (
        <p role="alert" className="rounded-md border border-red-500/40 bg-red-500/5 px-2.5 py-1.5 text-[11px] leading-4 text-red-800 dark:text-red-200">
          {localizeKnownFailureMessage(draft.launchMessage, t)}
        </p>
      )}

      {launchResult && (
        <p className={cn("rounded-md border px-2.5 py-1.5 text-[11px] leading-4", OUTCOME_TONE[launchResult.outcome])}>
          <span className="font-semibold capitalize">{launchResult.outcome}</span>
          {launchResult.duplicate ? ` ${t("job.recordedReused")}` : ""} · {localizeKnownFailureMessage(launchResult.message, t)}
        </p>
      )}

      <footer className="flex flex-wrap items-center gap-2 border-t border-border pt-2.5">
        <p className="min-w-0 flex-1 text-[10px] text-muted-foreground">
          {draft.hardBudgetMicros !== null && Number.isSafeInteger(draft.hardBudgetMicros) && draft.hardBudgetMicros >= HARD_BUDGET_MIN_MICROS
            ? t("job.budgetValid")
            : t("job.budgetInvalid")}
        </p>
        <Button type="button" variant="ghost" size="sm" onClick={onClose} disabled={isLaunching}>
          {t("job.close")}
        </Button>
        <Button type="button" size="sm" onClick={attemptLaunch} disabled={isLaunching} className="gap-1.5">
          {isLaunching ? (
            <>
              <Check className="size-3.5 animate-pulse" aria-hidden="true" />
              {t("job.launching")}
            </>
          ) : (
            <>
              <Rocket className="size-3.5" aria-hidden="true" />
              {t("job.launch")}
            </>
          )}
        </Button>
      </footer>
    </section>
  );
}
