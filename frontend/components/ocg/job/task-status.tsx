"use client";

import { Check, CircleDashed, CircleHelp, CircleMinus, CircleX, LoaderCircle } from "lucide-react";
import Link from "next/link";
import type { ControlTaskPlan } from "../contracts";
import { useI18n } from "../i18n";
import { useOpenJobDetails } from "./job-details-context";

type StepState = ControlTaskPlan["steps"][number]["state"];

function StepIcon({ state }: { state: StepState }) {
  const iconClass = "mt-0.5 size-4 shrink-0";
  switch (state) {
    case "completed":
      return (
        <span
          className={`${iconClass} flex items-center justify-center rounded-full bg-muted-foreground text-background`}
        >
          <Check aria-hidden="true" className="size-3" strokeWidth={3} />
        </span>
      );
    case "queued":
      return <CircleDashed aria-hidden="true" className={`${iconClass} text-muted-foreground`} />;
    case "failed":
      return <CircleX aria-hidden="true" className={`${iconClass} text-destructive`} />;
    case "cancelled":
      return <CircleMinus aria-hidden="true" className={`${iconClass} text-muted-foreground`} />;
    case "unknown":
      return <CircleHelp aria-hidden="true" className={`${iconClass} text-muted-foreground`} />;
    case "starting":
    case "waiting_for_provider":
    case "running_tools":
    case "waiting_for_user":
    case "rate_limited":
    case "running_tasks":
    case "synthesizing":
      return (
        <LoaderCircle
          aria-hidden="true"
          className={`${iconClass} animate-spin text-muted-foreground motion-reduce:animate-none`}
        />
      );
  }
}

export function TaskStatus({ plan }: { plan?: ControlTaskPlan | null }) {
  const { t } = useI18n();
  const openJobDetails = useOpenJobDetails();
  if (!plan) return null;
  const phase = plan.state;
  return (
    <div className="my-3 rounded-xl border border-border px-4 py-3 text-sm">
      <p
        role="status"
        className={
          phase === "failed"
            ? "mb-3 text-destructive"
            : phase === "cancelled" || phase === "unknown"
              ? "mb-3 text-muted-foreground"
              : "sr-only"
        }
      >
        {t("job.activity.plan")} · {t(`job.activity.${phase}`)}
      </p>
      <ol aria-label={t("job.activity.plan")} className="space-y-3">
        {plan.steps.map((step) => (
          <li key={step.conversation} className="flex items-start gap-3">
            <StepIcon state={step.state} />
            <div className="min-w-0">
              <Link
                className={`${step.state === "completed" ? "text-muted-foreground" : "text-foreground"} rounded-sm hover:underline focus-visible:outline-2 focus-visible:outline-offset-2`}
                href={`/jobs?job=${step.turn}`}
                onClick={(event) => {
                  if (
                    !openJobDetails ||
                    event.defaultPrevented ||
                    event.button !== 0 ||
                    event.metaKey ||
                    event.ctrlKey ||
                    event.shiftKey ||
                    event.altKey
                  )
                    return;
                  event.preventDefault();
                  openJobDetails(step.turn);
                }}
              >
                {step.title}
              </Link>
              <span className="sr-only"> · {t(`job.activity.${step.state}`)}</span>
              {(step.state === "failed" ||
                step.state === "cancelled" ||
                step.state === "unknown") && (
                <span aria-hidden="true" className="ml-2 text-xs text-muted-foreground">
                  {t(`job.activity.${step.state}`)}
                </span>
              )}
              {step.depends_on.length > 0 && (
                <p className="mt-1 text-xs text-muted-foreground">
                  {t("job.activity.dependsOn", {
                    steps: step.depends_on
                      .map(
                        (id) =>
                          plan.steps.find((candidate) => candidate.conversation === id)?.title ??
                          id,
                      )
                      .join(", "),
                  })}
                </p>
              )}
            </div>
          </li>
        ))}
      </ol>
    </div>
  );
}
