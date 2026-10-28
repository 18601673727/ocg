"use client";

import Link from "next/link";
import type { ControlTaskPlan } from "../contracts";
import { useI18n } from "../i18n";
import { useOpenJobDetails } from "./job-details-context";

export function TaskStatus({ plan }: { plan?: ControlTaskPlan | null }) {
  const { t } = useI18n();
  const openJobDetails = useOpenJobDetails();
  if (!plan) return null;
  const phase = plan.state;
  return (
    <div className="my-3 space-y-2 rounded-lg border border-border p-3 text-sm">
      <p
        role="status"
        className={phase === "failed" ? "text-destructive" : "text-muted-foreground"}
      >
        {t(`job.activity.${phase}`)}
      </p>
      <p className="font-medium">{t("job.activity.plan")}</p>
      <ol className="list-decimal space-y-2 pl-5">
        {plan.steps.map((step) => (
          <li key={step.conversation}>
            <Link
              className="underline underline-offset-2"
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
            </Link>{" "}
            · {t(`job.activity.${step.state}`)}
            {step.depends_on.length > 0 && (
              <p className="text-xs text-muted-foreground">
                {t("job.activity.dependsOn", {
                  steps: step.depends_on
                    .map(
                      (id) =>
                        plan.steps.findIndex((candidate) => candidate.conversation === id) + 1,
                    )
                    .join(", "),
                })}
              </p>
            )}
          </li>
        ))}
      </ol>
    </div>
  );
}
