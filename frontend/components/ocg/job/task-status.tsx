import { Check, CircleDashed, CircleHelp, CircleMinus, CircleX, LoaderCircle } from "@lucide/solid";
import { createMemo } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
import type { TaskPlan } from "../contracts";
import { useI18n } from "../i18n";
import { useOpenJobDetails } from "./job-details-context";

type StepState = TaskPlan["steps"][number]["state"];

function StepIcon(props: { state: StepState }) {
  const iconClass = "mt-0.5 size-4 shrink-0";
  switch (props.state) {
    case "completed":
      return (
        <span
          class={`${iconClass} flex items-center justify-center rounded-full bg-muted-foreground text-background`}
        >
          <Check aria-hidden="true" class="size-3" strokeWidth={3} />
        </span>
      );
    case "queued":
      return <CircleDashed aria-hidden="true" class={`${iconClass} text-muted-foreground`} />;
    case "failed":
      return <CircleX aria-hidden="true" class={`${iconClass} text-destructive`} />;
    case "cancelled":
      return <CircleMinus aria-hidden="true" class={`${iconClass} text-muted-foreground`} />;
    case "unknown":
      return <CircleHelp aria-hidden="true" class={`${iconClass} text-muted-foreground`} />;
    case "starting":
    case "waiting_for_provider":
    case "running_tools":
    case "waiting_for_user":
    case "rate_limited":
      return (
        <LoaderCircle
          aria-hidden="true"
          class={`${iconClass} animate-spin text-muted-foreground motion-reduce:animate-none`}
        />
      );
  }
}

export function TaskStatus(props: { plan?: TaskPlan | null }) {
  const i18n = useI18n();
  const openJobDetails = useOpenJobDetails();
  return (
    <>
      {(() => {
        if (!props.plan) return null;
        const phase = createMemo(() => props.plan!.state);
        return (
          <div class="my-3 rounded-xl border border-border px-4 py-3 text-sm">
            <p
              role="status"
              class={
                phase() === "failed"
                  ? "mb-3 text-destructive"
                  : phase() === "cancelled" || phase() === "unknown"
                    ? "mb-3 text-muted-foreground"
                    : "sr-only"
              }
            >
              {i18n.t("job.activity.plan")} · {i18n.t(`job.activity.${phase()}`)}
            </p>
            <ol aria-label={i18n.t("job.activity.plan")} class="space-y-3">
              {props.plan.steps.map((step) => (
                <li class="flex items-start gap-3">
                  <StepIcon state={step.state} />
                  <div class="min-w-0">
                    <WorkspaceLink
                      class={`${step.state === "completed" ? "text-muted-foreground" : "text-foreground"} rounded-sm hover:underline focus-visible:outline-2 focus-visible:outline-offset-2`}
                      href={`/jobs/${step.turn}`}
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
                    </WorkspaceLink>
                    <span class="sr-only"> · {i18n.t(`job.activity.${step.state}`)}</span>
                    {(step.state === "failed" ||
                      step.state === "cancelled" ||
                      step.state === "unknown") && (
                      <span aria-hidden="true" class="ml-2 text-xs text-muted-foreground">
                        {i18n.t(`job.activity.${step.state}`)}
                      </span>
                    )}
                    {step.depends_on.length > 0 && (
                      <p class="mt-1 text-xs text-muted-foreground">
                        {i18n.t("job.activity.dependsOn", {
                          steps: step.depends_on
                            .map(
                              (id) =>
                                props.plan!.steps.find((candidate) => candidate.conversation === id)
                                  ?.title ?? id,
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
      })()}
    </>
  );
}
