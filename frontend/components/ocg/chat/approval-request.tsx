import { createMemo, Show } from "solid-js";
import { ApprovalCard } from "@/components/ocg/chat/ui/approval-card";
import type { ApprovalAssessment } from "../contracts/api";
import { type I18nKey, useI18n } from "../i18n";

const ACTION_LABELS: Record<string, I18nKey> = {
  job_plan: "chat.card.plan",
  workspace_run_command: "chat.card.command",
  workspace_request_command: "chat.card.commandGrant",
  workspace_write_file: "chat.card.write",
  job_dispatch: "chat.card.dispatch",
  job_assign: "chat.card.dispatch",
  job_close: "chat.card.close",
};

export function ApprovalRequest(props: {
  tool: string;
  arguments: string;
  assessment?: ApprovalAssessment;
  disabled: boolean;
  onApprove: () => void;
  onReject: () => void;
}) {
  const i18n = useI18n();
  const percent = (value: number) =>
    new Intl.NumberFormat(i18n.locale, { style: "percent", maximumFractionDigits: 2 }).format(
      value,
    );
  let args: Record<string, unknown> = {};
  let formatted = props.arguments;
  try {
    const value: unknown = JSON.parse(props.arguments);
    formatted = JSON.stringify(value, null, 2);
    if (value && typeof value === "object" && !Array.isArray(value))
      args = Object.fromEntries(Object.entries(value));
  } catch {}
  const steps = createMemo(() =>
    props.tool === "job_plan" && Array.isArray(args.steps)
      ? args.steps.map((value: unknown, index) => {
          if (
            !value ||
            typeof value !== "object" ||
            !("title" in value) ||
            typeof value.title !== "string" ||
            !("objective" in value) ||
            typeof value.objective !== "string" ||
            !("depends_on" in value) ||
            !Array.isArray(value.depends_on) ||
            !value.depends_on.every(
              (dependency: unknown): dependency is number =>
                typeof dependency === "number" &&
                Number.isSafeInteger(dependency) &&
                dependency >= 0 &&
                dependency < index,
            )
          )
            return null;
          return {
            title: value.title,
            objective: value.objective,
            dependencies: value.depends_on,
            agent: "agent" in value && typeof value.agent === "string" ? value.agent : null,
            model: "model" in value && typeof value.model === "string" ? value.model : null,
          };
        })
      : null,
  );
  const plan = createMemo(() => {
    const current = steps();
    return current?.length && current.every((step) => step !== null) ? current : null;
  });
  const target = createMemo(() =>
    props.tool === "workspace_run_command" && typeof args.command === "string"
      ? args.command
      : props.tool === "workspace_request_command" &&
          Array.isArray(args.argv) &&
          args.argv.every((arg) => typeof arg === "string")
        ? args.argv.join(" ")
        : props.tool === "workspace_write_file" && typeof args.path === "string"
          ? args.path
          : null,
  );
  const note = createMemo(() =>
    props.tool === "job_plan" && Array.isArray(args.steps)
      ? i18n.t("chat.card.planNote", { count: args.steps.length })
      : props.tool === "workspace_request_command"
        ? i18n.t("chat.card.commandGrantNote")
        : i18n.t("chat.card.review"),
  );
  return (
    <ApprovalCard
      state="request"
      class="max-w-none text-xs"
      title={i18n.t(ACTION_LABELS[props.tool] ?? "chat.card.action")}
      subtitle={i18n.t("chat.card.approval")}
      description={note()}
      command={target() ?? undefined}
      disabled={props.disabled}
      allowOnceLabel={i18n.t("chat.card.approve")}
      denyLabel={i18n.t("chat.card.reject")}
      onAllowOnce={props.onApprove}
      onDeny={props.onReject}
    >
      <Show when={plan()}>
        {(currentPlan) => (
          <ol class="space-y-3 py-1" aria-label={i18n.t("chat.card.plan")}>
            {currentPlan().map((step, index, items) => (
              <li class="rounded-lg border border-border p-3">
                <div class="flex items-start gap-2.5">
                  <span
                    class="flex size-6 shrink-0 items-center justify-center rounded-full bg-muted text-xs font-medium tabular-nums"
                    aria-hidden="true"
                  >
                    {index + 1}
                  </span>
                  <div class="min-w-0 space-y-2">
                    <h3 class="text-sm font-medium break-words">{step.title}</h3>
                    <p class="text-xs text-muted-foreground">
                      {step.dependencies.length
                        ? i18n.t("job.activity.dependsOn", {
                            steps: step.dependencies
                              .map((dependency) => {
                                const title = items[dependency]?.title;
                                return title === undefined
                                  ? String(dependency + 1)
                                  : `${dependency + 1}. ${title}`;
                              })
                              .join(" · "),
                          })
                        : i18n.t("chat.card.independentStep")}
                    </p>
                    <p class="text-sm leading-relaxed whitespace-pre-wrap break-words">
                      {step.objective}
                    </p>
                    {(step.agent || step.model) && (
                      <dl class="space-y-1 text-xs text-muted-foreground">
                        {step.agent && (
                          <div>
                            <dt class="inline font-medium">{i18n.t("chat.agent")}: </dt>
                            <dd class="inline break-all">{step.agent}</dd>
                          </div>
                        )}
                        {step.model && (
                          <div>
                            <dt class="inline font-medium">{i18n.t("chat.model")}: </dt>
                            <dd class="inline break-all">{step.model}</dd>
                          </div>
                        )}
                      </dl>
                    )}
                  </div>
                </div>
              </li>
            ))}
          </ol>
        )}
      </Show>
      {props.assessment && (
        <div
          class="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground"
          role="status"
        >
          {props.assessment.confidence === null ? (
            <p>
              {i18n.t(
                props.assessment.state === "pending" || props.assessment.state === "running"
                  ? "chat.card.assessmentPending"
                  : props.assessment.state === "failed"
                    ? "chat.card.assessmentFailed"
                    : "chat.card.assessmentUnavailable",
              )}
            </p>
          ) : (
            <p>
              {i18n.t("chat.card.assessmentConfidence", {
                confidence: percent(props.assessment.confidence),
              })}
            </p>
          )}
          <p>
            {i18n.t("chat.card.assessmentThreshold", {
              threshold: percent(props.assessment.minimum_confidence),
            })}
          </p>
        </div>
      )}
      <details class="rounded-lg border border-border">
        <summary class="cursor-pointer px-3 py-2 text-muted-foreground">
          {i18n.t("chat.card.rawRequest")}
        </summary>
        <div class="space-y-2 border-t border-border p-3">
          <p class="text-muted-foreground">{i18n.t("chat.card.originalDetails")}</p>
          <p class="font-mono">{props.tool}</p>
          <pre class="ocg-subtle-scrollbar max-h-64 max-w-full overflow-auto overscroll-contain rounded-md bg-muted/50 p-3 font-mono text-xs whitespace-pre-wrap break-all">
            {formatted}
          </pre>
        </div>
      </details>
    </ApprovalCard>
  );
}
