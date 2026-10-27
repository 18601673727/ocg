"use client";

import { useI18n, type I18nKey } from "../i18n";
import type { ApprovalAssessment } from "../contracts/api";

const ACTION_LABELS: Record<string, I18nKey> = {
  job_plan: "chat.card.plan",
  workspace_run_command: "chat.card.command",
  workspace_request_command: "chat.card.commandGrant",
  workspace_write_file: "chat.card.write",
  job_dispatch: "chat.card.dispatch",
  job_assign: "chat.card.dispatch",
  job_close: "chat.card.close",
};

export function ApprovalRequest({
  tool,
  arguments: raw,
  assessment,
}: {
  tool: string;
  arguments: string;
  assessment?: ApprovalAssessment;
}) {
  const { t, locale } = useI18n();
  const percent = (value: number) =>
    new Intl.NumberFormat(locale, { style: "percent", maximumFractionDigits: 2 }).format(value);
  let args: Record<string, unknown> = {};
  let formatted = raw;
  try {
    const value: unknown = JSON.parse(raw);
    formatted = JSON.stringify(value, null, 2);
    if (value && typeof value === "object" && !Array.isArray(value))
      args = Object.fromEntries(Object.entries(value));
  } catch {
    /* The original request remains available for review. */
  }
  const steps =
    tool === "job_plan" && Array.isArray(args.steps)
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
      : null;
  const plan = steps?.length && steps.every((step) => step !== null) ? steps : null;
  const target =
    tool === "workspace_run_command" && typeof args.command === "string"
      ? args.command
      : tool === "workspace_request_command" &&
          Array.isArray(args.argv) &&
          args.argv.every((arg) => typeof arg === "string")
        ? args.argv.join(" ")
        : tool === "workspace_write_file" && typeof args.path === "string"
          ? args.path
          : null;
  const note =
    tool === "job_plan" && Array.isArray(args.steps)
      ? t("chat.card.planNote", { count: args.steps.length })
      : tool === "workspace_request_command"
        ? t("chat.card.commandGrantNote")
        : t("chat.card.review");
  return (
    <div className="space-y-2">
      <p className="text-sm font-medium">{t(ACTION_LABELS[tool] ?? "chat.card.action")}</p>
      <p className="text-muted-foreground">{note}</p>
      {plan && (
        <ol className="space-y-3 py-1" aria-label={t("chat.card.plan")}>
          {plan.map((step, index) => (
            <li key={index} className="rounded-lg border border-border p-3">
              <div className="flex items-start gap-2.5">
                <span
                  className="flex size-6 shrink-0 items-center justify-center rounded-full bg-muted text-xs font-medium tabular-nums"
                  aria-hidden="true"
                >
                  {index + 1}
                </span>
                <div className="min-w-0 space-y-2">
                  <h3 className="text-sm font-medium break-words">{step.title}</h3>
                  <p className="text-xs text-muted-foreground">
                    {step.dependencies.length
                      ? t("job.activity.dependsOn", {
                          steps: step.dependencies
                            .map((dependency) => `${dependency + 1}. ${plan[dependency].title}`)
                            .join(" · "),
                        })
                      : t("chat.card.independentStep")}
                  </p>
                  <p className="text-sm leading-relaxed whitespace-pre-wrap break-words">
                    {step.objective}
                  </p>
                  {(step.agent || step.model) && (
                    <dl className="space-y-1 text-xs text-muted-foreground">
                      {step.agent && (
                        <div>
                          <dt className="inline font-medium">{t("chat.agent")}: </dt>
                          <dd className="inline break-all">{step.agent}</dd>
                        </div>
                      )}
                      {step.model && (
                        <div>
                          <dt className="inline font-medium">{t("chat.model")}: </dt>
                          <dd className="inline break-all">{step.model}</dd>
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
      {target && <p className="break-all rounded-md bg-muted/50 px-3 py-2 font-mono">{target}</p>}
      {assessment && (
        <div
          className="rounded-md border border-border bg-muted/30 px-3 py-2 text-muted-foreground"
          role="status"
        >
          {assessment.confidence === null ? (
            <p>
              {t(
                assessment.state === "pending" || assessment.state === "running"
                  ? "chat.card.assessmentPending"
                  : assessment.state === "failed"
                    ? "chat.card.assessmentFailed"
                    : "chat.card.assessmentUnavailable",
              )}
            </p>
          ) : (
            <p>
              {t("chat.card.assessmentConfidence", { confidence: percent(assessment.confidence) })}
            </p>
          )}
          <p>
            {t("chat.card.assessmentThreshold", {
              threshold: percent(assessment.minimum_confidence),
            })}
          </p>
        </div>
      )}
      <details className="rounded-lg border border-border">
        <summary className="cursor-pointer px-3 py-2 text-muted-foreground">
          {t("chat.card.rawRequest")}
        </summary>
        <div className="space-y-2 border-t border-border p-3">
          <p className="text-muted-foreground">{t("chat.card.originalDetails")}</p>
          <p className="font-mono">{tool}</p>
          <pre className="ocg-subtle-scrollbar max-h-64 max-w-full overflow-auto overscroll-contain rounded-md bg-muted/50 p-3 font-mono text-xs whitespace-pre-wrap break-all">
            {formatted}
          </pre>
        </div>
      </details>
    </div>
  );
}
