import { createMemo } from "solid-js";
import type { NativeDecisionTurn } from "../contracts/conversations";
import { runtimeStateLabel, useI18n } from "../i18n";
import { executionDetail, executionLabel } from "../runtime/job-status";
import type { ChatMessage } from "../types";

export function DecisionDetails(props: {
  decision: NativeDecisionTurn;
  message: ChatMessage;
  full?: boolean;
}) {
  const i18n = useI18n();
  const { request, result } = props.decision;
  const callerState = (
    <pre class="overflow-x-auto whitespace-pre-wrap">{JSON.stringify(request.state, null, 2)}</pre>
  );
  const failure = createMemo(
    () =>
      props.decision.failure ??
      ((props.full === undefined ? false : props.full) ? props.message.failureReason : undefined),
  );
  return (
    <>
      <strong>
        {i18n.t("settings.agent.capability.decision")} ·{" "}
        {request.question.kind.replaceAll("_", " ")}
      </strong>
      <p class="mt-2 whitespace-pre-wrap">{request.question.prompt}</p>
      {request.question.options?.map((option) => (
        <p>
          {option.id}: {option.description}
        </p>
      ))}
      {request.question.levels?.map((level) => (
        <p>
          {level.value}: {level.description}
        </p>
      ))}
      {(props.full === undefined ? false : props.full) ? (
        <div>
          <strong>{i18n.t("planning.callerState")}</strong>
          {callerState}
        </div>
      ) : (
        <details class="mt-2">
          <summary>{i18n.t("planning.callerState")}</summary>
          {callerState}
        </details>
      )}
      {result ? (
        <>
          <strong>{i18n.t("planning.result")}</strong>
          <pre class="overflow-x-auto whitespace-pre-wrap">
            {JSON.stringify(
              (props.full === undefined ? false : props.full) ? result : result.outcome,
              null,
              2,
            )}
          </pre>
        </>
      ) : (
        <div class="mt-2" role="status">
          <p>
            <strong>
              {props.message.execution
                ? executionLabel(i18n.t, props.message.execution)
                : runtimeStateLabel(i18n.t, props.message.status)}
            </strong>
            {failure() ? ` · ${failure()}` : ""}
          </p>
          {props.message.execution && executionDetail(i18n.t, props.message.execution) && (
            <p class="text-muted-foreground">{executionDetail(i18n.t, props.message.execution)}</p>
          )}
          {props.message.status === "unknown" &&
            props.message.failureReason === undefined &&
            !(props.message.execution && executionDetail(i18n.t, props.message.execution)) && (
              <p class="text-muted-foreground">{i18n.t("job.phase.unknownDetail")}</p>
            )}
        </div>
      )}
    </>
  );
}
