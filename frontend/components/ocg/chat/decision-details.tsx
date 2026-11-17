import type { NativeDecisionTurn } from "../contracts/conversations";
import { useI18n } from "../i18n";
import { executionDetail, executionLabel } from "../runtime/job-status";
import type { ChatMessage } from "../types";

export function DecisionDetails({
  decision,
  message,
  full = false,
}: {
  decision: NativeDecisionTurn;
  message: ChatMessage;
  full?: boolean;
}) {
  const { t } = useI18n();
  const { request, result } = decision;
  const callerState = (
    <pre className="overflow-x-auto whitespace-pre-wrap">
      {JSON.stringify(request.state, null, 2)}
    </pre>
  );
  const failure = decision.failure ?? (full ? message.failureReason : undefined);
  return (
    <>
      <strong>Decision · {request.question.kind.replaceAll("_", " ")}</strong>
      <p className="mt-2 whitespace-pre-wrap">{request.question.prompt}</p>
      {request.question.options?.map((option) => (
        <p key={option.id}>
          {option.id}: {option.description}
        </p>
      ))}
      {request.question.levels?.map((level) => (
        <p key={level.value}>
          {level.value}: {level.description}
        </p>
      ))}
      {full ? (
        <div>
          <strong>{t("planning.callerState")}</strong>
          {callerState}
        </div>
      ) : (
        <details className="mt-2">
          <summary>Caller state</summary>
          {callerState}
        </details>
      )}
      {result ? (
        <>
          <strong>{full ? t("planning.result") : "Result"}</strong>
          <pre className="overflow-x-auto whitespace-pre-wrap">
            {JSON.stringify(full ? result : result.outcome, null, 2)}
          </pre>
        </>
      ) : (
        <div className="mt-2" role="status">
          <p>
            <strong>
              {message.execution
                ? executionLabel(t, message.execution)
                : message.status === "pending"
                  ? "Pending"
                  : `Job ${message.status}`}
            </strong>
            {failure ? ` · ${failure}` : ""}
          </p>
          {message.execution && executionDetail(t, message.execution) && (
            <p className="text-muted-foreground">{executionDetail(t, message.execution)}</p>
          )}
          {message.status === "unknown" &&
            message.failureReason === undefined &&
            !(message.execution && executionDetail(t, message.execution)) && (
              <p className="text-muted-foreground">
                The outcome could not be established; it is not reported as stopped.
              </p>
            )}
        </div>
      )}
    </>
  );
}
