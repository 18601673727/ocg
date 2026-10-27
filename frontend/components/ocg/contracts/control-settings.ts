import type * as Api from "./api";

type Confidence = { value: number; semantics: string };
export type ControlDecisionAnswer =
  | {
      kind: "choice";
      selected: string;
      distribution?: Record<string, number> | null;
      confidence?: Confidence | null;
    }
  | { kind: "binary_probability"; true_probability: number }
  | {
      kind: "ordinal_score";
      expectation: number;
      distribution?: number[] | null;
      confidence?: Confidence | null;
    };
export type ControlPredictionResult = {
  schema_version: number;
  outcome:
    | { kind: "answer"; answer: ControlDecisionAnswer }
    | { kind: "uncertain" | "unusable"; reason: string };
  requested_model: string;
  reported_model: string | null;
  model_version: string | null;
};
type WirePredictionResult = Omit<ControlPredictionResult, "outcome"> & {
  outcome:
    | { status: "answer"; answer: ControlDecisionAnswer }
    | { status: "uncertain" | "unusable"; reason: string };
};
type PredictionWork = Omit<Extract<Api.ContextWork, { kind: "prediction" }>, "result"> & {
  result: ControlPredictionResult | null;
};
export type ControlContextStatus = Omit<Api.ContextStatus, "work" | "steer_blocked_by"> & {
  steer_blocked_by?: "rule_based" | "manual";
  work: (Extract<Api.ContextWork, { kind: "summarization" }> | PredictionWork)[];
};

export function projectContextStatus(value: Api.ContextStatus): ControlContextStatus {
  return {
    ...value,
    steer_blocked_by: value.steer_blocked_by as ControlContextStatus["steer_blocked_by"],
    work: value.work.map((work) => {
      if (work.kind !== "prediction" || work.result === null)
        return work as PredictionWork | Extract<Api.ContextWork, { kind: "summarization" }>;
      const result = work.result as unknown as WirePredictionResult;
      const { status, ...outcome } = result.outcome;
      return {
        ...work,
        result: {
          ...result,
          outcome: { ...outcome, kind: status } as ControlPredictionResult["outcome"],
        },
      };
    }),
  };
}
