import type * as Api from "./api";

type Confidence = { value: number; semantics: string };
export type DecisionAnswer =
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
export type PredictionResult = {
  outcome:
    | { kind: "answer"; answer: DecisionAnswer }
    | { kind: "uncertain" | "unusable"; reason: string };
  requested_model: string;
  reported_model: string | null;
  model_version: string | null;
};
type WirePredictionResult = Omit<PredictionResult, "outcome"> & {
  outcome:
    | { status: "answer"; answer: DecisionAnswer }
    | { status: "uncertain" | "unusable"; reason: string };
};
type PredictionWork = Omit<Extract<Api.ContextWork, { kind: "prediction" }>, "result"> & {
  result: PredictionResult | null;
};
export type ContextStatus = Omit<Api.ContextStatus, "work" | "steer_blocked_by"> & {
  steer_blocked_by?: "rule_based" | "manual";
  work: (Extract<Api.ContextWork, { kind: "summarization" }> | PredictionWork)[];
};

export function projectContextStatus(value: Api.ContextStatus): ContextStatus {
  return {
    ...value,
    steer_blocked_by: value.steer_blocked_by as ContextStatus["steer_blocked_by"],
    work: value.work.map((work) => {
      if (work.kind !== "prediction" || work.result === null)
        return work as PredictionWork | Extract<Api.ContextWork, { kind: "summarization" }>;
      const result = work.result as unknown as WirePredictionResult;
      const { status, ...outcome } = result.outcome;
      return {
        ...work,
        result: {
          ...result,
          outcome: { ...outcome, kind: status } as PredictionResult["outcome"],
        },
      };
    }),
  };
}
