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
export type ContextStatus = Omit<Api.ContextStatus, "steer_blocked_by"> & {
  steer_blocked_by?: "rule_based" | "manual";
};

export function projectContextStatus(value: Api.ContextStatus): ContextStatus {
  return {
    ...value,
    steer_blocked_by: value.steer_blocked_by as ContextStatus["steer_blocked_by"],
  };
}
