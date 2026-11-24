import type { PartialToolBoundary } from "../../chat/job-transcript-model";
import type {
  ConversationTurn,
  Job,
  JobDetails,
  ProviderResponse,
  ToolCall,
} from "../../contracts";
import { taskPlanActive } from "../job-status";
import type { OutputHistory } from "../output-history";

export const TURN_PAGE_SIZE = 26;

export type Turn = {
  content_kind?: ConversationTurn["content_kind"];
  native_decision?: ConversationTurn["native_decision"];
  planning_target?: ConversationTurn["planning_target"];
  compaction?: ConversationTurn["compaction"];
  details?: JobDetails;
  entries?: ConversationTurn["entries"];
  sessionId: string;
  job: Job;
  input: ConversationTurn["input"];
  outputs: OutputHistory;
  outputRevision?: number;
  evidenceRevision: number;
  tools: ToolCall[];
  responses: ProviderResponse[];
  partialToolBoundaries?: PartialToolBoundary[] | null;
  partialToolTimes?: ReadonlyMap<number, number>;
  error?: string;
  toolError?: string;
  unknownReason?: string | null;
};

export function needsWatch(turn: Turn): boolean {
  return (
    turn.job.settlement.state === "unsettled" ||
    (turn.job.settlement.state === "unknown" &&
      (Boolean(turn.native_decision) ||
        (turn.details?.allocation?.holds_capacity ?? turn.job.stop_confirmed_at_us === null))) ||
    taskPlanActive(turn.details?.task_plan)
  );
}

export function turnCanRetry(turn: Turn): boolean {
  if (turn.content_kind !== "generative" || turn.native_decision || !turn.details) return false;
  if (turn.details.allocation?.holds_capacity) return false;
  switch (turn.job.settlement.state) {
    case "failed":
    case "cancelled":
      return true;
    case "unknown":
    case "unsettled":
    case "completed":
      return false;
  }
}

export function* turnAttachments(turn: Turn): Iterable<string> {
  yield* turn.input.attachments;
  for (const entry of turn.entries ?? []) yield* entry.attachments;
}
