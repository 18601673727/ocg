import {
  jobTranscript,
  TranscriptAccumulator,
  type TranscriptTurn,
} from "../../chat/job-transcript-model";
import type { ChatImage } from "../../contracts";
import { fileSnapshots } from "../../markdown/file-snapshot";
import type { ChatMessage } from "../../types";
import { nativeExecutionStatus } from "../job-status";
import type { OutputHistory, OutputView } from "../output-history";
import { type Turn, turnAttachments, turnCanRetry } from "./turn";

export class TurnProjection {
  private readonly projections = new WeakMap<
    Turn,
    {
      outputs: OutputHistory;
      length: number;
      aggregate: TranscriptAccumulator;
      arguments?: readonly unknown[];
      transcript?: TranscriptTurn[];
    }
  >();
  private readonly messageProjections = new WeakMap<
    Turn,
    { inputs: readonly unknown[]; messages: ChatMessage[] }
  >();

  constructor(private readonly images: ReadonlyMap<string, ChatImage>) {}

  private projectTurn(turn: Turn): { transcript: TranscriptTurn[]; result: OutputView | null } {
    let cached = this.projections.get(turn);
    if (!cached || cached.outputs !== turn.outputs || cached.length > turn.outputs.length) {
      cached = { outputs: turn.outputs, length: 0, aggregate: new TranscriptAccumulator() };
      this.projections.set(turn, cached);
    }
    while (cached.length < turn.outputs.length) {
      const output = turn.outputs.at(cached.length++);
      if (!output) throw new Error("Stored output projection is incomplete.");
      cached.aggregate.append(output);
    }
    const args = [
      turn.outputs,
      turn.outputs.length,
      turn.job,
      turn.input,
      turn.entries,
      turn.tools,
      turn.responses,
      turn.partialToolBoundaries,
      turn.details?.mutable_phase,
    ];
    if (!cached.arguments || args.some((value, index) => value !== cached!.arguments![index])) {
      const transcript = jobTranscript(
        turn.job.id,
        turn.entries && turn.entries.length > 0 ? null : turn.input.objective,
        turn.outputs,
        turn.job.settlement,
        turn.job.settlement.state === "unsettled" && turn.job.started_at_us !== null,
        undefined,
        turn.tools,
        turn.job.previous === null,
        turn.responses,
        turn.partialToolBoundaries,
        cached.aggregate,
      );
      cached.transcript =
        turn.details?.mutable_phase === "queued"
          ? transcript.filter(
              (message) =>
                !(message.role === "assistant" && message.status === "pending" && !message.content),
            )
          : transcript;
      cached.arguments = args;
    }
    return { transcript: cached.transcript ?? [], result: cached.aggregate.result };
  }

  private projectMessages(turn: Turn): ChatMessage[] {
    if (turn.native_decision) {
      const state = turn.job.settlement.state;
      return [
        {
          id: `${turn.job.id}:decision`,
          jobId: turn.job.id,
          commandId: turn.job.id,
          role: "assistant" as const,
          content: "",
          decision: turn.native_decision,
          planningTarget: turn.planning_target,
          failureReason: turn.unknownReason ?? undefined,
          status: state === "unsettled" ? ("pending" as const) : state,
          execution: nativeExecutionStatus(turn.job.execution_phase, turn.details),
          createdAt: new Date(turn.job.created_at_us / 1000).toISOString(),
        },
      ];
    }
    const execution = nativeExecutionStatus(
      turn.job.execution_phase,
      turn.details,
      turnCanRetry(turn),
    );
    const entries = turn.entries ?? [];
    const queued =
      turn.job.previous !== null &&
      turn.job.started_at_us === null &&
      turn.job.settlement.state === "unsettled";
    const inputs: ChatMessage[] = entries.map((entry) => ({
      execution,
      queued,
      id: `${turn.job.id}:entry:${entry.seq}`,
      jobId: turn.job.id,
      commandId: turn.job.id,
      role: "user",
      content: entry.text ?? "",
      childResults: entry.child_results,
      childNotice: entry.child_notice,
      status: "completed",
      createdAt: new Date(entry.at_us / 1000).toISOString(),
      images: entry.attachments.flatMap((id) => this.images.get(id) ?? []),
    }));
    const boundary: ChatMessage[] = turn.compaction
      ? [
          {
            id: `${turn.job.id}:compaction`,
            jobId: turn.job.id,
            commandId: turn.job.id,
            role: "assistant",
            content: "",
            status: "completed",
            intermediate: true,
            createdAt: new Date(turn.compaction.at_us / 1000).toISOString(),
            compaction: turn.compaction,
          },
        ]
      : [];
    const projected = this.projectTurn(turn);
    const transcript = projected.transcript;
    return [
      ...boundary,
      ...inputs,
      ...transcript.map((message) => ({
        id: message.id,
        jobId: turn.job.id,
        commandId: turn.job.id,
        role: message.role,
        content: message.content,
        status: message.status,
        intermediate: message.intermediate,
        activity: turn.details?.activity,
        taskPlan: turn.details?.task_plan,
        execution,
        createdAt: new Date((message.createdAtUs ?? turn.job.created_at_us) / 1000).toISOString(),
        ...(message.role === "user"
          ? { images: turn.input.attachments.flatMap((id) => this.images.get(id) ?? []) }
          : {
              toolCalls: [...(message.tools ?? [])],
              reasoning: message.reasoning,
              sourceSnapshots: message.intermediate
                ? undefined
                : fileSnapshots(projected.result?.value),
              reasoningDurationMs: message.reasoningDurationMs,
              failureReason: message.intermediate
                ? undefined
                : (turn.error ??
                  turn.toolError ??
                  (message.rejection ? undefined : message.detail) ??
                  turn.unknownReason ??
                  undefined),
              failureRejection: message.intermediate ? undefined : (message.rejection ?? undefined),
              failureCode:
                !message.intermediate && message.status === "unknown"
                  ? turn.details?.allocation?.holds_capacity
                    ? ("outcome-held" as const)
                    : turn.job.stop_confirmed_at_us === null
                      ? ("outcome-unconfirmed" as const)
                      : ("outcome-stopped" as const)
                  : undefined,
              failureAt:
                !message.intermediate &&
                !turn.error &&
                !turn.toolError &&
                message.status !== "completed" &&
                message.status !== "streaming" &&
                message.status !== "pending" &&
                turn.job.settlement.settled_at_us !== null
                  ? turn.job.settlement.settled_at_us / 1000
                  : undefined,
            }),
      })),
    ];
  }

  messagesForTurn(turn: Turn): ChatMessage[] {
    // OutputHistory grows in place, so its length is part of the projection inputs.
    const inputs = [
      turn.job,
      turn.input,
      turn.details,
      turn.entries,
      turn.outputs,
      turn.outputs.length,
      turn.tools,
      turn.responses,
      turn.partialToolBoundaries,
      turn.compaction,
      turn.content_kind,
      turn.native_decision,
      turn.planning_target,
      turn.error,
      turn.toolError,
      turn.unknownReason,
      ...Array.from(turnAttachments(turn), (id) => this.images.get(id)),
    ];
    const cached = this.messageProjections.get(turn);
    if (
      cached &&
      cached.inputs.length === inputs.length &&
      inputs.every((input, index) => input === cached.inputs[index])
    ) {
      return cached.messages;
    }
    const messages = this.projectMessages(turn);
    this.messageProjections.set(turn, { inputs, messages });
    return messages;
  }
}
