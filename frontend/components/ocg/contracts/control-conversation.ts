import type * as Api from "./api";
import type { ControlTreeNode } from "./control-shared";

export type ProjectedEntry = {
  seq: number;
  at_us: number;
  kind: Api.TurnEntry["kind"];
  message: number | null;
  text: string | null;
  attachments: string[];
};
export type ControlConversationTurn = Omit<
  Api.Turn,
  "entries" | "configuration" | "uncertain_calls"
> & {
  entries: ProjectedEntry[];
  input: Api.Input & { job: string };
};
export type ControlConversation = Omit<Api.Turns, "turns"> & { turns: ControlConversationTurn[] };
export type ControlChildConversation = Omit<
  Api.Child,
  "state" | "origin" | "job_state" | "recurrence"
> & {
  state: "open" | "closing" | "settled";
  origin?: "user" | "system" | "agent";
  job_state:
    | "failed"
    | "unknown"
    | "cancelled"
    | "running"
    | "completed"
    | "queued"
    | "waiting_for_user"
    | "waiting";
  recurrence:
    | (Omit<Api.Recurrence, "phase"> & { phase: "active" | "waiting_due" | "halted" })
    | null;
};
export type ControlChildren = Omit<Api.Children, "children"> & {
  children: ControlChildConversation[];
};
export type ControlConversationCard = Api.Card;
export type ControlJobTree = Omit<Api.Tree, "root"> & { root: ControlTreeNode };

export function materializeUserEntries(entries: readonly ProjectedEntry[]) {
  const visible = new Map<
    number,
    ProjectedEntry & { withdrawn: boolean; history: { content: string; at: string }[] }
  >();
  for (const entry of entries) {
    if (entry.kind === "user_message" || entry.kind === "agent_message") {
      visible.set(entry.seq, { ...entry, withdrawn: false, history: [] });
    } else if (entry.kind === "user_message_edit" || entry.kind === "user_message_withdraw") {
      const original = entry.message === null ? undefined : visible.get(entry.message);
      if (!original) continue;
      if (entry.kind === "user_message_edit") {
        original.history.push({
          content: original.text ?? "",
          at: new Date(entry.at_us / 1000).toISOString(),
        });
        original.text = entry.text;
        original.attachments = entry.attachments;
      } else {
        original.withdrawn = true;
      }
    }
  }
  return [...visible.values()];
}

export function projectConversation(page: Api.Turns): ControlConversation {
  return {
    ...page,
    turns: page.turns.map((turn) => {
      const entries = turn.entries.map((entry) => ({
        seq: entry.seq,
        kind: entry.kind,
        at_us: entry.at_us,
        message: "message" in entry ? entry.message : null,
        text: "text" in entry ? entry.text : null,
        attachments: "attachments" in entry ? entry.attachments : [],
      }));
      const messages = materializeUserEntries(entries).filter((entry) => !entry.withdrawn);
      return {
        job: turn.job,
        entries,
        compaction: turn.compaction,
        input: {
          job: turn.job.id,
          id: turn.job.input,
          objective: messages.length ? messages.map((entry) => entry.text ?? "").join("\n") : null,
          submitted_at_us: turn.job.created_at_us,
          attachments: messages.flatMap((entry) => entry.attachments),
          configuration: turn.configuration,
        },
        outputs: turn.outputs,
        outputs_truncated: turn.outputs_truncated,
      };
    }),
  };
}
