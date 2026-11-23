import {
  type Approval,
  type Children,
  ContractError,
  projectContextStatus,
  projectConversation,
  projectJob,
  TREE_MAX_DEPTH,
} from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { ApiError, type Send } from "./transport";
import { identity, jobIdentity, sequence, submissionKey } from "./validators";

export type ConversationEdit =
  | { title: string; hidden?: never }
  | { hidden: boolean; title?: never }
  | {
      limits: Pick<Api.ConversationLimits, "max_depth" | "max_auto_turns" | "max_iterations">;
      title?: never;
      hidden?: never;
    };

export function createConversationsApi(send: Send, conversationListeners: Set<() => void>) {
  return {
    onConversationsChanged(listener: () => void) {
      conversationListeners.add(listener);
      return () => {
        conversationListeners.delete(listener);
      };
    },

    async readConversations(
      settings: { before?: string; limit?: number; archived?: boolean; signal?: AbortSignal } = {},
    ) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      return (
        await send(
          "GET",
          `/api/v1/conversations${settings.archived ? "/archived" : ""}?${query.join("&")}`,
          { signal: settings.signal },
        )
      ).input as Api.Conversations;
    },

    async searchConversations(settings: {
      query: string;
      before?: string;
      limit?: number;
      signal?: AbortSignal;
    }) {
      const trimmed = settings.query.trim();
      if (!trimmed) throw new ContractError("query", "a non-empty search query");
      if (new TextEncoder().encode(trimmed).length > 500)
        throw new ContractError("query", "a search query of at most 500 UTF-8 bytes");
      const query = [`query=${encodeURIComponent(trimmed)}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      query.push(`limit=${settings.limit ?? 50}`);
      return (
        await send("GET", `/api/v1/conversations/search?${query.join("&")}`, {
          signal: settings.signal,
        })
      ).input as Api.Conversations;
    },

    async editConversation(root: string, edit: ConversationEdit, signal?: AbortSignal) {
      jobIdentity(root);
      const answer = await send("POST", `/api/v1/conversations/${root}`, {
        body: edit,
        sign: true,
        signal,
      });
      const result = (answer.input as Api.ConversationEdited).conversation;
      conversationListeners.forEach((listener) => listener());
      return result;
    },

    async readContextStatus(
      conversation: string,
      settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
    ) {
      const suffix = `?limit=${settings.limit ?? 50}${settings.before ? `&before=${jobIdentity(settings.before)}` : ""}`;
      return projectContextStatus(
        (
          await send("GET", `/api/v1/conversations/${jobIdentity(conversation)}/context${suffix}`, {
            signal: settings.signal,
          })
        ).input as Api.ContextStatus,
      );
    },
    async compactConversation(conversation: string, model: string, key: string) {
      return (
        await send("POST", `/api/v1/conversations/${jobIdentity(conversation)}/compact`, {
          body: { model: identity(model, "model") },
          key: submissionKey(key),
          sign: true,
        })
      ).input as Api.Compaction;
    },

    async readConversationLimits(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return (
        await send("GET", `/api/v1/conversations/${conversation}/limits`, {
          signal,
        })
      ).input as Api.ConversationLimits;
    },
    async resolveConversation(id: string, signal?: AbortSignal): Promise<string> {
      jobIdentity(id);
      try {
        await send("GET", `/api/v1/conversations/${id}/turns?limit=1`, { signal });
        return id;
      } catch (cause) {
        if (
          !(
            cause instanceof ApiError &&
            cause.status === 404 &&
            cause.code === "conversation_not_found"
          )
        )
          throw cause;
      }
      const record = projectJob(
        (await send("GET", `/api/v1/jobs/${id}`, { signal })).input as Api.JobDetails,
      );
      if (record.kind === "conversation") return record.id;
      if (!record.conversation) throw new Error("This Job is a process turn with no conversation.");
      return record.conversation;
    },
    async readConversationTurns(
      conversation: string,
      before?: string,
      limit = 20,
      signal?: AbortSignal,
    ) {
      jobIdentity(conversation);
      if (before) jobIdentity(before);
      const query = `?limit=${sequence(limit)}${before ? `&before=${before}` : ""}`;
      const answer = await send("GET", `/api/v1/conversations/${conversation}/turns${query}`, {
        signal,
      });
      return projectConversation(answer.input as Api.Turns);
    },

    async readChildren(conversation: string, before?: string, limit = 20, signal?: AbortSignal) {
      jobIdentity(conversation);
      if (before) jobIdentity(before);
      const query = `?limit=${sequence(limit)}${before ? `&before=${before}` : ""}`;
      return (
        await send("GET", `/api/v1/conversations/${conversation}/children${query}`, { signal })
      ).input as Children;
    },
    async readJobTree(
      job: string,
      settings: {
        after?: string;
        limit?: number;
        depth?: number;
        signal?: AbortSignal;
      } = {},
    ) {
      jobIdentity(job);
      const limit = settings.limit ?? 50;
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      const depth = settings.depth ?? 2;
      if (!Number.isSafeInteger(depth) || depth < 1 || depth > TREE_MAX_DEPTH) {
        throw new ContractError("depth", `an integer from 1 to ${TREE_MAX_DEPTH}`);
      }
      const query = [`limit=${limit}`, `depth=${depth}`];
      if (settings.after) query.push(`after=${jobIdentity(settings.after)}`);
      const answer = await send("GET", `/api/v1/jobs/${job}/tree?${query.join("&")}`, {
        signal: settings.signal,
      });
      return answer.input as import("@/components/ocg/contracts").JobTree;
    },
    async readCards(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      return (
        await send("GET", `/api/v1/conversations/${conversation}/cards`, {
          signal,
        })
      ).input as Api.Cards;
    },
    async decideApproval(
      approval: Approval,
      decision: { decision: "approve" | "reject" } | { answer: string },
      signal?: AbortSignal,
    ) {
      jobIdentity(approval[0]);
      sequence(approval[1]);
      sequence(approval[2]);
      const [turn, request, index] = approval;
      const result = (
        await send("POST", `/api/v1/approvals/${turn}/${request}/${index}`, {
          body: decision,
          sign: true,
          signal,
        })
      ).input as Api.ApprovalDecision;
      conversationListeners.forEach((listener) => listener());
      return result;
    },
    async holdConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const result = (
        await send("POST", `/api/v1/conversations/${conversation}/hold`, {
          body: {},
          sign: true,
          signal,
        })
      ).input as Api.Stop;
      conversationListeners.forEach((listener) => listener());
      return result;
    },
    async unholdConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const result = (
        await send("POST", `/api/v1/conversations/${conversation}/unhold`, {
          body: {},
          sign: true,
          signal,
        })
      ).input as Api.Stop;
      conversationListeners.forEach((listener) => listener());
      return result;
    },

    async retryConversation(conversation: string, turn: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      jobIdentity(turn);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/retry`, {
        body: { turn },
        sign: true,
        signal,
      });
      const result = answer.input as Api.Envelope<Api.Append>;
      conversationListeners.forEach((listener) => listener());
      return result;
    },
    async stopConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/stop`, {
        body: {},
        sign: true,
        signal,
      });
      return answer.input as Api.Stop;
    },
    async closeConversation(conversation: string, signal?: AbortSignal) {
      jobIdentity(conversation);
      const answer = await send("POST", `/api/v1/conversations/${conversation}/close`, {
        body: {},
        sign: true,
        signal,
      });
      return answer.input as Api.Stop;
    },
  };
}
