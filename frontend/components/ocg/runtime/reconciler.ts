import type { ChatMessage, OcgRuntimeEvent } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";
import { boundActivities, boundTimeline } from "./observability";

export type RuntimeDiagnosticCode =
  | "unknown-session"
  | "unknown-entity"
  | "command-rejected"
  | "command-failed"
  | "runtime-warning"
  | "runtime-error";

export type RuntimeDiagnostic = {
  code: RuntimeDiagnosticCode;
  severity: "info" | "warning" | "error";
  message: string;
  sessionId?: string;
};

export type RuntimeState = {
  snapshot: RuntimeSnapshot;
  diagnostics: RuntimeDiagnostic[];
};

type ApplyResult = { snapshot: RuntimeSnapshot; diagnostics: RuntimeDiagnostic[] };

const MAX_DIAGNOSTICS = 50;

export function reconcileRuntimeEvent(state: RuntimeState, event: OcgRuntimeEvent): RuntimeState {
  const applied = applyRuntimeEventToSnapshot(state.snapshot, event);
  return {
    snapshot: applied.snapshot,
    diagnostics: [...state.diagnostics, ...applied.diagnostics].slice(-MAX_DIAGNOSTICS),
  };
}

function diag(
  code: RuntimeDiagnosticCode,
  message: string,
  extra: Partial<RuntimeDiagnostic> = {},
  severity: RuntimeDiagnostic["severity"] = "warning",
): RuntimeDiagnostic {
  return { code, severity, message, ...extra };
}

function sessionExists(
  snapshot: RuntimeSnapshot,
  sessionId: string | undefined,
): sessionId is string {
  return sessionId !== undefined && snapshot.sessions.some((session) => session.id === sessionId);
}

function setMessages(
  snapshot: RuntimeSnapshot,
  sessionId: string,
  messages: ChatMessage[],
): RuntimeSnapshot {
  return {
    ...snapshot,
    messagesBySession: { ...snapshot.messagesBySession, [sessionId]: messages },
  };
}

function applyRuntimeEventToSnapshot(
  snapshot: RuntimeSnapshot,
  event: OcgRuntimeEvent,
): ApplyResult {
  const sessionId =
    "sessionId" in event && typeof event.sessionId === "string" ? event.sessionId : undefined;
  switch (event.type) {
    case "conversation.session-deleted": {
      if (!sessionExists(snapshot, sessionId)) return { snapshot, diagnostics: [] };
      const withoutSession = <T>(entries: Record<string, T>) =>
        Object.fromEntries(Object.entries(entries).filter(([id]) => id !== sessionId));
      return {
        snapshot: {
          ...snapshot,
          sessions: snapshot.sessions.filter((session) => session.id !== sessionId),
          messagesBySession: withoutSession(snapshot.messagesBySession),
          observabilityBySession: withoutSession(snapshot.observabilityBySession),
          executionBySession: withoutSession(snapshot.executionBySession),
          accountingBySession: withoutSession(snapshot.accountingBySession),
          ...(snapshot.chatQueues ? { chatQueues: withoutSession(snapshot.chatQueues) } : {}),
        },
        diagnostics: [],
      };
    }
    case "conversation.queue-updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [diag("unknown-session", "Queue for unknown session.", { sessionId })],
        };
      }
      return {
        snapshot: {
          ...snapshot,
          chatQueues: {
            ...snapshot.chatQueues,
            [sessionId]: { queue: event.queue, paused: event.paused },
          },
        },
        diagnostics: [],
      };
    }
    case "conversation.history-loaded": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [diag("unknown-session", "History for unknown session.", { sessionId })],
        };
      }
      const current = snapshot.messagesBySession[sessionId] ?? [];
      const incoming = event.messages;
      const matches = (left: ChatMessage, right: ChatMessage) =>
        left.id === right.id ||
        ((left.optimistic || right.optimistic) &&
          left.commandId !== undefined &&
          left.commandId === right.commandId &&
          left.role === right.role);
      const messages = incoming.map((message) => {
        const live = current.find((item) => matches(item, message));
        // A read taken during admission must not reset a live replay cursor or
        // downgrade a terminal optimistic message while settlement catches up.
        return live && message.status === "pending" ? live : message;
      });
      messages.push(
        ...current.filter(
          (message) =>
            message.optimistic &&
            message.commandId !== undefined &&
            !incoming.some((item) => matches(item, message)),
        ),
      );
      return { snapshot: setMessages(snapshot, sessionId, messages), diagnostics: [] };
    }
    case "runtime.status-changed":
      return { snapshot: { ...snapshot, status: event.status }, diagnostics: [] };

    case "conversation.session-created": {
      const session = event.session;
      const exists = snapshot.sessions.some((item) => item.id === session.id);
      if (exists) {
        return {
          snapshot: {
            ...snapshot,
            sessions: snapshot.sessions.map((item) => (item.id === session.id ? session : item)),
          },
          diagnostics: [],
        };
      }
      return {
        snapshot: {
          ...snapshot,
          sessions: [session, ...snapshot.sessions],
          messagesBySession: {
            ...snapshot.messagesBySession,
            [session.id]: snapshot.messagesBySession[session.id] ?? [],
          },
          observabilityBySession: {
            ...snapshot.observabilityBySession,
            [session.id]: snapshot.observabilityBySession[session.id] ?? null,
          },
          executionBySession: {
            ...snapshot.executionBySession,
            [session.id]: snapshot.executionBySession[session.id] ?? null,
          },
          accountingBySession: {
            ...snapshot.accountingBySession,
            [session.id]: snapshot.accountingBySession[session.id] ?? null,
          },
        },
        diagnostics: [],
      };
    }

    case "conversation.session-updated": {
      const session = event.session;
      if (!snapshot.sessions.some((item) => item.id === session.id)) {
        return {
          snapshot,
          diagnostics: [
            diag("unknown-session", `Update for unknown session "${session.id}".`, {
              sessionId: session.id,
            }),
          ],
        };
      }
      return {
        snapshot: {
          ...snapshot,
          sessions: snapshot.sessions.map((item) => (item.id === session.id ? session : item)),
        },
        diagnostics: [],
      };
    }

    case "conversation.message-started": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag("unknown-session", `Message started for unknown session "${sessionId ?? ""}".`, {
              sessionId,
            }),
          ],
        };
      }
      const message = event.message;
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const next = messages.some((item) => item.id === message.id)
        ? messages.map((item) => (item.id === message.id ? message : item))
        : [...messages, message];
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "conversation.message-image": {
      if (!sessionExists(snapshot, sessionId))
        return {
          snapshot,
          diagnostics: [diag("unknown-session", "Image for unknown session.", { sessionId })],
        };
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const current = messages.find((message) => message.id === event.messageId);
      if (
        !current ||
        current.role !== "assistant" ||
        (current.status !== "streaming" && current.status !== "pending")
      )
        return { snapshot, diagnostics: [] };
      const image = event.image;
      if (current.images?.some((existing) => existing.id === image.id))
        return { snapshot, diagnostics: [] };
      return {
        snapshot: setMessages(
          snapshot,
          sessionId,
          messages.map((message) =>
            message.id === current.id
              ? { ...message, images: [...(message.images ?? []), image] }
              : message,
          ),
        ),
        diagnostics: [],
      };
    }

    case "conversation.message-delta": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag("unknown-session", `Message delta for unknown session "${sessionId ?? ""}".`, {
              sessionId,
            }),
          ],
        };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === event.messageId);
      if (index < 0) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Delta for unknown message "${event.messageId}".`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const current = messages[index];
      if (
        current.status === "cancelled" ||
        current.status === "completed" ||
        current.status === "failed"
      ) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Delta ignored for terminal message "${current.id}" (${current.status}).`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const next = [...messages];
      next[index] = { ...current, content: current.content + event.delta };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "conversation.message-reasoning-delta": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag("unknown-session", `Reasoning delta for unknown session "${sessionId ?? ""}".`, {
              sessionId,
            }),
          ],
        };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === event.messageId);
      if (index < 0) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Reasoning delta for unknown message "${event.messageId}".`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const current = messages[index];
      if (
        current.status === "cancelled" ||
        current.status === "completed" ||
        current.status === "failed"
      ) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Reasoning delta ignored for terminal message "${current.id}" (${current.status}).`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const next = [...messages];
      next[index] = { ...current, reasoning: (current.reasoning ?? "") + event.delta };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "conversation.message-round-committed": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag("unknown-session", `Round boundary for unknown session "${sessionId ?? ""}".`, {
              sessionId,
            }),
          ],
        };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === event.messageId);
      if (index < 0) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Round boundary for unknown message "${event.messageId}".`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const current = messages[index];
      if (
        current.status === "completed" ||
        current.status === "failed" ||
        current.status === "cancelled"
      ) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Round boundary ignored for terminal message "${current.id}" (${current.status}).`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      // Only this round's provisional tail is dropped. The message identity, the
      // rounds already committed ahead of the boundary, and the turn itself all
      // survive, so a replacement attempt continues the same assistant message.
      const content = current.content.slice(0, Math.max(0, event.committedContentLength));
      const images = (current.images ?? []).slice(0, Math.max(0, event.committedImageCount));
      const reasoning = (current.reasoning ?? "").slice(
        0,
        Math.max(0, event.committedReasoningLength),
      );
      if (
        content === current.content &&
        images.length === (current.images?.length ?? 0) &&
        reasoning === (current.reasoning ?? "")
      ) {
        return { snapshot, diagnostics: [] };
      }
      const next = [...messages];
      next[index] = {
        ...current,
        content,
        ...(current.images ? { images } : {}),
        ...(current.reasoning !== undefined ? { reasoning } : {}),
      };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "conversation.message-completed": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-session",
              `Message completion for unknown session "${sessionId ?? ""}".`,
              { sessionId },
            ),
          ],
        };
      }
      const message = event.message;
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const exists = messages.some((item) => item.id === message.id);
      const next = exists
        ? messages.map((item) => (item.id === message.id ? message : item))
        : [...messages, message];
      return {
        snapshot: setMessages(snapshot, sessionId, next),
        diagnostics: exists
          ? []
          : [
              diag(
                "unknown-entity",
                `Completion for message "${message.id}" with no prior partial; inserted authoritatively.`,
                { sessionId },
                "info",
              ),
            ],
      };
    }

    case "cancelled": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag("unknown-session", `Cancellation for unknown session "${sessionId ?? ""}".`, {
              sessionId,
            }),
          ],
        };
      }
      if (event.messageId === undefined) {
        return { snapshot, diagnostics: [] };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === event.messageId);
      if (index < 0) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Cancellation for unknown message "${event.messageId}".`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const current = messages[index];
      if (
        current.status === "completed" ||
        current.status === "failed" ||
        current.status === "cancelled"
      ) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-entity",
              `Cancellation ignored; message "${current.id}" is already ${current.status}.`,
              { sessionId },
              "info",
            ),
          ],
        };
      }
      const next = [...messages];
      next[index] = { ...current, status: "cancelled" };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "observability.updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return {
          snapshot,
          diagnostics: [
            diag(
              "unknown-session",
              `Observability update for unknown session "${sessionId ?? ""}".`,
              { sessionId },
            ),
          ],
        };
      }
      const observability = event.observability;
      return {
        snapshot: {
          ...snapshot,
          observabilityBySession: {
            ...snapshot.observabilityBySession,
            [sessionId]: {
              ...observability,
              timeline: boundTimeline(observability.timeline),
              activities: boundActivities(observability.activities),
            },
          },
        },
        diagnostics: [],
      };
    }

    case "job.execution-updated": {
      if (!sessionExists(snapshot, sessionId)) return { snapshot, diagnostics: [] };
      return {
        snapshot: {
          ...snapshot,
          executionBySession: {
            ...snapshot.executionBySession,
            [sessionId]: event.execution,
          },
          accountingBySession: {
            ...snapshot.accountingBySession,
            [sessionId]: event.accounting,
          },
        },
        diagnostics: [],
      };
    }

    case "job.launch-updated": {
      const result = event.result;
      if (result.outcome === "accepted") return { snapshot, diagnostics: [] };
      const code: RuntimeDiagnosticCode =
        result.outcome === "rejected" ? "command-rejected" : "command-failed";
      return {
        snapshot,
        diagnostics: [diag(code, result.message, { sessionId }, "warning")],
      };
    }

    case "warning":
      return {
        snapshot,
        diagnostics: [diag("runtime-warning", event.message, { sessionId }, "warning")],
      };

    case "error":
      return {
        snapshot,
        diagnostics: [diag("runtime-error", event.message, { sessionId }, "error")],
      };
  }
}
