import { createEffect, createMemo, createSignal, For, Show } from "solid-js";
import { AttachmentTile, AttachmentTileRemove } from "@/components/ocg/chat/ui/attachment";
import {
  Composer,
  ComposerActions,
  ComposerAttachButton,
  ComposerAttachments,
  ComposerBar,
  ComposerSend,
  ComposerToolbar,
} from "@/components/ocg/chat/ui/composer";
import { Button } from "@/components/ui/button";
import { bindElementRef, elementRef } from "@/lib/ui-state";
import type { ChatImage, ChatModelSelection } from "../contracts";
import { useI18n } from "../i18n";
import { ApiError, type MutableJob, type MutableJobInput } from "../runtime/api-client";
import { useOcgRuntime } from "../runtime/runtime-context";
import { type ModelImageSupport, ModelSelector } from "./model-selector";

export const MUTABLE_CONTEXT_KEY = "ocg.mutable-composer.context.v1";
const QUEUED_KEY = "ocg.mutable-composer.queued.v1";
const UNFINISHED_KEY = "ocg.mutable-composer.unfinished.v1";
const PENDING_KEY = "ocg.mutable-composer.pending-submission.v1";
const AUTOSAVE_DELAY_MS = 1500;
const IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
const contexts = new Map<string, string>();
let navigationGuard: (() => Promise<boolean>) | null = null;
export async function confirmMutableComposerNavigation(): Promise<boolean> {
  return navigationGuard ? navigationGuard() : true;
}
const pendingKey = (context: string) => `${PENDING_KEY}:${context}`;
type Pointer = { context: string; job: string };
const empty = (): MutableJobInput => ({
  text: "",
  attachments: [],
  configuration: null,
  priority: "foreground",
});
const same = (a: MutableJobInput, b: MutableJobInput) =>
  a.text === b.text &&
  a.priority === b.priority &&
  a.attachments.length === b.attachments.length &&
  a.attachments.every((id, index) => id === b.attachments[index]) &&
  (a.configuration === null
    ? b.configuration === null
    : b.configuration !== null &&
      a.configuration.llm_profile === b.configuration.llm_profile &&
      a.configuration.agent_profile === b.configuration.agent_profile &&
      a.configuration.reasoning_effort === b.configuration.reasoning_effort);

function uuid7(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let time = Date.now();
  for (let i = 5; i >= 0; i--) {
    bytes[i] = time % 256;
    time = Math.floor(time / 256);
  }
  bytes[6] = (bytes[6] & 15) | 0x70;
  bytes[8] = (bytes[8] & 63) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
async function inputFingerprint(input: MutableJobInput): Promise<string> {
  const fields = [
    input.text,
    input.attachments,
    input.priority,
    input.configuration && [
      input.configuration.llm_profile,
      input.configuration.agent_profile,
      input.configuration.reasoning_effort,
    ],
  ];
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(fields)),
  );
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
const conversationContextKey = (conversation: string) =>
  `${MUTABLE_CONTEXT_KEY}:conversation:${conversation}`;
export function bindComposerContext(
  sessionId: string,
  context: string,
  conversation?: string,
): void {
  contexts.set(sessionId, context);
  const key = conversation ? conversationContextKey(conversation) : MUTABLE_CONTEXT_KEY;
  sessionStorage.setItem(key, context);
  localStorage.setItem(key, context);
}
export function contextForSession(sessionId: string, conversation?: string): string {
  const key = conversation ? conversationContextKey(conversation) : null;
  const context =
    contexts.get(sessionId) ??
    (key ? (sessionStorage.getItem(key) ?? localStorage.getItem(key)) : null) ??
    uuid7();
  bindComposerContext(sessionId, context, conversation);
  return context;
}
function rotateContext(sessionId: string, conversation?: string): string {
  const context = uuid7();
  bindComposerContext(sessionId, context, conversation);
  return context;
}
function clearGlobalContext(context: string) {
  if (localStorage.getItem(MUTABLE_CONTEXT_KEY) === context)
    localStorage.removeItem(MUTABLE_CONTEXT_KEY);
  if (sessionStorage.getItem(MUTABLE_CONTEXT_KEY) === context)
    sessionStorage.removeItem(MUTABLE_CONTEXT_KEY);
}
function queuedPointers(): Pointer[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(QUEUED_KEY) ?? "[]");
    if (!Array.isArray(value)) return [];
    return value.filter(
      (entry): entry is Pointer =>
        typeof entry?.context === "string" && typeof entry?.job === "string",
    );
  } catch {
    return [];
  }
}
function savePointer(pointer: Pointer) {
  const list = queuedPointers().filter((entry) => entry.job !== pointer.job);
  localStorage.setItem(QUEUED_KEY, JSON.stringify([...list, pointer]));
}
function forgetQueued(job: string) {
  localStorage.setItem(
    QUEUED_KEY,
    JSON.stringify(queuedPointers().filter((entry) => entry.job !== job)),
  );
}
function unfinishedContexts(): string[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(UNFINISHED_KEY) ?? "[]");
    return Array.isArray(value)
      ? value.filter((entry): entry is string => typeof entry === "string")
      : [];
  } catch {
    return [];
  }
}
function rememberUnfinished(context: string) {
  localStorage.setItem(
    UNFINISHED_KEY,
    JSON.stringify([...new Set([...unfinishedContexts(), context])]),
  );
}
function forgetUnfinished(context: string) {
  localStorage.setItem(
    UNFINISHED_KEY,
    JSON.stringify(unfinishedContexts().filter((item) => item !== context)),
  );
}
function config(selection?: ChatModelSelection): MutableJobInput["configuration"] {
  if (!selection?.llmProfile) return null;
  return {
    llm_profile: selection.llmProfile,
    agent_profile: selection.agentProfile ?? null,
    reasoning_effort: selection.effort,
  };
}
function fileData(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () =>
      typeof reader.result === "string"
        ? resolve(reader.result)
        : reject(new Error("Unable to read image."));
    reader.onerror = () => reject(reader.error ?? new Error("Unable to read image."));
    reader.readAsDataURL(file);
  });
}

type Props = {
  sessionId: string;
  context: string;
  conversation?: string;
  project?: string;
  selection?: ChatModelSelection;
  onSelectionChange: (value: ChatModelSelection | undefined) => void;
  onPreference: (value: ChatModelSelection) => void;
};
type Work = {
  context: string;
  job: MutableJob | null;
  desired: MutableJobInput;
  initial: MutableJobInput | null;
  association: { conversation?: string; project?: string } | null;
  dirty: boolean;
  conflict: boolean;
  promise: Promise<void> | null;
};
type PendingImage = { file: File; url: string; failed: boolean };

function isEmptyDraft(
  job: MutableJob | null,
  input: MutableJobInput,
  images: readonly PendingImage[],
) {
  return (
    job?.phase === "unsubmitted" &&
    input.text === "" &&
    input.attachments.length === 0 &&
    images.length === 0
  );
}

function hasDraftContent(input: MutableJobInput) {
  return input.text.trim().length > 0 || input.attachments.length > 0;
}

export function MutableComposer(props: Props) {
  const { api, client } = useOcgRuntime();
  const i18n = useI18n();
  const work = elementRef<Work>({
    context: props.context,
    job: null,
    desired: empty(),
    initial: null,
    association: null,
    dirty: false,
    conflict: false,
    promise: null,
  });
  const autosaveTimer = elementRef<ReturnType<typeof setTimeout> | null>(null);
  const draftBeforeQueueEdit = elementRef<{
    work: Work;
    images: ChatImage[];
    selection?: ChatModelSelection;
  } | null>(null);
  const scope = elementRef({
    conversation: props.conversation,
    project: props.selection?.project ?? props.project,
  });
  createEffect(
    () => ({
      conversation: props.conversation,
      project: props.selection?.project ?? props.project,
    }),
    (scopeNow) => {
      scope.current = scopeNow;
    },
  );
  const alive = elementRef(true);
  const [input, setInput] = createSignal<MutableJobInput>(empty);
  const [confirmed, setConfirmed] = createSignal<MutableJob | null>(null);
  const [ready, setReady] = createSignal(false);
  const [status, setStatus] = createSignal("Loading saved Job…");
  const [error, setError] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);
  const busyRef = elementRef(false);
  const [uncertain, setUncertain] = createSignal(false);
  const [conflict, setConflict] = createSignal(false);
  const [uploading, setUploading] = createSignal(0);
  const [dragActive, setDragActive] = createSignal(false);
  const [pendingImages, setPendingImages] = createSignal<PendingImage[]>([]);
  const pendingImagesRef = elementRef<PendingImage[]>([]);
  const uploadTask = elementRef<Promise<void> | null>(null);
  const uncertainRef = elementRef(false);
  createEffect(uncertain, (value) => {
    uncertainRef.current = value;
  });
  const [images, setImages] = createSignal<ChatImage[]>([]);
  const [queued, setQueued] = createSignal<MutableJob[]>([]);
  const [unfinished, setUnfinished] = createSignal<MutableJob[]>([]);
  const [editingQueue, setEditingQueue] = createSignal(false);
  const inputRef = elementRef<HTMLInputElement>(null);
  const modelTriggerRef = elementRef<HTMLButtonElement>(null);
  const explicitSelection = elementRef(false);
  const [support, setSupport] = createSignal<ModelImageSupport | null>(null);
  const imageSupport = (value: ModelImageSupport) => setSupport(value);
  const [queuedLoading, setQueuedLoading] = createSignal(false);
  const inputDisabled = createMemo(() => !ready() || busy() || conflict() || uncertain());
  const canAttach = createMemo(
    () => !inputDisabled() && uploading() === 0 && Boolean(support()?.supported),
  );
  const sendDisabled = createMemo(
    () =>
      inputDisabled() ||
      pendingImages().length > 0 ||
      (!editingQueue() && !input().text.trim() && !input().attachments.length),
  );
  const [shortcutIndex, setShortcutIndex] = createSignal(0);
  const [shortcutDismissed, setShortcutDismissed] = createSignal("");
  const shortcuts = createMemo(() => {
    const match = /(?:^|\s)\/([^\s/]*)$/.exec(input().text);
    if (!match || input().text === shortcutDismissed()) return [];
    const query = match[1].toLowerCase();
    const available = [
      {
        key: "attach",
        label: i18n.t("chat.composer.attachImage"),
        enabled: canAttach(),
        action: () => inputRef.current?.click(),
      },
      {
        key: "model",
        label: i18n.t("chat.executionSettings"),
        enabled: !inputDisabled() && !!props.selection?.llmProfile,
        action: () => modelTriggerRef.current?.click(),
      },
    ];
    return available.filter(
      ({ key, label, enabled }) =>
        enabled && (key.startsWith(query) || label.toLowerCase().includes(query)),
    );
  });
  function runShortcut(shortcut: ReturnType<typeof shortcuts>[number]) {
    changeText(input().text.replace(/(?:^|\s)\/[^\s/]*$/, ""));
    shortcut.action();
  }

  createEffect(
    () => [],
    () => {
      const beforeUnload = (event: BeforeUnloadEvent) => {
        if (
          !work.current.dirty &&
          !work.current.promise &&
          !pendingImagesRef.current.length &&
          !uncertainRef.current &&
          !busyRef.current
        )
          return;
        event.preventDefault();
        event.returnValue = "";
      };
      window.addEventListener("beforeunload", beforeUnload);
      return () => window.removeEventListener("beforeunload", beforeUnload);
    },
  );

  const loadImages = async (ids: string[]) => {
    if (!api) return [];
    return Promise.all(
      ids.map(async (id) => {
        const item = await api.readAttachment(id);
        if (!item.available) throw new Error(`Attachment ${id} is unavailable.`);
        return {
          id,
          name: item.display_name ?? "Image",
          media_type: item.mime,
          url: api.attachmentContentUrl(id),
        };
      }),
    );
  };
  const [queueError, setQueueError] = createSignal<string>();
  let queueRead = 0;
  const refreshQueue = async () => {
    if (!api) return;
    const version = ++queueRead;
    // Each submit rotates the draft context. Earlier queued Jobs still belong to
    // this composer when their turn is present in its conversation history.
    const messages = client.getSnapshot().messagesBySession[props.sessionId] ?? [];
    const conversationJobs = new Set(messages.map((message) => message.jobId));
    const pointers = queuedPointers().filter(
      ({ context, job }) => context === props.context || conversationJobs.has(job),
    );
    const results = await Promise.allSettled(
      pointers.map(({ context, job }) => api.readMutableJob(context, job)),
    );
    if (!alive.current || version !== queueRead) return;
    const rows: MutableJob[] = [];
    let failed = false;
    for (const [index, result] of results.entries()) {
      if (result.status === "fulfilled") {
        if (result.value.phase !== "queued") forgetQueued(pointers[index].job);
        else rows.push(result.value);
      } else if (result.reason instanceof ApiError && result.reason.status === 404) {
        forgetQueued(pointers[index].job);
      } else {
        failed = true;
      }
    }
    setQueued(rows);
    setQueueError(
      failed ? "Some queued Jobs could not be loaded. Retry to see their latest state." : undefined,
    );
  };
  const hasQueued = createMemo(() => queued().length > 0);
  createEffect(hasQueued, (hasQueued) => {
    if (!api || !hasQueued) return;
    let reading = false;
    let pending = false;
    let active = true;
    const refresh = async () => {
      if (reading) {
        pending = true;
        return;
      }
      reading = true;
      try {
        do {
          pending = false;
          await refreshQueue();
        } while (active && pending);
      } finally {
        reading = false;
      }
    };
    const unsubscribe = api.subscribeCatalog(
      () => void refresh(),
      (cause) => {
        if (active) setQueueError(cause instanceof Error ? cause.message : String(cause));
      },
    );
    return () => {
      active = false;
      unsubscribe();
    };
  });
  const refreshUnfinished = async () => {
    if (!api) return;
    const results = await Promise.allSettled(
      unfinishedContexts().map((item) => api.currentMutableJob(item)),
    );
    if (!alive.current) return;
    setUnfinished(
      results.flatMap((result) =>
        result.status === "fulfilled" &&
        result.value &&
        (scope.current.conversation
          ? result.value.conversation === scope.current.conversation
          : result.value.context === work.current.context)
          ? [result.value]
          : [],
      ),
    );
    if (results.some((result) => result.status === "rejected"))
      setError("Some unfinished Jobs could not be loaded. Retry to see their latest state.");
  };
  createEffect(
    () => [api, props.context, loadImages, refreshQueue, refreshUnfinished],
    () => {
      alive.current = true;
      if (!api) return;
      let cancelled = false;
      void (async () => {
        try {
          const pendingText = localStorage.getItem(pendingKey(props.context));
          const pending: {
            context: string;
            job: string;
            revision: number;
            fingerprint: string;
          } | null = pendingText ? JSON.parse(pendingText) : null;
          let job = await api.currentMutableJob(props.context);
          if (pending?.context === props.context) {
            const submitted = await api.readMutableJob(props.context, pending.job);
            if (
              (submitted.phase === "queued" ||
                submitted.phase === "executing" ||
                submitted.phase === "completed") &&
              (await inputFingerprint(submitted.input)) === pending.fingerprint
            ) {
              savePointer({
                context: props.context,
                job: submitted.id,
              });
              bindComposerContext(props.sessionId, props.context, submitted.conversation);
              if (!client.presentMutableJob)
                throw new Error("The submitted Job cannot yet be opened in history.");
              await client.presentMutableJob(props.sessionId, submitted);
              forgetUnfinished(props.context);
              localStorage.removeItem(pendingKey(props.context));
              clearGlobalContext(props.context);
              work.current.context = rotateContext(props.sessionId, submitted.conversation);
              job = null;
            } else if (
              submitted.phase === "unsubmitted" &&
              submitted.revision === pending.revision &&
              (await inputFingerprint(submitted.input)) === pending.fingerprint
            ) {
              job = submitted;
              setUncertain(true);
            } else {
              job = submitted;
              work.current.conflict = true;
              setConflict(true);
              setError(
                "The pending submission no longer matches the saved revision. Reconcile it before editing.",
              );
            }
          }
          if (cancelled) return;
          if (job) {
            if (scope.current.conversation && job.conversation !== scope.current.conversation)
              throw new Error(
                "The saved Job belongs to another conversation. Open that conversation to continue editing it.",
              );
            if (job.phase === "unsubmitted") rememberUnfinished(props.context);
            const restored = await loadImages(job.input.attachments);
            if (cancelled) return;
            work.current.job = job;
            work.current.desired = job.input;
            setInput(job.input);
            setConfirmed(job);
            setImages(restored);
            if (job.input.configuration)
              props.onSelectionChange({
                model: "",
                effort: job.input.configuration.reasoning_effort,
                llmProfile: job.input.configuration.llm_profile,
                agentProfile: job.input.configuration.agent_profile ?? undefined,
                project: job.project ?? undefined,
              });
          }
          setReady(true);
          setStatus(job ? "Saved" : "New Job");
          await refreshQueue();
          await refreshUnfinished();
        } catch (cause) {
          if (!cancelled) setError(cause instanceof Error ? cause.message : String(cause));
        }
      })();
      return () => {
        cancelled = true;
        alive.current = false;
      };
    },
  );

  const cancelAutosave = () => {
    if (autosaveTimer.current === null) return;
    clearTimeout(autosaveTimer.current);
    autosaveTimer.current = null;
  };
  createEffect(
    () => [cancelAutosave],
    () => cancelAutosave,
  );

  const flush = async function flushWork(autosave = false): Promise<void> {
    if (!autosave) cancelAutosave();
    else if (!alive.current || autosaveTimer.current !== null) return;
    const state = work.current;
    if (state.promise) {
      await state.promise;
      if (work.current === state && state.dirty) await flushWork(autosave);
      return;
    }
    if (!state.dirty || state.conflict) return;
    const run = async () => {
      while (state.dirty && !state.conflict) {
        if (alive.current) {
          setStatus("Saving…");
          setError(null);
        }
        try {
          if (!state.job) {
            state.initial ??= structuredClone(state.desired);
            state.association ??= scope.current.conversation
              ? { conversation: scope.current.conversation }
              : scope.current.project
                ? { project: scope.current.project }
                : {};
            try {
              state.job = await api.createMutableJob(
                state.context,
                state.initial,
                `create-${state.context}`,
                state.association,
              );
            } catch (cause) {
              const existing = await api.currentMutableJob(state.context).catch(() => null);
              if (!existing) throw cause;
              state.job = existing;
            }
            if (
              state.job.revision !== 1 ||
              !same(state.job.input, state.initial) ||
              (state.association.conversation &&
                state.job.conversation !== state.association.conversation) ||
              (!state.association.conversation &&
                state.job.project !== (state.association.project ?? null))
            ) {
              throw new ApiError("mutable_job_conflict", 409, false);
            }
            rememberUnfinished(state.context);
          } else if (isEmptyDraft(state.job, state.desired, pendingImagesRef.current)) {
            const id = state.job.id;
            try {
              await api.discardMutableJob(state.context, id, state.job.revision);
            } catch (cause) {
              if (
                cause instanceof ApiError &&
                cause.status >= 400 &&
                cause.status < 500 &&
                cause.status !== 404
              ) {
                throw cause;
              }
              const current = await api.currentMutableJob(state.context).catch(() => undefined);
              if (current !== null) throw cause;
            }
            forgetUnfinished(state.context);
            state.context = rotateContext(props.sessionId, scope.current.conversation);
            state.job = null;
            state.initial = null;
            state.association = null;
            if (alive.current) setUnfinished((jobs) => jobs.filter((job) => job.id !== id));
          } else if (!same(state.job.input, state.desired)) {
            state.job = await api.updateMutableJob(
              state.context,
              state.job.id,
              state.job.revision,
              structuredClone(state.desired),
            );
          }
          state.dirty = state.job
            ? !same(state.job.input, state.desired) ||
              isEmptyDraft(state.job, state.desired, pendingImagesRef.current)
            : hasDraftContent(state.desired);
          if (alive.current) {
            setConfirmed(state.job);
            setStatus(
              state.dirty
                ? autosave
                  ? "Waiting to save…"
                  : "Saving…"
                : state.job
                  ? "Saved"
                  : "New Job",
            );
          }
          if (autosave) break;
        } catch (cause) {
          if (
            cause instanceof ApiError &&
            cause.status === 409 &&
            cause.code !== "unsupported_execution_context"
          ) {
            state.conflict = true;
            if (alive.current) setConflict(true);
            if (state.job) {
              const latest = await api
                .readMutableJob(state.context, state.job.id)
                .catch(() => null);
              if (latest && alive.current) setConfirmed(latest);
            }
            if (alive.current)
              setError(
                "The saved Job changed elsewhere. Your local edits are not saved. Load the saved version before continuing.",
              );
          } else if (alive.current)
            setError(cause instanceof Error ? cause.message : String(cause));
          if (alive.current) setStatus("Local changes not saved");
          throw cause;
        }
      }
    };
    state.promise = run().finally(() => {
      state.promise = null;
    });
    await state.promise;
  };

  const scheduleAutosave = () => {
    cancelAutosave();
    const state = work.current;
    if (state.conflict || uncertainRef.current) return;
    if (isEmptyDraft(state.job, state.desired, pendingImagesRef.current)) state.dirty = true;
    if (!state.dirty) {
      setStatus(state.job ? "Saved" : "New Job");
      return;
    }
    setStatus("Waiting to save…");
    autosaveTimer.current = setTimeout(() => {
      autosaveTimer.current = null;
      void flush(true).catch(() => {});
    }, AUTOSAVE_DELAY_MS);
  };
  createEffect(
    () => [ready(), scheduleAutosave],
    () => {
      const state = work.current;
      if (ready() && isEmptyDraft(state.job, state.desired, pendingImagesRef.current))
        scheduleAutosave();
    },
  );
  createEffect(
    () => [flush],
    () => {
      const guard = async () => {
        const timeout = () => new Promise<void>((resolve) => setTimeout(resolve, 2000));
        if (uploadTask.current) await Promise.race([uploadTask.current.catch(() => {}), timeout()]);
        if (
          !busyRef.current &&
          !work.current.conflict &&
          (work.current.dirty || work.current.promise)
        ) {
          await Promise.race([flush().catch(() => {}), timeout()]);
        }
        if (
          !work.current.dirty &&
          !work.current.promise &&
          !pendingImagesRef.current.length &&
          !uncertainRef.current &&
          !busyRef.current
        )
          return true;
        return window.confirm(
          "Some input or images are not confirmed saved, or submission is still unresolved. Leave anyway? Your unsaved changes may be lost.",
        );
      };
      navigationGuard = guard;
      return () => {
        if (navigationGuard === guard) navigationGuard = null;
      };
    },
  );
  createEffect(
    () => [],
    () => () => {
      for (const pending of pendingImagesRef.current) URL.revokeObjectURL(pending.url);
    },
  );
  function change(next: MutableJobInput) {
    if (busyRef.current || uncertain()) return;
    const state = work.current;
    if (same(state.desired, next)) return;
    state.desired = next;
    state.dirty = state.job
      ? state.promise !== null || !same(state.job.input, next)
      : state.initial !== null || hasDraftContent(next);
    setInput(next);
    scheduleAutosave();
  }
  function changeText(text: string) {
    change({
      ...work.current.desired,
      text,
      configuration: work.current.desired.configuration ?? config(props.selection),
    });
  }
  function setPending(next: PendingImage[]) {
    pendingImagesRef.current = next;
    if (!alive.current) return;
    setPendingImages(next);
    const state = work.current;
    if (isEmptyDraft(state.job, state.desired, next)) scheduleAutosave();
  }
  async function add(pending: PendingImage[]) {
    if (!client.uploadChatImage) {
      setPending(
        pendingImagesRef.current.map((item) =>
          pending.includes(item) ? { ...item, failed: true } : item,
        ),
      );
      setError("Image upload is unavailable.");
      return;
    }
    setUploading((n) => n + pending.length);
    for (const item of pending) {
      try {
        const image = await client.uploadChatImage({
          project_id: "",
          name: item.file.name,
          data_url: await fileData(item.file),
        });
        setPending(pendingImagesRef.current.filter((entry) => entry !== item));
        URL.revokeObjectURL(item.url);
        if (!alive.current) {
          const state = work.current;
          state.desired = {
            ...state.desired,
            attachments: [...state.desired.attachments, image.id],
            configuration: state.desired.configuration ?? config(props.selection),
          };
          state.dirty = true;
          void flush().catch(() => {});
          continue;
        }
        setImages((old) => [...old, image]);
        change({
          ...work.current.desired,
          attachments: [...work.current.desired.attachments, image.id],
          configuration: work.current.desired.configuration ?? config(props.selection),
        });
      } catch (cause) {
        setPending(
          pendingImagesRef.current.map((entry) =>
            entry === item ? { ...entry, failed: true } : entry,
          ),
        );
        if (alive.current) setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        if (alive.current) setUploading((n) => n - 1);
      }
    }
  }
  function startUploads(items: PendingImage[]) {
    if (!items.length || uploadTask.current) return;
    const task = add(items);
    uploadTask.current = task;
    void task.finally(() => {
      if (uploadTask.current === task) uploadTask.current = null;
    });
  }
  function selectImages(files: readonly File[]) {
    if (!files.length || !canAttach() || uploadTask.current) return;
    if (
      work.current.desired.attachments.length + pendingImagesRef.current.length + files.length >
      8
    ) {
      setError("At most eight images are supported.");
      return;
    }
    const selected = files.map((file) => ({
      file,
      url: URL.createObjectURL(file),
      failed: false,
    }));
    setPending([...pendingImagesRef.current, ...selected]);
    startUploads(selected);
  }
  async function send() {
    if (
      !api ||
      busyRef.current ||
      pendingImagesRef.current.length ||
      work.current.conflict ||
      uncertain()
    )
      return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    try {
      await flush();
      const state = work.current;
      if (!state.job || state.dirty || state.conflict)
        throw new Error("Save the Job before sending.");
      if (editingQueue()) {
        const draft = draftBeforeQueueEdit.current;
        if (!client.presentMutableJob)
          throw new Error("The saved Job cannot yet be opened in history.");
        if (!draft) throw new Error("The original draft could not be restored.");
        await client.presentMutableJob(props.sessionId, state.job);
        work.current = draft.work;
        draftBeforeQueueEdit.current = null;
        setEditingQueue(false);
        setInput(draft.work.desired);
        setConfirmed(draft.work.job);
        setImages(draft.images);
        props.onSelectionChange(draft.selection);
        await refreshQueue();
        setStatus("Saved queued Job");
        return;
      }
      const submittedInput = structuredClone(state.desired);
      const revision = state.job.revision;
      const key = `submit-${state.context}-${state.job.id}`;
      const fingerprint = await inputFingerprint(submittedInput);
      localStorage.setItem(
        pendingKey(state.context),
        JSON.stringify({ context: state.context, job: state.job.id, revision, fingerprint }),
      );
      let result: MutableJob;
      try {
        result = await api.submitMutableJob(
          state.context,
          state.job.id,
          revision,
          submittedInput,
          key,
        );
      } catch (cause) {
        const observed = await api.readMutableJob(state.context, state.job.id).catch(() => null);
        if (!observed || observed.phase === "unsubmitted") {
          if (
            cause instanceof ApiError &&
            cause.status === 409 &&
            cause.code !== "unsupported_execution_context"
          ) {
            state.conflict = true;
            setConflict(true);
            if (observed) setConfirmed(observed);
            localStorage.removeItem(pendingKey(state.context));
            throw new Error(
              "The Job revision or conversation head changed. Your local input is preserved. Review the saved version; the Job will not be moved or rebased automatically.",
            );
          }
          if (cause instanceof ApiError && cause.status >= 400 && cause.status < 500)
            localStorage.removeItem(pendingKey(state.context));
          else setUncertain(true);
          throw cause;
        }
        if (!same(observed.input, submittedInput)) {
          state.conflict = true;
          setConflict(true);
          setConfirmed(observed);
          throw new Error(
            "The submitted Job differs from your local input. Your input has not been cleared.",
          );
        }
        result = observed;
      }
      if (result.phase !== "queued" && result.phase !== "executing" && result.phase !== "completed")
        throw new Error(`Job is ${result.phase}; input has not been cleared.`);
      bindComposerContext(props.sessionId, state.context, result.conversation);
      try {
        if (!client.presentMutableJob) throw new Error("History is unavailable.");
        await client.presentMutableJob(props.sessionId, result);
      } catch (cause) {
        setUncertain(true);
        throw new Error(
          `The Job was submitted, but history could not be opened. Reconcile the same Job. ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      savePointer({ context: state.context, job: result.id });
      forgetUnfinished(state.context);
      localStorage.removeItem(pendingKey(state.context));
      clearGlobalContext(state.context);
      const next = rotateContext(props.sessionId, result.conversation);
      state.context = next;
      state.job = null;
      state.initial = null;
      state.association = null;
      state.dirty = false;
      state.conflict = false;
      state.desired = empty();
      setConflict(false);
      setInput(empty());
      setConfirmed(null);
      setImages([]);
      setStatus(`Job ${result.id} is ${result.phase}.`);
      await refreshQueue();
      await refreshUnfinished();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function reconcileSubmission() {
    if (!api || busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setError(null);
    try {
      const text = localStorage.getItem(pendingKey(work.current.context));
      if (!text) throw new Error("The pending submission metadata is unavailable.");
      const pending = JSON.parse(text) as {
        context: string;
        job: string;
        revision: number;
        fingerprint: string;
      };
      const saved = await api.readMutableJob(pending.context, pending.job);
      if ((await inputFingerprint(saved.input)) !== pending.fingerprint)
        throw new Error(
          "The saved input changed; the pending submission cannot be reconciled automatically.",
        );
      let result = saved;
      if (saved.phase === "unsubmitted") {
        if (saved.revision !== pending.revision)
          throw new Error(
            "The saved revision changed; the pending submission cannot be retried safely.",
          );
        result = await api.submitMutableJob(
          pending.context,
          pending.job,
          pending.revision,
          saved.input,
          `submit-${pending.context}-${pending.job}`,
        );
      }
      if (result.phase !== "queued" && result.phase !== "executing" && result.phase !== "completed")
        throw new Error(`Job is ${result.phase}; input has not been cleared.`);
      if (!client.presentMutableJob)
        throw new Error("The Job was submitted, but history is unavailable. Retry reconciliation.");
      await client.presentMutableJob(props.sessionId, result);
      savePointer({ context: pending.context, job: result.id });
      forgetUnfinished(pending.context);
      if (!same(result.input, work.current.desired))
        throw new Error(
          "The submitted Job differs from your local input. Your input has not been cleared.",
        );
      localStorage.removeItem(pendingKey(pending.context));
      clearGlobalContext(pending.context);
      const state = work.current;
      state.context = rotateContext(props.sessionId, result.conversation);
      state.job = null;
      state.initial = null;
      state.association = null;
      state.dirty = false;
      state.desired = empty();
      setInput(empty());
      setImages([]);
      setConfirmed(null);
      setUncertain(false);
      setStatus(`Job ${result.id} is ${result.phase}.`);
      await refreshQueue();
      await refreshUnfinished();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function editQueued(job: MutableJob) {
    if (
      !api ||
      busyRef.current ||
      uncertain() ||
      work.current.conflict ||
      pendingImagesRef.current.length ||
      editingQueue()
    ) {
      return;
    }
    busyRef.current = true;
    setBusy(true);
    setQueuedLoading(true);
    try {
      await flush();
      const latest = await api.readMutableJob(job.context, job.id);
      if (props.conversation && latest.conversation !== props.conversation)
        throw new Error("This Job belongs to another conversation.");
      if (latest.phase !== "queued")
        throw new Error(`This Job is ${latest.phase} and can no longer be edited.`);
      const restored = await loadImages(latest.input.attachments);
      draftBeforeQueueEdit.current = {
        work: work.current,
        images: images(),
        selection: props.selection,
      };
      work.current = {
        context: latest.context,
        job: latest,
        desired: latest.input,
        initial: null,
        association: null,
        dirty: false,
        conflict: false,
        promise: null,
      };
      setInput(latest.input);
      setConfirmed(latest);
      setImages(restored);
      setEditingQueue(true);
      setStatus("Editing queued Job");
      setError(null);
      if (latest.input.configuration)
        props.onSelectionChange({
          model: "",
          effort: latest.input.configuration.reasoning_effort,
          llmProfile: latest.input.configuration.llm_profile,
          agentProfile: latest.input.configuration.agent_profile ?? undefined,
          project: latest.project ?? undefined,
        });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      setBusy(false);
      setQueuedLoading(false);
    }
  }
  async function withdraw(job: MutableJob) {
    if (!api || busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    try {
      const result = await api.withdrawQueuedJob(job.context, job.id, job.revision);
      if (result.phase !== "cancelled") throw new Error("Withdrawal was not confirmed.");
      forgetQueued(job.id);
      setStatus("Queued message withdrawn.");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      await refreshQueue();
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function cancelUnsupported(job: MutableJob) {
    if (
      !api ||
      busyRef.current ||
      uncertain() ||
      work.current.dirty ||
      work.current.promise ||
      pendingImagesRef.current.length ||
      editingQueue()
    )
      return;
    if (
      !window.confirm(
        "Cancel this queued Job if it cannot run because its history contains a Decision? Its input and history will be preserved.",
      )
    )
      return;
    busyRef.current = true;
    setBusy(true);
    try {
      const result = await api.cancelUnsupportedQueuedJob(job.context, job.id, job.revision);
      if (result.phase !== "cancelled") throw new Error("The Job cancellation was not confirmed.");
      forgetQueued(job.id);
      await refreshQueue();
      if (client.presentMutableJob) await client.presentMutableJob(props.sessionId, result);
      setStatus("Unsupported queued Job cancelled; its history is preserved.");
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function loadSaved() {
    if (!api || !work.current.job || busyRef.current || uploadTask.current) return;
    if (
      work.current.dirty &&
      !window.confirm("Loading the saved version replaces your unsaved text. Continue?")
    )
      return;
    cancelAutosave();
    busyRef.current = true;
    setBusy(true);
    try {
      const job = await api.readMutableJob(work.current.context, work.current.job.id);
      const restored = await loadImages(job.input.attachments);
      const unsavedImages = images().filter(
        (image) =>
          work.current.desired.attachments.includes(image.id) &&
          !job.input.attachments.includes(image.id),
      );
      const desired = {
        ...job.input,
        attachments: [...job.input.attachments, ...unsavedImages.map((image) => image.id)],
      };
      work.current.job = job;
      work.current.desired = desired;
      work.current.conflict = job.phase !== "unsubmitted" && job.phase !== "queued";
      work.current.dirty = unsavedImages.length > 0;
      setConflict(work.current.conflict);
      setInput(desired);
      setImages([...restored, ...unsavedImages]);
      setConfirmed(job);
      setError(
        work.current.conflict
          ? `This Job is ${job.phase} and cannot be edited.`
          : desired.attachments.length > 8
            ? "Remove images to meet the eight-image limit before saving."
            : null,
      );
      setStatus(work.current.dirty ? "Local image changes not saved" : "Saved");
      if (isEmptyDraft(job, desired, pendingImagesRef.current)) scheduleAutosave();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function openUnfinished(job: MutableJob) {
    if (
      !api ||
      busyRef.current ||
      uncertain() ||
      conflict() ||
      pendingImagesRef.current.length ||
      editingQueue()
    )
      return;
    busyRef.current = true;
    setBusy(true);
    try {
      await flush();
      const latest = await api.readMutableJob(job.context, job.id);
      if (props.conversation && latest.conversation !== props.conversation)
        throw new Error("This Job belongs to another conversation.");
      if (latest.phase !== "unsubmitted")
        throw new Error(`This Job is ${latest.phase} and is no longer unsubmitted.`);
      const restored = await loadImages(latest.input.attachments);
      const state = work.current;
      state.context = latest.context;
      state.job = latest;
      state.desired = latest.input;
      state.initial = null;
      state.dirty = false;
      bindComposerContext(props.sessionId, latest.context, props.conversation);
      setInput(latest.input);
      setConfirmed(latest);
      setImages(restored);
      setStatus("Saved");
      if (isEmptyDraft(latest, latest.input, pendingImagesRef.current)) scheduleAutosave();
      setError(null);
      if (latest.input.configuration)
        props.onSelectionChange({
          model: "",
          effort: latest.input.configuration.reasoning_effort,
          llmProfile: latest.input.configuration.llm_profile,
          agentProfile: latest.input.configuration.agent_profile ?? undefined,
          project: latest.project ?? undefined,
        });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  return (
    <Composer class="max-w-none space-y-2">
      {unfinished().some((job) => job.id !== confirmed()?.id) && (
        <div role="group" aria-label="Unsubmitted Jobs" class="space-y-1 text-sm">
          {unfinished()
            .filter((job) => job.id !== confirmed()?.id)
            .map((job) => (
              <div class="flex items-center gap-2">
                <span class="min-w-0 flex-1 truncate">
                  Unsubmitted: {job.input.text || "Image Job"}
                </span>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={
                    busy() || uncertain() || conflict() || uploading() > 0 || editingQueue()
                  }
                  onClick={() => void openUnfinished(job)}
                >
                  Open
                </Button>
              </div>
            ))}
        </div>
      )}
      <Show when={hasQueued()}>
        <div role="group" aria-label="Queued Jobs" class="space-y-1 text-sm">
          <For each={queued()} keyed={(item) => item.id}>
            {(row) => (
              <div class="flex items-center gap-2">
                <span
                  class="min-w-0 flex-1 truncate"
                  title={
                    row().autonomous_loopback_eligibility &&
                    !row().autonomous_loopback_eligibility!.eligible
                      ? `Not eligible for automatic dispatch (${row().autonomous_loopback_eligibility!.code}): ${row().autonomous_loopback_eligibility!.detail}`
                      : undefined
                  }
                >
                  {row().unsupported_decision_history ? "Blocked by Decision history" : row().phase}
                  {row().autonomous_loopback_eligibility &&
                  !row().autonomous_loopback_eligibility!.eligible
                    ? ` (not schedulable: ${row().autonomous_loopback_eligibility!.code})`
                    : ""}
                  : {row().input.text || "Image Job"}
                </span>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={
                    row().phase !== "queued" ||
                    queuedLoading() ||
                    inputDisabled() ||
                    pendingImages().length > 0 ||
                    editingQueue()
                  }
                  onClick={() => void editQueued(row())}
                >
                  Edit
                </Button>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={busy() || queuedLoading() || uncertain() || editingQueue()}
                  onClick={() => void withdraw(row())}
                >
                  Withdraw
                </Button>
                {row().unsupported_decision_history && (
                  <Button
                    type="button"
                    size="xs"
                    variant="outline"
                    disabled={
                      row().phase !== "queued" ||
                      queuedLoading() ||
                      busy() ||
                      uncertain() ||
                      editingQueue()
                    }
                    onClick={() => void cancelUnsupported(row())}
                  >
                    Cancel blocked Job
                  </Button>
                )}
              </div>
            )}
          </For>
        </div>
      </Show>

      <form
        class="aui-composer-root relative"
        onSubmit={(event) => {
          event.preventDefault();
          if (!sendDisabled()) void send();
        }}
        onDragOver={(event) => {
          if (!event.dataTransfer || !event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          event.dataTransfer.dropEffect = canAttach() ? "copy" : "none";
          if (canAttach()) setDragActive(true);
        }}
        onDragLeave={(event) => {
          if (
            event.relatedTarget instanceof Node &&
            event.currentTarget.contains(event.relatedTarget)
          )
            return;
          setDragActive(false);
        }}
        onDrop={(event) => {
          setDragActive(false);
          if (!event.dataTransfer || !event.dataTransfer.files.length) return;
          event.preventDefault();
          selectImages(
            Array.from(event.dataTransfer.files).filter((file) => IMAGE_TYPES.includes(file.type)),
          );
        }}
      >
        <ComposerBar
          dragActive={dragActive() && canAttach()}
          class="border-border shadow-sm focus-within:border-ring/50"
        >
          <ComposerAttachments class="aui-composer-attachments px-2 pt-1 empty:hidden">
            {images().map((image) => (
              <AttachmentTile
                src={image.url}
                name={image.name}
                isComposer
                remove={
                  <AttachmentTileRemove
                    name={image.name}
                    disabled={busy() || uncertain() || conflict()}
                    onClick={() => {
                      setImages((old) => old.filter((item) => item.id !== image.id));
                      change({
                        ...work.current.desired,
                        attachments: work.current.desired.attachments.filter(
                          (id) => id !== image.id,
                        ),
                      });
                    }}
                  />
                }
              />
            ))}
            {pendingImages().map((item) => (
              <AttachmentTile
                src={item.url}
                name={item.file.name}
                isComposer
                isUploading={!item.failed}
                isError={item.failed}
                errorMessage={item.failed ? "Upload failed" : undefined}
                remove={
                  item.failed && (
                    <AttachmentTileRemove
                      name={item.file.name}
                      onClick={() => {
                        setPending(pendingImagesRef.current.filter((entry) => entry !== item));
                        URL.revokeObjectURL(item.url);
                      }}
                    />
                  )
                }
              />
            ))}
          </ComposerAttachments>
          <textarea
            id="mutable-job-input"
            data-slot="composer-input"
            class="aui-composer-input field-sizing-content max-h-48 min-h-11 w-full resize-none overflow-y-auto bg-transparent px-3 py-2 text-base leading-6 caret-primary outline-none placeholder:text-muted-foreground"
            placeholder={i18n.t("aui.composerPlaceholder")}
            aria-label={i18n.t("aui.composerLabel")}
            rows={1}
            value={input().text}
            disabled={inputDisabled()}
            onInput={(event) => {
              setShortcutIndex(0);
              changeText(event.currentTarget.value);
            }}
            onKeyDown={(event) => {
              if (event.isComposing || !shortcuts().length) return;
              if (event.key === "Escape") {
                event.preventDefault();
                setShortcutDismissed(input().text);
              } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault();
                const count = shortcuts().length;
                setShortcutIndex(
                  (index) => (index + (event.key === "ArrowDown" ? 1 : count - 1)) % count,
                );
              } else if (event.key === "Enter" && !event.shiftKey) {
                const shortcut = shortcuts()[shortcutIndex()];
                if (shortcut) {
                  event.preventDefault();
                  runShortcut(shortcut);
                }
              }
            }}
            onPaste={(event) => {
              if (!canAttach()) return;
              const images = Array.from(event.clipboardData?.files ?? []).filter((file) =>
                IMAGE_TYPES.includes(file.type),
              );
              if (!images.length) return;
              event.preventDefault();
              selectImages(images);
            }}
          />
          <input
            ref={bindElementRef(inputRef)}
            type="file"
            accept={IMAGE_TYPES.join(",")}
            multiple
            hidden
            onChange={(event) => {
              selectImages(Array.from(event.target.files ?? []));
              event.target.value = "";
            }}
          />
          {pendingImages().some((item) => item.failed) && (
            <div role="alert" class="text-xs text-destructive">
              Image upload failed.{" "}
              <Button
                type="button"
                size="xs"
                variant="ghost"
                disabled={busy() || uploading() > 0}
                onClick={() => {
                  const retry = pendingImagesRef.current
                    .filter((item) => item.failed)
                    .map((item) => ({ ...item, failed: false }));
                  setPending([
                    ...pendingImagesRef.current.filter((item) => !item.failed),
                    ...retry,
                  ]);
                  startUploads(retry);
                }}
              >
                Retry upload
              </Button>
            </div>
          )}
          <ComposerToolbar class="gap-2">
            <ComposerAttachButton
              aria-label={i18n.t("chat.composer.attachImage")}
              title={i18n.t("chat.composer.attachImage")}
              class="shrink-0"
              disabled={!canAttach()}
              onClick={() => inputRef.current?.click()}
            />
            <div
              class="min-w-0 flex-1"
              onPointerDown={() => {
                explicitSelection.current = true;
              }}
              onKeyDown={() => {
                explicitSelection.current = true;
              }}
            >
              <ModelSelector
                triggerRef={modelTriggerRef}
                sessionId={props.sessionId}
                selection={props.selection}
                onChange={(value) => {
                  const projectChanged = value.project !== props.selection?.project;
                  props.onSelectionChange(value);
                  if (ready() && explicitSelection.current) {
                    explicitSelection.current = false;
                    if (!projectChanged)
                      change({ ...work.current.desired, configuration: config(value) });
                  }
                }}
                onPreference={props.onPreference}
                onImageSupportChange={imageSupport}
                busy={busy() || !ready() || uncertain() || conflict()}
                statusText={
                  conflict()
                    ? "Review the Job conflict before continuing."
                    : uncertain()
                      ? "Waiting to reconcile submission."
                      : busy()
                        ? "Saving Job…"
                        : undefined
                }
                projectLocked={Boolean(
                  props.conversation || confirmed() || work.current.association,
                )}
                lockedProject={
                  confirmed()
                    ? confirmed()!.project
                    : props.conversation
                      ? props.project
                      : work.current.association?.project
                }
              />
            </div>
            <ComposerActions class="shrink-0">
              {editingQueue() ? (
                <Button type="submit" size="sm" disabled={sendDisabled()}>
                  Save queued Job
                </Button>
              ) : (
                <ComposerSend
                  type="submit"
                  idle={sendDisabled()}
                  disabled={sendDisabled()}
                  aria-label={i18n.t("aui.sendMessage")}
                  title={i18n.t("aui.sendMessage")}
                />
              )}
            </ComposerActions>
          </ComposerToolbar>
        </ComposerBar>
        <Show when={shortcuts().length}>
          <div class="absolute inset-x-2 bottom-full z-20 mb-2 grid gap-1 rounded-lg border border-border bg-popover p-1 shadow-md">
            <For each={shortcuts()} keyed={(shortcut) => shortcut.key}>
              {(shortcut, index) => (
                <button
                  type="button"
                  class={[
                    "rounded px-3 py-2 text-left text-sm hover:bg-muted focus-visible:outline-2",
                    index() === shortcutIndex() ? "bg-muted" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  onClick={() => runShortcut(shortcut())}
                >
                  /{shortcut().key} · {shortcut().label}
                </button>
              )}
            </For>
          </div>
        </Show>
      </form>

      <p role="status" class="text-xs text-muted-foreground">
        {pendingImages().length ? "Images not yet saved" : status()}
        {confirmed() ? ` · revision ${confirmed()!.revision}` : ""}
        {uploading() ? ` · uploading ${uploading()}` : ""}
      </p>
      {uncertain() && (
        <div role="alert" class="text-xs text-destructive">
          Submission outcome is uncertain.{" "}
          <Button
            type="button"
            size="xs"
            variant="ghost"
            disabled={busy()}
            onClick={() => void reconcileSubmission()}
          >
            Reconcile submission
          </Button>
        </div>
      )}
      {queueError() && (
        <div role="alert" class="text-xs text-destructive">
          {queueError()}
          <Button type="button" size="xs" variant="ghost" onClick={() => void refreshQueue()}>
            Retry
          </Button>
        </div>
      )}
      {error() && (
        <div role="alert" class="text-xs text-destructive">
          {error()}{" "}
          {!uncertain() && (
            <Button
              type="button"
              size="xs"
              variant="ghost"
              disabled={busy() || (conflict() && uploading() > 0)}
              onClick={() =>
                !ready()
                  ? window.location.reload()
                  : conflict()
                    ? void loadSaved()
                    : (void flush().catch(() => {}), void refreshQueue(), void refreshUnfinished())
              }
            >
              {!ready() ? "Reload" : conflict() ? "Load saved version" : "Retry"}
            </Button>
          )}
        </div>
      )}
    </Composer>
  );
}
