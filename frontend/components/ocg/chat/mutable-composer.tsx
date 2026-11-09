"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import Image from "next/image";
import { useOcgRuntime } from "../runtime/runtime-context";
import {
  ControlTransportError,
  type MutableJob,
  type MutableJobInput,
} from "../runtime/control-client";
import type { ChatModelSelection, ChatImage } from "../contracts";
import { ModelSelector, type ModelImageSupport } from "./model-selector";

export const MUTABLE_CONTEXT_KEY = "ocg.mutable-composer.context.v1";
const QUEUED_KEY = "ocg.mutable-composer.queued.v1";
const UNFINISHED_KEY = "ocg.mutable-composer.unfinished.v1";
const PENDING_KEY = "ocg.mutable-composer.pending-submission.v1";
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
  onSelectionChange: (value: ChatModelSelection) => void;
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

export function MutableComposer({
  sessionId,
  context,
  conversation,
  project,
  selection,
  onSelectionChange,
  onPreference,
}: Props) {
  const { control, client } = useOcgRuntime();
  const work = useRef<Work>({
    context,
    job: null,
    desired: empty(),
    initial: null,
    association: null,
    dirty: false,
    conflict: false,
    promise: null,
  });
  const scope = useRef({ conversation, project: selection?.project ?? project });
  scope.current = { conversation, project: selection?.project ?? project };
  const alive = useRef(true);
  const [input, setInput] = useState<MutableJobInput>(empty);
  const [confirmed, setConfirmed] = useState<MutableJob | null>(null);
  const [ready, setReady] = useState(false);
  const [status, setStatus] = useState("Loading saved Job…");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [uncertain, setUncertain] = useState(false);
  const [conflict, setConflict] = useState(false);
  const [uploading, setUploading] = useState(0);
  const [pendingImages, setPendingImages] = useState<PendingImage[]>([]);
  const pendingImagesRef = useRef<PendingImage[]>([]);
  const uploadTask = useRef<Promise<void> | null>(null);
  const uncertainRef = useRef(false);
  uncertainRef.current = uncertain;
  const [images, setImages] = useState<ChatImage[]>([]);
  const [queued, setQueued] = useState<MutableJob[]>([]);
  const [unfinished, setUnfinished] = useState<MutableJob[]>([]);
  const [editingQueue, setEditingQueue] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const explicitSelection = useRef(false);
  const [support, setSupport] = useState<ModelImageSupport | null>(null);
  const imageSupport = useCallback((value: ModelImageSupport) => setSupport(value), []);
  const [queuedLoading, setQueuedLoading] = useState(false);

  useEffect(() => {
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
  }, []);

  const loadImages = useCallback(
    async (ids: string[]) => {
      if (!control) return [];
      return Promise.all(
        ids.map(async (id) => {
          const item = await control.readAttachment(id);
          if (!item.available) throw new Error(`Attachment ${id} is unavailable.`);
          return {
            id,
            name: item.display_name ?? "Image",
            media_type: item.mime,
            url: control.attachmentContentUrl(id),
          };
        }),
      );
    },
    [control],
  );
  const refreshQueue = useCallback(async () => {
    if (!control) return;
    const results = await Promise.allSettled(
      queuedPointers().map(({ context, job }) => control.readMutableJob(context, job)),
    );
    if (!alive.current) return;
    setQueued(
      results.flatMap((result) =>
        result.status === "fulfilled" &&
        result.value.phase === "queued" &&
        (scope.current.conversation
          ? result.value.conversation === scope.current.conversation
          : result.value.context === work.current.context)
          ? [result.value]
          : [],
      ),
    );
    if (results.some((result) => result.status === "rejected"))
      setError("Some queued Jobs could not be loaded. Retry to see their latest state.");
  }, [control]);
  const refreshUnfinished = useCallback(async () => {
    if (!control) return;
    const results = await Promise.allSettled(
      unfinishedContexts().map((item) => control.currentMutableJob(item)),
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
  }, [control]);
  useEffect(() => {
    alive.current = true;
    if (!control) return;
    let cancelled = false;
    void (async () => {
      try {
        const pendingText = localStorage.getItem(pendingKey(context));
        const pending: {
          context: string;
          job: string;
          revision: number;
          fingerprint: string;
        } | null = pendingText ? JSON.parse(pendingText) : null;
        let job = await control.currentMutableJob(context);
        if (pending?.context === context) {
          const submitted = await control.readMutableJob(context, pending.job);
          if (
            (submitted.phase === "queued" ||
              submitted.phase === "executing" ||
              submitted.phase === "completed") &&
            (await inputFingerprint(submitted.input)) === pending.fingerprint
          ) {
            savePointer({ context, job: submitted.id });
            bindComposerContext(sessionId, context, submitted.conversation);
            if (!client.presentMutableJob)
              throw new Error("The submitted Job cannot yet be opened in history.");
            await client.presentMutableJob(sessionId, submitted);
            forgetUnfinished(context);
            localStorage.removeItem(pendingKey(context));
            clearGlobalContext(context);
            work.current.context = rotateContext(sessionId, submitted.conversation);
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
          if (job.phase === "unsubmitted") rememberUnfinished(context);
          const restored = await loadImages(job.input.attachments);
          if (cancelled) return;
          work.current.job = job;
          work.current.desired = job.input;
          setInput(job.input);
          setConfirmed(job);
          setImages(restored);
          if (job.input.configuration)
            onSelectionChange({
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
    // Initial recovery belongs to this Composer identity, not each selection change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [control, context, loadImages, refreshQueue, refreshUnfinished]);

  const flush = useCallback(
    function flushWork(): Promise<void> {
      const state = work.current;
      if (state.promise) return state.promise;
      if (!control) return Promise.reject(new Error("Control Plane unavailable."));
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
                state.job = await control.createMutableJob(
                  state.context,
                  state.initial,
                  `create-${state.context}`,
                  state.association,
                );
              } catch (cause) {
                const existing = await control.currentMutableJob(state.context).catch(() => null);
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
                throw new ControlTransportError("mutable_job_conflict", 409, false);
              }
              rememberUnfinished(state.context);
            } else if (!same(state.job.input, state.desired)) {
              state.job = await control.updateMutableJob(
                state.context,
                state.job.id,
                state.job.revision,
                structuredClone(state.desired),
              );
            }
            state.dirty = !same(state.job.input, state.desired);
            if (alive.current) {
              setConfirmed(state.job);
              setStatus(state.dirty ? "Saving…" : "Saved");
            }
          } catch (cause) {
            if (
              cause instanceof ControlTransportError &&
              cause.status === 409 &&
              cause.code !== "unsupported_execution_context"
            ) {
              state.conflict = true;
              if (alive.current) setConflict(true);
              if (state.job) {
                const latest = await control
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
      let failed = false;
      state.promise = run()
        .catch((cause: unknown) => {
          failed = true;
          throw cause;
        })
        .finally(() => {
          state.promise = null;
          if (!failed && state.dirty && !state.conflict)
            queueMicrotask(() => {
              void flushWork().catch(() => {});
            });
        });
      return state.promise;
    },
    [control],
  );
  useEffect(() => {
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
  }, [flush]);
  useEffect(
    () => () => {
      for (const pending of pendingImagesRef.current) URL.revokeObjectURL(pending.url);
    },
    [],
  );
  function change(next: MutableJobInput, meaningful = true) {
    if (busyRef.current || uncertain) return;
    const state = work.current;
    state.desired = next;
    if (meaningful || state.job) state.dirty = true;
    setInput(next);
    if (state.dirty) {
      setStatus("Local changes not saved");
      void flush().catch(() => {});
    }
  }
  function setPending(next: PendingImage[]) {
    pendingImagesRef.current = next;
    if (alive.current) setPendingImages(next);
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
    change(
      {
        ...work.current.desired,
        configuration: work.current.desired.configuration ?? config(selection),
      },
      true,
    );
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
          };
          state.dirty = true;
          void flush().catch(() => {});
          continue;
        }
        setImages((old) => [...old, image]);
        change({
          ...work.current.desired,
          attachments: [...work.current.desired.attachments, image.id],
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
  function selectImages(files: FileList | null) {
    if (!files?.length) return;
    if (
      work.current.desired.attachments.length + pendingImagesRef.current.length + files.length >
      8
    ) {
      setError("At most eight images are supported.");
      return;
    }
    const selected = Array.from(files, (file) => ({
      file,
      url: URL.createObjectURL(file),
      failed: false,
    }));
    setPending([...pendingImagesRef.current, ...selected]);
    startUploads(selected);
  }
  async function send() {
    if (
      !control ||
      busyRef.current ||
      pendingImagesRef.current.length ||
      work.current.conflict ||
      uncertain
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
      if (editingQueue) {
        if (!client.presentMutableJob)
          throw new Error("The saved Job cannot yet be opened in history.");
        await client.presentMutableJob(sessionId, state.job);
        setEditingQueue(false);
        state.context = contextForSession(sessionId, state.job.conversation);
        state.job = null;
        state.initial = null;
        state.association = null;
        state.dirty = false;
        state.desired = empty();
        setInput(empty());
        setConfirmed(null);
        setImages([]);
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
        result = await control.submitMutableJob(
          state.context,
          state.job.id,
          revision,
          submittedInput,
          key,
        );
      } catch (cause) {
        const observed = await control
          .readMutableJob(state.context, state.job.id)
          .catch(() => null);
        if (!observed || observed.phase === "unsubmitted") {
          if (
            cause instanceof ControlTransportError &&
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
          if (cause instanceof ControlTransportError && cause.status >= 400 && cause.status < 500)
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
      bindComposerContext(sessionId, state.context, result.conversation);
      try {
        if (!client.presentMutableJob) throw new Error("History is unavailable.");
        await client.presentMutableJob(sessionId, result);
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
      const next = rotateContext(sessionId, result.conversation);
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
    if (!control || busyRef.current) return;
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
      const saved = await control.readMutableJob(pending.context, pending.job);
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
        result = await control.submitMutableJob(
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
      await client.presentMutableJob(sessionId, result);
      savePointer({ context: pending.context, job: result.id });
      forgetUnfinished(pending.context);
      if (!same(result.input, work.current.desired))
        throw new Error(
          "The submitted Job differs from your local input. Your input has not been cleared.",
        );
      localStorage.removeItem(pendingKey(pending.context));
      clearGlobalContext(pending.context);
      const state = work.current;
      state.context = rotateContext(sessionId, result.conversation);
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
    if (!control || busyRef.current || uncertain || work.current.job || work.current.dirty) {
      setError("Finish or discard the current Job before editing a queued Job.");
      return;
    }
    busyRef.current = true;
    setBusy(true);
    setQueuedLoading(true);
    try {
      const latest = await control.readMutableJob(job.context, job.id);
      if (conversation && latest.conversation !== conversation)
        throw new Error("This Job belongs to another conversation.");
      if (latest.phase !== "queued")
        throw new Error(`This Job is ${latest.phase} and can no longer be edited.`);
      const restored = await loadImages(latest.input.attachments);
      work.current.context = latest.context;
      work.current.job = latest;
      work.current.desired = latest.input;
      work.current.initial = null;
      setInput(latest.input);
      setConfirmed(latest);
      setImages(restored);
      setEditingQueue(true);
      setStatus("Editing queued Job");
      setError(null);
      if (latest.input.configuration)
        onSelectionChange({
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
  async function cancelUnsupported(job: MutableJob) {
    if (
      !control ||
      busyRef.current ||
      uncertain ||
      work.current.dirty ||
      work.current.promise ||
      pendingImagesRef.current.length ||
      editingQueue
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
      const result = await control.cancelUnsupportedQueuedJob(job.context, job.id, job.revision);
      if (result.phase !== "cancelled") throw new Error("The Job cancellation was not confirmed.");
      forgetQueued(job.id);
      await refreshQueue();
      if (client.presentMutableJob) await client.presentMutableJob(sessionId, result);
      setStatus("Unsupported queued Job cancelled; its history is preserved.");
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function discard() {
    if (
      !control ||
      !work.current.job ||
      editingQueue ||
      uncertain ||
      busyRef.current ||
      pendingImagesRef.current.length
    )
      return;
    busyRef.current = true;
    setBusy(true);
    try {
      await flush();
      const state = work.current;
      if (!state.job) return;
      await control.discardMutableJob(state.context, state.job.id, state.job.revision);
      forgetUnfinished(state.context);
      state.context = rotateContext(sessionId, conversation);
      state.job = null;
      state.initial = null;
      state.association = null;
      state.dirty = false;
      state.desired = empty();
      setInput(empty());
      setConfirmed(null);
      setImages([]);
      setStatus("Job discarded");
      setError(null);
      await refreshUnfinished();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }
  async function loadSaved() {
    if (!control || !work.current.job || busyRef.current || uploadTask.current) return;
    if (
      work.current.dirty &&
      !window.confirm("Loading the saved version replaces your unsaved text. Continue?")
    )
      return;
    busyRef.current = true;
    setBusy(true);
    try {
      const job = await control.readMutableJob(work.current.context, work.current.job.id);
      const restored = await loadImages(job.input.attachments);
      const unsavedImages = images.filter(
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
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function openUnfinished(job: MutableJob) {
    if (
      !control ||
      busyRef.current ||
      uncertain ||
      conflict ||
      pendingImagesRef.current.length ||
      editingQueue
    )
      return;
    busyRef.current = true;
    setBusy(true);
    try {
      await flush();
      const latest = await control.readMutableJob(job.context, job.id);
      if (conversation && latest.conversation !== conversation)
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
      bindComposerContext(sessionId, latest.context, conversation);
      setInput(latest.input);
      setConfirmed(latest);
      setImages(restored);
      setStatus("Saved");
      setError(null);
      if (latest.input.configuration)
        onSelectionChange({
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
    <div className="space-y-2 border-t border-border p-3">
      {unfinished.some((job) => job.id !== confirmed?.id) && (
        <div aria-label="Unsubmitted Jobs" className="space-y-1 text-sm">
          {unfinished
            .filter((job) => job.id !== confirmed?.id)
            .map((job) => (
              <div key={job.id} className="flex items-center gap-2">
                <span className="min-w-0 flex-1 truncate">
                  Unsubmitted: {job.input.text || "Image Job"}
                </span>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={busy || uncertain || conflict || uploading > 0 || editingQueue}
                  onClick={() => void openUnfinished(job)}
                >
                  Open
                </Button>
              </div>
            ))}
        </div>
      )}
      {queued.length > 0 && (
        <div aria-label="Queued Jobs" className="space-y-1 text-sm">
          {queued.map((job) => (
            <div key={job.id} className="flex items-center gap-2">
              <span className="min-w-0 flex-1 truncate">
                {job.unsupported_decision_history ? "Blocked by Decision history" : job.phase}:{" "}
                {job.input.text || "Image Job"}
              </span>
              <Button
                type="button"
                size="xs"
                variant="outline"
                disabled={job.phase !== "queued" || queuedLoading || busy || uncertain}
                onClick={() => void editQueued(job)}
              >
                Edit
              </Button>
              {job.unsupported_decision_history && (
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={
                    job.phase !== "queued" || queuedLoading || busy || uncertain || editingQueue
                  }
                  onClick={() => void cancelUnsupported(job)}
                >
                  Cancel blocked Job
                </Button>
              )}
            </div>
          ))}
        </div>
      )}
      <label className="sr-only" htmlFor="mutable-job-input">
        Message
      </label>
      <Textarea
        id="mutable-job-input"
        placeholder="Describe a task or ask a question…"
        value={input.text}
        disabled={!ready || busy || conflict || uncertain}
        onChange={(event) =>
          change({
            ...work.current.desired,
            text: event.target.value,
            configuration: work.current.desired.configuration ?? config(selection),
          })
        }
      />
      <div className="flex flex-wrap gap-2">
        {images.map((image) => (
          <div key={image.id} className="flex items-center gap-1">
            <Image
              src={image.url}
              alt={image.name}
              width={48}
              height={48}
              unoptimized
              className="h-12 w-12 rounded object-cover"
            />
            <Button
              type="button"
              size="xs"
              variant="ghost"
              disabled={busy || uncertain || conflict}
              onClick={() => {
                setImages((old) => old.filter((item) => item.id !== image.id));
                change({
                  ...work.current.desired,
                  attachments: work.current.desired.attachments.filter((id) => id !== image.id),
                });
              }}
            >
              Remove
            </Button>
          </div>
        ))}
      </div>
      <div className="flex flex-wrap gap-2">
        {pendingImages.map((item) => (
          <div key={item.url} className="flex items-center gap-1">
            <Image
              src={item.url}
              alt={item.file.name}
              width={48}
              height={48}
              unoptimized
              className="h-12 w-12 rounded object-cover"
            />
            <span className="text-xs">{item.failed ? "Upload failed" : "Uploading…"}</span>
            {item.failed && (
              <Button
                type="button"
                size="xs"
                variant="ghost"
                onClick={() => {
                  setPending(pendingImagesRef.current.filter((entry) => entry !== item));
                  URL.revokeObjectURL(item.url);
                }}
              >
                Remove
              </Button>
            )}
          </div>
        ))}
      </div>
      <input
        ref={inputRef}
        type="file"
        accept="image/png,image/jpeg,image/gif,image/webp"
        multiple
        hidden
        onChange={(event) => {
          selectImages(event.target.files);
          event.target.value = "";
        }}
      />
      {pendingImages.some((item) => item.failed) && (
        <div role="alert" className="text-xs text-destructive">
          Image upload failed.{" "}
          <Button
            type="button"
            size="xs"
            variant="ghost"
            disabled={busy || uploading > 0}
            onClick={() => {
              const retry = pendingImagesRef.current
                .filter((item) => item.failed)
                .map((item) => ({ ...item, failed: false }));
              setPending([...pendingImagesRef.current.filter((item) => !item.failed), ...retry]);
              startUploads(retry);
            }}
          >
            Retry upload
          </Button>
        </div>
      )}
      <div className="flex flex-wrap items-center gap-2">
        <div
          className="min-w-0 flex-1"
          onPointerDownCapture={() => {
            explicitSelection.current = true;
          }}
          onKeyDownCapture={() => {
            explicitSelection.current = true;
          }}
        >
          <ModelSelector
            sessionId={sessionId}
            selection={selection}
            onChange={(value) => {
              const projectChanged = value.project !== selection?.project;
              onSelectionChange(value);
              if (ready && explicitSelection.current) {
                explicitSelection.current = false;
                if (!projectChanged)
                  change({ ...work.current.desired, configuration: config(value) });
              }
            }}
            onPreference={onPreference}
            onImageSupportChange={imageSupport}
            busy={busy || !ready || uncertain || conflict}
            statusText={
              conflict
                ? "Review the Job conflict before continuing."
                : uncertain
                  ? "Waiting to reconcile submission."
                  : busy
                    ? "Saving Job…"
                    : undefined
            }
            projectLocked={Boolean(conversation || confirmed || work.current.association)}
            lockedProject={
              confirmed
                ? confirmed.project
                : conversation
                  ? project
                  : work.current.association?.project
            }
          />
        </div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={!ready || busy || uncertain || conflict || uploading > 0 || !support?.supported}
          onClick={() => inputRef.current?.click()}
        >
          Attach image
        </Button>
        {confirmed && !editingQueue && (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            disabled={busy || uncertain || conflict || pendingImages.length > 0}
            onClick={() => void discard()}
          >
            Discard
          </Button>
        )}
        <Button
          type="button"
          size="sm"
          disabled={
            !ready ||
            busy ||
            pendingImages.length > 0 ||
            conflict ||
            uncertain ||
            (!editingQueue && !input.text.trim() && !input.attachments.length)
          }
          onClick={() => void send()}
        >
          {editingQueue ? "Save queued Job" : "Send"}
        </Button>
      </div>
      <p role="status" className="text-xs text-muted-foreground">
        {pendingImages.length ? "Images not yet saved" : status}
        {confirmed ? ` · revision ${confirmed.revision}` : ""}
        {uploading ? ` · uploading ${uploading}` : ""}
      </p>
      {uncertain && (
        <div role="alert" className="text-xs text-destructive">
          Submission outcome is uncertain.{" "}
          <Button
            type="button"
            size="xs"
            variant="ghost"
            disabled={busy}
            onClick={() => void reconcileSubmission()}
          >
            Reconcile submission
          </Button>
        </div>
      )}
      {error && (
        <div role="alert" className="text-xs text-destructive">
          {error}{" "}
          {!uncertain && (
            <Button
              type="button"
              size="xs"
              variant="ghost"
              disabled={busy || (conflict && uploading > 0)}
              onClick={() =>
                !ready
                  ? window.location.reload()
                  : conflict
                    ? void loadSaved()
                    : (void flush().catch(() => {}), void refreshQueue(), void refreshUnfinished())
              }
            >
              {!ready ? "Reload" : conflict ? "Load saved version" : "Retry"}
            </Button>
          )}
        </div>
      )}
    </div>
  );
}
