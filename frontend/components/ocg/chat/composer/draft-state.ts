import { createEffect, createMemo, createSignal } from "solid-js";
import { elementRef } from "@/lib/ui-state";
import type { ChatImage, ChatModelSelection } from "../../contracts";
import { useI18n } from "../../i18n";
import { ApiError, type MutableJob, type MutableJobInput } from "../../runtime/api-client";
import { executionLabel } from "../../runtime/job-status";
import { useOcgRuntime } from "../../runtime/runtime-context";
import { useConversationView } from "../conversation-view";
import type { ModelImageSupport } from "../model-selector";
import { createAttachmentQueue, loadImages, type PendingImage } from "./attachments";
import {
  bindComposerContext,
  clearGlobalContext,
  forgetQueued,
  forgetUnfinished,
  pendingKey,
  queuedPointers,
  rememberUnfinished,
  rotateContext,
  savePointer,
  unfinishedContexts,
} from "./context";

const AUTOSAVE_DELAY_MS = 1500;
let navigationGuard: (() => Promise<boolean>) | null = null;
export async function confirmMutableComposerNavigation(): Promise<boolean> {
  return navigationGuard ? navigationGuard() : true;
}
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
function config(selection?: ChatModelSelection): MutableJobInput["configuration"] {
  if (!selection?.llmProfile) return null;
  return {
    llm_profile: selection.llmProfile,
    agent_profile: selection.agentProfile ?? null,
    reasoning_effort: selection.effort,
  };
}
export type ComposerProps = {
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
function errorMessage(cause: unknown) {
  return cause instanceof Error ? cause.message : String(cause);
}

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

export function createComposerDraft(props: ComposerProps) {
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
  const [status, setStatus] = createSignal<string | { submittedJob: string }>("Loading saved Job…");
  const { byJob } = useConversationView();
  const statusText = createMemo(() => {
    const current = status();
    if (typeof current === "string") return current;
    const execution = byJob()
      .get(current.submittedJob)
      ?.find((message) => message.execution)?.execution;
    return execution
      ? `Job ${current.submittedJob} · ${executionLabel(i18n.t, execution)}`
      : `Job ${current.submittedJob}`;
  });
  const [error, setError] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);
  const busyRef = elementRef(false);
  const [uncertain, setUncertain] = createSignal(false);
  const [conflict, setConflict] = createSignal(false);
  const uncertainRef = elementRef(false);
  createEffect(uncertain, (value) => {
    uncertainRef.current = value;
  });
  const [images, setImages] = createSignal<ChatImage[]>([]);
  const [queued, setQueued] = createSignal<MutableJob[]>([]);
  const [unfinished, setUnfinished] = createSignal<MutableJob[]>([]);
  const [editingQueue, setEditingQueue] = createSignal(false);
  const [support, setSupport] = createSignal<ModelImageSupport | null>(null);
  const imageSupport = (value: ModelImageSupport) => setSupport(value);
  const [queuedLoading, setQueuedLoading] = createSignal(false);
  const inputDisabled = createMemo(() => !ready() || busy() || conflict() || uncertain());
  const attachments = createAttachmentQueue(
    client,
    () => alive.current,
    () => canAttach(),
    () => work.current.desired.attachments.length,
    (pending) => {
      const state = work.current;
      if (isEmptyDraft(state.job, state.desired, pending)) scheduleAutosave();
    },
    (image) => {
      const state = work.current;
      const next = {
        ...state.desired,
        attachments: [...state.desired.attachments, image.id],
        configuration: state.desired.configuration ?? config(props.selection),
      };
      if (alive.current) {
        setImages((old) => [...old, image]);
        change(next);
      } else {
        state.desired = next;
        state.dirty = true;
        void flush().catch(() => {});
      }
    },
    setError,
  );
  const { pendingImages, pendingImagesRef, uploading, uploadTask } = attachments;
  const canAttach: () => boolean = createMemo(
    () => !inputDisabled() && uploading() === 0 && Boolean(support()?.supported),
  );
  const sendDisabled = createMemo(
    () =>
      inputDisabled() ||
      pendingImages().length > 0 ||
      (!editingQueue() && !input().text.trim() && !input().attachments.length),
  );
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
        if (active) setQueueError(errorMessage(cause));
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
  async function startDraftAfterJob(job: MutableJob, text = "") {
    if (scope.current.conversation && job.conversation !== scope.current.conversation)
      throw new Error("The saved Job belongs to another conversation.");

    const state = work.current;
    const sessionId = props.sessionId;
    let historyError: string | null = null;
    try {
      await client.presentMutableJob?.(sessionId, job);
    } catch (cause) {
      historyError = `Could not load Job ${job.id} in history. ${errorMessage(cause)}`;
    }
    if (!alive.current || work.current !== state || props.sessionId !== sessionId) return;

    const localImages = images().filter(
      (image) =>
        state.desired.attachments.includes(image.id) && !job.input.attachments.includes(image.id),
    );
    const desired = {
      ...empty(),
      text,
      attachments: localImages.map((image) => image.id),
      configuration: config(props.selection),
    };
    forgetUnfinished(job.context);
    forgetQueued(job.id);
    localStorage.removeItem(pendingKey(job.context));
    clearGlobalContext(job.context);
    work.current = {
      context: rotateContext(sessionId, job.conversation),
      job: null,
      desired,
      initial: null,
      association: { conversation: job.conversation },
      dirty: hasDraftContent(desired),
      conflict: false,
      promise: null,
    };
    draftBeforeQueueEdit.current = null;
    setEditingQueue(false);
    setConfirmed(null);
    setInput(desired);
    setImages(localImages);
    setConflict(false);
    setUncertain(false);
    uncertainRef.current = false;
    setError(historyError);
    setQueued((jobs) => jobs.filter((item) => item.id !== job.id));
    setUnfinished((jobs) => jobs.filter((item) => item.id !== job.id));
    if (work.current.dirty) scheduleAutosave();
    setStatus(`Job ${job.id} is ${job.phase}. A new draft is ready.`);
  }

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
            if (cancelled) return;
            if (submitted.phase !== "unsubmitted" && submitted.phase !== "queued") {
              await startDraftAfterJob(submitted);
              if (cancelled) return;
              setReady(true);
              await refreshQueue();
              await refreshUnfinished();
              return;
            }
            if (
              submitted.phase === "queued" &&
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
            const restored = await loadImages(api, job.input.attachments);
            if (cancelled) return;
            work.current.job = job;
            work.current.desired = job.input;
            setInput(job.input);
            setConfirmed(job);
            setImages(restored);
            restoreSelection(job);
          }
          setReady(true);
          setStatus(job ? "Saved" : "New Job");
          await refreshQueue();
          await refreshUnfinished();
        } catch (cause) {
          if (!cancelled) setError(errorMessage(cause));
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
          } else if (alive.current) setError(errorMessage(cause));
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
          `The Job was submitted, but history could not be opened. Reconcile the same Job. ${errorMessage(cause)}`,
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
      setStatus({ submittedJob: result.id });
      await refreshQueue();
      await refreshUnfinished();
    } catch (cause) {
      setError(errorMessage(cause));
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
      if (saved.phase !== "unsubmitted" && saved.phase !== "queued") {
        const localText = work.current.desired.text;
        await startDraftAfterJob(saved, localText === saved.input.text ? "" : localText);
        return;
      }
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
      setStatus({ submittedJob: result.id });
      await refreshQueue();
      await refreshUnfinished();
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }
  function restoreSelection(job: MutableJob) {
    const configuration = job.input.configuration;
    if (configuration)
      props.onSelectionChange({
        model: "",
        effort: configuration.reasoning_effort,
        llmProfile: configuration.llm_profile,
        agentProfile: configuration.agent_profile ?? undefined,
        project: job.project ?? undefined,
      });
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
      const restored = await loadImages(api, latest.input.attachments);
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
      restoreSelection(latest);
    } catch (cause) {
      setError(errorMessage(cause));
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
      setError(errorMessage(cause));
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
      setError(errorMessage(cause));
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
      if (job.phase !== "unsubmitted" && job.phase !== "queued") {
        await startDraftAfterJob(job);
        return;
      }
      const restored = await loadImages(api, job.input.attachments);
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
      work.current.conflict = false;
      work.current.dirty = unsavedImages.length > 0;
      setConflict(false);
      setInput(desired);
      setImages([...restored, ...unsavedImages]);
      setConfirmed(job);
      setError(
        desired.attachments.length > 8
          ? "Remove images to meet the eight-image limit before saving."
          : null,
      );
      setStatus(work.current.dirty ? "Local image changes not saved" : "Saved");
      if (isEmptyDraft(job, desired, pendingImagesRef.current)) scheduleAutosave();
    } catch (cause) {
      setError(errorMessage(cause));
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
      const restored = await loadImages(api, latest.input.attachments);
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
      restoreSelection(latest);
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      busyRef.current = false;
      if (alive.current) setBusy(false);
    }
  }

  function removeImage(id: string) {
    setImages((old) => old.filter((item) => item.id !== id));
    change({
      ...work.current.desired,
      attachments: work.current.desired.attachments.filter((item) => item !== id),
    });
  }
  function configure(value: ChatModelSelection) {
    change({ ...work.current.desired, configuration: config(value) });
  }
  return {
    input,
    confirmed,
    statusText,
    error,
    busy,
    uncertain,
    conflict,
    ready,
    inputDisabled,
    sendDisabled,
    images,
    attachments,
    removeImage,
    canAttach,
    imageSupport,
    configure,
    association: () => work.current.association,
    queued,
    unfinished,
    editingQueue,
    queuedLoading,
    queueError,
    hasQueued,
    refreshQueue,
    refreshUnfinished,
    openUnfinished,
    editQueued,
    withdraw,
    cancelUnsupported,
    changeText,
    send,
    reconcileSubmission,
    loadSaved,
    flush,
  };
}

export type ComposerDraft = ReturnType<typeof createComposerDraft>;
