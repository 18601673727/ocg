"use client";

import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  ControlTransportError,
  type ControlSubmission,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/canonical-client";
import { ContractError, type ControlCapability } from "@/components/ocg/contracts";

type Phase = "editing" | "submitting" | "pending" | "uncertain" | "admitted" | "refused";

/** A fresh idempotency key names one submission intent, once. */
function makeKey(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

// A refusal that says nothing was created may start a new intent; anything
// that may have created a Job keeps its key and is never submitted again.
function outcomeOf(error: unknown): "refused" | "uncertain" {
  if (error instanceof ControlTransportError) {
    if (error.status === 0) return "uncertain";
    if (error.status >= 400 && error.status < 500) return "refused";
    if (error.status === 503) return "refused";
    return "uncertain";
  }
  return "uncertain";
}

/** What Chat has already chosen: the form then asks only for the message. */
export type SubmissionTarget = {
  agentProfile: string;
  llmProfile: string;
  executor: string;
  declaredCapabilities?: ControlCapability[];
};

const MAX_SINGLE_IMAGE_BYTES = 4 * 1024 * 1024; // 4 MiB
const MAX_REQUEST_IMAGES = 8;
const MAX_TOTAL_IMAGE_BYTES = 16 * 1024 * 1024; // 16 MiB

type PendingImage = {
  id: string;
  file: File;
  previewUrl: string;
  name: string;
  sizeBytes: number;
  mime: "image/png" | "image/jpeg";
};

async function fileToBase64(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  let binary = "";
  const len = bytes.byteLength;
  const chunkSize = 8192;
  for (let i = 0; i < len; i += chunkSize) {
    const chunk = bytes.subarray(i, Math.min(i + chunkSize, len));
    binary += String.fromCharCode.apply(null, chunk as unknown as number[]);
  }
  return btoa(binary);
}

/**
 * Submits one Provider Job under one Idempotency-Key. The key survives
 * every retry and admission lookup of this intent: no timeout or pending
 * answer ever produces a second Job.
 */
export function JobSubmissionForm({
  client,
  onJob,
  target,
  previousJob,
  disabledReason,
}: {
  client: HttpCanonicalControlClient;
  onJob: (jobId: string) => void;
  // Present in Chat: the Agent Profile, LLM target and Executor come from the
  // selector. Absent in the Jobs view: they are typed by identity.
  target?: SubmissionTarget | null;
  // When continuing a multi-turn conversation in Chat, names the completed predecessor Job.
  previousJob?: string | null;
  // Explains why the next turn cannot be sent yet (e.g. while the current Job is running).
  disabledReason?: string | null;
}) {
  const chat = target !== undefined;
  const [agentProfile, setAgentProfile] = useState("");
  const [llmProfile, setLlmProfile] = useState("");
  const [executor, setExecutor] = useState("");
  const [objective, setObjective] = useState("");
  const [name, setName] = useState("");
  const [phase, setPhase] = useState<Phase>("editing");
  const [message, setMessage] = useState<string | null>(null);
  const [key, setKey] = useState<string | null>(null);
  const [availableLlms, setAvailableLlms] = useState<{ id: string; name: string; model: string; executors: string[]; declared_capabilities?: ControlCapability[] }[]>([]);
  const [availableAgents, setAvailableAgents] = useState<{ id: string; name: string; llm_targets: string[] }[]>([]);
  const [catalogVersion, setCatalogVersion] = useState(0);
  const [pendingImages, setPendingImages] = useState<PendingImage[]>([]);
  const [imageError, setImageError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const pendingImagesRef = useRef<PendingImage[]>([]);
  pendingImagesRef.current = pendingImages;

  const keyRef = useRef<string | null>(null);
  // What the held key was submitted with. A retry replays exactly this, so a
  // later selection or edit can never turn one key into two requests.
  const requestRef = useRef<ControlSubmission | null>(null);
  const reconciliation = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      if (reconciliation.current) clearTimeout(reconciliation.current);
      for (const img of pendingImagesRef.current) {
        try {
          URL.revokeObjectURL(img.previewUrl);
        } catch {}
      }
    };
  }, []);

  function clearPendingImages() {
    for (const img of pendingImages) {
      try {
        URL.revokeObjectURL(img.previewUrl);
      } catch {}
    }
    setPendingImages([]);
    setImageError(null);
  }

  function addImages(files: FileList | File[]) {
    setImageError(null);
    const newImages: PendingImage[] = [];
    let currentCount = pendingImages.length;
    let currentBytes = pendingImages.reduce((sum, img) => sum + img.sizeBytes, 0);

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      if (file.type !== "image/png" && file.type !== "image/jpeg") {
        setImageError(`Unsupported format for "${file.name}". Only PNG and JPEG images are supported.`);
        continue;
      }
      if (file.size > MAX_SINGLE_IMAGE_BYTES) {
        setImageError(`"${file.name}" is larger than 4 MiB (${(file.size / (1024 * 1024)).toFixed(1)} MiB).`);
        continue;
      }
      if (currentCount + 1 > MAX_REQUEST_IMAGES) {
        setImageError(`Cannot attach more than ${MAX_REQUEST_IMAGES} images.`);
        break;
      }
      if (currentBytes + file.size > MAX_TOTAL_IMAGE_BYTES) {
        setImageError(`Total image size would exceed ${MAX_TOTAL_IMAGE_BYTES / (1024 * 1024)} MiB.`);
        break;
      }

      currentCount++;
      currentBytes += file.size;
      newImages.push({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
        file,
        previewUrl: URL.createObjectURL(file),
        name: file.name,
        sizeBytes: file.size,
        mime: file.type as "image/png" | "image/jpeg",
      });
    }

    if (newImages.length > 0) {
      setPendingImages((existing) => [...existing, ...newImages]);
    }
  }

  function removeImage(id: string) {
    setImageError(null);
    setPendingImages((existing) => {
      const match = existing.find((img) => img.id === id);
      if (match) {
        try {
          URL.revokeObjectURL(match.previewUrl);
        } catch {}
      }
      return existing.filter((img) => img.id !== id);
    });
  }

  const visionSupported = target
    ? Boolean(target.declaredCapabilities?.includes("vision"))
    : availableLlms.find((l) => l.id === llmProfile)?.declared_capabilities?.includes("vision") ?? true;

  useEffect(() => client.onProfilesChanged(() => setCatalogVersion((version) => version + 1)), [client]);

  useEffect(() => {
    if (chat) return;
    const controller = new AbortController();
    client
      .readProfiles(controller.signal)
      .then((snapshot) => {
        if (controller.signal.aborted) return;
        setAvailableLlms(snapshot.llm_profiles);
        setAvailableAgents(snapshot.agent_profiles);
      })
      .catch(() => {
        // Manual identity entry remains available even if catalog read fails.
      });
    return () => controller.abort();
  }, [chat, client, catalogVersion]);

  const settled = phase === "submitting" || phase === "pending" || phase === "uncertain";

  function clearReconciliation() {
    if (reconciliation.current) clearTimeout(reconciliation.current);
    reconciliation.current = null;
  }

  function resetAfterChatAdmitted() {
    clearReconciliation();
    clearPendingImages();
    keyRef.current = null;
    requestRef.current = null;
    setKey(null);
    setPhase("editing");
    setMessage(null);
    setObjective("");
    setName("");
  }

  function reconcile(held: string) {
    client
      .readAdmission(held)
      .then((admission) => {
        if (!mounted.current || keyRef.current !== held) return;
        if (admission.status === "admitted" && admission.job) {
          clearReconciliation();
          if (chat) {
            resetAfterChatAdmitted();
          } else {
            setPhase("admitted");
            setMessage(`${admission.job} is admitted and running.`);
          }
          onJob(admission.job);
        } else if (admission.status === "uncertain") {
          clearReconciliation();
          setPhase("uncertain");
          setMessage("Whether this key created a Job is unknown. It is not submitted again.");
        } else if (admission.status === "not_found") {
          clearReconciliation();
          setPhase("refused");
          setMessage("This key created no Job, and none can be created with it any more.");
        } else {
          setMessage("Admission is still running under this key; it is not submitted again.");
          reconciliation.current = setTimeout(() => reconcile(held), 2000);
        }
      })
      .catch((error: unknown) => {
        if (!mounted.current || keyRef.current !== held) return;
        const code = error instanceof ControlTransportError ? error.code : "unreachable";
        setMessage(`The key could not be looked up (${code}); trying again.`);
        reconciliation.current = setTimeout(() => reconcile(held), 4000);
      });
  }

  async function submit(replay = false) {
    clearReconciliation();
    const held = keyRef.current ?? makeKey();
    keyRef.current = held;
    setKey(held);
    setPhase("submitting");
    setMessage(null);
    try {
      let attachmentIds: string[] = [];
      if (!replay && pendingImages.length > 0) {
        setMessage("Uploading image attachments…");
        for (const img of pendingImages) {
          try {
            const dataBase64 = await fileToBase64(img.file);
            const uploaded = await client.uploadAttachment({
              mime: img.mime,
              data_base64: dataBase64,
              display_name: img.name,
            });
            attachmentIds.push(uploaded.id);
          } catch (uploadError) {
            const msg = uploadError instanceof Error ? uploadError.message : "Upload failed";
            setPhase("refused");
            setMessage(`Image upload failed for "${img.name}": ${msg}`);
            return;
          }
        }
      } else if (replay && requestRef.current?.attachments) {
        attachmentIds = [...requestRef.current.attachments];
      }

      const chosenAgent = target ? target.agentProfile : agentProfile.trim();
      const request: ControlSubmission =
        replay && requestRef.current
          ? requestRef.current
          : {
              ...(chosenAgent ? { agent_profile: chosenAgent } : {}),
              llm_profile: target ? target.llmProfile : llmProfile.trim(),
              executor: target ? target.executor : executor.trim(),
              objective,
              ...(name.trim() ? { name: name.trim() } : {}),
              ...(previousJob ? { previous: previousJob } : {}),
              ...(attachmentIds.length > 0 ? { attachments: attachmentIds } : {}),
            };
      requestRef.current = request;
      const result = await client.submitJob(request, held);
      if (!mounted.current || keyRef.current !== held) return;
      if (result.status === "admitted" && result.job) {
        clearPendingImages();
        if (chat) {
          resetAfterChatAdmitted();
        } else {
          setPhase("admitted");
          setMessage(
            result.created
              ? `${result.job} was created and admitted by this request.`
              : `${result.job} was already admitted under this key.`,
          );
        }
        onJob(result.job);
      } else {
        setPhase("pending");
        setMessage("Admission is still running under this key; it is not submitted again.");
        reconciliation.current = setTimeout(() => reconcile(held), 2000);
      }
    } catch (error) {
      if (!mounted.current || keyRef.current !== held) return;
      const outcome = phase === "uncertain" ? "uncertain" : outcomeOf(error);
      setPhase(outcome);
      const code =
        error instanceof ControlTransportError
          ? error.detail
            ? `${error.code}: ${error.detail}`
            : error.code
          : error instanceof ContractError
            ? "contract"
            : "unreachable";
      setMessage(
        outcome === "uncertain"
          ? `The submission did not finish (${code}). Whether it created a Job is unknown; this key is not submitted again under another.`
          : `Nothing was submitted (${code}).`,
      );
      if (outcome === "uncertain") {
        reconciliation.current = setTimeout(() => reconcile(held), 4000);
      }
    }
  }

  function retrySame() {
    // The same intent, the same key: the API answers one Job for it.
    void submit(true);
  }

  function fresh() {
    if (phase !== "admitted" && phase !== "refused") return;
    clearReconciliation();
    clearPendingImages();
    keyRef.current = null;
    requestRef.current = null;
    setKey(null);
    setPhase("editing");
    setMessage(null);
    setObjective("");
    setName("");
  }

  return (
    <section className="flex flex-col gap-4 rounded-lg border border-border bg-card p-5 text-card-foreground">
      <header className="flex flex-col gap-1">
        <h2 className="font-heading text-lg">
          {chat ? (previousJob ? "Send follow-up message" : "Send a message") : "Submit a Job"}
        </h2>
        <p className="text-xs text-muted-foreground">
          {chat
            ? previousJob
              ? `This message will be submitted as a new Job linked to ${previousJob}, carrying the conversation's prior exchanges as context.`
              : "The message becomes one Job of the selected Agent. One Idempotency-Key names it through every retry."
            : "An existing LLM Profile and Executor, by their canonical identities, and an objective. One Idempotency-Key names this submission through every retry."}
        </p>
      </header>
      {chat ? null : (
        <>
          <div className="grid gap-3 sm:grid-cols-3">
            <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
              Agent Profile (optional)
              <Input
                list="ocg-agent-profile-options"
                value={agentProfile}
                onChange={(event) => {
                  const val = event.target.value;
                  setAgentProfile(val);
                  const matchedAgent = availableAgents.find((a) => a.id === val);
                  if (matchedAgent && matchedAgent.llm_targets.length > 0 && !llmProfile) {
                    const firstLlmId = matchedAgent.llm_targets[0];
                    setLlmProfile(firstLlmId);
                    const matchedLlm = availableLlms.find((l) => l.id === firstLlmId);
                    if (matchedLlm && matchedLlm.executors.length > 0 && !executor) {
                      setExecutor(matchedLlm.executors[0]);
                    }
                  }
                }}
                placeholder="profile:<n>"
                spellCheck={false}
                disabled={settled}
              />
              <datalist id="ocg-agent-profile-options">
                {availableAgents.map((agent) => (
                  <option key={agent.id} value={agent.id}>
                    {agent.name}
                  </option>
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
              LLM Profile
              <Input
                list="ocg-llm-profile-options"
                value={llmProfile}
                onChange={(event) => {
                  const val = event.target.value;
                  setLlmProfile(val);
                  const matchedLlm = availableLlms.find((l) => l.id === val);
                  if (matchedLlm && matchedLlm.executors.length > 0 && !executor) {
                    setExecutor(matchedLlm.executors[0]);
                  }
                }}
                placeholder="profile:<n>"
                spellCheck={false}
                disabled={settled}
              />
              <datalist id="ocg-llm-profile-options">
                {availableLlms.map((llm) => (
                  <option key={llm.id} value={llm.id}>
                    {llm.name} ({llm.model})
                  </option>
                ))}
              </datalist>
            </label>
            <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
              Executor
              <Input
                value={executor}
                onChange={(event) => setExecutor(event.target.value)}
                placeholder="executor:<n>"
                spellCheck={false}
                disabled={settled}
              />
            </label>
          </div>
        </>
      )}
      <div className="flex flex-col gap-2">
        <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          <div className="flex items-center justify-between">
            <span>{chat ? "Message" : "Objective"}</span>
            <span className="text-[11px] font-normal lowercase tracking-normal text-muted-foreground">
              Paste or attach PNG/JPEG (max 4 MiB)
            </span>
          </div>
          <Textarea
            value={objective}
            onChange={(event) => setObjective(event.target.value)}
            onPaste={(event) => {
              const items = event.clipboardData?.items;
              if (!items) return;
              const files: File[] = [];
              for (let i = 0; i < items.length; i++) {
                const item = items[i];
                if (item.kind === "file" && (item.type === "image/png" || item.type === "image/jpeg")) {
                  const file = item.getAsFile();
                  if (file) files.push(file);
                }
              }
              if (files.length > 0) {
                event.preventDefault();
                addImages(files);
              }
            }}
            placeholder={chat ? "Type a message or paste an image…" : "Enter the objective text…"}
            rows={4}
            disabled={settled}
          />
        </label>

        <input
          ref={fileInputRef}
          type="file"
          accept="image/png,image/jpeg"
          multiple
          className="hidden"
          onChange={(event) => {
            if (event.target.files) {
              addImages(event.target.files);
            }
            event.target.value = "";
          }}
        />

        {pendingImages.length > 0 ? (
          <div className="flex flex-wrap gap-2.5 pt-1">
            {pendingImages.map((img) => (
              <div
                key={img.id}
                className="group relative flex flex-col items-center rounded-lg border border-border bg-muted/40 p-1.5 transition-all hover:bg-muted/60"
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img
                  src={img.previewUrl}
                  alt={img.name}
                  className="h-20 w-20 rounded-md object-cover"
                />
                <div className="mt-1 flex max-w-[80px] items-center justify-between gap-1 text-[10px] text-muted-foreground">
                  <span className="truncate" title={img.name}>{img.name}</span>
                  <span className="font-mono">{(img.sizeBytes / 1024).toFixed(0)}KB</span>
                </div>
                <button
                  type="button"
                  disabled={settled}
                  onClick={() => removeImage(img.id)}
                  className="absolute -top-1.5 -right-1.5 flex h-5 w-5 items-center justify-center rounded-full bg-destructive text-[11px] font-bold text-destructive-foreground shadow hover:opacity-90 disabled:opacity-50"
                  aria-label={`Remove image ${img.name}`}
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        ) : null}

        {imageError ? (
          <p role="alert" className="text-xs text-destructive">
            {imageError}
          </p>
        ) : null}

        {!visionSupported && pendingImages.length > 0 ? (
          <p role="alert" className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-1.5 text-xs text-amber-600 dark:text-amber-400">
            Selected model does not declare Vision capability. Submitting image attachments will be rejected by configuration check.
          </p>
        ) : null}

        <div>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={settled || pendingImages.length >= MAX_REQUEST_IMAGES}
            onClick={() => fileInputRef.current?.click()}
            className="text-xs"
          >
            {pendingImages.length > 0
              ? `Add more images (${pendingImages.length}/${MAX_REQUEST_IMAGES})`
              : "Attach image (PNG/JPEG)"}
          </Button>
        </div>
      </div>
      {chat ? null : (
        <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          Name (optional)
          <Input
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={settled}
          />
        </label>
      )}
      {disabledReason ? (
        <p role="status" className="text-xs text-muted-foreground">
          {disabledReason}
        </p>
      ) : null}
      {key ? (
        <p className="text-xs text-muted-foreground">
          Idempotency-Key: <code className="font-geist-mono">{key}</code>
        </p>
      ) : null}
      {message ? <p className="text-sm text-muted-foreground">{message}</p> : null}
      {settled ? (
        <p className="text-xs text-muted-foreground">
          Resolve this submission before starting a new Job intent. Retries and lookups keep this key.
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {phase === "editing" || phase === "refused" ? (
          <Button
            onClick={() => void submit()}
            disabled={objective.trim().length === 0 || (chat && !target) || Boolean(disabledReason)}
          >
            {phase === "editing" ? (chat ? "Send" : "Submit Job") : chat ? "Send again" : "Submit again"}
          </Button>
        ) : null}
        {phase === "uncertain" ? (
          <Button variant="outline" onClick={retrySame}>
            Retry with this key
          </Button>
        ) : null}
        {phase === "uncertain" || phase === "pending" ? (
          <Button
            variant="outline"
            onClick={() => {
              if (key) {
                clearReconciliation();
                reconcile(key);
              }
            }}
          >
            Look the key up
          </Button>
        ) : null}
        {phase !== "editing" ? (
          <Button variant="ghost" disabled={settled} onClick={fresh}>
            {chat ? "New message" : "New submission"}
          </Button>
        ) : null}
      </div>
    </section>
  );
}
