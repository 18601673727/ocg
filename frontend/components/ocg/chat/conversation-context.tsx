import { createEffect, createMemo, createSignal } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Select, SelectItem } from "@/components/ui/select";
import { elementRef } from "@/lib/ui-state";
import type { ContextStatus, ProfilesSnapshot } from "../contracts";
import { type TranslateFn, useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { useOcgRuntime } from "../runtime/runtime-context";
import { describeError, selectableLlmProfiles } from "../settings/settings-parts";

type Status = ContextStatus;

function formatTokens(value: number): string {
  if (value < 1000) return String(value);
  if (value < 1_000_000) return `${(value / 1000).toFixed(1)}K`;
  return `${(value / 1_000_000).toFixed(1)}M`;
}

function compactionNotice(reason: string, t: TranslateFn): string {
  switch (reason) {
    case "no settled source":
      return t("context.noSettledSource");
    case "no eligible settled source":
      return t("context.sourceUnavailable");
    case "recent turns are protected":
      return t("context.recentProtected");
    case "summary source tool evidence is not supported":
      return t("context.sourceTools");
    case "summary source attachment unavailable":
      return t("context.sourceAttachment");
    case "summary verification record exceeds byte limit":
    case "summary verification byte budget exceeded":
    case "summary verification output budget exceeded":
    case "summary source text exceeds byte limit":
    case "summary source output overflow":
      return t("context.sourceLimit");
    case "Decision Jobs cannot be summary sources":
    case "summary source Job has no settled execution":
    case "summary source is not a settled conversation Job":
    case "summary source output timing is inconsistent":
      return t("context.sourceExecution");
    case "summary Executor no longer supports the source operation":
      return t("context.sourceExecutor");
    default:
      return reason;
  }
}

export function ConversationContext(props: {
  conversation: string;
  agentProfile?: string;
  actionsSupported?: boolean;
}) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  const [profiles, setProfiles] = createSignal<ProfilesSnapshot>();
  const [status, setStatus] = createSignal<Status>();
  const [model, setModel] = createSignal("");
  const [error, setError] = createSignal<string>();
  const [notice, setNotice] = createSignal<string>();
  const [busy, setBusy] = createSignal(false);
  const [retrying, setRetrying] = createSignal(false);
  const [refresh, setRefresh] = createSignal(0);
  const [older, setOlder] = createSignal<Status["work"]>([]);
  const [before, setBefore] = createSignal<string | null>(null);
  const request = elementRef<{ model: string; key: string } | null>(null);
  const initialized = elementRef(false);
  createEffect(
    () => [api, props.conversation, refresh(), i18n.t],
    () => {
      if (!api) return;
      const controller = new AbortController();
      let reading = false;
      async function tick() {
        if (reading) return;
        reading = true;
        try {
          const state = await api!.readContextStatus(props.conversation, {
            signal: controller.signal,
          });
          if (controller.signal.aborted) return;
          setStatus(state);
          if (!initialized.current) {
            setModel(state.summarizer ?? "");
            setBefore(state.next_before);
            initialized.current = true;
          }
        } catch (cause) {
          if (!controller.signal.aborted) setError(describeError(cause, i18n.t));
        } finally {
          reading = false;
        }
      }
      void api.readProfiles(controller.signal).then(
        (value) => {
          if (!controller.signal.aborted) setProfiles(value);
        },
        (cause) => {
          if (!controller.signal.aborted) setError(describeError(cause, i18n.t));
        },
      );
      const unsubscribe = api.subscribeCatalog(
        () => void tick(),
        (cause) => {
          if (!controller.signal.aborted) setError(describeError(cause, i18n.t));
        },
      );
      void tick();
      return () => {
        controller.abort();
        unsubscribe();
      };
    },
  );
  async function act(action: () => Promise<void>) {
    setBusy(true);
    setError(undefined);
    setNotice(undefined);
    try {
      await action();
      setRefresh((value) => value + 1);
    } catch (cause) {
      setError(describeError(cause, i18n.t));
    } finally {
      setBusy(false);
    }
  }
  const work = createMemo(() => [
    ...(status()?.work ?? []),
    ...older().filter((work) => !status()?.work.some((current) => current.job === work.job)),
  ]);
  const running = createMemo(() => busy() || Boolean(status()?.steer_blocked_by));
  const agent = createMemo(() =>
    props.agentProfile && profiles()
      ? profiles()!.agent_profiles.find((p) => p.id === props.agentProfile)
      : undefined,
  );
  const policySummarizer = createMemo(() => status()?.policy.summarization_model);
  const allowedModelIds = createMemo(
    () => new Set([policySummarizer(), ...(agent()?.llm_targets ?? [])]),
  );
  const selectableModels = createMemo(() =>
    profiles()
      ? selectableLlmProfiles(
          profiles()!.llm_profiles.filter((p) => allowedModelIds().has(p.id)),
          model(),
        )
      : [],
  );
  async function compact() {
    if (!api || !model()) return;
    await act(async () => {
      if (!request.current || request.current.model !== model())
        request.current = { model: model(), key: crypto.randomUUID() };
      setRetrying(true);
      try {
        const result = await api.compactConversation(
          props.conversation,
          model(),
          request.current.key,
        );
        request.current = null;
        setNotice(result.reason ? compactionNotice(result.reason, i18n.t) : undefined);
        setStatus(await api.readContextStatus(props.conversation));
      } finally {
        setRetrying(false);
      }
    });
  }
  return (
    <>
      <div class="grid grid-cols-[1fr_auto] items-center gap-x-4 gap-y-2">
        <dt class="text-muted-foreground">{i18n.t("context.used")}</dt>
        <dd class="flex items-center gap-2">
          <span class="tabular-nums">
            {status()?.estimated_tokens == null ? "—" : formatTokens(status()!.estimated_tokens!)}
          </span>
          <button
            type="button"
            class="text-foreground underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-ring disabled:cursor-default disabled:text-muted-foreground"
            disabled={!api || !status() || running()}
            onClick={() => (model() ? void compact() : setOpen(true))}
          >
            {i18n.t(running() ? "context.running" : "context.manual")}
          </button>
        </dd>
        {error() && !open() && (
          <dd class="col-span-2">
            <DismissibleNotice message={error()!} />
          </dd>
        )}
        {notice() && !open() && (
          <dd class="col-span-2" role="status">
            {notice()}
          </dd>
        )}
      </div>
      <Dialog open={open()} onOpenChange={setOpen}>
        <DialogContent class="flex max-h-[90dvh] flex-col overflow-hidden sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>{i18n.t("context.title")}</DialogTitle>
            <DialogDescription>{i18n.t("context.manualHint")}</DialogDescription>
          </DialogHeader>
          <ScrollArea
            class="min-h-0 flex-1"
            viewportProps={{ class: "ocg-subtle-scrollbar" }}
            contentProps={{ class: "space-y-6" }}
          >
            {error() && <DismissibleNotice message={error()!} />}
            {notice() && (
              <p role="status" class="text-sm">
                {notice()}
              </p>
            )}
            {status() && (
              <div class="space-y-2 text-xs text-muted-foreground">
                <p>
                  {i18n.t("context.usage", {
                    tokens: status()!.estimated_tokens ?? i18n.t("context.unknown"),
                    capacity: status()!.foreground_model?.capacity ?? i18n.t("context.unknown"),
                    mode: status()!.mode,
                    source: status()!.policy_source,
                  })}
                </p>
                <p>{i18n.t("context.preparedMode", { mode: status()!.prediction_mode })}</p>
                {status()!.predictive_unavailable && status()!.predictive && (
                  <p>
                    {status()!.predictive_unavailable === "foreground model capacity is unknown"
                      ? i18n.t("context.capacityUnavailable")
                      : status()!.predictive_unavailable}
                  </p>
                )}
              </div>
            )}
            {profiles() && status() && (
              <>
                <WorkspaceLink href="/settings/system" class="text-sm underline">
                  {i18n.t("context.globalTitle")}
                </WorkspaceLink>
                <div class="space-y-2 border-t border-border pt-3">
                  <label class="grid gap-1 text-xs">
                    {i18n.t("context.summarizer")}
                    <Select
                      disabled={busy()}
                      value={model()}
                      items={[
                        { value: "", label: i18n.t("context.summarizer") },
                        ...selectableModels().map((model) => ({
                          value: model.id,
                          label: model.enabled
                            ? model.name
                            : `${model.name} (${i18n.t("settings.llm.disabled")})`,
                        })),
                      ]}
                      onValueChange={(next) => {
                        if (next === null) return;
                        setModel(next);
                        request.current = null;
                        setRetrying(false);
                      }}
                      class="min-h-10 w-full rounded-md bg-background px-2 text-sm"
                    >
                      <SelectItem value="">{i18n.t("context.summarizer")}</SelectItem>
                      {selectableModels().map((model) => (
                        <SelectItem value={model.id} disabled={!model.enabled}>
                          {model.name}
                          {!model.enabled && ` (${i18n.t("settings.llm.disabled")})`}
                        </SelectItem>
                      ))}
                    </Select>
                  </label>
                  <Button
                    disabled={busy() || !model()}
                    variant="outline"
                    onClick={() => void compact()}
                  >
                    {busy()
                      ? i18n.t("context.saving")
                      : retrying()
                        ? i18n.t("context.retry")
                        : i18n.t("context.manual")}
                  </Button>
                </div>
              </>
            )}
            {work().length > 0 && (
              <ol class="space-y-2 border-t border-border pt-3">
                {work().map((work) => (
                  <li class="space-y-1 rounded-md bg-muted p-2 text-xs [overflow-wrap:anywhere]">
                    <p>
                      {work.kind === "summarization"
                        ? `${i18n.t("context.summarizer")} · ${work.stage}`
                        : i18n.t("context.predictor")}{" "}
                      · {work.state}
                    </p>
                    {work.kind === "summarization" && work.failure && (
                      <p class="text-destructive">{work.failure}</p>
                    )}
                    <p class="font-mono text-muted-foreground">{work.job}</p>
                    {work.kind === "summarization" && (
                      <>
                        <p>
                          {i18n.t("context.sourceRange", { count: work.source.turn_count })} ·{" "}
                          {work.source.first.turn} → {work.source.through.turn}
                        </p>
                        {work.prediction && (
                          <p>
                            {i18n.t("context.reservation", {
                              requests: work.reserved_requests,
                              tokens: work.reserved_tokens,
                            })}
                          </p>
                        )}
                      </>
                    )}
                    {work.kind === "summarization" &&
                      ["queued", "preparing", "prepared", "part_ready"].includes(work.state) && (
                        <Button
                          size="sm"
                          variant="ghost"
                          disabled={
                            busy() ||
                            !(props.actionsSupported === undefined ? true : props.actionsSupported)
                          }
                          title={
                            (props.actionsSupported === undefined ? true : props.actionsSupported)
                              ? undefined
                              : i18n.t("context.nativeUnsupported")
                          }
                          onClick={() =>
                            void act(async () => {
                              await api!.stopConversation(work.conversation);
                            })
                          }
                        >
                          {i18n.t("context.stop")}
                        </Button>
                      )}
                  </li>
                ))}
              </ol>
            )}
            {before() && (
              <Button
                variant="outline"
                disabled={busy()}
                onClick={() =>
                  void act(async () => {
                    const page = await api!.readContextStatus(props.conversation, {
                      before: before()!,
                    });
                    if (page.next_before && page.next_before >= before()!)
                      throw new Error("Context cursor did not advance");
                    setOlder((items) => [...items, ...page.work]);
                    setBefore(page.next_before);
                  })
                }
              >
                {i18n.t("context.more")}
              </Button>
            )}
          </ScrollArea>
        </DialogContent>
      </Dialog>
    </>
  );
}
