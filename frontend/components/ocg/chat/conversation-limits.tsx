import { createEffect, createSignal } from "solid-js";
import type * as Api from "@/components/ocg/contracts/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { type FormEvent } from "@/lib/ui-state";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { useOcgRuntime } from "../runtime/runtime-context";

export function ConversationLimitsEditor(props: { conversation: string }) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const [limits, setLimits] = createSignal<Api.ConversationLimits | null>(null);
  const [error, setError] = createSignal<string>();
  const [busy, setBusy] = createSignal(false);
  createEffect(
    () => [api, props.conversation],
    () => {
      if (!api) return;
      const controller = new AbortController();
      void api
        .readConversationLimits(props.conversation, controller.signal)
        .then(setLimits, (cause) => {
          if (!controller.signal.aborted)
            setError(cause instanceof Error ? cause.message : String(cause));
        });
      return () => controller.abort();
    },
  );

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!api || !limits()) return;
    setBusy(true);
    setError(undefined);
    try {
      const { max_depth, max_auto_turns, max_iterations } = limits()!;
      await api.editConversation(props.conversation, {
        limits: { max_depth, max_auto_turns, max_iterations },
      });
      setLimits(await api.readConversationLimits(props.conversation));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section class="space-y-3">
      <h3 class="text-xs font-semibold text-muted-foreground">{i18n.t("chat.limits.title")}</h3>
      {error() && <DismissibleNotice message={error()!} class="text-xs" />}
      {limits() && (
        <form class="space-y-3" onSubmit={(event) => void save(event)}>
          <p class="text-xs text-muted-foreground">
            {i18n.t("chat.limits.depth", {
              depth: limits()!.depth,
              remaining: Math.max(0, limits()!.max_depth - limits()!.depth),
            })}
          </p>
          {(
            [
              ["max_depth", "chat.limits.maxDepth", 16],
              ["max_auto_turns", "chat.limits.maxAutoTurns", 255],
              ["max_iterations", "chat.limits.maxIterations", 10000],
            ] as const
          ).map(([field, label, max]) => (
            <label class="flex items-center justify-between gap-3 text-xs">
              <span>{i18n.t(label)}</span>
              <Input
                aria-label={i18n.t(label)}
                type="number"
                min={0}
                max={max}
                step={1}
                required
                disabled={busy()}
                class="h-8 w-20 shrink-0 text-xs"
                value={Number.isFinite(limits()![field]) ? limits()![field] : ""}
                onInput={(event) =>
                  setLimits((current) =>
                    current ? { ...current, [field]: event.currentTarget.valueAsNumber } : current,
                  )
                }
              />
            </label>
          ))}
          <p class="text-xs text-muted-foreground">{i18n.t("chat.limits.hint")}</p>
          <Button type="submit" variant="outline" size="sm" disabled={busy()}>
            {i18n.t("common.save")}
          </Button>
        </form>
      )}
    </section>
  );
}
