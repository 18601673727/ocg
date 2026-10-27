"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { useEffect, useState, type FormEvent } from "react";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function ConversationLimitsEditor({ conversation }: { conversation: string }) {
  const { control } = useOcgRuntime();
  const { t } = useI18n();
  const [limits, setLimits] = useState<Api.ConversationLimits | null>(null);
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!control) return;
    const controller = new AbortController();
    void control
      .readConversationLimits(conversation, controller.signal)
      .then(setLimits, (cause) => {
        if (!controller.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      });
    return () => controller.abort();
  }, [control, conversation]);

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!control || !limits) return;
    setBusy(true);
    setError(undefined);
    try {
      const { max_depth, max_auto_turns, max_iterations } = limits;
      await control.editConversation(conversation, {
        limits: { max_depth, max_auto_turns, max_iterations },
      });
      setLimits(await control.readConversationLimits(conversation));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="space-y-3">
      <h3 className="text-xs font-semibold text-muted-foreground">{t("chat.limits.title")}</h3>
      {error && <DismissibleNotice message={error} className="text-xs" />}
      {limits && (
        <form className="space-y-3" onSubmit={(event) => void save(event)}>
          <p className="text-xs text-muted-foreground">
            {t("chat.limits.depth", {
              depth: limits.depth,
              remaining: Math.max(0, limits.max_depth - limits.depth),
            })}
          </p>
          {(
            [
              ["max_depth", "chat.limits.maxDepth", 16],
              ["max_auto_turns", "chat.limits.maxAutoTurns", 255],
              ["max_iterations", "chat.limits.maxIterations", 10000],
            ] as const
          ).map(([field, label, max]) => (
            <label key={field} className="flex items-center justify-between gap-3 text-xs">
              <span>{t(label)}</span>
              <Input
                aria-label={t(label)}
                type="number"
                min={0}
                max={max}
                step={1}
                required
                disabled={busy}
                className="h-8 w-20 shrink-0 text-xs"
                value={Number.isFinite(limits[field]) ? limits[field] : ""}
                onChange={(event) =>
                  setLimits({ ...limits, [field]: event.currentTarget.valueAsNumber })
                }
              />
            </label>
          ))}
          <p className="text-xs text-muted-foreground">{t("chat.limits.hint")}</p>
          <Button type="submit" variant="outline" size="sm" disabled={busy}>
            {t("common.save")}
          </Button>
        </form>
      )}
    </section>
  );
}
