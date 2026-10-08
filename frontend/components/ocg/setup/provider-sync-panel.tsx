"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, Check, Loader2, RefreshCcw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useI18n } from "../i18n";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient, type ProfileView } from "../profile/profile-client";
import type { SetupModel } from "../contracts";
import { createSetupClient } from "./setup-client";

/** What a sync found for one provider, relative to the models enabled in the Profile. */
interface SyncResult {
  revision: string;
  /** Listed by the provider but not enabled yet. */
  fresh: SetupModel[];
  /** Enabled in the Profile but no longer listed by the provider. */
  unavailable: string[];
  /** Model keys the user ticked to enable. */
  picked: Set<string>;
}

function discoveredLabel(discoveredAt: number | undefined, never: string): string {
  return discoveredAt ? new Date(discoveredAt * 1000).toLocaleString() : never;
}

/**
 * Re-lists each provider's models on demand. The backend refresh keeps enabled
 * models' labels and metadata current; this panel reports what changed so a
 * provider's catalog updates are visible and can be adopted deliberately.
 * Applying changes keeps the current default model.
 */
export function ProviderSyncPanel({ profileRefresh = 0 }: { profileRefresh?: number }) {
  const { t } = useI18n();
  const controlUrl = useOcgControlUrl();
  const profileClient = useMemo(() => (controlUrl ? createProfileClient(controlUrl, fetch) : null), [controlUrl]);
  const setupClient = useMemo(() => (controlUrl ? createSetupClient(controlUrl, fetch) : null), [controlUrl]);
  const [view, setView] = useState<ProfileView | null>(null);
  const [results, setResults] = useState<Record<string, SyncResult>>({});
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    if (!profileClient) return;
    let active = true;
    void profileClient.read().then((next) => { if (active) setView(next); })
      .catch((cause) => { if (active) setError(cause instanceof Error ? cause.message : String(cause)); });
    return () => { active = false; };
  }, [profileClient, profileRefresh]);

  const sync = useCallback(async (providerKey: string) => {
    if (!profileClient || !setupClient || busy) return;
    setBusy(providerKey);
    setError(null);
    setNotice(null);
    try {
      // The revision must be current: another change since the last read is refused.
      const current = await profileClient.read();
      if (!current.revision) throw new Error(t("settings.sync.noRevision"));
      const response = await setupClient.refreshModels(providerKey, current.revision);
      const after = await profileClient.read();
      setView(after);
      const enabled = Object.entries(after.profile?.models ?? {}).filter(([, model]) => model.provider === providerKey);
      const enabledKeys = new Set(enabled.map(([key]) => key));
      const listedKeys = new Set(response.models.map((model) => model.key));
      setResults((previous) => ({
        ...previous,
        [providerKey]: {
          revision: response.revision,
          fresh: response.models.filter((model) => !enabledKeys.has(model.key)),
          unavailable: enabled.map(([key]) => key).filter((key) => !listedKeys.has(key)),
          picked: new Set(),
        },
      }));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.refreshFailed"));
    } finally {
      setBusy(null);
    }
  }, [profileClient, setupClient, busy, t]);

  const apply = useCallback(async (providerKey: string, result: SyncResult, models: SetupModel[]) => {
    if (!profileClient || !setupClient || !view?.profile || busy) return;
    setBusy(providerKey);
    setError(null);
    try {
      const keep = Object.entries(view.profile.models)
        .filter(([key, model]) => model.provider === providerKey && !result.unavailable.includes(key))
        .map(([key, model]) => ({ key, id: model.id }));
      const add = models.filter((model) => result.picked.has(model.key)).map((model) => ({ key: model.key, id: model.id }));
      // No default is sent: syncing must not change what new Chats use.
      await setupClient.saveModels(providerKey, [...keep, ...add], undefined, result.revision);
      setView(await profileClient.read());
      setResults((previous) => Object.fromEntries(Object.entries(previous).filter(([key]) => key !== providerKey)));
      setNotice(t("settings.sync.applied"));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.saveFailed"));
    } finally {
      setBusy(null);
    }
  }, [profileClient, setupClient, view, busy, t]);

  function toggle(providerKey: string, modelKey: string) {
    setResults((previous) => {
      const result = previous[providerKey];
      if (!result) return previous;
      const picked = new Set(result.picked);
      if (!picked.delete(modelKey)) picked.add(modelKey);
      return { ...previous, [providerKey]: { ...result, picked } };
    });
  }

  const providers = Object.entries(view?.profile?.providers ?? {}).filter(
    ([, provider]) => Boolean(provider.endpoint) && (provider.protocol ?? "openai_compatible") !== "anthropic",
  );
  if (!controlUrl || (view && providers.length === 0)) return null;

  return (
    <div className="space-y-4 rounded-lg border border-border p-4" aria-busy={busy !== null}>
      <div>
        <h3 className="text-sm font-semibold">{t("settings.sync.title")}</h3>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">{t("settings.sync.desc")}</p>
      </div>
      {error && <p role="alert" className="break-words rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">{error}</p>}
      {notice && <p role="status" className="flex items-start gap-2 rounded-md border border-emerald-500/30 bg-emerald-500/5 p-3 text-sm"><Check className="mt-0.5 size-4 shrink-0 text-emerald-600" />{notice}</p>}
      {!view && !error && <p role="status" className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="size-4 animate-spin" />{t("profile.loading")}</p>}
      <ul className="space-y-3">
        {providers.map(([key, provider]) => {
          const result = results[key];
          const enabledCount = Object.values(view?.profile?.models ?? {}).filter((model) => model.provider === key).length;
          return (
            <li key={key} className="space-y-3 rounded-lg border border-border bg-muted/10 p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="min-w-0">
                  <p className="break-all text-sm font-medium">{provider.label}</p>
                  <p className="text-xs text-muted-foreground">
                    {t("settings.sync.status", {
                      listed: provider.catalog?.models.length ?? 0,
                      enabled: enabledCount,
                      when: discoveredLabel(provider.catalog?.discovered_at, t("settings.sync.never")),
                    })}
                  </p>
                </div>
                <Button size="sm" variant="outline" className="min-h-11 rounded-md tracking-normal normal-case" disabled={busy !== null} onClick={() => void sync(key)}>
                  {busy === key ? <Loader2 className="size-4 animate-spin" /> : <RefreshCcw className="size-4" />}{t("settings.sync.action")}
                </Button>
              </div>
              {result && result.fresh.length === 0 && result.unavailable.length === 0 && (
                <p role="status" className="text-sm text-muted-foreground">{t("settings.sync.upToDate")}</p>
              )}
              {result && result.unavailable.length > 0 && (
                <div role="status" className="space-y-1 rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-sm">
                  <p className="flex items-center gap-2 font-medium"><AlertTriangle className="size-4 text-amber-600" />{t("settings.sync.unavailable", { count: result.unavailable.length })}</p>
                  <p className="break-all font-mono text-xs text-muted-foreground">{result.unavailable.join(", ")}</p>
                  <p className="text-xs text-muted-foreground">{t("settings.sync.unavailableNote")}</p>
                </div>
              )}
              {result && result.fresh.length > 0 && (
                <div className="space-y-2">
                  <p className="text-[13px] font-medium">{t("settings.sync.fresh", { count: result.fresh.length })}</p>
                  <ul className="max-h-60 space-y-1 overflow-y-auto rounded-md border border-border p-2">
                    {result.fresh.map((model) => (
                      <li key={model.key}>
                        <label className="flex min-h-11 cursor-pointer items-center gap-2 rounded-md px-2 text-sm hover:bg-muted">
                          <input type="checkbox" checked={result.picked.has(model.key)} onChange={() => toggle(key, model.key)} disabled={busy !== null} aria-label={t("setup.selectNamedModel", { model: model.label })} />
                          <span className="min-w-0 break-all">{model.label}</span>
                        </label>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {result && (result.picked.size > 0 || result.unavailable.length > 0) && (
                <Button size="sm" className="min-h-11 rounded-md tracking-normal normal-case" disabled={busy !== null} onClick={() => void apply(key, result, result.fresh)}>
                  {busy === key && <Loader2 className="size-4 animate-spin" />}{t("settings.sync.apply")}
                </Button>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
