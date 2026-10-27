"use client";

import { useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { ControlProfilesSnapshot, ControlProviderModel } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { ProtocolSelect, type LlmProtocol } from "./protocol-select";
import { describeError } from "./settings-parts";

export function ProviderModelImport({
  client,
  snapshot,
  onImported,
}: {
  client: HttpCanonicalControlClient;
  snapshot: ControlProfilesSnapshot | null;
  onImported: (message: string) => void;
}) {
  return <ImportContent client={client} snapshot={snapshot} onImported={onImported} />;
}

function ImportContent({
  client,
  snapshot,
  onImported,
}: {
  client: HttpCanonicalControlClient;
  snapshot: ControlProfilesSnapshot | null;
  onImported: (message: string) => void;
}) {
  const { t } = useI18n();
  const providers = snapshot?.providers ?? [];
  const [sourceChoice, setSourceChoice] = useState<string>();
  const sourceId = sourceChoice ?? providers[0]?.profile_ids[0] ?? "new";
  const [newName, setNewName] = useState("");
  const [newProtocol, setNewProtocol] = useState<LlmProtocol>("openai_chat_completions");
  const [newEndpoint, setNewEndpoint] = useState("");
  const [newSecret, setNewSecret] = useState("");
  const [newReference] = useState(
    () => `OCG_IMPORT_${crypto.randomUUID().replaceAll("-", "").toUpperCase()}_API_KEY`,
  );
  const newConnection =
    sourceId === "new" && newName.trim() && newEndpoint.trim()
      ? {
          protocol: newProtocol,
          name: newName.trim(),
          endpoint: newEndpoint.trim(),
          ...(newSecret.trim()
            ? { credential: newSecret.trim(), credential_ref: newReference }
            : {}),
        }
      : null;
  const provider = providers.find((entry) => entry.profile_ids.includes(sourceId));
  const [models, setModels] = useState<ControlProviderModel[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [query, setQuery] = useState("");
  const [revision, setRevision] = useState(0);
  const [loading, setLoading] = useState(Boolean(provider));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // A committed import stays committed even when the following catalog refresh fails.
  const [result, setResult] = useState<string | null>(null);
  const endpoint =
    sourceId === "new"
      ? newConnection?.endpoint
      : (provider?.endpoint ??
        (provider?.protocol === "anthropic_messages"
          ? "https://api.anthropic.com/v1"
          : "https://api.openai.com/v1"));
  const protocol = sourceId === "new" ? newConnection?.protocol : provider?.protocol;
  const credentialRef = sourceId === "new" ? undefined : provider?.credential_ref;
  const credential = sourceId === "new" ? newConnection?.credential : undefined;
  const canDiscover = sourceId !== "new" || newConnection !== null;
  useEffect(() => {
    if (!endpoint || !canDiscover) return;
    const controller = new AbortController();
    let active = true;
    const timer = setTimeout(
      () => {
        setLoading(true);
        void client
          .discoverProviderModels(
            { endpoint, protocol, credential_ref: credentialRef, credential },
            controller.signal,
          )
          .then((answer) => {
            if (active)
              setModels(
                [...new Map(answer.models.map((model) => [model.id, model])).values()].sort(
                  (a, b) => a.id.localeCompare(b.id),
                ),
              );
          })
          .catch((reason) => {
            if (active) setError(describeError(reason, t));
          })
          .finally(() => {
            if (active) setLoading(false);
          });
      },
      sourceId === "new" ? 500 : 0,
    );
    return () => {
      active = false;
      clearTimeout(timer);
      controller.abort();
    };
  }, [canDiscover, client, credential, credentialRef, endpoint, protocol, revision, sourceId, t]);
  const existing = new Set(provider?.models ?? []);
  const importable = models.filter((model) => !existing.has(model.id));
  const matching = useMemo(
    () =>
      models.filter((model) =>
        model.id.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()),
      ),
    [models, query],
  );
  function selectSource(id: string) {
    setSourceChoice(id);
    setModels([]);
    setSelected(new Set());
    setQuery("");
    setError(null);
    setResult(null);
    setLoading(id !== "new");
    setRevision((value) => value + 1);
  }
  function refreshSource(id: string) {
    selectSource(id);
  }
  function changeNewConnection(update: () => void) {
    update();
    setModels([]);
    setSelected(new Set());
    setLoading(false);
    setError(null);
    setResult(null);
  }
  async function save() {
    const source = sourceId === "new" ? newConnection : sourceId;
    if (!source) return;
    setSaving(true);
    setError(null);
    try {
      const answer = await client.importModels(
        source,
        importable.filter((model) => selected.has(model.id)),
        ["chat"],
      );
      const message = t("settings.llm.imported", {
        created: answer.created,
        existing: answer.existing,
      });
      setResult(message);
      setSelected(new Set());
      onImported(message);
      if (sourceId === "new") {
        setNewName("");
        setNewEndpoint("");
        setNewSecret("");
        setModels([]);
      }
    } catch (reason) {
      setError(describeError(reason, t));
    } finally {
      setSaving(false);
    }
  }
  return (
    <div className="flex h-[70dvh] min-h-[32rem] flex-col gap-6">
      <div className="grid min-h-0 flex-1 grid-cols-[14rem_minmax(0,1fr)] gap-5 overflow-hidden">
        <div
          role="group"
          aria-label={t("settings.llm.provider")}
          className="ocg-subtle-scrollbar flex min-h-0 min-w-0 flex-col gap-2 overflow-y-auto overscroll-contain border-r border-border pr-4"
        >
          {providers.map((entry) => {
            const id = entry.profile_ids[0];
            const refreshing = sourceId === id && loading;
            return (
              <div key={id} className="relative">
                <Button
                  variant={entry === provider ? "selected" : "outline"}
                  size="sm"
                  aria-pressed={entry === provider}
                  disabled={saving}
                  className="h-auto min-h-9 w-full max-w-full justify-start py-2 pr-9 whitespace-normal text-left"
                  onClick={() => {
                    setNewSecret("");
                    selectSource(id);
                  }}
                >
                  <span className="min-w-0 break-words">{entry.name}</span>
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label={t("settings.llm.fetchModels")}
                  title={t("settings.llm.fetchModels")}
                  disabled={saving || refreshing}
                  className="absolute top-1/2 right-1 -translate-y-1/2"
                  onClick={() => refreshSource(id)}
                >
                  <RefreshCw className={refreshing ? "animate-spin" : undefined} />
                </Button>
              </div>
            );
          })}
          <Button
            variant={sourceId === "new" ? "selected" : "outline"}
            size="sm"
            aria-pressed={sourceId === "new"}
            disabled={saving}
            onClick={() => selectSource("new")}
          >
            {t("settings.llm.newProvider")}
          </Button>
        </div>
        <div className="flex min-h-0 min-w-0 flex-col gap-3 overflow-hidden">
          {sourceId === "new" && (
            <div className="grid shrink-0 grid-cols-1 gap-3 sm:grid-cols-2">
              <ProtocolSelect
                value={newProtocol}
                disabled={saving}
                onChange={(value) => changeNewConnection(() => setNewProtocol(value))}
              />
              <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                {t("settings.llm.provider")}
                <Input
                  value={newName}
                  disabled={saving}
                  onChange={(event) => changeNewConnection(() => setNewName(event.target.value))}
                  placeholder="Provider"
                  autoComplete="off"
                />
              </label>
              <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                {t("settings.llm.importEndpoint")}
                <Input
                  value={newEndpoint}
                  disabled={saving}
                  onChange={(event) =>
                    changeNewConnection(() => setNewEndpoint(event.target.value))
                  }
                  placeholder="https://api.example.com/v1"
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
              <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                {t("settings.llm.apiKey")}
                <Input
                  type="password"
                  value={newSecret}
                  disabled={saving}
                  onChange={(event) => changeNewConnection(() => setNewSecret(event.target.value))}
                  autoComplete="new-password"
                  spellCheck={false}
                />
              </label>
            </div>
          )}
          <Input
            aria-label={t("settings.llm.searchModels")}
            placeholder={t("settings.llm.searchModels")}
            value={query}
            disabled={saving}
            onChange={(event) => setQuery(event.target.value)}
          />
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <Button
              variant="ghost"
              size="xs"
              disabled={saving || !selected.size}
              onClick={() => setSelected(new Set())}
            >
              {t("settings.llm.clearSelection")}
            </Button>
            <span className="ml-auto tabular-nums">
              {selected.size} / {importable.length}
            </span>
          </div>
          <div
            aria-busy={loading}
            className="ocg-subtle-scrollbar min-h-24 min-w-0 flex-1 overflow-y-auto overscroll-contain rounded-md border border-border"
          >
            {loading ? (
              <p role="status" className="p-3 text-muted-foreground">
                {t("settings.llm.fetching")}
              </p>
            ) : matching.length === 0 ? (
              <p className="p-3 text-muted-foreground">{t("settings.llm.noModels")}</p>
            ) : (
              matching.map((model) => (
                <label
                  key={model.id}
                  className="flex cursor-pointer items-start gap-3 border-b border-border/50 p-3 last:border-b-0 hover:bg-muted/40 has-disabled:cursor-default has-disabled:text-muted-foreground"
                >
                  <input
                    type="checkbox"
                    className="mt-0.5 size-4 shrink-0 accent-primary"
                    checked={existing.has(model.id) || selected.has(model.id)}
                    disabled={saving || existing.has(model.id)}
                    onChange={() =>
                      setSelected((current) => {
                        const next = new Set(current);
                        if (!next.delete(model.id)) next.add(model.id);
                        return next;
                      })
                    }
                  />
                  <span className="min-w-0 flex-1 break-all">
                    {model.id}
                    <span className="mt-1 block text-xs text-muted-foreground">
                      {t("chat.effort")}:{" "}
                      {model.reasoning_efforts === null
                        ? t("settings.llm.effortsUnknown")
                        : model.reasoning_efforts.join(" / ") || t("chat.effortUnsupported")}
                    </span>
                    {model.context_window ? (
                      <span className="mt-1 block text-xs text-muted-foreground">
                        {model.context_window.toLocaleString()} Token
                      </span>
                    ) : null}
                  </span>
                  {existing.has(model.id) && (
                    <span className="shrink-0 text-xs">{t("settings.llm.alreadyAdded")}</span>
                  )}
                </label>
              ))
            )}
          </div>
        </div>
      </div>
      {error && (
        <p role="alert" className="break-words text-sm text-destructive">
          {error}
        </p>
      )}
      {result && (
        <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
          {result}
        </p>
      )}
      <div className="flex justify-end">
        <Button disabled={saving || loading || !selected.size} onClick={() => void save()}>
          {saving
            ? t("settings.llm.importing")
            : t("settings.llm.importSelected", {
                selected: selected.size,
                total: importable.length,
              })}
        </Button>
      </div>
    </div>
  );
}
