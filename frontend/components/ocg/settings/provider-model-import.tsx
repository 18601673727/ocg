import { RefreshCw, Trash2 } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ariaState } from "@/lib/ui-state";
import type { ProfilesSnapshot } from "../contracts";
import type { ProviderModel } from "../contracts/api";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";
import { draftsToInputs, type HeaderDraft, type HeaderDraftError } from "./header-drafts";
import { HeadersEditor } from "./headers-editor";
import { type LlmProtocol, ProtocolSelect } from "./protocol-select";
import { ApiKeyInput, describeError } from "./settings-parts";

function sourceName(name: string) {
  return name.replace(/\s+provider\s*$/i, "");
}

export function ProviderModelImport(props: {
  client: ApiClient;
  snapshot: ProfilesSnapshot | null;
  onImported: (message: string) => void;
}) {
  return (
    <ImportContent client={props.client} snapshot={props.snapshot} onImported={props.onImported} />
  );
}

function ImportContent(props: {
  client: ApiClient;
  snapshot: ProfilesSnapshot | null;
  onImported: (message: string) => void;
}) {
  const i18n = useI18n();
  const providers = createMemo(() => props.snapshot?.providers ?? []);
  const [sourceChoice, setSourceChoice] = createSignal<string>();
  const sourceId = createMemo(() => sourceChoice() ?? providers()[0]?.id ?? "new");
  const [newName, setNewName] = createSignal("");
  const [newProtocol, setNewProtocol] = createSignal<LlmProtocol>("openai_chat_completions");
  const [newEndpoint, setNewEndpoint] = createSignal("");
  const [newSecret, setNewSecret] = createSignal("");
  const [newHeaders, setNewHeaders] = createSignal<HeaderDraft[]>([]);
  const newHeaderRequest = createMemo(() => draftsToInputs(newHeaders()));
  const newHeaderError = createMemo<HeaderDraftError | undefined>(() => {
    const request = newHeaderRequest();
    return "error" in request ? request.error : undefined;
  });
  const newHeaderInputs = createMemo(() => {
    const request = newHeaderRequest();
    return "inputs" in request ? request.inputs : [];
  });
  const [newReference] = createSignal(
    (() => `OCG_IMPORT_${crypto.randomUUID().replaceAll("-", "").toUpperCase()}_API_KEY`)(),
  );
  const newConnection = createMemo(() =>
    sourceId() === "new" && newName().trim() && newEndpoint().trim() && !newHeaderError!()
      ? {
          protocol: newProtocol(),
          name: newName().trim(),
          endpoint: newEndpoint().trim(),
          ...(newSecret().trim()
            ? { credential: newSecret().trim(), credential_ref: newReference() }
            : {}),
          ...(newHeaderInputs().length > 0 ? { headers: newHeaderInputs() } : {}),
        }
      : null,
  );
  const provider = createMemo(() => providers().find((entry) => entry.id === sourceId()));
  const [models, setModels] = createSignal<ProviderModel[]>([]);
  const [selected, setSelected] = createSignal<Set<string>>(new Set());
  const [query, setQuery] = createSignal("");
  const [revision, setRevision] = createSignal(0);
  const [loading, setLoading] = createSignal(Boolean(provider()?.endpoint));
  const [saving, setSaving] = createSignal(false);
  const [removing, setRemoving] = createSignal<ProfilesSnapshot["providers"][number] | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  const [removeError, setRemoveError] = createSignal<string | null>(null);
  const [result, setResult] = createSignal<string | null>(null);
  const endpoint = createMemo(() =>
    sourceId() === "new" ? newConnection()?.endpoint : provider()?.endpoint,
  );
  const protocol = createMemo(() =>
    sourceId() === "new" ? newConnection()?.protocol : provider()?.protocol,
  );
  const credentialRef = createMemo(() =>
    sourceId() === "new" ? undefined : provider()?.credential_ref,
  );
  const credential = createMemo(() =>
    sourceId() === "new" ? newConnection()?.credential : undefined,
  );
  const headers = createMemo(() => (sourceId() === "new" ? newConnection()?.headers : undefined));
  const savedProfile = createMemo(() =>
    sourceId() === "new" ? undefined : provider()?.profile_ids[0],
  );
  const headersKey = createMemo(() => JSON.stringify(headers() ?? null));
  const canDiscover = createMemo(() => sourceId() !== "new" || newConnection() !== null);
  createEffect(
    () => [
      canDiscover(),
      props.client,
      credential(),
      credentialRef(),
      endpoint(),
      headersKey(),
      protocol(),
      revision(),
      savedProfile(),
      sourceId(),
      i18n.t,
    ],
    () => {
      if (!endpoint() || !canDiscover()) return;
      const controller = new AbortController();
      let active = true;
      const timer = setTimeout(
        () => {
          setLoading(true);
          void props.client
            .discoverProviderModels(
              {
                endpoint: endpoint()!,
                protocol: protocol(),
                credential_ref: credentialRef(),
                credential: credential(),
                ...(headers() ? { headers: headers() } : {}),
                ...(savedProfile() ? { profile_id: savedProfile() } : {}),
              },
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
              if (active) setError(describeError(reason, i18n.t));
            })
            .finally(() => {
              if (active) setLoading(false);
            });
        },
        sourceId() === "new" ? 500 : 0,
      );
      return () => {
        active = false;
        clearTimeout(timer);
        controller.abort();
      };
    },
  );
  const existing = createMemo(() => new Set(provider()?.models ?? []));
  // Other saved Sources with the same Endpoint, by model id. Each Source has its own
  // credentials and terms, so this only informs; it never blocks an import.
  const elsewhere = createMemo(() => {
    const names = new Map<string, string[]>();
    const target = endpoint()?.trim();
    if (!target) return names;
    for (const entry of providers()) {
      if (entry.id === sourceId() || entry.endpoint?.trim() !== target) continue;
      for (const model of entry.models) {
        names.set(model, [...(names.get(model) ?? []), sourceName(entry.name)]);
      }
    }
    return names;
  });
  const importable = createMemo(() => models().filter((model) => !existing().has(model.id)));
  const matching = createMemo(() =>
    models().filter((model) =>
      model.id.toLocaleLowerCase().includes(query().trim().toLocaleLowerCase()),
    ),
  );
  function focusNewSource() {
    setTimeout(() => document.getElementById("import-source-name")?.focus(), 0);
  }
  function selectSource(id: string) {
    setSourceChoice(id);
    setModels([]);
    setSelected(new Set<string>());
    setQuery("");
    setError(null);
    setResult(null);
    setLoading(Boolean(providers().find((entry) => entry.id === id)?.endpoint));
    setRevision((value) => value + 1);
  }
  function refreshSource(id: string) {
    selectSource(id);
  }
  function changeNewConnection(update: () => void) {
    update();
    setModels([]);
    setSelected(new Set<string>());
    setLoading(false);
    setError(null);
    setResult(null);
  }
  async function save() {
    const source = sourceId() === "new" ? newConnection() : provider()?.profile_ids[0];
    if (!source) return;
    setSaving(true);
    setError(null);
    try {
      const chosen = importable().filter((model) => selected().has(model.id));
      const answer = await props.client.importModels(source, chosen, ["chat"]);
      const message =
        answer.existing === 0
          ? i18n.t("settings.llm.importedNamed", {
              created: answer.created,
              names: chosen.map((model) => model.id).join(", "),
            })
          : i18n.t("settings.llm.imported", {
              created: answer.created,
              existing: answer.existing,
            });
      setResult(message);
      setSelected(new Set<string>());
      props.onImported(message);
      if (sourceId() === "new") {
        setNewName("");
        setNewEndpoint("");
        setNewSecret("");
        setNewHeaders([]);
        setModels([]);
      }
    } catch (reason) {
      setError(describeError(reason, i18n.t));
    } finally {
      setSaving(false);
    }
  }
  return (
    <div class="flex min-w-0 flex-col gap-6 md:h-[70dvh] md:min-h-[32rem]">
      <Dialog
        open={removing() !== null}
        onOpenChange={(open) => {
          if (!open && !saving()) setRemoving(null);
        }}
      >
        <DialogContent showCloseButton={!saving()}>
          <DialogHeader>
            <DialogTitle>{i18n.t("settings.llm.removeSourceTitle")}</DialogTitle>
            <DialogDescription>
              {i18n.t("settings.llm.removeSourceDescription", {
                name: removing() ? sourceName(removing()!.name) : "",
              })}
            </DialogDescription>
          </DialogHeader>
          {removeError() && (
            <p role="alert" class="text-sm text-destructive">
              {removeError()}
            </p>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={saving()} onClick={() => setRemoving(null)}>
              {i18n.t("common.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={saving()}
              onClick={() => {
                if (!removing()) return;
                setSaving(true);
                setRemoveError(null);
                void props.client
                  .removeProvider(removing()!.id)
                  .then(() => {
                    if (sourceId() === removing()!.id) selectSource("new");
                    setRemoving(null);
                    props.onImported(i18n.t("settings.llm.sourceRemoved"));
                  })
                  .catch((reason) => setRemoveError(describeError(reason, i18n.t)))
                  .finally(() => setSaving(false));
              }}
            >
              {saving() ? i18n.t("settings.saving") : i18n.t("common.remove")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <div class="grid min-h-0 flex-1 grid-cols-1 gap-5 md:grid-cols-[14rem_minmax(0,1fr)] md:overflow-hidden">
        <div
          role="group"
          aria-label={i18n.t("settings.llm.provider")}
          class="ocg-subtle-scrollbar flex min-h-0 min-w-0 flex-col gap-2 overflow-y-auto overscroll-contain border-b border-border pb-4 md:border-r md:border-b-0 md:pr-4 md:pb-0"
        >
          {providers().map((entry) => {
            const id = entry.id;
            const refreshing = sourceId() === id && loading();
            return (
              <div class="flex min-w-0 items-center gap-1">
                <Button
                  variant={entry === provider() ? "selected" : "outline"}
                  size="sm"
                  aria-pressed={ariaState(entry === provider())}
                  disabled={saving()}
                  class="h-auto min-h-9 min-w-0 flex-1 justify-start py-2 whitespace-normal text-left"
                  onClick={() => {
                    setNewSecret("");
                    selectSource(id);
                  }}
                >
                  <span class="min-w-0 break-words">{sourceName(entry.name)}</span>
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label={i18n.t("settings.llm.fetchModels")}
                  title={i18n.t("settings.llm.fetchModels")}
                  disabled={saving() || refreshing || !entry.endpoint}
                  onClick={() => refreshSource(id)}
                >
                  <RefreshCw class={refreshing ? "animate-spin" : undefined} />
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label={i18n.t("settings.llm.removeSource")}
                  title={i18n.t("settings.llm.removeSource")}
                  disabled={saving()}
                  onClick={() => {
                    setRemoveError(null);
                    setRemoving(entry);
                  }}
                >
                  <Trash2 />
                </Button>
              </div>
            );
          })}
          <Button
            variant={sourceId() === "new" ? "selected" : "outline"}
            size="sm"
            aria-pressed={ariaState(sourceId() === "new")}
            disabled={saving()}
            onClick={() => {
              selectSource("new");
              focusNewSource();
            }}
          >
            {i18n.t("settings.llm.newProvider")}
          </Button>
        </div>
        <div class="flex min-h-0 min-w-0 flex-col gap-3 md:overflow-hidden">
          {sourceId() === "new" && (
            <div class="grid shrink-0 grid-cols-1 gap-3 sm:grid-cols-2">
              <ProtocolSelect
                value={newProtocol()}
                disabled={saving()}
                onChange={(value) => changeNewConnection(() => setNewProtocol(value))}
              />
              <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                {i18n.t("settings.llm.provider")}
                <Input
                  id="import-source-name"
                  value={newName()}
                  disabled={saving()}
                  onInput={(event) => changeNewConnection(() => setNewName(event.target.value))}
                  placeholder="e.g. OpenRouter"
                  autocomplete="off"
                />
              </label>
              <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                {i18n.t("settings.llm.importEndpoint")}
                <Input
                  value={newEndpoint()}
                  disabled={saving()}
                  onInput={(event) => changeNewConnection(() => setNewEndpoint(event.target.value))}
                  placeholder="e.g. https://api.example.com/v1"
                  spellcheck="false"
                  autocomplete="off"
                />
              </label>
              <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
                {i18n.t("settings.llm.apiKey")}
                <ApiKeyInput
                  value={newSecret()}
                  disabled={saving()}
                  onChange={(event) => changeNewConnection(() => setNewSecret(event.target.value))}
                />
              </label>
              <div class="sm:col-span-2">
                <HeadersEditor
                  legend={i18n.t("headers.title")}
                  hint={i18n.t("headers.connectionHint")}
                  value={newHeaders()}
                  onChange={(next) => changeNewConnection(() => setNewHeaders(next))}
                  disabled={saving()}
                  allowSecret
                  error={newHeaderError!()}
                />
              </div>
            </div>
          )}
          {sourceId() !== "new" && provider() && !provider()!.endpoint ? (
            <p class="text-sm text-muted-foreground">
              {i18n.t("settings.llm.discoveryEndpointRequired")}
            </p>
          ) : null}
          <Input
            aria-label={i18n.t("settings.llm.searchModels")}
            placeholder={i18n.t("settings.llm.searchModels")}
            value={query()}
            disabled={saving()}
            onInput={(event) => setQuery(event.target.value)}
          />
          <div class="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <Button
              variant="ghost"
              size="xs"
              disabled={saving() || !selected().size}
              onClick={() => setSelected(new Set<string>())}
            >
              {i18n.t("settings.llm.clearSelection")}
            </Button>
            <Button
              class="ml-auto"
              disabled={saving() || loading() || !selected().size}
              onClick={() => void save()}
            >
              {saving()
                ? i18n.t("settings.llm.importing")
                : i18n.t("settings.llm.importSelected", {
                    selected: selected().size,
                    total: importable().length,
                  })}
            </Button>
          </div>
          <div
            aria-busy={ariaState(loading())}
            class="ocg-subtle-scrollbar max-h-80 min-h-24 min-w-0 flex-1 overflow-y-auto md:max-h-none overscroll-contain rounded-md border border-border"
          >
            {loading() ? (
              <p role="status" class="p-3 text-muted-foreground">
                {i18n.t("settings.llm.fetching")}
              </p>
            ) : matching().length === 0 ? (
              <p class="p-3 text-muted-foreground">{i18n.t("settings.llm.noModels")}</p>
            ) : (
              matching().map((model) => (
                <label class="flex cursor-pointer items-start gap-3 border-b border-border/50 p-3 last:border-b-0 hover:bg-muted/40 has-disabled:cursor-default has-disabled:text-muted-foreground">
                  <input
                    type="checkbox"
                    class="mt-0.5 size-4 shrink-0 accent-primary"
                    checked={existing().has(model.id) || selected().has(model.id)}
                    disabled={saving() || existing().has(model.id)}
                    onChange={() =>
                      setSelected((current) => {
                        const next = new Set(current);
                        if (!next.delete(model.id)) next.add(model.id);
                        return next;
                      })
                    }
                  />
                  <span class="min-w-0 flex-1 break-words">
                    {model.id}
                    <span class="mt-1 block text-xs text-muted-foreground">
                      {i18n.t("chat.effort")}:{" "}
                      {model.reasoning_efforts === null
                        ? i18n.t("settings.llm.effortsUnknown")
                        : model.reasoning_efforts.join(" / ") || i18n.t("chat.effortUnsupported")}
                    </span>
                    {model.context_window ? (
                      <span class="mt-1 block text-xs text-muted-foreground">
                        {model.context_window.toLocaleString()} Token
                      </span>
                    ) : null}
                    {!existing().has(model.id) && elsewhere().get(model.id) ? (
                      <span class="mt-1 block text-xs text-muted-foreground">
                        {i18n.t("settings.llm.addedElsewhere", {
                          names: elsewhere().get(model.id)!.join(", "),
                        })}
                      </span>
                    ) : null}
                  </span>
                  {existing().has(model.id) && (
                    <span class="shrink-0 text-xs">{i18n.t("settings.llm.alreadyAdded")}</span>
                  )}
                </label>
              ))
            )}
          </div>
        </div>
      </div>
      {error() && (
        <p role="alert" class="break-words text-sm text-destructive">
          {error()}
        </p>
      )}
      {result() && (
        <p role="status" class="text-sm text-emerald-700 dark:text-emerald-400">
          {result()}
        </p>
      )}
    </div>
  );
}
