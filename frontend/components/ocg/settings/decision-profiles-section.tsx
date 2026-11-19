import { createMemo, createSignal } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
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
import { Select, SelectItem } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { type FormEvent } from "@/lib/ui-state";
import type { ProfilesSnapshot } from "../contracts";
import type { DecisionLimits as DecisionLimits } from "../contracts/api";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import type { ApiClient, ProfileWriteRequest } from "../runtime/api-client";
import { isProfileConflict } from "../runtime/api-client";
import {
  draftsToInputs,
  type HeaderDraft,
  type HeaderDraftError,
  headersToDrafts,
} from "./header-drafts";
import { HeadersEditor } from "./headers-editor";
import { ApiKeyInput, describeError, Section, selectableLlmProfiles } from "./settings-parts";

const emptyLimits: DecisionLimits = {
  max_input_tokens_per_request: null,
  max_output_tokens_per_request: null,
  max_input_tokens_per_question: null,
  max_output_tokens_per_question: null,
};
type ProfileEditorProps = {
  client: ApiClient;
  profiles: ProfilesSnapshot;
  onSaved(profiles: ProfilesSnapshot): void;
  onManageModels?(): void;
};

export function DecisionProfilesSection(props: ProfileEditorProps) {
  return <ProfileEditor {...props} kind="decision_agent" />;
}

function ProfileEditor(props: ProfileEditorProps & { kind: "decision_model" | "decision_agent" }) {
  const i18n = useI18n();
  const [editing, setEditing] = createSignal<string>();
  const [editingSeq, setEditingSeq] = createSignal<number>();
  const [name, setName] = createSignal("");
  const [model, setModel] = createSignal("");
  const [endpoint, setEndpoint] = createSignal("");
  const [credential, setCredential] = createSignal("");
  const [credentialRef, setCredentialRef] = createSignal<string | null>(null);
  const [headers, setHeaders] = createSignal<HeaderDraft[]>([]);
  const [headerError, setHeaderError] = createSignal<HeaderDraftError>();
  const [capabilities, setCapabilities] = createSignal<string[]>(["choice"]);
  const [limits, setLimits] = createSignal<DecisionLimits>(emptyLimits);
  const [instructions, setInstructions] = createSignal("");
  const [models, setModels] = createSignal<string[]>([]);
  const [defaultModel, setDefaultModel] = createSignal<string | null>(null);
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string>();
  const [deleting, setDeleting] = createSignal<{ id: string; name: string }>();
  function reset() {
    setEditing(undefined);
    setEditingSeq(undefined);
    setName("");
    setModel("");
    setEndpoint("");
    setCredential("");
    setCredentialRef(null);
    setHeaders([]);
    setHeaderError(undefined);
    setCapabilities(["choice"]);
    setLimits(emptyLimits);
    setInstructions("");
    setModels([]);
    setDefaultModel(null);
    setError(undefined);
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    const headerRequest = draftsToInputs(headers());
    if ("error" in headerRequest) {
      setHeaderError(headerRequest.error);
      return;
    }
    setHeaderError(undefined);
    setBusy(true);
    setError(undefined);
    const payload: ProfileWriteRequest =
      props.kind === "decision_model"
        ? {
            kind: props.kind,
            name: name().trim(),
            model: model().trim(),
            endpoint: endpoint().trim(),
            protocol: "typesafe_systemone",
            declared_capabilities: capabilities(),
            limits: limits(),
            credential_ref: credentialRef(),
            headers: headerRequest.inputs,
            ...(editingSeq() === undefined ? {} : { expected_edit_seq: editingSeq() }),
            ...(credential() ? { credential: credential() } : {}),
          }
        : {
            kind: props.kind,
            name: name().trim(),
            instructions: instructions().trim() || null,
            models: models(),
            default_model: defaultModel(),
            ...(editingSeq() === undefined ? {} : { expected_edit_seq: editingSeq() }),
          };
    try {
      const result = editing()
        ? await props.client.updateProfile(editing()!, payload)
        : await props.client.createProfile(payload);
      props.onSaved(result);
      reset();
    } catch (cause) {
      if (isProfileConflict(cause) && editing()) {
        setError(
          i18n.t(
            props.kind === "decision_agent" ? "settings.agent.conflict" : "settings.llm.conflict",
          ),
        );
        try {
          const latest = await props.client.readProfiles();
          props.onSaved(latest);
          const current = [...latest.decision_profiles, ...latest.decision_agent_profiles].find(
            (profile) => profile.id === editing(),
          );
          if (current) setEditingSeq(current.edit_seq);
        } catch {}
      } else {
        setError(describeError(cause, i18n.t));
      }
    } finally {
      setBusy(false);
      setCredential("");
    }
  }
  const availableModels = createMemo(() => [
    ...selectableLlmProfiles(props.profiles.llm_profiles, ...models()),
    ...props.profiles.decision_profiles,
  ]);
  const selectedModels = createMemo(() =>
    availableModels().filter((model) => models().includes(model.id)),
  );
  return (
    <Section
      id={
        props.kind === "decision_model" ? "settings-decision-models" : "settings-decision-profiles"
      }
      title={i18n.t(props.kind === "decision_model" ? "decision.modelsTitle" : "decision.title")}
      description={i18n.t(
        props.kind === "decision_model" ? "decision.modelsDescription" : "decision.description",
      )}
    >
      <Dialog
        open={!!deleting()}
        onOpenChange={(open) => {
          if (!busy() && !open) setDeleting(undefined);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {i18n.t("decision.deleteTitle", { name: deleting()?.name ?? "" })}
            </DialogTitle>
            <DialogDescription>{i18n.t("decision.deleteHint")}</DialogDescription>
          </DialogHeader>
          {error() && <DismissibleNotice message={error()!} />}
          <DialogFooter>
            <Button variant="outline" disabled={busy()} onClick={() => setDeleting(undefined)}>
              {i18n.t("common.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={busy()}
              onClick={() => {
                if (!deleting()) return;
                setBusy(true);
                setError(undefined);
                void props.client
                  .deleteLlmProfile(deleting()!.id)
                  .then(
                    (result) => {
                      props.onSaved(result);
                      if (editing() === deleting()!.id) reset();
                      setDeleting(undefined);
                    },
                    (cause) => setError(describeError(cause, i18n.t)),
                  )
                  .finally(() => setBusy(false));
              }}
            >
              {i18n.t("common.remove")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <form
        onSubmit={(event) => void save(event)}
        class="mb-2 space-y-3 border-b border-border pb-4"
      >
        <fieldset disabled={busy()} class="space-y-3">
          {editing() && (
            <Button type="button" variant="ghost" onClick={reset}>
              {i18n.t("decision.new")}
            </Button>
          )}
          <label class="grid gap-1 text-xs">
            {i18n.t("decision.name")}
            <Input required value={name()} onInput={(event) => setName(event.target.value)} />
          </label>
          {props.kind === "decision_model" ? (
            <>
              <label class="grid gap-1 text-xs">
                {i18n.t("decision.modelId")}
                <Input required value={model()} onInput={(event) => setModel(event.target.value)} />
              </label>
              <label class="grid gap-1 text-xs">
                {i18n.t("decision.endpoint")}
                <Input
                  required
                  type="url"
                  value={endpoint()}
                  onInput={(event) => setEndpoint(event.target.value)}
                />
              </label>
              <label class="grid gap-1 text-xs">
                {i18n.t("decision.credential")}
                <ApiKeyInput
                  value={credential()}
                  onChange={(event) => setCredential(event.target.value)}
                />
              </label>
              <HeadersEditor
                legend={i18n.t("headers.title")}
                hint={i18n.t("headers.connectionHint")}
                value={headers()}
                onChange={setHeaders}
                disabled={busy()}
                allowSecret
                error={headerError()}
              />
              <fieldset class="space-y-2">
                <legend class="text-xs">{i18n.t("decision.capabilities")}</legend>
                {(["choice", "binary_probability", "ordinal_score"] as const).map((capability) => (
                  <label class="mr-3 inline-flex items-center gap-1 text-xs">
                    <input
                      type="checkbox"
                      checked={capabilities().includes(capability)}
                      onChange={(event) =>
                        setCapabilities(
                          event.target.checked
                            ? [...capabilities(), capability]
                            : capabilities().filter((value) => value !== capability),
                        )
                      }
                    />
                    {i18n.t(`decision.${capability}`)}
                  </label>
                ))}
              </fieldset>
              <details>
                <summary class="cursor-pointer text-xs">{i18n.t("decision.limits")}</summary>
                <div class="mt-2 grid grid-cols-2 gap-2">
                  {(
                    [
                      "max_input_tokens_per_request",
                      "max_output_tokens_per_request",
                      "max_input_tokens_per_question",
                      "max_output_tokens_per_question",
                    ] as const
                  ).map((field) => (
                    <label class="grid gap-1 text-xs">
                      {i18n.t(`decision.${field}`)}
                      <Input
                        type="number"
                        min={1}
                        max={1_000_000}
                        step={1}
                        value={limits()[field] ?? ""}
                        onInput={(event) =>
                          setLimits({
                            ...limits(),
                            [field]: event.target.value ? event.currentTarget.valueAsNumber : null,
                          })
                        }
                      />
                    </label>
                  ))}
                </div>
              </details>
            </>
          ) : (
            <>
              <label class="grid gap-1 text-xs">
                {i18n.t("decision.instructions")}
                <Textarea
                  value={instructions()}
                  onInput={(event) => setInstructions(event.target.value)}
                />
              </label>
              <fieldset class="space-y-2">
                <legend class="text-xs">{i18n.t("decision.availableModels")}</legend>
                {availableModels().length === 0 ? (
                  <div class="space-y-2 text-xs">
                    <p class="text-muted-foreground">{i18n.t("decision.noModels")}</p>
                    <WorkspaceLink
                      href="/settings/models"
                      class="inline-flex min-h-9 items-center rounded text-primary underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-ring"
                      onClick={(event) => {
                        if (
                          !props.onManageModels ||
                          event.button !== 0 ||
                          event.metaKey ||
                          event.ctrlKey ||
                          event.shiftKey ||
                          event.altKey
                        )
                          return;
                        event.preventDefault();
                        props.onManageModels();
                      }}
                    >
                      {i18n.t("decision.addModels")}
                    </WorkspaceLink>
                  </div>
                ) : (
                  availableModels().map((model) => (
                    <label class="flex items-center gap-2 text-xs">
                      <input
                        type="checkbox"
                        checked={models().includes(model.id)}
                        disabled={
                          "enabled" in model && !model.enabled && !models().includes(model.id)
                        }
                        onChange={(event) => {
                          setModels(
                            event.target.checked
                              ? [...models(), model.id]
                              : models().filter((value) => value !== model.id),
                          );
                          if (defaultModel() === model.id && !event.target.checked)
                            setDefaultModel(null);
                        }}
                      />
                      {model.name}
                      {"enabled" in model &&
                        !model.enabled &&
                        ` (${i18n.t("settings.llm.disabled")})`}
                    </label>
                  ))
                )}
              </fieldset>
              {selectedModels().length > 0 && (
                <label class="grid gap-1 text-xs">
                  {i18n.t("profile.defaultModel")}
                  <Select
                    value={defaultModel() ?? ""}
                    items={[
                      { value: "", label: i18n.t("decision.firstModel") },
                      ...selectedModels().map((model) => ({
                        value: model.id,
                        label:
                          "enabled" in model && !model.enabled
                            ? `${model.name} (${i18n.t("settings.llm.disabled")})`
                            : model.name,
                      })),
                    ]}
                    onValueChange={(next) => setDefaultModel(next || null)}
                    class="min-h-10 w-full rounded-md bg-background px-2"
                  >
                    <SelectItem value="">{i18n.t("decision.firstModel")}</SelectItem>
                    {selectedModels().map((model) => (
                      <SelectItem value={model.id} disabled={"enabled" in model && !model.enabled}>
                        {model.name}
                        {"enabled" in model &&
                          !model.enabled &&
                          ` (${i18n.t("settings.llm.disabled")})`}
                      </SelectItem>
                    ))}
                  </Select>
                </label>
              )}
              <p class="text-xs text-muted-foreground">{i18n.t("decision.inputOnly")}</p>
            </>
          )}
          {error() && <DismissibleNotice message={error()!} />}
          <Button
            type="submit"
            disabled={
              !name().trim() ||
              (props.kind === "decision_model" ? !capabilities().length : !selectedModels().length)
            }
          >
            {busy() ? i18n.t("context.saving") : i18n.t("common.save")}
          </Button>
        </fieldset>
      </form>
      <div class="divide-y divide-border py-2">
        {props.kind === "decision_model" &&
          props.profiles.decision_profiles.map((profile) => (
            <div class="flex items-center justify-between gap-2 py-2 text-sm">
              <span>
                {profile.name} · {profile.model}
              </span>
              <div class="flex gap-1">
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy() || !profile.editable}
                  onClick={() => {
                    reset();
                    setEditing(profile.id);
                    setEditingSeq(profile.edit_seq);
                    setName(profile.name);
                    setModel(profile.model);
                    setEndpoint(profile.endpoint ?? "");
                    setCredentialRef(profile.credential_ref);
                    setHeaders(headersToDrafts(profile.headers));
                    setCapabilities(profile.declared_capabilities);
                    setLimits(profile.limits);
                  }}
                >
                  {i18n.t("decision.edit")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy() || !profile.editable}
                  onClick={() => {
                    setError(undefined);
                    setDeleting({ id: profile.id, name: profile.name });
                  }}
                >
                  {i18n.t("common.remove")}
                </Button>
              </div>
            </div>
          ))}
        {props.kind === "decision_agent" &&
          props.profiles.decision_agent_profiles.map((profile) => (
            <div class="flex items-center justify-between gap-2 py-2 text-sm">
              <span>
                {profile.name} · {i18n.t("decision.agent")}
              </span>
              <div class="flex gap-1">
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy() || !profile.editable}
                  onClick={() => {
                    reset();
                    setEditing(profile.id);
                    setEditingSeq(profile.edit_seq);
                    setName(profile.name);
                    setInstructions(profile.instructions ?? "");
                    setModels(profile.models);
                    setDefaultModel(profile.default_model);
                  }}
                >
                  {i18n.t("decision.edit")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy() || !profile.editable}
                  onClick={() => {
                    setError(undefined);
                    setDeleting({ id: profile.id, name: profile.name });
                  }}
                >
                  {i18n.t("common.remove")}
                </Button>
              </div>
            </div>
          ))}
      </div>
    </Section>
  );
}
