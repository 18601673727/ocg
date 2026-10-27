"use client";

import { useState, type FormEvent } from "react";
import Link from "next/link";
import type { ControlDecisionLimits, ControlProfilesSnapshot } from "../contracts";
import type {
  HttpCanonicalControlClient,
  ControlProfileWriteRequest,
} from "../runtime/control-client";
import { useI18n } from "../i18n";
import { Section, describeError } from "./settings-parts";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";

const emptyLimits: ControlDecisionLimits = {
  max_input_tokens_per_request: null,
  max_output_tokens_per_request: null,
  max_input_tokens_per_question: null,
  max_output_tokens_per_question: null,
};
type ProfileEditorProps = {
  client: HttpCanonicalControlClient;
  profiles: ControlProfilesSnapshot;
  onSaved(profiles: ControlProfilesSnapshot): void;
  onManageModels?(): void;
};

export function DecisionProfilesSection(props: ProfileEditorProps) {
  return <ProfileEditor {...props} kind="decision_agent" />;
}

function ProfileEditor({
  client,
  profiles,
  onSaved,
  onManageModels,
  kind,
}: ProfileEditorProps & { kind: "decision_model" | "decision_agent" }) {
  const { t } = useI18n();
  const [editing, setEditing] = useState<string>();
  const [name, setName] = useState("");
  const [model, setModel] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [credential, setCredential] = useState("");
  const [credentialRef, setCredentialRef] = useState<string | null>(null);
  const [capabilities, setCapabilities] = useState<string[]>(["choice"]);
  const [limits, setLimits] = useState<ControlDecisionLimits>(emptyLimits);
  const [instructions, setInstructions] = useState("");
  const [models, setModels] = useState<string[]>([]);
  const [defaultModel, setDefaultModel] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [deleting, setDeleting] = useState<{ id: string; name: string }>();
  function reset() {
    setEditing(undefined);
    setName("");
    setModel("");
    setEndpoint("");
    setCredential("");
    setCredentialRef(null);
    setCapabilities(["choice"]);
    setLimits(emptyLimits);
    setInstructions("");
    setModels([]);
    setDefaultModel(null);
    setError(undefined);
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(undefined);
    const payload: ControlProfileWriteRequest =
      kind === "decision_model"
        ? {
            kind,
            name: name.trim(),
            model: model.trim(),
            endpoint: endpoint.trim(),
            protocol: "typesafe_systemone",
            declared_capabilities: capabilities,
            limits,
            credential_ref: credentialRef,
            ...(credential ? { credential } : {}),
          }
        : {
            kind,
            name: name.trim(),
            instructions: instructions.trim() || null,
            models,
            default_model: defaultModel,
          };
    try {
      const result = editing
        ? await client.updateProfile(editing, payload)
        : await client.createProfile(payload);
      onSaved(result);
      reset();
    } catch (cause) {
      setError(describeError(cause, t));
    } finally {
      setBusy(false);
      setCredential("");
    }
  }
  const availableModels = [...profiles.llm_profiles, ...profiles.decision_profiles];
  const selectedModels = availableModels.filter((model) => models.includes(model.id));
  return (
    <Section
      id={kind === "decision_model" ? "settings-decision-models" : "settings-decision-profiles"}
      title={t(kind === "decision_model" ? "decision.modelsTitle" : "decision.title")}
      description={t(
        kind === "decision_model" ? "decision.modelsDescription" : "decision.description",
      )}
    >
      <Dialog
        open={!!deleting}
        onOpenChange={(open) => {
          if (!busy && !open) setDeleting(undefined);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t("decision.deleteTitle", { name: deleting?.name ?? "" })}</DialogTitle>
            <DialogDescription>{t("decision.deleteHint")}</DialogDescription>
          </DialogHeader>
          {error && <DismissibleNotice message={error} />}
          <DialogFooter>
            <Button variant="outline" disabled={busy} onClick={() => setDeleting(undefined)}>
              {t("common.cancel")}
            </Button>
            <Button
              variant="destructive"
              disabled={busy}
              onClick={() => {
                if (!deleting) return;
                setBusy(true);
                setError(undefined);
                void client
                  .deleteLlmProfile(deleting.id)
                  .then(
                    (result) => {
                      onSaved(result);
                      if (editing === deleting.id) reset();
                      setDeleting(undefined);
                    },
                    (cause) => setError(describeError(cause, t)),
                  )
                  .finally(() => setBusy(false));
              }}
            >
              {t("common.remove")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <form
        onSubmit={(event) => void save(event)}
        className="mb-2 space-y-3 border-b border-border pb-4"
      >
        <fieldset disabled={busy} className="space-y-3">
          {editing && (
            <Button type="button" variant="ghost" onClick={reset}>
              {t("decision.new")}
            </Button>
          )}
          <label className="grid gap-1 text-xs">
            {t("decision.name")}
            <Input required value={name} onChange={(event) => setName(event.target.value)} />
          </label>
          {kind === "decision_model" ? (
            <>
              <label className="grid gap-1 text-xs">
                {t("decision.modelId")}
                <Input required value={model} onChange={(event) => setModel(event.target.value)} />
              </label>
              <label className="grid gap-1 text-xs">
                {t("decision.endpoint")}
                <Input
                  required
                  type="url"
                  value={endpoint}
                  onChange={(event) => setEndpoint(event.target.value)}
                />
              </label>
              <label className="grid gap-1 text-xs">
                {t("decision.credential")}
                <Input
                  type="password"
                  autoComplete="new-password"
                  value={credential}
                  onChange={(event) => setCredential(event.target.value)}
                />
              </label>
              <fieldset className="space-y-2">
                <legend className="text-xs">{t("decision.capabilities")}</legend>
                {(["choice", "binary_probability", "ordinal_score"] as const).map((capability) => (
                  <label key={capability} className="mr-3 inline-flex items-center gap-1 text-xs">
                    <input
                      type="checkbox"
                      checked={capabilities.includes(capability)}
                      onChange={(event) =>
                        setCapabilities(
                          event.target.checked
                            ? [...capabilities, capability]
                            : capabilities.filter((value) => value !== capability),
                        )
                      }
                    />
                    {t(`decision.${capability}`)}
                  </label>
                ))}
              </fieldset>
              <details>
                <summary className="cursor-pointer text-xs">{t("decision.limits")}</summary>
                <div className="mt-2 grid grid-cols-2 gap-2">
                  {(
                    [
                      "max_input_tokens_per_request",
                      "max_output_tokens_per_request",
                      "max_input_tokens_per_question",
                      "max_output_tokens_per_question",
                    ] as const
                  ).map((field) => (
                    <label key={field} className="grid gap-1 text-xs">
                      {t(`decision.${field}`)}
                      <Input
                        type="number"
                        min={1}
                        max={1_000_000}
                        step={1}
                        value={limits[field] ?? ""}
                        onChange={(event) =>
                          setLimits({
                            ...limits,
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
              <label className="grid gap-1 text-xs">
                {t("decision.instructions")}
                <Textarea
                  value={instructions}
                  onChange={(event) => setInstructions(event.target.value)}
                />
              </label>
              <fieldset className="space-y-2">
                <legend className="text-xs">{t("decision.availableModels")}</legend>
                {availableModels.length === 0 ? (
                  <div className="space-y-2 text-xs">
                    <p className="text-muted-foreground">{t("decision.noModels")}</p>
                    <Link
                      href="/settings/models"
                      className="inline-flex min-h-9 items-center rounded text-primary underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-ring"
                      onClick={(event) => {
                        if (
                          !onManageModels ||
                          event.button !== 0 ||
                          event.metaKey ||
                          event.ctrlKey ||
                          event.shiftKey ||
                          event.altKey
                        )
                          return;
                        event.preventDefault();
                        onManageModels();
                      }}
                    >
                      {t("decision.addModels")}
                    </Link>
                  </div>
                ) : (
                  availableModels.map((model) => (
                    <label key={model.id} className="flex items-center gap-2 text-xs">
                      <input
                        type="checkbox"
                        checked={models.includes(model.id)}
                        onChange={(event) => {
                          setModels(
                            event.target.checked
                              ? [...models, model.id]
                              : models.filter((value) => value !== model.id),
                          );
                          if (defaultModel === model.id && !event.target.checked)
                            setDefaultModel(null);
                        }}
                      />
                      {model.name}
                    </label>
                  ))
                )}
              </fieldset>
              {selectedModels.length > 0 && (
                <label className="grid gap-1 text-xs">
                  {t("profile.defaultModel")}
                  <Select
                    value={defaultModel ?? ""}
                    items={[
                      { value: "", label: t("decision.firstModel") },
                      ...selectedModels.map((model) => ({ value: model.id, label: model.name })),
                    ]}
                    onValueChange={(next) => setDefaultModel(next || null)}
                  >
                    <SelectTrigger className="min-h-10 w-full rounded-md bg-background px-2">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="">{t("decision.firstModel")}</SelectItem>
                      {selectedModels.map((model) => (
                        <SelectItem key={model.id} value={model.id}>
                          {model.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </label>
              )}
              <p className="text-xs text-muted-foreground">{t("decision.inputOnly")}</p>
            </>
          )}
          {error && <DismissibleNotice message={error} />}
          <Button
            type="submit"
            disabled={
              !name.trim() ||
              (kind === "decision_model" ? !capabilities.length : !selectedModels.length)
            }
          >
            {busy ? t("context.saving") : t("common.save")}
          </Button>
        </fieldset>
      </form>
      <div className="divide-y divide-border py-2">
        {kind === "decision_model" &&
          profiles.decision_profiles.map((profile) => (
            <div key={profile.id} className="flex items-center justify-between gap-2 py-2 text-sm">
              <span>
                {profile.name} · {profile.model}
              </span>
              <div className="flex gap-1">
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy || !profile.editable}
                  onClick={() => {
                    reset();
                    setEditing(profile.id);
                    setName(profile.name);
                    setModel(profile.model);
                    setEndpoint(profile.endpoint ?? "");
                    setCredentialRef(profile.credential_ref);
                    setCapabilities(profile.declared_capabilities);
                    setLimits(profile.limits);
                  }}
                >
                  {t("decision.edit")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy || !profile.editable}
                  onClick={() => {
                    setError(undefined);
                    setDeleting({ id: profile.id, name: profile.name });
                  }}
                >
                  {t("common.remove")}
                </Button>
              </div>
            </div>
          ))}
        {kind === "decision_agent" &&
          profiles.decision_agent_profiles.map((profile) => (
            <div key={profile.id} className="flex items-center justify-between gap-2 py-2 text-sm">
              <span>
                {profile.name} · {t("decision.agent")}
              </span>
              <div className="flex gap-1">
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy || !profile.editable}
                  onClick={() => {
                    reset();
                    setEditing(profile.id);
                    setName(profile.name);
                    setInstructions(profile.instructions ?? "");
                    setModels(profile.models);
                    setDefaultModel(profile.default_model);
                  }}
                >
                  {t("decision.edit")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy || !profile.editable}
                  onClick={() => {
                    setError(undefined);
                    setDeleting({ id: profile.id, name: profile.name });
                  }}
                >
                  {t("common.remove")}
                </Button>
              </div>
            </div>
          ))}
      </div>
    </Section>
  );
}
