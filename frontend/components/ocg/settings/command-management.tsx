import { Check } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Select, SelectItem } from "@/components/ui/select";
import type { ProfilesSnapshot, Project } from "../contracts";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";
import { describeError, selectableLlmProfiles } from "./settings-parts";

export function ProjectDecisionConfigButton(props: {
  client: ApiClient;
  project: Project | null;
  disabled: boolean;
  onChanged(project: Project): void;
}) {
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  const [profiles, setProfiles] = createSignal<ProfilesSnapshot | null>(null);
  const [selected, setSelected] = createSignal("");
  const [cutoff, setCutoff] = createSignal("0.6");
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string>();
  const [status, setStatus] = createSignal("");
  createEffect(
    () => [props.client, open(), i18n.t],
    () => {
      if (!open()) return;
      const controller = new AbortController();
      void props.client
        .readProfiles(controller.signal)
        .then((value) => {
          if (!controller.signal.aborted) setProfiles(value);
        })
        .catch((cause) => {
          if (!controller.signal.aborted) setError(describeError(cause, i18n.t));
        });
      return () => controller.abort();
    },
  );

  async function configure() {
    if (!props.project) return;
    setBusy(true);
    setError(undefined);
    try {
      const updated = await props.client.configureProjectDecision(
        props.project.id,
        selected() === "default" || selected() === "off" ? null : selected(),
        selected() === "off"
          ? (props.project.decision?.minimum_confidence ?? 0.6)
          : Number(cutoff()),
        selected() !== "off",
      );
      props.onChanged(updated);
      setStatus(i18n.t(selected() === "off" ? "commands.disabled" : "commands.enabled"));
      setOpen(false);
    } catch (cause) {
      setError(describeError(cause, i18n.t));
    } finally {
      setBusy(false);
    }
  }
  const decisionProfiles = createMemo(() => profiles()?.decision_agent_profiles ?? []);
  const models = createMemo(() => [
    ...selectableLlmProfiles(
      profiles()?.llm_profiles ?? [],
      props.project?.decision?.decision_profile,
    ),
    ...(profiles()?.decision_profiles ?? []),
  ]);
  const choices = createMemo(() => [...decisionProfiles(), ...models()]);
  const confidence = createMemo(() => Number(cutoff()));
  const validCutoff = createMemo(
    () =>
      cutoff().trim() !== "" &&
      Number.isFinite(confidence()) &&
      confidence() >= 0 &&
      confidence() <= 1 &&
      Math.abs(confidence() * 1000 - Math.round(confidence() * 1000)) < 1e-9,
  );
  return (
    <Dialog
      open={open()}
      onOpenChange={(value) => {
        if (value) {
          setSelected(
            props.project?.decision?.enabled === false
              ? "off"
              : (props.project?.decision?.decision_profile ?? "default"),
          );
          setCutoff(String(props.project?.decision?.minimum_confidence ?? 0.6));
          setError(undefined);
          setStatus("");
        }
        setOpen(value);
      }}
    >
      <DialogTrigger
        render={
          <button
            type="button"
            class="flex cursor-pointer items-center gap-2 text-sm disabled:cursor-not-allowed disabled:opacity-50"
          />
        }
        disabled={props.disabled || !props.project}
        title={!props.project ? i18n.t("commands.saveFirst") : undefined}
      >
        <span
          aria-hidden="true"
          class="flex size-4 items-center justify-center rounded-sm border border-input"
        >
          {props.project?.decision?.enabled !== false && <Check class="size-3" />}
        </span>
        <span>{i18n.t("commands.assist")}</span>
        <span class="sr-only">
          {i18n.t(
            props.project?.decision?.enabled !== false
              ? "settings.pools.enabled"
              : "settings.pools.disabled",
          )}
        </span>
      </DialogTrigger>
      <DialogContent class="grid-cols-[minmax(0,1fr)] max-w-lg">
        <DialogHeader>
          <DialogTitle>{i18n.t("commands.manage")}</DialogTitle>
          <DialogDescription>{i18n.t("commands.description")}</DialogDescription>
        </DialogHeader>
        <p class="text-sm text-muted-foreground">{i18n.t("commands.scope")}</p>
        <label class="flex flex-col gap-1 text-sm">
          {i18n.t("commands.profile")}
          <Select
            value={selected()}
            items={[
              { value: "default", label: i18n.t("commands.default") },
              { value: "off", label: i18n.t("commands.disable") },
              ...models().map((model) => ({
                value: model.id,
                label:
                  "enabled" in model && !model.enabled
                    ? `${model.name} · ${model.model} (${i18n.t("settings.llm.disabled")})`
                    : `${model.name} · ${model.model}`,
              })),
              ...decisionProfiles().map((profile) => ({
                value: profile.id,
                label: profile.name,
              })),
            ]}
            disabled={busy()}
            onValueChange={(next) => {
              if (next !== null) setSelected(next);
            }}
            class="h-10 w-full min-w-0 rounded-md border-input bg-background px-3"
          >
            <SelectItem value="default">{i18n.t("commands.default")}</SelectItem>
            <SelectItem value="off">{i18n.t("commands.disable")}</SelectItem>
            <optgroup label={i18n.t("settings.nav.models")}>
              {models().map((model) => (
                <SelectItem value={model.id} disabled={"enabled" in model && !model.enabled}>
                  {model.name} · {model.model}
                  {"enabled" in model && !model.enabled && ` (${i18n.t("settings.llm.disabled")})`}
                </SelectItem>
              ))}
            </optgroup>
            <optgroup label={i18n.t("decision.title")}>
              {decisionProfiles().map((profile) => (
                <SelectItem value={profile.id}>{profile.name}</SelectItem>
              ))}
            </optgroup>
          </Select>
        </label>
        <label class="flex flex-col gap-1 text-sm">
          {i18n.t("commands.cutoff")}
          <Input
            type="number"
            min="0"
            max="1"
            step="0.001"
            value={cutoff()}
            disabled={busy() || selected() === "off"}
            onInput={(event) => setCutoff(event.target.value)}
          />
        </label>
        <p class="text-xs text-muted-foreground">{i18n.t("commands.cutoffHelp")}</p>
        {status() && (
          <p role="status" class="text-sm text-muted-foreground">
            {status()}
          </p>
        )}
        {error() && (
          <p role="alert" class="text-sm text-destructive">
            {error()}
          </p>
        )}
        <DialogFooter>
          <Button
            type="button"
            disabled={
              busy() ||
              (selected() !== "off" && !validCutoff()) ||
              (selected() !== "default" &&
                selected() !== "off" &&
                !choices().some((profile) => profile.id === selected()))
            }
            onClick={() => void configure()}
          >
            {i18n.t(busy() ? "commands.saving" : "commands.enable")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
